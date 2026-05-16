"""CSV upload + Spark ingest dispatch.

v0.4 scope: file save + preview is functional. The actual Spark dispatch is
a stub — it records an IngestJob and immediately marks it failed with a
"v0.4.1" note. Wiring docker exec into spark-submit requires an upstream
change in the UDP repo (a `ingest_csv.py` Spark job file). That ships
separately and is intentionally not added here so this PR stays additive.

TODO (v0.4.1):
  - Add `udp/scripts/ingest_csv.py` to the UDP repo (Spark job that reads
    the uploaded CSV from a bind-mounted volume and writes the Iceberg table
    via the REST catalog).
  - Replace the kick_off_csv_ingest stub with an asyncio task that:
      1. docker cp / bind-mount the upload path into the spark container
      2. docker exec udp-spark-master spark-submit /opt/scripts/ingest_csv.py ...
      3. tail stdout/stderr into the install's event bus (kind="log", step="ingest")
      4. on success, set IngestJob.state="success" + rows_written from the Spark counter
      5. on failure, capture the last 200 stderr lines into IngestJob.error
"""
from __future__ import annotations
import asyncio
import csv
import io
import json
import os
import re
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, BinaryIO, Literal, Optional

from pydantic import BaseModel, Field

from .config import WORK_DIR


# ---------- Upload sizing / paths ----------

_DEFAULT_MAX_UPLOAD_MB = 500


def _max_upload_bytes() -> int:
    raw = os.environ.get("LHS_UPLOAD_MAX_MB", str(_DEFAULT_MAX_UPLOAD_MB))
    try:
        mb = int(raw)
    except ValueError:
        mb = _DEFAULT_MAX_UPLOAD_MB
    return max(1, mb) * 1024 * 1024


UPLOAD_CHUNK_BYTES = 1024 * 1024  # 1 MB

_INGEST_JOBS_FILE = WORK_DIR / "ingest_jobs.json"
_UPLOADS_DIR = WORK_DIR / "uploads"

# Loose filename guard so we can't be talked into traversing out of the upload root.
_SAFE_FILENAME_RE = re.compile(r"^[A-Za-z0-9._\-\(\) ]+$")


class UploadTooLargeError(ValueError):
    pass


class UploadInvalidError(ValueError):
    pass


def _safe_filename(raw: str) -> str:
    name = Path(raw).name.strip() or "upload.csv"
    if not _SAFE_FILENAME_RE.match(name):
        # Strip everything outside the safe set; fall back to a generic name.
        cleaned = "".join(c for c in name if _SAFE_FILENAME_RE.match(c))
        name = cleaned or "upload.csv"
    if len(name) > 200:
        name = name[-200:]
    return name


async def save_csv_upload(
    install_id: str,
    upload_id: str,
    file_stream: BinaryIO,
    filename: str,
) -> Path:
    """Stream an uploaded CSV to disk under WORK_DIR/uploads/{install_id}/{upload_id}/.

    Enforces the hard size cap WHILE streaming so we never page > LHS_UPLOAD_MAX_MB
    into memory. Atomic: writes to a .part file then renames.
    """
    max_bytes = _max_upload_bytes()
    safe_name = _safe_filename(filename)
    dest_dir = _UPLOADS_DIR / install_id / upload_id
    dest_dir.mkdir(parents=True, exist_ok=True)
    final_path = dest_dir / safe_name
    tmp_path = final_path.with_suffix(final_path.suffix + ".part")

    total = 0
    # file_stream may be sync (UploadFile.file) or async — handle the common sync case
    # since Starlette's UploadFile exposes a sync read on its underlying SpooledTemporaryFile.
    try:
        with tmp_path.open("wb") as out:
            while True:
                chunk = file_stream.read(UPLOAD_CHUNK_BYTES)
                if asyncio.iscoroutine(chunk):
                    chunk = await chunk
                if not chunk:
                    break
                total += len(chunk)
                if total > max_bytes:
                    out.close()
                    tmp_path.unlink(missing_ok=True)
                    raise UploadTooLargeError(
                        f"upload exceeds cap of {max_bytes // (1024*1024)} MB"
                    )
                out.write(chunk)
        os.replace(tmp_path, final_path)
    except UploadTooLargeError:
        raise
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise
    return final_path


# ---------- CSV preview / type inference ----------

_BOOL_TRUE = {"true", "t", "yes", "y", "1"}
_BOOL_FALSE = {"false", "f", "no", "n", "0"}
_DATETIME_FORMATS = (
    "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S",
    "%Y/%m/%d", "%d/%m/%Y", "%m/%d/%Y",
)


def _looks_like_int(s: str) -> bool:
    if not s:
        return False
    if s[0] in "+-":
        s = s[1:]
    return s.isdigit()


def _looks_like_float(s: str) -> bool:
    try:
        float(s)
        return True
    except ValueError:
        return False


def _looks_like_bool(s: str) -> bool:
    return s.lower() in _BOOL_TRUE or s.lower() in _BOOL_FALSE


def _looks_like_datetime(s: str) -> bool:
    for fmt in _DATETIME_FORMATS:
        try:
            datetime.strptime(s, fmt)
            return True
        except ValueError:
            continue
    return False


def _infer_column_type(values: list[str]) -> str:
    """Walk a column's sample values; return the narrowest type that fits all."""
    non_null = [v for v in values if v is not None and v != ""]
    if not non_null:
        return "string"
    if all(_looks_like_int(v) for v in non_null):
        return "int"
    if all(_looks_like_float(v) for v in non_null):
        return "double"
    if all(_looks_like_bool(v) for v in non_null):
        return "boolean"
    if all(_looks_like_datetime(v) for v in non_null):
        return "timestamp"
    return "string"


def preview_csv(file_path: Path, sample_rows: int = 1000) -> dict:
    """Sniff delimiter + encoding, read up to sample_rows, infer types per column."""
    if not file_path.exists():
        raise UploadInvalidError(f"upload not found: {file_path}")

    # Try utf-8 first, fall back to latin-1 so we never explode on a stray byte.
    encoding = "utf-8"
    try:
        with file_path.open("r", encoding="utf-8", newline="") as f:
            sniff_sample = f.read(64 * 1024)
    except UnicodeDecodeError:
        encoding = "latin-1"
        with file_path.open("r", encoding="latin-1", newline="") as f:
            sniff_sample = f.read(64 * 1024)

    if not sniff_sample.strip():
        raise UploadInvalidError("uploaded file is empty")

    try:
        dialect = csv.Sniffer().sniff(sniff_sample, delimiters=",;\t|")
        delimiter = dialect.delimiter
    except csv.Error:
        delimiter = ","

    rows: list[list[str]] = []
    headers: list[str] = []
    with file_path.open("r", encoding=encoding, newline="") as f:
        reader = csv.reader(f, delimiter=delimiter)
        for i, row in enumerate(reader):
            if i == 0:
                headers = [c.strip() or f"col_{idx}" for idx, c in enumerate(row)]
                continue
            rows.append(row)
            if len(rows) >= sample_rows:
                break

    if not headers:
        raise UploadInvalidError("could not parse header row")

    # De-dup headers (Iceberg won't accept duplicate column names).
    seen: dict[str, int] = {}
    deduped: list[str] = []
    for h in headers:
        if h in seen:
            seen[h] += 1
            deduped.append(f"{h}_{seen[h]}")
        else:
            seen[h] = 0
            deduped.append(h)
    headers = deduped

    columns: list[dict] = []
    for idx, name in enumerate(headers):
        col_values = [r[idx] if idx < len(r) else "" for r in rows]
        inferred = _infer_column_type(col_values)
        nullable = any(v is None or v == "" for v in col_values) or not rows
        sample_values = [v for v in col_values[:5]]
        columns.append({
            "name": name,
            "inferred_type": inferred,
            "sample_values": sample_values,
            "nullable": nullable,
        })

    return {
        "columns": columns,
        "row_sample": rows[:20],
        "detected_delimiter": delimiter,
        "detected_encoding": encoding,
        "total_lines_sampled": len(rows),
    }


# ---------- IngestJob registry ----------

IngestState = Literal["pending", "preview", "running", "success", "failed"]
IngestKind = Literal["csv"]


class IngestJob(BaseModel):
    job_id: str
    install_id: str
    kind: IngestKind
    state: IngestState
    target: dict = Field(default_factory=dict)  # {database, table}
    source: dict = Field(default_factory=dict)  # {upload_id, filename, path}
    created_at: float
    updated_at: float
    error: Optional[str] = None
    rows_written: Optional[int] = None


_INGEST_JOBS: dict[str, IngestJob] = {}
_INGEST_LOCK = threading.RLock()
_INGEST_DIRTY = False
_INGEST_FLUSH_TIMER: Optional[threading.Timer] = None
_INGEST_WRITE_DEBOUNCE_SEC = 0.25


def _ingest_atomic_write(data: str) -> None:
    _INGEST_JOBS_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = _INGEST_JOBS_FILE.with_suffix(_INGEST_JOBS_FILE.suffix + ".tmp")
    tmp.write_text(data, encoding="utf-8")
    for _ in range(5):
        try:
            os.replace(tmp, _INGEST_JOBS_FILE)
            return
        except PermissionError:
            time.sleep(0.1)
    # Last-ditch: leave the tmp file so the data isn't lost.


def _persist_ingest_locked(*, force: bool = False) -> None:
    global _INGEST_DIRTY, _INGEST_FLUSH_TIMER
    _INGEST_DIRTY = True
    if force:
        if _INGEST_FLUSH_TIMER is not None:
            _INGEST_FLUSH_TIMER.cancel()
            _INGEST_FLUSH_TIMER = None
        _write_ingest_now_locked()
        return
    if _INGEST_FLUSH_TIMER is None:
        _INGEST_FLUSH_TIMER = threading.Timer(_INGEST_WRITE_DEBOUNCE_SEC, _flush_ingest_from_timer)
        _INGEST_FLUSH_TIMER.daemon = True
        _INGEST_FLUSH_TIMER.start()


def _write_ingest_now_locked() -> None:
    global _INGEST_DIRTY
    payload = {jid: j.model_dump() for jid, j in _INGEST_JOBS.items()}
    _ingest_atomic_write(json.dumps(payload, indent=2))
    _INGEST_DIRTY = False


def _flush_ingest_from_timer() -> None:
    global _INGEST_FLUSH_TIMER
    with _INGEST_LOCK:
        _INGEST_FLUSH_TIMER = None
        if _INGEST_DIRTY:
            try:
                _write_ingest_now_locked()
            except Exception:
                import logging
                logging.getLogger("lhs.ingest").exception("ingest flush failed")


def _load_ingest_jobs() -> None:
    if not _INGEST_JOBS_FILE.exists():
        return
    try:
        raw = json.loads(_INGEST_JOBS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return
    for jid, data in raw.items():
        try:
            _INGEST_JOBS[jid] = IngestJob(**data)
        except Exception:
            continue


_load_ingest_jobs()


def list_jobs(install_id: str) -> list[IngestJob]:
    with _INGEST_LOCK:
        return sorted(
            (j for j in _INGEST_JOBS.values() if j.install_id == install_id),
            key=lambda j: j.created_at,
            reverse=True,
        )


def get_job(job_id: str) -> Optional[IngestJob]:
    with _INGEST_LOCK:
        return _INGEST_JOBS.get(job_id)


async def kick_off_csv_ingest(
    install_id: str,
    upload_id: str,
    schema_confirm: list[dict],
    target: dict,
    source: Optional[dict] = None,
) -> IngestJob:
    """Stub: register the job and mark it failed pending v0.4.1 Spark wiring.

    The preview/save path is fully functional — this just doesn't dispatch
    the actual write. See module docstring for the v0.4.1 plan.
    """
    if not isinstance(target, dict) or not target.get("database") or not target.get("table"):
        raise UploadInvalidError("target must include {database, table}")

    now = time.time()
    job_id = f"ing_{uuid.uuid4().hex[:10]}"
    job = IngestJob(
        job_id=job_id,
        install_id=install_id,
        kind="csv",
        state="failed",
        target={"database": str(target["database"]), "table": str(target["table"])},
        source={
            "upload_id": upload_id,
            "schema_columns": [c.get("name") for c in (schema_confirm or [])],
            **(source or {}),
        },
        created_at=now,
        updated_at=now,
        error="Spark ingest dispatch is v0.4.1 — schema preview is functional",
        rows_written=None,
    )
    with _INGEST_LOCK:
        _INGEST_JOBS[job_id] = job
        _persist_ingest_locked(force=True)
    return job
