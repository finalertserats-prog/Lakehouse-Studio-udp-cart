"""CSV upload + Spark ingest dispatch.

v0.5 scope: file save + preview AND the actual Spark dispatch are functional.

Flow:
  1. `save_csv_upload()` streams an UploadFile to WORK_DIR/uploads/{install_id}/
     {upload_id}/{filename} with a hard size cap.
  2. `preview_csv()` sniffs the delimiter, infers per-column types from a
     sample, and returns a schema preview to the UI.
  3. `kick_off_csv_ingest()` registers an IngestJob (pending) and launches a
     background asyncio task that:
        a. `docker cp` the host CSV into the udp-minio-client container
        b. `mc cp` it into `s3://datalake/_staging/{install_id}/{job_id}/file.csv`
        c. writes the Spark job script (`_STUDIO_CSV_JOB_PY`) to the
           udp-spark container at `/tmp/ingest_{job_id}.py` via stdin
        d. `spark-submit` the job; streams stdout/stderr to the install's
           event bus (kind="log", step="ingest")
        e. parses the final `ROWS_WRITTEN=<n>` line and marks the job
           success (+ rows_written) or failed (+ last stderr lines)

No UDP-repo changes required — the Spark job script lives entirely as a
Python string constant here. The spark container already has its `udp`
REST catalog configured by `runner._patch_spark_defaults`.
"""
from __future__ import annotations
import asyncio
import csv
import io
import json
import logging
import os
import re
import shutil
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, BinaryIO, Literal, Optional

from pydantic import BaseModel, Field

from .config import WORK_DIR
from .events import bus
from .models import LogEvent
from .state import store

_log = logging.getLogger("lhs.ingest")


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
IngestKind = Literal["csv", "postgres", "mysql"]


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


# ---------- Spark dispatch ----------

# Container names match the running UDP stack (compose project prefix `udp-`
# plus the manifest's service_name). See backend/structured_smoke.py:209 and
# backend/backup.py:40 for the matching constants used elsewhere.
_SPARK_CONTAINER = "udp-spark"
_MINIO_CLIENT_CONTAINER = "udp-minio-client"

# How long the spark-submit may run before we kill it. CSV ingest of a 500MB
# file on a single Spark executor on a laptop comfortably fits inside 30 min;
# if the user hits the cap we surface a clear timeout error rather than
# orphaning the subprocess.
_SPARK_SUBMIT_TIMEOUT_SEC = 1800
_DOCKER_CP_TIMEOUT_SEC = 600
_MC_CP_TIMEOUT_SEC = 600
_DOCKER_QUICK_TIMEOUT_SEC = 30

# Where in the staging bucket we drop the uploaded CSV. The Spark job reads
# from here via s3a:// (hadoop-aws is bundled in the spark-iceberg image).
_STAGING_PREFIX = "_staging"
_STAGING_BUCKET = "datalake"
_MC_ALIAS = "minio"

# Strict identifier regex for Iceberg database/table names so we can splice
# the target into the spark-submit argv without shell-quoting concerns.
_IDENT_SAFE_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]{0,127}$")

# Iceberg types we accept from the schema preview / user confirmation. The
# Spark job maps these into pyspark.sql.types. Anything else falls back to
# string (matching the inference fallback in _infer_column_type).
_ALLOWED_ICEBERG_TYPES = frozenset({
    "string", "int", "long", "double", "float", "boolean", "timestamp", "date",
})


# The Spark job script. Written into the spark container at /tmp/ingest_<id>.py
# and executed via spark-submit. Reads the staged CSV from s3a, applies the
# user-confirmed schema, writes via the `udp` REST catalog already configured
# in spark-defaults.conf (see runner._patch_spark_defaults).
#
# Final line of stdout MUST be `ROWS_WRITTEN=<n>` — the backend parses this to
# populate IngestJob.rows_written. We also print `ROWS_WRITTEN=0` on the empty-
# file path so the parser always finds a value.
_STUDIO_CSV_JOB_PY = r'''#!/usr/bin/env python3
"""Studio CSV-to-Iceberg ingest job. Generated/dispatched by backend.ingest.

Usage:
  spark-submit /tmp/ingest_<id>.py <s3a_source> --target <db.table> --schema <json>

The --schema argument is a JSON-encoded list of {"name", "type", "nullable"}
column dicts. Types map to pyspark.sql.types per _TYPE_MAP below.
"""
import argparse
import json
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, DoubleType, FloatType,
    BooleanType, TimestampType, DateType,
)


_TYPE_MAP = {
    "string": StringType(),
    "int": IntegerType(),
    "long": LongType(),
    "double": DoubleType(),
    "float": FloatType(),
    "boolean": BooleanType(),
    "timestamp": TimestampType(),
    "date": DateType(),
}


def _build_schema(cols):
    fields = []
    for c in cols:
        name = c["name"]
        t = _TYPE_MAP.get(str(c.get("type", "string")).lower(), StringType())
        nullable = bool(c.get("nullable", True))
        fields.append(StructField(name, t, nullable))
    return StructType(fields)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("source", help="s3a:// path to the staged CSV")
    parser.add_argument("--target", required=True,
                        help="db.table — Iceberg target in the `udp` catalog")
    parser.add_argument("--schema", required=True,
                        help="JSON list of {name,type,nullable} column dicts")
    parser.add_argument("--header", default="true",
                        help="CSV has header row (default: true)")
    parser.add_argument("--delimiter", default=",",
                        help="CSV delimiter (default: ,)")
    args = parser.parse_args()

    try:
        cols = json.loads(args.schema)
    except Exception as e:
        print("SCHEMA_PARSE_ERROR=" + str(e), file=sys.stderr)
        sys.exit(2)
    if not isinstance(cols, list) or not cols:
        print("SCHEMA_PARSE_ERROR=schema must be a non-empty list", file=sys.stderr)
        sys.exit(2)

    db_table = args.target
    if "." not in db_table:
        print("TARGET_PARSE_ERROR=target must be db.table", file=sys.stderr)
        sys.exit(2)
    db, tbl = db_table.split(".", 1)

    spark = (SparkSession.builder
             .appName("lhs-csv-ingest")
             .getOrCreate())

    # Ensure the namespace exists in the udp catalog before createOrReplace.
    spark.sql("CREATE NAMESPACE IF NOT EXISTS udp." + db)

    schema = _build_schema(cols)
    reader = (spark.read
              .option("header", args.header)
              .option("delimiter", args.delimiter)
              .option("mode", "PERMISSIVE")
              .schema(schema))
    df = reader.csv(args.source)

    # writeTo uses Iceberg's V2 API; createOrReplace handles both the
    # "first ingest into this table" case and the "re-ingest" case without
    # needing the user to drop the table first.
    df.writeTo("udp." + db + "." + tbl).createOrReplace()

    # Count after the write. For large tables this is O(rows) on the
    # Iceberg snapshot — acceptable for v0.5 single-file ingest sizes.
    n = spark.table("udp." + db + "." + tbl).count()
    print("ROWS_WRITTEN=" + str(n))
    spark.stop()


if __name__ == "__main__":
    main()
'''


def _validate_target(target: dict) -> tuple[str, str]:
    """Return (database, table) after validating both as safe identifiers."""
    if not isinstance(target, dict):
        raise UploadInvalidError("target must be an object with {database, table}")
    db = str(target.get("database", "")).strip()
    tbl = str(target.get("table", "")).strip()
    if not db or not tbl:
        raise UploadInvalidError("target must include both 'database' and 'table'")
    if not _IDENT_SAFE_RE.match(db):
        raise UploadInvalidError(f"target.database {db!r} must match {_IDENT_SAFE_RE.pattern}")
    if not _IDENT_SAFE_RE.match(tbl):
        raise UploadInvalidError(f"target.table {tbl!r} must match {_IDENT_SAFE_RE.pattern}")
    return db, tbl


def _normalize_schema(schema_confirm: list[dict]) -> list[dict]:
    """Coerce the user-supplied schema into the {name,type,nullable} shape
    the Spark job expects. Unknown types fall back to 'string' to match the
    inference path. Raises UploadInvalidError on empty or malformed input."""
    if not isinstance(schema_confirm, list) or not schema_confirm:
        raise UploadInvalidError("schema_overrides must be a non-empty list")
    out: list[dict] = []
    seen: set[str] = set()
    for i, col in enumerate(schema_confirm):
        if not isinstance(col, dict):
            raise UploadInvalidError(f"schema_overrides[{i}] must be an object")
        name = str(col.get("name", "")).strip()
        if not name:
            raise UploadInvalidError(f"schema_overrides[{i}].name is required")
        if name in seen:
            raise UploadInvalidError(f"schema_overrides duplicate column {name!r}")
        seen.add(name)
        # Prefer the user-confirmed type; fall back to the inferred type.
        raw_type = str(
            col.get("type") or col.get("inferred_type") or "string"
        ).lower().strip()
        if raw_type not in _ALLOWED_ICEBERG_TYPES:
            raw_type = "string"
        nullable = bool(col.get("nullable", True))
        out.append({"name": name, "type": raw_type, "nullable": nullable})
    return out


def _find_upload_path(install_id: str, upload_id: str) -> Path:
    """Locate the previously-saved CSV under WORK_DIR/uploads/{install}/{upload}/.
    Each upload directory holds exactly one file (the safe-renamed CSV)."""
    upload_dir = _UPLOADS_DIR / install_id / upload_id
    if not upload_dir.is_dir():
        raise UploadInvalidError(f"upload {upload_id} not found for install {install_id}")
    files = [p for p in upload_dir.iterdir() if p.is_file() and p.suffix != ".part"]
    if not files:
        raise UploadInvalidError(f"upload {upload_id} has no file on disk")
    # If multiple (shouldn't happen — save_csv_upload writes exactly one), pick
    # the newest so re-uploads work.
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0]


def _emit_log(install_id: str, line: str, *, stream: str = "stdout") -> None:
    """Publish an ingest log line into the install's event bus, mirroring the
    LogEvent shape that runner.py uses for install pipeline steps."""
    try:
        bus.publish_nowait(LogEvent(
            install_id=install_id,
            ts=time.time(),
            kind="log",
            step="ingest",
            stream=stream,  # type: ignore[arg-type]
            line=line,
        ))
    except Exception:
        _log.exception("ingest log publish failed")


def _update_job(job_id: str, **fields: Any) -> Optional[IngestJob]:
    """Mutate an IngestJob in-place under the registry lock + persist."""
    with _INGEST_LOCK:
        job = _INGEST_JOBS.get(job_id)
        if job is None:
            return None
        data = job.model_dump()
        data.update(fields)
        data["updated_at"] = time.time()
        try:
            updated = IngestJob(**data)
        except Exception:
            _log.exception("ingest job update validation failed")
            return job
        _INGEST_JOBS[job_id] = updated
        _persist_ingest_locked(force=True)
        return updated


async def _docker(args: list[str], *, timeout: int,
                  stdin_data: Optional[bytes] = None) -> tuple[int, str, str]:
    """Thin async wrapper around `docker <args>` that returns (rc, stdout, stderr).
    Returns rc=124 on timeout (matches the convention in runner._run_bash)."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "docker", *args,
            stdin=asyncio.subprocess.PIPE if stdin_data is not None else None,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except (FileNotFoundError, NotImplementedError, OSError) as e:
        return 127, "", f"failed to spawn docker: {e}"

    try:
        out, err = await asyncio.wait_for(
            proc.communicate(input=stdin_data),
            timeout=timeout,
        )
    except asyncio.TimeoutError:
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        try:
            await asyncio.wait_for(proc.wait(), timeout=5)
        except asyncio.TimeoutError:
            pass
        return 124, "", f"docker {' '.join(args[:2])} timed out after {timeout}s"

    return (
        proc.returncode if proc.returncode is not None else 1,
        out.decode("utf-8", errors="replace"),
        err.decode("utf-8", errors="replace"),
    )


async def _container_running(name: str) -> bool:
    rc, out, _ = await _docker(
        ["ps", "--filter", f"name={name}",
         "--filter", "status=running", "--format", "{{.Names}}"],
        timeout=_DOCKER_QUICK_TIMEOUT_SEC,
    )
    if rc != 0:
        return False
    # `docker ps --filter name=X` does a substring match, so confirm an exact
    # line match before trusting it.
    return name in out.splitlines()


def _parse_rows_written(stdout: str) -> Optional[int]:
    """Walk stdout from the bottom looking for the sentinel `ROWS_WRITTEN=<n>`
    line. Returns None if not found (caller treats this as a failed write
    even when spark-submit exited 0)."""
    for line in reversed(stdout.splitlines()):
        line = line.strip()
        if not line.startswith("ROWS_WRITTEN="):
            continue
        try:
            return int(line.split("=", 1)[1])
        except (ValueError, IndexError):
            return None
    return None


async def _stage_csv_to_minio(
    install_id: str, job_id: str, host_csv: Path,
) -> tuple[str, Optional[str]]:
    """`docker cp` the host CSV into the mc-client container, then `mc cp`
    it into the staging bucket. Returns (s3a_uri, error_or_None)."""
    container_tmp = f"/tmp/lhs_ingest_{job_id}.csv"
    s3_key = f"{_STAGING_PREFIX}/{install_id}/{job_id}/file.csv"
    s3a_uri = f"s3a://{_STAGING_BUCKET}/{s3_key}"

    _emit_log(install_id, f"[ingest:{job_id}] docker cp {host_csv.name} -> {_MINIO_CLIENT_CONTAINER}:{container_tmp}")
    rc, _, err = await _docker(
        ["cp", str(host_csv), f"{_MINIO_CLIENT_CONTAINER}:{container_tmp}"],
        timeout=_DOCKER_CP_TIMEOUT_SEC,
    )
    if rc != 0:
        return s3a_uri, f"docker cp -> minio-client failed (rc={rc}): {err.strip()[:200]}"

    _emit_log(install_id, f"[ingest:{job_id}] mc cp -> {_MC_ALIAS}/{_STAGING_BUCKET}/{s3_key}")
    rc, out, err = await _docker(
        ["exec", _MINIO_CLIENT_CONTAINER, "mc", "cp",
         container_tmp, f"{_MC_ALIAS}/{_STAGING_BUCKET}/{s3_key}"],
        timeout=_MC_CP_TIMEOUT_SEC,
    )
    # Best-effort cleanup of the in-container tmp file; ignore failures.
    await _docker(
        ["exec", _MINIO_CLIENT_CONTAINER, "rm", "-f", container_tmp],
        timeout=_DOCKER_QUICK_TIMEOUT_SEC,
    )
    if rc != 0:
        return s3a_uri, f"mc cp failed (rc={rc}): {(err or out).strip()[:200]}"

    return s3a_uri, None


async def _write_spark_job(job_id: str) -> Optional[str]:
    """Pipe _STUDIO_CSV_JOB_PY into /tmp/ingest_<job>.py inside the spark
    container via `docker exec -i ... tee`. Returns None on success or an
    error message string on failure.

    We use `tee` + stdin instead of a heredoc because the script contains
    Python triple-quoted strings, which conflict with shell heredoc parsing
    even with quoted EOF markers on some `sh` builds."""
    target_path = f"/tmp/ingest_{job_id}.py"
    rc, _, err = await _docker(
        ["exec", "-i", _SPARK_CONTAINER, "sh", "-c", f"cat > {target_path}"],
        timeout=_DOCKER_QUICK_TIMEOUT_SEC,
        stdin_data=_STUDIO_CSV_JOB_PY.encode("utf-8"),
    )
    if rc != 0:
        return f"failed to write spark job to container (rc={rc}): {err.strip()[:200]}"
    return None


async def _spark_submit(
    install_id: str, job_id: str, s3a_uri: str, db: str, tbl: str,
    schema_cols: list[dict],
) -> tuple[int, str, str]:
    """Run spark-submit inside udp-spark and stream stdout/stderr into the
    event bus line-by-line. Returns (rc, full_stdout, full_stderr)."""
    target_path = f"/tmp/ingest_{job_id}.py"
    schema_json = json.dumps(schema_cols, separators=(",", ":"))
    argv = [
        "exec", _SPARK_CONTAINER, "spark-submit", target_path,
        s3a_uri,
        "--target", f"{db}.{tbl}",
        "--schema", schema_json,
    ]
    _emit_log(install_id,
              f"[ingest:{job_id}] spark-submit {target_path} {s3a_uri} "
              f"--target {db}.{tbl}")

    try:
        proc = await asyncio.create_subprocess_exec(
            "docker", *argv,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except (FileNotFoundError, NotImplementedError, OSError) as e:
        return 127, "", f"failed to spawn spark-submit: {e}"

    out_buf: list[str] = []
    err_buf: list[str] = []

    async def _drain(stream: asyncio.StreamReader, buf: list[str], kind: str) -> None:
        while True:
            raw = await stream.readline()
            if not raw:
                return
            text = raw.decode("utf-8", errors="replace").rstrip("\r\n")
            buf.append(text)
            # Spark is chatty; only the studio_ingest sentinel lines and the
            # final ROWS_WRITTEN line are interesting, but tailing everything
            # makes failures debuggable from the UI.
            _emit_log(install_id, f"[ingest:{job_id}] {text}", stream=kind)

    drain_out = asyncio.create_task(_drain(proc.stdout, out_buf, "stdout"))  # type: ignore[arg-type]
    drain_err = asyncio.create_task(_drain(proc.stderr, err_buf, "stderr"))  # type: ignore[arg-type]

    timed_out = False
    try:
        await asyncio.wait_for(proc.wait(), timeout=_SPARK_SUBMIT_TIMEOUT_SEC)
    except asyncio.TimeoutError:
        timed_out = True
        _emit_log(install_id,
                  f"[ingest:{job_id}] spark-submit exceeded {_SPARK_SUBMIT_TIMEOUT_SEC}s; killing",
                  stream="stderr")
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        try:
            await asyncio.wait_for(proc.wait(), timeout=10)
        except asyncio.TimeoutError:
            pass
    finally:
        for t in (drain_out, drain_err):
            try:
                await asyncio.wait_for(t, timeout=5)
            except asyncio.TimeoutError:
                t.cancel()
            except Exception:
                pass

    rc = 124 if timed_out else (proc.returncode if proc.returncode is not None else 1)
    return rc, "\n".join(out_buf), "\n".join(err_buf)


async def _run_csv_ingest(
    job_id: str, install_id: str, host_csv: Path,
    db: str, tbl: str, schema_cols: list[dict],
) -> None:
    """Background task that performs the actual ingest. Single source of
    truth for state transitions: pending -> running -> success|failed."""
    _update_job(job_id, state="running", error=None)
    _emit_log(install_id, f"[ingest:{job_id}] starting (file={host_csv.name}, target=udp.{db}.{tbl})")

    # Preflight: containers must be running. We check both upfront so the user
    # gets one clear error instead of two cascading ones.
    for cname in (_MINIO_CLIENT_CONTAINER, _SPARK_CONTAINER):
        if not await _container_running(cname):
            msg = f"required container {cname} is not running"
            _emit_log(install_id, f"[ingest:{job_id}] {msg}", stream="stderr")
            _update_job(job_id, state="failed", error=msg)
            return

    # Step 1: stage the CSV into MinIO.
    s3a_uri, stage_err = await _stage_csv_to_minio(install_id, job_id, host_csv)
    if stage_err:
        _emit_log(install_id, f"[ingest:{job_id}] {stage_err}", stream="stderr")
        _update_job(job_id, state="failed", error=stage_err)
        return

    # Step 2: drop the job script into the spark container.
    write_err = await _write_spark_job(job_id)
    if write_err:
        _emit_log(install_id, f"[ingest:{job_id}] {write_err}", stream="stderr")
        _update_job(job_id, state="failed", error=write_err)
        return

    # Step 3: spark-submit.
    rc, stdout_blob, stderr_blob = await _spark_submit(
        install_id, job_id, s3a_uri, db, tbl, schema_cols,
    )

    # Best-effort cleanup of the in-container job script (don't fail on this).
    await _docker(
        ["exec", _SPARK_CONTAINER, "rm", "-f", f"/tmp/ingest_{job_id}.py"],
        timeout=_DOCKER_QUICK_TIMEOUT_SEC,
    )

    if rc != 0:
        # Surface the tail of stderr so the UI has actionable detail.
        tail = "\n".join(stderr_blob.splitlines()[-30:]) if stderr_blob else ""
        err = f"spark-submit exited {rc}" + (f": {tail}" if tail else "")
        _emit_log(install_id, f"[ingest:{job_id}] FAILED: {err[:300]}", stream="stderr")
        _update_job(job_id, state="failed", error=err)
        return

    rows = _parse_rows_written(stdout_blob)
    if rows is None:
        msg = "spark-submit completed but ROWS_WRITTEN sentinel not found in stdout"
        _emit_log(install_id, f"[ingest:{job_id}] {msg}", stream="stderr")
        _update_job(job_id, state="failed", error=msg)
        return

    _emit_log(install_id, f"[ingest:{job_id}] SUCCESS rows_written={rows}")
    _update_job(job_id, state="success", rows_written=rows, error=None)


async def kick_off_csv_ingest(
    install_id: str,
    upload_id: str,
    schema_confirm: list[dict],
    target: dict,
    source: Optional[dict] = None,
) -> IngestJob:
    """Register an IngestJob and dispatch the Spark write in the background.

    Returns the job immediately (state="pending"). The background task
    transitions the job to running -> success|failed and streams logs to
    the install's event bus under step="ingest"."""
    # Refuse to dispatch unless the install is fully READY. Reuses the
    # state registry the runner pipeline updates so we don't have to
    # reach into main.py for the _require_install_ready helper.
    rec = store.get(install_id)
    if rec is None:
        raise UploadInvalidError(f"install {install_id!r} not found")
    if rec.state != "READY":
        raise UploadInvalidError(
            f"install is in state {rec.state}; READY required for ingest"
        )

    db, tbl = _validate_target(target)
    schema_cols = _normalize_schema(schema_confirm)
    host_csv = _find_upload_path(install_id, upload_id)

    # Hard refuse if docker isn't on PATH — the Studio host can't reach the
    # stack at all in that case. Same guard sql_editor.run_user_sql uses.
    if shutil.which("docker") is None:
        raise UploadInvalidError(
            "docker CLI not on PATH on the Studio host; cannot dispatch Spark ingest"
        )

    now = time.time()
    job_id = f"ing_{uuid.uuid4().hex[:10]}"
    job = IngestJob(
        job_id=job_id,
        install_id=install_id,
        kind="csv",
        state="pending",
        target={"database": db, "table": tbl},
        source={
            "upload_id": upload_id,
            "filename": host_csv.name,
            "path": str(host_csv),
            "schema_columns": [c["name"] for c in schema_cols],
            **(source or {}),
        },
        created_at=now,
        updated_at=now,
        error=None,
        rows_written=None,
    )
    with _INGEST_LOCK:
        _INGEST_JOBS[job_id] = job
        _persist_ingest_locked(force=True)

    # Fire-and-forget the background task. We deliberately don't await it
    # so the POST /ingest endpoint returns the job_id immediately and the
    # UI can poll GET /ingest/{job_id} for state transitions.
    asyncio.create_task(
        _run_csv_ingest(job_id, install_id, host_csv, db, tbl, schema_cols)
    )
    return job


# ---------- JDBC ingest (Postgres / MySQL via spark-submit) ----------
#
# Real path enabled by the `backend/jdbc_extras.py` opt-in override (which
# downloads the JDBC jars into a docker volume mounted on the spark service
# at /opt/spark/jars/jdbc). The flow mirrors the CSV path:
#
#   1. Preflight: jdbc_extras enabled + udp-spark container running.
#   2. Decrypt source credential (data_sources._decrypt_password).
#   3. Build a JDBC URL (no embedded credential -- user/password go through
#      Spark's `option("user", ...).option("password", ...)`, never on the URL
#      and never logged).
#   4. Render a Spark Python job referencing the user/password values via
#      argparse (the values never appear in shell echo or stdout because
#      argparse populates argv -- which we redact() before any bus emit).
#   5. `docker exec -i udp-spark sh -c "cat > /tmp/lhs_pg_ingest_<id>.py"`
#      to write the script via stdin.
#   6. `docker exec udp-spark spark-submit` to run it. stdout is parsed for
#      the `ROWS_WRITTEN=<n>` sentinel (same convention as the CSV path).
#   7. Best-effort cleanup of the in-container script.

# Spark job written into the spark container and run via spark-submit.
# Reads from JDBC, writes via the `udp` REST catalog (already configured
# in spark-defaults.conf by runner._patch_spark_defaults). The `user` and
# `password` arrive as argv -- argparse parses them into a Namespace
# that's never echoed to stdout. The job itself NEVER prints argv.
_STUDIO_JDBC_JOB_PY = r'''#!/usr/bin/env python3
"""Studio JDBC-to-Iceberg ingest job. Generated/dispatched by backend.ingest.

Usage:
  spark-submit /tmp/lhs_<kind>_ingest_<id>.py \
      --jdbc-url <url> --driver <fqcn> \
      --user <u> --password <p> \
      --remote-table <db.table> \
      --target <db.table>

The user/password values arrive on the spark-submit argv. We deliberately
do NOT echo them to stdout/stderr -- argparse parses them and we hand
them straight to the JDBC reader options.
"""
import argparse
import sys

from pyspark.sql import SparkSession


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--jdbc-url", required=True,
                        help="JDBC URL (no credentials embedded)")
    parser.add_argument("--driver", required=True,
                        help="JDBC driver FQCN (e.g. org.postgresql.Driver)")
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--remote-table", required=True,
                        help="Remote table identifier (e.g. public.orders)")
    parser.add_argument("--target", required=True,
                        help="Iceberg target db.table in the udp catalog")
    parser.add_argument("--fetch-size", default="10000",
                        help="JDBC fetchSize (default 10000)")
    args = parser.parse_args()

    if "." not in args.target:
        print("TARGET_PARSE_ERROR=target must be db.table", file=sys.stderr)
        sys.exit(2)
    target_db, target_tbl = args.target.split(".", 1)

    spark = (SparkSession.builder
             .appName("lhs-jdbc-ingest")
             .getOrCreate())

    spark.sql("CREATE NAMESPACE IF NOT EXISTS udp." + target_db)

    # Read via JDBC. Iceberg / Spark catalog options come from spark-defaults.conf;
    # the JDBC reader needs only url + driver + dbtable + user + password.
    df = (spark.read
          .format("jdbc")
          .option("url", args.jdbc_url)
          .option("driver", args.driver)
          .option("dbtable", args.remote_table)
          .option("user", args.user)
          .option("password", args.password)
          .option("fetchsize", args.fetch_size)
          .load())

    df.writeTo("udp." + target_db + "." + target_tbl).createOrReplace()

    # Count for the IngestJob.rows_written field. O(rows) on the snapshot --
    # acceptable for v0.5 single-table ingest sizes.
    n = spark.table("udp." + target_db + "." + target_tbl).count()
    print("ROWS_WRITTEN=" + str(n))
    spark.stop()


if __name__ == "__main__":
    main()
'''


# Strict identifier regex for the REMOTE table reference. We accept
# `schema.table` (Postgres) and `database.table` (MySQL) by allowing a
# single `.`. Splicing into spark-submit argv as a single arg, so we just
# need to keep shell-meta out.
_REMOTE_TABLE_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]{0,127}"
                              r"(\.[a-zA-Z_][a-zA-Z0-9_]{0,127})?$")


def _validate_remote_table(name: str) -> str:
    if not isinstance(name, str) or not name.strip():
        raise UploadInvalidError("remote table name is required")
    name = name.strip()
    if not _REMOTE_TABLE_RE.match(name):
        raise UploadInvalidError(
            f"remote table {name!r} must match {_REMOTE_TABLE_RE.pattern}"
        )
    return name


async def _write_jdbc_job(job_id: str, kind: str) -> tuple[Optional[str], str]:
    """Pipe _STUDIO_JDBC_JOB_PY into /tmp/lhs_<kind>_ingest_<job_id>.py inside
    the spark container via `docker exec -i ... sh -c "cat > path"`.
    Returns (error_or_None, target_path)."""
    target_path = f"/tmp/lhs_{kind}_ingest_{job_id}.py"
    rc, _, err = await _docker(
        ["exec", "-i", _SPARK_CONTAINER, "sh", "-c", f"cat > {target_path}"],
        timeout=_DOCKER_QUICK_TIMEOUT_SEC,
        stdin_data=_STUDIO_JDBC_JOB_PY.encode("utf-8"),
    )
    if rc != 0:
        return (
            f"failed to write JDBC spark job to container (rc={rc}): "
            f"{err.strip()[:200]}",
            target_path,
        )
    return None, target_path


async def _jdbc_spark_submit(
    install_id: str, job_id: str, target_path: str,
    jdbc_url: str, driver_fqcn: str,
    user: str, password: str,
    remote_table: str, target_db_table: str,
) -> tuple[int, str, str]:
    """Run spark-submit inside udp-spark with the JDBC argv. Streams stdout
    and stderr into the event bus line-by-line, with redact() applied to
    every line we emit so the password can never leak via the log channel.

    Mirrors the CSV path's _spark_submit but with the JDBC-specific argv
    shape.
    """
    # Import locally to avoid a top-of-file dependency just for this helper.
    from .redact import redact

    argv = [
        "exec", _SPARK_CONTAINER, "spark-submit", target_path,
        "--jdbc-url", jdbc_url,
        "--driver", driver_fqcn,
        "--user", user,
        "--password", password,
        "--remote-table", remote_table,
        "--target", target_db_table,
    ]
    # Echo a redacted form of the command so the operator can see what ran
    # without seeing the credential. redact() handles `--password X`
    # patterns via the CLI-flag rule in backend/redact.py.
    _emit_log(
        install_id,
        redact(
            f"[ingest:{job_id}] spark-submit {target_path} "
            f"--jdbc-url {jdbc_url} --driver {driver_fqcn} "
            f"--user {user} --password {password} "
            f"--remote-table {remote_table} --target {target_db_table}"
        ),
    )

    try:
        proc = await asyncio.create_subprocess_exec(
            "docker", *argv,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except (FileNotFoundError, NotImplementedError, OSError) as e:
        return 127, "", f"failed to spawn spark-submit: {e}"

    out_buf: list[str] = []
    err_buf: list[str] = []

    async def _drain(stream: asyncio.StreamReader, buf: list[str], kind: str) -> None:
        while True:
            raw = await stream.readline()
            if not raw:
                return
            text = raw.decode("utf-8", errors="replace").rstrip("\r\n")
            buf.append(text)
            # Belt-and-suspenders: redact every line in case Spark ever
            # echoes the JDBC URL with embedded credentials in a stack
            # trace (it doesn't on the happy path, but stack traces are
            # never predictable).
            _emit_log(install_id, f"[ingest:{job_id}] {redact(text)}",
                      stream=kind)

    drain_out = asyncio.create_task(_drain(proc.stdout, out_buf, "stdout"))  # type: ignore[arg-type]
    drain_err = asyncio.create_task(_drain(proc.stderr, err_buf, "stderr"))  # type: ignore[arg-type]

    timed_out = False
    try:
        await asyncio.wait_for(proc.wait(), timeout=_SPARK_SUBMIT_TIMEOUT_SEC)
    except asyncio.TimeoutError:
        timed_out = True
        _emit_log(install_id,
                  f"[ingest:{job_id}] spark-submit exceeded "
                  f"{_SPARK_SUBMIT_TIMEOUT_SEC}s; killing",
                  stream="stderr")
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        try:
            await asyncio.wait_for(proc.wait(), timeout=10)
        except asyncio.TimeoutError:
            pass
    finally:
        for t in (drain_out, drain_err):
            try:
                await asyncio.wait_for(t, timeout=5)
            except asyncio.TimeoutError:
                t.cancel()
            except Exception:
                pass

    rc = 124 if timed_out else (proc.returncode if proc.returncode is not None else 1)
    # IMPORTANT: never return the raw password in the buffers (we don't
    # echo it ourselves, but redact stderr again at the boundary for
    # defense-in-depth).
    return rc, redact("\n".join(out_buf)), redact("\n".join(err_buf))


async def _run_jdbc_ingest(
    job_id: str, install_id: str, kind: str,
    jdbc_url: str, driver_fqcn: str,
    user: str, password: str,
    remote_table: str, target_db: str, target_tbl: str,
) -> None:
    """Background task driving a JDBC ingest job through its lifecycle.

    Single source of truth for state transitions: pending -> running ->
    success|failed. Mirrors the structure of _run_csv_ingest so the two
    code paths read identically and bugs found in one apply to the other.
    """
    from .redact import redact

    _update_job(job_id, state="running", error=None)
    _emit_log(install_id,
              redact(f"[ingest:{job_id}] starting (kind={kind}, "
                     f"jdbc-url={jdbc_url}, target=udp.{target_db}.{target_tbl})"))

    # Preflight: spark container must be running.
    if not await _container_running(_SPARK_CONTAINER):
        msg = f"required container {_SPARK_CONTAINER} is not running"
        _emit_log(install_id, f"[ingest:{job_id}] {msg}", stream="stderr")
        _update_job(job_id, state="failed", error=msg)
        return

    # Step 1: write the JDBC spark job to the container.
    write_err, target_path = await _write_jdbc_job(job_id, kind)
    if write_err:
        _emit_log(install_id, f"[ingest:{job_id}] {write_err}", stream="stderr")
        _update_job(job_id, state="failed", error=write_err)
        return

    # Step 2: spark-submit it.
    rc, stdout_blob, stderr_blob = await _jdbc_spark_submit(
        install_id, job_id, target_path,
        jdbc_url, driver_fqcn, user, password,
        remote_table, f"{target_db}.{target_tbl}",
    )

    # Best-effort cleanup of the in-container script (don't fail on this).
    await _docker(
        ["exec", _SPARK_CONTAINER, "rm", "-f", target_path],
        timeout=_DOCKER_QUICK_TIMEOUT_SEC,
    )

    if rc != 0:
        tail = "\n".join(stderr_blob.splitlines()[-30:]) if stderr_blob else ""
        err = f"spark-submit exited {rc}" + (f": {tail}" if tail else "")
        _emit_log(install_id, f"[ingest:{job_id}] FAILED: {err[:300]}",
                  stream="stderr")
        _update_job(job_id, state="failed", error=err)
        return

    rows = _parse_rows_written(stdout_blob)
    if rows is None:
        msg = ("spark-submit completed but ROWS_WRITTEN sentinel not found "
               "in stdout")
        _emit_log(install_id, f"[ingest:{job_id}] {msg}", stream="stderr")
        _update_job(job_id, state="failed", error=msg)
        return

    _emit_log(install_id, f"[ingest:{job_id}] SUCCESS rows_written={rows}")
    _update_job(job_id, state="success", rows_written=rows, error=None)


def _postgres_jdbc_url(host: str, port: int, database: str) -> str:
    """Build a Postgres JDBC URL with NO embedded credential.
    user/password are passed via spark.read.option(...) instead so they
    never appear in argv echo or stack traces."""
    return f"jdbc:postgresql://{host}:{port}/{database}"


def _mysql_jdbc_url(host: str, port: int, database: str) -> str:
    """Build a MySQL JDBC URL with NO embedded credential. Includes
    `useSSL=false` (most v0.5 lakehouse installs connect to an in-VPC DB
    without TLS) and `serverTimezone=UTC` (Connector/J 8+ requires an
    explicit timezone when the server's tz isn't named)."""
    return (f"jdbc:mysql://{host}:{port}/{database}"
            "?useSSL=false&serverTimezone=UTC")


async def kick_off_postgres_ingest(
    install_id: str,
    source_id: str,
    table_name: str,
    target: dict,
) -> IngestJob:
    """Dispatch a Postgres -> Iceberg ingest via the JDBC extras override.

    Refuses if the JDBC extras override hasn't been enabled (operator runs
    `POST /api/installs/<id>/jdbc/enable` first to side-load the driver
    jar onto the spark service). Once the jar is present, this path
    matches the CSV ingest flow byte-for-byte: register the job, dispatch
    a background spark-submit, stream logs via redact(), parse
    ROWS_WRITTEN.
    """
    # Local imports to keep the module-level import graph clean and to avoid
    # circulars (data_sources / jdbc_extras don't depend on ingest).
    from . import data_sources as ds_mod
    from . import jdbc_extras as jdbc_mod

    if not isinstance(target, dict) or not target.get("database") or not target.get("table"):
        raise UploadInvalidError("target must include {database, table}")
    table_name = _validate_remote_table(table_name)
    target_db, target_tbl = _validate_target(target)

    src = await ds_mod.get_source(source_id)
    if src is None:
        raise UploadInvalidError(f"data source {source_id} not found")
    if src.kind != "postgres":
        raise UploadInvalidError(
            f"data source {source_id} is kind={src.kind}, expected postgres")
    if src.install_id != install_id:
        raise UploadInvalidError(
            f"data source {source_id} belongs to a different install")

    rec = store.get(install_id)
    if rec is None:
        raise UploadInvalidError(f"install {install_id!r} not found")
    if rec.state != "READY":
        raise UploadInvalidError(
            f"install is in state {rec.state}; READY required for ingest"
        )

    # The override-file gate. We mark the job FAILED (not raise) here so
    # the UI's job-history view shows a clear actionable error rather
    # than the POST itself failing -- consistent with the CSV path's
    # preflight pattern of surfacing failures via the bus.
    if not jdbc_mod.is_jdbc_enabled(install_id):
        now = time.time()
        job_id = f"ing_{uuid.uuid4().hex[:10]}"
        job = IngestJob(
            job_id=job_id, install_id=install_id, kind="postgres",
            state="failed",
            target={"database": target_db, "table": target_tbl},
            source={
                "source_id": source_id, "source_name": src.name,
                "remote_table": table_name,
                "host": src.host, "port": src.port, "database": src.database,
            },
            created_at=now, updated_at=now,
            error="Run POST /api/installs/.../jdbc/enable first",
            rows_written=None,
        )
        with _INGEST_LOCK:
            _INGEST_JOBS[job_id] = job
            _persist_ingest_locked(force=True)
        return job

    # Stage source credential -- decrypt NOW so misconfigured key surfaces
    # synchronously (better UX than failing inside the background task).
    try:
        password = ds_mod._decrypt_password(source_id)
    except Exception as e:
        raise UploadInvalidError(
            f"could not access stored credential: {type(e).__name__}")

    if shutil.which("docker") is None:
        raise UploadInvalidError(
            "docker CLI not on PATH on the Studio host; cannot dispatch "
            "Spark ingest"
        )

    jdbc_url = _postgres_jdbc_url(src.host, src.port, src.database)

    now = time.time()
    job_id = f"ing_{uuid.uuid4().hex[:10]}"
    job = IngestJob(
        job_id=job_id,
        install_id=install_id,
        kind="postgres",
        state="pending",
        target={"database": target_db, "table": target_tbl},
        source={
            "source_id": source_id,
            "source_name": src.name,
            "remote_table": table_name,
            "host": src.host,
            "port": src.port,
            "database": src.database,
            # NEVER embed password / username:password URL here. The
            # decrypted value lives only in the `password` local + the
            # background task's argv -- both of which are redacted on every
            # log emit.
        },
        created_at=now,
        updated_at=now,
        error=None,
        rows_written=None,
    )
    with _INGEST_LOCK:
        _INGEST_JOBS[job_id] = job
        _persist_ingest_locked(force=True)

    asyncio.create_task(_run_jdbc_ingest(
        job_id, install_id, "pg",
        jdbc_url, "org.postgresql.Driver",
        src.username, password,
        table_name, target_db, target_tbl,
    ))
    return job


async def kick_off_mysql_ingest(
    install_id: str,
    source_id: str,
    table_name: str,
    target: dict,
) -> IngestJob:
    """Dispatch a MySQL -> Iceberg ingest via the JDBC extras override.

    Mirrors kick_off_postgres_ingest. The only differences are the source
    kind check, the JDBC URL builder (Connector/J needs `useSSL=false`
    and `serverTimezone=UTC`), and the driver FQCN.
    """
    from . import data_sources as ds_mod
    from . import jdbc_extras as jdbc_mod

    if not isinstance(target, dict) or not target.get("database") or not target.get("table"):
        raise UploadInvalidError("target must include {database, table}")
    table_name = _validate_remote_table(table_name)
    target_db, target_tbl = _validate_target(target)

    src = await ds_mod.get_source(source_id)
    if src is None:
        raise UploadInvalidError(f"data source {source_id} not found")
    if src.kind != "mysql":
        raise UploadInvalidError(
            f"data source {source_id} is kind={src.kind}, expected mysql")
    if src.install_id != install_id:
        raise UploadInvalidError(
            f"data source {source_id} belongs to a different install")

    rec = store.get(install_id)
    if rec is None:
        raise UploadInvalidError(f"install {install_id!r} not found")
    if rec.state != "READY":
        raise UploadInvalidError(
            f"install is in state {rec.state}; READY required for ingest"
        )

    if not jdbc_mod.is_jdbc_enabled(install_id):
        now = time.time()
        job_id = f"ing_{uuid.uuid4().hex[:10]}"
        job = IngestJob(
            job_id=job_id, install_id=install_id, kind="mysql",
            state="failed",
            target={"database": target_db, "table": target_tbl},
            source={
                "source_id": source_id, "source_name": src.name,
                "remote_table": table_name,
                "host": src.host, "port": src.port, "database": src.database,
            },
            created_at=now, updated_at=now,
            error="Run POST /api/installs/.../jdbc/enable first",
            rows_written=None,
        )
        with _INGEST_LOCK:
            _INGEST_JOBS[job_id] = job
            _persist_ingest_locked(force=True)
        return job

    try:
        password = ds_mod._decrypt_password(source_id)
    except Exception as e:
        raise UploadInvalidError(
            f"could not access stored credential: {type(e).__name__}")

    if shutil.which("docker") is None:
        raise UploadInvalidError(
            "docker CLI not on PATH on the Studio host; cannot dispatch "
            "Spark ingest"
        )

    jdbc_url = _mysql_jdbc_url(src.host, src.port, src.database)

    now = time.time()
    job_id = f"ing_{uuid.uuid4().hex[:10]}"
    job = IngestJob(
        job_id=job_id,
        install_id=install_id,
        kind="mysql",
        state="pending",
        target={"database": target_db, "table": target_tbl},
        source={
            "source_id": source_id,
            "source_name": src.name,
            "remote_table": table_name,
            "host": src.host,
            "port": src.port,
            "database": src.database,
            # NEVER embed the credential here.
        },
        created_at=now,
        updated_at=now,
        error=None,
        rows_written=None,
    )
    with _INGEST_LOCK:
        _INGEST_JOBS[job_id] = job
        _persist_ingest_locked(force=True)

    asyncio.create_task(_run_jdbc_ingest(
        job_id, install_id, "mysql",
        jdbc_url, "com.mysql.cj.jdbc.Driver",
        src.username, password,
        table_name, target_db, target_tbl,
    ))
    return job
