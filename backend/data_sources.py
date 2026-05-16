"""External data source registry for the Ingest module.

v0.5.1 supports Postgres and MySQL. The registry lets users register
remote databases (host/port/db/user/password), test connectivity, and use
them as sources for an Iceberg ingest job. Passwords are encrypted at rest
with Fernet (AES-128 in CBC + HMAC-SHA256) and never returned over the API.

Persistence: WORK_DIR/data_sources.json, with the same debounced atomic-write
pattern as state.py / ingest.py so we don't stall on Windows AV/OneDrive.

Encryption strategy:
  - Key lives in env var LHS_SECRETS_KEY (preferred for production).
  - On first use, if missing, we generate a Fernet key and persist it to
    WORK_DIR/.secrets_key (chmod 600 best-effort on Windows). We log loudly
    so the operator knows to copy it into their secret manager / env file.
  - The raw key is never logged. Only "key generated, persisted at <path>"
    appears in logs.
  - Decryption is internal — only the ingest layer calls `_decrypt_password`.

v0.5 plan (Postgres -> Iceberg dispatch):
  - Bump the Spark image in the UDP repo to include `postgresql-42.7.x.jar`
    on the Spark classpath. Until then `kick_off_postgres_ingest` is a stub
    that marks jobs failed with a clear "pending v0.5" message.
  - Add `udp/scripts/ingest_postgres.py` Spark job that:
      1. Reads from the registered Postgres via JDBC (table or query pushdown)
      2. Writes to Iceberg via the REST catalog
      3. Reports rows_written for the IngestJob counter
  - Wire `kick_off_postgres_ingest` to spawn that job through `docker exec
    udp-spark-master spark-submit ...` (same pattern as the v0.4.1 CSV path).
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Literal, Optional

from cryptography.fernet import Fernet, InvalidToken
from pydantic import BaseModel, Field, field_validator

from .config import WORK_DIR


log = logging.getLogger("lhs.data_sources")


# ---------- Models ----------

SourceKind = Literal["postgres", "mysql"]


class DataSource(BaseModel):
    """Public-facing data source record. Never carries the password."""
    source_id: str
    install_id: str
    kind: SourceKind
    name: str
    host: str
    port: int
    database: str
    username: str
    has_password: bool
    created_at: float
    last_tested_at: Optional[float] = None


class DataSourceCreateRequest(BaseModel):
    """Write-only request. `password` is consumed on create and never returned."""
    kind: SourceKind = "postgres"
    name: str = Field(min_length=1, max_length=120)
    host: str = Field(min_length=1, max_length=253)
    port: int = Field(ge=1, le=65535)
    database: str = Field(min_length=1, max_length=128)
    username: str = Field(min_length=1, max_length=128)
    password: str = Field(min_length=1, max_length=512)

    @field_validator("name", "host", "database", "username")
    @classmethod
    def _no_control_chars(cls, v: str) -> str:
        if any(ord(c) < 32 for c in v):
            raise ValueError("control characters are not allowed")
        return v.strip()


# ---------- Password strength ----------

class WeakPasswordError(ValueError):
    pass


class DataSourceNotFoundError(LookupError):
    pass


_LETTER_RE = re.compile(r"[A-Za-z]")
_DIGIT_RE = re.compile(r"\d")
_SPECIAL_RE = re.compile(r"[!@#$%^&*()_+\-=\[\]{}|;:,.<>?]")
_COMMON_PASSWORDS = {
    "password", "password123", "admin", "12345678", "qwertyuiop",
    "letmein", "welcome", "changeme", "iloveyou", "monkey", "dragon",
    "abc123", "111111", "1q2w3e4r", "sunshine", "princess",
}


def _check_password_strength(password: str) -> None:
    """Reject weak DB credentials at the boundary. v0.5 hardened policy
    per Gemini's review: longer min length, special-char required, common
    passwords blacklisted."""
    problems: list[str] = []
    if len(password) < 12:
        problems.append("at least 12 characters")
    if not _LETTER_RE.search(password):
        problems.append("at least one letter")
    if not _DIGIT_RE.search(password):
        problems.append("at least one digit")
    if not _SPECIAL_RE.search(password):
        problems.append("at least one special character (!@#$%^&*()_+-=[]{}|;:,.<>?)")
    if password.lower() in _COMMON_PASSWORDS:
        problems.append("not in the common-password blacklist")
    if problems:
        raise WeakPasswordError("password must have: " + "; ".join(problems))


# ---------- Fernet key bootstrap ----------

_SECRETS_KEY_FILE = WORK_DIR / ".secrets_key"
_KEY_LOCK = threading.Lock()
_FERNET: Optional[Fernet] = None


def _get_fernet() -> Fernet:
    """Return the process-wide Fernet, generating/loading the key as needed.

    Key precedence:
      1. LHS_SECRETS_KEY env var (preferred)
      2. WORK_DIR/.secrets_key (auto-generated on first use)
    """
    global _FERNET
    if _FERNET is not None:
        return _FERNET
    with _KEY_LOCK:
        if _FERNET is not None:  # double-check after lock
            return _FERNET

        raw = os.environ.get("LHS_SECRETS_KEY", "").strip()
        if raw:
            try:
                _FERNET = Fernet(raw.encode("ascii"))
                return _FERNET
            except (ValueError, TypeError) as e:
                # Fall through to file-based key rather than crash the app.
                log.error("LHS_SECRETS_KEY env var is set but invalid: %s", e)

        if _SECRETS_KEY_FILE.exists():
            try:
                raw = _SECRETS_KEY_FILE.read_text(encoding="utf-8").strip()
                _FERNET = Fernet(raw.encode("ascii"))
                return _FERNET
            except (OSError, ValueError, TypeError) as e:
                log.error("could not load existing secrets key from %s: %s",
                          _SECRETS_KEY_FILE, e)

        # Generate a new key.
        new_key = Fernet.generate_key()
        _SECRETS_KEY_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = _SECRETS_KEY_FILE.with_suffix(_SECRETS_KEY_FILE.suffix + ".tmp")
        tmp.write_text(new_key.decode("ascii"), encoding="utf-8")
        os.replace(tmp, _SECRETS_KEY_FILE)
        try:
            os.chmod(_SECRETS_KEY_FILE, 0o600)
        except OSError:
            # Windows often won't honor 0600 — that's fine, best effort.
            pass

        # IMPORTANT: never log the key itself.
        log.warning(
            "LHS secrets key auto-generated and persisted at %s. "
            "Copy this file into your secret manager and set LHS_SECRETS_KEY "
            "in production. Losing this file makes all stored data-source "
            "passwords unrecoverable.",
            _SECRETS_KEY_FILE,
        )
        _FERNET = Fernet(new_key)
        return _FERNET


def _encrypt(plaintext: str) -> str:
    return _get_fernet().encrypt(plaintext.encode("utf-8")).decode("ascii")


def _decrypt(ciphertext: str) -> str:
    try:
        return _get_fernet().decrypt(ciphertext.encode("ascii")).decode("utf-8")
    except InvalidToken as e:
        raise RuntimeError(
            "could not decrypt data source password (key rotated or corrupted record)"
        ) from e


# ---------- Persistence (debounced atomic write — mirrors ingest.py) ----------

_DATA_SOURCES_FILE = WORK_DIR / "data_sources.json"
_DS_LOCK = threading.RLock()
# Internal storage shape: source_id -> {"record": DataSource.model_dump(),
#                                       "encrypted_password": "..." | None}
_DS_STORE: dict[str, dict[str, Any]] = {}
_DS_DIRTY = False
_DS_FLUSH_TIMER: Optional[threading.Timer] = None
_DS_WRITE_DEBOUNCE_SEC = 0.25


def _ds_atomic_write(data: str) -> None:
    _DATA_SOURCES_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = _DATA_SOURCES_FILE.with_suffix(_DATA_SOURCES_FILE.suffix + ".tmp")
    tmp.write_text(data, encoding="utf-8")
    for _ in range(5):
        try:
            os.replace(tmp, _DATA_SOURCES_FILE)
            return
        except PermissionError:
            time.sleep(0.1)
    # Last-ditch: leave the tmp so data isn't lost.


def _write_ds_now_locked() -> None:
    global _DS_DIRTY
    payload = {sid: entry for sid, entry in _DS_STORE.items()}
    _ds_atomic_write(json.dumps(payload, indent=2))
    _DS_DIRTY = False


def _flush_ds_from_timer() -> None:
    global _DS_FLUSH_TIMER
    with _DS_LOCK:
        _DS_FLUSH_TIMER = None
        if _DS_DIRTY:
            try:
                _write_ds_now_locked()
            except Exception:
                log.exception("data_sources flush failed")


def _persist_ds_locked(*, force: bool = False) -> None:
    global _DS_DIRTY, _DS_FLUSH_TIMER
    _DS_DIRTY = True
    if force:
        if _DS_FLUSH_TIMER is not None:
            _DS_FLUSH_TIMER.cancel()
            _DS_FLUSH_TIMER = None
        _write_ds_now_locked()
        return
    if _DS_FLUSH_TIMER is None:
        _DS_FLUSH_TIMER = threading.Timer(_DS_WRITE_DEBOUNCE_SEC, _flush_ds_from_timer)
        _DS_FLUSH_TIMER.daemon = True
        _DS_FLUSH_TIMER.start()


def _load_data_sources() -> None:
    if not _DATA_SOURCES_FILE.exists():
        return
    try:
        raw = json.loads(_DATA_SOURCES_FILE.read_text(encoding="utf-8"))
    except Exception:
        return
    for sid, entry in raw.items():
        if not isinstance(entry, dict) or "record" not in entry:
            continue
        try:
            # Validate against the model to drop any malformed legacy rows.
            DataSource(**entry["record"])
        except Exception:
            continue
        _DS_STORE[sid] = entry


_load_data_sources()


# ---------- Public CRUD ----------

def _scrub(entry: dict[str, Any]) -> DataSource:
    return DataSource(**entry["record"])


async def create_source(install_id: str, request: DataSourceCreateRequest) -> DataSource:
    """Encrypt the password and persist a new DataSource."""
    _check_password_strength(request.password)

    now = time.time()
    source_id = f"src_{uuid.uuid4().hex[:12]}"
    record = DataSource(
        source_id=source_id,
        install_id=install_id,
        kind=request.kind,
        name=request.name,
        host=request.host,
        port=request.port,
        database=request.database,
        username=request.username,
        has_password=True,
        created_at=now,
        last_tested_at=None,
    )
    encrypted = _encrypt(request.password)
    with _DS_LOCK:
        _DS_STORE[source_id] = {
            "record": record.model_dump(),
            "encrypted_password": encrypted,
        }
        _persist_ds_locked(force=True)
    return record


async def list_sources(install_id: str) -> list[DataSource]:
    with _DS_LOCK:
        return sorted(
            (_scrub(e) for e in _DS_STORE.values()
             if e["record"].get("install_id") == install_id),
            key=lambda s: s.created_at,
            reverse=True,
        )


async def get_source(source_id: str) -> Optional[DataSource]:
    with _DS_LOCK:
        entry = _DS_STORE.get(source_id)
        if not entry:
            return None
        return _scrub(entry)


async def delete_source(source_id: str) -> None:
    with _DS_LOCK:
        if source_id in _DS_STORE:
            _DS_STORE.pop(source_id, None)
            _persist_ds_locked(force=True)


def _decrypt_password(source_id: str) -> str:
    """INTERNAL ONLY. Used by the ingest layer to obtain the live credential.

    Never expose this over the API. Never log the returned value.
    """
    with _DS_LOCK:
        entry = _DS_STORE.get(source_id)
        if not entry:
            raise DataSourceNotFoundError(source_id)
        encrypted = entry.get("encrypted_password")
        if not encrypted:
            raise RuntimeError(f"data source {source_id} has no stored password")
        return _decrypt(encrypted)


def _mark_tested_locked(source_id: str, ts: float) -> None:
    entry = _DS_STORE.get(source_id)
    if not entry:
        return
    entry["record"]["last_tested_at"] = ts
    _persist_ds_locked()


# ---------- Connection test ----------

_TEST_TIMEOUT_SEC = 5


def _sync_test_postgres(host: str, port: int, database: str,
                        username: str, password: str) -> dict[str, Any]:
    """Blocking psycopg connect + introspect. Caller wraps in to_thread."""
    # Import inside the worker so the module is still importable in
    # environments where psycopg isn't installed (e.g. unit tests for the
    # CRUD path that never call `test_source`).
    import psycopg  # noqa: WPS433 (intentional local import)

    started = time.time()
    conninfo = psycopg.conninfo.make_conninfo(
        host=host,
        port=port,
        dbname=database,
        user=username,
        password=password,
        connect_timeout=_TEST_TIMEOUT_SEC,
    )
    with psycopg.connect(conninfo, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW server_version")
            row = cur.fetchone()
            server_version = row[0] if row else None
            cur.execute(
                "SELECT schema_name FROM information_schema.schemata "
                "WHERE schema_name NOT IN ('pg_catalog','information_schema') "
                "AND schema_name NOT LIKE 'pg_toast%' "
                "AND schema_name NOT LIKE 'pg_temp_%' "
                "ORDER BY schema_name LIMIT 200"
            )
            schemas = [r[0] for r in cur.fetchall()]
    latency_ms = int((time.time() - started) * 1000)
    return {
        "ok": True,
        "latency_ms": latency_ms,
        "server_version": server_version,
        "schemas": schemas,
        "error": None,
    }


def _sync_test_mysql(host: str, port: int, database: str,
                     username: str, password: str) -> dict[str, Any]:
    """Blocking pymysql connect + introspect. Caller wraps in to_thread.

    Mirrors _sync_test_postgres: opens a real connection, runs SELECT 1
    to prove auth, then SHOW DATABASES to enumerate schemas the user can
    see. Returns the same shape as the Postgres test path so the UI
    doesn't need to branch on `kind`.
    """
    # Local import so the module remains importable when pymysql isn't
    # installed (same rationale as the psycopg local import above).
    import pymysql  # noqa: WPS433

    started = time.time()
    conn = pymysql.connect(
        host=host,
        port=int(port),
        database=database,
        user=username,
        password=password,
        connect_timeout=_TEST_TIMEOUT_SEC,
        read_timeout=_TEST_TIMEOUT_SEC,
        write_timeout=_TEST_TIMEOUT_SEC,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
            cur.execute("SELECT VERSION()")
            row = cur.fetchone()
            server_version = row[0] if row else None
            cur.execute("SHOW DATABASES")
            # Filter out the system schemas users typically don't care about.
            system = {"information_schema", "performance_schema", "mysql", "sys"}
            schemas = [r[0] for r in cur.fetchall() if r[0] not in system][:200]
    finally:
        try:
            conn.close()
        except Exception:
            pass
    latency_ms = int((time.time() - started) * 1000)
    return {
        "ok": True,
        "latency_ms": latency_ms,
        "server_version": server_version,
        "schemas": schemas,
        "error": None,
    }


async def test_source(source_id: str) -> dict[str, Any]:
    """Open a real connection (5s hard timeout) and report basic facts.

    Runs in a worker thread so it never blocks the event loop. asyncio.wait_for
    enforces the wall-clock cap independent of the driver's own connect_timeout.
    Dispatches to the correct driver based on `src.kind` (postgres or mysql).
    """
    src = await get_source(source_id)
    if not src:
        raise DataSourceNotFoundError(source_id)
    secret = _decrypt_password(source_id)

    if src.kind == "mysql":
        worker = _sync_test_mysql
    else:
        worker = _sync_test_postgres

    try:
        result = await asyncio.wait_for(
            asyncio.to_thread(
                worker,
                src.host, src.port, src.database, src.username, secret,
            ),
            timeout=_TEST_TIMEOUT_SEC + 1,  # tiny slack over driver-level timeout
        )
    except asyncio.TimeoutError:
        return {
            "ok": False,
            "latency_ms": None,
            "server_version": None,
            "schemas": [],
            "error": f"connection timed out after {_TEST_TIMEOUT_SEC}s",
        }
    except Exception as e:
        # Don't leak the raw connection string / password in the error.
        return {
            "ok": False,
            "latency_ms": None,
            "server_version": None,
            "schemas": [],
            "error": f"{type(e).__name__}: {e}",
        }

    with _DS_LOCK:
        _mark_tested_locked(source_id, time.time())
    return result
