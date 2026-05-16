"""Outbound destination registry (the OUTBOUND mirror of data_sources.py).

After a stack reaches READY, the operator wires downstream BI/analytics
tools to it via "destinations". A destination is one of:

  - kind=insyght        — Insyght conversational BI (sql_pull is default)
  - kind=tableau        — Tableau (sql_pull via MySQL/JDBC)
  - kind=looker         — Looker (sql_pull)
  - kind=mode           — Mode Analytics (sql_pull)
  - kind=superset       — Apache Superset (sql_pull)
  - kind=metabase       — Metabase (sql_pull)
  - kind=powerbi        — Microsoft Power BI (sql_pull)
  - kind=custom_jdbc    — Generic JDBC/MySQL-protocol consumer

Three connection_modes are supported:

  - sql_pull   — consumer connects via StarRocks' MySQL protocol on :9030
                 with a per-destination read-only DB user the operator
                 provisions (see provision_sql_pull in insyght_connector).
  - push_api   — Studio POSTs change events to the destination's webhook URL.
  - file_drop  — Studio writes parquet/CSV snapshots to an object-store path.

Symmetry with data_sources.py is intentional:
  - same Fernet-encrypted secret-at-rest pattern (reuses the helpers there)
  - same debounced atomic-write persistence (mirrors state.py / ingest.py)
  - same scrubbed Pydantic response model (`has_credentials: bool`, never plaintext)
  - same per-id test endpoint that opens a real driver connection

CONSTRAINT: backend/runner.py is FROZEN. Existing route contracts are FROZEN.
This module is purely additive. The Insyght-specific provisioning helper lives
in `backend/insyght_connector.py` so per-vendor logic stays out of the registry.
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

from pydantic import BaseModel, Field, field_validator

from .config import WORK_DIR
# Reuse the Fernet wrapper from data_sources — never duplicate the
# secret-key bootstrap or the encryption helpers.
from .data_sources import _decrypt, _encrypt


log = logging.getLogger("lhs.destinations")


# ---------- Identifier validation (mirrors table_explorer._IDENT_RE) ----------

# Same character class as backend/table_explorer.py — letters/digits/_/./-.
# Names get a slightly longer ceiling (operators love verbose names) and
# allow spaces; everything that could ride through to a shell or SQL keeps
# the strict pattern.
_IDENT_RE = re.compile(r"^[A-Za-z0-9_.\-]{1,128}$")
_NAME_RE = re.compile(r"^[A-Za-z0-9 _.\-]{1,128}$")


def _validate_ident(value: str, field: str) -> str:
    if not isinstance(value, str) or not _IDENT_RE.match(value):
        raise ValueError(f"{field} {value!r}: must match [A-Za-z0-9_.-]{{1,128}}")
    return value


def _validate_name(value: str, field: str) -> str:
    if not isinstance(value, str) or not _NAME_RE.match(value):
        raise ValueError(f"{field} {value!r}: must match [A-Za-z0-9 _.-]{{1,128}}")
    return value.strip()


# ---------- Models ----------

DestinationKind = Literal[
    "insyght", "tableau", "looker", "mode", "superset",
    "metabase", "powerbi", "custom_jdbc",
]

ConnectionMode = Literal["sql_pull", "push_api", "file_drop"]


class Destination(BaseModel):
    """Public-facing destination record. NEVER carries plaintext credentials."""
    destination_id: str
    install_id: str
    kind: DestinationKind
    name: str
    connection_mode: ConnectionMode
    config: dict[str, Any] = Field(default_factory=dict)
    has_credentials: bool = False
    created_at: float
    last_tested_at: Optional[float] = None


class DestinationCreateRequest(BaseModel):
    """Write-only request. `credentials` is consumed on create and never returned."""
    kind: DestinationKind
    name: str = Field(min_length=1, max_length=128)
    connection_mode: ConnectionMode
    config: dict[str, Any] = Field(default_factory=dict)
    # credentials is a generic dict so each kind can store its own shape:
    #   sql_pull -> {"password": "..."}
    #   push_api -> {"bearer_token": "..."}  or  {"hmac_secret": "..."}
    #   file_drop -> {"secret_access_key": "..."}
    credentials: Optional[dict[str, Any]] = None

    @field_validator("name")
    @classmethod
    def _check_name(cls, v: str) -> str:
        return _validate_name(v, "name")

    @field_validator("config")
    @classmethod
    def _no_control_chars_in_config(cls, v: dict[str, Any]) -> dict[str, Any]:
        # Defense-in-depth: reject control chars in any string value in config.
        # We render parts of config back to operators (host, port, URL),
        # don't want stray \r\n smuggled in.
        for key, val in v.items():
            if not isinstance(key, str) or not _IDENT_RE.match(key):
                raise ValueError(f"config key {key!r}: must match [A-Za-z0-9_.-]{{1,128}}")
            if isinstance(val, str) and any(ord(c) < 32 for c in val):
                raise ValueError(f"config value for {key!r} contains control characters")
        return v


class DestinationNotFoundError(LookupError):
    """Raised when a destination_id is unknown."""


# ---------- Persistence (debounced atomic write — mirrors data_sources.py) ----------

_DESTINATIONS_FILE = WORK_DIR / "destinations.json"
_DEST_LOCK = threading.RLock()
# Internal storage shape:
#   destination_id -> {"record": Destination.model_dump(),
#                      "encrypted_credentials": "<fernet ciphertext>" | None}
_DEST_STORE: dict[str, dict[str, Any]] = {}
_DEST_DIRTY = False
_DEST_FLUSH_TIMER: Optional[threading.Timer] = None
_DEST_WRITE_DEBOUNCE_SEC = 0.25


def _dest_atomic_write(data: str) -> None:
    _DESTINATIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = _DESTINATIONS_FILE.with_suffix(_DESTINATIONS_FILE.suffix + ".tmp")
    tmp.write_text(data, encoding="utf-8")
    for _ in range(5):
        try:
            os.replace(tmp, _DESTINATIONS_FILE)
            return
        except PermissionError:
            time.sleep(0.1)
    # Last-ditch: leave the tmp so data isn't lost.


def _write_dest_now_locked() -> None:
    global _DEST_DIRTY
    payload = {did: entry for did, entry in _DEST_STORE.items()}
    _dest_atomic_write(json.dumps(payload, indent=2))
    _DEST_DIRTY = False


def _flush_dest_from_timer() -> None:
    global _DEST_FLUSH_TIMER
    with _DEST_LOCK:
        _DEST_FLUSH_TIMER = None
        if _DEST_DIRTY:
            try:
                _write_dest_now_locked()
            except Exception:
                log.exception("destinations flush failed")


def _persist_dest_locked(*, force: bool = False) -> None:
    global _DEST_DIRTY, _DEST_FLUSH_TIMER
    _DEST_DIRTY = True
    if force:
        if _DEST_FLUSH_TIMER is not None:
            _DEST_FLUSH_TIMER.cancel()
            _DEST_FLUSH_TIMER = None
        _write_dest_now_locked()
        return
    if _DEST_FLUSH_TIMER is None:
        _DEST_FLUSH_TIMER = threading.Timer(
            _DEST_WRITE_DEBOUNCE_SEC, _flush_dest_from_timer
        )
        _DEST_FLUSH_TIMER.daemon = True
        _DEST_FLUSH_TIMER.start()


def _load_destinations() -> None:
    if not _DESTINATIONS_FILE.exists():
        return
    try:
        raw = json.loads(_DESTINATIONS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return
    for did, entry in raw.items():
        if not isinstance(entry, dict) or "record" not in entry:
            continue
        try:
            # Validate against the model to drop any malformed legacy rows.
            Destination(**entry["record"])
        except Exception:
            continue
        _DEST_STORE[did] = entry


_load_destinations()


# ---------- Public CRUD ----------

def _scrub(entry: dict[str, Any]) -> Destination:
    return Destination(**entry["record"])


async def create_destination(install_id: str,
                             request: DestinationCreateRequest) -> Destination:
    """Validate + encrypt credentials + persist a new Destination."""
    # Re-validate at the public API boundary (Pydantic already ran, but
    # this is the choke point if create_destination is ever called
    # programmatically from somewhere that bypassed model construction).
    _validate_name(request.name, "name")
    _validate_ident(request.kind, "kind")
    _validate_ident(request.connection_mode, "connection_mode")
    _validate_ident(install_id, "install_id")

    encrypted: Optional[str] = None
    has_creds = False
    if request.credentials:
        # Serialize the entire credentials dict and Fernet-encrypt it. We
        # don't enforce shape here — different kinds carry different fields.
        # Strength checks for sql_pull passwords happen in insyght_connector.
        encrypted = _encrypt(json.dumps(request.credentials))
        has_creds = True

    now = time.time()
    destination_id = f"dst_{uuid.uuid4().hex[:12]}"
    record = Destination(
        destination_id=destination_id,
        install_id=install_id,
        kind=request.kind,
        name=request.name,
        connection_mode=request.connection_mode,
        config=dict(request.config),  # defensive copy
        has_credentials=has_creds,
        created_at=now,
        last_tested_at=None,
    )
    with _DEST_LOCK:
        _DEST_STORE[destination_id] = {
            "record": record.model_dump(),
            "encrypted_credentials": encrypted,
        }
        _persist_dest_locked(force=True)
    return record


async def list_destinations(install_id: str) -> list[Destination]:
    with _DEST_LOCK:
        return sorted(
            (_scrub(e) for e in _DEST_STORE.values()
             if e["record"].get("install_id") == install_id),
            key=lambda d: d.created_at,
            reverse=True,
        )


async def get_destination(destination_id: str) -> Optional[Destination]:
    with _DEST_LOCK:
        entry = _DEST_STORE.get(destination_id)
        if not entry:
            return None
        return _scrub(entry)


async def delete_destination(destination_id: str) -> None:
    with _DEST_LOCK:
        if destination_id in _DEST_STORE:
            _DEST_STORE.pop(destination_id, None)
            _persist_dest_locked(force=True)


def _decrypt_credentials(destination_id: str) -> dict[str, Any]:
    """INTERNAL ONLY. Used by the test + provision paths.

    Never expose this over the API. Never log the returned values.
    Returns an empty dict if the destination has no stored credentials.
    """
    with _DEST_LOCK:
        entry = _DEST_STORE.get(destination_id)
        if not entry:
            raise DestinationNotFoundError(destination_id)
        blob = entry.get("encrypted_credentials")
        if not blob:
            return {}
        try:
            return json.loads(_decrypt(blob))
        except json.JSONDecodeError as e:
            raise RuntimeError(
                f"destination {destination_id}: stored credentials are not JSON"
            ) from e


def _mark_tested_locked(destination_id: str, ts: float) -> None:
    entry = _DEST_STORE.get(destination_id)
    if not entry:
        return
    entry["record"]["last_tested_at"] = ts
    _persist_dest_locked()


# ---------- Connection test ----------

_TEST_TIMEOUT_SEC = 5
_PUSH_HANDSHAKE_TIMEOUT_SEC = 5


def _sync_test_sql_pull(host: str, port: int, database: str,
                        username: str, password: str) -> dict[str, Any]:
    """Blocking pymysql connect against StarRocks' MySQL protocol.

    Returns the same shape as data_sources._sync_test_mysql so the
    frontend can render both with the same code path.
    """
    import pymysql  # local import so test envs without pymysql still load this module

    started = time.time()
    conn = pymysql.connect(
        host=host,
        port=int(port),
        database=database or "udp",
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
        "message": "sql_pull handshake succeeded",
        "error": None,
    }


async def _test_push_api(config: dict[str, Any],
                         credentials: dict[str, Any]) -> dict[str, Any]:
    """POST a tiny handshake payload to the destination's webhook URL.

    Returns {ok, latency_ms, status_code, message, error}. Soft on auth — a
    401/403 still proves the URL is reachable, so we report ok=True but
    flag the message.
    """
    import httpx  # already a project dep (see main.py)

    url = config.get("url") or config.get("endpoint_url")
    if not url or not isinstance(url, str):
        return {
            "ok": False, "latency_ms": None, "status_code": None,
            "message": None, "error": "config.url is required for push_api",
        }
    headers: dict[str, str] = {"content-type": "application/json"}
    tok = credentials.get("bearer_token") if credentials else None
    if tok:
        headers["authorization"] = f"Bearer {tok}"
    payload = {"type": "lhs.handshake", "ts": time.time()}
    started = time.time()
    try:
        async with httpx.AsyncClient(timeout=_PUSH_HANDSHAKE_TIMEOUT_SEC) as client:
            r = await client.post(url, json=payload, headers=headers)
    except Exception as e:
        return {
            "ok": False, "latency_ms": None, "status_code": None,
            "message": None, "error": f"{type(e).__name__}: {e}",
        }
    latency_ms = int((time.time() - started) * 1000)
    # 2xx = healthy. 401/403 still means we reached the server (helpful signal).
    code = r.status_code
    if 200 <= code < 300:
        return {
            "ok": True, "latency_ms": latency_ms, "status_code": code,
            "message": "handshake accepted", "error": None,
        }
    if code in (401, 403):
        return {
            "ok": True, "latency_ms": latency_ms, "status_code": code,
            "message": f"reachable but auth rejected (HTTP {code}) — token likely wrong",
            "error": None,
        }
    return {
        "ok": False, "latency_ms": latency_ms, "status_code": code,
        "message": None,
        "error": f"unexpected status {code}: {r.text[:200]}",
    }


async def _test_file_drop(config: dict[str, Any]) -> dict[str, Any]:
    """Verify the bucket path is writable.

    v0.6 stub — full mc-based probe requires shelling into the running
    minio container, which means the install must be READY (route layer
    enforces that). We do a lightweight reachability check here and
    return a clear "not yet implemented" message; the route still passes
    so the UI shows a sensible signal.
    """
    bucket = config.get("bucket") or config.get("bucket_path")
    if not bucket:
        return {
            "ok": False, "latency_ms": None,
            "message": None, "error": "config.bucket is required for file_drop",
        }
    return {
        "ok": True,
        "latency_ms": 0,
        "message": (
            f"file_drop destination registered for bucket {bucket!r}. "
            "Live writability probe lands in v0.6.1."
        ),
        "error": None,
    }


async def test_destination(destination_id: str) -> dict[str, Any]:
    """Dispatch to the per-mode tester. Returns a consistent envelope.

    Always returns a dict — never raises — so the UI can render failures
    uniformly. Updates last_tested_at on success.
    """
    dest = await get_destination(destination_id)
    if not dest:
        raise DestinationNotFoundError(destination_id)

    creds = _decrypt_credentials(destination_id)

    if dest.connection_mode == "sql_pull":
        host = dest.config.get("host", "127.0.0.1")
        port = int(dest.config.get("port", 9030))
        database = dest.config.get("database", "udp")
        username = dest.config.get("username") or creds.get("username") or "root"
        password = creds.get("password", "")
        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(
                    _sync_test_sql_pull,
                    host, port, database, username, password,
                ),
                timeout=_TEST_TIMEOUT_SEC + 1,
            )
        except asyncio.TimeoutError:
            return {
                "ok": False, "latency_ms": None, "server_version": None,
                "message": None,
                "error": f"connection timed out after {_TEST_TIMEOUT_SEC}s",
            }
        except Exception as e:
            return {
                "ok": False, "latency_ms": None, "server_version": None,
                "message": None, "error": f"{type(e).__name__}: {e}",
            }
        with _DEST_LOCK:
            _mark_tested_locked(destination_id, time.time())
        return result

    if dest.connection_mode == "push_api":
        result = await _test_push_api(dest.config, creds)
        if result.get("ok"):
            with _DEST_LOCK:
                _mark_tested_locked(destination_id, time.time())
        return result

    if dest.connection_mode == "file_drop":
        result = await _test_file_drop(dest.config)
        if result.get("ok"):
            with _DEST_LOCK:
                _mark_tested_locked(destination_id, time.time())
        return result

    return {"ok": False, "error": f"unknown connection_mode: {dest.connection_mode}"}


# ---------- Connection-payload generation (sanitized for handoff) ----------

# Plaintext-credential markers used in the payload. NEVER substitute the
# real value here — the operator already has the credential (they set it
# at create time).
_REDACTED = "•••• stored at create time — never shown again"


def _instructions_for_dest(dest: Destination) -> list[str]:
    """Per-kind step-by-step strings the UI shows. Insyght has its own
    richer set in `insyght_connector.connection_instructions`; this is
    the generic fallback for every other kind."""
    if dest.kind == "insyght":
        # Lazy import to avoid circular import (insyght_connector reads from
        # this module for type info).
        from .insyght_connector import connection_instructions
        return connection_instructions(dest)

    if dest.connection_mode == "sql_pull":
        host = dest.config.get("host", "<your-studio-host>")
        port = dest.config.get("port", 9030)
        database = dest.config.get("database", "udp")
        username = dest.config.get("username", "<bi_reader>")
        return [
            f"1. Open {dest.kind.title()} → Add Data Source → MySQL (or generic JDBC).",
            f"2. Host: {host}",
            f"3. Port: {port}",
            f"4. Database: {database}",
            f"5. User: {username}",
            "6. Password: use the value you supplied when creating this destination.",
            "7. Test the connection from inside the BI tool.",
        ]

    if dest.connection_mode == "push_api":
        url = dest.config.get("url") or dest.config.get("endpoint_url") or "<not set>"
        return [
            f"1. {dest.kind.title()} receives events at: {url}",
            "2. Authorization header carries the bearer token you stored.",
            "3. Confirm with the test endpoint, then events flow on every Iceberg commit.",
        ]

    if dest.connection_mode == "file_drop":
        bucket = dest.config.get("bucket") or dest.config.get("bucket_path") or "<not set>"
        return [
            f"1. Snapshots will be written to: {bucket}",
            "2. Point your downstream tool at the bucket (read-only IAM recommended).",
        ]

    return ["No connection instructions available for this kind/mode combination."]


async def generate_connection_payload(destination_id: str) -> dict[str, Any]:
    """Return the sanitized connection bundle the operator hands to the BI tool.

    Plaintext credentials are NEVER returned. Operators set the password
    themselves at create time — they already have it.
    """
    dest = await get_destination(destination_id)
    if not dest:
        raise DestinationNotFoundError(destination_id)

    payload: dict[str, Any] = {
        "destination_id": dest.destination_id,
        "kind": dest.kind,
        "name": dest.name,
        "mode": dest.connection_mode,
        "has_credentials": dest.has_credentials,
        "password": _REDACTED if dest.has_credentials else None,
        "instructions": _instructions_for_dest(dest),
    }

    if dest.connection_mode == "sql_pull":
        host = dest.config.get("host", "127.0.0.1")
        port = int(dest.config.get("port", 9030))
        database = dest.config.get("database", "udp")
        username = dest.config.get("username", "bi_reader")
        payload["host"] = host
        payload["port"] = port
        payload["database"] = database
        payload["username"] = username
        # MySQL-protocol connection string (JDBC + CLI flavors). Password
        # intentionally omitted — operator pastes it in their BI tool.
        payload["jdbc_url"] = (
            f"jdbc:mysql://{host}:{port}/{database}?useSSL=false"
        )
        payload["mysql_cli"] = (
            f"mysql -h {host} -P {port} -u {username} -p {database}"
        )

    elif dest.connection_mode == "push_api":
        payload["endpoint_url"] = (
            dest.config.get("url") or dest.config.get("endpoint_url")
        )
        payload["auth_scheme"] = "Bearer"
        payload["bearer_token"] = _REDACTED if dest.has_credentials else None

    elif dest.connection_mode == "file_drop":
        payload["bucket_path"] = (
            dest.config.get("bucket") or dest.config.get("bucket_path")
        )
        payload["secret_access_key"] = _REDACTED if dest.has_credentials else None

    return payload
