"""Audit log persistence (v0.5 — opt-in, pure additive).

Subscribes to the in-process event bus, filters for state transitions and
critical errors, scrubs payloads through :mod:`backend.redact`, and persists
the result to a separate SQLite DB at ``WORK_DIR/audit.sqlite``.

Enable with ``LHS_AUDIT_ENABLED=true``. Retention defaults to 90 days; override
with ``LHS_AUDIT_RETENTION_DAYS=<N>``.

Design constraints honoured:
- backend/runner.py FROZEN — no modifications here, runner just publishes
  to the bus as it always has.
- backend/events.py contract FROZEN — we additively wrap the singleton
  bus instance's ``publish`` / ``publish_nowait`` methods with a tee that
  forwards to a single global asyncio.Queue. Original callers see no
  behavioural change; if the audit subscriber is never started, the wrap
  is never installed and the bus is byte-identical to v0.4.
- Pure stdlib sqlite3 only (no SQLAlchemy / aiosqlite).
- DB writes never block the bus on failure — they log and drop.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any, Optional

from pydantic import BaseModel, Field

from .config import WORK_DIR
from .events import bus
from .models import LogEvent
from .redact import MASK, SECRET_KEYS, redact

log = logging.getLogger("lhs.audit")


# --------------------------------------------------------------------------- #
# Constants                                                                   #
# --------------------------------------------------------------------------- #

_ENABLED_ENV = "LHS_AUDIT_ENABLED"
_RETENTION_ENV = "LHS_AUDIT_RETENTION_DAYS"
_DEFAULT_RETENTION_DAYS = 90

# Default tenant id used when audit runs alongside the v0.x single-tenant
# state store. The full multi-tenant schema requires a tenant FK, but the
# audit SQLite is standalone (no tenants table loaded) — we still write the
# column for forward compatibility with the v1.0 migration.
_DEFAULT_TENANT_ID = "default"


# --------------------------------------------------------------------------- #
# Model                                                                       #
# --------------------------------------------------------------------------- #


class AuditEntry(BaseModel):
    """One row in the audit log.

    Mirrors the v1.0 multi-tenant ``audit_log`` table from
    ``backend/v1/multi_tenant_schema.py`` minus the tenant + user FKs (those
    are required at the v1.0 schema level but not yet meaningful in v0.5).
    """

    entry_id: str = Field(default_factory=lambda: f"aud_{uuid.uuid4().hex[:16]}")
    ts: float
    actor: str  # user_id, or "system"
    action: str  # e.g. "install.create", "install.state_change", "install.error"
    resource_type: str  # e.g. "install"
    resource_id: str
    redacted_payload: dict[str, Any] = Field(default_factory=dict)
    ip: Optional[str] = None


# --------------------------------------------------------------------------- #
# Enable / DB init                                                            #
# --------------------------------------------------------------------------- #


def is_enabled() -> bool:
    """``True`` iff ``LHS_AUDIT_ENABLED`` is set to a truthy value.

    Truthy: ``true``, ``1``, ``yes``, ``on`` (case-insensitive). Anything
    else — including unset — is False, preserving v0.4 default behaviour.
    """
    raw = os.environ.get(_ENABLED_ENV)
    if not raw:
        return False
    return raw.strip().lower() in {"true", "1", "yes", "on"}


def is_scheduler_enabled() -> bool:
    """``True`` if ``LHS_AUDIT_SCHEDULER_ENABLED`` is truthy, defaulting to ``is_enabled()``."""
    raw = os.environ.get("LHS_AUDIT_SCHEDULER_ENABLED")
    if not raw:
        return is_enabled()
    return raw.strip().lower() in {"true", "1", "yes", "on"}


def _retention_days() -> int:
    raw = os.environ.get(_RETENTION_ENV)
    if not raw:
        return _DEFAULT_RETENTION_DAYS
    try:
        n = int(raw.strip())
    except ValueError:
        log.warning("invalid %s=%r; falling back to %d",
                    _RETENTION_ENV, raw, _DEFAULT_RETENTION_DAYS)
        return _DEFAULT_RETENTION_DAYS
    if n <= 0:
        log.warning("%s=%d must be > 0; falling back to %d",
                    _RETENTION_ENV, n, _DEFAULT_RETENTION_DAYS)
        return _DEFAULT_RETENTION_DAYS
    return n


def _db_path() -> Path:
    return WORK_DIR / "audit.sqlite"


_BUSY_TIMEOUT_MS = 5000  # 5s — accommodates a concurrent retention purge


def _connect(sqlite_path: Path) -> sqlite3.Connection:
    """Open the audit DB with concurrency-safe pragmas.

    Codex-flagged 2026-05-17: raw ``sqlite3.connect`` with no WAL + no
    busy_timeout means a concurrent retention purge can produce
    ``database is locked`` on the subscriber, dropping audit writes.
    WAL + 5s busy_timeout is the standard hardening combo for a
    single-process, multi-thread SQLite workload.
    """
    con = sqlite3.connect(str(sqlite_path), timeout=_BUSY_TIMEOUT_MS / 1000)
    try:
        con.execute(f"PRAGMA busy_timeout = {_BUSY_TIMEOUT_MS}")
        con.execute("PRAGMA journal_mode = WAL")
        con.execute("PRAGMA synchronous = NORMAL")
    except sqlite3.DatabaseError:
        # PRAGMAs are best-effort; never let hardening crash the DB open.
        pass
    return con


def init_audit_db(sqlite_path: Optional[Path] = None) -> Path:
    """Create (idempotent) the ``audit_log`` table at ``WORK_DIR/audit.sqlite``.

    We re-use the CREATE TABLE string from
    :data:`backend.v1.multi_tenant_schema.SCHEMA` so the audit row shape stays
    single-sourced. The standalone DB does NOT carry tenants / users tables,
    so the FK clauses parse but stay dormant — SQLite only enforces FKs when
    the referenced table exists AND ``PRAGMA foreign_keys = ON`` is set.

    Returns the path of the DB.
    """
    # Local import keeps this module import-cheap and avoids dragging the
    # whole v1 package in for users who never enable audit.
    from .v1.multi_tenant_schema import INDEXES, SCHEMA

    path = Path(sqlite_path) if sqlite_path is not None else _db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    con = _connect(path)
    try:
        # We deliberately do NOT enable foreign_keys here: the standalone
        # audit DB has no tenants / users tables, and turning on FK checks
        # would crash inserts. The v1.0 unified DB will keep FKs on.
        cur = con.cursor()
        cur.execute(SCHEMA["audit_log"])
        # Subset of the v1 indexes that doesn't reference unrelated tables.
        for stmt in INDEXES:
            if "audit" in stmt:
                cur.execute(stmt)
        con.commit()
    finally:
        con.close()
    return path


# --------------------------------------------------------------------------- #
# Persistence                                                                 #
# --------------------------------------------------------------------------- #


def _row_to_entry(row: sqlite3.Row | tuple) -> AuditEntry:
    """Map a SELECT row back into an AuditEntry. Columns are ordered by
    the SELECT in :func:`query`.
    """
    entry_id, ts, actor, action, resource_type, resource_id, payload_json, ip = row
    try:
        payload = json.loads(payload_json) if payload_json else {}
        if not isinstance(payload, dict):
            payload = {"_raw": payload}
    except json.JSONDecodeError:
        payload = {"_raw": payload_json}
    return AuditEntry(
        entry_id=str(entry_id),
        ts=float(ts),
        actor=str(actor or "system"),
        action=str(action),
        resource_type=str(resource_type),
        resource_id=str(resource_id or ""),
        redacted_payload=payload,
        ip=str(ip) if ip is not None else None,
    )


def _write_sync(entry: AuditEntry, sqlite_path: Path) -> None:
    """Synchronous insert. Called via ``asyncio.to_thread`` from :func:`write`."""
    payload_json = json.dumps(entry.redacted_payload, default=str)
    con = _connect(sqlite_path)
    try:
        cur = con.cursor()
        # We use the AUTOINCREMENT integer id as the primary key (matches the
        # v1 schema) but ALSO record our own stable entry_id inside the
        # redacted_payload so :func:`query` can round-trip the AuditEntry.
        payload_with_eid = dict(entry.redacted_payload)
        payload_with_eid.setdefault("_entry_id", entry.entry_id)
        payload_json = json.dumps(payload_with_eid, default=str)
        cur.execute(
            "INSERT INTO audit_log "
            "(tenant_id, user_id, action, resource_type, resource_id, ts, ip, redacted_payload) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                _DEFAULT_TENANT_ID,
                entry.actor if entry.actor != "system" else None,
                entry.action,
                entry.resource_type,
                entry.resource_id,
                entry.ts,
                entry.ip,
                payload_json,
            ),
        )
        con.commit()
    finally:
        con.close()


async def write(entry: AuditEntry, sqlite_path: Optional[Path] = None) -> None:
    """Atomic, non-blocking insert. Log + drop on any DB failure.

    The bus must NEVER stall waiting for the audit log — that would couple
    install latency to disk I/O. We dispatch the synchronous insert to a
    worker thread and swallow any sqlite errors with a logged warning.
    """
    path = Path(sqlite_path) if sqlite_path is not None else _db_path()
    try:
        await asyncio.to_thread(_write_sync, entry, path)
    except sqlite3.Error as e:
        log.warning("audit write dropped (%s): %s", entry.action, e)
    except Exception:
        log.exception("unexpected audit write failure (%s)", entry.action)


def _query_sync(
    *,
    sqlite_path: Path,
    actor: Optional[str],
    action: Optional[str],
    resource_type: Optional[str],
    since_ts: Optional[float],
    limit: int,
) -> list[AuditEntry]:
    where: list[str] = []
    params: list[Any] = []
    if actor is not None:
        if actor == "system":
            where.append("user_id IS NULL")
        else:
            where.append("user_id = ?")
            params.append(actor)
    if action is not None:
        where.append("action = ?")
        params.append(action)
    if resource_type is not None:
        where.append("resource_type = ?")
        params.append(resource_type)
    if since_ts is not None:
        where.append("ts >= ?")
        params.append(float(since_ts))

    sql = (
        "SELECT id, ts, COALESCE(user_id, 'system'), action, resource_type, "
        "       COALESCE(resource_id, ''), redacted_payload, ip "
        "FROM audit_log"
    )
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY ts DESC LIMIT ?"
    params.append(max(1, min(int(limit), 5000)))

    con = _connect(sqlite_path)
    try:
        cur = con.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
    finally:
        con.close()

    out: list[AuditEntry] = []
    for r in rows:
        entry = _row_to_entry(r)
        # Restore the stable entry_id we stashed in the payload at write time.
        payload = dict(entry.redacted_payload)
        eid = payload.pop("_entry_id", None)
        if eid:
            entry = entry.model_copy(update={"entry_id": str(eid), "redacted_payload": payload})
        out.append(entry)
    return out


async def query(
    actor: Optional[str] = None,
    action: Optional[str] = None,
    resource_type: Optional[str] = None,
    since_ts: Optional[float] = None,
    limit: int = 200,
    *,
    sqlite_path: Optional[Path] = None,
) -> list[AuditEntry]:
    """Query the audit log. All filters AND together. Newest first."""
    path = Path(sqlite_path) if sqlite_path is not None else _db_path()
    if not path.exists():
        return []
    return await asyncio.to_thread(
        _query_sync,
        sqlite_path=path,
        actor=actor,
        action=action,
        resource_type=resource_type,
        since_ts=since_ts,
        limit=limit,
    )


def _retention_prune_sync(older_than_days: int, sqlite_path: Path) -> int:
    cutoff = time.time() - (older_than_days * 86400.0)
    con = _connect(sqlite_path)
    try:
        cur = con.cursor()
        cur.execute("DELETE FROM audit_log WHERE ts < ?", (cutoff,))
        deleted = cur.rowcount or 0
        con.commit()
    finally:
        con.close()
    return int(deleted)


async def retention_prune(
    older_than_days: int = _DEFAULT_RETENTION_DAYS,
    *,
    sqlite_path: Optional[Path] = None,
) -> int:
    """Delete rows older than ``older_than_days``. Returns the count deleted."""
    if older_than_days <= 0:
        raise ValueError("older_than_days must be > 0")
    path = Path(sqlite_path) if sqlite_path is not None else _db_path()
    if not path.exists():
        return 0
    return await asyncio.to_thread(_retention_prune_sync, older_than_days, path)


# --------------------------------------------------------------------------- #
# Bus subscription                                                            #
# --------------------------------------------------------------------------- #


# Event kinds the audit subscriber considers worth persisting. ``log`` lines
# are firehose-noisy and not actionable for compliance review.
_AUDITED_KINDS: frozenset[str] = frozenset({"state", "error", "step_start", "step_end"})


def _classify(event: LogEvent) -> tuple[str, str]:
    """Map a LogEvent to (action, resource_type) for the audit row.

    The contract is intentionally narrow — any future caller adding a new
    LogEvent kind will fall into the generic ``install.event`` bucket
    without breaking the schema.
    """
    if event.kind == "state":
        return f"install.state_change", "install"
    if event.kind == "error":
        return "install.error", "install"
    if event.kind == "step_start":
        return "install.step_start", "install"
    if event.kind == "step_end":
        return "install.step_end", "install"
    return f"install.{event.kind}", "install"


def _build_payload(event: LogEvent) -> dict[str, Any]:
    """Pull a structured payload out of a LogEvent and scrub strings."""
    raw: dict[str, Any] = {}
    if event.step is not None:
        raw["step"] = event.step
    if event.status is not None:
        raw["status"] = event.status
    if event.stream is not None:
        raw["stream"] = event.stream
    if event.line is not None:
        raw["line"] = redact(event.line)
    if event.payload:
        # Run every string value through redact() so secrets that leaked into
        # structured payloads at publish time are scrubbed before persist.
        raw["payload"] = _redact_payload(event.payload)
    return raw


def _is_secret_key(key: Any) -> bool:
    """``True`` if a dict key matches (case-insensitively) one of the known
    secret env var / config field names. Substring match for keys like
    ``api_token`` -> matches ``TOKEN`` in the SECRET_KEYS set.
    """
    if not isinstance(key, str):
        return False
    upper = key.upper()
    if upper in SECRET_KEYS:
        return True
    return any(sk in upper for sk in SECRET_KEYS)


def _redact_payload(payload: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in payload.items():
        # Defence in depth: if the KEY itself names a secret, mask the value
        # outright. redact()'s regexes only match KEY=VALUE inside a string;
        # they cannot scrub a bare value where the "key" is the dict key.
        if _is_secret_key(k) and isinstance(v, (str, int, float)):
            out[k] = MASK
            continue
        if isinstance(v, str):
            out[k] = redact(v)
        elif isinstance(v, dict):
            out[k] = _redact_payload(v)
        elif isinstance(v, list):
            out[k] = [
                redact(item) if isinstance(item, str)
                else _redact_payload(item) if isinstance(item, dict)
                else item
                for item in v
            ]
        else:
            out[k] = v
    return out


class AuditSubscriber:
    """Background task: tees every bus publish into our own queue, filters,
    redacts, and persists.

    The bus contract is FROZEN, so we install a per-instance method wrap on
    the singleton ``bus`` object when :meth:`start` is called and remove it
    in :meth:`stop`. Original callers (runner, ingest, main) are unchanged
    and unaware. If the wrap fails to install for any reason, the subscriber
    falls back to a no-op task — the rest of the app keeps running.
    """

    def __init__(self, sqlite_path: Optional[Path] = None):
        self._sqlite_path = Path(sqlite_path) if sqlite_path is not None else _db_path()
        self._queue: asyncio.Queue[LogEvent] = asyncio.Queue(maxsize=10000)
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        # Saved originals so :meth:`stop` can restore them exactly.
        self._orig_publish_nowait = None
        self._orig_publish = None

    # ---- bus tee ----

    def _install_bus_tap(self) -> None:
        if self._orig_publish_nowait is not None:
            return  # already tapped

        # Bind originals from the unbound types to avoid recursion when we
        # call them from inside the wrappers.
        orig_pub = bus.publish
        orig_pub_nowait = bus.publish_nowait
        queue = self._queue

        def _tap_event(evt: LogEvent) -> None:
            try:
                queue.put_nowait(evt)
            except asyncio.QueueFull:
                # Subscriber is slow / DB is slow. Drop the audit copy rather
                # than back-pressure the bus.
                pass
            except Exception:
                log.exception("audit tap failed for event %s", getattr(evt, "kind", "?"))

        async def wrapped_publish(event: LogEvent) -> None:
            await orig_pub(event)
            _tap_event(event)

        def wrapped_publish_nowait(event: LogEvent) -> None:
            orig_pub_nowait(event)
            _tap_event(event)

        self._orig_publish = orig_pub
        self._orig_publish_nowait = orig_pub_nowait
        bus.publish = wrapped_publish  # type: ignore[method-assign]
        bus.publish_nowait = wrapped_publish_nowait  # type: ignore[method-assign]

    def _remove_bus_tap(self) -> None:
        if self._orig_publish_nowait is None:
            return
        try:
            bus.publish = self._orig_publish  # type: ignore[method-assign]
            bus.publish_nowait = self._orig_publish_nowait  # type: ignore[method-assign]
        except Exception:
            log.exception("failed to restore bus publish methods")
        self._orig_publish = None
        self._orig_publish_nowait = None

    # ---- lifecycle ----

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        # Ensure the DB exists. init_audit_db is idempotent.
        try:
            init_audit_db(self._sqlite_path)
        except Exception:
            log.exception("audit init_audit_db failed; subscriber disabled")
            return
        self._stop.clear()
        try:
            self._install_bus_tap()
        except Exception:
            log.exception("audit bus tap install failed; subscriber disabled")
            return
        self._task = asyncio.create_task(self._run(), name="audit-subscriber")

    async def stop(self) -> None:
        self._stop.set()
        self._remove_bus_tap()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                evt = await asyncio.wait_for(self._queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                raise
            try:
                await self._handle(evt)
            except Exception:
                # One poisoned event must not crash the long-running task.
                log.exception("audit handler failed for event %s",
                              getattr(evt, "kind", "?"))

    async def _handle(self, event: LogEvent) -> None:
        if event.kind not in _AUDITED_KINDS:
            return
        action, resource_type = _classify(event)
        payload = _build_payload(event)
        entry = AuditEntry(
            ts=float(event.ts or time.time()),
            actor="system",  # bus events are emitted by the orchestrator, not a user
            action=action,
            resource_type=resource_type,
            resource_id=str(event.install_id),
            redacted_payload=payload,
            ip=None,
        )
        await write(entry, sqlite_path=self._sqlite_path)


class RetentionScheduler:
    """Background task: periodically prunes old audit log entries."""

    def __init__(self, sqlite_path: Optional[Path] = None):
        self._sqlite_path = Path(sqlite_path) if sqlite_path is not None else _db_path()
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

    def _interval_seconds(self) -> float:
        raw = os.environ.get("LHS_AUDIT_RETENTION_INTERVAL_SECONDS")
        if not raw:
            return 86400.0
        try:
            n = float(raw.strip())
        except ValueError:
            log.warning("invalid LHS_AUDIT_RETENTION_INTERVAL_SECONDS=%r; falling back to 86400", raw)
            return 86400.0
        if n <= 0:
            log.warning("LHS_AUDIT_RETENTION_INTERVAL_SECONDS=%s must be > 0; falling back to 86400", n)
            return 86400.0
        return n

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        # Ensure the DB exists. init_audit_db is idempotent.
        try:
            init_audit_db(self._sqlite_path)
        except Exception:
            log.exception("audit init_audit_db failed; scheduler disabled")
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="audit-scheduler")

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.sleep(self._interval_seconds())
            except asyncio.CancelledError:
                raise
            if self._stop.is_set():
                break
            try:
                deleted = await retention_prune(_retention_days(), sqlite_path=self._sqlite_path)
                log.info("audit retention_prune deleted %d rows", deleted)
            except Exception:
                log.exception("audit retention_prune failed")


# --------------------------------------------------------------------------- #
# Global singleton                                                            #
# --------------------------------------------------------------------------- #


_GLOBAL_SUBSCRIBER: Optional[AuditSubscriber] = None


def get_subscriber() -> AuditSubscriber:
    """Lazy global accessor — main.py uses this in startup / shutdown."""
    global _GLOBAL_SUBSCRIBER
    if _GLOBAL_SUBSCRIBER is None:
        _GLOBAL_SUBSCRIBER = AuditSubscriber()
    return _GLOBAL_SUBSCRIBER


def reset_subscriber_for_tests() -> None:
    """Drop the cached subscriber so the next ``get_subscriber()`` rebuilds
    one — used by tests that need a fresh queue + DB path."""
    global _GLOBAL_SUBSCRIBER
    _GLOBAL_SUBSCRIBER = None


_GLOBAL_SCHEDULER: Optional[RetentionScheduler] = None


def get_scheduler() -> RetentionScheduler:
    """Lazy global accessor — main.py uses this in startup / shutdown."""
    global _GLOBAL_SCHEDULER
    if _GLOBAL_SCHEDULER is None:
        _GLOBAL_SCHEDULER = RetentionScheduler()
    return _GLOBAL_SCHEDULER


def reset_scheduler_for_tests() -> None:
    """Drop the cached scheduler so the next ``get_scheduler()`` rebuilds
    one — used by tests that need a fresh DB path."""
    global _GLOBAL_SCHEDULER
    _GLOBAL_SCHEDULER = None
