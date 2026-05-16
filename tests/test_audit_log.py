"""Unit tests for backend.audit_log.

Hermetic — every test isolates the DB into a tmp_path so we never touch the
real `work/audit.sqlite`. The bus is monkey-patched per test where needed so
state mutations from one subscriber test never leak into another.
"""
from __future__ import annotations

import asyncio
import os
import time
import uuid

import pytest

from backend import audit_log
from backend.events import bus
from backend.models import LogEvent


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _new_install_id() -> str:
    return f"inst_{uuid.uuid4().hex[:10]}"


@pytest.fixture(autouse=True)
def _reset_subscriber():
    """Each test starts with no cached subscriber and a guaranteed-untouched
    bus (the test that monkey-patches via start() also reliably restores
    via stop(), but we belt-and-suspenders here)."""
    audit_log.reset_subscriber_for_tests()
    orig_pub = bus.publish
    orig_pub_nowait = bus.publish_nowait
    yield
    # Restore the bus methods in case a test errored before stop() could run.
    bus.publish = orig_pub
    bus.publish_nowait = orig_pub_nowait
    audit_log.reset_subscriber_for_tests()


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / "audit.sqlite"


# ---------------------------------------------------------------------------
# Test 1 — is_enabled defaults False
# ---------------------------------------------------------------------------


def test_is_enabled_defaults_false(monkeypatch):
    """An unset env var must yield False so v0.4 behaviour is unchanged."""
    monkeypatch.delenv("LHS_AUDIT_ENABLED", raising=False)
    assert audit_log.is_enabled() is False


def test_is_enabled_truthy_values(monkeypatch):
    for val in ("true", "TRUE", "1", "yes", "on", "  True  "):
        monkeypatch.setenv("LHS_AUDIT_ENABLED", val)
        assert audit_log.is_enabled() is True, f"value {val!r} should be truthy"


def test_is_enabled_falsy_values(monkeypatch):
    for val in ("false", "0", "no", "off", "", "anything-else"):
        monkeypatch.setenv("LHS_AUDIT_ENABLED", val)
        assert audit_log.is_enabled() is False, f"value {val!r} should be falsy"


# ---------------------------------------------------------------------------
# Test 2 — write + query round-trip
# ---------------------------------------------------------------------------


def test_write_and_query_round_trip(db_path):
    """A written entry must be retrievable by every filter dimension."""
    audit_log.init_audit_db(db_path)

    install_id = _new_install_id()
    entry = audit_log.AuditEntry(
        ts=time.time(),
        actor="usr_alice",
        action="install.create",
        resource_type="install",
        resource_id=install_id,
        redacted_payload={"stack_id": "udp-local-v0.2", "host": "localhost"},
        ip="127.0.0.1",
    )

    asyncio.run(audit_log.write(entry, sqlite_path=db_path))

    # No-filter query returns the row.
    all_rows = asyncio.run(audit_log.query(sqlite_path=db_path))
    assert len(all_rows) == 1
    fetched = all_rows[0]
    assert fetched.entry_id == entry.entry_id
    assert fetched.actor == "usr_alice"
    assert fetched.action == "install.create"
    assert fetched.resource_type == "install"
    assert fetched.resource_id == install_id
    assert fetched.redacted_payload["stack_id"] == "udp-local-v0.2"
    assert fetched.redacted_payload["host"] == "localhost"
    assert fetched.ip == "127.0.0.1"

    # Filter by actor.
    by_actor = asyncio.run(audit_log.query(actor="usr_alice", sqlite_path=db_path))
    assert len(by_actor) == 1
    by_other = asyncio.run(audit_log.query(actor="usr_bob", sqlite_path=db_path))
    assert by_other == []

    # Filter by action.
    by_action = asyncio.run(audit_log.query(action="install.create", sqlite_path=db_path))
    assert len(by_action) == 1
    by_other_action = asyncio.run(audit_log.query(action="install.delete", sqlite_path=db_path))
    assert by_other_action == []

    # Filter by resource_type.
    by_rtype = asyncio.run(audit_log.query(resource_type="install", sqlite_path=db_path))
    assert len(by_rtype) == 1
    by_other_rtype = asyncio.run(audit_log.query(resource_type="backup", sqlite_path=db_path))
    assert by_other_rtype == []

    # Filter by since_ts in the future — must return nothing.
    by_future = asyncio.run(audit_log.query(since_ts=time.time() + 3600, sqlite_path=db_path))
    assert by_future == []


def test_query_when_db_missing_returns_empty(tmp_path):
    """Query must NOT crash if the DB file has not been created yet."""
    missing = tmp_path / "never-created.sqlite"
    result = asyncio.run(audit_log.query(sqlite_path=missing))
    assert result == []


# ---------------------------------------------------------------------------
# Test 3 — retention_prune deletes old entries
# ---------------------------------------------------------------------------


def test_retention_prune_deletes_old_entries(db_path):
    """Rows whose ts is older than the cutoff must be removed; newer rows kept."""
    audit_log.init_audit_db(db_path)

    now = time.time()
    old_ts = now - (100 * 86400.0)     # 100 days ago
    recent_ts = now - (10 * 86400.0)   # 10 days ago

    old_entry = audit_log.AuditEntry(
        ts=old_ts,
        actor="system",
        action="install.state_change",
        resource_type="install",
        resource_id=_new_install_id(),
        redacted_payload={"status": "READY"},
    )
    recent_entry = audit_log.AuditEntry(
        ts=recent_ts,
        actor="system",
        action="install.state_change",
        resource_type="install",
        resource_id=_new_install_id(),
        redacted_payload={"status": "READY"},
    )
    asyncio.run(audit_log.write(old_entry, sqlite_path=db_path))
    asyncio.run(audit_log.write(recent_entry, sqlite_path=db_path))

    # Sanity: both rows present.
    pre = asyncio.run(audit_log.query(sqlite_path=db_path))
    assert len(pre) == 2

    deleted = asyncio.run(audit_log.retention_prune(older_than_days=90, sqlite_path=db_path))
    assert deleted == 1

    post = asyncio.run(audit_log.query(sqlite_path=db_path))
    assert len(post) == 1
    assert abs(post[0].ts - recent_ts) < 1.0  # the recent row survived


def test_retention_prune_zero_days_rejected(db_path):
    """older_than_days must be > 0 — protects against accidental nuke."""
    audit_log.init_audit_db(db_path)
    with pytest.raises(ValueError):
        asyncio.run(audit_log.retention_prune(older_than_days=0, sqlite_path=db_path))


# ---------------------------------------------------------------------------
# Test 4 — subscriber redacts secret-looking fields before write
# ---------------------------------------------------------------------------


def test_subscriber_redacts_secrets_before_write(db_path):
    """End-to-end: publish a LogEvent that carries a secret in the line and
    payload, then verify the persisted row has the secret scrubbed."""

    async def _run():
        sub = audit_log.AuditSubscriber(sqlite_path=db_path)
        await sub.start()
        try:
            install_id = _new_install_id()
            evt = LogEvent(
                install_id=install_id,
                ts=time.time(),
                kind="state",
                status="READY",
                line='MINIO_ROOT_PASSWORD=supersecretpw123 starting up',
                payload={
                    "AWS_SECRET_ACCESS_KEY": "AKIAEXAMPLEKEY123456",
                    "details": {
                        "POSTGRES_PASSWORD": "anothersecret!",
                        "nested_token": "BEARER eyJsupposedJWT.payload.sig",
                    },
                    "harmless": "ok",
                },
            )
            bus.publish_nowait(evt)
            # Give the subscriber's queue + write thread time to drain.
            for _ in range(50):
                rows = await audit_log.query(
                    resource_type="install",
                    sqlite_path=db_path,
                )
                if rows:
                    break
                await asyncio.sleep(0.05)
            assert rows, "subscriber never persisted the event"
            row = rows[0]
            assert row.action == "install.state_change"
            assert row.resource_id == install_id
            # `line` is redacted — the original password substring must NOT
            # appear anywhere in the persisted line.
            line = row.redacted_payload.get("line", "")
            assert "supersecretpw123" not in line
            assert "MINIO_ROOT_PASSWORD" in line  # key kept, value masked
            assert "********" in line
            # Payload string values are redacted recursively.
            persisted_payload = row.redacted_payload.get("payload", {})
            secret_val = persisted_payload.get("AWS_SECRET_ACCESS_KEY", "")
            assert "AKIAEXAMPLEKEY123456" not in secret_val
            nested = persisted_payload.get("details", {})
            assert "anothersecret!" not in nested.get("POSTGRES_PASSWORD", "")
            # Non-secret values pass through untouched.
            assert persisted_payload.get("harmless") == "ok"
        finally:
            await sub.stop()

    asyncio.run(_run())


def test_subscriber_skips_uninteresting_kinds(db_path):
    """`log` events are the firehose — they must NOT be persisted, only the
    audited kinds (state / error / step_start / step_end) make it through."""

    async def _run():
        sub = audit_log.AuditSubscriber(sqlite_path=db_path)
        await sub.start()
        try:
            install_id = _new_install_id()
            for _ in range(5):
                bus.publish_nowait(LogEvent(
                    install_id=install_id,
                    ts=time.time(),
                    kind="log",
                    stream="stdout",
                    line="routine progress message",
                ))
            # Now a state event that SHOULD be persisted.
            bus.publish_nowait(LogEvent(
                install_id=install_id,
                ts=time.time(),
                kind="state",
                status="READY",
            ))
            for _ in range(50):
                rows = await audit_log.query(sqlite_path=db_path)
                if rows:
                    break
                await asyncio.sleep(0.05)
            # Exactly one row — the state event. The five `log` events were
            # filtered out before persist.
            assert len(rows) == 1
            assert rows[0].action == "install.state_change"
        finally:
            await sub.stop()

    asyncio.run(_run())
