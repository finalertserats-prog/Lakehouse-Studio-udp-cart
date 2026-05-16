"""Unit tests for the MySQL data-source path.

Hermetic — no network. The `test_source` test mocks `pymysql.connect`; the
ingest stub test pokes the IngestJob registry directly. The CRUD tests just
exercise the on-disk registry (which `conftest.py`'s `LHS_WORK_DIR` fixture
should ideally isolate; we isolate per-test by using uuid-prefixed install
ids so we don't collide with other suites' fixtures).
"""
from __future__ import annotations

import asyncio
import uuid
from unittest.mock import MagicMock, patch

import pytest

from backend import data_sources as ds_mod
from backend import ingest as ingest_mod


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# A credential string that comfortably passes the v0.5 hardened strength
# policy: 14 chars, letter + digit + special char, not blacklisted.
_STRONG_SECRET = "MysqlT3st!Cred#"
_WEAK_SECRET = "abc"  # too short, no digit, no special


def _new_install_id() -> str:
    return f"inst_{uuid.uuid4().hex[:10]}"


def _mysql_create_request(secret: str) -> ds_mod.DataSourceCreateRequest:
    return ds_mod.DataSourceCreateRequest(
        kind="mysql",
        name="test mysql",
        host="db.example.com",
        port=3306,
        database="appdb",
        username="appuser",
        password=secret,
    )


# ---------------------------------------------------------------------------
# 1. Weak credential rejection on create
# ---------------------------------------------------------------------------

def test_create_mysql_source_rejects_weak_credential():
    install_id = _new_install_id()
    req = _mysql_create_request(_WEAK_SECRET)
    with pytest.raises(ds_mod.WeakPasswordError):
        asyncio.run(ds_mod.create_source(install_id, req))


# ---------------------------------------------------------------------------
# 2. Create + list shows the MySQL source
# ---------------------------------------------------------------------------

def test_create_and_list_mysql_source_round_trips():
    install_id = _new_install_id()
    req = _mysql_create_request(_STRONG_SECRET)

    created = asyncio.run(ds_mod.create_source(install_id, req))
    assert created.kind == "mysql"
    assert created.host == "db.example.com"
    assert created.port == 3306
    assert created.has_password is True
    # The DataSource response NEVER carries the cleartext credential.
    assert not hasattr(created, "password")

    listed = asyncio.run(ds_mod.list_sources(install_id))
    assert any(s.source_id == created.source_id for s in listed)
    assert all(s.install_id == install_id for s in listed)

    # Cleanup so we don't leak rows into other tests' on-disk store.
    asyncio.run(ds_mod.delete_source(created.source_id))


# ---------------------------------------------------------------------------
# 3. test_source with mocked pymysql.connect returns the expected shape
# ---------------------------------------------------------------------------

def test_test_source_mysql_returns_expected_shape():
    install_id = _new_install_id()
    req = _mysql_create_request(_STRONG_SECRET)
    created = asyncio.run(ds_mod.create_source(install_id, req))

    # Build a fake pymysql cursor that responds to the 3 queries the
    # _sync_test_mysql worker issues, in order:
    #   1. SELECT 1            -> (1,)
    #   2. SELECT VERSION()    -> ('8.0.36',)
    #   3. SHOW DATABASES      -> 4 rows incl. system schemas that should
    #                              be filtered out by the worker.
    fake_cursor = MagicMock()
    fake_cursor.fetchone.side_effect = [(1,), ("8.0.36",)]
    fake_cursor.fetchall.return_value = [
        ("appdb",),
        ("analytics",),
        ("information_schema",),  # system — should be filtered
        ("mysql",),                # system — should be filtered
    ]
    # The worker uses `with conn.cursor() as cur:` so the context manager
    # must yield our cursor.
    fake_cursor.__enter__ = MagicMock(return_value=fake_cursor)
    fake_cursor.__exit__ = MagicMock(return_value=False)

    fake_conn = MagicMock()
    fake_conn.cursor.return_value = fake_cursor
    fake_conn.close.return_value = None

    fake_pymysql = MagicMock()
    fake_pymysql.connect.return_value = fake_conn

    # The worker does `import pymysql` at call time. Inject the fake module.
    with patch.dict("sys.modules", {"pymysql": fake_pymysql}):
        result = asyncio.run(ds_mod.test_source(created.source_id))

    assert result["ok"] is True
    assert result["server_version"] == "8.0.36"
    # System schemas filtered out.
    assert "information_schema" not in result["schemas"]
    assert "mysql" not in result["schemas"]
    assert "appdb" in result["schemas"]
    assert "analytics" in result["schemas"]
    assert result["error"] is None
    assert isinstance(result["latency_ms"], int)

    # pymysql.connect was called with our source's connection facts.
    fake_pymysql.connect.assert_called_once()
    kwargs = fake_pymysql.connect.call_args.kwargs
    assert kwargs["host"] == "db.example.com"
    assert kwargs["port"] == 3306
    assert kwargs["database"] == "appdb"
    assert kwargs["user"] == "appuser"

    asyncio.run(ds_mod.delete_source(created.source_id))


# ---------------------------------------------------------------------------
# 4. kick_off_mysql_ingest -- without JDBC extras enabled, returns a failed
#    job with the actionable "enable JDBC first" message.
#
# v0.5.1 replaced the lifecycle stub with the real JDBC spark-submit path
# (see backend/jdbc_extras.py + the new POST /api/installs/.../jdbc/enable
# route). When the operator hasn't run the enable route yet, the job is
# created in the failed state immediately so the UI surfaces a clear
# next-action message rather than dispatching a doomed spark-submit.
# ---------------------------------------------------------------------------

def test_kick_off_mysql_ingest_without_jdbc_extras_fails_with_actionable_message():
    install_id = _new_install_id()

    # Register the install in the state store so the new READY-state guard
    # passes (the real ingest path verifies install.state == "READY", same
    # as the CSV path). The install_dir doesn't need real on-disk files
    # for this test -- the JDBC enable check returns False before any FS
    # operations on the install_dir would happen.
    from pathlib import Path as _P
    import tempfile as _tf
    from backend.state import store as _store
    tmp_dir = _P(_tf.mkdtemp(prefix=f"lhs_test_{install_id}_"))
    rec = _store.create(
        stack_id="udp-local-v0.2",
        host="local",
        install_dir=str(tmp_dir),
        steps=[],
    )
    # The real install_id is whatever store.create generated; rebind so
    # the data-source row below references it.
    install_id = rec.install_id
    _store.update_state(install_id, "READY")

    req = _mysql_create_request(_STRONG_SECRET)
    created = asyncio.run(ds_mod.create_source(install_id, req))

    async def _run() -> ingest_mod.IngestJob:
        job = await ingest_mod.kick_off_mysql_ingest(
            install_id=install_id,
            source_id=created.source_id,
            table_name="orders",
            target={"database": "ingest", "table": "orders"},
        )
        return ingest_mod.get_job(job.job_id) or job

    final = asyncio.run(_run())

    assert final.kind == "mysql"
    assert final.state == "failed"
    assert final.error is not None
    # The new actionable message points the operator at the enable route.
    assert "/jdbc/enable" in final.error
    assert final.target == {"database": "ingest", "table": "orders"}
    assert final.source["source_id"] == created.source_id
    assert final.source["remote_table"] == "orders"

    asyncio.run(ds_mod.delete_source(created.source_id))
    # Best-effort cleanup of the store row + tmp dir.
    with _store._lock:  # type: ignore[attr-defined]
        _store._records.pop(install_id, None)  # type: ignore[attr-defined]
        _store._persist_locked(force=True)  # type: ignore[attr-defined]
    try:
        import shutil as _sh
        _sh.rmtree(tmp_dir, ignore_errors=True)
    except Exception:
        pass
