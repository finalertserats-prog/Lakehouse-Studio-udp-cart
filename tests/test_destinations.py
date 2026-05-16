"""Unit tests for backend/destinations.py + backend/insyght_connector.py.

Hermetic — no docker, no network. The sql_pull tester mocks pymysql; the
push_api tester mocks httpx.AsyncClient; the provision path mocks the
admin SQL subprocess helper.

We use uuid-prefixed install ids so we don't collide with other suites'
on-disk registry state (same pattern as tests/test_data_sources_mysql.py).
"""
from __future__ import annotations

import asyncio
import json
import shutil
import tempfile
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from backend import destinations as dst_mod
from backend import insyght_connector as insyght_mod


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Strong-looking string; the destinations module itself doesn't enforce a
# strength policy on its credentials dict — that's a per-connector concern
# (insyght_connector validates length + forbidden chars before SQL interp).
_PW = "DestT3st!Pass#2026"


def _new_install_id() -> str:
    return f"inst_{uuid.uuid4().hex[:10]}"


def _ready_install() -> str:
    """Create a store-record in READY state so the route-layer guard would pass.

    The destinations module itself doesn't check install state — that's the
    route handler's job — but tests that exercise both layers use this.
    """
    from backend.state import store as _store
    tmp_dir = Path(tempfile.mkdtemp(prefix="lhs_dest_test_"))
    rec = _store.create(
        stack_id="udp-local-v0.2",
        host="local",
        install_dir=str(tmp_dir),
        steps=[],
    )
    _store.update_state(rec.install_id, "READY")
    return rec.install_id


def _insyght_sql_pull_request() -> dst_mod.DestinationCreateRequest:
    return dst_mod.DestinationCreateRequest(
        kind="insyght",
        name="Insyght prod",
        connection_mode="sql_pull",
        config={
            "host": "127.0.0.1",
            "port": 9030,
            "database": "udp",
            "username": "insyght_reader",
        },
        credentials={"password": _PW},
    )


# ---------------------------------------------------------------------------
# 1. Create + list + scrubbing
# ---------------------------------------------------------------------------

def test_create_insyght_destination_sql_pull_round_trips():
    install_id = _new_install_id()
    req = _insyght_sql_pull_request()

    created = asyncio.run(dst_mod.create_destination(install_id, req))
    assert created.kind == "insyght"
    assert created.connection_mode == "sql_pull"
    assert created.has_credentials is True
    # Public model has NO `credentials` / `password` field.
    assert not hasattr(created, "credentials")
    assert not hasattr(created, "password")

    listed = asyncio.run(dst_mod.list_destinations(install_id))
    assert any(d.destination_id == created.destination_id for d in listed)

    # Cleanup so we don't leak the row into other tests' on-disk store.
    asyncio.run(dst_mod.delete_destination(created.destination_id))


# ---------------------------------------------------------------------------
# 2. Credentials are encrypted at rest and not returned via any public model
# ---------------------------------------------------------------------------

def test_credentials_encrypted_at_rest_and_never_returned():
    install_id = _new_install_id()
    req = _insyght_sql_pull_request()
    created = asyncio.run(dst_mod.create_destination(install_id, req))

    # Pull the raw on-disk entry. It must NOT contain the plaintext password.
    with dst_mod._DEST_LOCK:
        entry = dst_mod._DEST_STORE[created.destination_id]
        on_disk_blob = json.dumps(entry, default=str)
    assert _PW not in on_disk_blob, "plaintext password leaked into the on-disk entry!"
    assert entry["encrypted_credentials"] is not None

    # Public model serialization also has no plaintext.
    public = created.model_dump()
    assert _PW not in json.dumps(public)
    assert "credentials" not in public
    assert "password" not in public

    # Round-trip decrypt confirms the right value is stored.
    decrypted = dst_mod._decrypt_credentials(created.destination_id)
    assert decrypted == {"password": _PW}

    asyncio.run(dst_mod.delete_destination(created.destination_id))


# ---------------------------------------------------------------------------
# 3. test_destination dispatches to pymysql for sql_pull and returns the
#    expected envelope.
# ---------------------------------------------------------------------------

def test_test_destination_sql_pull_mocks_pymysql():
    install_id = _new_install_id()
    req = _insyght_sql_pull_request()
    created = asyncio.run(dst_mod.create_destination(install_id, req))

    fake_cursor = MagicMock()
    fake_cursor.fetchone.side_effect = [(1,), ("StarRocks 3.3.12",)]
    fake_cursor.__enter__ = MagicMock(return_value=fake_cursor)
    fake_cursor.__exit__ = MagicMock(return_value=False)
    fake_conn = MagicMock()
    fake_conn.cursor.return_value = fake_cursor
    fake_conn.close.return_value = None
    fake_pymysql = MagicMock()
    fake_pymysql.connect.return_value = fake_conn

    with patch.dict("sys.modules", {"pymysql": fake_pymysql}):
        result = asyncio.run(dst_mod.test_destination(created.destination_id))

    assert result["ok"] is True
    assert result["server_version"] == "StarRocks 3.3.12"
    assert result["error"] is None
    assert isinstance(result["latency_ms"], int)
    # last_tested_at advanced.
    refreshed = asyncio.run(dst_mod.get_destination(created.destination_id))
    assert refreshed.last_tested_at is not None

    # Connect call carried the config + the decrypted credential.
    kwargs = fake_pymysql.connect.call_args.kwargs
    assert kwargs["host"] == "127.0.0.1"
    assert kwargs["port"] == 9030
    assert kwargs["database"] == "udp"
    assert kwargs["user"] == "insyght_reader"
    assert kwargs["password"] == _PW

    asyncio.run(dst_mod.delete_destination(created.destination_id))


# ---------------------------------------------------------------------------
# 4. test_destination dispatches to httpx for push_api and returns the
#    expected envelope.
# ---------------------------------------------------------------------------

def test_test_destination_push_api_mocks_httpx():
    install_id = _new_install_id()
    req = dst_mod.DestinationCreateRequest(
        kind="insyght",
        name="Insyght webhook",
        connection_mode="push_api",
        config={"url": "https://insyght.example/lhs/events"},
        credentials={"bearer_token": "tok-fake-xyz"},
    )
    created = asyncio.run(dst_mod.create_destination(install_id, req))

    fake_response = MagicMock()
    fake_response.status_code = 200
    fake_response.text = "{}"

    fake_client = AsyncMock()
    fake_client.post = AsyncMock(return_value=fake_response)
    fake_client.__aenter__ = AsyncMock(return_value=fake_client)
    fake_client.__aexit__ = AsyncMock(return_value=False)

    # _test_push_api does a local `import httpx`. Replace the cached module
    # entry so the local import picks up our fake.
    import sys
    fake_httpx = MagicMock()
    fake_httpx.AsyncClient = MagicMock(return_value=fake_client)
    with patch.dict(sys.modules, {"httpx": fake_httpx}):
        result = asyncio.run(dst_mod.test_destination(created.destination_id))

    assert result["ok"] is True
    assert result["status_code"] == 200
    assert result["error"] is None

    asyncio.run(dst_mod.delete_destination(created.destination_id))


# ---------------------------------------------------------------------------
# 5. provision_sql_pull builds the right SQL and the admin runner is invoked
#    with that SQL.
# ---------------------------------------------------------------------------

def test_provision_sql_pull_builds_create_user_and_grant():
    install_id = _new_install_id()
    req = _insyght_sql_pull_request()
    created = asyncio.run(dst_mod.create_destination(install_id, req))
    creds = dst_mod._decrypt_credentials(created.destination_id)

    captured: dict = {}

    async def fake_admin_sql(sql, container=None, timeout=30):
        captured["sql"] = sql
        return {"success": True, "stdout": "", "stderr": None}

    with patch.object(insyght_mod, "_run_admin_sql", side_effect=fake_admin_sql):
        result = asyncio.run(insyght_mod.provision_sql_pull(
            install_id, dict(created.config), creds,
        ))

    assert result["success"] is True
    assert result["user_created"] is True
    sql = captured["sql"]
    # Both statements present, with the validated identifiers interpolated.
    assert "CREATE USER 'insyght_reader'@'%'" in sql
    assert "GRANT SELECT_PRIV ON udp.* TO 'insyght_reader'@'%'" in sql
    # IMPORTANT: the password ends up in the SQL (it must — that's how
    # MySQL CREATE USER works) but it never escapes the closure.
    assert _PW in sql  # validates the interpolation worked

    asyncio.run(dst_mod.delete_destination(created.destination_id))


# ---------------------------------------------------------------------------
# 6. connection_payload returns the right shape and NO plaintext credential.
# ---------------------------------------------------------------------------

def test_connection_payload_omits_plaintext_credentials():
    install_id = _new_install_id()
    req = _insyght_sql_pull_request()
    created = asyncio.run(dst_mod.create_destination(install_id, req))

    payload = asyncio.run(
        dst_mod.generate_connection_payload(created.destination_id)
    )

    # Required shape.
    assert payload["kind"] == "insyght"
    assert payload["mode"] == "sql_pull"
    assert payload["host"] == "127.0.0.1"
    assert payload["port"] == 9030
    assert payload["database"] == "udp"
    assert payload["username"] == "insyght_reader"
    assert payload["jdbc_url"].startswith("jdbc:mysql://127.0.0.1:9030/udp")
    assert payload["mysql_cli"].startswith("mysql -h 127.0.0.1 -P 9030")
    # Instructions list is present and non-empty.
    assert isinstance(payload["instructions"], list)
    assert len(payload["instructions"]) >= 5

    # Plaintext credential MUST be absent everywhere in the payload.
    blob = json.dumps(payload)
    assert _PW not in blob
    assert payload["has_credentials"] is True
    # The redaction marker appears where the password would.
    assert payload["password"] == dst_mod._REDACTED

    asyncio.run(dst_mod.delete_destination(created.destination_id))


# ---------------------------------------------------------------------------
# 7. Delete works, and after delete the destination is unknown.
# ---------------------------------------------------------------------------

def test_delete_destination_removes_record():
    install_id = _new_install_id()
    req = _insyght_sql_pull_request()
    created = asyncio.run(dst_mod.create_destination(install_id, req))

    assert asyncio.run(dst_mod.get_destination(created.destination_id)) is not None
    asyncio.run(dst_mod.delete_destination(created.destination_id))
    assert asyncio.run(dst_mod.get_destination(created.destination_id)) is None
    # Listing also no longer surfaces it.
    listed = asyncio.run(dst_mod.list_destinations(install_id))
    assert not any(d.destination_id == created.destination_id for d in listed)


# ---------------------------------------------------------------------------
# 8. Unknown destination_id raises DestinationNotFoundError on test+payload.
# ---------------------------------------------------------------------------

def test_unknown_destination_id_raises_not_found():
    bogus = "dst_does_not_exist_xx"
    with pytest.raises(dst_mod.DestinationNotFoundError):
        asyncio.run(dst_mod.test_destination(bogus))
    with pytest.raises(dst_mod.DestinationNotFoundError):
        asyncio.run(dst_mod.generate_connection_payload(bogus))
    # get_destination returns None (does NOT raise).
    assert asyncio.run(dst_mod.get_destination(bogus)) is None
    # delete is idempotent — no exception, no row to remove.
    asyncio.run(dst_mod.delete_destination(bogus))


# ---------------------------------------------------------------------------
# 9. Route-layer guard: POST /test refuses if install != READY.
#    Tests the route handler directly through FastAPI's TestClient.
# ---------------------------------------------------------------------------

def test_route_test_refuses_if_install_not_ready():
    """The /test route is supposed to refuse sql_pull destinations whose
    install isn't in READY state. We invoke the route handler directly
    (TestClient has a starlette/httpx version mismatch on this env) and
    expect HTTPException(409)."""
    from fastapi import HTTPException
    from backend.main import post_destination_test, _require_install_ready
    from backend.state import store as _store

    tmp_dir = Path(tempfile.mkdtemp(prefix="lhs_dest_routetest_"))
    rec = _store.create(
        stack_id="udp-local-v0.2",
        host="local",
        install_dir=str(tmp_dir),
        steps=[],
    )
    # Explicitly NOT READY.
    _store.update_state(rec.install_id, "INSPECTING")

    # Manually persist a sql_pull destination tied to this install.
    req = _insyght_sql_pull_request()
    created = asyncio.run(dst_mod.create_destination(rec.install_id, req))

    # Direct handler invocation: should raise 409.
    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(post_destination_test(created.destination_id))
    assert exc_info.value.status_code == 409
    assert "READY" in str(exc_info.value.detail)

    # Sanity: bumping state to READY removes the guard.
    _store.update_state(rec.install_id, "READY")
    # We don't actually want to hit pymysql here, so just verify
    # _require_install_ready no longer raises.
    _require_install_ready(rec.install_id)  # should not raise

    asyncio.run(dst_mod.delete_destination(created.destination_id))
    with _store._lock:  # type: ignore[attr-defined]
        _store._records.pop(rec.install_id, None)  # type: ignore[attr-defined]
        _store._persist_locked(force=True)  # type: ignore[attr-defined]
    shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# 10. Identifier validation: bad kind / connection_mode rejected by Pydantic.
# ---------------------------------------------------------------------------

def test_bad_kind_or_mode_rejected():
    with pytest.raises(Exception):  # Pydantic ValidationError
        dst_mod.DestinationCreateRequest(
            kind="nope-bad",  # not in DestinationKind Literal
            name="x",
            connection_mode="sql_pull",
        )
    with pytest.raises(Exception):
        dst_mod.DestinationCreateRequest(
            kind="insyght",
            name="x",
            connection_mode="nope-mode",  # not in ConnectionMode Literal
        )
    # Control chars in name are rejected (the _NAME_RE regex doesn't allow them).
    with pytest.raises(Exception):
        dst_mod.DestinationCreateRequest(
            kind="insyght",
            name="bad\x00name",
            connection_mode="sql_pull",
        )


# ---------------------------------------------------------------------------
# 11. Insyght provisioner refuses unsafe passwords (single-quote / backslash).
# ---------------------------------------------------------------------------

def test_insyght_provisioner_refuses_dangerous_password_chars():
    install_id = _new_install_id()
    req = dst_mod.DestinationCreateRequest(
        kind="insyght",
        name="bad pw insyght",
        connection_mode="sql_pull",
        config={
            "host": "127.0.0.1", "port": 9030,
            "database": "udp", "username": "insyght_reader",
        },
        credentials={"password": "has'quote"},
    )
    created = asyncio.run(dst_mod.create_destination(install_id, req))
    creds = dst_mod._decrypt_credentials(created.destination_id)

    with pytest.raises(ValueError):
        asyncio.run(insyght_mod.provision_sql_pull(
            install_id, dict(created.config), creds,
        ))

    asyncio.run(dst_mod.delete_destination(created.destination_id))
