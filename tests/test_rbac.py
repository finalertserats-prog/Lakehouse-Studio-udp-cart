"""Tests for the opt-in RBAC layer.

Hermetic — every test gets its own temp SQLite DB via ``rbac_auth.init_rbac_db``.
No network, no shared mutable state across tests.
"""
from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from backend import bootstrap_rbac
from backend import rbac_auth as rbac_mod

# Best-effort TestClient import. Older Starlette/httpx combos here mismatch
# (Client.__init__ doesn't take `app=`), so the FastAPI-level e2e tests
# below get skipped rather than failed. The rbac_auth module + bootstrap
# CLI are fully covered without TestClient.
try:
    from fastapi.testclient import TestClient as _TestClient
    _TESTCLIENT_OK = True
    try:
        _probe = _TestClient(__import__("fastapi").FastAPI())  # type: ignore[arg-type]
        del _probe
    except TypeError:
        _TESTCLIENT_OK = False
except Exception:  # pragma: no cover - safety net
    _TESTCLIENT_OK = False


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _isolated_rbac_db(tmp_path: Path, monkeypatch):
    """Point every test at its own SQLite file and clear cached Fernet/state."""
    db_path = tmp_path / "rbac.sqlite"
    rbac_mod.init_rbac_db(db_path)
    # Default is OFF; individual tests opt in.
    monkeypatch.delenv("LHS_RBAC_ENABLED", raising=False)
    yield db_path


def _run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# 1. RBAC is OFF by default
# ---------------------------------------------------------------------------


def test_rbac_disabled_by_default(monkeypatch):
    monkeypatch.delenv("LHS_RBAC_ENABLED", raising=False)
    assert rbac_mod.is_rbac_enabled() is False


@pytest.mark.parametrize("flag", ["1", "true", "TRUE", "yes", "on"])
def test_rbac_enabled_when_flag_truthy(monkeypatch, flag):
    monkeypatch.setenv("LHS_RBAC_ENABLED", flag)
    assert rbac_mod.is_rbac_enabled() is True


# ---------------------------------------------------------------------------
# 2. create_user returns a plaintext token ONCE; DB stores only the hash
# ---------------------------------------------------------------------------


def test_create_user_returns_plaintext_once_and_stores_hash():
    user, plaintext = _run(rbac_mod.create_user("ops@example.com", "OWNER"))
    assert user.email == "ops@example.com"
    assert user.role == "OWNER"
    # api_token on the model is the hash, never the plaintext.
    assert user.api_token != plaintext
    assert len(user.api_token) == 64  # sha256 hex
    assert len(plaintext) >= 24  # token_urlsafe(24) => ~32 chars

    # Listing surfaces the user but not the plaintext.
    users = _run(rbac_mod.list_users())
    assert len(users) == 1
    assert users[0].user_id == user.user_id
    assert users[0].api_token == user.api_token


def test_create_user_rejects_unknown_role():
    with pytest.raises(ValueError, match="unknown role"):
        _run(rbac_mod.create_user("ops@example.com", "GOD"))


def test_create_user_rejects_invalid_email():
    with pytest.raises(ValueError, match="email"):
        _run(rbac_mod.create_user("nope", "OWNER"))


# ---------------------------------------------------------------------------
# 3. authenticate round-trips the plaintext token
# ---------------------------------------------------------------------------


def test_authenticate_succeeds_with_bearer_header():
    user, plaintext = _run(rbac_mod.create_user("a@example.com", "ADMIN"))

    got = _run(rbac_mod.authenticate(f"Bearer {plaintext}", None))
    assert got is not None
    assert got.user_id == user.user_id


def test_authenticate_succeeds_with_x_studio_token():
    user, plaintext = _run(rbac_mod.create_user("b@example.com", "OPERATOR"))

    got = _run(rbac_mod.authenticate(None, plaintext))
    assert got is not None
    assert got.user_id == user.user_id


def test_authenticate_rejects_unknown_token():
    _run(rbac_mod.create_user("c@example.com", "VIEWER"))
    got = _run(rbac_mod.authenticate("Bearer NOT_A_REAL_TOKEN_xxxxxxxxxxxxxx", None))
    assert got is None


def test_authenticate_rejects_missing_header():
    got = _run(rbac_mod.authenticate(None, None))
    assert got is None


# ---------------------------------------------------------------------------
# 4. require_permission honours the v1 ROUTE_PERMISSIONS map
# ---------------------------------------------------------------------------


def test_require_permission_owner_passes_protected_route():
    owner, _ = _run(rbac_mod.create_user("owner@example.com", "OWNER"))
    allowed = _run(rbac_mod.require_permission(owner, "/api/installs", "POST"))
    assert allowed is True


def test_require_permission_viewer_blocked_on_install_create():
    viewer, _ = _run(rbac_mod.create_user("viewer@example.com", "VIEWER"))
    allowed = _run(rbac_mod.require_permission(viewer, "/api/installs", "POST"))
    assert allowed is False


def test_require_permission_unmapped_route_passes():
    # Routes that aren't in the v1 ROUTE_PERMISSIONS map have no permission
    # gate and resolve to True for every role.
    viewer, _ = _run(rbac_mod.create_user("v2@example.com", "VIEWER"))
    allowed = _run(rbac_mod.require_permission(viewer, "/healthz", "GET"))
    assert allowed is True


# ---------------------------------------------------------------------------
# 5. bootstrap_rbac CLI: first user OK, second call refused
# ---------------------------------------------------------------------------


def test_bootstrap_creates_first_user_and_refuses_second(capsys, tmp_path, monkeypatch):
    # Re-init so the per-test fixture's DB is fresh and empty.
    db_path = tmp_path / "boot.sqlite"
    rbac_mod.init_rbac_db(db_path)

    rc = bootstrap_rbac.main(["--email", "first@example.com", "--role", "OWNER"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "RBAC bootstrap successful" in out
    assert "first@example.com" in out
    assert "OWNER" in out
    # The plaintext token was printed once.
    assert "API TOKEN" in out

    # Second invocation must refuse (count_users > 0).
    rc2 = bootstrap_rbac.main(["--email", "second@example.com", "--role", "ADMIN"])
    assert rc2 == 2
    err = capsys.readouterr().err
    assert "refusing to bootstrap" in err


# ---------------------------------------------------------------------------
# 6. End-to-end via the FastAPI app: opt-in flag flips behaviour
# ---------------------------------------------------------------------------


def test_app_main_imports_rbac_module():
    """Smoke test: backend.main wires backend.rbac_auth and exposes the 4
    RBAC routes regardless of whether the flag is on."""
    from backend import main as main_mod
    assert hasattr(main_mod, "rbac_mod")
    paths = {(tuple(sorted(getattr(r, "methods", []) or [])), r.path)
             for r in main_mod.app.routes if hasattr(r, "path")}
    assert (("POST",), "/api/rbac/users") in paths
    assert (("GET",), "/api/rbac/users") in paths
    assert (("DELETE",), "/api/rbac/users/{user_id}") in paths
    assert (("GET",), "/api/rbac/me") in paths


@pytest.mark.skipif(not _TESTCLIENT_OK,
                    reason="installed Starlette TestClient is incompatible with httpx version")
def test_app_rbac_disabled_me_returns_503(monkeypatch):
    """With RBAC off, /api/rbac/me returns 503 (the gate fires before auth)."""
    monkeypatch.delenv("LHS_RBAC_ENABLED", raising=False)
    monkeypatch.delenv("LHS_AUTH_TOKEN", raising=False)
    import importlib
    from backend import main as main_mod
    importlib.reload(main_mod)

    client = _TestClient(main_mod.app)
    r = client.get("/api/rbac/me")
    assert r.status_code == 503


@pytest.mark.skipif(not _TESTCLIENT_OK,
                    reason="installed Starlette TestClient is incompatible with httpx version")
def test_app_rbac_enabled_me_returns_caller_identity(monkeypatch, tmp_path):
    """With RBAC on + a bootstrapped user, /api/rbac/me returns that user."""
    db_path = tmp_path / "e2e.sqlite"
    rbac_mod.init_rbac_db(db_path)
    user, plaintext = _run(rbac_mod.create_user("e2e@example.com", "OWNER"))

    monkeypatch.setenv("LHS_RBAC_ENABLED", "true")
    monkeypatch.delenv("LHS_AUTH_TOKEN", raising=False)
    import importlib
    from backend import main as main_mod
    importlib.reload(main_mod)

    client = _TestClient(main_mod.app)
    r = client.get("/api/rbac/me", headers={"Authorization": f"Bearer {plaintext}"})
    assert r.status_code == 200
    body = r.json()
    assert body["user"]["email"] == "e2e@example.com"
    assert body["user"]["role"] == "OWNER"
    assert body["user"]["user_id"] == user.user_id
