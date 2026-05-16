"""Tests for the `/api/v1/` alias middleware.

The middleware in `backend/main.py` rewrites incoming `/api/v1/<rest>` paths
to `/api/<rest>` for both HTTP and WebSocket scopes BEFORE Starlette's
router matches. These tests assert the alias is transparent — same body,
same status, same WebSocket behaviour — without ever registering a
duplicate route.

Hermetic: no live HTTP, no Docker. Everything runs through `TestClient`
which speaks ASGI directly to the in-process app, so the middleware is
exercised end-to-end.
"""
from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

from backend.events import bus
from backend.main import app
from backend.models import LogEvent


@pytest.fixture(scope="module")
def client() -> TestClient:
    return TestClient(app)


# ---------------------------------------------------------------------------
# HTTP alias parity
# ---------------------------------------------------------------------------


def test_auth_status_alias_returns_identical_body(client: TestClient) -> None:
    """GET /api/auth/status and GET /api/v1/auth/status must be byte-identical."""
    unversioned = client.get("/api/auth/status")
    versioned = client.get("/api/v1/auth/status")

    assert unversioned.status_code == 200
    assert versioned.status_code == 200
    assert unversioned.json() == versioned.json()
    # Sanity: the documented payload shape is preserved.
    assert "auth_required" in unversioned.json()


def test_cart_validate_v1_echoes_validation_response(client: TestClient) -> None:
    """POST /api/v1/cart/validate must return the same validation envelope
    as the un-versioned route — proves the alias works for POST + JSON
    body, not just GET."""
    payload = {"cart": []}

    versioned = client.post("/api/v1/cart/validate", json=payload)
    unversioned = client.post("/api/cart/validate", json=payload)

    assert versioned.status_code == 200
    assert unversioned.status_code == 200

    body = versioned.json()
    # Documented response keys from backend.cart.validate_cart — assert the
    # shape so a future refactor that drops a key fails this test, not just
    # production.
    for key in ("valid", "complete", "score", "components_in_cart"):
        assert key in body, f"missing key {key!r} in {body!r}"
    assert versioned.json() == unversioned.json()


def test_v1_nonexistent_returns_404_not_redirect(client: TestClient) -> None:
    """An unknown path under /api/v1/ must surface FastAPI's standard 404
    JSON envelope — never a 3xx redirect to /api/. The rewrite happens
    inside the ASGI scope, not as an HTTP redirect, so the client only
    ever sees the final 404 from the router."""
    resp = client.get("/api/v1/nonexistent", follow_redirects=False)

    assert resp.status_code == 404, (
        f"expected 404 for unknown v1 path, got {resp.status_code} "
        f"with body {resp.text!r}"
    )
    # FastAPI's default 404 envelope.
    assert resp.json() == {"detail": "Not Found"}


def test_v1_alias_registers_zero_extra_routes() -> None:
    """The middleware must not register any /api/v1/* route. If a future
    change accidentally bolts on a v1 router, this guard fires."""
    v1_routes = [
        r for r in app.routes if getattr(r, "path", "").startswith("/api/v1/")
    ]
    assert v1_routes == [], (
        f"middleware must alias /api/v1/, not register routes; found: "
        f"{[r.path for r in v1_routes]}"
    )


def test_openapi_doc_still_reflects_canonical_paths(client: TestClient) -> None:
    """OpenAPI doc should still describe the un-versioned canonical paths.
    v1 is an alias, not a separately documented surface — the spec stays
    small and the contract stays single-sourced."""
    resp = client.get("/openapi.json")
    assert resp.status_code == 200
    spec = resp.json()
    paths = spec.get("paths", {})
    # At least one /api/ path must still be documented.
    assert any(p.startswith("/api/") for p in paths), "expected /api/ paths in spec"
    # No /api/v1/ path should be in the spec — the alias is invisible to
    # OpenAPI by design.
    assert not any(p.startswith("/api/v1/") for p in paths), (
        f"unexpected /api/v1/ paths in OpenAPI spec: "
        f"{[p for p in paths if p.startswith('/api/v1/')]}"
    )


# ---------------------------------------------------------------------------
# WebSocket alias
# ---------------------------------------------------------------------------


def test_websocket_v1_alias_receives_initial_frame(client: TestClient) -> None:
    """A client connecting to /api/v1/installs/{id}/logs must reach the
    same handler as /api/installs/{id}/logs. We seed one historical event
    into the bus first, then connect via the v1 path and assert the
    handler replays that frame — proves the WebSocket-scope rewrite path
    in the middleware is wired up.

    Uses a synthetic install_id so we don't depend on `store.get(...)`
    returning anything (the WS handler doesn't gate on the store; only
    HTTP routes do)."""
    install_id = "test-v1-alias-ws"

    # Seed a historical event so the WS handler has something to flush.
    seed = LogEvent(
        install_id=install_id,
        ts=time.time(),
        kind="log",
        stream="stdout",
        line="hello from /api/v1 alias",
    )
    bus.publish_nowait(seed)

    try:
        with client.websocket_connect(f"/api/v1/installs/{install_id}/logs") as ws:
            payload = ws.receive_json()
    finally:
        # Tear down history so we don't leak across the test module run.
        bus._history.pop(install_id, None)  # noqa: SLF001 — test-only cleanup
        bus._next_seq.pop(install_id, None)  # noqa: SLF001

    assert payload["install_id"] == install_id
    assert payload["line"] == "hello from /api/v1 alias"
    assert payload["kind"] == "log"
