"""Tests for the P0.3 AI-provisioning opt-in gate.

AI-driven provisioning lets an LLM generate configs + commands that run against
Docker, so it must be OFF by default and only reachable when an operator
explicitly opts in (LHS_AI_PROVISION_ENABLED) with an LLM key configured.
"""
from __future__ import annotations

import pytest

import backend.ai_provisioner as ap

_LLM_KEYS = ("LITELLM_API_KEY", "OPENAI_API_KEY", "ANTHROPIC_API_KEY")


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    monkeypatch.delenv(ap.PROVISION_ENABLE_ENV, raising=False)
    for k in _LLM_KEYS:
        monkeypatch.delenv(k, raising=False)
    yield


def test_disabled_by_default():
    ok, why = ap.provisioning_status()
    assert ok is False
    assert "disabled by default" in why
    assert ap.PROVISION_ENABLE_ENV in why


def test_enabled_flag_without_key_is_refused(monkeypatch):
    monkeypatch.setenv(ap.PROVISION_ENABLE_ENV, "1")
    ok, why = ap.provisioning_status()
    assert ok is False
    assert "no LLM API key" in why


@pytest.mark.parametrize("key_env", _LLM_KEYS)
def test_enabled_with_flag_and_any_llm_key(monkeypatch, key_env):
    monkeypatch.setenv(ap.PROVISION_ENABLE_ENV, "1")
    monkeypatch.setenv(key_env, "x")
    assert ap.provisioning_status() == (True, "")


@pytest.mark.parametrize("val", ["0", "false", "no", "off", "disabled", ""])
def test_falsey_flag_values_keep_it_disabled(monkeypatch, val):
    monkeypatch.setenv(ap.PROVISION_ENABLE_ENV, val)
    monkeypatch.setenv("LITELLM_API_KEY", "x")
    ok, _ = ap.provisioning_status()
    assert ok is False


# ---------------------------------------------------------------------------
# Endpoint wiring — the executing endpoints must 403 when provisioning is off.
# ---------------------------------------------------------------------------

def _client():
    from fastapi.testclient import TestClient
    from backend.main import app
    return TestClient(app, raise_server_exceptions=False)


@pytest.mark.parametrize("path, body", [
    ("/api/ai-provision/start", {"stack_id": "udp-local-v0.2", "cart_selections": {"minio": "x"}}),
    ("/api/stack-builder/build", {"selected": ["minio"]}),
    ("/api/image-build/start", {"image_id": "spark-hudi", "research": {"x": 1}}),
])
def test_executing_endpoints_are_403_when_disabled(path, body):
    try:
        client = _client()
    except Exception as exc:  # pragma: no cover - env without fastapi test client
        pytest.skip(f"TestClient unavailable: {exc}")
    resp = client.post(path, json=body)
    assert resp.status_code == 403, resp.text
    assert "disabled by default" in resp.text
