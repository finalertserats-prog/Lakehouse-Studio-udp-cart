"""CLI tests using click.testing.CliRunner + httpx.MockTransport.

No real network. No real backend. Every test wires a MockTransport into
cli.client.make_client by monkeypatching at the import site used by main.py.
"""
from __future__ import annotations

import json
from typing import Any, Callable

import httpx
import pytest
from click.testing import CliRunner

from cli import main as cli_main


def _mock_make_client(handler: Callable[[httpx.Request], httpx.Response]):
    """Return a stand-in for cli.client.make_client that always uses MockTransport."""
    transport = httpx.MockTransport(handler)

    def _factory(server, auth_token, transport_override=None):
        return httpx.Client(
            base_url=server.rstrip("/"),
            headers={"Accept": "application/json"},
            transport=transport,
        )

    return _factory


def _patch_client(monkeypatch, handler: Callable[[httpx.Request], httpx.Response]) -> None:
    """Replace make_client at the spot where main.py imported it."""
    monkeypatch.setattr(cli_main, "make_client", _mock_make_client(handler))


def _json(payload: Any, status: int = 200) -> httpx.Response:
    return httpx.Response(status, json=payload)


# ---------- 1. root --help lists every group ----------

def test_root_help_lists_command_groups():
    runner = CliRunner()
    result = runner.invoke(cli_main.cli, ["--help"])
    assert result.exit_code == 0, result.output
    out = result.output
    for group in ("catalog", "templates", "stacks", "install",
                  "health", "backup", "tables", "ai", "export"):
        assert group in out, f"missing group {group} in --help output"


# ---------- 2. catalog list renders rows ----------

def test_catalog_list_table(monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/catalog"
        return _json({
            "categories": [
                {
                    "id": "storage",
                    "components": [
                        {"id": "minio", "name": "MinIO", "version": "RELEASE.2024",
                         "readiness": "ga"},
                    ],
                },
                {
                    "id": "query",
                    "components": [
                        {"id": "trino", "name": "Trino", "version": "445", "readiness": "ga"},
                    ],
                },
            ],
            "goals": [],
            "recommended_sets": {},
        })
    _patch_client(monkeypatch, handler)
    runner = CliRunner()
    result = runner.invoke(cli_main.cli, ["catalog", "list"])
    assert result.exit_code == 0, result.output
    assert "minio" in result.output
    assert "trino" in result.output


# ---------- 3. JSON output mode passes data through unchanged ----------

def test_stacks_list_json_mode(monkeypatch):
    payload = [
        {"id": "udp-local-v0.2", "name": "UDP", "version": "0.2.0",
         "maturity": "ga", "components": [{"id": "minio"}, {"id": "trino"}]},
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/stacks"
        return _json(payload)
    _patch_client(monkeypatch, handler)
    runner = CliRunner()
    result = runner.invoke(cli_main.cli, ["--output", "json", "stacks", "list"])
    assert result.exit_code == 0, result.output
    parsed = json.loads(result.output)
    assert parsed[0]["id"] == "udp-local-v0.2"


# ---------- 4. install create POSTs the expected body ----------

def test_install_create_posts_body(monkeypatch):
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == "/api/installs"
        captured["body"] = json.loads(request.content)
        return _json({
            "install_id": "ins_abc123", "stack_id": "udp-local-v0.2",
            "state": "INSPECTING", "host": "localhost",
            "install_dir": "/tmp/udp", "lake_name": "calm-river",
        })
    _patch_client(monkeypatch, handler)
    runner = CliRunner()
    result = runner.invoke(cli_main.cli, [
        "install", "create", "udp-local-v0.2",
        "--host", "localhost",
        "--install-dir", "/tmp/udp",
        "--lake", "calm-river",
    ])
    assert result.exit_code == 0, result.output
    assert captured["body"] == {
        "stack_id": "udp-local-v0.2", "host": "localhost",
        "install_dir": "/tmp/udp", "lake_name": "calm-river",
    }
    assert "ins_abc123" in result.output


# ---------- 5. backend 404 surfaces as exit 1 with a clean message ----------

def test_404_becomes_click_error(monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        return _json({"detail": "stack not found"}, status=404)
    _patch_client(monkeypatch, handler)
    runner = CliRunner()
    result = runner.invoke(cli_main.cli, ["stacks", "compat", "does-not-exist"])
    assert result.exit_code == 1
    assert "404" in result.output
    assert "stack not found" in result.output


# ---------- 6. install status renders the steps table ----------

def test_install_status_steps_table(monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/installs/ins_xyz"
        return _json({
            "install_id": "ins_xyz", "stack_id": "udp", "state": "READY",
            "host": "localhost", "install_dir": "/tmp/udp",
            "steps": [
                {"id": "clone", "name": "clone repo", "status": "success",
                 "exit_code": 0, "duration_ms": 1234},
                {"id": "doctor", "name": "doctor", "status": "success",
                 "exit_code": 0, "duration_ms": 800},
            ],
        })
    _patch_client(monkeypatch, handler)
    runner = CliRunner()
    result = runner.invoke(cli_main.cli, ["install", "status", "ins_xyz"])
    assert result.exit_code == 0, result.output
    assert "clone" in result.output
    assert "doctor" in result.output
    assert "READY" in result.output


# ---------- 7. retry posts step_id ----------

def test_retry_passes_step_id(monkeypatch):
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        captured["body"] = json.loads(request.content)
        return _json({"resumed_at": "doctor"})
    _patch_client(monkeypatch, handler)
    runner = CliRunner()
    result = runner.invoke(cli_main.cli, ["install", "retry", "ins_x", "doctor"])
    assert result.exit_code == 0, result.output
    assert captured["path"] == "/api/installs/ins_x/steps/retry"
    assert captured["body"] == {"step_id": "doctor"}


# ---------- 8. export streams bytes to disk ----------

def test_export_writes_file(monkeypatch, tmp_path):
    blob = b"GZIP-FAKE-BYTES" * 32

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=blob,
                              headers={"content-type": "application/gzip"})
    _patch_client(monkeypatch, handler)
    out = tmp_path / "bundle.tar.gz"
    runner = CliRunner()
    result = runner.invoke(cli_main.cli, [
        "export", "ins_e", "-o", str(out),
    ])
    assert result.exit_code == 0, result.output
    assert out.exists()
    assert out.read_bytes() == blob
