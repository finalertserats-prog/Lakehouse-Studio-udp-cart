"""Unit tests for the per-service docker log viewer (backend/service_logs.py).

Hermetic — never spawns docker. The `get_service_logs` tests mock
`asyncio.create_subprocess_exec` so we can inspect the argv that would
have been passed to docker compose.
"""
from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from backend.service_logs import (
    MAX_TAIL,
    _build_logs_argv,
    _validate_service_name,
    get_service_logs,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

_KNOWN_SERVICES = {"minio", "iceberg-rest", "spark", "starrocks-fe", "starrocks-be"}


def _fake_proc(stdout: bytes = b"", stderr: bytes = b"", returncode: int = 0):
    """Build a MagicMock that quacks like an asyncio subprocess."""
    proc = MagicMock()
    proc.returncode = returncode
    proc.communicate = AsyncMock(return_value=(stdout, stderr))
    proc.wait = AsyncMock(return_value=returncode)
    proc.kill = MagicMock()
    return proc


# ---------------------------------------------------------------------------
# 1. _validate_service_name accepts known names
# ---------------------------------------------------------------------------

def test_validate_service_name_accepts_known_names():
    # No exception raised for any name in the manifest set.
    for name in _KNOWN_SERVICES:
        _validate_service_name(name, _KNOWN_SERVICES)


def test_validate_service_name_accepts_alnum_and_dash_underscore():
    # Charset check passes for compose-legal characters when also in manifest.
    services = {"abc_123", "fancy-svc"}
    _validate_service_name("abc_123", services)
    _validate_service_name("fancy-svc", services)


# ---------------------------------------------------------------------------
# 2. _validate_service_name rejects unsafe + unknown names
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("bad_name", [
    "../etc/passwd",          # path traversal
    "../../root",             # path traversal
    "spark;rm -rf /",         # semicolon -> shell-meta
    "spark | cat",            # pipe
    "spark`whoami`",          # backticks
    "spark$(id)",             # command substitution
    "spark service",          # space
    "spark\nminio",           # newline
    "spark'inj",              # quote
    "spark\"inj",             # dquote
    "--malicious",            # leading dashes still rejected? actually -- is allowed by charset, but membership check catches it
])
def test_validate_service_name_rejects_unsafe_characters(bad_name):
    # All of these should fail SOMETHING — either charset check OR membership check.
    with pytest.raises(ValueError):
        _validate_service_name(bad_name, _KNOWN_SERVICES)


def test_validate_service_name_rejects_name_not_in_manifest():
    # Charset is fine, but the install's manifest doesn't declare this service.
    with pytest.raises(ValueError, match="not declared in this install's manifest"):
        _validate_service_name("postgres", _KNOWN_SERVICES)


def test_validate_service_name_rejects_empty_string():
    with pytest.raises(ValueError, match="non-empty"):
        _validate_service_name("", _KNOWN_SERVICES)


def test_validate_service_name_rejects_non_string():
    with pytest.raises(ValueError, match="non-empty"):
        _validate_service_name(None, _KNOWN_SERVICES)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# 3. get_service_logs builds the right docker compose logs argv
# ---------------------------------------------------------------------------

def test_get_service_logs_builds_expected_argv(tmp_path):
    """Mock subprocess and assert the argv we pass to docker compose."""
    install_dir = tmp_path / "install"
    install_dir.mkdir()

    fake = _fake_proc(stdout=b"line one\nline two\n", returncode=0)

    captured = {}

    async def _capture(*argv, cwd=None, stdout=None, stderr=None):
        captured["argv"] = list(argv)
        captured["cwd"] = cwd
        return fake

    with patch("backend.service_logs.shutil.which", return_value="/usr/bin/docker"), \
         patch("backend.service_logs.asyncio.create_subprocess_exec", side_effect=_capture):
        result = asyncio.run(get_service_logs(install_dir, "minio", tail=50, since="10m"))

    assert captured["argv"][:3] == ["docker", "compose", "logs"]
    assert "minio" in captured["argv"]
    # --tail 50 and --since 10m must both be passed through
    assert "--tail" in captured["argv"]
    tail_idx = captured["argv"].index("--tail")
    assert captured["argv"][tail_idx + 1] == "50"
    assert "--since" in captured["argv"]
    since_idx = captured["argv"].index("--since")
    assert captured["argv"][since_idx + 1] == "10m"
    # Stream-mode follow flag MUST NOT appear on a snapshot call
    assert "-f" not in captured["argv"]
    # --no-color so the UI doesn't have to strip escape sequences
    assert "--no-color" in captured["argv"]
    # cwd is the install dir
    assert captured["cwd"] == str(install_dir)

    # Returned shape
    assert result["service"] == "minio"
    assert result["lines"] == ["line one", "line two"]
    assert result["truncated"] is False
    assert "fetched_at" in result and isinstance(result["fetched_at"], float)


def test_get_service_logs_no_since_omits_flag(tmp_path):
    install_dir = tmp_path
    fake = _fake_proc(stdout=b"", returncode=0)
    captured = {}

    async def _capture(*argv, **_):
        captured["argv"] = list(argv)
        return fake

    with patch("backend.service_logs.shutil.which", return_value="/usr/bin/docker"), \
         patch("backend.service_logs.asyncio.create_subprocess_exec", side_effect=_capture):
        asyncio.run(get_service_logs(install_dir, "spark", tail=100))

    assert "--since" not in captured["argv"]


# ---------------------------------------------------------------------------
# 4. Tail capped at MAX_TAIL even when caller asks for more
# ---------------------------------------------------------------------------

def test_get_service_logs_tail_capped_at_max(tmp_path):
    """Caller asks for 5000 lines -> we ask docker for MAX_TAIL and flag truncated."""
    install_dir = tmp_path
    fake = _fake_proc(stdout=b"x\n" * 10, returncode=0)
    captured = {}

    async def _capture(*argv, **_):
        captured["argv"] = list(argv)
        return fake

    with patch("backend.service_logs.shutil.which", return_value="/usr/bin/docker"), \
         patch("backend.service_logs.asyncio.create_subprocess_exec", side_effect=_capture):
        result = asyncio.run(get_service_logs(install_dir, "minio", tail=5000))

    # Argv must show the capped tail, not 5000
    tail_idx = captured["argv"].index("--tail")
    assert captured["argv"][tail_idx + 1] == str(MAX_TAIL)
    # And the response must flag the truncation so the UI can warn the user
    assert result["truncated"] is True


def test_get_service_logs_output_capped_at_max(tmp_path):
    """Even if docker returns more than MAX_TAIL lines, we trim and flag."""
    install_dir = tmp_path
    # Build a payload of MAX_TAIL + 50 lines
    payload = "\n".join(f"line {i}" for i in range(MAX_TAIL + 50)).encode("utf-8") + b"\n"
    fake = _fake_proc(stdout=payload, returncode=0)

    async def _capture(*argv, **_):
        return fake

    with patch("backend.service_logs.shutil.which", return_value="/usr/bin/docker"), \
         patch("backend.service_logs.asyncio.create_subprocess_exec", side_effect=_capture):
        result = asyncio.run(get_service_logs(install_dir, "minio", tail=200))

    assert len(result["lines"]) == MAX_TAIL
    assert result["truncated"] is True
    # We kept the TAIL, not the head
    assert result["lines"][-1] == f"line {MAX_TAIL + 50 - 1}"


# ---------------------------------------------------------------------------
# 5. Failure paths return {"error": "..."} rather than raising
# ---------------------------------------------------------------------------

def test_get_service_logs_returns_error_when_docker_missing(tmp_path):
    with patch("backend.service_logs.shutil.which", return_value=None):
        result = asyncio.run(get_service_logs(tmp_path, "minio"))
    assert "error" in result
    assert "docker" in result["error"].lower()


def test_get_service_logs_rejects_bad_since(tmp_path):
    """Free-form since strings must be rejected before docker is ever spawned."""
    # Pre-empt the shutil.which check so we know rejection isn't due to docker absence.
    with patch("backend.service_logs.shutil.which", return_value="/usr/bin/docker"):
        with pytest.raises(ValueError, match="Go duration"):
            asyncio.run(get_service_logs(tmp_path, "minio", since="; rm -rf /"))


# ---------------------------------------------------------------------------
# 6. _build_logs_argv helper — stream variant includes -f
# ---------------------------------------------------------------------------

def test_build_logs_argv_stream_includes_follow():
    argv = _build_logs_argv("minio", tail=100, since=None, follow=True)
    assert "-f" in argv
    assert "--tail" in argv
    assert argv[argv.index("--tail") + 1] == "100"
    assert "--no-color" in argv
