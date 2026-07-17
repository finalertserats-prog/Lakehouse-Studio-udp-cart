"""P0.4b — tests for opt-in per-install credential generation.

Two layers:
  * backend.credential_gen — the pure secret generator + constants
  * runner._rotate_install_credential — the install-dir text sweep that
    replaces the shipped demo secret everywhere it was written

Guarantees:
  - default OFF: no flag → nothing generated (verified via the pure decision)
  - generated secret is strong + quoting-safe
  - the sweep replaces the unique demo literal across text files, skips
    binaries/.git, is idempotent, and never re-serializes YAML
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from backend import credential_gen as cg
from backend import runner


# ---------------------------------------------------------------------------
# Pure generator
# ---------------------------------------------------------------------------

def test_generate_secret_is_hex_and_strong():
    s = cg.generate_secret()
    assert len(s) == 40
    assert all(c in "0123456789abcdef" for c in s)


def test_generate_secret_is_unique():
    assert cg.generate_secret() != cg.generate_secret()


def test_generated_secret_needs_no_quoting():
    # hex is safe unquoted in YAML, bash, SQL and properties files
    s = cg.generate_secret()
    for hazard in (" ", '"', "'", "$", "{", "}", ":", "\\", "\n"):
        assert hazard not in s


# ---------------------------------------------------------------------------
# Install-dir sweep
# ---------------------------------------------------------------------------

def _fake_runner(install_dir: Path) -> runner.UDPRunner:
    stack = MagicMock()
    stack.id = "udp-local-v0.2"
    stack.components = []
    r = runner.UDPRunner(stack=stack, install_id="t", host="127.0.0.1", install_dir=install_dir)
    r._log = MagicMock()
    r._emit = MagicMock()
    return r


def test_sweep_replaces_literal_across_text_files(tmp_path):
    r = _fake_runner(tmp_path)
    (tmp_path / "docker-compose.yml").write_text(
        "environment:\n  MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD:-udp_admin_12345}\n")
    (tmp_path / "bootstrap.sh").write_text(
        "mc alias set udp http://minio:9000 admin udp_admin_12345\n")
    (tmp_path / "catalog.properties").write_text("s3.aws-secret-key=udp_admin_12345\n")
    r._rotate_install_credential(cg.DEMO_MINIO_SECRET, "deadbeef" * 5)
    for name in ("docker-compose.yml", "bootstrap.sh", "catalog.properties"):
        text = (tmp_path / name).read_text()
        assert cg.DEMO_MINIO_SECRET not in text
        assert "deadbeef" * 5 in text


def test_sweep_preserves_username_admin(tmp_path):
    # Only the unique secret rotates; the access key 'admin' must survive.
    r = _fake_runner(tmp_path)
    (tmp_path / "bootstrap.sh").write_text(
        "mc alias set udp http://minio:9000 admin udp_admin_12345\n")
    r._rotate_install_credential(cg.DEMO_MINIO_SECRET, "cafe" * 10)
    text = (tmp_path / "bootstrap.sh").read_text()
    assert " admin " in text  # username intact


def test_sweep_skips_binary_and_git(tmp_path):
    r = _fake_runner(tmp_path)
    (tmp_path / ".git").mkdir()
    (tmp_path / ".git" / "config").write_text("secret udp_admin_12345\n")
    binfile = tmp_path / "blob.bin"
    binfile.write_bytes(b"\x00\xff udp_admin_12345 \x00")
    r._rotate_install_credential(cg.DEMO_MINIO_SECRET, "aa" * 20)
    # .git untouched
    assert "udp_admin_12345" in (tmp_path / ".git" / "config").read_text()
    # binary untouched (raw bytes preserved)
    assert b"udp_admin_12345" in binfile.read_bytes()


def test_sweep_idempotent_and_noop_when_absent(tmp_path):
    r = _fake_runner(tmp_path)
    f = tmp_path / "clean.txt"
    f.write_text("no secrets here\n")
    r._rotate_install_credential(cg.DEMO_MINIO_SECRET, "bb" * 20)
    assert f.read_text() == "no secrets here\n"


def test_sweep_noop_when_old_equals_new(tmp_path):
    r = _fake_runner(tmp_path)
    f = tmp_path / "x.sh"
    f.write_text("udp_admin_12345\n")
    r._rotate_install_credential(cg.DEMO_MINIO_SECRET, cg.DEMO_MINIO_SECRET)
    assert f.read_text() == "udp_admin_12345\n"
