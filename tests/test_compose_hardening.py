"""P0.2 — tests for container runtime hardening.

Two layers:
  * backend.compose_hardening.build_harden_overlay — the pure override builder
  * runner._write_harden_overlay / _effective_service_names / reconstruct —
    the glue that enumerates effective services, writes the override file,
    and registers it LAST so it layers over base + fragment + opt-in overlays.

The critical guarantees:
  - default hardening (no-new-privileges) is ON and applies to every service
  - strict cap-drop is OFF unless LHS_HARDEN_STRICT is set
  - the whole thing is disable-able via LHS_HARDEN_RUNTIME_DISABLED
  - the override never adds a service to the `up -d` list (services == [])
  - the override never sets an `image` (it only modifies existing services)
"""
from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

from backend import compose_hardening as ch
from backend import runner


# ---------------------------------------------------------------------------
# Pure builder — build_harden_overlay
# ---------------------------------------------------------------------------

def test_default_overlay_sets_no_new_privileges_on_every_service():
    doc = ch.build_harden_overlay(["minio", "spark", "trino"])
    svcs = doc["services"]
    assert set(svcs) == {"minio", "spark", "trino"}
    for name, opts in svcs.items():
        assert opts["security_opt"] == ["no-new-privileges:true"]
        # default (non-strict) must NOT touch capabilities or limits
        assert "cap_drop" not in opts
        assert "cap_add" not in opts
        assert "pids_limit" not in opts


def test_default_overlay_never_sets_image():
    # An override that set `image` could accidentally redefine a service.
    doc = ch.build_harden_overlay(["minio"])
    assert "image" not in doc["services"]["minio"]


def test_strict_overlay_drops_all_and_regrants_minimal():
    doc = ch.build_harden_overlay(["minio"], strict=True)
    opts = doc["services"]["minio"]
    assert opts["security_opt"] == ["no-new-privileges:true"]
    assert opts["cap_drop"] == ["ALL"]
    assert "SETUID" in opts["cap_add"] and "CHOWN" in opts["cap_add"]
    # dangerous caps must never be re-granted
    for danger in ("SYS_ADMIN", "NET_ADMIN", "NET_RAW", "SYS_PTRACE", "SYS_MODULE"):
        assert danger not in opts["cap_add"]
    assert isinstance(opts["pids_limit"], int) and opts["pids_limit"] > 0


def test_builder_dedupes_and_drops_empty_names():
    doc = ch.build_harden_overlay(["a", "a", "", None, "b"])  # type: ignore[list-item]
    assert list(doc["services"].keys()) == ["a", "b"]


def test_empty_input_yields_empty_services():
    assert ch.build_harden_overlay([]) == {"services": {}}


def test_overlay_is_yaml_serializable():
    doc = ch.build_harden_overlay(["minio", "spark"], strict=True)
    text = yaml.dump(doc)
    round_tripped = yaml.safe_load(text)
    assert round_tripped == doc


# ---------------------------------------------------------------------------
# Runner glue
# ---------------------------------------------------------------------------

def _fake_runner(install_dir: Path, stack_id: str = "udp-local-v0.2") -> runner.UDPRunner:
    stack = MagicMock()
    stack.id = stack_id
    stack.components = []
    r = runner.UDPRunner(
        stack=stack,
        install_id="test_install",
        host="127.0.0.1",
        install_dir=install_dir,
    )
    r._log = MagicMock()
    r._emit = MagicMock()
    return r


def _write_base_compose(install_dir: Path, services: list[str]) -> None:
    doc = {"services": {s: {"image": f"img/{s}:latest"} for s in services}}
    (install_dir / "docker-compose.yml").write_text(yaml.dump(doc), encoding="utf-8")


@pytest.fixture(autouse=True)
def _clean_harden_env():
    saved = {}
    for k in (ch.DISABLE_ENV, ch.STRICT_ENV):
        saved[k] = os.environ.pop(k, None)
    yield
    for k, v in saved.items():
        if v is not None:
            os.environ[k] = v
        else:
            os.environ.pop(k, None)


def test_effective_service_names_reads_base_and_overlays(tmp_path):
    r = _fake_runner(tmp_path)
    _write_base_compose(tmp_path, ["minio", "iceberg-rest", "spark"])
    r._overlays = [{"name": "frag", "file": tmp_path / "f.yml", "services": ["nessie", "trino"]}]
    names = r._effective_service_names()
    assert set(names) == {"minio", "iceberg-rest", "spark", "nessie", "trino"}


def test_write_harden_overlay_default_on(tmp_path):
    r = _fake_runner(tmp_path)
    _write_base_compose(tmp_path, ["minio", "spark"])
    r._write_harden_overlay({})
    path = tmp_path / ch.OVERLAY_FILENAME
    assert path.exists()
    doc = yaml.safe_load(path.read_text())
    assert doc["services"]["minio"]["security_opt"] == ["no-new-privileges:true"]
    assert doc["services"]["spark"]["security_opt"] == ["no-new-privileges:true"]
    # cap_drop must be absent by default
    assert "cap_drop" not in doc["services"]["minio"]
    # registered LAST, with NO services (never adds to `up -d`)
    assert r._overlays[-1]["name"] == "harden"
    assert r._overlays[-1]["services"] == []
    assert r._overlays[-1]["file"] == path


def test_write_harden_overlay_disabled_by_flag(tmp_path):
    r = _fake_runner(tmp_path)
    _write_base_compose(tmp_path, ["minio"])
    r._write_harden_overlay({ch.DISABLE_ENV: "1"})
    assert not (tmp_path / ch.OVERLAY_FILENAME).exists()
    assert r._overlays == []


def test_write_harden_overlay_strict_via_flag(tmp_path):
    r = _fake_runner(tmp_path)
    _write_base_compose(tmp_path, ["minio"])
    r._write_harden_overlay({ch.STRICT_ENV: "true"})
    doc = yaml.safe_load((tmp_path / ch.OVERLAY_FILENAME).read_text())
    assert doc["services"]["minio"]["cap_drop"] == ["ALL"]


def test_write_harden_overlay_skips_when_no_services(tmp_path):
    r = _fake_runner(tmp_path)
    # No base compose on disk, no overlays → nothing to harden.
    r._write_harden_overlay({})
    assert not (tmp_path / ch.OVERLAY_FILENAME).exists()
    assert r._overlays == []


def test_reconstruct_from_disk_reattaches_harden_last(tmp_path):
    r = _fake_runner(tmp_path)
    # Simulate a prior env step having written the overlay file.
    (tmp_path / ch.OVERLAY_FILENAME).write_text("services: {}\n", encoding="utf-8")
    r._reconstruct_overlays_from_disk()
    assert r._overlays, "expected at least the harden overlay to be recovered"
    assert r._overlays[-1]["name"] == "harden"
    assert r._overlays[-1]["services"] == []


# ---------------------------------------------------------------------------
# Coverage-gap fix — services declared only in an overlay FILE (not its
# metadata) are still enumerated and therefore hardened.
# ---------------------------------------------------------------------------

def test_effective_service_names_parses_overlay_file_contents(tmp_path):
    r = _fake_runner(tmp_path)
    _write_base_compose(tmp_path, ["minio"])
    # Overlay file defines a service its metadata does NOT list.
    frag = tmp_path / "docker-compose.fragment.yml"
    frag.write_text(yaml.dump({"services": {"nessie": {"image": "n:1"},
                                            "extra": {"image": "e:1"}}}),
                    encoding="utf-8")
    r._overlays = [{"name": "frag", "file": frag, "services": ["nessie"]}]  # 'extra' omitted
    names = r._effective_service_names()
    assert "extra" in names, "service present only in the overlay file must still be hardened"
    assert {"minio", "nessie", "extra"} <= set(names)


# ---------------------------------------------------------------------------
# Codex-flagged edge cases — dedupe + disable-on-reconstruct.
# ---------------------------------------------------------------------------

def test_write_harden_overlay_dedupes_on_double_call(tmp_path):
    r = _fake_runner(tmp_path)
    _write_base_compose(tmp_path, ["minio"])
    r._write_harden_overlay({})
    r._write_harden_overlay({})
    harden_entries = [o for o in r._overlays if o["name"] == "harden"]
    assert len(harden_entries) == 1


def test_reconstruct_dedupes_existing_harden(tmp_path):
    r = _fake_runner(tmp_path)
    (tmp_path / ch.OVERLAY_FILENAME).write_text("services: {}\n", encoding="utf-8")
    # Pre-seed a harden entry as if env step already ran.
    r._overlays = [{"name": "harden", "file": tmp_path / ch.OVERLAY_FILENAME, "services": []}]
    r._reconstruct_overlays_from_disk()
    assert len([o for o in r._overlays if o["name"] == "harden"]) == 1


def test_reconstruct_honors_disable_flag(tmp_path, monkeypatch):
    r = _fake_runner(tmp_path)
    # A stale overlay file exists, but hardening is now disabled — it must NOT
    # be re-attached (disable means disable, even across a retry).
    (tmp_path / ch.OVERLAY_FILENAME).write_text("services: {}\n", encoding="utf-8")
    monkeypatch.setenv(ch.DISABLE_ENV, "1")
    r._reconstruct_overlays_from_disk()
    assert [o for o in r._overlays if o["name"] == "harden"] == []


# ---------------------------------------------------------------------------
# Raw-argv coverage — COMPOSE_FILE points compose at the overlay for stacks
# whose start command doesn't pass explicit -f (enterprise-hadoop/streaming).
# ---------------------------------------------------------------------------

def test_base_compose_filename_detection(tmp_path):
    r = _fake_runner(tmp_path)
    assert r._base_compose_filename() is None
    (tmp_path / "docker-compose.yml").write_text("services: {}\n", encoding="utf-8")
    assert r._base_compose_filename() == "docker-compose.yml"
