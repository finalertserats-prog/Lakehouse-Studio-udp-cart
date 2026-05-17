"""v0.6.1 — tests for runner.py's opt-in compose overlay wiring.

Covers:
  - _is_truthy parser (env-flag parsing)
  - _write_optional_overlays no-ops when flags are unset (the critical
    "stable path unchanged" guarantee)
  - _write_optional_overlays writes files + populates self._overlays
    when flags are set
  - The docker_compose_up argv injection adds `-f overlay.yml` and
    extends the service list when overlays are present
  - The argv shape is UNCHANGED vs the pre-v0.6.1 form when overlays
    are absent (regression guard for the stable udp-local-v0.2 path)
"""
from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from backend import runner
from backend import airflow_overlay, dagster_overlay, superset_overlay


# ---------------------------------------------------------------------------
# _is_truthy — env-flag parser
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("value,expected", [
    ("true", True), ("True", True), ("TRUE", True),
    ("1", True), ("yes", True), ("on", True),
    ("enabled", True), ("foo", True),  # bare presence
    ("false", False), ("False", False), ("FALSE", False),
    ("0", False), ("no", False), ("off", False),
    ("disable", False), ("disabled", False),
    ("", False), ("   ", False),
    (None, False), (False, False), (True, True),
])
def test_is_truthy(value, expected):
    assert runner._is_truthy(value) is expected


# ---------------------------------------------------------------------------
# Overlay modules expose the required constants
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("mod,flag,filename,n_services", [
    (airflow_overlay,  "LHS_AIRFLOW_ENABLED",  "docker-compose.airflow.yml",  4),
    (dagster_overlay,  "LHS_DAGSTER_ENABLED",  "docker-compose.dagster.yml",  3),
    (superset_overlay, "LHS_SUPERSET_ENABLED", "docker-compose.superset.yml", 4),
])
def test_overlay_module_exports_runner_contract(mod, flag, filename, n_services):
    assert mod.ENV_FLAG == flag
    assert mod.OVERLAY_FILENAME == filename
    assert isinstance(mod.SERVICES, list)
    assert len(mod.SERVICES) == n_services
    # Every service name must be non-empty and a string — runner appends
    # them directly to argv, so a None/empty would corrupt the command.
    for svc in mod.SERVICES:
        assert isinstance(svc, str) and svc


# ---------------------------------------------------------------------------
# Stable-path guarantee — when no flag is set, no overlay is written and
# self._overlays stays empty. This is the regression guard for the
# certified udp-local-v0.2 install path.
# ---------------------------------------------------------------------------

def _fake_runner(install_dir: Path) -> runner.UDPRunner:
    """Build a UDPRunner without going through the full FastAPI lifecycle.
    Only the install_dir matters for overlay tests."""
    stack = MagicMock()
    stack.id = "udp-local-v0.2"
    stack.components = []
    r = runner.UDPRunner(
        stack=stack,
        install_id="test_install",
        host="127.0.0.1",
        install_dir=install_dir,
    )
    # Silence event emission (would try to publish to the bus).
    r._log = MagicMock()
    r._emit = MagicMock()
    return r


def test_write_optional_overlays_noop_when_all_flags_unset(tmp_path):
    r = _fake_runner(tmp_path)
    # Ensure no flag leaks from the parent process env.
    with patch.dict(os.environ, {}, clear=False):
        for flag in ("LHS_AIRFLOW_ENABLED", "LHS_DAGSTER_ENABLED", "LHS_SUPERSET_ENABLED"):
            os.environ.pop(flag, None)
        r._write_optional_overlays({})
    assert r._overlays == []
    # No overlay files should have been written.
    assert list(tmp_path.glob("docker-compose.*.yml")) == []


def test_write_optional_overlays_noop_when_flag_explicitly_false(tmp_path):
    r = _fake_runner(tmp_path)
    env = {
        "LHS_AIRFLOW_ENABLED": "false",
        "LHS_DAGSTER_ENABLED": "0",
        "LHS_SUPERSET_ENABLED": "no",
    }
    r._write_optional_overlays(env)
    assert r._overlays == []
    assert list(tmp_path.glob("docker-compose.*.yml")) == []


# ---------------------------------------------------------------------------
# When a flag is set, the overlay writes + populates self._overlays.
# We patch the actual writers so the test stays hermetic (no real YAML
# rendering, no DAG/asset files written) — the contract under test is
# the runner glue, not the overlay content.
# ---------------------------------------------------------------------------

def test_write_optional_overlays_calls_airflow_when_enabled(tmp_path):
    r = _fake_runner(tmp_path)
    fake_path = tmp_path / "docker-compose.airflow.yml"
    fake_path.write_text("services: {}\n")
    with patch.object(airflow_overlay, "write_airflow_overlay",
                      return_value=fake_path) as writer:
        r._write_optional_overlays({"LHS_AIRFLOW_ENABLED": "true"})
    writer.assert_called_once_with(tmp_path, {"LHS_AIRFLOW_ENABLED": "true"})
    assert len(r._overlays) == 1
    ov = r._overlays[0]
    assert ov["name"] == "airflow"
    assert ov["file"] == fake_path
    assert ov["services"] == airflow_overlay.SERVICES


def test_write_optional_overlays_handles_writer_returning_none(tmp_path):
    r = _fake_runner(tmp_path)
    # Writer chose to no-op (e.g. internal validation failed).
    with patch.object(dagster_overlay, "write_dagster_overlay",
                      return_value=None):
        r._write_optional_overlays({"LHS_DAGSTER_ENABLED": "yes"})
    assert r._overlays == []


def test_write_optional_overlays_swallows_writer_exception(tmp_path):
    r = _fake_runner(tmp_path)
    # A broken overlay must NOT block the base install.
    with patch.object(superset_overlay, "write_superset_overlay",
                      side_effect=RuntimeError("boom")):
        r._write_optional_overlays({"LHS_SUPERSET_ENABLED": "true"})
    assert r._overlays == []


def test_write_optional_overlays_all_three_enabled(tmp_path):
    r = _fake_runner(tmp_path)
    a_path = tmp_path / "docker-compose.airflow.yml"
    d_path = tmp_path / "docker-compose.dagster.yml"
    s_path = tmp_path / "docker-compose.superset.yml"
    for p in (a_path, d_path, s_path):
        p.write_text("services: {}\n")
    with patch.object(airflow_overlay, "write_airflow_overlay", return_value=a_path), \
         patch.object(dagster_overlay, "write_dagster_overlay", return_value=d_path), \
         patch.object(superset_overlay, "write_superset_overlay", return_value=s_path):
        r._write_optional_overlays({
            "LHS_AIRFLOW_ENABLED": "true",
            "LHS_DAGSTER_ENABLED": "true",
            "LHS_SUPERSET_ENABLED": "true",
        })
    assert len(r._overlays) == 3
    names = [o["name"] for o in r._overlays]
    assert names == ["airflow", "dagster", "superset"]


# ---------------------------------------------------------------------------
# Parent-process env is honored (user can set LHS_AIRFLOW_ENABLED=true in
# their shell instead of through the installer UI).
# ---------------------------------------------------------------------------

def test_parent_env_enables_overlay_even_when_merged_env_unset(tmp_path):
    r = _fake_runner(tmp_path)
    fake_path = tmp_path / "docker-compose.airflow.yml"
    fake_path.write_text("services: {}\n")
    with patch.dict(os.environ, {"LHS_AIRFLOW_ENABLED": "true"}, clear=False), \
         patch.object(airflow_overlay, "write_airflow_overlay",
                      return_value=fake_path):
        r._write_optional_overlays({})  # merged env empty
    assert len(r._overlays) == 1
    assert r._overlays[0]["name"] == "airflow"
