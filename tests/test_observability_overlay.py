"""v0.6.2 — tests for the Prometheus + Grafana + Loki observability overlay.

Mirrors `tests/test_runner_overlays.py` shape. Covers:

  - Module-level constants the runner.py glue depends on
    (ENV_FLAG, OVERLAY_FILENAME, SERVICES)
  - Writer returns None when the flag is unset (default behavior — no
    overlay file appears in install_dir)
  - Writer returns the overlay Path when the flag is set, with the file
    actually written to disk
  - Generated YAML mentions every service in SERVICES (sanity check that
    nobody renamed a service without bumping SERVICES too)
  - validate_overlay returns a list (empty when no overlay; non-empty
    when overlay file exists but config dir is missing)
  - Idempotency — writing twice doesn't blow up and produces identical files
  - Render functions produce valid-shaped YAML (basic structural checks)

All tests are hermetic: tmp_path fixture, no real docker, no network.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from backend import observability_overlay


# ---------------------------------------------------------------------------
# Module-level contract — the constants runner.py imports.
# ---------------------------------------------------------------------------


def test_env_flag_constant():
    """runner._write_optional_overlays reads ENV_FLAG to know what env var
    to check before invoking the writer. Must match the docs exactly."""
    assert observability_overlay.ENV_FLAG == "LHS_OBSERVABILITY_ENABLED"


def test_overlay_filename_constant():
    """runner appends this via `-f <filename>` to the compose argv."""
    assert observability_overlay.OVERLAY_FILENAME == "docker-compose.observability.yml"


def test_services_list_constant():
    """runner appends these to the `docker compose up -d <services>`
    argv. Order matches the founding architecture doc § 5.6.1."""
    assert observability_overlay.SERVICES == ["prometheus", "grafana", "loki"]


def test_services_are_all_non_empty_strings():
    """Empty / None entries would corrupt the runner's compose argv."""
    for svc in observability_overlay.SERVICES:
        assert isinstance(svc, str)
        assert svc, "service name must be non-empty"


# ---------------------------------------------------------------------------
# Writer behavior — disabled flag.
# ---------------------------------------------------------------------------


def test_writer_returns_none_when_flag_unset(tmp_path):
    """Default behavior: no flag → no write → no overlay file appears in
    install_dir. This is the stable-path guarantee that mirrors the
    airflow/dagster/superset overlays."""
    result = observability_overlay.write_observability_overlay(tmp_path, {})
    assert result is None
    assert not (tmp_path / observability_overlay.OVERLAY_FILENAME).exists()
    assert not (tmp_path / "observability").exists()


@pytest.mark.parametrize("falsy_value", ["", "false", "False", "0", "no", "off", "disabled"])
def test_writer_returns_none_when_flag_explicitly_false(tmp_path, falsy_value):
    """Explicit falsy values still no-op. Mirrors the runner._is_truthy
    contract — anything other than {1, true, yes, on} stays off."""
    result = observability_overlay.write_observability_overlay(
        tmp_path, {observability_overlay.ENV_FLAG: falsy_value}
    )
    assert result is None


# ---------------------------------------------------------------------------
# Writer behavior — enabled flag.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("truthy_value", ["1", "true", "True", "TRUE", "yes", "on"])
def test_writer_returns_path_when_flag_set(tmp_path, truthy_value):
    """Truthy flag → writer returns the overlay Path and the file exists."""
    result = observability_overlay.write_observability_overlay(
        tmp_path, {observability_overlay.ENV_FLAG: truthy_value}
    )
    assert result is not None
    assert isinstance(result, Path)
    assert result == tmp_path / observability_overlay.OVERLAY_FILENAME
    assert result.exists()


def test_writer_creates_observability_config_directory(tmp_path):
    """All three config files (prometheus.yml, grafana-datasources.yml,
    loki-config.yaml) must be written to the observability/ subdir so
    the services have something to mount."""
    observability_overlay.write_observability_overlay(
        tmp_path, {observability_overlay.ENV_FLAG: "true"}
    )
    cfg_dir = tmp_path / "observability"
    assert cfg_dir.is_dir()
    assert (cfg_dir / "prometheus.yml").exists()
    assert (cfg_dir / "grafana-datasources.yml").exists()
    assert (cfg_dir / "loki-config.yaml").exists()


def test_writer_is_idempotent(tmp_path):
    """Re-running with the same install_dir overwrites every file. Same
    contract as the sibling overlays — runner.py calls this on every
    install, so non-idempotent writes would accumulate cruft."""
    env = {observability_overlay.ENV_FLAG: "true"}
    p1 = observability_overlay.write_observability_overlay(tmp_path, env)
    body1 = p1.read_text(encoding="utf-8")
    p2 = observability_overlay.write_observability_overlay(tmp_path, env)
    body2 = p2.read_text(encoding="utf-8")
    assert body1 == body2


# ---------------------------------------------------------------------------
# Rendered compose YAML — sanity checks.
# ---------------------------------------------------------------------------


def test_rendered_compose_mentions_every_service(tmp_path):
    """SERVICES is the contract runner.py uses; the rendered compose
    file MUST define each one or the `compose up -d <svc>` will fail.
    Regression guard: nobody can rename a service without bumping
    SERVICES too."""
    observability_overlay.write_observability_overlay(
        tmp_path, {observability_overlay.ENV_FLAG: "true"}
    )
    body = (tmp_path / observability_overlay.OVERLAY_FILENAME).read_text(encoding="utf-8")
    for svc in observability_overlay.SERVICES:
        # Service defined under `services:` block. Two-space indent matches
        # the renderer's output shape.
        assert f"  {svc}:" in body, f"service {svc!r} missing from rendered overlay"


def test_rendered_compose_pins_image_tags(tmp_path):
    """No `:latest` — every image must carry an explicit version tag, in
    line with the certified-stack philosophy that nothing floats."""
    observability_overlay.write_observability_overlay(
        tmp_path, {observability_overlay.ENV_FLAG: "true"}
    )
    body = (tmp_path / observability_overlay.OVERLAY_FILENAME).read_text(encoding="utf-8")
    assert "prom/prometheus:v2.55" in body
    assert "grafana/grafana:11." in body
    assert "grafana/loki:3." in body
    assert ":latest" not in body


def test_rendered_compose_uses_external_network(tmp_path):
    """The overlay must attach to the base stack's existing network so
    Prometheus can scrape trino/starrocks/minio by service name. Creating
    a second network would isolate the observability containers."""
    observability_overlay.write_observability_overlay(
        tmp_path, {observability_overlay.ENV_FLAG: "true"}
    )
    body = (tmp_path / observability_overlay.OVERLAY_FILENAME).read_text(encoding="utf-8")
    assert "external: true" in body


def test_grafana_datasources_point_at_prometheus_and_loki(tmp_path):
    """Grafana is pre-provisioned with both datasources so first-time
    operators see working data sources on login — the whole point of the
    'pre-configure' acceptance criterion in the task spec."""
    observability_overlay.write_observability_overlay(
        tmp_path, {observability_overlay.ENV_FLAG: "true"}
    )
    ds = (tmp_path / "observability" / "grafana-datasources.yml").read_text(encoding="utf-8")
    assert "http://prometheus:9090" in ds
    assert "http://loki:3100" in ds


def test_prometheus_scrapes_lakehouse_components(tmp_path):
    """Acceptance criterion from the task spec: pre-configure Prometheus
    to scrape MinIO, Trino, StarRocks endpoints. Targets that don't exist
    in the cart will show as 'down' — that's expected."""
    observability_overlay.write_observability_overlay(
        tmp_path, {observability_overlay.ENV_FLAG: "true"}
    )
    prom = (tmp_path / "observability" / "prometheus.yml").read_text(encoding="utf-8")
    assert "minio" in prom
    assert "trino" in prom
    assert "starrocks" in prom


# ---------------------------------------------------------------------------
# validate_overlay — shape contract.
# ---------------------------------------------------------------------------


def test_validate_overlay_returns_empty_list_when_not_enabled(tmp_path):
    """No overlay file → validation has nothing to check → empty list."""
    problems = observability_overlay.validate_overlay(tmp_path)
    assert problems == []
    assert isinstance(problems, list)


def test_validate_overlay_returns_list_shape_when_overlay_missing_config(tmp_path):
    """Overlay file exists but observability/ folder missing → must report
    problems. Shape contract: list of strings, non-empty."""
    # Simulate the partial-write state: overlay file exists, config dir doesn't.
    (tmp_path / observability_overlay.OVERLAY_FILENAME).write_text("services: {}\n")
    problems = observability_overlay.validate_overlay(tmp_path)
    assert isinstance(problems, list)
    assert len(problems) >= 1
    for p in problems:
        assert isinstance(p, str)
        assert p, "every problem string must be non-empty"


def test_validate_overlay_after_clean_write_returns_empty_list(tmp_path):
    """Round-trip: write the overlay then validate — should be clean."""
    observability_overlay.write_observability_overlay(
        tmp_path, {observability_overlay.ENV_FLAG: "true"}
    )
    problems = observability_overlay.validate_overlay(tmp_path)
    assert problems == []
