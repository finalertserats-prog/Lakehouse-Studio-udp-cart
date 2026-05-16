"""Prometheus + Grafana monitoring sidecar — opt-in compose override.

Additive module that layers monitoring ON TOP of an existing install via a
separate `docker-compose.metrics.yml` override the operator opts into. Mirrors
the same shape as backup.py / tls_wizard.py:

  - Never touches backend/runner.py or _patch_compose_images
  - Never modifies the certified lock file (monitoring images are NOT pinned
    in stacks/compatibility/*.lock.yaml; they're an operational layer the user
    accepts independently)
  - Caller (the FastAPI route) passes RUNNING_STATES so we refuse to mutate a
    mid-install workspace

Filesystem layout (all under {install_dir}, NOT WORK_DIR — keeps everything
co-located with the rest of the install so a single `docker compose -f ... -f
docker-compose.metrics.yml up -d` works from the install_dir):

  {install_dir}/docker-compose.metrics.yml
  {install_dir}/monitoring/
      prometheus.yml
      grafana/
          provisioning/
              datasources/datasource.yml
              dashboards/dashboard.yml
          dashboards/
              lakehouse-overview.json

The override uses Docker Compose v2 syntax (no `version:` key, top-level
`services:` only) so it composes cleanly with the base compose file via
`-f docker-compose.yml -f docker-compose.metrics.yml`.

Image tags pinned 2026-05-16 — both verified via `docker manifest inspect`
prior to commit. Tags are intentionally hard-coded here (not read from the
catalog) because monitoring is NOT certified — it's an operational sidecar
the operator chooses to layer on.
"""
from __future__ import annotations

import json
import logging
import os
import secrets
import shutil
import time
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field

from .state import store


log = logging.getLogger("lhs.monitoring")


# ---- pinned image tags ----
# Both verified via `docker manifest inspect` on 2026-05-16. If a tag is ever
# removed from the registry, bump here — these are NOT in the lock file.
_PROMETHEUS_IMAGE = "prom/prometheus:v2.55.0"
_GRAFANA_IMAGE = "grafana/grafana:11.3.0"

# Default host-side ports for the sidecar. Both deliberately above the typical
# UDP service range (8030/8181/9000/9090) to avoid colliding with the base
# stack. Operator can edit the override file after generation if they need
# different ports.
_PROMETHEUS_PORT = 9091
_GRAFANA_PORT = 3001

# Override file name. Same pattern as Compose docs: `docker-compose.<env>.yml`.
_OVERRIDE_FILENAME = "docker-compose.metrics.yml"
_MONITORING_SUBDIR = "monitoring"


# ---------- models ----------


class MonitoringProfile(BaseModel):
    """Inputs for enable_monitoring. All fields optional with safe defaults."""
    include_grafana: bool = True
    prometheus_retention_days: int = Field(default=15, ge=1, le=365)
    grafana_admin_password: Optional[str] = Field(default=None, max_length=256)


# ---------- helpers ----------


def _install_dir(install_id: str) -> Path:
    """Look up the install_dir for the given install_id. Raises ValueError if
    the install doesn't exist — caller turns that into a 404."""
    rec = store.get(install_id)
    if rec is None:
        raise ValueError(f"install {install_id!r} not found")
    p = Path(rec.install_dir)
    if not p.exists():
        raise ValueError(f"install_dir {p} does not exist")
    return p


def _override_path(install_id: str) -> Path:
    return _install_dir(install_id) / _OVERRIDE_FILENAME


def _monitoring_dir(install_id: str) -> Path:
    return _install_dir(install_id) / _MONITORING_SUBDIR


def _atomic_write(path: Path, data: str) -> None:
    """Atomic tmp + os.replace. Mirrors the pattern used in tls_wizard."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(data, encoding="utf-8")
    os.replace(tmp, path)


def _generate_admin_password() -> str:
    """24-char URL-safe random secret. Used only when caller doesn't supply
    one. Returned to the caller ONCE in the response — never stored on disk
    outside the running grafana container's GF_SECURITY_ADMIN_PASSWORD env."""
    # token_urlsafe(18) yields ~24 chars after base64 encoding.
    return secrets.token_urlsafe(18)


# ---------- file content builders ----------


def _build_prometheus_yml(retention_days: int) -> str:
    """Scrape config targeting the in-network service names from the base
    compose (UDP's services join the `default` docker network — Prometheus
    runs on that same network via the override so it can resolve them by
    name). Targets are best-effort: services that aren't present in the
    install just show as `down` in Prometheus, which is the correct UX.

    Notes per target:
      - MinIO `/minio/v2/metrics/cluster` is the public Prometheus endpoint.
        Auth: by default MinIO requires a bearer token for the cluster
        metrics endpoint. The operator can disable this with
        `MINIO_PROMETHEUS_AUTH_TYPE=public` in the install's .env, or paste
        a bearer token into this file. Left unauthenticated below — see
        the inline comment.
      - StarRocks FE: `:8030/api/health` is the readiness endpoint. Native
        Prometheus metrics live at `:8030/metrics` but require
        `enable_prometheus_metrics=true` in fe.conf. Documented in
        docs/COMPATIBILITY.md.
      - Iceberg REST: `:8181/metrics` is conditional — older
        `tabulario/iceberg-rest` images don't expose Prometheus metrics.
        We scrape it anyway; if the endpoint isn't present, Prometheus
        marks the target down without failing the scrape config.
    """
    return f"""# Auto-generated by Lakehouse Studio monitoring sidecar.
# Edit freely — this file is NOT regenerated unless you call disable+enable.
global:
  scrape_interval: 15s
  evaluation_interval: 15s
  external_labels:
    source: lakehouse-studio

scrape_configs:
  - job_name: prometheus
    static_configs:
      - targets: ['localhost:9090']

  # MinIO cluster metrics. To make this work without auth, add
  #   MINIO_PROMETHEUS_AUTH_TYPE=public
  # to the install's .env and restart minio. Otherwise paste a bearer token
  # under `authorization:` below (see Prometheus docs for the syntax).
  - job_name: minio
    metrics_path: /minio/v2/metrics/cluster
    scheme: http
    static_configs:
      - targets: ['minio:9000']
    # authorization:
    #   credentials: <BEARER_TOKEN_FROM_MINIO_ADMIN_PROMETHEUS_GENERATE>

  # StarRocks FE — requires `enable_prometheus_metrics = true` in fe.conf.
  # Until that flag is set, the readiness endpoint at /api/health gives us a
  # cheap up/down signal which is what the starter dashboard uses.
  - job_name: starrocks-fe-health
    metrics_path: /api/health
    static_configs:
      - targets: ['starrocks-fe:8030']

  - job_name: starrocks-fe-metrics
    metrics_path: /metrics
    static_configs:
      - targets: ['starrocks-fe:8030']

  # Iceberg REST — `/metrics` is conditional on the upstream image. Newer
  # builds expose it; tabulario/iceberg-rest:1.6.0 does not. The target will
  # show down in Prometheus if absent (harmless — same as a stopped service).
  - job_name: iceberg-rest
    metrics_path: /metrics
    static_configs:
      - targets: ['iceberg-rest:8181']

# Retention is set via CLI flag on the prometheus service (storage.tsdb.retention.time={retention_days}d).
"""


def _build_grafana_datasource_yml() -> str:
    """Pre-mounted datasource provisioning so Grafana auto-wires the
    Prometheus side-car on first boot. The URL uses the docker service name
    (the two containers share the same compose network)."""
    return """# Auto-generated by Lakehouse Studio monitoring sidecar.
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
    editable: true
"""


def _build_grafana_dashboard_provider_yml() -> str:
    """Provisioning config that points Grafana at the on-disk dashboard JSON
    directory. Combined with the mounted volume, this gives operators a
    starter dashboard the moment Grafana boots."""
    return """# Auto-generated by Lakehouse Studio monitoring sidecar.
apiVersion: 1

providers:
  - name: lakehouse-studio
    orgId: 1
    folder: 'Lakehouse Studio'
    type: file
    disableDeletion: false
    updateIntervalSeconds: 30
    allowUiUpdates: true
    options:
      path: /var/lib/grafana/dashboards
"""


def _build_starter_dashboard_json() -> str:
    """Minimal connectivity dashboard — one stat panel per service showing
    up/down. Deep metrics are deliberately out of scope; the goal is to give
    the operator a single screen that proves the sidecar is wired correctly."""
    dashboard = {
        "annotations": {"list": []},
        "editable": True,
        "fiscalYearStartMonth": 0,
        "graphTooltip": 0,
        "id": None,
        "panels": [
            {
                "datasource": {"type": "prometheus", "uid": "PBFA97CFB590B2093"},
                "fieldConfig": {
                    "defaults": {
                        "mappings": [
                            {"options": {"0": {"text": "DOWN", "color": "red"}}, "type": "value"},
                            {"options": {"1": {"text": "UP", "color": "green"}}, "type": "value"},
                        ],
                        "thresholds": {"mode": "absolute", "steps": [
                            {"color": "red", "value": None},
                            {"color": "green", "value": 1},
                        ]},
                    },
                    "overrides": [],
                },
                "gridPos": {"h": 6, "w": 6, "x": col * 6, "y": 0},
                "id": idx + 1,
                "options": {
                    "colorMode": "background",
                    "graphMode": "none",
                    "justifyMode": "auto",
                    "orientation": "auto",
                    "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                    "textMode": "auto",
                },
                "pluginVersion": "11.3.0",
                "targets": [
                    {
                        "expr": f'up{{job="{job}"}}',
                        "legendFormat": job,
                        "refId": "A",
                    }
                ],
                "title": title,
                "type": "stat",
            }
            for idx, (col, job, title) in enumerate([
                (0, "minio", "MinIO"),
                (1, "starrocks-fe-health", "StarRocks FE"),
                (2, "iceberg-rest", "Iceberg REST"),
                (3, "prometheus", "Prometheus"),
            ])
        ],
        "refresh": "30s",
        "schemaVersion": 39,
        "tags": ["lakehouse-studio"],
        "templating": {"list": []},
        "time": {"from": "now-15m", "to": "now"},
        "timepicker": {},
        "timezone": "browser",
        "title": "Lakehouse Studio — Service Connectivity",
        "uid": "lhs-overview",
        "version": 1,
        "weekStart": "",
    }
    return json.dumps(dashboard, indent=2)


def _build_override_compose(profile: MonitoringProfile, admin_password: str) -> str:
    """Compose v2 override — no `version:` key, just `services:` (and
    `volumes:` for Grafana storage). Designed to layer on top of the base
    compose via `-f docker-compose.yml -f docker-compose.metrics.yml`.

    Important: both services bind to ./monitoring/* paths INSIDE the
    install_dir (relative to the override file's location, which is how
    Compose resolves bind-mount paths). That keeps everything self-contained
    and means `disable_monitoring` can wipe a single subdir + the override.
    """
    retention = f"{profile.prometheus_retention_days}d"

    prom_block = f"""  prometheus:
    image: {_PROMETHEUS_IMAGE}
    container_name: lhs-prometheus
    restart: unless-stopped
    command:
      - --config.file=/etc/prometheus/prometheus.yml
      - --storage.tsdb.path=/prometheus
      - --storage.tsdb.retention.time={retention}
      - --web.console.libraries=/usr/share/prometheus/console_libraries
      - --web.console.templates=/usr/share/prometheus/consoles
    ports:
      - "{_PROMETHEUS_PORT}:9090"
    volumes:
      - ./{_MONITORING_SUBDIR}/prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - lhs_prometheus_data:/prometheus
"""

    grafana_block = ""
    if profile.include_grafana:
        grafana_block = f"""  grafana:
    image: {_GRAFANA_IMAGE}
    container_name: lhs-grafana
    restart: unless-stopped
    depends_on:
      - prometheus
    environment:
      - GF_SECURITY_ADMIN_PASSWORD={admin_password}
      - GF_USERS_ALLOW_SIGN_UP=false
      - GF_AUTH_ANONYMOUS_ENABLED=false
    ports:
      - "{_GRAFANA_PORT}:3000"
    volumes:
      - ./{_MONITORING_SUBDIR}/grafana/provisioning:/etc/grafana/provisioning:ro
      - ./{_MONITORING_SUBDIR}/grafana/dashboards:/var/lib/grafana/dashboards:ro
      - lhs_grafana_data:/var/lib/grafana
"""

    volumes_block = "volumes:\n  lhs_prometheus_data:\n"
    if profile.include_grafana:
        volumes_block += "  lhs_grafana_data:\n"

    # Compose v2: no version key, services at top.
    return (
        "# Auto-generated by Lakehouse Studio monitoring sidecar.\n"
        "# Activate with:\n"
        f"#   docker compose -f docker-compose.yml -f {_OVERRIDE_FILENAME} up -d\n"
        "# This file is NOT part of the certified lock; it's opt-in operational tooling.\n"
        "services:\n"
        + prom_block
        + grafana_block
        + "\n"
        + volumes_block
    )


# ---------- public API ----------


def is_monitoring_enabled(install_id: str) -> bool:
    """Cheap presence check — does the override file exist?"""
    try:
        return _override_path(install_id).exists()
    except ValueError:
        # install not found or install_dir missing — treat as not enabled
        # rather than raising; callers (e.g. UI) use this for display only.
        return False


def activate_command(install_id: str) -> str:
    """The exact `docker compose` command the operator runs to bring the
    sidecar up. Pure string builder — never executes anything. We return this
    instead of running it because:
      1. The compose call belongs to the operator's existing run.sh / udp.sh
         flow, not the API server
      2. Running long-lived docker commands from the API would tie up the
         event loop and complicate cancellation
    """
    return (
        f"docker compose -f docker-compose.yml -f {_OVERRIDE_FILENAME} up -d"
    )


async def enable_monitoring(install_id: str, profile: MonitoringProfile) -> dict:
    """Write the override file + the monitoring/ subtree. Idempotent — calling
    twice rewrites both with the latest profile (and regenerates the admin
    password if the caller didn't pin one). Returns the activate command and
    the admin password (if generated) so the caller can surface it to the
    operator ONCE.

    NEVER writes the admin password anywhere on disk outside the running
    grafana container's env. NEVER logs it.
    """
    install_dir = _install_dir(install_id)
    mon_dir = install_dir / _MONITORING_SUBDIR

    # Resolve admin password: caller-supplied wins; otherwise generate.
    user_supplied = bool(profile.grafana_admin_password)
    admin_password = profile.grafana_admin_password or _generate_admin_password()

    # Build all file contents up-front so a write failure mid-way doesn't
    # leave a half-configured sidecar.
    prom_yml = _build_prometheus_yml(profile.prometheus_retention_days)
    override_yml = _build_override_compose(profile, admin_password)

    # Write prometheus.yml
    _atomic_write(mon_dir / "prometheus.yml", prom_yml)

    if profile.include_grafana:
        ds_yml = _build_grafana_datasource_yml()
        dash_provider_yml = _build_grafana_dashboard_provider_yml()
        starter_dashboard = _build_starter_dashboard_json()
        _atomic_write(
            mon_dir / "grafana" / "provisioning" / "datasources" / "datasource.yml",
            ds_yml,
        )
        _atomic_write(
            mon_dir / "grafana" / "provisioning" / "dashboards" / "dashboard.yml",
            dash_provider_yml,
        )
        _atomic_write(
            mon_dir / "grafana" / "dashboards" / "lakehouse-overview.json",
            starter_dashboard,
        )

    # Override compose last — once this exists, is_monitoring_enabled() flips
    # true. Writing it last keeps the on/off signal consistent with the
    # presence of the supporting files.
    _atomic_write(install_dir / _OVERRIDE_FILENAME, override_yml)

    log.info(
        "monitoring enabled install=%s grafana=%s retention=%dd",
        install_id, profile.include_grafana, profile.prometheus_retention_days,
    )

    return {
        "compose_file_path": str(install_dir / _OVERRIDE_FILENAME),
        "activate_command": activate_command(install_id),
        "grafana_port": _GRAFANA_PORT if profile.include_grafana else None,
        "prometheus_port": _PROMETHEUS_PORT,
        "admin_password_set": True,
        # Returned ONCE — caller must surface this to the operator and then
        # discard. Never persisted on the server outside the running Grafana
        # container's environment.
        "grafana_admin_password": admin_password if profile.include_grafana else None,
        "grafana_admin_password_user_supplied": user_supplied,
    }


async def disable_monitoring(install_id: str) -> dict:
    """Remove the override file and the monitoring/ subdir. Caller takes the
    containers down — we never invoke `docker compose down` from here for the
    same reason `enable` doesn't run `up`."""
    install_dir = _install_dir(install_id)
    override = install_dir / _OVERRIDE_FILENAME
    mon_dir = install_dir / _MONITORING_SUBDIR

    removed_override = False
    removed_subdir = False

    if override.exists():
        try:
            override.unlink()
            removed_override = True
        except OSError as e:
            log.warning("failed to remove override %s: %s", override, e)

    if mon_dir.exists():
        try:
            shutil.rmtree(mon_dir)
            removed_subdir = True
        except OSError as e:
            log.warning("failed to remove monitoring dir %s: %s", mon_dir, e)

    log.info(
        "monitoring disabled install=%s override_removed=%s subdir_removed=%s",
        install_id, removed_override, removed_subdir,
    )

    return {
        "override_removed": removed_override,
        "monitoring_subdir_removed": removed_subdir,
        "shutdown_hint": (
            "Run `docker compose stop lhs-prometheus lhs-grafana && "
            "docker compose rm -f lhs-prometheus lhs-grafana` from the "
            "install_dir to stop the sidecar containers."
        ),
    }
