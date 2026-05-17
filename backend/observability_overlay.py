"""Observability sidecar — Prometheus + Grafana + Loki opt-in compose overlay.

PURE ADDITIVE MODULE. Same shape as `airflow_overlay.py`,
`dagster_overlay.py`, `superset_overlay.py`, `caddy_tls.py`, and
`jdbc_extras.py`:

  - NEVER touches the base `docker-compose.yml` that
    `runner._patch_compose_images()` writes (FROZEN — certified-stack
    contract).
  - Writes a sibling `docker-compose.observability.yml` the operator opts
    into via the env flag `LHS_OBSERVABILITY_ENABLED=true` (default OFF).
    runner.py consults that flag during the env step and appends our
    override file via `-f` so compose merges it with the base stack.
  - All sensitive values (Grafana admin password) come from env vars with
    safe placeholder defaults that log a LOUD warning if left unchanged.

This overlay closes the v0.6.2 gap against §5.6.1 of the founding
architecture doc, which explicitly lists Prometheus + Grafana + Loki as
the core observability category alongside storage / catalog / processing.
Section 5.6.3 also mentions OpenLineage / Marquez for lineage which is a
separate catalog-only entry (no overlay required at this scope).

Services defined in the override:

  - prometheus      prom/prometheus v2.55.1, scrape config pre-wired for
                    MinIO (/minio/v2/metrics/cluster), Trino (/v1/jmx),
                    StarRocks FE (/metrics), Loki (/metrics) and itself.
                    Healthcheck via the /-/healthy endpoint.
  - grafana         grafana/grafana 11.3.1, pre-provisioned with a
                    Prometheus datasource pointing at prometheus:9090 and
                    a Loki datasource pointing at loki:3100. Admin password
                    comes from env, defaults to a CHANGE_ME placeholder.
                    Port 3001 on host (3000 collides with Dagster sidecar).
  - loki            grafana/loki 3.2.1, pre-configured for local
                    filesystem storage (no S3 backend in this overlay —
                    that's a future tier). 7-day retention to keep disk
                    usage bounded on a single-host install.

Network: external `<install_dir.name>_default` (overridable via
LHS_DOCKER_NETWORK), same pattern as airflow/dagster/superset so the
scrape targets resolve by name (trino, starrocks-fe, minio).

Image tags pinned 2026-05-17:
  - prom/prometheus:v2.55.1   — current 2.x stable. Apache-2.0.
  - grafana/grafana:11.3.1    — current 11.x stable. AGPL-3.0.
  - grafana/loki:3.2.1        — current 3.x stable. AGPL-3.0.

NOTE: Grafana 11.x is AGPL-3.0 (relicensed from Apache-2.0 in 2021); Loki
3.x is also AGPL-3.0. Both ship as binaries and the AGPL terms apply to
modifications of the server itself, not to dashboards/datasources/users
of the server — i.e., normal operational use does not trigger AGPL
distribution obligations. The catalog entries document this so operators
making redistribution decisions see the license clearly.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional


log = logging.getLogger("lhs.observability_overlay")


# ---- pinned image tags (verified 2026-05-17) ----
_PROMETHEUS_IMAGE = "prom/prometheus:v2.55.1"
_GRAFANA_IMAGE = "grafana/grafana:11.3.1"
_LOKI_IMAGE = "grafana/loki:3.2.1"

# Filename the runner appends with `-f` when LHS_OBSERVABILITY_ENABLED is true.
# Exported at module level so runner.py can import + use without parsing.
OVERLAY_FILENAME = "docker-compose.observability.yml"

# Env flag the runner checks before calling write_observability_overlay().
# Mirrors LHS_AIRFLOW_ENABLED / LHS_DAGSTER_ENABLED / LHS_SUPERSET_ENABLED.
ENV_FLAG = "LHS_OBSERVABILITY_ENABLED"

# Service names this overlay adds. Runner appends these to the
# explicit `docker compose up -d <services>` argv so they spin up
# alongside the base stack's services.
SERVICES = ["prometheus", "grafana", "loki"]

# Named volumes — distinct from the base stack's volumes so `docker
# volume rm` is targeted.
_PROM_VOLUME = "prometheus-data"
_GRAFANA_VOLUME = "grafana-data"
_LOKI_VOLUME = "loki-data"

# Host-side port mappings. Choices:
#   prometheus  9090 → 9090   (Prometheus default, no collision in our stacks)
#   grafana     3001 → 3000   (3000 collides with Dagster webserver; we shift)
#   loki        3100 → 3100   (Loki default, no collision)
_PROM_HOST_PORT = 9090
_PROM_CONTAINER_PORT = 9090
_GRAFANA_HOST_PORT = 3001
_GRAFANA_CONTAINER_PORT = 3000
_LOKI_HOST_PORT = 3100
_LOKI_CONTAINER_PORT = 3100

# Subdirectory holding the on-disk observability config files. Mounted
# read-only into each service so config changes survive `docker compose
# down` + `up` cycles.
_CFG_SUBDIR = "observability"

# Placeholder secret. We do NOT generate a random per-install value —
# operator sets it in .env and we surface a loud warning at write time.
_DEFAULT_GRAFANA_ADMIN_USER = "admin"
_DEFAULT_GRAFANA_ADMIN_PASSWORD = "CHANGE_ME_grafana_admin"  # noqa: S105 — explicit placeholder


# ---------- helpers ----------


def _atomic_write(path: Path, data: str) -> None:
    """tmp + os.replace atomic write. Mirrors the sibling overlays —
    avoids half-written files on Ctrl-C or power loss."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(data, encoding="utf-8")
    os.replace(tmp, path)


def _network_name(install_dir: Path, env: dict) -> str:
    """Resolve the docker network name to attach to. Same logic as the
    sibling overlays. The base stack lives on `<install_dir.name>_default`
    by docker compose convention; env can override via LHS_DOCKER_NETWORK."""
    explicit = env.get("LHS_DOCKER_NETWORK", "").strip()
    if explicit:
        return explicit
    return f"{install_dir.name}_default"


def _resolve_secret(env: dict, key: str, default: str) -> tuple[str, bool]:
    """Read `key` from env, return (value, is_default). is_default=True
    means the operator did NOT override and we should log a warning."""
    raw = env.get(key)
    if raw is None or raw.strip() == "":
        return default, True
    return raw, False


def _warn_default(key: str, hint: str) -> None:
    """Loud one-liner warning that a secret is still at the placeholder."""
    log.warning(
        "OBSERVABILITY OVERLAY: %s is using the default placeholder value. %s",
        key, hint,
    )


# ---------- config rendering ----------


def _render_prometheus_config() -> str:
    """Render the prometheus.yml scrape config.

    Targets:
      - prometheus self-scrape (/metrics on :9090)
      - loki         (/metrics on :3100)
      - minio        (/minio/v2/metrics/cluster — requires
                      MINIO_PROMETHEUS_AUTH_TYPE=public on the MinIO
                      container; we document this in the overlay header
                      but do NOT mutate the base stack's MinIO config)
      - trino        (/v1/jmx on :8080 — when trino is in the cart)
      - starrocks-fe (/metrics on :8030)

    Scrape targets that don't exist in the cart will simply show as
    "down" in the Prometheus UI — that's acceptable and noted in the
    Grafana dashboard so operators understand why.
    """
    return """# Lakehouse Studio Prometheus config.
# Generated by backend/observability_overlay.py.
#
# Scrape targets cover the lakehouse stack's exposed metrics endpoints.
# Targets that don't exist in the current cart (e.g. trino in a Spark-only
# stack) show as down — that's expected and visible in the dashboards.

global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: prometheus
    static_configs:
      - targets: ['prometheus:9090']

  - job_name: loki
    static_configs:
      - targets: ['loki:3100']

  - job_name: minio
    metrics_path: /minio/v2/metrics/cluster
    # Requires MINIO_PROMETHEUS_AUTH_TYPE=public on the MinIO container.
    # The base stack doesn't set this by default; operator adds it via
    # the .env's LHS_MINIO_EXTRA_ENV mechanism if they want MinIO metrics.
    static_configs:
      - targets: ['minio:9000']

  - job_name: trino
    metrics_path: /v1/jmx
    static_configs:
      - targets: ['trino:8080']

  - job_name: starrocks-fe
    metrics_path: /metrics
    static_configs:
      - targets: ['starrocks-fe:8030']

  - job_name: starrocks-be
    metrics_path: /metrics
    static_configs:
      - targets: ['starrocks-be:8040']
"""


def _render_grafana_datasources() -> str:
    """Render the Grafana datasources.yml that gets dropped into
    /etc/grafana/provisioning/datasources/. Pre-wires Prometheus + Loki
    so first-time operators see a working datasource on login."""
    return """# Lakehouse Studio Grafana datasources.
# Generated by backend/observability_overlay.py.
#
# Pre-provisioned so the operator sees working datasources on first
# login. The actual dashboards are operator-built — we deliberately
# don't ship opinionated dashboards in v0.6.2 because the cart-driven
# stack composition means every install has a different set of
# services to chart.

apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
    editable: true

  - name: Loki
    type: loki
    access: proxy
    url: http://loki:3100
    editable: true
"""


def _render_loki_config() -> str:
    """Render loki-config.yaml.

    Local filesystem storage with 7-day retention — bounded disk usage
    on a single-host install. Operators scaling out add an S3 backend
    via the LOKI_STORAGE_* env vars (not in scope for v0.6.2).
    """
    return """# Lakehouse Studio Loki config.
# Generated by backend/observability_overlay.py.
#
# Single-binary mode with local filesystem storage. Good for a
# single-host install; operators scaling out swap the storage backend
# to S3-compatible later.
#
# 7-day retention keeps disk usage bounded — Loki rotates and deletes
# chunks older than the retention window automatically.

auth_enabled: false

server:
  http_listen_port: 3100
  grpc_listen_port: 9096

common:
  instance_addr: 127.0.0.1
  path_prefix: /loki
  storage:
    filesystem:
      chunks_directory: /loki/chunks
      rules_directory: /loki/rules
  replication_factor: 1
  ring:
    kvstore:
      store: inmemory

schema_config:
  configs:
    - from: 2024-01-01
      store: tsdb
      object_store: filesystem
      schema: v13
      index:
        prefix: index_
        period: 24h

limits_config:
  retention_period: 168h
  reject_old_samples: true
  reject_old_samples_max_age: 168h

compactor:
  working_directory: /loki/compactor
  delete_request_store: filesystem
  retention_enabled: true
"""


# ---------- compose override rendering ----------


def _render_overlay(network: str, grafana_admin_user: str,
                    grafana_admin_password: str) -> str:
    """Render the docker-compose.observability.yml content.

    Three services + three named volumes + one external network. We use
    Compose v2 syntax (no `version:` key) to match the base file and the
    other overlays.
    """
    return (
        "# docker-compose.observability.yml -- Prometheus + Grafana + Loki overlay.\n"
        "# Generated by backend/observability_overlay.py.\n"
        "#\n"
        f"# Activated automatically when {ENV_FLAG}=true is set in the\n"
        "# install's .env. runner.py appends this file via `-f` so compose\n"
        "# merges it with the base stack.\n"
        "#\n"
        "# This file is an OVERRIDE. The base docker-compose.yml is FROZEN\n"
        "# (certified-stack contract). Observability is an operational extra,\n"
        "# NOT part of the certified compatibility lock.\n"
        "services:\n"
        "  prometheus:\n"
        f"    image: {_PROMETHEUS_IMAGE}\n"
        "    container_name: lhs-prometheus\n"
        "    restart: unless-stopped\n"
        "    command:\n"
        "      - '--config.file=/etc/prometheus/prometheus.yml'\n"
        "      - '--storage.tsdb.path=/prometheus'\n"
        "      - '--web.console.libraries=/usr/share/prometheus/console_libraries'\n"
        "      - '--web.console.templates=/usr/share/prometheus/consoles'\n"
        "    volumes:\n"
        f"      - ./{_CFG_SUBDIR}/prometheus.yml:/etc/prometheus/prometheus.yml:ro\n"
        f"      - {_PROM_VOLUME}:/prometheus\n"
        "    ports:\n"
        f"      - \"{_PROM_HOST_PORT}:{_PROM_CONTAINER_PORT}\"\n"
        "    healthcheck:\n"
        "      test: [\"CMD\", \"wget\", \"--quiet\", \"--tries=1\", \"--spider\", \"http://localhost:9090/-/healthy\"]\n"
        "      interval: 30s\n"
        "      timeout: 10s\n"
        "      retries: 5\n"
        "    networks:\n"
        "      - default\n"
        "  grafana:\n"
        f"    image: {_GRAFANA_IMAGE}\n"
        "    container_name: lhs-grafana\n"
        "    restart: unless-stopped\n"
        "    depends_on:\n"
        "      prometheus:\n"
        "        condition: service_healthy\n"
        "    environment:\n"
        f"      GF_SECURITY_ADMIN_USER: \"{grafana_admin_user}\"\n"
        f"      GF_SECURITY_ADMIN_PASSWORD: \"{grafana_admin_password}\"\n"
        "      GF_USERS_ALLOW_SIGN_UP: \"false\"\n"
        "      GF_AUTH_ANONYMOUS_ENABLED: \"false\"\n"
        "    volumes:\n"
        f"      - {_GRAFANA_VOLUME}:/var/lib/grafana\n"
        f"      - ./{_CFG_SUBDIR}/grafana-datasources.yml:/etc/grafana/provisioning/datasources/datasources.yml:ro\n"
        "    ports:\n"
        f"      - \"{_GRAFANA_HOST_PORT}:{_GRAFANA_CONTAINER_PORT}\"\n"
        "    healthcheck:\n"
        "      test: [\"CMD-SHELL\", \"wget --quiet --tries=1 --spider http://localhost:3000/api/health || exit 1\"]\n"
        "      interval: 30s\n"
        "      timeout: 10s\n"
        "      retries: 5\n"
        "    networks:\n"
        "      - default\n"
        "  loki:\n"
        f"    image: {_LOKI_IMAGE}\n"
        "    container_name: lhs-loki\n"
        "    restart: unless-stopped\n"
        "    command: -config.file=/etc/loki/loki-config.yaml\n"
        "    volumes:\n"
        f"      - ./{_CFG_SUBDIR}/loki-config.yaml:/etc/loki/loki-config.yaml:ro\n"
        f"      - {_LOKI_VOLUME}:/loki\n"
        "    ports:\n"
        f"      - \"{_LOKI_HOST_PORT}:{_LOKI_CONTAINER_PORT}\"\n"
        "    healthcheck:\n"
        "      test: [\"CMD-SHELL\", \"wget --quiet --tries=1 --spider http://localhost:3100/ready || exit 1\"]\n"
        "      interval: 30s\n"
        "      timeout: 10s\n"
        "      retries: 5\n"
        "    networks:\n"
        "      - default\n"
        "volumes:\n"
        f"  {_PROM_VOLUME}:\n"
        f"  {_GRAFANA_VOLUME}:\n"
        f"  {_LOKI_VOLUME}:\n"
        "networks:\n"
        "  default:\n"
        f"    name: {network}\n"
        "    external: true\n"
    )


# ---------- validation ----------


def validate_overlay(install_dir: Path) -> list[str]:
    """Return a list of human-readable problems for /healthz.

    Empty list = healthy / not enabled. We check:
      - if the overlay file exists, the observability/ config folder must too
      - each config file must exist (prometheus.yml, grafana-datasources.yml,
        loki-config.yaml)

    We do NOT check container health here — that's docker's job. Static
    file consistency only, callable cheaply by the API layer.
    """
    problems: list[str] = []
    overlay = install_dir / OVERLAY_FILENAME
    if not overlay.exists():
        # not enabled — nothing to validate
        return problems

    cfg_dir = install_dir / _CFG_SUBDIR
    if not cfg_dir.exists():
        problems.append(
            f"observability overlay present but {cfg_dir} missing — "
            "prometheus/grafana/loki will fail to read their configs"
        )
        return problems

    for required in ("prometheus.yml", "grafana-datasources.yml",
                     "loki-config.yaml"):
        if not (cfg_dir / required).exists():
            problems.append(f"observability config missing {required}")

    return problems


# ---------- public API ----------


def write_observability_overlay(install_dir: Path, env: dict) -> Optional[Path]:
    """Write the observability overlay + config files into install_dir.

    Returns the Path to the written docker-compose.observability.yml, or
    None if the env flag is OFF (in which case nothing is written).

    Idempotent — re-running with the same install_dir overwrites every
    generated file. Safe to call from `runner._step_env` every install.
    The env dict is the merged install env (.env contents + process env
    overrides the operator may have supplied).
    """
    flag = env.get(ENV_FLAG, "").strip().lower()
    if flag not in ("1", "true", "yes", "on"):
        log.debug("observability overlay disabled (%s=%r)", ENV_FLAG, flag)
        return None

    network = _network_name(install_dir, env)

    admin_user, user_is_default = _resolve_secret(
        env, "GRAFANA_ADMIN_USER", _DEFAULT_GRAFANA_ADMIN_USER)
    admin_password, pw_is_default = _resolve_secret(
        env, "GRAFANA_ADMIN_PASSWORD", _DEFAULT_GRAFANA_ADMIN_PASSWORD)

    if user_is_default:
        log.info("OBSERVABILITY OVERLAY: GRAFANA_ADMIN_USER defaulting to 'admin'")
    if pw_is_default:
        _warn_default(
            "GRAFANA_ADMIN_PASSWORD",
            "Set it in the install's .env before exposing port 3001 to anything beyond localhost.",
        )

    # Write config files FIRST so the services never start pointed at
    # missing files. Compose merge would otherwise race against the
    # bind-mounts.
    cfg_dir = install_dir / _CFG_SUBDIR
    _atomic_write(cfg_dir / "prometheus.yml", _render_prometheus_config())
    _atomic_write(cfg_dir / "grafana-datasources.yml", _render_grafana_datasources())
    _atomic_write(cfg_dir / "loki-config.yaml", _render_loki_config())

    overlay_path = install_dir / OVERLAY_FILENAME
    overlay_body = _render_overlay(network, admin_user, admin_password)
    _atomic_write(overlay_path, overlay_body)

    log.info(
        "observability overlay written install_dir=%s network=%s admin_user=%s",
        install_dir, network, admin_user,
    )
    return overlay_path
