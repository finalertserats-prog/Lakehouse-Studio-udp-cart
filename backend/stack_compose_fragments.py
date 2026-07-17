"""v0.6.1 — per-stack docker-compose fragment writers.

PURE ADDITIVE MODULE. Same atomic-write + external-network shape as
`airflow_overlay.py`, but with a critical contract difference:

  * Airflow / Dagster / Superset overlays are OPT-IN operational
    extras gated by `LHS_*_ENABLED` env flags. They never run by
    default.
  * Stack compose fragments here are REQUIRED to make the four
    `candidate` stacks installable at all. UDP's upstream
    docker-compose.yml does NOT ship a `nessie`, `polaris`,
    `hive-metastore`, or `postgres-hms` service definition, so each
    of those stacks would fail at the `docker compose up` step.

The runner calls `write_fragment(stack_id, install_dir, env)` from
`_step_env` after `_write_optional_overlays`. The writer returns either
a `Path` (fragment written) or `None` (no fragment needed — the stable
`udp-local-v0.2` stack hits this branch). When a path is returned, the
runner prepends an entry into `self._overlays` so the docker_compose_up
argv injects `-f docker-compose.fragment.yml` BEFORE any opt-in overlays
(so the fragment's services are visible to anything that depends on them).

Each render function returns the YAML body as a string. `write_fragment`
is responsible for writing it atomically to disk. This keeps the render
functions trivially unit-testable without touching the filesystem.

All services attach to the base stack's docker network via an `external:
true` declaration, exactly like `airflow_overlay.py`. Without this they
would land on a second network and couldn't talk to MinIO (or each other)
by service name.

Image tags pinned 2026-05-17 — matched against the manifests in
`stacks/iceberg-nessie-trino-local-v0.1.yaml`,
`stacks/hudi-hms-spark-local-v0.1.yaml`,
`stacks/delta-hms-spark-trino-local-v0.1.yaml`, and
`stacks/iceberg-polaris-spark-local-v0.1.yaml`.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Callable, Optional


log = logging.getLogger("lhs.stack_compose_fragments")


# ---- pinned image tags (verified 2026-05-17 against stack manifests) ----
_NESSIE_IMAGE = "ghcr.io/projectnessie/nessie:0.99.0"
_HMS_IMAGE = "bitsondatadev/hive-metastore:latest"
# tabulario/spark-iceberg (same image the udp stacks use): ships PySpark 3.5 +
# Iceberg S3FileIO. Added to stacks that lack a Spark to run the multi-format
# ETL (e.g. Nessie). Delta/Hudi/hadoop-aws come in at submit time via --packages.
_SPARK_ICEBERG_IMAGE = "tabulario/spark-iceberg:3.5.5_1.8.1"
_POLARIS_IMAGE = "apache/polaris:1.4.1"  # bumped from 1.0.1 — that tag never published; 1.4.1 is real + has CVE fixes (Gemini research 2026-05-17, verified via docker manifest inspect)
_POSTGRES_IMAGE = "postgres:15-alpine"
_MYSQL_IMAGE = "mysql:8.0"
# Trino 481 — updated from 475 on 2026-06-17 to match lock files.
_TRINO_IMAGE = "trinodb/trino:481"
# JupyterLab with PySpark 3.5 pre-installed (AI/ML Research stack).
_JUPYTER_IMAGE = "jupyter/all-spark-notebook:spark-3.5.0"
# OpenLineage server (Marquez) + web UI + its dedicated Postgres (Fintech stack).
_MARQUEZ_IMAGE = "marquezproject/marquez:0.50.0"
_MARQUEZ_WEB_IMAGE = "marquezproject/marquez-web:0.50.0"
_PG_MARQUEZ_VOLUME = "udp-postgres-marquez-data"


# Filename the runner injects with `-f` when a fragment is needed.
# Exported so runner.py and tests can import it without parsing.
FRAGMENT_FILENAME = "docker-compose.fragment.yml"


# Service names each fragment adds. The runner appends these to the
# explicit `docker compose up -d <services>` argv so the fragment's
# services come up alongside the base stack's services rather than
# being filtered out by the runner's per-cart service list.
FRAGMENT_SERVICES: dict[str, list[str]] = {
    # Local Demo is now a MULTI-FORMAT lakehouse: it gains a Hive Metastore so
    # that when the user picks Delta or Hudi, StarRocks can register a
    # delta_catalog / hudi_catalog against HMS (Iceberg keeps its REST catalog).
    # HMS is harmless for the Iceberg case — it just goes unused.
    "udp-local-v0.2":                    ["mysql-hms", "hive-metastore"],
    # Superset belongs to Startup Analytics, NOT the Local Demo (udp-local-v0.2).
    # Keying it here (per-stack) is what makes the two templates install
    # genuinely different stacks instead of both dragging in Superset.
    "startup-analytics-local-v0.1":      ["superset", "mysql-hms", "hive-metastore"],
    "ai-ml-research-local-v0.1":         ["trino", "jupyter", "mysql-hms", "hive-metastore"],
    "udp-trino-local-v0.1":              ["trino", "mysql-hms", "hive-metastore"],
    "fintech-compliance-local-v0.1":     ["trino", "openlineage", "postgres-marquez", "marquez-web", "mysql-hms", "hive-metastore"],
    "iceberg-nessie-trino-local-v0.1":  ["nessie", "trino", "postgres-airflow", "airflow", "spark", "mysql-hms", "hive-metastore"],
    "hudi-hms-spark-local-v0.1":        ["mysql-hms", "hive-metastore"],
    "delta-hms-spark-trino-local-v0.1": ["mysql-hms", "hive-metastore", "trino"],
    "iceberg-polaris-spark-local-v0.1": ["postgres-polaris", "polaris"],
}


# Named volumes for DB-backing service data, kept distinct per stack so
# `docker volume rm` is targeted and side-by-side installs don't clobber
# each other. Idempotent — re-running docker compose up reuses the volume.
#
# RENAMED 2026-05-17 to force a fresh MySQL data dir: the v0.6.2 refactor
# swapped postgres → mysql for the HMS backing service but kept the same
# named volume (`udp-postgres-hms-data`). MySQL 8 on first boot saw the
# leftover postgres files and aborted with:
#   [ERROR] [MY-010457] --initialize specified but the data directory
#   has files in it. Aborting.
# Renaming the constant to `udp-mysql-hms-data` gives MySQL a virgin
# data dir on every host that has never run the new volume name before.
# Operators migrating from a previous postgres-hms install can reclaim
# the ~200 MB of orphaned postgres data with:
#   docker volume rm udp-postgres-hms-data
# Polaris still uses Postgres, so `_PG_POLARIS_VOLUME` is unchanged.
_MYSQL_HMS_VOLUME = "udp-mysql-hms-data"
_PG_POLARIS_VOLUME = "udp-postgres-polaris-data"


# ---------- helpers ----------


def _atomic_write(path: Path, data: str) -> None:
    """tmp + os.replace atomic write. Same pattern as airflow_overlay.py
    — avoids half-written files on Ctrl-C or power loss."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(data, encoding="utf-8")
    os.replace(tmp, path)


def _network_name(install_dir: Path, env: dict) -> str:
    """Resolve the docker network name to attach to. The base stack lives
    on `<install_dir.name>_default` by docker compose convention; the env
    can override via LHS_DOCKER_NETWORK if the operator renamed it."""
    explicit = (env.get("LHS_DOCKER_NETWORK") or "").strip()
    if explicit:
        return explicit
    return f"{install_dir.name}_default"


# ---------- render functions ----------


def _trino_service_yaml(env: dict) -> str:
    """Return ONLY the Trino service YAML block (indented under `services:`).

    No `services:` header, no `networks:` declaration — caller composes
    those. This lets multiple renderers (`_render_udp_trino_fragment`,
    `_render_nessie_fragment`, `_render_delta_fragment`) all share the
    exact same Trino service definition without copy-paste drift.

    Bug fix 2026-05-17 VPS install attempt of udp-trino-local-v0.1:
    UDP's upstream docker-compose.yml has no Trino service definition,
    so `docker compose up -d ... trino ...` fails with `no such
    service: trino`. This block supplies it.

    Trino reads its own catalog config (iceberg/nessie/delta .properties)
    from /etc/trino/catalog/, which the per-stack bootstrap script
    writes via `docker cp` after the container is up — so no bind mount
    or depends_on chain is required at compose level.
    """
    return (
        "  trino:\n"
        f"    image: {_TRINO_IMAGE}\n"
        "    container_name: udp-trino\n"
        "    restart: unless-stopped\n"
        "    environment:\n"
        "      # JVM + memory caps wired via compose interpolation so the\n"
        "      # operator can tune via the install's .env without\n"
        "      # re-rendering this fragment. Defaults match the manifest\n"
        "      # env_defaults block (10 GB recommended tier — 3 GB heap).\n"
        "      JAVA_TOOL_OPTIONS: ${TRINO_JAVA_OPTS:--Xms3G -Xmx3G -XX:+UseG1GC -XX:+ExplicitGCInvokesConcurrent -XX:+ExitOnOutOfMemoryError}\n"
        "      TRINO_QUERY_MAX_MEMORY_PER_NODE: ${TRINO_QUERY_MAX_MEMORY_PER_NODE:-1.5GB}\n"
        "      TRINO_QUERY_MAX_MEMORY: ${TRINO_QUERY_MAX_MEMORY:-1.5GB}\n"
        "    ports:\n"
        # Host port is env-overridable (TRINO_HTTP_PORT) so Trino stacks can
        # dodge a host that already runs something on 8080 (common on shared
        # servers). Container side stays 8080. Default preserves prior behavior.
        "      - \"${TRINO_HTTP_PORT:-8080}:8080\"\n"
        "    healthcheck:\n"
        "      test: [\"CMD-SHELL\", \"curl -fsS http://localhost:8080/v1/info >/dev/null\"]\n"
        "      interval: 15s\n"
        "      timeout: 5s\n"
        "      retries: 30\n"
        "      start_period: 60s\n"
        "    networks:\n"
        "      - default\n"
    )


def _render_udp_trino_fragment(env: dict) -> str:
    """Render the Trino-only fragment for udp-trino-local-v0.1.

    UDP's upstream docker-compose.yml ships minio + iceberg-rest +
    starrocks-fe + starrocks-be + create-bucket, but NOT trino. The
    udp-trino-local-v0.1 stack's `start` step calls `docker compose up
    -d minio iceberg-rest trino starrocks-fe starrocks-be create-bucket`
    — without this fragment that fails with `no such service: trino`.

    Trino-only fragment (no Nessie, no HMS). The stack uses the upstream
    iceberg-rest catalog, which the bootstrap wires via
    /etc/trino/catalog/iceberg.properties after Trino starts.
    """
    return (
        "# docker-compose.fragment.yml -- Trino service for "
        "udp-trino-local-v0.1.\n"
        "# Generated by backend/stack_compose_fragments.py.\n"
        "#\n"
        "# REQUIRED: UDP's upstream docker-compose.yml does not ship a\n"
        "# Trino service. This fragment supplies it so `docker compose\n"
        "# up -d ... trino ...` actually has a service to bring up.\n"
        "services:\n"
        + _trino_service_yaml(env)
        # Hive Metastore so this Trino stack is multi-format too (Delta/Hudi catalogs).
        + _hms_services_yaml(env)
        + "volumes:\n"
        f"  {_MYSQL_HMS_VOLUME}:\n"
        + "networks:\n"
        "  default:\n"
        # Name the compose default network explicitly with a HYPHEN so a
        # container's reverse-DNS PTR is `<container>.<net>` with no underscore.
        # The default `<project>_default` has an underscore, which is an illegal
        # URI hostname char and breaks HMS self-resolution -> StarRocks
        # getAllDatabases. Install-specific (via LHS_NET) so installs don't share.
        '    name: "${LHS_NET:-udp-net}"\n'
    )


def _jupyter_service_yaml(env: dict) -> str:
    """Return ONLY the JupyterLab service YAML block (indented under services:).

    jupyter/all-spark-notebook ships PySpark 3.5 + the Python data stack.
    Host port 8889 (container 8888) so it never collides with the base UDP
    Spark notebook on 8888. AWS creds point at MinIO so s3a:// reads work
    from a notebook; the operator points Spark at the iceberg-rest catalog
    for Iceberg tables (same warehouse the Trino/StarRocks engines read).
    """
    user = env.get("MINIO_ROOT_USER", "admin")
    pw = env.get("MINIO_ROOT_PASSWORD", "udp_admin_12345")
    token = env.get("JUPYTER_TOKEN", "lakehouse")
    return (
        "  jupyter:\n"
        f"    image: {_JUPYTER_IMAGE}\n"
        "    container_name: udp-jupyter\n"
        "    restart: unless-stopped\n"
        "    environment:\n"
        "      JUPYTER_ENABLE_LAB: \"yes\"\n"
        f"      JUPYTER_TOKEN: {token}\n"
        f"      AWS_ACCESS_KEY_ID: {user}\n"
        f"      AWS_SECRET_ACCESS_KEY: {pw}\n"
        "      AWS_REGION: us-east-1\n"
        "    ports:\n"
        "      - \"8889:8888\"\n"
        "    healthcheck:\n"
        "      test: [\"CMD-SHELL\", \"curl -fsS http://localhost:8888/api >/dev/null\"]\n"
        "      interval: 15s\n"
        "      timeout: 5s\n"
        "      retries: 12\n"
        "      start_period: 30s\n"
        "    networks:\n"
        "      - default\n"
    )


def _render_ai_ml_fragment(env: dict) -> str:
    """Render the Trino + JupyterLab fragment for ai-ml-research-local-v0.1.

    The AI/ML Research stack is the udp-trino runtime (MinIO + Iceberg-REST +
    Spark + StarRocks from UDP's base compose) PLUS Trino for federated SQL
    and JupyterLab for notebooks. UDP's upstream compose ships neither Trino
    nor Jupyter, and one fragment is produced per stack id, so this renderer
    supplies BOTH — reusing the shared `_trino_service_yaml()` block so the
    Trino definition never drifts from the other Trino-bearing stacks.
    """
    return (
        "# docker-compose.fragment.yml -- Trino + JupyterLab services for "
        "ai-ml-research-local-v0.1.\n"
        "# Generated by backend/stack_compose_fragments.py.\n"
        "#\n"
        "# REQUIRED: UDP's upstream docker-compose.yml ships neither Trino nor\n"
        "# JupyterLab. This fragment supplies both so the AI/ML Research stack\n"
        "# actually brings up a query engine AND a notebook surface.\n"
        "services:\n"
        + _trino_service_yaml(env)
        + _jupyter_service_yaml(env)
        # Hive Metastore so AI/ML Research is multi-format too (Delta/Hudi catalogs).
        + _hms_services_yaml(env)
        + "volumes:\n"
        f"  {_MYSQL_HMS_VOLUME}:\n"
        + "networks:\n"
        "  default:\n"
        # Name the compose default network explicitly with a HYPHEN so a
        # container's reverse-DNS PTR is `<container>.<net>` with no underscore.
        # The default `<project>_default` has an underscore, which is an illegal
        # URI hostname char and breaks HMS self-resolution -> StarRocks
        # getAllDatabases. Install-specific (via LHS_NET) so installs don't share.
        '    name: "${LHS_NET:-udp-net}"\n'
    )


def _render_fintech_fragment(env: dict) -> str:
    """Render the Trino + OpenLineage(Marquez) fragment for
    fintech-compliance-local-v0.1.

    The Fintech Compliance stack is the udp-trino runtime (MinIO + Iceberg-REST
    + Spark + Trino + StarRocks) PLUS OpenLineage for data-lineage / audit
    trails — the template's headline feature. UDP's upstream compose ships
    neither Trino nor OpenLineage, so this fragment supplies:

      * trino            — shared Trino service block (via _trino_service_yaml)
      * postgres-marquez — a DEDICATED Postgres whose db/role/password are all
                           `marquez` (Marquez's config hardcodes those). A
                           dedicated DB means Marquez auto-migrates its schema
                           on first boot — no separate bootstrap step needed.
      * openlineage      — Marquez server on 5000 (API) + 5001 (admin).
    """
    user = env.get("MINIO_ROOT_USER", "admin")  # noqa: F841 (kept for symmetry)
    return (
        "# docker-compose.fragment.yml -- Trino + OpenLineage(Marquez) for "
        "fintech-compliance-local-v0.1.\n"
        "# Generated by backend/stack_compose_fragments.py.\n"
        "#\n"
        "# REQUIRED: UDP's upstream docker-compose.yml ships neither Trino nor\n"
        "# OpenLineage. This fragment supplies both plus Marquez's dedicated\n"
        "# Postgres so the compliance stack actually delivers lineage tracking.\n"
        "services:\n"
        + _trino_service_yaml(env)
        + "  postgres-marquez:\n"
        f"    image: {_POSTGRES_IMAGE}\n"
        "    container_name: udp-postgres-marquez\n"
        "    restart: unless-stopped\n"
        "    environment:\n"
        "      POSTGRES_USER: marquez\n"
        "      POSTGRES_PASSWORD: marquez\n"
        "      POSTGRES_DB: marquez\n"
        "    volumes:\n"
        f"      - {_PG_MARQUEZ_VOLUME}:/var/lib/postgresql/data\n"
        "    healthcheck:\n"
        "      test: [\"CMD-SHELL\", \"pg_isready -U marquez\"]\n"
        "      interval: 10s\n"
        "      timeout: 5s\n"
        "      retries: 20\n"
        "      start_period: 20s\n"
        "    networks:\n"
        "      - default\n"
        "  openlineage:\n"
        f"    image: {_MARQUEZ_IMAGE}\n"
        "    container_name: udp-openlineage\n"
        "    restart: unless-stopped\n"
        "    environment:\n"
        "      MARQUEZ_PORT: \"5000\"\n"
        "      MARQUEZ_ADMIN_PORT: \"5001\"\n"
        "      MARQUEZ_CONFIG: \"\"\n"
        "      POSTGRES_HOST: udp-postgres-marquez\n"
        "      POSTGRES_PORT: \"5432\"\n"
        "      POSTGRES_DB: marquez\n"
        "      POSTGRES_USER: marquez\n"
        "      POSTGRES_PASSWORD: marquez\n"
        "    depends_on:\n"
        "      postgres-marquez:\n"
        "        condition: service_healthy\n"
        "    ports:\n"
        "      - \"5000:5000\"\n"
        "      - \"5001:5001\"\n"
        "    healthcheck:\n"
        "      test: [\"CMD-SHELL\", \"wget -qO- http://localhost:5000/api/v1/namespaces >/dev/null 2>&1 || exit 1\"]\n"
        "      interval: 15s\n"
        "      timeout: 5s\n"
        "      retries: 20\n"
        "      start_period: 45s\n"
        "    networks:\n"
        "      - default\n"
        # Marquez WEB UI — the browsable lineage GRAPH (port 3000). The
        # openlineage service above is the API (5000); this proxies /api/v1 to
        # it. WEB_PORT MUST be set or the image logs 'listening on port
        # undefined' and never binds.
        "  marquez-web:\n"
        f"    image: {_MARQUEZ_WEB_IMAGE}\n"
        "    container_name: udp-marquez-web\n"
        "    restart: unless-stopped\n"
        "    environment:\n"
        "      MARQUEZ_HOST: udp-openlineage\n"
        "      MARQUEZ_PORT: \"5000\"\n"
        "      WEB_PORT: \"3000\"\n"
        "    depends_on:\n"
        "      openlineage:\n"
        "        condition: service_started\n"
        "    ports:\n"
        "      - \"3000:3000\"\n"
        "    networks:\n"
        "      - default\n"
        # Hive Metastore so Fintech Compliance is multi-format too (Delta/Hudi catalogs).
        + _hms_services_yaml(env)
        + "networks:\n"
        "  default:\n"
        '    name: "${LHS_NET:-udp-net}"\n'
        "volumes:\n"
        f"  {_PG_MARQUEZ_VOLUME}: {{}}\n"
        f"  {_MYSQL_HMS_VOLUME}:\n"
    )


def _render_nessie_fragment(env: dict) -> str:
    """Render the Nessie + Trino fragment for iceberg-nessie-trino-local-v0.1.

    Two services: `nessie` on port 19120 (Iceberg REST endpoint) and
    `trino` on port 8080 (federated SQL).

    Nessie uses an in-memory version store for the pilot (no persistent
    volume — restart wipes branches/commits, which is fine for v0.1
    candidate scope). OIDC explicitly disabled so the catalog accepts
    anonymous Iceberg REST calls from Spark / Trino / StarRocks without
    token plumbing in the bootstrap script.

    Trino is added here (vs. its own fragment) because the stack-id
    dispatch produces one fragment per stack. The Trino service block
    is shared with `_render_udp_trino_fragment` and
    `_render_delta_fragment` via `_trino_service_yaml()` to avoid drift.
    """
    return (
        "# docker-compose.fragment.yml -- Nessie + Trino services for "
        "iceberg-nessie-trino-local-v0.1.\n"
        "# Generated by backend/stack_compose_fragments.py.\n"
        "#\n"
        "# REQUIRED: UDP's upstream docker-compose.yml does not ship a\n"
        "# Nessie or Trino service. This fragment supplies both so\n"
        "# `docker compose up` for this stack actually has a catalog +\n"
        "# query engine to bring up.\n"
        "services:\n"
        "  nessie:\n"
        f"    image: {_NESSIE_IMAGE}\n"
        "    container_name: udp-nessie\n"
        "    restart: unless-stopped\n"
        "    environment:\n"
        "      # IN_MEMORY is the pilot-scope default. Swap to JDBC and\n"
        "      # add a postgres-nessie service when promoting past v0.1.\n"
        "      NESSIE_VERSION_STORE_TYPE: IN_MEMORY\n"
        "      QUARKUS_OIDC_TENANT_ENABLED: \"false\"\n"
        "      # S3 credentials via standard AWS SDK v2 env vars.\n"
        "      # Nessie 0.99 schema does not have access-key-id /\n"
        "      # secret-access-key under nessie.catalog.service.s3.*\n"
        "      # so those property names cause SRCFG00050 at boot.\n"
        "      # AWS SDK v2 EnvironmentVariableCredentialsProvider picks\n"
        "      # these up before Quarkus config is consulted.\n"
        f"      AWS_ACCESS_KEY_ID: {env.get('MINIO_ROOT_USER', 'admin')}\n"
        f"      AWS_SECRET_ACCESS_KEY: {env.get('MINIO_ROOT_PASSWORD', 'udp_admin_12345')}\n"
        "      AWS_REGION: us-east-1\n"
        "    volumes:\n"
        "      - ./nessie.properties:/deployments/config/application.properties:ro\n"
        "    ports:\n"
        "      - \"19120:19120\"\n"
        "    healthcheck:\n"
        "      test: [\"CMD\", \"curl\", \"-fsS\", \"http://localhost:9000/q/health\"]\n"
        "      interval: 10s\n"
        "      timeout: 5s\n"
        "      retries: 12\n"
        "    networks:\n"
        "      - default\n"
        + _trino_service_yaml(env)
        + _airflow_services_yaml(env)
        # ADDITIVE 3-catalog feature: Spark runs the multi-format ETL and Hive
        # Metastore backs the Hudi/Delta catalogs. The Nessie/Trino/StarRocks/
        # Airflow build above is untouched. Spark reaches Nessie's iceberg REST
        # endpoint (via ETLV_ICE_URI in the smoke) for the iceberg format.
        + "  spark:\n"
        f"    image: {_SPARK_ICEBERG_IMAGE}\n"
        "    container_name: udp-spark\n"
        "    restart: unless-stopped\n"
        "    environment:\n"
        f"      AWS_ACCESS_KEY_ID: {env.get('MINIO_ROOT_USER', 'admin')}\n"
        f"      AWS_SECRET_ACCESS_KEY: {env.get('MINIO_ROOT_PASSWORD', 'udp_admin_12345')}\n"
        "      AWS_REGION: us-east-1\n"
        "    healthcheck:\n"
        "      test: [\"CMD-SHELL\", \"bash -c 'echo > /dev/tcp/localhost/8888' 2>/dev/null\"]\n"
        "      interval: 15s\n"
        "      timeout: 5s\n"
        "      retries: 12\n"
        "      start_period: 30s\n"
        "    networks:\n"
        "      - default\n"
        + _hms_services_yaml(env)
        # Rename the compose default network to a HYPHENATED name: a container's
        # reverse-PTR is `<container>.<net>`, and the default `<project>_default`
        # has an underscore (illegal URI host char) that breaks StarRocks
        # getAllDatabases against HMS. LHS_NET (set by the runner per-install)
        # resolves to `iceberg-nessie-trino-net`; the Nessie bootstrap's
        # `docker run --network` is updated to match.
        + "networks:\n"
        "  default:\n"
        '    name: "${LHS_NET:-udp-net}"\n'
        "volumes:\n"
        "  udp_airflow_postgres_data: {}\n"
        "  udp_airflow_dags: {}\n"
        "  udp_airflow_logs: {}\n"
        f"  {_MYSQL_HMS_VOLUME}:\n"
    )


def _airflow_services_yaml(env: dict) -> str:
    """Postgres + Airflow standalone for iceberg-nessie-trino-local-v0.1.

    Airflow runs in `standalone` mode (webserver + scheduler in one process)
    backed by a dedicated Postgres instance. Uses LocalExecutor — no Redis or
    Celery needed for a single-node pilot. Host port 9090 avoids collision
    with Trino on 8080.

    _AIRFLOW_WWW_USER_* env vars are read by the standalone entrypoint on
    first boot to create the admin user; subsequent boots skip user creation.
    """
    af_user = env.get("AIRFLOW_ADMIN_USER", "admin")
    af_pass = env.get("AIRFLOW_ADMIN_PASSWORD", "admin")
    return (
        "  postgres-airflow:\n"
        "    image: postgres:15-alpine\n"
        "    container_name: udp-postgres-airflow\n"
        "    restart: unless-stopped\n"
        "    environment:\n"
        "      POSTGRES_USER: airflow\n"
        "      POSTGRES_PASSWORD: airflow\n"
        "      POSTGRES_DB: airflow\n"
        "    volumes:\n"
        "      - udp_airflow_postgres_data:/var/lib/postgresql/data\n"
        "    healthcheck:\n"
        '      test: ["CMD", "pg_isready", "-U", "airflow"]\n'
        "      interval: 5s\n"
        "      timeout: 5s\n"
        "      retries: 10\n"
        "    networks:\n"
        "      - default\n"
        "  airflow:\n"
        "    image: apache/airflow:2.10.4-python3.11\n"
        "    container_name: udp-airflow\n"
        "    restart: unless-stopped\n"
        "    command: standalone\n"
        "    depends_on:\n"
        "      postgres-airflow:\n"
        "        condition: service_healthy\n"
        "    environment:\n"
        "      AIRFLOW__CORE__EXECUTOR: LocalExecutor\n"
        "      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres-airflow:5432/airflow\n"
        "      AIRFLOW__CORE__LOAD_EXAMPLES: \"false\"\n"
        "      AIRFLOW__WEBSERVER__EXPOSE_CONFIG: \"true\"\n"
        "      AIRFLOW__WEBSERVER__SECRET_KEY: lakehouse-studio-pilot\n"
        # Right-sized for a loaded dev host: the defaults (4 sync workers,
        # 120s master timeout) make gunicorn kill itself on cold start when
        # the whole lakehouse stack is booting alongside Airflow.
        "      AIRFLOW__WEBSERVER__WORKERS: \"2\"\n"
        "      AIRFLOW__WEBSERVER__WEB_SERVER_MASTER_TIMEOUT: \"300\"\n"
        "      AIRFLOW__WEBSERVER__WEB_SERVER_WORKER_TIMEOUT: \"300\"\n"
        f"      _AIRFLOW_WWW_USER_CREATE: \"true\"\n"
        f"      _AIRFLOW_WWW_USER_USERNAME: {af_user}\n"
        f"      _AIRFLOW_WWW_USER_PASSWORD: {af_pass}\n"
        "    volumes:\n"
        "      - udp_airflow_dags:/opt/airflow/dags\n"
        "      - udp_airflow_logs:/opt/airflow/logs\n"
        "    ports:\n"
        '      - "9090:8080"\n'
        "    healthcheck:\n"
        '      test: ["CMD", "curl", "-fsS", "http://localhost:8080/health"]\n'
        "      interval: 30s\n"
        "      timeout: 10s\n"
        "      retries: 10\n"
        "      start_period: 90s\n"
        "    networks:\n"
        "      - default\n"
    )


def _hms_services_yaml(env: dict) -> str:
    """Return ONLY the mysql-hms + hive-metastore service YAML blocks (indented
    under `services:`). Reused by every stack that wants a Hive Metastore so the
    multi-format (Delta/Hudi) catalog capability is identical everywhere — no
    per-stack drift. Caller supplies the `services:` header, the
    `${_MYSQL_HMS_VOLUME}` volume declaration, and the trailing networks block.
    (The bind-mounted metastore-site.xml is dropped by write_fragment.)
    """
    return (
        "  mysql-hms:\n"
        f"    image: {_MYSQL_IMAGE}\n"
        "    container_name: udp-mysql-hms\n"
        "    restart: unless-stopped\n"
        # MySQL 8 defaults to caching_sha2_password, which the old Hive
        # Metastore 3.0 JDBC driver can't negotiate over a non-SSL TCP
        # connection (localhost healthcheck passes, but HMS over the network
        # gets "Access denied"). Force native password so HMS connects.
        '    command: ["--default-authentication-plugin=mysql_native_password"]\n'
        "    environment:\n"
        "      MYSQL_DATABASE: metastore\n"
        "      MYSQL_USER: hive\n"
        "      MYSQL_PASSWORD: ${HMS_DB_PASSWORD:-hive_password_pilot}\n"
        "      MYSQL_ROOT_PASSWORD: ${HMS_DB_ROOT_PASSWORD:-root_password_pilot}\n"
        "    volumes:\n"
        f"      - {_MYSQL_HMS_VOLUME}:/var/lib/mysql\n"
        "    expose:\n"
        '      - "3306"\n'
        "    healthcheck:\n"
        '      test: ["CMD-SHELL", "mysql -h 127.0.0.1 -uhive -p$${MYSQL_PASSWORD} -D metastore -e \\"SELECT 1\\" >/dev/null"]\n'
        "      interval: 10s\n"
        "      timeout: 5s\n"
        "      retries: 30\n"
        "      start_period: 60s\n"
        "    networks:\n"
        "      - default\n"
        "  hive-metastore:\n"
        f"    image: {_HMS_IMAGE}\n"
        "    container_name: udp-hive-metastore\n"
        # Clean hostname so HMS's self-resolved canonical name is `hive-metastore`
        # not `udp-hive-metastore.<net>` — the network-name underscore is an
        # illegal URI host char and breaks StarRocks getAllDatabases.
        "    hostname: hive-metastore\n"
        "    restart: unless-stopped\n"
        "    depends_on:\n"
        "      mysql-hms:\n"
        "        condition: service_started\n"
        "    environment:\n"
        "      METASTORE_DB_HOSTNAME: mysql-hms\n"
        "    volumes:\n"
        "      - ./hive-metastore-site.xml:/opt/apache-hive-metastore-3.0.0-bin/conf/metastore-site.xml:ro\n"
        "    expose:\n"
        '      - "9083"\n'
        "    healthcheck:\n"
        '      test: ["CMD-SHELL", "bash -c \'</dev/tcp/127.0.0.1/9083\'"]\n'
        "      interval: 15s\n"
        "      timeout: 5s\n"
        "      retries: 20\n"
        "      start_period: 30s\n"
        "    networks:\n"
        "      - default\n"
    )


def _render_hms_fragment(env: dict) -> str:
    """MySQL + Hive Metastore for hudi-hms-spark + delta-hms-spark-trino.

    bitsondatadev/hive-metastore is MySQL-only by design (entrypoint
    hard-codes port 3306 + dbType mysql). MySQL JDBC driver bundled,
    no jar download needed. Operator MUST also bind-mount a
    metastore-site.xml — written by `_render_hms_site_xml()` and
    dropped into install_dir by write_fragment.
    """
    return (
        "# docker-compose.fragment.yml -- MySQL + Hive Metastore for\n"
        "# hudi-hms-spark-local-v0.1 / delta-hms-spark-trino-local-v0.1.\n"
        "# Generated by backend/stack_compose_fragments.py.\n"
        "services:\n"
        + _hms_services_yaml(env)
        + "volumes:\n"
        f"  {_MYSQL_HMS_VOLUME}:\n"
        "networks:\n"
        "  default:\n"
        # Name the compose default network explicitly with a HYPHEN so a
        # container's reverse-DNS PTR is `<container>.<net>` with no underscore.
        # The default `<project>_default` has an underscore, which is an illegal
        # URI hostname char and breaks HMS self-resolution -> StarRocks
        # getAllDatabases. Install-specific (via LHS_NET) so installs don't share.
        '    name: "${LHS_NET:-udp-net}"\n'
    )


def _render_delta_fragment(env: dict) -> str:
    """Render the HMS + Trino fragment for delta-hms-spark-trino-local-v0.1.

    Same MySQL + Hive Metastore body as `_render_hms_fragment` PLUS the
    Trino service appended before the trailing `volumes:` + `networks:`
    blocks. We can't just append to `_render_hms_fragment`'s output
    because that block emits its own trailing `volumes:` + `networks:`
    sections — the Trino service would land AFTER those, which is
    invalid YAML (services must come before top-level volumes/networks
    blocks for clean ordering, even if compose tolerates the reverse).

    Hudi-HMS-Spark uses `_render_hms_fragment` (no Trino); only the
    Delta stack needs Trino layered on top, hence the separate renderer.
    """
    return (
        "# docker-compose.fragment.yml -- MySQL + Hive Metastore + Trino\n"
        "# for delta-hms-spark-trino-local-v0.1.\n"
        "# Generated by backend/stack_compose_fragments.py.\n"
        "services:\n"
        "  mysql-hms:\n"
        f"    image: {_MYSQL_IMAGE}\n"
        "    container_name: udp-mysql-hms\n"
        "    restart: unless-stopped\n"
        # MySQL 8 defaults to caching_sha2_password, which the old Hive
        # Metastore 3.0 JDBC driver can't negotiate over a non-SSL TCP
        # connection (localhost healthcheck passes, but HMS over the network
        # gets "Access denied"). Force native password so HMS connects.
        '    command: ["--default-authentication-plugin=mysql_native_password"]\n'
        "    environment:\n"
        "      MYSQL_DATABASE: metastore\n"
        "      MYSQL_USER: hive\n"
        "      MYSQL_PASSWORD: ${HMS_DB_PASSWORD:-hive_password_pilot}\n"
        "      MYSQL_ROOT_PASSWORD: ${HMS_DB_ROOT_PASSWORD:-root_password_pilot}\n"
        "    volumes:\n"
        f"      - {_MYSQL_HMS_VOLUME}:/var/lib/mysql\n"
        "    expose:\n"
        '      - "3306"\n'
        "    healthcheck:\n"
        '      test: ["CMD-SHELL", "mysql -h 127.0.0.1 -uhive -p$${MYSQL_PASSWORD} -D metastore -e \\"SELECT 1\\" >/dev/null"]\n'
        "      interval: 10s\n"
        "      timeout: 5s\n"
        "      retries: 30\n"
        "      start_period: 60s\n"
        "    networks:\n"
        "      - default\n"
        "  hive-metastore:\n"
        f"    image: {_HMS_IMAGE}\n"
        "    container_name: udp-hive-metastore\n"
        # Clean hostname so HMS's self-resolved canonical name is `hive-metastore`
        # and not `udp-hive-metastore.<project>_default` — the underscore in the
        # compose network name is an illegal URI hostname char and breaks
        # StarRocks getAllDatabases against the metastore.
        "    hostname: hive-metastore\n"
        "    restart: unless-stopped\n"
        "    depends_on:\n"
        "      mysql-hms:\n"
        "        condition: service_started\n"
        "    environment:\n"
        "      METASTORE_DB_HOSTNAME: mysql-hms\n"
        "    volumes:\n"
        "      - ./hive-metastore-site.xml:/opt/apache-hive-metastore-3.0.0-bin/conf/metastore-site.xml:ro\n"
        "    expose:\n"
        '      - "9083"\n'
        "    healthcheck:\n"
        '      test: ["CMD-SHELL", "bash -c \'</dev/tcp/127.0.0.1/9083\'"]\n'
        "      interval: 15s\n"
        "      timeout: 5s\n"
        "      retries: 20\n"
        "      start_period: 30s\n"
        "    networks:\n"
        "      - default\n"
        + _trino_service_yaml(env)
        + "volumes:\n"
        f"  {_MYSQL_HMS_VOLUME}:\n"
        "networks:\n"
        "  default:\n"
        # Name the compose default network explicitly with a HYPHEN so a
        # container's reverse-DNS PTR is `<container>.<net>` with no underscore.
        # The default `<project>_default` has an underscore, which is an illegal
        # URI hostname char and breaks HMS self-resolution -> StarRocks
        # getAllDatabases. Install-specific (via LHS_NET) so installs don't share.
        '    name: "${LHS_NET:-udp-net}"\n'
    )


def _render_nessie_properties(env: dict) -> str:
    """Quarkus application.properties for Nessie 0.99 S3 + warehouse config.

    Bind-mounted into the Nessie container at
    /deployments/config/application.properties (Quarkus's standard
    config location, auto-loaded on boot).

    Credentials use the Nessie secrets subsystem (not direct property
    keys). Nessie 0.99 startup logs confirm: "secrets are retrieved only
    from the Quarkus configuration" when no external secrets manager is
    configured. Secret type BASIC = access-key-id (name) + secret-access-key
    (secret). The access-key.name property references the secret by name.
    """
    minio_user = env.get("MINIO_ROOT_USER", "admin")
    minio_pass = env.get("MINIO_ROOT_PASSWORD", "udp_admin_12345")
    return (
        "# Nessie 0.99 catalog S3 + warehouse config.\n"
        "# Credentials via Quarkus-config secrets (no external secrets manager).\n"
        "# auth-type=APPLICATION_GLOBAL uses AWS SDK DefaultCredentialsProvider\n"
        "# which reads AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY from the\n"
        "# nessie service env (see docker-compose.fragment.yml).\n"
        "# STATIC (the default) requires a URN secret reference — not supported\n"
        "# without an external secrets manager in pilot scope.\n"
        "nessie.catalog.service.s3.default-options.auth-type=APPLICATION_GLOBAL\n"
        "nessie.catalog.default-warehouse=warehouse\n"
        "nessie.catalog.warehouses.warehouse.location=s3://datalake/warehouse\n"
        "nessie.catalog.service.s3.default-options.endpoint=http://minio:9000\n"
        "nessie.catalog.service.s3.default-options.region=us-east-1\n"
        "nessie.catalog.service.s3.default-options.path-style-access=true\n"
    )


def _render_hms_site_xml(env: dict) -> str:
    """metastore-site.xml that bitsondatadev HMS bind-mount expects."""
    pw = env.get("HMS_DB_PASSWORD", "hive_password_pilot")
    # HMS itself creates managed database/table directories on the warehouse
    # (s3a://datalake/warehouse). Without S3 creds it gets 403 Forbidden from
    # MinIO when a Spark hive_sync / saveAsTable creates a new database. Give
    # HMS the same MinIO credentials the engines use.
    s3_ep  = env.get("S3_ENDPOINT") or "http://minio:9000"
    s3_key = env.get("MINIO_ROOT_USER") or env.get("AWS_ACCESS_KEY_ID") or "admin"
    s3_sec = env.get("MINIO_ROOT_PASSWORD") or env.get("AWS_SECRET_ACCESS_KEY") or "udp_admin_12345"
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="no"?>\n'
        '<configuration>\n'
        '  <property>\n'
        '    <name>javax.jdo.option.ConnectionURL</name>\n'
        '    <value>jdbc:mysql://mysql-hms:3306/metastore?useSSL=false&amp;allowPublicKeyRetrieval=true</value>\n'
        '  </property>\n'
        '  <property>\n'
        '    <name>javax.jdo.option.ConnectionDriverName</name>\n'
        '    <value>com.mysql.cj.jdbc.Driver</value>\n'
        '  </property>\n'
        '  <property>\n'
        '    <name>javax.jdo.option.ConnectionUserName</name>\n'
        '    <value>hive</value>\n'
        '  </property>\n'
        '  <property>\n'
        '    <name>javax.jdo.option.ConnectionPassword</name>\n'
        f'    <value>{pw}</value>\n'
        '  </property>\n'
        '  <property>\n'
        '    <name>metastore.thrift.uris</name>\n'
        '    <value>thrift://localhost:9083</value>\n'
        '  </property>\n'
        '  <property>\n'
        '    <name>metastore.task.threads.always</name>\n'
        '    <value>org.apache.hadoop.hive.metastore.events.EventCleanerTask</value>\n'
        '  </property>\n'
        '  <property>\n'
        '    <name>metastore.expression.proxy</name>\n'
        '    <value>org.apache.hadoop.hive.metastore.DefaultPartitionExpressionProxy</value>\n'
        '  </property>\n'
        '  <property>\n'
        '    <name>metastore.warehouse.dir</name>\n'
        '    <value>s3a://datalake/warehouse</value>\n'
        '  </property>\n'
        '  <property><name>fs.s3a.endpoint</name>'
        f'<value>{s3_ep}</value></property>\n'
        '  <property><name>fs.s3a.access.key</name>'
        f'<value>{s3_key}</value></property>\n'
        '  <property><name>fs.s3a.secret.key</name>'
        f'<value>{s3_sec}</value></property>\n'
        '  <property><name>fs.s3a.path.style.access</name><value>true</value></property>\n'
        '  <property><name>fs.s3a.connection.ssl.enabled</name><value>false</value></property>\n'
        '  <property><name>fs.s3a.impl</name>'
        '<value>org.apache.hadoop.fs.s3a.S3AFileSystem</value></property>\n'
        '</configuration>\n'
    )


def _render_polaris_fragment(env: dict) -> str:
    """Render the Polaris fragment.

    Two services: `postgres-polaris` (JDBC persistence backing store)
    and `polaris` (catalog REST on 8181 + management API on 8182).
    Host port 5433 for the Postgres so it doesn't conflict with HMS
    Postgres on 5432 if an operator ever runs both stacks side-by-side.
    Inside the docker network both are still on 5432 (the container
    port is unchanged), so polaris's JDBC URL points at port 5432.
    """
    return (
        "# docker-compose.fragment.yml -- Polaris + backing Postgres for\n"
        "# iceberg-polaris-spark-local-v0.1.\n"
        "# Generated by backend/stack_compose_fragments.py.\n"
        "#\n"
        "# REQUIRED: UDP's upstream docker-compose.yml does not ship\n"
        "# Polaris or its backing Postgres. This fragment supplies both\n"
        "# so the Polaris-backed stack actually has a catalog to bring up.\n"
        "#\n"
        "# Host port 5433 for the Postgres — 5432 may be held by the HMS\n"
        "# Postgres if an operator runs both stacks on the same host. The\n"
        "# CONTAINER side stays on 5432, so polaris's JDBC URL uses 5432.\n"
        "services:\n"
        "  postgres-polaris:\n"
        f"    image: {_POSTGRES_IMAGE}\n"
        "    container_name: udp-postgres-polaris\n"
        "    restart: unless-stopped\n"
        "    environment:\n"
        "      POSTGRES_USER: polaris\n"
        "      POSTGRES_PASSWORD: ${POLARIS_DB_PASSWORD:-polaris_password_pilot}\n"
        "      POSTGRES_DB: polaris\n"
        "    volumes:\n"
        f"      - {_PG_POLARIS_VOLUME}:/var/lib/postgresql/data\n"
        # Bug fix 2026-05-17 VPS install: don't bind host port (same
        # rationale as postgres-hms — Polaris only reaches its backing
        # DB over the docker network; no host port needed).
        "    expose:\n"
        "      - \"5432\"\n"
        "    healthcheck:\n"
        "      test: [\"CMD\", \"pg_isready\", \"-U\", \"polaris\", \"-d\", \"polaris\"]\n"
        "      interval: 10s\n"
        "      timeout: 5s\n"
        "      retries: 10\n"
        "    networks:\n"
        "      - default\n"
        "  polaris:\n"
        f"    image: {_POLARIS_IMAGE}\n"
        "    container_name: udp-polaris\n"
        "    restart: unless-stopped\n"
        "    depends_on:\n"
        "      postgres-polaris:\n"
        "        condition: service_healthy\n"
        "    environment:\n"
        # Codex P0 fix 2026-05-17: Polaris 1.4.x uses Quarkus datasource
        # env vars + the persistence type is `relational-jdbc`, not `jdbc`.
        # The bootstrap script also expects POLARIS_BOOTSTRAP_CREDENTIALS
        # to seed the realm's root principal credential — without it the
        # script's token request to /api/catalog/v1/oauth/tokens 401s.
        "      POLARIS_PERSISTENCE_TYPE: relational-jdbc\n"
        "      QUARKUS_DATASOURCE_DB_KIND: postgresql\n"
        "      QUARKUS_DATASOURCE_JDBC_URL: jdbc:postgresql://postgres-polaris:5432/polaris?sslmode=disable\n"
        "      QUARKUS_DATASOURCE_USERNAME: polaris\n"
        "      QUARKUS_DATASOURCE_PASSWORD: ${POLARIS_DB_PASSWORD:-polaris_password_pilot}\n"
        "      # Bootstrap credential the runner_extra_scripts polaris\n"
        "      # bootstrap uses to obtain the first OAuth2 token.\n"
        "      # Gemini research 2026-05-17: Polaris 1.4.x expects a TRIPLE\n"
        "      # comma-separated `realm,clientId,clientSecret` -- NOT\n"
        "      # `realm,clientId:clientSecret` (colon between id+secret is\n"
        "      # wrong and causes the bootstrap to skip seeding the root\n"
        "      # principal, leading to 401 on /api/catalog/v1/oauth/tokens).\n"
        "      # Ref: https://github.com/apache/polaris/issues/348\n"
        "      POLARIS_BOOTSTRAP_CREDENTIALS: ${POLARIS_BOOTSTRAP_CREDENTIALS:-default-realm,root,s3cr3t}\n"
        "      # Gemini Polaris 1.4.1 audit 2026-05-17: without an explicit\n"
        "      # realm-context realms list, Polaris 1.4.1 defaults to a\n"
        "      # different realm name and the bootstrap credentials above\n"
        "      # never apply -- the root principal is seeded into a realm\n"
        "      # nobody talks to, so the bootstrap script's token request\n"
        "      # 401s. Pin the realm explicitly so realm IDs line up across\n"
        "      # bootstrap, Spark, and StarRocks.\n"
        "      # Ref: https://polaris.apache.org/docs/configuration/#bootstrapping\n"
        "      POLARIS_REALM_CONTEXT_REALMS: default-realm\n"
        "    ports:\n"
        "      - \"8181:8181\"\n"
        "      - \"8182:8182\"\n"
        "    healthcheck:\n"
        # Live VPS debug 2026-05-17 (inst_d58762cb19 + inst_945d4eca29):
        # Polaris container logs showed `GET /q/health` returning 404 in
        # a tight loop -- that path isn't exposed by the Polaris 1.4.x
        # image (Quarkus health is on the management port but auth-gated
        # in this build). Switch to a pure TCP probe: if the catalog
        # port is accepting connections, the JVM is up. Avoids the
        # 401-loop noise from auth-gated HTTP probes.
        "      test: [\"CMD-SHELL\", \"bash -c 'echo > /dev/tcp/127.0.0.1/8181' || exit 1\"]\n"
        "      interval: 10s\n"
        "      timeout: 5s\n"
        "      retries: 30\n"
        "      start_period: 60s\n"
        "    networks:\n"
        "      - default\n"
        "volumes:\n"
        f"  {_PG_POLARIS_VOLUME}:\n"
        # Codex P0 fix 2026-05-17: the fragment used to declare
        # `default: { external: true }` which only works if the network
        # is pre-created. With docker compose -f base -f fragment, the
        # base compose creates `default` on first start and our fragment
        # joins by reference. No `external:` and no explicit `name:`
        # needed — let compose's merge logic do the work.
        "networks:\n"
        "  default:\n"
        # Name the compose default network explicitly with a HYPHEN so a
        # container's reverse-DNS PTR is `<container>.<net>` with no underscore.
        # The default `<project>_default` has an underscore, which is an illegal
        # URI hostname char and breaks HMS self-resolution -> StarRocks
        # getAllDatabases. Install-specific (via LHS_NET) so installs don't share.
        '    name: "${LHS_NET:-udp-net}"\n'
    )


def _network_name_token(env: dict) -> str:
    """Resolve the docker network name from the env dict alone.

    Mirrors `_network_name` but without an install_dir — we can't always
    know it at render time (tests render directly). When the env carries
    `LHS_DOCKER_NETWORK`, honor it; otherwise default to `udp_default`,
    which docker compose auto-creates for a directory literally named
    `udp`. The runner's call path passes the real network name in via
    env when needed; tests rely on the default.
    """
    explicit = (env.get("LHS_DOCKER_NETWORK") or "").strip()
    if explicit:
        return explicit
    return env.get("LHS_BASE_NETWORK_NAME") or "udp_default"


def _render_superset_fragment(env: dict) -> str:
    """Superset for the startup-analytics-local-v0.1 stack.

    Single-container Superset with SQLite backend — no extra Postgres or
    Redis needed for pilot scope. Bootstrap initialises the DB, creates
    the admin user, and runs superset init. Port 8088 on the host.
    """
    secret = env.get("SUPERSET_SECRET_KEY", "lakehouse-studio-pilot")
    return (
        "# docker-compose.fragment.yml -- Superset for udp-local-v0.2.\n"
        "# Generated by backend/stack_compose_fragments.py.\n"
        "services:\n"
        "  superset:\n"
        "    image: apache/superset:4.1.1\n"
        "    container_name: udp-superset\n"
        "    restart: unless-stopped\n"
        "    environment:\n"
        f"      SUPERSET_SECRET_KEY: {secret}\n"
        "      SUPERSET_LOAD_EXAMPLES: \"false\"\n"
        "      SUPERSET_WEBSERVER_PORT: \"8088\"\n"
        "    volumes:\n"
        "      - udp_superset_home:/app/superset_home\n"
        "    ports:\n"
        '      - "8088:8088"\n'
        "    healthcheck:\n"
        '      test: ["CMD", "curl", "-fsS", "http://localhost:8088/health"]\n'
        "      interval: 30s\n"
        "      timeout: 10s\n"
        "      retries: 10\n"
        "      start_period: 120s\n"
        "    networks:\n"
        "      - default\n"
        # Hive Metastore so Startup Analytics is multi-format too: choosing
        # Delta/Hudi registers a delta_catalog/hudi_catalog in StarRocks.
        + _hms_services_yaml(env)
        + "networks:\n"
        "  default:\n"
        '    name: "${LHS_NET:-udp-net}"\n'
        "volumes:\n"
        "  udp_superset_home: {}\n"
        f"  {_MYSQL_HMS_VOLUME}:\n"
    )


# ---------- dispatch ----------


_FRAGMENT_RENDERERS: dict[str, Callable[[dict], str]] = {
    "udp-local-v0.2":                    _render_hms_fragment,
    "startup-analytics-local-v0.1":      _render_superset_fragment,
    "ai-ml-research-local-v0.1":         _render_ai_ml_fragment,
    "udp-trino-local-v0.1":              _render_udp_trino_fragment,
    "fintech-compliance-local-v0.1":     _render_fintech_fragment,
    "iceberg-nessie-trino-local-v0.1":  _render_nessie_fragment,
    "hudi-hms-spark-local-v0.1":        _render_hms_fragment,
    "delta-hms-spark-trino-local-v0.1": _render_delta_fragment,
    "iceberg-polaris-spark-local-v0.1": _render_polaris_fragment,
}


# ---------- public API ----------


def write_fragment(stack_id: str, install_dir: Path,
                   env: dict[str, str]) -> Optional[Path]:
    """Write the per-stack compose fragment into install_dir.

    Returns the Path to the written `docker-compose.fragment.yml`, or
    `None` if the given `stack_id` has no fragment registered (the
    stable `udp-local-v0.2` stack hits this branch — UDP's upstream
    compose already has every service it needs).

    Idempotent — re-running with the same install_dir overwrites the
    file atomically. Safe to call from `runner._step_env` every install.

    The env dict is the merged install env (manifest defaults + user
    overrides). We pass it through to the renderer so the network name
    can be honored via `LHS_DOCKER_NETWORK` / `LHS_BASE_NETWORK_NAME`.
    For the install path, the renderer's `_network_name_token` default
    of `udp_default` is then OVERRIDDEN by the per-install network the
    runner injects through env.
    """
    renderer = _FRAGMENT_RENDERERS.get(stack_id)
    if renderer is None:
        log.debug("no fragment renderer for stack_id=%r", stack_id)
        return None

    # If the env doesn't already carry an explicit network name, fall
    # back to the install_dir-derived default so the fragment attaches
    # to the right network for THIS install rather than a generic one.
    enriched = dict(env or {})
    if not (enriched.get("LHS_DOCKER_NETWORK") or "").strip():
        enriched["LHS_BASE_NETWORK_NAME"] = _network_name(install_dir, enriched)

    body = renderer(enriched)
    path = install_dir / FRAGMENT_FILENAME
    _atomic_write(path, body)
    # For HMS-using stacks, also drop a metastore-site.xml alongside the
    # fragment — bind-mounted into the HMS container so its entrypoint
    # picks up the right JDBC URL. Refactored 2026-05-17 after VPS
    # install attempts 2-6 fought the image's MySQL-only assumption.
    if stack_id in ("hudi-hms-spark-local-v0.1", "delta-hms-spark-trino-local-v0.1",
                    "udp-local-v0.2", "startup-analytics-local-v0.1",
                    "ai-ml-research-local-v0.1", "fintech-compliance-local-v0.1",
                    "udp-trino-local-v0.1", "iceberg-nessie-trino-local-v0.1"):
        site_path = install_dir / "hive-metastore-site.xml"
        _atomic_write(site_path, _render_hms_site_xml(enriched))
        log.info("wrote hive-metastore-site.xml for stack=%s", stack_id)
    # 2026-05-17: Nessie 0.99 SmallRye env-var reverse-mapping doesn't
    # resolve dotted-hyphenated secret-config names reliably. Drop a
    # canonical Quarkus application.properties next to the fragment so
    # the compose bind-mount picks up the real S3 + warehouse config.
    if stack_id == "iceberg-nessie-trino-local-v0.1":
        props_path = install_dir / "nessie.properties"
        _atomic_write(props_path, _render_nessie_properties(enriched))
        log.info("wrote nessie.properties for stack=%s", stack_id)
    log.info(
        "stack compose fragment written stack_id=%s install_dir=%s",
        stack_id, install_dir,
    )
    return path
