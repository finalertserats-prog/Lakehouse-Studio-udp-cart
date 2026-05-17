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
_POLARIS_IMAGE = "apache/polaris:1.4.1"  # bumped from 1.0.1 — that tag never published; 1.4.1 is real + has CVE fixes (Gemini research 2026-05-17, verified via docker manifest inspect)
_POSTGRES_IMAGE = "postgres:15-alpine"
_MYSQL_IMAGE = "mysql:8.0"


# Filename the runner injects with `-f` when a fragment is needed.
# Exported so runner.py and tests can import it without parsing.
FRAGMENT_FILENAME = "docker-compose.fragment.yml"


# Service names each fragment adds. The runner appends these to the
# explicit `docker compose up -d <services>` argv so the fragment's
# services come up alongside the base stack's services rather than
# being filtered out by the runner's per-cart service list.
FRAGMENT_SERVICES: dict[str, list[str]] = {
    "iceberg-nessie-trino-local-v0.1":  ["nessie"],
    "hudi-hms-spark-local-v0.1":        ["mysql-hms", "hive-metastore"],
    "delta-hms-spark-trino-local-v0.1": ["mysql-hms", "hive-metastore"],
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


def _render_nessie_fragment(env: dict) -> str:
    """Render the Nessie fragment.

    Single service: `nessie` on port 19120, in-memory version store for
    the pilot (no persistent volume — restart wipes branches/commits,
    which is fine for v0.1 candidate scope). OIDC explicitly disabled
    so the catalog accepts anonymous Iceberg REST calls from Spark /
    Trino / StarRocks without token plumbing in the bootstrap script.
    """
    return (
        "# docker-compose.fragment.yml -- Nessie service for "
        "iceberg-nessie-trino-local-v0.1.\n"
        "# Generated by backend/stack_compose_fragments.py.\n"
        "#\n"
        "# REQUIRED: UDP's upstream docker-compose.yml does not ship a\n"
        "# Nessie service. This fragment supplies it so `docker compose\n"
        "# up` for this stack actually has a catalog to bring up.\n"
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
        "    ports:\n"
        "      - \"19120:19120\"\n"
        "    healthcheck:\n"
        "      test: [\"CMD\", \"curl\", \"-fsS\", \"http://localhost:19120/q/health\"]\n"
        "      interval: 10s\n"
        "      timeout: 5s\n"
        "      retries: 12\n"
        "    networks:\n"
        "      - default\n"
        # Codex P0 fix 2026-05-17: the fragment used to declare
        # `default: { external: true }` which only works if the network
        # is pre-created. With docker compose -f base -f fragment, the
        # base compose creates `default` on first start and our fragment
        # joins by reference. No `external:` and no explicit `name:`
        # needed — let compose's merge logic do the work.
        "networks:\n"
        "  default: {}\n"
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
        "  mysql-hms:\n"
        f"    image: {_MYSQL_IMAGE}\n"
        "    container_name: udp-mysql-hms\n"
        "    restart: unless-stopped\n"
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
        # Codex review 2026-05-17: prefer checking the HMS user + db over
        # bare root ping — this proves init scripts ran AND user/grants
        # are in place AND the metastore db exists. Without this stricter
        # check, mysqladmin ping can go green during the MySQL official
        # image's TWO-PHASE init (temp startup then final startup), and
        # the dependent HMS container then crashes against an incomplete
        # MySQL. start_period 60s is generous enough for the cold-volume
        # init on a stock VPS.
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
        "    restart: unless-stopped\n"
        "    depends_on:\n"
        # 2026-05-17 fix: was `condition: service_healthy` which compose v5
        # enforces synchronously at up-time. MySQL's first-init takes
        # 30-60s for permission setup; compose timed out at ~4s waiting.
        # HMS's own entrypoint has its own `nc -z $METASTORE_DB_HOSTNAME
        # 3306` wait loop, so service_started is sufficient — HMS will
        # block-loop until MySQL is reachable, then schematool runs.
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
        "volumes:\n"
        f"  {_MYSQL_HMS_VOLUME}:\n"
        "networks:\n"
        "  default: {}\n"
    )


def _render_hms_site_xml(env: dict) -> str:
    """metastore-site.xml that bitsondatadev HMS bind-mount expects."""
    pw = env.get("HMS_DB_PASSWORD", "hive_password_pilot")
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
        "      QUARKUS_DATASOURCE_JDBC_URL: jdbc:postgresql://postgres-polaris:5432/polaris\n"
        "      QUARKUS_DATASOURCE_USERNAME: polaris\n"
        "      QUARKUS_DATASOURCE_PASSWORD: ${POLARIS_DB_PASSWORD:-polaris_password_pilot}\n"
        "      # Bootstrap credential the runner_extra_scripts polaris\n"
        "      # bootstrap uses to obtain the first OAuth2 token. Format\n"
        "      # is `realm,client_id:client_secret`.\n"
        "      POLARIS_BOOTSTRAP_CREDENTIALS: ${POLARIS_BOOTSTRAP_CREDENTIALS:-default-realm,root:s3cr3t}\n"
        "    ports:\n"
        "      - \"8181:8181\"\n"
        "      - \"8182:8182\"\n"
        "    healthcheck:\n"
        "      test: [\"CMD\", \"curl\", \"-fsS\", \"http://localhost:8182/q/health\"]\n"
        "      interval: 15s\n"
        "      timeout: 5s\n"
        "      retries: 12\n"
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
        "  default: {}\n"
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


# ---------- dispatch ----------


_FRAGMENT_RENDERERS: dict[str, Callable[[dict], str]] = {
    "iceberg-nessie-trino-local-v0.1":  _render_nessie_fragment,
    "hudi-hms-spark-local-v0.1":        _render_hms_fragment,
    "delta-hms-spark-trino-local-v0.1": _render_hms_fragment,
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
    if stack_id in ("hudi-hms-spark-local-v0.1", "delta-hms-spark-trino-local-v0.1"):
        site_path = install_dir / "hive-metastore-site.xml"
        _atomic_write(site_path, _render_hms_site_xml(enriched))
        log.info("wrote hive-metastore-site.xml for stack=%s", stack_id)
    log.info(
        "stack compose fragment written stack_id=%s install_dir=%s",
        stack_id, install_dir,
    )
    return path
