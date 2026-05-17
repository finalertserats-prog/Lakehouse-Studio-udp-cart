"""Dagster orchestration sidecar — opt-in compose override.

PURE ADDITIVE MODULE. Mirrors the shape of `airflow_overlay.py`,
`caddy_tls.py`, `monitoring.py`, and `jdbc_extras.py`:

  - NEVER touches the base `docker-compose.yml` that
    `runner._patch_compose_images()` writes (FROZEN — certified-stack
    contract).
  - Writes a sibling `docker-compose.dagster.yml` the operator opts into
    via the env flag `LHS_DAGSTER_ENABLED=true` (default OFF). runner.py
    consults that flag during the env step and appends our override file
    via `-f` so compose merges it with the base stack.
  - All sensitive values (postgres password) come from env vars with safe
    placeholder defaults that log a LOUD warning if left unchanged.

Services defined in the override:

  - dagster-postgres   postgres:15-alpine, named volume `dagster-pgdata`,
                       healthcheck via pg_isready. Backing store for the
                       runs / events / schedules tables.
  - dagster-webserver  dagster/dagster-celery-docker:1.9.4, host port
                       3000 mapped to container 3000. Runs
                       `dagster-webserver -h 0.0.0.0 -p 3000`.
  - dagster-daemon     same image, no host port, runs `dagster-daemon
                       run`. Schedules + sensors require the daemon —
                       without it the webserver is read-only.

Network: external `<install_dir.name>_default` (overridable via
LHS_DOCKER_NETWORK), same pattern as airflow_overlay so spark-iceberg /
starrocks-fe resolve by name.

Project layout: we also drop
  install_dir/dagster_project/definitions.py   — minimal asset
  install_dir/dagster_project/dagster.yaml     — instance config

…and mount both into the containers. DAGSTER_HOME points at
/opt/dagster/dagster_home which the override creates as a tmpfs / volume
mount, with dagster.yaml symlinked / copied in via the bind mount.

Image tags pinned 2026-05-16:
  - dagster/dagster-celery-docker:1.9.4 — official Dagster image, latest
    1.9.x at time of write. Includes dagster-webserver + dagster-daemon
    + the Postgres storage extension we need. Multi-arch.
  - postgres:15-alpine — same as Airflow's metadata DB, keeps the image
    set small.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional


log = logging.getLogger("lhs.dagster_overlay")


# ---- pinned image tags (verified 2026-05-16) ----
_DAGSTER_IMAGE = "dagster/dagster-celery-docker:1.9.4"
_POSTGRES_IMAGE = "postgres:15-alpine"

# Filename the runner appends with `-f` when LHS_DAGSTER_ENABLED is true.
# Exported at module level so runner.py can import + use without parsing.
OVERLAY_FILENAME = "docker-compose.dagster.yml"

# Env flag the runner checks before calling write_dagster_overlay().
ENV_FLAG = "LHS_DAGSTER_ENABLED"

# Service names this overlay adds. Runner appends these to the
# explicit `docker compose up -d <services>` argv so they spin up
# alongside the base stack's services.
SERVICES = ["dagster-postgres", "dagster-webserver", "dagster-daemon"]

# Named volume holding the Dagster metadata Postgres data.
_PG_VOLUME = "dagster-pgdata"

# Host-side port mapping for the webserver. 3000 is Dagster's documented
# default and rarely collides with the lakehouse stack (StarRocks FE at
# 8030, Iceberg REST at 8181, Spark Jupyter at 8888, MinIO console at
# 9001) — leaving it at 3000 keeps the tutorial copy-paste flow working.
_WEB_HOST_PORT = 3000
_WEB_CONTAINER_PORT = 3000

# Subdirectory holding the Dagster project (definitions.py + dagster.yaml).
# Mounted into both webserver + daemon at /opt/dagster/app.
_PROJECT_SUBDIR = "dagster_project"
_PROJECT_CONTAINER_PATH = "/opt/dagster/app"

# DAGSTER_HOME is where the instance keeps its dagster.yaml and any
# transient artifacts. We point it at /opt/dagster/dagster_home and bind
# the dagster.yaml into it.
_DAGSTER_HOME = "/opt/dagster/dagster_home"

# Placeholder secret. We do NOT generate a random per-install value —
# the operator should set this in .env and we surface a loud warning at
# write time if they don't.
_DEFAULT_PG_PASSWORD = "CHANGE_ME_dagster_pg"  # noqa: S105 — explicit placeholder


# ---------- helpers ----------


def _atomic_write(path: Path, data: str) -> None:
    """tmp + os.replace atomic write. Same pattern as the sibling overlays."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(data, encoding="utf-8")
    os.replace(tmp, path)


def _network_name(install_dir: Path, env: dict) -> str:
    """Resolve the docker network name. Same logic as airflow_overlay."""
    explicit = env.get("LHS_DOCKER_NETWORK", "").strip()
    if explicit:
        return explicit
    return f"{install_dir.name}_default"


def _resolve_secret(env: dict, key: str, default: str) -> tuple[str, bool]:
    raw = env.get(key)
    if raw is None or raw.strip() == "":
        return default, True
    return raw, False


def _warn_default(key: str, hint: str) -> None:
    log.warning(
        "DAGSTER OVERLAY: %s is using the default placeholder value. %s",
        key, hint,
    )


# ---------- compose override rendering ----------


def _render_overlay(network: str, pg_password: str) -> str:
    """Render the docker-compose.dagster.yml content.

    Three services + one named volume + one external network. Compose v2
    syntax (no `version:` key). The webserver and daemon share IDENTICAL
    env + mounts — the only thing that differs is the `command:` so each
    process knows what to run.
    """
    # Shared env block. DAGSTER_PG_* are read by dagster.yaml below to
    # build the postgres connection string at runtime — keeps secrets out
    # of the on-disk dagster.yaml.
    common_env = (
        f"      DAGSTER_HOME: {_DAGSTER_HOME}\n"
        "      DAGSTER_PG_HOST: dagster-postgres\n"
        "      DAGSTER_PG_PORT: \"5432\"\n"
        "      DAGSTER_PG_USERNAME: dagster\n"
        f"      DAGSTER_PG_PASSWORD: \"{pg_password}\"\n"
        "      DAGSTER_PG_DB: dagster\n"
    )

    # Shared volumes. The dagster.yaml on disk is bind-mounted into
    # DAGSTER_HOME so the instance picks up Postgres config on boot.
    common_volumes = (
        "    volumes:\n"
        f"      - ./{_PROJECT_SUBDIR}:{_PROJECT_CONTAINER_PATH}\n"
        f"      - ./{_PROJECT_SUBDIR}/dagster.yaml:{_DAGSTER_HOME}/dagster.yaml:ro\n"
    )

    return (
        "# docker-compose.dagster.yml -- Dagster orchestration overlay.\n"
        "# Generated by backend/dagster_overlay.py.\n"
        "#\n"
        f"# Activated automatically when {ENV_FLAG}=true is set in the\n"
        "# install's .env. runner.py appends this file via `-f` so compose\n"
        "# merges it with the base stack.\n"
        "#\n"
        "# This file is an OVERRIDE. The base docker-compose.yml is FROZEN\n"
        "# (certified-stack contract). Dagster is an operational extra, NOT\n"
        "# part of the certified compatibility lock.\n"
        "services:\n"
        "  dagster-postgres:\n"
        f"    image: {_POSTGRES_IMAGE}\n"
        "    container_name: lhs-dagster-postgres\n"
        "    restart: unless-stopped\n"
        "    environment:\n"
        "      POSTGRES_USER: dagster\n"
        f"      POSTGRES_PASSWORD: \"{pg_password}\"\n"
        "      POSTGRES_DB: dagster\n"
        "    volumes:\n"
        f"      - {_PG_VOLUME}:/var/lib/postgresql/data\n"
        "    healthcheck:\n"
        "      test: [\"CMD\", \"pg_isready\", \"-U\", \"dagster\"]\n"
        "      interval: 10s\n"
        "      timeout: 5s\n"
        "      retries: 5\n"
        "    networks:\n"
        "      - default\n"
        "  dagster-webserver:\n"
        f"    image: {_DAGSTER_IMAGE}\n"
        "    container_name: lhs-dagster-webserver\n"
        "    restart: unless-stopped\n"
        "    depends_on:\n"
        "      dagster-postgres:\n"
        "        condition: service_healthy\n"
        "    command: [\"dagster-webserver\", \"-h\", \"0.0.0.0\", \"-p\", \"3000\",\n"
        f"             \"-w\", \"{_PROJECT_CONTAINER_PATH}/workspace.yaml\"]\n"
        "    environment:\n"
        + common_env +
        "    ports:\n"
        f"      - \"{_WEB_HOST_PORT}:{_WEB_CONTAINER_PORT}\"\n"
        + common_volumes +
        "    networks:\n"
        "      - default\n"
        "  dagster-daemon:\n"
        f"    image: {_DAGSTER_IMAGE}\n"
        "    container_name: lhs-dagster-daemon\n"
        "    restart: unless-stopped\n"
        "    depends_on:\n"
        "      dagster-postgres:\n"
        "        condition: service_healthy\n"
        "    command: [\"dagster-daemon\", \"run\"]\n"
        "    environment:\n"
        + common_env +
        common_volumes +
        "    networks:\n"
        "      - default\n"
        "volumes:\n"
        f"  {_PG_VOLUME}:\n"
        "networks:\n"
        "  default:\n"
        f"    name: {network}\n"
        "    external: true\n"
    )


def _render_dagster_yaml() -> str:
    """Render the Dagster instance config that backs the storage onto our
    sidecar Postgres. Values come from env vars so secrets stay out of
    this on-disk file (DAGSTER_PG_PASSWORD is interpolated at boot by
    Dagster's yaml env: substitution)."""
    return """# Auto-generated by Lakehouse Studio dagster overlay.
# Edit freely — this file is NOT regenerated unless the overlay is rewritten.
#
# Storage is backed by the sidecar postgres so runs / events / schedules
# survive container restarts. Connection params come from env vars set on
# both the webserver and daemon containers.

storage:
  postgres:
    postgres_db:
      hostname:
        env: DAGSTER_PG_HOST
      username:
        env: DAGSTER_PG_USERNAME
      password:
        env: DAGSTER_PG_PASSWORD
      db_name:
        env: DAGSTER_PG_DB
      port:
        env: DAGSTER_PG_PORT

run_coordinator:
  module: dagster.core.run_coordinator
  class: QueuedRunCoordinator
  config:
    max_concurrent_runs: 10

# Local run launcher — runs each job in a subprocess on the webserver
# container. Sufficient for the single-host v0.4 use case; swap to
# DockerRunLauncher later if the operator scales horizontally.
run_launcher:
  module: dagster.core.launcher
  class: DefaultRunLauncher
"""


def _render_workspace_yaml() -> str:
    """Render the workspace.yaml the webserver loads to discover the
    user's Definitions object."""
    return """# Auto-generated by Lakehouse Studio dagster overlay.
# Points the webserver at definitions.py in this folder.
load_from:
  - python_file:
      relative_path: definitions.py
      working_directory: /opt/dagster/app
"""


def _render_definitions_py() -> str:
    """Render a minimal Definitions object with one asset that calls into
    the lakehouse via `docker exec spark-iceberg spark-sql`. Same shape
    as the Airflow demo DAG — we use a subprocess call rather than the
    dagster-spark integration to keep the image surface area small."""
    return '''"""Lakehouse Studio Dagster project.

Generated by backend/dagster_overlay.py.

A single asset that materializes by running a spark-sql query against the
lakehouse via `docker exec spark-iceberg`. This avoids needing the
dagster-spark integration baked into the image — the spark-iceberg
container already has spark-sql on PATH.

The asset is intentionally tiny so first-time operators see one green
node in the asset graph after the first materialization. Replace freely
with real assets that target your own Iceberg tables.
"""
from __future__ import annotations

import subprocess

from dagster import Definitions, asset, AssetExecutionContext, MaterializeResult


@asset(
    group_name="lakehouse_studio_demo",
    description=(
        "Demo asset: runs `SHOW DATABASES IN udp` against the Iceberg REST "
        "catalog via the spark-iceberg container. Proves the wiring is live."
    ),
)
def lhs_demo_smoke(context: AssetExecutionContext) -> MaterializeResult:
    cmd = [
        "docker", "exec", "spark-iceberg",
        "spark-sql",
        "--conf", "spark.sql.catalog.udp.type=rest",
        "-e", "SHOW DATABASES IN udp;",
    ]
    context.log.info("running: %s", " ".join(cmd))
    result = subprocess.run(
        cmd, capture_output=True, text=True, timeout=120, check=False
    )
    if result.returncode != 0:
        context.log.error("stderr: %s", result.stderr)
        raise RuntimeError(f"spark-sql exited {result.returncode}")
    return MaterializeResult(
        metadata={
            "stdout_preview": result.stdout[:2000],
            "exit_code": result.returncode,
        }
    )


defs = Definitions(assets=[lhs_demo_smoke])
'''


# ---------- validation ----------


def validate_overlay(install_dir: Path) -> list[str]:
    """Return a list of human-readable problems for /healthz.

    Empty list = healthy / not enabled. Static-file consistency only;
    container health is docker's job.
    """
    problems: list[str] = []
    overlay = install_dir / OVERLAY_FILENAME
    if not overlay.exists():
        return problems

    project_dir = install_dir / _PROJECT_SUBDIR
    if not project_dir.exists():
        problems.append(
            f"dagster overlay present but {project_dir} missing — "
            "webserver will fail to start (no workspace)"
        )
        return problems

    defs_py = project_dir / "definitions.py"
    if not defs_py.exists():
        problems.append(f"dagster project missing {defs_py.name}")
    else:
        try:
            compile(defs_py.read_text(encoding="utf-8"), str(defs_py), "exec")
        except SyntaxError as e:
            problems.append(f"definitions.py has a syntax error: {e}")

    for required in ("dagster.yaml", "workspace.yaml"):
        if not (project_dir / required).exists():
            problems.append(f"dagster project missing {required}")

    return problems


# ---------- public API ----------


def write_dagster_overlay(install_dir: Path, env: dict) -> Optional[Path]:
    """Write the Dagster overlay + project files into install_dir.

    Returns the Path to the written docker-compose.dagster.yml, or None
    if the env flag is OFF.

    Idempotent — re-running overwrites every generated file. Safe to call
    from `runner._step_env` every install.
    """
    flag = env.get(ENV_FLAG, "").strip().lower()
    if flag not in ("1", "true", "yes", "on"):
        log.debug("dagster overlay disabled (%s=%r)", ENV_FLAG, flag)
        return None

    network = _network_name(install_dir, env)
    pg_password, pw_is_default = _resolve_secret(
        env, "DAGSTER_PG_PASSWORD", _DEFAULT_PG_PASSWORD)

    if pw_is_default:
        _warn_default(
            "DAGSTER_PG_PASSWORD",
            "Set it in the install's .env before exposing port 3000 to anything beyond localhost.",
        )

    project_dir = install_dir / _PROJECT_SUBDIR

    # Write project files FIRST so the webserver never starts pointed at
    # a missing workspace.yaml / definitions.py.
    _atomic_write(project_dir / "definitions.py", _render_definitions_py())
    _atomic_write(project_dir / "workspace.yaml", _render_workspace_yaml())
    _atomic_write(project_dir / "dagster.yaml", _render_dagster_yaml())

    overlay_path = install_dir / OVERLAY_FILENAME
    overlay_body = _render_overlay(network, pg_password)
    _atomic_write(overlay_path, overlay_body)

    log.info(
        "dagster overlay written install_dir=%s network=%s",
        install_dir, network,
    )
    return overlay_path
