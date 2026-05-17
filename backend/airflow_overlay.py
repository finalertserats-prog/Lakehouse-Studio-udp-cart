"""Airflow orchestration sidecar — opt-in compose override.

PURE ADDITIVE MODULE. Same shape as `caddy_tls.py`, `monitoring.py`, and
`jdbc_extras.py`:

  - NEVER touches the base `docker-compose.yml` that
    `runner._patch_compose_images()` writes (FROZEN — certified-stack
    contract).
  - Writes a sibling `docker-compose.airflow.yml` the operator opts into
    via the env flag `LHS_AIRFLOW_ENABLED=true` (default OFF). runner.py
    consults that flag during the env step and appends our override file
    via `-f` so compose merges it with the base stack.
  - All sensitive values (admin password, fernet key) come from env vars
    with safe placeholder defaults that log a LOUD warning if left
    unchanged. We never persist a generated secret to disk outside the
    operator's own .env / secret store.

Services defined in the override:

  - airflow-postgres   postgres:15-alpine, named volume `airflow-pgdata`,
                       healthcheck via pg_isready
  - airflow-init       apache/airflow:2.10.4-python3.11 one-shot; runs
                       `airflow db migrate` + `airflow users create` for
                       the admin account, then exits 0
  - airflow-webserver  apache/airflow:2.10.4-python3.11, host port 8088
                       mapped to container 8080 (Airflow's UI port).
                       depends_on airflow-init service_completed_successfully
  - airflow-scheduler  same image, no host port, depends_on airflow-init

Network: joins the base stack's docker network via an `external: true`
declaration. The network name defaults to `<install_dir.name>_default`
(docker compose's auto-naming convention) but can be overridden via env.
We declare it external so this override does NOT create a second network
— upstream resolution (spark-iceberg, starrocks-fe, etc) requires sharing
the base stack's network.

Demo DAG: we also drop `install_dir/dags/lhs_demo_dag.py`, a single-task
daily-scheduled DAG that does NOT auto-run (catchup=False, start_date
pinned to the future via `days_ago(1)` only triggers on operator-initiated
run). It uses BashOperator to `docker exec spark-iceberg spark-submit`
against the lakehouse — keeps the dependency surface tiny (no need to bake
the SparkSubmitOperator's provider into a custom image).

Image tag pinned 2026-05-16:
  - apache/airflow:2.10.4-python3.11 — multi-arch (amd64 + arm64), latest
    stable 2.10.x at time of write. DO NOT use `:latest` — Airflow 3.x
    will introduce config breaks and we want override files to remain
    valid.
  - postgres:15-alpine — matches the version Airflow's docs recommend for
    2.10.x metadata DB.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional


log = logging.getLogger("lhs.airflow_overlay")


# ---- pinned image tags (verified 2026-05-16) ----
_AIRFLOW_IMAGE = "apache/airflow:2.10.4-python3.11"
_POSTGRES_IMAGE = "postgres:15-alpine"

# Filename the runner appends with `-f` when LHS_AIRFLOW_ENABLED is true.
# Exported at module level so runner.py can import + use without parsing.
OVERLAY_FILENAME = "docker-compose.airflow.yml"

# Service names this overlay adds. Runner appends these to the
# explicit `docker compose up -d <services>` argv so they spin up
# alongside the base stack's services (rather than being filtered out
# by the runner's per-cart service list).
SERVICES = ["airflow-postgres", "airflow-init", "airflow-webserver", "airflow-scheduler"]

# Env flag the runner checks before calling write_airflow_overlay().
# Mirrors LHS_TLS_ENABLED / LHS_MONITORING_ENABLED style.
ENV_FLAG = "LHS_AIRFLOW_ENABLED"

# Named volume holding the Airflow metadata Postgres data. Kept distinct
# from the base stack's volumes so `docker volume rm` is targeted.
_PG_VOLUME = "airflow-pgdata"

# Host-side port mapping for the webserver. 8088 because 8080 is commonly
# taken (StarRocks FE web UI on some stacks, generic proxies). Container
# side stays at Airflow's default 8080.
_WEB_HOST_PORT = 8088
_WEB_CONTAINER_PORT = 8080

# Subdirectory the operator drops DAG .py files into. Mounted into both
# webserver + scheduler at /opt/airflow/dags (Airflow's default).
_DAGS_SUBDIR = "dags"
_DAGS_CONTAINER_PATH = "/opt/airflow/dags"

# Placeholder secrets — these SHOULD be overridden by the operator's .env.
# We log a LOUD warning at write-time if they're still the defaults. We do
# NOT generate a random value: a random per-install secret would break the
# operator's "same compose, same admin login" expectation, and we don't
# want to persist it.
_DEFAULT_ADMIN_USER = "admin"
_DEFAULT_ADMIN_PASSWORD = "CHANGE_ME_airflow_admin"  # noqa: S105 — explicit placeholder
_DEFAULT_FERNET_KEY = "CHANGE_ME_generate_with_python_fernet"  # noqa: S105 — explicit placeholder


# ---------- helpers ----------


def _atomic_write(path: Path, data: str) -> None:
    """tmp + os.replace atomic write. Same pattern as monitoring.py /
    jdbc_extras.py — avoids half-written files on Ctrl-C or power loss."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(data, encoding="utf-8")
    os.replace(tmp, path)


def _network_name(install_dir: Path, env: dict) -> str:
    """Resolve the docker network name to attach to. The base stack lives
    on `<install_dir.name>_default` by docker compose convention; the env
    can override via LHS_DOCKER_NETWORK if the operator renamed it."""
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
    """Loud one-liner warning that a secret is still at the placeholder.
    Goes through the module logger so it surfaces in /healthz/log feeds."""
    log.warning(
        "AIRFLOW OVERLAY: %s is using the default placeholder value. %s",
        key, hint,
    )


# ---------- compose override rendering ----------


def _render_overlay(network: str, admin_user: str, admin_password: str,
                    fernet_key: str) -> str:
    """Render the docker-compose.airflow.yml content.

    Four services + one named volume + one external network. We use
    Compose v2 syntax (no `version:` key) to match the base file and the
    other overlays.
    """
    # Common env block shared across all 3 airflow services. We use
    # LocalExecutor (not Celery) because v0.4's UX promise is "one
    # compose up brings the whole thing up" — Celery would add a Redis
    # broker + worker container for negligible benefit on a single-host
    # install. LocalExecutor scales to dozens of concurrent tasks on one
    # box, which is well within the v0.4 use case.
    #
    # AIRFLOW__CORE__LOAD_EXAMPLES is forced off — the example DAGs are
    # noisy and the operator should only see their own DAGs + our demo.
    common_env = (
        "      AIRFLOW__CORE__EXECUTOR: LocalExecutor\n"
        "      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: "
        "postgresql+psycopg2://airflow:airflow@airflow-postgres:5432/airflow\n"
        f"      AIRFLOW__CORE__FERNET_KEY: \"{fernet_key}\"\n"
        "      AIRFLOW__CORE__LOAD_EXAMPLES: \"false\"\n"
        "      AIRFLOW__CORE__DAGS_FOLDER: /opt/airflow/dags\n"
        "      AIRFLOW__WEBSERVER__EXPOSE_CONFIG: \"false\"\n"
    )

    return (
        "# docker-compose.airflow.yml -- Airflow orchestration overlay.\n"
        "# Generated by backend/airflow_overlay.py.\n"
        "#\n"
        f"# Activated automatically when {ENV_FLAG}=true is set in the\n"
        "# install's .env. runner.py appends this file via `-f` so compose\n"
        "# merges it with the base stack.\n"
        "#\n"
        "# This file is an OVERRIDE. The base docker-compose.yml is FROZEN\n"
        "# (certified-stack contract). Airflow is an operational extra, NOT\n"
        "# part of the certified compatibility lock.\n"
        "services:\n"
        "  airflow-postgres:\n"
        f"    image: {_POSTGRES_IMAGE}\n"
        "    container_name: lhs-airflow-postgres\n"
        "    restart: unless-stopped\n"
        "    environment:\n"
        "      POSTGRES_USER: airflow\n"
        "      POSTGRES_PASSWORD: airflow\n"
        "      POSTGRES_DB: airflow\n"
        "    volumes:\n"
        f"      - {_PG_VOLUME}:/var/lib/postgresql/data\n"
        "    healthcheck:\n"
        "      test: [\"CMD\", \"pg_isready\", \"-U\", \"airflow\"]\n"
        "      interval: 10s\n"
        "      timeout: 5s\n"
        "      retries: 5\n"
        "    networks:\n"
        "      - default\n"
        "  airflow-init:\n"
        f"    image: {_AIRFLOW_IMAGE}\n"
        "    container_name: lhs-airflow-init\n"
        "    # One-shot init: migrate schema + create admin user, then exit.\n"
        "    restart: \"no\"\n"
        "    depends_on:\n"
        "      airflow-postgres:\n"
        "        condition: service_healthy\n"
        "    environment:\n"
        + common_env +
        f"      _AIRFLOW_ADMIN_USER: \"{admin_user}\"\n"
        f"      _AIRFLOW_ADMIN_PASSWORD: \"{admin_password}\"\n"
        "    entrypoint: [\"/bin/bash\", \"-c\"]\n"
        "    command:\n"
        "      - |\n"
        "        set -e\n"
        "        airflow db migrate\n"
        "        airflow users create \\\n"
        "          --username \"$${_AIRFLOW_ADMIN_USER}\" \\\n"
        "          --password \"$${_AIRFLOW_ADMIN_PASSWORD}\" \\\n"
        "          --firstname Admin --lastname User \\\n"
        "          --role Admin --email admin@example.invalid \\\n"
        "          || echo \"admin user already exists, skipping\"\n"
        "    volumes:\n"
        f"      - ./{_DAGS_SUBDIR}:{_DAGS_CONTAINER_PATH}\n"
        "    networks:\n"
        "      - default\n"
        "  airflow-webserver:\n"
        f"    image: {_AIRFLOW_IMAGE}\n"
        "    container_name: lhs-airflow-webserver\n"
        "    restart: unless-stopped\n"
        "    depends_on:\n"
        "      airflow-init:\n"
        "        condition: service_completed_successfully\n"
        "    command: [\"airflow\", \"webserver\"]\n"
        "    environment:\n"
        + common_env +
        "    ports:\n"
        f"      - \"{_WEB_HOST_PORT}:{_WEB_CONTAINER_PORT}\"\n"
        "    volumes:\n"
        f"      - ./{_DAGS_SUBDIR}:{_DAGS_CONTAINER_PATH}\n"
        "    healthcheck:\n"
        "      test: [\"CMD\", \"curl\", \"--fail\", \"http://localhost:8080/health\"]\n"
        "      interval: 30s\n"
        "      timeout: 10s\n"
        "      retries: 5\n"
        "    networks:\n"
        "      - default\n"
        "  airflow-scheduler:\n"
        f"    image: {_AIRFLOW_IMAGE}\n"
        "    container_name: lhs-airflow-scheduler\n"
        "    restart: unless-stopped\n"
        "    depends_on:\n"
        "      airflow-init:\n"
        "        condition: service_completed_successfully\n"
        "    command: [\"airflow\", \"scheduler\"]\n"
        "    environment:\n"
        + common_env +
        "    volumes:\n"
        f"      - ./{_DAGS_SUBDIR}:{_DAGS_CONTAINER_PATH}\n"
        "    networks:\n"
        "      - default\n"
        "volumes:\n"
        f"  {_PG_VOLUME}:\n"
        "networks:\n"
        "  default:\n"
        f"    name: {network}\n"
        "    external: true\n"
    )


def _render_demo_dag() -> str:
    """Render the single-task demo DAG that calls into the lakehouse via
    `docker exec`. We use BashOperator (always available, no provider
    install) instead of SparkSubmitOperator (would require
    apache-airflow-providers-apache-spark + a Spark client install in the
    Airflow image — too much surface area for v0.4).

    catchup=False + start_date one day in the past means the DAG appears
    in the UI but does NOT auto-trigger on first scheduler tick. The
    operator clicks "Trigger DAG" when they want to test it.
    """
    return '''"""Lakehouse Studio demo DAG.

Generated by backend/airflow_overlay.py.

A single BashOperator task that runs `spark-submit` against the lakehouse
via `docker exec spark-iceberg`. This avoids needing the
apache-airflow-providers-apache-spark provider inside the Airflow image —
the existing spark-iceberg container already has spark-submit on PATH.

This DAG does NOT auto-run. catchup=False + a past start_date means it
shows up in the UI but stays in the "no runs yet" state until the operator
clicks "Trigger DAG" manually. Delete or modify this file freely — Airflow
re-scans the dags/ folder every 30s by default.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "lakehouse-studio",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="lhs_demo_dag",
    description="Lakehouse Studio demo: run spark-submit against the lakehouse.",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse-studio", "demo"],
) as dag:

    # Single-task demo. The `spark.sql.catalog.udp` catalog is configured
    # by Lakehouse Studio's spark-defaults.conf to point at the Iceberg
    # REST endpoint, so this query is the simplest possible smoke test.
    demo_query = BashOperator(
        task_id="lhs_demo_spark_smoke",
        bash_command=(
            "docker exec spark-iceberg "
            "spark-sql --conf spark.sql.catalog.udp.type=rest "
            "-e 'SHOW DATABASES IN udp;'"
        ),
    )
'''


# ---------- validation ----------


def validate_overlay(install_dir: Path) -> list[str]:
    """Return a list of human-readable problems for /healthz.

    Empty list = healthy / not enabled. We check:
      - if the overlay file exists, the dags/ folder must exist too
      - the demo DAG file should still be parseable Python (cheap syntax
        check via compile())

    We do NOT check container health here — that's docker's job. This is
    a static-file consistency check the API layer can call cheaply.
    """
    problems: list[str] = []
    overlay = install_dir / OVERLAY_FILENAME
    if not overlay.exists():
        # not enabled — nothing to validate
        return problems

    dags_dir = install_dir / _DAGS_SUBDIR
    if not dags_dir.exists():
        problems.append(
            f"airflow overlay present but {dags_dir} missing — "
            "scheduler will start with zero DAGs"
        )

    demo = dags_dir / "lhs_demo_dag.py"
    if demo.exists():
        try:
            compile(demo.read_text(encoding="utf-8"), str(demo), "exec")
        except SyntaxError as e:
            problems.append(f"demo DAG {demo} has a syntax error: {e}")

    return problems


# ---------- public API ----------


def write_airflow_overlay(install_dir: Path, env: dict) -> Optional[Path]:
    """Write the Airflow overlay + demo DAG into install_dir.

    Returns the Path to the written docker-compose.airflow.yml, or None if
    the env flag is OFF (in which case nothing is written).

    Idempotent — re-running with the same install_dir overwrites both
    files. Safe to call from `runner._step_env` every install. The env
    dict is the merged install env (.env contents + process env overrides
    the operator may have supplied).
    """
    flag = env.get(ENV_FLAG, "").strip().lower()
    if flag not in ("1", "true", "yes", "on"):
        log.debug("airflow overlay disabled (%s=%r)", ENV_FLAG, flag)
        return None

    network = _network_name(install_dir, env)

    admin_user, user_is_default = _resolve_secret(
        env, "AIRFLOW_ADMIN_USER", _DEFAULT_ADMIN_USER)
    admin_password, pw_is_default = _resolve_secret(
        env, "AIRFLOW_ADMIN_PASSWORD", _DEFAULT_ADMIN_PASSWORD)
    fernet_key, fernet_is_default = _resolve_secret(
        env, "AIRFLOW_FERNET_KEY", _DEFAULT_FERNET_KEY)

    if user_is_default:
        # username is not strictly a secret but still worth flagging
        log.info("AIRFLOW OVERLAY: AIRFLOW_ADMIN_USER defaulting to 'admin'")
    if pw_is_default:
        _warn_default(
            "AIRFLOW_ADMIN_PASSWORD",
            "Set it in the install's .env before exposing port 8088 to anything beyond localhost.",
        )
    if fernet_is_default:
        _warn_default(
            "AIRFLOW_FERNET_KEY",
            "Generate with: python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\"",
        )

    overlay_body = _render_overlay(network, admin_user, admin_password,
                                   fernet_key)
    overlay_path = install_dir / OVERLAY_FILENAME

    # DAG file FIRST so the scheduler never starts pointed at a missing
    # folder. Compose merge would otherwise race against the bind-mount.
    dags_dir = install_dir / _DAGS_SUBDIR
    demo_path = dags_dir / "lhs_demo_dag.py"
    _atomic_write(demo_path, _render_demo_dag())

    _atomic_write(overlay_path, overlay_body)

    log.info(
        "airflow overlay written install_dir=%s network=%s admin_user=%s",
        install_dir, network, admin_user,
    )
    return overlay_path
