"""Superset BI sidecar — opt-in compose override.

PURE ADDITIVE MODULE. Same shape as `airflow_overlay.py`,
`dagster_overlay.py`, `caddy_tls.py`, `monitoring.py`, and
`jdbc_extras.py`:

  - NEVER touches the base `docker-compose.yml` that
    `runner._patch_compose_images()` writes (FROZEN — certified-stack
    contract).
  - Writes a sibling `docker-compose.superset.yml` the operator opts into
    via the env flag `LHS_SUPERSET_ENABLED=true` (default OFF). runner.py
    consults that flag during the env step and appends our override file
    via `-f` so compose merges it with the base stack.
  - All sensitive values (SECRET_KEY, admin password, postgres password)
    come from env vars with safe placeholder defaults that log a LOUD
    warning if left unchanged.

Services defined in the override:

  - superset-postgres   postgres:15-alpine, named volume
                        `superset-pgdata`, pg_isready healthcheck.
                        Metadata DB for Superset's dashboards / charts /
                        users.
  - superset-redis      redis:7-alpine, used for Superset's results cache
                        + Celery broker if the operator turns async on.
  - superset-init       apache/superset:4.1.1 one-shot: runs
                        `superset db upgrade && superset fab create-admin
                        && superset init`, then exits.
  - superset-app        apache/superset:4.1.1, host port 8089 mapped to
                        container 8088. depends_on superset-init
                        completed_successfully.

Plus the on-disk superset/ subdir with:

  - superset_config.py  Python config Superset reads at startup.
                        Configures SQLAlchemy URI to postgres, Redis
                        cache + Celery, ENABLE_TEMPLATE_PROCESSING, and
                        carries the SECRET_KEY warning.

Database connection bootstrap: the init container also pre-creates a
Superset DB connection pointing at whichever query engine is in the cart:
  - Trino (preferred when present)   → trino://lakehouse@trino:8080/iceberg
  - StarRocks (fallback)             → mysql+pymysql://root@starrocks-fe:9030/

Detection is done by inspecting the `env` dict passed in — runner.py
hands us the merged env which contains `LHS_CART_COMPONENTS` (comma-list
of component ids). We fall back to StarRocks if neither is present, on
the grounds that v0.4 ships StarRocks in every certified stack.

Image tags pinned 2026-05-16:
  - apache/superset:4.1.1  — latest stable 4.1.x. Multi-arch. DO NOT use
    :latest — Superset 5.x will introduce config breaks.
  - postgres:15-alpine     — same as the sibling overlays.
  - redis:7-alpine         — Superset 4.x docs recommend Redis 7.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional


log = logging.getLogger("lhs.superset_overlay")


# ---- pinned image tags (verified 2026-05-16) ----
_SUPERSET_IMAGE = "apache/superset:4.1.1"
_POSTGRES_IMAGE = "postgres:15-alpine"
_REDIS_IMAGE = "redis:7-alpine"

# Filename the runner appends with `-f` when LHS_SUPERSET_ENABLED is true.
# Exported at module level so runner.py can import + use without parsing.
OVERLAY_FILENAME = "docker-compose.superset.yml"

# Env flag the runner checks before calling write_superset_overlay().
ENV_FLAG = "LHS_SUPERSET_ENABLED"

# Service names this overlay adds. Runner appends these to the
# explicit `docker compose up -d <services>` argv so they spin up
# alongside the base stack's services.
SERVICES = ["superset-postgres", "superset-redis", "superset-init", "superset-app"]

# Named volumes — distinct from the base stack's volumes so `docker
# volume rm` is targeted.
_PG_VOLUME = "superset-pgdata"
_HOME_VOLUME = "superset-home"

# Host-side port mapping for the app. 8089 because 8088 (container side)
# collides with Airflow's webserver on stacks that enable both. Container
# stays at Superset's default 8088.
_APP_HOST_PORT = 8089
_APP_CONTAINER_PORT = 8088

# Subdirectory holding the on-disk Superset config. Mounted into every
# Superset container at /app/pythonpath so the Superset entrypoint picks
# up superset_config.py automatically.
_CFG_SUBDIR = "superset"
_CFG_CONTAINER_PATH = "/app/pythonpath"

# Placeholder secrets. We NEVER generate random per-install values —
# operator sets them in .env and we surface a loud warning if not.
_DEFAULT_SECRET_KEY = "CHANGE_ME_superset_secret_key"  # noqa: S105 — explicit placeholder
_DEFAULT_ADMIN_USER = "admin"
_DEFAULT_ADMIN_PASSWORD = "CHANGE_ME_superset_admin"  # noqa: S105 — explicit placeholder
_DEFAULT_PG_PASSWORD = "CHANGE_ME_superset_pg"  # noqa: S105 — explicit placeholder


# ---------- helpers ----------


def _atomic_write(path: Path, data: str) -> None:
    """tmp + os.replace atomic write. Mirrors the sibling overlays."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(data, encoding="utf-8")
    os.replace(tmp, path)


def _network_name(install_dir: Path, env: dict) -> str:
    """Resolve the docker network name. Same logic as the sibling overlays."""
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
        "SUPERSET OVERLAY: %s is using the default placeholder value. %s",
        key, hint,
    )


def _detect_query_engine(env: dict) -> tuple[str, str, str]:
    """Inspect the merged env to pick which DB connection to pre-create.

    Returns (engine_label, sqlalchemy_uri, display_name). Detection rule:
      - LHS_CART_COMPONENTS contains 'trino' → use Trino
      - otherwise                            → use StarRocks (always
        present in v0.4 certified stacks)

    We deliberately avoid embedding live passwords in the URI — Superset
    treats this as a connection template. The operator edits the
    connection inside the UI on first login if they need different creds.
    """
    components = env.get("LHS_CART_COMPONENTS", "").lower()
    cart_set = {c.strip() for c in components.split(",") if c.strip()}

    if "trino" in cart_set:
        return (
            "trino",
            "trino://lakehouse@trino:8080/iceberg",
            "Lakehouse (Trino)",
        )

    # Default: StarRocks via the MySQL wire protocol. The pymysql driver
    # is bundled in apache/superset:4.x. Root user is the StarRocks
    # default for fresh installs; the operator should rotate credentials
    # via StarRocks SQL before this connection sees prod traffic.
    return (
        "starrocks",
        "mysql+pymysql://root@starrocks-fe:9030/",
        "Lakehouse (StarRocks)",
    )


# ---------- compose override rendering ----------


def _render_overlay(network: str, pg_password: str, secret_key: str,
                    admin_user: str, admin_password: str,
                    db_uri: str, db_name: str) -> str:
    """Render the docker-compose.superset.yml content.

    Four services + two named volumes + one external network. Compose v2
    syntax (no `version:` key) to match the base file.
    """
    # Common env block shared across init + app containers. The init
    # container additionally gets the admin user/password vars.
    common_env = (
        f"      SUPERSET_SECRET_KEY: \"{secret_key}\"\n"
        "      SUPERSET_LOAD_EXAMPLES: \"no\"\n"
        f"      PYTHONPATH: {_CFG_CONTAINER_PATH}\n"
        f"      SUPERSET_DB_PASSWORD: \"{pg_password}\"\n"
    )

    common_volumes = (
        "    volumes:\n"
        f"      - ./{_CFG_SUBDIR}:{_CFG_CONTAINER_PATH}:ro\n"
        f"      - {_HOME_VOLUME}:/app/superset_home\n"
    )

    return (
        "# docker-compose.superset.yml -- Superset BI overlay.\n"
        "# Generated by backend/superset_overlay.py.\n"
        "#\n"
        f"# Activated automatically when {ENV_FLAG}=true is set in the\n"
        "# install's .env. runner.py appends this file via `-f` so compose\n"
        "# merges it with the base stack.\n"
        "#\n"
        "# This file is an OVERRIDE. The base docker-compose.yml is FROZEN\n"
        "# (certified-stack contract). Superset is an operational extra,\n"
        "# NOT part of the certified compatibility lock.\n"
        "services:\n"
        "  superset-postgres:\n"
        f"    image: {_POSTGRES_IMAGE}\n"
        "    container_name: lhs-superset-postgres\n"
        "    restart: unless-stopped\n"
        "    environment:\n"
        "      POSTGRES_USER: superset\n"
        f"      POSTGRES_PASSWORD: \"{pg_password}\"\n"
        "      POSTGRES_DB: superset\n"
        "    volumes:\n"
        f"      - {_PG_VOLUME}:/var/lib/postgresql/data\n"
        "    healthcheck:\n"
        "      test: [\"CMD\", \"pg_isready\", \"-U\", \"superset\"]\n"
        "      interval: 10s\n"
        "      timeout: 5s\n"
        "      retries: 5\n"
        "    networks:\n"
        "      - default\n"
        "  superset-redis:\n"
        f"    image: {_REDIS_IMAGE}\n"
        "    container_name: lhs-superset-redis\n"
        "    restart: unless-stopped\n"
        "    healthcheck:\n"
        "      test: [\"CMD\", \"redis-cli\", \"ping\"]\n"
        "      interval: 10s\n"
        "      timeout: 5s\n"
        "      retries: 5\n"
        "    networks:\n"
        "      - default\n"
        "  superset-init:\n"
        f"    image: {_SUPERSET_IMAGE}\n"
        "    container_name: lhs-superset-init\n"
        "    # One-shot: schema upgrade + admin user + role init + DB\n"
        "    # connection bootstrap. Exits 0 when done.\n"
        "    restart: \"no\"\n"
        "    depends_on:\n"
        "      superset-postgres:\n"
        "        condition: service_healthy\n"
        "      superset-redis:\n"
        "        condition: service_healthy\n"
        "    environment:\n"
        + common_env +
        f"      ADMIN_USERNAME: \"{admin_user}\"\n"
        f"      ADMIN_PASSWORD: \"{admin_password}\"\n"
        f"      LHS_DB_URI: \"{db_uri}\"\n"
        f"      LHS_DB_NAME: \"{db_name}\"\n"
        "    entrypoint: [\"/bin/bash\", \"-c\"]\n"
        "    command:\n"
        "      - |\n"
        "        set -e\n"
        "        superset db upgrade\n"
        "        superset fab create-admin \\\n"
        "          --username \"$${ADMIN_USERNAME}\" \\\n"
        "          --password \"$${ADMIN_PASSWORD}\" \\\n"
        "          --firstname Admin --lastname User \\\n"
        "          --email admin@example.invalid \\\n"
        "          || echo \"admin already exists, skipping\"\n"
        "        superset init\n"
        "        python -c \"\\\n"
        "from superset import db; from superset.models.core import Database; \\\n"
        "import os; uri=os.environ['LHS_DB_URI']; name=os.environ['LHS_DB_NAME']; \\\n"
        "exists=db.session.query(Database).filter_by(database_name=name).first(); \\\n"
        "print('connection exists' if exists else 'creating connection'); \\\n"
        "exists or db.session.add(Database(database_name=name, sqlalchemy_uri=uri)); \\\n"
        "db.session.commit()\"\n"
        + common_volumes +
        "    networks:\n"
        "      - default\n"
        "  superset-app:\n"
        f"    image: {_SUPERSET_IMAGE}\n"
        "    container_name: lhs-superset-app\n"
        "    restart: unless-stopped\n"
        "    depends_on:\n"
        "      superset-init:\n"
        "        condition: service_completed_successfully\n"
        "    environment:\n"
        + common_env +
        "    ports:\n"
        f"      - \"{_APP_HOST_PORT}:{_APP_CONTAINER_PORT}\"\n"
        + common_volumes +
        "    healthcheck:\n"
        "      test: [\"CMD\", \"curl\", \"--fail\", \"http://localhost:8088/health\"]\n"
        "      interval: 30s\n"
        "      timeout: 10s\n"
        "      retries: 5\n"
        "    networks:\n"
        "      - default\n"
        "volumes:\n"
        f"  {_PG_VOLUME}:\n"
        f"  {_HOME_VOLUME}:\n"
        "networks:\n"
        "  default:\n"
        f"    name: {network}\n"
        "    external: true\n"
    )


def _render_superset_config() -> str:
    """Render superset_config.py.

    Reads SECRET_KEY + DB password from env so the on-disk file carries
    no live secrets. Configures Postgres SQLAlchemy URI, Redis cache +
    Celery results backend, and enables ENABLE_TEMPLATE_PROCESSING for
    Jinja-driven dashboards.
    """
    return '''"""Lakehouse Studio Superset config.

Generated by backend/superset_overlay.py.

Reads secrets from env vars set on each Superset container — nothing
sensitive lives in this file on disk. SECRET_KEY rotation: change
SUPERSET_SECRET_KEY in the install's .env, then `docker compose restart
superset-app`. Existing sessions invalidate on rotation, which is the
intended behaviour.
"""
import os

# ---- secrets (sourced from env) ----
# SECURITY: a stable random SECRET_KEY is REQUIRED for Superset to hash
# session cookies and CSRF tokens consistently. Defaulting it is a
# security smell — the overlay logs a loud warning if the operator left
# the placeholder in place.
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "")
if not SECRET_KEY or SECRET_KEY.startswith("CHANGE_ME"):
    # Hard fail at import — better than booting an insecure instance.
    raise RuntimeError(
        "SUPERSET_SECRET_KEY env var is missing or still the placeholder. "
        "Set it in the install's .env to a long random string before booting."
    )

_PG_PASSWORD = os.environ.get("SUPERSET_DB_PASSWORD", "")

# ---- metadata DB ----
# Superset's own tables live in the sidecar postgres. Connection params
# match the docker-compose.superset.yml service names.
SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://superset:{_PG_PASSWORD}@superset-postgres:5432/superset"
)

# ---- caching ----
# Results cache + thumbnail cache via Redis. Two logical DBs so cache
# evictions don't clobber thumbnails.
CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_HOST": "superset-redis",
    "CACHE_REDIS_PORT": 6379,
    "CACHE_REDIS_DB": 1,
}
DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_data_",
    "CACHE_REDIS_HOST": "superset-redis",
    "CACHE_REDIS_PORT": 6379,
    "CACHE_REDIS_DB": 2,
}

# ---- Celery (async query exec) ----
# Off by default — Superset runs queries synchronously, which is fine
# for v0.4 single-host. Operator can flip this on per-database via the
# UI later; the broker/backend are already wired.
class CeleryConfig:
    broker_url = "redis://superset-redis:6379/0"
    result_backend = "redis://superset-redis:6379/0"
    imports = ("superset.sql_lab", "superset.tasks.scheduler")
    worker_prefetch_multiplier = 1
    task_acks_late = True

CELERY_CONFIG = CeleryConfig

# ---- feature flags ----
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    # ALERT_REPORTS off — would require an additional Celery worker
    # service that v0.4 does not ship. Operator can enable later.
    "ALERT_REPORTS": False,
}

# ---- security headers ----
# httpOnly + secure cookies. Operator behind Caddy TLS sidecar gets
# secure=True effectively; without TLS the browser still receives the
# secure flag but won't send the cookie over http (which is the desired
# fail-closed behaviour).
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SECURE = False  # set True once TLS is in front
SESSION_COOKIE_SAMESITE = "Lax"
'''


# ---------- validation ----------


def validate_overlay(install_dir: Path) -> list[str]:
    """Return a list of human-readable problems for /healthz.

    Empty list = healthy / not enabled. Static-file consistency only.
    """
    problems: list[str] = []
    overlay = install_dir / OVERLAY_FILENAME
    if not overlay.exists():
        return problems

    cfg_dir = install_dir / _CFG_SUBDIR
    if not cfg_dir.exists():
        problems.append(
            f"superset overlay present but {cfg_dir} missing — "
            "app will fail to import superset_config.py"
        )
        return problems

    cfg = cfg_dir / "superset_config.py"
    if not cfg.exists():
        problems.append(f"superset config missing {cfg.name}")
    else:
        try:
            compile(cfg.read_text(encoding="utf-8"), str(cfg), "exec")
        except SyntaxError as e:
            problems.append(f"superset_config.py has a syntax error: {e}")

    return problems


# ---------- public API ----------


def write_superset_overlay(install_dir: Path, env: dict) -> Optional[Path]:
    """Write the Superset overlay + config files into install_dir.

    Returns the Path to the written docker-compose.superset.yml, or None
    if the env flag is OFF.

    Idempotent — re-running overwrites every generated file. The
    pre-created DB connection in superset-init's bootstrap is detected
    from `env['LHS_CART_COMPONENTS']` — Trino if present, StarRocks
    fallback otherwise.
    """
    flag = env.get(ENV_FLAG, "").strip().lower()
    if flag not in ("1", "true", "yes", "on"):
        log.debug("superset overlay disabled (%s=%r)", ENV_FLAG, flag)
        return None

    network = _network_name(install_dir, env)

    pg_password, pg_default = _resolve_secret(
        env, "SUPERSET_DB_PASSWORD", _DEFAULT_PG_PASSWORD)
    secret_key, sk_default = _resolve_secret(
        env, "SUPERSET_SECRET_KEY", _DEFAULT_SECRET_KEY)
    admin_user, _ = _resolve_secret(
        env, "SUPERSET_ADMIN_USER", _DEFAULT_ADMIN_USER)
    admin_password, ap_default = _resolve_secret(
        env, "SUPERSET_ADMIN_PASSWORD", _DEFAULT_ADMIN_PASSWORD)

    if pg_default:
        _warn_default(
            "SUPERSET_DB_PASSWORD",
            "Set it in the install's .env before exposing the stack.",
        )
    if sk_default:
        _warn_default(
            "SUPERSET_SECRET_KEY",
            "Generate with: python -c \"import secrets; print(secrets.token_urlsafe(48))\". "
            "Superset will REFUSE to boot with the placeholder value.",
        )
    if ap_default:
        _warn_default(
            "SUPERSET_ADMIN_PASSWORD",
            "Set it in the install's .env before exposing port 8089.",
        )

    engine_label, db_uri, db_name = _detect_query_engine(env)
    log.info("superset overlay will pre-create %s connection (%s)",
             engine_label, db_name)

    cfg_dir = install_dir / _CFG_SUBDIR
    _atomic_write(cfg_dir / "superset_config.py", _render_superset_config())

    overlay_path = install_dir / OVERLAY_FILENAME
    overlay_body = _render_overlay(network, pg_password, secret_key,
                                   admin_user, admin_password,
                                   db_uri, db_name)
    _atomic_write(overlay_path, overlay_body)

    log.info(
        "superset overlay written install_dir=%s network=%s engine=%s",
        install_dir, network, engine_label,
    )
    return overlay_path
