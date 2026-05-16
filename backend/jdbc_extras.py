"""JDBC driver extras — opt-in compose override that side-loads Postgres /
MySQL JDBC jars onto the spark service.

PURE ADDITIVE MODULE. Mirrors the shape of `caddy_tls.py` and `monitoring.py`:

  - NEVER touches the base `docker-compose.yml` that
    `runner._patch_compose_images()` writes (FROZEN — certified-stack
    contract).
  - Writes a sibling `docker-compose.jdbc.yml` the operator opts into via
    an explicit `docker compose -f docker-compose.yml -f
    docker-compose.jdbc.yml up -d jdbc-extras` command surfaced from this
    module.
  - The override defines:
      1. A one-shot init container `jdbc-extras` (image: curlimages/curl)
         that downloads the requested JDBC jars from Maven Central into a
         named docker volume (`spark_jdbc_jars`). Idempotent — re-running
         skips jars that already exist.
      2. A spark service block that mounts that named volume read-only at
         `/opt/spark/jars/jdbc`. Compose merge appends the volume mount onto
         the base spark service definition; the spark classpath picks up
         everything under `/opt/spark/jars/jdbc` because spark-iceberg's
         entrypoint sets `SPARK_CLASSPATH` to include `jars/*`.

Why a separate volume instead of bind-mounting from the host:

  - On Windows + Docker Desktop, bind-mounting a host directory into the
    spark container hits permission + path-translation issues that have
    burned us before. A named volume sidesteps both.
  - The init container runs `curl -fL --retry 3` from inside the Docker
    network, so corporate proxies that have already been configured for
    Docker pull-through work transparently.

Why an init container instead of pre-baking a custom Spark image:

  - The certified Spark image (tabulario/spark-iceberg:3.5.5_1.8.1) is
    pinned in the lock file. Repackaging it would invalidate the lock.
  - JDBC drivers are operational extras, not part of the certified surface
    — they belong in an override layer the operator opts into.

Jar pins (verified reachable 2026-05-16 — see verify section in
docs/COMPATIBILITY.md):

  - postgresql-42.7.4.jar from Maven Central (1.04 MB)
  - mysql-connector-j-9.0.0.jar from Maven Central (2.47 MB)

We pin via Maven Central (repo1.maven.org) rather than vendor download
mirrors because Maven Central guarantees immutable artifacts — once a
version is published it cannot be retracted or republished. The Postgres
project's `jdbc.postgresql.org/download/` mirror serves the same bytes but
isn't covered by Maven's immutability policy.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

from pydantic import BaseModel, Field, field_validator

from .state import store


log = logging.getLogger("lhs.jdbc_extras")


# ---- pinned driver versions (verified 2026-05-16) ----
#
# When bumping these, run:
#   curl -I https://repo1.maven.org/maven2/org/postgresql/postgresql/<v>/postgresql-<v>.jar
#   curl -I https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/<v>/mysql-connector-j-<v>.jar
# and confirm a 200 before committing.
_DEFAULT_POSTGRES_VERSION = "42.7.4"
_DEFAULT_MYSQL_VERSION = "9.0.0"

# Image used to run the one-shot jar fetch. Pinned (not :latest) so the
# override file produces byte-identical results across runs. curlimages/curl
# is the official curl team image — small (~5 MB), multi-arch, no shell
# surprises.
_FETCHER_IMAGE = "curlimages/curl:8.10.1"

# Filenames written into the install_dir. Base compose is `docker-compose.yml`
# (FROZEN); ours is the sibling override that compose merges in.
_OVERRIDE_FILENAME = "docker-compose.jdbc.yml"

# Named volume that the init container populates and the spark service
# mounts read-only. Kept distinct from the base compose's volumes so we
# can `docker volume rm` it independently on disable.
_JDBC_VOLUME = "spark_jdbc_jars"

# Service name (matches the entry in the base compose). We do NOT define
# the spark image/command here — compose merge appends only the new volume
# mount onto the base service definition.
_SPARK_SERVICE = "spark-iceberg"

# In-container mount path. spark-iceberg's entrypoint adds `$SPARK_HOME/jars/*`
# to the classpath; mounting under `jars/jdbc` keeps our additions visually
# separate from the image's bundled jars so an operator inspecting the
# container can tell what came from the override.
_JDBC_MOUNT_PATH = "/opt/spark/jars/jdbc"


# ---------- models ----------


class JdbcExtrasProfile(BaseModel):
    """Inputs for enable_jdbc_extras. Defaults install postgres only — MySQL
    is opt-in because most v0.5 ingest users start with Postgres and we'd
    rather not download an extra 2.5 MB jar that nobody uses."""
    include_postgres: bool = True
    include_mysql: bool = False
    postgres_driver_version: str = Field(default=_DEFAULT_POSTGRES_VERSION,
                                         max_length=32)
    mysql_driver_version: str = Field(default=_DEFAULT_MYSQL_VERSION,
                                      max_length=32)

    @field_validator("postgres_driver_version", "mysql_driver_version")
    @classmethod
    def _safe_version(cls, v: str) -> str:
        """Restrict to characters that are safe to splice into a Maven URL
        and a docker-compose YAML string without quoting. Maven version
        strings are alnum + `.` + `-` + `_`; anything else is rejected."""
        v = v.strip()
        if not v:
            raise ValueError("driver version cannot be empty")
        for ch in v:
            if not (ch.isalnum() or ch in "._-"):
                raise ValueError(
                    f"driver version {v!r} contains illegal character {ch!r}"
                )
        return v


# ---------- helpers ----------


def _install_dir(install_id: str) -> Path:
    """Resolve the install_dir for the given install_id, raising ValueError
    on either unknown install or missing on-disk directory. Caller turns
    that into an HTTP 400 / 404."""
    rec = store.get(install_id)
    if rec is None:
        raise ValueError(f"install {install_id!r} not found")
    p = Path(rec.install_dir)
    if not p.exists():
        raise ValueError(
            f"install_dir {p} does not exist (was the install removed?)"
        )
    return p


def _override_path(install_id: str) -> Path:
    return _install_dir(install_id) / _OVERRIDE_FILENAME


def _atomic_write(path: Path, data: str) -> None:
    """tmp + os.replace atomic write. Mirrors the pattern used in
    monitoring.py / data_sources.py so we don't half-write the override on
    a power loss or Ctrl-C."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(data, encoding="utf-8")
    os.replace(tmp, path)


def _postgres_jar_name(version: str) -> str:
    return f"postgresql-{version}.jar"


def _mysql_jar_name(version: str) -> str:
    return f"mysql-connector-j-{version}.jar"


def _postgres_jar_url(version: str) -> str:
    """Maven Central URL for the Postgres JDBC jar. The repo1.maven.org
    layout is `<groupId-as-path>/<artifactId>/<version>/<artifactId>-<version>.jar`.
    """
    return (
        f"https://repo1.maven.org/maven2/org/postgresql/postgresql/"
        f"{version}/postgresql-{version}.jar"
    )


def _mysql_jar_url(version: str) -> str:
    """Maven Central URL for MySQL Connector/J. Oracle publishes under
    `com.mysql:mysql-connector-j` (the older `mysql:mysql-connector-java`
    coordinate is unmaintained past 8.0.33 — we use the new one)."""
    return (
        f"https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/"
        f"{version}/mysql-connector-j-{version}.jar"
    )


# ---------- compose override rendering ----------


def _render_fetch_command(profile: JdbcExtrasProfile) -> str:
    """Build the `sh -c` script the init container runs.

    Idempotent: skips a jar if it already exists. `curl -fL --retry 3`
    fails the container on a non-2xx (which compose surfaces as a non-zero
    exit code in `docker compose logs jdbc-extras`).

    The script writes into `/jars` which is the in-container mount point of
    the named volume.
    """
    lines = ["set -eu", "mkdir -p /jars", "cd /jars"]

    if profile.include_postgres:
        jar = _postgres_jar_name(profile.postgres_driver_version)
        url = _postgres_jar_url(profile.postgres_driver_version)
        lines.append(
            f'if [ ! -f "{jar}" ]; then '
            f'echo "fetching {jar}"; '
            f'curl -fL --retry 3 --retry-delay 2 -o "{jar}.tmp" "{url}" && '
            f'mv "{jar}.tmp" "{jar}"; '
            f'else echo "{jar} already present"; fi'
        )

    if profile.include_mysql:
        jar = _mysql_jar_name(profile.mysql_driver_version)
        url = _mysql_jar_url(profile.mysql_driver_version)
        lines.append(
            f'if [ ! -f "{jar}" ]; then '
            f'echo "fetching {jar}"; '
            f'curl -fL --retry 3 --retry-delay 2 -o "{jar}.tmp" "{url}" && '
            f'mv "{jar}.tmp" "{jar}"; '
            f'else echo "{jar} already present"; fi'
        )

    lines.append("echo done; ls -l /jars")
    return " && ".join(lines)


def _render_override(profile: JdbcExtrasProfile) -> str:
    """Render the docker-compose.jdbc.yml content.

    Two service blocks:
      - jdbc-extras: one-shot init container that downloads jars into the
        named volume. Marked `restart: "no"` because once the jars are in
        place it should not re-run on stack restart (the spark service uses
        the populated volume directly).
      - spark-iceberg: appends ONLY the new volume mount. Compose merge
        semantics on the same service name preserve every other key from
        the base file (image, command, ports, env, etc).

    A top-level `volumes:` block declares the named volume so compose
    creates it on first `up`.
    """
    fetch_script = _render_fetch_command(profile)

    # YAML block scalar emit: we use a single-quoted string so the shell
    # script's single chars and `$` don't need YAML escaping. Compose
    # passes the value through verbatim to `sh -c`.
    # The script uses ONLY double-quotes internally, so single-quote
    # wrapping in YAML is safe.
    return (
        "# docker-compose.jdbc.yml -- JDBC driver side-load override.\n"
        "# Generated by backend/jdbc_extras.py.\n"
        "#\n"
        "# Activate with:\n"
        "#   docker compose -f docker-compose.yml -f docker-compose.jdbc.yml up -d jdbc-extras\n"
        "# Once `docker compose ps jdbc-extras` shows exit-0, the named volume\n"
        f"# {_JDBC_VOLUME} holds the requested JDBC jars. Recreate the spark\n"
        "# service so it picks up the new mount:\n"
        "#   docker compose -f docker-compose.yml -f docker-compose.jdbc.yml up -d --no-deps "
        f"{_SPARK_SERVICE}\n"
        "#\n"
        "# This file is an OVERRIDE. The base docker-compose.yml is FROZEN\n"
        "# (certified-stack contract). Drivers are operational extras, NOT\n"
        "# part of the certified compatibility lock.\n"
        "services:\n"
        "  jdbc-extras:\n"
        f"    image: {_FETCHER_IMAGE}\n"
        "    container_name: lhs-jdbc-extras\n"
        "    # One-shot init container -- exits 0 once jars are downloaded.\n"
        "    # `restart: no` ensures it never re-runs on stack restart; the\n"
        "    # populated volume persists and the spark service consumes it.\n"
        "    restart: \"no\"\n"
        "    entrypoint: [\"sh\", \"-c\"]\n"
        f"    command: ['{fetch_script}']\n"
        "    volumes:\n"
        f"      - {_JDBC_VOLUME}:/jars\n"
        f"  {_SPARK_SERVICE}:\n"
        "    # Append-only: compose merge keeps the base image/command/env;\n"
        "    # we only contribute the read-only JDBC jars mount.\n"
        "    volumes:\n"
        f"      - {_JDBC_VOLUME}:{_JDBC_MOUNT_PATH}:ro\n"
        "    depends_on:\n"
        "      jdbc-extras:\n"
        "        condition: service_completed_successfully\n"
        "volumes:\n"
        f"  {_JDBC_VOLUME}:\n"
    )


# ---------- public API ----------


def is_jdbc_enabled(install_id: str) -> bool:
    """Cheap presence check — does the override file exist on disk?

    Returns False (does not raise) if the install_id is unknown or its
    install_dir is missing — callers use this for UI status, not for
    guarding writes."""
    try:
        rec = store.get(install_id)
        if rec is None:
            return False
        return (Path(rec.install_dir) / _OVERRIDE_FILENAME).exists()
    except Exception:  # pragma: no cover -- defensive, never raise from a status check
        return False


def jdbc_activate_command(install_id: str) -> str:
    """Return the exact command the operator runs to bring the init
    container up. We surface this rather than executing it ourselves --
    the operator's stack lifecycle stays in their hands, same as the Caddy
    sidecar.
    """
    return (
        "docker compose -f docker-compose.yml -f docker-compose.jdbc.yml "
        "up -d jdbc-extras"
    )


def jdbc_deactivate_command(install_id: str) -> str:
    """Granular teardown -- stops + removes ONLY the jdbc-extras container.

    Does NOT touch the spark service (Caddy fix pattern: a `compose down`
    here would clobber the whole stack). The named volume `spark_jdbc_jars`
    is intentionally NOT removed so re-enabling skips the download; if the
    operator wants a clean slate they can `docker volume rm spark_jdbc_jars`
    after this.
    """
    return (
        "docker compose -f docker-compose.yml -f docker-compose.jdbc.yml "
        "stop jdbc-extras && "
        "docker compose -f docker-compose.yml -f docker-compose.jdbc.yml "
        "rm -f jdbc-extras"
    )


async def enable_jdbc_extras(install_id: str,
                             profile: JdbcExtrasProfile) -> dict:
    """Write the docker-compose.jdbc.yml override into the install_dir.

    Idempotent — re-running with the same install_id rewrites the override
    with the latest profile. The operator then runs the returned activate
    command to download the requested jars.

    Refuses to write a no-op override: if both include_postgres AND
    include_mysql are False there is nothing for the init container to do,
    which would be a confusing footgun.
    """
    if not (profile.include_postgres or profile.include_mysql):
        raise ValueError(
            "must include at least one of include_postgres / include_mysql"
        )

    install_dir = _install_dir(install_id)
    override_path = install_dir / _OVERRIDE_FILENAME

    override_body = _render_override(profile)
    _atomic_write(override_path, override_body)

    log.info(
        "jdbc-extras enabled install=%s postgres=%s(%s) mysql=%s(%s)",
        install_id,
        profile.include_postgres, profile.postgres_driver_version,
        profile.include_mysql, profile.mysql_driver_version,
    )

    return {
        "compose_file_path": str(override_path),
        "activate_command": jdbc_activate_command(install_id),
        "postgres_pinned": (
            profile.postgres_driver_version if profile.include_postgres else None
        ),
        "mysql_pinned": (
            profile.mysql_driver_version if profile.include_mysql else None
        ),
        "volume_name": _JDBC_VOLUME,
        "mount_path": _JDBC_MOUNT_PATH,
    }


async def disable_jdbc_extras(install_id: str) -> dict:
    """Remove the override file from the install_dir.

    Caller is responsible for running the returned deactivate command
    FIRST so docker compose can still reconcile the running init container
    against its definition. Pulling the override file out from under a
    running stack works in compose v2 (it just complains on the next
    `up`), but the granular teardown is cleaner.

    The named volume `spark_jdbc_jars` is intentionally LEFT IN PLACE —
    re-enabling skips the download. Use `docker volume rm spark_jdbc_jars`
    for a clean slate.
    """
    install_dir = _install_dir(install_id)
    override_path = install_dir / _OVERRIDE_FILENAME

    removed = False
    if override_path.exists():
        try:
            override_path.unlink()
            removed = True
        except OSError as e:
            log.warning("failed to remove jdbc override %s: %s",
                        override_path, e)

    log.info("jdbc-extras disabled install=%s override_removed=%s",
             install_id, removed)

    return {
        "disabled": removed,
        "deactivate_command": jdbc_deactivate_command(install_id),
        "volume_retained": _JDBC_VOLUME,
        "volume_purge_hint": (
            f"docker volume rm {_JDBC_VOLUME}  # only after stopping the spark service"
        ),
    }
