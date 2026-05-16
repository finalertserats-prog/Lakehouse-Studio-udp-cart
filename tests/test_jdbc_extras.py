"""Unit tests for backend.jdbc_extras.

Hermetic -- no docker calls, no network. Each test:

  - Creates an InstallRecord in the real state store under a uuid-prefixed
    install_id so we never collide with other suites' fixtures or with a
    user's real installs on disk.
  - Points the InstallRecord at a per-test tmp_path so write operations
    land in pytest's tmp tree, not the user's workspace.
  - Cleans the store entry on tear-down (best-effort).

The four tests cover the public surface required to unblock the FastAPI
routes:

  1. enable writes the override file with the expected content (Postgres
     URL, MySQL URL, init container, volume mount on spark-iceberg).
  2. disable removes the override file from disk.
  3. is_jdbc_enabled flips False -> True -> False around enable/disable.
  4. jdbc_activate_command returns the exact `docker compose ... up -d`
     command shape the operator runs.
"""
from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from backend import jdbc_extras as jdbc_mod
from backend.state import store


def _make_install(tmp_path: Path) -> str:
    """Register a minimal InstallRecord via store.create() pointing at
    tmp_path, then transition it to READY (the only state the JDBC routes
    are required to operate against). Returns the install_id."""
    install_dir = tmp_path / "install"
    install_dir.mkdir(parents=True, exist_ok=True)
    rec = store.create(
        stack_id="udp-local-v0.2",
        host="local",
        install_dir=str(install_dir),
        steps=[],
    )
    # Drop straight to READY -- the JDBC routes only require the install
    # to exist with a non-RUNNING state; jdbc_extras itself doesn't check
    # state at all (the route layer does that).
    store.update_state(rec.install_id, "READY")
    return rec.install_id


@pytest.fixture()
def install_id(tmp_path: Path):
    """Set up a transient install + clean it up on tear-down so the on-disk
    state file doesn't accumulate test records between runs."""
    iid = _make_install(tmp_path)
    yield iid
    # Best-effort: drop the record out of the in-memory store so we don't
    # pollute sibling test suites. The store doesn't expose a public
    # delete, so we poke the internal dict under the lock.
    with store._lock:  # type: ignore[attr-defined]
        store._records.pop(iid, None)  # type: ignore[attr-defined]
        store._persist_locked(force=True)  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# 1. enable writes the override with the expected content
# ---------------------------------------------------------------------------

def test_enable_writes_override_with_expected_content(install_id, tmp_path):
    profile = jdbc_mod.JdbcExtrasProfile(
        include_postgres=True,
        include_mysql=True,
        postgres_driver_version="42.7.4",
        mysql_driver_version="9.0.0",
    )
    result = asyncio.run(jdbc_mod.enable_jdbc_extras(install_id, profile))

    # Result shape matches the contract (compose_file_path, activate_command,
    # postgres_pinned, mysql_pinned).
    assert result["postgres_pinned"] == "42.7.4"
    assert result["mysql_pinned"] == "9.0.0"
    assert result["compose_file_path"].endswith("docker-compose.jdbc.yml")
    assert "docker compose" in result["activate_command"]

    # File was actually written into the install_dir.
    override = Path(result["compose_file_path"])
    assert override.exists(), "override file should exist after enable"

    body = override.read_text(encoding="utf-8")

    # Must declare the init container and the spark-iceberg merge target.
    assert "jdbc-extras:" in body
    assert "spark-iceberg:" in body
    assert "curlimages/curl" in body
    # Maven Central URLs for both jars, with the requested versions.
    assert "postgresql-42.7.4.jar" in body
    assert "mysql-connector-j-9.0.0.jar" in body
    assert "repo1.maven.org" in body
    # Named volume declared once at top level + mounted on both services.
    assert "spark_jdbc_jars:" in body
    assert "/opt/spark/jars/jdbc" in body
    # Compose v2: no `version:` key at the top of the file.
    assert not body.lstrip().startswith("version:")


def test_enable_postgres_only_omits_mysql(install_id):
    """Confirm the include flags actually gate which jar URL appears."""
    profile = jdbc_mod.JdbcExtrasProfile(include_postgres=True,
                                         include_mysql=False)
    result = asyncio.run(jdbc_mod.enable_jdbc_extras(install_id, profile))
    body = Path(result["compose_file_path"]).read_text(encoding="utf-8")

    assert "postgresql-42.7.4.jar" in body
    assert "mysql-connector-j" not in body
    assert result["postgres_pinned"] == "42.7.4"
    assert result["mysql_pinned"] is None


# ---------------------------------------------------------------------------
# 2. disable removes the override
# ---------------------------------------------------------------------------

def test_disable_removes_override_file(install_id):
    profile = jdbc_mod.JdbcExtrasProfile()
    enable_result = asyncio.run(jdbc_mod.enable_jdbc_extras(install_id, profile))
    override = Path(enable_result["compose_file_path"])
    assert override.exists()

    disable_result = asyncio.run(jdbc_mod.disable_jdbc_extras(install_id))

    assert disable_result["disabled"] is True
    assert not override.exists(), "override file should be removed after disable"
    # Deactivate command surfaces the granular stop+rm (not `compose down`).
    assert "stop jdbc-extras" in disable_result["deactivate_command"]
    assert "rm -f jdbc-extras" in disable_result["deactivate_command"]


def test_disable_when_not_enabled_is_idempotent(install_id):
    """Calling disable on a non-enabled install reports disabled=False but
    does not raise -- matches the idempotency contract of caddy_tls /
    monitoring."""
    result = asyncio.run(jdbc_mod.disable_jdbc_extras(install_id))
    assert result["disabled"] is False
    # Deactivate command is still surfaced for the UI even on a no-op disable.
    assert "docker compose" in result["deactivate_command"]


# ---------------------------------------------------------------------------
# 3. is_jdbc_enabled reflects state
# ---------------------------------------------------------------------------

def test_is_jdbc_enabled_reflects_state(install_id):
    # Initially disabled.
    assert jdbc_mod.is_jdbc_enabled(install_id) is False

    # After enable, becomes True.
    asyncio.run(jdbc_mod.enable_jdbc_extras(install_id,
                                            jdbc_mod.JdbcExtrasProfile()))
    assert jdbc_mod.is_jdbc_enabled(install_id) is True

    # After disable, back to False.
    asyncio.run(jdbc_mod.disable_jdbc_extras(install_id))
    assert jdbc_mod.is_jdbc_enabled(install_id) is False


def test_is_jdbc_enabled_unknown_install_returns_false():
    """Unknown install_ids never raise from the status check -- callers
    use this for UI state and would crash on a 404."""
    assert jdbc_mod.is_jdbc_enabled("inst_does_not_exist") is False


# ---------------------------------------------------------------------------
# 4. jdbc_activate_command shape
# ---------------------------------------------------------------------------

def test_jdbc_activate_command_shape(install_id):
    cmd = jdbc_mod.jdbc_activate_command(install_id)
    # Exact shape: both compose files in order, target the init container.
    assert cmd == (
        "docker compose -f docker-compose.yml -f docker-compose.jdbc.yml "
        "up -d jdbc-extras"
    )


# ---------------------------------------------------------------------------
# Bonus: validation guard rails
# ---------------------------------------------------------------------------

def test_enable_rejects_empty_profile(install_id):
    """include_postgres=False AND include_mysql=False would write a no-op
    override -- enable refuses with a clear ValueError."""
    profile = jdbc_mod.JdbcExtrasProfile(include_postgres=False,
                                         include_mysql=False)
    with pytest.raises(ValueError, match="include_postgres"):
        asyncio.run(jdbc_mod.enable_jdbc_extras(install_id, profile))


def test_driver_version_rejects_shell_meta():
    """The version is spliced into a Maven URL + YAML; reject anything
    that could break either."""
    with pytest.raises(ValueError, match="illegal character"):
        jdbc_mod.JdbcExtrasProfile(postgres_driver_version="42.7.4; rm -rf /")
