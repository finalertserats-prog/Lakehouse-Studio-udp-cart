"""Insyght-specific connector logic.

Insyght is a conversational BI surface that sits on top of a SQL warehouse.
For Lakehouse Studio the default integration is `sql_pull` against the
StarRocks Frontend's MySQL protocol on port 9030.

This module is intentionally narrow:
  - default_config(install_id)          — pre-fills the destination config
  - provision_sql_pull(...)             — creates the read-only DB user
  - provision_push_api(...)             — STUB (real Insyght API spec TBD)
  - connection_instructions(dest)       — UI text shown to operators

Why a SEPARATE admin-SQL helper instead of extending sql_editor.run_user_sql?
  sql_editor.py has a hard read-only invariant (its allow-list rejects
  CREATE/GRANT). We keep that invariant intact and do admin SQL here through
  a dedicated `docker exec udp-starrocks-fe mysql ...` path. The verbs are
  whitelisted ("CREATE USER" / "GRANT") and identifiers are validated against
  a strict regex before any string interpolation hits the shell.
"""
from __future__ import annotations

import asyncio
import logging
import re
import shutil
from typing import Any, Optional

from .destinations import Destination, _decrypt_credentials


log = logging.getLogger("lhs.insyght")


INSYGHT_DEFAULTS: dict[str, Any] = {
    "connection_mode": "sql_pull",
    "starrocks_user_role": "insyght_reader",
    "starrocks_host": "127.0.0.1",
    "starrocks_mysql_port": 9030,
    "starrocks_database": "udp",
    "container_name": "udp-starrocks-fe",
}


# Strict identifier regex for DB usernames. We use a TIGHT pattern (no dot,
# no hyphen) because the username gets interpolated into a SQL statement —
# even with quoting it's safer to forbid anything that looks like syntax.
_DB_IDENT_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,62}$")

# Reasonable password ceiling. The lower bound comes from the strength
# checker in data_sources (12 chars + complexity).
_PW_MAX_LEN = 256


def default_config(install_id: str) -> dict[str, Any]:
    """Return a pre-filled Insyght destination config for this install.

    The operator just supplies the password (and optionally tweaks the
    StarRocks host if their stack runs somewhere non-default).
    """
    return {
        "kind": "insyght",
        "connection_mode": INSYGHT_DEFAULTS["connection_mode"],
        "host": INSYGHT_DEFAULTS["starrocks_host"],
        "port": INSYGHT_DEFAULTS["starrocks_mysql_port"],
        "database": INSYGHT_DEFAULTS["starrocks_database"],
        "username": INSYGHT_DEFAULTS["starrocks_user_role"],
        "install_id": install_id,
    }


def _validate_db_username(name: str) -> str:
    if not isinstance(name, str) or not _DB_IDENT_RE.match(name):
        raise ValueError(
            f"db username {name!r}: must start with a letter and match "
            "^[A-Za-z][A-Za-z0-9_]{0,62}$"
        )
    return name


def _validate_db_database(name: str) -> str:
    # StarRocks DB names follow MySQL rules — same strict pattern as user.
    if not isinstance(name, str) or not _DB_IDENT_RE.match(name):
        raise ValueError(
            f"database name {name!r}: must match ^[A-Za-z][A-Za-z0-9_]{{0,62}}$"
        )
    return name


def _validate_password_for_sql(pw: str) -> str:
    if not isinstance(pw, str) or not pw:
        raise ValueError("password is required for sql_pull provisioning")
    if len(pw) > _PW_MAX_LEN:
        raise ValueError(f"password exceeds max length ({_PW_MAX_LEN})")
    if any(ord(c) < 32 for c in pw):
        raise ValueError("password contains control characters")
    # MySQL string literals use single quotes. We refuse single quotes,
    # backslashes, and other escape chars so the interpolation stays safe
    # even if the caller forgot to escape.
    if "'" in pw or "\\" in pw or "\x00" in pw:
        raise ValueError(
            "password contains characters not supported here "
            "(single quote, backslash, NUL) — choose another"
        )
    return pw


async def _run_admin_sql(sql: str, container: str = None,
                         timeout: int = 30) -> dict[str, Any]:
    """Run an ADMIN SQL statement (CREATE USER / GRANT) against StarRocks.

    Separate from sql_editor.run_user_sql which is intentionally read-only.
    Caller is responsible for validating every identifier interpolated into
    `sql` BEFORE handing the string to this function — we do no parsing.
    """
    container = container or INSYGHT_DEFAULTS["container_name"]
    if shutil.which("docker") is None:
        return {
            "success": False,
            "error": "docker CLI not on PATH (this Studio host can't reach docker)",
        }
    cmd = [
        "docker", "exec", "-i", container,
        "mysql", "-h", "127.0.0.1", "-P", "9030", "-u", "root",
        "--batch", "--raw", "-e", sql,
    ]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except (FileNotFoundError, NotImplementedError, OSError) as e:
        return {"success": False, "error": f"failed to spawn subprocess: {e}"}

    try:
        stdout_b, stderr_b = await asyncio.wait_for(
            proc.communicate(), timeout=timeout,
        )
    except asyncio.TimeoutError:
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        try:
            await asyncio.wait_for(proc.wait(), timeout=5)
        except asyncio.TimeoutError:
            pass
        return {"success": False, "error": f"admin SQL timed out after {timeout}s"}

    out = stdout_b.decode("utf-8", "replace")
    err = stderr_b.decode("utf-8", "replace")
    if proc.returncode != 0:
        return {
            "success": False,
            "error": (err.strip() or out.strip()
                      or f"mysql exited {proc.returncode}"),
        }
    return {"success": True, "stdout": out.strip(), "stderr": err.strip() or None}


async def provision_sql_pull(install_id: str,
                             dest_config: dict[str, Any],
                             credentials: Optional[dict[str, Any]] = None,
                             ) -> dict[str, Any]:
    """Create the per-destination StarRocks user and grant SELECT.

    Runs (effectively):
        CREATE USER '<user>'@'%' IDENTIFIED BY '<pw>';
        GRANT SELECT_PRIV ON <db>.* TO '<user>'@'%';

    Returns `{success, user_created, grants, error}`. The actual password
    NEVER appears in the response.
    """
    username = _validate_db_username(
        dest_config.get("username") or INSYGHT_DEFAULTS["starrocks_user_role"]
    )
    database = _validate_db_database(
        dest_config.get("database") or INSYGHT_DEFAULTS["starrocks_database"]
    )
    pw_blob = dict(credentials or {})
    pw = _validate_password_for_sql(pw_blob.get("password", ""))

    # Build the two statements. Identifiers were validated above; pw was
    # validated to contain no single-quotes, backslashes, or NULs.
    # StarRocks uses MySQL-compatible syntax for CREATE USER + GRANT.
    create_stmt = f"CREATE USER '{username}'@'%' IDENTIFIED BY '{pw}'"
    grant_stmt = f"GRANT SELECT_PRIV ON {database}.* TO '{username}'@'%'"

    # Execute as two statements separated by ; so MySQL runs both in one
    # session. CREATE USER + GRANT are independent — if CREATE fails on
    # "user already exists" we still want to retry the GRANT idempotently.
    sql = f"{create_stmt}; {grant_stmt};"
    result = await _run_admin_sql(sql)
    if not result.get("success"):
        # Soft-handle "user already exists": fall back to grant-only so
        # operators can re-run provisioning after a config tweak.
        err = (result.get("error") or "").lower()
        if "already exists" in err or "duplicate" in err:
            grant_only = await _run_admin_sql(grant_stmt)
            if grant_only.get("success"):
                return {
                    "success": True,
                    "user_created": False,
                    "user_already_existed": True,
                    "grants": [f"SELECT_PRIV ON {database}.* TO '{username}'@'%'"],
                    "message": (
                        f"user {username!r} already existed; refreshed grants"
                    ),
                    "error": None,
                }
        return {
            "success": False,
            "user_created": False,
            "grants": [],
            "error": result.get("error"),
        }
    return {
        "success": True,
        "user_created": True,
        "user_already_existed": False,
        "grants": [f"SELECT_PRIV ON {database}.* TO '{username}'@'%'"],
        "message": f"created user {username!r} and granted SELECT on {database}.*",
        "error": None,
    }


async def provision_push_api(install_id: str,
                             dest_config: dict[str, Any],
                             credentials: Optional[dict[str, Any]] = None,
                             ) -> dict[str, Any]:
    """STUB for v0.6.1.

    Real provisioning needs the Insyght webhook handshake spec — what
    fields they require in the registration call, what shape of response
    they return, and the schema of events they want emitted. None of that
    is published yet (user input pending).
    """
    return {
        "success": False,
        "stub": True,
        "message": (
            "push_api provisioning is a stub in v0.6. The real handshake "
            "with Insyght's webhook registration endpoint requires their "
            "published API spec (event schema, auth scheme, retry policy). "
            "Track this in the v0.6.1 milestone. Until then, use "
            "connection_mode=sql_pull which is the supported default."
        ),
        "error": "not_implemented",
    }


def connection_instructions(dest: Destination) -> list[str]:
    """Step-by-step strings the UI shows for the Connect Insyght card."""
    cfg = dest.config or {}
    host = cfg.get("host") or INSYGHT_DEFAULTS["starrocks_host"]
    port = cfg.get("port") or INSYGHT_DEFAULTS["starrocks_mysql_port"]
    database = cfg.get("database") or INSYGHT_DEFAULTS["starrocks_database"]
    user = cfg.get("username") or INSYGHT_DEFAULTS["starrocks_user_role"]

    if dest.connection_mode == "sql_pull":
        return [
            "1. Open Insyght → Data Sources → Add → MySQL.",
            f"2. Host: {host}",
            f"3. Port: {port}",
            f"4. Database: {database}",
            f"5. User: {user}",
            "6. Password: use the value you supplied when creating this "
            "destination — Studio never stores it in cleartext or echoes "
            "it back.",
            "7. Test the connection inside Insyght → Save.",
            "8. Optional: pin a default workspace so future Insyght "
            "queries land in this warehouse by default.",
        ]
    if dest.connection_mode == "push_api":
        url = cfg.get("url") or cfg.get("endpoint_url") or "<not set>"
        return [
            "PUSH API mode is a v0.6.1 preview — not yet supported end-to-end.",
            f"1. Insyght webhook URL (to receive events): {url}",
            "2. Bearer token is stored encrypted; rotate via destination delete + recreate.",
            "3. Once Insyght publishes their public webhook spec, Studio "
            "will register automatically and start streaming Iceberg "
            "commit events.",
        ]
    return [
        f"Connection mode {dest.connection_mode!r} is not currently supported "
        "for Insyght. Use sql_pull."
    ]
