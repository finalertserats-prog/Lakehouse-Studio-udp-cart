"""Multi-tenant SQLite schema + JSON→SQLite migration script.

Today: ``backend/state.py`` keeps every install in a single JSON file
(``state.json``), implicitly single-tenant. That's fine for the v0.x
single-operator pilot but won't survive v1.0 (per-customer auth, audit log,
billing tier, RBAC).

This module defines the v1.0 schema as raw ``CREATE TABLE`` strings (no
ORM — we want to keep migrations transparent and the v1.0 session free to
pick its own ORM later) and a one-shot ``migrate_from_json`` script that
reads the current ``state.json`` and bulk-loads it into a fresh SQLite DB
under a single "default" tenant.

NOT WIRED. ``store`` in ``state.py`` continues to be the source of truth
for the running app. The v1.0 session will run ``migrate_from_json`` once,
swap ``state.store`` for a SQLite-backed implementation, and update
``main.py`` to inject tenant + user context into every request.
"""
from __future__ import annotations
import json
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Optional


# --------------------------------------------------------------------------- #
# Schema                                                                       #
# --------------------------------------------------------------------------- #

SCHEMA: dict[str, str] = {
    "tenants": """
        CREATE TABLE IF NOT EXISTS tenants (
            id            TEXT PRIMARY KEY,
            name          TEXT NOT NULL UNIQUE,
            billing_tier  TEXT NOT NULL DEFAULT 'free',
            created_at    REAL NOT NULL
        )
    """,
    "users": """
        CREATE TABLE IF NOT EXISTS users (
            id          TEXT PRIMARY KEY,
            tenant_id   TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
            email       TEXT NOT NULL,
            role        TEXT NOT NULL DEFAULT 'viewer',
            created_at  REAL NOT NULL,
            UNIQUE(tenant_id, email)
        )
    """,
    "roles": """
        CREATE TABLE IF NOT EXISTS roles (
            id           TEXT PRIMARY KEY,
            name         TEXT NOT NULL UNIQUE,
            permissions  TEXT NOT NULL  -- JSON array of Permission names
        )
    """,
    # Mirrors backend/models.py InstallRecord shape, plus tenant_id.
    # Steps / outputs stay as JSON columns — they're shaped data the v0.x
    # app already serialises that way.
    "installs": """
        CREATE TABLE IF NOT EXISTS installs (
            id            TEXT PRIMARY KEY,                    -- install_id
            tenant_id     TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
            stack_id      TEXT NOT NULL,
            host          TEXT NOT NULL,
            install_dir   TEXT NOT NULL,
            state         TEXT NOT NULL,
            created_at    REAL NOT NULL,
            updated_at    REAL NOT NULL,
            steps_json    TEXT NOT NULL DEFAULT '[]',
            outputs_json  TEXT NOT NULL DEFAULT '{}',
            error         TEXT,
            lake_name     TEXT,
            goal          TEXT,
            cart_json     TEXT NOT NULL DEFAULT '[]'
        )
    """,
    "audit_log": """
        CREATE TABLE IF NOT EXISTS audit_log (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            tenant_id        TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
            user_id          TEXT REFERENCES users(id) ON DELETE SET NULL,
            action           TEXT NOT NULL,           -- e.g. install.create, backup.restore
            resource_type    TEXT NOT NULL,           -- install, backup, user, role
            resource_id      TEXT,
            ts               REAL NOT NULL,
            ip               TEXT,
            redacted_payload TEXT                     -- JSON, secrets already scrubbed
        )
    """,
}

INDEXES: list[str] = [
    "CREATE INDEX IF NOT EXISTS idx_users_tenant ON users(tenant_id)",
    "CREATE INDEX IF NOT EXISTS idx_installs_tenant ON installs(tenant_id)",
    "CREATE INDEX IF NOT EXISTS idx_installs_state ON installs(state)",
    "CREATE INDEX IF NOT EXISTS idx_audit_tenant_ts ON audit_log(tenant_id, ts DESC)",
    "CREATE INDEX IF NOT EXISTS idx_audit_resource ON audit_log(resource_type, resource_id)",
]


# --------------------------------------------------------------------------- #
# Init + migrate                                                               #
# --------------------------------------------------------------------------- #

def init_schema(sqlite_path: str | Path) -> None:
    """Create tables + indexes if they don't already exist. Idempotent."""
    path = Path(sqlite_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(path))
    try:
        con.execute("PRAGMA foreign_keys = ON")
        cur = con.cursor()
        for stmt in SCHEMA.values():
            cur.execute(stmt)
        for stmt in INDEXES:
            cur.execute(stmt)
        con.commit()
    finally:
        con.close()


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def migrate_from_json(
    json_path: str | Path,
    sqlite_path: str | Path,
    default_tenant_name: str = "default",
) -> dict:
    """Bulk-load ``state.json`` into a fresh SQLite DB.

    Idempotent: re-running with the same input is a no-op for already-present
    install_ids and the default tenant. Returns ``{"tenants": N, "installs":
    N, "skipped": N}``.
    """
    json_path = Path(json_path)
    sqlite_path = Path(sqlite_path)

    init_schema(sqlite_path)

    raw: dict = {}
    if json_path.exists():
        try:
            raw = json.loads(json_path.read_text(encoding="utf-8"))
        except Exception as e:
            raise ValueError(f"could not parse {json_path}: {e}") from e

    counts = {"tenants": 0, "installs": 0, "skipped": 0}

    con = sqlite3.connect(str(sqlite_path))
    try:
        con.execute("PRAGMA foreign_keys = ON")
        cur = con.cursor()

        # 1. Ensure default tenant exists.
        cur.execute("SELECT id FROM tenants WHERE name = ?", (default_tenant_name,))
        row = cur.fetchone()
        if row:
            tenant_id = row[0]
        else:
            tenant_id = _new_id("tnt")
            cur.execute(
                "INSERT INTO tenants (id, name, billing_tier, created_at) VALUES (?, ?, ?, ?)",
                (tenant_id, default_tenant_name, "free", time.time()),
            )
            counts["tenants"] = 1

        # 2. Bulk-insert installs.
        for install_id, rec in raw.items():
            if not isinstance(rec, dict):
                counts["skipped"] += 1
                continue
            cur.execute("SELECT 1 FROM installs WHERE id = ?", (install_id,))
            if cur.fetchone():
                counts["skipped"] += 1
                continue
            cur.execute(
                """INSERT INTO installs
                   (id, tenant_id, stack_id, host, install_dir, state,
                    created_at, updated_at, steps_json, outputs_json,
                    error, lake_name, goal, cart_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    install_id,
                    tenant_id,
                    str(rec.get("stack_id", "")),
                    str(rec.get("host", "")),
                    str(rec.get("install_dir", "")),
                    str(rec.get("state", "DRAFT")),
                    float(rec.get("created_at") or time.time()),
                    float(rec.get("updated_at") or time.time()),
                    json.dumps(rec.get("steps") or []),
                    json.dumps(rec.get("outputs") or {}),
                    rec.get("error"),
                    rec.get("lake_name"),
                    rec.get("goal"),
                    json.dumps(rec.get("cart") or []),
                ),
            )
            counts["installs"] += 1

        con.commit()
    finally:
        con.close()

    return counts


def seed_builtin_roles(sqlite_path: str | Path) -> int:
    """Insert the four built-in roles (OWNER/ADMIN/OPERATOR/VIEWER) defined
    in ``backend/v1/rbac.py``. Idempotent. Returns number inserted."""
    # Import inside the function so this module stays import-cheap and to
    # honour the "no cross-module imports outside v1" boundary.
    from .rbac import BUILTIN_ROLES

    init_schema(sqlite_path)
    inserted = 0
    con = sqlite3.connect(str(sqlite_path))
    try:
        cur = con.cursor()
        for role in BUILTIN_ROLES.values():
            cur.execute("SELECT 1 FROM roles WHERE name = ?", (role.name,))
            if cur.fetchone():
                continue
            cur.execute(
                "INSERT INTO roles (id, name, permissions) VALUES (?, ?, ?)",
                (
                    _new_id("rol"),
                    role.name,
                    json.dumps(sorted(p.name for p in role.permissions)),
                ),
            )
            inserted += 1
        con.commit()
    finally:
        con.close()
    return inserted
