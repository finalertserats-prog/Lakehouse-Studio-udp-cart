"""Opt-in RBAC layer wired into the running FastAPI app.

This is the runtime bridge from the v1.0 scaffold (``backend/v1/rbac.py`` +
``backend/v1/multi_tenant_schema.py``) to the live app. It is fully OPT-IN:
unless the ``LHS_RBAC_ENABLED`` env var is set to a truthy value, none of
this module's behaviour activates and the legacy single-shared-token auth in
``backend/main.py`` is untouched.

What this module owns
---------------------
- The ``User`` Pydantic model (id, email, role name, hashed token).
- A small SQLite layer that REUSES ``v1.multi_tenant_schema.init_schema`` so
  the schema stays single-sourced. We seed the four built-in roles and a
  single "default" tenant on first init.
- Token generation (32 chars, ``secrets.token_urlsafe``) + sha256 hashing.
  Plaintext tokens are returned to the CALLER exactly ONCE at creation time
  and are NEVER persisted anywhere on disk.
- Authentication: parse a ``Bearer`` / ``X-Studio-Token`` header, look the
  hashed token up in SQLite, return a ``User`` or ``None``.
- Permission check: ``required_permission(method, path)`` -> ``Permission``;
  the user's role must include it. Routes that aren't in the v1 route map
  are permitted (the v1 scaffold documents this fall-through behaviour).

What this module does NOT own
-----------------------------
- The legacy ``_require_auth`` (still defined in ``main.py``).
- Audit logging (the schema is there; wiring it up is a follow-on).
- Tenant separation. Every user lives under the single "default" tenant
  the schema migration creates. Multi-tenant is a v1.0 concern.
"""
from __future__ import annotations

import hashlib
import os
import secrets
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, EmailStr, Field

from .config import WORK_DIR
from .v1.multi_tenant_schema import init_schema, seed_builtin_roles
from .v1.rbac import BUILTIN_ROLES, Permission, has_permission, required_permission


# --------------------------------------------------------------------------- #
# Config                                                                       #
# --------------------------------------------------------------------------- #

# Single source of truth for the RBAC DB path. Lives next to state.json under
# WORK_DIR so backup/move semantics already in place for Studio data apply.
RBAC_DB_PATH: Path = WORK_DIR / "rbac.sqlite"

# Token length in URL-safe characters. 32 chars * ~6 bits/char ~= 192 bits of
# entropy — overkill for an operator API token but cheap.
_TOKEN_NBYTES = 24  # ~32 chars when base64-url-encoded

# Tenant name used by the opt-in scaffold. Multi-tenant is v1.0.
_DEFAULT_TENANT_NAME = "default"

_VALID_ROLE_NAMES: frozenset[str] = frozenset(BUILTIN_ROLES.keys())


# --------------------------------------------------------------------------- #
# Models                                                                       #
# --------------------------------------------------------------------------- #


class User(BaseModel):
    """INTERNAL user shape used inside the auth layer ONLY.

    ``api_token`` is the sha256 HASH of the plaintext token — never the
    plaintext value itself. Even hashed, this MUST NOT leave the auth
    boundary: route handlers return UserPublic instead. Exposing a hash
    in API responses widens the offline-brute-force surface.
    """

    user_id: str = Field(min_length=1)
    email: str = Field(min_length=3, max_length=320)
    role: str = Field(min_length=1)
    api_token: str = Field(min_length=64, max_length=64,
                           description="sha256 hex of the plaintext token; INTERNAL ONLY")


class UserPublic(BaseModel):
    """Wire shape returned by API routes. Excludes the token hash so
    nothing credential-shaped leaves the server (per Gemini v0.5.1 review).
    """

    user_id: str
    email: str
    role: str


def to_public(u: User) -> UserPublic:
    return UserPublic(user_id=u.user_id, email=u.email, role=u.role)


# --------------------------------------------------------------------------- #
# Helpers                                                                      #
# --------------------------------------------------------------------------- #


def is_rbac_enabled() -> bool:
    """Read LHS_RBAC_ENABLED at call time so tests can flip it via monkeypatch."""
    raw = (os.environ.get("LHS_RBAC_ENABLED") or "").strip().lower()
    return raw in ("1", "true", "yes", "on")


def _hash_token(plaintext: str) -> str:
    return hashlib.sha256(plaintext.encode("utf-8")).hexdigest()


def _generate_token() -> str:
    # token_urlsafe returns ~4/3 the byte count in URL-safe chars; 24 bytes
    # -> 32 chars (no padding).
    return secrets.token_urlsafe(_TOKEN_NBYTES)


def _connect() -> sqlite3.Connection:
    con = sqlite3.connect(str(RBAC_DB_PATH))
    con.execute("PRAGMA foreign_keys = ON")
    return con


def _ensure_default_tenant(con: sqlite3.Connection) -> str:
    cur = con.cursor()
    cur.execute("SELECT id FROM tenants WHERE name = ?", (_DEFAULT_TENANT_NAME,))
    row = cur.fetchone()
    if row:
        return row[0]
    tenant_id = f"tnt_{uuid.uuid4().hex[:12]}"
    cur.execute(
        "INSERT INTO tenants (id, name, billing_tier, created_at) VALUES (?, ?, ?, ?)",
        (tenant_id, _DEFAULT_TENANT_NAME, "free", time.time()),
    )
    con.commit()
    return tenant_id


def _ensure_token_column(con: sqlite3.Connection) -> None:
    """The v1 schema's ``users`` table doesn't carry an api_token column —
    that's a v1.0-multi-tenant concern (per-user sessions / JWT). The opt-in
    RBAC layer adds it idempotently so we don't fork the v1 schema."""
    cur = con.cursor()
    cur.execute("PRAGMA table_info(users)")
    cols = {row[1] for row in cur.fetchall()}
    if "token_hash" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN token_hash TEXT")
        con.commit()
    # Index lookups by token go through token_hash; unique on (tenant_id,
    # token_hash) so the same hash can't collide cross-tenant either.
    cur.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_token_hash "
        "ON users(token_hash) WHERE token_hash IS NOT NULL"
    )
    con.commit()


# --------------------------------------------------------------------------- #
# Init                                                                         #
# --------------------------------------------------------------------------- #


def init_rbac_db(sqlite_path: Optional[Path] = None) -> Path:
    """Idempotently create the RBAC schema + seed roles + default tenant.

    Reuses ``backend/v1/multi_tenant_schema.init_schema`` so the table
    definitions remain single-sourced.
    """
    global RBAC_DB_PATH
    path = Path(sqlite_path) if sqlite_path is not None else RBAC_DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    init_schema(path)
    seed_builtin_roles(path)
    # Override the module-level path so subsequent helpers use the
    # caller-provided location (test isolation).
    RBAC_DB_PATH = path
    con = _connect()
    try:
        _ensure_default_tenant(con)
        _ensure_token_column(con)
    finally:
        con.close()
    return path


# --------------------------------------------------------------------------- #
# User management                                                              #
# --------------------------------------------------------------------------- #


def _row_to_user(row: tuple) -> User:
    user_id, email, role, token_hash = row
    return User(user_id=user_id, email=email, role=role, api_token=token_hash or "0" * 64)


async def create_user(email: str, role: str) -> tuple[User, str]:
    """Create a user with a freshly-minted API token.

    Returns ``(user, plaintext_token)``. The plaintext token is returned
    ONCE — the caller is responsible for surfacing it to the operator. The
    DB never sees the plaintext.
    """
    if role not in _VALID_ROLE_NAMES:
        raise ValueError(f"unknown role {role!r}; valid: {sorted(_VALID_ROLE_NAMES)}")
    email = (email or "").strip()
    if not email or "@" not in email:
        raise ValueError("email is required and must contain '@'")

    plaintext = _generate_token()
    token_hash = _hash_token(plaintext)
    user_id = f"usr_{uuid.uuid4().hex[:12]}"

    con = _connect()
    try:
        tenant_id = _ensure_default_tenant(con)
        _ensure_token_column(con)
        cur = con.cursor()
        cur.execute(
            "INSERT INTO users (id, tenant_id, email, role, created_at, token_hash) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (user_id, tenant_id, email, role, time.time(), token_hash),
        )
        con.commit()
    except sqlite3.IntegrityError as e:
        raise ValueError(f"could not create user: {e}") from e
    finally:
        con.close()

    user = User(user_id=user_id, email=email, role=role, api_token=token_hash)
    return user, plaintext


async def list_users() -> list[User]:
    con = _connect()
    try:
        _ensure_token_column(con)
        cur = con.cursor()
        cur.execute("SELECT id, email, role, token_hash FROM users ORDER BY created_at ASC")
        rows = cur.fetchall()
    finally:
        con.close()
    return [_row_to_user(r) for r in rows]


async def get_user(user_id: str) -> Optional[User]:
    con = _connect()
    try:
        _ensure_token_column(con)
        cur = con.cursor()
        cur.execute("SELECT id, email, role, token_hash FROM users WHERE id = ?", (user_id,))
        row = cur.fetchone()
    finally:
        con.close()
    return _row_to_user(row) if row else None


async def delete_user(user_id: str) -> bool:
    con = _connect()
    try:
        cur = con.cursor()
        cur.execute("DELETE FROM users WHERE id = ?", (user_id,))
        changed = cur.rowcount
        con.commit()
    finally:
        con.close()
    return bool(changed)


async def count_users() -> int:
    con = _connect()
    try:
        cur = con.cursor()
        cur.execute("SELECT COUNT(*) FROM users")
        (n,) = cur.fetchone()
    finally:
        con.close()
    return int(n)


# --------------------------------------------------------------------------- #
# Authentication + authorisation                                               #
# --------------------------------------------------------------------------- #


def _extract_token(authorization_header: Optional[str], x_studio_token: Optional[str] = None) -> Optional[str]:
    if authorization_header:
        h = authorization_header.strip()
        if h.lower().startswith("bearer "):
            return h.split(" ", 1)[1].strip() or None
        # bare token in Authorization is also accepted to mirror _require_auth
        return h or None
    if x_studio_token:
        s = x_studio_token.strip()
        return s or None
    return None


async def authenticate(
    authorization_header: Optional[str],
    x_studio_token: Optional[str] = None,
) -> Optional[User]:
    """Constant-time-ish token lookup.

    Returns the matching ``User`` or ``None``. We hash first and let SQLite
    do an indexed equality lookup — there's no string-compare loop over all
    tokens.
    """
    plaintext = _extract_token(authorization_header, x_studio_token)
    if not plaintext:
        return None
    token_hash = _hash_token(plaintext)
    con = _connect()
    try:
        _ensure_token_column(con)
        cur = con.cursor()
        cur.execute(
            "SELECT id, email, role, token_hash FROM users WHERE token_hash = ?",
            (token_hash,),
        )
        row = cur.fetchone()
    finally:
        con.close()
    if not row:
        return None
    # Defence in depth: constant-time compare on the way out so timing
    # attacks against the DB-index lookup don't leak hash prefixes.
    if not secrets.compare_digest(row[3] or "", token_hash):
        return None
    return _row_to_user(row)


async def require_permission(user: User, route_path: str, method: str = "GET") -> bool:
    """Check whether ``user`` may invoke ``method route_path``.

    Resolution rules (matches the v1 scaffold's documented intent):
      1. Look up the v1 ``ROUTE_PERMISSIONS`` map for an exact ``(method,
         path)`` match. If absent the route has no permission gate -> True.
      2. Resolve the user's role from ``BUILTIN_ROLES``. Unknown role -> False.
      3. The role must include the required permission.
    """
    perm: Optional[Permission] = required_permission(method, route_path)
    if perm is None:
        return True
    role = BUILTIN_ROLES.get(user.role)
    if role is None:
        return False
    return has_permission(role, perm)
