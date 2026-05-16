"""Role-based access control for the v1.0 architecture.

Today: ``backend/main.py`` enforces a single shared bearer token
(``LHS_AUTH_TOKEN``) — anyone who holds it can do anything. That's
acceptable for a single-operator pilot, broken for multi-tenant v1.0.

This module defines the v1.0 ``Permission`` enum, ``Role`` model, four
built-in roles, a route → permission map, and a stub FastAPI dependency
that always returns True. The stub documents what the real check will do
once ``state.py`` is multi-tenant and every request carries a user id.

NOT WIRED. ``main.py`` continues to use ``_require_auth`` until the v1.0
session swaps it for ``rbac_check``.
"""
from __future__ import annotations
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class Permission(Enum):
    INSTALL_CREATE   = "install.create"
    INSTALL_VIEW     = "install.view"
    INSTALL_DELETE   = "install.delete"
    # Install-scoped state mutations that aren't create/delete:
    # cancel, retry, skip, rollback, uninstall, control, plus per-install
    # config changes like TLS, JDBC, monitoring, destinations, data
    # sources, DQ checks. OPERATOR has this; VIEWER does not.
    INSTALL_MUTATE   = "install.mutate"
    BACKUP_CREATE    = "backup.create"
    BACKUP_RESTORE   = "backup.restore"
    BACKUP_DELETE    = "backup.delete"
    UPGRADE_EXECUTE  = "upgrade.execute"
    SQL_RUN          = "sql.run"
    AUDIT_VIEW       = "audit.view"
    SETTINGS_WRITE   = "settings.write"
    BILLING_VIEW     = "billing.view"


class Role(BaseModel):
    name: str = Field(min_length=1, max_length=64)
    permissions: set[Permission] = Field(default_factory=set)

    # Pydantic v2: Enum values aren't hashable through the JSON pipeline by
    # default. Keep this model in-memory only (don't serialise to a dict
    # blindly); the SQLite layer in multi_tenant_schema serialises permission
    # NAMES as a JSON array, not the model itself.
    model_config = {"arbitrary_types_allowed": True}


# Convenience: all permissions, for OWNER + helpers.
_ALL = set(Permission)


BUILTIN_ROLES: dict[str, Role] = {
    "OWNER":    Role(name="OWNER",    permissions=_ALL),
    "ADMIN":    Role(name="ADMIN",    permissions=_ALL - {Permission.BILLING_VIEW}),
    "OPERATOR": Role(
        name="OPERATOR",
        permissions=_ALL - {
            Permission.SETTINGS_WRITE,
            Permission.INSTALL_DELETE,
            Permission.BILLING_VIEW,
        },
    ),
    "VIEWER":   Role(
        name="VIEWER",
        permissions={
            Permission.INSTALL_VIEW,
            Permission.AUDIT_VIEW,
        },
    ),
}


def has_permission(role: Role, perm: Permission) -> bool:
    return perm in role.permissions


# Maps API routes to the permission they require. The v1.0 session will read
# this in the real ``rbac_check`` and 403 anything that doesn't match. Keep
# the matching simple (method + path prefix) — wildcards / regex matching is
# the v1.0 session's call.
ROUTE_PERMISSIONS: dict[tuple[str, str], Permission] = {
    # --- Original scaffold entries (some paths historically stale, kept
    # for backward compatibility with any consumer that still imports the
    # constant by name) ----------------------------------------------------
    ("POST",   "/api/installs"):                 Permission.INSTALL_CREATE,
    ("GET",    "/api/installs"):                 Permission.INSTALL_VIEW,
    ("GET",    "/api/installs/{install_id}"):    Permission.INSTALL_VIEW,
    ("DELETE", "/api/installs/{install_id}"):    Permission.INSTALL_DELETE,
    ("POST",   "/api/installs/{install_id}/sql"):      Permission.SQL_RUN,
    ("GET",    "/api/audit"):                    Permission.AUDIT_VIEW,
    ("PUT",    "/api/settings"):                 Permission.SETTINGS_WRITE,
    ("GET",    "/api/billing"):                  Permission.BILLING_VIEW,

    # --- 2026-05-17 hardening — cover the WRITE-RISK routes flagged in
    # the docs/RBAC.md matrix. Without these, VIEWER could call any
    # state-changing route below. -------------------------------------------
    # Install lifecycle mutations (cancel / retry / skip / rollback /
    # uninstall / control). All are per-install state changes that
    # OPERATOR must be able to do but VIEWER must not.
    ("POST",   "/api/installs/{install_id}/cancel"):         Permission.INSTALL_MUTATE,
    ("POST",   "/api/installs/{install_id}/steps/retry"):    Permission.INSTALL_MUTATE,
    ("POST",   "/api/installs/{install_id}/steps/skip"):     Permission.INSTALL_MUTATE,
    ("POST",   "/api/installs/{install_id}/steps/rollback"): Permission.INSTALL_MUTATE,
    ("POST",   "/api/installs/{install_id}/uninstall"):      Permission.INSTALL_MUTATE,
    ("POST",   "/api/installs/{install_id}/control"):        Permission.INSTALL_MUTATE,
    # Backups + DR
    ("POST",   "/api/installs/{install_id}/backups"):          Permission.BACKUP_CREATE,
    ("PUT",    "/api/installs/{install_id}/backups/schedule"): Permission.BACKUP_CREATE,
    ("POST",   "/api/backups/{backup_id}/restore"):            Permission.BACKUP_RESTORE,
    ("DELETE", "/api/backups/{backup_id}"):                    Permission.BACKUP_DELETE,
    # Upgrade execution (real path; old `/upgrade` entry above is stale)
    ("POST",   "/api/installs/{install_id}/upgrades/execute"): Permission.UPGRADE_EXECUTE,
    # Per-install config: TLS, security, sidecars (caddy/jdbc/monitoring).
    # These are install-scoped state, not global Studio settings, so they
    # belong on INSTALL_MUTATE (OPERATOR-callable) not SETTINGS_WRITE.
    ("POST",   "/api/installs/{install_id}/tls/generate"):                Permission.INSTALL_MUTATE,
    ("DELETE", "/api/tls/certs/{cert_id}"):                               Permission.INSTALL_MUTATE,
    ("POST",   "/api/installs/{install_id}/security/rotate-password"):    Permission.INSTALL_MUTATE,
    ("POST",   "/api/installs/{install_id}/tls/caddy/enable"):            Permission.INSTALL_MUTATE,
    ("POST",   "/api/installs/{install_id}/tls/caddy/disable"):           Permission.INSTALL_MUTATE,
    ("POST",   "/api/installs/{install_id}/jdbc/enable"):                 Permission.INSTALL_MUTATE,
    ("POST",   "/api/installs/{install_id}/jdbc/disable"):                Permission.INSTALL_MUTATE,
    ("POST",   "/api/installs/{install_id}/monitoring/enable"):           Permission.INSTALL_MUTATE,
    ("POST",   "/api/installs/{install_id}/monitoring/disable"):          Permission.INSTALL_MUTATE,
    # Ingest jobs (CSV upload + Postgres/MySQL pull). These create work,
    # treat them as INSTALL_CREATE-tier so the same OPERATOR gate applies.
    ("POST",   "/api/installs/{install_id}/uploads"):           Permission.INSTALL_CREATE,
    ("POST",   "/api/installs/{install_id}/ingest"):            Permission.INSTALL_CREATE,
    ("POST",   "/api/installs/{install_id}/ingest/postgres"):   Permission.INSTALL_CREATE,
    ("POST",   "/api/installs/{install_id}/ingest/mysql"):      Permission.INSTALL_CREATE,
    # Sources + Destinations: per-install registration with stored
    # encrypted credentials. Same OPERATOR-callable tier.
    ("POST",   "/api/installs/{install_id}/data-sources"):          Permission.INSTALL_MUTATE,
    ("DELETE", "/api/data-sources/{source_id}"):                    Permission.INSTALL_MUTATE,
    ("POST",   "/api/installs/{install_id}/destinations"):          Permission.INSTALL_MUTATE,
    ("POST",   "/api/destinations/{destination_id}/provision"):     Permission.INSTALL_MUTATE,
    ("DELETE", "/api/destinations/{destination_id}"):               Permission.INSTALL_MUTATE,
    # Data Quality checks — per-install rules.
    ("POST",   "/api/installs/{install_id}/dq/checks"):  Permission.INSTALL_MUTATE,
    ("DELETE", "/api/dq/checks/{check_id}"):             Permission.INSTALL_MUTATE,
}


def required_permission(method: str, route: str) -> Optional[Permission]:
    """Look up the permission a route needs. ``None`` = no permission gate
    (e.g. ``/healthz``, ``/api/auth/status``). The v1.0 session may switch to
    pattern matching — for now an exact-match lookup is enough to capture
    the protected surface."""
    return ROUTE_PERMISSIONS.get((method.upper(), route))


def rbac_check(request) -> bool:  # pragma: no cover - scaffold stub
    """STUB FastAPI dependency. Always returns True in the scaffold.

    The v1.0 implementation will:
      1. Extract the authenticated user from ``request.state.user`` (set by
         an upstream auth middleware that swaps the current shared-token
         check for per-user sessions / JWT / OIDC).
      2. Load that user's ``Role`` from the ``users`` + ``roles`` SQLite
         tables (see ``multi_tenant_schema.py``).
      3. Look up ``required_permission(request.method, request.scope["route"]
         .path)``.
      4. ``has_permission(role, perm)`` → True passes the dependency, False
         raises ``HTTPException(403)``.
      5. Write an entry into ``audit_log`` for every state-changing call.

    Wiring this in is a one-liner change in ``main.py``: swap ``AuthDep =
    Depends(_require_auth)`` for ``AuthDep = Depends(rbac_check)``. The
    surface area of ``dependencies=[AuthDep, CatalogOk]`` stays the same.
    """
    return True
