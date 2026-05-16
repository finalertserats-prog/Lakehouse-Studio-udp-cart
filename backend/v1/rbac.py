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
    BACKUP_CREATE    = "backup.create"
    BACKUP_RESTORE   = "backup.restore"
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
    ("POST",   "/api/installs"):                 Permission.INSTALL_CREATE,
    ("GET",    "/api/installs"):                 Permission.INSTALL_VIEW,
    ("GET",    "/api/installs/{install_id}"):    Permission.INSTALL_VIEW,
    ("DELETE", "/api/installs/{install_id}"):    Permission.INSTALL_DELETE,
    ("POST",   "/api/installs/{install_id}/backup"):   Permission.BACKUP_CREATE,
    ("POST",   "/api/installs/{install_id}/restore"):  Permission.BACKUP_RESTORE,
    ("POST",   "/api/installs/{install_id}/upgrade"):  Permission.UPGRADE_EXECUTE,
    ("POST",   "/api/installs/{install_id}/sql"):      Permission.SQL_RUN,
    ("GET",    "/api/audit"):                    Permission.AUDIT_VIEW,
    ("PUT",    "/api/settings"):                 Permission.SETTINGS_WRITE,
    ("GET",    "/api/billing"):                  Permission.BILLING_VIEW,
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
