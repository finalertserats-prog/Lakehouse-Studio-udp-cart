"""CLI helper to bootstrap the very first RBAC user.

Once RBAC is enabled (``LHS_RBAC_ENABLED=true``), every API call needs a
real user/token — but there's no way to *create* a user via the API
without first having one. This module breaks the chicken-and-egg by
seeding exactly one OWNER from the command line, BEFORE the API is open
to traffic.

Usage:

    python -m backend.bootstrap_rbac --email ops@example.com --role OWNER

Safety properties:

  - Refuses to run if any user already exists. There is no "reset"; once
    the first OWNER exists, all subsequent user management goes through
    the authenticated ``/api/rbac/users`` route.
  - Prints the plaintext API token EXACTLY ONCE on stdout, with a clear
    "store this now" warning. The DB only ever sees the sha256 hash.
  - Does NOT require ``LHS_RBAC_ENABLED`` to be set to *run* — the
    operator may bootstrap first, then flip the env var. Init is
    idempotent on the schema side (rerunning is safe if no users exist).
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from typing import Optional

from . import rbac_auth as rbac_mod
from .v1.rbac import BUILTIN_ROLES


_DEFAULT_ROLE = "OWNER"


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m backend.bootstrap_rbac",
        description=(
            "Create the FIRST RBAC user. Refuses if any user already "
            "exists. Prints the plaintext token exactly once."
        ),
    )
    parser.add_argument(
        "--email", required=True,
        help="email address for the new user (used as the login identifier)",
    )
    parser.add_argument(
        "--role", default=_DEFAULT_ROLE,
        choices=sorted(BUILTIN_ROLES.keys()),
        help=f"role for the new user (default: {_DEFAULT_ROLE})",
    )
    return parser.parse_args(argv)


async def _bootstrap(email: str, role: str) -> int:
    rbac_mod.init_rbac_db()

    existing = await rbac_mod.count_users()
    if existing > 0:
        print(
            f"ERROR: refusing to bootstrap — {existing} user(s) already exist. "
            "Use the authenticated /api/rbac/users endpoint to add more.",
            file=sys.stderr,
        )
        return 2

    try:
        user, plaintext = await rbac_mod.create_user(email=email, role=role)
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

    # The plaintext token is printed exactly ONCE here. The DB only holds
    # the sha256 hash; losing this token means deleting + recreating the
    # user.
    print("=" * 72)
    print("RBAC bootstrap successful")
    print("=" * 72)
    print(f"  user_id : {user.user_id}")
    print(f"  email   : {user.email}")
    print(f"  role    : {user.role}")
    print()
    print("API TOKEN (store this now — it will NOT be shown again):")
    print(f"  {plaintext}")
    print()
    print("Next steps:")
    print("  1. Save the token in your secret manager.")
    print("  2. Set LHS_RBAC_ENABLED=true in the Studio environment.")
    print("  3. Restart the Studio backend.")
    print("  4. Use the token in the Authorization: Bearer <token> header.")
    print("=" * 72)
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(argv)
    return asyncio.run(_bootstrap(email=args.email, role=args.role))


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
