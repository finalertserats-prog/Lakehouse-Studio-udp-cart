"""P0.4b — opt-in per-install credential generation (default OFF).

The stacks ship a documented demo/pilot MinIO secret (``udp_admin_12345``) that
is public in the repo and identical on every install. When an operator opts in
via ``LHS_GENERATE_CREDENTIALS``, the runner generates a strong random secret and
rewrites the shipped literal across the install directory so no two installs
share the known default.

Design constraints that keep this "stable, not compromise":

* **Default OFF.** Without the flag, nothing is generated and every consumer
  resolves to the ``${MINIO_ROOT_PASSWORD:-udp_admin_12345}`` default exactly as
  before — the certified install path stays byte-identical.
* **Rotate the SECRET, not the username.** The access key stays ``admin``. The
  password literal is unique, so a plain string replace across install-dir text
  files is unambiguous and complete; the username ``admin`` is a common word and
  is deliberately left alone.
* **Shell/YAML/SQL-safe secret.** ``token_hex`` yields ``[0-9a-f]`` only, so the
  value needs no quoting anywhere it lands (compose YAML, bash, mc args, Trino
  properties, StarRocks SQL).

This module is pure; the runner owns the filesystem sweep.
"""
from __future__ import annotations

import secrets

GENERATE_ENV = "LHS_GENERATE_CREDENTIALS"

# The demo/pilot MinIO secret shipped in the public repo — the single canonical
# literal the install-dir sweep replaces when generation is enabled. Documented,
# non-production, allowlisted in .gitleaks.toml (udp_admin_\d+).
DEMO_MINIO_SECRET = "udp_admin_12345"  # noqa: S105 - documented public placeholder

# Env var name whose value carries the (generated or default) MinIO secret.
MINIO_SECRET_ENV = "MINIO_ROOT_PASSWORD"


def generate_secret() -> str:
    """Return a strong, quoting-safe secret (40 hex chars = 160 bits)."""
    return secrets.token_hex(20)
