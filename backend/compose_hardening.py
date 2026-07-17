"""P0.2 container runtime hardening — compose override generation.

The certified stacks run from a cloned upstream ``docker-compose.yml`` that is
patched in place (``runner._patch_compose_images``). Its StarRocks ``command``
heredocs are fragile multi-line strings, so we do NOT re-serialize that file to
add security options. Instead we harden at runtime with a compose OVERRIDE file
merged via ``-f docker-compose.harden.yml``. Compose deep-merges override files
by service name, so the override layers security options onto existing services
without touching the base compose text.

Two tiers, deliberately:

* **Default (always on).** ``security_opt: ["no-new-privileges:true"]`` on every
  service. This blocks setuid/setgid privilege escalation inside the container
  and is compatible with every certified stack — none rely on setuid helpers in
  pilot mode. It is the universally-safe hardening we can turn on without
  per-stack verification.

* **Strict (opt-in, ``LHS_HARDEN_STRICT``).** Additionally ``cap_drop: [ALL]`` +
  a minimal ``cap_add`` re-grant and a ``pids_limit``. Blanket capability
  dropping genuinely needs per-stack verification (HDFS/StarRocks can require
  specific caps), so it is OFF by default. Turning it on without testing would
  trade real stability for nominal hardening — exactly the compromise the
  "stable, not compromise" rule forbids.

This module is pure (no env / filesystem access) so it is trivially testable.
The runner resolves the flags and owns the file write.
"""
from __future__ import annotations

from typing import Any, Iterable

# Env flags (resolved by the runner, documented here so they live next to the
# behavior they gate).
DISABLE_ENV = "LHS_HARDEN_RUNTIME_DISABLED"
STRICT_ENV = "LHS_HARDEN_STRICT"

OVERLAY_FILENAME = "docker-compose.harden.yml"

# Capabilities retained under strict mode. This is Docker's conservative default
# grant minus the ones almost nothing needs — kept broad enough that ordinary
# workloads (bind low ports, chown files, drop to a service user) still work,
# narrow enough to remove the dangerous ones (SYS_ADMIN, NET_ADMIN, NET_RAW,
# SYS_PTRACE, …) that an attacker would want. Strict mode still needs per-stack
# verification; this list is a sane starting point, not a proven-safe universal.
_STRICT_CAP_ADD: tuple[str, ...] = (
    "CHOWN",
    "DAC_OVERRIDE",
    "FOWNER",
    "FSETID",
    "SETGID",
    "SETUID",
    "SETPCAP",
    "NET_BIND_SERVICE",
    "KILL",
)

# Fork-bomb guard under strict mode. Generous so JVM-heavy services (Spark,
# StarRocks, Trino) that spawn thousands of threads are unaffected in practice.
_STRICT_PIDS_LIMIT = 8192


def _clean_names(service_names: Iterable[str]) -> list[str]:
    """Dedupe (order-preserving) and drop empty/None names."""
    return list(dict.fromkeys(n for n in service_names if n))


def build_harden_overlay(
    service_names: Iterable[str],
    *,
    strict: bool = False,
) -> dict[str, Any]:
    """Build a docker-compose override doc hardening each named service.

    Returns ``{"services": {name: {...security options...}}}`` — a valid compose
    override that adds ``security_opt`` (and, under *strict*, ``cap_drop`` /
    ``cap_add`` / ``pids_limit``) to services already defined in earlier ``-f``
    files. Never sets ``image``, so it only ever modifies existing services.
    """
    names = _clean_names(service_names)
    services: dict[str, Any] = {}
    for name in names:
        opts: dict[str, Any] = {"security_opt": ["no-new-privileges:true"]}
        if strict:
            opts["cap_drop"] = ["ALL"]
            opts["cap_add"] = list(_STRICT_CAP_ADD)
            opts["pids_limit"] = _STRICT_PIDS_LIMIT
        services[name] = opts
    return {"services": services}
