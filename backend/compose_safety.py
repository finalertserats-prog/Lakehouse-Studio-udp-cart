"""Compose safety gate — reject dangerous constructs in generated stacks.

`stack_composer` / `custom_stack_runner` build a docker-compose spec from a
user- or AI-chosen component selection and then `docker compose up` it. Running
an arbitrary compose is remote-code-execution by design: a service that mounts
`/var/run/docker.sock`, runs `privileged`, shares the host PID/network
namespace, or bind-mounts `/` can trivially escape to the host.

This module is the trust boundary for the *composed* path (the frozen certified
stacks don't go through the composer, so they're unaffected). It inspects a
parsed compose doc and returns structured violations; `assert_compose_safe`
raises when any are found so the runner fails the install with a clear reason.

The policy is deny-by-default for host-escape primitives, with a small
allowlist of Linux capabilities that are safe for data-plane containers.
"""
from __future__ import annotations

import re
from dataclasses import dataclass


class ComposeSafetyError(Exception):
    """Raised when a composed stack contains a host-escape / breakout construct."""


@dataclass(frozen=True)
class Violation:
    service: str
    kind: str      # short slug, e.g. "privileged", "docker-socket"
    detail: str

    def __str__(self) -> str:
        return f"[{self.kind}] service '{self.service}': {self.detail}"


# Namespace-sharing keys whose value must never be the host.
_HOST_NAMESPACE_KEYS = ("pid", "ipc", "userns_mode", "cgroup", "uts")

# Capabilities safe for a data-plane container. Anything else (SYS_ADMIN,
# NET_ADMIN, SYS_PTRACE, ALL, …) is a rejection — those enable host escape,
# packet capture, or ptrace of other processes.
_ALLOWED_CAPS = {
    "CHOWN", "DAC_OVERRIDE", "FOWNER", "FSETID", "KILL", "SETGID", "SETUID",
    "SETPCAP", "NET_BIND_SERVICE", "SETFCAP",
}

# Absolute host paths that must never be bind-mounted into a container.
# Matched as a prefix on the normalized host side of a bind mount.
_FORBIDDEN_HOST_PREFIXES = (
    "/var/run/docker", "/run/docker",           # the daemon socket == host root
    "/etc", "/root", "/proc", "/sys", "/dev",
    "/boot", "/var/lib/docker", "/var/run", "/run",
    "/usr", "/bin", "/sbin", "/lib",
)

# security_opt values that disable the container's confinement.
_FORBIDDEN_SECURITY_OPT = ("seccomp:unconfined", "apparmor:unconfined", "systempaths=unconfined")


def _host_side(volume) -> str | None:
    """Return the host path of a bind mount, or None for named volumes / anon.

    Handles both short form ("host:container[:mode]" or "named:container") and
    long form ({type: bind, source: ..., target: ...}).
    """
    if isinstance(volume, dict):
        if (volume.get("type") or "bind") == "bind":
            return str(volume.get("source") or "")
        return None  # tmpfs / volume type
    if not isinstance(volume, str):
        return None
    # Short form. A leading "/" , "./" , "../" or "~" means a host bind mount;
    # a bare token (no slash before the first ":") is a named volume.
    head = volume.split(":", 1)[0]
    if head.startswith(("/", "./", "../", "~")):
        return head
    return None


def _is_true(value) -> bool:
    return value is True or (isinstance(value, str) and value.strip().lower() == "true")


def _norm(path: str) -> str:
    # collapse duplicate slashes and a trailing slash for prefix matching
    p = re.sub(r"/+", "/", path.strip())
    return p[:-1] if len(p) > 1 and p.endswith("/") else p


def scan_service(name: str, svc: dict) -> list[Violation]:
    out: list[Violation] = []
    if not isinstance(svc, dict):
        return out

    if _is_true(svc.get("privileged")):
        out.append(Violation(name, "privileged", "privileged: true grants full host access"))

    for key in _HOST_NAMESPACE_KEYS:
        val = svc.get(key)
        if isinstance(val, str) and val.strip().lower() in ("host", '"host"'):
            out.append(Violation(name, f"{key}-host", f"{key}: host shares the host {key} namespace"))

    nm = svc.get("network_mode")
    if isinstance(nm, str) and nm.strip().lower() == "host":
        out.append(Violation(name, "network-host", "network_mode: host exposes the host network stack"))

    for vol in svc.get("volumes") or []:
        host = _host_side(vol)
        if host is None:
            continue
        h = _norm(host)
        if ".." in h.split("/"):
            out.append(Violation(name, "path-traversal", f"bind mount escapes via '..': {host}"))
            continue
        if "docker.sock" in h:
            out.append(Violation(name, "docker-socket", f"mounts the Docker socket: {host}"))
            continue
        if h == "/" or any(h == p or h.startswith(p + "/") for p in _FORBIDDEN_HOST_PREFIXES):
            out.append(Violation(name, "host-path", f"bind-mounts a sensitive host path: {host}"))

    for cap in svc.get("cap_add") or []:
        c = str(cap).strip().upper()
        if c not in _ALLOWED_CAPS:
            out.append(Violation(name, "cap-add", f"adds non-allowlisted capability {c}"))

    if svc.get("devices"):
        out.append(Violation(name, "devices", "maps host devices into the container"))

    for opt in svc.get("security_opt") or []:
        o = str(opt).strip().lower().replace(" ", "")
        if any(bad in o for bad in _FORBIDDEN_SECURITY_OPT):
            out.append(Violation(name, "security-opt", f"disables container confinement: {opt}"))

    return out


def scan_compose_doc(doc: dict) -> list[Violation]:
    """Return every safety violation across all services in a compose doc."""
    if not isinstance(doc, dict):
        return []
    services = doc.get("services")
    if not isinstance(services, dict):
        return []
    violations: list[Violation] = []
    for name, svc in services.items():
        violations.extend(scan_service(str(name), svc))
    return violations


def assert_compose_safe(doc: dict) -> None:
    """Raise ComposeSafetyError listing all violations, or return None if clean."""
    violations = scan_compose_doc(doc)
    if violations:
        lines = "\n  ".join(str(v) for v in violations)
        raise ComposeSafetyError(
            f"refusing to run composed stack — {len(violations)} unsafe construct(s):\n  {lines}"
        )
