"""Trust boundary for AI/LLM-generated provisioning artifacts.

`ai_provisioner` asks an LLM to emit a provisioning plan — Trino catalog files,
post-start shell commands, and connectivity checks — and used to execute that
output directly via ``shell=True``. **LLM output is untrusted input.** A prompt
injection smuggled through a stack name, component description, or a manipulated
model turns "generate a plan" into arbitrary host command execution.

This module is the gate every AI-emitted artifact must pass before it runs:

* ``validate_catalog_filename`` — strict allowlist; blocks path traversal and
  shell-metacharacter filenames. Catalog *content* is never interpolated into a
  shell string (the caller writes it over stdin), so only the name needs a gate.
* ``vet_provisioning_command`` — each command must lead with an allowlisted
  binary (docker control-plane ops + read-only probes/filters) and must contain
  no host-escape / exfiltration / container-breakout token. Anything else is
  refused and reported, never executed.

The goal is defense in depth: even a fully compromised plan can, at worst, run
read-only probes and docker control-plane ops against Studio-managed
containers — it cannot escape to the host, exfiltrate, or spin privileged
containers.
"""
from __future__ import annotations

import re

# Catalog filenames become a path segment inside the Trino container
# (/data/trino/etc/catalog/<name>). Strict: a leading alnum, then alnum/._-,
# ending in .properties. No slashes, no `..`, no quotes, no whitespace.
_CATALOG_FILENAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*\.properties$")


def validate_catalog_filename(name: str) -> str:
    """Return a safe catalog filename or raise ValueError.

    Appends `.properties` if missing (the AI plan sometimes omits it), then
    enforces the strict allowlist. Rejects path traversal and any character
    that could break out of the container path or a shell context.
    """
    if not isinstance(name, str) or not name.strip():
        raise ValueError("catalog filename must be a non-empty string")
    candidate = name.strip()
    if not candidate.endswith(".properties"):
        candidate = candidate + ".properties"
    if "/" in candidate or "\\" in candidate or ".." in candidate:
        raise ValueError(f"unsafe catalog filename (path chars): {name!r}")
    if not _CATALOG_FILENAME_RE.match(candidate):
        raise ValueError(f"unsafe catalog filename: {name!r}")
    return candidate


# Leading executable of each command segment must be one of these. Docker
# control-plane verbs + read-only network/db probes + read-only text filters.
_ALLOWED_COMMAND_HEADS = frozenset({
    "docker",
    # connectivity / health probes
    "curl", "wget", "nc", "ncat", "pg_isready", "psql", "mysql", "mysqladmin",
    "redis-cli", "ping",
    # harmless flow / read-only text filters (used in probe pipelines)
    "sleep", "echo", "true", "false", "test", "[",
    "grep", "awk", "head", "tail", "wc", "cut", "tr", "sort", "uniq", "cat", "jq",
})

# `docker <subcmd>` is only allowed for control-plane verbs against existing,
# Studio-managed containers. `docker run`/`create`/`build` are refused from AI
# plans — those are how a plan would spin a privileged / host-mounting container.
_ALLOWED_DOCKER_SUBCOMMANDS = frozenset({
    "exec", "restart", "start", "stop", "kill", "logs", "inspect", "cp",
    "ps", "port", "top", "stats", "wait", "compose",
})

# Any of these anywhere in the command → refuse. Host escape, container
# breakout, exfiltration, chaining-to-shell, filesystem destruction.
_DANGEROUS_SUBSTRINGS = (
    "$(", "`",                       # command substitution
    "/dev/tcp", "/dev/udp",          # bash network reverse shells
    "-e /bin", "-e sh", "-e bash",   # nc -e reverse shell
    "bash -i", "sh -i",              # interactive reverse shell
    "| sh", "|sh", "| bash", "|bash",  # pipe to shell (curl|sh)
    "sudo", "chown", "chmod ",
    "rm -rf /", "rm -fr /", ":(){",  # fork bomb / root wipe
    "mkfifo", "crontab", "systemctl", "service ",
    "/etc/passwd", "/etc/shadow", "/root/.ssh", ".ssh/authorized_keys",
    "--privileged", "privileged: true", "privileged=true",
    "docker.sock", "/var/run/docker",
    "--pid=host", "--pid host", "pid: host",
    "--network=host", "--net=host", "network_mode: host",
    "-v /", "--volume /", "--mount ", ":/host", ":/etc", ":/root",
    "> /etc", ">/etc", "> /root", ">/root", "> /usr", "> /bin", "> /home",
)

_SEGMENT_SPLIT_RE = re.compile(r"\s*(?:&&|\|\||;|\||\n)\s*")


def vet_provisioning_command(cmd: str) -> tuple[bool, str]:
    """Return (ok, reason). ok=True only if every segment leads with an
    allowlisted binary and no dangerous token appears anywhere.

    `reason` is a short human-readable explanation when refused (for logging)
    or an empty string when allowed.
    """
    if not isinstance(cmd, str) or not cmd.strip():
        return False, "empty command"
    lowered = cmd.lower()
    for bad in _DANGEROUS_SUBSTRINGS:
        if bad in lowered:
            return False, f"contains blocked token {bad!r}"
    for segment in _SEGMENT_SPLIT_RE.split(cmd):
        seg = segment.strip()
        if not seg:
            continue
        tokens = seg.split()
        head = tokens[0]
        if head not in _ALLOWED_COMMAND_HEADS:
            return False, f"command head {head!r} is not allowlisted"
        if head == "docker" and len(tokens) >= 2:
            sub = tokens[1]
            if sub not in _ALLOWED_DOCKER_SUBCOMMANDS:
                return False, f"docker subcommand {sub!r} is not allowlisted"
    return True, ""
