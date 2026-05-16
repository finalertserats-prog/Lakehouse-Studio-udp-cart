"""Per-service docker compose log viewer for an installed stack.

Read-only. Does NOT touch the install pipeline, runner, or any state record.
Purely observational — wraps `docker compose logs <service>` with strict
input validation, a hard line cap, and a hard wall-clock timeout so the UI
can render a per-service log panel without ever being able to make the
backend hang.

Two surfaces:
  * `get_service_logs(install_dir, service_name, tail=..., since=...)` ->
    one-shot snapshot, returns up to 500 lines and a `truncated` flag.
  * `stream_service_logs(install_dir, service_name)` -> async generator that
    yields decoded lines from `docker compose logs <service> -f` until the
    caller stops consuming (cancellation cleans up the subprocess).

Service names are validated against the manifest's known component
service_name set AND a strict charset, so we never shell out an arbitrary
string. We use `create_subprocess_exec` (argv, no shell) regardless — this
is defence in depth, not the only defence.
"""
from __future__ import annotations
import asyncio
import re
import shutil
import time
from pathlib import Path
from typing import AsyncGenerator, Optional


# Hard caps. The HTTP route may not ask for more than `MAX_TAIL` lines, and
# every snapshot call is bounded by `SNAPSHOT_TIMEOUT_SEC`.
MAX_TAIL = 500
SNAPSHOT_TIMEOUT_SEC = 10.0

# `--since` accepts either a Go-style duration (e.g. "10m", "1h30m", "45s")
# or an RFC3339 timestamp. We only accept the duration form — it's all the
# UI offers and it sidesteps timezone footguns. Anything else is rejected
# at the route layer (this module trusts the caller, but never shells out
# a free-form string).
_SINCE_DURATION_RE = re.compile(r"^[0-9]+(?:\.[0-9]+)?[smhd]$")

# Only allow conservative service-name characters. docker compose itself is
# more permissive (lowercase letters, digits, hyphen, underscore) but we
# keep the set tight on purpose — the value is concatenated into the argv
# list for `docker compose logs`.
_SERVICE_NAME_RE = re.compile(r"^[A-Za-z0-9_-]+$")


def _validate_service_name(name: str, manifest_services: set[str]) -> None:
    """Raise ValueError if `name` is unsafe OR not declared in the manifest.

    Two-layer check:
      1. Charset — must match _SERVICE_NAME_RE. Blocks path traversal
         (`../`), shell-meta (`;`, `|`, `&`, backticks), spaces, quotes, etc.
      2. Membership — must appear in the manifest's known service_name set.
         Even a syntactically-clean name is rejected if the install doesn't
         actually declare it.
    """
    if not isinstance(name, str) or not name:
        raise ValueError("service_name must be a non-empty string")
    if not _SERVICE_NAME_RE.match(name):
        raise ValueError(
            f"service_name {name!r} contains disallowed characters "
            f"(only [A-Za-z0-9_-] allowed)"
        )
    if name not in manifest_services:
        raise ValueError(
            f"service_name {name!r} is not declared in this install's manifest"
        )


def _validate_since(since: Optional[str]) -> Optional[str]:
    """Return the value unchanged if it's a safe Go-duration, else raise."""
    if since is None or since == "":
        return None
    if not isinstance(since, str):
        raise ValueError("since must be a string")
    if not _SINCE_DURATION_RE.match(since):
        raise ValueError(
            f"since {since!r} must be a Go duration like '10m', '1h', '45s'"
        )
    return since


def _build_logs_argv(
    service_name: str,
    tail: int,
    since: Optional[str],
    follow: bool = False,
) -> list[str]:
    """Compose the argv for `docker compose logs ...`. Internal helper —
    centralised so the snapshot, stream, and test paths all agree on it."""
    argv = ["docker", "compose", "logs", service_name, "--tail", str(tail)]
    if since:
        argv += ["--since", since]
    if follow:
        argv.append("-f")
    # `--no-color` makes the output ANSI-free so the UI can render it as-is
    # without having to strip escape sequences itself.
    argv.append("--no-color")
    return argv


async def get_service_logs(
    install_dir: Path,
    service_name: str,
    tail: int = 200,
    since: Optional[str] = None,
) -> dict:
    """Snapshot the last `tail` lines (capped at MAX_TAIL) for a service.

    Returns a dict with:
      * service     — the validated service name we asked for
      * lines       — list[str] (at most MAX_TAIL entries)
      * truncated   — True if the caller asked for more than MAX_TAIL
      * fetched_at  — unix timestamp (float) when the snapshot was taken
      * error       — present only if docker is unreachable or the call
                      failed; the caller should surface it to the UI

    NEVER raises — failures come back as `{"error": "..."}` in the result so
    the route layer can return 200 with a useful body and the UI can
    distinguish "no logs yet" from "docker is down".
    """
    requested_tail = int(tail) if tail is not None else 200
    capped_tail = max(1, min(requested_tail, MAX_TAIL))
    truncated = requested_tail > MAX_TAIL

    since_norm = _validate_since(since)

    fetched_at = time.time()
    base = {
        "service": service_name,
        "lines": [],
        "truncated": truncated,
        "fetched_at": fetched_at,
    }

    if shutil.which("docker") is None:
        return {**base, "error": "docker CLI not on PATH"}
    if not install_dir.exists():
        return {**base, "error": f"install_dir does not exist: {install_dir}"}

    argv = _build_logs_argv(service_name, capped_tail, since_norm, follow=False)
    try:
        proc = await asyncio.create_subprocess_exec(
            *argv,
            cwd=str(install_dir),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except (FileNotFoundError, NotImplementedError, OSError) as e:
        return {**base, "error": f"failed to spawn docker: {e}"}

    try:
        stdout_b, stderr_b = await asyncio.wait_for(
            proc.communicate(), timeout=SNAPSHOT_TIMEOUT_SEC
        )
    except asyncio.TimeoutError:
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        try:
            await asyncio.wait_for(proc.wait(), timeout=2.0)
        except asyncio.TimeoutError:
            pass
        return {**base, "error": f"docker compose logs timed out after {SNAPSHOT_TIMEOUT_SEC}s"}

    if proc.returncode != 0:
        err = stderr_b.decode("utf-8", "replace").strip()
        return {**base, "error": err or f"docker compose logs exited {proc.returncode}"}

    text = stdout_b.decode("utf-8", "replace")
    lines = [ln for ln in text.splitlines() if ln != ""]
    # Safety net — docker should already respect --tail, but enforce the cap
    # on our side so an unexpectedly-chatty service can't blow the response.
    if len(lines) > MAX_TAIL:
        lines = lines[-MAX_TAIL:]
        truncated = True

    return {
        "service": service_name,
        "lines": lines,
        "truncated": truncated,
        "fetched_at": fetched_at,
    }


async def stream_service_logs(
    install_dir: Path,
    service_name: str,
) -> AsyncGenerator[str, None]:
    """Live-tail a single service's logs as an async generator of lines.

    Spawns `docker compose logs <service> -f --tail 100`, yields each
    decoded line as it arrives, and tears down the subprocess on
    cancellation. The caller is expected to be a WebSocket handler that
    iterates until the client disconnects.

    Yields no lines (and exits silently) if docker is unreachable — the WS
    handler should send a sentinel message before opening the stream so the
    UI sees the error rather than an empty tail.
    """
    if shutil.which("docker") is None:
        return
    if not install_dir.exists():
        return

    argv = _build_logs_argv(service_name, tail=100, since=None, follow=True)
    try:
        proc = await asyncio.create_subprocess_exec(
            *argv,
            cwd=str(install_dir),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
    except (FileNotFoundError, NotImplementedError, OSError):
        return

    assert proc.stdout is not None
    try:
        while True:
            raw = await proc.stdout.readline()
            if not raw:
                break
            line = raw.decode("utf-8", "replace").rstrip("\r\n")
            if line == "":
                continue
            yield line
    except asyncio.CancelledError:
        raise
    finally:
        if proc.returncode is None:
            try:
                proc.kill()
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(proc.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pass
