"""Structured smoke tests against a deployed UDP stack.

UDP ships its own `./udp smoke-test` script which prints text; this module
runs the same conceptual checks via docker exec and returns each as a
pass/fail record so the UI can render them as cards instead of a wall of
logs.

Hardened per code review:
- Subprocesses are explicitly killed + reaped on timeout (no zombies)
- Checks run in parallel via asyncio.gather (bounded by per-check timeout)
- JSON responses size-capped before parsing
- MySQL "Warning:" prefix lines filtered before tab-splitting
- Pre-flight `docker info` short-circuits all checks when daemon is down
"""
from __future__ import annotations
import asyncio
import json
import shutil
import time
from typing import Any


_JSON_CAP_BYTES = 64 * 1024  # don't try to parse multi-MB error pages


async def _run(cmd: list[str], timeout: int = 15) -> tuple[int, str, str]:
    """Run a subprocess with hard cleanup on timeout. Returns (rc, stdout, stderr)."""
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
    except (FileNotFoundError, NotImplementedError, OSError) as e:
        return 127, "", f"failed to spawn: {e}"

    try:
        stdout_b, stderr_b = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        # Critical: kill + reap the process so we don't leak zombies / connections.
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        try:
            await asyncio.wait_for(proc.wait(), timeout=5)
        except asyncio.TimeoutError:
            pass
        return 124, "", f"timeout after {timeout}s"

    rc = proc.returncode if proc.returncode is not None else 1
    return rc, stdout_b.decode("utf-8", "replace"), stderr_b.decode("utf-8", "replace")


def _strip_mysql_warnings(out: str) -> list[str]:
    """MySQL prints 'Warning: Using a password ...' / 'mysql: [Warning] ...' to stdout.
    Filter those before tab-splitting."""
    return [ln for ln in out.splitlines()
            if ln.strip()
            and not ln.lower().startswith("warning:")
            and not ln.lower().startswith("mysql:")]


def _result(name: str, status: str, message: str, *, evidence: str | None = None, duration_ms: int = 0) -> dict:
    return {
        "name": name, "status": status, "message": message,
        "evidence": (evidence or None) if evidence is None else evidence[:500],
        "duration_ms": duration_ms,
    }


async def _check_docker_daemon() -> dict:
    t0 = time.monotonic()
    rc, out, err = await _run(["docker", "version", "--format", "{{.Server.Version}}"], timeout=8)
    dt = int((time.monotonic() - t0) * 1000)
    if rc == 0 and out.strip():
        return _result("Docker daemon", "passed", f"v{out.strip().splitlines()[0]}", duration_ms=dt)
    return _result("Docker daemon", "failed", "daemon not reachable",
                   evidence=(err or out)[:200], duration_ms=dt)


async def _check_container_running(container: str) -> dict:
    t0 = time.monotonic()
    rc, out, err = await _run(
        ["docker", "inspect", "-f",
         "{{.State.Status}} {{if .State.Health}}{{.State.Health.Status}}{{else}}no-healthcheck{{end}}",
         container], timeout=10
    )
    dt = int((time.monotonic() - t0) * 1000)
    if rc != 0:
        return _result(f"{container} running", "failed",
                       "container not found", evidence=err.strip()[:200], duration_ms=dt)
    s = out.strip().split()
    state = s[0] if s else "?"
    health = s[1] if len(s) > 1 else "no-healthcheck"
    if state == "running" and health in ("healthy", "no-healthcheck", "<no value>"):
        return _result(f"{container} running", "passed",
                       f"state={state}, health={health}", duration_ms=dt)
    if state == "running":
        return _result(f"{container} running", "warning",
                       f"state={state}, health={health}", duration_ms=dt)
    return _result(f"{container} running", "failed",
                   f"state={state}, health={health}", duration_ms=dt)


async def _check_iceberg_rest() -> dict:
    t0 = time.monotonic()
    rc, out, err = await _run(
        ["curl", "-sf", "--max-time", "8", "http://localhost:8181/v1/config"],
        timeout=10,
    )
    dt = int((time.monotonic() - t0) * 1000)
    if rc != 0:
        return _result("Iceberg REST /v1/config", "failed",
                       "REST API did not respond", evidence=(err or out)[:200], duration_ms=dt)
    body = out[:_JSON_CAP_BYTES]
    try:
        data = json.loads(body)
        wh = (data.get("defaults") or {}).get("warehouse") or (data.get("overrides") or {}).get("warehouse")
        return _result("Iceberg REST /v1/config", "passed",
                       f"warehouse={wh or '?'}", duration_ms=dt)
    except json.JSONDecodeError:
        return _result("Iceberg REST /v1/config", "warning",
                       "responded but body wasn't JSON", evidence=body[:200], duration_ms=dt)


async def _check_starrocks_fe_ping() -> dict:
    t0 = time.monotonic()
    rc, out, err = await _run(
        ["docker", "exec", "-i", "udp-starrocks-fe",
         "mysql", "-h", "127.0.0.1", "-P", "9030", "-u", "root", "-e", "SELECT 1"],
        timeout=15,
    )
    dt = int((time.monotonic() - t0) * 1000)
    if rc == 0:
        return _result("StarRocks FE SELECT 1", "passed",
                       "FE responds to MySQL protocol", duration_ms=dt)
    return _result("StarRocks FE SELECT 1", "failed",
                   "FE not responding on port 9030",
                   evidence=(err or out)[:200], duration_ms=dt)


async def _check_starrocks_backends_alive() -> dict:
    t0 = time.monotonic()
    rc, out, err = await _run(
        ["docker", "exec", "-i", "udp-starrocks-fe",
         "mysql", "-h", "127.0.0.1", "-P", "9030", "-u", "root",
         "--batch", "--raw", "-e", "SHOW BACKENDS"],
        timeout=15,
    )
    dt = int((time.monotonic() - t0) * 1000)
    if rc != 0:
        return _result("StarRocks BE alive", "failed",
                       "couldn't query SHOW BACKENDS", evidence=(err or out)[:200], duration_ms=dt)
    lines = _strip_mysql_warnings(out)
    if len(lines) < 2:
        return _result("StarRocks BE alive", "failed",
                       "no backends registered with FE", duration_ms=dt)
    header = lines[0].split("\t")
    try:
        alive_idx = header.index("Alive")
    except ValueError:
        return _result("StarRocks BE alive", "warning",
                       "BE registered but Alive column missing",
                       evidence=", ".join(header)[:200], duration_ms=dt)
    alive = []
    for ln in lines[1:]:
        cols = ln.split("\t")
        if len(cols) > alive_idx:
            alive.append(cols[alive_idx])
    if alive and all(a.strip().lower() == "true" for a in alive):
        return _result("StarRocks BE alive", "passed",
                       f"{len(alive)} backend(s) alive", duration_ms=dt)
    return _result("StarRocks BE alive", "failed",
                   f"backends not all alive: {alive}", duration_ms=dt)


async def _check_demo_data() -> dict:
    t0 = time.monotonic()
    rc, out, err = await _run(
        ["docker", "exec", "-i", "udp-starrocks-fe",
         "mysql", "-h", "127.0.0.1", "-P", "9030", "-u", "root",
         "--batch", "--raw", "-e",
         "SELECT COUNT(*) FROM app_analytics.demo_customer_summary"],
        timeout=15,
    )
    dt = int((time.monotonic() - t0) * 1000)
    if rc != 0:
        combined = (err or out).lower()
        if "doesn't exist" in combined or "unknown database" in combined:
            return _result("Demo data: customer_summary", "warning",
                           "table not created yet (bootstrap may not have finished)",
                           evidence=(err or out)[:200], duration_ms=dt)
        return _result("Demo data: customer_summary", "failed",
                       "couldn't query app_analytics.demo_customer_summary",
                       evidence=(err or out)[:200], duration_ms=dt)
    lines = _strip_mysql_warnings(out)
    if len(lines) < 2:
        return _result("Demo data: customer_summary", "failed",
                       "no rows in output", duration_ms=dt)
    try:
        n = int(lines[1].strip())
    except ValueError:
        return _result("Demo data: customer_summary", "warning",
                       f"unexpected output: {lines[1][:80]}", duration_ms=dt)
    if n > 0:
        return _result("Demo data: customer_summary", "passed",
                       f"{n} rows", duration_ms=dt)
    return _result("Demo data: customer_summary", "warning",
                   "table exists but is empty (bootstrap may not have completed)", duration_ms=dt)


_CONTAINER_CHECKS = (
    "udp-minio", "udp-iceberg-rest", "udp-spark",
    "udp-starrocks-fe", "udp-starrocks-be",
)
_API_CHECKS = (
    _check_iceberg_rest, _check_starrocks_fe_ping,
    _check_starrocks_backends_alive, _check_demo_data,
)


async def run_structured_smoke() -> dict:
    if shutil.which("docker") is None:
        return {"overall": "failed", "checks": [], "passed": 0, "warning": 0, "failed": 1, "total": 1,
                "error": "docker CLI not on PATH on this Studio host"}

    # Pre-flight: if daemon is down, short-circuit
    daemon = await _check_docker_daemon()
    if daemon["status"] == "failed":
        return {"overall": "failed", "checks": [daemon], "passed": 0, "warning": 0, "failed": 1, "total": 1,
                "error": "Docker daemon not reachable; no further checks attempted"}

    # Run all checks in parallel — bounded by each check's own timeout.
    # Label each task so we can report the right check name when a coroutine
    # raises instead of returning a result dict.
    labeled = (
        [(f"container:{c}", _check_container_running(c)) for c in _CONTAINER_CHECKS]
        + [(fn.__name__.lstrip("_"), fn()) for fn in _API_CHECKS]
    )
    labels = [name for name, _ in labeled]
    tasks = [coro for _, coro in labeled]
    results_raw = await asyncio.gather(*tasks, return_exceptions=True)

    results: list[dict] = [daemon]
    for label, r in zip(labels, results_raw):
        if isinstance(r, Exception):
            results.append(_result(label, "failed",
                                   f"check raised {type(r).__name__}: {r}"))
        else:
            results.append(r)

    failed = sum(1 for r in results if r["status"] == "failed")
    warn = sum(1 for r in results if r["status"] == "warning")
    passed = sum(1 for r in results if r["status"] == "passed")
    overall = "failed" if failed else ("warning" if warn else "passed")
    return {
        "overall": overall,
        "passed": passed, "warning": warn, "failed": failed,
        "total": len(results),
        "checks": results,
    }
