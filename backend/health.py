"""Live per-service health probe for an installed stack.

Read-only. Does NOT touch the install pipeline, compose patcher, or any
state record. Purely observational — `docker compose ps` for container
state + an HTTP/TCP probe per service for liveness.

Returned shape is intentionally flat and JSON-serialisable so the UI can
render a status grid without further massaging.
"""
from __future__ import annotations
import asyncio
import json
import shutil
import socket
import time
from pathlib import Path
from typing import Any

from .stack_manifest import StackManifest


# Per-component liveness probe spec. Keys are component `id` from the catalog.
# `kind=http` → GET the URL, expect 2xx/3xx. `kind=tcp` → connect to host:port.
# All probes use a short timeout — we want a snapshot, not a wait.
_PROBES: dict[str, dict[str, Any]] = {
    "minio":        {"kind": "http", "url": "http://{host}:9000/minio/health/live"},
    "iceberg-rest": {"kind": "http", "url": "http://{host}:8181/v1/config"},
    "spark":        {"kind": "http", "url": "http://{host}:8888/api"},
    "starrocks-fe": {"kind": "http", "url": "http://{host}:8030/api/bootstrap"},
    "starrocks-be": {"kind": "tcp",  "host": "{host}", "port": 9050},
}

_PROBE_TIMEOUT = 3.0


async def _http_probe(url: str, timeout: float) -> dict[str, Any]:
    """Best-effort HTTP probe via urllib in a thread. No external deps."""
    def _do() -> dict[str, Any]:
        import urllib.request
        import urllib.error
        try:
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return {"status": "passed", "detail": f"HTTP {resp.status}"}
        except urllib.error.HTTPError as e:
            # 4xx/5xx still means the service is responding — graded as warning, not down
            if 400 <= e.code < 600:
                return {"status": "warning", "detail": f"HTTP {e.code}"}
            return {"status": "failed", "detail": f"HTTP {e.code}: {e.reason}"}
        except urllib.error.URLError as e:
            return {"status": "failed", "detail": f"unreachable: {e.reason}"}
        except Exception as e:
            return {"status": "unknown", "detail": f"{type(e).__name__}: {e}"[:200]}
    return await asyncio.to_thread(_do)


async def _tcp_probe(host: str, port: int, timeout: float) -> dict[str, Any]:
    def _do() -> dict[str, Any]:
        try:
            with socket.create_connection((host, port), timeout=timeout):
                return {"status": "passed", "detail": f"tcp {host}:{port} open"}
        except (socket.timeout, ConnectionRefusedError) as e:
            return {"status": "failed", "detail": f"tcp closed: {type(e).__name__}"}
        except OSError as e:
            return {"status": "failed", "detail": f"tcp error: {e}"}
    return await asyncio.to_thread(_do)


async def _compose_ps_states(install_dir: Path, timeout: float = 10.0) -> dict[str, str]:
    """Run `docker compose ps --format json` and return {service_name: state}.

    Empty dict on any failure — health probes still run; the UI just shows
    the container_state as 'unknown' for everything.
    """
    if shutil.which("docker") is None or not install_dir.exists():
        return {}
    try:
        proc = await asyncio.create_subprocess_exec(
            "docker", "compose", "ps", "--format", "json", "--all",
            cwd=str(install_dir),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout_b, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            return {}
    except Exception:
        return {}

    states: dict[str, str] = {}
    text = stdout_b.decode("utf-8", "replace").strip()
    if not text:
        return {}
    # Compose emits either one JSON-per-line OR a single JSON array. Handle both.
    if text.startswith("["):
        try:
            for row in json.loads(text):
                if isinstance(row, dict) and "Service" in row:
                    states[row["Service"]] = row.get("State", "unknown")
        except json.JSONDecodeError:
            pass
    else:
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
                if isinstance(row, dict) and "Service" in row:
                    states[row["Service"]] = row.get("State", "unknown")
            except json.JSONDecodeError:
                continue
    return states


async def get_stack_health(
    manifest: StackManifest,
    install_dir: Path,
    host: str = "localhost",
) -> dict[str, Any]:
    """Snapshot of every component's container state + liveness probe."""
    started = time.time()
    cart_components = manifest.components

    # docker compose ps + every probe in parallel — total wall time ≈ slowest probe
    ps_task = asyncio.create_task(_compose_ps_states(install_dir))
    probe_tasks: dict[str, asyncio.Task] = {}
    for comp in cart_components:
        cid = comp["id"]
        probe = _PROBES.get(cid)
        if probe is None:
            continue
        if probe["kind"] == "http":
            url = probe["url"].format(host=host)
            probe_tasks[cid] = asyncio.create_task(_http_probe(url, _PROBE_TIMEOUT))
        elif probe["kind"] == "tcp":
            probe_tasks[cid] = asyncio.create_task(
                _tcp_probe(probe["host"].format(host=host), probe["port"], _PROBE_TIMEOUT)
            )

    states = await ps_task

    services: list[dict[str, Any]] = []
    for comp in cart_components:
        cid = comp["id"]
        service_name = comp.get("service_name", cid)
        # Some manifest components map to multiple compose services (e.g. an
        # `id: starrocks` umbrella with separate FE+BE services). For now we
        # match on service_name 1:1 and fall back to the component id.
        container_state = states.get(service_name, "unknown")
        probe_task = probe_tasks.get(cid)
        if probe_task is not None:
            try:
                probe_result = await probe_task
            except Exception as e:
                probe_result = {"status": "unknown", "detail": f"probe crashed: {e}"}
        else:
            probe_result = {"status": "skipped", "detail": "no probe registered"}

        services.append({
            "id": cid,
            "name": comp.get("name", cid),
            "service_name": service_name,
            "container_state": container_state,
            "probe": probe_result,
        })

    # Roll-up: healthy if every component's container is running AND its probe
    # is passed/skipped. Degraded if any warning. Down if any failed.
    container_ok = all(s["container_state"] == "running" for s in services)
    probe_statuses = [s["probe"]["status"] for s in services]
    if not services:
        overall = "unknown"
    elif "failed" in probe_statuses or not container_ok:
        overall = "down" if all(s["container_state"] != "running" for s in services) else "degraded"
    elif "warning" in probe_statuses:
        overall = "degraded"
    else:
        overall = "healthy"

    return {
        "checked_at": started,
        "duration_ms": int((time.time() - started) * 1000),
        "host": host,
        "overall": overall,
        "services": services,
    }
