from __future__ import annotations
import shutil
import socket
import subprocess
import platform
from typing import Optional

import psutil

from .models import InspectionCheck, InspectionReport
from .stack_manifest import StackManifest


def _run(cmd: list[str], timeout: int = 10) -> tuple[int, str]:
    try:
        r = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout, shell=False
        )
        out = (r.stdout or "") + (r.stderr or "")
        return r.returncode, out.strip()
    except FileNotFoundError:
        return 127, ""
    except subprocess.TimeoutExpired:
        return 124, ""
    except Exception as e:
        return 1, str(e)


def _check_command(name: str, args: list[str]) -> InspectionCheck:
    if shutil.which(args[0]) is None:
        return InspectionCheck(
            name=name, status="failed", message=f"{args[0]} not found in PATH"
        )
    code, out = _run(args)
    if code == 0:
        first = (out.splitlines() or [""])[0][:120]
        return InspectionCheck(
            name=name, status="passed", message=first or "available"
        )
    return InspectionCheck(
        name=name, status="failed", message=f"{args[0]} returned exit {code}", detail=out[:300]
    )


def _check_docker_daemon() -> InspectionCheck:
    if shutil.which("docker") is None:
        return InspectionCheck(
            name="docker_daemon", status="failed", message="docker CLI not installed"
        )
    # `docker version --format` returns daemon version much faster than `info`.
    code, out = _run(["docker", "version", "--format", "{{.Server.Version}}"], timeout=8)
    if code == 0 and out:
        return InspectionCheck(
            name="docker_daemon", status="passed", message=f"daemon up (v{out.splitlines()[0]})"
        )
    # Fallback to ping
    code2, out2 = _run(["docker", "info", "--format", "{{.ServerVersion}}"], timeout=10)
    if code2 == 0 and out2:
        return InspectionCheck(
            name="docker_daemon", status="passed", message=f"daemon up (v{out2.splitlines()[0]})"
        )
    msg = "Docker daemon not reachable. Start Docker Desktop / dockerd."
    return InspectionCheck(
        name="docker_daemon", status="failed", message=msg, detail=(out or out2)[:300]
    )


def _check_port_free(port: int, host: str = "127.0.0.1") -> InspectionCheck:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(0.5)
            res = s.connect_ex((host, port))
        if res == 0:
            return InspectionCheck(
                name=f"port_{port}",
                status="warning",
                message=f"port {port} already in use on {host}",
            )
        return InspectionCheck(
            name=f"port_{port}", status="passed", message=f"port {port} free"
        )
    except Exception as e:
        return InspectionCheck(
            name=f"port_{port}", status="warning", message=f"port {port} check failed: {e}"
        )


def _check_ram(min_gb: float, recommended_gb: float) -> InspectionCheck:
    total_gb = psutil.virtual_memory().total / (1024 ** 3)
    if total_gb >= recommended_gb:
        return InspectionCheck(
            name="ram", status="passed",
            message=f"{total_gb:.1f} GB total (>= recommended {recommended_gb})"
        )
    if total_gb >= min_gb:
        return InspectionCheck(
            name="ram", status="warning",
            message=f"{total_gb:.1f} GB total (>= minimum {min_gb}, below recommended {recommended_gb})"
        )
    return InspectionCheck(
        name="ram", status="failed",
        message=f"{total_gb:.1f} GB total (below minimum {min_gb} GB)"
    )


def _check_cpu(min_cores: int, recommended_cores: int) -> InspectionCheck:
    cores = psutil.cpu_count(logical=True) or 0
    if cores >= recommended_cores:
        return InspectionCheck(
            name="cpu", status="passed",
            message=f"{cores} logical cores (>= recommended {recommended_cores})"
        )
    if cores >= min_cores:
        return InspectionCheck(
            name="cpu", status="warning",
            message=f"{cores} logical cores (>= minimum {min_cores}, below recommended {recommended_cores})"
        )
    return InspectionCheck(
        name="cpu", status="failed",
        message=f"{cores} logical cores (below minimum {min_cores})"
    )


def _check_disk(min_gb: float, path: str = "/") -> InspectionCheck:
    try:
        target = path if platform.system() != "Windows" else "C:\\"
        u = psutil.disk_usage(target)
        free_gb = u.free / (1024 ** 3)
        if free_gb >= min_gb:
            return InspectionCheck(
                name="disk", status="passed",
                message=f"{free_gb:.1f} GB free on {target} (>= minimum {min_gb})"
            )
        return InspectionCheck(
            name="disk", status="warning",
            message=f"{free_gb:.1f} GB free on {target} (below minimum {min_gb})"
        )
    except Exception as e:
        return InspectionCheck(
            name="disk", status="warning", message=f"disk check failed: {e}"
        )


def _check_bash() -> InspectionCheck:
    if shutil.which("bash") is None:
        return InspectionCheck(
            name="bash", status="failed",
            message="bash not found in PATH (required to run UDP scripts)"
        )
    code, out = _run(["bash", "--version"])
    if code == 0:
        first = out.splitlines()[0] if out else "available"
        return InspectionCheck(name="bash", status="passed", message=first[:120])
    return InspectionCheck(name="bash", status="failed", message="bash --version failed")


def inspect(stack: StackManifest, host: str = "localhost") -> InspectionReport:
    checks: list[InspectionCheck] = []

    checks.append(_check_command("docker", ["docker", "--version"]))
    checks.append(_check_docker_daemon())

    # Docker Compose v2 is `docker compose`, v1 is `docker-compose`.
    if shutil.which("docker") is not None:
        code, out = _run(["docker", "compose", "version"])
        if code == 0:
            first = (out.splitlines() or [""])[0][:120]
            checks.append(InspectionCheck(
                name="docker_compose", status="passed", message=first
            ))
        else:
            checks.append(_check_command("docker_compose", ["docker-compose", "--version"]))
    else:
        checks.append(InspectionCheck(
            name="docker_compose", status="failed", message="docker not installed"
        ))

    checks.append(_check_command("git", ["git", "--version"]))
    checks.append(_check_bash())

    reqs = stack.requirements
    checks.append(_check_ram(
        min_gb=float(reqs.get("minimum_ram_gb", 8)),
        recommended_gb=float(reqs.get("recommended_ram_gb", 16)),
    ))
    checks.append(_check_cpu(
        min_cores=int(reqs.get("minimum_cpu_cores", 4)),
        recommended_cores=int(reqs.get("recommended_cpu_cores", 8)),
    ))
    checks.append(_check_disk(min_gb=float(reqs.get("minimum_disk_gb", 50))))

    for p in stack.required_ports:
        checks.append(_check_port_free(p, host="127.0.0.1"))

    has_failed = any(c.status == "failed" for c in checks)
    has_warning = any(c.status == "warning" for c in checks)
    if has_failed:
        overall = "failed"
    elif has_warning:
        overall = "warning"
    else:
        overall = "passed"

    return InspectionReport(
        host=host,
        overall=overall,
        checks=checks,
        recommended=not has_failed,
    )
