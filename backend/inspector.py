from __future__ import annotations
import shutil
import socket
import subprocess
import platform
from typing import Optional

import psutil

from .models import InspectionCheck, InspectionReport
from .stack_manifest import StackManifest


def _sshpass_prefix(password: str) -> list[str]:
    """Return ['sshpass', '-p', password] if sshpass is installed, else []."""
    if shutil.which("sshpass"):
        return ["sshpass", "-p", password]
    return []


def _ssh_prefix(host: str, ssh_user: str, ssh_port: int = 22,
                ssh_key_path: Optional[str] = None,
                ssh_password: Optional[str] = None) -> list[str]:
    """Build the SSH command prefix for a remote command."""
    cmd = ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=10"]
    # BatchMode=yes makes SSH exit immediately instead of prompting for a
    # password — only safe when we know key auth will work. Disable it when
    # a password was explicitly provided so that sshpass can inject it.
    if not ssh_password:
        cmd += ["-o", "BatchMode=yes"]
    cmd += ["-p", str(ssh_port)]
    if ssh_key_path:
        cmd += ["-i", ssh_key_path]
    cmd.append(f"{ssh_user}@{host}")
    return cmd


def _run_remote(host: str, ssh_user: str, remote_cmd: str,
                ssh_port: int = 22, ssh_key_path: Optional[str] = None,
                ssh_password: Optional[str] = None,
                timeout: int = 15) -> tuple[int, str]:
    """Run a shell command on a remote host via SSH and return (rc, output)."""
    prefix = _ssh_prefix(host, ssh_user, ssh_port, ssh_key_path, ssh_password)
    if ssh_password:
        sp = _sshpass_prefix(ssh_password)
        if not sp:
            return 1, (
                "sshpass is required for password authentication but is not installed. "
                "Run: sudo apt-get install -y sshpass"
            )
        prefix = sp + prefix
    try:
        r = subprocess.run(
            prefix + [remote_cmd],
            capture_output=True, text=True, timeout=timeout, shell=False,
        )
        out = (r.stdout or "") + (r.stderr or "")
        return r.returncode, out.strip()
    except FileNotFoundError:
        return 127, "ssh not found in PATH"
    except subprocess.TimeoutExpired:
        return 124, "SSH command timed out"
    except Exception as e:
        return 1, str(e)


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
    swap_gb = psutil.swap_memory().total / (1024 ** 3)
    effective_gb = total_gb + swap_gb

    if total_gb >= recommended_gb:
        return InspectionCheck(
            name="ram", status="passed",
            message=f"{total_gb:.1f} GB total (>= recommended {recommended_gb:.1f} GB)"
        )
    if total_gb >= min_gb:
        return InspectionCheck(
            name="ram", status="warning",
            message=f"{total_gb:.1f} GB RAM (>= minimum {min_gb:.1f} GB, below recommended {recommended_gb:.1f} GB)"
        )
    # Physical RAM is below the minimum — check if swap closes the gap.
    if effective_gb >= min_gb:
        return InspectionCheck(
            name="ram", status="warning",
            message=(
                f"{total_gb:.1f} GB RAM + {swap_gb:.1f} GB swap = {effective_gb:.1f} GB effective "
                f"(meets minimum {min_gb:.1f} GB via swap; recommended {recommended_gb:.1f} GB RAM)"
            ),
        )
    return InspectionCheck(
        name="ram", status="failed",
        message=(
            f"{total_gb:.1f} GB RAM + {swap_gb:.1f} GB swap = {effective_gb:.1f} GB "
            f"(below minimum {min_gb:.1f} GB)"
        ),
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
    # Don't trust the first PATH match blindly: on Windows, the WSL launcher
    # shim at System32\bash.exe frequently resolves ahead of Git Bash and
    # fails outright when WSL has no Linux distro installed. Try every
    # candidate (same resolution order the install pipeline uses) until one
    # actually runs. See runner._iter_bash_candidates for the full rationale.
    from .runner import _iter_bash_candidates

    candidates = _iter_bash_candidates()
    if not candidates:
        return InspectionCheck(
            name="bash", status="failed",
            message="bash not found in PATH (required to run UDP scripts)"
        )
    tried: list[str] = []
    for c in candidates:
        code, out = _run([c, "--version"])
        if code == 0:
            first = out.splitlines()[0] if out else "available"
            return InspectionCheck(name="bash", status="passed", message=first[:120])
        tried.append(c)
    return InspectionCheck(
        name="bash", status="failed",
        message="bash --version failed for every candidate on PATH",
        detail=(
            "Tried: " + ", ".join(tried) + ". On Windows this is usually the "
            "WSL launcher shim at System32\\bash.exe shadowing Git Bash."
        ),
    )


def _inspect_remote_cluster(stack: StackManifest, host: str) -> InspectionReport:
    """Pre-flight for mode=remote-cluster stacks.

    Docker/git/bash are not required — the stack already runs on bare-metal
    servers. We only verify that this machine can reach the cluster nodes over
    TCP on at least one known port per node, and check local RAM/CPU/disk are
    sufficient to run Studio itself.
    """
    checks: list[InspectionCheck] = []

    # Connectivity probe: try to TCP-connect to each node's first port.
    cluster = stack.data.get("cluster", {})
    nodes = cluster.get("nodes", [])
    for node in nodes:
        hostname = node.get("hostname", "")
        services = node.get("services", [])
        # Find the first component listed for this host that has a port
        probe_port: Optional[int] = None
        for comp in stack.components:
            if comp.get("host") == hostname:
                ports = comp.get("ports", [])
                if ports:
                    probe_port = int(ports[0])
                    break
        if not probe_port:
            checks.append(InspectionCheck(
                name=f"node_{hostname.split('.')[0]}",
                status="warning",
                message=f"{hostname}: no port to probe (skipped)",
            ))
            continue
        try:
            with socket.create_connection((hostname, probe_port), timeout=5):
                pass
            checks.append(InspectionCheck(
                name=f"node_{hostname.split('.')[0]}",
                status="passed",
                message=f"{hostname}:{probe_port} reachable ({', '.join(services[:3])}{'...' if len(services) > 3 else ''})",
            ))
        except OSError as e:
            checks.append(InspectionCheck(
                name=f"node_{hostname.split('.')[0]}",
                status="warning",
                message=f"{hostname}:{probe_port} unreachable ({e}) — cluster may still work if VPN is required",
            ))

    # Local resource checks — Studio itself needs very little. The manifest's
    # requirements describe the REMOTE cluster, not this machine, so we use
    # fixed Studio-only minimums here.
    checks.append(_check_ram(min_gb=1.0, recommended_gb=4.0))
    checks.append(_check_cpu(min_cores=1, recommended_cores=2))
    checks.append(_check_disk(min_gb=2.0))

    # No ports to check locally — ports belong to the remote cluster.
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


def _inspect_remote_ssh(
    stack: StackManifest, host: str,
    ssh_user: str, ssh_port: int = 22, ssh_key_path: Optional[str] = None,
    ssh_password: Optional[str] = None,
) -> InspectionReport:
    """Pre-flight checks for a remote host reachable via SSH.

    SSHes into the host and runs the same checks (docker, compose, bash, git,
    RAM, CPU, disk, ports) that the local inspect() runs, but executed on the
    remote machine.
    """
    checks: list[InspectionCheck] = []

    # Convenience: partial so every call gets ssh_password automatically
    def _r(remote_cmd: str, timeout: int = 15) -> tuple[int, str]:
        return _run_remote(host, ssh_user, remote_cmd,
                           ssh_port, ssh_key_path, ssh_password, timeout)

    # ── 1. Basic SSH connectivity ──────────────────────────────────────────
    # Check sshpass availability before the first connection attempt when
    # a password was supplied.
    if ssh_password and not shutil.which("sshpass"):
        checks.append(InspectionCheck(
            name="ssh_connect",
            status="failed",
            message="sshpass is required for password auth but is not installed",
            detail=(
                "Install it on the Studio machine with:\n"
                "  sudo apt-get install -y sshpass\n"
                "Or configure SSH key-based auth on the remote host and leave the password field empty."
            ),
        ))
        return InspectionReport(host=host, overall="failed", checks=checks, recommended=False)

    rc, out = _r("echo ssh-ok", timeout=15)
    if rc != 0:
        checks.append(InspectionCheck(
            name="ssh_connect",
            status="failed",
            message=f"Cannot SSH to {ssh_user}@{host}:{ssh_port}",
            detail=out[:300] or "Connection refused or timed out",
        ))
        return InspectionReport(host=host, overall="failed", checks=checks, recommended=False)
    checks.append(InspectionCheck(
        name="ssh_connect", status="passed",
        message=f"SSH connection to {ssh_user}@{host}:{ssh_port} OK",
    ))

    # ── 2. Docker CLI + daemon ─────────────────────────────────────────────
    rc, out = _r("docker --version 2>&1")
    if rc == 0 and out:
        checks.append(InspectionCheck(
            name="docker", status="passed", message=(out.splitlines()[0])[:120],
        ))
        rc2, out2 = _r("docker version --format '{{.Server.Version}}' 2>&1", timeout=12)
        if rc2 == 0 and out2:
            checks.append(InspectionCheck(
                name="docker_daemon", status="passed",
                message=f"daemon up (v{out2.splitlines()[0]})",
            ))
        else:
            checks.append(InspectionCheck(
                name="docker_daemon", status="failed",
                message="Docker daemon not reachable on remote host",
                detail=out2[:300],
            ))
    else:
        checks.append(InspectionCheck(
            name="docker", status="failed", message="docker CLI not found on remote host",
        ))
        checks.append(InspectionCheck(
            name="docker_daemon", status="failed", message="docker not installed",
        ))

    # ── 3. Docker Compose ──────────────────────────────────────────────────
    rc, out = _r("docker compose version 2>&1")
    if rc == 0:
        checks.append(InspectionCheck(
            name="docker_compose", status="passed",
            message=(out.splitlines()[0])[:120],
        ))
    else:
        rc2, out2 = _r("docker-compose --version 2>&1")
        if rc2 == 0:
            checks.append(InspectionCheck(
                name="docker_compose", status="passed",
                message=(out2.splitlines()[0])[:120],
            ))
        else:
            checks.append(InspectionCheck(
                name="docker_compose", status="failed",
                message="docker compose / docker-compose not found on remote host",
            ))

    # ── 4. git + bash ──────────────────────────────────────────────────────
    for tool, cmd in [("git", "git --version 2>&1"), ("bash", "bash --version 2>&1")]:
        rc, out = _r(cmd)
        if rc == 0 and out:
            checks.append(InspectionCheck(
                name=tool, status="passed", message=(out.splitlines()[0])[:120],
            ))
        else:
            checks.append(InspectionCheck(
                name=tool, status="failed",
                message=f"{tool} not found on remote host",
            ))

    # ── 5. RAM / CPU / disk ────────────────────────────────────────────────
    reqs = stack.requirements
    min_ram = float(reqs.get("minimum_ram_gb", 8))
    rec_ram = float(reqs.get("recommended_ram_gb", 16))
    min_cpu = int(reqs.get("minimum_cpu_cores", 4))
    rec_cpu = int(reqs.get("recommended_cpu_cores", 8))
    min_disk = float(reqs.get("minimum_disk_gb", 50))

    rc, out = _r(
        "awk '/MemTotal/{t=$2} /MemFree/{f=$2} END{printf \"%.1f %.1f\", t/1048576, f/1048576}' /proc/meminfo",
    )
    if rc == 0 and out:
        try:
            total_gb, _ = (float(x) for x in out.split())
            if total_gb >= rec_ram:
                checks.append(InspectionCheck(
                    name="ram", status="passed",
                    message=f"{total_gb:.1f} GB RAM (>= recommended {rec_ram:.1f} GB)",
                ))
            elif total_gb >= min_ram:
                checks.append(InspectionCheck(
                    name="ram", status="warning",
                    message=f"{total_gb:.1f} GB RAM (>= minimum {min_ram:.1f} GB, below recommended {rec_ram:.1f} GB)",
                ))
            else:
                checks.append(InspectionCheck(
                    name="ram", status="failed",
                    message=f"{total_gb:.1f} GB RAM (below minimum {min_ram:.1f} GB)",
                ))
        except Exception:
            checks.append(InspectionCheck(name="ram", status="warning", message=f"RAM check failed: {out}"))
    else:
        checks.append(InspectionCheck(name="ram", status="warning", message="Could not read /proc/meminfo"))

    rc, out = _r("nproc 2>/dev/null || grep -c ^processor /proc/cpuinfo")
    if rc == 0 and out.strip().isdigit():
        cores = int(out.strip())
        if cores >= rec_cpu:
            checks.append(InspectionCheck(
                name="cpu", status="passed",
                message=f"{cores} logical cores (>= recommended {rec_cpu})",
            ))
        elif cores >= min_cpu:
            checks.append(InspectionCheck(
                name="cpu", status="warning",
                message=f"{cores} cores (>= minimum {min_cpu}, below recommended {rec_cpu})",
            ))
        else:
            checks.append(InspectionCheck(
                name="cpu", status="failed",
                message=f"{cores} cores (below minimum {min_cpu})",
            ))
    else:
        checks.append(InspectionCheck(name="cpu", status="warning", message="Could not read CPU count"))

    rc, out = _r("df -BG / | awk 'NR==2{print $4}' | tr -d 'G'")
    if rc == 0 and out.strip().isdigit():
        free_gb = int(out.strip())
        if free_gb >= min_disk:
            checks.append(InspectionCheck(
                name="disk", status="passed",
                message=f"{free_gb} GB free on / (>= minimum {min_disk:.0f} GB)",
            ))
        else:
            checks.append(InspectionCheck(
                name="disk", status="warning",
                message=f"{free_gb} GB free on / (below minimum {min_disk:.0f} GB)",
            ))
    else:
        checks.append(InspectionCheck(name="disk", status="warning", message="Could not check disk space"))

    # ── 6. Port availability on remote host ───────────────────────────────
    for p in stack.required_ports:
        rc, out = _r(
            f"bash -c 'echo >/dev/tcp/127.0.0.1/{p}' 2>/dev/null && echo in-use || echo free",
            timeout=8,
        )
        if out.strip() == "in-use":
            checks.append(InspectionCheck(
                name=f"port_{p}", status="warning",
                message=f"port {p} already in use on remote host",
            ))
        else:
            checks.append(InspectionCheck(
                name=f"port_{p}", status="passed",
                message=f"port {p} free on remote host",
            ))

    has_failed = any(c.status == "failed" for c in checks)
    has_warning = any(c.status == "warning" for c in checks)
    overall = "failed" if has_failed else ("warning" if has_warning else "passed")
    return InspectionReport(
        host=host, overall=overall, checks=checks, recommended=not has_failed,
    )


def inspect(stack: StackManifest, host: str = "localhost",
            ssh_user: Optional[str] = None, ssh_port: int = 22,
            ssh_key_path: Optional[str] = None,
            ssh_password: Optional[str] = None) -> InspectionReport:
    if stack.is_remote_cluster:
        return _inspect_remote_cluster(stack, host)

    _is_local = host in ("localhost", "127.0.0.1", "::1")
    if not _is_local and ssh_user:
        return _inspect_remote_ssh(stack, host, ssh_user, ssh_port, ssh_key_path, ssh_password)

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
