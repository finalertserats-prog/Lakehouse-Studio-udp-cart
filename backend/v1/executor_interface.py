"""Executor abstraction for the v1.0 architecture.

Today every "do a thing on the host" call (``docker compose up``,
``docker exec``, ``docker compose ps``, port probes) is inlined as a
subprocess call in ``runner.py`` / ``health.py`` / ``backup.py``. That
hardwires the control plane to "local Docker on the same box."

v1.0 needs to drive three kinds of targets without rewriting orchestration:
  * Local Docker Compose (today)
  * Kubernetes (kubectl / helm against a kubeconfig)
  * A customer-side Go agent over outbound mTLS gRPC

This module defines the ``Executor`` protocol all three implement. The
reference implementation (``LocalDockerExecutor``) duplicates — does NOT
import — the shell patterns already in ``runner.py``. Duplication is
deliberate: ``runner.py`` is frozen, and this file proves the protocol can
actually wrap the current behaviour.

Nothing here is wired into ``main.py``. See ``backend/v1/__init__.py`` for
the migration order.
"""
from __future__ import annotations
import asyncio
import platform
import shutil
import subprocess
import time
from pathlib import Path
from typing import Literal, Optional, Protocol, runtime_checkable

from pydantic import BaseModel, Field


ExecutorKind = Literal["docker_compose", "kubernetes", "ssh_vm"]


class ExecutorTarget(BaseModel):
    """Where to run things. One ``ExecutorTarget`` describes one customer env."""

    kind: ExecutorKind
    host: str = Field(min_length=1, max_length=253)
    kubeconfig_path: Optional[str] = None
    ssh_key_path: Optional[str] = None
    # Optional: for ssh_vm, the agent endpoint (host:port). For kubernetes,
    # the namespace to install into. Free-form to keep the scaffold flexible.
    extra: dict[str, str] = Field(default_factory=dict)


class ExecResult(BaseModel):
    """Outcome of a single executor call. JSON-serialisable so it can flow
    through the event bus / audit log unchanged."""

    success: bool
    stdout: str = ""
    stderr: str = ""
    exit_code: int = 0
    duration_ms: int = 0


@runtime_checkable
class Executor(Protocol):
    """Abstract control plane → target adapter.

    Every method is async. ``inspect`` returns a free-form dict (target
    capabilities, docker/k8s version, available services). Everything else
    returns ``ExecResult`` so callers don't need to know which kind of
    target they're talking to.

    Implementations MUST NOT raise for "the command ran and failed" — that's
    a non-zero ``exit_code``. They MAY raise for "I cannot reach the target
    at all" (config error, network unreachable, missing kubeconfig).
    """

    async def inspect(self, target: ExecutorTarget) -> dict: ...

    async def compose_up(
        self,
        target: ExecutorTarget,
        install_dir: Path,
        services: list[str],
    ) -> ExecResult: ...

    async def compose_down(
        self,
        target: ExecutorTarget,
        install_dir: Path,
    ) -> ExecResult: ...

    async def exec_in_container(
        self,
        target: ExecutorTarget,
        container_name: str,
        cmd: list[str],
    ) -> ExecResult: ...

    async def get_logs(
        self,
        target: ExecutorTarget,
        container_name: str,
        tail: int = 200,
    ) -> str: ...

    async def port_probe(
        self,
        target: ExecutorTarget,
        port: int,
    ) -> bool: ...


# --------------------------------------------------------------------------- #
# Reference implementation — local Docker Compose                              #
# --------------------------------------------------------------------------- #

def _bash_executable() -> str:
    """Mirrors ``runner._bash_executable`` — kept separate to honour the
    "frozen runner.py, no cross-imports" constraint."""
    bash = shutil.which("bash")
    if not bash:
        raise RuntimeError("bash not found in PATH. Install Git Bash or any POSIX bash.")
    return bash


def _posix_path(p: Path) -> str:
    """Windows ``C:\\foo`` → ``/c/foo``. Same logic as ``runner._to_posix_path``,
    duplicated so v1 doesn't import from runner."""
    if platform.system() != "Windows":
        return str(p)
    s = str(Path(p).resolve())
    if s.startswith("\\\\") or len(s) < 3 or s[1] != ":":
        return s.replace("\\", "/")
    drive = s[0].lower()
    rest = s[2:].replace("\\", "/")
    if not rest.startswith("/"):
        rest = "/" + rest
    return f"/{drive}{rest}"


def _sh_quote(s: str) -> str:
    if not s or any(c in s for c in " \t\"'\\$`!|&;()<>*?[]{}"):
        return "'" + s.replace("'", "'\\''") + "'"
    return s


async def _run(
    argv: list[str],
    *,
    cwd: Optional[Path] = None,
    timeout: int = 120,
    use_bash: bool = True,
) -> ExecResult:
    """Single chokepoint for every subprocess call in this module.

    ``use_bash=True`` wraps the command in ``bash -c`` so cross-platform
    behaviour matches ``runner.py`` exactly. ``False`` shells out directly
    (used for short reachability probes like ``docker version``)."""
    started = time.time()

    if use_bash:
        bash = _bash_executable()
        prefix = ""
        if cwd is not None:
            prefix = f"cd {_sh_quote(_posix_path(cwd))} && "
        cmd_str = prefix + " ".join(_sh_quote(a) for a in argv)
        proc = await asyncio.create_subprocess_exec(
            bash, "-c", cmd_str,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    else:
        proc = await asyncio.create_subprocess_exec(
            *argv,
            cwd=str(cwd) if cwd else None,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

    try:
        stdout_b, stderr_b = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        try:
            await asyncio.wait_for(proc.wait(), timeout=5)
        except asyncio.TimeoutError:
            pass
        return ExecResult(
            success=False,
            stdout="",
            stderr=f"[timeout after {timeout}s]",
            exit_code=124,
            duration_ms=int((time.time() - started) * 1000),
        )

    rc = proc.returncode if proc.returncode is not None else 1
    return ExecResult(
        success=rc == 0,
        stdout=stdout_b.decode("utf-8", "replace"),
        stderr=stderr_b.decode("utf-8", "replace"),
        exit_code=rc,
        duration_ms=int((time.time() - started) * 1000),
    )


class LocalDockerExecutor:
    """Drives Docker Compose on the same host the control plane runs on.

    Wraps the same subprocess patterns ``runner.py`` / ``health.py`` /
    ``backup.py`` already use today. Real shell-outs — this is not a mock.
    """

    async def inspect(self, target: ExecutorTarget) -> dict:
        """Report whether ``docker`` and ``docker compose`` are usable here."""
        out: dict = {"kind": target.kind, "host": target.host}
        if shutil.which("docker") is None:
            out["docker"] = {"available": False, "reason": "not on PATH"}
            return out
        res = await _run(["docker", "version", "--format", "{{.Server.Version}}"],
                         timeout=10, use_bash=False)
        out["docker"] = {
            "available": res.success,
            "version": res.stdout.strip() if res.success else None,
            "error": None if res.success else res.stderr.strip()[:200],
        }
        compose = await _run(["docker", "compose", "version", "--short"],
                             timeout=10, use_bash=False)
        out["compose"] = {
            "available": compose.success,
            "version": compose.stdout.strip() if compose.success else None,
        }
        return out

    async def compose_up(
        self,
        target: ExecutorTarget,
        install_dir: Path,
        services: list[str],
    ) -> ExecResult:
        """``docker compose up -d <services>`` from ``install_dir``.

        Pattern matches ``runner._step_cmd`` for the ``docker_compose_up``
        command type. Empty service list → start the whole compose project.
        """
        if not install_dir.exists():
            return ExecResult(success=False, exit_code=2,
                              stderr=f"install_dir does not exist: {install_dir}")
        argv = ["docker", "compose", "up", "-d"]
        argv.extend(services)
        return await _run(argv, cwd=install_dir, timeout=900)

    async def compose_down(
        self,
        target: ExecutorTarget,
        install_dir: Path,
    ) -> ExecResult:
        if not install_dir.exists():
            return ExecResult(success=False, exit_code=2,
                              stderr=f"install_dir does not exist: {install_dir}")
        return await _run(["docker", "compose", "down"], cwd=install_dir, timeout=300)

    async def exec_in_container(
        self,
        target: ExecutorTarget,
        container_name: str,
        cmd: list[str],
    ) -> ExecResult:
        """``docker exec <container_name> <cmd...>``.

        Same pattern as the ``docker exec udp-starrocks-fe mysql ...`` calls
        in ``runner._STUDIO_BOOTSTRAP_SH`` (just routed through Python
        instead of inlined in a heredoc shell script).
        """
        argv = ["docker", "exec", container_name, *cmd]
        return await _run(argv, timeout=120, use_bash=False)

    async def get_logs(
        self,
        target: ExecutorTarget,
        container_name: str,
        tail: int = 200,
    ) -> str:
        res = await _run(
            ["docker", "logs", "--tail", str(tail), container_name],
            timeout=30, use_bash=False,
        )
        # Compose logs go to stderr on docker CLI; merge for caller convenience.
        return (res.stdout + res.stderr).strip()

    async def port_probe(self, target: ExecutorTarget, port: int) -> bool:
        """TCP-connect to ``target.host:port``. Mirrors ``health._tcp_probe``."""
        import socket
        def _do() -> bool:
            try:
                with socket.create_connection((target.host, port), timeout=3.0):
                    return True
            except OSError:
                return False
        return await asyncio.to_thread(_do)


# --------------------------------------------------------------------------- #
# Future implementations — stubs only                                          #
# --------------------------------------------------------------------------- #

_K8S_TODO = (
    "v1.0 — needs kubectl wiring. Plan: shell to `kubectl` with "
    "KUBECONFIG=target.kubeconfig_path, render docker-compose.yml into a "
    "Helm chart (or use Kompose) per stack, then `helm upgrade --install`. "
    "Logs via `kubectl logs`, exec via `kubectl exec`, port probe via a "
    "`kubectl port-forward` + TCP connect or an in-cluster job."
)

_SSH_AGENT_TODO = (
    "v1.0 — needs the Go agent binary + outbound mTLS gRPC channel. "
    "See backend/v1/proto/agent.proto for the service contract. The agent "
    "runs on the customer host and dials OUT to the control plane (so we "
    "don't need an inbound port on customer infra). Auth: mTLS with a "
    "per-tenant client cert minted by the control plane on agent enrolment."
)


class KubernetesExecutor:
    """STUB. ``v1.0`` will wire this against ``kubectl`` and ``helm``."""

    async def inspect(self, target: ExecutorTarget) -> dict:
        raise NotImplementedError(_K8S_TODO)

    async def compose_up(
        self,
        target: ExecutorTarget,
        install_dir: Path,
        services: list[str],
    ) -> ExecResult:
        raise NotImplementedError(_K8S_TODO)

    async def compose_down(self, target: ExecutorTarget, install_dir: Path) -> ExecResult:
        raise NotImplementedError(_K8S_TODO)

    async def exec_in_container(
        self,
        target: ExecutorTarget,
        container_name: str,
        cmd: list[str],
    ) -> ExecResult:
        raise NotImplementedError(_K8S_TODO)

    async def get_logs(
        self,
        target: ExecutorTarget,
        container_name: str,
        tail: int = 200,
    ) -> str:
        raise NotImplementedError(_K8S_TODO)

    async def port_probe(self, target: ExecutorTarget, port: int) -> bool:
        raise NotImplementedError(_K8S_TODO)


class SshAgentExecutor:
    """STUB for the Go-agent-over-mTLS-gRPC future.

    The agent runs on customer infrastructure and dials the control plane.
    All calls in this class translate to gRPC requests defined in
    ``backend/v1/proto/agent.proto``. Required .proto messages mirror the
    method signatures here 1:1 — ``ExecResult`` matches the proto
    ``ExecResult`` message.
    """

    async def inspect(self, target: ExecutorTarget) -> dict:
        raise NotImplementedError(_SSH_AGENT_TODO)

    async def compose_up(
        self,
        target: ExecutorTarget,
        install_dir: Path,
        services: list[str],
    ) -> ExecResult:
        raise NotImplementedError(_SSH_AGENT_TODO)

    async def compose_down(self, target: ExecutorTarget, install_dir: Path) -> ExecResult:
        raise NotImplementedError(_SSH_AGENT_TODO)

    async def exec_in_container(
        self,
        target: ExecutorTarget,
        container_name: str,
        cmd: list[str],
    ) -> ExecResult:
        raise NotImplementedError(_SSH_AGENT_TODO)

    async def get_logs(
        self,
        target: ExecutorTarget,
        container_name: str,
        tail: int = 200,
    ) -> str:
        raise NotImplementedError(_SSH_AGENT_TODO)

    async def port_probe(self, target: ExecutorTarget, port: int) -> bool:
        raise NotImplementedError(_SSH_AGENT_TODO)
