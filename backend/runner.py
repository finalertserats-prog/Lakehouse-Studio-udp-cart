from __future__ import annotations
import asyncio
import os
import platform
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

from .config import WORK_DIR
from .events import bus
from .models import LogEvent, StepStatus
from .stack_manifest import StackManifest
from .state import store


SECRET_KEYS = (
    "MINIO_ROOT_PASSWORD",
    "AWS_SECRET_ACCESS_KEY",
    "STARROCKS_ROOT_PASSWORD",
)


def _redact(line: str) -> str:
    out = line
    for key in SECRET_KEYS:
        if key in out:
            # crude but effective: anything after = up to space/eol
            head, _, tail = out.partition(key + "=")
            if tail:
                end = len(tail)
                for ch in (" ", "\t", "\n", "\r"):
                    pos = tail.find(ch)
                    if pos != -1 and pos < end:
                        end = pos
                out = head + key + "=" + "*" * 8 + tail[end:]
    return out


def _build_steps(stack: StackManifest) -> list[StepStatus]:
    return [
        StepStatus(id="prepare", title="Prepare workspace"),
        StepStatus(id="clone", title="Clone UDP repository"),
        StepStatus(id="env", title="Write .env file"),
        StepStatus(id="doctor", title="Run doctor checks"),
        StepStatus(id="start", title="Start stack (docker compose up)"),
        StepStatus(id="bootstrap", title="Bootstrap demo lakehouse"),
        StepStatus(id="smoke", title="Run smoke tests"),
        StepStatus(id="finalize", title="Capture outputs"),
    ]


def _bash_executable() -> str:
    bash = shutil.which("bash")
    if not bash:
        raise RuntimeError(
            "bash not found in PATH. Install Git Bash (Windows) or any POSIX bash."
        )
    return bash


def _to_posix_path(p: Path) -> str:
    """On Windows, bash needs /c/Users/... not C:\\Users\\..."""
    if platform.system() != "Windows":
        return str(p)
    s = str(p)
    drive = s[0].lower()
    rest = s[2:].replace("\\", "/")
    return f"/{drive}{rest}"


class UDPRunner:
    def __init__(self, stack: StackManifest, install_id: str, host: str, install_dir: Path):
        self.stack = stack
        self.install_id = install_id
        self.host = host
        self.install_dir = install_dir
        self._proc: Optional[asyncio.subprocess.Process] = None
        self._cancel = False

    # ---------- event helpers ----------

    def _emit(self, kind: str, **kwargs) -> None:
        evt = LogEvent(install_id=self.install_id, ts=time.time(), kind=kind, **kwargs)  # type: ignore[arg-type]
        bus.publish_nowait(evt)

    def _step_start(self, step_id: str) -> None:
        store.update_step(self.install_id, step_id, status="running", started_at=time.time())
        self._emit("step_start", step=step_id, status="running")

    def _step_end(self, step_id: str, success: bool, exit_code: int = 0, message: Optional[str] = None) -> None:
        status = "success" if success else "failed"
        store.update_step(
            self.install_id, step_id,
            status=status, finished_at=time.time(),
            exit_code=exit_code, message=message,
        )
        self._emit("step_end", step=step_id, status=status, payload={"exit_code": exit_code, "message": message})

    def _log(self, step_id: str, stream: str, line: str) -> None:
        self._emit("log", step=step_id, stream=stream, line=_redact(line))  # type: ignore[arg-type]

    def _set_state(self, state: str) -> None:
        store.update_state(self.install_id, state)  # type: ignore[arg-type]
        self._emit("state", status=state)

    # ---------- subprocess plumbing ----------

    async def _run_bash(self, step_id: str, argv: list[str], cwd: Path, timeout: int) -> int:
        """Run a command under bash so UDP's shell scripts work cross-platform."""
        bash = _bash_executable()
        # Build a single command string: cd to cwd in posix form, then run argv.
        posix_cwd = _to_posix_path(cwd)
        quoted = " ".join(self._sh_quote(a) for a in argv)
        cmd_str = f"cd {self._sh_quote(posix_cwd)} && {quoted}"

        self._log(step_id, "stdout", f"$ {cmd_str}")

        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        # Ensure git on Windows doesn't try to convert line endings inside the repo
        env.setdefault("GIT_TERMINAL_PROMPT", "0")

        self._proc = await asyncio.create_subprocess_exec(
            bash, "-lc", cmd_str,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )

        async def _drain(stream: asyncio.StreamReader, kind: str) -> None:
            while True:
                raw = await stream.readline()
                if not raw:
                    return
                try:
                    text = raw.decode("utf-8", errors="replace").rstrip("\r\n")
                except Exception:
                    text = repr(raw)
                self._log(step_id, kind, text)

        try:
            await asyncio.wait_for(
                asyncio.gather(
                    _drain(self._proc.stdout, "stdout"),  # type: ignore[arg-type]
                    _drain(self._proc.stderr, "stderr"),  # type: ignore[arg-type]
                    self._proc.wait(),
                ),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            self._log(step_id, "stderr", f"[timeout after {timeout}s; killing]")
            try:
                self._proc.kill()
            except ProcessLookupError:
                pass
            await self._proc.wait()
            return 124
        finally:
            rc = self._proc.returncode if self._proc.returncode is not None else 1
            self._proc = None
        return rc

    @staticmethod
    def _sh_quote(s: str) -> str:
        if not s or any(c in s for c in " \t\"'\\$`!|&;()<>*?[]{}"):
            return "'" + s.replace("'", "'\\''") + "'"
        return s

    async def cancel(self) -> None:
        self._cancel = True
        if self._proc is not None:
            try:
                self._proc.kill()
            except ProcessLookupError:
                pass

    # ---------- pipeline steps ----------

    async def _step_prepare(self) -> bool:
        self._step_start("prepare")
        try:
            self.install_dir.parent.mkdir(parents=True, exist_ok=True)
            self._log("prepare", "stdout", f"workspace: {self.install_dir}")
            self._step_end("prepare", True)
            return True
        except Exception as e:
            self._step_end("prepare", False, message=str(e))
            return False

    async def _step_clone(self) -> bool:
        self._step_start("clone")
        repo = self.stack.repository
        url = repo.get("url")
        ref = repo.get("ref", "main")
        if (self.install_dir / ".git").exists():
            self._log("clone", "stdout", f"existing repo at {self.install_dir}, pulling latest")
            rc = await self._run_bash("clone", ["git", "fetch", "origin", ref], self.install_dir, timeout=120)
            if rc != 0:
                self._step_end("clone", False, exit_code=rc, message="git fetch failed")
                return False
            rc = await self._run_bash("clone", ["git", "checkout", ref], self.install_dir, timeout=60)
            if rc != 0:
                self._step_end("clone", False, exit_code=rc, message="git checkout failed")
                return False
            rc = await self._run_bash("clone", ["git", "reset", "--hard", f"origin/{ref}"], self.install_dir, timeout=60)
            ok = rc == 0
            self._step_end("clone", ok, exit_code=rc)
            return ok
        # Clone fresh into install_dir.parent then move; simpler: clone directly into install_dir
        self.install_dir.parent.mkdir(parents=True, exist_ok=True)
        rc = await self._run_bash(
            "clone",
            ["git", "clone", "--branch", ref, "--depth", "1", url, _to_posix_path(self.install_dir)],
            cwd=self.install_dir.parent,
            timeout=300,
        )
        ok = rc == 0
        self._step_end("clone", ok, exit_code=rc)
        return ok

    async def _step_env(self, overrides: dict[str, str]) -> bool:
        self._step_start("env")
        env_path = self.install_dir / ".env"
        merged = {**self.stack.env_defaults, **overrides}
        # chmod scripts to be executable (UDP install.sh does this; we mirror it).
        try:
            for name in ("udp",):
                p = self.install_dir / name
                if p.exists():
                    p.chmod(p.stat().st_mode | 0o111)
            scripts_dir = self.install_dir / "scripts"
            if scripts_dir.is_dir():
                for p in scripts_dir.glob("*.sh"):
                    p.chmod(p.stat().st_mode | 0o111)
        except Exception:
            pass
        try:
            lines = [f"{k}={v}" for k, v in merged.items()]
            env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            # Echo redacted preview
            for k in merged:
                v = "*" * 8 if k in SECRET_KEYS or "PASSWORD" in k or "SECRET" in k else merged[k]
                self._log("env", "stdout", f"{k}={v}")
            self._step_end("env", True)
            return True
        except Exception as e:
            self._step_end("env", False, message=str(e))
            return False

    async def _step_cmd(self, step_id: str, cmd_name: str) -> bool:
        self._step_start(step_id)
        try:
            spec = self.stack.command(cmd_name)
        except KeyError as e:
            self._step_end(step_id, False, message=str(e))
            return False
        rc = await self._run_bash(step_id, list(spec["argv"]), self.install_dir, int(spec.get("timeout", 600)))
        ok = rc == 0
        self._step_end(step_id, ok, exit_code=rc)
        return ok

    async def _step_finalize(self) -> bool:
        self._step_start("finalize")
        urls = self.stack.output_urls(self.host)
        conns = self.stack.output_connections(self.host)
        outputs = {"urls": urls, "connections": conns}
        store.set_outputs(self.install_id, outputs)
        self._emit("result", payload=outputs)
        # Capture evidence: result.json, system-info.json, full-log.txt
        try:
            from .evidence import capture
            rec = store.get(self.install_id)
            if rec:
                out_dir = capture(rec)
                outputs["evidence_dir"] = str(out_dir)
                store.set_outputs(self.install_id, outputs)
                self._log("finalize", "stdout", f"evidence captured: {out_dir}")
        except Exception as e:
            self._log("finalize", "stderr", f"evidence capture failed: {e}")
        self._step_end("finalize", True)
        return True

    # ---------- top-level orchestration ----------

    async def run(self, env_overrides: dict[str, str]) -> None:
        try:
            self._set_state("INSPECTING")  # caller did the inspection already
            self._set_state("READY_TO_INSTALL")

            self._set_state("CLONING_REPO")
            if not await self._step_prepare(): return self._fail("prepare failed")
            if self._cancel: return self._fail("cancelled")
            if not await self._step_clone(): return self._fail("clone failed")

            self._set_state("WRITING_ENV")
            if not await self._step_env(env_overrides): return self._fail("env write failed")

            self._set_state("RUNNING_DOCTOR")
            if not await self._step_cmd("doctor", "doctor"): return self._fail("doctor failed")

            self._set_state("STARTING_STACK")
            if not await self._step_cmd("start", "start"): return self._fail("docker compose up failed")

            self._set_state("BOOTSTRAPPING")
            if not await self._step_cmd("bootstrap", "bootstrap"): return self._fail("bootstrap failed")

            self._set_state("SMOKE_TESTING")
            if not await self._step_cmd("smoke", "smoke"): return self._fail("smoke test failed")

            await self._step_finalize()
            self._set_state("READY")
            self._emit("state", status="READY")
        except Exception as e:
            self._fail(f"unexpected: {e}")

    def _fail(self, msg: str) -> None:
        store.update_state(self.install_id, "FAILED", error=msg)
        self._emit("state", status="FAILED", payload={"error": msg})
        self._emit("error", line=msg)


def make_steps(stack: StackManifest) -> list[StepStatus]:
    return _build_steps(stack)


async def run_command(install_id: str, install_dir: Path, host: str, stack: StackManifest, cmd_name: str) -> int:
    """One-shot command for stop/clean/status, with logs piped through the event bus."""
    runner = UDPRunner(stack, install_id, host, install_dir)
    runner._step_start(cmd_name)
    try:
        spec = stack.command(cmd_name)
    except KeyError as e:
        runner._step_end(cmd_name, False, message=str(e))
        return 1
    rc = await runner._run_bash(cmd_name, list(spec["argv"]), install_dir, int(spec.get("timeout", 300)))
    runner._step_end(cmd_name, rc == 0, exit_code=rc)
    return rc
