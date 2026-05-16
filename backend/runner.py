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
from .redact import redact, sanitize_env_overrides, quote_env_value, SECRET_KEYS
from .stack_manifest import StackManifest
from .state import store


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
    """On Windows, bash needs /c/Users/... not C:\\Users\\...

    Guards: only handle absolute drive-letter paths (C:\\…). Refuses UNC
    (\\\\server\\share) and long-path-prefixed (\\\\?\\) paths; falls back to
    the raw string for non-Windows.
    """
    if platform.system() != "Windows":
        return str(p)
    s = str(Path(p).resolve())
    # UNC / long-path / weird: bail out by returning the original string.
    # Bash inside Git for Windows can usually handle forward-slashed paths.
    if s.startswith("\\\\") or len(s) < 3 or s[1] != ":":
        return s.replace("\\", "/")
    drive = s[0].lower()
    rest = s[2:].replace("\\", "/")
    if not rest.startswith("/"):
        rest = "/" + rest
    return f"/{drive}{rest}"


# Env vars to pass to child subprocesses. Keep the surface small; explicitly
# drop credentials present in the parent process env (CI tokens, AWS keys, etc.).
_ENV_ALLOW = {
    "PATH", "HOME", "USER", "USERNAME", "USERPROFILE", "LANG", "LC_ALL", "TZ",
    "TMP", "TEMP", "TMPDIR",
    # Docker on Windows / WSL
    "DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH",
    # MSYS / Git Bash
    "MSYSTEM", "MSYS", "MSYSTEM_PREFIX", "MINGW_PREFIX",
    # Locale needed by docker compose
    "COLUMNS", "LINES", "TERM",
    # systemroot is needed for various Windows shell utilities
    "SYSTEMROOT", "SYSTEMDRIVE", "COMSPEC", "WINDIR", "PROGRAMFILES", "PROGRAMFILES(X86)",
}


def _build_subprocess_env() -> dict[str, str]:
    src = os.environ
    out = {k: v for k, v in src.items() if k in _ENV_ALLOW or k.startswith("LHS_")}
    out["PYTHONUNBUFFERED"] = "1"
    out["GIT_TERMINAL_PROMPT"] = "0"
    # docker compose v2 needs HOME
    out.setdefault("HOME", src.get("HOME", src.get("USERPROFILE", "")))
    return out


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
        self._emit("log", step=step_id, stream=stream, line=redact(line))  # type: ignore[arg-type]

    def _set_state(self, state: str) -> None:
        store.update_state(self.install_id, state)  # type: ignore[arg-type]
        self._emit("state", status=state)

    # ---------- subprocess plumbing ----------

    async def _run_bash(self, step_id: str, argv: list[str], cwd: Path, timeout: int) -> int:
        """Run a command under bash so UDP's shell scripts work cross-platform."""
        bash = _bash_executable()
        posix_cwd = _to_posix_path(cwd)
        quoted = " ".join(self._sh_quote(a) for a in argv)
        cmd_str = f"cd {self._sh_quote(posix_cwd)} && {quoted}"

        # Redact the echoed command in case argv contains a credential.
        self._log(step_id, "stdout", redact(f"$ {cmd_str}"))

        env = _build_subprocess_env()

        proc = await asyncio.create_subprocess_exec(
            bash, "-c", cmd_str,  # no -l: don't source user profile
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
        self._proc = proc

        async def _drain(stream: asyncio.StreamReader, kind: str) -> None:
            try:
                while True:
                    raw = await stream.readline()
                    if not raw:
                        return
                    try:
                        text = raw.decode("utf-8", errors="replace").rstrip("\r\n")
                    except Exception:
                        text = repr(raw)
                    self._log(step_id, kind, text)
            except asyncio.CancelledError:
                return

        drain_out = asyncio.create_task(_drain(proc.stdout, "stdout"))  # type: ignore[arg-type]
        drain_err = asyncio.create_task(_drain(proc.stderr, "stderr"))  # type: ignore[arg-type]

        timed_out = False
        try:
            await asyncio.wait_for(proc.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            timed_out = True
            self._log(step_id, "stderr", f"[timeout after {timeout}s; killing]")
            try:
                proc.kill()
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(proc.wait(), timeout=10)
            except asyncio.TimeoutError:
                pass
        finally:
            # Always drain to EOF, even on timeout or cancel.
            for t in (drain_out, drain_err):
                try:
                    await asyncio.wait_for(t, timeout=5)
                except asyncio.TimeoutError:
                    t.cancel()
                except Exception:
                    pass
            if self._proc is proc:
                self._proc = None

        if timed_out:
            return 124
        rc = proc.returncode
        return rc if rc is not None else 1

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

        # Sanitize user overrides; reject anything dangerous outright.
        clean_overrides, rejections = sanitize_env_overrides(overrides)
        for r in rejections:
            self._log("env", "stderr", f"rejected override {r}")
        if rejections and not clean_overrides:
            # If everything was rejected and nothing came through, still proceed
            # with defaults — but tell the user.
            pass

        # Defaults are trusted (from the manifest), but quote them too for safety.
        merged: dict[str, str] = {**self.stack.env_defaults, **clean_overrides}

        # Make UDP scripts executable. On Windows chmod is a near-noop, but on
        # Linux/macOS it matters. Don't swallow surprising errors silently.
        try:
            for name in ("udp",):
                p = self.install_dir / name
                if p.exists():
                    p.chmod(p.stat().st_mode | 0o111)
            scripts_dir = self.install_dir / "scripts"
            if scripts_dir.is_dir():
                for p in scripts_dir.glob("*.sh"):
                    p.chmod(p.stat().st_mode | 0o111)
        except Exception as e:
            self._log("env", "stderr", f"chmod warning: {e}")

        try:
            lines = [f"{k}={quote_env_value(v)}" for k, v in merged.items()]
            env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            try:
                env_path.chmod(0o600)
            except Exception:
                pass
            # Echo redacted preview line-by-line.
            for k, v in merged.items():
                is_secret = (
                    k in SECRET_KEYS
                    or "PASSWORD" in k.upper()
                    or "SECRET" in k.upper()
                    or "TOKEN" in k.upper()
                )
                shown = ("********" if v else "(empty)") if is_secret else v
                self._log("env", "stdout", f"{k}={shown}")
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
        evidence_ok = True
        try:
            from .evidence import capture
            rec = store.get(self.install_id)
            if rec:
                out_dir = capture(rec)
                outputs["evidence_dir"] = str(out_dir)
                store.set_outputs(self.install_id, outputs)
                self._log("finalize", "stdout", f"evidence captured: {out_dir}")
        except Exception as e:
            evidence_ok = False
            self._log("finalize", "stderr", f"evidence capture failed: {e}")
        # Step is success only if evidence wrote cleanly; stack is still READY either way.
        self._step_end("finalize", evidence_ok,
                       message=None if evidence_ok else "evidence capture failed (stack is still READY)")
        return evidence_ok

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
