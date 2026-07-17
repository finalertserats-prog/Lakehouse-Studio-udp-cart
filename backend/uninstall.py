"""Full Uninstall — wipe a deployed lakehouse AND remove it from Studio.

This is the most destructive operation Studio offers. Order of operations:
  1. Verify nothing is mid-flight (state not in RUNNING_STATES, no live task)
  2. Verify install_dir actually looks like a UDP clone (don't nuke arbitrary dirs)
  3. Run `./udp clean` if Docker is reachable — best-effort, log on failure
  4. Archive evidence to evidence/.archive/{stack}/{id}/
  5. Remove the install directory (shutil.rmtree)
  6. Delete the install record from state
  7. Publish a final event so anyone watching the WS sees it

Each step is logged; a failure in step 3 doesn't block 4-6 (the user
chose Uninstall — they want to forget this install).
"""
from __future__ import annotations
import asyncio
import shutil
import stat
import time
from pathlib import Path
from typing import Optional

from .config import EVIDENCE_DIR, WORK_DIR
from .events import bus
from .models import LogEvent
from .runner import run_command
from .stack_manifest import StackManifest
from .state import store


class UninstallError(ValueError):
    pass


def _is_udp_clone(path: Path) -> bool:
    """Safety: only remove dirs that look like a UDP clone."""
    return (path / ".git").exists() and (path / "udp").exists()


def _on_rmtree_error(func, path, exc_info):
    """shutil.rmtree handler for Windows read-only files in .git."""
    try:
        Path(path).chmod(stat.S_IWRITE)
        func(path)
    except Exception:
        pass


def _archive_evidence(stack_id: str, install_id: str) -> Optional[Path]:
    src = EVIDENCE_DIR / stack_id / install_id
    if not src.exists():
        return None
    dst_parent = EVIDENCE_DIR / ".archive" / stack_id
    dst_parent.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%dT%H%M%S")
    dst = dst_parent / f"{install_id}_{ts}"
    try:
        shutil.move(str(src), str(dst))
        return dst
    except Exception:
        return None


def _is_compose_build(path: Path) -> bool:
    """Safety for custom/Quick-Install builds: only remove a dir that is under
    WORK_DIR and actually holds a generated docker-compose.yml. Prevents
    rmtree of anything outside the studio work area."""
    try:
        path.resolve().relative_to(WORK_DIR.resolve())
    except (ValueError, OSError):
        return False
    return (path / "docker-compose.yml").exists()


async def uninstall_custom(install_id: str, running_states: frozenset[str],
                           install_tasks: dict) -> dict:
    """Uninstall a custom / Quick-Install build (no stack manifest).

    These run off a generated docker-compose.yml in WORK_DIR/<name> rather than
    a UDP clone, so cleanup is `docker compose down -v` + rmtree — there's no
    `./udp clean` and no manifest to load.
    """
    rec = store.get(install_id)
    if not rec:
        raise UninstallError(f"install {install_id!r} not found")
    if rec.state in running_states:
        raise UninstallError(f"cannot uninstall while state is {rec.state}; cancel first")
    if install_id in install_tasks and not install_tasks[install_id].done():
        raise UninstallError("an install task is still running; cancel before uninstall")

    install_dir = Path(rec.install_dir)
    steps: list[dict] = []

    def _step(name: str, status: str, detail: str = ""):
        steps.append({"name": name, "status": status, "detail": detail})
        bus.publish_nowait(LogEvent(
            install_id=install_id, ts=time.time(),
            kind="log", stream="stdout" if status != "failed" else "stderr",
            step="uninstall", line=f"[uninstall] {name}: {status}{(' — ' + detail) if detail else ''}",
        ))

    compose_file = install_dir / "docker-compose.yml"

    # Step 1: docker compose down -v (best-effort; if Docker is down, log + continue)
    if compose_file.exists():
        try:
            proc = await asyncio.create_subprocess_exec(
                "docker", "compose", "-f", str(compose_file), "down", "-v", "--remove-orphans",
                cwd=str(install_dir),
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
            )
            out, _ = await proc.communicate()
            if proc.returncode == 0:
                _step("docker-compose-down", "passed", "containers + volumes removed")
            else:
                tail = (out or b"").decode(errors="replace").strip().splitlines()[-1:] or [""]
                _step("docker-compose-down", "warning", f"exit {proc.returncode}: {tail[0]} — continuing")
        except Exception as e:
            _step("docker-compose-down", "warning", f"{type(e).__name__}: {e} — continuing")
    else:
        _step("docker-compose-down", "skipped", "no docker-compose.yml found")

    # Step 2: archive evidence (if any)
    archived = _archive_evidence(rec.stack_id, install_id)
    _step("evidence-archive", "passed" if archived else "skipped",
          str(archived) if archived else "no evidence found")

    # Step 3: remove the install directory (only if it's a real compose build dir)
    if install_dir.exists():
        if _is_compose_build(install_dir):
            try:
                shutil.rmtree(install_dir, onerror=_on_rmtree_error)
                _step("rm-install-dir", "passed", str(install_dir))
            except Exception as e:
                _step("rm-install-dir", "failed", f"{type(e).__name__}: {e}")
        else:
            _step("rm-install-dir", "skipped",
                  f"{install_dir} is not a WORK_DIR compose build — leaving on disk")
    else:
        _step("rm-install-dir", "skipped", "already gone")

    # Step 4: delete the install record
    deleted = store.delete(install_id)
    _step("delete-record", "passed" if deleted else "skipped",
          "removed from state.json" if deleted else "record already gone")

    bus.publish_nowait(LogEvent(install_id=install_id, ts=time.time(), kind="state", status="UNINSTALLED"))
    return {"uninstalled": True, "install_id": install_id, "steps": steps,
            "archived_to": str(archived) if archived else None}


async def uninstall(install_id: str, stack: StackManifest, running_states: frozenset[str],
                    install_tasks: dict) -> dict:
    """Run the full uninstall pipeline. Caller is the FastAPI route handler."""
    rec = store.get(install_id)
    if not rec:
        raise UninstallError(f"install {install_id!r} not found")

    if rec.state in running_states:
        raise UninstallError(f"cannot uninstall while state is {rec.state}; cancel first")
    if install_id in install_tasks and not install_tasks[install_id].done():
        raise UninstallError("an install task is still running; cancel before uninstall")

    install_dir = Path(rec.install_dir)
    steps: list[dict] = []

    def _step(name: str, status: str, detail: str = ""):
        steps.append({"name": name, "status": status, "detail": detail})
        bus.publish_nowait(LogEvent(
            install_id=install_id, ts=time.time(),
            kind="log", stream="stdout" if status != "failed" else "stderr",
            step="uninstall", line=f"[uninstall] {name}: {status}{(' — ' + detail) if detail else ''}",
        ))

    # Step 1: refuse if it's not a UDP clone (don't rmtree arbitrary dirs)
    if install_dir.exists():
        if not _is_udp_clone(install_dir):
            _step("safety-check", "failed",
                  f"{install_dir} doesn't look like a UDP clone — refusing to remove")
            raise UninstallError(
                f"install_dir {install_dir} doesn't look like a UDP clone "
                "(missing .git or udp script). Refusing to remove."
            )
        _step("safety-check", "passed", str(install_dir))
    else:
        _step("safety-check", "passed", "install_dir doesn't exist; skipping rm")

    # Step 2: docker compose down -v (best effort; if Docker is down, log and continue)
    if install_dir.exists() and _is_udp_clone(install_dir):
        try:
            rc = await run_command(install_id, install_dir, rec.host, stack, "clean")
            if rc == 0:
                _step("docker-clean", "passed", "containers + volumes removed")
            else:
                _step("docker-clean", "warning", f"./udp clean exited {rc} — continuing")
        except Exception as e:
            _step("docker-clean", "warning", f"./udp clean raised {type(e).__name__}: {e} — continuing")
    else:
        _step("docker-clean", "skipped", "no install_dir to clean")

    # Step 3: archive evidence (move, not copy — evidence/.archive/)
    archived = _archive_evidence(rec.stack_id, install_id)
    if archived:
        _step("evidence-archive", "passed", str(archived))
    else:
        _step("evidence-archive", "skipped", "no evidence found")

    # Step 4: remove the install directory
    if install_dir.exists():
        try:
            # rmtree onerror handles Windows .git read-only files
            shutil.rmtree(install_dir, onerror=_on_rmtree_error)
            _step("rm-install-dir", "passed", str(install_dir))
        except Exception as e:
            _step("rm-install-dir", "failed", f"{type(e).__name__}: {e}")
            # Don't raise — still try to remove the record so it doesn't haunt Studio
    else:
        _step("rm-install-dir", "skipped", "already gone")

    # Step 5: delete the install record from state
    deleted = store.delete(install_id)
    _step("delete-record", "passed" if deleted else "skipped",
          "removed from state.json" if deleted else "record already gone")

    # Final event so any open WS gets a clean "this install is now ZERO" signal
    bus.publish_nowait(LogEvent(
        install_id=install_id, ts=time.time(),
        kind="state", status="UNINSTALLED",
    ))

    return {
        "uninstalled": True,
        "install_id": install_id,
        "steps": steps,
        "archived_to": str(archived) if archived else None,
    }
