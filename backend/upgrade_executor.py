"""Upgrade Executor — DESTRUCTIVE.

Runs a previously-simulated upgrade against a live install. Designed as the
v0.4.1 follow-up to /api/stacks/{stack_id}/upgrades/simulate: simulate is
read-only, execute actually swaps the running images.

Pipeline (each step publishes LogEvents to the bus with step="upgrade"):

  1. PREFLIGHT     — backup_id must exist AND belong to this install_id.
  2. SIMULATE      — re-run compatibility.simulate_upgrade; refuse if not "pass".
  3. STATE GATE    — install must not be in a RUNNING_STATES state and must
                     not have a live task in _INSTALL_TASKS.
  4. COMPOSE DOWN  — `docker compose down` in install_dir.
  5. IMAGE PULL    — `docker pull <image>:<tag>` for every proposed component.
  6. COMPOSE UP    — write a `docker-compose.upgrade.yml` overlay with image
                     overrides (DO NOT mutate the base compose.yml) and run
                     `docker compose -f docker-compose.yml -f docker-compose.upgrade.yml up -d`.
                     Sleep 60s waiting for containers to settle.
  7. SMOKE         — invoke `bash {install_dir}/scripts/lhs-smoke.sh`.

Any failure AFTER compose_down triggers ROLLBACK:
  - restore_backup(backup_id) re-extracts the backup over install_dir.
  - `docker compose -f docker-compose.yml up -d` brings the stack back up
    against the BASE compose.yml (no overlay, original tags).

Per design: we DO NOT mutate the lock file on success. The response carries
a "proposed_new_lock" hint the operator can PR manually with evidence.

Persistence: UpgradeExecution records live in memory + WORK_DIR/upgrade_executions.json
(debounced atomic writes, same shape as IngestJob / BackupRecord).

Notes on safety:
- Every shell-out uses asyncio.create_subprocess_exec with explicit args. No shell=True.
- Component IDs in `proposed` are validated against the lock — unknown IDs raise 400 upstream.
- We never modify backend/runner.py or the install pipeline; this module is fully additive.
"""
from __future__ import annotations
import asyncio
import json
import os
import shutil
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Literal, Optional

import yaml
from pydantic import BaseModel, Field

from .backup import BackupError, get_backup, restore_backup
from .compatibility import load_lock, simulate_upgrade
from .config import WORK_DIR
from .events import bus
from .models import LogEvent
from .stack_manifest import load_manifest
from .state import store


# ---------- types ----------

UpgradeState = Literal[
    "preflight",
    "backup_verified",
    "compose_down",
    "image_pull",
    "compose_up",
    "smoke",
    "success",
    "failed",
    "rolled_back",
]


class UpgradeExecutionError(ValueError):
    """Pre-execution invariant breach — surfaced to the caller as 4xx."""


class UpgradeExecution(BaseModel):
    execution_id: str
    install_id: str
    stack_id: str
    proposed: dict[str, str]
    backup_id: str
    started_at: float
    finished_at: Optional[float] = None
    state: UpgradeState = "preflight"
    steps: list[dict] = Field(default_factory=list)
    error: Optional[str] = None
    proposed_new_lock: Optional[dict] = None


# ---------- persistence (mirrors IngestJob / BackupRecord pattern) ----------

_EXEC_FILE = WORK_DIR / "upgrade_executions.json"
_EXECUTIONS: dict[str, UpgradeExecution] = {}
_EXEC_LOCK = threading.RLock()
_EXEC_DIRTY = False
_EXEC_FLUSH_TIMER: Optional[threading.Timer] = None
_EXEC_WRITE_DEBOUNCE_SEC = 0.25


def _exec_atomic_write(data: str) -> None:
    _EXEC_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = _EXEC_FILE.with_suffix(_EXEC_FILE.suffix + ".tmp")
    tmp.write_text(data, encoding="utf-8")
    for _ in range(5):
        try:
            os.replace(tmp, _EXEC_FILE)
            return
        except PermissionError:
            time.sleep(0.1)


def _write_exec_now_locked() -> None:
    global _EXEC_DIRTY
    payload = {eid: e.model_dump() for eid, e in _EXECUTIONS.items()}
    _exec_atomic_write(json.dumps(payload, indent=2))
    _EXEC_DIRTY = False


def _persist_exec_locked(*, force: bool = False) -> None:
    global _EXEC_DIRTY, _EXEC_FLUSH_TIMER
    _EXEC_DIRTY = True
    if force:
        if _EXEC_FLUSH_TIMER is not None:
            _EXEC_FLUSH_TIMER.cancel()
            _EXEC_FLUSH_TIMER = None
        _write_exec_now_locked()
        return
    if _EXEC_FLUSH_TIMER is None:
        _EXEC_FLUSH_TIMER = threading.Timer(_EXEC_WRITE_DEBOUNCE_SEC, _flush_exec_from_timer)
        _EXEC_FLUSH_TIMER.daemon = True
        _EXEC_FLUSH_TIMER.start()


def _flush_exec_from_timer() -> None:
    global _EXEC_FLUSH_TIMER
    with _EXEC_LOCK:
        _EXEC_FLUSH_TIMER = None
        if _EXEC_DIRTY:
            try:
                _write_exec_now_locked()
            except Exception:
                import logging
                logging.getLogger("lhs.upgrade_executor").exception("execution flush failed")


def _load_executions() -> None:
    if not _EXEC_FILE.exists():
        return
    try:
        raw = json.loads(_EXEC_FILE.read_text(encoding="utf-8"))
    except Exception:
        return
    for eid, data in raw.items():
        try:
            _EXECUTIONS[eid] = UpgradeExecution(**data)
        except Exception:
            continue


_load_executions()


def list_executions(install_id: str) -> list[UpgradeExecution]:
    with _EXEC_LOCK:
        return sorted(
            (e for e in _EXECUTIONS.values() if e.install_id == install_id),
            key=lambda e: e.started_at,
            reverse=True,
        )


def get_execution(execution_id: str) -> Optional[UpgradeExecution]:
    with _EXEC_LOCK:
        return _EXECUTIONS.get(execution_id)


# ---------- helpers ----------

def _emit(install_id: str, line: str, *, stream: str = "stdout") -> None:
    bus.publish_nowait(LogEvent(
        install_id=install_id, ts=time.time(),
        kind="log", stream=stream,  # type: ignore[arg-type]
        step="upgrade", line=line,
    ))


def _save_locked(exec_rec: UpgradeExecution, *, force: bool = False) -> None:
    _EXECUTIONS[exec_rec.execution_id] = exec_rec
    _persist_exec_locked(force=force)


def _save(exec_rec: UpgradeExecution, *, force: bool = False) -> None:
    with _EXEC_LOCK:
        _save_locked(exec_rec, force=force)


def _record_step(exec_rec: UpgradeExecution, name: str, status: str, detail: str = "") -> None:
    exec_rec.steps.append({"name": name, "status": status, "detail": detail, "ts": time.time()})
    _emit(
        exec_rec.install_id,
        f"[upgrade] {name}: {status}{(' - ' + detail) if detail else ''}",
        stream="stdout" if status not in ("failed", "error") else "stderr",
    )
    _save(exec_rec)


async def _docker(args: list[str], *, cwd: Optional[Path] = None, timeout: int = 600) -> tuple[int, str, str]:
    """Run a docker command with explicit args (no shell). Returns (rc, out, err)."""
    proc = await asyncio.create_subprocess_exec(
        "docker", *args,
        cwd=str(cwd) if cwd else None,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        return 124, "", f"docker {' '.join(args)} timed out after {timeout}s"
    return (
        proc.returncode if proc.returncode is not None else 1,
        out.decode("utf-8", errors="replace"),
        err.decode("utf-8", errors="replace"),
    )


async def _bash(script_path: Path, *, cwd: Optional[Path] = None, timeout: int = 600) -> tuple[int, str, str]:
    """Run a bash script with explicit args (no shell). Returns (rc, out, err)."""
    proc = await asyncio.create_subprocess_exec(
        "bash", str(script_path),
        cwd=str(cwd) if cwd else None,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        return 124, "", f"bash {script_path} timed out after {timeout}s"
    return (
        proc.returncode if proc.returncode is not None else 1,
        out.decode("utf-8", errors="replace"),
        err.decode("utf-8", errors="replace"),
    )


def _build_overlay(
    proposed: dict[str, str],
    lock_components_by_id: dict[str, dict],
    manifest_components_by_id: dict[str, dict],
) -> tuple[str, dict[str, str], dict[str, str]]:
    """Render a docker-compose overlay that overrides image: for each proposed
    component. Returns (yaml_text, {component_id: service_name},
    {component_id: full_image_ref}).

    The yaml is intentionally minimal (only `services:` with `image:` entries)
    so we don't accidentally clobber compose features (volumes, env, deps).
    """
    services_block: dict[str, dict] = {}
    service_names: dict[str, str] = {}
    image_refs: dict[str, str] = {}
    for cid, new_tag in proposed.items():
        lock_comp = lock_components_by_id.get(cid, {})
        manifest_comp = manifest_components_by_id.get(cid, {})
        image_repo = lock_comp.get("image")
        if not image_repo:
            # Should have been caught by the unknown-component check upstream;
            # if not, refuse rather than write a half-broken overlay.
            raise UpgradeExecutionError(f"lock has no image for component '{cid}'")
        # Prefer manifest's service_name (the actual compose service); fall
        # back to the component id when no override is registered.
        service_name = manifest_comp.get("service_name") or cid
        full_ref = f"{image_repo}:{new_tag}"
        services_block[service_name] = {"image": full_ref}
        service_names[cid] = service_name
        image_refs[cid] = full_ref

    overlay = {"services": services_block}
    return yaml.safe_dump(overlay, sort_keys=True), service_names, image_refs


# ---------- main entrypoint ----------

async def execute_upgrade(
    install_id: str,
    proposed: dict[str, str],
    backup_id: str,
    running_states: Optional[frozenset[str]] = None,
    install_tasks: Optional[dict] = None,
) -> UpgradeExecution:
    """Run the upgrade pipeline against a live install.

    THIS IS DESTRUCTIVE. backup_id is REQUIRED. Any failure after compose_down
    triggers a rollback to the captured backup. Caller is responsible for
    passing in RUNNING_STATES + _INSTALL_TASKS from main.py (same dependency
    injection pattern as backup.restore_backup).
    """
    if not proposed:
        raise UpgradeExecutionError("proposed must be non-empty")
    if not backup_id:
        raise UpgradeExecutionError("backup_id is required for execute (no exceptions)")

    install_rec = store.get(install_id)
    if install_rec is None:
        raise UpgradeExecutionError(f"install {install_id!r} not found")
    install_dir = Path(install_rec.install_dir)
    if not install_dir.exists():
        raise UpgradeExecutionError(f"install_dir {install_dir} does not exist")

    stack_id = install_rec.stack_id
    lock = load_lock(stack_id)
    if lock is None:
        raise UpgradeExecutionError(f"no compatibility lock for stack '{stack_id}'")
    lock_by_id = {c["id"]: c for c in lock.get("components", [])}

    # Validate every proposed component_id exists in the lock — surfaces as
    # 400 in the FastAPI route.
    unknown = [cid for cid in proposed.keys() if cid not in lock_by_id]
    if unknown:
        raise UpgradeExecutionError(f"unknown component(s) in proposed: {unknown}")

    execution_id = f"upg_{uuid.uuid4().hex[:10]}"
    exec_rec = UpgradeExecution(
        execution_id=execution_id,
        install_id=install_id,
        stack_id=stack_id,
        proposed=proposed,
        backup_id=backup_id,
        started_at=time.time(),
        state="preflight",
    )
    _save(exec_rec, force=True)
    _emit(install_id, f"[upgrade] start execution_id={execution_id} proposed={proposed}")

    # ---- PREFLIGHT ----
    backup = await get_backup(backup_id)
    if backup is None:
        exec_rec.state = "failed"
        exec_rec.error = f"backup {backup_id!r} not found"
        exec_rec.finished_at = time.time()
        _record_step(exec_rec, "preflight", "failed", exec_rec.error)
        _save(exec_rec, force=True)
        return exec_rec
    if backup.install_id != install_id:
        exec_rec.state = "failed"
        exec_rec.error = (
            f"backup {backup_id!r} belongs to install {backup.install_id!r}, "
            f"not {install_id!r}"
        )
        exec_rec.finished_at = time.time()
        _record_step(exec_rec, "preflight", "failed", exec_rec.error)
        _save(exec_rec, force=True)
        return exec_rec

    # ---- STATE GATE ----
    if running_states and install_rec.state in running_states:
        exec_rec.state = "failed"
        exec_rec.error = (
            f"refuse to upgrade while install state is {install_rec.state}; cancel first"
        )
        exec_rec.finished_at = time.time()
        _record_step(exec_rec, "preflight", "failed", exec_rec.error)
        _save(exec_rec, force=True)
        return exec_rec
    if install_tasks is not None:
        live = install_tasks.get(install_id)
        if live is not None and not live.done():
            exec_rec.state = "failed"
            exec_rec.error = "an install task is still running; cancel before upgrade"
            exec_rec.finished_at = time.time()
            _record_step(exec_rec, "preflight", "failed", exec_rec.error)
            _save(exec_rec, force=True)
            return exec_rec

    # Try to load the manifest for service_name lookups. If the catalog has
    # drifted such that the manifest is gone, fall back to component id.
    try:
        manifest = load_manifest(stack_id)
        manifest_by_id = {c["id"]: c for c in manifest.components}
    except Exception:
        manifest_by_id = {}

    _record_step(exec_rec, "preflight", "passed",
                 f"backup {backup_id} owned by install, state={install_rec.state}")

    # ---- SIMULATE ----
    sim = await simulate_upgrade(stack_id, proposed)
    if sim.get("verdict") != "pass":
        exec_rec.state = "failed"
        exec_rec.error = f"simulate verdict={sim.get('verdict')!r}; refusing to execute"
        exec_rec.finished_at = time.time()
        _record_step(exec_rec, "simulate", "failed", exec_rec.error)
        exec_rec.steps[-1]["sim"] = sim  # attach simulation detail for the UI
        _save(exec_rec, force=True)
        return exec_rec
    _record_step(exec_rec, "simulate", "passed",
                 f"verdict=pass over {len(proposed)} components")

    exec_rec.state = "backup_verified"
    _save(exec_rec)

    # From here on, every failure must drop into the rollback path.
    base_compose = install_dir / "docker-compose.yml"
    overlay_path = install_dir / "docker-compose.upgrade.yml"
    smoke_script = install_dir / "scripts" / "lhs-smoke.sh"

    async def _rollback(reason: str) -> None:
        """Bring the stack back to the pre-upgrade state. Best-effort: log
        every step, but never raise out of the rollback itself."""
        _record_step(exec_rec, "rollback-start", "running", reason)
        try:
            # 1. tear down the (possibly broken) upgraded stack
            rc, out, err = await _docker(
                ["compose", "down"], cwd=install_dir, timeout=300,
            )
            _record_step(exec_rec, "rollback-compose-down",
                         "passed" if rc == 0 else "warning",
                         (err or out).strip()[:200])
            # 2. restore the backup (this overwrites install_dir/* with the snapshot)
            restore = await restore_backup(
                backup_id,
                running_states=None,  # we already gated upstream; restore must proceed now
                install_tasks=None,
            )
            _record_step(exec_rec, "rollback-restore",
                         "passed" if restore.success else "failed",
                         restore.error or "restored from backup")
            # 3. drop the overlay if it landed on disk
            try:
                overlay_path.unlink(missing_ok=True)
            except Exception:
                pass
            # 4. bring the stack back up against the BASE compose.yml only
            if base_compose.exists():
                rc, out, err = await _docker(
                    ["compose", "-f", str(base_compose), "up", "-d"],
                    cwd=install_dir, timeout=600,
                )
                _record_step(exec_rec, "rollback-compose-up",
                             "passed" if rc == 0 else "failed",
                             (err or out).strip()[:200])
            else:
                _record_step(exec_rec, "rollback-compose-up", "skipped",
                             "base compose.yml missing post-restore")
        except Exception as e:
            _record_step(exec_rec, "rollback-error", "failed",
                         f"{type(e).__name__}: {e}")

    try:
        # ---- COMPOSE DOWN ----
        exec_rec.state = "compose_down"
        _save(exec_rec)
        rc, out, err = await _docker(
            ["compose", "down"], cwd=install_dir, timeout=300,
        )
        if rc != 0:
            raise UpgradeExecutionError(
                f"compose down failed (rc={rc}): {(err or out).strip()[:300]}"
            )
        _record_step(exec_rec, "compose-down", "passed",
                     f"{len(out.splitlines())} stdout lines, {len(err.splitlines())} stderr lines")

        # ---- IMAGE PULL ----
        exec_rec.state = "image_pull"
        _save(exec_rec)
        pulled: list[str] = []
        for cid, new_tag in proposed.items():
            lock_comp = lock_by_id[cid]
            image_repo = lock_comp.get("image")
            if not image_repo:
                raise UpgradeExecutionError(f"lock has no image for component '{cid}'")
            full_ref = f"{image_repo}:{new_tag}"
            rc, out, err = await _docker(["pull", full_ref], timeout=900)
            if rc != 0:
                raise UpgradeExecutionError(
                    f"docker pull {full_ref} failed (rc={rc}): {(err or out).strip()[:300]}"
                )
            pulled.append(full_ref)
            _record_step(exec_rec, f"image-pull:{cid}", "passed", full_ref)
        _record_step(exec_rec, "image-pull", "passed", f"{len(pulled)} images")

        # ---- COMPOSE UP (with overlay) ----
        exec_rec.state = "compose_up"
        _save(exec_rec)
        overlay_yaml, service_names, image_refs = _build_overlay(
            proposed, lock_by_id, manifest_by_id,
        )
        overlay_path.write_text(overlay_yaml, encoding="utf-8")
        _record_step(exec_rec, "overlay-written", "passed",
                     f"{overlay_path.name} overrides {list(service_names.values())}")
        rc, out, err = await _docker(
            ["compose",
             "-f", str(base_compose),
             "-f", str(overlay_path),
             "up", "-d"],
            cwd=install_dir, timeout=900,
        )
        if rc != 0:
            raise UpgradeExecutionError(
                f"compose up failed (rc={rc}): {(err or out).strip()[:300]}"
            )
        _record_step(exec_rec, "compose-up", "passed", "containers started; waiting 60s")
        await asyncio.sleep(60)

        # ---- SMOKE ----
        exec_rec.state = "smoke"
        _save(exec_rec)
        if not smoke_script.exists():
            raise UpgradeExecutionError(
                f"smoke script missing: {smoke_script}"
            )
        rc, out, err = await _bash(smoke_script, cwd=install_dir, timeout=600)
        if rc != 0:
            raise UpgradeExecutionError(
                f"smoke failed (rc={rc}): {(err or out).strip()[:300]}"
            )
        _record_step(exec_rec, "smoke", "passed", "lhs-smoke.sh exit=0")

        # ---- SUCCESS ----
        exec_rec.state = "success"
        exec_rec.finished_at = time.time()
        # Build a non-mutating suggestion the operator can PR into the lock.
        proposed_new_lock = {
            "stack_id": stack_id,
            "based_on_version_id": lock.get("version_id"),
            "components": [
                {
                    "id": cid,
                    "image": lock_by_id[cid].get("image"),
                    "from_tag": lock_by_id[cid].get("tag"),
                    "to_tag": new_tag,
                }
                for cid, new_tag in proposed.items()
            ],
            "note": (
                "Upgrade executed and smoke passed at runtime. "
                "Lock file NOT auto-mutated — submit this as a manual PR with evidence."
            ),
        }
        exec_rec.proposed_new_lock = proposed_new_lock
        _record_step(exec_rec, "complete", "passed",
                     f"upgrade succeeded; {len(proposed)} components live on new tags")
        _save(exec_rec, force=True)
        return exec_rec

    except Exception as e:
        # Anything that escaped the pipeline after compose_down -> rollback.
        err_msg = f"{type(e).__name__}: {e}"
        _record_step(exec_rec, "upgrade-error", "failed", err_msg)
        await _rollback(err_msg)
        exec_rec.state = "rolled_back"
        exec_rec.error = err_msg
        exec_rec.finished_at = time.time()
        _save(exec_rec, force=True)
        return exec_rec
