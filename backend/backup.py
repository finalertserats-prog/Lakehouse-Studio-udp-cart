"""Backup / Restore — capture and re-apply a deployed lakehouse install.

Additive v0.5.0 module (matches notifications.py / ingest.py shape).

Two backup kinds:
  - "metadata": just the install dir scaffolding (compose, .env, scripts) + a
    JSON snapshot of the InstallRecord. Cheap; restores config drift.
  - "full":     metadata + a `mc mirror minio/datalake` so MinIO bucket data
    is captured too. Bigger; restores actual lakehouse contents.

Tarballs live at: WORK_DIR / "backups" / {install_id} / {backup_id}.tar.gz
Sidecar manifest at the same path with `.json` suffix (so list_backups can
scan without untarring every archive).

Restore is gated by RUNNING_STATES + active _INSTALL_TASKS — refuses to
overwrite an install that's mid-pipeline. Caller passes these in from main.py
so backup.py doesn't have to import the FastAPI module (avoids a cycle).
"""
from __future__ import annotations
import asyncio
import json
import logging
import os
import shutil
import tarfile
import time
import uuid
from pathlib import Path
from typing import Literal, Optional

from pydantic import BaseModel, Field

from .config import WORK_DIR
from .events import bus
from .models import LogEvent
from .state import store


BackupKind = Literal["metadata", "full"]

_BACKUPS_ROOT = WORK_DIR / "backups"
_MINIO_CLIENT_CONTAINER = "udp-minio-client"
_MINIO_ALIAS = "minio"
_MINIO_BUCKET = "datalake"

_SCHEDULES_FILE = WORK_DIR / "backup_schedules.json"
_SCHEDULER_TICK_SEC = 60.0

_log = logging.getLogger("lhs.backup.scheduler")


class BackupError(ValueError):
    """Safety violation or invariant breakage during backup/restore."""


class BackupRecord(BaseModel):
    backup_id: str
    install_id: str
    kind: BackupKind
    created_at: float
    size_bytes: int
    path: str
    manifest_summary: dict = Field(default_factory=dict)


class RestoreResult(BaseModel):
    restore_id: str
    backup_id: str
    install_id: str
    started_at: float
    finished_at: float
    success: bool
    steps: list[dict] = Field(default_factory=list)
    error: Optional[str] = None


# ---------- helpers ----------

def _emit(install_id: str, line: str, *, stream: str = "stdout") -> None:
    bus.publish_nowait(LogEvent(
        install_id=install_id, ts=time.time(),
        kind="log", stream=stream,  # type: ignore[arg-type]
        step="backup", line=line,
    ))


def _backup_dir_for(install_id: str, *, make: bool = True) -> Path:
    """Return the per-install backup dir.

    `make=True` (default) creates the directory; legacy write paths assumed
    this side effect. `make=False` is for READ-ONLY callers (the DR drill
    integrity probe, listing endpoints) where mkdir on a bogus install_id
    would silently create a phantom backup directory. Codex-flagged 2026-05-17.
    """
    d = _BACKUPS_ROOT / install_id
    if make:
        d.mkdir(parents=True, exist_ok=True)
    return d


def _tar_path(install_id: str, backup_id: str, *, make: bool = True) -> Path:
    return _backup_dir_for(install_id, make=make) / f"{backup_id}.tar.gz"


def _sidecar_path(install_id: str, backup_id: str, *, make: bool = True) -> Path:
    return _backup_dir_for(install_id, make=make) / f"{backup_id}.tar.gz.json"


async def _docker_exec(args: list[str], *, timeout: int = 300) -> tuple[int, str, str]:
    """Run a docker command, capture stdout/stderr. Returns (rc, out, err)."""
    proc = await asyncio.create_subprocess_exec(
        "docker", *args,
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


async def _minio_client_running() -> bool:
    rc, out, _ = await _docker_exec(
        ["ps", "--filter", f"name={_MINIO_CLIENT_CONTAINER}",
         "--filter", "status=running", "--format", "{{.Names}}"],
        timeout=10,
    )
    return rc == 0 and _MINIO_CLIENT_CONTAINER in out


async def _mc_mirror_into_tar(install_id: str, scratch_dir: Path) -> tuple[bool, str]:
    """`mc mirror minio/datalake` from the minio-client container into a
    host-side scratch dir we then add to the tarball. Returns (ok, detail)."""
    if not await _minio_client_running():
        return False, f"{_MINIO_CLIENT_CONTAINER} not running; skipping mc mirror"
    # mc mirror writes inside the container; we then `docker cp` it out.
    container_tmp = "/tmp/lhs-backup-mirror"
    rc, _, err = await _docker_exec(
        ["exec", _MINIO_CLIENT_CONTAINER, "sh", "-c",
         f"rm -rf {container_tmp} && mkdir -p {container_tmp} && "
         f"mc mirror --overwrite {_MINIO_ALIAS}/{_MINIO_BUCKET} {container_tmp}"],
        timeout=1800,
    )
    if rc != 0:
        return False, f"mc mirror failed (rc={rc}): {err.strip()[:200]}"
    rc, _, err = await _docker_exec(
        ["cp", f"{_MINIO_CLIENT_CONTAINER}:{container_tmp}/.",
         str(scratch_dir)],
        timeout=600,
    )
    if rc != 0:
        return False, f"docker cp from minio-client failed (rc={rc}): {err.strip()[:200]}"
    # Best-effort cleanup inside the container.
    await _docker_exec(
        ["exec", _MINIO_CLIENT_CONTAINER, "rm", "-rf", container_tmp],
        timeout=30,
    )
    return True, "mc mirror complete"


async def _mc_mirror_restore(install_id: str, scratch_dir: Path) -> tuple[bool, str]:
    if not await _minio_client_running():
        return False, f"{_MINIO_CLIENT_CONTAINER} not running; skipping mc restore"
    container_tmp = "/tmp/lhs-restore-mirror"
    # Stage the host scratch dir inside the container, then mc mirror it back.
    rc, _, err = await _docker_exec(
        ["exec", _MINIO_CLIENT_CONTAINER, "sh", "-c",
         f"rm -rf {container_tmp} && mkdir -p {container_tmp}"],
        timeout=30,
    )
    if rc != 0:
        return False, f"failed to prep restore tmp: {err.strip()[:200]}"
    rc, _, err = await _docker_exec(
        ["cp", f"{scratch_dir}/.",
         f"{_MINIO_CLIENT_CONTAINER}:{container_tmp}"],
        timeout=600,
    )
    if rc != 0:
        return False, f"docker cp to minio-client failed (rc={rc}): {err.strip()[:200]}"
    rc, _, err = await _docker_exec(
        ["exec", _MINIO_CLIENT_CONTAINER, "sh", "-c",
         f"mc mirror --overwrite {container_tmp} {_MINIO_ALIAS}/{_MINIO_BUCKET}"],
        timeout=1800,
    )
    if rc != 0:
        return False, f"mc mirror restore failed (rc={rc}): {err.strip()[:200]}"
    await _docker_exec(
        ["exec", _MINIO_CLIENT_CONTAINER, "rm", "-rf", container_tmp],
        timeout=30,
    )
    return True, "mc mirror restore complete"


# ---------- backup ----------

# Files / dirs we always include in a metadata backup (relative to install_dir).
_METADATA_ENTRIES: tuple[str, ...] = ("docker-compose.yml", ".env", "scripts")


async def create_backup(install_id: str, kind: BackupKind = "metadata") -> BackupRecord:
    """Build a tarball + sidecar manifest for the given install."""
    rec = store.get(install_id)
    if rec is None:
        raise BackupError(f"install {install_id!r} not found")
    install_dir = Path(rec.install_dir)
    if not install_dir.exists():
        raise BackupError(f"install_dir {install_dir} does not exist")

    backup_id = uuid.uuid4().hex
    tar_path = _tar_path(install_id, backup_id)
    sidecar = _sidecar_path(install_id, backup_id)
    created_at = time.time()

    _emit(install_id, f"[backup] start kind={kind} id={backup_id}")

    # Pre-write the install record snapshot to a temp file inside backups dir
    # so it gets included in the tarball under a stable name.
    snapshot_path = _backup_dir_for(install_id) / f"{backup_id}.install_record.json"
    snapshot_path.write_text(json.dumps(rec.model_dump(), indent=2), encoding="utf-8")

    minio_scratch: Optional[Path] = None
    minio_ok = False
    minio_detail = "metadata-only backup"
    files_count = 0

    try:
        with tarfile.open(tar_path, mode="w:gz") as tar:
            # Metadata entries from the install_dir
            for rel in _METADATA_ENTRIES:
                src = install_dir / rel
                if src.exists():
                    tar.add(str(src), arcname=f"install_dir/{rel}")
                    files_count += sum(1 for _ in src.rglob("*")) if src.is_dir() else 1
            # Install record snapshot
            tar.add(str(snapshot_path), arcname="install_record.json")
            files_count += 1

            if kind == "full":
                minio_scratch = _backup_dir_for(install_id) / f"{backup_id}.minio"
                minio_scratch.mkdir(parents=True, exist_ok=True)
                minio_ok, minio_detail = await _mc_mirror_into_tar(install_id, minio_scratch)
                if minio_ok:
                    tar.add(str(minio_scratch), arcname="minio_data")
                    files_count += sum(1 for _ in minio_scratch.rglob("*"))
                else:
                    _emit(install_id, f"[backup] mc mirror skipped: {minio_detail}", stream="stderr")
    finally:
        # Always clean up the on-disk snapshot + scratch dir (they're now in
        # the tarball or were never useful).
        snapshot_path.unlink(missing_ok=True)
        if minio_scratch is not None:
            shutil.rmtree(minio_scratch, ignore_errors=True)

    manifest_summary = {
        "stack_id": rec.stack_id,
        "source_install_dir": str(install_dir),
        "files_count": files_count,
        "contains_minio_data": bool(kind == "full" and minio_ok),
        "minio_detail": minio_detail,
    }
    size_bytes = tar_path.stat().st_size if tar_path.exists() else 0

    record = BackupRecord(
        backup_id=backup_id,
        install_id=install_id,
        kind=kind,
        created_at=created_at,
        size_bytes=size_bytes,
        path=str(tar_path),
        manifest_summary=manifest_summary,
    )
    sidecar.write_text(json.dumps(record.model_dump(), indent=2), encoding="utf-8")

    _emit(install_id,
          f"[backup] done id={backup_id} size={size_bytes} files={files_count} "
          f"minio={'yes' if manifest_summary['contains_minio_data'] else 'no'}")
    return record


# ---------- list / get / delete ----------

async def list_backups(install_id: str) -> list[BackupRecord]:
    out: list[BackupRecord] = []
    target = _BACKUPS_ROOT / install_id
    if not target.exists():
        return out
    for side in sorted(target.glob("*.tar.gz.json")):
        try:
            raw = json.loads(side.read_text(encoding="utf-8"))
            out.append(BackupRecord(**raw))
        except Exception:
            continue
    return sorted(out, key=lambda r: r.created_at, reverse=True)


async def get_backup(backup_id: str) -> Optional[BackupRecord]:
    if not _BACKUPS_ROOT.exists():
        return None
    for side in _BACKUPS_ROOT.glob(f"*/{backup_id}.tar.gz.json"):
        try:
            raw = json.loads(side.read_text(encoding="utf-8"))
            return BackupRecord(**raw)
        except Exception:
            continue
    return None


async def delete_backup(backup_id: str) -> None:
    rec = await get_backup(backup_id)
    if rec is None:
        raise BackupError(f"backup {backup_id!r} not found")
    Path(rec.path).unlink(missing_ok=True)
    sidecar = Path(rec.path + ".json")
    sidecar.unlink(missing_ok=True)


# ---------- restore ----------

async def restore_backup(
    backup_id: str,
    *,
    running_states: Optional[frozenset[str]] = None,
    install_tasks: Optional[dict] = None,
    restore_data: bool = True,
) -> RestoreResult:
    """Extract a backup over its source install_dir.

    `restore_data=False` skips the MinIO data restore even if the backup is
    "full" — used by upgrade-executor rollback so we never clobber object-
    storage contents the user wrote AFTER the backup but BEFORE the rollback.
    Metadata (compose, .env, scripts) still restores either way; those are
    the only files the upgrade path could have touched.

    Caller (the FastAPI route) passes RUNNING_STATES + _INSTALL_TASKS so we
    refuse to clobber a mid-pipeline install. Pure no-op if the install is
    in a terminal state and no live task is registered.
    """
    rec = await get_backup(backup_id)
    if rec is None:
        raise BackupError(f"backup {backup_id!r} not found")

    install_rec = store.get(rec.install_id)
    if install_rec is None:
        raise BackupError(f"install {rec.install_id!r} no longer exists")

    if running_states and install_rec.state in running_states:
        raise BackupError(
            f"cannot restore while install state is {install_rec.state}; cancel first"
        )
    if install_tasks is not None:
        live = install_tasks.get(rec.install_id)
        if live is not None and not live.done():
            raise BackupError("an install task is still running; cancel before restore")

    restore_id = uuid.uuid4().hex
    started_at = time.time()
    steps: list[dict] = []

    def _step(name: str, status: str, detail: str = "") -> None:
        steps.append({"name": name, "status": status, "detail": detail})
        _emit(rec.install_id,
              f"[restore] {name}: {status}{(' — ' + detail) if detail else ''}",
              stream="stdout" if status != "failed" else "stderr")

    install_dir = Path(install_rec.install_dir)
    minio_scratch: Optional[Path] = None

    try:
        tar_path = Path(rec.path)
        if not tar_path.exists():
            raise BackupError(f"backup tarball missing: {tar_path}")
        _step("locate-tarball", "passed", str(tar_path))

        install_dir.mkdir(parents=True, exist_ok=True)

        with tarfile.open(tar_path, mode="r:gz") as tar:
            # Extract install_dir/* over the live install_dir, and stage minio_data
            # into a scratch dir for the mc restore step.
            members = tar.getmembers()
            install_members = [m for m in members if m.name.startswith("install_dir/")]
            minio_members = [m for m in members if m.name.startswith("minio_data/")]

            for m in install_members:
                # Strip the leading "install_dir/" so it lands at install_dir root.
                m.name = m.name[len("install_dir/"):]
                if not m.name:
                    continue
                # Zip-slip guard: reject any member whose resolved path escapes
                # install_dir. Defense-in-depth — backups are produced by our
                # own create_backup today, but a future "import backup" flow
                # could expose us to hostile archives.
                resolved = (install_dir / m.name).resolve()
                if not str(resolved).startswith(str(install_dir.resolve())):
                    raise BackupError(
                        f"refusing to extract {m.name!r}: escapes install_dir"
                    )
                tar.extract(m, path=install_dir)
            _step("extract-metadata", "passed",
                  f"{len(install_members)} entries -> {install_dir}")

            if minio_members and rec.kind == "full" and restore_data:
                minio_scratch = _backup_dir_for(rec.install_id) / f"restore_{restore_id}.minio"
                minio_scratch.mkdir(parents=True, exist_ok=True)
                for m in minio_members:
                    m.name = m.name[len("minio_data/"):]
                    if not m.name:
                        continue
                    # Same zip-slip guard for the minio scratch dir.
                    resolved = (minio_scratch / m.name).resolve()
                    if not str(resolved).startswith(str(minio_scratch.resolve())):
                        raise BackupError(
                            f"refusing to extract {m.name!r}: escapes minio_scratch"
                        )
                    tar.extract(m, path=minio_scratch)
                _step("extract-minio", "passed", f"{len(minio_members)} entries staged")
            elif rec.kind == "full" and not restore_data:
                _step("extract-minio", "skipped",
                      "restore_data=False (upgrade rollback) — preserving live MinIO contents")
            elif rec.kind == "full":
                _step("extract-minio", "skipped", "no minio_data in tarball")

        if rec.kind == "full" and restore_data and minio_scratch is not None:
            ok, detail = await _mc_mirror_restore(rec.install_id, minio_scratch)
            _step("mc-restore", "passed" if ok else "warning", detail)

        # If the install was idle (CLEANED/STOPPED) the restored config is
        # effectively the new READY baseline. Don't downgrade an already-READY
        # install or override a FAILED one.
        if install_rec.state in ("CLEANED", "STOPPED"):
            store.update_state(rec.install_id, "READY")
            _step("state-update", "passed", "CLEANED/STOPPED -> READY")
        else:
            _step("state-update", "skipped", f"state={install_rec.state}; not touching")

        return RestoreResult(
            restore_id=restore_id,
            backup_id=backup_id,
            install_id=rec.install_id,
            started_at=started_at,
            finished_at=time.time(),
            success=True,
            steps=steps,
            error=None,
        )
    except BackupError as e:
        _step("error", "failed", str(e))
        return RestoreResult(
            restore_id=restore_id,
            backup_id=backup_id,
            install_id=rec.install_id,
            started_at=started_at,
            finished_at=time.time(),
            success=False,
            steps=steps,
            error=str(e),
        )
    except Exception as e:
        _step("error", "failed", f"{type(e).__name__}: {e}")
        return RestoreResult(
            restore_id=restore_id,
            backup_id=backup_id,
            install_id=rec.install_id,
            started_at=started_at,
            finished_at=time.time(),
            success=False,
            steps=steps,
            error=f"{type(e).__name__}: {e}",
        )
    finally:
        if minio_scratch is not None:
            shutil.rmtree(minio_scratch, ignore_errors=True)


# ---------- scheduling ----------


class BackupSchedule(BaseModel):
    install_id: str
    enabled: bool = False
    interval_hours: int = Field(default=24, ge=1, le=168)
    kind: Literal["metadata", "full"] = "metadata"
    last_run_at: Optional[float] = None
    next_run_at: Optional[float] = None


def _read_schedules_file() -> dict[str, dict]:
    if not _SCHEDULES_FILE.exists():
        return {}
    try:
        raw = json.loads(_SCHEDULES_FILE.read_text(encoding="utf-8"))
    except Exception:
        # Corrupt schedules file — back it up and start fresh so the scheduler
        # never wedges on a bad JSON parse.
        try:
            _SCHEDULES_FILE.replace(_SCHEDULES_FILE.with_suffix(".json.bak"))
        except Exception:
            pass
        return {}
    if not isinstance(raw, dict):
        return {}
    return raw


def _atomic_write_schedules(data: dict[str, dict]) -> None:
    """tmp-write + replace, with retry on Windows AV/OneDrive flakiness.
    Mirrors backend/state.py's _atomic_write pattern."""
    _SCHEDULES_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = _SCHEDULES_FILE.with_suffix(_SCHEDULES_FILE.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
    last_err: Optional[Exception] = None
    delay = 0.1
    for _ in range(5):
        try:
            os.replace(tmp, _SCHEDULES_FILE)
            return
        except PermissionError as e:
            last_err = e
            time.sleep(delay)
            delay = min(delay * 2, 1.0)
    if last_err is not None:
        raise last_err


def load_schedule(install_id: str) -> BackupSchedule:
    """Return the saved schedule for `install_id`, or a disabled default."""
    raw = _read_schedules_file()
    entry = raw.get(install_id)
    if not isinstance(entry, dict):
        return BackupSchedule(install_id=install_id)
    try:
        # Force install_id to match the key so we never return a mismatched record.
        entry = {**entry, "install_id": install_id}
        return BackupSchedule(**entry)
    except Exception:
        return BackupSchedule(install_id=install_id)


def save_schedule(install_id: str, body: dict) -> BackupSchedule:
    """Upsert a schedule. If enabled, recompute next_run_at = now + interval."""
    current = load_schedule(install_id)
    merged = current.model_dump()
    # Only accept the fields the caller is allowed to change; install_id is
    # bound to the path param, last_run_at is server-owned.
    for key in ("enabled", "interval_hours", "kind"):
        if key in body and body[key] is not None:
            merged[key] = body[key]
    merged["install_id"] = install_id

    schedule = BackupSchedule(**merged)
    if schedule.enabled:
        now = time.time()
        # Recompute on every save so a flipped-enabled or shrunk-interval
        # schedule fires off the new cadence, not the old one.
        schedule.next_run_at = now + schedule.interval_hours * 3600
    else:
        schedule.next_run_at = None

    all_schedules = _read_schedules_file()
    all_schedules[install_id] = schedule.model_dump()
    _atomic_write_schedules(all_schedules)
    return schedule


def list_all_schedules() -> list[BackupSchedule]:
    """Return every persisted schedule. Used by the scheduler loop."""
    out: list[BackupSchedule] = []
    raw = _read_schedules_file()
    for install_id, entry in raw.items():
        if not isinstance(entry, dict):
            continue
        try:
            entry = {**entry, "install_id": install_id}
            out.append(BackupSchedule(**entry))
        except Exception:
            continue
    return out


def _persist_run_update(install_id: str, *, last_run_at: float, next_run_at: Optional[float]) -> None:
    """Patch only the run-timestamp fields on an existing schedule entry.

    Done as a focused read-modify-write so we don't clobber a concurrent
    save_schedule() that may have toggled enabled/interval between our read
    of list_all_schedules() and this update.
    """
    all_schedules = _read_schedules_file()
    entry = all_schedules.get(install_id)
    if not isinstance(entry, dict):
        return
    entry["last_run_at"] = last_run_at
    if next_run_at is None:
        entry.pop("next_run_at", None)
    else:
        entry["next_run_at"] = next_run_at
    all_schedules[install_id] = entry
    _atomic_write_schedules(all_schedules)


class BackupScheduler:
    """Async loop that fires scheduled backups.

    Tick interval is _SCHEDULER_TICK_SEC (60s). Each tick scans all schedules
    and for every enabled one whose next_run_at <= now, calls create_backup.
    A create_backup exception MUST NOT crash the loop — we wrap it, publish
    to the bus, and leave next_run_at intact so the next tick retries.
    """

    def __init__(self) -> None:
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._run_loop(), name="backup-scheduler")

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None

    async def _run_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=_SCHEDULER_TICK_SEC)
                return
            except asyncio.TimeoutError:
                pass
            try:
                await self._tick()
            except Exception:
                # Catch-all: a malformed schedules file or unexpected I/O error
                # in _tick must never kill the scheduler task.
                _log.exception("backup scheduler tick failed")

    async def _tick(self) -> None:
        now = time.time()
        for sched in list_all_schedules():
            if not sched.enabled:
                continue
            if sched.next_run_at is None or sched.next_run_at > now:
                continue
            await self._fire(sched, now)

    async def _fire(self, sched: BackupSchedule, now: float) -> None:
        try:
            await create_backup(sched.install_id, kind=sched.kind)
        except BackupError as e:
            # Expected domain failure (install missing, dir gone) — log to the
            # install's bus so the UI surfaces it, and leave next_run_at intact
            # so the next tick retries without piling up missed runs.
            _emit(
                sched.install_id,
                f"[backup-scheduler] scheduled backup failed: {e}",
                stream="stderr",
            )
            return
        except Exception as e:
            # Unexpected failure (docker hiccup, disk full). Same retry policy:
            # next_run_at stays put so we try again on the next tick.
            _emit(
                sched.install_id,
                f"[backup-scheduler] scheduled backup error: {type(e).__name__}: {e}",
                stream="stderr",
            )
            _log.exception("scheduled backup raised for install=%s", sched.install_id)
            return
        # Success: advance the cursor.
        next_run = now + sched.interval_hours * 3600
        try:
            _persist_run_update(sched.install_id, last_run_at=now, next_run_at=next_run)
        except Exception:
            _log.exception("failed to persist schedule cursor for %s", sched.install_id)


_GLOBAL_SCHEDULER: Optional[BackupScheduler] = None


def get_scheduler() -> BackupScheduler:
    """Lazy singleton accessor for the global backup scheduler."""
    global _GLOBAL_SCHEDULER
    if _GLOBAL_SCHEDULER is None:
        _GLOBAL_SCHEDULER = BackupScheduler()
    return _GLOBAL_SCHEDULER


# ---------------------------------------------------------------------------
# DR drill — non-destructive backup integrity verification
#
# A "DR drill" verifies that backup tarballs are READABLE and structurally
# sane WITHOUT touching the live install. We do NOT call restore_backup
# here because that's destructive (it overwrites the install_dir and can
# trigger an mc mirror restore). The drill walks the most recent backup
# for each install, opens its .tar.gz, validates the manifest sidecar
# matches the tarball contents, parses install_record.json, and confirms
# that `full` backups contain a minio_data tree.
#
# Opt-in via LHS_DR_DRILL_ENABLED. Interval defaults to weekly (604800s)
# and can be tuned via LHS_DR_DRILL_INTERVAL_SECONDS.
# ---------------------------------------------------------------------------


class DrillResult(BaseModel):
    backup_id: str
    install_id: str
    kind: BackupKind
    size_bytes: int
    members_count: int
    has_install_record: bool
    has_minio_data: bool  # True iff a minio_data/* member was present
    install_record_ok: bool  # parsed cleanly + install_id matches
    ok: bool
    errors: list[str] = Field(default_factory=list)
    drilled_at: float


def _verify_sync(install_id: str, backup_id: str) -> DrillResult:
    """Synchronous integrity check on one backup tarball. Runs in a worker
    thread (gzip + tar are CPU-bound) so the scheduler loop isn't blocked
    by a multi-GB `full` backup."""
    # Read-only paths: never create the per-install dir as a side effect.
    tar_path = _tar_path(install_id, backup_id, make=False)
    sidecar = _sidecar_path(install_id, backup_id, make=False)

    errors: list[str] = []
    members_count = 0
    has_install_record = False
    has_minio_data = False
    install_record_ok = False
    size_bytes = 0
    kind: BackupKind = "metadata"

    if not sidecar.exists():
        errors.append(f"sidecar manifest missing at {sidecar.name}")
    else:
        try:
            sidecar_data = json.loads(sidecar.read_text(encoding="utf-8"))
            kind = sidecar_data.get("kind") or "metadata"
            size_bytes = int(sidecar_data.get("size_bytes") or 0)
        except Exception as e:
            errors.append(f"sidecar parse failed: {type(e).__name__}: {e}")

    if not tar_path.exists():
        errors.append(f"tarball missing at {tar_path.name}")
        return DrillResult(
            backup_id=backup_id, install_id=install_id, kind=kind,
            size_bytes=size_bytes, members_count=0,
            has_install_record=False, has_minio_data=False,
            install_record_ok=False, ok=False, errors=errors,
            drilled_at=time.time(),
        )

    try:
        with tarfile.open(tar_path, mode="r:gz") as tar:
            for member in tar:
                members_count += 1
                if member.name == "install_record.json":
                    has_install_record = True
                    # Cap on install_record.json — realistically a few KB.
                    # Without this, a corrupted tarball could OOM the worker
                    # via an unbounded `f.read()`. Codex-flagged 2026-05-17.
                    _INSTALL_RECORD_MAX_BYTES = 1 * 1024 * 1024  # 1 MB
                    if int(getattr(member, "size", 0) or 0) > _INSTALL_RECORD_MAX_BYTES:
                        errors.append(
                            f"install_record.json suspiciously large "
                            f"({member.size} bytes) — refusing to read"
                        )
                    else:
                        try:
                            f = tar.extractfile(member)
                            if f is None:
                                errors.append("install_record.json is not a regular file")
                            else:
                                raw = f.read(_INSTALL_RECORD_MAX_BYTES + 1).decode("utf-8")
                                if len(raw) > _INSTALL_RECORD_MAX_BYTES:
                                    errors.append(
                                        "install_record.json exceeded read cap"
                                    )
                                else:
                                    doc = json.loads(raw)
                                    if doc.get("install_id") != install_id:
                                        errors.append(
                                            f"install_record.json install_id mismatch: "
                                            f"got {doc.get('install_id')!r}, want {install_id!r}"
                                        )
                                    else:
                                        install_record_ok = True
                        except Exception as e:
                            errors.append(
                                f"install_record.json parse failed: {type(e).__name__}: {e}"
                            )
                elif member.name.startswith("minio_data/"):
                    has_minio_data = True
    except tarfile.TarError as e:
        errors.append(f"tar open/iter failed: {type(e).__name__}: {e}")
    except OSError as e:
        errors.append(f"tar I/O failed: {type(e).__name__}: {e}")

    if not has_install_record and not errors:
        errors.append("tarball missing install_record.json")
    if kind == "full" and not has_minio_data and not errors:
        errors.append("kind=full but tarball has no minio_data/ tree")

    return DrillResult(
        backup_id=backup_id,
        install_id=install_id,
        kind=kind,
        size_bytes=size_bytes,
        members_count=members_count,
        has_install_record=has_install_record,
        has_minio_data=has_minio_data,
        install_record_ok=install_record_ok,
        ok=(not errors),
        errors=errors,
        drilled_at=time.time(),
    )


async def verify_backup(install_id: str, backup_id: str) -> DrillResult:
    """Non-destructive backup integrity check. Returns a DrillResult."""
    return await asyncio.to_thread(_verify_sync, install_id, backup_id)


async def verify_latest_backups() -> list[DrillResult]:
    """Verify the most recent backup for every install that has at least
    one. Returns the per-install DrillResult list (empty when no backups
    exist anywhere on disk)."""
    out: list[DrillResult] = []
    if not _BACKUPS_ROOT.exists():
        return out
    for install_dir in sorted(_BACKUPS_ROOT.iterdir()):
        if not install_dir.is_dir():
            continue
        install_id = install_dir.name
        try:
            records = await list_backups(install_id)
        except Exception:
            _log.exception("list_backups failed for %s", install_id)
            continue
        if not records:
            continue
        # list_backups returns sidecars in glob order — pick newest by created_at.
        latest = max(records, key=lambda r: r.created_at)
        try:
            result = await verify_backup(install_id, latest.backup_id)
        except Exception as e:
            result = DrillResult(
                backup_id=latest.backup_id,
                install_id=install_id,
                kind=latest.kind,
                size_bytes=latest.size_bytes,
                members_count=0,
                has_install_record=False, has_minio_data=False,
                install_record_ok=False, ok=False,
                errors=[f"verify raised: {type(e).__name__}: {e}"],
                drilled_at=time.time(),
            )
        out.append(result)
        if result.ok:
            _emit(install_id,
                  f"[dr-drill] backup {latest.backup_id[:12]} OK "
                  f"(kind={result.kind}, {result.members_count} members)")
        else:
            _emit(install_id,
                  f"[dr-drill] backup {latest.backup_id[:12]} FAILED: "
                  f"{'; '.join(result.errors[:3])}",
                  stream="stderr")
    return out


_DRILL_LOG = logging.getLogger("lhs.backup.dr-drill")
_DRILL_INTERVAL_ENV = "LHS_DR_DRILL_INTERVAL_SECONDS"
_DRILL_ENABLED_ENV = "LHS_DR_DRILL_ENABLED"
_DRILL_DEFAULT_INTERVAL = 604800.0  # one week


def is_drill_enabled() -> bool:
    raw = os.environ.get(_DRILL_ENABLED_ENV)
    if not raw:
        return False
    return raw.strip().lower() in {"true", "1", "yes", "on"}


def _drill_interval_seconds() -> float:
    raw = os.environ.get(_DRILL_INTERVAL_ENV)
    if not raw:
        return _DRILL_DEFAULT_INTERVAL
    try:
        n = float(raw.strip())
    except ValueError:
        _DRILL_LOG.warning(
            "invalid %s=%r; falling back to %d",
            _DRILL_INTERVAL_ENV, raw, int(_DRILL_DEFAULT_INTERVAL),
        )
        return _DRILL_DEFAULT_INTERVAL
    if n <= 0:
        _DRILL_LOG.warning(
            "%s=%s must be > 0; falling back to %d",
            _DRILL_INTERVAL_ENV, n, int(_DRILL_DEFAULT_INTERVAL),
        )
        return _DRILL_DEFAULT_INTERVAL
    return n


class BackupDrillScheduler:
    """Periodic non-destructive backup integrity verifier.

    Mirrors the AuditSubscriber/RetentionScheduler shape. Opt-in via
    LHS_DR_DRILL_ENABLED. Per-iteration exceptions are caught so a single
    bad backup can't crash the loop."""

    def __init__(self) -> None:
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="backup-dr-drill")

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.sleep(_drill_interval_seconds())
            except asyncio.CancelledError:
                raise
            if self._stop.is_set():
                break
            try:
                results = await verify_latest_backups()
                bad = [r for r in results if not r.ok]
                _DRILL_LOG.info(
                    "dr-drill verified %d backups (%d failed)",
                    len(results), len(bad),
                )
            except Exception:
                _DRILL_LOG.exception("dr-drill iteration failed")


_GLOBAL_DRILL_SCHEDULER: Optional[BackupDrillScheduler] = None


def get_drill_scheduler() -> BackupDrillScheduler:
    """Lazy singleton accessor for the DR drill scheduler."""
    global _GLOBAL_DRILL_SCHEDULER
    if _GLOBAL_DRILL_SCHEDULER is None:
        _GLOBAL_DRILL_SCHEDULER = BackupDrillScheduler()
    return _GLOBAL_DRILL_SCHEDULER


def reset_drill_scheduler_for_tests() -> None:
    global _GLOBAL_DRILL_SCHEDULER
    _GLOBAL_DRILL_SCHEDULER = None
