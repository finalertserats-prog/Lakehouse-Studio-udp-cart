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


def _backup_dir_for(install_id: str) -> Path:
    d = _BACKUPS_ROOT / install_id
    d.mkdir(parents=True, exist_ok=True)
    return d


def _tar_path(install_id: str, backup_id: str) -> Path:
    return _backup_dir_for(install_id) / f"{backup_id}.tar.gz"


def _sidecar_path(install_id: str, backup_id: str) -> Path:
    return _backup_dir_for(install_id) / f"{backup_id}.tar.gz.json"


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
) -> RestoreResult:
    """Extract a backup over its source install_dir.

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
                tar.extract(m, path=install_dir)
            _step("extract-metadata", "passed",
                  f"{len(install_members)} entries -> {install_dir}")

            if minio_members and rec.kind == "full":
                minio_scratch = _backup_dir_for(rec.install_id) / f"restore_{restore_id}.minio"
                minio_scratch.mkdir(parents=True, exist_ok=True)
                for m in minio_members:
                    m.name = m.name[len("minio_data/"):]
                    if not m.name:
                        continue
                    tar.extract(m, path=minio_scratch)
                _step("extract-minio", "passed", f"{len(minio_members)} entries staged")
            elif rec.kind == "full":
                _step("extract-minio", "skipped", "no minio_data in tarball")

        if rec.kind == "full" and minio_scratch is not None:
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
