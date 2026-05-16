from __future__ import annotations
import json
import os
import threading
import time
import uuid
from pathlib import Path
from typing import Optional

from .config import STATE_FILE
from .models import InstallRecord, InstallState, StepStatus


# Any state that is not one of these is "in-flight" and should be reconciled
# back to FAILED on server restart.
_TERMINAL_STATES: frozenset[str] = frozenset({"READY", "FAILED", "STOPPED", "CLEANED", "DRAFT"})


def _atomic_write(path: Path, data: str, *, retries: int = 5, delay: float = 0.1) -> None:
    """tmp-write + replace, with retry on Windows AV/OneDrive flakiness."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(data, encoding="utf-8")
    last_err: Exception | None = None
    for _ in range(retries):
        try:
            os.replace(tmp, path)
            return
        except PermissionError as e:
            last_err = e
            time.sleep(delay)
            delay = min(delay * 2, 1.0)
    raise last_err if last_err else RuntimeError("atomic write failed")


class StateStore:
    def __init__(self, path: Path = STATE_FILE):
        self.path = path
        self._records: dict[str, InstallRecord] = {}
        self._lock = threading.RLock()
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            self.path.replace(self.path.with_suffix(".json.bak"))
            return
        for rid, data in raw.items():
            try:
                rec = InstallRecord(**data)
            except Exception:
                continue
            # Reconcile any non-terminal record: server restarted mid-flight.
            if rec.state not in _TERMINAL_STATES:
                rec.state = "FAILED"
                rec.error = (rec.error or "") + " | server restarted mid-install; record reconciled to FAILED"
                rec.updated_at = time.time()
                for s in rec.steps:
                    if s.status == "running":
                        s.status = "failed"
                        s.finished_at = rec.updated_at
                        s.message = (s.message or "") + " (reconciled on restart)"
            self._records[rid] = rec
        # Persist reconciliation so the on-disk state matches what we now hold.
        try:
            self._persist_locked()
        except Exception:
            pass

    def _persist_locked(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        data = {k: v.model_dump() for k, v in self._records.items()}
        _atomic_write(self.path, json.dumps(data, indent=2))

    def _persist(self) -> None:
        with self._lock:
            self._persist_locked()

    def create(
        self,
        stack_id: str,
        host: str,
        install_dir: str,
        steps: list[StepStatus],
    ) -> InstallRecord:
        with self._lock:
            now = time.time()
            install_id = f"inst_{uuid.uuid4().hex[:10]}"
            record = InstallRecord(
                install_id=install_id,
                stack_id=stack_id,
                host=host,
                install_dir=install_dir,
                state="DRAFT",
                created_at=now,
                updated_at=now,
                steps=steps,
            )
            self._records[install_id] = record
            self._persist_locked()
            return record

    def get(self, install_id: str) -> Optional[InstallRecord]:
        with self._lock:
            return self._records.get(install_id)

    def list(self) -> list[InstallRecord]:
        with self._lock:
            return sorted(self._records.values(), key=lambda r: r.created_at, reverse=True)

    def update_state(self, install_id: str, state: InstallState, error: Optional[str] = None) -> None:
        with self._lock:
            rec = self._records.get(install_id)
            if not rec:
                return
            rec.state = state
            rec.updated_at = time.time()
            if error is not None:
                rec.error = error
            self._persist_locked()

    def update_step(
        self,
        install_id: str,
        step_id: str,
        *,
        status: Optional[str] = None,
        started_at: Optional[float] = None,
        finished_at: Optional[float] = None,
        exit_code: Optional[int] = None,
        message: Optional[str] = None,
    ) -> None:
        with self._lock:
            rec = self._records.get(install_id)
            if not rec:
                return
            for s in rec.steps:
                if s.id == step_id:
                    if status is not None:
                        s.status = status  # type: ignore[assignment]
                    if started_at is not None:
                        s.started_at = started_at
                    if finished_at is not None:
                        s.finished_at = finished_at
                    if exit_code is not None:
                        s.exit_code = exit_code
                    if message is not None:
                        s.message = message
                    break
            rec.updated_at = time.time()
            self._persist_locked()

    def set_outputs(self, install_id: str, outputs: dict) -> None:
        with self._lock:
            rec = self._records.get(install_id)
            if not rec:
                return
            rec.outputs = outputs
            rec.updated_at = time.time()
            self._persist_locked()


store = StateStore()
