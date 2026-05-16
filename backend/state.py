from __future__ import annotations
import json
import time
import uuid
from pathlib import Path
from typing import Optional

from .config import STATE_FILE
from .models import InstallRecord, InstallState, StepStatus


class StateStore:
    def __init__(self, path: Path = STATE_FILE):
        self.path = path
        self._records: dict[str, InstallRecord] = {}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            for rid, data in raw.items():
                self._records[rid] = InstallRecord(**data)
        except Exception:
            # Corrupt state file: start fresh, keep the old one as .bak
            self.path.replace(self.path.with_suffix(".json.bak"))

    def _persist(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        data = {k: v.model_dump() for k, v in self._records.items()}
        tmp = self.path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
        tmp.replace(self.path)

    def create(
        self,
        stack_id: str,
        host: str,
        install_dir: str,
        steps: list[StepStatus],
    ) -> InstallRecord:
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
        self._persist()
        return record

    def get(self, install_id: str) -> Optional[InstallRecord]:
        return self._records.get(install_id)

    def list(self) -> list[InstallRecord]:
        return sorted(self._records.values(), key=lambda r: r.created_at, reverse=True)

    def update_state(self, install_id: str, state: InstallState, error: Optional[str] = None) -> None:
        rec = self._records.get(install_id)
        if not rec:
            return
        rec.state = state
        rec.updated_at = time.time()
        if error is not None:
            rec.error = error
        self._persist()

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
        self._persist()

    def set_outputs(self, install_id: str, outputs: dict) -> None:
        rec = self._records.get(install_id)
        if not rec:
            return
        rec.outputs = outputs
        rec.updated_at = time.time()
        self._persist()


store = StateStore()
