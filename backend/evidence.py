from __future__ import annotations
import json
import platform
import socket
import time
from pathlib import Path

import psutil

from .config import EVIDENCE_DIR
from .events import bus
from .models import InstallRecord


def _system_info() -> dict:
    vm = psutil.virtual_memory()
    try:
        du = psutil.disk_usage("C:\\" if platform.system() == "Windows" else "/")
        disk = {"total_gb": round(du.total / 1024**3, 2), "free_gb": round(du.free / 1024**3, 2)}
    except Exception:
        disk = {}
    return {
        "os": platform.system(),
        "os_version": platform.version(),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "hostname": socket.gethostname(),
        "cpu_cores_logical": psutil.cpu_count(logical=True),
        "cpu_cores_physical": psutil.cpu_count(logical=False),
        "ram_total_gb": round(vm.total / 1024**3, 2),
        "disk": disk,
    }


def capture(record: InstallRecord) -> Path:
    stack_dir = EVIDENCE_DIR / record.stack_id / record.install_id
    stack_dir.mkdir(parents=True, exist_ok=True)

    result = {
        "install_id": record.install_id,
        "stack_id": record.stack_id,
        "host": record.host,
        "state": record.state,
        "created_at": record.created_at,
        "updated_at": record.updated_at,
        "duration_sec": round(record.updated_at - record.created_at, 1),
        "steps": [s.model_dump() for s in record.steps],
        "outputs": record.outputs,
        "error": record.error,
    }
    (stack_dir / "result.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    (stack_dir / "system-info.json").write_text(json.dumps(_system_info(), indent=2), encoding="utf-8")

    # Dump the full log history
    log_path = stack_dir / "full-log.txt"
    with log_path.open("w", encoding="utf-8") as f:
        for evt in bus.history(record.install_id):
            ts = time.strftime("%H:%M:%S", time.localtime(evt.ts))
            if evt.kind == "log":
                f.write(f"[{ts}] {evt.step or '?':10} {evt.stream or '?':6} {evt.line or ''}\n")
            elif evt.kind == "step_start":
                f.write(f"[{ts}] === START {evt.step} ===\n")
            elif evt.kind == "step_end":
                f.write(f"[{ts}] === END   {evt.step} ({evt.status}) ===\n")
            elif evt.kind == "state":
                f.write(f"[{ts}] *** STATE {evt.status} ***\n")
            elif evt.kind == "error":
                f.write(f"[{ts}] !!! ERROR {evt.line}\n")

    return stack_dir
