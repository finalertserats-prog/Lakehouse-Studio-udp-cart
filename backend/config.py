from pathlib import Path
import os

ROOT = Path(__file__).resolve().parent.parent
STACKS_DIR = ROOT / "stacks"
WORK_DIR = Path(os.environ.get("LHS_WORK_DIR", ROOT / "work"))
EVIDENCE_DIR = ROOT / "evidence"
STATE_FILE = WORK_DIR / "state.json"

WORK_DIR.mkdir(parents=True, exist_ok=True)
EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)

HOST = os.environ.get("LHS_HOST", "127.0.0.1")
BIND = os.environ.get("LHS_BIND", HOST)  # what uvicorn binds to (use 0.0.0.0 for VPS)
PORT = int(os.environ.get("LHS_PORT", "7878"))
