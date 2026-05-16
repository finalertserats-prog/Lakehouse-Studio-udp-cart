from __future__ import annotations
import asyncio
import json
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from .config import ROOT, WORK_DIR
from .events import bus
from .inspector import inspect
from .models import InstallRequest, InspectionReport
from .runner import UDPRunner, make_steps, run_command
from .stack_manifest import list_manifests, load_manifest
from .state import store

app = FastAPI(title="LakeHouse Studio", version="0.1.0")

FRONTEND_DIR = ROOT / "frontend"


# ---------- Stacks ----------

@app.get("/api/stacks")
def get_stacks():
    manifests = list_manifests()
    return [
        {
            "id": m.id,
            "name": m.name,
            "version": m.data.get("version"),
            "maturity": m.data.get("maturity"),
            "description": m.data.get("description", "").strip(),
            "components": [
                {"id": c["id"], "name": c["name"], "version": c.get("version"), "category": c.get("category")}
                for c in m.components
            ],
            "requirements": m.requirements,
            "ports": m.data.get("ports", {}),
        }
        for m in manifests
    ]


@app.get("/api/stacks/{stack_id}")
def get_stack(stack_id: str):
    try:
        m = load_manifest(stack_id)
    except KeyError:
        raise HTTPException(404, f"Stack {stack_id} not found")
    return m.data


# ---------- Inspection ----------

class InspectRequest(BaseModel):
    stack_id: str
    host: str = "localhost"


@app.post("/api/inspect", response_model=InspectionReport)
def post_inspect(body: InspectRequest):
    try:
        m = load_manifest(body.stack_id)
    except KeyError:
        raise HTTPException(404, f"Stack {body.stack_id} not found")
    return inspect(m, host=body.host)


# ---------- Installs ----------

@app.get("/api/installs")
def list_installs():
    return [r.model_dump() for r in store.list()]


@app.get("/api/installs/{install_id}")
def get_install(install_id: str):
    rec = store.get(install_id)
    if not rec:
        raise HTTPException(404, "install not found")
    return rec.model_dump()


@app.post("/api/installs")
async def create_install(body: InstallRequest):
    try:
        m = load_manifest(body.stack_id)
    except KeyError:
        raise HTTPException(404, f"Stack {body.stack_id} not found")

    install_dir = Path(body.install_dir) if body.install_dir else (WORK_DIR / "udp")
    install_dir = install_dir.resolve()

    rec = store.create(
        stack_id=m.id,
        host=body.host,
        install_dir=str(install_dir),
        steps=make_steps(m),
    )

    runner = UDPRunner(m, rec.install_id, body.host, install_dir)
    asyncio.create_task(runner.run(body.env_overrides))
    return rec.model_dump()


class ControlRequest(BaseModel):
    action: str  # status | stop | clean | smoke


@app.post("/api/installs/{install_id}/control")
async def control_install(install_id: str, body: ControlRequest):
    rec = store.get(install_id)
    if not rec:
        raise HTTPException(404, "install not found")
    try:
        m = load_manifest(rec.stack_id)
    except KeyError:
        raise HTTPException(404, "stack not found")

    action_to_cmd = {
        "status": "status",
        "stop": "stop",
        "clean": "clean",
        "smoke": "smoke",
    }
    if body.action not in action_to_cmd:
        raise HTTPException(400, f"unknown action {body.action}")
    cmd_name = action_to_cmd[body.action]

    install_dir = Path(rec.install_dir)
    rc = await run_command(rec.install_id, install_dir, rec.host, m, cmd_name)

    if body.action == "stop" and rc == 0:
        store.update_state(rec.install_id, "STOPPED")
    elif body.action == "clean" and rc == 0:
        store.update_state(rec.install_id, "CLEANED")
    return {"exit_code": rc}


# ---------- Logs WebSocket ----------

@app.websocket("/api/installs/{install_id}/logs")
async def ws_logs(websocket: WebSocket, install_id: str):
    await websocket.accept()
    # Replay history first
    for evt in bus.history(install_id):
        try:
            await websocket.send_text(evt.model_dump_json())
        except Exception:
            return
    q = await bus.subscribe(install_id)
    try:
        while True:
            evt = await q.get()
            await websocket.send_text(evt.model_dump_json())
    except WebSocketDisconnect:
        pass
    finally:
        await bus.unsubscribe(install_id, q)


# ---------- Frontend ----------

@app.get("/")
def root():
    return FileResponse(FRONTEND_DIR / "index.html")


@app.get("/healthz")
def healthz():
    return {"ok": True}


if FRONTEND_DIR.exists():
    assets_dir = FRONTEND_DIR / "assets"
    if assets_dir.exists():
        app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")
    app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")
