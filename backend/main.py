from __future__ import annotations
import asyncio
import json
import os
import secrets
from pathlib import Path
from typing import Any, Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from .config import ROOT, WORK_DIR
from .events import bus
from .inspector import inspect
from .models import InstallRequest, InspectionReport
from .paths import InstallDirError, validate_install_dir
from .runner import UDPRunner, make_steps, run_command
from .stack_manifest import list_manifests, load_manifest
from .state import store

app = FastAPI(title="LakeHouse Studio", version="0.1.0")

FRONTEND_DIR = ROOT / "frontend"

# Track in-flight install tasks so they're not GC'd and so we can cancel them.
_INSTALL_TASKS: dict[str, asyncio.Task] = {}

# Optional shared-token auth. Set LHS_AUTH_TOKEN to enable. Required for any
# deployment that listens on a non-loopback interface (e.g. VPS).
AUTH_TOKEN: Optional[str] = os.environ.get("LHS_AUTH_TOKEN") or None


def _require_auth(authorization: Optional[str] = Header(default=None),
                  x_studio_token: Optional[str] = Header(default=None)):
    if not AUTH_TOKEN:
        return  # auth disabled
    presented = None
    if authorization and authorization.lower().startswith("bearer "):
        presented = authorization.split(" ", 1)[1].strip()
    elif x_studio_token:
        presented = x_studio_token.strip()
    if not presented or not secrets.compare_digest(presented, AUTH_TOKEN):
        raise HTTPException(401, "auth required")


AuthDep = Depends(_require_auth)


# ---------- Auth status ----------

@app.get("/api/auth/status")
def auth_status():
    return {"auth_required": bool(AUTH_TOKEN)}


# ---------- Stacks ----------

@app.get("/api/stacks", dependencies=[AuthDep])
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


@app.get("/api/stacks/{stack_id}", dependencies=[AuthDep])
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


@app.post("/api/inspect", response_model=InspectionReport, dependencies=[AuthDep])
def post_inspect(body: InspectRequest):
    try:
        m = load_manifest(body.stack_id)
    except KeyError:
        raise HTTPException(404, f"Stack {body.stack_id} not found")
    return inspect(m, host=body.host)


# ---------- Installs ----------

@app.get("/api/installs", dependencies=[AuthDep])
def list_installs():
    return [r.model_dump() for r in store.list()]


@app.get("/api/installs/{install_id}", dependencies=[AuthDep])
def get_install(install_id: str):
    rec = store.get(install_id)
    if not rec:
        raise HTTPException(404, "install not found")
    return rec.model_dump()


@app.post("/api/installs", dependencies=[AuthDep])
async def create_install(body: InstallRequest):
    try:
        m = load_manifest(body.stack_id)
    except KeyError:
        raise HTTPException(404, f"Stack {body.stack_id} not found")

    raw_dir = body.install_dir or str(WORK_DIR / "udp")
    try:
        install_dir = validate_install_dir(raw_dir)
    except InstallDirError as e:
        raise HTTPException(400, f"install_dir rejected: {e}")

    rec = store.create(
        stack_id=m.id,
        host=body.host,
        install_dir=str(install_dir),
        steps=make_steps(m),
    )

    runner = UDPRunner(m, rec.install_id, body.host, install_dir)

    async def _wrap() -> None:
        try:
            await runner.run(body.env_overrides)
        finally:
            _INSTALL_TASKS.pop(rec.install_id, None)

    task = asyncio.create_task(_wrap(), name=f"install:{rec.install_id}")
    _INSTALL_TASKS[rec.install_id] = task
    return rec.model_dump()


@app.post("/api/installs/{install_id}/cancel", dependencies=[AuthDep])
async def cancel_install(install_id: str):
    task = _INSTALL_TASKS.get(install_id)
    if not task or task.done():
        raise HTTPException(404, "no running install task")
    task.cancel()
    return {"cancelled": True}


@app.on_event("shutdown")
async def _shutdown():
    # Cancel any in-flight install tasks so we don't leave child processes around.
    tasks = list(_INSTALL_TASKS.values())
    for t in tasks:
        t.cancel()
    for t in tasks:
        try:
            await asyncio.wait_for(t, timeout=5)
        except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
            pass


class ControlRequest(BaseModel):
    action: str  # status | stop | clean | smoke


@app.post("/api/installs/{install_id}/control", dependencies=[AuthDep])
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
    # Origin guard: only accept connections from the Studio UI itself.
    origin = websocket.headers.get("origin", "")
    host_hdr = websocket.headers.get("host", "")
    if origin:
        from urllib.parse import urlparse
        parsed = urlparse(origin)
        # Allow same-host origins and localhost variants.
        allowed_hosts = {host_hdr, "localhost", "127.0.0.1"}
        if parsed.hostname not in allowed_hosts and parsed.netloc != host_hdr:
            await websocket.close(code=1008)
            return

    await websocket.accept()
    # Subscribe BEFORE snapshotting history so any event published in between
    # ends up in the live queue. We then dedupe by id+ts on replay.
    q = await bus.subscribe(install_id)
    try:
        history, _ = bus.history_snapshot(install_id)
        seen: set[tuple[float, str | None, str | None, str | None]] = set()
        for evt in history:
            key = (evt.ts, evt.kind, evt.step, evt.line)
            seen.add(key)
            try:
                await websocket.send_text(evt.model_dump_json())
            except Exception:
                return
        while True:
            evt = await q.get()
            key = (evt.ts, evt.kind, evt.step, evt.line)
            if key in seen:
                seen.discard(key)
                continue
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
