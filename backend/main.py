from __future__ import annotations
import asyncio
import json
import os
import secrets
import time
from pathlib import Path
from typing import Any, Optional

import httpx
from fastapi import Depends, FastAPI, File, Header, HTTPException, Request, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator

from .config import ROOT, WORK_DIR
from .events import bus
from .inspector import inspect
from .models import InstallRequest, InspectionReport, LogEvent
from .notifications import NotifyEvent, get_dispatcher, notify
from .catalog import (
    categories as catalog_categories,
    goals as catalog_goals,
    recommended_sets as catalog_recommended_sets,
    validate_catalog,
)
from .compatibility import (
    list_locks,
    list_upgrade_candidates,
    load_lock,
    load_upgrades,
    lock_summary,
    precheck_image_availability,
    simulate_upgrade,
    validate_against_catalog,
)
from .health import get_stack_health
from .gitops_export import build_export
from .compliance import get_compliance, validate_compliance
from .templates import get_template_detail, list_templates, validate_templates
from .demo_query import list_queries, run_demo_query
from .error_explainer import explain as explain_error
from . import ai_assistant as ai_mod
from .lake_namer import suggest as suggest_lake_names, is_valid as is_valid_lake_name, normalize as normalize_lake_name
from .paths import InstallDirError, validate_install_dir
from .providers import match_plans, cheapest_overall
from .runner import UDPRunner, make_steps, mark_step_skipped, retry_install, run_command
from .scorer import score_stack
from .sizer import size_stack
from .sql_editor import run_user_sql
from .stack_manifest import list_manifests, load_manifest
from .state import store
from .structured_smoke import run_structured_smoke
from . import ingest as ingest_mod
from . import data_sources as data_sources_mod
from . import table_explorer
from . import backup as backup_mod
from . import monitoring as monitoring_mod
from . import tls_wizard as tls_mod
from . import caddy_tls as caddy_mod
from . import upgrade_executor as upgrade_exec_mod

app = FastAPI(title="LakeHouse Studio", version="0.1.0")

FRONTEND_DIR = ROOT / "frontend"

# Track in-flight install tasks so they're not GC'd and so we can cancel them.
_INSTALL_TASKS: dict[str, asyncio.Task] = {}

# Any state in this set is "actively doing work" — used to gate operations
# like rollback that would race a running install.
RUNNING_STATES = frozenset({
    "INSPECTING", "READY_TO_INSTALL", "CLONING_REPO", "WRITING_ENV",
    "RUNNING_DOCTOR", "STARTING_STACK", "BOOTSTRAPPING", "SMOKE_TESTING",
})
TERMINAL_STATES = frozenset({"READY", "FAILED", "STOPPED", "CLEANED"})


def _make_install_task_wrapper(install_id: str, coro_factory):
    """Wrap an install coroutine factory with exception safety.

    Guarantees:
    - asyncio.CancelledError is NOT treated as failure (user-triggered cancel).
    - Any other exception transitions the install to FAILED, but ONLY if the
      install isn't already in a terminal state (don't overwrite a clean READY).
    - An error event is published to the bus so the UI surfaces the crash.
    - The task is removed from _INSTALL_TASKS in finally.
    """
    async def _wrapped() -> None:
        try:
            await coro_factory()
        except asyncio.CancelledError:
            # User cancelled. State stays where it is; the cancel endpoint
            # already set it appropriately.
            raise
        except Exception as e:
            import logging
            logger = logging.getLogger("lhs.install")
            logger.exception("install task %s crashed", install_id)
            try:
                rec = store.get(install_id)
                if rec and rec.state not in TERMINAL_STATES:
                    store.update_state(install_id, "FAILED",
                                       error=f"orchestrator crash: {type(e).__name__}: {e}")
                    bus.publish_nowait(LogEvent(
                        install_id=install_id, ts=time.time(), kind="error",
                        line=f"orchestrator crashed: {type(e).__name__}: {e}",
                    ))
                    bus.publish_nowait(LogEvent(
                        install_id=install_id, ts=time.time(), kind="state",
                        status="FAILED",
                    ))
            except Exception:
                logger.exception("crash-handler itself failed for %s", install_id)
            try:
                await notify(
                    install_id,
                    "install_failed",
                    "critical",
                    f"Install failed at orchestrator",
                    f"orchestrator crash: {type(e).__name__}: {e}",
                    links={"diagnose": f"/api/installs/{install_id}/diagnose"},
                )
            except Exception:
                pass  # never let notifications break the install
        finally:
            _INSTALL_TASKS.pop(install_id, None)
    return _wrapped

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


# Catalog validation on startup. If problems are found we don't crash the
# app (it can still serve /healthz), but every catalog-dependent route will
# return 503 with a clear message until the YAML is fixed.
_CATALOG_PROBLEMS: list[str] = []

# Per-stack compatibility drift problems (catalog vs lock file). Surfaced via
# /healthz so operators see drift before they ship. Non-fatal: install still
# proceeds, but the install-time registry precheck will catch the worst cases.
_COMPAT_PROBLEMS: dict[str, list[str]] = {}


_TEMPLATE_PROBLEMS: list[str] = []
_COMPLIANCE_PROBLEMS: list[str] = []


@app.on_event("startup")
async def _validate_catalog_on_startup() -> None:
    global _CATALOG_PROBLEMS, _COMPAT_PROBLEMS, _TEMPLATE_PROBLEMS, _COMPLIANCE_PROBLEMS
    _CATALOG_PROBLEMS = validate_catalog()
    if _CATALOG_PROBLEMS:
        import logging
        for p in _CATALOG_PROBLEMS:
            logging.getLogger("lhs.catalog").error("catalog problem: %s", p)

    # Templates + compliance are NON-FATAL — picker just shows empty state if broken.
    _COMPLIANCE_PROBLEMS = validate_compliance()
    _TEMPLATE_PROBLEMS = validate_templates()
    if _COMPLIANCE_PROBLEMS or _TEMPLATE_PROBLEMS:
        import logging
        log = logging.getLogger("lhs.templates")
        for p in _COMPLIANCE_PROBLEMS:
            log.warning("compliance problem: %s", p)
        for p in _TEMPLATE_PROBLEMS:
            log.warning("template problem: %s", p)

    # Cross-check every stack manifest against its compatibility lock. A
    # mismatch means someone edited the catalog without bumping the lock —
    # the "matrix is the moat" promise has been broken silently.
    import logging
    compat_log = logging.getLogger("lhs.compat")
    _COMPAT_PROBLEMS = {}
    locked = set(list_locks())
    for m in list_manifests():
        if m.id not in locked:
            continue  # no lock yet — acceptable for uncertified stacks
        problems = validate_against_catalog(m.id, m.components)
        if problems:
            _COMPAT_PROBLEMS[m.id] = problems
            for p in problems:
                compat_log.error("compat drift [%s]: %s", m.id, p)

    # Start the notifications dispatcher (mtime-poll config reloader).
    await get_dispatcher().start()


def _require_catalog_ok() -> None:
    if _CATALOG_PROBLEMS:
        raise HTTPException(503, {"error": "catalog invalid", "problems": _CATALOG_PROBLEMS})


CatalogOk = Depends(_require_catalog_ok)


# ---------- Auth status ----------

@app.get("/api/auth/status")
def auth_status():
    return {"auth_required": bool(AUTH_TOKEN)}


# ---------- Catalog / Goals (the "shop" surface) ----------

@app.get("/api/catalog", dependencies=[AuthDep, CatalogOk])
def get_catalog():
    """Component catalog: categories + their pickable components + alternates marked coming-soon."""
    return {"categories": catalog_categories(), "goals": catalog_goals(), "recommended_sets": catalog_recommended_sets()}


@app.get("/api/goals", dependencies=[AuthDep, CatalogOk])
def get_goals():
    return catalog_goals()


# ---------- Templates (use-case-shaped views over recommended_sets) ----------

@app.get("/api/templates", dependencies=[AuthDep, CatalogOk])
def get_templates():
    """Lightweight list for the picker grid: id, label, pitch, readiness, tags."""
    return {"templates": list_templates()}


@app.get("/api/templates/{template_id}", dependencies=[AuthDep, CatalogOk])
def get_template(template_id: str):
    """Full detail: certified cart + display-only pending + resolved compliance.

    Does NOT mutate any state. Front-end uses this to pre-fill the cart
    after the user explicitly clicks a template card.
    """
    detail = get_template_detail(template_id)
    if detail is None:
        raise HTTPException(404, f"template '{template_id}' not found")
    return detail


@app.get("/api/compliance/{tag}", dependencies=[AuthDep, CatalogOk])
def get_compliance_block(tag: str):
    """One compliance framing entry (HIPAA / PCI / SOC 2 / GDPR).

    Lazy-loaded by the UI so long-form prose only ships when a user
    expands the panel.
    """
    block = get_compliance(tag)
    if block is None:
        raise HTTPException(404, f"compliance tag '{tag}' not found")
    return {"tag": tag, **block}


# ---------- Cart validation ----------

class CartRequest(BaseModel):
    cart: list[str] = []

    @field_validator("cart")
    @classmethod
    def _validate(cls, v):
        from .models import _validate_component_id_list
        return _validate_component_id_list(v, "cart")


@app.post("/api/cart/validate", dependencies=[AuthDep, CatalogOk])
def post_cart_validate(body: CartRequest):
    from .cart import validate_cart
    return validate_cart(body.cart)


@app.get("/api/cart/recommended", dependencies=[AuthDep, CatalogOk])
def get_recommended_cart():
    from .cart import recommended_cart
    return {"cart": recommended_cart()}


# ---------- Lake names ----------

@app.get("/api/lake-names/suggest", dependencies=[AuthDep])
def get_lake_name_suggestion(n: int = 1):
    return {"suggestions": suggest_lake_names(max(1, min(n, 20)))}


class LakeNameValidateRequest(BaseModel):
    name: str


@app.post("/api/lake-names/validate", dependencies=[AuthDep])
def post_lake_name_validate(body: LakeNameValidateRequest):
    ok, why = is_valid_lake_name(body.name)
    return {"valid": ok, "reason": why if not ok else None, "normalized": normalize_lake_name(body.name)}


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


@app.get("/api/stacks/{stack_id}/sizing", dependencies=[AuthDep])
def get_stack_sizing(stack_id: str):
    """Per-tier resource totals + matched VPS plans + cheapest overall + score."""
    try:
        m = load_manifest(stack_id)
    except KeyError:
        raise HTTPException(404, f"Stack {stack_id} not found")
    tiers = size_stack(m)
    out = {"stack_id": m.id, "name": m.name, "tiers": {}}
    for tier_name, tier in tiers.items():
        matches = match_plans(tier["totals"])
        cheapest = cheapest_overall(tier["totals"])
        out["tiers"][tier_name] = {
            **tier,
            "matched_providers": matches,
            "cheapest_overall": cheapest,
            "score": score_stack(m, tier=tier_name),
        }
    return out


@app.get("/api/stacks/{stack_id}/score", dependencies=[AuthDep])
def get_stack_score(stack_id: str, tier: str = "recommended"):
    try:
        m = load_manifest(stack_id)
    except KeyError:
        raise HTTPException(404, f"Stack {stack_id} not found")
    return score_stack(m, tier=tier)


@app.get("/api/stacks/{stack_id}", dependencies=[AuthDep])
def get_stack(stack_id: str):
    try:
        m = load_manifest(stack_id)
    except KeyError:
        raise HTTPException(404, f"Stack {stack_id} not found")
    return m.data


# ---------- Compatibility (the matrix-as-moat surface) ----------

@app.get("/api/stacks/{stack_id}/compatibility", dependencies=[AuthDep])
def get_stack_compatibility(stack_id: str):
    """Lock-file summary + any catalog-vs-lock drift detected at startup.

    The lock file is the authoritative record of which versions were verified
    working together. UI surfaces this so operators see what's actually been
    certified before they install.
    """
    try:
        m = load_manifest(stack_id)
    except KeyError:
        raise HTTPException(404, f"Stack {stack_id} not found")
    summary = lock_summary(stack_id)
    if summary is None:
        return {
            "stack_id": stack_id,
            "certified": False,
            "summary": None,
            "drift": [f"no compatibility lock exists for stack '{stack_id}'"],
            "components_in_catalog": len(m.components),
        }
    return {
        "stack_id": stack_id,
        "certified": True,
        "summary": summary,
        "drift": _COMPAT_PROBLEMS.get(stack_id, []),
        "components_in_catalog": len(m.components),
    }


@app.get("/api/stacks/{stack_id}/upgrades", dependencies=[AuthDep])
def get_stack_upgrades(stack_id: str):
    """Hand-curated upgrade candidates for this stack.

    Reads stacks/compatibility/{stack_id}.upgrades.yaml. Empty list if no
    upgrades file exists. UI surfaces this as a 'N updates available' badge.
    """
    try:
        load_manifest(stack_id)
    except KeyError:
        raise HTTPException(404, f"Stack {stack_id} not found")
    return {
        "stack_id": stack_id,
        "candidates": list_upgrade_candidates(stack_id),
        "has_upgrades_file": load_upgrades(stack_id) is not None,
    }


class UpgradeSimulateRequest(BaseModel):
    proposed: dict[str, str] = Field(min_length=1, max_length=32)


@app.post("/api/stacks/{stack_id}/upgrades/simulate", dependencies=[AuthDep])
async def post_stack_upgrade_simulate(stack_id: str, body: UpgradeSimulateRequest):
    """Dry-run an upgrade. Overlays proposed tags on the lock, runs registry
    precheck + walks known-incompatible + walks constraints. Returns a
    verdict (pass | unknown | fail). Never mutates the lock.
    """
    try:
        load_manifest(stack_id)
    except KeyError:
        raise HTTPException(404, f"Stack {stack_id} not found")
    if load_lock(stack_id) is None:
        raise HTTPException(404, f"no compatibility lock for stack '{stack_id}'")
    return await simulate_upgrade(stack_id, body.proposed)


# DESTRUCTIVE — actually swaps the running stack's images. Requires a
# backup_id (no exceptions). Any failure after compose-down triggers rollback
# via backup restore + base compose up. See backend/upgrade_executor.py for
# the full pipeline.
class UpgradeExecuteRequest(BaseModel):
    proposed: dict[str, str] = Field(min_length=1, max_length=32)
    backup_id: str = Field(min_length=1, max_length=64)


@app.post("/api/installs/{install_id}/upgrades/execute", dependencies=[AuthDep])
async def post_install_upgrade_execute(install_id: str, body: UpgradeExecuteRequest):
    """Execute a previously-simulated upgrade against a live install.

    DESTRUCTIVE. Requires backup_id (must belong to install_id). Pipeline:
    preflight -> simulate -> compose down -> image pull -> compose up
    (with docker-compose.upgrade.yml overlay) -> smoke. Any failure after
    compose-down triggers rollback (restore backup + base compose up).

    Refuses (409) if the install is in a RUNNING_STATES state or already
    has an in-flight install/upgrade task. The lock file is NEVER mutated
    on success — the response carries `proposed_new_lock` for manual PR.
    """
    rec = store.get(install_id)
    if not rec:
        raise HTTPException(404, "install not found")
    # Race against in-flight install/retry/skip tasks.
    if install_id in _INSTALL_TASKS and not _INSTALL_TASKS[install_id].done():
        raise HTTPException(409, "an install task is still running; cancel before upgrade")
    if rec.state in RUNNING_STATES:
        raise HTTPException(409, f"cannot upgrade while state is {rec.state}; cancel first")
    try:
        result = await upgrade_exec_mod.execute_upgrade(
            install_id=install_id,
            proposed=body.proposed,
            backup_id=body.backup_id,
            running_states=RUNNING_STATES,
            install_tasks=_INSTALL_TASKS,
        )
    except upgrade_exec_mod.UpgradeExecutionError as e:
        # Pre-execution invariants (unknown component, missing lock, etc.).
        msg = str(e)
        # Unknown component / bad proposed shape -> 400; missing install/lock -> 404.
        if "unknown component" in msg or "must be non-empty" in msg or "is required" in msg:
            raise HTTPException(400, msg)
        if "not found" in msg or "no compatibility lock" in msg or "does not exist" in msg:
            raise HTTPException(404, msg)
        raise HTTPException(409, msg)
    return result.model_dump()


@app.get("/api/installs/{install_id}/upgrades/executions", dependencies=[AuthDep])
def list_upgrade_executions(install_id: str):
    """List previous upgrade executions for an install (most recent first)."""
    rec = store.get(install_id)
    if not rec:
        raise HTTPException(404, "install not found")
    return [e.model_dump() for e in upgrade_exec_mod.list_executions(install_id)]


@app.get("/api/upgrades/executions/{execution_id}", dependencies=[AuthDep])
def get_upgrade_execution(execution_id: str):
    """Fetch a single upgrade execution by id."""
    e = upgrade_exec_mod.get_execution(execution_id)
    if e is None:
        raise HTTPException(404, "execution not found")
    return e.model_dump()


@app.post("/api/stacks/{stack_id}/compatibility/precheck", dependencies=[AuthDep])
async def post_stack_compat_precheck(stack_id: str, timeout: int = 10):
    """Verify every image+tag in the lock file STILL exists on its registry.

    This is the v0.3-shipping-disaster prevention: a tag that worked at
    certification time can be removed upstream, and the only way to know
    before install is to ask the registry. Runs `docker manifest inspect`
    in parallel for every component.
    """
    try:
        load_manifest(stack_id)
    except KeyError:
        raise HTTPException(404, f"Stack {stack_id} not found")
    if load_lock(stack_id) is None:
        raise HTTPException(404, f"no compatibility lock for stack '{stack_id}'")
    # Clamp timeout to a reasonable range — registry calls shouldn't hold
    # the request open for more than a minute per component.
    timeout = max(2, min(int(timeout), 60))
    return await precheck_image_availability(stack_id, timeout=timeout)


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

    # Validate / normalize lake name if provided
    lake_name = None
    if body.lake_name:
        ok, why = is_valid_lake_name(body.lake_name)
        if not ok:
            raise HTTPException(400, f"lake_name rejected: {why}")
        lake_name = normalize_lake_name(body.lake_name)

    # Validate goal against known goals (if catalog is loaded)
    if body.goal:
        if _CATALOG_PROBLEMS:
            raise HTTPException(503, "cannot validate goal: catalog has errors")
        known_goals = {g["id"] for g in catalog_goals()}
        if body.goal not in known_goals:
            raise HTTPException(400, f"unknown goal '{body.goal}'; known: {sorted(known_goals)}")

    # Validate cart components against the catalog (cart was already validated
    # for shape/dedup/identifier-rules by the Pydantic field validator)
    if body.cart:
        if _CATALOG_PROBLEMS:
            raise HTTPException(503, "cannot validate cart: catalog has errors")
        from .catalog import component_index
        known_components = set(component_index().keys())
        unknown = [cid for cid in body.cart if cid not in known_components]
        if unknown:
            raise HTTPException(400, f"unknown component(s) in cart: {unknown}")

    rec = store.create(
        stack_id=m.id,
        host=body.host,
        install_dir=str(install_dir),
        steps=make_steps(m),
        lake_name=lake_name,
        goal=body.goal,
        cart=body.cart or [],
    )

    runner = UDPRunner(m, rec.install_id, body.host, install_dir)
    wrapped = _make_install_task_wrapper(
        rec.install_id, lambda: runner.run(body.env_overrides)
    )
    task = asyncio.create_task(wrapped(), name=f"install:{rec.install_id}")
    _INSTALL_TASKS[rec.install_id] = task
    return rec.model_dump()


@app.post("/api/installs/{install_id}/cancel", dependencies=[AuthDep])
async def cancel_install(install_id: str):
    task = _INSTALL_TASKS.get(install_id)
    if not task or task.done():
        raise HTTPException(404, "no running install task")
    task.cancel()
    return {"cancelled": True}


class StepActionRequest(BaseModel):
    step_id: str
    env_overrides: dict[str, str] = {}


@app.post("/api/installs/{install_id}/steps/retry", dependencies=[AuthDep])
async def post_step_retry(install_id: str, body: StepActionRequest):
    """Resume a failed install from the given step. Resets that step + everything after to pending."""
    rec = store.get(install_id)
    if not rec:
        raise HTTPException(404, "install not found")
    if rec.state not in ("FAILED",):
        raise HTTPException(409, f"retry only valid from FAILED; current state is {rec.state}")
    if install_id in _INSTALL_TASKS and not _INSTALL_TASKS[install_id].done():
        raise HTTPException(409, "install task is still running")
    try:
        m = load_manifest(rec.stack_id)
    except KeyError:
        raise HTTPException(404, "stack not found")

    install_dir = Path(rec.install_dir)
    wrapped = _make_install_task_wrapper(
        install_id,
        lambda: retry_install(m, install_id, rec.host, install_dir, body.env_overrides, body.step_id)
    )
    task = asyncio.create_task(wrapped(), name=f"retry:{install_id}:{body.step_id}")
    _INSTALL_TASKS[install_id] = task
    return {"resumed_at": body.step_id}


@app.post("/api/installs/{install_id}/steps/skip", dependencies=[AuthDep])
async def post_step_skip(install_id: str, body: StepActionRequest):
    """Skip a non-critical step and continue from the next one."""
    rec = store.get(install_id)
    if not rec:
        raise HTTPException(404, "install not found")
    if rec.state not in ("FAILED",):
        raise HTTPException(409, f"skip only valid from FAILED; current state is {rec.state}")
    if install_id in _INSTALL_TASKS and not _INSTALL_TASKS[install_id].done():
        raise HTTPException(409, "install task is still running")
    next_id = mark_step_skipped(install_id, body.step_id)
    if next_id is None:
        raise HTTPException(400, f"step '{body.step_id}' is not skippable or unknown")

    try:
        m = load_manifest(rec.stack_id)
    except KeyError:
        raise HTTPException(404, "stack not found")
    install_dir = Path(rec.install_dir)
    wrapped = _make_install_task_wrapper(
        install_id,
        lambda: retry_install(m, install_id, rec.host, install_dir, body.env_overrides, next_id)
    )
    task = asyncio.create_task(wrapped(), name=f"skip:{install_id}:{body.step_id}")
    _INSTALL_TASKS[install_id] = task
    return {"skipped": body.step_id, "resumed_at": next_id}


@app.get("/api/installs/{install_id}/export", dependencies=[AuthDep])
def get_install_export(install_id: str):
    """Download the deployed stack as a GitOps bundle (.tar.gz).

    Contents: post-patched docker-compose.yml + SCRUBBED .env (secrets
    replaced with `<rotate-me>`) + Studio's scripts/ + the stack manifest
    + the compatibility lock file + a README. Bring it up on any host
    with `docker compose up -d` (after rotating the secrets).
    """
    from fastapi.responses import Response
    rec = store.get(install_id)
    if not rec:
        raise HTTPException(404, "install not found")
    install_dir = Path(rec.install_dir)
    if not install_dir.exists():
        raise HTTPException(409, "install_dir missing — nothing to export")
    blob, fname = build_export(install_id, install_dir, rec.stack_id, rec.lake_name)
    return Response(
        content=blob,
        media_type="application/gzip",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


@app.post("/api/installs/{install_id}/uninstall", dependencies=[AuthDep])
async def post_uninstall(install_id: str):
    """Full uninstall: wipe the deployed stack AND remove from Studio.
    Order: docker clean → archive evidence → rm install_dir → delete record."""
    rec = store.get(install_id)
    if not rec:
        raise HTTPException(404, "install not found")
    try:
        m = load_manifest(rec.stack_id)
    except KeyError:
        raise HTTPException(404, "stack not found")
    from .uninstall import uninstall as _do_uninstall, UninstallError
    try:
        result = await _do_uninstall(install_id, m, RUNNING_STATES, _INSTALL_TASKS)
    except UninstallError as e:
        raise HTTPException(409, str(e))
    return result


# ---------- TLS + Password rotation wizard (post-install opt-in hardening) ----------

# Cert generation is additive — never touches the install pipeline or the
# compose patcher. Self-signed certs land in WORK_DIR/tls/{install_id}/ with
# a sidecar manifest so list/get/delete don't have to parse PEM. The wizard
# also exposes password rotation for MinIO (.env edit) and StarRocks
# (returns SQL — env-var rotation does not work for the StarRocks root user).


def _public_cert(rec: tls_mod.GeneratedCert) -> dict:
    """Strip key_path from any cert record before returning it over the API.
    The private key MUST never leave the server filesystem."""
    data = rec.model_dump()
    data.pop("key_path", None)
    return data


@app.post("/api/installs/{install_id}/tls/generate", dependencies=[AuthDep])
async def post_tls_generate(install_id: str, body: tls_mod.CertSpec):
    """Generate a TLS cert for the given install. Returns the public cert
    record (cert_id + fingerprint + metadata) — never the key path."""
    rec = store.get(install_id)
    if not rec:
        raise HTTPException(404, "install not found")
    try:
        if body.kind == "self_signed":
            generated = await tls_mod.generate_self_signed(install_id, body)
        else:
            generated = await tls_mod.generate_letsencrypt(install_id, body)
    except NotImplementedError as e:
        raise HTTPException(501, str(e))
    except RuntimeError as e:
        # cryptography package missing or signing failed.
        raise HTTPException(503, str(e))
    except ValueError as e:
        raise HTTPException(400, str(e))
    return _public_cert(generated)


@app.get("/api/installs/{install_id}/tls/certs", dependencies=[AuthDep])
def get_tls_certs(install_id: str):
    """List public cert metadata for an install. Key paths are scrubbed."""
    rec = store.get(install_id)
    if not rec:
        raise HTTPException(404, "install not found")
    return {"certs": [_public_cert(c) for c in tls_mod.list_certs(install_id)]}


@app.delete("/api/tls/certs/{cert_id}", status_code=204, dependencies=[AuthDep])
def delete_tls_cert(cert_id: str):
    """Remove cert + key + sidecar. 404 if cert_id is unknown."""
    try:
        tls_mod.delete_cert(cert_id)
    except ValueError as e:
        raise HTTPException(404, str(e))
    return None


class PasswordRotateRequest(BaseModel):
    service: str = Field(min_length=1, max_length=32)
    new_password: str = Field(min_length=1, max_length=256)


@app.post("/api/installs/{install_id}/security/rotate-password", dependencies=[AuthDep])
async def post_rotate_password(install_id: str, body: PasswordRotateRequest):
    """Rotate the root password for minio or starrocks. The rotate function
    never returns the password and never logs it."""
    rec = store.get(install_id)
    if not rec:
        raise HTTPException(404, "install not found")
    if body.service not in ("minio", "starrocks"):
        raise HTTPException(400, f"unknown service {body.service!r}; expected 'minio' or 'starrocks'")
    try:
        return await tls_mod.rotate_password(
            install_id,
            body.service,  # type: ignore[arg-type]
            body.new_password,
            running_states=RUNNING_STATES,
        )
    except ValueError as e:
        raise HTTPException(400, str(e))


class PasswordStrengthRequest(BaseModel):
    password: str = Field(min_length=1, max_length=256)


@app.post("/api/installs/{install_id}/security/password-strength", dependencies=[AuthDep])
def post_password_strength(install_id: str, body: PasswordStrengthRequest):
    """Pre-submit strength check. Helper for the UI; mirrors server rules so
    the UI never proposes a password the server will reject. NEVER logs the
    password."""
    rec = store.get(install_id)
    if not rec:
        raise HTTPException(404, "install not found")
    return tls_mod.password_strength_hint(body.password)


# ---------- Caddy TLS sidecar (opt-in HTTPS-by-default hardening) ----------
#
# These two routes write/remove the docker-compose.tls.yml override + Caddyfile
# in the install_dir. They DO NOT touch the base docker-compose.yml the
# install pipeline writes -- that file is FROZEN (certified-stack contract).
# Activation is an explicit `docker compose ... up -d caddy` the operator
# runs themselves; we surface the command so they retain control.


@app.post("/api/installs/{install_id}/tls/caddy/enable", dependencies=[AuthDep])
async def post_caddy_enable(install_id: str, body: caddy_mod.TlsProfile):
    """Write a Caddy TLS sidecar override into the install_dir.

    Refuses if the install is mid-pipeline (RUNNING_STATES) -- flipping
    on a TLS sidecar while the stack is being clone/started would race
    docker compose. Returns the override path + the exact activate
    command the operator runs from the install_dir.
    """
    rec = store.get(install_id)
    if not rec:
        raise HTTPException(404, "install not found")
    if rec.state in RUNNING_STATES:
        raise HTTPException(
            409,
            f"cannot enable TLS sidecar while install state is {rec.state}; "
            f"wait for it to reach a terminal state first",
        )
    try:
        override_path = await caddy_mod.write_caddy_override(install_id, body)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {
        "compose_file_path": str(override_path),
        "activate_command": caddy_mod.caddy_activate_command(install_id),
        "kind": body.kind,
        "domain": body.domain,
    }


@app.post("/api/installs/{install_id}/tls/caddy/disable", dependencies=[AuthDep])
async def post_caddy_disable(install_id: str):
    """Remove the Caddy override + Caddyfile from the install_dir.

    Refuses if the install is mid-pipeline. The caller is responsible
    for running the returned deactivate command FIRST -- the route does
    not stop the caddy container itself (the operator retains control
    over their stack lifecycle).
    """
    rec = store.get(install_id)
    if not rec:
        raise HTTPException(404, "install not found")
    if rec.state in RUNNING_STATES:
        raise HTTPException(
            409,
            f"cannot disable TLS sidecar while install state is {rec.state}; "
            f"wait for it to reach a terminal state first",
        )
    try:
        await caddy_mod.disable_caddy_override(install_id)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {
        "disabled": True,
        "deactivate_command": caddy_mod.caddy_deactivate_command(install_id),
    }


# ---------- Prometheus + Grafana monitoring sidecar (opt-in operational tooling) ----------
#
# Same pattern as the Caddy TLS sidecar above: write a docker-compose
# override + supporting config into the install_dir, return the activate
# command, never touch the base compose file or the install pipeline.
# Monitoring is NOT part of the certified lock file — it's an opt-in
# operational layer the operator adopts on their own terms.


@app.post("/api/installs/{install_id}/monitoring/enable", dependencies=[AuthDep])
async def post_monitoring_enable(install_id: str, body: monitoring_mod.MonitoringProfile):
    """Write the docker-compose.metrics.yml override + monitoring/ subtree.

    Refuses if the install is mid-pipeline (RUNNING_STATES) — same reasoning
    as the Caddy sidecar: layering an override on a workspace that's still
    being cloned/started races docker compose.

    If the caller doesn't pin grafana_admin_password, a 24-char random secret
    is generated and returned ONCE in the response. The server never stores
    it; the operator must save it.
    """
    rec = store.get(install_id)
    if not rec:
        raise HTTPException(404, "install not found")
    if rec.state in RUNNING_STATES:
        raise HTTPException(
            409,
            f"cannot enable monitoring sidecar while install state is {rec.state}; "
            f"wait for it to reach a terminal state first",
        )
    try:
        return await monitoring_mod.enable_monitoring(install_id, body)
    except ValueError as e:
        raise HTTPException(400, str(e))


@app.post("/api/installs/{install_id}/monitoring/disable", dependencies=[AuthDep])
async def post_monitoring_disable(install_id: str):
    """Remove the override file + the monitoring/ subdir from the install_dir.

    Refuses if the install is mid-pipeline. The caller is responsible for
    running the returned shutdown hint FIRST — the route does not stop the
    prometheus/grafana containers (the operator retains stack-lifecycle
    control, same as the Caddy sidecar).
    """
    rec = store.get(install_id)
    if not rec:
        raise HTTPException(404, "install not found")
    if rec.state in RUNNING_STATES:
        raise HTTPException(
            409,
            f"cannot disable monitoring sidecar while install state is {rec.state}; "
            f"wait for it to reach a terminal state first",
        )
    try:
        return await monitoring_mod.disable_monitoring(install_id)
    except ValueError as e:
        raise HTTPException(400, str(e))


# ---------- Backup / Restore ----------

class BackupCreateRequest(BaseModel):
    kind: str = Field(default="metadata", pattern=r"^(metadata|full)$")


@app.post("/api/installs/{install_id}/backups", dependencies=[AuthDep])
async def post_install_backup(install_id: str, body: BackupCreateRequest):
    """Create a backup tarball for the given install. kind: metadata | full."""
    rec = store.get(install_id)
    if not rec:
        raise HTTPException(404, "install not found")
    try:
        record = await backup_mod.create_backup(install_id, kind=body.kind)  # type: ignore[arg-type]
    except backup_mod.BackupError as e:
        raise HTTPException(409, str(e))
    return record.model_dump()


@app.get("/api/installs/{install_id}/backups", dependencies=[AuthDep])
async def list_install_backups(install_id: str):
    rec = store.get(install_id)
    if not rec:
        raise HTTPException(404, "install not found")
    return [r.model_dump() for r in await backup_mod.list_backups(install_id)]


@app.post("/api/backups/{backup_id}/restore", dependencies=[AuthDep])
async def post_backup_restore(backup_id: str):
    """Restore a backup over its source install. Refuses if install is in
    a RUNNING_STATES state or has a live install task."""
    try:
        result = await backup_mod.restore_backup(
            backup_id,
            running_states=RUNNING_STATES,
            install_tasks=_INSTALL_TASKS,
        )
    except backup_mod.BackupError as e:
        raise HTTPException(409, str(e))
    return result.model_dump()


@app.delete("/api/backups/{backup_id}", status_code=204, dependencies=[AuthDep])
async def delete_backup_route(backup_id: str):
    try:
        await backup_mod.delete_backup(backup_id)
    except backup_mod.BackupError as e:
        raise HTTPException(404, str(e))
    return None


@app.post("/api/installs/{install_id}/steps/rollback", dependencies=[AuthDep])
async def post_step_rollback(install_id: str):
    """Run ./udp clean to tear down the stack and remove volumes."""
    rec = store.get(install_id)
    if not rec:
        raise HTTPException(404, "install not found")
    # Block rollback while an install/retry/skip task is in-flight — running
    # `docker compose down` mid-install races the orchestrator.
    if rec.state in RUNNING_STATES:
        raise HTTPException(409, f"cannot rollback while state is {rec.state}; cancel first")
    if install_id in _INSTALL_TASKS and not _INSTALL_TASKS[install_id].done():
        raise HTTPException(409, "an install task is still running; cancel it first")
    try:
        m = load_manifest(rec.stack_id)
    except KeyError:
        raise HTTPException(404, "stack not found")
    install_dir = Path(rec.install_dir)
    rc = await run_command(install_id, install_dir, rec.host, m, "clean")
    if rc == 0:
        store.update_state(install_id, "CLEANED")
    return {"exit_code": rc}


@app.post("/api/installs/{install_id}/smoke-structured", dependencies=[AuthDep])
async def post_structured_smoke(install_id: str):
    rec = store.get(install_id)
    if not rec:
        raise HTTPException(404, "install not found")
    return await run_structured_smoke()


@app.get("/api/installs/{install_id}/health", dependencies=[AuthDep])
async def get_install_health(install_id: str):
    """Live per-service health snapshot for an installed stack.

    Read-only: container state via `docker compose ps` + an HTTP/TCP probe
    per component. Safe to call repeatedly (the UI polls this for the
    health dashboard). Does not touch state.
    """
    rec = store.get(install_id)
    if not rec:
        raise HTTPException(404, "install not found")
    try:
        m = load_manifest(rec.stack_id)
    except KeyError:
        raise HTTPException(404, "stack not found")
    return await get_stack_health(m, Path(rec.install_dir), host=rec.host)


@app.get("/api/installs/{install_id}/diagnose", dependencies=[AuthDep])
def diagnose_install(install_id: str):
    """Inspect a failed install and produce an actionable explanation."""
    rec = store.get(install_id)
    if not rec:
        raise HTTPException(404, "install not found")
    if rec.state not in ("FAILED",):
        return {"explanation": None, "note": f"state is {rec.state}; diagnose is only useful for FAILED"}
    # Find failed step + collect log tail from the event bus
    failed_step = next((s.id for s in rec.steps if s.status == "failed"), None)
    exit_code = next((s.exit_code for s in rec.steps if s.status == "failed"), None)
    history = bus.history(install_id)
    log_tail = [e.line for e in history if e.kind == "log" and e.line]
    explanation = explain_error(failed_step, log_tail, exit_code)
    return {"explanation": explanation}


@app.get("/api/demo-queries", dependencies=[AuthDep])
def get_demo_queries():
    return list_queries()


class DemoQueryRequest(BaseModel):
    query_id: str


@app.post("/api/installs/{install_id}/demo-query", dependencies=[AuthDep])
async def post_demo_query(install_id: str, body: DemoQueryRequest):
    rec = store.get(install_id)
    if not rec:
        raise HTTPException(404, "install not found")
    if rec.state != "READY":
        raise HTTPException(409, f"install is in state {rec.state}; READY required")
    try:
        result = await run_demo_query(body.query_id)
    except KeyError as e:
        raise HTTPException(404, str(e))
    return result


# ---------- SQL editor (sandboxed free-form SQL) ----------

class SqlEditorRequest(BaseModel):
    sql: str = Field(min_length=1, max_length=10_000)


SQL_MAX_ROWS = 1000
SQL_TIMEOUT_SEC = 30


@app.post("/api/installs/{install_id}/sql", dependencies=[AuthDep])
async def post_sql_editor(install_id: str, body: SqlEditorRequest):
    """Run sandboxed read-only SQL against the deployed stack. Allowed
    leading keywords: SELECT/SHOW/DESCRIBE/EXPLAIN/WITH. Hard 30s timeout,
    1000-row result cap, every call audit-logged to the install's event bus."""
    rec = store.get(install_id)
    if not rec:
        raise HTTPException(404, "install not found")
    if rec.state != "READY":
        raise HTTPException(409, f"install is in state {rec.state}; READY required")

    from .sql_editor import run_user_sql

    # Audit-log the query before execution (truncated for log hygiene)
    audit_sql = body.sql.strip().replace("\n", " ")
    if len(audit_sql) > 200:
        audit_sql = audit_sql[:200] + "..."
    bus.publish_nowait(LogEvent(
        install_id=install_id, ts=time.time(), kind="log", stream="stdout",
        step="sql", line=f"[sql-editor] running: {audit_sql}",
    ))

    result = await run_user_sql(body.sql, timeout=SQL_TIMEOUT_SEC)

    # Enforce row cap
    if result.get("rows") and len(result["rows"]) > SQL_MAX_ROWS:
        result["rows"] = result["rows"][:SQL_MAX_ROWS]
        result["truncated"] = True
        result["truncated_at"] = SQL_MAX_ROWS

    # Audit the outcome
    if result.get("error"):
        bus.publish_nowait(LogEvent(
            install_id=install_id, ts=time.time(), kind="log", stream="stderr",
            step="sql", line=f"[sql-editor] error: {result['error']}",
        ))
    else:
        bus.publish_nowait(LogEvent(
            install_id=install_id, ts=time.time(), kind="log", stream="stdout",
            step="sql", line=f"[sql-editor] returned {result.get('row_count', 0)} rows",
        ))

    return result


# ---------- Notifications ----------

@app.get("/api/notifications/config", dependencies=[AuthDep])
def get_notifications_config():
    """Current notifications config with secrets scrubbed.

    Any non-empty password / webhook / token field is replaced with the
    literal string "<configured>" so the UI can show "enabled" without
    leaking the resolved value.
    """
    return get_dispatcher().get_public_config()


class NotificationTestRequest(BaseModel):
    channel: str = Field(min_length=1, max_length=32)


@app.post("/api/notifications/test", dependencies=[AuthDep])
async def post_notifications_test(body: NotificationTestRequest):
    """Send a synthetic event through exactly one channel for end-to-end verification."""
    evt = NotifyEvent(
        event_type="test",
        severity="info",
        install_id=None,
        title="Lakehouse Studio test notification",
        body=f"Verification ping for channel '{body.channel}'.",
        ts=time.time(),
    )
    ok, detail = await get_dispatcher().send_through(body.channel, evt)
    return {"ok": ok, "detail": detail}


# ---------- AI Assistant ("Ask Studio") ----------

@app.get("/api/ai/status")
def get_ai_status():
    """Public status: {enabled, model, reason}. No secrets returned.

    Unauthenticated so the chat panel can render its disabled-state badge
    before the user even has a token configured.
    """
    return ai_mod.status()


@app.post("/api/ai/ask", response_model=ai_mod.ChatResponse, dependencies=[AuthDep])
async def post_ai_ask(body: ai_mod.ChatRequest):
    """Grounded answer over project context (lock + state + error catalog + docs).

    Returns a clear "AI unavailable" response (not 5xx) when the API key is
    missing, the SDK isn't installed, or the upstream call fails — so the
    UI degrades cleanly. Pydantic enforces question <= 2000 chars and
    history <= 5 turns.
    """
    return await ai_mod.ask(body)


# ---------- CSV ingest + Table Explorer ----------

class IngestRequest(BaseModel):
    upload_id: str = Field(min_length=1, max_length=64)
    schema_overrides: list[dict] = Field(default_factory=list)
    target: dict = Field(default_factory=dict)  # {database, table}


def _require_install_ready(install_id: str):
    rec = store.get(install_id)
    if not rec:
        raise HTTPException(404, "install not found")
    if rec.state != "READY":
        raise HTTPException(409, f"install is in state {rec.state}; READY required")
    return rec


@app.post("/api/installs/{install_id}/uploads", dependencies=[AuthDep])
async def post_csv_upload(install_id: str, file: UploadFile = File(...)):
    """Stream a CSV upload to disk and return a preview of the inferred schema.

    Hard cap LHS_UPLOAD_MAX_MB (default 500 MB). The file is streamed in
    chunks so we never page the whole thing into memory.
    """
    _require_install_ready(install_id)

    if not file.filename:
        raise HTTPException(400, "filename is required")

    upload_id = f"upl_{secrets.token_hex(6)}"
    try:
        saved_path = await ingest_mod.save_csv_upload(
            install_id=install_id,
            upload_id=upload_id,
            file_stream=file.file,
            filename=file.filename,
        )
    except ingest_mod.UploadTooLargeError as e:
        raise HTTPException(413, str(e))
    except ingest_mod.UploadInvalidError as e:
        raise HTTPException(400, str(e))
    finally:
        try:
            await file.close()
        except Exception:
            pass

    try:
        preview = ingest_mod.preview_csv(saved_path)
    except ingest_mod.UploadInvalidError as e:
        raise HTTPException(400, f"preview failed: {e}")
    except Exception as e:
        raise HTTPException(500, f"preview failed: {type(e).__name__}: {e}")

    return {
        "upload_id": upload_id,
        "filename": saved_path.name,
        "size_bytes": saved_path.stat().st_size,
        "preview": preview,
    }


@app.post("/api/installs/{install_id}/ingest", dependencies=[AuthDep])
async def post_ingest(install_id: str, body: IngestRequest):
    """Kick off (currently: register-then-fail) a CSV ingest job."""
    _require_install_ready(install_id)
    try:
        job = await ingest_mod.kick_off_csv_ingest(
            install_id=install_id,
            upload_id=body.upload_id,
            schema_confirm=body.schema_overrides,
            target=body.target,
        )
    except ingest_mod.UploadInvalidError as e:
        raise HTTPException(400, str(e))
    return job.model_dump()


@app.get("/api/installs/{install_id}/ingest", dependencies=[AuthDep])
def list_ingest_jobs(install_id: str):
    rec = store.get(install_id)
    if not rec:
        raise HTTPException(404, "install not found")
    return [j.model_dump() for j in ingest_mod.list_jobs(install_id)]


@app.get("/api/installs/{install_id}/ingest/{job_id}", dependencies=[AuthDep])
def get_ingest_job(install_id: str, job_id: str):
    rec = store.get(install_id)
    if not rec:
        raise HTTPException(404, "install not found")
    job = ingest_mod.get_job(job_id)
    if not job or job.install_id != install_id:
        raise HTTPException(404, "ingest job not found")
    return job.model_dump()


# ---------- External data sources (Postgres for now) ----------

class PostgresIngestRequest(BaseModel):
    source_id: str = Field(min_length=1, max_length=64)
    table_name: str = Field(min_length=1, max_length=256)
    target: dict = Field(default_factory=dict)  # {database, table}


@app.post("/api/installs/{install_id}/data-sources", dependencies=[AuthDep])
async def post_data_source(install_id: str, body: data_sources_mod.DataSourceCreateRequest):
    """Register an external data source (Postgres). Stored credential is
    encrypted at rest and never echoed back. The response uses the scrubbed
    DataSource model (has_password: bool, no plaintext)."""
    _require_install_ready(install_id)
    try:
        record = await data_sources_mod.create_source(install_id, body)
    except data_sources_mod.WeakPasswordError as e:
        raise HTTPException(400, str(e))
    return record.model_dump()


@app.get("/api/installs/{install_id}/data-sources", dependencies=[AuthDep])
async def get_data_sources(install_id: str):
    rec = store.get(install_id)
    if not rec:
        raise HTTPException(404, "install not found")
    sources = await data_sources_mod.list_sources(install_id)
    return [s.model_dump() for s in sources]


@app.post("/api/data-sources/{source_id}/test", dependencies=[AuthDep])
async def post_data_source_test(source_id: str):
    """Open a real connection with a 5s hard timeout. Returns
    {ok, latency_ms, server_version, schemas, error}."""
    try:
        result = await data_sources_mod.test_source(source_id)
    except data_sources_mod.DataSourceNotFoundError:
        raise HTTPException(404, "data source not found")
    return result


@app.delete("/api/data-sources/{source_id}", status_code=204, dependencies=[AuthDep])
async def delete_data_source(source_id: str):
    src = await data_sources_mod.get_source(source_id)
    if src is None:
        raise HTTPException(404, "data source not found")
    await data_sources_mod.delete_source(source_id)
    return None


@app.post("/api/installs/{install_id}/ingest/postgres", dependencies=[AuthDep])
async def post_ingest_postgres(install_id: str, body: PostgresIngestRequest):
    """Kick off a Postgres -> Iceberg ingest job. v0.4.1 is a stub that walks
    the IngestJob lifecycle and ends with a 'pending v0.5' message."""
    _require_install_ready(install_id)
    try:
        job = await ingest_mod.kick_off_postgres_ingest(
            install_id=install_id,
            source_id=body.source_id,
            table_name=body.table_name,
            target=body.target,
        )
    except ingest_mod.UploadInvalidError as e:
        raise HTTPException(400, str(e))
    return job.model_dump()


@app.get("/api/installs/{install_id}/tables", dependencies=[AuthDep])
async def get_tables(install_id: str):
    """List every (namespace, table) pair the Iceberg REST catalog knows about."""
    _require_install_ready(install_id)
    try:
        namespaces = await table_explorer.list_namespaces()
    except Exception as e:
        raise HTTPException(502, f"iceberg catalog unreachable: {type(e).__name__}: {e}")

    out: list[dict] = []
    for ns in namespaces:
        try:
            tables = await table_explorer.list_tables(ns)
        except Exception:
            continue
        out.extend(tables)
    return {"namespaces": namespaces, "tables": out}


@app.get("/api/installs/{install_id}/tables/{namespace}/{name}", dependencies=[AuthDep])
async def get_table_detail(install_id: str, namespace: str, name: str):
    _require_install_ready(install_id)
    try:
        return await table_explorer.get_table_info(namespace, name)
    except httpx.HTTPStatusError as e:
        status = e.response.status_code if e.response is not None else 502
        if status == 404:
            raise HTTPException(404, f"table {namespace}.{name} not found")
        raise HTTPException(502, f"iceberg catalog error: {status}")
    except Exception as e:
        raise HTTPException(502, f"iceberg catalog unreachable: {type(e).__name__}: {e}")


@app.on_event("shutdown")
async def _shutdown():
    import logging
    log = logging.getLogger("lhs.shutdown")
    # Cancel any in-flight install tasks so we don't leave child processes around.
    tasks = list(_INSTALL_TASKS.values())
    for t in tasks:
        t.cancel()
    for t in tasks:
        try:
            await asyncio.wait_for(t, timeout=5)
        except asyncio.TimeoutError:
            log.warning("install task %s did not cancel within 5s", t.get_name())
        except asyncio.CancelledError:
            pass
        except Exception:
            log.exception("install task %s failed during cancellation", t.get_name())
    # Flush any debounced state writes so a graceful shutdown loses nothing.
    try:
        store.flush()
    except Exception:
        log.exception("state flush failed")
    # Stop notifications config-poll task.
    try:
        await get_dispatcher().stop()
    except Exception:
        log.exception("notify dispatcher stop failed")


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

    # Block destructive actions while an install task is running. status/smoke
    # are read-only and safe to run anytime.
    if body.action in ("stop", "clean"):
        if rec.state in RUNNING_STATES:
            raise HTTPException(409, f"cannot {body.action} while state is {rec.state}; cancel first")
        if install_id in _INSTALL_TASKS and not _INSTALL_TASKS[install_id].done():
            raise HTTPException(409, f"an install task is still running; cancel before {body.action}")

    install_dir = Path(rec.install_dir)
    rc = await run_command(rec.install_id, install_dir, rec.host, m, cmd_name)

    if rc == 0:
        if body.action == "stop":
            store.update_state(rec.install_id, "STOPPED")
        elif body.action == "clean":
            store.update_state(rec.install_id, "CLEANED")
        return {"exit_code": rc, "ok": True}

    # Non-zero rc → publish error event so the UI sees it, and surface as 502.
    bus.publish_nowait(LogEvent(
        install_id=rec.install_id, ts=time.time(), kind="error",
        line=f"{body.action} exited with code {rc}",
    ))
    raise HTTPException(502, {"error": f"{body.action} failed", "exit_code": rc})


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
    # Sequence-based replay: client may pass ?last_seq=N to resume from N+1.
    # If N is older than buffered history, we send a `reset` event and replay
    # everything we still have.
    try:
        last_seq = int(websocket.query_params.get("last_seq", "0"))
    except (TypeError, ValueError):
        last_seq = 0

    # Subscribe BEFORE snapshotting history so any event published in between
    # ends up in the live queue. seq numbers guarantee we don't double-deliver.
    q = await bus.subscribe(install_id)
    try:
        history, last_history_seq, reset_needed = bus.history_snapshot(install_id, since_seq=last_seq)
        if reset_needed:
            await websocket.send_text(LogEvent(
                install_id=install_id, ts=time.time(), kind="reset",
                line="resuming from older snapshot — buffered history truncated"
            ).model_dump_json())
        for evt in history:
            try:
                await websocket.send_text(evt.model_dump_json())
            except Exception:
                return
        while True:
            try:
                evt = await q.get()
            except asyncio.CancelledError:
                raise
            # Guard: if a client passed last_seq that's NEWER than what the
            # live event has (rare; happens with parallel reconnects), don't
            # re-send what they already have.
            if evt.seq is not None and evt.seq <= last_history_seq:
                continue
            try:
                await websocket.send_text(evt.model_dump_json())
            except Exception:
                # Client disconnected mid-stream; bail to finally.
                return
    except WebSocketDisconnect:
        pass
    except Exception:
        import logging
        logging.getLogger("lhs.ws").exception("ws handler crashed for install %s", install_id)
    finally:
        await bus.unsubscribe(install_id, q)


# ---------- Frontend ----------

@app.get("/")
def root():
    return FileResponse(FRONTEND_DIR / "index.html")


@app.get("/healthz")
def healthz():
    return {
        "ok": not _CATALOG_PROBLEMS,
        "catalog_problems": _CATALOG_PROBLEMS,
        "compat_problems": _COMPAT_PROBLEMS,
        "certified_stacks": list_locks(),
        "template_problems": _TEMPLATE_PROBLEMS,
        "compliance_problems": _COMPLIANCE_PROBLEMS,
    }


if FRONTEND_DIR.exists():
    assets_dir = FRONTEND_DIR / "assets"
    if assets_dir.exists():
        app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")
    app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")
