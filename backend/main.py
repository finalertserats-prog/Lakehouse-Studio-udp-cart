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
    destinations as catalog_destinations,
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
from . import destinations as destinations_mod
from . import insyght_connector as insyght_mod
from . import table_explorer
from . import backup as backup_mod
from . import monitoring as monitoring_mod
from . import tls_wizard as tls_mod
from . import caddy_tls as caddy_mod
from . import jdbc_extras as jdbc_mod
from . import upgrade_executor as upgrade_exec_mod
from . import rbac_auth as rbac_mod
from . import data_quality as dq_mod
from . import audit_log

app = FastAPI(title="LakeHouse Studio", version="0.1.0")


# ---------- API versioning (PDD Section 5.1.3) ----------
#
# Every existing route is reachable under BOTH `/api/...` and `/api/v1/...`.
# The un-versioned path is preserved for backward compatibility with the
# current frontend; the versioned path is the forward-compatible surface
# for future API consumers. When v2 lands, /api/v1/ keeps working as-is
# and the un-versioned /api/ continues to alias to v1 until a deprecation
# period elapses.
#
# Implementation: a pure ASGI middleware that rewrites the scope path
# from `/api/v1/...` to `/api/...` BEFORE Starlette's router matches.
# Zero duplicate route registrations — `len(app.routes)` is unchanged
# and OpenAPI still reflects the canonical un-versioned set.
#
# Both HTTP and WebSocket scopes carry "path" — the rewrite is identical
# for both, so a single ASGI middleware handles them in one place.
# (`@app.middleware("http")` would skip WebSocket handshakes entirely.)

_V1_PREFIX = "/api/v1/"
_API_PREFIX = "/api/"


class V1AliasMiddleware:
    """Rewrite `/api/v1/<rest>` -> `/api/<rest>` for HTTP and WebSocket scopes.

    Pure ASGI middleware so the rewrite applies to BOTH http and websocket
    scope types. Lifespan and any other scope types pass through untouched.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope.get("type") in ("http", "websocket"):
            path = scope.get("path", "")
            if path.startswith(_V1_PREFIX):
                rewritten = _API_PREFIX + path[len(_V1_PREFIX):]
                # Build a shallow copy so we never mutate an upstream scope
                # dict that something else might hold a reference to.
                scope = dict(scope)
                scope["path"] = rewritten
                if "raw_path" in scope and scope["raw_path"] is not None:
                    scope["raw_path"] = rewritten.encode("latin-1")
        await self.app(scope, receive, send)


app.add_middleware(V1AliasMiddleware)


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


async def _require_auth(request: Request,
                        authorization: Optional[str] = Header(default=None),
                        x_studio_token: Optional[str] = Header(default=None)):
    # OPT-IN RBAC path. When LHS_RBAC_ENABLED is truthy, the per-user token
    # store in backend/rbac_auth.py takes over. Token -> User lookup, then a
    # per-route permission check against the v1 ROUTE_PERMISSIONS map. The
    # legacy single-token path below stays the default — flipping the env
    # var is the only way to enable RBAC, and the route surface is unchanged.
    if rbac_mod.is_rbac_enabled():
        user = await rbac_mod.authenticate(authorization, x_studio_token)
        if user is None:
            raise HTTPException(401, "auth required")
        route = request.scope.get("route")
        route_path = getattr(route, "path", request.url.path)
        allowed = await rbac_mod.require_permission(user, route_path, request.method)
        if not allowed:
            raise HTTPException(403, "forbidden")
        # Stash the user on request.state for downstream handlers (audit log
        # wiring is a follow-on; nothing reads this yet).
        request.state.rbac_user = user
        return
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

    # Start the backup scheduler (60s-tick loop firing enabled schedules).
    await backup_mod.get_scheduler().start()

    # Opt-in DR drill — periodically verifies the latest backup tarball per
    # install is readable + structurally sound. Non-destructive (no restore).
    if backup_mod.is_drill_enabled():
        try:
            await backup_mod.get_drill_scheduler().start()
        except Exception:
            log.exception("DR drill scheduler failed to start; continuing without it")

    # Start the audit subscriber if the operator enabled it. Opt-in via
    # LHS_AUDIT_ENABLED=true; default behaviour is unchanged.
    if audit_log.is_enabled():
        try:
            audit_log.init_audit_db()
            await audit_log.get_subscriber().start()
            if audit_log.is_scheduler_enabled():
                await audit_log.get_scheduler().start()
        except Exception:
            import logging
            logging.getLogger("lhs.audit").exception(
                "audit subscriber failed to start; continuing without audit"
            )


def _require_catalog_ok() -> None:
    # Warnings (e.g. "warning — stack lock status is candidate") MUST NOT
    # 503 the routes — they're informational, not blocking. Mirrors the
    # healthz error/warning split. Errors still block.
    errors = [p for p in _CATALOG_PROBLEMS if "warning —" not in (p or "")]
    if errors:
        raise HTTPException(503, {"error": "catalog invalid", "problems": errors})


CatalogOk = Depends(_require_catalog_ok)


# ---------- Auth status ----------

@app.get("/api/auth/status")
def auth_status():
    return {"auth_required": bool(AUTH_TOKEN)}


# ---------- Catalog / Goals (the "shop" surface) ----------

@app.get("/api/catalog", dependencies=[AuthDep, CatalogOk])
def get_catalog():
    """Component catalog: categories + their pickable components + alternates
    marked coming-soon + downstream destinations (BI tools the user can
    connect AFTER the lakehouse is built)."""
    return {
        "categories": catalog_categories(),
        "goals": catalog_goals(),
        "recommended_sets": catalog_recommended_sets(),
        "destinations": catalog_destinations(),
    }


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

    # Per-environment isolation: when the request names a tier, derive a
    # unique default install_dir suffix so dev/staging/prod can coexist on
    # the same host without colliding on /work/udp. Explicit install_dir
    # always wins — operators who want full control still get it.
    default_subdir = "udp" if not body.environment else f"udp-{body.environment}"
    raw_dir = body.install_dir or str(WORK_DIR / default_subdir)
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

    # Validate goal against known goals (if catalog is loaded).
    # Warnings (e.g. "warning — stack lock status is candidate") must NOT
    # block installs — they're informational. Same filter as _require_catalog_ok.
    catalog_errors = [p for p in _CATALOG_PROBLEMS if "warning —" not in (p or "")]
    if body.goal:
        if catalog_errors:
            raise HTTPException(503, "cannot validate goal: catalog has errors")
        known_goals = {g["id"] for g in catalog_goals()}
        if body.goal not in known_goals:
            raise HTTPException(400, f"unknown goal '{body.goal}'; known: {sorted(known_goals)}")

    # Validate cart components against the catalog (cart was already validated
    # for shape/dedup/identifier-rules by the Pydantic field validator)
    if body.cart:
        if catalog_errors:
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
        environment=body.environment,
    )

    # Per-environment isolation: inject UDP_PROJECT_NAME suffix + UDP_ENV
    # so docker-compose containers + volumes don't collide across tiers on
    # the same host. The stack manifest's env_defaults provide the base
    # values; we only patch when the request specifies an environment, and
    # we never overwrite an operator-provided override.
    install_env_overrides = dict(body.env_overrides)
    if body.environment:
        base_project = (
            install_env_overrides.get("UDP_PROJECT_NAME")
            or m.env_defaults.get("UDP_PROJECT_NAME")
            or "unified-data-plug"
        )
        install_env_overrides.setdefault(
            "UDP_PROJECT_NAME", f"{base_project}-{body.environment}"
        )
        install_env_overrides.setdefault("UDP_ENV", body.environment)

    runner = UDPRunner(m, rec.install_id, body.host, install_dir)
    wrapped = _make_install_task_wrapper(
        rec.install_id, lambda: runner.run(install_env_overrides)
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


# ---------- JDBC driver extras (opt-in side-load for Postgres / MySQL ingest) ----------
#
# Same override-file pattern as the Caddy TLS sidecar above: write a
# docker-compose.jdbc.yml override into the install_dir, return the
# activate command, never touch the base compose file or the install
# pipeline. The override declares a one-shot init container that
# downloads the requested JDBC jars into a named docker volume; the
# spark service mounts that volume so Spark sees the drivers on its
# classpath. Required to unblock the real Postgres/MySQL ingest path.


@app.post("/api/installs/{install_id}/jdbc/enable", dependencies=[AuthDep])
async def post_jdbc_enable(install_id: str, body: jdbc_mod.JdbcExtrasProfile):
    """Write the docker-compose.jdbc.yml override into the install_dir.

    Refuses if the install is mid-pipeline (RUNNING_STATES) -- layering an
    override on a workspace that's still being cloned/started races docker
    compose. Returns the override path, the activate command, and the
    pinned driver versions so the operator can verify what will be
    downloaded.
    """
    rec = store.get(install_id)
    if not rec:
        raise HTTPException(404, "install not found")
    if rec.state in RUNNING_STATES:
        raise HTTPException(
            409,
            f"cannot enable JDBC extras while install state is {rec.state}; "
            f"wait for it to reach a terminal state first",
        )
    try:
        return await jdbc_mod.enable_jdbc_extras(install_id, body)
    except ValueError as e:
        raise HTTPException(400, str(e))


@app.post("/api/installs/{install_id}/jdbc/disable", dependencies=[AuthDep])
async def post_jdbc_disable(install_id: str):
    """Remove the docker-compose.jdbc.yml override from the install_dir.

    Refuses if the install is mid-pipeline. The caller is responsible for
    running the returned deactivate command FIRST -- the route does not
    stop the jdbc-extras init container itself (same lifecycle-control
    contract as the Caddy / monitoring sidecars). The named volume
    holding the downloaded jars is intentionally retained so re-enabling
    skips the download.
    """
    rec = store.get(install_id)
    if not rec:
        raise HTTPException(404, "install not found")
    if rec.state in RUNNING_STATES:
        raise HTTPException(
            409,
            f"cannot disable JDBC extras while install state is {rec.state}; "
            f"wait for it to reach a terminal state first",
        )
    try:
        return await jdbc_mod.disable_jdbc_extras(install_id)
    except ValueError as e:
        raise HTTPException(400, str(e))


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


class BackupScheduleUpdateRequest(BaseModel):
    enabled: bool
    interval_hours: int = Field(default=24, ge=1, le=168)
    kind: str = Field(default="metadata", pattern=r"^(metadata|full)$")


@app.get("/api/installs/{install_id}/backups/schedule", dependencies=[AuthDep])
def get_install_backup_schedule(install_id: str):
    """Return the persisted auto-backup schedule for this install,
    or a disabled default if none has been saved yet."""
    rec = store.get(install_id)
    if not rec:
        raise HTTPException(404, "install not found")
    return backup_mod.load_schedule(install_id).model_dump()


@app.put("/api/installs/{install_id}/backups/schedule", dependencies=[AuthDep])
def put_install_backup_schedule(install_id: str, body: BackupScheduleUpdateRequest):
    """Upsert the auto-backup schedule. Enabling (or re-saving while enabled)
    recomputes next_run_at = now + interval_hours."""
    rec = store.get(install_id)
    if not rec:
        raise HTTPException(404, "install not found")
    schedule = backup_mod.save_schedule(install_id, body.model_dump())
    return schedule.model_dump()


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


# ---------- Audit Log ----------

@app.get("/api/audit", dependencies=[AuthDep])
async def get_audit(
    actor: Optional[str] = None,
    action: Optional[str] = None,
    resource_type: Optional[str] = None,
    since: Optional[str] = None,
    limit: int = 200,
):
    """Query the persisted audit log.

    Returns ``503`` when ``LHS_AUDIT_ENABLED`` is not set — surfaces the
    opt-in nature so callers know to enable the feature rather than getting
    a confusing empty list.

    ``since`` accepts either a unix timestamp (float seconds) or an ISO-8601
    datetime string. Limit is clamped server-side to 5000.
    """
    if not audit_log.is_enabled():
        raise HTTPException(503, {
            "error": "audit log disabled",
            "hint": "set LHS_AUDIT_ENABLED=true and restart the server",
        })
    since_ts: Optional[float] = None
    if since is not None and since != "":
        try:
            since_ts = float(since)
        except ValueError:
            from datetime import datetime
            try:
                since_ts = datetime.fromisoformat(since).timestamp()
            except ValueError:
                raise HTTPException(400, f"invalid 'since' value {since!r}; expected float seconds or ISO-8601")
    entries = await audit_log.query(
        actor=actor,
        action=action,
        resource_type=resource_type,
        since_ts=since_ts,
        limit=limit,
    )
    return [e.model_dump() for e in entries]


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


@app.post(
    "/api/components/{component_id}/recommend",
    response_model=ai_mod.ComponentRecommendation,
    dependencies=[AuthDep],
)
async def post_component_recommend(
    component_id: str,
    body: Optional[ai_mod.ComponentRecommendationRequest] = None,
):
    """Per-component LLM-grounded recommendation. Body is optional — when
    omitted, the recommendation uses just the component's catalog entry and
    siblings (no operator context). Returns a graceful disabled-state shape
    (not 5xx) when the AI assistant isn't enabled, matching /api/ai/ask."""
    if body is None:
        body = ai_mod.ComponentRecommendationRequest(component_id=component_id)
    elif body.component_id != component_id:
        raise HTTPException(
            400,
            "component_id in path and body must match "
            f"('{component_id}' vs '{body.component_id}')",
        )
    return await ai_mod.recommend_component(body)


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


# ---------- Destinations (OUTBOUND mirror of data-sources) ----------
#
# Symmetric to the data_sources block above. After a stack reaches READY,
# the operator wires downstream BI/analytics tools to it. Routes are
# additive — no existing contract is touched.

@app.post("/api/installs/{install_id}/destinations", dependencies=[AuthDep])
async def post_destination(install_id: str,
                           body: destinations_mod.DestinationCreateRequest):
    """Register a downstream destination (Insyght / Tableau / etc.). Credentials
    are encrypted at rest and never echoed back. Response uses the scrubbed
    Destination model (has_credentials: bool, no plaintext)."""
    _require_install_ready(install_id)
    try:
        record = await destinations_mod.create_destination(install_id, body)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return record.model_dump()


@app.get("/api/installs/{install_id}/destinations", dependencies=[AuthDep])
async def get_destinations(install_id: str):
    rec = store.get(install_id)
    if not rec:
        raise HTTPException(404, "install not found")
    items = await destinations_mod.list_destinations(install_id)
    return [d.model_dump() for d in items]


@app.get("/api/destinations/{destination_id}", dependencies=[AuthDep])
async def get_destination_by_id(destination_id: str):
    dest = await destinations_mod.get_destination(destination_id)
    if dest is None:
        raise HTTPException(404, "destination not found")
    return dest.model_dump()


@app.post("/api/destinations/{destination_id}/test", dependencies=[AuthDep])
async def post_destination_test(destination_id: str):
    """Dispatch to the per-mode tester. sql_pull mode requires the install
    to be in READY state since it opens a real connection to StarRocks."""
    dest = await destinations_mod.get_destination(destination_id)
    if dest is None:
        raise HTTPException(404, "destination not found")
    if dest.connection_mode == "sql_pull":
        _require_install_ready(dest.install_id)
    try:
        return await destinations_mod.test_destination(destination_id)
    except destinations_mod.DestinationNotFoundError:
        raise HTTPException(404, "destination not found")


@app.post("/api/destinations/{destination_id}/provision", dependencies=[AuthDep])
async def post_destination_provision(destination_id: str):
    """Provision the downstream-tool-facing artifacts for this destination.

    For Insyght + sql_pull: creates a per-destination StarRocks read-only
    user and grants SELECT on the configured database.
    """
    dest = await destinations_mod.get_destination(destination_id)
    if dest is None:
        raise HTTPException(404, "destination not found")
    _require_install_ready(dest.install_id)

    creds = destinations_mod._decrypt_credentials(destination_id)

    if dest.kind == "insyght" and dest.connection_mode == "sql_pull":
        result = await insyght_mod.provision_sql_pull(
            dest.install_id, dest.config, creds,
        )
    elif dest.kind == "insyght" and dest.connection_mode == "push_api":
        result = await insyght_mod.provision_push_api(
            dest.install_id, dest.config, creds,
        )
    else:
        # Other vendors are sql_pull-only via the same StarRocks user path.
        # We reuse the Insyght provisioner since the SQL is identical for
        # any MySQL-protocol consumer.
        result = await insyght_mod.provision_sql_pull(
            dest.install_id, dest.config, creds,
        )
    return result


@app.get("/api/destinations/{destination_id}/connection", dependencies=[AuthDep])
async def get_destination_connection(destination_id: str):
    """Return the sanitized connection bundle the operator hands to the BI tool.

    Plaintext credentials are NEVER returned — operators already have them
    (they set them at create time).
    """
    try:
        return await destinations_mod.generate_connection_payload(destination_id)
    except destinations_mod.DestinationNotFoundError:
        raise HTTPException(404, "destination not found")


@app.delete("/api/destinations/{destination_id}", status_code=204, dependencies=[AuthDep])
async def delete_destination(destination_id: str):
    dest = await destinations_mod.get_destination(destination_id)
    if dest is None:
        raise HTTPException(404, "destination not found")
    await destinations_mod.delete_destination(destination_id)
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


class MysqlIngestRequest(BaseModel):
    source_id: str = Field(min_length=1, max_length=64)
    table_name: str = Field(min_length=1, max_length=256)
    target: dict = Field(default_factory=dict)  # {database, table}


@app.post("/api/installs/{install_id}/ingest/mysql", dependencies=[AuthDep])
async def post_ingest_mysql(install_id: str, body: MysqlIngestRequest):
    """Kick off a MySQL -> Iceberg ingest job. v0.5.1 is a stub that walks
    the IngestJob lifecycle and ends with a 'pending v0.5.1' message —
    Spark image needs `mysql-connector-j-X.jar` first."""
    _require_install_ready(install_id)
    try:
        job = await ingest_mod.kick_off_mysql_ingest(
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


# ---------- Data Quality checks (additive, READY-only) ----------
#
# Lightweight per-table assertions. Every check translates to a single
# read-only SELECT routed through `sql_editor.run_user_sql`, which only
# accepts SELECT/SHOW/DESCRIBE/EXPLAIN/WITH. Identifiers (namespace, table,
# column) are validated at the API boundary against the same _IDENT_RE
# (`^[A-Za-z0-9_.\-]{1,128}$`) that table_explorer uses — that strict
# allowlist is the SQL-injection moat. Persistence: WORK_DIR/dq_checks.json
# + WORK_DIR/dq_results.json (debounced atomic write, mirrors state.py).


class DQCheckCreateRequest(BaseModel):
    namespace: str = Field(min_length=1, max_length=128)
    table: str = Field(min_length=1, max_length=128)
    kind: str = Field(min_length=1, max_length=32)
    column: Optional[str] = Field(default=None, max_length=128)
    expected: Optional[float] = None
    enabled: bool = True


@app.post("/api/installs/{install_id}/dq/checks", dependencies=[AuthDep])
async def post_dq_check(install_id: str, body: DQCheckCreateRequest):
    """Create a Data Quality check for a table in an installed stack.

    Refuses unless the install is in state READY. Validates the
    kind/column/expected requirement matrix and the identifier allowlist
    (`^[A-Za-z0-9_.\\-]{1,128}$`) at the boundary. Any rejected identifier
    raises 400; the SQL templates downstream only ever see allowlisted
    identifiers.
    """
    _require_install_ready(install_id)
    try:
        check = await dq_mod.create_check(install_id, body.model_dump())
    except ValueError as e:
        raise HTTPException(400, str(e))
    return check.model_dump()


@app.get("/api/installs/{install_id}/dq/checks", dependencies=[AuthDep])
async def get_dq_checks(install_id: str):
    """List all DQ checks registered against this install (newest first)."""
    _require_install_ready(install_id)
    checks = await dq_mod.list_checks(install_id)
    return [c.model_dump() for c in checks]


@app.delete("/api/dq/checks/{check_id}", status_code=204, dependencies=[AuthDep])
async def delete_dq_check(check_id: str):
    """Remove a DQ check. 404 if it doesn't exist. Past DQResults are
    retained but become orphans (list_results filters by install ownership,
    so they fall out of the per-install result view)."""
    try:
        await dq_mod.delete_check(check_id)
    except KeyError:
        raise HTTPException(404, f"dq check {check_id!r} not found")
    return None


@app.post("/api/dq/checks/{check_id}/run", dependencies=[AuthDep])
async def post_dq_check_run(check_id: str):
    """Execute a DQ check now. Builds + runs the read-only SELECT, compares
    the observed COUNT(*) to the threshold, persists a DQResult, and
    returns it. Refuses if the owning install isn't READY."""
    check = await dq_mod.get_check(check_id)
    if check is None:
        raise HTTPException(404, f"dq check {check_id!r} not found")
    rec = store.get(check.install_id)
    if not rec:
        raise HTTPException(404, "install not found")
    if rec.state != "READY":
        raise HTTPException(409, f"install is in state {rec.state}; READY required")
    try:
        result = await dq_mod.run_check(check_id)
    except KeyError:
        raise HTTPException(404, f"dq check {check_id!r} not found")
    return result.model_dump()


@app.get("/api/installs/{install_id}/dq/results", dependencies=[AuthDep])
async def get_dq_results(
    install_id: str,
    check_id: Optional[str] = None,
    limit: int = 20,
):
    """Recent DQResults for an install, optionally filtered by check_id."""
    _require_install_ready(install_id)
    results = await dq_mod.list_results(install_id, check_id=check_id, limit=limit)
    return [r.model_dump() for r in results]


# ---------- RBAC (opt-in admin surface) ----------
#
# These routes are present on the running app at all times, but they only do
# anything useful when LHS_RBAC_ENABLED is truthy. When RBAC is off, every
# request through _require_auth resolves the legacy single-token path and
# request.state.rbac_user is never set — so anything that requires a real
# RBAC user returns 503. That keeps the install-time experience identical
# for operators who never opt in.


def _require_rbac_enabled() -> None:
    if not rbac_mod.is_rbac_enabled():
        raise HTTPException(503, "RBAC is not enabled on this install")


def _require_rbac_user(request: Request) -> "rbac_mod.User":
    user = getattr(request.state, "rbac_user", None)
    if user is None:
        # AuthDep should have set this when RBAC is on. If it's missing
        # something is wrong with the wiring — treat as unauthenticated.
        raise HTTPException(401, "auth required")
    return user


def _require_role(user: "rbac_mod.User", allowed: set[str]) -> None:
    if user.role not in allowed:
        raise HTTPException(403, "forbidden")


class RbacUserCreateRequest(BaseModel):
    email: str = Field(min_length=3, max_length=320)
    role: str = Field(min_length=1, max_length=32)

    @field_validator("role")
    @classmethod
    def _role_known(cls, v: str) -> str:
        from .v1.rbac import BUILTIN_ROLES
        if v not in BUILTIN_ROLES:
            raise ValueError(f"unknown role {v!r}; valid: {sorted(BUILTIN_ROLES)}")
        return v


@app.post("/api/rbac/users", dependencies=[AuthDep])
async def post_rbac_user(request: Request, body: RbacUserCreateRequest):
    """Create a new RBAC user. OWNER only. Returns the plaintext token ONCE."""
    _require_rbac_enabled()
    user = _require_rbac_user(request)
    _require_role(user, {"OWNER"})
    try:
        created, plaintext = await rbac_mod.create_user(body.email, body.role)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {
        # Scrubbed via UserPublic — never expose the hashed token over HTTP.
        "user": rbac_mod.to_public(created).model_dump(),
        # Plaintext surfaced exactly once. The DB never stores it.
        "api_token": plaintext,
        "warning": "store this token now — it will not be shown again",
    }


@app.get("/api/rbac/users", dependencies=[AuthDep])
async def get_rbac_users(request: Request):
    """List RBAC users. OWNER + ADMIN."""
    _require_rbac_enabled()
    user = _require_rbac_user(request)
    _require_role(user, {"OWNER", "ADMIN"})
    users = await rbac_mod.list_users()
    return {"users": [rbac_mod.to_public(u).model_dump() for u in users]}


@app.delete("/api/rbac/users/{user_id}", status_code=204, dependencies=[AuthDep])
async def delete_rbac_user(user_id: str, request: Request):
    """Delete an RBAC user. OWNER only. Cannot self-delete."""
    _require_rbac_enabled()
    user = _require_rbac_user(request)
    _require_role(user, {"OWNER"})
    if user.user_id == user_id:
        raise HTTPException(400, "an OWNER cannot delete themselves")
    deleted = await rbac_mod.delete_user(user_id)
    if not deleted:
        raise HTTPException(404, f"user {user_id} not found")
    return None


@app.get("/api/rbac/me", dependencies=[AuthDep])
async def get_rbac_me(request: Request):
    """Return the calling user's RBAC identity. Any authenticated role."""
    _require_rbac_enabled()
    user = _require_rbac_user(request)
    return {"user": rbac_mod.to_public(user).model_dump()}


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
    # Stop backup scheduler tick loop.
    try:
        await backup_mod.get_scheduler().stop()
    except Exception:
        log.exception("backup scheduler stop failed")
    if backup_mod.is_drill_enabled():
        try:
            await backup_mod.get_drill_scheduler().stop()
        except Exception:
            log.exception("DR drill scheduler stop failed")
    # Stop the audit subscriber if it was enabled. Idempotent — safe to call
    # even if start() was never invoked or already failed.
    if audit_log.is_enabled():
        try:
            if audit_log.is_scheduler_enabled():
                await audit_log.get_scheduler().stop()
            await audit_log.get_subscriber().stop()
        except Exception:
            log.exception("audit subscriber/scheduler stop failed")


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


# ---------- Per-service Docker logs (snapshot + WS stream) ----------
#
# Read-only viewer over `docker compose logs <service>` for an installed
# stack. The snapshot route is hard-capped at 500 lines and 10s wall-clock;
# the WebSocket mirrors the existing /logs WS auth (origin guard + close
# codes) so the UI's reconnect logic treats both streams identically.

def _manifest_service_names(rec) -> set[str]:
    """Return the set of compose service names declared by an install's
    manifest. Falls back to component ids when service_name is missing."""
    try:
        m = load_manifest(rec.stack_id)
    except KeyError:
        return set()
    out: set[str] = set()
    for comp in m.components:
        out.add(comp.get("service_name") or comp.get("id"))
    return {s for s in out if s}


@app.get("/api/installs/{install_id}/services/{service_name}/logs", dependencies=[AuthDep])
async def get_service_logs_route(
    install_id: str,
    service_name: str,
    tail: int = 200,
    since: Optional[str] = None,
):
    """Snapshot the last N lines of one service's docker compose logs.

    404 if install not found. 400 if service_name is not declared in the
    install's manifest OR contains characters outside [A-Za-z0-9_-].
    Hard 500-line cap, 10s wall-clock timeout — see service_logs.MAX_TAIL /
    SNAPSHOT_TIMEOUT_SEC. Failures (docker missing, exit non-zero, timeout)
    come back as 200 with an `error` field so the UI can render the message
    without bouncing through a separate error path.
    """
    from .service_logs import _validate_service_name, get_service_logs

    rec = store.get(install_id)
    if not rec:
        raise HTTPException(404, "install not found")
    services = _manifest_service_names(rec)
    if not services:
        raise HTTPException(404, "stack not found")
    try:
        _validate_service_name(service_name, services)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return await get_service_logs(Path(rec.install_dir), service_name, tail=tail, since=since)


@app.websocket("/api/installs/{install_id}/services/{service_name}/logs/stream")
async def ws_service_logs(websocket: WebSocket, install_id: str, service_name: str):
    """Live-tail one service's docker compose logs over a WebSocket.

    Mirrors /api/installs/{install_id}/logs for auth + close codes so the
    frontend's reconnect loop works unchanged:
      * Origin guard (1008 on cross-origin) — same allow-list as /logs.
      * 4001 if install_id is unknown.
      * 4002 if service_name fails validation (charset OR not in manifest).
    On success we accept the socket then stream decoded log lines as raw
    text frames (one frame per line). Cancellation tears down the docker
    subprocess via stream_service_logs's finally clause.
    """
    from .service_logs import _validate_service_name, stream_service_logs

    origin = websocket.headers.get("origin", "")
    host_hdr = websocket.headers.get("host", "")
    if origin:
        from urllib.parse import urlparse
        parsed = urlparse(origin)
        allowed_hosts = {host_hdr, "localhost", "127.0.0.1"}
        if parsed.hostname not in allowed_hosts and parsed.netloc != host_hdr:
            await websocket.close(code=1008)
            return

    rec = store.get(install_id)
    if not rec:
        await websocket.close(code=4001)
        return
    services = _manifest_service_names(rec)
    try:
        _validate_service_name(service_name, services)
    except ValueError:
        await websocket.close(code=4002)
        return

    # Per Gemini v0.5.1 review: when RBAC is on, a logged-in OPERATOR/VIEWER
    # could otherwise tail any install's logs by guessing install_id. Gate
    # the stream on the same Bearer-token permission check as the HTTP route.
    # No-op when RBAC is off (legacy single-token mode keeps origin-guard
    # only, matching the existing /logs WS).
    if rbac_mod.is_rbac_enabled():
        auth_hdr = websocket.headers.get("authorization", "")
        user = None
        if auth_hdr:
            try:
                user = await rbac_mod.authenticate(auth_hdr)
            except Exception:
                user = None
        if user is None:
            await websocket.close(code=4003)  # unauthorized
            return
        try:
            allowed = await rbac_mod.require_permission(user, "/api/installs/{install_id}/logs")
        except Exception:
            allowed = False
        if not allowed:
            await websocket.close(code=4003)
            return

    await websocket.accept()
    try:
        async for line in stream_service_logs(Path(rec.install_dir), service_name):
            try:
                await websocket.send_text(line)
            except Exception:
                return
    except WebSocketDisconnect:
        pass
    except Exception:
        import logging
        logging.getLogger("lhs.ws").exception(
            "ws service-logs handler crashed for install %s svc %s", install_id, service_name
        )


# ---------- Frontend ----------

@app.get("/")
def root():
    return FileResponse(FRONTEND_DIR / "index.html")


@app.get("/healthz")
def healthz():
    # Validators surface both errors AND warnings in the same problems list.
    # Warnings (prefixed with "warning —") describe expected non-fatal states
    # like a candidate-status stack reference — they should be visible but
    # MUST NOT flip `ok` to false.
    def _is_error(msg: str) -> bool:
        return "warning —" not in (msg or "")
    cat_errors = [p for p in _CATALOG_PROBLEMS if _is_error(p)]
    tpl_errors = [p for p in _TEMPLATE_PROBLEMS if _is_error(p)]
    comp_errors = [p for p in _COMPLIANCE_PROBLEMS if _is_error(p)]
    return {
        "ok": not (cat_errors or tpl_errors or comp_errors),
        "catalog_problems": _CATALOG_PROBLEMS,
        "compat_problems": _COMPAT_PROBLEMS,
        "certified_stacks": list_locks(),
        "template_problems": _TEMPLATE_PROBLEMS,
        "compliance_problems": _COMPLIANCE_PROBLEMS,
        "errors_count": len(cat_errors) + len(tpl_errors) + len(comp_errors),
        "warnings_count": (
            len(_CATALOG_PROBLEMS) + len(_TEMPLATE_PROBLEMS) + len(_COMPLIANCE_PROBLEMS)
            - len(cat_errors) - len(tpl_errors) - len(comp_errors)
        ),
    }


if FRONTEND_DIR.exists():
    assets_dir = FRONTEND_DIR / "assets"
    if assets_dir.exists():
        app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")
    app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")
