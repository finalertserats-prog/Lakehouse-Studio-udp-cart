"""Compatibility lock file loader + install-time enforcement.

Reads stacks/compatibility/<stack-id>.lock.yaml for each certified stack.
Provides two enforcement points:

  1. Catalog validation at startup — every stack referenced by the catalog
     must have a matching lock file. Mismatch (catalog version != lock
     version) is reported via /healthz.

  2. Install-time precheck — before kicking off `docker compose up`, verify
     that every image tag in the lock file STILL EXISTS on its registry.
     This catches the v0.3-shipping disaster pattern: a tag that worked
     when the stack was certified gets removed upstream, and the install
     fails 5 minutes in with a "not found" error instead of upfront.

Per the founding architecture doc: the matrix is the moat. This module is
the enforcement layer.
"""
from __future__ import annotations
import asyncio
import shutil
from functools import lru_cache
from pathlib import Path
from typing import Any
import yaml

from .config import ROOT


COMPAT_DIR = ROOT / "stacks" / "compatibility"


# Strict docker image:tag regex. We pass image refs to `docker manifest inspect`
# via subprocess_exec (no shell), so injection isn't a concern, BUT a malformed
# ref can be misinterpreted by docker itself (e.g. "--rm" parsed as a flag).
# Reject anything not matching real docker naming conventions before exec.
import re as _re
_DOCKER_IMAGE_RE = _re.compile(r"^[a-z0-9][a-z0-9._\-/]{0,254}$")
_DOCKER_TAG_RE = _re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.\-]{0,127}$")


def _safe_image_ref(image: str, tag: str) -> bool:
    """True iff image+tag look like real docker refs. Use before subprocess_exec."""
    return bool(
        isinstance(image, str) and isinstance(tag, str)
        and _DOCKER_IMAGE_RE.match(image) and _DOCKER_TAG_RE.match(tag)
    )


class CompatibilityError(ValueError):
    """A stack's lock file is missing, malformed, or contradicts the catalog."""


@lru_cache(maxsize=32)
def load_lock(stack_id: str) -> dict[str, Any] | None:
    """Load a stack's compatibility lock. Returns None if no lock exists."""
    path = COMPAT_DIR / f"{stack_id}.lock.yaml"
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise CompatibilityError(f"{path.name}: root must be a mapping")
    return data


def list_locks() -> list[str]:
    """All stack_ids that have a lock file."""
    return sorted(p.stem.replace(".lock", "") for p in COMPAT_DIR.glob("*.lock.yaml"))


def validate_against_catalog(stack_id: str, catalog_components: list[dict]) -> list[str]:
    """Cross-check the catalog's components against the stack's lock file.

    Returns the list of mismatches (image:tag in catalog doesn't match lock).
    Empty list = consistent. Non-empty = catalog has drifted from lock.
    """
    lock = load_lock(stack_id)
    if lock is None:
        return [f"no compatibility lock file at {COMPAT_DIR}/{stack_id}.lock.yaml"]
    problems: list[str] = []
    lock_by_id = {c["id"]: c for c in lock.get("components", [])}
    cat_by_id = {c["id"]: c for c in catalog_components}

    # Spot the obvious drift cases
    for cid, cat_comp in cat_by_id.items():
        lock_comp = lock_by_id.get(cid)
        if lock_comp is None:
            # catalog has a component the lock doesn't cover — explicit lock-add needed
            problems.append(f"component '{cid}' is in catalog but missing from lock")
            continue
        cat_image = cat_comp.get("image", "")
        if ":" in cat_image:
            cat_repo, cat_tag = cat_image.rsplit(":", 1)
            lock_repo = lock_comp.get("image", "")
            lock_tag = lock_comp.get("tag", "")
            if cat_repo != lock_repo:
                problems.append(
                    f"component '{cid}': catalog repo {cat_repo!r} != lock repo {lock_repo!r}"
                )
            if cat_tag != lock_tag:
                problems.append(
                    f"component '{cid}': catalog tag {cat_tag!r} != lock tag {lock_tag!r} — bump the lock with evidence"
                )
    return problems


async def precheck_image_availability(stack_id: str, timeout: int = 10) -> dict:
    """Before installing, verify every image tag in the lock STILL exists on the
    registry. Returns {ok, checks: [...]}.

    Each check: {image, tag, status: "available"|"missing"|"unknown", detail}.
    Status "missing" means the registry returned "not found" — install would
    fail at `docker compose up` minutes later. Stop now and surface a clear
    error rather than letting the user wait.
    """
    lock = load_lock(stack_id)
    if lock is None:
        return {"ok": False, "error": f"no lock file for {stack_id}", "checks": []}
    if shutil.which("docker") is None:
        return {"ok": False, "error": "docker CLI not on PATH", "checks": []}

    checks: list[dict] = []

    async def _check_one(image: str, tag: str) -> dict:
        if not _safe_image_ref(image, tag):
            return {"image": image, "tag": tag, "status": "unknown",
                    "detail": "refused: image/tag does not match docker naming rules"}
        ref = f"{image}:{tag}"
        try:
            proc = await asyncio.create_subprocess_exec(
                "docker", "manifest", "inspect", ref,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                stdout_b, stderr_b = await asyncio.wait_for(proc.communicate(), timeout=timeout)
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
                return {"image": image, "tag": tag, "status": "unknown",
                        "detail": f"timed out after {timeout}s"}
            stdout = stdout_b.decode("utf-8", "replace")
            stderr = stderr_b.decode("utf-8", "replace")
            if proc.returncode == 0 and stdout.strip().startswith("{"):
                return {"image": image, "tag": tag, "status": "available", "detail": None}
            err = (stderr or stdout).strip().splitlines()[0][:200] if (stderr or stdout) else ""
            if "no such manifest" in err.lower() or "not found" in err.lower():
                return {"image": image, "tag": tag, "status": "missing", "detail": err}
            return {"image": image, "tag": tag, "status": "unknown", "detail": err}
        except Exception as e:
            return {"image": image, "tag": tag, "status": "unknown",
                    "detail": f"{type(e).__name__}: {e}"}

    # Check all images in parallel — much faster than sequential
    tasks = []
    for comp in lock.get("components", []):
        image = comp.get("image")
        tag = comp.get("tag")
        if image and tag:
            tasks.append(_check_one(image, tag))
    checks = await asyncio.gather(*tasks)

    missing = [c for c in checks if c["status"] == "missing"]
    return {
        "ok": not missing,
        "stack_id": stack_id,
        "missing_count": len(missing),
        "checks": checks,
    }


@lru_cache(maxsize=32)
def load_upgrades(stack_id: str) -> dict[str, Any] | None:
    """Load a stack's upgrade candidates from the sibling .upgrades.yaml file.
    Returns None if no upgrades file exists (acceptable — candidates are opt-in).
    """
    path = COMPAT_DIR / f"{stack_id}.upgrades.yaml"
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise CompatibilityError(f"{path.name}: root must be a mapping")
    return data


def list_upgrade_candidates(stack_id: str) -> list[dict[str, Any]]:
    """Per-candidate rows: current_tag from lock + candidate_tag + feasibility hint.
    Empty list if no upgrades file exists.
    """
    upgrades = load_upgrades(stack_id)
    if upgrades is None:
        return []
    lock = load_lock(stack_id) or {}
    lock_by_id = {c["id"]: c for c in lock.get("components", [])}
    cached = {
        # key = (component_id, candidate_tag) -> verdict
        (entry["component_id"], entry["tag"]): entry.get("verdict")
        for entry in (upgrades.get("pairwise_tested") or [])
        if isinstance(entry, dict)
    }
    out = []
    for cand in upgrades.get("candidates", []) or []:
        cid = cand.get("component_id")
        cur = lock_by_id.get(cid, {})
        out.append({
            "component_id": cid,
            "component_name": cur.get("name", cid),
            "current_tag": cur.get("tag"),
            "candidate_tag": cand.get("tag"),
            "source": cand.get("source"),
            "discovered_at": cand.get("discovered_at"),
            "notes": cand.get("notes"),
            "feasibility_hint": cached.get((cid, cand.get("tag"))),  # pass | unknown | fail | None
        })
    return out


async def simulate_upgrade(stack_id: str, proposed: dict[str, str]) -> dict[str, Any]:
    """Dry-run an upgrade. Overlays `proposed: {component_id: tag}` on the
    lock, runs registry precheck on the overlay, walks incompatible[] and
    constraints[] for known-bad / unknown-pair / pass-cached results.

    Returns {verdict, proposed, image_checks, constraint_results, incompatible_hits}.
    Never mutates the lock — promotion requires a human PR with evidence.
    """
    lock = load_lock(stack_id)
    if lock is None:
        return {"verdict": "fail", "error": f"no lock file for {stack_id}",
                "proposed": proposed, "image_checks": [], "constraint_results": [],
                "incompatible_hits": []}
    upgrades = load_upgrades(stack_id) or {}
    cached_pairwise = {
        (entry["component_id"], entry["tag"]): entry.get("verdict")
        for entry in (upgrades.get("pairwise_tested") or [])
        if isinstance(entry, dict)
    }

    lock_by_id = {c["id"]: dict(c) for c in lock.get("components", [])}
    for cid, new_tag in proposed.items():
        if cid not in lock_by_id:
            return {"verdict": "fail", "error": f"unknown component '{cid}'",
                    "proposed": proposed, "image_checks": [], "constraint_results": [],
                    "incompatible_hits": []}
        lock_by_id[cid]["tag"] = new_tag

    # Registry precheck on the OVERLAY (not the original lock).
    image_checks: list[dict] = []
    if shutil.which("docker") is not None:
        async def _check_one_overlay(image: str, tag: str) -> dict:
            if not _safe_image_ref(image, tag):
                return {"image": image, "tag": tag, "status": "unknown",
                        "detail": "refused: image/tag does not match docker naming rules"}
            ref = f"{image}:{tag}"
            try:
                proc = await asyncio.create_subprocess_exec(
                    "docker", "manifest", "inspect", ref,
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                )
                stdout_b, stderr_b = await asyncio.wait_for(proc.communicate(), timeout=10)
                stdout = stdout_b.decode("utf-8", "replace")
                stderr = stderr_b.decode("utf-8", "replace")
                if proc.returncode == 0 and stdout.strip().startswith("{"):
                    return {"image": image, "tag": tag, "status": "available", "detail": None}
                err = (stderr or stdout).strip().splitlines()[0][:200] if (stderr or stdout) else ""
                if "no such manifest" in err.lower() or "not found" in err.lower():
                    return {"image": image, "tag": tag, "status": "missing", "detail": err}
                return {"image": image, "tag": tag, "status": "unknown", "detail": err}
            except Exception as e:
                return {"image": image, "tag": tag, "status": "unknown",
                        "detail": f"{type(e).__name__}: {e}"}
        tasks = []
        for cid in proposed.keys():
            comp = lock_by_id[cid]
            img, tg = comp.get("image"), comp.get("tag")
            if img and tg:
                tasks.append(_check_one_overlay(img, tg))
        image_checks = await asyncio.gather(*tasks) if tasks else []
    else:
        image_checks = [{"image": "?", "tag": "?", "status": "unknown",
                         "detail": "docker CLI not on PATH"}]

    # Walk known-incompatible combinations. Match by `component:tag` strings.
    incompatible_hits: list[dict] = []
    for entry in lock.get("incompatible", []) or []:
        combos = entry.get("combination", []) or []
        for combo in combos:
            if isinstance(combo, str) and ":" in combo:
                cid, tg = combo.split(":", 1)
                if proposed.get(cid) == tg:
                    incompatible_hits.append({
                        "matched": combo, "reason": entry.get("reason", ""),
                    })

    # Walk pairwise constraints. Mark each rule pass | unknown | pass-cached.
    proposed_ids = set(proposed.keys())
    constraint_results: list[dict] = []
    for c in lock.get("constraints", []) or []:
        pair = c.get("between", []) or []
        touches = bool(set(pair) & proposed_ids)
        if not touches:
            constraint_results.append({"between": pair, "rule": c.get("rule"), "status": "pass",
                                       "detail": "proposed change does not affect this pair"})
            continue
        # Check cache for any of the proposed entries that participate in this pair.
        cached_hit = False
        for cid in (set(pair) & proposed_ids):
            if (cid, proposed[cid]) in cached_pairwise:
                cached_hit = True
                break
        if cached_hit:
            constraint_results.append({"between": pair, "rule": c.get("rule"),
                                       "status": "pass-cached",
                                       "detail": "matches pairwise_tested entry"})
        else:
            constraint_results.append({"between": pair, "rule": c.get("rule"),
                                       "status": "unknown",
                                       "detail": "no cached evidence — needs manual validation"})

    if incompatible_hits or any(ck["status"] == "missing" for ck in image_checks):
        verdict = "fail"
    elif any(cr["status"] == "unknown" for cr in constraint_results):
        verdict = "unknown"
    elif any(ck["status"] == "unknown" for ck in image_checks):
        verdict = "unknown"
    else:
        verdict = "pass"

    return {
        "verdict": verdict,
        "proposed": proposed,
        "image_checks": image_checks,
        "constraint_results": constraint_results,
        "incompatible_hits": incompatible_hits,
    }


def lock_summary(stack_id: str) -> dict[str, Any] | None:
    """A small summary of the lock for surfacing in the UI / healthz."""
    lock = load_lock(stack_id)
    if not lock:
        return None
    return {
        "stack_id": stack_id,
        "version_id": lock.get("version_id"),
        "status": lock.get("status"),
        "certified_at": lock.get("certified_at"),
        "components_pinned": len(lock.get("components", [])),
        "constraints": len(lock.get("constraints", [])),
        "incompatible_combinations": len(lock.get("incompatible", [])),
    }
