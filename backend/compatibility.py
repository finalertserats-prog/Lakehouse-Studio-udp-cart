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
