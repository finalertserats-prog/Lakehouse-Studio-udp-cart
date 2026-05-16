"""Component catalog loader.

Reads stacks/components-catalog.yaml and surfaces categorized component
data to the UI. Validated on FastAPI startup so a malformed catalog fails
loudly at boot rather than as a 500 on the first cart request.
"""
from __future__ import annotations
from functools import lru_cache
from pathlib import Path
from typing import Any
import yaml

from .config import ROOT


CATALOG_PATH = ROOT / "stacks" / "components-catalog.yaml"
_MAX_CATALOG_BYTES = 512 * 1024  # 512 KB ceiling — well above realistic catalog size


class CatalogError(ValueError):
    """Catalog YAML is malformed, missing required keys, or has bad references."""


@lru_cache(maxsize=1)
def load_catalog() -> dict[str, Any]:
    if not CATALOG_PATH.exists():
        raise CatalogError(f"catalog file not found: {CATALOG_PATH}")
    size = CATALOG_PATH.stat().st_size
    if size > _MAX_CATALOG_BYTES:
        raise CatalogError(
            f"catalog file too large: {size} bytes (max {_MAX_CATALOG_BYTES})"
        )
    with CATALOG_PATH.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if data is None:
        raise CatalogError("catalog file is empty")
    if not isinstance(data, dict):
        raise CatalogError(f"catalog root must be a mapping (got {type(data).__name__})")
    return data


def validate_catalog() -> list[str]:
    """Run all structural checks. Returns the list of problems found
    (empty list = OK). Called on FastAPI startup; never raises.
    """
    problems: list[str] = []
    try:
        data = load_catalog()
    except CatalogError as e:
        return [str(e)]
    except Exception as e:
        return [f"catalog load failed: {type(e).__name__}: {e}"]

    cats = data.get("categories")
    if not isinstance(cats, list) or not cats:
        problems.append("categories: must be a non-empty list")
        return problems

    seen_cat_ids: set[str] = set()
    seen_component_ids: set[str] = set()
    component_to_category: dict[str, str] = {}

    for i, cat in enumerate(cats):
        if not isinstance(cat, dict):
            problems.append(f"categories[{i}]: must be a mapping")
            continue
        cid = cat.get("id")
        if not isinstance(cid, str) or not cid:
            problems.append(f"categories[{i}]: missing 'id'")
            continue
        if cid in seen_cat_ids:
            problems.append(f"categories[{i}]: duplicate category id '{cid}'")
        seen_cat_ids.add(cid)
        if not isinstance(cat.get("label"), str) or not cat["label"]:
            problems.append(f"categories[{cid}]: missing 'label'")
        if not isinstance(cat.get("description", ""), str):
            problems.append(f"categories[{cid}]: 'description' must be a string")

        comps = cat.get("components") or []
        if not isinstance(comps, list):
            problems.append(f"categories[{cid}]: 'components' must be a list")
            continue
        for j, comp in enumerate(comps):
            if not isinstance(comp, dict):
                problems.append(f"categories[{cid}].components[{j}]: must be a mapping")
                continue
            comp_id = comp.get("id")
            if not isinstance(comp_id, str) or not comp_id:
                problems.append(f"categories[{cid}].components[{j}]: missing 'id'")
                continue
            if comp_id in seen_component_ids:
                problems.append(
                    f"duplicate component id '{comp_id}' (in categories '{component_to_category[comp_id]}' and '{cid}')"
                )
            seen_component_ids.add(comp_id)
            component_to_category[comp_id] = cid
            if not isinstance(comp.get("name"), str) or not comp["name"]:
                problems.append(f"component '{comp_id}': missing 'name'")
            if "version" in comp and not isinstance(comp["version"], (str, int, float)):
                problems.append(f"component '{comp_id}': 'version' must be scalar")

            # Cart-UX content fields. These are WARNINGS only — the catalog
            # still loads, install still proceeds. Surfaced via /healthz so
            # the team sees them before shipping a cart card with no logo.
            # Applies to certified (recommended/compatible) components only;
            # we don't warn about unfleshed coming_soon entries here.
            is_certified = bool(comp.get("recommended")) or bool(comp.get("compatible"))
            if is_certified:
                if not (isinstance(comp.get("logo"), str) and comp["logo"]):
                    problems.append(
                        f"component '{comp_id}': warning — missing 'logo' (cart UX will fall back to a placeholder)"
                    )
                if not (isinstance(comp.get("tagline"), str) and comp["tagline"]):
                    problems.append(
                        f"component '{comp_id}': warning — missing 'tagline' (cart card subtitle will be empty)"
                    )

    # Validate goals reference known recommended_sets
    goals = data.get("goals") or []
    rec_sets = data.get("recommended_sets") or {}
    for g in goals:
        if not isinstance(g, dict): continue
        gid = g.get("id", "<unknown>")
        rs = g.get("recommended_set")
        if rs and rs not in rec_sets:
            problems.append(f"goal '{gid}': recommended_set '{rs}' is not defined")

    # Validate recommended_sets reference known components
    for rs_id, rs in rec_sets.items():
        if not isinstance(rs, dict): continue
        for comp_id in rs.get("components", []) or []:
            if comp_id not in seen_component_ids:
                problems.append(
                    f"recommended_set '{rs_id}': references unknown component '{comp_id}'"
                )

    # Warn when a recommended_set points at a stack whose compatibility
    # lock is `status: candidate`. The set is allowed to surface in the
    # UI (we explicitly ship candidates for visibility) but operators
    # and /healthz need to see the non-certified status BEFORE someone
    # picks it for a real install. Import locally to avoid a startup
    # circular import — compatibility imports catalog via main.py.
    try:
        from .compatibility import load_lock  # local import
    except Exception:  # pragma: no cover — defensive
        load_lock = None
    if load_lock is not None:
        for rs_id, rs in rec_sets.items():
            if not isinstance(rs, dict): continue
            stack_id = rs.get("stack_id")
            if not isinstance(stack_id, str) or not stack_id:
                continue
            try:
                lock = load_lock(stack_id)
            except Exception as e:
                problems.append(
                    f"recommended_set '{rs_id}': warning — could not load lock for stack '{stack_id}' ({type(e).__name__}: {e})"
                )
                continue
            if lock is None:
                # No lock file at all — uncertified stack. Surface as warning.
                problems.append(
                    f"recommended_set '{rs_id}': warning — stack '{stack_id}' has no compatibility lock file (uncertified)"
                )
                continue
            status = lock.get("status")
            if status == "candidate":
                problems.append(
                    f"recommended_set '{rs_id}': warning — stack '{stack_id}' lock status is 'candidate' (not pilot-stable; no end-to-end install evidence yet)"
                )

    return problems


def categories() -> list[dict[str, Any]]:
    cat = load_catalog()
    out = list(cat.get("categories", []))
    out.sort(key=lambda c: c.get("order", 999))
    return out


def categories() -> list[dict[str, Any]]:
    cat = load_catalog()
    out = list(cat.get("categories", []))
    out.sort(key=lambda c: c.get("order", 999))
    return out


def goals() -> list[dict[str, Any]]:
    return list(load_catalog().get("goals", []))


def recommended_sets() -> dict[str, Any]:
    return dict(load_catalog().get("recommended_sets", {}))


def component_index() -> dict[str, dict[str, Any]]:
    """Map component_id → component dict (enriched with category_id)."""
    out: dict[str, dict[str, Any]] = {}
    for cat in categories():
        for c in cat.get("components", []) or []:
            out[c["id"]] = {**c, "category_id": cat["id"], "category_label": cat["label"]}
    return out


def required_category_ids() -> list[str]:
    return [c["id"] for c in categories() if not c.get("optional", False)]


def optional_category_ids() -> list[str]:
    return [c["id"] for c in categories() if c.get("optional", False)]
