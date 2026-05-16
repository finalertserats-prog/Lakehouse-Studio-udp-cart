"""Resource sizer.

Sums per-component resource_profiles across the stack and adds host overhead
(OS + Docker engine + headroom). Produces three tiers the user can pick from
on the "Pick a size" screen.

Inputs come from stacks/*.yaml `components[].resource_profiles.<tier>`.
"""
from __future__ import annotations
from typing import Literal

from .stack_manifest import StackManifest


Tier = Literal["minimal", "recommended", "comfortable"]
TIERS: tuple[Tier, ...] = ("minimal", "recommended", "comfortable")

# Host overhead added to the sum of component requests.
# OS + Docker engine + filesystem cache + network buffers + headroom.
_OVERHEAD = {
    "minimal":     {"cpu": 1, "ram_gb": 1, "disk_gb": 20},
    "recommended": {"cpu": 2, "ram_gb": 2, "disk_gb": 40},
    "comfortable": {"cpu": 2, "ram_gb": 4, "disk_gb": 60},
}

_TIER_META = {
    "minimal":     {"label": "Minimal", "blurb": "Smallest VPS that can run the stack end-to-end. Demo data only; expect slow queries on real workloads."},
    "recommended": {"label": "Recommended", "blurb": "Comfortable for the full pilot — install, smoke tests, demo queries, light experimentation."},
    "comfortable": {"label": "Comfortable", "blurb": "Room for actual workloads (10s of GB of data, multiple concurrent queries). Where to land for a small production proof-of-concept."},
}


def _zero():
    return {"cpu": 0, "ram_gb": 0, "disk_gb": 0}


def _add(a: dict, b: dict) -> dict:
    return {k: (a.get(k, 0) + b.get(k, 0)) for k in ("cpu", "ram_gb", "disk_gb")}


def size_stack(stack: StackManifest) -> dict:
    """Return {minimal,recommended,comfortable: {totals, components, overhead, label, blurb}}."""
    out: dict = {}
    for tier in TIERS:
        components = []
        sum_req = _zero()
        for c in stack.components:
            profile = (c.get("resource_profiles") or {}).get(tier) or {}
            req = {
                "cpu": int(profile.get("cpu", 0)),
                "ram_gb": int(profile.get("ram_gb", 0)),
                "disk_gb": int(profile.get("disk_gb", 0)),
            }
            components.append({"id": c["id"], "name": c["name"], **req})
            sum_req = _add(sum_req, req)
        overhead = _OVERHEAD[tier]
        totals = _add(sum_req, overhead)
        meta = _TIER_META[tier]
        out[tier] = {
            "label": meta["label"],
            "blurb": meta["blurb"],
            "components": components,
            "component_totals": sum_req,
            "overhead": overhead,
            "totals": totals,
        }
    return out


def fits(totals: dict, plan: dict) -> bool:
    """Does a VPS plan satisfy a totals requirement?"""
    return (
        plan["cpu"] >= totals["cpu"]
        and plan["ram_gb"] >= totals["ram_gb"]
        and plan["disk_gb"] >= totals["disk_gb"]
    )
