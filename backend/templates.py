"""Templates — use-case-shaped views over recommended_sets.

PURE PRESENTATION LAYER. Templates compose existing recommended_sets +
display-only pending_components + compliance framing. They never feed
the install pipeline directly — that always runs off recommended_sets
and the certified lock files.

Constraint: pending_components are display strings, never component ids.
The validator below rejects any pending entry whose `name` would
collide with a real catalog component id (prevents accidental cart
pollution if someone tries to wire them into validate_cart later).
"""
from __future__ import annotations
from typing import Any

from .catalog import component_index, load_catalog, recommended_sets
from .compliance import get_compliance, known_tags


_ALLOWED_READINESS = frozenset({"pilot", "preview", "ga"})


def load_templates() -> list[dict[str, Any]]:
    return list(load_catalog().get("templates", []) or [])


def list_templates() -> list[dict[str, Any]]:
    """Lightweight template list — id, label, icon, pitch, readiness, tags.
    For the picker grid; not the full detail payload.
    """
    out: list[dict[str, Any]] = []
    for t in load_templates():
        out.append({
            "id": t.get("id"),
            "label": t.get("label"),
            "icon": t.get("icon"),
            "elevator_pitch": t.get("elevator_pitch"),
            "persona": t.get("persona"),
            "readiness": t.get("readiness", "pilot"),
            "compliance_tags": list(t.get("compliance_tags") or []),
            "pending_count": len(t.get("pending_components") or []),
        })
    return out


def get_template_detail(template_id: str) -> dict[str, Any] | None:
    """Full payload for one template: merged certified cart + pending list +
    resolved compliance entries. Validates that the referenced
    recommended_set exists; returns None if the template id is unknown.
    """
    tpl = next((t for t in load_templates() if t.get("id") == template_id), None)
    if tpl is None:
        return None

    rs_id = tpl.get("recommended_set")
    rec_sets = recommended_sets()
    rs = rec_sets.get(rs_id) or {}
    cart_ids = list(rs.get("components") or [])

    # Resolve pending entries — display-only; do NOT cross-check against catalog ids.
    pending = []
    for p in tpl.get("pending_components") or []:
        pending.append({
            "name": p.get("name"),
            "role": p.get("role"),
            "eta": p.get("eta"),
            "reason": p.get("reason"),
        })

    compliance_blocks: list[dict[str, Any]] = []
    for tag in tpl.get("compliance_tags") or []:
        entry = get_compliance(tag)
        if entry is not None:
            compliance_blocks.append({"tag": tag, **entry})

    return {
        "id": tpl.get("id"),
        "label": tpl.get("label"),
        "icon": tpl.get("icon"),
        "elevator_pitch": tpl.get("elevator_pitch"),
        "persona": tpl.get("persona"),
        "intended_for": list(tpl.get("intended_for") or []),
        "anti_use_cases": list(tpl.get("anti_use_cases") or []),
        "hero_use_cases": list(tpl.get("hero_use_cases") or []),
        "readiness": tpl.get("readiness", "pilot"),
        "recommended_set": rs_id,
        "stack_id": rs.get("stack_id"),
        "cart": cart_ids,
        "pending": pending,
        "compliance": compliance_blocks,
        # Installability: only pilot / ga templates expose Install. preview is gated.
        "installable": tpl.get("readiness", "pilot") in {"pilot", "ga"},
    }


def validate_templates() -> list[str]:
    """Structural checks. Returns problems list (empty = OK).
    Enforces the anti-patterns from the design doc:
    - pending name MUST NOT collide with a real catalog component id
    - referenced recommended_set MUST exist
    - referenced compliance tag MUST resolve
    - readiness MUST be pilot | preview | ga
    """
    problems: list[str] = []
    try:
        tpls = load_templates()
    except Exception as e:
        return [f"templates load failed: {type(e).__name__}: {e}"]
    if not tpls:
        return problems  # No templates is fine; the picker just shows empty state.

    cat_ids = set(component_index().keys())
    rs_ids = set(recommended_sets().keys())
    comp_tags = known_tags()
    seen_ids: set[str] = set()

    for i, t in enumerate(tpls):
        if not isinstance(t, dict):
            problems.append(f"templates[{i}]: must be a mapping")
            continue
        tid = t.get("id")
        if not isinstance(tid, str) or not tid:
            problems.append(f"templates[{i}]: missing 'id'")
            continue
        if tid in seen_ids:
            problems.append(f"templates[{i}]: duplicate template id '{tid}'")
        seen_ids.add(tid)

        if not isinstance(t.get("label"), str):
            problems.append(f"template '{tid}': missing 'label'")
        if not isinstance(t.get("elevator_pitch"), str):
            problems.append(f"template '{tid}': missing 'elevator_pitch'")

        rs = t.get("recommended_set")
        if rs and rs not in rs_ids:
            problems.append(f"template '{tid}': recommended_set '{rs}' does not exist")

        readiness = t.get("readiness", "pilot")
        if readiness not in _ALLOWED_READINESS:
            problems.append(
                f"template '{tid}': readiness '{readiness}' not in {sorted(_ALLOWED_READINESS)}"
            )

        for tag in t.get("compliance_tags") or []:
            if tag not in comp_tags:
                problems.append(
                    f"template '{tid}': compliance_tag '{tag}' has no entry in compliance.yaml"
                )

        for j, p in enumerate(t.get("pending_components") or []):
            if not isinstance(p, dict):
                problems.append(f"template '{tid}'.pending[{j}]: must be a mapping")
                continue
            name = p.get("name")
            if not isinstance(name, str) or not name:
                problems.append(f"template '{tid}'.pending[{j}]: missing 'name'")
                continue
            # The anti-pattern guard from the design: pending names cannot
            # accidentally introduce a real component id into the cart.
            # We compare a lowercased slug-ish form.
            slug = name.lower().replace(" ", "-").replace("(", "").replace(")", "").replace("/", "-")
            if slug in cat_ids:
                problems.append(
                    f"template '{tid}'.pending[{j}]: name '{name}' collides with catalog "
                    f"component id '{slug}' — would risk cart pollution"
                )

    return problems
