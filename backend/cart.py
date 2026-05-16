"""Cart validator + live compatibility score.

Powers the "shop for components" UI. Given a list of component ids in the
cart, returns:
  - compatibility status (compatible / warning / incompatible)
  - live score (0-100; grows as required categories fill)
  - missing required categories
  - one-at-a-time recommendation (the next required pick)
  - warnings for any component not in the catalog or not in the UDP set

For the v0.3 pilot the only valid combination is the UDP recommended set.
"""
from __future__ import annotations
from typing import Optional

from .catalog import categories, required_category_ids, component_index, recommended_sets


# The pilot's known-good combination. Anything outside this is "experimental"
# in the warning sense — we can let the user *see* the alternates as
# coming-soon, but a cart with non-UDP components scores 0 for compatibility.
UDP_RECOMMENDED_SET: frozenset[str] = frozenset({
    "iceberg", "iceberg-rest", "minio", "spark-iceberg", "starrocks",
})


def validate_cart(cart: list[str]) -> dict:
    cart = list(dict.fromkeys(cart or []))  # dedupe, preserve order
    cart_set = set(cart)

    idx = component_index()
    required = required_category_ids()

    # Map: category_id → set of component_ids in cart that fill it
    by_category: dict[str, list[str]] = {}
    for cid in cart:
        comp = idx.get(cid)
        if comp is None:
            continue
        by_category.setdefault(comp["category_id"], []).append(cid)

    missing = [cat for cat in required if cat not in by_category]
    filled = len(required) - len(missing)
    score = int(round(100 * filled / max(1, len(required))))

    # Warnings
    warnings: list[str] = []
    unknown = [cid for cid in cart if cid not in idx]
    for cid in unknown:
        warnings.append(f"'{cid}' is not in the catalog")

    not_in_udp = [cid for cid in cart if cid in idx and cid not in UDP_RECOMMENDED_SET]
    for cid in not_in_udp:
        comp = idx.get(cid, {})
        warnings.append(f"'{comp.get('name', cid)}' is not part of the certified UDP set (v0.3 pilot ships UDP only)")

    # Multiple components in the same required category — flag
    for cat_id, items in by_category.items():
        if len(items) > 1:
            warnings.append(f"category '{cat_id}' has multiple components ({', '.join(items)}); pick one")

    # Recommendations: surface the next missing required category and suggest
    # the UDP component for it
    recommendations: list[dict] = []
    if missing:
        next_cat = missing[0]
        for cat in categories():
            if cat["id"] != next_cat:
                continue
            for comp in cat.get("components", []) or []:
                if comp.get("recommended") and comp["id"] in UDP_RECOMMENDED_SET:
                    recommendations.append({
                        "category": next_cat,
                        "category_label": cat.get("label", next_cat),
                        "component_id": comp["id"],
                        "component_name": comp["name"],
                        "reason": f"Recommended choice for {cat.get('label', next_cat)} in the UDP pilot stack",
                    })
                    break
            break

    # Compatibility verdict
    if unknown or not_in_udp:
        compatibility = "incompatible"
    elif missing or any(len(v) > 1 for v in by_category.values()):
        compatibility = "warning"
    else:
        compatibility = "compatible"

    # `valid` = "this cart is a strict subset of the UDP recommended set, with
    # no duplicates" — useful for the UI to know whether the cart is on a
    # happy path even if not complete yet.
    valid = (
        not unknown
        and not not_in_udp
        and all(len(v) <= 1 for v in by_category.values())
    )

    return {
        "valid": valid,
        "complete": not missing and not (unknown or not_in_udp),
        "score": score,
        "components_in_cart": cart,
        "missing_categories": missing,
        "recommendations": recommendations,
        "warnings": warnings,
        "compatibility": compatibility,
        "fills": {cat: ids for cat, ids in by_category.items()},
    }


def recommended_cart() -> list[str]:
    """Return the UDP recommended set, ordered by required category order."""
    idx = component_index()
    out: list[str] = []
    for cat in categories():
        for comp in cat.get("components", []) or []:
            if comp.get("recommended") and comp["id"] in UDP_RECOMMENDED_SET:
                out.append(comp["id"])
                break
    return out
