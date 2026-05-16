"""Component catalog loader.

Reads stacks/components-catalog.yaml and surfaces categorized component
data to the UI. Static within a process — cached on first load.
"""
from __future__ import annotations
from functools import lru_cache
from pathlib import Path
from typing import Any
import yaml

from .config import ROOT


CATALOG_PATH = ROOT / "stacks" / "components-catalog.yaml"


@lru_cache(maxsize=1)
def load_catalog() -> dict[str, Any]:
    with CATALOG_PATH.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


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
