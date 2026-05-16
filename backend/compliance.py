"""Compliance framing loader.

Reads stacks/compliance.yaml. Every entry MUST have a disclaimer that
makes clear this is reference framing, NOT certification — the loader
rejects entries that omit the disclaimer.

Templates reference these entries by tag (e.g. `compliance_tags: [hipaa]`).
The UI lazy-loads one entry at a time so the long-form prose doesn't
bloat the template list payload.
"""
from __future__ import annotations
from functools import lru_cache
from pathlib import Path
from typing import Any
import yaml

from .config import ROOT


COMPLIANCE_PATH = ROOT / "stacks" / "compliance.yaml"
_MAX_BYTES = 256 * 1024


class ComplianceError(ValueError):
    """compliance.yaml is malformed, missing required keys, or absent."""


@lru_cache(maxsize=1)
def load_compliance() -> dict[str, Any]:
    if not COMPLIANCE_PATH.exists():
        # Compliance content is optional — return empty registry, not an error.
        return {"compliance": {}}
    size = COMPLIANCE_PATH.stat().st_size
    if size > _MAX_BYTES:
        raise ComplianceError(f"compliance.yaml too large: {size} bytes (max {_MAX_BYTES})")
    with COMPLIANCE_PATH.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if data is None:
        return {"compliance": {}}
    if not isinstance(data, dict):
        raise ComplianceError("compliance.yaml root must be a mapping")
    return data


def validate_compliance() -> list[str]:
    """Structural + policy checks. Returns a list of problems (empty = OK).
    Most important policy check: every entry must have a `disclaimer`.
    """
    problems: list[str] = []
    try:
        data = load_compliance()
    except ComplianceError as e:
        return [str(e)]
    except Exception as e:
        return [f"compliance load failed: {type(e).__name__}: {e}"]

    entries = data.get("compliance", {})
    if not isinstance(entries, dict):
        return ["compliance: root key must be a mapping of tag → entry"]
    for tag, entry in entries.items():
        if not isinstance(entry, dict):
            problems.append(f"compliance[{tag}]: must be a mapping")
            continue
        if not isinstance(entry.get("disclaimer", ""), str) or not entry.get("disclaimer", "").strip():
            problems.append(
                f"compliance[{tag}]: MUST have a non-empty 'disclaimer' field — every regime needs one"
            )
        if not isinstance(entry.get("label", ""), str):
            problems.append(f"compliance[{tag}]: 'label' must be a string")
    return problems


def known_tags() -> set[str]:
    return set(load_compliance().get("compliance", {}).keys())


def get_compliance(tag: str) -> dict[str, Any] | None:
    return load_compliance().get("compliance", {}).get(tag)
