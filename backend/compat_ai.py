"""AI-powered compatibility research using LiteLLM.

Given an anchor component + version and the full live version lists for all
other components, asks the LLM to return the best compatible version for each
component.  Results are cached in-memory per (anchor_id, anchor_version) pair
so repeated picks don't re-call the LLM.
"""
from __future__ import annotations

import json
import os
import re
import time
from typing import Any

from dotenv import load_dotenv

load_dotenv()  # reads .env from cwd / project root

import litellm

litellm.suppress_debug_info = True

_BASE_URL = os.environ.get("LITELLM_BASE_URL", "")
_API_KEY  = os.environ.get("LITELLM_API_KEY", "")
_MODEL    = os.environ.get("LITELLM_MODEL", "gpt-4o-mini")

# Cache: (anchor_id, anchor_version) → (timestamp, {comp_id: version})
_AI_CACHE: dict[tuple[str, str], tuple[float, dict[str, str]]] = {}
_AI_CACHE_TTL = 86400  # 24 hours — compat matrices don't change daily


def _cached_result(anchor_id: str, anchor_version: str) -> dict[str, str] | None:
    key = (anchor_id, anchor_version)
    entry = _AI_CACHE.get(key)
    if entry and (time.time() - entry[0]) < _AI_CACHE_TTL:
        return entry[1]
    return None


def _store_result(anchor_id: str, anchor_version: str, result: dict[str, str]) -> None:
    _AI_CACHE[(anchor_id, anchor_version)] = (time.time(), result)


def clear_ai_cache(anchor_id: str | None = None, anchor_version: str | None = None) -> None:
    if anchor_id and anchor_version:
        _AI_CACHE.pop((anchor_id, anchor_version), None)
    elif anchor_id:
        for k in list(_AI_CACHE.keys()):
            if k[0] == anchor_id:
                _AI_CACHE.pop(k, None)
    else:
        _AI_CACHE.clear()


def _build_prompt(anchor_id: str, anchor_version: str,
                  available_versions: dict[str, list[str]]) -> str:
    """Build the LLM prompt for compatibility research."""
    lines = [
        f"You are an expert in open-source data lakehouse technology compatibility.",
        f"",
        f"The user has selected: **{anchor_id}** version **{anchor_version}**.",
        f"",
        f"For each component listed below, choose the SINGLE best compatible version",
        f"based on official compatibility matrices, release notes, and known working",
        f"combinations. Pick the newest version that is known to work with {anchor_id} {anchor_version}.",
        f"If you are uncertain about a component, still pick the most likely compatible version.",
        f"",
        f"Available versions per component (newest first):",
    ]
    for comp_id, versions in sorted(available_versions.items()):
        if comp_id == anchor_id:
            continue
        if not versions:
            continue
        # Show up to 15 versions to keep prompt concise
        shown = versions[:15]
        lines.append(f"  {comp_id}: {', '.join(shown)}")

    lines += [
        "",
        "Return ONLY a valid JSON object — no markdown, no explanation.",
        "Format:",
        "{",
        '  "component_id": "chosen_version",',
        '  ...',
        "}",
        "",
        "Only include components for which you have a confident recommendation.",
        "Use the exact version strings from the lists above.",
    ]
    return "\n".join(lines)


def research_compat(
    anchor_id: str,
    anchor_version: str,
    available_versions: dict[str, list[str]],
    force_refresh: bool = False,
) -> dict[str, Any]:
    """Return AI-researched compatible versions for all components.

    Returns:
        {
          "anchor_id": str,
          "anchor_version": str,
          "compat": {"comp_id": "version", ...},
          "cached": bool,
          "error": str | None,
        }
    """
    if not force_refresh:
        cached = _cached_result(anchor_id, anchor_version)
        if cached is not None:
            return {
                "anchor_id": anchor_id,
                "anchor_version": anchor_version,
                "compat": cached,
                "cached": True,
                "error": None,
            }

    prompt = _build_prompt(anchor_id, anchor_version, available_versions)

    try:
        kwargs: dict[str, Any] = {
            "model": _MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.1,
            "max_tokens": 800,
        }
        if _BASE_URL:
            kwargs["base_url"] = _BASE_URL
        if _API_KEY:
            kwargs["api_key"] = _API_KEY

        response = litellm.completion(**kwargs)
        raw = response.choices[0].message.content.strip()

        # Strip markdown code fences if present
        raw = re.sub(r'^```(?:json)?\s*', '', raw)
        raw = re.sub(r'\s*```$', '', raw)

        compat: dict[str, str] = json.loads(raw)

        # Validate: every returned version must exist in the available list
        validated: dict[str, str] = {}
        for comp_id, version in compat.items():
            allowed = available_versions.get(comp_id, [])
            if version in allowed:
                validated[comp_id] = version
            else:
                # Best-effort: try to find the closest version in the list
                for v in allowed:
                    if v.startswith(version) or version.startswith(v.split(".")[0]):
                        validated[comp_id] = v
                        break

        _store_result(anchor_id, anchor_version, validated)
        return {
            "anchor_id": anchor_id,
            "anchor_version": anchor_version,
            "compat": validated,
            "cached": False,
            "error": None,
        }

    except Exception as exc:
        return {
            "anchor_id": anchor_id,
            "anchor_version": anchor_version,
            "compat": {},
            "cached": False,
            "error": str(exc),
        }
