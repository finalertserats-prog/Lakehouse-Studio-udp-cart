"""Lake-name generator.

Suggests an atmospheric name for the user's lakehouse. Names curated by
Gemini in a one-shot generation against a brief prompt: 1-2 words,
lowercase, evocative — water / cosmic / geological / mythological vibes,
plus minimal power-names a la Stripe/Linear/Vercel.
"""
from __future__ import annotations
import random
import re

NAMES: list[str] = [
    # water / oceanic
    "deepwater", "abyssal", "stillwater", "obsidian tide", "fathom",
    "riptide", "pelagic", "undertow", "tarn",
    # cosmic / celestial
    "pulsar", "nebula", "quasar", "void", "lunar sea",
    "astral", "zenith", "solaris", "equinox",
    # geological / earthen
    "basalt", "caldera", "tectonic", "moraine", "silica",
    "mantle", "rift", "bedrock", "monolith",
    # mythological
    "styx", "lethe", "aether", "hyperion", "nyx",
    "pontus", "yggdrasil", "jotun",
    # power names
    "nexus", "axiom", "flux", "vertex", "aura",
    "loom", "forge", "prism", "echo", "apex",
    "strata", "glacier", "trench", "halcyon", "borealis",
]

NAME_RE = re.compile(r"^[a-z0-9][a-z0-9 \-]{0,30}[a-z0-9]$")


def suggest(n: int = 1) -> list[str]:
    return random.sample(NAMES, k=min(n, len(NAMES)))


def is_valid(name: str) -> tuple[bool, str]:
    name = (name or "").strip().lower()
    if not name:
        return False, "name is required"
    if len(name) < 2:
        return False, "must be at least 2 characters"
    if len(name) > 32:
        return False, "must be at most 32 characters"
    if not NAME_RE.match(name):
        return False, "use lowercase letters, digits, spaces, or hyphens only"
    return True, ""


def normalize(name: str) -> str:
    return (name or "").strip().lower()
