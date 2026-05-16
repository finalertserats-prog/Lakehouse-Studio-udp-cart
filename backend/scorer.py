"""Stack score — 100 points across 5 dimensions.

Per the additions roadmap:
  Compatibility / Resource Fit / Production Readiness / Security / Operational Maturity

Scoring is deliberately conservative: a pilot stack with default creds and
no TLS scores around 60-70, not 100. That mismatch is the *point* — the
score should make it obvious when a stack isn't production-ready.

Scores depend on (stack manifest) AND optionally (chosen tier + chosen
VPS plan) so the same stack scores higher when a user picks the
"comfortable" tier than the "minimal" tier.
"""
from __future__ import annotations
from typing import Optional

from .sizer import size_stack
from .stack_manifest import StackManifest


def _score_compatibility(stack: StackManifest) -> tuple[int, str]:
    """Are all components pinned to known-good versions?"""
    if not stack.components:
        return 0, "stack defines no components"
    score = 100
    notes = []
    for c in stack.components:
        v = (c.get("version") or "").lower()
        if not v:
            score -= 15
            notes.append(f"{c['name']} has no version")
        elif v in ("latest", "main", "master") or v.endswith("-latest"):
            score -= 10
            notes.append(f"{c['name']} pinned to '{v}' (not a fixed version)")
    score = max(0, score)
    msg = "All components pinned to fixed versions" if not notes else "; ".join(notes[:3])
    return score, msg


def _score_resource_fit(stack: StackManifest, tier: str = "recommended") -> tuple[int, str]:
    """Does the chosen tier exceed the stated minimums by a comfortable margin?"""
    sizing = size_stack(stack) or {}
    if not sizing:
        return 0, "no resource profile data in stack manifest"
    if tier not in sizing:
        # fall back to any available tier
        tier = "recommended" if "recommended" in sizing else next(iter(sizing))
    totals = sizing[tier]["totals"]
    reqs = stack.requirements
    min_ram = float(reqs.get("minimum_ram_gb", 8))
    rec_ram = float(reqs.get("recommended_ram_gb", 16))
    min_cpu = int(reqs.get("minimum_cpu_cores", 4))
    rec_cpu = int(reqs.get("recommended_cpu_cores", 8))

    ram_ratio = totals["ram_gb"] / rec_ram if rec_ram else 1.0
    cpu_ratio = totals["cpu"] / rec_cpu if rec_cpu else 1.0

    # 100 if both ratios >= 1.0; degrade linearly as we approach the minimum.
    def _band(ratio: float) -> int:
        if ratio >= 1.5: return 100
        if ratio >= 1.0: return 90
        if ratio >= 0.75: return 70
        if ratio >= 0.5: return 40
        return 20

    score = (_band(ram_ratio) + _band(cpu_ratio)) // 2
    msg = (f"tier '{tier}': {totals['ram_gb']} GB / {totals['cpu']} cpu "
           f"vs recommended {rec_ram:.0f} GB / {rec_cpu} cpu")
    return score, msg


def _score_production_readiness(stack: StackManifest) -> tuple[int, str]:
    """Pilot stacks start at 60. Bumps would come from features that don't exist yet:
    TLS configuration, backup wiring, persistent external storage, monitoring stack.
    """
    maturity = (stack.data.get("maturity") or "pilot").lower()
    if maturity in ("certified", "production"):
        base = 90
        msg = "Certified/production stack with smoke evidence"
    elif maturity == "stable":
        base = 75
        msg = "Stable stack but not production-certified"
    else:
        base = 60
        msg = "Pilot/demo stack — not production-hardened"

    # Penalty: default storage is local Docker volumes
    if any(c.get("category") == "object_storage" and "minio" in c.get("id", "").lower() for c in stack.components):
        base -= 10
        msg += "; local MinIO (no managed object storage)"
    # Penalty: no TLS by default
    base -= 10
    msg += "; no TLS configured"
    return max(0, min(100, base)), msg


def _score_security(stack: StackManifest) -> tuple[int, str]:
    """Score based on default-secret posture, root accounts, exposed services."""
    score = 70  # baseline for a pilot stack
    notes = []

    defaults = stack.env_defaults
    # Default admin/admin or empty root password?
    if defaults.get("MINIO_ROOT_USER", "").lower() == "admin":
        score -= 10
        notes.append("MinIO uses default 'admin' user")
    if not defaults.get("STARROCKS_ROOT_PASSWORD"):
        score -= 15
        notes.append("StarRocks root has empty password")
    if defaults.get("MINIO_ROOT_PASSWORD", "").startswith("udp_"):
        score -= 5
        notes.append("MinIO uses well-known default password (UDP shipped)")

    # Ports exposed publicly by default
    public_ports = stack.required_ports
    if len(public_ports) >= 5:
        score -= 5
        notes.append(f"{len(public_ports)} ports exposed (no built-in firewall)")

    msg = "; ".join(notes[:3]) if notes else "Reasonable baseline (rotate defaults before exposing)"
    return max(0, min(100, score)), msg


def _score_operational_maturity(stack: StackManifest) -> tuple[int, str]:
    """How operable is this stack day-2?"""
    mode = stack.data.get("mode", "")
    commands = stack.data.get("commands", {})
    score = 50
    notes = []
    if "doctor" in commands: score += 10
    if "smoke" in commands or "smoke_test" in commands: score += 10
    if "status" in commands: score += 10
    if "stop" in commands: score += 5
    if "clean" in commands: score += 5
    if "backup" in commands: score += 10
    else: notes.append("no backup command")
    if "upgrade" in commands: score += 5
    else: notes.append("no upgrade command")
    if mode == "kubernetes":
        score += 5
    elif mode == "docker-compose":
        notes.append("docker-compose mode (single-host)")
    msg = "; ".join(notes[:3]) if notes else "Strong day-2 surface"
    return max(0, min(100, score)), msg


def score_stack(stack: StackManifest, tier: str = "recommended") -> dict:
    dims = [
        ("compatibility",       "Compatibility",         _score_compatibility(stack)),
        ("resource_fit",        "Resource Fit",          _score_resource_fit(stack, tier)),
        ("production_readiness","Production Readiness",  _score_production_readiness(stack)),
        ("security",            "Security",              _score_security(stack)),
        ("operational_maturity","Operational Maturity",  _score_operational_maturity(stack)),
    ]
    breakdown = []
    total = 0
    for key, label, (sc, msg) in dims:
        total += sc
        breakdown.append({"id": key, "label": label, "score": sc, "note": msg})
    overall = total // len(dims)
    grade = _grade(overall)
    return {
        "overall": overall,
        "grade": grade,
        "tier": tier,
        "breakdown": breakdown,
        "headline": _headline(overall, stack),
    }


def _grade(score: int) -> str:
    if score >= 90: return "A — production-ready"
    if score >= 80: return "B — pilot+"
    if score >= 70: return "C — pilot"
    if score >= 60: return "D — demo only"
    return "F — not recommended"


def _headline(score: int, stack: StackManifest) -> str:
    name = stack.name
    if score >= 90:
        return f"{name} is production-ready by current evidence."
    if score >= 80:
        return f"{name} is solid for pilot work and on track for production."
    if score >= 70:
        return f"{name} is good for pilots and demos. Production needs hardening (TLS, backups, secret rotation)."
    if score >= 60:
        return f"{name} works for local demos. Do not expose to untrusted networks without changes."
    return f"{name} has serious gaps for any non-throwaway use."
