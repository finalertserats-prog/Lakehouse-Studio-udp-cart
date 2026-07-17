#!/usr/bin/env python3
"""Catalog integrity lint — a CI gate for the certification metadata layer.

Enforces the invariants that were violated in the v0.6.2 sync (an unlocked
enterprise-hadoop stack, a stack with no recommended_set, a recommended_set
that didn't mirror its certified lock). Run it in CI so this class of gap
can never ship again.

Checks (each a hard failure, exit code 1):
  1. Lock coverage      — every stack manifest has a stacks/compatibility/*.lock.yaml
  2. Rec-set coverage   — every docker-compose stack has a recommended_set whose
                          stack_id points back at it (remote-cluster stacks are
                          exempt: you don't "shop" a bare-metal cluster)
  3. Candidate contract — a lock is `status: candidate` iff its evidence[] is
                          empty; `pilot-stable` iff it has >=1 evidence record

Usage:
    python scripts/catalog_lint.py           # human-readable report
    python scripts/catalog_lint.py --quiet   # only print on failure
Exit code 0 = clean, 1 = one or more violations.
"""
from __future__ import annotations

import sys
from pathlib import Path

# Make `backend` importable when run from the repo root or anywhere.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backend import catalog as catalog_mod
from backend import compatibility as compatibility_mod
from backend import stack_manifest as stack_manifest_mod

# Stack `mode` values that install a local Docker stack — these are the ones a
# user assembles from a cart, so they need a recommended_set. Remote-cluster
# locks (no Docker) are exempt from the rec-set check.
_DOCKER_MODES = {"docker-compose", "docker"}


def _rec_set_stack_ids() -> set[str]:
    return {
        rs.get("stack_id")
        for rs in (catalog_mod.recommended_sets() or {}).values()
        if isinstance(rs, dict) and rs.get("stack_id")
    }


def check() -> list[str]:
    """Return a list of violation strings (empty = clean)."""
    problems: list[str] = []

    manifests = stack_manifest_mod.list_manifests()
    locks = set(compatibility_mod.list_locks())
    rec_stack_ids = _rec_set_stack_ids()

    # 1. Lock coverage.
    for m in manifests:
        if m.id not in locks:
            problems.append(f"[lock] stack '{m.id}' has no compatibility .lock.yaml")

    # 2. Recommended-set coverage for docker stacks.
    for m in manifests:
        mode = (m.data.get("mode") or "").strip()
        if mode in _DOCKER_MODES and m.id not in rec_stack_ids:
            problems.append(
                f"[recset] docker stack '{m.id}' is not referenced by any "
                f"recommended_set — the cart can't suggest it"
            )

    # 3. Candidate contract: status:candidate iff evidence empty.
    for stack_id in sorted(locks):
        try:
            lock = compatibility_mod.load_lock(stack_id)
        except Exception as exc:  # noqa: BLE001 - report, don't crash the lint
            problems.append(f"[lock] '{stack_id}' failed to load: {exc}")
            continue
        if not isinstance(lock, dict):
            continue
        status = lock.get("status")
        has_evidence = bool(lock.get("evidence"))
        if status == "candidate" and has_evidence:
            problems.append(
                f"[contract] '{stack_id}' is status:candidate but carries "
                f"evidence — promote it to pilot-stable"
            )
        if status == "pilot-stable" and not has_evidence:
            problems.append(
                f"[contract] '{stack_id}' is status:pilot-stable but has no "
                f"evidence[] record to justify the promotion"
            )

    return problems


def main(argv: list[str]) -> int:
    quiet = "--quiet" in argv
    problems = check()
    if problems:
        print("Catalog lint FAILED — {} problem(s):".format(len(problems)))
        for p in problems:
            print(f"  - {p}")
        return 1
    if not quiet:
        n = len(stack_manifest_mod.list_manifests())
        print(f"Catalog lint OK — {n} stacks, all locked + consistent.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
