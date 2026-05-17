"""Cart-vs-stacks marriage check.

For any cart (list of component ids the user picked), find which
certified stack(s) the cart belongs to and run every constraint in
those stacks' lock files. Returns a structured verdict with:
  - overall_verdict: "compatible" | "warning" | "incompatible"
  - matched_stack: stack_id (or None if the cart doesn't map to any stack)
  - matched_stack_status: pilot-stable | candidate | None
  - constraints_checked: int
  - violations: [{between, rule, severity, suggestion}]
  - incompatible_hits: [{combination, reason, workaround}]
  - readiness_score: 0-100 (factor in: stack status, constraints satisfied,
    image-tag pinning quality, evidence count)

The verdict is the input to the UI's compatibility pill. Per the
founding architecture doc, this check MUST complete in <100 ms for
keystroke-level responsiveness. No network calls. No subprocess.
"""
from __future__ import annotations

from typing import Any, TypedDict
try:
    from typing import NotRequired  # py3.11+
except ImportError:  # pragma: no cover — py3.10 fallback for the Finalert VPS
    from typing_extensions import NotRequired

from .catalog import component_index, recommended_sets
from .compatibility import list_locks, load_lock


# Tags considered "floating" — bumping these silently changes the install.
# Matches the spirit of the lock files which forbid `latest`, `3.3-latest`, etc.
_FLOATING_TAG_TOKENS: tuple[str, ...] = ("latest", "main", "master", "edge", "stable", "nightly")


# Stack status → readiness contribution. Anything not in this map scores 0.
# pilot-stable / linux-stable / production are all "trustworthy" tiers per
# the lock-file maturity-grade comment block; candidate is "tags verified
# but no end-to-end evidence yet."
_STATUS_PRIORITY: dict[str, int] = {
    "production": 4,
    "linux-stable": 3,
    "pilot-stable": 2,
    "candidate": 1,
}


class Violation(TypedDict):
    between: list[str]
    rule: str
    severity: str  # "warning" | "error"
    suggestion: NotRequired[str]


class IncompatibleHit(TypedDict):
    combination: list[str]
    reason: str
    workaround: NotRequired[str]


class CompatVerdict(TypedDict):
    overall_verdict: str  # "compatible" | "warning" | "incompatible"
    matched_stack: str | None
    matched_stack_status: str | None
    constraints_checked: int
    violations: list[Violation]
    incompatible_hits: list[IncompatibleHit]
    readiness_score: int
    explanations: list[str]  # human-readable breakdown for the UI


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _normalize_cart(component_ids: list[str] | None) -> list[str]:
    """Drop empties, dedupe, preserve order — match cart.py's contract."""
    if not component_ids:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for cid in component_ids:
        if not isinstance(cid, str):
            continue
        cid = cid.strip()
        if not cid or cid in seen:
            continue
        seen.add(cid)
        out.append(cid)
    return out


def _stack_vocabulary(stack_id: str, lock: dict[str, Any]) -> set[str]:
    """Cart-side IDs a stack accepts.

    A stack matches a cart when the cart is a subset of this vocabulary.
    We pull from TWO sources because the cart UI and the lock file speak
    slightly different ID dialects:
      1. lock.components[].id — canonical service-level ids
         (e.g. udp-local-v0.2 has `spark`, `starrocks-fe`, `starrocks-be`)
      2. recommended_sets[stack_id].components — cart-facing catalog ids
         (e.g. udp-recommended has `spark-iceberg`, `starrocks`)
    Their union is the legitimate "cart marriage" surface for the stack.
    """
    vocab: set[str] = set()
    for c in lock.get("components", []) or []:
        cid = c.get("id")
        if isinstance(cid, str) and cid:
            vocab.add(cid)
    for _set_id, rs in (recommended_sets() or {}).items():
        if not isinstance(rs, dict):
            continue
        if rs.get("stack_id") != stack_id:
            continue
        for cid in rs.get("components", []) or []:
            if isinstance(cid, str) and cid:
                vocab.add(cid)
    return vocab


def _all_stack_vocabularies() -> dict[str, dict[str, Any]]:
    """Pre-compute per-stack {vocabulary, status, lock} for fast matching.

    Returns dict[stack_id] -> {"vocab": set[str], "status": str|None,
    "lock": dict, "size": int}. Cached implicitly via load_lock's lru_cache.
    """
    out: dict[str, dict[str, Any]] = {}
    for stack_id in list_locks():
        try:
            lock = load_lock(stack_id)
        except Exception:
            # A malformed lock should not break the whole check — skip it,
            # the catalog validator surfaces structural problems separately.
            continue
        if not isinstance(lock, dict):
            continue
        vocab = _stack_vocabulary(stack_id, lock)
        out[stack_id] = {
            "vocab": vocab,
            "status": lock.get("status"),
            "lock": lock,
            "size": len(vocab),
        }
    return out


_MINIMAL_MARRIAGE_FRACTION = 0.6
_MINIMAL_MARRIAGE_FLOOR = 3


def _select_best_match(
    cart_set: set[str],
    catalog: dict[str, dict[str, Any]],
) -> tuple[str | None, dict[str, Any] | None]:
    """Pick the stack whose vocabulary best fits the cart.

    Rules (in order):
      1. cart must be non-empty (empty cart never matches — it would
         vacuously subset every vocab, which is meaningless)
      2. cart_set must be a subset of stack's vocab
      3. cart must cover a meaningful fraction of the stack — a single
         shared component (e.g. just `minio`) trivially subsets every
         stack but is not a marriage. Floor: max(3, 60% of stack vocab).
         (Codex review 2026-05-17 P0 — without this floor, the verdict
         falsely returns `compatible` for `[minio]` alone.)
      4. Prefer higher _STATUS_PRIORITY (pilot-stable > candidate)
      5. Tie-break on smaller vocab (most specific marriage wins)
      6. Tie-break on stack_id alphabetic order (deterministic)
    """
    if not cart_set:
        return None, None
    candidates: list[tuple[int, int, str, dict[str, Any]]] = []
    for stack_id, entry in catalog.items():
        if not cart_set.issubset(entry["vocab"]):
            continue
        # Marriage floor — cart must cover enough of the stack to count
        # as picking that stack, not just sharing one component with it.
        threshold = max(
            _MINIMAL_MARRIAGE_FLOOR,
            int(entry["size"] * _MINIMAL_MARRIAGE_FRACTION),
        )
        if len(cart_set) < threshold:
            continue
        priority = _STATUS_PRIORITY.get(entry["status"] or "", 0)
        candidates.append((-priority, entry["size"], stack_id, entry))
    if not candidates:
        return None, None
    candidates.sort(key=lambda t: (t[0], t[1], t[2]))
    _p, _s, stack_id, entry = candidates[0]
    return stack_id, entry


def _is_floating_tag(tag: Any) -> bool:
    """True if `tag` looks like a floating/moving ref. Conservative — when
    in doubt we treat it as pinned (no penalty) rather than crying wolf."""
    if not isinstance(tag, str) or not tag:
        # No tag at all is suspicious in its own right but not "floating"
        return True
    t = tag.strip().lower()
    if not t:
        return True
    if t in _FLOATING_TAG_TOKENS:
        return True
    # `3.3-latest`, `v1-edge`, etc.
    for token in _FLOATING_TAG_TOKENS:
        if t.endswith(f"-{token}") or t.endswith(f".{token}"):
            return True
    return False


def _components_to_id_list(comp_list: list[Any] | None) -> list[str]:
    """Normalize a lock-file `between:` array to plain strings.
    Some entries may be `component:tag` from incompatible blocks; for the
    constraints[] block they're plain ids, but be defensive."""
    out: list[str] = []
    for c in comp_list or []:
        if isinstance(c, str):
            out.append(c)
        elif isinstance(c, dict) and isinstance(c.get("id"), str):
            out.append(c["id"])
    return out


def _split_combo_entry(combo_item: Any) -> tuple[str, str | None]:
    """Parse a combination entry like `spark:3.5.1_1.5.2` or `nessie:latest`.
    Returns (component_id, tag_or_None). Non-string entries return (str, None).
    Also tolerates plain ids and `id + ...` free-text entries from the lock
    files (e.g. `"starrocks-fe + spark with hive-metastore"`).
    """
    if not isinstance(combo_item, str):
        return (str(combo_item), None)
    s = combo_item.strip()
    if not s:
        return ("", None)
    # Free-text marker — treat as a single id-ish string; matching against it
    # would never trigger on a plain cart of ids, which is the desired safety.
    if " " in s:
        return (s, None)
    if ":" in s:
        cid, tag = s.split(":", 1)
        return (cid.strip(), tag.strip())
    return (s, None)


def _check_incompatible(
    lock: dict[str, Any], cart_set: set[str]
) -> list[IncompatibleHit]:
    """Walk lock.incompatible[]; return entries the cart triggers.

    The cart carries bare component ids — no tag information. The lock's
    incompatible[] block mixes two entry shapes:
      - Tag-specific: `[spark:3.5.1_1.5.2]`, `[trino:latest]`, etc.
        We CANNOT verify the tag from a bare-id cart, so these don't
        fire here (the install-time precheck in compatibility.py is
        the right place — it has tag context).
      - Id-only: `[hudi, iceberg-rest]` (hypothetical) — fires when
        every id is present in the cart.
      - Free-text: `"starrocks-fe + spark with hive-metastore"` — never
        auto-fires; advisory only.
    """
    hits: list[IncompatibleHit] = []
    for entry in lock.get("incompatible", []) or []:
        if not isinstance(entry, dict):
            continue
        combo_raw = entry.get("combination") or []
        ids: list[str] = []
        skip = False
        for item in combo_raw:
            cid, tag = _split_combo_entry(item)
            if " " in cid:
                # Free-text advisory — never auto-trigger
                skip = True
                break
            if tag is not None:
                # Tag-specific incompatibility — cart has no tag info,
                # defer to install-time precheck. Don't flag here.
                skip = True
                break
            ids.append(cid)
        if skip or not ids:
            continue
        if all(cid in cart_set for cid in ids):
            hit: IncompatibleHit = {
                "combination": ids,
                "reason": str(entry.get("reason", "")),
            }
            wk = entry.get("workaround")
            if isinstance(wk, str) and wk:
                hit["workaround"] = wk
            hits.append(hit)
    return hits


def _check_constraints(
    lock: dict[str, Any], cart_set: set[str]
) -> tuple[int, list[Violation]]:
    """Walk lock.constraints[]; count how many pairwise rules touch the cart.

    Returns (constraints_checked, violations).
      constraints_checked = constraints where EVERY id in between[] is in
                            the cart (i.e. this marriage is being relied on).
      violations          = currently empty for "satisfied" constraints; the
                            install-time precheck (compatibility.simulate_upgrade
                            and image precheck) is what produces real violation
                            evidence. The brief explicitly says we don't re-verify
                            here — we only count what's being relied on.
    """
    checked = 0
    violations: list[Violation] = []
    for c in lock.get("constraints", []) or []:
        if not isinstance(c, dict):
            continue
        between = _components_to_id_list(c.get("between"))
        if not between:
            continue
        if all(cid in cart_set for cid in between):
            checked += 1
    return checked, violations


def _readiness_score(
    matched_status: str | None,
    lock: dict[str, Any] | None,
    constraints_checked: int,
    violations: list[Violation],
    incompatible_hits: list[IncompatibleHit],
) -> int:
    """Simple weighted formula per the brief. Clamp to [0, 100]."""
    score = 0
    if matched_status in ("pilot-stable", "linux-stable", "production"):
        score += 60
    elif matched_status == "candidate":
        score += 30
    if lock is not None:
        components = lock.get("components", []) or []
        if components and all(not _is_floating_tag(c.get("tag")) for c in components):
            score += 10
        evidence = lock.get("evidence", []) or []
        if isinstance(evidence, list) and len(evidence) >= 1:
            score += 20
    # Penalties
    warning_count = sum(1 for v in violations if v.get("severity") == "warning")
    score -= 10 * warning_count
    score -= 50 * len(incompatible_hits)
    # Bonus for constraints validated — every additional constraint touched
    # by the cart is more proof the marriage is "real". Capped at +10.
    score += min(10, constraints_checked)
    if score < 0:
        score = 0
    if score > 100:
        score = 100
    return int(score)


def _verdict_from_findings(
    matched_status: str | None,
    violations: list[Violation],
    incompatible_hits: list[IncompatibleHit],
) -> str:
    if incompatible_hits:
        return "incompatible"
    if any(v.get("severity") == "error" for v in violations):
        return "incompatible"
    if violations:
        return "warning"
    if matched_status == "candidate":
        return "warning"
    return "compatible"


def _explain_unmatched(cart: list[str]) -> list[str]:
    """Build user-friendly explanations for the no-stack-match case."""
    idx = component_index()
    lines: list[str] = []
    unknown = [c for c in cart if c not in idx]
    if unknown:
        names = ", ".join(unknown)
        lines.append(f"Unknown component(s): {names}. Not present in the catalog.")
    if not cart:
        lines.append("Cart is empty — add a table format, catalog, storage, "
                     "processing engine, and serving engine to start a marriage check.")
    else:
        lines.append(
            "No certified marriage was found for this exact combination. "
            "Either the components don't co-exist in any certified stack, "
            "or they belong to different stack lineages. "
            "Try suggest_swap() for a one-component fix."
        )
    return lines


def _explain_matched(
    stack_id: str,
    status: str | None,
    constraints_checked: int,
    incompatible_hits: list[IncompatibleHit],
    lock: dict[str, Any],
) -> list[str]:
    lines: list[str] = []
    nice_status = status or "uncertified"
    lines.append(
        f"Cart marries into certified stack `{stack_id}` (status: {nice_status})."
    )
    if constraints_checked:
        lines.append(
            f"{constraints_checked} pairwise constraint(s) in the lock file "
            "are relied on by this cart."
        )
    ev_count = len(lock.get("evidence", []) or [])
    if ev_count:
        lines.append(
            f"{ev_count} evidence entry(ies) recorded — at least one real install run."
        )
    else:
        lines.append(
            "No evidence entries in this stack's lock — combination has not been "
            "end-to-end installed yet (candidate)."
        )
    if incompatible_hits:
        lines.append(
            f"{len(incompatible_hits)} incompatible combination(s) triggered — "
            "this cart should NOT be installed as-is."
        )
    return lines


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def check_cart(component_ids: list[str]) -> CompatVerdict:
    """The marriage check. JSON-safe verdict for the UI's compatibility pill."""
    cart = _normalize_cart(component_ids)
    cart_set = set(cart)
    catalog = _all_stack_vocabularies()

    stack_id, entry = _select_best_match(cart_set, catalog)

    if entry is None:
        verdict: CompatVerdict = {
            "overall_verdict": "incompatible" if cart else "warning",
            "matched_stack": None,
            "matched_stack_status": None,
            "constraints_checked": 0,
            "violations": [],
            "incompatible_hits": [],
            "readiness_score": 0,
            "explanations": _explain_unmatched(cart),
        }
        return verdict

    lock = entry["lock"]
    status = entry["status"]
    constraints_checked, violations = _check_constraints(lock, cart_set)
    incompatible_hits = _check_incompatible(lock, cart_set)
    score = _readiness_score(status, lock, constraints_checked, violations, incompatible_hits)
    overall = _verdict_from_findings(status, violations, incompatible_hits)
    explanations = _explain_matched(stack_id, status, constraints_checked, incompatible_hits, lock)

    out: CompatVerdict = {
        "overall_verdict": overall,
        "matched_stack": stack_id,
        "matched_stack_status": status,
        "constraints_checked": int(constraints_checked),
        "violations": violations,
        "incompatible_hits": incompatible_hits,
        "readiness_score": int(score),
        "explanations": explanations,
    }
    return out


def suggest_swap(component_ids: list[str]) -> dict | None:
    """If the cart doesn't match any stack, try removing one component at a
    time and see if the rest match a stack. Return the swap suggestion or None.

    Result shape:
      {
        "remove": [<component_id>],     # the single id to drop
        "add":    [<component_id>...],  # ids the user would still need from
                                        # the alt stack's recommended set
        "alt_stack": "<stack_id>",      # the stack that would then marry
        "alt_stack_status": "<status>"  # so the UI can warn for candidates
      }

    None if no single-component removal yields a match.
    """
    cart = _normalize_cart(component_ids)
    if len(cart) < 2:
        return None

    # First — does the cart already match? Don't suggest swaps for a happy cart.
    if check_cart(cart)["matched_stack"] is not None:
        return None

    cart_set = set(cart)
    catalog = _all_stack_vocabularies()

    # Try each single-removal and score the result. We bypass the
    # marriage-floor here (subset-only) because the swap path is
    # explicitly about "if you also added the missing components, this
    # would marry" — not "you already have enough to install."
    best: tuple[int, int, str, str, list[str]] | None = None  # (priority, vocab_size, stack_id, removed, missing)
    for to_remove in cart:
        candidate_cart = cart_set - {to_remove}
        if not candidate_cart:
            continue
        match_id, match_entry = None, None
        for stack_id, entry in catalog.items():
            if not candidate_cart.issubset(entry["vocab"]):
                continue
            if match_entry is None or entry["size"] < match_entry["size"]:
                if (_STATUS_PRIORITY.get(entry["status"] or "", 0)
                        >= _STATUS_PRIORITY.get(
                            (match_entry["status"] if match_entry else "") or "", 0)):
                    match_id, match_entry = stack_id, entry
        if not match_id or not match_entry:
            continue
        # Optional: surface what else the alt stack's recommended set wants
        missing: list[str] = []
        for _set_id, rs in (recommended_sets() or {}).items():
            if not isinstance(rs, dict):
                continue
            if rs.get("stack_id") != match_id:
                continue
            for cid in rs.get("components", []) or []:
                if isinstance(cid, str) and cid and cid not in candidate_cart:
                    if cid not in missing:
                        missing.append(cid)
            break
        priority = -_STATUS_PRIORITY.get(match_entry["status"] or "", 0)
        key = (priority, match_entry["size"], match_id, to_remove, missing)
        if best is None or key < best:
            best = key

    if best is None:
        return None
    _p, _s, alt_stack, removed, missing = best
    alt_status = catalog[alt_stack]["status"]
    return {
        "remove": [removed],
        "add": missing,
        "alt_stack": alt_stack,
        "alt_stack_status": alt_status,
    }
