"""Cart compatibility explainer — the product promise made executable.

Given any cart of component ids the user picked, this module returns a
**plain-English explanation** of whether the combination will install,
won't install, or has never been tested end-to-end — with version-level
evidence pulled from the lock files and component catalog.

The verdict surface intentionally collapses to three buckets so the UI
can render a pill (green / red / amber):

  will_work   — cart matches a certified stack with source-verified
                marriages spanning every component and no known
                incompatibilities. Show the constraint chain as evidence.
  wont_work   — cart triggers a known-incompatible combination in any
                lock OR pairs of cart components have no compatible_with
                overlap per the catalog (e.g. hudi + iceberg-rest, where
                Hudi has no Iceberg-REST client). Be specific: name the
                bad pair, the source, the workaround.
  untested    — cart looks plausible (every pair has at least one
                compatible_with overlap) but no certified stack covers
                this exact combination AND no incompatible[] hits exist.
                Surface what's known per-pair and suggest authoring an
                evidence record.

No network. No subprocess. The lock files + components-catalog.yaml
are read via the existing lru_cache'd loaders so steady-state cost is
well under the 100 ms keystroke-latency target.

This module is read-only against the lock files; promotion still
requires a human PR with evidence (same contract as compatibility.py).
"""
from __future__ import annotations

from typing import Any, TypedDict
try:
    from typing import NotRequired  # py3.11+
except ImportError:  # pragma: no cover — py3.10 fallback for the Finalert VPS
    from typing_extensions import NotRequired

from .catalog import (
    component_index,
    recommended_sets,
    required_category_ids,
    optional_category_ids,
)
from .compatibility import list_locks, load_lock


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


class Reason(TypedDict):
    """One line of plain-English evidence for the verdict.

    `kind` is the discriminator the UI uses to pick an icon / colour:
      - marriage_proven      : a lock-file constraint chain spans this pair
      - marriage_unknown     : pair looks compatible per the catalog but
                               no certified stack pins it; awaiting evidence
      - known_incompatible   : an incompatible[] hit or a catalog-level
                               compatible_with mismatch — won't work
      - needs_auxiliary      : the cart names a component whose role demands
                               a backing service that isn't in the cart
                               (e.g. HMS wants Postgres)
      - missing_category     : a required category (table format, catalog,
                               storage, processing, serving) is empty
    """
    kind: str
    between: NotRequired[list[str]]
    explanation: str
    source: NotRequired[str]
    missing: NotRequired[str]
    for_: NotRequired[str]  # python keyword; serialized as "for" by `_dump`


class Swap(TypedDict):
    remove: list[str]
    add: list[str]


class AlternativeCart(TypedDict):
    swap: Swap
    result: str


class CartExplanation(TypedDict):
    verdict: str  # "will_work" | "wont_work" | "untested"
    headline: str
    reasons: list[dict]  # JSON-serialised Reason entries (with "for" key)
    matched_stack: str | None
    matched_stack_status: str | None
    graduation_path: str
    missing_components: list[str]
    extra_components: list[str]
    alternative_carts: list[AlternativeCart]


# ---------------------------------------------------------------------------
# Module-level constants — small dictionaries tied to the catalog's reality.
# ---------------------------------------------------------------------------


# Components whose role implicitly requires a backing relational store.
# Driven from the lock files' real shape:
#   - hive-metastore needs Postgres (Hudi-HMS-Spark + Delta-HMS-Spark-Trino
#     candidate locks both pin postgres:15-alpine)
#   - polaris needs Postgres (iceberg-polaris-spark-local-v0.1 lock)
# Map id -> (auxiliary_id, plain-english reason).
_NEEDS_AUXILIARY: dict[str, tuple[str, str]] = {
    "hive-metastore": (
        "postgres",
        "Hive Metastore is a stateless Thrift service — it needs a "
        "relational backing DB for its schema (DBS, TBLS, SDS, PARTITIONS). "
        "Postgres 15-alpine is the version every candidate HMS lock pins.",
    ),
    "polaris": (
        "postgres",
        "Polaris's persistence layer holds catalogs, principals, and grants "
        "in a relational DB. The lock pins postgres:15-alpine.",
    ),
}


# Status → verdict bias. Anything in this set is "trustworthy enough to call
# will_work" when the cart matches a stack with one of these statuses.
_TRUSTWORTHY_STATUSES: frozenset[str] = frozenset({
    "pilot-stable", "linux-stable", "production",
})


# Component IDs that the UI cart treats as a "format-only" pick — they have
# no runtime and never need an auxiliary; their compatible_with array names
# the engines/catalogs they work with, not the storage they sit on.
_FORMAT_ONLY_COMPONENTS: frozenset[str] = frozenset({"iceberg", "hudi", "delta"})


# ---------------------------------------------------------------------------
# Helpers — cart normalisation + stack matching
# ---------------------------------------------------------------------------


def _normalize_cart(component_ids: list[str] | None) -> list[str]:
    """Drop falsy entries, dedupe, preserve order."""
    if not component_ids:
        return []
    seen: set[str] = set()
    out: list[str] = []
    for cid in component_ids:
        if not isinstance(cid, str):
            continue
        c = cid.strip()
        if not c or c in seen:
            continue
        seen.add(c)
        out.append(c)
    return out


def _stack_vocabulary(stack_id: str, lock: dict[str, Any]) -> set[str]:
    """Cart-side IDs a stack accepts.

    Matches the dialect-bridging logic in compat_check._stack_vocabulary:
    the cart UI uses catalog ids (e.g. `starrocks`), the lock file uses
    service-level ids (`starrocks-fe`, `starrocks-be`). Their union is
    the legitimate marriage surface.
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


def _all_stack_catalog() -> dict[str, dict[str, Any]]:
    """Per-stack {vocab, status, lock, recommended_set_components}.

    Cached implicitly via load_lock's lru_cache + recommended_sets's
    underlying load_catalog cache.
    """
    out: dict[str, dict[str, Any]] = {}
    rsets = recommended_sets() or {}
    rs_by_stack: dict[str, list[str]] = {}
    for _id, rs in rsets.items():
        if not isinstance(rs, dict):
            continue
        sid = rs.get("stack_id")
        if isinstance(sid, str) and sid:
            rs_by_stack[sid] = [
                c for c in (rs.get("components") or []) if isinstance(c, str)
            ]
    for stack_id in list_locks():
        try:
            lock = load_lock(stack_id)
        except Exception:
            continue
        if not isinstance(lock, dict):
            continue
        out[stack_id] = {
            "vocab": _stack_vocabulary(stack_id, lock),
            "status": lock.get("status"),
            "lock": lock,
            "recommended_set_components": rs_by_stack.get(stack_id, []),
        }
    return out


def _find_exact_match(
    cart_set: set[str], catalog: dict[str, dict[str, Any]]
) -> tuple[str | None, dict[str, Any] | None]:
    """Pick the BEST stack whose vocab covers the cart.

    "Best" = prefers trustworthy status, then narrowest vocab (most
    specific marriage wins), then alpha for determinism.

    Vocab-subset match is necessary but NOT sufficient for a will_work
    verdict — see _is_full_match for the additional gate the explainer
    applies (cart must cover the stack's recommended_set so it represents
    a real marriage, not just a partial pick).
    """
    if not cart_set:
        return None, None
    candidates: list[tuple[int, int, str, dict[str, Any]]] = []
    for stack_id, entry in catalog.items():
        if not cart_set.issubset(entry["vocab"]):
            continue
        status = entry["status"] or ""
        # Higher is better — negate for ascending sort
        priority = 2 if status in _TRUSTWORTHY_STATUSES else (1 if status == "candidate" else 0)
        candidates.append((-priority, len(entry["vocab"]), stack_id, entry))
    if not candidates:
        return None, None
    candidates.sort(key=lambda t: (t[0], t[1], t[2]))
    _p, _s, sid, entry = candidates[0]
    return sid, entry


def _is_full_match(cart_set: set[str], entry: dict[str, Any]) -> bool:
    """True when the cart covers the matched stack's recommended_set.

    Per the brief's will_work definition: the cart must match a certified
    stack with a "source-verified constraint chain spanning all
    components". A constraint chain spans all components only when the
    cart contains every component the stack's recommended_set names —
    otherwise we're looking at a partial cart that happens to be a
    subset of the stack's vocabulary but doesn't yet form a real
    marriage.

    The recommended_set is the cart-UX layer's view of "what marries into
    this stack" (vs the lock components[] which carries service-level FE/BE
    splits). A cart that covers the recommended_set is treated as a full
    marriage — adding the FE/BE detail components alongside the unified
    `starrocks` id is fine because of the dialect-bridging vocab union.
    """
    rs = entry.get("recommended_set_components") or []
    if not rs:
        # No recommended_set declared — fall back to vocab match (we already
        # know cart_set issubset vocab from _find_exact_match).
        return True
    rs_set = set(rs)
    # Direct coverage: cart names every recommended_set id.
    if rs_set.issubset(cart_set):
        return True
    # Dialect-bridging coverage: e.g. recommended_set has `starrocks` but
    # cart has `starrocks-fe + starrocks-be`. Check that every NOT-in-cart
    # recommended id has an equivalent set of detail ids in the cart that
    # appear in the stack vocab. The simple rule: each missing
    # recommended_set id must have at least one cart member sharing its
    # name as a prefix (e.g. `starrocks` -> `starrocks-fe`).
    vocab = entry.get("vocab") or set()
    for rs_id in rs_set - cart_set:
        candidates = [c for c in cart_set if c in vocab and c.startswith(rs_id + "-")]
        if not candidates:
            return False
    return True


# ---------------------------------------------------------------------------
# Helpers — per-component compatibility graph (from catalog.compatible_with)
# ---------------------------------------------------------------------------


def _compatibility_graph() -> dict[str, set[str]]:
    """Undirected graph: cid -> set of cids it is documented to work with.

    Source: each component's `compatible_with` array in
    stacks/components-catalog.yaml. We make the relation symmetric so
    "X has Y in its list" implies "Y is compatible with X" — the catalog
    is occasionally asymmetric in the raw YAML and that asymmetry has
    bitten readers before.

    Components NOT present in this graph have no documented compatibility
    surface and are treated as "any pairing is unknown" rather than
    "any pairing is bad" — so the explainer never invents wont_work
    verdicts from missing-data alone.
    """
    idx = component_index()
    g: dict[str, set[str]] = {}
    for cid, comp in idx.items():
        bag = g.setdefault(cid, set())
        for partner in (comp.get("compatible_with") or []):
            if isinstance(partner, str) and partner:
                bag.add(partner)
                # Symmetrise — "A says it works with B" implies B<->A
                g.setdefault(partner, set()).add(cid)
    return g


# ---------------------------------------------------------------------------
# Helpers — incompatible[] scanning across ALL lock files
# ---------------------------------------------------------------------------


def _split_combo_entry(item: Any) -> tuple[str, str | None]:
    """Parse a lock incompatible[] combination entry.

    Shapes seen across the real lock files:
      "spark:3.5.1_1.5.2"                       — id + tag
      "trino:latest"                            — id + floating-tag
      "trino:<460"                              — id + version constraint
      "hudi"                                    — bare id (hypothetical)
      "starrocks-fe + spark with hive-metastore" — free-text advisory
      "iceberg-rest:1.6.0 in the same stack"    — id + tag + free-text suffix

    Returns (cleaned_id, tag_or_None). The cleaned_id has any trailing
    free-text stripped, so "iceberg-rest:1.6.0 in the same stack" becomes
    ("iceberg-rest", "1.6.0 in the same stack") — the id half is what we
    compare against the cart.
    """
    if not isinstance(item, str):
        return (str(item), None)
    s = item.strip()
    if not s:
        return ("", None)
    if ":" in s:
        cid, tag = s.split(":", 1)
        cid = cid.strip()
        # Some entries embed free text in the id half too — clip at the
        # first space which is never valid in a real component id.
        if " " in cid:
            return (cid.split(" ", 1)[0], tag.strip())
        return (cid, tag.strip())
    # No colon — could be a bare id or a free-text advisory
    if " " in s:
        return (s.split(" ", 1)[0], None)
    return (s, None)


def _is_known_component(cid: str, idx: dict[str, Any]) -> bool:
    """Tolerates the FE/BE split — `starrocks` cart-id maps to two lock ids."""
    if cid in idx:
        return True
    # Lock-level ids that don't have a 1:1 catalog row but are still real
    # (starrocks-fe, starrocks-be, postgres, minio-client). Allowlist
    # rather than over-restrict.
    return cid in {
        "starrocks-fe", "starrocks-be", "postgres", "minio-client", "spark",
    }


def _scan_incompatibilities(
    cart_set: set[str], stack_locks: dict[str, dict[str, Any]]
) -> list[Reason]:
    """Walk every lock's incompatible[] block; collect entries the cart triggers.

    Trigger rules — entries fire ONLY when ALL of the following hold:
      1. Every element in the `combination` is a bare component id (no tag)
         OR a `<id>:` pair where the tag half is a free-text advisory like
         "1.6.0 in the same stack" — those tag-suffixed advisories ARE
         enforceable at the explainer level because they describe a
         logical co-presence rule, not a specific-version constraint.
      2. Every id maps to a cart entry.
      3. The combination has at least 2 ids (single-id "won't pull"
         entries like `[spark-hudi:3.5.0_0.15.0]` describe missing-image
         conditions that the install-time precheck owns — not the cart
         explainer).

    Tag-specific entries (`[trino:latest]`, `[starrocks-fe:3.3-latest,
    starrocks-be:3.3-latest]`, `[spark:3.5.1_1.5.2]`) are deferred to
    install-time precheck — the cart has no tag context and bumping a
    tag is what bumps the lock.

    Free-text advisories ("starrocks-fe + spark with hive-metastore")
    never match a plain-id cart — that's intentional.

    We scan ALL locks because cross-stack incompatibilities are real
    (e.g. the Hudi lock flags spark-hudi + iceberg-rest "in the same
    stack", which is the canonical sentinel for the brief's
    hudi+iceberg-rest cart).
    """
    out: list[Reason] = []
    seen_combos: set[tuple[str, ...]] = set()
    for stack_id, entry in stack_locks.items():
        lock = entry["lock"]
        for inc in lock.get("incompatible", []) or []:
            if not isinstance(inc, dict):
                continue
            combo_raw = inc.get("combination") or []
            ids: list[str] = []
            skip = False
            for item in combo_raw:
                cid, tag = _split_combo_entry(item)
                if not cid:
                    skip = True
                    break
                # Tag-specific entries are install-time precheck's job.
                # Treat a tag as "specific" UNLESS it's clearly free-text
                # describing a co-presence rule rather than a version pin.
                if tag is not None and not _tag_is_freetext_advisory(tag):
                    skip = True
                    break
                ids.append(cid)
            if skip or len(ids) < 2:
                continue
            if not all(cid in cart_set for cid in ids):
                continue
            key = tuple(sorted(set(ids)))
            if key in seen_combos:
                continue
            seen_combos.add(key)
            reason_text = str(inc.get("reason") or "").strip()
            workaround = str(inc.get("workaround") or "").strip()
            full = reason_text
            if workaround:
                full = f"{reason_text} Workaround: {workaround}"
            out.append({
                "kind": "known_incompatible",
                "between": list(key),
                "explanation": full or (
                    "documented as incompatible in the lock file"
                ),
                "source": f"{stack_id}.lock.yaml incompatible[]",
            })
    return out


def _tag_is_freetext_advisory(tag: str) -> bool:
    """Distinguish "0.15.0" / "latest" / "<460" (version specifiers) from
    "1.6.0 in the same stack" / "in the same stack" (co-presence advisories).

    The heuristic: a tag with a space in it is a free-text advisory —
    real docker tags are space-free.
    """
    return " " in (tag or "")


# ---------------------------------------------------------------------------
# Helpers — per-pair compatibility scan from the catalog
# ---------------------------------------------------------------------------


# Category pairs where direct binding in compatible_with is the load-bearing
# signal of compatibility. Two components in these category pairs that
# don't name each other in their compatible_with arrays — AND don't coexist
# in any certified stack — are flagged as catalog-level incompatible.
#
# Why these and not others? Because these are the wire-level marriages:
#   - table_format ↔ catalog : the catalog must speak the format's spec
#   - table_format ↔ processing : the engine must have the format's runtime
#   - catalog ↔ processing : the engine must have a client for the catalog
#   - catalog ↔ serving : same as above for the serving side
#
# Pairs like (table_format ↔ object_storage) or (processing ↔ object_storage)
# bind THROUGH the catalog/format layer, not directly — so a missing
# compatible_with entry there is not a smoking gun.
_DIRECT_BINDING_CATEGORY_PAIRS: frozenset[frozenset[str]] = frozenset({
    frozenset({"table_format", "catalog"}),
    frozenset({"table_format", "processing"}),
    frozenset({"catalog", "processing"}),
    frozenset({"catalog", "serving"}),
})


def _coexist_in_any_stack(
    a: str, b: str, stack_catalog: dict[str, dict[str, Any]]
) -> bool:
    """True if any stack's vocab contains BOTH `a` and `b` simultaneously.

    A pair that coexists in any certified stack cannot be catalog-level
    incompatible — the stack itself is the proof of compatibility,
    overriding any apparent gap in the compatible_with arrays.
    """
    for entry in stack_catalog.values():
        vocab = entry.get("vocab") or set()
        if a in vocab and b in vocab:
            return True
    return False


def _pair_compatibility_reasons(
    cart: list[str],
    graph: dict[str, set[str]],
    idx: dict[str, Any],
    stack_catalog: dict[str, dict[str, Any]],
) -> tuple[list[Reason], list[Reason]]:
    """Walk every pair in the cart. Classify as marriage_unknown (no overlap
    documented per catalog) or marriage_incompatible (no overlap AND in a
    direct-binding category pair AND no stack hosts both).

    Returns (incompatible_reasons, unknown_reasons).

    The wont_work bar is intentionally high: catalog-level incompatible
    only fires when (a) the two components are in categories that REQUIRE
    a direct client/connector binding, (b) neither side names the other
    in compatible_with, and (c) no certified stack hosts both. Anything
    that doesn't clear all three bars is at worst marriage_unknown —
    silence is safer than inventing a verdict.

    Components not present in the catalog index are skipped here; the
    caller surfaces them separately via the unknown-components path.
    """
    incompat: list[Reason] = []
    unknown: list[Reason] = []
    seen_pairs: set[tuple[str, str]] = set()
    cart_in_idx = [c for c in cart if c in idx]
    for i, a in enumerate(cart_in_idx):
        for b in cart_in_idx[i + 1:]:
            key = (a, b) if a < b else (b, a)
            if key in seen_pairs:
                continue
            seen_pairs.add(key)
            cat_a = idx.get(a, {}).get("category_id")
            cat_b = idx.get(b, {}).get("category_id")
            # Skip same-category — two formats / two catalogs is a different
            # failure mode (the cart UX prevents it; not the explainer's job).
            if cat_a and cat_b and cat_a == cat_b:
                continue
            a_links = graph.get(a, set())
            b_links = graph.get(b, set())
            # Documented marriage in either direction — informational only.
            if b in a_links or a in b_links:
                unknown.append({
                    "kind": "marriage_unknown",
                    "between": [a, b],
                    "explanation": (
                        f"{idx[a].get('name', a)} and {idx[b].get('name', b)} "
                        "are documented as compatible per the catalog's "
                        "compatible_with arrays, but no certified stack pins "
                        "this exact pair end-to-end yet."
                    ),
                    "source": (
                        "components-catalog.yaml "
                        f"compatible_with arrays of '{a}' and '{b}'"
                    ),
                })
                continue
            # No direct binding documented. Three more bars to clear before
            # we cry wolf.
            if not cat_a or not cat_b:
                continue  # missing category metadata — silence
            if frozenset({cat_a, cat_b}) not in _DIRECT_BINDING_CATEGORY_PAIRS:
                continue  # not a wire-level marriage — silence
            if _coexist_in_any_stack(a, b, stack_catalog):
                continue  # a stack hosts both → not incompatible
            incompat.append({
                "kind": "known_incompatible",
                "between": [a, b],
                "explanation": (
                    f"{idx[a].get('name', a)} ({cat_a}) and "
                    f"{idx[b].get('name', b)} ({cat_b}) have no documented "
                    "client/connector binding — neither names the other in "
                    "its compatible_with array, and no certified stack hosts "
                    "both."
                ),
                "source": (
                    "components-catalog.yaml "
                    f"compatible_with arrays of '{a}' and '{b}'"
                ),
            })
    return incompat, unknown


# ---------------------------------------------------------------------------
# Helpers — constraint chain for the matched stack
# ---------------------------------------------------------------------------


def _build_dialect_bridge(
    lock: dict[str, Any], entry: dict[str, Any]
) -> dict[str, set[str]]:
    """Map lock-id → set of cart-side equivalents.

    The lock file's `components[].id` and the catalog's recommended_set
    component ids speak different dialects:
      - UDP lock has `spark`, recommended_set has `spark-iceberg`
      - All locks have `starrocks-fe` + `starrocks-be`, recommended_set has
        unified `starrocks`
    The bridge is built per-stack from BOTH sides of the vocab so the
    explainer can fire a constraint like `[spark, iceberg-rest]` even when
    the cart says `spark-iceberg` (and vice versa).

    Returns dict mapping every lock-id to the union of {itself} plus any
    cart-side id from the stack's recommended_set that's "prefix-related"
    (one is a prefix of the other with a `-`-separated tail).
    """
    bridge: dict[str, set[str]] = {}
    lock_ids = [
        c.get("id") for c in (lock.get("components") or [])
        if isinstance(c, dict) and isinstance(c.get("id"), str)
    ]
    rs_ids = entry.get("recommended_set_components") or []
    for lid in lock_ids:
        if not lid:
            continue
        equiv = {lid}
        for rid in rs_ids:
            if rid == lid:
                equiv.add(rid)
            elif rid.startswith(lid + "-") or lid.startswith(rid + "-"):
                equiv.add(rid)
        bridge[lid] = equiv
    return bridge


def _constraint_satisfied_by_cart(
    between: list[str], cart_set: set[str], bridge: dict[str, set[str]]
) -> bool:
    """True if every id in `between` is covered by the cart, either
    directly or through a dialect-bridge equivalent (so `spark` in a
    constraint counts as covered when the cart contains `spark-iceberg`).
    """
    for b in between:
        equiv = bridge.get(b, {b})
        if not (equiv & cart_set):
            return False
    return True


def _constraint_chain_reasons(
    lock: dict[str, Any],
    cart_set: set[str],
    stack_id: str,
    bridge: dict[str, set[str]],
) -> list[Reason]:
    """Promote every lock constraint whose `between` is fully covered by
    the cart (with dialect bridging) into a marriage_proven reason.

    Each reason cites its source line via the lock-file index, so a UI
    can deep-link to the exact constraint.
    """
    out: list[Reason] = []
    constraints = lock.get("constraints", []) or []
    for idx_pos, c in enumerate(constraints):
        if not isinstance(c, dict):
            continue
        between = [b for b in (c.get("between") or []) if isinstance(b, str)]
        if not between:
            continue
        if not _constraint_satisfied_by_cart(between, cart_set, bridge):
            continue
        rule = str(c.get("rule") or "").strip()
        verified = str(c.get("verified") or "").strip()
        explanation = rule
        if verified:
            explanation = f"{rule} Evidence: {verified}"
        out.append({
            "kind": "marriage_proven",
            "between": between,
            "explanation": explanation or "constraint satisfied",
            "source": f"{stack_id}.lock.yaml constraints[{idx_pos}]",
        })
    return out


# ---------------------------------------------------------------------------
# Helpers — missing required category + needs-auxiliary scan
# ---------------------------------------------------------------------------


def _missing_categories_for_cart(
    cart: list[str], matched_entry: dict[str, Any] | None = None
) -> list[str]:
    """Return required-category ids the cart hasn't filled.

    Catalog categories: table_format, catalog, object_storage, processing,
    serving, (orchestration | bi_optional | observability are optional).

    Dialect-bridging: cart entries like `starrocks-fe` aren't in the
    component index (only `starrocks` is). When a matched stack is
    supplied, we treat a cart entry as filling category C if EITHER:
      (a) the entry is in the catalog index under category C, OR
      (b) the entry is in the matched stack's vocab AND has a recommended_set
          sibling that maps to category C (so `starrocks-fe` in vocab with
          `starrocks` in recommended_set fills `serving`).

    If a matched stack is supplied, categories that the matched stack's
    own recommended_set also doesn't fill are EXCLUDED from the missing
    list — that's the stack's deliberate scope, not a gap in the cart.
    Concrete example: the hudi-hms-spark candidate stack has no serving
    engine in its recommended_set, so a HUDI cart shouldn't be flagged
    for "missing serving".
    """
    idx = component_index()
    required = required_category_ids()
    filled: set[str] = set()
    for cid in cart:
        cat = idx.get(cid, {}).get("category_id")
        if cat:
            filled.add(cat)
    # Dialect-bridging — promote vocab-only cart entries to their
    # recommended_set sibling's category.
    if matched_entry is not None:
        rs = matched_entry.get("recommended_set_components") or []
        vocab = matched_entry.get("vocab") or set()
        for cid in cart:
            if cid in idx or cid not in vocab:
                continue
            # Find a recommended_set sibling that prefixes this cart entry.
            for sib in rs:
                if cid == sib or cid.startswith(sib + "-"):
                    sib_cat = idx.get(sib, {}).get("category_id")
                    if sib_cat:
                        filled.add(sib_cat)
                    break
    missing = [c for c in required if c not in filled]
    if matched_entry is None or not missing:
        return missing
    # Exclude categories the matched stack's recommended_set also doesn't fill.
    rs = matched_entry.get("recommended_set_components") or []
    rs_filled: set[str] = set()
    for cid in rs:
        cat = idx.get(cid, {}).get("category_id")
        if cat:
            rs_filled.add(cat)
    return [c for c in missing if c in rs_filled]


def _needs_auxiliary_reasons(cart_set: set[str]) -> list[Reason]:
    """For each cart component that requires a backing service, flag if the
    backing service isn't in the cart. The install pipeline will inject it
    automatically (per the candidate lock files), so this is informational.
    """
    out: list[Reason] = []
    for cid, (aux, why) in _NEEDS_AUXILIARY.items():
        if cid in cart_set and aux not in cart_set:
            out.append({
                "kind": "needs_auxiliary",
                "missing": aux,
                "for_": cid,
                "explanation": why,
                "source": "stacks/compatibility/*.lock.yaml components[]",
            })
    return out


# ---------------------------------------------------------------------------
# Helpers — alternative-cart search (one-swap-away)
# ---------------------------------------------------------------------------


def _alternative_carts(
    cart: list[str], catalog: dict[str, dict[str, Any]]
) -> list[AlternativeCart]:
    """Find single-component swaps that turn this cart into a stack match.

    Strategy:
      1. Try removing each cart component, see if the rest matches a stack.
         If yes, the swap is "remove X". Add[] is empty.
      2. If no pure-removal works, try a single substitute: remove X, then
         add one of the recommended_set components from each candidate
         stack — if the resulting cart matches, that's the swap suggestion.

    Caps at 3 suggestions, sorted with trustworthy stacks first.
    """
    out: list[AlternativeCart] = []
    seen: set[tuple[tuple[str, ...], tuple[str, ...], str]] = set()
    cart_set = set(cart)

    def _record(remove: list[str], add: list[str], stack_id: str, status: str | None):
        key = (tuple(sorted(remove)), tuple(sorted(add)), stack_id)
        if key in seen:
            return
        seen.add(key)
        status_label = f"({status})" if status else ""
        result = f"matches {stack_id} {status_label}".strip()
        out.append({
            "swap": {"remove": remove, "add": add},
            "result": result,
        })

    # Step 1 — pure-removal scan
    for to_remove in cart:
        candidate_set = cart_set - {to_remove}
        if not candidate_set:
            continue
        sid, entry = _find_exact_match(candidate_set, catalog)
        if sid and entry:
            _record([to_remove], [], sid, entry["status"])

    # Step 2 — substitution scan (only if step 1 found nothing)
    if not out:
        for sid, entry in catalog.items():
            rs_components = entry.get("recommended_set_components") or []
            if not rs_components:
                continue
            rs_set = set(rs_components)
            # What's in the cart but NOT in this stack's recommended set?
            extras = [c for c in cart if c not in entry["vocab"]]
            # What's in the stack's recommended set but NOT in cart?
            missing = [c for c in rs_components if c not in cart_set]
            if len(extras) == 1 and len(missing) >= 1:
                # Single substitute: remove the one extra, add the missing.
                _record(extras, missing, sid, entry["status"])

    # Sort: trustworthy stacks first, then by stack id for determinism
    def _rank(alt: AlternativeCart) -> tuple[int, str]:
        # parse "matches <stack_id> (<status>)"
        text = alt["result"]
        is_trustworthy = any(s in text for s in ("pilot-stable", "linux-stable", "production"))
        return (0 if is_trustworthy else 1, text)
    out.sort(key=_rank)
    return out[:3]


# ---------------------------------------------------------------------------
# Helpers — headline + graduation path
# ---------------------------------------------------------------------------


def _format_proven_marriages_count(reasons: list[Reason]) -> int:
    return sum(1 for r in reasons if r["kind"] == "marriage_proven")


def _headline_will_work(
    stack_id: str, status: str | None, proven_count: int
) -> str:
    status_label = status or "uncertified"
    if status in _TRUSTWORTHY_STATUSES:
        return (
            f"This combination works — backed by {proven_count} source-verified "
            f"marriage(s) in {stack_id} ({status_label}); evidence recorded in the lock file."
        )
    # Candidate path — image tags verified but no end-to-end evidence yet
    return (
        f"This combination matches the {status_label} stack {stack_id}, "
        f"with {proven_count} source-verified marriage(s) — but no end-to-end "
        "install evidence has been recorded yet. Image tags verified on the "
        "registry; install at your own risk and contribute an evidence record."
    )


def _headline_wont_work(reasons: list[Reason]) -> str:
    # Pick the strongest known_incompatible reason for the headline.
    inc = [r for r in reasons if r["kind"] == "known_incompatible"]
    if not inc:
        return "Won't install: the combination violates a documented compatibility rule."
    r = inc[0]
    pair = r.get("between") or []
    names = " + ".join(pair) if pair else "components"
    short = r["explanation"]
    # Truncate to keep the headline readable in a UI pill.
    if len(short) > 220:
        short = short[:217].rstrip() + "..."
    return f"Won't install: {names} aren't compatible — {short}"


def _headline_untested(cart: list[str], unknown_count: int) -> str:
    if not cart:
        return "Cart is empty — pick at least a table format and an object store to start."
    pair_clause = (
        f"Each of the {unknown_count} cross-component pair(s) looks compatible "
        "per the catalog, but "
    ) if unknown_count else ""
    return (
        f"Plausible but untested: no certified stack covers this exact combination. "
        f"{pair_clause}nobody has run it end-to-end. Submit an install evidence "
        "record to make it pilot-stable."
    )


def _headline_empty() -> str:
    return "Cart is empty — pick at least a table format and an object store to start."


def _headline_no_required(cart: list[str], missing: list[str]) -> str:
    pretty = ", ".join(missing)
    return (
        f"Cart is missing required component(s) for category: {pretty}. "
        "A lakehouse needs at minimum a table format, a catalog, object "
        "storage, a processing engine, and a serving engine."
    )


def _graduation_path(stack_id: str | None, status: str | None) -> str:
    """How to take a stack from candidate → pilot-stable.

    Empty string for non-candidate verdicts — there's nothing to graduate
    from on a pilot-stable+ stack, and graduation isn't meaningful for an
    untested cart that doesn't even match a stack yet.
    """
    if not stack_id:
        return ""
    if status not in ("candidate",):
        return ""
    return (
        f"To promote `{stack_id}` from candidate to pilot-stable: "
        "(1) Run install end-to-end on Windows + Docker Desktop and capture install_id. "
        "(2) Run install end-to-end on Linux (Ubuntu 22.04). "
        "(3) Smoke test passes on both. "
        f"(4) Append evidence[] entry to stacks/compatibility/{stack_id}.lock.yaml "
        "with both install_ids. "
        "(5) Bump version_id (semver patch) and flip status to pilot-stable. "
        f"See docs/GRADUATION_RUNBOOK.md § `{stack_id}` for the full per-stack checklist."
    )


# ---------------------------------------------------------------------------
# JSON normalisation — the public dict uses "for" (a Python keyword) as a
# key, so we accept "for_" internally and rename at the boundary.
# ---------------------------------------------------------------------------


def _dump_reason(r: Reason) -> dict:
    out: dict = {"kind": r["kind"], "explanation": r["explanation"]}
    if "between" in r:
        out["between"] = list(r["between"])
    if "source" in r:
        out["source"] = r["source"]
    if "missing" in r:
        out["missing"] = r["missing"]
    if "for_" in r:
        out["for"] = r["for_"]
    return out


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def explain_cart(component_ids: list[str]) -> CartExplanation:
    """Plain-English compatibility explanation for any cart combination.

    Performance contract: <100 ms under the lock-file lru_cache (warmed
    after the first call). No network, no subprocess.
    """
    cart = _normalize_cart(component_ids)
    cart_set = set(cart)
    idx = component_index()
    catalog = _all_stack_catalog()
    graph = _compatibility_graph()

    # 0. Empty cart — short-circuit.
    if not cart:
        return {
            "verdict": "untested",
            "headline": _headline_empty(),
            "reasons": [{
                "kind": "missing_category",
                "explanation": (
                    "Empty cart. A lakehouse needs at minimum a table format, "
                    "a catalog, object storage, a processing engine, and a "
                    "serving engine — pick one per category to get a real "
                    "compatibility verdict."
                ),
                "source": "components-catalog.yaml categories[]",
            }],
            "matched_stack": None,
            "matched_stack_status": None,
            "graduation_path": "",
            "missing_components": list(required_category_ids()),
            "extra_components": [],
            "alternative_carts": [],
        }

    # 1. Scan for catalog-level incompatibilities (pair has no
    #    compatible_with overlap AND is in a direct-binding category pair
    #    AND no stack hosts both), and lock-level incompatibilities (any
    #    lock's incompatible[] block triggered by the cart).
    pair_incompat, pair_unknown = _pair_compatibility_reasons(cart, graph, idx, catalog)
    lock_incompat = _scan_incompatibilities(cart_set, catalog)
    all_incompat = lock_incompat + pair_incompat

    # 2. Try to match an existing stack.
    matched_stack_id, matched_entry = _find_exact_match(cart_set, catalog)

    # 3. Find missing-required-category problems (scoped by the matched
    #    stack's own recommended_set so candidate stacks like
    #    hudi-hms-spark with no serving engine don't false-positive).
    missing_categories = _missing_categories_for_cart(cart, matched_entry)

    # 4. Auxiliary-required reasons (informational, never block a verdict).
    aux_reasons = _needs_auxiliary_reasons(cart_set)

    # 5. Unknown components (in cart but not in catalog AND not a known
    #    lock-level id like starrocks-fe/starrocks-be).
    unknown_components = [c for c in cart if not _is_known_component(c, idx)]

    # 6. Compute extra components (in cart but not in the matched stack).
    if matched_entry is not None:
        vocab = matched_entry["vocab"]
        extra_components = [c for c in cart if c not in vocab]
    else:
        extra_components = []

    # 7. Compute missing-from-matched-stack components (for full clarity
    #    of what the cart still needs to add to be "complete" vs the
    #    matched stack's recommended set). Applies dialect bridging so
    #    `starrocks` in the recommended_set counts as filled when the
    #    cart contains `starrocks-fe` + `starrocks-be`.
    if matched_entry is not None:
        rs_components = matched_entry.get("recommended_set_components") or []
        vocab = matched_entry.get("vocab") or set()
        missing_for_complete = []
        for rid in rs_components:
            if rid in cart_set:
                continue
            # Cart-side dialect equivalents are anything in the stack vocab
            # whose id starts with `<rid>-` and is in the cart.
            equivalents = [
                c for c in cart_set if c in vocab and c.startswith(rid + "-")
            ]
            if not equivalents:
                missing_for_complete.append(rid)
    else:
        missing_for_complete = []

    # 8. Decide verdict.
    if all_incompat:
        # ===== wont_work =====
        reasons: list[Reason] = list(all_incompat)
        # Include needs_auxiliary and unknown-component info as supporting
        # context — but the verdict is still wont_work.
        reasons.extend(aux_reasons)
        for u in unknown_components:
            reasons.append({
                "kind": "known_incompatible",
                "between": [u],
                "explanation": (
                    f"'{u}' is not in the component catalog — cannot verify any "
                    "compatibility claim. Either it's a typo, or the catalog "
                    "needs an entry for it before this cart can be evaluated."
                ),
                "source": "stacks/components-catalog.yaml",
            })
        headline = _headline_wont_work(reasons)
        alternatives = _alternative_carts(cart, catalog)
        # Same matched_stack discipline as the untested branch — only
        # report a match when the cart is a real full match.
        is_full = bool(matched_entry and _is_full_match(cart_set, matched_entry))
        return {
            "verdict": "wont_work",
            "headline": headline,
            "reasons": [_dump_reason(r) for r in reasons],
            "matched_stack": matched_stack_id if is_full else None,
            "matched_stack_status": (
                matched_entry["status"] if (matched_entry and is_full) else None
            ),
            "graduation_path": "",
            "missing_components": missing_for_complete,
            "extra_components": extra_components,
            "alternative_carts": alternatives,
        }

    if (
        matched_stack_id
        and matched_entry
        and not missing_categories
        and _is_full_match(cart_set, matched_entry)
    ):
        # ===== will_work =====
        status = matched_entry["status"]
        lock = matched_entry["lock"]
        bridge = _build_dialect_bridge(lock, matched_entry)
        proven = _constraint_chain_reasons(lock, cart_set, matched_stack_id, bridge)
        reasons = list(proven) + list(pair_unknown) + list(aux_reasons)
        proven_count = _format_proven_marriages_count(reasons)
        headline = _headline_will_work(matched_stack_id, status, proven_count)
        graduation = _graduation_path(matched_stack_id, status)
        # Alternative carts are still surfaced for will_work when the
        # cart is a candidate match — they suggest a stronger alternative
        # if one exists (e.g. swap into a pilot-stable variant).
        alternatives: list[AlternativeCart] = []
        if status == "candidate":
            alternatives = _alternative_carts(cart, catalog)
        return {
            "verdict": "will_work",
            "headline": headline,
            "reasons": [_dump_reason(r) for r in reasons],
            "matched_stack": matched_stack_id,
            "matched_stack_status": status,
            "graduation_path": graduation,
            "missing_components": missing_for_complete,
            "extra_components": extra_components,
            "alternative_carts": alternatives,
        }

    # ===== untested =====
    reasons = list(pair_unknown) + list(aux_reasons)
    if missing_categories:
        # Add a missing_category reason for each empty required slot.
        for cat in missing_categories:
            reasons.append({
                "kind": "missing_category",
                "explanation": (
                    f"Required category '{cat}' is empty. A lakehouse needs at "
                    "minimum a table format, a catalog, object storage, a "
                    "processing engine, and a serving engine to be installable."
                ),
                "source": "stacks/components-catalog.yaml categories[]",
            })
    # Unknown-component info is supporting context for untested too.
    for u in unknown_components:
        reasons.append({
            "kind": "marriage_unknown",
            "between": [u],
            "explanation": (
                f"'{u}' is not in the component catalog — its compatibility "
                "with the rest of the cart cannot be verified."
            ),
            "source": "stacks/components-catalog.yaml",
        })
    alternatives = _alternative_carts(cart, catalog)
    if missing_categories:
        headline = _headline_no_required(cart, missing_categories)
    else:
        headline = _headline_untested(cart, len(pair_unknown))
    # matched_stack is reported on untested ONLY when the cart is a true
    # full match (recommended_set covered). Otherwise it's misleading
    # (a single-component cart shouldn't claim to "match" a 5-component
    # stack just because its vocab is a superset). Partial matches are
    # surfaced via alternative_carts instead.
    is_full = bool(matched_entry and _is_full_match(cart_set, matched_entry))
    return {
        "verdict": "untested",
        "headline": headline,
        "reasons": [_dump_reason(r) for r in reasons],
        "matched_stack": matched_stack_id if is_full else None,
        "matched_stack_status": (
            matched_entry["status"] if (matched_entry and is_full) else None
        ),
        "graduation_path": "",
        "missing_components": missing_for_complete,
        "extra_components": extra_components,
        "alternative_carts": alternatives,
    }
