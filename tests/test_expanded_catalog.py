"""Tests for the v0.6 catalog + stack expansion.

Covers:
  - 8 new components promoted from coming_soon (hudi, delta, nessie,
    hive-metastore, polaris, spark-hudi, spark-delta, airflow, dagster,
    superset)
  - 1 new optional category (bi_optional)
  - 4 new stack manifests + 4 new compatibility lock files (all
    `status: candidate` — no evidence yet, per the certification contract)

All tests are hermetic — no Docker calls, no network. They validate
structure, cross-references, and the candidate-status invariant.
"""
from __future__ import annotations

import pytest

from backend import catalog as catalog_mod
from backend import compatibility as compatibility_mod
from backend import stack_manifest as stack_manifest_mod


# ---------------------------------------------------------------------------
# Fixtures — clear loader caches between tests so on-disk fixture edits
# during dev don't leak state across runs.
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _clear_caches():
    catalog_mod.load_catalog.cache_clear()
    compatibility_mod.load_lock.cache_clear()
    yield
    catalog_mod.load_catalog.cache_clear()
    compatibility_mod.load_lock.cache_clear()


# ---------------------------------------------------------------------------
# New components — every promoted entry exists with required fields.
# ---------------------------------------------------------------------------

NEW_COMPONENT_IDS = [
    "hudi", "delta",
    "nessie", "hive-metastore", "polaris",
    "spark-hudi", "spark-delta",
    "airflow", "dagster",
    "superset",
]


@pytest.mark.parametrize("comp_id", NEW_COMPONENT_IDS)
def test_new_component_exists_in_catalog(comp_id):
    idx = catalog_mod.component_index()
    assert comp_id in idx, f"new component '{comp_id}' missing from catalog"


@pytest.mark.parametrize("comp_id", NEW_COMPONENT_IDS)
def test_new_component_has_name_logo_tagline(comp_id):
    """Every promoted component must have the cart-UX fields per validate_catalog()."""
    idx = catalog_mod.component_index()
    c = idx[comp_id]
    assert c.get("name"), f"{comp_id}: missing name"
    assert c.get("logo"), f"{comp_id}: missing logo"
    assert c.get("tagline"), f"{comp_id}: missing tagline"


@pytest.mark.parametrize("comp_id", NEW_COMPONENT_IDS)
def test_new_component_marked_candidate(comp_id):
    """Promotion contract: new components ship as candidate (no end-to-end
    evidence in a stack lock yet). Mirrors Trino's pattern."""
    idx = catalog_mod.component_index()
    c = idx[comp_id]
    assert c.get("status_badge") == "Candidate", (
        f"{comp_id}: should ship as Candidate until pilot-stable evidence"
    )
    assert c.get("recommended") is False, (
        f"{comp_id}: should not be recommended without evidence"
    )


def test_new_components_compatible_with_references_resolve():
    """Every id in `compatible_with` must point at an actual catalog component."""
    idx = catalog_mod.component_index()
    known_ids = set(idx)
    problems = []
    for comp_id in NEW_COMPONENT_IDS:
        compat = idx[comp_id].get("compatible_with", []) or []
        for ref in compat:
            if ref not in known_ids:
                problems.append(f"{comp_id}: compatible_with -> unknown '{ref}'")
    assert problems == [], problems


# ---------------------------------------------------------------------------
# New bi_optional category — must exist, must be optional.
# ---------------------------------------------------------------------------

def test_bi_optional_category_exists():
    cat_ids = [c["id"] for c in catalog_mod.categories()]
    assert "bi_optional" in cat_ids


def test_bi_optional_category_is_optional():
    cat = next(c for c in catalog_mod.categories() if c["id"] == "bi_optional")
    assert cat.get("optional") is True
    assert cat["id"] not in catalog_mod.required_category_ids()


def test_bi_optional_contains_superset():
    cat = next(c for c in catalog_mod.categories() if c["id"] == "bi_optional")
    comp_ids = [c["id"] for c in cat.get("components", []) or []]
    assert "superset" in comp_ids


# ---------------------------------------------------------------------------
# New stack manifests — all 4 load and look like real stacks.
# ---------------------------------------------------------------------------

NEW_STACK_IDS = [
    "iceberg-nessie-trino-local-v0.1",
    "hudi-hms-spark-local-v0.1",
    "delta-hms-spark-trino-local-v0.1",
    "iceberg-polaris-spark-local-v0.1",
]


@pytest.mark.parametrize("stack_id", NEW_STACK_IDS)
def test_new_stack_manifest_loads(stack_id):
    m = stack_manifest_mod.load_manifest(stack_id)
    assert m.id == stack_id
    assert m.name
    assert m.components, f"{stack_id}: no components"


@pytest.mark.parametrize("stack_id", NEW_STACK_IDS)
def test_new_stack_has_required_commands(stack_id):
    m = stack_manifest_mod.load_manifest(stack_id)
    for cmd in ("doctor", "start", "bootstrap", "smoke"):
        assert cmd in m.data.get("commands", {}), (
            f"{stack_id}: missing '{cmd}' command (runner.py requires it)"
        )


@pytest.mark.parametrize("stack_id", NEW_STACK_IDS)
def test_new_stack_maturity_is_candidate(stack_id):
    m = stack_manifest_mod.load_manifest(stack_id)
    assert m.data.get("maturity") == "candidate"


@pytest.mark.parametrize("stack_id", NEW_STACK_IDS)
def test_new_stack_certification_block(stack_id):
    """Promotion contract: smoke and bootstrap are required for promotion;
    evidence_dir must be set so install runs have a deterministic place to
    capture artifacts."""
    m = stack_manifest_mod.load_manifest(stack_id)
    cert = m.data.get("certification", {})
    assert cert.get("smoke_test_required") is True
    assert cert.get("demo_bootstrap_required") is True
    assert cert.get("evidence_dir", "").startswith("evidence/")


# ---------------------------------------------------------------------------
# New lock files — each exists with status: candidate and evidence: [].
# That's the whole candidate contract.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("stack_id", NEW_STACK_IDS)
def test_new_lock_file_exists(stack_id):
    lock = compatibility_mod.load_lock(stack_id)
    assert lock is not None, f"{stack_id}: lock file missing"


@pytest.mark.parametrize("stack_id", NEW_STACK_IDS)
def test_new_lock_status_is_candidate(stack_id):
    lock = compatibility_mod.load_lock(stack_id)
    assert lock.get("status") == "candidate"


@pytest.mark.parametrize("stack_id", NEW_STACK_IDS)
def test_new_lock_evidence_is_empty(stack_id):
    """The candidate contract: status: candidate iff evidence is empty.
    Any first evidence entry should flip the status to pilot-stable in the
    same commit, with a corresponding bump to certified_at + version_id.
    """
    lock = compatibility_mod.load_lock(stack_id)
    assert lock.get("evidence", []) == []


@pytest.mark.parametrize("stack_id", NEW_STACK_IDS)
def test_new_lock_has_required_fields(stack_id):
    lock = compatibility_mod.load_lock(stack_id)
    for k in ("schema_version", "stack_id", "version_id", "certified_at",
              "status", "components", "host_requirements"):
        assert k in lock, f"{stack_id}: lock missing '{k}'"


@pytest.mark.parametrize("stack_id", NEW_STACK_IDS)
def test_new_lock_components_all_have_image_and_tag(stack_id):
    lock = compatibility_mod.load_lock(stack_id)
    for c in lock["components"]:
        assert c.get("image"), f"{stack_id}: component {c.get('id')} missing image"
        assert c.get("tag"), f"{stack_id}: component {c.get('id')} missing tag"


# ---------------------------------------------------------------------------
# Image-tag policy — no `:latest` or floating tags in lock files.
# Exception: known-flagged-for-fix bitsondatadev/hive-metastore:latest is
# allowed temporarily but logged here so the test catches anything else.
# ---------------------------------------------------------------------------

KNOWN_PERMITTED_FLOATING_TAGS = {
    # Tracked: needs SHA pin before pilot-stable promotion. See lock file notes.
    ("bitsondatadev/hive-metastore", "latest"),
}


@pytest.mark.parametrize("stack_id", NEW_STACK_IDS)
def test_lock_components_avoid_floating_tags(stack_id):
    lock = compatibility_mod.load_lock(stack_id)
    bad = []
    for c in lock["components"]:
        if c.get("tag") in ("latest", "main", "master"):
            pair = (c.get("image", ""), c.get("tag", ""))
            if pair not in KNOWN_PERMITTED_FLOATING_TAGS:
                bad.append(f"{c.get('id')}: {pair[0]}:{pair[1]}")
    assert bad == [], f"{stack_id}: floating-tag violations: {bad}"


# ---------------------------------------------------------------------------
# Total stack count — guards against silent removal of a stack manifest
# during refactors. Bump this constant when adding a new stack.
# ---------------------------------------------------------------------------

EXPECTED_TOTAL_STACKS = 6  # 2 existing (udp-local-v0.2, udp-trino) + 4 new


def test_total_stack_count_matches_expectation():
    manifests = stack_manifest_mod.list_manifests()
    ids = sorted(m.id for m in manifests)
    assert len(ids) == EXPECTED_TOTAL_STACKS, (
        f"expected {EXPECTED_TOTAL_STACKS} stacks, found {len(ids)}: {ids}. "
        "If you added/removed a stack, bump EXPECTED_TOTAL_STACKS."
    )


def test_every_stack_has_a_lock_file():
    """No stack manifest should ship without a compatibility lock — even
    candidates need one to record the image tags being shipped."""
    manifests = stack_manifest_mod.list_manifests()
    locks = set(compatibility_mod.list_locks())
    missing = [m.id for m in manifests if m.id not in locks]
    assert missing == [], f"stacks without locks: {missing}"


# ---------------------------------------------------------------------------
# Catalog validation — the on-disk catalog still passes its own validator
# after the promotions. Only the pre-existing Trino-candidate warning is
# tolerated (we know about it; promoting Trino out of candidate is the only
# fix and that requires real install evidence which is separate work).
# ---------------------------------------------------------------------------

def test_validate_catalog_only_warns_about_known_candidate_recommended_sets():
    problems = catalog_mod.validate_catalog()
    # All other warnings would indicate real catalog drift introduced by
    # this expansion — fail noisily so the contributor sees it.
    non_candidate_warnings = [
        p for p in problems
        if "lock status is 'candidate'" not in p
    ]
    assert non_candidate_warnings == [], (
        f"unexpected catalog problems: {non_candidate_warnings}"
    )


# ---------------------------------------------------------------------------
# Runner script-set dispatch — every stack id must have a bootstrap + smoke
# script set registered, otherwise installs silently fall back to whatever
# the manifest argv points at (which won't exist on disk).
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("stack_id", NEW_STACK_IDS)
def test_runner_has_script_set_for_new_stack(stack_id):
    from backend.runner import _STUDIO_SCRIPT_SETS
    assert stack_id in _STUDIO_SCRIPT_SETS, (
        f"runner._STUDIO_SCRIPT_SETS missing entry for '{stack_id}' — "
        "installs will silently fall back to UDP's native scripts which "
        "expect components this stack doesn't ship"
    )


@pytest.mark.parametrize("stack_id", NEW_STACK_IDS)
def test_runner_script_filenames_match_manifest(stack_id):
    """The runner writes scripts under the filenames in the dispatch dict;
    the manifest's commands.bootstrap/smoke argv reference those filenames.
    They MUST match or bash will look for the wrong file."""
    from backend.runner import _STUDIO_SCRIPT_SETS
    m = stack_manifest_mod.load_manifest(stack_id)
    (boot_name, _), (smoke_name, _) = _STUDIO_SCRIPT_SETS[stack_id]
    boot_argv = m.data["commands"]["bootstrap"]["argv"]
    smoke_argv = m.data["commands"]["smoke"]["argv"]
    assert boot_name in boot_argv[-1], (
        f"{stack_id}: dispatch writes '{boot_name}' but manifest runs '{boot_argv}'"
    )
    assert smoke_name in smoke_argv[-1], (
        f"{stack_id}: dispatch writes '{smoke_name}' but manifest runs '{smoke_argv}'"
    )


# ---------------------------------------------------------------------------
# Recommended sets — every new stack must be registered in components-catalog
# so the cart compatibility checker can suggest the right marriage.
# ---------------------------------------------------------------------------

def test_recommended_sets_cover_all_new_stacks():
    sets_by_stack_id = {
        rs.get("stack_id"): rs_id
        for rs_id, rs in catalog_mod.recommended_sets().items()
    }
    missing = [sid for sid in NEW_STACK_IDS if sid not in sets_by_stack_id]
    assert missing == [], (
        f"recommended_sets missing entries for: {missing}. "
        "Without a set the cart's marriage check can't suggest the matching stack."
    )
