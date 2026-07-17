"""Unit tests for backend.compat_check (cart marriage check).

These tests run against the real on-disk lock files and catalog. No
patching, no mocks — the brief explicitly forbids network/subprocess
and the module already honors that. Fixtures used:
  - stacks/components-catalog.yaml
  - stacks/compatibility/udp-local-v0.2.lock.yaml       (pilot-stable)
  - stacks/compatibility/iceberg-nessie-trino-local-v0.1.lock.yaml (candidate)
  - other *.lock.yaml siblings for the no-match / suggest-swap paths
"""
from __future__ import annotations

import json
import time

import pytest

from backend import catalog as catalog_mod
from backend import compat_check
from backend import compatibility


@pytest.fixture(autouse=True)
def _clear_caches():
    """Each test starts with cold caches so on-disk fixtures are re-read."""
    compatibility.load_lock.cache_clear()
    compatibility.load_upgrades.cache_clear()
    catalog_mod.load_catalog.cache_clear()
    yield
    compatibility.load_lock.cache_clear()
    compatibility.load_upgrades.cache_clear()
    catalog_mod.load_catalog.cache_clear()


# ---------------------------------------------------------------------------
# Carts used across multiple tests — keep them near the top so a future
# catalog drift makes the intent obvious to a reader.
# ---------------------------------------------------------------------------

UDP_CART = ["iceberg", "spark-iceberg", "starrocks-fe", "starrocks-be",
            "iceberg-rest", "minio"]

NESSIE_CART = ["iceberg", "nessie", "trino", "starrocks-fe", "starrocks-be", "minio"]

# Hudi + iceberg-rest is the explicit "no real marriage" sentinel from the brief:
# Hudi is HMS-only in the certified set, and iceberg-rest belongs to Iceberg stacks.
HUDI_ICEBERG_REST_CART = ["hudi", "iceberg-rest"]


# ---------------------------------------------------------------------------
# 1. Empty cart returns a verdict (not a crash) with explanations.
# ---------------------------------------------------------------------------

def test_empty_cart_returns_verdict_with_explanations():
    verdict = compat_check.check_cart([])
    assert isinstance(verdict, dict)
    assert verdict["matched_stack"] is None
    assert verdict["matched_stack_status"] is None
    assert verdict["constraints_checked"] == 0
    assert verdict["violations"] == []
    assert verdict["incompatible_hits"] == []
    assert isinstance(verdict["explanations"], list)
    assert len(verdict["explanations"]) >= 1
    # Empty cart is not "incompatible" — it's "warning" (nothing to check).
    assert verdict["overall_verdict"] == "warning"
    assert 0 <= verdict["readiness_score"] <= 100


def test_none_cart_is_treated_as_empty():
    verdict = compat_check.check_cart(None)  # type: ignore[arg-type]
    assert verdict["matched_stack"] is None
    assert verdict["overall_verdict"] == "warning"


# ---------------------------------------------------------------------------
# 2. UDP cart matches udp-local-v0.2 (pilot-stable, high readiness).
# ---------------------------------------------------------------------------

def test_udp_cart_matches_pilot_stable_with_high_score():
    verdict = compat_check.check_cart(UDP_CART)
    assert verdict["matched_stack"] == "udp-local-v0.2"
    assert verdict["matched_stack_status"] == "pilot-stable"
    assert verdict["overall_verdict"] == "compatible"
    # 60 (pilot-stable) + 10 (no floating tags) + 20 (>=1 evidence) +
    # constraints_checked bonus → should comfortably clear 80.
    assert verdict["readiness_score"] >= 80
    assert verdict["constraints_checked"] >= 1
    assert verdict["incompatible_hits"] == []


def test_udp_cart_returns_explanations_naming_stack():
    verdict = compat_check.check_cart(UDP_CART)
    blob = " ".join(verdict["explanations"])
    assert "udp-local-v0.2" in blob


# ---------------------------------------------------------------------------
# 3. Nessie cart matches iceberg-nessie-trino-local-v0.1 (candidate).
# ---------------------------------------------------------------------------

def test_nessie_cart_matches_stack():
    verdict = compat_check.check_cart(NESSIE_CART)
    assert verdict["matched_stack"] == "iceberg-nessie-trino-local-v0.1"
    # Promoted to pilot-stable in the 2026-07-17 VPS campaign (has evidence),
    # so it now reports pilot-stable + a "compatible" verdict.
    assert verdict["matched_stack_status"] == "pilot-stable"
    assert verdict["overall_verdict"] == "compatible"


def test_nessie_and_udp_carts_both_score_high_pilot_stable():
    udp = compat_check.check_cart(UDP_CART)
    nessie = compat_check.check_cart(NESSIE_CART)
    # Both are now pilot-stable with evidence, so both clear the 80 bar
    # (pilot-stable 60 + evidence 20 + constraint bonus).
    assert nessie["readiness_score"] >= 80
    assert udp["readiness_score"] >= 80
    assert 0 <= nessie["readiness_score"] <= 100


# ---------------------------------------------------------------------------
# 4. UDP "obvious marriage" matches without warnings.
# ---------------------------------------------------------------------------

def test_obvious_udp_marriage_is_compatible():
    cart = ["iceberg", "spark-iceberg", "starrocks-fe", "starrocks-be",
            "iceberg-rest", "minio"]
    verdict = compat_check.check_cart(cart)
    assert verdict["matched_stack"] == "udp-local-v0.2"
    assert verdict["overall_verdict"] == "compatible"
    assert verdict["incompatible_hits"] == []


# ---------------------------------------------------------------------------
# 5. Nessie "obvious marriage" matches without unwanted warnings beyond
#    the candidate-status warning.
# ---------------------------------------------------------------------------

def test_obvious_nessie_marriage_matches_correctly():
    cart = ["iceberg", "nessie", "trino", "starrocks-fe", "starrocks-be", "minio"]
    verdict = compat_check.check_cart(cart)
    assert verdict["matched_stack"] == "iceberg-nessie-trino-local-v0.1"
    # Pilot-stable stack → "compatible", no incompatible hits.
    assert verdict["incompatible_hits"] == []
    assert verdict["overall_verdict"] == "compatible"


# ---------------------------------------------------------------------------
# 6. Hudi + iceberg-rest — no real marriage. suggest_swap must be non-None.
# ---------------------------------------------------------------------------

def test_hudi_plus_iceberg_rest_has_no_marriage():
    verdict = compat_check.check_cart(HUDI_ICEBERG_REST_CART)
    assert verdict["matched_stack"] is None
    assert verdict["matched_stack_status"] is None
    assert verdict["readiness_score"] == 0


def test_hudi_plus_iceberg_rest_suggest_swap_is_non_null():
    swap = compat_check.suggest_swap(HUDI_ICEBERG_REST_CART)
    assert swap is not None
    assert "remove" in swap and isinstance(swap["remove"], list) and swap["remove"]
    # The removed item must be one of the original cart entries.
    assert swap["remove"][0] in HUDI_ICEBERG_REST_CART
    assert "alt_stack" in swap and isinstance(swap["alt_stack"], str)
    assert "add" in swap and isinstance(swap["add"], list)


def test_happy_cart_returns_no_swap_suggestion():
    # A cart that already marries should never get a "swap" suggestion.
    assert compat_check.suggest_swap(UDP_CART) is None


# ---------------------------------------------------------------------------
# 7. readiness_score stays within [0, 100] across many cart shapes.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "cart",
    [
        [],
        ["iceberg"],
        UDP_CART,
        NESSIE_CART,
        HUDI_ICEBERG_REST_CART,
        ["minio"],
        ["this-component-does-not-exist"],
        ["iceberg", "spark-iceberg", "starrocks-fe", "starrocks-be",
         "iceberg-rest", "minio", "this-component-does-not-exist"],
    ],
)
def test_readiness_score_clamped_to_unit_range(cart):
    verdict = compat_check.check_cart(cart)
    assert isinstance(verdict["readiness_score"], int)
    assert 0 <= verdict["readiness_score"] <= 100


# ---------------------------------------------------------------------------
# 8. Verdict serializes cleanly to JSON.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("cart", [UDP_CART, NESSIE_CART, HUDI_ICEBERG_REST_CART, []])
def test_verdict_is_json_serializable(cart):
    verdict = compat_check.check_cart(cart)
    # `default=` left unset on purpose — failure here means the verdict
    # contains a non-basic type that the FastAPI response would also choke on.
    raw = json.dumps(verdict)
    parsed = json.loads(raw)
    # Round-trips cleanly back to the same shape.
    assert parsed["matched_stack"] == verdict["matched_stack"]
    assert parsed["readiness_score"] == verdict["readiness_score"]
    assert parsed["overall_verdict"] == verdict["overall_verdict"]


def test_verdict_has_only_basic_python_types():
    verdict = compat_check.check_cart(UDP_CART)
    # Whitelisted scalar types — anything outside fails json.dumps cleanly
    # anyway, but assert explicitly so a future TypedDict drift breaks loudly.
    def _check(v):
        if v is None or isinstance(v, (bool, int, float, str)):
            return True
        if isinstance(v, list):
            return all(_check(x) for x in v)
        if isinstance(v, dict):
            return all(isinstance(k, str) and _check(x) for k, x in v.items())
        return False
    assert _check(verdict)


# ---------------------------------------------------------------------------
# 9. Performance — <100 ms guard per the founding architecture doc.
# ---------------------------------------------------------------------------

def test_check_cart_runs_well_under_100ms():
    # Warm the lru_cache so we measure steady-state, not first-call YAML load.
    compat_check.check_cart(UDP_CART)
    start = time.perf_counter()
    for _ in range(20):
        compat_check.check_cart(UDP_CART)
    elapsed_ms_per_call = (time.perf_counter() - start) * 1000 / 20
    # Generous margin (20x headroom) to keep CI noise from flapping.
    assert elapsed_ms_per_call < 100, f"{elapsed_ms_per_call:.2f}ms per call"


# ---------------------------------------------------------------------------
# 10. Defensive — duplicate ids in the cart are deduped.
# ---------------------------------------------------------------------------

def test_duplicate_cart_entries_do_not_affect_match():
    cart = list(UDP_CART) + list(UDP_CART)  # everything twice
    verdict = compat_check.check_cart(cart)
    assert verdict["matched_stack"] == "udp-local-v0.2"
    assert verdict["overall_verdict"] == "compatible"
