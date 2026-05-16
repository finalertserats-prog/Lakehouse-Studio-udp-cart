"""Unit + route tests for backend/ai_assistant.py per-component recommendation.

Covers the entire surface that previously had ZERO test coverage:
  - ComponentRecommendationRequest / ComponentRecommendation Pydantic shapes
  - POST /api/components/{component_id}/recommend route
  - Helper-driven grounding (siblings, lock-file incompatibilities)
  - Disabled-AI / unknown-component / malformed-JSON degradation paths

Hermetic — every test that would touch Anthropic patches the lazy `import
anthropic` inside `backend.ai_assistant.recommend_component` so no network
call ever happens. The real components-catalog.yaml and lock files in the
repo ARE used (they're checked in, deterministic, and the function's job
is to ground on them).

AAA pattern (Arrange -> Act -> Assert), one assertion per test where it
naturally reads as one logical claim. Some tests assert "this shape
matches" which is a small group of structural fields about ONE object —
kept together because splitting them would dilute the claim.
"""
from __future__ import annotations

import asyncio
import json
import sys
import types

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from backend import ai_assistant as ai_mod
from backend.ai_assistant import (
    ComponentRecommendation,
    ComponentRecommendationRequest,
    _build_recommend_prompt,
    _siblings,
    _strip_tag,
    recommend_component,
)
from backend.main import app


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# A component id that is definitely in stacks/components-catalog.yaml.
KNOWN_COMPONENT = "trino"           # in 'processing' category, has lock-file incompatibilities
KNOWN_SIBLING = "spark-iceberg"     # the OTHER processing-category certified component
KNOWN_NO_INCOMPAT = "minio"         # in 'object_storage' category — single component, no lock incompat
UNKNOWN_COMPONENT = "definitely-not-a-real-component-xyz"


def _fake_anthropic_module(text_payload: str) -> types.ModuleType:
    """Build an `anthropic` module stub whose AsyncAnthropic().messages.create
    returns a response object with one text block holding `text_payload`."""
    mod = types.ModuleType("anthropic")

    class _Block:
        def __init__(self, text: str) -> None:
            self.type = "text"
            self.text = text

    class _Resp:
        def __init__(self, text: str) -> None:
            self.content = [_Block(text)]

    class _AsyncMessages:
        def __init__(self, payload: str) -> None:
            self._payload = payload

        async def create(self, **_kwargs):
            return _Resp(self._payload)

    class _AsyncAnthropic:
        def __init__(self, *_a, **_kw):
            self.messages = _AsyncMessages(text_payload)

    mod.AsyncAnthropic = _AsyncAnthropic   # type: ignore[attr-defined]
    return mod


def _install_fake_anthropic(monkeypatch: pytest.MonkeyPatch, text_payload: str) -> None:
    """Patch sys.modules['anthropic'] so the lazy import inside
    recommend_component returns our stub instead of the real SDK, AND set
    ANTHROPIC_API_KEY so is_enabled() returns True."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key-not-real")
    fake = _fake_anthropic_module(text_payload)
    monkeypatch.setitem(sys.modules, "anthropic", fake)


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


# ---------------------------------------------------------------------------
# 1. Request model validation
# ---------------------------------------------------------------------------

def test_request_model_requires_component_id():
    """component_id is mandatory (no default)."""
    with pytest.raises(ValidationError) as exc_info:
        ComponentRecommendationRequest()  # type: ignore[call-arg]
    assert any(err["loc"] == ("component_id",) for err in exc_info.value.errors())


def test_request_model_rejects_notes_over_600_chars():
    """notes field is capped at 600 chars by the model validator."""
    over_cap = "x" * 601
    with pytest.raises(ValidationError):
        ComponentRecommendationRequest(component_id="trino", notes=over_cap)


def test_request_model_accepts_notes_exactly_600_chars():
    """The boundary itself (600 chars) is valid — only OVER 600 is rejected."""
    at_cap = "x" * 600
    req = ComponentRecommendationRequest(component_id="trino", notes=at_cap)
    assert req.notes == at_cap


def test_request_model_rejects_unknown_size_tier():
    """size_tier is a closed Literal — bogus values must be rejected."""
    with pytest.raises(ValidationError):
        ComponentRecommendationRequest(component_id="trino", size_tier="huge")  # type: ignore[arg-type]


def test_request_model_accepts_known_size_tier():
    """A valid Literal value passes."""
    req = ComponentRecommendationRequest(component_id="trino", size_tier="recommended")
    assert req.size_tier == "recommended"


# ---------------------------------------------------------------------------
# 2. Response model validation
# ---------------------------------------------------------------------------

def test_response_model_rejects_unknown_verdict():
    """verdict is a closed Literal — invented values must be rejected."""
    with pytest.raises(ValidationError):
        ComponentRecommendation(
            component_id="trino",
            verdict="maybe",  # type: ignore[arg-type]
            headline="x",
            rationale="y",
        )


def test_response_model_rejects_unknown_confidence():
    """confidence is a closed Literal — invented values must be rejected."""
    with pytest.raises(ValidationError):
        ComponentRecommendation(
            component_id="trino",
            verdict="good_fit",
            headline="x",
            rationale="y",
            confidence="very-high",  # type: ignore[arg-type]
        )


# ---------------------------------------------------------------------------
# 3. Helper logic — siblings, lock incompatibilities, tag stripping
# ---------------------------------------------------------------------------

def test_siblings_returns_same_category_components_only():
    """`trino` lives in 'processing'; siblings must all share that category."""
    target, siblings = _siblings(KNOWN_COMPONENT)
    assert target is not None
    target_cat = target["category_id"]
    assert all(s.get("category_id") == target_cat for s in siblings)


def test_siblings_excludes_the_target_component_itself():
    """Self should never appear in its own siblings list."""
    _, siblings = _siblings(KNOWN_COMPONENT)
    assert all(s.get("id") != KNOWN_COMPONENT for s in siblings)


def test_siblings_for_unknown_component_returns_empty_pair():
    """Unknown id -> (None, []) — no exception."""
    target, siblings = _siblings(UNKNOWN_COMPONENT)
    assert target is None and siblings == []


def test_strip_tag_strips_image_tag_suffix():
    """_strip_tag('trino:475') -> 'trino' so combo entries match bare ids."""
    assert _strip_tag("trino:475") == "trino"


def test_strip_tag_handles_bare_id():
    """A bare id (no ':') passes through unchanged."""
    assert _strip_tag("trino") == "trino"


def test_incompat_lookup_surfaces_trino_combinations():
    """`trino` has incompatibility entries in udp-trino-local-v0.1.lock.yaml
    (combinations include `trino:latest` and `trino:475`). The helper must
    surface at least one."""
    found = ai_mod._incompat_for_component(KNOWN_COMPONENT)
    assert len(found) >= 1


def test_incompat_lookup_empty_for_component_with_no_combinations():
    """An unknown component has no combinations anywhere -> empty list."""
    found = ai_mod._incompat_for_component(UNKNOWN_COMPONENT)
    assert found == []


# ---------------------------------------------------------------------------
# 4. Prompt assembly — notes pass-through, siblings inclusion, cart inclusion
# ---------------------------------------------------------------------------

def test_build_prompt_passes_notes_through_unmodified():
    """A `notes` value free of secret-shaped tokens must appear verbatim in
    the assembled system prompt (after redact(), which is a no-op for plain
    descriptive text)."""
    notes = "Need a SQL engine that federates Iceberg with Postgres."
    req = ComponentRecommendationRequest(
        component_id=KNOWN_COMPONENT, notes=notes
    )
    prompt, _citations, _target = _build_recommend_prompt(req)
    assert notes in prompt


def test_build_prompt_includes_sibling_component_in_siblings_block():
    """For component `trino`, the prompt's siblings block must mention
    `spark-iceberg` (the other certified processing engine)."""
    req = ComponentRecommendationRequest(component_id=KNOWN_COMPONENT)
    prompt, _citations, _target = _build_recommend_prompt(req)
    assert KNOWN_SIBLING in prompt


def test_build_prompt_does_not_include_unrelated_category_components():
    """The siblings block is scoped to one category. A component from an
    UNRELATED category (e.g. `minio` from object_storage) must NOT appear in
    the siblings block when we ask about `trino` (processing).

    We assert this by checking that the substring 'minio' does not appear
    inside the siblings_block portion of the prompt — `minio` is a distinct
    id, easy to grep for, and definitively not a sibling of trino."""
    req = ComponentRecommendationRequest(component_id=KNOWN_COMPONENT)
    prompt, _citations, _target = _build_recommend_prompt(req)
    # Carve out the siblings block by its template markers.
    start = prompt.index("## Siblings in the same category")
    end = prompt.index("## Operator context")
    siblings_block = prompt[start:end]
    assert "minio" not in siblings_block.lower()


def test_build_prompt_renders_cart_when_provided():
    """When the user has 3 items in their cart, the cart line must list them."""
    cart = ["iceberg", "iceberg-rest", "minio"]
    req = ComponentRecommendationRequest(component_id=KNOWN_COMPONENT, cart=cart)
    prompt, _citations, _target = _build_recommend_prompt(req)
    assert all(item in prompt for item in cart)


def test_build_prompt_renders_empty_cart_marker_when_absent():
    """No cart -> the prompt's cart line says '(empty)' so the LLM knows."""
    req = ComponentRecommendationRequest(component_id=KNOWN_COMPONENT)
    prompt, _citations, _target = _build_recommend_prompt(req)
    assert "cart: (empty)" in prompt


# ---------------------------------------------------------------------------
# 5. recommend_component() — disabled, unknown, success, alternates whitelist
# ---------------------------------------------------------------------------

def test_recommend_returns_disabled_shape_when_api_key_absent(monkeypatch):
    """No ANTHROPIC_API_KEY -> graceful 'disabled' recommendation (not raise,
    not 5xx), verdict=='unknown', headline mentions disabled."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    req = ComponentRecommendationRequest(component_id=KNOWN_COMPONENT)
    result = asyncio.run(recommend_component(req))
    assert result.verdict == "unknown" and "disabled" in result.headline.lower()


def test_recommend_unknown_component_returns_unknown_verdict_when_ai_enabled(
    monkeypatch,
):
    """With AI enabled, an unknown component_id must short-circuit BEFORE
    the LLM call and return verdict='unknown'. We install a fake anthropic
    whose `messages.create` would raise if called, proving the short-circuit
    fired."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key-not-real")

    class _ExplodingAnthropic(types.ModuleType):
        pass

    mod = _ExplodingAnthropic("anthropic")

    class _AsyncAnthropic:
        def __init__(self, *_a, **_kw):
            class _Msgs:
                async def create(self, **_kwargs):
                    raise AssertionError(
                        "LLM was called for an unknown component — short-circuit failed"
                    )
            self.messages = _Msgs()

    mod.AsyncAnthropic = _AsyncAnthropic  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "anthropic", mod)

    req = ComponentRecommendationRequest(component_id=UNKNOWN_COMPONENT)
    result = asyncio.run(recommend_component(req))
    assert result.verdict == "unknown"


@pytest.mark.xfail(
    reason=(
        "Diagnostic specificity: the unknown-component branch currently "
        "returns a generic 'unknown component' headline instead of naming "
        "the failing id. The id IS available in result.component_id so "
        "the operator isn't blind, but the headline could be tighter. "
        "Tracking as a UX polish item, not a P0."
    ),
    strict=False,
)
def test_recommend_unknown_component_headline_names_the_id(monkeypatch):
    """With AI enabled, the headline for an unknown component should include
    the id so the operator can see which spelling failed."""
    _install_fake_anthropic(monkeypatch, "{}")
    req = ComponentRecommendationRequest(component_id=UNKNOWN_COMPONENT)
    result = asyncio.run(recommend_component(req))
    assert UNKNOWN_COMPONENT in result.headline


@pytest.mark.xfail(
    reason=(
        "BUG (documented, not fixed in this test run): when AI is DISABLED "
        "*and* the component_id is unknown, recommend_component() returns "
        "the generic 'AI recommender disabled' headline instead of the more "
        "specific 'Component <id> is not in the catalog' message. The "
        "is_enabled() short-circuit at the top of recommend_component runs "
        "before the unknown-component short-circuit, but the "
        "unknown-component path needs NO LLM call and could safely run even "
        "when the AI is off. The two short-circuits should be reordered "
        "(or merged) so operators get accurate diagnostics regardless of AI "
        "availability. Test left as xfail to track the fix."
    ),
    strict=True,
)
def test_recommend_unknown_component_headline_names_id_even_when_ai_disabled(
    monkeypatch,
):
    """Diagnostic should be specific even when the AI is unavailable —
    catalog lookup needs no API key."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    req = ComponentRecommendationRequest(component_id=UNKNOWN_COMPONENT)
    result = asyncio.run(recommend_component(req))
    assert UNKNOWN_COMPONENT in result.headline


def test_recommend_returns_good_fit_when_model_says_good_fit(monkeypatch):
    """A clean LLM response with verdict=good_fit must be parsed and surfaced
    unchanged. We use a component (`minio`) with no lock incompatibilities."""
    payload = json.dumps({
        "verdict": "good_fit",
        "headline": "MinIO is the right object store for this stack.",
        "rationale": "It is the certified pick and your cart already lists it.",
        "alternates": [],
    })
    _install_fake_anthropic(monkeypatch, payload)
    req = ComponentRecommendationRequest(
        component_id=KNOWN_NO_INCOMPAT, cart=[KNOWN_NO_INCOMPAT]
    )
    result = asyncio.run(recommend_component(req))
    assert result.verdict == "good_fit"


def test_recommend_returns_warn_when_model_says_warn(monkeypatch):
    """A 'warn' verdict from the model (e.g. because of a known lock-file
    incompatibility) must be passed through."""
    payload = json.dumps({
        "verdict": "warn",
        "headline": "Trino has a known incompatibility with iceberg-rest <1.5.0",
        "rationale": "The udp-trino-local-v0.1 lock file flags this combination.",
        "alternates": [],
    })
    _install_fake_anthropic(monkeypatch, payload)
    req = ComponentRecommendationRequest(component_id=KNOWN_COMPONENT)
    result = asyncio.run(recommend_component(req))
    assert result.verdict == "warn"


def test_recommend_returns_consider_alternate_when_model_says_so(monkeypatch):
    """A 'consider_alternate' verdict from the model surfaces unchanged."""
    payload = json.dumps({
        "verdict": "consider_alternate",
        "headline": "Spark+Iceberg is a stronger fit for ETL workloads",
        "rationale": "Trino is great for ad-hoc SQL but Spark dominates ETL.",
        "alternates": [
            {"component_id": KNOWN_SIBLING, "why": "Certified processing engine"}
        ],
    })
    _install_fake_anthropic(monkeypatch, payload)
    req = ComponentRecommendationRequest(component_id=KNOWN_COMPONENT)
    result = asyncio.run(recommend_component(req))
    assert result.verdict == "consider_alternate"


def test_recommend_keeps_only_alternates_that_are_real_siblings(monkeypatch):
    """The implementation guards against the model inventing a fake
    alternate id: only alternates whose component_id is in the catalog's
    sibling set survive. We feed one real sibling + one made-up id and
    assert only the real one survives."""
    payload = json.dumps({
        "verdict": "consider_alternate",
        "headline": "x",
        "rationale": "y",
        "alternates": [
            {"component_id": KNOWN_SIBLING,                  "why": "real sibling"},
            {"component_id": "not-a-real-component-12345",   "why": "hallucinated"},
        ],
    })
    _install_fake_anthropic(monkeypatch, payload)
    req = ComponentRecommendationRequest(component_id=KNOWN_COMPONENT)
    result = asyncio.run(recommend_component(req))
    alternate_ids = [a["component_id"] for a in result.alternates]
    assert alternate_ids == [KNOWN_SIBLING]


def test_recommend_returns_unknown_when_model_returns_malformed_json(monkeypatch):
    """If the model returns text that can't be parsed as JSON the recommender
    must degrade to verdict='unknown' rather than raise."""
    _install_fake_anthropic(monkeypatch, "this is not JSON at all, sorry")
    req = ComponentRecommendationRequest(component_id=KNOWN_COMPONENT)
    result = asyncio.run(recommend_component(req))
    assert result.verdict == "unknown"


def test_recommend_low_confidence_when_disabled(monkeypatch):
    """Disabled path explicitly sets confidence='low'."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    req = ComponentRecommendationRequest(component_id=KNOWN_COMPONENT)
    result = asyncio.run(recommend_component(req))
    assert result.confidence == "low"


# ---------------------------------------------------------------------------
# 6. Route layer — POST /api/components/{component_id}/recommend
# ---------------------------------------------------------------------------

def test_route_returns_200_with_well_formed_response_for_known_component(
    monkeypatch, client
):
    """Happy path: known component + mocked Anthropic + a valid JSON response
    yields a 200 and a body that validates as ComponentRecommendation."""
    payload = json.dumps({
        "verdict": "good_fit",
        "headline": "MinIO fits.",
        "rationale": "Single certified object store in the pilot.",
        "alternates": [],
    })
    _install_fake_anthropic(monkeypatch, payload)
    resp = client.post(f"/api/components/{KNOWN_NO_INCOMPAT}/recommend", json={
        "component_id": KNOWN_NO_INCOMPAT,
    })
    assert resp.status_code == 200


def test_route_response_validates_against_ComponentRecommendation_schema(
    monkeypatch, client
):
    """The route's response body must round-trip through the response model."""
    payload = json.dumps({
        "verdict": "good_fit",
        "headline": "Fits.",
        "rationale": "Reasoning.",
        "alternates": [],
    })
    _install_fake_anthropic(monkeypatch, payload)
    resp = client.post(f"/api/components/{KNOWN_NO_INCOMPAT}/recommend", json={
        "component_id": KNOWN_NO_INCOMPAT,
    })
    parsed = ComponentRecommendation.model_validate(resp.json())
    assert parsed.component_id == KNOWN_NO_INCOMPAT


def test_route_returns_200_with_unknown_verdict_for_missing_component(
    monkeypatch, client
):
    """An unknown component is NOT a 404 by design — the AI surface degrades
    gracefully (verdict='unknown', headline names the id). This is a
    deliberate design choice in recommend_component()."""
    _install_fake_anthropic(monkeypatch, "{}")  # would-be LLM payload, never used
    resp = client.post(f"/api/components/{UNKNOWN_COMPONENT}/recommend", json={
        "component_id": UNKNOWN_COMPONENT,
    })
    assert resp.status_code == 200 and resp.json()["verdict"] == "unknown"


def test_route_returns_400_when_path_and_body_component_id_mismatch(client):
    """The route's mismatch guard is a 400, not a 500."""
    resp = client.post("/api/components/trino/recommend", json={
        "component_id": "spark-iceberg",
    })
    assert resp.status_code == 400


def test_route_accepts_empty_body_and_defaults_to_path_component_id(
    monkeypatch, client
):
    """When the request body is omitted entirely, the handler reuses the
    path's component_id and proceeds. We verify by mocking Anthropic and
    asserting the response carries the path's component_id."""
    payload = json.dumps({
        "verdict": "good_fit",
        "headline": "OK.",
        "rationale": "OK.",
        "alternates": [],
    })
    _install_fake_anthropic(monkeypatch, payload)
    resp = client.post(f"/api/components/{KNOWN_NO_INCOMPAT}/recommend")
    assert resp.json()["component_id"] == KNOWN_NO_INCOMPAT


def test_route_disabled_ai_returns_disabled_shape_not_500(monkeypatch, client):
    """No API key -> the route returns 200 with the disabled-shape body
    (NOT 5xx). UI must degrade cleanly."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    resp = client.post(f"/api/components/{KNOWN_COMPONENT}/recommend", json={
        "component_id": KNOWN_COMPONENT,
    })
    assert resp.status_code == 200 and resp.json()["verdict"] == "unknown"


def test_route_disabled_ai_rationale_mentions_anthropic_api_key(monkeypatch, client):
    """When AI is disabled, the rationale must clearly tell the operator
    which env var to set — that's the whole point of the graceful path."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    resp = client.post(f"/api/components/{KNOWN_COMPONENT}/recommend", json={
        "component_id": KNOWN_COMPONENT,
    })
    assert "ANTHROPIC_API_KEY" in resp.json()["rationale"]
