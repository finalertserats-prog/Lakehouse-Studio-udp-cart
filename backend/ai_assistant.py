"""Grounded AI assistant for the success screen — "Ask Studio".

Pure additive scaffold (v0.4.x). The assistant answers questions about an
install by building a grounded system prompt from real project context:

  - The certified lock file for the install's stack (compatibility.lock_summary)
  - The install's recent state (steps, error, redacted)
  - A summary of the error_explainer pattern catalog (known failure modes)
  - docs/COMPATIBILITY.md policy text

It then calls the Anthropic API (claude-haiku-4-5-20251001 — cheap, fast)
and returns the answer plus citations to the lock file / error pattern /
doc paragraphs used.

Graceful degradation EVERYWHERE: if ANTHROPIC_API_KEY is missing, the
`anthropic` package isn't installed, the API returns an error, or the
network is unreachable — the assistant returns a clear "AI unavailable"
ChatResponse instead of crashing the route.

Privacy invariants:
  - The ANTHROPIC_API_KEY value is never logged.
  - User-supplied install state is run through `redact()` before it enters
    the prompt. No raw env values, passwords, tokens, or webhook URLs reach
    the LLM.
  - The system prompt forbids claims of real-time access — the model only
    sees a snapshot prepared on this server at request time.
"""
from __future__ import annotations
import logging
import os
import time
from pathlib import Path
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, field_validator

from .compatibility import lock_summary, list_locks
from .config import ROOT
from .error_explainer import _PATTERNS as _ERROR_PATTERNS
from .redact import redact
from .state import store


log = logging.getLogger("lhs.ai")


# ---- public model name + env knobs ----

# Anthropic model — fast + cheap; the assistant is a UI helper, not a coder.
MODEL_NAME = "claude-haiku-4-5-20251001"

# Soft caps on prompt-side input. Pydantic enforces these on the request.
_MAX_QUESTION_CHARS = 2000
_MAX_HISTORY_TURNS = 5

# Generation knobs — operator-tunable via env so admins can scale up for
# complex queries or scale down for tighter SLOs. Safe fallbacks on any
# parse error so a typo in env never crashes the AI panel.
def _env_int(name: str, default: int) -> int:
    try:
        return max(1, int(os.environ.get(name, default)))
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return max(0.5, float(os.environ.get(name, default)))
    except (TypeError, ValueError):
        return default


_MAX_OUTPUT_TOKENS = _env_int("LHS_AI_MAX_OUTPUT_TOKENS", 700)        # ~300 words + padding
_RESPONSE_TIMEOUT_SEC = _env_float("LHS_AI_RESPONSE_TIMEOUT_SEC", 30.0)  # Anthropic client timeout

# Cached docs/COMPATIBILITY.md content. Re-read at startup time only.
_COMPAT_DOC_PATH = ROOT / "docs" / "COMPATIBILITY.md"


# ---- Pydantic schemas (consumed by main.py routes) ----

class ChatMessage(BaseModel):
    role: Literal["user", "assistant"]
    content: str = Field(min_length=1, max_length=_MAX_QUESTION_CHARS)
    ts: float
    citations: list[str] = Field(default_factory=list)


class ChatRequest(BaseModel):
    install_id: Optional[str] = None
    question: str = Field(min_length=1, max_length=_MAX_QUESTION_CHARS)
    history: list[ChatMessage] = Field(default_factory=list)

    @field_validator("history")
    @classmethod
    def _cap_history(cls, v: list[ChatMessage]) -> list[ChatMessage]:
        if len(v) > _MAX_HISTORY_TURNS * 2:
            # Conservatively keep the most-recent N turns (user+assistant = 2 entries).
            return v[-(_MAX_HISTORY_TURNS * 2):]
        return v


class Citation(BaseModel):
    kind: Literal["lock", "error_pattern", "doc", "install_state"]
    label: str
    detail: Optional[str] = None


class SuggestedAction(BaseModel):
    label: str
    kind: Literal["route", "url", "hint"] = "hint"
    target: Optional[str] = None


class ChatResponse(BaseModel):
    answer: str
    citations: list[Citation] = Field(default_factory=list)
    confidence: Literal["high", "medium", "low"] = "medium"
    suggested_actions: list[SuggestedAction] = Field(default_factory=list)
    model: Optional[str] = None


# ---- enablement / status ----

def is_enabled() -> bool:
    """True iff the assistant can plausibly be called.

    Requires both an API key in the env AND the `anthropic` package importable.
    Either missing -> the assistant returns a graceful "unavailable" response.
    """
    if not os.environ.get("ANTHROPIC_API_KEY"):
        return False
    try:
        import anthropic  # noqa: F401
        return True
    except ImportError:
        return False


def status() -> dict[str, Any]:
    """Public view for /api/ai/status — no secrets, just presence + model."""
    enabled = is_enabled()
    return {
        "enabled": enabled,
        "model": MODEL_NAME if enabled else None,
        # Help operators self-diagnose why it's off without leaking the key value.
        "reason": None if enabled else _disabled_reason(),
    }


def _disabled_reason() -> str:
    if not os.environ.get("ANTHROPIC_API_KEY"):
        return "ANTHROPIC_API_KEY env var not set"
    try:
        import anthropic  # noqa: F401
        return ""
    except ImportError:
        return "anthropic Python package not installed (pip install anthropic>=0.40.0)"


# ---- prompt assembly ----

# Catalog of known error categories, derived from error_explainer._PATTERNS.
# This is a STATIC summary — pattern code is the source of truth; we just
# describe the categories so the LLM knows what failures Studio recognizes.
_ERROR_CATEGORY_SUMMARY: str = ""


def _build_error_catalog_summary() -> str:
    """One short line per known error category for the system prompt."""
    global _ERROR_CATEGORY_SUMMARY
    if _ERROR_CATEGORY_SUMMARY:
        return _ERROR_CATEGORY_SUMMARY
    lines: list[str] = []
    seen: set[str] = set()
    for pat in _ERROR_PATTERNS:
        cat = pat.get("category", "")
        if cat in seen:
            continue
        seen.add(cat)
        title = pat.get("title", "")
        lines.append(f"- {cat}: {title}")
    _ERROR_CATEGORY_SUMMARY = "\n".join(lines)
    return _ERROR_CATEGORY_SUMMARY


def _load_compat_doc() -> str:
    """docs/COMPATIBILITY.md, cached after first read. Truncated to keep prompt small."""
    try:
        text = _COMPAT_DOC_PATH.read_text(encoding="utf-8")
    except Exception as e:
        log.warning("ai: failed to load COMPATIBILITY.md: %s", e)
        return ""
    # Cap at ~8 KB — the doc is short but the cap is defensive.
    if len(text) > 8192:
        text = text[:8192] + "\n…(truncated)"
    return text


def _summarize_lock_for_prompt(lock: dict[str, Any]) -> str:
    """Compact, human-readable view of a lock summary for the LLM prompt."""
    out: list[str] = []
    out.append(f"Stack: {lock.get('stack_id')}  version: {lock.get('version_id')}")
    out.append(f"Status: {lock.get('status')}  certified_at: {lock.get('certified_at')}")
    notes = lock.get("status_notes")
    if notes:
        out.append(f"Notes: {notes}")
    out.append("Certified components (image:tag):")
    for c in (lock.get("components") or [])[:32]:
        cid = c.get("id", "?")
        nm = c.get("name", cid)
        out.append(f"  - {cid} ({nm}): {c.get('image')}:{c.get('tag')}")
    incompat = lock.get("incompatible") or []
    if incompat:
        out.append("Known-incompatible combinations:")
        for entry in incompat[:8]:
            combo = " + ".join(entry.get("combination") or [])
            reason = (entry.get("reason") or "")[:160]
            out.append(f"  - {combo} :: {reason}")
    hreq = lock.get("host_requirements") or {}
    if hreq:
        flat = ", ".join(f"{k}={v}" for k, v in hreq.items())
        out.append(f"Host requirements: {flat}")
    return "\n".join(out)


def _summarize_install_for_prompt(install_id: str) -> tuple[str, Optional[dict[str, Any]]]:
    """(redacted human summary, raw record-as-dict-or-None)."""
    rec = store.get(install_id)
    if rec is None:
        return f"(no install record found for install_id={install_id})", None
    parts: list[str] = []
    parts.append(f"install_id: {rec.install_id}")
    parts.append(f"stack: {rec.stack_id}  state: {rec.state}  host: {rec.host}")
    if rec.lake_name:
        parts.append(f"lake_name: {rec.lake_name}")
    if rec.goal:
        parts.append(f"goal: {rec.goal}")
    if rec.cart:
        parts.append(f"cart: {', '.join(rec.cart)}")
    # Steps (just status + title, no inline log lines)
    parts.append("steps:")
    for s in rec.steps[:32]:
        msg = redact((s.message or "")[:200]) if s.message else ""
        suffix = f"  // {msg}" if msg else ""
        parts.append(f"  - [{s.status}] {s.id}: {s.title}{suffix}")
    if rec.error:
        parts.append(f"last_error: {redact(rec.error)[:500]}")
    return "\n".join(parts), rec.model_dump()


_SYSTEM_PROMPT_TEMPLATE = """You are "Studio Assistant", an in-app helper for Lakehouse Studio. \
You answer questions from the operator who is looking at the install \
success screen.

GROUNDING RULES (do not break these):
1. Answer ONLY from the SNAPSHOT below — the certified lock file, the install \
   state, the error-pattern catalog, and the COMPATIBILITY.md excerpt. \
   You DO NOT have real-time access to the running stack, the registries, \
   the host, or the internet.
2. When the question is about certified versions, CITE the lock file for the \
   stack and quote the exact image:tag rather than guessing.
3. When the question is about a failure, MAP it to a category in the \
   error_explainer catalog if one matches, and reference that category by name.
4. If the SNAPSHOT does not contain the answer, say so. NEVER invent an image \
   tag, a constraint, or a fix step that isn't in the snapshot.
5. Keep answers under 300 words unless the operator explicitly asks for more \
   detail. Use short paragraphs and at most one fenced code block.
6. Refuse to discuss anything outside Lakehouse Studio / data-lakehouse ops.

OUTPUT STYLE:
- Plain prose. No markdown headings, no emojis, no marketing language.
- When you cite something, refer to it by short label like "(lock: udp-local-v0.2)" \
  or "(error_pattern: port_conflict)" or "(doc: COMPATIBILITY.md)" inline.

CONFIDENCE:
- "high" — answer is directly supported by the snapshot
- "medium" — answer is inferred from the snapshot
- "low" — snapshot is sparse; you're hedging

=========================== SNAPSHOT (read-only) ===========================

## Compatibility policy excerpt (docs/COMPATIBILITY.md)
{compat_doc}

## Known install-failure categories (Studio's error_explainer)
{error_catalog}

## Certified lock files installed on this server
{available_locks}

## Lock file in scope for THIS conversation
{scoped_lock}

## Install record in scope (redacted)
{install_state}

============================ END SNAPSHOT ============================
"""


def _build_system_prompt(install_id: Optional[str]) -> tuple[str, list[Citation], Optional[dict]]:
    """Returns (system_prompt, citations_for_grounding_sources, raw_record_or_None)."""
    citations: list[Citation] = []
    scoped_lock_text = "(no install_id supplied — no stack scoped)"
    install_text = "(no install_id supplied)"
    raw_rec: Optional[dict] = None

    if install_id:
        install_text, raw_rec = _summarize_install_for_prompt(install_id)
        if raw_rec is not None:
            citations.append(Citation(
                kind="install_state",
                label=f"install:{install_id}",
                detail=f"state={raw_rec.get('state')}",
            ))
            stack_id = raw_rec.get("stack_id")
            if stack_id:
                lock = lock_summary(stack_id)
                if lock is not None:
                    scoped_lock_text = _summarize_lock_for_prompt(lock)
                    citations.append(Citation(
                        kind="lock",
                        label=f"lock:{stack_id}",
                        detail=f"version={lock.get('version_id')} status={lock.get('status')}",
                    ))
                else:
                    scoped_lock_text = f"(no lock file found for stack '{stack_id}')"

    compat_doc = _load_compat_doc()
    if compat_doc:
        citations.append(Citation(
            kind="doc", label="doc:COMPATIBILITY.md",
            detail=f"{len(compat_doc)} chars loaded",
        ))

    error_catalog = _build_error_catalog_summary()
    # Always cite the error catalog — it's part of every prompt.
    citations.append(Citation(
        kind="error_pattern", label="error_catalog",
        detail=f"{error_catalog.count(chr(10)) + 1} categories",
    ))

    prompt = _SYSTEM_PROMPT_TEMPLATE.format(
        compat_doc=compat_doc or "(unavailable)",
        error_catalog=error_catalog or "(empty)",
        available_locks=", ".join(list_locks()) or "(none)",
        scoped_lock=scoped_lock_text,
        install_state=install_text,
    )
    return prompt, citations, raw_rec


# ---- suggested-action inference ----

def _infer_actions(question: str, raw_rec: Optional[dict]) -> list[SuggestedAction]:
    """Heuristic-only — derive a handful of useful next-step buttons.

    These never auto-trigger anything; the UI renders them as click hints.
    """
    actions: list[SuggestedAction] = []
    ql = question.lower()
    if raw_rec is None:
        return actions
    install_id = raw_rec.get("install_id")
    state = raw_rec.get("state")
    # On a failed install, offer the diagnose route — it returns the
    # error_explainer verdict for the operator's UI.
    if state == "FAILED":
        actions.append(SuggestedAction(
            label="Run diagnose",
            kind="route",
            target=f"/api/installs/{install_id}/diagnose",
        ))
    # If the operator asks about logs, point them at the WS endpoint.
    if "log" in ql or "logs" in ql:
        actions.append(SuggestedAction(
            label="Open live logs",
            kind="route",
            target=f"/api/installs/{install_id}/logs",
        ))
    # If the operator asks about upgrade / version bump, surface the upgrades route.
    if "upgrade" in ql or "bump" in ql or "newer version" in ql:
        stack_id = raw_rec.get("stack_id")
        if stack_id:
            actions.append(SuggestedAction(
                label="See upgrade candidates",
                kind="route",
                target=f"/api/stacks/{stack_id}/upgrades",
            ))
    # Sizing questions -> sizing route
    if "size" in ql or "sizing" in ql or "ram" in ql or "memory" in ql or "cpu" in ql:
        stack_id = raw_rec.get("stack_id")
        if stack_id:
            actions.append(SuggestedAction(
                label="See sizing recommendation",
                kind="route",
                target=f"/api/stacks/{stack_id}/sizing",
            ))
    return actions[:4]


# ---- main entry point ----

def _disabled_response() -> ChatResponse:
    """Graceful 'AI unavailable' shape returned whenever we cannot call out."""
    return ChatResponse(
        answer=(
            "AI assistant is disabled — set ANTHROPIC_API_KEY env var "
            "(and `pip install anthropic>=0.40.0`) on the Studio server to enable."
        ),
        citations=[],
        confidence="low",
        suggested_actions=[],
        model=None,
    )


async def ask(req: ChatRequest) -> ChatResponse:
    """Answer a question from the success screen, grounded in real project context."""
    if not is_enabled():
        return _disabled_response()

    # Build the grounded prompt + collect grounding citations.
    try:
        system_prompt, citations, raw_rec = _build_system_prompt(req.install_id)
    except Exception as e:
        log.exception("ai: prompt build failed: %s", e)
        return ChatResponse(
            answer=f"Couldn't build a grounded prompt: {type(e).__name__}. "
                   "The assistant is degraded — please retry later.",
            citations=[], confidence="low",
            suggested_actions=[], model=None,
        )

    # Compose the message list — history (capped) + the new question.
    messages: list[dict[str, str]] = []
    for m in req.history[-(_MAX_HISTORY_TURNS * 2):]:
        # Defensively redact any historical content too — operators may have
        # pasted snippets of logs into past turns.
        messages.append({"role": m.role, "content": redact(m.content)})
    messages.append({"role": "user", "content": redact(req.question)})

    # Lazy import — keeps `is_enabled()` honest and avoids ImportError at module load.
    try:
        import anthropic  # type: ignore
    except ImportError:
        return _disabled_response()

    try:
        client = anthropic.AsyncAnthropic(
            api_key=os.environ["ANTHROPIC_API_KEY"],
            timeout=_RESPONSE_TIMEOUT_SEC,
        )
        resp = await client.messages.create(
            model=MODEL_NAME,
            max_tokens=_MAX_OUTPUT_TOKENS,
            system=system_prompt,
            messages=messages,
        )
    except Exception as e:
        # NEVER log the API key value. Log only the exception class + message.
        log.warning("ai: Anthropic call failed: %s: %s", type(e).__name__, e)
        return ChatResponse(
            answer=(
                "AI assistant is temporarily unavailable "
                f"({type(e).__name__}). Please retry in a moment. "
                "Studio's other features are unaffected."
            ),
            citations=citations, confidence="low",
            suggested_actions=[], model=MODEL_NAME,
        )

    # Extract plain-text answer from the response. The SDK returns a list of
    # content blocks; we concatenate any text blocks.
    answer_parts: list[str] = []
    try:
        for block in resp.content:
            if getattr(block, "type", "") == "text":
                answer_parts.append(getattr(block, "text", ""))
    except Exception:
        pass
    answer = ("".join(answer_parts) or "").strip()
    if not answer:
        answer = ("The model returned an empty response. "
                  "Try rephrasing the question.")

    # Confidence inference: if we had no install scope AND only doc/error
    # catalog were available, downgrade to "medium". If the model itself
    # said "I don't know" / "not in the snapshot", drop to "low".
    confidence: Literal["high", "medium", "low"] = "high"
    al = answer.lower()
    if any(p in al for p in ("don't know", "not in the snapshot",
                              "no information", "cannot answer",
                              "unavailable in the snapshot")):
        confidence = "low"
    elif raw_rec is None:
        confidence = "medium"

    actions = _infer_actions(req.question, raw_rec)

    return ChatResponse(
        answer=answer,
        citations=citations,
        confidence=confidence,
        suggested_actions=actions,
        model=MODEL_NAME,
    )
