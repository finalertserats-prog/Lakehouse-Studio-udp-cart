# Notebook Index — Lakehouse Studio

Master map of project knowledge captured across sessions.

## Sessions

- [2026-05-16 — v0.4 + v0.5 + v0.5.1 closure push](sessions/2026-05-16-v04-v05-mega-push.md) — 24 commits, 92 routes, 15 new backend modules, 1 CLI, 2 certified-track stacks, full v0.5.1 hardening pass
- [2026-05-16 (Part B) — post-restart hardening + NotebookLM sync fix](sessions/2026-05-16b-post-restart-hardening-and-notebooklm-sync.md) — catalog-503 fix, RBAC UserPublic split, ws_service_logs RBAC gate, sync-to-drive.py patched for .md→.txt
- [2026-05-16 (Part C) — Insyght connector + destinations framework](sessions/2026-05-16c-insyght-destinations-connector.md) — outbound BI connector, Insyght-specific provisioning (SELECT_PRIV), catalog endpoint exposes destinations[]
- [2026-05-17 — Pilot hardening, doc closure, RBAC tightening (trio push)](sessions/2026-05-17-pilot-hardening-doc-rbac-trio-push.md) — 14 commits, full-team parallel (Codex 7 dispatches + Gemini 3 + 3 sub-agents), pilot scope ~85% → ~90%, RBAC permissions tightened on 31 WRITE-RISK routes
- [2026-05-18 — VPS deep clean + udp-local-v0.2 H1 lock update](sessions/2026-05-18-vps-cleanup-and-h1-lock-update.md) — 3 commits, vps-cleanup.sh shipped (hostname-gated to srv1541349), VPS reclaimed 28 GB, lock file aligned with already-landed fix; 4 memory writes capturing SSH-key-blindness + check-first-then-talk feedback
- [2026-07-18 — Phase 0 security hardening (VPS re-verified)](sessions/2026-07-18-phase0-security-hardening.md) — 4 commits (P0.3 AI-provision gate, P0.2 runtime hardening + raw-argv coverage fix, P0.4b credential generation), all default-off/byte-identical; re-certified live on VPS (34 containers hardened across 3 stacks, MinIO secret rotation proven on 2); P0.5 egress deferred (needs package pre-bake); 559 tests pass

## Where things live

- `notebook/sessions/` — per-day session summaries (this dir's primary content)
- `notebook/.meta/` — machine-readable scores, lint status (created by harness scripts)
- `notebook/memory/` — semantic/procedural/episodic memory (if/when populated)

## Project conventions captured

- **`backend/runner.py` is FROZEN.** Every new feature must layer on top of the stable install pipeline. Override-file pattern for compose changes (caddy_tls / monitoring / jdbc_extras as examples).
- **Lock file as moat.** `stacks/compatibility/<stack-id>.lock.yaml` is the source of truth for certified versions. Status flow: candidate → pilot-stable → linux-stable → production. Promotion requires a real install + evidence block append.
- **Opt-in features default off.** `LHS_RBAC_ENABLED`, `LHS_AUDIT_ENABLED`, `ANTHROPIC_API_KEY` — legacy single-token + no-AI behavior remains the default.
- **API versioning via middleware.** OpenAPI surface stays un-versioned (canonical); `/api/v1/*` is an alias that rewrites scope before routing. Future breaking changes land at `/api/v2/`.
- **Healthz separates warnings from errors** via the `"warning —"` prefix. `errors_count` flips `ok` to false; warnings only surface in the problems list.

## Tool reliability notes

- **Codex via `/codex:rescue`**: silently downgrades sandbox to read-only in this Windows + cross-drive environment despite explicit `workspace-write` request. Fix: `~/.codex/config.toml` → `[sandbox] default_mode = "workspace-write"`. Stuck processes accumulate over time — kill with `powershell Get-Process codex | Stop-Process -Force`.
- **Gemini Flash via gemini CLI**: reliable for read-only review/research, hits workspace-trust issues on D:/ file writes from a /tmp working dir. Best path: code reviews + architecture audits.
- **Claude general-purpose subagents**: the only reliable code-writing path here. Concurrent main.py editors merge cleanly when each adds routes near existing similar routes.
