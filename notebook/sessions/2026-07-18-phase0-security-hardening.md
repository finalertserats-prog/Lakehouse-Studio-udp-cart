# Session: 2026-07-18 — Phase 0 security hardening (VPS re-verified)

Executed the enterprise-roadmap **Phase 0 hardening** pass on the freshly-synced v0.6.2 codebase. Four security items shipped and were re-certified live on the Finalert VPS (not just unit-green); the fifth (egress control) was deliberately deferred by the user because it's blocked on a larger prerequisite.

The through-line of the session was the user's standing rule — **"everything stable, not compromise"** — applied to security work: every change to the certified run path was proven on real installs before being trusted, and the design of each item defaults to OFF / byte-identical so existing certified stacks can't regress. The re-verify earned its keep: it caught a silent coverage gap that unit tests had passed clean (see Patterns).

## What Was Done

### Commits landed (4, in order — all pushed to `finalertserats-prog/Lakehouse-Studio-udp-cart` `main`)
1. `bb88c6a` feat(security): **P0.3** — AI provisioning opt-in (default-off gate) — `backend/ai_provisioner.py` +32, `backend/main.py` +23, `tests/test_ai_provision_gate.py` NEW (75) — 130 insertions
2. `6483aa7` feat(security): **P0.2** — container runtime hardening (no-new-privileges) — `backend/compose_hardening.py` NEW (89), `backend/runner.py` +83, `tests/test_compose_hardening.py` NEW (172) — 344 insertions
3. `fc7a815` fix(security): **P0.2** — cover raw-argv stacks + Codex-flagged robustness — `backend/runner.py` (+99/-18), `tests/test_compose_hardening.py` +63 — 144 insertions
4. `5bf0493` feat(security): **P0.4b** — opt-in per-install credential generation (default off) — `backend/credential_gen.py` NEW (41), `backend/runner.py` +56, `tests/test_credential_gen.py` NEW (110) — 207 insertions

(P0.1 compose-safety validator and P0.4 secret-scan CI gate had landed in the prior session; this session completed P0.2 / P0.3 / P0.4b and closed out Phase 0 minus the deferred P0.5.)

### P0.3 — AI-provisioning opt-in gate (default OFF)
AI-driven provisioning lets an LLM generate configs + shell commands that run against Docker, so it must be unreachable unless an operator explicitly opts in.
- `ai_provisioner.provisioning_status() -> (enabled, reason)`: true only when `LHS_AI_PROVISION_ENABLED` is truthy **AND** an LLM API key is configured.
- Gated the three executing handlers — `ai-provision/start`, `stack-builder/build`, `image-build/start` — with a `403 + reason` when disabled.
- New public `GET /api/ai-provision/availability` so the UI can disable the AI-Build action instead of surfacing a 403 only after the click.
- 14 tests (status logic + endpoint 403 wiring).

### P0.2 — container runtime hardening
Harden the certified run path **without** re-serializing the cloned base `docker-compose.yml` (its StarRocks `command` heredocs are fragile). Done via a compose **override** file merged with `-f`.
- `backend/compose_hardening.py` (pure): `build_harden_overlay(names, strict=)`. Default = `security_opt: ["no-new-privileges:true"]` on every service (safe for all certified stacks). Strict (opt-in `LHS_HARDEN_STRICT`) adds `cap_drop:[ALL]` + minimal `cap_add` + `pids_limit` — OFF by default because blanket cap-drop needs per-stack verification (HDFS/StarRocks).
- `runner._write_harden_overlay()` writes `docker-compose.harden.yml` and registers it LAST with `services:[]` so it modifies existing services without touching the `up -d` list. `_effective_service_names()` enumerates the effective service set. Disable with `LHS_HARDEN_RUNTIME_DISABLED`. Recovered across retries in `_reconstruct_overlays_from_disk()`.
- **Coverage-gap fix (`fc7a815`):** raw-argv start stacks (enterprise-hadoop / streaming / techsophy run `bash -c "docker compose up …"` with no explicit `-f`) never received the overlay → 0/21 hardened despite the file being written. Fixed by setting `COMPOSE_FILE=<base>:docker-compose.harden.yml` for the start step; compose ignores it when explicit `-f` is present, so the UDP-clone path is unaffected. Also folded in Codex-review fixes: `_effective_service_names` now parses the actual overlay **files** (union of service keys) instead of trusting per-overlay metadata; dedupe of the `harden` entry; reconstruct honors the disable flag.

### P0.4b — opt-in per-install credential generation (default OFF)
The stacks ship a public demo MinIO secret (`udp_admin_12345`) identical on every install.
- `backend/credential_gen.py` (pure): `generate_secret()` (40 hex chars = 160 bits, quoting-safe) + the canonical demo literal + flag constant.
- `runner._step_env`: when `LHS_GENERATE_CREDENTIALS` is set (and no explicit override), generate `MINIO_ROOT_PASSWORD` into the merged env → flows to `.env` → compose → the MinIO server and every `${MINIO_ROOT_PASSWORD:-…}` consumer.
- `runner._rotate_install_credential`: after all env-step writers run, sweep the install dir and replace the **unique** demo literal across every text artifact (bootstrap/smoke scripts, patched compose, StarRocks conf injection, configs). Plain string replace — no YAML round-trip, so heredocs survive; skips binaries/`.git`; username `admin` deliberately left alone.
- Default OFF → certified path byte-identical.

### VPS re-verification (live on srv1541349 — `ssh -i ~/.ssh/lhs_deploy root@187.127.139.234`)
VPS was found **already populated** (36 cached images, 263 GB free) — NOT wiped as memory suggested — so re-verify was fast. Each stack installed with `install_harness.py --no-teardown`, proof captured, then torn down (install→verify→delete, one at a time, never overworking the host).

| Stack | Compose path | P0.2 proof | P0.4b proof |
|---|---|---|---|
| udp-local-v0.2 (StarRocks) | UDP-clone + `docker_compose_up` | 7/7 `no-new-privileges`, smoke ✅ | secret rotated (40-hex, ≠ default), smoke ✅ |
| enterprise-hadoop-v1.0 (HDFS/Ranger) | raw-argv (`COMPOSE_FILE` fix) | 0/21 → **21/21** after fix, smoke ✅ | — |
| iceberg-polaris-spark-local-v0.1 (OAuth2) | UDP-clone + fragment | 6/6, smoke ✅ | — |
| iceberg-nessie-trino-local-v0.1 (Trino s3) | UDP-clone + fragment | — | rotated, Trino round-trip through MinIO ✅ (smoke green on re-run) |

Every lighter stack is a capability subset of these three (both compose paths + the capability-sensitive HDFS stack), so uniform-transform hardening is proven safe across the fleet.

### Codex review
Ran `codex_review.sh --last` on the P0.2 commit in parallel with the VPS install. It flagged real robustness issues (best-effort enumeration, duplicate overlay entries, metadata-vs-file-contents, missing edge-case tests) — all folded into `fc7a815` before finalizing.

### Memory writes
- `memory/project_p05_egress_needs_prebake.md` — P0.5 deferred; needs package pre-bake (below).
- `memory/MEMORY.md` — added the P0.5 pointer; corrected the stale "VPS wiped 2026-05-18" note (it's re-populated: 36 images, 263 GB free as of 2026-07-17).

## Key Decisions Made

- **Every Phase 0 item defaults OFF / byte-identical.** P0.3 gate (403 unless opted in), P0.2 strict cap-drop (opt-in), P0.4b credential gen (opt-in). The default no-new-privileges hardening is the one exception that's on-by-default — because it's the universally-safe subset, proven non-breaking on the hardest stacks. Rationale: a security change to the certified run path must not be able to regress an existing install; opt-in makes the blast radius zero until an operator chooses otherwise.

- **Harden via a compose override file, not by editing the base compose.** The cloned UDP `docker-compose.yml` carries StarRocks `command` heredocs that a YAML round-trip would mangle. A `-f docker-compose.harden.yml` override merges by service name and leaves the base text untouched. `COMPOSE_FILE` extends the same mechanism to stacks whose start command doesn't pass explicit `-f`.

- **P0.4b rotates via a post-write install-dir sweep of a UNIQUE literal, not a 30-site edit.** The password `udp_admin_12345` is unambiguous (unlike the username `admin`), and every consumer artifact is written during the env step, so one text sweep after all writers catches bootstrap bash, smoke scripts, patched compose, StarRocks conf injection, and configs at once — far lower risk than threading a generated value through ~30 hardcoded sites.

- **Strict cap-drop stays opt-in.** Blanket `cap_drop:[ALL]` on HDFS/StarRocks without per-stack verification would trade real stability for nominal hardening — exactly the compromise the "stable, not compromise" rule forbids. Shipped as `LHS_HARDEN_STRICT` with a documented "needs per-stack verify" caveat.

- **A red smoke was investigated, not hand-waved.** The first nessie run failed on `StarRocks ERROR 1064: Failed to find backend to execute`. Rather than assume it was the credential change, checked the failure: the Trino round-trip through MinIO s3 had already succeeded (proving rotation worked on the Trino path), and the error was BE-liveness, not s3-auth. Confirmed transient by a clean re-run (1m05s vs the failed 40s — the BE hadn't registered in time).

- **P0.5 (egress control) deferred by the user.** Default-deny egress is architecturally incompatible with the current bootstrap model (every Spark stack runs `spark-submit --packages`, downloading Maven jars at runtime; `internal:true` breaks all of them). The real fix is pre-baking packages into images; a cheap opt-in `LHS_RESTRICT_EGRESS` flag was offered but the user chose to defer entirely. Recorded in memory.

## What's Pending / Next Steps

- **P0.5 egress control** — the only remaining Phase 0 item. Prerequisite: pre-bake all Maven/pip packages into the stack images so nothing is fetched at runtime; only then can egress-deny be default-on and safe for every stack. See `memory/project_p05_egress_needs_prebake.md`. Pick up in a dedicated infra session.

- **Rotate the shipped demo credentials before any exposed/production deployment.** P0.4b makes rotation *possible* (`LHS_GENERATE_CREDENTIALS=1`), but default installs still ship `udp_admin_12345`, `*_pilot`, `marquez`, `udp_admin_…`. Enable the flag or set real overrides, and rotate the remaining internal-only DB creds (HMS/ranger/postgres) — P0.4b currently rotates only the host-exposed MinIO secret.

- **Extend credential generation beyond MinIO (optional follow-up).** The sweep mechanism generalizes to a `{literal: generated}` map; the internal-only DB passwords are lower-priority (not host-exposed) but could be rotated the same way.

## Patterns Learned

- **Unit-green ≠ verified for run-path changes.** P0.2's unit suite (546 passed) was fully green, yet enterprise-hadoop came up **0/21 hardened** on the VPS — the overlay file was written but never referenced, because that stack's start command is a raw argv that never got `-f`. The gap was invisible to unit tests (which asserted the overlay was *written* and *registered*, not that compose *consumed* it) and only surfaced under `docker inspect` on a live install. Rule reinforced: for changes to the certified run path, "the transform ran" must be proven at the point of effect (running-container state), not just at the point of code.

- **Verify the hardest representative cases, not the whole fleet.** A uniform code transform applied identically to every stack doesn't need all 12 re-certified — it needs the cases that stress the transform's edges: both compose paths (docker_compose_up vs raw-argv) and the capability-sensitive stack (HDFS). udp-local + enterprise-hadoop + polaris + nessie covered every edge; the rest are strict subsets. This respected "never overwork the VPS" while giving real confidence.

- **Investigate a red result before attributing blame.** The instinct on a smoke failure during a security change is to suspect the change. The discipline is to read the failure tail: the Trino s3 round-trip had already passed (rotation proof), and the actual error was orthogonal (StarRocks BE registration timing). A clean re-run confirmed it. Attributing the failure to the change without checking would have triggered a wrong "fix" to working code.

- **Codex review in parallel with a long VPS wait is free leverage.** The install took several minutes of pure waiting; running `codex_review.sh --last` during that window surfaced four robustness fixes that landed in the same follow-up commit. Zero added wall-clock.

## Files Changed

### Source (committed + pushed to `finalertserats-prog/Lakehouse-Studio-udp-cart` `main`)
- `backend/ai_provisioner.py` — P0.3 gate logic (`bb88c6a`)
- `backend/main.py` — P0.3 endpoint gates + availability endpoint (`bb88c6a`)
- `backend/compose_hardening.py` — NEW, P0.2 pure overlay builder (`6483aa7`)
- `backend/credential_gen.py` — NEW, P0.4b pure secret generator (`5bf0493`)
- `backend/runner.py` — P0.2 overlay wiring + coverage fix + P0.4b generation & sweep (`6483aa7`, `fc7a815`, `5bf0493`)
- `tests/test_ai_provision_gate.py` — NEW (`bb88c6a`)
- `tests/test_compose_hardening.py` — NEW + extended (`6483aa7`, `fc7a815`)
- `tests/test_credential_gen.py` — NEW (`5bf0493`)

### Notebook + memory (this session)
- `notebook/sessions/2026-07-18-phase0-security-hardening.md` — this file
- `memory/project_p05_egress_needs_prebake.md` — new
- `memory/MEMORY.md` — updated (P0.5 pointer + VPS-state correction)

### VPS side-effects (not source, no git artifact)
- Installed + verified + torn down: udp-local-v0.2, enterprise-hadoop-v1.0, iceberg-polaris-spark-local-v0.1, iceberg-nessie-trino-local-v0.1 (nessie twice — flake re-run)
- VPS left clean (0 Studio containers), repo synced to `5bf0493`

## Test State
Full suite: **559 passed**, 4 skipped, 1 xfailed, 1 xpassed. New tests this session: P0.3 (14), P0.2 (18), P0.4b (8).
