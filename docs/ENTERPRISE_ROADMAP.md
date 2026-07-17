# Lakehouse Studio — Enterprise-Grade Roadmap

**Status as of 2026-07-17** (post-sync to `manaskiran/LakeHouse-Studio` v0.6.2)

This roadmap is the concrete path from "works and is stable for pilots" to
**enterprise grade**: safe to run untrusted/multi-tenant workloads, hardened
supply chain, evidence-backed stacks, and operable at scale. It is grounded in
the current codebase and the LLM Council review of 2026-07-17.

## Where we are today (honest baseline)

| Dimension | State |
|---|---|
| Test suite | ✅ 498 passed / 0 failed (was 24 failing at sync) |
| Dependencies | ✅ install cleanly (litellm needs a prebuilt wheel via pip ≥ 25) |
| Catalog integrity | ✅ enforced by `scripts/catalog_lint.py` CI gate |
| AI-provisioning RCE | ✅ gated by `backend/ai_safety.py` (LLM output no longer reaches a raw shell) |
| Certified stacks | 🟡 2 pilot-stable (`udp-local-v0.2`, `hudi-hms-spark`), rest `candidate` — no install evidence |
| Auth / RBAC | 🟡 present (SQLite audit + RBAC flags) — not SSO/OIDC, not hardened |
| Sandboxing | 🔴 stacks run on the host Docker daemon with no isolation policy |
| Supply chain | 🔴 images mostly tag-pinned, not digest-pinned; no image scanning |
| HA / DR | 🟡 backup + DR drills exist; single-host only |

Legend: ✅ done · 🟡 partial · 🔴 gap.

---

## Phase 0 — Close the safety boundary (highest priority)

The AI-provisioning RCE is gated, but the sandbox story is not finished. These
are the "expensive if wrong" items.

- **P0.1 — `stack_composer` / `image_builder` input validation.** The compose
  is Studio-generated (good — not user-pasted YAML), but validate the resolved
  plan against a schema before rendering: reject `privileged`, `pid: host`,
  `network_mode: host`, host bind-mounts (esp. `/var/run/docker.sock`, `/`,
  `/etc`), and `cap_add` outside an allowlist. Add the same denylist to
  `image_builder` Dockerfile/base-image handling. *(Reuse the token list already
  in `backend/ai_safety.py`.)*
- **P0.2 — Container runtime policy.** Run generated stacks with
  `--cap-drop=ALL` (+ minimal re-adds), read-only root FS where possible,
  CPU/mem/pids limits, and an isolated bridge network with no host route.
  Investigate rootless Docker / a socket proxy so Studio never hands raw daemon
  access to a stack.
- **P0.3 — Human-in-the-loop for AI plans.** Before any AI-composed stack
  actually launches, show a diff/preview of the resolved compose + commands and
  require explicit confirmation. Default AI provisioning **off** unless an API
  key + explicit opt-in are present.
- **P0.4 — Secrets hygiene.** Source all credentials (litellm/Anthropic keys,
  DB passwords) from env/secret-manager; never log them, never bake them into
  generated configs. Force non-default credential generation at provision time
  for Ranger/Hive/StarRocks/Airflow. Add a secret-scan (`gitleaks`/
  `ecc-agentshield`) CI gate.
- **P0.5 — Egress control** on built/run images to prevent data exfiltration
  from a compromised stack.

## Phase 1 — Prove the stacks (evidence + release gates)

- **P1.1 — E2E smoke evidence** for every `candidate` stack (install → health →
  teardown), recorded as an `evidence[]` record — the same bar that earned
  `udp-local-v0.2` and `hudi` their pilot-stable badges. Prioritize the two new
  headline stacks: `enterprise-hadoop-v1.0` and `streaming-local-v1.0`.
- **P1.2 — Release gates in CI** (per council): block release unless (a) unit
  suite green, (b) `catalog_lint` clean, (c) compose validation passes, (d)
  security scan passes, (e) ≥1 clean install/health run per promoted stack.
- **P1.3 — Digest-pin images.** Move lock files from `image:tag` to
  `image@sha256:…`. `bitsondatadev/hive-metastore:latest` is a known floating
  tag flagged in tests — pin it first.
- **P1.4 — Supply-chain scanning.** Trivy/Grype on every image before it enters
  a stack; `pip-audit` on Python deps; generate an SBOM per release.

## Phase 2 — Enterprise access & governance

- **P2.1 — AuthN.** SSO/OIDC login; httpOnly session cookies; CSRF protection
  on state-changing routes; auth middleware on **all** API endpoints (audit for
  gaps).
- **P2.2 — AuthZ.** Promote the existing RBAC from flags to enforced,
  per-resource policies; wire Ranger/Polaris policy stores for data-plane
  authz; make the audit log tamper-evident (append-only, hash-chained).
- **P2.3 — Multi-tenancy.** Per-tenant network/volume/project-name isolation so
  side-by-side installs cannot see or clobber each other (partly present via
  install-specific `LHS_NET` + named volumes — formalize and test it).
- **P2.4 — Data governance.** Promote OpenLineage lineage + data-quality checks
  out of `candidate`; document retention/PII handling for regulated templates
  (fintech/healthcare).

## Phase 3 — Reliability, observability, scale

- **P3.1 — Observability GA.** Promote Prometheus + Grafana + Loki from
  `candidate` to a real install path; structured JSON logging across the
  backend; alerting rules; per-stack dashboards.
- **P3.2 — HA / multi-host.** Move beyond single-host compose: document (and
  test) a multi-node path; add resource quotas + graceful degradation.
- **P3.3 — DR.** Extend the existing backup/DR drills to full
  restore-from-scratch rehearsals with recorded RTO/RPO.
- **P3.4 — Upgrade/migration paths.** Every lock change ships a migration note;
  test in-place upgrades between certified versions.

## Phase 4 — Quality & release engineering

- **P4.1 — Coverage to 80%+** (unit + integration + E2E) per the project
  standard; add integration tests that actually spin up compose fragments, not
  just unit-test the generator.
- **P4.2 — Overlay-drift audit.** Because the v0.6.2 sync was a manual overlay
  onto an unrelated history, add a periodic diff against upstream to catch
  silent semantic drops (test-passing ≠ full parity).
- **P4.3 — Accessibility.** V0.6 ships a VPAT — keep it current; add automated
  a11y checks to CI.

---

## Suggested sequencing

1. **Now → 2 weeks:** Phase 0 (P0.1–P0.5) — finish the security boundary. This
   is what gates "can we let anyone but a trusted operator use it."
2. **2–6 weeks:** Phase 1 — evidence + release gates + digest pinning. Turns
   "candidate" stacks into trustworthy ones and stops regressions shipping.
3. **1–2 months:** Phase 2 — auth/RBAC/multi-tenancy. Required before any shared
   or customer-facing deployment.
4. **Ongoing:** Phases 3–4 — reliability, observability, coverage, drift audits.

## Definition of "enterprise grade" (exit criteria)

- No untrusted input (user **or** LLM) can reach a host shell, the Docker
  daemon, or an unvalidated compose — enforced by tests.
- Every advertised stack is `pilot-stable` with recorded install evidence and
  digest-pinned, scanned images.
- SSO + enforced RBAC + tamper-evident audit on every endpoint and data plane.
- Multi-tenant isolation, HA path, and rehearsed DR with published RTO/RPO.
- Green CI release gate: unit + catalog-lint + compose-policy + security scan +
  per-stack smoke evidence.
