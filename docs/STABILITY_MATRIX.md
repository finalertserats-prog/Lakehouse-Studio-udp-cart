# Stability Matrix

> Honest, at-a-glance view of which Lakehouse Studio stacks have **real install evidence** versus which ship as `candidate` (image tags verified on the registry, but never plugged in end-to-end).

This is the operator-facing companion to [`COMPATIBILITY.md`](COMPATIBILITY.md). That doc explains *how* a stack gets certified (the promotion ladder, the lock-file contract, the evidence record format). This doc shows *where every stack stands today* on that ladder — and what's still needed to move each one up.

**How to read this doc:**

- Skim the [stack matrix](#stack-matrix) for the one-line status per stack
- Cross-check the [OS coverage](#os-coverage-matrix) before picking a stack for a real install
- Check the [component compatibility grid](#component-compatibility-matrix) when picking the *combination* (table format × catalog) for a new stack proposal
- Read [what "candidate" really means](#what-candidate-really-means) before ever installing one
- Use the [promotion checklist](#promotion-checklist) if you want to turn a candidate into `pilot-stable`

---

## The 4-tier maturity ladder

Quoted verbatim from `stacks/compatibility/udp-local-v0.2.lock.yaml` (lines 31-34 + the candidate clarification from the trino lock):

```
candidate       — image tags verified present on registry via `docker manifest inspect`;
                  combination NOT installed end-to-end; evidence[] is empty
pilot-stable    — install completes, lakehouse readable, smoke may need Skip on some OS
linux-stable    — verified end-to-end on a Linux VPS, smoke passes clean
production      — additional hardening (TLS, backups, RBAC) certified
```

The lock file's `status:` field on each stack is the authoritative source. Studio's `/api/stacks/{id}/lock-summary` route exposes this verbatim so the UI can show it without re-interpreting.

---

## Stack matrix

| Stack ID | Table Format | Catalog | Processing | Serving | BI | Status | Last Evidence | OS Verified | What's Needed to Promote |
|---|---|---|---|---|---|---|---|---|---|
| `udp-local-v0.2` | Iceberg | Iceberg-REST | Spark | StarRocks | — | **pilot-stable** | 2026-05-16 (Windows Docker Desktop, `inst_0d13069722`) | Win11 + Docker Desktop (with smoke-step Skip caveat) | Linux droplet evidence → `linux-stable` |
| `udp-trino-local-v0.1` | Iceberg | Iceberg-REST | Trino | StarRocks | — | candidate | none | none | Windows + Linux install evidence; bootstrap + smoke already written |
| `iceberg-nessie-trino-local-v0.1` | Iceberg | Nessie | Trino | StarRocks | — | candidate | none | none | Image-tag-verify + install evidence (Windows + Linux) |
| `hudi-hms-spark-local-v0.1` | Hudi | HMS + Postgres | Spark | — | — | candidate (TBD — lock file not yet written) | none | none | Build `lakehousestudio/spark-hudi:3.5.0_0.15.0` image, ship bake script, then install evidence |
| `delta-hms-spark-trino-local-v0.1` | Delta | HMS + Postgres | Spark + Trino | — | — | candidate (TBD — lock file not yet written) | none | none | Build `lakehousestudio/spark-delta:3.5.0_3.2.1` image, ship bake script, then install evidence |
| `iceberg-polaris-spark-local-v0.1` | Iceberg | Polaris + Postgres | Spark | StarRocks | — | candidate (TBD — lock file not yet written) | none | none | Image-tag-verify (`apache/polaris:1.0.1`) + install evidence |

**TBD rows** mark stacks whose `.yaml` manifest is in flight from a sibling subagent. When their `stacks/compatibility/<stack-id>.lock.yaml` files land, this table should be updated in the same commit that closes out the manifest work.

---

## OS coverage matrix

Legend:

- ✅ verified — install completes end-to-end with evidence recorded in the lock file
- ⚠ partial — install completes but at least one step needs a documented Skip (root cause logged)
- ❌ untested — never run on this OS, no evidence either way
- 🚫 known broken — runtime failure reproduced and recorded in `incompatible[]`

| Stack | Win11 + Docker Desktop | macOS + Docker Desktop | Ubuntu 22.04 | RHEL 9 |
|---|---|---|---|---|
| `udp-local-v0.2` | ⚠ (smoke Skip — StarRocks→MinIO via AWS SDK fails; lakehouse data IS reachable via Spark) | ❌ | ❌ (likely-works per lock file, not verified) | ❌ |
| `udp-trino-local-v0.1` | ❌ | ❌ | ❌ | ❌ |
| `iceberg-nessie-trino-local-v0.1` | ❌ | ❌ | ❌ | ❌ |
| `hudi-hms-spark-local-v0.1` | ❌ | ❌ | ❌ | ❌ |
| `delta-hms-spark-trino-local-v0.1` | ❌ | ❌ | ❌ | ❌ |
| `iceberg-polaris-spark-local-v0.1` | ❌ | ❌ | ❌ | ❌ |

**Today's reality:** only `udp-local-v0.2` × Win11 has any green-ish cell. Everything else is untested. The Windows `⚠` on v0.2 is documented in detail in `udp-local-v0.2.lock.yaml` under `evidence[0].smoke_failure_root_cause` — the lakehouse data is readable via Spark (proven with a real `SELECT * FROM udp.curated.demo_customer_summary` returning AMER/APAC/EMEA rows), only the StarRocks BE → MinIO query path is broken on Docker Desktop's Windows network stack.

---

## Component compatibility matrix

This is the **table-format × catalog** compatibility surface as expressed in `stacks/components-catalog.yaml` (via each component's `compatible_with` list). Cells:

- ✅ certified-in-stack-X — at least one named stack uses this combination today
- ⚠ engine-supports-but-no-stack — the components can talk per upstream docs, but no Studio stack pins the combination yet
- ❌ incompatible — the components are known not to work together (rare; recorded where applicable)

| Table Format ↓ \ Catalog → | Iceberg-REST | Nessie | HMS + Postgres | Polaris + Postgres |
|---|---|---|---|---|
| **Iceberg** | ✅ certified in `udp-local-v0.2` (pilot-stable) and `udp-trino-local-v0.1` (candidate) | ✅ certified in `iceberg-nessie-trino-local-v0.1` (candidate) | ⚠ Iceberg + HMS is engine-supported (Spark and Trino both can register Iceberg tables in HMS) but no Studio stack pins it | ✅ certified in `iceberg-polaris-spark-local-v0.1` (candidate, manifest TBD) |
| **Hudi** | ❌ Hudi does not have a first-class Iceberg-REST adapter (per `components-catalog.yaml` `hudi.compatible_with`) | ⚠ Nessie has experimental Hudi support upstream, no Studio stack | ✅ certified in `hudi-hms-spark-local-v0.1` (candidate, manifest TBD) | ❌ Polaris is an Iceberg-only catalog |
| **Delta** | ❌ Delta does not target the Iceberg REST spec | ⚠ Delta uniform catalog mode can sit behind Nessie, no Studio stack | ✅ certified in `delta-hms-spark-trino-local-v0.1` (candidate, manifest TBD) | ❌ Polaris is an Iceberg-only catalog |

**Source:** `compatible_with` arrays in `stacks/components-catalog.yaml` for `iceberg`, `hudi`, `delta`, `iceberg-rest`, `nessie`, `hive-metastore`, `polaris`.

"Certified" here means "exists as a recommended_set with a matching `.lock.yaml`," not "has install evidence" — see the [stack matrix](#stack-matrix) above for the evidence view.

---

## Add-on overlays

Three optional components layer on top of **any** of the 6 base stacks:

| Overlay | Component (catalog entry) | Image / Tag | Role | Status |
|---|---|---|---|---|
| Orchestration | `airflow` | `apache/airflow:2.10.4-python3.11` | DAG scheduler | candidate (catalog-listed, not wired into pipeline) |
| Orchestration | `dagster` | `dagster/dagster-celery-docker:1.9.4` | Asset-first scheduler | candidate (catalog-listed, not wired into pipeline) |
| BI | `superset` | `apache/superset:4.1.1` | Self-service BI | candidate (catalog-listed, not wired into pipeline) |

**Important scope note:** these overlays are **not yet wired into `_STUDIO_SCRIPT_SETS`** in `backend/runner.py` (which is what dispatches per-stack bootstrap + smoke scripts). They appear in the component catalog as `Candidate, Optional`, but the install pipeline cannot actually deploy them today — selecting one in the UI is currently a no-op at install time.

Wiring them into the pipeline as compose overrides (same pattern as Caddy TLS / Monitoring / JDBC sidecars — see `COMPATIBILITY.md` § "TLS Sidecar" and § "Monitoring Sidecar") is **v0.6 work**, tracked separately from the 6-stack expansion.

---

## What "candidate" really means

A `candidate` stack is **not installable end-to-end yet**. Concretely, all of the following are true at certification time:

1. **Image tags exist on the registry.** Every component image has been confirmed reachable via `docker manifest inspect <image>:<tag>` on the `certified_at` date. This catches the classic "the tag we documented got deleted by upstream" failure mode (which is exactly what bit `tabulario/spark-iceberg:3.5.1_1.5.2` after UDP shipped — see `udp-local-v0.2.lock.yaml` § `incompatible[0]`).
2. **Pairwise compatibility constraints are documented but not runtime-verified.** The `constraints[]` block in each candidate lock file cites upstream docs (trino.io, iceberg.apache.org, StarRocks GitHub PRs) but those `verified` strings are **documentation citations, not runtime evidence**.
3. **No `docker compose up` has been run** for the candidate combination. Studio's install pipeline has never executed `prepare → clone → env → doctor → start → bootstrap → smoke → finalize` against the candidate's images.
4. **Smoke tests may not exist beyond a stub.** Most candidates ship without a working bootstrap + smoke script pair, or with a stub that needs further work. (Exception: `udp-trino-local-v0.1` already has both — `_STUDIO_TRINO_BOOTSTRAP_SH` and `_STUDIO_TRINO_SMOKE_SH` in `backend/runner.py` — so it's the closest candidate to promotion.)
5. **The lock file's `evidence[]` array is intentionally empty.** Studio's UI surfaces this via `/healthz` as a `recommended_set '<name>': warning — stack '<id>' lock status is 'candidate'` line, on purpose, so operators never silently pick a candidate for a real install.

**To promote a candidate to `pilot-stable` you need both:**

- (a) A successful end-to-end install captured as an `evidence[]` entry in the lock file, with the `install_id`, host metadata, per-step result, and (if smoke needed a Skip) the documented root cause.
- (b) At least one cross-engine query proving the lakehouse is actually readable — e.g., for `udp-local-v0.2` the evidence record includes the literal rows returned by a Spark `SELECT * FROM udp.curated.demo_customer_summary`. The lakehouse must be *demonstrably* readable, not "the install script exited 0."

---

## Promotion checklist

For an operator who wants to take a `candidate` to `pilot-stable`:

1. **Run install end-to-end on at least one host.** Full pipeline `prepare → clone → env → doctor → start → bootstrap → smoke → finalize`. Either Win11 + Docker Desktop or Linux Docker is acceptable for the first evidence record; both are required for `pilot-stable` to be confidently usable cross-OS.
2. **Capture the `install_id` plus the smoke step's full output.** Studio assigns each install a UUID-shaped id (e.g. `inst_0d13069722`) — find it in the install row at `/api/installs`. Save the smoke step's stdout/stderr — including any rows actually returned by a cross-engine SELECT.
3. **Append an `evidence[]` entry** to `stacks/compatibility/<stack-id>.lock.yaml`. Schema mirrors `udp-local-v0.2.lock.yaml` `evidence[0]`:
   ```yaml
   evidence:
     - id: "YYYY-MM-DD-<os-shorthand>"
       timestamp: "YYYY-MM-DDTHH:MM:SS+TZ"
       operator: <your-email>
       host:
         os: "..."
         docker: "..."
         ram_gb: <n>
         cpu_cores: <n>
       via: "Lakehouse Studio v<version> — ..."
       install_id: <id>
       result:
         prepare: passed
         clone: passed
         env: passed
         doctor: passed
         start: passed
         bootstrap: passed
         smoke: passed | failed | passed_after_skip
         finalize: passed | passed_after_skip
       # If smoke failed or was Skipped, REQUIRED:
       smoke_failure_root_cause: |
         <multiline explanation — what failed, why, what was tried, what works>
       # REQUIRED for pilot-stable: proof the lakehouse is readable
       lakehouse_proof_via_<engine>:
         - "<actual row 1 returned>"
         - "<actual row 2 returned>"
   ```
4. **Bump `version_id`** in the lock file. Semver: patch for evidence-only promotion (e.g. `0.1.0` → `0.1.1`), minor for any constraint relaxation, major for a behavioral change.
5. **Flip `status: candidate` → `status: pilot-stable`** in the lock file and update `status_notes` to reference the new evidence id(s).
6. **Bump `certified_at`** to the moment of promotion (ISO-8601 with timezone).
7. **Update the README's "Certified stacks" table** ([`README.md`](../README.md) § Certified stacks) — promote the stack from the candidate row to the pilot-stable badge, and bump the count.

The same checklist applies for `pilot-stable → linux-stable`: just add a second evidence entry from a real Linux VPS install where the smoke step passes clean (no Skips), and flip the status.

---

## Cross-reference

- **[`README.md`](../README.md) § "Certified stacks"** — today this table lists only the 2 stacks that have lock files committed (`udp-local-v0.2` pilot-stable + `udp-trino-local-v0.1` candidate). It should be updated as the remaining 4 stacks land their manifests, and again every time a candidate is promoted out of candidate via the [promotion checklist](#promotion-checklist) above.
- **[`COMPATIBILITY.md`](COMPATIBILITY.md)** — the full promotion-ladder definition, the per-stack certified-component table, the upgrade-planner contract, and the override-file pattern that all the sidecars (Caddy / Monitoring / JDBC) follow.
- **`stacks/compatibility/*.lock.yaml`** — the authoritative source. Anything in this doc that disagrees with a lock file is wrong; the lock file wins.
- **`stacks/components-catalog.yaml`** — the source of truth for component-level `compatible_with` lists that drive the [component compatibility matrix](#component-compatibility-matrix) above.

**When in doubt:** if this doc says a stack is `pilot-stable` but the lock file says `candidate`, trust the lock file and update this doc.
