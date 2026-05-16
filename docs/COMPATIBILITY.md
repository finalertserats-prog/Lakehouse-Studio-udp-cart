# Compatibility Matrix

Studio's central strategic claim is that **the compatibility matrix is the moat** — every version combination in a certified stack has been verified working together, with evidence, and Studio refuses to install combinations that haven't.

This document is the human-readable view. The authoritative source is the per-stack lock files under `stacks/compatibility/`.

## Why this exists

Open-source data lakehouse stacks are notoriously fragile because every component (Iceberg, Spark, StarRocks, MinIO, Hive Metastore, Trino…) ships on its own release cadence and the pairwise compatibility surface is huge. A clean `bash install.sh` last quarter often doesn't work today because:

- A Docker image tag was deleted from the registry
- A component's healthcheck broke when the image base changed
- A breaking change shipped in a "patch" release upstream
- A subtle env-var requirement was added that wasn't documented

We learned this the hard way stabilizing UDP — see [UDP PR #6](https://github.com/finalertserats-prog/Unified-Data-Plug/pull/6) which catalogues 10 distinct issues found in a single end-to-end install on a fresh clone.

**The fix:** treat the verified-working version set as a first-class artifact, not a side-effect of "whatever's in the YAML today."

## Policy

1. **Every certified stack has a lock file** at `stacks/compatibility/<stack-id>.lock.yaml`.
2. **The lock file pins exact tags** — never `latest`, never floating tags like `3.3-latest`.
3. **Studio's installer reads the lock file** and prefers its versions over any user-supplied catalog entry, unless the user explicitly overrides (a v0.4 feature).
4. **Bumping a version requires an evidence entry** — a full end-to-end install on a fresh clone, with `prepare → finalize` all green (or documented `Skip` with root cause).
5. **Adding a new component requires updating constraints** — pairwise compatibility rules between the new component and every existing one, with evidence.
6. **Recording incompatibility is as important as recording compatibility** — the `incompatible` block in each lock file documents combinations we KNOW are broken so they're never re-tried.

## How to propose a version bump

1. Edit `stacks/compatibility/<stack-id>.lock.yaml`:
   - Update the component's `tag`
   - Update `verified` reasoning under the affected `constraints`
   - Bump `version_id` (semver — patch for tag bump, minor for component swap, major for behavioral change)
2. Update the matching component entry in `stacks/components-catalog.yaml`.
3. Run a full Studio install end-to-end on a fresh clone.
4. Append an `evidence` block with the install_id, host details, and step-by-step result.
5. If you discover a NEW incompatible combination during the bump (e.g. the new version doesn't work with one existing component), record it in the `incompatible` block — that learning is the most valuable artifact.

## Current certified stacks

### `udp-local-v0.2` — Unified Data Plug (local Docker)

| Component | Image | Tag | Verified |
|---|---|---|---|
| MinIO | `minio/minio` | `RELEASE.2025-04-22T22-12-26Z` | ✅ |
| MinIO Client (mc) | `minio/mc` | `RELEASE.2025-04-16T18-13-26Z` | ✅ |
| Iceberg REST | `tabulario/iceberg-rest` | `1.6.0` | ✅ |
| Spark + Iceberg | `tabulario/spark-iceberg` | `3.5.5_1.8.1` | ✅ |
| StarRocks FE | `starrocks/fe-ubuntu` | `3.3.12` | ✅ |
| StarRocks BE | `starrocks/be-ubuntu` | `3.3.12` | ✅ |

**Status:** `pilot-stable` — installs to READY on Windows + Docker Desktop; Linux verification pending → `linux-stable`.

**Caveat (Windows only):** StarRocks BE → MinIO SQL query path hits a documented AWS SDK + Docker Desktop network interaction. Lakehouse is built correctly (Spark can read everything); only the StarRocks SELECT path fails. Documented + tracked in [`udp-local-v0.2.lock.yaml`](../stacks/compatibility/udp-local-v0.2.lock.yaml).

## What "validating before finalizing" means in practice

When a contributor proposes adding a new component (e.g. swapping Spark for Trino, or adding Dagster as an orchestrator), the workflow is:

1. **Research phase** — check the component's upstream docs + GitHub issues + relevant Stack Overflow / forum threads for known compatibility issues with the components in our existing stack. Document what you find.
2. **Tag selection** — verify the proposed image tag actually exists on the registry (`docker manifest inspect <image>:<tag>`). Pin to a specific patch version, never a floating tag.
3. **Local validation** — run a full install end-to-end on a fresh clone of Studio. All steps green or documented `Skip` with root cause.
4. **Lock file update** — add the new component to the relevant `lock.yaml` with full constraint table.
5. **Evidence record** — append the install_id + result.

**Skipping these steps means the bump is rejected** — even if the change "looks correct."

## Future automation (v0.4 roadmap)

- **Studio compatibility-check at install time:** verify every image tag in the lock file still exists on the registry before starting the install (catch removed tags upfront, not 5 minutes into a `docker compose up`).
- **Nightly canary run:** CI workflow that runs the full install on the certified stack against current registries, alerting if a previously-working tag has disappeared.
- **Compatibility solver UI:** when the cart screen shows alternates ("coming soon: Trino, Flink"), the underlying compatibility matrix gates them — clicking a non-validated combination shows a clear "this combination is not certified; here's what you'd need to validate" prompt.
- **PR-driven matrix expansion:** community contributors submit lock-file updates as PRs with evidence; merged PRs flow into the next Studio release's catalog.
