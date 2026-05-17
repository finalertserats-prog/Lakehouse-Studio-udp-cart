# Graduation Runbook

> Per-stack checklist for promoting a `candidate` lock to `pilot-stable`. The product promise is honesty: nothing crosses the candidate → pilot-stable line without real end-to-end install evidence. This doc is the operator-facing complement to [`STABILITY_MATRIX.md`](STABILITY_MATRIX.md) (current state) and [`COMPATIBILITY.md`](COMPATIBILITY.md) (rules of the road).

**How to read this doc:** each candidate stack has its own section with the exact gates that need to close. Tick each box, append an `evidence[]` entry to the matching lock file, bump `version_id`, flip `status` to `pilot-stable`, and update [`STABILITY_MATRIX.md`](STABILITY_MATRIX.md). The pattern mirrors the promotion checklist in `STABILITY_MATRIX.md` § "Promotion checklist".

**One bedrock rule, restated for every section:** the `evidence[]` array in the lock file is append-only and authoritative. If this doc disagrees with a lock file, trust the lock file and update this doc.

---

## udp-trino-local-v0.1 — candidate

**Lock file:** [`stacks/compatibility/udp-trino-local-v0.1.lock.yaml`](../stacks/compatibility/udp-trino-local-v0.1.lock.yaml)
**Components:** Iceberg + Iceberg-REST + MinIO + Trino + StarRocks (FE+BE)

### Gates to pilot-stable

- [ ] Run install end-to-end on Windows 11 + Docker Desktop. Capture `install_id` from `/api/installs`.
- [ ] Run install end-to-end on Linux (Ubuntu 22.04). Capture `install_id`.
- [ ] Smoke test passes on both: Trino runs `SELECT * FROM iceberg.curated.demo_customer_summary` and returns the expected 3 region rows (AMER/APAC/EMEA); StarRocks runs the same SELECT against the shared Iceberg-REST catalog.
- [ ] Append `evidence[]` entry to the lock file with both `install_id`s, host metadata, per-step pass/fail, and the literal rows returned by Trino's SELECT (the `lakehouse_proof_via_trino` block).
- [ ] If the smoke step needed a Skip, document `smoke_failure_root_cause` verbatim from the install log.
- [ ] Bump `version_id` 0.1.0 → 0.1.1 (semver patch — evidence-only promotion).
- [ ] Flip `status: candidate` → `status: pilot-stable` and update `status_notes`.
- [ ] Bump `certified_at` to ISO-8601 + timezone of the promotion moment.
- [ ] Update [`STABILITY_MATRIX.md`](STABILITY_MATRIX.md) row for this stack and the README's "Certified stacks" table.

### Expected time
20 minutes on a 16 GB Linux box. The Trino heap config (3 GB `-Xmx`, 1.5 GB per-query cap) is already pinned in `env_defaults` of the manifest — no tuning needed.

### Marriages already source-verified
8 — Trino↔Iceberg-REST (REST spec + format-version), Trino↔MinIO (path-style + credentials), StarRocks↔Iceberg-REST (PR #55416), StarRocks FE↔BE, StarRocks↔MinIO (env-var trio), Trino↔StarRocks (shared catalog endpoint). All grounded in upstream docs per the lock file's `constraints[].verified` strings.

### Marriages requiring install proof
- That the candidate's bootstrap + smoke script pair (`_STUDIO_TRINO_BOOTSTRAP_SH` + `_STUDIO_TRINO_SMOKE_SH` in `backend/runner.py`) run cleanly against this exact image-tag combination.
- That the Windows AWS-SDK→MinIO `UnknownHostException` caveat from `udp-local-v0.2` reproduces (or doesn't) in this stack — same StarRocks version, same MinIO, different processing engine.

### Known caveats while still candidate
The bootstrap script writes `/etc/trino/catalog/iceberg.properties` at install time; on Windows Docker Desktop the bind-mount path semantics are different from Linux and the bootstrap must run via `docker exec` (not via host file write). If the install fails at the catalog-properties step on Windows, that's the most likely culprit.

---

## iceberg-nessie-trino-local-v0.1 — candidate

**Lock file:** [`stacks/compatibility/iceberg-nessie-trino-local-v0.1.lock.yaml`](../stacks/compatibility/iceberg-nessie-trino-local-v0.1.lock.yaml)
**Components:** Iceberg + Nessie + MinIO + Trino + StarRocks (FE+BE)

### Gates to pilot-stable

- [ ] **GATE 3 first.** Add a Nessie persistence backend (Postgres or RocksDB) to the lock and the compose file before any install. Nessie 0.99 defaults to in-memory persistence; a container restart drops every commit, branch, and table pointer — only the MinIO data survives. The lock file calls this out under the `[nessie]` durability constraint and the `[nessie:0.99.0 (in-memory persistence), production posture]` incompatible entry. Without GATE 3 closure, the install is dev-only by definition.
- [ ] Run install end-to-end on Windows 11 + Docker Desktop with the post-GATE-3 lock. Capture `install_id`.
- [ ] Run install end-to-end on Linux (Ubuntu 22.04). Capture `install_id`.
- [ ] Smoke test passes on both: Spark (external writer) creates an Iceberg table via Nessie's `/iceberg/main` REST endpoint, writes 3 region rows; Trino reads the same table; StarRocks reads the same table — all via the shared Nessie branch URL.
- [ ] Restart Nessie container mid-test; verify the table survives (this is the GATE 3 acceptance test).
- [ ] Append `evidence[]` entry to the lock file with both `install_id`s, host metadata, per-step result, and the Trino + StarRocks proof rows.
- [ ] Bump `version_id` 0.1.0 → 0.1.1.
- [ ] Flip `status: candidate` → `status: pilot-stable` and update `status_notes` to reference the new evidence id(s).
- [ ] Bump `certified_at`.
- [ ] Update [`STABILITY_MATRIX.md`](STABILITY_MATRIX.md) and README.

### Expected time
35 minutes the first time (most of it is GATE 3 work: adding the persistence backend and a port-published-only-internal compose binding). Subsequent installs follow the same shape as the udp-trino candidate.

### Marriages already source-verified
16 — Trino↔Nessie (branch-URL contract, REST spec v1), Nessie↔Trino (min-client version), Spark-writer↔Nessie↔Trino (warehouse-path agreement), Trino↔MinIO (path-style), StarRocks↔Nessie (PR #55416 + catalog-type), StarRocks FE↔BE, StarRocks↔MinIO (env-var trio), Trino↔StarRocks↔Nessie (warehouse parity), Nessie↔Trino↔StarRocks (Iceberg format-version v2), Trino↔Nessie (TLS/auth posture for local dev), StarRocks↔Nessie (wire shape), Trino↔MinIO (credential propagation), StarRocks↔MinIO (S3 properties), Nessie (durability), Trino↔MinIO↔Nessie (port-binding uniqueness), Nessie↔Trino↔StarRocks (volume contract).

### Marriages requiring install proof
- That Nessie's `/iceberg/main` round-trips cleanly between an external Spark writer and the in-stack Trino + StarRocks readers.
- That StarRocks 3.3.12 accepts Nessie's branch-scoped URI verbatim (same shape as plain Iceberg-REST URI, never runtime-checked against Nessie).
- That the post-GATE-3 persistence-backed Nessie survives a container restart with all branches intact.

### Known caveats while still candidate
The Windows AWS-SDK→MinIO `UnknownHostException` from `udp-local-v0.2` is likely to also bite this stack (same StarRocks version, same MinIO surface); record it under `smoke_failure_root_cause` if the install hits it.

---

## hudi-hms-spark-local-v0.1 — candidate

**Lock file:** [`stacks/compatibility/hudi-hms-spark-local-v0.1.lock.yaml`](../stacks/compatibility/hudi-hms-spark-local-v0.1.lock.yaml)
**Components:** Hudi + Hive Metastore + Postgres + MinIO + Spark+Hudi

### Gates to pilot-stable

- [ ] **GATE 1 first — bake the Studio image.** `lakehousestudio/spark-hudi:3.5.0_0.15.0` does not exist on any registry yet. Promotion requires:
   - [ ] Commit `scripts/images/Dockerfile.spark-hudi` (followup work — Dockerfile not yet written). The image must bundle `hudi-spark3.5-bundle_2.12-0.15.0.jar` (pinned exactly, no `latest`) + Hive Metastore client jars + S3A path-style config + `hoodie.datasource.hive_sync.use_jdbc=false` defaults.
   - [ ] Bake the image through Studio's release pipeline.
   - [ ] Push to `docker.io/lakehousestudio/spark-hudi:3.5.0_0.15.0`.
   - [ ] Record the resulting digest SHA in the lock alongside the tag.
- [ ] **GATE 2 — pin `bitsondatadev/hive-metastore` to a digest SHA.** The current `latest` tag is acceptable only while the stack is candidate; promotion requires lifting it to the digest the verifying host pulled.
- [ ] Run install end-to-end on Windows 11 + Docker Desktop. Capture `install_id`.
- [ ] Run install end-to-end on Linux (Ubuntu 22.04). Capture `install_id`.
- [ ] Smoke test passes on both: Spark+Hudi runs `CREATE TABLE … USING HUDI` and a `MERGE INTO`, the table syncs to HMS (visible via `SHOW TABLES`), and a read-back through Spark+Hudi returns the merged rows.
- [ ] Restart Postgres container mid-test; verify HMS schema and table pointers survive (the named-volume contract from the `[hive-metastore, postgres] VOLUME MOUNT` constraint).
- [ ] Append `evidence[]` entry to the lock file with both `install_id`s, host metadata, per-step result, and the Spark+Hudi proof rows from a `SELECT * FROM hudi_table` after the MERGE.
- [ ] Bump `version_id` 0.1.0 → 0.1.1.
- [ ] Flip `status: candidate` → `status: pilot-stable` and update `status_notes`.
- [ ] Bump `certified_at` and fill in the `certified_on` os/docker/docker_compose (currently `TBD`).
- [ ] Update [`STABILITY_MATRIX.md`](STABILITY_MATRIX.md) and README.

### Expected time
45 minutes the first time — GATE 1 image bake dominates. Subsequent installs after the image is published follow the same 20-25 minute shape as the udp-trino candidate.

### Marriages already source-verified
13 — HMS↔Postgres (schema-init dependency), Spark+Hudi↔HMS (Thrift port 9083), Spark+Hudi↔HMS↔Postgres (triangle dependency), Spark+Hudi↔MinIO (path-style + endpoint), HMS↔MinIO (catalog-only contract), Spark+Hudi↔HMS (table-format ↔ catalog version range), Spark+Hudi (jar-bundle parity), Spark+Hudi↔HMS (Thrift wire compatibility), HMS↔Postgres (driver + Postgres major-version range), HMS↔Spark+Hudi (TLS/auth boundary), Spark+Hudi↔MinIO (credential propagation), Spark+Hudi↔HMS↔Postgres↔MinIO (port binding uniqueness), HMS↔Postgres (volume mount contract).

### Marriages requiring install proof
- That Spark 3.5.0 + Hudi 0.15.0 in the Studio-built image syncs cleanly to `bitsondatadev/hive-metastore:latest` (or the to-be-pinned digest) on first write.
- The exact set of `hoodie.*` properties required to make MERGE INTO + HMS-sync work as a pair.
- That HMS-on-Postgres schema-init runs without the deadlock-on-first-start failure mode the bitsondatadev image is occasionally reported to hit.

### Known caveats while still candidate
The image is not on any registry. Any install attempt will fail at `docker compose up` with a pull error until GATE 1 closes — this is documented in the lock's `incompatible[]` block.

---

## delta-hms-spark-trino-local-v0.1 — candidate

**Lock file:** [`stacks/compatibility/delta-hms-spark-trino-local-v0.1.lock.yaml`](../stacks/compatibility/delta-hms-spark-trino-local-v0.1.lock.yaml)
**Components:** Delta + Hive Metastore + Postgres + MinIO + Spark+Delta + Trino

### Gates to pilot-stable

- [ ] **GATE 1 first — bake the Studio image.** `lakehousestudio/spark-delta:3.5.0_3.2.1` does not exist on any registry yet. Promotion requires:
   - [ ] Commit `scripts/images/Dockerfile.spark-delta` (followup work). The image must bundle `delta-spark_2.12-3.2.1.jar` + `delta-storage-3.2.1.jar` (pinned exactly) + Hive Metastore client jars + S3A path-style config + bake `spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension` and `spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog` into `spark-defaults.conf` (the silent-failure mode in the `delta without DeltaSparkSessionExtension` incompatible entry).
   - [ ] Bake the image through Studio's release pipeline.
   - [ ] Push to `docker.io/lakehousestudio/spark-delta:3.5.0_3.2.1`.
   - [ ] Record the resulting digest SHA in the lock alongside the tag.
- [ ] **GATE 2 — pin `bitsondatadev/hive-metastore` to a digest SHA.** Same caveat as the Hudi candidate.
- [ ] Run install end-to-end on Windows 11 + Docker Desktop. Capture `install_id`.
- [ ] Run install end-to-end on Linux (Ubuntu 22.04). Capture `install_id`.
- [ ] Smoke test passes on both: Spark+Delta runs `CREATE TABLE … USING DELTA`, writes 3 region rows, syncs to HMS; Trino reads the same table via its `delta-lake` connector pointed at the same HMS; readback returns identical rows from both engines.
- [ ] Verify the HMS-recorded table type is `delta` (not generic Parquet) — the `DeltaSparkSessionExtension` silent-failure mode is the most-common candidate-stage bug.
- [ ] Restart Postgres container mid-test; verify HMS schema and Delta table pointers survive (same named-volume contract as the Hudi candidate).
- [ ] Append `evidence[]` entry to the lock file with both `install_id`s, host metadata, per-step result, AND the rows returned by both Spark+Delta and Trino.
- [ ] Bump `version_id` 0.1.0 → 0.1.1.
- [ ] Flip `status: candidate` → `status: pilot-stable` and update `status_notes`.
- [ ] Bump `certified_at` and fill in `certified_on`.
- [ ] Update [`STABILITY_MATRIX.md`](STABILITY_MATRIX.md) and README.

### Expected time
50 minutes the first time — GATE 1 image bake plus the additional Trino-delta-lake verification step. Subsequent installs are 25-30 minutes (the extra reader engine adds smoke-test surface).

### Marriages already source-verified
17 — HMS↔Postgres (schema-init), Spark+Delta↔HMS (CREATE TABLE … USING DELTA pattern), Trino↔HMS (delta-lake connector requirements), Spark+Delta↔Trino↔HMS (triangle dependency), Spark+Delta↔MinIO (path-style), Trino↔MinIO (delta-lake connector S3 config), Trino↔MinIO (credentials), Spark+Delta↔Trino (Delta protocol versions), Spark+Delta (jar-bundle parity), Spark+Delta↔HMS (CATALOG ⇄ PROCESSING ENGINE config), Trino↔HMS (CATALOG ⇄ SERVING wire), Trino↔HMS↔Spark+Delta (TLS/auth boundary), HMS↔Postgres (driver + Postgres major-version range), Spark+Delta↔Trino↔MinIO (warehouse path agreement), Spark+Delta↔Trino↔HMS↔Postgres↔MinIO (port binding), HMS↔Postgres (volume mount), Spark+Delta↔Trino (concurrent-writer safety).

### Marriages requiring install proof
- That Spark+Delta with `DeltaSparkSessionExtension` produces HMS-syncable Delta tables (the silent-mismatch failure mode).
- That Trino 475's delta-lake connector reads tables written by Spark+Delta 3.2.1 through the same HMS without schema or protocol-version mismatch.
- That the default Delta protocol pair `(minReaderVersion=1, minWriterVersion=2)` is what Spark+Delta 3.2.1 writes by default (vs. enabling deletion vectors, which would require Trino 442+; we pin 475 so this is fine but the runtime evidence is missing).

### Known caveats while still candidate
The image is not on any registry. Same blocker as the Hudi candidate. Additionally, the port-binding overlap with the Hudi candidate (both use 9083 + 5432 + 9000/9001) is a real footgun for hosts that try to run both stacks side-by-side — the install pre-flight must check for collisions.

---

## iceberg-polaris-spark-local-v0.1 — candidate

**Lock file:** [`stacks/compatibility/iceberg-polaris-spark-local-v0.1.lock.yaml`](../stacks/compatibility/iceberg-polaris-spark-local-v0.1.lock.yaml)
**Components:** Iceberg + Polaris + Postgres + MinIO + Spark+Iceberg + StarRocks (FE+BE)

### Gates to pilot-stable

- [ ] Run install end-to-end on Windows 11 + Docker Desktop. Capture `install_id`.
- [ ] Run install end-to-end on Linux (Ubuntu 22.04). Capture `install_id`.
- [ ] **OAuth2 token flow end-to-end** — the headline contract to validate:
   - [ ] Bootstrap registers MinIO at Polaris's `/api/management/v1/catalogs/{cat}/storage-configs` so credential vending works for every subsequent table.
   - [ ] Spark obtains an OAuth2 token via `oauth2-server-uri=http://polaris:8181/api/catalog/v1/oauth/tokens` + `credential=<client_id>:<client_secret>` (colon form), creates an Iceberg table, writes 3 region rows.
   - [ ] StarRocks obtains its own OAuth2 token (using its own principal or the same), reads the same table via its external Polaris catalog configured with `iceberg.rest.security.type=oauth2` + the matching credential/server-uri/scope properties.
   - [ ] Spark + StarRocks both see the same `studio_catalog.db.table` — no namespace divergence (the single-warehouse contract).
- [ ] Restart Postgres container mid-test; verify the catalog's root principal credentials and registered tables survive (the named-volume contract — Polaris re-bootstrap regenerates the root client_id/client_secret and locks out every existing client).
- [ ] Optional: bump `polaris.authentication.token-ttl` to 24h before running any long ETL smoke query — the default 1h token expiry can bite a BE-side scan that outlives the FE cache (documented `incompatible` entry).
- [ ] Append `evidence[]` entry to the lock file with both `install_id`s, host metadata, per-step result, and the proof rows from both Spark and StarRocks.
- [ ] Bump `version_id` 0.1.0 → 0.1.1.
- [ ] Flip `status: candidate` → `status: pilot-stable` and update `status_notes`.
- [ ] Bump `certified_at` and fill in `certified_on`.
- [ ] Update [`STABILITY_MATRIX.md`](STABILITY_MATRIX.md) and README.

### Expected time
40 minutes the first time. OAuth2 wiring is the bulk of the unknown — pre-built `apache/polaris:1.0.1` works out of the box, but the bootstrap script must inject matching catalog properties on both Spark and StarRocks. After the first successful install the script is reusable.

### Marriages already source-verified
17 — Polaris↔Postgres (bootstrap dependency), Spark↔Polaris (OAuth2 token flow), Polaris↔Spark (storage credential vending), StarRocks↔Polaris (OAuth2 catalog properties), StarRocks↔Polaris (PR #55416), StarRocks FE↔BE, StarRocks↔MinIO (env-var trio), Spark↔StarRocks↔Polaris (warehouse parity), Polaris↔Spark↔StarRocks (table-format ↔ catalog version range), Polaris↔Postgres (schema/driver pin), Polaris↔Spark↔StarRocks (TLS/AUTH boundary + token TTL), Polaris↔Spark (OAuth2 wire details), Polaris↔StarRocks (OAuth2 wire details + scope), Polaris↔MinIO (storage-credential vending pattern), Polaris↔Postgres↔Spark↔StarRocks↔MinIO (port binding), Polaris↔Postgres (volume mount), Spark↔StarRocks↔Polaris (warehouse + namespace agreement).

### Marriages requiring install proof
- That Polaris 1.0.1's OAuth2 client-credentials flow round-trips cleanly with the Iceberg 1.8 client bundled in `tabulario/spark-iceberg:3.5.5_1.8.1`.
- That Polaris's vended FileIO properties are honored verbatim by StarRocks 3.3.12 (vs. catalog-level creds overriding vended creds).
- That Polaris 1.0.1's Postgres schema-init runs cleanly on `postgres:15-alpine` (changelog notes call out 0.x → 1.0 migration changes).
- Whether the Windows AWS-SDK→MinIO `UnknownHostException` from `udp-local-v0.2` reproduces in this stack — same StarRocks + MinIO, different catalog auth path.

### Known caveats while still candidate
Port 8181 collides with `udp-local-v0.2`'s `iceberg-rest:8181` if both stacks try to run on the same host. The install pre-flight check (TBD) must catch this; today it's documented but not enforced. Polaris's 1h default token TTL is a long-ETL footgun — bump it before any production-scale smoke run.

---

## Known-incompatible combinations (will NEVER work)

These combinations are documented across the lock files' `incompatible[]` blocks and the per-component `compatible_with` arrays in `stacks/components-catalog.yaml`. The compatibility explainer (`backend/compat_explainer.py`) auto-detects each one and renders a `wont_work` verdict with the specific source citation.

### Format ⇄ Catalog mismatches (the most-common cart mistakes)

| Cart contains | Reason | Correct alternative |
|---|---|---|
| **Hudi + Iceberg-REST** | Hudi has no first-class Iceberg-REST adapter. Hudi's only certified catalog is Hive Metastore (per `hudi.compatible_with`). | Swap Iceberg-REST for Hive Metastore → matches `hudi-hms-spark-local-v0.1`. |
| **Delta + Iceberg-REST** | Delta does not target the Iceberg REST spec. Delta's certified catalog is Hive Metastore (per `delta.compatible_with`). | Swap Iceberg-REST for Hive Metastore → matches `delta-hms-spark-trino-local-v0.1`. |
| **Delta + Polaris** | Polaris is an Iceberg-only catalog (per `polaris.compatible_with`). | Swap Delta for Iceberg + spark-iceberg → matches `iceberg-polaris-spark-local-v0.1`. |
| **Hudi + Polaris** | Polaris is Iceberg-only; same root cause as Delta + Polaris. | Swap Polaris for Hive Metastore → matches `hudi-hms-spark-local-v0.1`. |
| **Hudi + Nessie** | Nessie has experimental Hudi support upstream, but no Studio stack pins it (per `nessie.compatible_with` listing iceberg-only). | Pick Iceberg if you want Nessie, or Hive Metastore if you want Hudi. |
| **Delta + Nessie** | Same shape — Delta UniForm catalog mode can sit behind Nessie upstream, but no Studio stack pins it. | Pick Iceberg if you want Nessie, or Hive Metastore if you want Delta. |

### Two table formats in one warehouse (silent corruption)

| Cart contains | Reason | Workaround |
|---|---|---|
| **Hudi + Iceberg (same stack)** | Hudi tables and Iceberg tables are distinct formats; visible-by-name but unreadable-by-wrong-engine. No interop layer in Studio. Documented in `hudi-hms-spark-local-v0.1.lock.yaml` `incompatible[]`. | One table format per stack. |
| **Delta + Hudi + Iceberg (same warehouse)** | Three table formats in one warehouse prefix produces a catalog of mixed-format tables. Each engine reads only its own format; the others appear broken. Documented in `delta-hms-spark-trino-local-v0.1.lock.yaml` `incompatible[]`. | One table format per stack. Cross-stack interop requires XTable / Delta UniForm — not covered by any lock. |

### Two catalog servers in one stack (catalog drift)

| Cart contains | Reason | Workaround |
|---|---|---|
| **Iceberg-REST + Nessie** | Two competing Iceberg-REST servers on the same MinIO warehouse leads to two separate catalogs both claiming authority. Clients pointed at one cannot see tables created via the other. Documented in `iceberg-nessie-trino-local-v0.1.lock.yaml` `incompatible[]`. | Pick one. `udp-local-v0.2` uses Iceberg-REST; `iceberg-nessie-trino-local-v0.1` uses Nessie. |
| **Polaris + Iceberg-REST** | Same root cause as Iceberg-REST + Nessie. Polaris and `tabulario/iceberg-rest` are alternatives, not complements. Documented in `iceberg-polaris-spark-local-v0.1.lock.yaml` `incompatible[]`. | Pick one. |

### Image tags that will never resolve

| Cart contains | Reason | Workaround |
|---|---|---|
| **spark:3.5.1_1.5.2** | Tag was REMOVED from Docker Hub after UDP shipped. Documented in `udp-local-v0.2.lock.yaml` and carried forward in every Iceberg-Spark stack's `incompatible[]`. | Bump to `tabulario/spark-iceberg:3.5.5_1.8.1` (the current certified tag). |
| **lakehousestudio/spark-hudi:3.5.0_0.15.0** | Image not yet built — see GATE 1 in `hudi-hms-spark-local-v0.1.lock.yaml`. | Bake the image from the (TBD) `scripts/images/Dockerfile.spark-hudi` before attempting install. |
| **lakehousestudio/spark-delta:3.5.0_3.2.1** | Same — see GATE 1 in `delta-hms-spark-trino-local-v0.1.lock.yaml`. | Bake the image from the (TBD) `scripts/images/Dockerfile.spark-delta` before attempting install. |

### Floating tags that violate the lock-file contract

| Tag pattern | Why it's incompatible | Pin to |
|---|---|---|
| `trino:latest` | Lock-file contract requires a stable tag; `latest` moves with every Trino release. | `trinodb/trino:475` |
| `apache/polaris:latest` | Polaris's OAuth2 token-flow property names have changed during incubation. | `apache/polaris:1.0.1` |
| `nessie:latest` | Nessie's Iceberg-REST adapter URL casing has changed between minor versions. | `ghcr.io/projectnessie/nessie:0.99.0` |
| `starrocks-fe:3.3-latest`, `starrocks-be:3.3-latest` | Historically resolved to 3.3.10 (pre-3.3.12) which has the Iceberg REST FileIO propagation bug fixed by PR #55416. | `starrocks/fe-ubuntu:3.3.12` + `starrocks/be-ubuntu:3.3.12` |
| `postgres:latest` | Major-version bumps change pg_dump/pg_restore output formats and minor SQL behaviors HMS / Polaris schema-init expect. | `postgres:15-alpine` |
| `bitsondatadev/hive-metastore:latest` (in pilot-stable) | OK in the candidate window only. Promotion requires lifting the tag to the verifying host's digest SHA. | The SHA captured during the GATE 2 verification install. |

### Engine-version mismatches that silently produce wrong answers

| Combination | Failure mode | Fix |
|---|---|---|
| **Spark+Delta with `delta.enableDeletionVectors=true` read by `trino:<442`** | Pre-442 Trino silently returns rows that should be soft-deleted — wrong answers, no error. | Keep Trino at 475 or any future numeric tag, AND/OR set `delta.enableDeletionVectors=false` on tables any pre-442 reader must consume. |
| **Hudi 0.15 writer alongside a Hudi 1.0+ writer on the same warehouse** | Mixed timeline-instant formats. MERGE INTO from the 0.15 image can produce corrupt commits. | Single Hudi-version writer per warehouse OR coordinated 0.x → 1.0 cutover. Cross-writer guard rails are a v0.2 followup. |
| **Iceberg format-version=3 readers without Iceberg 1.9+ clients** | StarRocks 3.3.12 (Iceberg 1.5 client) and Trino 475 (Iceberg 1.6 client) both fail to read v3 tables. | Keep writer on Iceberg 1.8.x and pin `format-version=2` on table creation until both reader engines bump. |

### Authentication / posture mismatches

| Combination | Failure mode | Fix |
|---|---|---|
| **Spark/StarRocks clients without `oauth2` properties against Polaris** | Polaris 1.0.1 has authentication enabled by default — no "just turn auth off" path. 401 on every Iceberg-REST call. | Bootstrap MUST inject `rest.auth.type=oauth2` + colon-form credential + full token endpoint URI for both engines. |
| **Polaris with `token-ttl=1h` + long ETL BE query >1h** | Cached token expires mid-scan; BE re-requests S3 credentials via expired token; 401s mid-scan. | Bump `polaris.authentication.token-ttl` to 24h (or longer) for ETL-heavy workloads. |
| **Polaris with ephemeral Postgres volume** | Every restart re-bootstraps Polaris with a NEW root client_id/client_secret. Existing clients get 401s. | Always declare a named volume for `/var/lib/postgresql/data`. |
| **HMS exposed to non-loopback hosts without SASL + TLS** | HMS Thrift on 9083 carries no encryption / no auth — local-dev posture only. Any non-loopback exposure breaks the security claim. | Keep HMS Thrift internal-only OR add SASL + TLS (not covered by current locks). |

### Postgres major-version drift

| Combination | Failure mode | Fix |
|---|---|---|
| **`postgres:16-alpine` + HMS** | Postgres 16 changed default `password_encryption` and tightened SCRAM behaviors. The JDBC driver baked into `bitsondatadev/hive-metastore` (42.x line) works in most paths but schema-init has not been runtime-verified. | Stay on `postgres:15-alpine` (the locked pin) or run schema-init end-to-end and capture evidence before promoting. |
| **`postgres:16-alpine` + Polaris** | Same root cause — JDBC driver compatibility on schema-init / migration paths is not yet verified. | Stay on `postgres:15-alpine`. |
| **Polaris 0.x Postgres data volume mounted into Polaris 1.0.1** | Polaris 1.0 changed the JDBC store schema vs. 0.x. Mounting a 0.x volume into 1.0.1 produces schema-mismatch errors at first start. | Start with a fresh Postgres volume for Polaris 1.0.1. If migrating from 0.x, dump and re-bootstrap. |

---

## How to file an evidence record

The schema is documented inline in `udp-local-v0.2.lock.yaml` `evidence[0]` and mirrored in `STABILITY_MATRIX.md` § "Promotion checklist". Briefly:

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
    via: "Lakehouse Studio v<version> — <UI / CLI>"
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
      <multiline explanation>
    # REQUIRED for pilot-stable: proof the lakehouse is readable
    lakehouse_proof_via_<engine>:
      - "<actual row 1 returned>"
      - "<actual row 2 returned>"
```

If the actual rows differ from the expected set, that's a smoke failure — record it as such with the diff under `smoke_failure_root_cause`. Honest accounting wins.

---

## Cross-reference

- **[`STABILITY_MATRIX.md`](STABILITY_MATRIX.md)** — current state of every stack, OS coverage, component compatibility grid.
- **[`COMPATIBILITY.md`](COMPATIBILITY.md)** — full promotion-ladder definition, per-stack certified-component table, upgrade-planner contract.
- **`stacks/compatibility/*.lock.yaml`** — the authoritative source for every stack's pinned tags, constraints, evidence, and incompatible combinations.
- **`stacks/components-catalog.yaml`** — per-component `compatible_with` lists that drive the catalog-level compatibility graph.
- **`backend/compat_explainer.py`** — the runtime that turns any cart into a plain-English `will_work` / `wont_work` / `untested` verdict pulling from all of the above.
