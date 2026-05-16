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

## Candidate Stacks

A **candidate stack** is one whose lock file carries `status: candidate` rather than `pilot-stable` (or higher). Candidates have had their image tags individually verified on the registry, and their pairwise compatibility constraints have been grounded in upstream documentation, but the COMBINATION has not been installed end-to-end and there is NO evidence record yet. They are surfaced in the UI on purpose — so contributors can pick them up and turn them into certified stacks — but Studio's `/healthz` emits a warning whenever a recommended_set references one, and operators should not pick a candidate for a real install.

The distinction in one line:

- **pilot-stable / linux-stable / production** → "we have run this and have receipts"
- **candidate** → "we have verified the parts exist and the rules look right; nobody has plugged it in yet"

### `udp-trino-local-v0.1` — Unified Data Plug (Trino variant, candidate)

| Component | Image | Tag | Verified present on registry |
|---|---|---|---|
| MinIO | `minio/minio` | `RELEASE.2025-04-22T22-12-26Z` | ✅ |
| MinIO Client (mc) | `minio/mc` | `RELEASE.2025-04-16T18-13-26Z` | ✅ |
| Iceberg REST | `tabulario/iceberg-rest` | `1.6.0` | ✅ |
| Trino | `trinodb/trino` | `475` | ✅ |
| StarRocks FE | `starrocks/fe-ubuntu` | `3.3.12` | ✅ |
| StarRocks BE | `starrocks/be-ubuntu` | `3.3.12` | ✅ |

**Status:** `candidate` — image tags all verified on Docker Hub on 2026-05-16, but no install has ever been run end-to-end against this combination. Evidence array is intentionally empty.

**Promotion TODO list** (to reach `pilot-stable`):

1. **Write `scripts/lhs-trino-bootstrap.sh`** — must seed `raw`/`curated`/`analytics` schemas through Trino SQL (`CREATE SCHEMA iceberg.raw ...`, `INSERT INTO ...`) instead of PySpark. The script must write `/etc/trino/catalog/iceberg.properties` inside the trino container with `iceberg.catalog.type=rest`, `iceberg.rest-catalog.uri=$ICEBERG_REST_URI`, `fs.s3.enabled=true`, `s3.path-style-access=true`, `s3.endpoint=$S3_ENDPOINT`, `s3.aws-access-key=$AWS_ACCESS_KEY_ID`, `s3.aws-secret-key=$AWS_SECRET_ACCESS_KEY`.
2. **Write `scripts/lhs-trino-smoke.sh`** — must round-trip a row through Trino (write via `INSERT INTO iceberg.curated.demo`) and verify it reads back from BOTH Trino AND StarRocks against the same Iceberg-REST catalog. Mirror the shape of `lhs-smoke.sh` so the harness can reuse its result-parsing.
3. **Run end-to-end on Windows + Docker Desktop** — full pipeline `prepare → clone → env → doctor → start → bootstrap → smoke → finalize`. Capture host metadata (Docker version, RAM, CPU cores) and per-step result. Append as the first `evidence[]` entry in `udp-trino-local-v0.1.lock.yaml`.
4. **Run end-to-end on Linux Docker** — same as above. Append as a second `evidence[]` entry. Both required because the v0.2 stack already established that the Windows AWS-SDK→MinIO path has a UnknownHostException quirk; the Trino candidate could hit the same or a different OS-specific issue, and we need to know which side of the line it falls on before promotion.
5. **Decide Trino JVM heap config** for the `recommended` resource profile (10 GB RAM) — without an explicit `-Xmx` in `JAVA_OPTS`, Trino defaults to a fraction of container RAM that may not match the operator's expectation. Document the chosen value in the manifest's `env_defaults` and the lock file's `host_requirements`.
6. **Wire `backend/runner.py`** to route `udp-trino-local-v0.1` installs through the new scripts WITHOUT perturbing the v0.2 pipeline. The frozen-runner constraint says step 5 of the promotion gate cannot regress v0.2 — add an opt-in branch keyed on `manifest.id`, never a default-flow change.
7. **Flip the lock file's `status:`** from `candidate` to `pilot-stable` and update `status_notes` with the evidence ids. Bump `version_id` from `0.1.0` to `0.1.1` (or higher, semver applies — patch for evidence-only promotion, minor for any constraint relaxation).

Until item 7 ships, `udp-trino-local-v0.1` stays a candidate. The /healthz endpoint will continue emitting the `recommended_set 'udp-trino-recommended': warning — stack 'udp-trino-local-v0.1' lock status is 'candidate'` line, by design.

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

## Upgrade Planner (v0.4)

The Upgrade Planner lets operators see *what could be bumped* in a certified stack without touching the lock file. Candidates live in a sibling YAML next to the lock — e.g. `stacks/compatibility/udp-local-v0.2.upgrades.yaml` — and the loader rejects any candidate tag that hasn't been confirmed via `docker manifest inspect` ahead of time.

Two read-only routes drive the surface:

- `GET /api/stacks/{stack_id}/upgrades` — returns one row per candidate with the current lock tag, the candidate tag, the source (`hand_curated` for now), and a feasibility hint if a prior simulate has been cached.
- `POST /api/stacks/{stack_id}/upgrades/simulate` — body `{proposed: {component_id: tag}}`. Overlays the proposed tags on the lock (never mutating it), reruns the registry precheck on the overlay, walks `incompatible[]` for known-bad combos, and classifies every `constraints[]` rule as `pass` (proposed doesn't touch the pair), `pass-cached` (a prior `pairwise_tested` entry confirms it), or `unknown` (touches but no cached evidence). Aggregation: any `fail` → `fail`; any `unknown` → `unknown`; else `pass`.

The planner deliberately stops at *simulation*. Applying an upgrade — that requires a backup_id and a re-entry through the install pipeline — is deferred to v0.4.1 so we don't ship a one-way door before the rollback story is wired up.

## TLS Sidecar

The certified stack ships HTTP-only by default — adding TLS as part of the install pipeline would change `docker-compose.yml` and invalidate the lock-file contract. Instead, v0.4.1 adds an **opt-in Caddy TLS sidecar** via a sibling override file the operator activates when they want HTTPS termination.

**The override-file pattern.** The base `docker-compose.yml` is FROZEN (`runner._patch_compose_images` produces it byte-for-byte regardless of TLS profile). The Caddy module writes two sibling files alongside it:

- `docker-compose.tls.yml` — a Caddy service definition + named `caddy_data` / `caddy_config` volumes
- `Caddyfile` — path-based routing for the four primary UIs (`/minio`, `/iceberg`, `/spark`, `/starrocks`) on a single virtual host with TLS termination on port 443

Two profiles are supported:

| Profile | Use when | Trust model |
|---|---|---|
| `self_signed` | offline/dev installs | Caddy generates an internal CA + per-host leaf cert. Browser warns until the operator imports `/data/caddy/pki/authorities/local/root.crt` into the OS trust store. |
| `letsencrypt` | public installs with a real domain | Caddy issues via ACME HTTP-01 using `{domain}` and `{email}`. Auto-renews at 60 days. Requires inbound ports 80 + 443 reachable from the public internet. |

**The activate command** (surfaced from the route, NOT run by Studio):

```bash
docker compose -f docker-compose.yml -f docker-compose.tls.yml up -d caddy
```

The operator runs this themselves from the `install_dir`. Studio writes the override files and surfaces the command — it never restarts the operator's stack on its own.

**Volume persistence (CRITICAL).** The Caddy service mounts two named Docker volumes:

- `caddy_data` — holds the issued certs **and the ACME account key**
- `caddy_config` — runtime cache

**Do NOT recreate `caddy_data` on stack upgrades.** Losing the ACME account forces Caddy to register a new one, and the new account will burn Let's Encrypt's "duplicate certificate" rate limit (5 per week per FQDN). On a domain that's already hit that ceiling, issuance fails for the rest of the rolling window. The same volume also holds the self-signed internal CA — recreating it invalidates any trust-store entries the operator pinned for the previous root.

**What we deliberately do not ship in v0.4.1:**

- DNS-01 challenges (would need a custom Caddy image with the DNS-provider plugin baked in)
- Per-service certs (out of scope; the sidecar pattern wins on operational simplicity)
- Auto-stop of the existing caddy container on `disable` (the operator retains lifecycle control — the route returns the `docker compose ... down` command instead)

Pinned image: `caddy:2.8-alpine` (verified via `docker manifest inspect` on 2026-05-16; multi-arch amd64 + arm64).

## Monitoring Sidecar

Studio ships an opt-in Prometheus + Grafana monitoring stack as a Docker Compose **override file** — the same pattern as the Caddy TLS sidecar. Monitoring is layered ON TOP of the base install via a separate `docker-compose.metrics.yml` the operator activates explicitly.

**Crucial scope note:** the monitoring sidecar is **NOT** part of the certified compatibility lock file. It is an opt-in operational layer — image tags are pinned in `backend/monitoring.py`, not in `stacks/compatibility/*.lock.yaml`. This is deliberate:

- The certified lock represents the *minimum verified-working data lakehouse*. Monitoring is observability for that lake, not part of it.
- The operator chooses whether to accept the prometheus/grafana versions independently of accepting the lakehouse stack.
- Bumping a monitoring image tag does NOT require a full end-to-end install re-verification — it only affects observability.

### How it works

Two routes drive the override:

- `POST /api/installs/{install_id}/monitoring/enable` body `{include_grafana, prometheus_retention_days, grafana_admin_password}` — writes the override file + `monitoring/` subtree into the install_dir.
- `POST /api/installs/{install_id}/monitoring/disable` — removes the override file + `monitoring/` subdir. Does NOT stop the containers — the response includes a shutdown hint so the operator retains lifecycle control.

Both routes refuse if the install is in `RUNNING_STATES` (same guard as Caddy/backup).

### Files written into the install_dir

```
{install_dir}/
  docker-compose.metrics.yml                  # the override
  monitoring/
    prometheus.yml                            # scrape config
    grafana/
      provisioning/
        datasources/datasource.yml            # auto-wires Prometheus
        dashboards/dashboard.yml              # provisioning provider
      dashboards/
        lakehouse-overview.json               # starter connectivity dashboard
```

### Activation

The enable route returns the exact command (it never runs `docker compose up` itself — keeps lifecycle control with the operator):

```bash
cd {install_dir}
docker compose -f docker-compose.yml -f docker-compose.metrics.yml up -d
```

Default host-side ports: Prometheus on `9091`, Grafana on `3001`. Both are deliberately above the typical UDP service range so they don't collide.

### Pinned images (verified 2026-05-16)

| Service | Image | Tag | Verified |
|---|---|---|---|
| Prometheus | `prom/prometheus` | `v2.55.0` | `docker manifest inspect` |
| Grafana | `grafana/grafana` | `11.3.0` | `docker manifest inspect` |

Both tags exist on Docker Hub at the time of pinning. They are **not** re-checked at install-time precheck (that precheck only runs against the certified lock).

### Scrape target caveats

The generated `prometheus.yml` targets four jobs. Each has a real-world gotcha:

- **MinIO** (`/minio/v2/metrics/cluster`): by default MinIO requires a bearer token on the cluster metrics endpoint. To make scrapes work without auth, set `MINIO_PROMETHEUS_AUTH_TYPE=public` in the install's `.env` and restart MinIO. Alternatively paste a bearer token under `authorization.credentials` in `monitoring/prometheus.yml` after enabling.
- **StarRocks FE** (`:8030/api/health` + `:8030/metrics`): the readiness endpoint gives an up/down signal out of the box. The native `/metrics` endpoint **requires** `enable_prometheus_metrics = true` in `fe.conf` — without it, the metrics target shows down (the health target stays up).
- **Iceberg REST** (`:8181/metrics`): conditional on the upstream image. `tabulario/iceberg-rest:1.6.0` (the version in the current certified lock) does **not** expose `/metrics`. The target will show down — this is expected and harmless.
- **Prometheus itself**: self-scrape on `localhost:9090`, always up when the container is running.

### Grafana admin password handling

- If the caller supplies a password in the enable request body, that value is injected into `GF_SECURITY_ADMIN_PASSWORD` and the response confirms it was user-supplied.
- If the caller does NOT supply one, the backend generates a 24-character URL-safe random secret, injects it into the override file, and returns it in the response **ONCE**. The server never persists it outside the running Grafana container's environment — the operator must save it immediately.

### What the override does NOT do

- It does not modify `docker-compose.yml` (the certified base) — strict additive layering only.
- It does not touch `backend/runner.py` or `_patch_compose_images` — the install pipeline is frozen.
- It does not register a new entry in `stacks/compatibility/*.lock.yaml` — monitoring is intentionally outside the certified surface.
- It does not run `docker compose up` for you — the operator retains explicit lifecycle control (same model as the Caddy sidecar).

## JDBC Extras

The certified Spark image (`tabulario/spark-iceberg:3.5.5_1.8.1`) ships **without** Postgres / MySQL JDBC drivers on the classpath. Repackaging it would invalidate the lock file, so v0.5.1 adds an **opt-in JDBC side-load override** the operator activates when they want real Postgres or MySQL ingest.

Same override-file pattern as the Caddy TLS sidecar + the Monitoring sidecar: a sibling `docker-compose.jdbc.yml` the operator opts into. Required to unblock `POST /api/installs/{install_id}/ingest/postgres` and `.../ingest/mysql` — both real ingest paths refuse with a `Run POST /api/installs/.../jdbc/enable first` message until the override is active.

### How it works

The override declares two service blocks:

1. **`jdbc-extras`** — a one-shot init container (image: `curlimages/curl:8.10.1`) that runs `curl -fL --retry 3` against Maven Central to download the requested JDBC jars into a named docker volume (`spark_jdbc_jars`). Idempotent: skips any jar already present in the volume.
2. **`spark-iceberg`** — appends ONLY a new read-only volume mount (`spark_jdbc_jars:/opt/spark/jars/jdbc:ro`). Compose merge preserves every other key from the base service definition (image, command, ports, env). The spark-iceberg entrypoint adds `$SPARK_HOME/jars/*` to the classpath, so the JDBC jars are picked up automatically on next container start.

A `depends_on: jdbc-extras { condition: service_completed_successfully }` clause on the spark service guarantees the spark container only restarts after the init container has exited 0 (i.e. jars are in place).

### Two routes drive the override

- `POST /api/installs/{install_id}/jdbc/enable` body `{include_postgres, include_mysql, postgres_driver_version, mysql_driver_version}` — writes `docker-compose.jdbc.yml` into the install_dir. Defaults: postgres `42.7.4`, mysql `9.0.0`. `include_postgres=True` by default; `include_mysql=False` by default (most users start with Postgres only — keeps the download to 1 MB).
- `POST /api/installs/{install_id}/jdbc/disable` — removes the override file. Returns a granular `stop` + `rm -f` of just the `jdbc-extras` container (NOT a `compose down` — the rest of the lakehouse stays serving). The `spark_jdbc_jars` volume is intentionally **retained** so re-enabling skips the download; use `docker volume rm spark_jdbc_jars` for a clean slate.

Both routes refuse if the install is in `RUNNING_STATES` (same guard as Caddy / monitoring).

### Activation

The enable route returns the exact command (it never runs `docker compose up` itself):

```bash
cd {install_dir}
docker compose -f docker-compose.yml -f docker-compose.jdbc.yml up -d jdbc-extras
# Once `docker compose ps jdbc-extras` shows exit-0, recreate the spark
# service so it picks up the new mount:
docker compose -f docker-compose.yml -f docker-compose.jdbc.yml up -d --no-deps spark-iceberg
```

### Pinned driver versions (verified reachable 2026-05-16)

| Driver | Maven coordinate | Version | URL pattern | Size |
|---|---|---|---|---|
| Postgres JDBC | `org.postgresql:postgresql` | `42.7.4` | `https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar` | 1.04 MB |
| MySQL Connector/J | `com.mysql:mysql-connector-j` | `9.0.0` | `https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/9.0.0/mysql-connector-j-9.0.0.jar` | 2.47 MB |

Both URLs returned `HTTP/1.1 200 OK` with `Content-Type: application/java-archive` when verified ahead of pinning. Bytes pinned via Maven Central (not vendor mirrors) for the immutability guarantee — once a version is published to Central, it cannot be retracted or republished.

When bumping these, run:

```bash
curl -I https://repo1.maven.org/maven2/org/postgresql/postgresql/<v>/postgresql-<v>.jar
curl -I https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/<v>/mysql-connector-j-<v>.jar
```

and confirm a 200 before committing the version change in `backend/jdbc_extras.py`.

### Credential handling

The decrypted source credential (from `backend/data_sources.py::_decrypt_password`) reaches Spark via `--user` / `--password` argv on `spark-submit`. The credential **never** appears in:

- The JDBC URL (built without embedded `user:pass@`)
- Stored job records (`IngestJob.source` deliberately omits the credential)
- The log stream — every line published to the event bus is run through `backend/redact.py::redact()`, which masks `--password X`, `KEY=value`, and `scheme://user:secret@host` patterns before emit.

### What the override does NOT do

- It does not modify `docker-compose.yml` (the certified base) — strict additive layering only.
- It does not touch `backend/runner.py` or the install pipeline — the install pipeline is frozen.
- It does not register a new entry in `stacks/compatibility/*.lock.yaml` — JDBC drivers are operational extras, intentionally outside the certified surface.
- It does not run `docker compose up` for you — the operator retains explicit lifecycle control (same model as the Caddy + monitoring sidecars).
- It does not delete the downloaded jars on disable — the named volume is retained so re-enabling is fast.

