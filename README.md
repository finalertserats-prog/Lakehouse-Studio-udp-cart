# Lakehouse Studio

UI-driven, compatibility-validated installer and operator for open data lakehouses. Lakehouse Studio handles the full lifecycle of your data platform — from "shopping" for a certified stack to day-2 operations like audit, backups, ingest, and AI-assisted troubleshooting.

## What it does

1. **Browse & shop** certified lakehouse stacks (e.g. MinIO + Iceberg + Spark + StarRocks).
2. **Inspect & validate** your infrastructure with pre-flight checks (Docker, RAM, CPU, disk, port conflicts).
3. **Install & verify** with a hardened pipeline: clone → env → doctor → start → bootstrap → smoke-test, streamed live over WebSocket with per-step progress.
4. **Operate & scale** with built-in RBAC, audit logging, scheduled backups + DR drills, multi-source ingest (CSV/MySQL/Postgres), downstream BI destinations (Insyght + others), and an AI assistant grounded in the compatibility matrix.

## Certified stacks

| Stack ID | Table Format | Catalog | Engines | Status |
|---|---|---|---|---|
| `udp-local-v0.2` | Iceberg | Iceberg REST | Spark + StarRocks | ![pilot-stable](https://img.shields.io/badge/status-pilot--stable-green) |
| `udp-trino-local-v0.1` | Iceberg | Iceberg REST | Trino + StarRocks | ![candidate](https://img.shields.io/badge/status-candidate-yellow) |
| `iceberg-nessie-trino-local-v0.1` | Iceberg | **Nessie** (git-for-data) | Trino + StarRocks | ![candidate](https://img.shields.io/badge/status-candidate-yellow) |
| `iceberg-polaris-spark-local-v0.1` | Iceberg | **Polaris** (RBAC + cred vending) | Spark + StarRocks | ![candidate](https://img.shields.io/badge/status-candidate-yellow) |
| `hudi-hms-spark-local-v0.1` | **Hudi** (streaming-first) | Hive Metastore + Postgres | Spark | ![candidate](https://img.shields.io/badge/status-candidate-yellow) |
| `delta-hms-spark-trino-local-v0.1` | **Delta Lake** | Hive Metastore + Postgres | Spark + Trino | ![candidate](https://img.shields.io/badge/status-candidate-yellow) |

Promotion to `pilot-stable` requires at least one passing end-to-end install captured as an `evidence[]` record in the stack's `.lock.yaml`. See [docs/COMPATIBILITY.md](docs/COMPATIBILITY.md) and the [stability matrix](docs/STABILITY_MATRIX.md) for the honest state of every stack × OS combination.

### Optional add-ons (opt-in via env flags)

Layer any of these on top of any stack via the override-compose pattern:

| Add-on | Env flag | Purpose |
|---|---|---|
| Apache Airflow 2.10 | `LHS_AIRFLOW_ENABLED=true` | DAG scheduler — auto-includes Postgres backing |
| Dagster 1.9 | `LHS_DAGSTER_ENABLED=true` | Asset-first orchestrator — Dagit UI + daemon |
| Apache Superset 4.1 | `LHS_SUPERSET_ENABLED=true` | Open-source BI — auto-wires to StarRocks (MySQL dialect) or Trino |

Overlay modules live at `backend/{airflow,dagster,superset}_overlay.py`. Each writes a `docker-compose.<name>.yml` next to the base compose; the runner appends them via `-f` during the start step when the env flag is on.

## Feature pillars

### 1. Install pipeline
State-machine driven `shop → inspect → install → smoke → query` loop. Live log streaming over WebSocket, per-step progress, retry / skip / cancel / rollback, post-failure recovery UI. Optional `environment` tier (dev/staging/prod) lets multiple installs co-exist on the same host without compose collisions.

### 2. Compatibility matrix as a moat
Every certified stack is backed by a `stacks/compatibility/<stack>.lock.yaml` pinning exact image tags and documenting pairwise constraints with `evidence[]` records. No `latest` tags. No silent breaking "patch" updates. The Upgrade Planner surfaces hand-curated candidate tag bumps with simulate + execute paths.

### 3. RBAC (opt-in)
Bootstrap an OWNER, then create ADMIN / OPERATOR / VIEWER users via the API. Bearer tokens hashed sha256 at rest. One default tenant in v0.5; multi-tenancy is a v1.0 concern. See [docs/RBAC.md](docs/RBAC.md) for the full permission matrix.

### 4. Audit & retention
Opt-in SQLite audit trail at `work/audit.sqlite`. Subscribes to the event bus, scrubs every payload via the same redactor used by log streaming, persists state changes / errors / step boundaries. Automatic retention pruning when `LHS_AUDIT_SCHEDULER_ENABLED=true`. See [docs/AUDIT.md](docs/AUDIT.md).

### 5. Backups & DR
`metadata` (config-only) and `full` (MinIO data mirror) backup kinds, scheduled per-install or manual. Opt-in DR drill scheduler (`LHS_DR_DRILL_ENABLED=true`) verifies the latest tarball per install is structurally sound on a tick — non-destructive, reads only.

### 6. Ingest (CSV / MySQL / Postgres)
Universal ingest path using Spark-Iceberg. Opt-in JDBC side-loader pulls Postgres/MySQL drivers on demand so the base image stays lean. CSV uploads land in MinIO and feed the same ingest job runner.

### 7. Destinations & AI assistant
Provision downstream BI tools (Insyght + 7 placeholders for Tableau/Looker/Mode/Superset/Metabase/PowerBI/custom JDBC) via three connection modes: `sql_pull` (BI tool reads StarRocks with a provisioned reader user), `push_api`, or `file_drop`. See [docs/DESTINATIONS.md](docs/DESTINATIONS.md). The AI assistant exposes `/api/ai/ask` and `/api/components/{id}/recommend`, both grounded in the catalog + lock files.

## Requirements

- **Python 3.11+**
- **Docker Desktop** (Linux containers) or Docker Engine
- **bash** and **git** in PATH

Verified on Windows 11 with Docker Desktop + Git Bash, and on Linux Docker.

## Quick start

```bash
# Linux / macOS / Git Bash
bash run.sh

# Windows (PowerShell)
powershell -ExecutionPolicy Bypass -File .\run.ps1
```

The server boots on `http://127.0.0.1:7878` by default (`LHS_BIND` + `LHS_PORT` to override). Open it in a browser, pick a stack, and walk through the wizard.

## Opt-in feature flags

| Flag | Default | Purpose |
|---|---|---|
| `LHS_AUTH_TOKEN` | _(unset)_ | Legacy single-token auth. Disabled when RBAC is on. |
| `LHS_RBAC_ENABLED` | `false` | Per-user bearer tokens with role-based authorization |
| `LHS_AUDIT_ENABLED` | `false` | Persist audit events to `work/audit.sqlite` |
| `LHS_AUDIT_SCHEDULER_ENABLED` | `false` | Auto-prune audit rows older than `LHS_AUDIT_RETENTION_DAYS` (default 90) |
| `LHS_DR_DRILL_ENABLED` | `false` | Periodic backup integrity probe (non-destructive) |
| `ANTHROPIC_API_KEY` | _(unset)_ | Enables the AI assistant routes (`/api/ai/*`) |
| `LHS_WORK_DIR` | `./work` | Where installs, state, audit DB, and backups live on disk |

## API surface

- Canonical paths are **un-versioned** (`/api/installs`, `/api/stacks`, etc.).
- `/api/v1/*` is an alias maintained by middleware; future breaking changes will land at `/api/v2/*`. See [docs/API_VERSIONING.md](docs/API_VERSIONING.md).
- **WebSocket** install log stream: `/api/installs/{id}/logs` (and service-level: `/api/installs/{id}/services/{svc}/logs/stream`).
- **OpenAPI** auto-docs at `/docs` when the server is running.

## Where to find more

- [API Versioning](docs/API_VERSIONING.md) — contract stability and the v1 alias
- [Audit & Retention](docs/AUDIT.md) — compliance trail + schema
- [Compatibility Matrix](docs/COMPATIBILITY.md) — how stacks get certified
- [Destinations](docs/DESTINATIONS.md) — connecting BI tools like Insyght
- [RBAC](docs/RBAC.md) — roles, tokens, and the full route permission map
- [v1.0 Architecture](docs/architecture/v1/README.md) — migration plan for multi-tenant + multi-target
