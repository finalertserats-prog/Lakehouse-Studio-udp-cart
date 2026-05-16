# Lakehouse Studio — API endpoint reference

Generated from the actual route table in `backend/main.py`. For auth /
RBAC requirements per endpoint, see [docs/RBAC.md](RBAC.md). For the v1
alias semantics (`/api/v1/*` rewrites onto canonical `/api/*`), see
[docs/API_VERSIONING.md](API_VERSIONING.md).

## Auth & RBAC

- `GET  /api/auth/status` — Check whether shared-token auth is enabled.
- `POST /api/rbac/users` — Create an RBAC user; returns the plaintext token exactly once.
- `GET  /api/rbac/users` — List registered RBAC users (hashes only).
- `DELETE /api/rbac/users/{user_id}` — Remove an RBAC user. OWNERs cannot self-delete.
- `GET  /api/rbac/me` — Identity + role of the calling user.

## Catalog & picker

- `GET  /api/catalog` — Full component catalog (categories, goals, destinations).
- `GET  /api/goals` — Business goals used to recommend stack configurations.
- `GET  /api/templates` — Pre-configured stack templates for specific use cases.
- `GET  /api/templates/{template_id}` — Template detail + compliance tags.
- `GET  /api/compliance/{tag}` — Long-form prose for a compliance framing (HIPAA, SOC 2, ...).
- `POST /api/cart/validate` — Validate a list of component IDs for compatibility.
- `GET  /api/cart/recommended` — Default recommended component set.
- `GET  /api/lake-names/suggest` — Generate lakehouse name suggestions.
- `POST /api/lake-names/validate` — Validate + normalize a proposed lakehouse name.
- `GET  /api/stacks` — All available stack manifests + their requirements.
- `GET  /api/stacks/{stack_id}` — Full manifest for a stack.
- `GET  /api/stacks/{stack_id}/sizing` — Resource requirements + VPS plan matches.
- `GET  /api/stacks/{stack_id}/score` — Maturity + quality score for a stack.

## Installs & operations

- `POST /api/inspect` — Inspect a target host's prerequisites.
- `GET  /api/installs` — List installations.
- `GET  /api/installs/{install_id}` — Current state + config for an install.
- `POST /api/installs` — Create a new install; begin orchestration.
- `POST /api/installs/{install_id}/cancel` — Cancel an in-flight task.
- `POST /api/installs/{install_id}/steps/retry` — Resume from a failed step.
- `POST /api/installs/{install_id}/steps/skip` — Skip a failed non-critical step.
- `GET  /api/installs/{install_id}/export` — GitOps bundle (.tar.gz) for re-import.
- `POST /api/installs/{install_id}/uninstall` — Tear down a deployed stack.
- `POST /api/installs/{install_id}/steps/rollback` — Manually trigger cleanup of volumes + containers.
- `POST /api/installs/{install_id}/smoke-structured` — Automated smoke test against a READY stack.
- `GET  /api/installs/{install_id}/health` — Live health snapshot of all containers + services.
- `GET  /api/installs/{install_id}/diagnose` — AI-explained root cause for a failed install.
- `POST /api/installs/{install_id}/control` — Execute lifecycle commands (status, stop, clean, smoke).
- `WS   /api/installs/{install_id}/logs` — Live orchestration log stream.
- `GET  /api/installs/{install_id}/services/{service_name}/logs` — Recent Docker logs snapshot for one service.
- `WS   /api/installs/{install_id}/services/{service_name}/logs/stream` — Live Docker logs stream for one service.

## Upgrades & backups

- `GET  /api/stacks/{stack_id}/upgrades` — Hand-curated upgrade candidates.
- `POST /api/stacks/{stack_id}/upgrades/simulate` — Dry-run; checks compatibility + registry availability.
- `POST /api/installs/{install_id}/upgrades/execute` — Destructive image swap upgrade.
- `GET  /api/installs/{install_id}/upgrades/executions` — Upgrade attempt history for an install.
- `GET  /api/upgrades/executions/{execution_id}` — Detail for one upgrade execution.
- `POST /api/installs/{install_id}/backups` — Create a manual `metadata` or `full` backup.
- `GET  /api/installs/{install_id}/backups` — List backups for an install.
- `POST /api/backups/{backup_id}/restore` — Restore an install from a backup.
- `DELETE /api/backups/{backup_id}` — Delete a backup tarball.
- `GET  /api/installs/{install_id}/backups/schedule` — Get the automated backup schedule.
- `PUT  /api/installs/{install_id}/backups/schedule` — Configure / update the backup schedule.

## Security / TLS / sidecars

- `GET  /api/stacks/{stack_id}/compatibility` — Catalog-vs-lock drift check.
- `POST /api/stacks/{stack_id}/compatibility/precheck` — Verify every image in the lock exists on its registry.
- `POST /api/installs/{install_id}/tls/generate` — Generate self-signed or Let's Encrypt cert.
- `GET  /api/installs/{install_id}/tls/certs` — List cert metadata (public only).
- `DELETE /api/tls/certs/{cert_id}` — Remove cert + private key from the server.
- `POST /api/installs/{install_id}/security/rotate-password` — Rotate root passwords for internal services.
- `POST /api/installs/{install_id}/security/password-strength` — Server-side password strength check.
- `POST /api/installs/{install_id}/tls/caddy/enable` — Add HTTPS Caddy sidecar.
- `POST /api/installs/{install_id}/tls/caddy/disable` — Remove the Caddy sidecar.
- `POST /api/installs/{install_id}/jdbc/enable` — Side-load Postgres + MySQL JDBC drivers for Spark.
- `POST /api/installs/{install_id}/jdbc/disable` — Remove the JDBC side-loader.
- `POST /api/installs/{install_id}/monitoring/enable` — Deploy Prometheus + Grafana sidecar.
- `POST /api/installs/{install_id}/monitoring/disable` — Remove the monitoring sidecar.

## SQL & ingest

- `GET  /api/demo-queries` — Pre-written demo queries for the stack.
- `POST /api/installs/{install_id}/demo-query` — Run a selected demo query.
- `POST /api/installs/{install_id}/sql` — Run sandboxed read-only SQL against the data lake.
- `POST /api/installs/{install_id}/uploads` — Upload a CSV; preview inferred schema.
- `POST /api/installs/{install_id}/ingest` — Register a CSV ingestion job.
- `GET  /api/installs/{install_id}/ingest` — List ingestion jobs for an install.
- `GET  /api/installs/{install_id}/ingest/{job_id}` — Status + detail for one ingestion job.
- `POST /api/installs/{install_id}/ingest/postgres` — Start a Postgres source ingestion job.
- `POST /api/installs/{install_id}/ingest/mysql` — Start a MySQL source ingestion job.
- `GET  /api/installs/{install_id}/tables` — Browse Iceberg namespaces + tables.
- `GET  /api/installs/{install_id}/tables/{namespace}/{name}` — Schema + metadata for one table.

## Sources & destinations

- `POST /api/installs/{install_id}/data-sources` — Register an external Postgres source.
- `GET  /api/installs/{install_id}/data-sources` — List external sources.
- `POST /api/data-sources/{source_id}/test` — Test connection to a registered source.
- `DELETE /api/data-sources/{source_id}` — Remove a source registration.
- `POST /api/installs/{install_id}/destinations` — Register a downstream BI tool destination.
- `GET  /api/installs/{install_id}/destinations` — List destinations for an install.
- `GET  /api/destinations/{destination_id}` — Detail for one destination.
- `POST /api/destinations/{destination_id}/test` — Test connectivity to a destination.
- `POST /api/destinations/{destination_id}/provision` — Provision required access + artifacts.
- `GET  /api/destinations/{destination_id}/connection` — Sanitized connection bundle for the destination tool.
- `DELETE /api/destinations/{destination_id}` — Remove a destination registration.

## Data Quality

- `POST /api/installs/{install_id}/dq/checks` — Create a DQ assertion for a table.
- `GET  /api/installs/{install_id}/dq/checks` — List DQ checks for an install.
- `DELETE /api/dq/checks/{check_id}` — Remove a DQ check.
- `POST /api/dq/checks/{check_id}/run` — Execute a DQ check; persist the result.
- `GET  /api/installs/{install_id}/dq/results` — Recent DQ results for an install.

## AI & notifications

- `GET  /api/notifications/config` — Notification settings (secrets scrubbed).
- `POST /api/notifications/test` — Send a test notification through one channel.
- `GET  /api/ai/status` — AI assistant availability + model config.
- `POST /api/ai/ask` — General AI question grounded in project context.
- `POST /api/components/{component_id}/recommend` — AI-grounded recommendation for one component.

## Audit

- `GET  /api/audit` — Query the persisted audit log.

## Health & static

- `GET  /` — Lakehouse Studio frontend (single-page app).
- `GET  /healthz` — System-wide health summary (catalog + template problems, with warnings split from errors).
