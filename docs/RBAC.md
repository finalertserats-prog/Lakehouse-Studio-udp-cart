# RBAC (Role-Based Access Control)

Lakehouse Studio ships an **opt-in** RBAC layer. By default the backend
uses the legacy single-shared-token model (`LHS_AUTH_TOKEN`). When
`LHS_RBAC_ENABLED=true`, the per-user token store in
`backend/rbac_auth.py` takes over and every API call is authorized
against a role-based permission map.

This is a v0.5 bridge between the v1 multi-tenant scaffold
(`backend/v1/`) and the running app. It is intentionally minimal:
- One default tenant. Multi-tenancy stays a v1.0 concern.
- No password login. Every user authenticates with a bearer API token.
- No JWT, no OIDC. Tokens are 32-char URL-safe random strings hashed
  with sha256 at rest.

## Enabling RBAC

1. **Bootstrap the first OWNER user** (the API has no way to create the
   first user — it would chicken-and-egg). The bootstrap CLI refuses if
   any user already exists:

   ```bash
   python -m backend.bootstrap_rbac --email ops@example.com --role OWNER
   ```

   The plaintext token is printed **exactly once** on stdout. Store it
   in your secret manager immediately; the database only holds the
   sha256 hash, so a lost token means deleting and recreating the user.

2. **Flip the flag and restart the backend:**

   ```bash
   export LHS_RBAC_ENABLED=true
   # restart the Studio backend
   ```

3. **Authenticate with the bootstrap token:**

   ```bash
   curl -H "Authorization: Bearer <token>" \
        http://localhost:8000/api/rbac/me
   ```

4. **Create additional users via the authenticated API:**

   ```bash
   curl -X POST -H "Authorization: Bearer <owner-token>" \
        -H "Content-Type: application/json" \
        -d '{"email":"alice@example.com","role":"ADMIN"}' \
        http://localhost:8000/api/rbac/users
   ```

## RBAC endpoints

| Method | Path                         | Required role     | Notes |
|--------|------------------------------|-------------------|-------|
| POST   | `/api/rbac/users`            | OWNER             | Returns the plaintext token once. |
| GET    | `/api/rbac/users`            | OWNER, ADMIN      | Hashes only, never plaintext.     |
| DELETE | `/api/rbac/users/{user_id}`  | OWNER             | An OWNER cannot self-delete.      |
| GET    | `/api/rbac/me`               | Any authed user   | Returns the caller's identity.    |

When RBAC is **disabled**, all four routes return `503 RBAC is not
enabled on this install`.

## Role permission matrix

Roles are defined in `backend/v1/rbac.py`. The v1 scaffold also defines
a `ROUTE_PERMISSIONS` map (`backend/v1/rbac.py:79`) that maps API routes
to the permission they require. Unmapped routes have no permission gate
(they only require a valid token).

| Permission         | OWNER | ADMIN | OPERATOR | VIEWER |
|--------------------|:-----:|:-----:|:--------:|:------:|
| `install.create`   |   YES |   YES |   YES    |  no    |
| `install.view`     |   YES |   YES |   YES    |  YES   |
| `install.delete`   |   YES |   YES |   no     |  no    |
| `backup.create`    |   YES |   YES |   YES    |  no    |
| `backup.restore`   |   YES |   YES |   YES    |  no    |
| `upgrade.execute`  |   YES |   YES |   YES    |  no    |
| `sql.run`          |   YES |   YES |   YES    |  no    |
| `audit.view`       |   YES |   YES |   YES    |  YES   |
| `settings.write`   |   YES |   YES |   no     |  no    |
| `billing.view`     |   YES |   no  |   no     |  no    |

Rule of thumb:
- **OWNER** can do everything, including billing.
- **ADMIN** can do everything operational, but not billing.
- **OPERATOR** can run installs and queries, but not delete installs or
  change settings.
- **VIEWER** is read-only (install + audit).

## Complete route matrix (audited 2026-05-17)

The table below reflects the **actual** auth requirement enforced in
`backend/main.py` at the time of audit, not just what `ROUTE_PERMISSIONS`
documents. When `LHS_RBAC_ENABLED=true`, `AuthDep` authenticates the
user, then checks `backend/v1/rbac.py::ROUTE_PERMISSIONS` by exact
`(method, path)`. Any authenticated route MISSING from that map falls
through to **VIEWER** (any authenticated role can call it). Routes
without `AuthDep` are **PUBLIC**. `/api/v1/...` aliases inherit the
same policy as their canonical counterparts.

> ⚠️ **Known gap**: Many state-changing routes below are currently
> reachable by VIEWER because they're not in `ROUTE_PERMISSIONS`. Items
> flagged **WRITE-RISK** should be tightened to OPERATOR at minimum.
> Tracking this as a follow-up — see the "Known RBAC gaps" section
> below.

### Auth / RBAC

| Method | Path | Effective role | Notes |
|---|---|---|---|
| GET | `/api/auth/status` | PUBLIC | No auth dep. |
| POST | `/api/rbac/users` | OWNER | Extra `_require_role({"OWNER"})`. |
| GET | `/api/rbac/users` | ADMIN | OWNER/ADMIN. |
| DELETE | `/api/rbac/users/{user_id}` | OWNER | Owner-only gate. |
| GET | `/api/rbac/me` | VIEWER | Any authenticated user. |
| GET | `/` | PUBLIC | Static frontend. |
| GET | `/healthz` | PUBLIC | Health/status. |

### Catalog / picker / stacks

| Method | Path | Effective role | Notes |
|---|---|---|---|
| GET | `/api/catalog` | VIEWER | Read-only. |
| GET | `/api/goals` | VIEWER | Read-only. |
| GET | `/api/templates` | VIEWER | |
| GET | `/api/templates/{template_id}` | VIEWER | |
| GET | `/api/compliance/{tag}` | VIEWER | |
| POST | `/api/cart/validate` | VIEWER | Validation only, no state change. |
| GET | `/api/cart/recommended` | VIEWER | |
| GET | `/api/lake-names/suggest` | VIEWER | |
| POST | `/api/lake-names/validate` | VIEWER | Validation only. |
| GET | `/api/stacks` | VIEWER | |
| GET | `/api/stacks/{stack_id}` | VIEWER | |
| GET | `/api/stacks/{stack_id}/sizing` | VIEWER | |
| GET | `/api/stacks/{stack_id}/score` | VIEWER | |
| GET | `/api/stacks/{stack_id}/compatibility` | VIEWER | |
| POST | `/api/stacks/{stack_id}/compatibility/precheck` | VIEWER | Runs registry/docker checks; ideally OPERATOR. |
| GET | `/api/stacks/{stack_id}/upgrades` | VIEWER | |
| POST | `/api/stacks/{stack_id}/upgrades/simulate` | VIEWER | Dry run. |
| POST | `/api/components/{component_id}/recommend` | VIEWER | AI recommendation. |

### Installs / ops

| Method | Path | Effective role | Notes |
|---|---|---|---|
| GET | `/api/installs` | VIEWER | Mapped `install.view`. |
| GET | `/api/installs/{install_id}` | VIEWER | Mapped `install.view`. |
| POST | `/api/installs` | OPERATOR | Mapped `install.create`. |
| POST | `/api/installs/{install_id}/cancel` | VIEWER | **WRITE-RISK** — should be OPERATOR. |
| POST | `/api/installs/{install_id}/steps/retry` | VIEWER | **WRITE-RISK** — restarts install. |
| POST | `/api/installs/{install_id}/steps/skip` | VIEWER | **WRITE-RISK** — mutates step state. |
| POST | `/api/installs/{install_id}/steps/rollback` | VIEWER | **WRITE-RISK** — destructive. |
| GET | `/api/installs/{install_id}/export` | VIEWER | |
| POST | `/api/installs/{install_id}/uninstall` | VIEWER | **WRITE-RISK** — destructive; should be ADMIN. |
| POST | `/api/installs/{install_id}/control` | VIEWER | **WRITE-RISK** — stop/clean mutate stack. |
| POST | `/api/installs/{install_id}/smoke-structured` | VIEWER | Executes smoke checks. |
| GET | `/api/installs/{install_id}/health` | VIEWER | |
| GET | `/api/installs/{install_id}/diagnose` | VIEWER | |
| GET | `/api/installs/{install_id}/services/{service_name}/logs` | VIEWER | |
| WS | `/api/installs/{install_id}/logs` | PUBLIC | No token/RBAC check; origin guard only. |
| WS | `/api/installs/{install_id}/services/{service_name}/logs/stream` | VIEWER | Manual RBAC check when enabled. |

### Upgrades / backups

| Method | Path | Effective role | Notes |
|---|---|---|---|
| POST | `/api/installs/{install_id}/upgrades/execute` | VIEWER | **WRITE-RISK** — destructive. |
| GET | `/api/installs/{install_id}/upgrades/executions` | VIEWER | |
| GET | `/api/upgrades/executions/{execution_id}` | VIEWER | |
| POST | `/api/installs/{install_id}/backups` | VIEWER | **WRITE-RISK** — creates backup. |
| GET | `/api/installs/{install_id}/backups` | VIEWER | |
| POST | `/api/backups/{backup_id}/restore` | VIEWER | **WRITE-RISK** — destructive restore. |
| DELETE | `/api/backups/{backup_id}` | VIEWER | **WRITE-RISK** — should be OPERATOR/ADMIN. |
| GET | `/api/installs/{install_id}/backups/schedule` | VIEWER | |
| PUT | `/api/installs/{install_id}/backups/schedule` | VIEWER | **WRITE-RISK** — schedule mutation. |

### Security / TLS / monitoring

| Method | Path | Effective role | Notes |
|---|---|---|---|
| POST | `/api/installs/{install_id}/tls/generate` | VIEWER | **WRITE-RISK** — creates cert/key material. |
| GET | `/api/installs/{install_id}/tls/certs` | VIEWER | |
| DELETE | `/api/tls/certs/{cert_id}` | VIEWER | **WRITE-RISK**. |
| POST | `/api/installs/{install_id}/security/rotate-password` | VIEWER | **WRITE-RISK** — credential rotation; should be ADMIN. |
| POST | `/api/installs/{install_id}/security/password-strength` | VIEWER | Validation only. |
| POST | `/api/installs/{install_id}/tls/caddy/enable` | VIEWER | **WRITE-RISK** — writes override files. |
| POST | `/api/installs/{install_id}/tls/caddy/disable` | VIEWER | **WRITE-RISK**. |
| POST | `/api/installs/{install_id}/jdbc/enable` | VIEWER | **WRITE-RISK** — writes override files. |
| POST | `/api/installs/{install_id}/jdbc/disable` | VIEWER | **WRITE-RISK**. |
| POST | `/api/installs/{install_id}/monitoring/enable` | VIEWER | **WRITE-RISK** — writes config/secrets. |
| POST | `/api/installs/{install_id}/monitoring/disable` | VIEWER | **WRITE-RISK**. |

### SQL / ingest / data

| Method | Path | Effective role | Notes |
|---|---|---|---|
| POST | `/api/installs/{install_id}/sql` | OPERATOR | Mapped `sql.run`. |
| GET | `/api/demo-queries` | VIEWER | |
| POST | `/api/installs/{install_id}/demo-query` | VIEWER | Executes read query. |
| POST | `/api/installs/{install_id}/uploads` | VIEWER | **WRITE-RISK** — stores uploaded file. |
| POST | `/api/installs/{install_id}/ingest` | VIEWER | **WRITE-RISK** — creates ingest job. |
| GET | `/api/installs/{install_id}/ingest` | VIEWER | |
| GET | `/api/installs/{install_id}/ingest/{job_id}` | VIEWER | |
| POST | `/api/installs/{install_id}/ingest/postgres` | VIEWER | **WRITE-RISK**. |
| POST | `/api/installs/{install_id}/ingest/mysql` | VIEWER | **WRITE-RISK**. |
| GET | `/api/installs/{install_id}/tables` | VIEWER | |
| GET | `/api/installs/{install_id}/tables/{namespace}/{name}` | VIEWER | |

### Sources / destinations / DQ

| Method | Path | Effective role | Notes |
|---|---|---|---|
| POST | `/api/installs/{install_id}/data-sources` | VIEWER | **WRITE-RISK** — stores encrypted credentials. |
| GET | `/api/installs/{install_id}/data-sources` | VIEWER | |
| POST | `/api/data-sources/{source_id}/test` | VIEWER | Opens external connection. |
| DELETE | `/api/data-sources/{source_id}` | VIEWER | **WRITE-RISK**. |
| POST | `/api/installs/{install_id}/destinations` | VIEWER | **WRITE-RISK** — stores credentials/config. |
| GET | `/api/installs/{install_id}/destinations` | VIEWER | |
| GET | `/api/destinations/{destination_id}` | VIEWER | |
| POST | `/api/destinations/{destination_id}/test` | VIEWER | Opens external connection. |
| POST | `/api/destinations/{destination_id}/provision` | VIEWER | **WRITE-RISK** — creates downstream DB/API artifacts. |
| GET | `/api/destinations/{destination_id}/connection` | VIEWER | Returns sanitized connection bundle. |
| DELETE | `/api/destinations/{destination_id}` | VIEWER | **WRITE-RISK**. |
| POST | `/api/installs/{install_id}/dq/checks` | VIEWER | **WRITE-RISK** — creates persisted DQ check. |
| GET | `/api/installs/{install_id}/dq/checks` | VIEWER | |
| DELETE | `/api/dq/checks/{check_id}` | VIEWER | **WRITE-RISK**. |
| POST | `/api/dq/checks/{check_id}/run` | VIEWER | Executes DQ SQL and persists result. |
| GET | `/api/installs/{install_id}/dq/results` | VIEWER | |

### AI / notifications / audit

| Method | Path | Effective role | Notes |
|---|---|---|---|
| GET | `/api/ai/status` | PUBLIC | No auth dep. |
| POST | `/api/ai/ask` | VIEWER | |
| GET | `/api/notifications/config` | VIEWER | |
| POST | `/api/notifications/test` | VIEWER | Sends test notification; ideally OPERATOR. |
| GET | `/api/audit` | VIEWER | Mapped `audit.view`. |

### Known RBAC gaps

- `ROUTE_PERMISSIONS` references some routes that no longer exist
  (`DELETE /api/installs/{install_id}`, `POST /backup`, `POST /restore`,
  `POST /upgrade`). The mapping is for stale paths so the permission
  is never actually consulted on real traffic.
- Many state-changing routes are authenticated-only (default VIEWER)
  and should be tightened to OPERATOR or ADMIN. Tracked as a follow-up
  to extend `ROUTE_PERMISSIONS` to cover the WRITE-RISK paths above.
- Public routes today: `/`, `/healthz`, `/api/auth/status`,
  `/api/ai/status`, and `WS /api/installs/{install_id}/logs`.

## Security notes

- **Tokens are stored as sha256 hashes.** Losing a token means
  re-issuing one. There is no recovery path.
- **The RBAC SQLite DB** lives at `<WORK_DIR>/rbac.sqlite`. Back it up
  with your other Studio data.
- **Token transmission** must go over TLS in production. The bearer
  token grants the same powers as a password.
- **The bootstrap CLI does not require `LHS_RBAC_ENABLED` to run.** This
  is intentional — the operator bootstraps first, then flips the flag.
- **Self-deletion is blocked.** An OWNER cannot delete their own user
  via the API (avoids accidental lockout).

## Disabling RBAC

Set `LHS_RBAC_ENABLED=` (empty) or unset it entirely, then restart the
backend. The legacy single-token path resumes and the four `/api/rbac/*`
routes start returning `503`. The user records in `rbac.sqlite` remain
intact, so flipping back is reversible.
