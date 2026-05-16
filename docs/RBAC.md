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
