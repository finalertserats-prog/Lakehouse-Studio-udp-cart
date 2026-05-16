# Destinations

`backend/destinations.py` + `backend/insyght_connector.py` (v0.6 — Insyght as the first concrete connector)

## Concept: sources and destinations are symmetric

Lakehouse Studio has two registries that look almost identical:

| Concern               | Inbound (sources)               | Outbound (destinations)         |
|-----------------------|---------------------------------|---------------------------------|
| Module                | `backend/data_sources.py`       | `backend/destinations.py`       |
| Persistence           | `WORK_DIR/data_sources.json`    | `WORK_DIR/destinations.json`    |
| Public model          | `DataSource` (no plaintext)     | `Destination` (no plaintext)    |
| Secret-at-rest        | Fernet via `_get_fernet`        | **Reuses** the same Fernet      |
| Test endpoint         | `POST /api/data-sources/{id}/test` | `POST /api/destinations/{id}/test` |
| Per-vendor logic      | inline by `kind`                | per-vendor module (`insyght_connector.py`) |

Sources read **into** the lakehouse (Postgres → Iceberg). Destinations are
read **out of** the lakehouse (StarRocks → BI tool). The data flow is
opposite; the registry shape is the same.

## Why a separate registry?

- Different lifecycle. Sources are created **before** ingest; destinations
  are created **after** the stack reaches READY.
- Different threat model. Source credentials are for systems **we** read;
  destination credentials are for systems we **emit to** (push_api) or
  per-destination StarRocks users **we provision** (sql_pull).
- Different routes. Existing source routes are FROZEN; mirroring them with
  a parallel namespace keeps both backwards-compatible and contract-clean.

## Connection modes

Each destination picks exactly one of three modes:

### sql_pull (default, GA in v0.6)

The downstream tool connects directly to StarRocks' MySQL protocol on port
9030 using a per-destination read-only user that Studio provisions for it.

Flow:
1. Operator creates the destination with `kind=insyght`, `connection_mode=sql_pull`,
   and supplies a password.
2. Studio encrypts the password with Fernet and stores it.
3. Operator clicks **Provision** → Studio runs
   `CREATE USER 'insyght_reader'@'%' IDENTIFIED BY '<password>'` and
   `GRANT SELECT_PRIV ON udp.* TO 'insyght_reader'@'%'` against StarRocks
   via the dedicated admin-SQL path in `insyght_connector._run_admin_sql`.
4. Operator clicks **View Connection** → Studio renders the connection
   bundle (host/port/database/username) for the operator to paste into
   the BI tool. The password is **never** displayed; the operator already
   has it (they set it in step 1).
5. Operator clicks **Test** → Studio opens a real pymysql connection
   using the stored password to prove credentials work end-to-end.

### push_api (preview, v0.6.1)

Studio POSTs change events (Iceberg commits, ingest completions, etc.) to
a webhook URL the operator configures. The handshake test is implemented;
real provisioning requires the vendor's webhook registration spec.

**For Insyght: STUB.** Real implementation pending the published Insyght
API spec — what shape of event they expect, what auth scheme, what retry
policy. Tracked in v0.6.1.

### file_drop (stub in v0.6)

Studio writes snapshot files (parquet/CSV) to an object-store bucket on a
schedule. The test endpoint validates the config shape; the actual write
loop lands in v0.6.1.

## Insyght-specific notes

The default is `sql_pull` and that's what works today. Default config:

```yaml
host: 127.0.0.1
port: 9030
database: udp
username: insyght_reader
```

Wired through `insyght_connector.default_config(install_id)`. Operators
can override any field at create time (e.g. for a different StarRocks
host, or a non-default lake database).

`push_api` mode exists in the model but its provisioner is a documented
stub. The frontend should grey it out for Insyght until v0.6.1.

## Why admin SQL is separate from sql_editor

`backend/sql_editor.py` has a hard read-only invariant: its allow-list
rejects every destructive verb (CREATE, GRANT, etc.). That invariant is
load-bearing — operators run free-form SQL through that path and we don't
want to widen the attack surface.

The Insyght provisioner needs `CREATE USER` + `GRANT`, so it has its own
`_run_admin_sql` helper in `insyght_connector.py`. It uses the same
`docker exec udp-starrocks-fe mysql ...` pattern but with a tight
whitelist of verbs and aggressive identifier/password validation before
any string interpolation hits the shell.

## API surface (added in v0.6)

All routes are AuthDep. Routes that talk to running StarRocks (test,
provision) require the install to be in READY state.

| Method | Path | Purpose |
|--------|------|---------|
| POST   | `/api/installs/{install_id}/destinations`              | Create a destination |
| GET    | `/api/installs/{install_id}/destinations`              | List destinations for an install |
| GET    | `/api/destinations/{destination_id}`                   | Get one destination (scrubbed) |
| POST   | `/api/destinations/{destination_id}/test`              | Open a real driver connection / handshake |
| POST   | `/api/destinations/{destination_id}/provision`         | Create StarRocks user + grants (sql_pull) |
| GET    | `/api/destinations/{destination_id}/connection`        | Sanitized connection bundle (no plaintext) |
| DELETE | `/api/destinations/{destination_id}`                   | Forget the destination |

The route count moved from 92 to 99 (+7).

### Sample connection payload (sql_pull)

```json
{
  "destination_id": "dst_a1b2c3d4e5f6",
  "kind": "insyght",
  "name": "Insyght prod",
  "mode": "sql_pull",
  "has_credentials": true,
  "password": "•••• stored at create time — never shown again",
  "instructions": ["1. Open Insyght → Data Sources → Add → MySQL.", "..."],
  "host": "127.0.0.1",
  "port": 9030,
  "database": "udp",
  "username": "insyght_reader",
  "jdbc_url": "jdbc:mysql://127.0.0.1:9030/udp?useSSL=false",
  "mysql_cli": "mysql -h 127.0.0.1 -P 9030 -u insyght_reader -p udp"
}
```

The redaction marker (`••••`) lives in `destinations._REDACTED`. Operators
see it everywhere the credential **would** be — by design.

## Adding a new destination kind

1. Extend the `DestinationKind` Literal in `backend/destinations.py`:
   ```python
   DestinationKind = Literal[
       "insyght", "tableau", ..., "my_new_tool",
   ]
   ```
2. Add a catalog entry under `destinations:` in
   `stacks/components-catalog.yaml` with logo + tagline.
3. If the new vendor needs custom provisioning, add a per-vendor module
   alongside `insyght_connector.py` (don't bury vendor logic in the
   registry).
4. If the new vendor speaks MySQL protocol and just needs a SELECT-only
   user, you can reuse `insyght_connector.provision_sql_pull` directly —
   the SQL is vendor-neutral.
5. Drop a logo SVG into `frontend/assets/logos/{id}.svg` (or rely on the
   monogram fallback).
6. Add a smoke test to `tests/test_destinations.py` for the new kind.

## What's NOT in v0.6

- **push_api end-to-end for Insyght** — STUB. Awaits Insyght's webhook
  spec (user input pending).
- **file_drop live writability probe** — stub returns a sensible
  success-with-caveat message.
- **Per-destination event filtering** — every destination today sees the
  whole lake. Filtering by namespace/table lands when push_api lands.
- **Credential rotation** — today, rotate = delete + recreate. A proper
  PATCH endpoint that re-encrypts in-place lands later.
