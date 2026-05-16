# Audit Log

Lakehouse Studio v0.5 ships an **opt-in**, pure-additive audit trail that
records every install state transition and critical error to a separate
SQLite database. The default v0.4 behaviour is unchanged — when the feature
is off, no DB is created and the event bus is byte-identical to before.

## Why a separate DB

Studio's primary state lives in `work/state.json` (per-install records, no
history). The audit log answers a different question — *"who/what changed
this install, and when?"* — and the operator must be able to truncate or
prune it without risking installer state. A standalone `work/audit.sqlite`
keeps these concerns isolated and matches the v1.0 multi-tenant schema
under [`backend/v1/multi_tenant_schema.py`](../backend/v1/multi_tenant_schema.py)
so the eventual migration is a copy-paste of rows, not a reshape.

## Enable

```sh
export LHS_AUDIT_ENABLED=true
# Optional — defaults to 90 days.
export LHS_AUDIT_RETENTION_DAYS=90
```

Then restart the server. On startup the audit subscriber:

1. Initialises the schema at `work/audit.sqlite` (idempotent — safe to
   re-enable across restarts).
2. Taps `backend.events.bus` and forwards every published `LogEvent` to a
   private async queue.
3. Filters for the event kinds that matter for compliance
   (`state`, `error`, `step_start`, `step_end`) and drops the rest.
4. Scrubs every payload string through `backend.redact` before INSERT —
   matching the same patterns used by the live log stream and notifications
   drivers.
5. Persists the row off the bus thread via `asyncio.to_thread`, so disk I/O
   never back-pressures the installer.

If the subscriber fails to start, the rest of the app keeps running and a
warning is logged under the `lhs.audit` logger.

## Database location

```
work/audit.sqlite
```

(or whatever `LHS_WORK_DIR` is set to, prefixed onto `audit.sqlite`).

## Schema

The `audit_log` table is defined in
[`backend/v1/multi_tenant_schema.py`](../backend/v1/multi_tenant_schema.py)
and re-used verbatim here:

| column            | type    | notes                                    |
|-------------------|---------|------------------------------------------|
| id                | INTEGER | autoincrement primary key                |
| tenant_id         | TEXT    | `"default"` in v0.5; v1.0 will populate  |
| user_id           | TEXT    | NULL for system-emitted events           |
| action            | TEXT    | e.g. `install.state_change`              |
| resource_type     | TEXT    | e.g. `install`                           |
| resource_id       | TEXT    | the install_id for bus-sourced rows      |
| ts                | REAL    | unix seconds                             |
| ip                | TEXT    | NULL for system-emitted events           |
| redacted_payload  | TEXT    | JSON; secrets already scrubbed           |

Indexes: `(tenant_id, ts DESC)` for tenant scans, `(resource_type, resource_id)`
for per-install history.

The `AuditEntry` Pydantic model in `backend/audit_log.py` mirrors this
shape and is what `/api/audit` returns.

## API

### `GET /api/audit`

Authenticated (uses `AuthDep`). Returns `503` when `LHS_AUDIT_ENABLED` is
not set.

Query parameters:

| param          | type   | notes                                                  |
|----------------|--------|--------------------------------------------------------|
| `actor`        | string | match by `user_id`; pass `"system"` to find unattributed rows |
| `action`       | string | exact match, e.g. `install.state_change`               |
| `resource_type`| string | exact match, e.g. `install`                            |
| `since`        | string | unix seconds (float) OR ISO-8601 datetime              |
| `limit`        | int    | default 200, max 5000                                  |

### Examples

```sh
# Every state change in the last 24 hours
curl -H "Authorization: Bearer $LHS_AUTH_TOKEN" \
     "http://localhost:8080/api/audit?action=install.state_change&since=$(($(date +%s) - 86400))"

# Everything for one install
curl -H "Authorization: Bearer $LHS_AUTH_TOKEN" \
     "http://localhost:8080/api/audit?resource_type=install&limit=50" \
     | jq '.[] | select(.resource_id == "inst_abc123")'

# System-emitted errors only
curl -H "Authorization: Bearer $LHS_AUTH_TOKEN" \
     "http://localhost:8080/api/audit?actor=system&action=install.error"
```

The response is a JSON array of `AuditEntry` objects:

```json
[
  {
    "entry_id": "aud_8f9a1b2c3d4e5f60",
    "ts": 1715817600.123,
    "actor": "system",
    "action": "install.state_change",
    "resource_type": "install",
    "resource_id": "inst_abc123",
    "redacted_payload": {
      "status": "READY",
      "step": "smoke"
    },
    "ip": null
  }
]
```

## Retention

`LHS_AUDIT_RETENTION_DAYS` (default `90`) is honoured by the
`audit_log.retention_prune(older_than_days=N)` coroutine. Two ways to
run it:

### Automatic (recommended)

Set `LHS_AUDIT_SCHEDULER_ENABLED=true` and restart. The retention
scheduler starts alongside the subscriber and prunes rows once per
`LHS_AUDIT_PRUNE_INTERVAL_SECONDS` (default `86400` — daily). Lifecycle
is wired into the FastAPI lifespan in `backend/main.py`, so the
scheduler stops cleanly on shutdown.

```sh
export LHS_AUDIT_ENABLED=true
export LHS_AUDIT_SCHEDULER_ENABLED=true
# Optional overrides
export LHS_AUDIT_RETENTION_DAYS=180
export LHS_AUDIT_PRUNE_INTERVAL_SECONDS=43200   # twice daily
```

### Manual / on-demand

Useful for one-off cleanups or when the scheduler is intentionally off:

```sh
python -c "
import asyncio
from backend import audit_log
deleted = asyncio.run(audit_log.retention_prune())
print(f'pruned {deleted} rows')
"
```

### Concurrency note

All audit DB writes go through `_connect()`, which sets
`journal_mode=WAL`, `synchronous=NORMAL`, and `busy_timeout=5000`. The
retention purge and the bus subscriber can therefore run concurrently
without `database is locked` errors dropping audit writes.

## Disabling

Unset `LHS_AUDIT_ENABLED` (or set it to `false`) and restart. The DB file
remains on disk — delete it manually if you want a fresh start. No
in-process state outlives the shutdown.
