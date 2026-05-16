# v1.0 Architecture — Starter Scaffolds

This directory documents the four foundational interfaces a future v1.0 build
will rest on. The code lives under `backend/v1/`. **Nothing in `backend/v1/`
is wired into the running app.** Switching v1.0 on requires explicit work in
`backend/main.py` to flip the imports.

Until that flip happens, `backend/v1/` is dead weight on the import graph —
exactly as intended. Any module outside `backend/v1/` that imports from
`backend/v1/` should fail review.

## The four scaffolds

### 1. `backend/v1/executor_interface.py` — control-plane → target abstraction

Today every "do a thing on the host" call (`docker compose up`,
`docker exec ...`, container log scrape, TCP probe) is inlined as a
subprocess call in `backend/runner.py`, `backend/health.py`,
`backend/backup.py`. That hardwires the control plane to "local Docker
on the same box."

The `Executor` protocol defines six methods (`inspect`, `compose_up`,
`compose_down`, `exec_in_container`, `get_logs`, `port_probe`) and ships
three implementations:

- `LocalDockerExecutor` — the current behaviour. Real shell-outs, mirrors
  the patterns in `runner.py`. This is the reference implementation that
  v1.0 will retrofit into the existing call sites first.
- `KubernetesExecutor` — stub. Raises `NotImplementedError` with a clear
  "needs kubectl/helm wiring" message.
- `SshAgentExecutor` — stub. Documents that it delegates to the Go agent
  over the gRPC contract in `backend/v1/proto/agent.proto`.

`ExecutorTarget` describes WHERE to run (`docker_compose` /
`kubernetes` / `ssh_vm`, plus the kubeconfig / ssh key paths it needs).
`ExecResult` is the uniform return type — `{success, stdout, stderr,
exit_code, duration_ms}` — so callers don't care which target ran the
command.

### 2. `backend/v1/multi_tenant_schema.py` — JSON → SQLite

Today: `backend/state.py` is one JSON file, one tenant implied.

`SCHEMA` is five `CREATE TABLE` strings (no ORM): `tenants`, `users`,
`roles`, `installs`, `audit_log`. `installs` mirrors the current
`InstallRecord` Pydantic model plus a `tenant_id` FK.

- `init_schema(sqlite_path)` — idempotent table + index creation.
- `migrate_from_json(json_path, sqlite_path, default_tenant_name)` —
  one-shot bulk migration. Reads `state.json`, inserts every install
  under a single "default" tenant. Idempotent — re-running with the same
  input skips already-present `install_id`s.
- `seed_builtin_roles(sqlite_path)` — inserts the four roles defined in
  `rbac.py` (OWNER / ADMIN / OPERATOR / VIEWER).

NOT WIRED. `state.store` continues to be the source of truth. v1.0 will
run `migrate_from_json` once, then swap `store` for a SQLite-backed
implementation.

### 3. `backend/v1/rbac.py` — Permission / Role / route map

- `Permission` enum — 10 permissions covering installs, backups,
  upgrades, SQL, audit, settings, billing.
- `Role` Pydantic model — `name` + `set[Permission]`.
- `BUILTIN_ROLES` — `OWNER` (all), `ADMIN` (all except billing),
  `OPERATOR` (no settings, no delete), `VIEWER` (read-only).
- `has_permission(role, perm)` — single-line helper.
- `required_permission(method, route)` — looks up which permission a
  given API route needs. Exact-match for now; v1.0 can switch to
  pattern matching if it wants.
- `rbac_check(request)` — stub FastAPI dependency. **Always returns
  True in this scaffold.** The docstring spells out exactly what the
  v1.0 implementation needs to do.

Wiring this in is a one-liner in `main.py`: swap
`AuthDep = Depends(_require_auth)` for `AuthDep = Depends(rbac_check)`
once `request.state.user` is populated by an upstream middleware.

### 4. `backend/v1/proto/agent.proto` — Go agent gRPC contract

Defines `service AgentControl` with six RPCs matching the `Executor`
methods 1:1. `ExecResult` matches `executor_interface.ExecResult`.
`StreamLogs` is server-streaming (long-lived); everything else is
unary. mTLS only — per-agent client cert minted at enrolment.

The agent dials OUT to the control plane (so customer firewalls don't
need an inbound rule), and the control plane multiplexes per-tenant
agents over a single port.

No generated `_pb2.py` / `_pb2_grpc.py` is committed — v1.0's build step
runs `protoc` against this file.

## Migration order

The order is dictated by dependency, not preference. Skip a step and the
later steps regress.

1. **`Executor` abstraction first.**
   Route every existing shell-out through `LocalDockerExecutor` —
   `runner.py`, `health.py`, `backup.py`. Zero behaviour change, but
   afterwards every shell call has exactly one chokepoint. No targeting
   work yet; just refactor in place.

2. **RBAC next.**
   Add an auth middleware that populates `request.state.user`. Swap
   `_require_auth` for `rbac_check`. Roles are still tracked in memory
   at this point (or in the existing JSON) — multi-tenant SQLite comes
   in step 3.

3. **Multi-tenant SQLite.**
   Run `migrate_from_json` on a maintenance window. Rewrite `state.py`'s
   `StateStore` to back onto SQLite. All callsites continue to use the
   same `store.get` / `store.update_state` API. Every install gains a
   `tenant_id`; every request is scoped to a tenant via the user object
   from step 2.

4. **`KubernetesExecutor`.**
   Implement against `kubectl` + `helm`. Add a new `ExecutorTarget.kind
   = "kubernetes"` path to the install pipeline. Existing Docker
   Compose installs unchanged.

5. **`SshAgentExecutor` + Go agent.**
   Generate the gRPC stubs from `agent.proto`. Ship the Go agent
   binary. Add the enrolment endpoint (mints the mTLS client cert).
   Now the control plane can drive installs on customer infrastructure
   it has no SSH key for.

## Verifying nothing is wired

```bash
python -c "from backend.main import app; \
           import backend.v1.executor_interface, \
                  backend.v1.multi_tenant_schema, \
                  backend.v1.rbac; \
           print('routes:', len(app.routes))"
```

The route count MUST match the pre-scaffold baseline. If it changes, an
import side-effect leaked from `backend/v1/` into the running app — fix
before merging.
