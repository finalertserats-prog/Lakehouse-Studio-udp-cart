# `lks` — Lakehouse Studio CLI

Thin client over the Studio HTTP API. No backend coupling — every command
maps to one or more REST/WS calls on the running Studio server.

## Install

The CLI ships in-tree. Install dependencies from the project root:

```bash
pip install -r requirements.txt
```

Two ways to invoke:

```bash
# As a module (always works)
python -m cli --help

# As a console script (after `pip install -e .` once pyproject.toml is added)
lks --help
```

## Global flags

| Flag        | Default                 | Purpose                                        |
|-------------|-------------------------|------------------------------------------------|
| `--server`  | `http://127.0.0.1:7878` | Studio server base URL                         |
| `--token`   | env `LHS_TOKEN`         | Bearer credential (only needed if server enforces auth) |
| `--output`  | `table`                 | `table` or `json` (json is jq-friendly)        |

## Command map

```
catalog
  list                          List catalog components by category
templates
  list                          List use-case templates
stacks
  list                          List stack manifests
  compat   <stack_id>           Lock summary + drift
  upgrades <stack_id>           Curated upgrade candidates
install
  create   <stack_id> [opts]    Kick off a new install
  status   <install_id>         Install state + per-step table
  logs     <install_id> [-f]    History; with -f, stream WS events
  retry    <install_id> <step>  Resume from a FAILED step
  skip     <install_id> <step>  Skip a step and continue
  cancel   <install_id>         Cancel in-flight install
health     <install_id>         Per-service health snapshot
backup
  create   <install_id> [--kind metadata|full]
  list     <install_id>
  restore  <backup_id>
  delete   <backup_id>
tables
  list     <install_id>                       Every (ns, table)
  describe <install_id> <ns> <name>           Single-table detail
ai
  ask      <install_id> "<question>"          Ask Studio
export     <install_id> -o stack.tar.gz       GitOps bundle download
```

## Examples

```bash
# Browse what's installable
python -m cli catalog list
python -m cli templates list
python -m cli stacks list

# Inspect a stack's certified versions
python -m cli stacks compat udp-local-v0.2

# Start an install
python -m cli install create udp-local-v0.2 \
    --host localhost --install-dir /tmp/udp --lake calm-river

# Watch it
python -m cli install status ins_abc123
python -m cli install logs   ins_abc123 --follow

# Resume after a failure
python -m cli install retry ins_abc123 doctor
python -m cli install skip  ins_abc123 smoke

# After READY
python -m cli health   ins_abc123
python -m cli tables   list ins_abc123
python -m cli tables   describe ins_abc123 default events
python -m cli ai ask   ins_abc123 "why is iceberg-rest unhealthy?"

# Backup / restore
python -m cli backup create  ins_abc123 --kind full
python -m cli backup list    ins_abc123
python -m cli backup restore bkp_xyz

# GitOps export
python -m cli export ins_abc123 -o stack.tar.gz

# JSON output, pipe through jq
python -m cli --output json stacks list | jq '.[].id'
```

## Auth

Studio's auth is opt-in (controlled by `LHS_AUTH_TOKEN` on the server). If
enabled, set the same value on the client:

```bash
export LHS_TOKEN='your-credential-here'
python -m cli stacks list

# Or per-call
python -m cli --token 'your-credential-here' stacks list
```

The credential rides as a Bearer header. It is never logged.

## Exit codes

| Code | Meaning                                                       |
|------|---------------------------------------------------------------|
| 0    | Success                                                       |
| 1    | Backend returned 4xx/5xx, or the network call itself failed   |
| 2    | Click argument parsing error                                  |

## Streaming logs (`--follow`)

`install logs --follow` opens a WebSocket to `/api/installs/{id}/logs`.
This requires the `websockets` package:

```bash
pip install websockets
```

Without `--follow`, the command returns buffered history over plain HTTP
and has no extra dependency.
