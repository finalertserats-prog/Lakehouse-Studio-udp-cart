# LakeHouse Studio

UI-driven, compatibility-validated installer for open data lakehouses. First certified stack: **Unified Data Plug (UDP)** — MinIO + Iceberg REST + Spark + StarRocks.

This is the **pilot build**: one stack, one deployment target (localhost via Docker), one killer flow — shop → inspect → install → smoke test → query.

## What it does

1. **Browse** certified stacks (currently UDP v0.2).
2. **Inspect** the target machine — Docker, Compose, bash, RAM, CPU, disk, port conflicts.
3. **Install** by cloning UDP, writing `.env`, running doctor → start → bootstrap → smoke-test.
4. **Stream** every log line live over WebSocket with per-step progress.
5. **Verify** with the demo dataset; show MinIO / Iceberg / Spark / StarRocks URLs.
6. **Operate** — status / stop / clean / re-run smoke test from the same UI.

The architecture mirrors the founding spec (Presentation / Orchestration / Intelligence / Knowledge / Execution / Target), narrowed to a working MVP.

## Requirements

- **Python 3.11+** with pip
- **Docker Desktop** (Linux containers) — UDP runs as Docker Compose
- **bash** in PATH — comes with Git for Windows / WSL / any Linux/macOS
- **git** in PATH

Verified on Windows 11 with Docker Desktop + Git Bash.

## Quick start — local

```powershell
# Windows (PowerShell)
powershell -ExecutionPolicy Bypass -File .\run.ps1
```

```bash
# Linux / macOS / Git Bash
bash run.sh
```

The script: creates `.venv/`, installs deps, boots uvicorn on `127.0.0.1:7878`. Open <http://127.0.0.1:7878> and click **Start Pilot Install**.

## Quick start — VPS (recommended for real pilots)

UDP needs ~50 GB disk + Docker; install Studio + UDP on the VPS together rather than driving from a laptop.

```bash
# 1. On the VPS (Ubuntu 22.04+ recommended):
sudo apt update && sudo apt install -y python3.11 python3.11-venv git docker.io docker-compose-plugin
sudo systemctl enable --now docker
sudo usermod -aG docker $USER && newgrp docker   # logout/login if needed

# 2. Clone Studio
git clone https://github.com/finalertserats-prog/Lakehouse-Studio-udp-cart.git
cd Lakehouse-Studio-udp-cart

# 3. Generate an auth token + bind to all interfaces
export LHS_AUTH_TOKEN=$(python3 -c 'import secrets;print(secrets.token_urlsafe(32))')
export LHS_BIND=0.0.0.0
export LHS_HOST=<your-vps-public-hostname-or-ip>
echo "Studio auth token: $LHS_AUTH_TOKEN"   # save this; paste it in browser

# 4. Run
bash run.sh
```

Then on your laptop browser: `http://<VPS_IP>:7878` — Studio prompts for the token on first request. In the install flow, set **Host** to your VPS's public hostname/IP so the success-screen URLs (MinIO, Spark, StarRocks) work from your laptop.

**Security note:** UDP exposes services on ports 9000/9001/8181/8888/8030/9030. By default these are bound to all interfaces on the VPS. Restrict via firewall (`ufw`) or VPN before exposing publicly.

## Configuration

| Env var | Default | Purpose |
|---|---|---|
| `LHS_HOST` | `127.0.0.1` | Hostname shown to user in URLs |
| `LHS_BIND` | `$LHS_HOST` | Interface uvicorn binds to. Set `0.0.0.0` on a VPS. |
| `LHS_PORT` | `7878` | Studio HTTP port |
| `LHS_AUTH_TOKEN` | _(unset)_ | If set, all API requests require `Authorization: Bearer <token>`. **Required when binding to non-loopback.** |
| `LHS_WORK_DIR` | `./work` | Where UDP gets cloned by default |

## Layout

```
.
├── backend/                FastAPI app
│   ├── main.py             REST + WebSocket
│   ├── inspector.py        Pre-flight checks
│   ├── runner.py           UDP subprocess driver
│   ├── stack_manifest.py   Loads stacks/*.yaml
│   ├── state.py            Install records (JSON-backed)
│   ├── events.py           Per-install event bus
│   └── models.py           Pydantic models
├── frontend/
│   └── index.html          Single-page UI (Tailwind via CDN, vanilla JS)
├── stacks/
│   └── udp-local-v0.2.yaml First entry in the compatibility matrix
├── work/                   Created at runtime; UDP cloned here
├── evidence/               (future) per-install evidence artifacts
├── requirements.txt
├── run.ps1 / run.sh
└── README.md
```

## API

| Method | Path | Purpose |
|---|---|---|
| GET    | `/api/stacks`                              | List stacks |
| GET    | `/api/stacks/{id}`                         | Full manifest |
| POST   | `/api/inspect`                             | Run pre-flight checks |
| GET    | `/api/installs`                            | List installs |
| POST   | `/api/installs`                            | Start an install |
| GET    | `/api/installs/{id}`                       | Install record |
| WS     | `/api/installs/{id}/logs`                  | Live log stream (replays history) |
| POST   | `/api/installs/{id}/control`               | `{action: status\|stop\|clean\|smoke}` |

## Pilot scope (what's IN / what's OUT)

**In:**
- Single stack (UDP)
- Single target (localhost or VPS where Studio runs)
- Subprocess execution via local `bash`
- Live log streaming + state machine + retry/skip/rollback
- Day-2 ops: status / stop / clean / smoke / structured smoke / SQL editor
- Pre-flight inspection (Docker / Compose / RAM / disk / ports)
- Secret redaction in logs + evidence files

**Out (deferred — see "Deferred risks" below):**
- SSH-to-remote-server execution (agent comes later)
- Multi-tenant / SSO
- Rust SAT solver (manifest is the matrix v0)
- Kubernetes target
- Air-gapped / signed offline archives
- Billing / SaaS control plane

The deferred items are real and important — they're just not what makes the pilot work. The pilot's job is to prove the **shop → install → query** loop end-to-end.

---

## Deferred risks (read this before evaluating Studio)

Studio v0.3 has been through 5 parallel code reviews + Codex adversarial dissent (council pass, commits `c287ac0` + `62ca672`). All CRITICAL and HIGH bug fixes from those reviews are applied. The items below are **acknowledged design decisions, not undiscovered bugs.** Each one trades v0.3 simplicity for v0.4+ work.

### 🟠 Cart→Stack is a "Guided Illusion" *(Gemini's #1 architectural call-out)*
The Build screen lets you pick components category-by-category and computes a live compatibility score. **But the install always runs the same `stacks/udp-local-v0.2.yaml` regardless of what's in your cart.** This is acceptable for v0.3 because UDP is the only certified stack — there's no other valid combination to compile to. It becomes a real smell the moment a second stack exists. **v0.4 fix:** build a Cart-to-Stack compiler that generates a manifest from the cart selections.

### 🟠 Auth token in `localStorage`, not httpOnly cookie
The bearer token for `LHS_AUTH_TOKEN` is stored in browser `localStorage` so JavaScript can attach it to fetch headers. **If an XSS vulnerability sneaks past the F3 escaping audit, the token can be exfiltrated.** We escaped every server-JSON interpolation we could find, but the threat surface remains. **Mitigated by:** every numeric and string from the server now goes through `esc()`; component IDs validated against an allowlist regex; CSP not yet set. **v0.4 fix:** move to httpOnly session cookies, which requires a session backend we don't have yet (~2-3 days).

### 🟠 Transport coupling — `runner.py` hardcoded to local `bash`
The install runner uses `asyncio.create_subprocess_exec(bash, ...)` directly. There's no abstraction for SSH-to-remote-host, agent-RPC, or Kubernetes exec. **For v0.3 this is fine** (Studio runs on the same machine as Docker). **For v0.4 remote-VPS deployment** you'll need a `Transport` interface with `LocalTransport`, `SSHTransport`, and eventually `AgentTransport` implementations. Building this with one concrete user is premature abstraction; better done when SSH is the second user.

### 🟡 No process supervisor / install re-attach
If the FastAPI process crashes mid-install, the install record is **automatically reconciled to `FAILED` on next startup** (via `StateStore._load`'s non-terminal-state sweep). But the actual subprocess running `docker compose up` becomes an orphan; we don't try to re-attach to it. **Mitigated by:** rollback works after reconciliation, so the user can recover. **v0.4 fix:** PID file per install + reconnect logic on restart.

### 🟡 Sync FastAPI handlers can block the event loop
`POST /api/inspect` runs ~8 subprocess calls (Docker version, port probes, etc.) synchronously — up to 30s total. Each call holds an event-loop thread. **Mitigated by:** FastAPI runs sync handlers in a 40-slot threadpool; single-user pilot can absorb the load. **v0.4 fix:** convert to `async def` + `asyncio.to_thread(...)` for the blocking subprocess calls (~1 day).

### 🟡 Scoring/sizing/cart logic baked into Python, not the catalog YAML
`backend/scorer.py`, `backend/sizer.py`, and `backend/cart.py` encode knowledge about UDP components in Python. **Painful when you add a second stack** — you'll edit five Python modules to teach Studio about its scoring. **v0.4 fix:** move per-component scoring rules + size profiles into the catalog YAML, treat them as data.

### 🟡 Two manifest schemas (`stacks/components-catalog.yaml` + `stacks/udp-local-v0.2.yaml`)
The catalog and the stack manifest are parsed by separate loaders (`catalog.py` + `stack_manifest.py`). Both currently validate cleanly. **Schema debt** — adding a field for "minimum K8s version" means touching two files. **v0.4 fix:** unify: catalog = library of component definitions, stack = bill-of-materials referencing catalog IDs.

### 🟡 Push-based state mutation (not a single state machine)
Runner writes to the event bus and the state store as separate operations. If `evidence.py` fails to capture (rare), the state still transitions to `READY`. **No transactional invariant** across the two stores. Mitigated by `_step_finalize` now reporting evidence-capture failure on the step itself (Bucket 1 fix). **v0.4 fix:** single TaskTree state machine, with the event bus + state store as projections.

### 🟢 Per-install state files (Codex specifically warned against changing this without a migration plan)
Performance review flagged that `state.py` rewrites the entire `state.json` file on every step transition (~32 writes per install). **Fixed in v0.3 with a 250ms debounce** + terminal-state force-flush (commit `c287ac0`). Per-install files would split it further but require a migration. **Deferred until necessary** — debounce is sufficient at v0.3 scale.

### 🟢 Free-form SQL editor sandbox
`POST /api/installs/{id}/sql` accepts user SQL but pre-validates it: only `SELECT / SHOW / DESCRIBE / EXPLAIN / WITH` leading keywords, destructive verbs (`DROP`, `DELETE`, `INSERT`, `UPDATE`, `TRUNCATE`, etc.) rejected anywhere in the cleaned-comments-and-strings SQL, 10KB max, 30s timeout, 1000-row cap, audit-logged to the event bus. **Mitigations applied; the sandbox is not provably airtight** — clever SQL constructs (CTEs, window functions, JSON queries) could in theory bypass. **v0.4 consideration:** use a proper SQL parser (e.g., `sqlglot`) for AST-level validation if the sandbox proves insufficient.

### 🟢 Council review — security-reviewer never returned
The 6th parallel reviewer was a `security-reviewer` agent that ran for 30+ minutes without returning. The other 5 reviewers (code-reviewer ×2, silent-failure-hunter, Gemini architectural, performance-optimizer) all surfaced findings that were applied. **Risk:** a CRITICAL specifically in the OWASP / WebSocket / subprocess-escape area might exist that none of the 5 other lenses caught. **Mitigation:** if found, it's a follow-up commit.

---

## v0.4 roadmap (in priority order)

1. **Cart-to-Stack compiler** — unblocks adding any 2nd stack
2. **Move scoring/sizing rules from Python to catalog YAML** — required before #1 is sensible
3. **Unify the two manifest schemas** — needed for #1 + #2
4. **Real data onboarding** — CSV upload → Iceberg table → query (the post-install "now what?")
5. **Transport abstraction** + SSH transport — unlocks remote-VPS deployment
6. **httpOnly cookie auth** + session backend
7. **Async-ify sync FastAPI handlers** — performance under multi-user load
8. **Process supervisor + restart re-attach** — true crash-resilience
9. **Stack templates** for the 5 use-case goals (currently all map to UDP)
10. **Hosted SaaS control plane** — multi-tenant

## License

Code TBD (will be Apache 2.0 on public release). The compatibility manifest format is intended to become an open standard.
