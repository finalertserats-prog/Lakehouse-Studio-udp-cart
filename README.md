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
- Single target (localhost)
- Subprocess execution via local `bash`
- Live log streaming + state machine
- Day-2: status / stop / clean / smoke
- Secret redaction in logs

**Out (deferred):**
- SSH-to-remote-server execution (agent comes later)
- Multi-tenant / auth / SSO
- Rust SAT solver (manifest is the matrix v0)
- Kubernetes target
- Air-gapped / signed offline archives
- Billing / SaaS control plane

The deferred items are real and important — they're just not what makes the pilot work. The pilot's job is to prove the **shop → install → query** loop end-to-end.

## License

Code TBD (will be Apache 2.0 on public release). The compatibility manifest format is intended to become an open standard.
