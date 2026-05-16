#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

if [ ! -d ".venv" ]; then
  echo "Creating virtual environment..."
  python -m venv .venv || python3 -m venv .venv
fi

if [ -f ".venv/Scripts/pip.exe" ]; then
  PIP=".venv/Scripts/pip.exe"
  PY=".venv/Scripts/python.exe"
else
  PIP=".venv/bin/pip"
  PY=".venv/bin/python"
fi

"$PIP" install --quiet --disable-pip-version-check -r requirements.txt

LHS_HOST="${LHS_HOST:-127.0.0.1}"
LHS_BIND="${LHS_BIND:-$LHS_HOST}"
LHS_PORT="${LHS_PORT:-7878}"

if [ "$LHS_BIND" != "127.0.0.1" ] && [ "$LHS_BIND" != "localhost" ] && [ -z "${LHS_AUTH_TOKEN:-}" ]; then
  echo "WARN: binding to non-loopback ($LHS_BIND) without LHS_AUTH_TOKEN." >&2
  echo "      Anyone who can reach $LHS_BIND:$LHS_PORT can drive installs." >&2
fi

echo "LakeHouse Studio listening on $LHS_BIND:$LHS_PORT (open http://$LHS_HOST:$LHS_PORT)"
exec "$PY" -m uvicorn backend.main:app --host "$LHS_BIND" --port "$LHS_PORT"
