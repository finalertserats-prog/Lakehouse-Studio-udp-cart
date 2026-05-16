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
LHS_PORT="${LHS_PORT:-7878}"

echo "LakeHouse Studio starting at http://$LHS_HOST:$LHS_PORT"
exec "$PY" -m uvicorn backend.main:app --host "$LHS_HOST" --port "$LHS_PORT"
