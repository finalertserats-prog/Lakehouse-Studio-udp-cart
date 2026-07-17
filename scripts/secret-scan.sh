#!/usr/bin/env bash
# secret-scan.sh — run the same gitleaks scan the CI gate runs, locally.
#
#   scripts/secret-scan.sh            # scan the working tree + history
#   scripts/secret-scan.sh --staged   # scan only staged changes (fast pre-commit)
#
# Exit 0 = clean, 1 = secret(s) found, 2 = gitleaks not installed.
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
CONFIG="$ROOT/.gitleaks.toml"

if ! command -v gitleaks >/dev/null 2>&1; then
  echo "gitleaks is not installed."
  echo "  macOS:  brew install gitleaks"
  echo "  linux:  curl -sSL https://github.com/gitleaks/gitleaks/releases/latest/download/gitleaks_8.21.2_linux_x64.tar.gz | tar xz gitleaks && sudo install -m0755 gitleaks /usr/local/bin/"
  exit 2
fi

cd "$ROOT"
if [ "${1:-}" = "--staged" ]; then
  echo "-> scanning staged changes ..."
  gitleaks protect --staged --config "$CONFIG" --redact --verbose --exit-code 1
else
  echo "-> scanning working tree + full history ..."
  gitleaks detect --source . --config "$CONFIG" --redact --verbose --exit-code 1
fi
echo "✅ no secrets found."
