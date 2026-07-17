#!/usr/bin/env bash
# Build Apache Ranger 3.0.0-SNAPSHOT from Apache GitHub master branch.
# This replaces the stable 2.8.0 images with SNAPSHOT builds that match production.
#
# WARNING: First build takes 30-60 minutes (Maven downloads ~1GB of dependencies).
# Subsequent builds are fast (~2 min) due to Docker layer cache.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STACK_DIR="$(dirname "$SCRIPT_DIR")"

echo "[ranger-snapshot] building Ranger 3.0.0-SNAPSHOT from source..."
echo "[ranger-snapshot] this will take 30-60 minutes on first run."
echo ""

docker build \
  --target ranger-admin \
  -t ehd-ranger-admin:3.0.0-SNAPSHOT \
  "${STACK_DIR}/docker/ranger-build/"

docker build \
  --target ranger-usersync \
  -t ehd-ranger-usersync:3.0.0-SNAPSHOT \
  "${STACK_DIR}/docker/ranger-build/"

echo ""
echo "[ranger-snapshot] done. Images:"
docker images | grep 'ehd-ranger'
echo ""
echo "[ranger-snapshot] update docker-compose.yml to use 3.0.0-SNAPSHOT tags, then:"
echo "  docker compose up -d --no-build ranger-admin ranger-usersync"
