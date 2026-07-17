#!/usr/bin/env bash
# Pre-flight checks for streaming-local-v1.0
set -euo pipefail

PASS=0; FAIL=0
ok()   { echo "  [OK]   $1"; PASS=$((PASS+1)); }
fail() { echo "  [FAIL] $1"; FAIL=$((FAIL+1)); }
warn() { echo "  [WARN] $1"; }

echo "[doctor] streaming-local-v1.0 pre-flight checks"

# Docker
docker info >/dev/null 2>&1 && ok "Docker daemon running" || fail "Docker not running"

# Docker Compose
docker compose version >/dev/null 2>&1 && ok "Docker Compose available" || fail "docker compose not available"

# RAM
TOTAL_RAM=$(docker run --rm --memory=128m alpine sh -c "cat /proc/meminfo" 2>/dev/null | grep MemTotal | awk '{print int($2/1024/1024)}' || echo "?")
if [ "$TOTAL_RAM" != "?" ] && [ "$TOTAL_RAM" -ge 8 ]; then
  ok "RAM >= 8 GB ($TOTAL_RAM GB reported by Docker)"
else
  warn "Could not verify RAM; recommend >= 8 GB"
fi

# Ports — a port held by this stack's OWN sl-* container is fine:
# `docker compose up` reuses/recreates those containers in place. Only a
# FOREIGN process/container squatting on a port is a real conflict.
OWN_PORTS=$(docker ps --filter "name=^sl-" --format '{{.Ports}}' 2>/dev/null \
  | grep -oE ':[0-9]+->' | grep -oE '[0-9]+' | sort -u || true)
for PORT in 9010 9011 8282 9092 8083 8890 8034 9034; do
  if ! (echo > /dev/tcp/localhost/$PORT) 2>/dev/null; then
    ok "Port $PORT free"
  elif echo "$OWN_PORTS" | grep -qx "$PORT"; then
    ok "Port $PORT in use by this stack's own container (compose will reuse it)"
  else
    fail "Port $PORT already in use"
  fi
done

# Required images present OR pullable
for IMG in \
  "minio/minio:RELEASE.2025-04-22T22-12-26Z" \
  "tabulario/iceberg-rest:1.6.0" \
  "apache/kafka:3.8.0" \
  "tabulario/spark-iceberg:3.5.5_1.8.1" \
  "starrocks/fe-ubuntu:3.3.12" \
  "starrocks/be-ubuntu:3.3.12"; do
  if docker image inspect "$IMG" >/dev/null 2>&1; then
    ok "Image cached: $IMG"
  else
    warn "Image not cached (will pull): $IMG"
  fi
done

echo ""
echo "[doctor] $PASS checks passed, $FAIL failed"
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
