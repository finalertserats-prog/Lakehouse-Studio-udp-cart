#!/usr/bin/env bash
# Pre-flight checks for Enterprise Hadoop Datalake stack.
set -euo pipefail

echo "[ehd-doctor] running pre-flight checks..."
fail=0

# Docker present and daemon running
if ! command -v docker >/dev/null 2>&1; then
  echo "MISSING: docker not found"; fail=1
else
  echo "OK: docker present"
  if ! docker info >/dev/null 2>&1; then
    echo "ERROR: docker daemon not running"; fail=1
  else
    echo "OK: docker daemon running"
  fi
fi

# Docker Compose
if docker compose version >/dev/null 2>&1; then
  echo "OK: docker compose (plugin) found"
elif command -v docker-compose >/dev/null 2>&1; then
  echo "OK: docker-compose (standalone) found"
else
  echo "MISSING: Docker Compose not found"; fail=1
fi

# RAM — minimum 24 GB
mem_kb="$(awk '/MemTotal/ {print $2}' /proc/meminfo 2>/dev/null || echo 0)"
mem_gb=$(( mem_kb / 1024 / 1024 ))
if [ "$mem_kb" -lt 23000000 ]; then
  echo "WARN: only ~${mem_gb} GB RAM detected — minimum 24 GB required (stack may OOM)"
else
  echo "OK: RAM ~${mem_gb} GB (>= 24 GB)"
fi

# Disk — minimum 40 GB free
free_kb="$(df --output=avail . 2>/dev/null | tail -n1 || echo 0)"
free_gb=$(( ${free_kb:-0} / 1024 / 1024 ))
if [ "${free_kb:-0}" -lt 40000000 ]; then
  echo "WARN: only ~${free_gb} GB free — minimum 40 GB required"
else
  echo "OK: disk free ~${free_gb} GB (>= 40 GB)"
fi

# Required ports
for port in 9870 8088 18030 19030 10000 18080; do
  if command -v ss >/dev/null 2>&1; then
    busy=$(ss -tlnp 2>/dev/null | awk '{print $4}' | grep -c ":${port}$" || true)
  elif command -v lsof >/dev/null 2>&1; then
    busy=$(lsof -i ":$port" 2>/dev/null | grep -c LISTEN || true)
  else
    busy=0
  fi
  if [ "${busy:-0}" -gt 0 ]; then
    echo "WARN: port $port is already in use"
  else
    echo "OK: port $port free"
  fi
done

if [ "$fail" -eq 0 ]; then
  echo "[ehd-doctor] all required checks passed"
else
  echo "[ehd-doctor] one or more required checks FAILED"
fi
exit "$fail"
