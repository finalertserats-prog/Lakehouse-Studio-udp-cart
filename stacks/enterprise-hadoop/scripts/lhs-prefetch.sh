#!/usr/bin/env bash
# Lakehouse Studio — Pre-pull JARs needed before docker compose up
# Downloads Tez 0.10.4 and Hudi 1.0.1 JARs to local jars/ directories.
# Must run once before: docker compose up -d
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$(dirname "$SCRIPT_DIR")"

mkdir -p jars/tez jars/hudi

# Persistent cache shared across installs (survives fresh clones)
CACHE_DIR="${HOME}/.lakehouse-studio/cache/enterprise-hadoop"
mkdir -p "$CACHE_DIR/tez" "$CACHE_DIR/hudi"

cached_copy() {
  local src=$1 dest=$2
  if [ -f "$src" ]; then
    cp "$src" "$dest"
    echo "[prefetch]   restored from cache"
    return 0
  fi
  return 1
}

save_to_cache() {
  local src=$1 dest=$2
  cp "$src" "$dest"
}

curl_robust() {
  local url=$1 dest=$2 min_size=${3:-1000000}
  local tmp="${dest}.tmp"
  local attempt=0
  while true; do
    attempt=$((attempt + 1))
    rm -f "$tmp"
    echo "[prefetch]   attempt $attempt..."
    if curl -L --progress-bar --speed-limit 8192 --speed-time 30 --max-time 600 \
         "$url" -o "$tmp" 2>&1; then
      local sz
      sz=$(stat -c%s "$tmp" 2>/dev/null || echo 0)
      if [ "$sz" -ge "$min_size" ]; then
        mv "$tmp" "$dest"
        return 0
      fi
      echo "[prefetch]   download too small (${sz} bytes) — retrying..."
    else
      echo "[prefetch]   curl failed or stalled — retrying..."
    fi
    sleep 5
  done
}

# ── Apache Tez 0.10.4 ────────────────────────────────────────────────────────

TEZ_TAR=jars/tez/apache-tez-0.10.4-bin.tar.gz
TEZ_MIRRORS=(
  "https://dlcdn.apache.org/tez/0.10.4/apache-tez-0.10.4-bin.tar.gz"
  "https://archive.apache.org/dist/tez/0.10.4/apache-tez-0.10.4-bin.tar.gz"
)

if [ ! -f "$TEZ_TAR" ]; then
  echo "[prefetch] downloading Apache Tez 0.10.4..."
  if ! cached_copy "$CACHE_DIR/tez/apache-tez-0.10.4-bin.tar.gz" "$TEZ_TAR"; then
    downloaded=0
    for mirror in "${TEZ_MIRRORS[@]}"; do
      echo "[prefetch]   trying $mirror ..."
      if curl_robust "$mirror" "$TEZ_TAR" 50000000; then
        downloaded=1; break
      fi
      echo "[prefetch]   mirror failed, trying next..."
      rm -f "$TEZ_TAR"
    done
    if [ "$downloaded" = "0" ]; then echo "[prefetch] ERROR: all Tez mirrors failed"; exit 1; fi
    save_to_cache "$TEZ_TAR" "$CACHE_DIR/tez/apache-tez-0.10.4-bin.tar.gz"
  fi
fi

if [ ! -f "jars/tez/tez-common-0.10.4.jar" ]; then
  echo "[prefetch] extracting Tez JARs..."
  tar -xzf "$TEZ_TAR" -C jars/tez/ --strip-components=1
  echo "[prefetch] Tez JARs ready in jars/tez/"
else
  echo "[prefetch] Tez JARs already extracted"
fi

# ── Hudi 1.0.1 — Spark 3.4 bundle ───────────────────────────────────────────

HUDI_SPARK=jars/hudi/hudi-spark3.4-bundle_2.12-1.0.1.jar
if [ ! -f "$HUDI_SPARK" ]; then
  echo "[prefetch] downloading hudi-spark3.4-bundle_2.12-1.0.1.jar..."
  if ! cached_copy "$CACHE_DIR/hudi/hudi-spark3.4-bundle_2.12-1.0.1.jar" "$HUDI_SPARK"; then
    curl_robust \
      "https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.4-bundle_2.12/1.0.1/hudi-spark3.4-bundle_2.12-1.0.1.jar" \
      "$HUDI_SPARK" 100000000
    save_to_cache "$HUDI_SPARK" "$CACHE_DIR/hudi/hudi-spark3.4-bundle_2.12-1.0.1.jar"
  fi
else
  echo "[prefetch] hudi-spark3.4-bundle already present"
fi

# ── Hudi 1.0.1 — Hadoop MR bundle (for Hive) ─────────────────────────────────

HUDI_MR=jars/hudi/hudi-hadoop-mr-bundle-1.0.1.jar
if [ ! -f "$HUDI_MR" ]; then
  echo "[prefetch] downloading hudi-hadoop-mr-bundle-1.0.1.jar..."
  if ! cached_copy "$CACHE_DIR/hudi/hudi-hadoop-mr-bundle-1.0.1.jar" "$HUDI_MR"; then
    curl_robust \
      "https://repo1.maven.org/maven2/org/apache/hudi/hudi-hadoop-mr-bundle/1.0.1/hudi-hadoop-mr-bundle-1.0.1.jar" \
      "$HUDI_MR" 30000000
    save_to_cache "$HUDI_MR" "$CACHE_DIR/hudi/hudi-hadoop-mr-bundle-1.0.1.jar"
  fi
else
  echo "[prefetch] hudi-hadoop-mr-bundle already present"
fi

echo "[prefetch] all JARs ready — you can now run: docker compose up -d"
