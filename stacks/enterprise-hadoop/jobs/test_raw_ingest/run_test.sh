#!/usr/bin/env bash
# Test raw ingest pipeline — CLI runner (no Airflow required)
#
# Runs all 3 stages against the local Docker stack:
#   Stage 1: S3 gayatri2datalake staging → local HDFS (namenode:9870 WebHDFS)
#   Stage 2: local HDFS CSV → Hudi CoW table (spark-submit in ehd-spark)
#   Verify : Hive / StarRocks can see the test table
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ETL_CONFIG="${SCRIPT_DIR}/etl_config.yaml"
HUDI_JAR="/tmp/spark-bootstrap/hudi-spark3.4-bundle_2.12-1.0.1.jar"

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║   Lakehouse Studio — Test Raw Ingest Pipeline        ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""

# ── Prerequisites ──────────────────────────────────────────────────────────────

echo "[check] Python packages..."
pip install boto3 hdfs pyyaml --quiet --break-system-packages 2>/dev/null || \
pip install boto3 hdfs pyyaml --quiet

echo "[check] HDFS namenode reachable..."
curl -sf "http://localhost:9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus" \
  >/dev/null || { echo "ERROR: HDFS NameNode not reachable. Is the stack running?"; exit 1; }
echo "  HDFS OK"

echo "[check] ehd-spark container..."
docker inspect ehd-spark --format '{{.State.Status}}' 2>/dev/null | grep -q running \
  || { echo "ERROR: ehd-spark container not running."; exit 1; }
echo "  ehd-spark OK"

echo "[check] Hudi JAR in ehd-spark..."
docker exec ehd-spark test -f "${HUDI_JAR}" 2>/dev/null \
  || { echo "WARN: Hudi JAR not found. Run bootstrap first: bash scripts/lhs-bootstrap.sh"; }

# ── Stage 1: S3 → local HDFS ──────────────────────────────────────────────────

echo ""
echo "━━━ Stage 1: S3 → local HDFS ━━━━━━━━━━━━━━━━━━━━━━━━━━"
python3 "${SCRIPT_DIR}/01_s3_to_hdfs.py" \
  --etlconfig "${ETL_CONFIG}" \
  --script-home "${SCRIPT_DIR}"

echo ""
echo "[verify] HDFS contents after stage 1:"
docker exec ehd-namenode hdfs dfs -ls -R /techsophy/raw/test/biometric/ 2>/dev/null || \
  echo "  (no files yet — check logs/stage1_s3_to_hdfs.log)"

# ── Stage 2: HDFS → Hudi (spark-submit in ehd-spark) ─────────────────────────

echo ""
echo "━━━ Stage 2: HDFS CSV → Hudi CoW ━━━━━━━━━━━━━━━━━━━━━━━"

# Copy stage 2 script into the container
docker exec ehd-spark mkdir -p /tmp/test_ingest
docker cp "${SCRIPT_DIR}/02_ingest_to_hudi.py" ehd-spark:/tmp/test_ingest/02_ingest_to_hudi.py

docker exec ehd-spark \
  /opt/spark/bin/spark-submit \
  --master local[2] \
  --jars "${HUDI_JAR}" \
  /tmp/test_ingest/02_ingest_to_hudi.py \
  --csv_dir  /techsophy/raw/test/biometric/csvs/ \
  --yaml_dir /techsophy/raw/test/biometric/yamls/ \
  --hdfs_uri hdfs://namenode:9820 \
  --warehouse_dir /tmp/hive/warehouse \
  --yarn_hostname resourcemanager

# ── Verify: check Hive sees the test table ────────────────────────────────────

echo ""
echo "━━━ Verify: Hive / Beeline ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
docker exec ehd-hiveserver2 beeline \
  -u 'jdbc:hive2://localhost:10000' \
  -e 'SHOW DATABASES; USE default; SHOW TABLES;' \
  2>/dev/null | grep -v "^SLF4J" || true

echo ""
echo "━━━ Verify: HDFS Hudi files ━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
docker exec ehd-namenode hdfs dfs -ls /techsophy/raw/test/biometric/ 2>/dev/null || true

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║   Pipeline complete                                  ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""
echo "  Hive   : docker exec -it ehd-hiveserver2 beeline -u 'jdbc:hive2://localhost:10000'"
echo "  StarRocks: mysql -h 127.0.0.1 -P 19030 -u root -e 'set catalog hudi_catalog; show databases;'"
echo ""
