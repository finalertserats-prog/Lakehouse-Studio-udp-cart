#!/usr/bin/env bash
# Streaming Lakehouse smoke test — streaming-local-v1.0
set -euo pipefail

export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

MINIO_BUCKET="${MINIO_BUCKET:-streaming-lake}"
MINIO_USER="${MINIO_ROOT_USER:-admin}"
MINIO_PASS="${MINIO_ROOT_PASSWORD:-streaming123}"

echo "[sl-smoke] checking MinIO..."
docker exec sl-minio mc ready local >/dev/null 2>&1 \
  && echo "  minio OK" || { echo "FAIL: minio"; exit 1; }

echo "[sl-smoke] checking Iceberg REST catalog..."
docker exec sl-spark curl -sf "http://sl-iceberg-rest:8181/v1/config" >/dev/null \
  && echo "  iceberg-rest OK" || { echo "FAIL: iceberg-rest"; exit 1; }

echo "[sl-smoke] checking Kafka broker..."
docker exec sl-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list >/dev/null 2>&1 \
  && echo "  kafka OK" || { echo "FAIL: kafka"; exit 1; }

echo "[sl-smoke] checking Kafka topic 'orders' exists..."
docker exec sl-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list 2>/dev/null | grep -q "^orders$" \
  && echo "  orders topic OK" || { echo "FAIL: orders topic missing"; exit 1; }

echo "[sl-smoke] checking Flink JobManager REST API..."
docker exec sl-flink-jobmanager curl -sf "http://localhost:8081/overview" >/dev/null \
  && echo "  flink jobmanager OK" || { echo "FAIL: flink jobmanager"; exit 1; }

echo "[sl-smoke] checking Flink TaskManager registered..."
TM_COUNT=$(docker exec sl-flink-jobmanager \
  curl -sf "http://localhost:8081/taskmanagers" 2>/dev/null | \
  python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d.get('taskmanagers',[])))" 2>/dev/null || echo "0")
[ "${TM_COUNT:-0}" -ge 1 ] \
  && echo "  flink taskmanager OK ($TM_COUNT registered)" \
  || { echo "FAIL: no flink taskmanagers"; exit 1; }

echo "[sl-smoke] checking Flink pipeline job running..."
JOB_STATE=$(docker exec sl-flink-jobmanager \
  curl -sf "http://localhost:8081/jobs" 2>/dev/null | \
  python3 -c "
import sys, json
d = json.load(sys.stdin)
running = [j for j in d.get('jobs', []) if j.get('status') in ('RUNNING', 'CREATED')]
print('running' if running else 'none')
" 2>/dev/null || echo "unknown")
[ "$JOB_STATE" = "running" ] \
  && echo "  flink pipeline job OK" \
  || echo "  WARN: no running Flink jobs (pipeline may still be starting)"

echo "[sl-smoke] checking Spark notebook..."
docker exec sl-spark curl -sf "http://localhost:8888" >/dev/null 2>&1 \
  && echo "  spark notebook OK" || { echo "FAIL: spark notebook"; exit 1; }

echo "[sl-smoke] checking Iceberg demo namespace exists..."
NS_LIST=$(docker exec sl-spark curl -sf "http://sl-iceberg-rest:8181/v1/namespaces" 2>/dev/null || echo "{}")
echo "$NS_LIST" | python3 -c "
import sys, json
d = json.load(sys.stdin)
namespaces = d.get('namespaces', [])
flat = [''.join(n) for n in namespaces]
print('demo namespace OK' if 'demo' in flat else 'WARN: demo namespace not yet created')
" 2>/dev/null || echo "  WARN: could not list namespaces"

echo "[sl-smoke] checking Iceberg orders table via Spark..."
ROW_COUNT=$(docker exec sl-spark spark-sql \
  --conf "spark.sql.catalog.rest=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.rest.type=rest" \
  --conf "spark.sql.catalog.rest.uri=http://sl-iceberg-rest:8181" \
  --conf "spark.sql.catalog.rest.io-impl=org.apache.iceberg.aws.s3.S3FileIO" \
  --conf "spark.sql.catalog.rest.s3.endpoint=http://sl-minio:9000" \
  --conf "spark.sql.catalog.rest.s3.path-style-access=true" \
  --conf "spark.sql.catalog.rest.warehouse=s3://${MINIO_BUCKET}/warehouse" \
  --conf "spark.hadoop.fs.s3a.endpoint=http://sl-minio:9000" \
  --conf "spark.hadoop.fs.s3a.path.style.access=true" \
  --conf "spark.hadoop.fs.s3a.access.key=${MINIO_USER}" \
  --conf "spark.hadoop.fs.s3a.secret.key=${MINIO_PASS}" \
  -e "SELECT COUNT(*) FROM rest.demo.orders;" 2>/dev/null | tail -1 || echo "0")
echo "  iceberg orders row count: ${ROW_COUNT}"
[ "${ROW_COUNT:-0}" -ge 1 ] \
  && echo "  iceberg table readable OK" \
  || echo "  WARN: no rows yet — Flink may need more time to checkpoint"

echo "[sl-smoke] checking StarRocks FE..."
docker exec sl-starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root \
  -e "SHOW CATALOGS;" 2>/dev/null | grep -q "iceberg_rest_catalog" \
  && echo "  starrocks iceberg_rest_catalog OK" \
  || echo "  WARN: starrocks iceberg_rest_catalog not yet registered"

# ── ADDITIVE 3-catalog verification (iceberg / hudi / delta) ─────────────────
# Studio drops scripts/lhs-etl-verify.sh for this stack (generated from the
# shared ETL block, streaming values substituted). It runs the chosen table
# format's ETL and registers/queries its catalog (hudi_catalog / delta_catalog
# via HMS, iceberg via the REST catalog). Kept non-fatal so a first-run hiccup
# doesn't block the streaming install; the log above is authoritative.
if [ -f scripts/lhs-etl-verify.sh ]; then
  echo "[sl-smoke] running additive 3-catalog ETL verification..."
  if bash scripts/lhs-etl-verify.sh; then
    echo "  [sl-smoke] 3-catalog ETL verification OK"
  else
    echo "  [sl-smoke] WARN: 3-catalog ETL verification did not fully pass (see log above)"
  fi
else
  echo "[sl-smoke] (no lhs-etl-verify.sh — 3-catalog feature not generated for this install)"
fi

echo "[sl-smoke] passed"
