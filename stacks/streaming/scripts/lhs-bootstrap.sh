#!/usr/bin/env bash
# Streaming Lakehouse bootstrap — streaming-local-v1.0
# Sets up demo Kafka topics, Iceberg tables (via Spark), and a
# Flink SQL pipeline that continuously reads orders from Kafka and
# writes them to Iceberg in micro-batches.
set -euo pipefail

# Prevent MSYS/Git-Bash on Windows from converting /opt/... container paths
# to Windows paths before they reach docker.exe
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

MINIO_BUCKET="${MINIO_BUCKET:-streaming-lake}"
MINIO_USER="${MINIO_ROOT_USER:-admin}"
MINIO_PASS="${MINIO_ROOT_PASSWORD:-streaming123}"
ICEBERG_URI="http://sl-iceberg-rest:8181"
KAFKA_BROKER="sl-kafka:9092"

wait_http() {
  local label="$1" url="$2"
  echo "[bootstrap] waiting for $label..."
  for _i in $(seq 1 30); do
    if docker exec sl-spark curl -sf "$url" >/dev/null 2>&1; then
      echo "  $label OK"; return 0
    fi
    [ "$_i" = "30" ] && { echo "ERROR: $label not ready after 5 min"; exit 1; }
    echo "  ($_i/30) $label not ready"; sleep 10
  done
}

# ── 1. Wait for dependencies ──────────────────────────────────────────────────
echo "[bootstrap] waiting for Kafka..."
for _i in $(seq 1 30); do
  if docker exec sl-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
    echo "  Kafka OK"; break
  fi
  [ "$_i" = "30" ] && { echo "ERROR: Kafka not ready"; exit 1; }
  echo "  ($_i/30) Kafka not ready"; sleep 10
done

wait_http "Iceberg REST" "http://sl-iceberg-rest:8181/v1/config"
wait_http "Spark"        "http://sl-spark:8888"

echo "[bootstrap] waiting for Flink JobManager..."
for _i in $(seq 1 30); do
  if docker exec sl-flink-jobmanager curl -sf "http://localhost:8081/overview" >/dev/null 2>&1; then
    echo "  Flink JM OK"; break
  fi
  [ "$_i" = "30" ] && { echo "ERROR: Flink JobManager not ready"; exit 1; }
  echo "  ($_i/30) Flink JM not ready"; sleep 10
done

echo "[bootstrap] waiting for StarRocks FE..."
for _i in $(seq 1 30); do
  if docker exec sl-starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root -e "SELECT 1" >/dev/null 2>&1; then
    echo "  StarRocks FE OK"; break
  fi
  [ "$_i" = "30" ] && { echo "ERROR: StarRocks FE not ready"; exit 1; }
  echo "  ($_i/30) StarRocks FE not ready"; sleep 10
done

# ── 2. Create Kafka topics ────────────────────────────────────────────────────
echo "[bootstrap] creating Kafka topics..."
docker exec sl-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic orders \
  --partitions 3 \
  --replication-factor 1
docker exec sl-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic raw-events \
  --partitions 3 \
  --replication-factor 1
echo "  topics created"

# ── 3. Create Iceberg namespace + tables via Spark SQL ───────────────────────
echo "[bootstrap] creating Iceberg schema and tables..."
docker exec sl-spark spark-sql \
  --conf "spark.sql.defaultCatalog=rest" \
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
  -e "CREATE NAMESPACE IF NOT EXISTS rest.demo;" 2>/dev/null

docker exec sl-spark spark-sql \
  --conf "spark.sql.defaultCatalog=rest" \
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
  -e "
    CREATE TABLE IF NOT EXISTS rest.demo.orders (
      order_id  STRING,
      customer  STRING,
      amount    DOUBLE,
      region    STRING,
      event_ts  TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (days(event_ts));

    CREATE TABLE IF NOT EXISTS rest.demo.events (
      event_id   STRING,
      event_type STRING,
      payload    STRING,
      event_ts   TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (days(event_ts));
  " 2>/dev/null
echo "  Iceberg tables created"

# ── 4. Produce sample orders to Kafka (idempotent — only seed an empty topic) ─
# Re-running bootstrap (e.g. a UI "Retry") must not re-seed: each extra seed +
# each extra Flink job compounds into duplicate rows in Iceberg. Skip if the
# orders topic already holds records.
echo "[bootstrap] producing sample orders to Kafka..."
# Kafka 3.8 removed the legacy `kafka.tools.GetOffsetShell` class and the
# `--broker-list` flag. Use the supported wrapper + `--bootstrap-server`.
# Output is `topic:partition:offset`, so the awk sum of $3 still counts records.
EXISTING_ORDERS=$(docker exec sl-kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic orders 2>/dev/null | awk -F: '{sum+=$3} END{print sum+0}')
if [ "${EXISTING_ORDERS:-0}" -gt 0 ]; then
  echo "  orders topic already has ${EXISTING_ORDERS} record(s) — skipping seed (idempotent)"
else
  docker exec sl-kafka bash -c "
    for i in 1 2 3 4 5 6 7 8 9 10; do
      echo \"{\\\"order_id\\\":\\\"ORD-\$i\\\",\\\"customer\\\":\\\"customer-\$((RANDOM % 5 + 1))\\\",\\\"amount\\\":\$((RANDOM % 500 + 10)).\$((RANDOM % 99)),\\\"region\\\":\\\"APAC\\\",\\\"event_ts\\\":\\\"\$(date -u +%Y-%m-%dT%H:%M:%SZ)\\\"}\"
    done | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders
  "
  echo "  sample orders produced"
fi

# ── 5. Submit Flink SQL pipeline: Kafka → Iceberg ────────────────────────────
echo "[bootstrap] submitting Flink Kafka→Iceberg pipeline..."
FLINK_SQL=$(cat <<'FLINKSQL'
SET 'execution.runtime-mode' = 'streaming';
SET 'parallelism.default' = '1';
-- The Iceberg sink only commits data files on checkpoints — without an
-- interval, checkpointing is DISABLED and rows never become visible.
SET 'execution.checkpointing.interval' = '10s';

-- S3FileIO (AWS SDK v2, from iceberg-aws-bundle in /opt/flink/lib) — the same
-- FileIO Spark uses, so both engines read/write the s3:// table paths natively.
CREATE CATALOG rest_catalog WITH (
  'type'                 = 'iceberg',
  'catalog-type'         = 'rest',
  'uri'                  = 'http://sl-iceberg-rest:8181',
  'io-impl'              = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint'          = 'http://sl-minio:9000',
  's3.path-style-access' = 'true',
  's3.access-key-id'     = 'admin',
  's3.secret-access-key' = 'streaming123',
  'client.region'        = 'us-east-1'
);

USE CATALOG rest_catalog;
USE demo;

CREATE TEMPORARY TABLE kafka_orders (
  order_id  STRING,
  customer  STRING,
  amount    DOUBLE,
  region    STRING,
  event_ts  STRING
) WITH (
  'connector'                           = 'kafka',
  'topic'                               = 'orders',
  'properties.bootstrap.servers'        = 'sl-kafka:9092',
  'properties.group.id'                 = 'flink-iceberg-sink',
  'scan.startup.mode'                   = 'earliest-offset',
  'format'                              = 'json',
  'json.ignore-parse-errors'            = 'true'
);

INSERT INTO orders
SELECT
  order_id,
  customer,
  amount,
  region,
  TO_TIMESTAMP(event_ts, 'yyyy-MM-dd''T''HH:mm:ss''Z''')
FROM kafka_orders;
FLINKSQL
)

# Idempotent: the streaming INSERT job runs forever, so a re-run must not submit
# a second copy (two jobs → double-writes into Iceberg). Skip if one is running.
ALREADY_RUNNING=$(docker exec sl-flink-jobmanager curl -sf "http://localhost:8081/jobs" 2>/dev/null | \
  python3 -c "import sys,json; d=json.load(sys.stdin); print(len([j for j in d.get('jobs',[]) if j.get('status')=='RUNNING']))" 2>/dev/null || echo 0)
if [ "${ALREADY_RUNNING:-0}" -ge 1 ]; then
  echo "  Flink pipeline already running (${ALREADY_RUNNING} job) — skipping submit (idempotent)"
else
  printf '%s' "$FLINK_SQL" | docker exec -i sl-flink-jobmanager bash -c 'cat > /tmp/streaming_pipeline.sql'
  docker exec sl-flink-jobmanager \
    /opt/flink/bin/sql-client.sh -f /tmp/streaming_pipeline.sql \
    2>&1 | docker exec -i sl-flink-jobmanager bash -c 'cat > /tmp/flink_submit.log' || true
  echo "  Flink pipeline submitted (check /tmp/flink_submit.log inside sl-flink-jobmanager)"
fi

# ── 6. Wait for Flink job to appear ──────────────────────────────────────────
echo "[bootstrap] waiting for Flink job to start..."
for _i in $(seq 1 20); do
  JOB_COUNT=$(docker exec sl-flink-jobmanager \
    curl -sf "http://localhost:8081/jobs" 2>/dev/null | \
    python3 -c "import sys,json; d=json.load(sys.stdin); print(len([j for j in d.get('jobs',[]) if j.get('status') in ('RUNNING','CREATED')]))" 2>/dev/null || echo "0")
  if [ "${JOB_COUNT:-0}" -ge 1 ]; then
    echo "  Flink pipeline running (${JOB_COUNT} job(s))"; break
  fi
  [ "$_i" = "20" ] && echo "  WARN: Flink job not detected — check Flink UI at :8083"
  echo "  ($_i/20) waiting for Flink job..."; sleep 5
done

# ── 7. Create StarRocks external catalog for Iceberg ─────────────────────────
echo "[bootstrap] wiring StarRocks → Iceberg REST catalog..."
docker exec sl-starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root 2>/dev/null <<'STARSQL'
CREATE EXTERNAL CATALOG IF NOT EXISTS iceberg_rest_catalog
COMMENT 'Streaming Lakehouse Iceberg via REST catalog + MinIO'
PROPERTIES (
  "type"                                = "iceberg",
  "iceberg.catalog.type"                = "rest",
  "iceberg.catalog.uri"                 = "http://sl-iceberg-rest:8181",
  "aws.s3.use_aws_sdk_default_behavior" = "false",
  "aws.s3.enable_path_style_access"     = "true",
  "aws.s3.access_key"                   = "admin",
  "aws.s3.secret_key"                   = "streaming123",
  "aws.s3.endpoint"                     = "http://sl-minio:9000",
  "aws.s3.region"                       = "us-east-1",
  "s3.endpoint"                         = "http://sl-minio:9000",
  "s3.path-style-access"               = "true",
  "s3.access-key-id"                    = "admin",
  "s3.secret-access-key"               = "streaming123",
  "client.region"                       = "us-east-1"
);
STARSQL
echo "  StarRocks catalog created"

# ── 8. Seed a few more events and verify Iceberg via Spark ───────────────────
echo "[bootstrap] verifying Iceberg table via Spark..."
sleep 15  # allow Flink to flush its first checkpoint
ROW_COUNT=$(docker exec sl-spark spark-sql \
  --conf "spark.sql.defaultCatalog=rest" \
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
echo "  Iceberg orders table row count: ${ROW_COUNT}"

echo "[bootstrap] streaming-local-v1.0 bootstrap complete"
echo "  Flink UI:        http://localhost:8083"
echo "  Iceberg REST:    http://localhost:8282/v1/namespaces"
echo "  MinIO Console:   http://localhost:9011"
echo "  Spark Notebook:  http://localhost:8890"
echo "  StarRocks MySQL: mysql -h 127.0.0.1 -P 9034 -u root"
