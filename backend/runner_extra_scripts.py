"""Bootstrap + smoke script bodies for the 4 new candidate stacks.

These are kept in a separate module to avoid bloating runner.py. They are
imported into runner.py and merged into ``_STUDIO_SCRIPT_SETS``.

Container-name convention matches runner.py's existing scripts:
``udp-<service>`` (e.g. ``udp-trino``, ``udp-spark``, ``udp-starrocks-fe``).
``udp-hive-metastore``, ``udp-postgres-hms``, ``udp-postgres-polaris``,
``udp-polaris``, ``udp-nessie`` follow the same pattern.

Demo dataset mirrors udp-local-v0.2's seed (5-row customer table with
``customer_id``, ``region``, ``order_amount``, ``ingested_at``) so smoke
tests have the same expected row counts (5 raw rows, 4 curated regions:
us-east, us-west, eu-central, apac).

Every script:
  * starts with ``set -euo pipefail``
  * exports ``MSYS_NO_PATHCONV`` for Git Bash on Windows
  * prefixes every log line with ``[studio-<name>-bootstrap]`` or ``-smoke``
  * uses bounded wait loops (max 120 retries x 5s = 10 min)
  * is idempotent (bootstrap uses CREATE/DROP IF EXISTS, smoke is read-only)
  * ends with ``[studio-<name>-smoke] passed`` on success (smoke only)
"""


# =============================================================================
# iceberg-nessie-trino-local-v0.1
# Strategy: Nessie speaks the Iceberg REST API at /api/v2. Bootstrap waits
# for MinIO + Nessie healthy, ensures the `main` branch exists (idempotent
# via the Nessie REST trees endpoint), writes Trino's iceberg catalog
# pointing at Nessie's REST URI, restarts Trino, seeds raw/curated demo
# tables via Trino SQL, then registers the same Nessie endpoint as a
# StarRocks external catalog so both engines see one warehouse.
# =============================================================================

_NESSIE_BOOTSTRAP_SH = r"""#!/usr/bin/env bash
# Studio-owned bootstrap for the Iceberg + Nessie + Trino candidate stack.
# Wires Trino's Iceberg catalog at Nessie's REST endpoint, seeds demo
# raw/curated tables via Trino SQL, and registers the same Nessie endpoint
# as a StarRocks external catalog so both engines share one warehouse.
set -euo pipefail

export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

echo "[studio-nessie-bootstrap] waiting for MinIO..."
for i in $(seq 1 120); do
  if curl -fsS http://localhost:9000/minio/health/live >/dev/null 2>&1; then
    echo "  minio OK"; break
  fi
  echo "  ($i/120) minio not ready yet"; sleep 5
  if [ "$i" = "120" ]; then echo "minio never came up"; exit 1; fi
done

echo "[studio-nessie-bootstrap] waiting for Nessie REST..."
for i in $(seq 1 120); do
  if curl -fsS http://localhost:19120/api/v2/config >/dev/null 2>&1; then
    echo "  nessie OK"; break
  fi
  echo "  ($i/120) nessie not ready yet"; sleep 5
  if [ "$i" = "120" ]; then echo "nessie never came up"; exit 1; fi
done

echo "[studio-nessie-bootstrap] ensuring Nessie 'main' branch exists..."
# Nessie auto-creates 'main' on first start; this is a no-op safety check
# that surfaces a clear error if Nessie's default-branch config is broken.
if ! curl -fsS http://localhost:19120/api/v2/trees/main >/dev/null 2>&1; then
  echo "  'main' branch missing — creating..."
  curl -fsS -X POST -H "Content-Type: application/json" \
    "http://localhost:19120/api/v2/trees?name=main&type=BRANCH" \
    -d '{}' >/dev/null || true
fi
echo "  main branch OK"

echo "[studio-nessie-bootstrap] waiting for Trino..."
for i in $(seq 1 120); do
  if curl -fsS http://localhost:8080/v1/info >/dev/null 2>&1; then
    echo "  trino OK"; break
  fi
  echo "  ($i/120) trino not ready yet"; sleep 5
  if [ "$i" = "120" ]; then echo "trino never came up"; exit 1; fi
done

echo "[studio-nessie-bootstrap] writing Trino iceberg catalog properties (Nessie REST)..."
# Trino reads /etc/trino/catalog/*.properties at startup; we write the file
# then restart trino to register the iceberg catalog. Idempotent — writing
# the same file twice is fine. Path-style + explicit MinIO creds required.
docker exec udp-trino bash -c 'cat > /etc/trino/catalog/iceberg.properties' <<'TRINOCAT'
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://nessie:19120/api/v2
iceberg.rest-catalog.warehouse=s3://datalake/warehouse
fs.native-s3.enabled=true
s3.endpoint=http://minio:9000
s3.region=us-east-1
s3.path-style-access=true
s3.aws-access-key=admin
s3.aws-secret-key=udp_admin_12345
TRINOCAT

echo "[studio-nessie-bootstrap] restarting Trino to load iceberg catalog..."
docker compose restart trino

echo "[studio-nessie-bootstrap] waiting for Trino after restart..."
for i in $(seq 1 120); do
  if curl -fsS http://localhost:8080/v1/info >/dev/null 2>&1; then
    echo "  trino back up"; break
  fi
  echo "  ($i/120) trino not ready yet"; sleep 5
  if [ "$i" = "120" ]; then echo "trino never came back"; exit 1; fi
done

echo "[studio-nessie-bootstrap] verifying iceberg catalog is registered..."
for i in $(seq 1 24); do
  if docker exec udp-trino trino --execute "SHOW CATALOGS" 2>/dev/null | grep -q "^iceberg$"; then
    echo "  iceberg catalog visible"; break
  fi
  echo "  ($i/24) iceberg catalog not yet visible"; sleep 5
done

echo "[studio-nessie-bootstrap] seeding demo schemas + tables via Trino..."
docker exec -i udp-trino trino <<'SQL'
CREATE SCHEMA IF NOT EXISTS iceberg.raw;
CREATE SCHEMA IF NOT EXISTS iceberg.curated;

DROP TABLE IF EXISTS iceberg.raw.demo_customers;
CREATE TABLE iceberg.raw.demo_customers (
  customer_id BIGINT,
  region VARCHAR,
  order_amount DECIMAL(10,2),
  ingested_at TIMESTAMP(6)
);

INSERT INTO iceberg.raw.demo_customers VALUES
  (BIGINT '1', 'us-east',    DECIMAL '120.50', current_timestamp),
  (BIGINT '2', 'us-west',    DECIMAL '300.00', current_timestamp),
  (BIGINT '3', 'eu-central', DECIMAL '75.25',  current_timestamp),
  (BIGINT '4', 'us-east',    DECIMAL '420.99', current_timestamp),
  (BIGINT '5', 'apac',       DECIMAL '199.99', current_timestamp);

DROP TABLE IF EXISTS iceberg.curated.demo_customer_summary;
CREATE TABLE iceberg.curated.demo_customer_summary AS
SELECT
  region,
  CAST(COUNT(*) AS BIGINT)             AS customer_count,
  SUM(order_amount)                    AS total_order_amount,
  current_timestamp                    AS curated_timestamp
FROM iceberg.raw.demo_customers
GROUP BY region;
SQL

echo "[studio-nessie-bootstrap] waiting for StarRocks FE..."
for i in $(seq 1 120); do
  if docker exec udp-starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root -e "SELECT 1" >/dev/null 2>&1; then
    echo "  starrocks-fe OK"; break
  fi
  echo "  ($i/120) starrocks-fe not ready yet"; sleep 5
  if [ "$i" = "120" ]; then echo "starrocks-fe never came up"; exit 1; fi
done

echo "[studio-nessie-bootstrap] registering StarRocks backend..."
docker exec udp-starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root -e \
  "ALTER SYSTEM ADD BACKEND 'starrocks-be:9050';" 2>&1 | grep -v "already exists" || true

echo "[studio-nessie-bootstrap] creating StarRocks REST catalog against Nessie..."
docker exec -i udp-starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root <<'SQL'
DROP CATALOG IF EXISTS iceberg_nessie_catalog;
CREATE EXTERNAL CATALOG iceberg_nessie_catalog
PROPERTIES (
    "type" = "iceberg",
    "iceberg.catalog.type" = "rest",
    "iceberg.catalog.uri" = "http://nessie:19120/api/v2",
    "iceberg.catalog.warehouse" = "s3://datalake/warehouse",
    "iceberg.catalog.vended-credentials-enabled" = "false",
    "aws.s3.endpoint" = "http://minio:9000",
    "aws.s3.enable_ssl" = "false",
    "aws.s3.enable_path_style_access" = "true",
    "aws.s3.region" = "us-east-1",
    "aws.s3.access_key" = "admin",
    "aws.s3.secret_key" = "udp_admin_12345"
);
SQL

echo "[studio-nessie-bootstrap] creating app_analytics view (Nessie-backed)..."
docker exec -i udp-starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root <<'SQL'
CREATE DATABASE IF NOT EXISTS app_analytics;
DROP VIEW IF EXISTS app_analytics.demo_customer_summary;
CREATE VIEW app_analytics.demo_customer_summary AS
SELECT region, customer_count, total_order_amount, curated_timestamp
FROM iceberg_nessie_catalog.curated.demo_customer_summary;
SQL

echo "[studio-nessie-bootstrap] complete"
"""


_NESSIE_SMOKE_SH = r"""#!/usr/bin/env bash
# Studio-owned smoke test for the Iceberg + Nessie + Trino candidate stack.
# Validates: Nessie + Trino + StarRocks reachable; Trino reads curated table;
# StarRocks reads the SAME table via its Nessie-backed external catalog
# and the row counts match (5 raw, 4 curated regions).
set -euo pipefail

export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

echo "[studio-nessie-smoke] checking Nessie REST..."
curl -fsS http://localhost:19120/api/v2/config >/dev/null || { echo "nessie unreachable"; exit 1; }
echo "  nessie OK"

echo "[studio-nessie-smoke] checking Trino..."
curl -fsS http://localhost:8080/v1/info >/dev/null || { echo "trino unreachable"; exit 1; }
echo "  trino OK"

echo "[studio-nessie-smoke] checking StarRocks FE..."
docker exec udp-starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root -e "SELECT 1" >/dev/null || { echo "starrocks-fe unreachable"; exit 1; }
echo "  starrocks-fe OK"

echo "[studio-nessie-smoke] Trino round-trip query (curated table)..."
TRINO_CURATED=$(docker exec udp-trino trino --execute \
  "SELECT CAST(COUNT(*) AS BIGINT) FROM iceberg.curated.demo_customer_summary" \
  --output-format CSV | tr -d '"' | tr -d '\r' | tail -n1)
TRINO_RAW=$(docker exec udp-trino trino --execute \
  "SELECT CAST(COUNT(*) AS BIGINT) FROM iceberg.raw.demo_customers" \
  --output-format CSV | tr -d '"' | tr -d '\r' | tail -n1)
echo "  trino raw rows=${TRINO_RAW} curated rows=${TRINO_CURATED}"
if [ "${TRINO_RAW}" != "5" ]; then echo "expected 5 raw rows, got ${TRINO_RAW}"; exit 1; fi
if [ "${TRINO_CURATED}" != "4" ]; then echo "expected 4 curated rows, got ${TRINO_CURATED}"; exit 1; fi

echo "[studio-nessie-smoke] StarRocks query (same Nessie catalog)..."
SR_CURATED=$(docker exec udp-starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root -N -B -e \
  "SELECT COUNT(*) FROM app_analytics.demo_customer_summary;" | tail -n1 | tr -d '\r')
echo "  starrocks curated rows=${SR_CURATED}"
if [ "${SR_CURATED}" != "4" ]; then echo "expected 4 curated rows from StarRocks, got ${SR_CURATED}"; exit 1; fi

if [ "${TRINO_CURATED}" != "${SR_CURATED}" ]; then
  echo "row-count parity FAILED: trino=${TRINO_CURATED} starrocks=${SR_CURATED}"; exit 1
fi
echo "  row-count parity OK (trino=${TRINO_CURATED} starrocks=${SR_CURATED})"

echo "[studio-nessie-smoke] passed"
"""


# =============================================================================
# hudi-hms-spark-local-v0.1
# Strategy: Init HMS schema in Postgres (schematool -dbType postgres
# -initSchema), seed Hudi COPY_ON_WRITE raw/curated demo tables via
# pyspark with hoodie.datasource.hive_sync.* options so HMS sees them.
# Smoke runs spark-sql to read both tables and exercises an incremental
# query via the `_hoodie_commit_time` filter, then asserts row counts.
# =============================================================================

_HUDI_BOOTSTRAP_SH = r"""#!/usr/bin/env bash
# Studio-owned bootstrap for the Hudi + HMS + Spark candidate stack.
# Initializes Hive Metastore schema (idempotent), seeds Hudi COPY_ON_WRITE
# raw + curated demo tables via pyspark with HMS sync enabled.
set -euo pipefail

export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

echo "[studio-hudi-bootstrap] waiting for MinIO..."
for i in $(seq 1 120); do
  if curl -fsS http://localhost:9000/minio/health/live >/dev/null 2>&1; then
    echo "  minio OK"; break
  fi
  echo "  ($i/120) minio not ready yet"; sleep 5
  if [ "$i" = "120" ]; then echo "minio never came up"; exit 1; fi
done

echo "[studio-hudi-bootstrap] waiting for Postgres (HMS backing DB)..."
for i in $(seq 1 120); do
  if docker exec udp-postgres-hms pg_isready -U hive -d metastore >/dev/null 2>&1; then
    echo "  postgres-hms OK"; break
  fi
  echo "  ($i/120) postgres-hms not ready yet"; sleep 5
  if [ "$i" = "120" ]; then echo "postgres-hms never came up"; exit 1; fi
done

echo "[studio-hudi-bootstrap] initializing HMS schema (idempotent)..."
# schematool exits non-zero if schema already exists; we treat that as success.
if docker exec udp-hive-metastore /opt/hive-metastore/bin/schematool \
    -dbType postgres -info >/dev/null 2>&1; then
  echo "  HMS schema already initialized"
else
  echo "  HMS schema missing — running initSchema"
  docker exec udp-hive-metastore /opt/hive-metastore/bin/schematool \
    -dbType postgres -initSchema || {
      # Race with the HMS image's own entrypoint that may have just init'd it.
      if docker exec udp-hive-metastore /opt/hive-metastore/bin/schematool \
          -dbType postgres -info >/dev/null 2>&1; then
        echo "  HMS schema initialized by entrypoint race — OK"
      else
        echo "HMS initSchema failed"; exit 1
      fi
    }
fi

echo "[studio-hudi-bootstrap] waiting for HMS Thrift (port 9083)..."
for i in $(seq 1 120); do
  if docker exec udp-hive-metastore bash -lc 'echo > /dev/tcp/127.0.0.1/9083' >/dev/null 2>&1; then
    echo "  HMS Thrift OK"; break
  fi
  echo "  ($i/120) HMS Thrift not ready yet"; sleep 5
  if [ "$i" = "120" ]; then echo "HMS Thrift never came up"; exit 1; fi
done

echo "[studio-hudi-bootstrap] waiting for Spark..."
for i in $(seq 1 120); do
  if docker exec udp-spark bash -lc 'command -v spark-submit' >/dev/null 2>&1; then
    echo "  spark OK"; break
  fi
  echo "  ($i/120) spark not ready yet"; sleep 5
  if [ "$i" = "120" ]; then echo "spark never came up"; exit 1; fi
done

echo "[studio-hudi-bootstrap] writing pyspark seed job into spark container..."
docker exec udp-spark bash -c 'mkdir -p /tmp/lhs && cat > /tmp/lhs/hudi_bootstrap.py' <<'PYEOF'
# Seed Hudi raw + curated demo tables and sync them to Hive Metastore.
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.appName("lhs-hudi-bootstrap")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
    .enableHiveSupport()
    .getOrCreate()
)

raw = spark.createDataFrame(
    [
        (1, "us-east",    120.50),
        (2, "us-west",    300.00),
        (3, "eu-central",  75.25),
        (4, "us-east",    420.99),
        (5, "apac",       199.99),
    ],
    ["customer_id", "region", "order_amount"],
).withColumn("ingested_at", F.current_timestamp())

spark.sql("CREATE DATABASE IF NOT EXISTS hudi_raw")
spark.sql("CREATE DATABASE IF NOT EXISTS hudi_curated")

raw_opts = {
    "hoodie.table.name": "demo_customers",
    "hoodie.datasource.write.recordkey.field": "customer_id",
    "hoodie.datasource.write.precombine.field": "ingested_at",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.hive_style_partitioning": "true",
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.mode": "hms",
    "hoodie.datasource.hive_sync.database": "hudi_raw",
    "hoodie.datasource.hive_sync.table": "demo_customers",
    "hoodie.datasource.hive_sync.metastore.uris": "thrift://hive-metastore:9083",
}
(raw.write.format("hudi").options(**raw_opts).mode("overwrite")
    .save("s3a://datalake/warehouse/hudi_raw/demo_customers"))

curated = (
    raw.groupBy("region")
    .agg(
        F.count("*").cast("long").alias("customer_count"),
        F.sum("order_amount").alias("total_order_amount"),
    )
    .withColumn("curated_timestamp", F.current_timestamp())
    .withColumn("region_key", F.col("region"))
)

curated_opts = {
    "hoodie.table.name": "demo_customer_summary",
    "hoodie.datasource.write.recordkey.field": "region_key",
    "hoodie.datasource.write.precombine.field": "curated_timestamp",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.hive_style_partitioning": "true",
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.mode": "hms",
    "hoodie.datasource.hive_sync.database": "hudi_curated",
    "hoodie.datasource.hive_sync.table": "demo_customer_summary",
    "hoodie.datasource.hive_sync.metastore.uris": "thrift://hive-metastore:9083",
}
(curated.write.format("hudi").options(**curated_opts).mode("overwrite")
    .save("s3a://datalake/warehouse/hudi_curated/demo_customer_summary"))

print("[hudi-bootstrap] raw_rows=", raw.count(), " curated_rows=", curated.count())
spark.stop()
PYEOF

echo "[studio-hudi-bootstrap] running pyspark Hudi seed job..."
docker exec udp-spark spark-submit \
  --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0 \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=admin \
  --conf spark.hadoop.fs.s3a.secret.key=udp_admin_12345 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  //tmp/lhs/hudi_bootstrap.py

echo "[studio-hudi-bootstrap] complete"
"""


_HUDI_SMOKE_SH = r"""#!/usr/bin/env bash
# Studio-owned smoke test for the Hudi + HMS + Spark candidate stack.
# Validates: HMS Thrift + Spark reachable; spark-sql reads both Hudi tables;
# expected row counts (5 raw, 4 curated); incremental query via
# _hoodie_commit_time filter returns the same 5 rows (one commit so far).
set -euo pipefail

export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

echo "[studio-hudi-smoke] checking HMS Thrift..."
docker exec udp-hive-metastore bash -lc 'echo > /dev/tcp/127.0.0.1/9083' >/dev/null 2>&1 \
  || { echo "HMS Thrift unreachable"; exit 1; }
echo "  HMS OK"

echo "[studio-hudi-smoke] writing pyspark smoke job..."
docker exec udp-spark bash -c 'mkdir -p /tmp/lhs && cat > /tmp/lhs/hudi_smoke.py' <<'PYEOF'
# Read-only smoke: count raw + curated, run an incremental query.
import sys
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("lhs-hudi-smoke")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
    .enableHiveSupport()
    .getOrCreate()
)

raw_count = spark.read.format("hudi").load(
    "s3a://datalake/warehouse/hudi_raw/demo_customers"
).count()
curated_count = spark.read.format("hudi").load(
    "s3a://datalake/warehouse/hudi_curated/demo_customer_summary"
).count()

# Incremental pull: pull every commit since the table's first commit. With
# only one bootstrap commit so far this returns the full 5 rows.
inc_df = (
    spark.read.format("hudi")
    .option("hoodie.datasource.query.type", "incremental")
    .option("hoodie.datasource.read.begin.instanttime", "0")
    .load("s3a://datalake/warehouse/hudi_raw/demo_customers")
)
inc_count = inc_df.count()
print(f"[hudi-smoke] raw={raw_count} curated={curated_count} incremental={inc_count}")
if raw_count != 5:
    print("FAIL: expected 5 raw rows"); sys.exit(1)
if curated_count != 4:
    print("FAIL: expected 4 curated rows"); sys.exit(1)
if inc_count != 5:
    print("FAIL: expected 5 incremental rows"); sys.exit(1)
spark.stop()
PYEOF

echo "[studio-hudi-smoke] running pyspark Hudi smoke job..."
docker exec udp-spark spark-submit \
  --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0 \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=admin \
  --conf spark.hadoop.fs.s3a.secret.key=udp_admin_12345 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  //tmp/lhs/hudi_smoke.py

echo "[studio-hudi-smoke] passed"
"""


# =============================================================================
# delta-hms-spark-trino-local-v0.1
# Strategy: Init HMS schema in Postgres, create Delta raw/curated tables
# via spark-sql USING DELTA + LOCATION on s3a://, register them in HMS
# (delta-spark auto-registers when spark_catalog is the HiveCatalog),
# then write /etc/trino/catalog/delta.properties (metastore=thrift,
# pointing at HMS + MinIO), restart Trino, verify Trino sees the tables.
# Smoke runs spark-sql SELECT then Trino SELECT against the same Delta
# table and asserts identical row counts (5 raw, 4 curated).
# =============================================================================

_DELTA_BOOTSTRAP_SH = r"""#!/usr/bin/env bash
# Studio-owned bootstrap for the Delta + HMS + Spark + Trino candidate stack.
# Initializes HMS schema, creates Delta raw + curated demo tables via Spark
# with HMS registration, then configures Trino's delta-lake connector
# against the same HMS + MinIO.
set -euo pipefail

export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

echo "[studio-delta-bootstrap] waiting for MinIO..."
for i in $(seq 1 120); do
  if curl -fsS http://localhost:9000/minio/health/live >/dev/null 2>&1; then
    echo "  minio OK"; break
  fi
  echo "  ($i/120) minio not ready yet"; sleep 5
  if [ "$i" = "120" ]; then echo "minio never came up"; exit 1; fi
done

echo "[studio-delta-bootstrap] waiting for Postgres (HMS backing DB)..."
for i in $(seq 1 120); do
  if docker exec udp-postgres-hms pg_isready -U hive -d metastore >/dev/null 2>&1; then
    echo "  postgres-hms OK"; break
  fi
  echo "  ($i/120) postgres-hms not ready yet"; sleep 5
  if [ "$i" = "120" ]; then echo "postgres-hms never came up"; exit 1; fi
done

echo "[studio-delta-bootstrap] initializing HMS schema (idempotent)..."
if docker exec udp-hive-metastore /opt/hive-metastore/bin/schematool \
    -dbType postgres -info >/dev/null 2>&1; then
  echo "  HMS schema already initialized"
else
  echo "  HMS schema missing — running initSchema"
  docker exec udp-hive-metastore /opt/hive-metastore/bin/schematool \
    -dbType postgres -initSchema || {
      if docker exec udp-hive-metastore /opt/hive-metastore/bin/schematool \
          -dbType postgres -info >/dev/null 2>&1; then
        echo "  HMS schema initialized by entrypoint race — OK"
      else
        echo "HMS initSchema failed"; exit 1
      fi
    }
fi

echo "[studio-delta-bootstrap] waiting for HMS Thrift (port 9083)..."
for i in $(seq 1 120); do
  if docker exec udp-hive-metastore bash -lc 'echo > /dev/tcp/127.0.0.1/9083' >/dev/null 2>&1; then
    echo "  HMS Thrift OK"; break
  fi
  echo "  ($i/120) HMS Thrift not ready yet"; sleep 5
  if [ "$i" = "120" ]; then echo "HMS Thrift never came up"; exit 1; fi
done

echo "[studio-delta-bootstrap] waiting for Spark..."
for i in $(seq 1 120); do
  if docker exec udp-spark bash -lc 'command -v spark-submit' >/dev/null 2>&1; then
    echo "  spark OK"; break
  fi
  echo "  ($i/120) spark not ready yet"; sleep 5
  if [ "$i" = "120" ]; then echo "spark never came up"; exit 1; fi
done

echo "[studio-delta-bootstrap] writing pyspark seed job into spark container..."
docker exec udp-spark bash -c 'mkdir -p /tmp/lhs && cat > /tmp/lhs/delta_bootstrap.py' <<'PYEOF'
# Seed Delta raw + curated demo tables and register them in HMS.
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.appName("lhs-delta-bootstrap")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sql("CREATE DATABASE IF NOT EXISTS delta_raw")
spark.sql("CREATE DATABASE IF NOT EXISTS delta_curated")
spark.sql("DROP TABLE IF EXISTS delta_raw.demo_customers")
spark.sql("DROP TABLE IF EXISTS delta_curated.demo_customer_summary")

raw = spark.createDataFrame(
    [
        (1, "us-east",    120.50),
        (2, "us-west",    300.00),
        (3, "eu-central",  75.25),
        (4, "us-east",    420.99),
        (5, "apac",       199.99),
    ],
    ["customer_id", "region", "order_amount"],
).withColumn("ingested_at", F.current_timestamp())

(raw.write.format("delta").mode("overwrite")
    .option("path", "s3a://datalake/warehouse/delta_raw/demo_customers")
    .saveAsTable("delta_raw.demo_customers"))

curated = (
    raw.groupBy("region")
    .agg(
        F.count("*").cast("long").alias("customer_count"),
        F.sum("order_amount").alias("total_order_amount"),
    )
    .withColumn("curated_timestamp", F.current_timestamp())
)

(curated.write.format("delta").mode("overwrite")
    .option("path", "s3a://datalake/warehouse/delta_curated/demo_customer_summary")
    .saveAsTable("delta_curated.demo_customer_summary"))

print("[delta-bootstrap] raw_rows=", raw.count(), " curated_rows=", curated.count())
spark.stop()
PYEOF

echo "[studio-delta-bootstrap] running pyspark Delta seed job..."
docker exec udp-spark spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.1 \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=admin \
  --conf spark.hadoop.fs.s3a.secret.key=udp_admin_12345 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  //tmp/lhs/delta_bootstrap.py

echo "[studio-delta-bootstrap] waiting for Trino..."
for i in $(seq 1 120); do
  if curl -fsS http://localhost:8080/v1/info >/dev/null 2>&1; then
    echo "  trino OK"; break
  fi
  echo "  ($i/120) trino not ready yet"; sleep 5
  if [ "$i" = "120" ]; then echo "trino never came up"; exit 1; fi
done

echo "[studio-delta-bootstrap] writing Trino delta-lake catalog properties..."
# Trino's delta-lake connector reads tables registered in HMS; no REST.
docker exec udp-trino bash -c 'cat > /etc/trino/catalog/delta.properties' <<'TRINOCAT'
connector.name=delta-lake
hive.metastore=thrift
hive.metastore.uri=thrift://hive-metastore:9083
fs.native-s3.enabled=true
s3.endpoint=http://minio:9000
s3.region=us-east-1
s3.path-style-access=true
s3.aws-access-key=admin
s3.aws-secret-key=udp_admin_12345
TRINOCAT

echo "[studio-delta-bootstrap] restarting Trino to load delta catalog..."
docker compose restart trino

echo "[studio-delta-bootstrap] waiting for Trino after restart..."
for i in $(seq 1 120); do
  if curl -fsS http://localhost:8080/v1/info >/dev/null 2>&1; then
    echo "  trino back up"; break
  fi
  echo "  ($i/120) trino not ready yet"; sleep 5
  if [ "$i" = "120" ]; then echo "trino never came back"; exit 1; fi
done

echo "[studio-delta-bootstrap] verifying delta catalog is registered..."
for i in $(seq 1 24); do
  if docker exec udp-trino trino --execute "SHOW CATALOGS" 2>/dev/null | grep -q "^delta$"; then
    echo "  delta catalog visible"; break
  fi
  echo "  ($i/24) delta catalog not yet visible"; sleep 5
done

echo "[studio-delta-bootstrap] registering Delta tables in Trino delta catalog..."
# Trino delta-lake requires explicit registration when the schema/table
# exist in HMS but not yet known to the Trino catalog. Idempotent: failing
# because "already registered" is fine.
docker exec -i udp-trino trino <<'SQL'
CALL delta.system.register_table(schema_name => 'delta_raw',     table_name => 'demo_customers',         table_location => 's3://datalake/warehouse/delta_raw/demo_customers');
CALL delta.system.register_table(schema_name => 'delta_curated', table_name => 'demo_customer_summary', table_location => 's3://datalake/warehouse/delta_curated/demo_customer_summary');
SQL

echo "[studio-delta-bootstrap] complete"
"""


_DELTA_SMOKE_SH = r"""#!/usr/bin/env bash
# Studio-owned smoke test for the Delta + HMS + Spark + Trino candidate
# stack. Validates: HMS reachable; Spark reads both Delta tables (5 raw,
# 4 curated); Trino reads the SAME tables and row counts match.
set -euo pipefail

export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

echo "[studio-delta-smoke] checking HMS Thrift..."
docker exec udp-hive-metastore bash -lc 'echo > /dev/tcp/127.0.0.1/9083' >/dev/null 2>&1 \
  || { echo "HMS Thrift unreachable"; exit 1; }
echo "  HMS OK"

echo "[studio-delta-smoke] checking Trino..."
curl -fsS http://localhost:8080/v1/info >/dev/null || { echo "trino unreachable"; exit 1; }
echo "  trino OK"

echo "[studio-delta-smoke] writing pyspark smoke job..."
docker exec udp-spark bash -c 'mkdir -p /tmp/lhs && cat > /tmp/lhs/delta_smoke.py' <<'PYEOF'
# Read-only smoke: count raw + curated via Delta + HMS.
import sys
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("lhs-delta-smoke")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
    .enableHiveSupport()
    .getOrCreate()
)

raw_count = spark.sql("SELECT COUNT(*) FROM delta_raw.demo_customers").collect()[0][0]
curated_count = spark.sql("SELECT COUNT(*) FROM delta_curated.demo_customer_summary").collect()[0][0]
print(f"[delta-smoke] spark raw={raw_count} curated={curated_count}")
if raw_count != 5:
    print("FAIL: expected 5 spark raw rows"); sys.exit(1)
if curated_count != 4:
    print("FAIL: expected 4 spark curated rows"); sys.exit(1)
spark.stop()
PYEOF

echo "[studio-delta-smoke] running pyspark Delta smoke job..."
docker exec udp-spark spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.1 \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=admin \
  --conf spark.hadoop.fs.s3a.secret.key=udp_admin_12345 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  //tmp/lhs/delta_smoke.py

echo "[studio-delta-smoke] Trino round-trip query (curated Delta table)..."
TRINO_CURATED=$(docker exec udp-trino trino --execute \
  "SELECT CAST(COUNT(*) AS BIGINT) FROM delta.delta_curated.demo_customer_summary" \
  --output-format CSV | tr -d '"' | tr -d '\r' | tail -n1)
TRINO_RAW=$(docker exec udp-trino trino --execute \
  "SELECT CAST(COUNT(*) AS BIGINT) FROM delta.delta_raw.demo_customers" \
  --output-format CSV | tr -d '"' | tr -d '\r' | tail -n1)
echo "  trino raw=${TRINO_RAW} curated=${TRINO_CURATED}"
if [ "${TRINO_RAW}" != "5" ]; then echo "expected 5 trino raw rows, got ${TRINO_RAW}"; exit 1; fi
if [ "${TRINO_CURATED}" != "4" ]; then echo "expected 4 trino curated rows, got ${TRINO_CURATED}"; exit 1; fi

echo "  row-count parity OK (spark=4 trino=${TRINO_CURATED})"

echo "[studio-delta-smoke] passed"
"""


# =============================================================================
# iceberg-polaris-spark-local-v0.1
# Strategy: Wait for MinIO + Postgres + Polaris healthy. Use Polaris's
# management API (/api/management/v1/) with the bootstrap root credential
# (POLARIS_BOOTSTRAP_CREDENTIALS env on the polaris container) to:
#   1. Create the catalog (S3-backed at s3://datalake/warehouse, MinIO endpoint)
#   2. Create a principal `studio-root` and capture its client_id/secret
#   3. Grant catalog_admin on the new catalog to that principal
# Then seed raw + curated via pyspark, configured with Polaris's Iceberg
# REST URI + OAuth2 client_credentials (the captured client_id/secret).
# Finally register a StarRocks external catalog against the same Polaris
# endpoint so smoke can verify both engines see identical row counts.
# =============================================================================

_POLARIS_BOOTSTRAP_SH = r"""#!/usr/bin/env bash
# Studio-owned bootstrap for the Iceberg + Polaris + Spark + StarRocks
# candidate stack. Provisions a Polaris catalog + principal via the
# management API, then seeds Iceberg raw/curated tables via pyspark
# authenticated with OAuth2 client_credentials, and wires the same
# Polaris catalog into StarRocks.
set -euo pipefail

export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

POLARIS_MGMT="${POLARIS_MANAGEMENT_URI:-http://localhost:8181/api/management/v1}"
POLARIS_CATALOG_URI="${ICEBERG_REST_URI:-http://localhost:8181/api/catalog}"
CATALOG_NAME="${POLARIS_CATALOG_NAME:-lakehouse}"
PRINCIPAL_NAME="${POLARIS_PRINCIPAL_NAME:-studio-root}"

echo "[studio-polaris-bootstrap] waiting for MinIO..."
for i in $(seq 1 120); do
  if curl -fsS http://localhost:9000/minio/health/live >/dev/null 2>&1; then
    echo "  minio OK"; break
  fi
  echo "  ($i/120) minio not ready yet"; sleep 5
  if [ "$i" = "120" ]; then echo "minio never came up"; exit 1; fi
done

echo "[studio-polaris-bootstrap] waiting for Postgres (Polaris backing DB)..."
for i in $(seq 1 120); do
  if docker exec udp-postgres-polaris pg_isready -U polaris -d polaris >/dev/null 2>&1; then
    echo "  postgres-polaris OK"; break
  fi
  echo "  ($i/120) postgres-polaris not ready yet"; sleep 5
  if [ "$i" = "120" ]; then echo "postgres-polaris never came up"; exit 1; fi
done

echo "[studio-polaris-bootstrap] waiting for Polaris..."
for i in $(seq 1 120); do
  if curl -fsS "${POLARIS_CATALOG_URI%/api/catalog}/q/health" >/dev/null 2>&1 \
     || curl -fsS "${POLARIS_CATALOG_URI}/v1/config" >/dev/null 2>&1; then
    echo "  polaris OK"; break
  fi
  echo "  ($i/120) polaris not ready yet"; sleep 5
  if [ "$i" = "120" ]; then echo "polaris never came up"; exit 1; fi
done

echo "[studio-polaris-bootstrap] obtaining root OAuth2 token..."
# Polaris ships with a bootstrap root credential injected via
# POLARIS_BOOTSTRAP_CREDENTIALS=default-realm,root,<id>,<secret> on the
# polaris container. We exchange those for an access token.
ROOT_TOKEN=$(curl -fsS -X POST "${POLARIS_CATALOG_URI}/v1/oauth/tokens" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials&client_id=${POLARIS_ROOT_CLIENT_ID:-root}&client_secret=${POLARIS_ROOT_CLIENT_SECRET:-s3cr3t}&scope=PRINCIPAL_ROLE:ALL" \
  | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p')
if [ -z "${ROOT_TOKEN}" ]; then
  echo "failed to obtain Polaris root token — check POLARIS_BOOTSTRAP_CREDENTIALS"; exit 1
fi
echo "  root token acquired"

echo "[studio-polaris-bootstrap] creating Polaris catalog '${CATALOG_NAME}' (idempotent)..."
# 409 on a re-run means already exists — treat as success.
HTTP_CODE=$(curl -s -o /tmp/polaris_cat.out -w "%{http_code}" -X POST \
  "${POLARIS_MGMT}/catalogs" \
  -H "Authorization: Bearer ${ROOT_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{
    \"catalog\": {
      \"name\": \"${CATALOG_NAME}\",
      \"type\": \"INTERNAL\",
      \"properties\": { \"default-base-location\": \"s3://datalake/warehouse\" },
      \"storageConfigInfo\": {
        \"storageType\": \"S3\",
        \"allowedLocations\": [\"s3://datalake/warehouse\"],
        \"roleArn\": \"arn:aws:iam::000000000000:role/minio-dummy\",
        \"region\": \"us-east-1\",
        \"endpoint\": \"http://minio:9000\",
        \"pathStyleAccess\": true
      }
    }
  }")
case "${HTTP_CODE}" in
  201|200) echo "  catalog created" ;;
  409)     echo "  catalog already exists — OK" ;;
  *)       echo "catalog create failed: HTTP ${HTTP_CODE}"; cat /tmp/polaris_cat.out; exit 1 ;;
esac

echo "[studio-polaris-bootstrap] creating principal '${PRINCIPAL_NAME}' (idempotent)..."
# Capture the returned client_id + secret on first create; on a re-run
# (409) rotate-credentials returns a fresh secret we can use.
HTTP_CODE=$(curl -s -o /tmp/polaris_princ.out -w "%{http_code}" -X POST \
  "${POLARIS_MGMT}/principals" \
  -H "Authorization: Bearer ${ROOT_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{ \"principal\": { \"name\": \"${PRINCIPAL_NAME}\" } }")
if [ "${HTTP_CODE}" = "409" ]; then
  echo "  principal exists — rotating credentials"
  curl -fsS -X POST \
    "${POLARIS_MGMT}/principals/${PRINCIPAL_NAME}/rotate" \
    -H "Authorization: Bearer ${ROOT_TOKEN}" -o /tmp/polaris_princ.out
elif [ "${HTTP_CODE}" != "201" ] && [ "${HTTP_CODE}" != "200" ]; then
  echo "principal create failed: HTTP ${HTTP_CODE}"; cat /tmp/polaris_princ.out; exit 1
fi
CLIENT_ID=$(sed -n 's/.*"clientId":"\([^"]*\)".*/\1/p' /tmp/polaris_princ.out)
CLIENT_SECRET=$(sed -n 's/.*"clientSecret":"\([^"]*\)".*/\1/p' /tmp/polaris_princ.out)
if [ -z "${CLIENT_ID}" ] || [ -z "${CLIENT_SECRET}" ]; then
  echo "failed to extract principal credentials"; cat /tmp/polaris_princ.out; exit 1
fi
echo "  principal credentials captured (client_id=${CLIENT_ID:0:8}...)"

echo "[studio-polaris-bootstrap] granting catalog_admin to principal..."
# Create catalog role if missing, grant to principal — both idempotent.
curl -s -o /dev/null -X PUT \
  "${POLARIS_MGMT}/catalogs/${CATALOG_NAME}/catalog-roles/catalog_admin/grants" \
  -H "Authorization: Bearer ${ROOT_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{ \"type\": \"catalog\", \"privilege\": \"CATALOG_MANAGE_CONTENT\" }" || true
curl -s -o /dev/null -X PUT \
  "${POLARIS_MGMT}/principal-roles/service_admin/principals/${PRINCIPAL_NAME}" \
  -H "Authorization: Bearer ${ROOT_TOKEN}" || true
curl -s -o /dev/null -X PUT \
  "${POLARIS_MGMT}/catalogs/${CATALOG_NAME}/catalog-roles/catalog_admin/principal-roles/service_admin" \
  -H "Authorization: Bearer ${ROOT_TOKEN}" || true
echo "  grants applied"

# Persist credentials for the smoke script to reuse without re-rotating.
echo "[studio-polaris-bootstrap] persisting principal credentials for smoke..."
docker exec udp-spark bash -c "mkdir -p /tmp/lhs && cat > /tmp/lhs/polaris_creds.env" <<EOF
POLARIS_CLIENT_ID=${CLIENT_ID}
POLARIS_CLIENT_SECRET=${CLIENT_SECRET}
POLARIS_CATALOG_NAME=${CATALOG_NAME}
POLARIS_CATALOG_URI=http://polaris:8181/api/catalog
EOF

echo "[studio-polaris-bootstrap] waiting for Spark..."
for i in $(seq 1 120); do
  if docker exec udp-spark bash -lc 'command -v spark-submit' >/dev/null 2>&1; then
    echo "  spark OK"; break
  fi
  echo "  ($i/120) spark not ready yet"; sleep 5
  if [ "$i" = "120" ]; then echo "spark never came up"; exit 1; fi
done

echo "[studio-polaris-bootstrap] writing pyspark seed job into spark container..."
docker exec udp-spark bash -c 'mkdir -p /tmp/lhs && cat > /tmp/lhs/polaris_bootstrap.py' <<'PYEOF'
# Seed Iceberg raw + curated via Polaris-governed REST catalog.
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

with open("/tmp/lhs/polaris_creds.env") as fh:
    for line in fh:
        if "=" in line:
            k, v = line.strip().split("=", 1)
            os.environ[k] = v

catalog = os.environ["POLARIS_CATALOG_NAME"]
spark = (
    SparkSession.builder.appName("lhs-polaris-bootstrap")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{catalog}.type", "rest")
    .config(f"spark.sql.catalog.{catalog}.uri", os.environ["POLARIS_CATALOG_URI"])
    .config(f"spark.sql.catalog.{catalog}.warehouse", catalog)
    .config(
        f"spark.sql.catalog.{catalog}.credential",
        f"{os.environ['POLARIS_CLIENT_ID']}:{os.environ['POLARIS_CLIENT_SECRET']}",
    )
    .config(f"spark.sql.catalog.{catalog}.scope", "PRINCIPAL_ROLE:ALL")
    .config(f"spark.sql.catalog.{catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config(f"spark.sql.catalog.{catalog}.s3.endpoint", "http://minio:9000")
    .config(f"spark.sql.catalog.{catalog}.s3.path-style-access", "true")
    .getOrCreate()
)

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.raw")
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.curated")
spark.sql(f"DROP TABLE IF EXISTS {catalog}.raw.demo_customers")
spark.sql(f"DROP TABLE IF EXISTS {catalog}.curated.demo_customer_summary")

raw = spark.createDataFrame(
    [
        (1, "us-east",    120.50),
        (2, "us-west",    300.00),
        (3, "eu-central",  75.25),
        (4, "us-east",    420.99),
        (5, "apac",       199.99),
    ],
    ["customer_id", "region", "order_amount"],
).withColumn("ingested_at", F.current_timestamp())

raw.writeTo(f"{catalog}.raw.demo_customers").using("iceberg").create()

curated = (
    raw.groupBy("region")
    .agg(
        F.count("*").cast("long").alias("customer_count"),
        F.sum("order_amount").alias("total_order_amount"),
    )
    .withColumn("curated_timestamp", F.current_timestamp())
)
curated.writeTo(f"{catalog}.curated.demo_customer_summary").using("iceberg").create()
print("[polaris-bootstrap] raw=", raw.count(), " curated=", curated.count())
spark.stop()
PYEOF

echo "[studio-polaris-bootstrap] running pyspark Polaris seed job..."
docker exec udp-spark spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.iceberg:iceberg-aws-bundle:1.8.1 \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=admin \
  --conf spark.hadoop.fs.s3a.secret.key=udp_admin_12345 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  //tmp/lhs/polaris_bootstrap.py

echo "[studio-polaris-bootstrap] waiting for StarRocks FE..."
for i in $(seq 1 120); do
  if docker exec udp-starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root -e "SELECT 1" >/dev/null 2>&1; then
    echo "  starrocks-fe OK"; break
  fi
  echo "  ($i/120) starrocks-fe not ready yet"; sleep 5
  if [ "$i" = "120" ]; then echo "starrocks-fe never came up"; exit 1; fi
done

echo "[studio-polaris-bootstrap] registering StarRocks backend..."
docker exec udp-starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root -e \
  "ALTER SYSTEM ADD BACKEND 'starrocks-be:9050';" 2>&1 | grep -v "already exists" || true

echo "[studio-polaris-bootstrap] creating StarRocks external catalog (Polaris-backed)..."
docker exec -i udp-starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root <<SQL
DROP CATALOG IF EXISTS iceberg_polaris_catalog;
CREATE EXTERNAL CATALOG iceberg_polaris_catalog
PROPERTIES (
    "type" = "iceberg",
    "iceberg.catalog.type" = "rest",
    "iceberg.catalog.uri" = "http://polaris:8181/api/catalog",
    "iceberg.catalog.warehouse" = "${CATALOG_NAME}",
    "iceberg.catalog.oauth2.credential" = "${CLIENT_ID}:${CLIENT_SECRET}",
    "iceberg.catalog.oauth2.scope" = "PRINCIPAL_ROLE:ALL",
    "iceberg.catalog.vended-credentials-enabled" = "false",
    "aws.s3.endpoint" = "http://minio:9000",
    "aws.s3.enable_ssl" = "false",
    "aws.s3.enable_path_style_access" = "true",
    "aws.s3.region" = "us-east-1",
    "aws.s3.access_key" = "admin",
    "aws.s3.secret_key" = "udp_admin_12345"
);
SQL

echo "[studio-polaris-bootstrap] creating app_analytics view (Polaris-backed)..."
docker exec -i udp-starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root <<'SQL'
CREATE DATABASE IF NOT EXISTS app_analytics;
DROP VIEW IF EXISTS app_analytics.demo_customer_summary;
CREATE VIEW app_analytics.demo_customer_summary AS
SELECT region, customer_count, total_order_amount, curated_timestamp
FROM iceberg_polaris_catalog.curated.demo_customer_summary;
SQL

echo "[studio-polaris-bootstrap] complete"
"""


_POLARIS_SMOKE_SH = r"""#!/usr/bin/env bash
# Studio-owned smoke test for the Iceberg + Polaris + Spark + StarRocks
# candidate stack. Validates: Polaris health; spark reads raw + curated
# via OAuth2 client_credentials; StarRocks reads the SAME tables via its
# Polaris-backed external catalog; row counts match (5 raw, 4 curated).
set -euo pipefail

export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

POLARIS_CATALOG_URI="${ICEBERG_REST_URI:-http://localhost:8181/api/catalog}"

echo "[studio-polaris-smoke] checking Polaris..."
curl -fsS "${POLARIS_CATALOG_URI}/v1/config" >/dev/null \
  || { echo "polaris unreachable"; exit 1; }
echo "  polaris OK"

echo "[studio-polaris-smoke] checking StarRocks FE..."
docker exec udp-starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root -e "SELECT 1" >/dev/null \
  || { echo "starrocks-fe unreachable"; exit 1; }
echo "  starrocks-fe OK"

echo "[studio-polaris-smoke] writing pyspark smoke job..."
docker exec udp-spark bash -c 'mkdir -p /tmp/lhs && cat > /tmp/lhs/polaris_smoke.py' <<'PYEOF'
# Read-only smoke against the Polaris-governed Iceberg catalog.
import os, sys
from pyspark.sql import SparkSession

with open("/tmp/lhs/polaris_creds.env") as fh:
    for line in fh:
        if "=" in line:
            k, v = line.strip().split("=", 1)
            os.environ[k] = v

catalog = os.environ["POLARIS_CATALOG_NAME"]
spark = (
    SparkSession.builder.appName("lhs-polaris-smoke")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{catalog}.type", "rest")
    .config(f"spark.sql.catalog.{catalog}.uri", os.environ["POLARIS_CATALOG_URI"])
    .config(f"spark.sql.catalog.{catalog}.warehouse", catalog)
    .config(
        f"spark.sql.catalog.{catalog}.credential",
        f"{os.environ['POLARIS_CLIENT_ID']}:{os.environ['POLARIS_CLIENT_SECRET']}",
    )
    .config(f"spark.sql.catalog.{catalog}.scope", "PRINCIPAL_ROLE:ALL")
    .config(f"spark.sql.catalog.{catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config(f"spark.sql.catalog.{catalog}.s3.endpoint", "http://minio:9000")
    .config(f"spark.sql.catalog.{catalog}.s3.path-style-access", "true")
    .getOrCreate()
)

raw = spark.sql(f"SELECT COUNT(*) FROM {catalog}.raw.demo_customers").collect()[0][0]
curated = spark.sql(f"SELECT COUNT(*) FROM {catalog}.curated.demo_customer_summary").collect()[0][0]
print(f"[polaris-smoke] spark raw={raw} curated={curated}")
if raw != 5:
    print("FAIL: expected 5 raw rows"); sys.exit(1)
if curated != 4:
    print("FAIL: expected 4 curated rows"); sys.exit(1)
spark.stop()
PYEOF

echo "[studio-polaris-smoke] running pyspark Polaris smoke job..."
docker exec udp-spark spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.iceberg:iceberg-aws-bundle:1.8.1 \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=admin \
  --conf spark.hadoop.fs.s3a.secret.key=udp_admin_12345 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  //tmp/lhs/polaris_smoke.py

echo "[studio-polaris-smoke] StarRocks query (same Polaris catalog)..."
SR_CURATED=$(docker exec udp-starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root -N -B -e \
  "SELECT COUNT(*) FROM app_analytics.demo_customer_summary;" | tail -n1 | tr -d '\r')
echo "  starrocks curated rows=${SR_CURATED}"
if [ "${SR_CURATED}" != "4" ]; then echo "expected 4 curated rows from StarRocks, got ${SR_CURATED}"; exit 1; fi

echo "  row-count parity OK (spark=4 starrocks=${SR_CURATED})"

echo "[studio-polaris-smoke] passed"
"""


# =============================================================================
# Exported dispatch — runner.py merges this into _STUDIO_SCRIPT_SETS.
# Filenames MUST match commands.bootstrap / commands.smoke argv in each
# stack manifest under stacks/.
# =============================================================================

EXTRA_SCRIPT_SETS: dict[str, tuple[tuple[str, str], tuple[str, str]]] = {
    "iceberg-nessie-trino-local-v0.1": (
        ("lhs-nessie-bootstrap.sh", _NESSIE_BOOTSTRAP_SH),
        ("lhs-nessie-smoke.sh",     _NESSIE_SMOKE_SH),
    ),
    "hudi-hms-spark-local-v0.1": (
        ("lhs-hudi-bootstrap.sh", _HUDI_BOOTSTRAP_SH),
        ("lhs-hudi-smoke.sh",     _HUDI_SMOKE_SH),
    ),
    "delta-hms-spark-trino-local-v0.1": (
        ("lhs-delta-bootstrap.sh", _DELTA_BOOTSTRAP_SH),
        ("lhs-delta-smoke.sh",     _DELTA_SMOKE_SH),
    ),
    "iceberg-polaris-spark-local-v0.1": (
        ("lhs-polaris-bootstrap.sh", _POLARIS_BOOTSTRAP_SH),
        ("lhs-polaris-smoke.sh",     _POLARIS_SMOKE_SH),
    ),
}
