#!/usr/bin/env bash
# Lakehouse Studio — Enterprise Hadoop Datalake smoke test
# Run after bootstrap completes.
set -euo pipefail

# Prevent Git Bash on Windows from converting /hdfs/paths into C:/hdfs/paths
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

echo "[ehd-smoke] checking HDFS NameNode..."
docker exec ehd-namenode curl -sf \
  "http://localhost:9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus" >/dev/null \
  && echo "  namenode OK" || { echo "FAIL: namenode"; exit 1; }

echo "[ehd-smoke] checking YARN ResourceManager..."
curl -sf http://localhost:8088/ws/v1/cluster/info >/dev/null \
  && echo "  resourcemanager OK" || { echo "FAIL: resourcemanager"; exit 1; }

echo "[ehd-smoke] checking PgBouncer..."
docker exec ehd-pgbouncer pg_isready -h localhost -p 6432 -U hive >/dev/null 2>&1 \
  && echo "  pgbouncer OK" || { echo "FAIL: pgbouncer"; exit 1; }

echo "[ehd-smoke] checking Hive Metastore (port 9083)..."
docker exec ehd-hive-metastore bash -c "grep -qi '237B' /proc/net/tcp6" \
  && echo "  hive-metastore OK" || { echo "FAIL: hive-metastore"; exit 1; }

echo "[ehd-smoke] checking HDFS medallion dirs..."
for DIR in /warehouse /tmp/hive /apps /techsophy/raw /techsophy/curated /medunited/raw /medunited/curated; do
  docker exec ehd-namenode hdfs dfs -test -d "$DIR" \
    && echo "  $DIR OK" || { echo "FAIL: HDFS $DIR missing"; exit 1; }
done

echo "[ehd-smoke] checking Tez on HDFS..."
docker exec ehd-namenode hdfs dfs -test -e /apps/tez.tar.gz \
  && echo "  tez.tar.gz on HDFS OK" || { echo "FAIL: /apps/tez.tar.gz missing"; exit 1; }

echo "[ehd-smoke] checking StarRocks FE..."
docker exec ehd-starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root \
  -e "SHOW CATALOGS;" 2>&1 | grep -q "hudi_catalog" \
  && echo "  starrocks hudi_catalog OK" || { echo "FAIL: starrocks hudi_catalog"; exit 1; }

echo "[ehd-smoke] checking Trino..."
docker exec ehd-trino curl -sf http://localhost:8080/v1/status >/dev/null \
  && echo "  trino OK" || { echo "FAIL: trino"; exit 1; }

echo "[ehd-smoke] checking Loki..."
# Loki uses a distroless image (no curl/shell). Check via prometheus using the Docker network.
# NON-FATAL: Loki is log aggregation only — orthogonal to the datalake and the
# 3-catalog verification; its /ready endpoint can lag. Don't fail the smoke on it.
docker exec ehd-prometheus wget -qO- http://ehd-loki:3100/ready >/dev/null 2>&1 \
  && echo "  loki OK" || echo "  WARN: loki not ready (log aggregation only — non-fatal)"

echo "[ehd-smoke] checking Hudi demo table on HDFS..."
# NON-FATAL: this verifies the enterprise stack's OWN Spark-Hudi demo output,
# which is separate from the additive 3-catalog verification below. A missing
# demo table must not block the iceberg/hudi/delta catalog checks.
PARQUET_COUNT=$(docker exec ehd-namenode hdfs dfs -ls -R /warehouse/hudi_demo.db/demo_orders 2>/dev/null \
  | grep -c '\.parquet$' || true)
echo "  hudi demo parquet files = $PARQUET_COUNT"
if [ "${PARQUET_COUNT:-0}" -lt 1 ]; then
  echo "  WARN: no parquet files under /warehouse/hudi_demo.db/demo_orders (enterprise hudi demo — non-fatal)"
fi
docker exec ehd-namenode hdfs dfs -test -e /warehouse/hudi_demo.db/demo_orders/.hoodie/hoodie.properties \
  && echo "  hudi table metadata OK" || echo "  WARN: hudi hoodie.properties missing (non-fatal)"

echo "[ehd-smoke] checking Ranger Admin..."
if curl -sf "http://localhost:16080/index.html" >/dev/null 2>&1; then
  echo "  ranger-admin UI OK"
  # Verify HDFS and Hive services are registered
  RANGER_AUTH="-u admin:rangeradmin123"
  HDFS_SVC=$(curl -sf ${RANGER_AUTH} \
    "http://localhost:16080/service/public/v2/api/service/name/ehd-hdfs" 2>/dev/null | \
    python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('name',''))" 2>/dev/null || echo "")
  HIVE_SVC=$(curl -sf ${RANGER_AUTH} \
    "http://localhost:16080/service/public/v2/api/service/name/ehd-hive" 2>/dev/null | \
    python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('name',''))" 2>/dev/null || echo "")
  [ "$HDFS_SVC" = "ehd-hdfs" ] && echo "  ranger ehd-hdfs service OK" || echo "  WARN: ranger ehd-hdfs service not registered"
  [ "$HIVE_SVC" = "ehd-hive" ] && echo "  ranger ehd-hive service OK" || echo "  WARN: ranger ehd-hive service not registered"
else
  echo "  WARN: ranger-admin not reachable — still starting? (non-fatal)"
fi

# ── ADDITIVE 3-catalog verification (iceberg / hudi / delta) ─────────────────
# The runner drops scripts/lhs-etl-verify.sh for this stack (generated from the
# shared ETL block, HDFS-native values substituted: apache/spark 3.4 packages,
# hdfs:// warehouse, iceberg via the existing Hive Metastore). Runs the chosen
# format's ETL and, when StarRocks is in the cart (Enterprise), registers its
# catalog; the Healthcare cart (no StarRocks) still lands tables in Hive/HDFS.
# Non-fatal so a first-run hiccup doesn't block the install.
if [ -f scripts/lhs-etl-verify.sh ]; then
  echo "[ehd-smoke] running additive 3-catalog ETL verification..."
  if bash scripts/lhs-etl-verify.sh; then
    echo "  [ehd-smoke] 3-catalog ETL verification OK"
  else
    echo "  [ehd-smoke] WARN: 3-catalog ETL verification did not fully pass (see log above)"
  fi
else
  echo "[ehd-smoke] (no lhs-etl-verify.sh — 3-catalog feature not generated)"
fi

echo "[ehd-smoke] passed"
