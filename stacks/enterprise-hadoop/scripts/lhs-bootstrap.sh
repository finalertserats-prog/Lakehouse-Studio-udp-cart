#!/usr/bin/env bash
# Lakehouse Studio — Enterprise Hadoop Datalake bootstrap
# Matches production: techsophy.com dev cluster (Hive 4.0.1 + Tez 0.10.4 + Hudi 1.0.1)
# Run from the stack work directory (work/enterprise-hadoop).
set -euo pipefail

# Prevent Git Bash on Windows from converting /hdfs/paths into C:/hdfs/paths
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

# docker cp requires Windows-style host paths on Windows
winpath() { if command -v cygpath >/dev/null 2>&1; then cygpath -w "$1"; else echo "$1"; fi; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STACK_DIR="$(dirname "$SCRIPT_DIR")"
cd "$STACK_DIR"

# ── Wait helpers ──────────────────────────────────────────────────────────────

wait_http() {
  local name=$1 url=$2 max=${3:-30}
  echo "[bootstrap] waiting for $name..."
  for i in $(seq 1 $max); do
    if curl -sf "$url" >/dev/null 2>&1; then echo "  $name OK"; return 0; fi
    echo "  ($i/$max) $name not ready"; sleep 10
  done
  echo "ERROR: $name did not start in time"; exit 1
}

wait_port_tcp6() {
  local name=$1 container=$2 hex_port=$3 max=${4:-30}
  echo "[bootstrap] waiting for $name..."
  for i in $(seq 1 $max); do
    if docker exec "$container" bash -c "grep -qi '$hex_port' /proc/net/tcp6" 2>/dev/null; then
      echo "  $name OK"; return 0
    fi
    echo "  ($i/$max) $name not ready"; sleep 10
  done
  echo "ERROR: $name did not start in time"; exit 1
}

wait_mysql() {
  local name=$1 container=$2 max=${3:-60}
  echo "[bootstrap] waiting for $name..."
  for i in $(seq 1 $max); do
    if docker exec "$container" mysql -h 127.0.0.1 -P 9030 -u root -e "SELECT 1" >/dev/null 2>&1; then
      echo "  $name OK"; return 0
    fi
    echo "  ($i/$max) $name not ready"; sleep 5
  done
  echo "ERROR: $name did not start in time"; exit 1
}

wait_ranger() {
  local max=${1:-60}
  echo "[bootstrap] waiting for Ranger Admin..."
  for i in $(seq 1 $max); do
    if curl -sf "http://localhost:16080/index.html" >/dev/null 2>&1; then
      echo "  Ranger Admin OK"; return 0
    fi
    echo "  ($i/$max) Ranger Admin not ready"; sleep 10
  done
  echo "WARN: Ranger Admin did not start in time (non-fatal)"; return 0
}

# ── 1. HDFS + YARN readiness ─────────────────────────────────────────────────

# HDFS NameNode HTTP port (9870) can be unreachable from the host after a Docker
# Desktop restart on Windows (port-mapping glitch). Check from inside the container.
echo "[bootstrap] waiting for HDFS NameNode..."
for _i in $(seq 1 30); do
  if docker exec ehd-namenode curl -sf \
      "http://localhost:9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus" \
      >/dev/null 2>&1; then
    echo "  HDFS NameNode OK"; break
  fi
  if [ "$_i" = "30" ]; then echo "ERROR: HDFS NameNode did not start in time"; exit 1; fi
  echo "  ($_i/30) HDFS NameNode not ready"; sleep 10
done

wait_http "YARN ResourceManager" "http://localhost:8088/ws/v1/cluster/info"

# ── 2. HDFS directories ───────────────────────────────────────────────────────

echo "[bootstrap] creating HDFS directories..."
docker exec ehd-namenode hdfs dfs -mkdir -p /user/root
docker exec ehd-namenode hdfs dfs -mkdir -p /warehouse
docker exec ehd-namenode hdfs dfs -mkdir -p /tmp
docker exec ehd-namenode hdfs dfs -mkdir -p /tmp/hive
docker exec ehd-namenode hdfs dfs -mkdir -p /apps

docker exec ehd-namenode hdfs dfs -chmod -R 777 /tmp
docker exec ehd-namenode hdfs dfs -chmod -R 777 /user
docker exec ehd-namenode hdfs dfs -chmod -R 777 /warehouse

# Medallion zones — techsophy tenant
docker exec ehd-namenode hdfs dfs -mkdir -p /techsophy/raw
docker exec ehd-namenode hdfs dfs -mkdir -p /techsophy/curated
docker exec ehd-namenode hdfs dfs -mkdir -p /techsophy/service
docker exec ehd-namenode hdfs dfs -mkdir -p /techsophy/archive
docker exec ehd-namenode hdfs dfs -chmod -R 775 /techsophy

# Medallion zones — medunited tenant
docker exec ehd-namenode hdfs dfs -mkdir -p /medunited/raw
docker exec ehd-namenode hdfs dfs -mkdir -p /medunited/curated
docker exec ehd-namenode hdfs dfs -mkdir -p /medunited/service
docker exec ehd-namenode hdfs dfs -chmod -R 775 /medunited

# ── 3. Stage orders_demo sample pipeline schema ───────────────────────────────

bash "$SCRIPT_DIR/setup_orders_demo.sh"

# ── 5. Stage Tez on HDFS (JARs pre-fetched by lhs-prefetch.sh) ───────────────

echo "[bootstrap] staging Apache Tez 0.10.4 on HDFS..."
TEZ_TAR=jars/tez/apache-tez-0.10.4-bin.tar.gz

if [ ! -f "$TEZ_TAR" ]; then
  echo "[bootstrap] Tez tarball not found — running prefetch..."
  bash "$SCRIPT_DIR/lhs-prefetch.sh"
fi

if ! docker exec ehd-namenode hdfs dfs -test -e /apps/tez.tar.gz 2>/dev/null; then
  echo "  uploading tez.tar.gz to HDFS /apps/..."
  docker cp "$(winpath "$TEZ_TAR")" ehd-namenode:/tmp/tez.tar.gz
  docker exec ehd-namenode hdfs dfs -put /tmp/tez.tar.gz /apps/tez.tar.gz
  docker exec ehd-namenode hdfs dfs -chmod 755 /apps/tez.tar.gz
  echo "  Tez staged on HDFS"
else
  echo "  Tez already on HDFS"
fi

# ── 6. Verify Hudi JARs (pre-fetched by lhs-prefetch.sh) ─────────────────────

HUDI_SPARK_JAR=jars/hudi/hudi-spark3.4-bundle_2.12-1.0.1.jar
HUDI_MR_JAR=jars/hudi/hudi-hadoop-mr-bundle-1.0.1.jar

if [ ! -f "$HUDI_SPARK_JAR" ] || [ ! -f "$HUDI_MR_JAR" ]; then
  echo "[bootstrap] Hudi JARs missing — running prefetch..."
  bash "$SCRIPT_DIR/lhs-prefetch.sh"
fi
echo "[bootstrap] Hudi JARs present"

# ── 7. Hive Metastore readiness ───────────────────────────────────────────────

# Port 9083 = 0x237B in hex
wait_port_tcp6 "Hive Metastore (port 9083)" "ehd-hive-metastore" "237B" 40

# ── 8. StarRocks readiness + catalog setup ────────────────────────────────────

wait_mysql "StarRocks FE" "ehd-starrocks-fe"

echo "[bootstrap] registering StarRocks BE..."
docker exec ehd-starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root -e \
  "ALTER SYSTEM ADD BACKEND 'starrocks-be:9050';" 2>&1 | grep -v "already exists" || true

sleep 10

echo "[bootstrap] creating StarRocks Hudi catalog..."
docker exec ehd-starrocks-fe bash -c "
  mysql -h 127.0.0.1 -P 9030 -u root -e \"
    DROP CATALOG IF EXISTS hudi_catalog;
    CREATE EXTERNAL CATALOG hudi_catalog
    PROPERTIES (
      'type' = 'hudi',
      'hive.metastore.uris' = 'thrift://hive-metastore:9083',
      'hive.metastore.timeout' = '120'
    );
  \"
" 2>&1 | grep -v "^$" || true

# ── 9. Run Spark demo Hudi bootstrap ─────────────────────────────────────────

echo "[bootstrap] running Spark demo Hudi bootstrap..."
docker exec ehd-spark mkdir -p /tmp/spark-bootstrap
docker cp "$(winpath "$HUDI_SPARK_JAR")" ehd-spark:/tmp/spark-bootstrap/hudi-spark3.4-bundle_2.12-1.0.1.jar
docker cp "$(winpath "$SCRIPT_DIR/../jobs/bootstrap_demo_hudi.py")" ehd-spark:/tmp/spark-bootstrap/bootstrap_demo_hudi.py
docker exec ehd-spark /opt/spark/bin/spark-submit \
  --master local[2] \
  --jars /tmp/spark-bootstrap/hudi-spark3.4-bundle_2.12-1.0.1.jar \
  /tmp/spark-bootstrap/bootstrap_demo_hudi.py

# ── 10. Trino readiness ───────────────────────────────────────────────────────

# Same Docker Desktop port-mapping caveat as HDFS — check from inside the container.
echo "[bootstrap] waiting for Trino..."
for _i in $(seq 1 30); do
  if docker exec ehd-trino curl -sf "http://localhost:8080/v1/status" >/dev/null 2>&1; then
    echo "  Trino OK"; break
  fi
  if [ "$_i" = "30" ]; then echo "ERROR: Trino did not start in time"; exit 1; fi
  echo "  ($_i/30) Trino not ready"; sleep 10
done

# ── 11. Ranger Admin readiness + service registration ────────────────────────

wait_ranger 2  # Ranger Admin removed from default cart; short timeout

RANGER_URL="http://localhost:16080"
RANGER_AUTH="-u admin:rangeradmin123"

if curl -sf ${RANGER_AUTH} "${RANGER_URL}/service/public/v2/api/service/count" >/dev/null 2>&1; then
  echo "[bootstrap] registering Ranger HDFS service..."
  curl -sf ${RANGER_AUTH} -X POST \
    -H "Content-Type: application/json" \
    "${RANGER_URL}/service/public/v2/api/service" \
    -d '{
      "name": "ehd-hdfs",
      "type": "hdfs",
      "description": "Enterprise Hadoop HDFS",
      "configs": {
        "username": "hadoop",
        "password": "hadoop",
        "fs.default.name": "hdfs://namenode:9820",
        "hadoop.security.authorization": "false",
        "hadoop.security.authentication": "simple",
        "hadoop.security.auth_to_local": "RULE:DEFAULT",
        "dfs.datanode.kerberos.principal": "",
        "dfs.namenode.kerberos.principal": "",
        "dfs.secondary.namenode.kerberos.principal": ""
      }
    }' 2>&1 | grep -v "already exists" | grep -v "^$" || true

  echo "[bootstrap] registering Ranger Hive service..."
  curl -sf ${RANGER_AUTH} -X POST \
    -H "Content-Type: application/json" \
    "${RANGER_URL}/service/public/v2/api/service" \
    -d '{
      "name": "ehd-hive",
      "type": "hiveServer2",
      "description": "Enterprise Hadoop HiveServer2",
      "configs": {
        "username": "hive",
        "password": "HiveAdmin",
        "jdbc.driverClassName": "org.apache.hive.jdbc.HiveDriver",
        "jdbc.url": "jdbc:hive2://hiveserver2:10000/default"
      }
    }' 2>&1 | grep -v "already exists" | grep -v "^$" || true

  echo "[bootstrap] Ranger services registered"
else
  echo "[bootstrap] Ranger Admin not reachable — skipping service registration (non-fatal)"
fi

echo "[bootstrap] complete"
