#!/usr/bin/env bash
# Stage the orders_demo schema YAML + ACK to HDFS so the
# sample_orders_hudi_pipeline Airflow DAG can be triggered immediately
# after bootstrap completes.
#
# Safe to re-run: all HDFS ops use -f (overwrite) or mkdir -p (idempotent).
set -euo pipefail

export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

# docker cp requires Windows-style host paths on Windows (Git Bash converts
# /d/... to D:\d: which is invalid). Use cygpath -w when available.
winpath() { if command -v cygpath >/dev/null 2>&1; then cygpath -w "$1"; else echo "$1"; fi; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STACK_DIR="$(dirname "$SCRIPT_DIR")"
YAML_SRC="$STACK_DIR/sample_data/orders_demo_schema.yaml"

echo "[orders_demo] staging schema YAML to HDFS..."

# HDFS directories for the orders_demo pipeline
docker exec ehd-namenode hdfs dfs -mkdir -p /techsophy/orders_demo/yamls
docker exec ehd-namenode hdfs dfs -mkdir -p /techsophy/orders_demo/csvs
# yamls/ needs only Airflow+Spark read access; csvs/ is a drop zone like /tmp
docker exec ehd-namenode hdfs dfs -chmod 775 /techsophy/orders_demo
docker exec ehd-namenode hdfs dfs -chmod 775 /techsophy/orders_demo/yamls
docker exec ehd-namenode hdfs dfs -chmod 777 /techsophy/orders_demo/csvs

# Upload schema YAML into the namenode container then put on HDFS
docker cp "$(winpath "$YAML_SRC")" ehd-namenode:/tmp/orders_demo_schema.yaml
docker exec ehd-namenode hdfs dfs -put -f \
  /tmp/orders_demo_schema.yaml \
  /techsophy/orders_demo/yamls/orders_demo_schema.yaml
docker exec ehd-namenode hdfs dfs -chmod 644 \
  /techsophy/orders_demo/yamls/orders_demo_schema.yaml

# Write the ACK marker — IngestMultipleCsvs requires both .yaml + .ack present
docker exec ehd-namenode bash -c \
  "printf 'ok\n' > /tmp/orders_demo_schema.ack && \
   hdfs dfs -put -f /tmp/orders_demo_schema.ack \
   /techsophy/orders_demo/yamls/orders_demo_schema.ack && \
   hdfs dfs -chmod 644 /techsophy/orders_demo/yamls/orders_demo_schema.ack"

echo "[orders_demo] staged:"
docker exec ehd-namenode hdfs dfs -ls /techsophy/orders_demo/yamls/
echo "[orders_demo] DAG 'sample_orders_hudi_pipeline' is ready — trigger it from Airflow UI."
