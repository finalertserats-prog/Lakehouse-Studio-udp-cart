"""
sample_orders_hudi_pipeline — Hudi ingestion using the LIVE IngestMultipleCsvs
script (jobs/ingestMultipleCSVWithACKToHDFS.py).  Same pattern as the
techsophy.com production DAGs:

    start
      └─ generate_csv          Python builds N synthetic orders → /tmp/orders.csv
      └─ land_csv_with_ack     WebHDFS PUT to /techsophy/orders_demo/csvs/
                                 + writes the parent .ack marker the script needs
      └─ spark_submit_ingest   docker exec ehd-spark spark-submit ingestMultipleCSVWithACKToHDFS.py
                                 — reads YAML, applies schema, writes Hudi,
                                   creates Hive table via JDBC sync (HiveServer2)
      └─ verify                Reads default.orders_demo back via Trino hive catalog
    end

YAML config lives at hdfs://namenode:9820/techsophy/orders_demo/yamls/orders_demo_schema.yaml
(staged once during install — see scripts/setup_orders_demo.sh if you need to redo).
"""

from __future__ import annotations
import csv
import random
import subprocess
import time
from datetime import datetime, timezone, timedelta

import requests
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

HDFS_NAMENODE_WEBHDFS = "http://namenode:9870/webhdfs/v1"
HDFS_USER = "hadoop"
TRINO_URL = "http://trino:8080/v1/statement"
TRINO_USER = "admin"

YAML_DIR = "/techsophy/orders_demo/yamls"
CSV_DIR  = "/techsophy/orders_demo/csvs"
TABLE_KEY = "orders_demo"        # matches the prefix used in YAML filename + CSV files

ROW_COUNT = 1_000
REGIONS = ["APAC", "EMEA", "AMER"]


# ── helpers ─────────────────────────────────────────────────────────────────
def _webhdfs_mkdirs(path):
    requests.put(
        f"{HDFS_NAMENODE_WEBHDFS}{path}?op=MKDIRS&user.name={HDFS_USER}",
        timeout=30,
    ).raise_for_status()


def _webhdfs_put(path, body):
    r1 = requests.put(
        f"{HDFS_NAMENODE_WEBHDFS}{path}?op=CREATE&overwrite=true&user.name={HDFS_USER}",
        allow_redirects=False, timeout=30,
    )
    if r1.status_code != 307:
        raise RuntimeError(f"WebHDFS CREATE expected 307, got {r1.status_code}: {r1.text}")
    requests.put(r1.headers["Location"], data=body, timeout=180).raise_for_status()


def _trino_execute(sql):
    headers = {"X-Trino-User": TRINO_USER, "Content-Type": "text/plain"}
    resp = requests.post(TRINO_URL, data=sql, headers=headers, timeout=60)
    resp.raise_for_status()
    rows, payload = [], resp.json()
    while True:
        if "error" in payload:
            raise RuntimeError(payload["error"]["message"])
        if payload.get("data"):
            rows.extend(payload["data"])
        nxt = payload.get("nextUri")
        if not nxt:
            return rows
        nr = requests.get(nxt, headers=headers, timeout=60)
        nr.raise_for_status()
        payload = nr.json()


# ── tasks ───────────────────────────────────────────────────────────────────
def generate_csv(**ctx):
    run_id = ctx["run_id"].replace(":", "_").replace("+", "_")
    path = f"/tmp/orders_demo_{run_id}.csv"
    rng = random.Random()
    # IngestMultipleCsvs sets `lineSep='\n'` on the Spark CSV reader, so we MUST
    # write Unix line endings.  Python's csv.writer defaults to '\r\n', which leaves
    # a trailing '\r' on the last column ('ts\r', '1779438351\r') and makes the
    # long-cast return null → Hudi precombine fails with "Ordering value is null".
    with open(path, "w", newline="") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(["order_id", "order_ref", "region", "amount", "ts"])
        base_ts = int(time.time())
        for i in range(ROW_COUNT):
            w.writerow([
                i + 1,
                f"ORD-{i+1:06d}",
                rng.choice(REGIONS),
                f"{rng.uniform(10.0, 5_000.0):.2f}",
                base_ts + i,
            ])
    print(f"[generate_csv] wrote {ROW_COUNT} rows → {path}")
    return path


def land_csv_with_ack(**ctx):
    local = ctx["ti"].xcom_pull(task_ids="generate_csv")
    _webhdfs_mkdirs(CSV_DIR)
    # IngestMultipleCsvs groups CSVs by `re.sub(r'_\d+$', '', basename_no_ext)`,
    # so the filename MUST be `{TABLE_KEY}_<digits>.csv` — anything else (e.g. a
    # verbose timestamp with non-digit tail) won't map back to the table key
    # and the script will report "No CSV files found".  Use epoch as the suffix.
    csv_path = f"{CSV_DIR}/{TABLE_KEY}_{int(time.time())}.csv"
    ack_path = f"{CSV_DIR}/{TABLE_KEY}.ack"
    with open(local, "rb") as fh:
        body = fh.read()
    _webhdfs_put(csv_path, body)
    _webhdfs_put(ack_path, b"ready\n")
    print(f"[land_csv_with_ack] uploaded {len(body):,} bytes → hdfs://namenode:9820{csv_path}")
    print(f"[land_csv_with_ack] wrote parent ACK → hdfs://namenode:9820{ack_path}")
    return csv_path


def verify(**ctx):
    rows = _trino_execute("SELECT count(*) FROM hudi.default.orders_demo")
    cnt  = int(rows[0][0])
    print(f"[verify] hudi.default.orders_demo  rows = {cnt}")
    print("[verify] per-region:")
    for region, n in _trino_execute(
        "SELECT region, count(*) FROM hudi.default.orders_demo GROUP BY region ORDER BY region"
    ):
        print(f"  {region:>4}  {n}")
    if cnt < ROW_COUNT:
        raise AssertionError(f"row count {cnt} < expected {ROW_COUNT}")


# spark-submit command (same shape as live, adapted to this docker stack)
SPARK_SUBMIT_CMD = (
    'docker exec '
    '  -e HADOOP_HOME=/opt/hadoop-3.4.1 '
    '  -e JAVA_HOME=/opt/java/openjdk '
    '  -e LD_LIBRARY_PATH=/opt/hadoop-3.4.1/lib/native:/opt/java/openjdk/lib/server '
    '  -e WEBHDFS_HOST=namenode -e WEBHDFS_PORT=9870 '
    '  ehd-spark /opt/spark/bin/spark-submit '
    '    --master local[2] '
    '    --driver-memory 1g '
    '    --executor-memory 1g '
    '    --files /opt/spark/conf/hive-site.xml '
    '    --jars /opt/spark/extra-jars/hudi/hudi-spark3.4-bundle_2.12-1.0.1.jar '
    '    /home/spark/jobs/ingestMultipleCSVWithACKToHDFS.py '
    '      --input_format    csv '
    '      --hdfs_uri        hdfs://namenode:9820 '
    '      --yarn_hostname   resourcemanager '
    f'      --warehouse_dir   hdfs://namenode:9820/tmp/hive/warehouse '
    f'      --yaml_dir        {YAML_DIR} '
    f'      --csv_dir         {CSV_DIR} '
    f'      --table_names     {TABLE_KEY}'
)


with DAG(
    dag_id="sample_orders_hudi_pipeline",
    description="Live IngestMultipleCsvs pipeline: CSV+ACK → Hudi → Hive (JDBC sync) → Trino",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["sample", "ingest", "hudi", "live-script"],
    default_args={"retries": 0, "retry_delay": timedelta(minutes=2)},
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    t_gen   = PythonOperator(task_id="generate_csv", python_callable=generate_csv)
    t_land  = PythonOperator(task_id="land_csv_with_ack", python_callable=land_csv_with_ack)
    t_spark = BashOperator(task_id="spark_submit_ingest",
                           bash_command=SPARK_SUBMIT_CMD,
                           execution_timeout=timedelta(minutes=15))
    t_check = PythonOperator(task_id="verify", python_callable=verify)

    start >> t_gen >> t_land >> t_spark >> t_check >> end
