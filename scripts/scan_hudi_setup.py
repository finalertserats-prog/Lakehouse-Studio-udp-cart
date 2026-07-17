#!/usr/bin/env python3
"""Read-only scan of how the live cluster successfully runs Hudi 1.0.1 against
Hive 4.0.1.  We need to figure out:
  1. Which Hudi jar version is actually on the classpath when ingestion runs
  2. Whether a Hive 4-compatible client jar is sitting next to/overriding it
  3. The exact spark-submit command shape Airflow uses
  4. Any hudi-defaults.conf or hive-site.xml tweaks
"""
from __future__ import annotations
import os, sys
from pathlib import Path
import paramiko

PASSWORD = os.environ["LHS_SCAN_PASSWORD"]
EVIDENCE = Path(__file__).resolve().parent.parent / "evidence" / "live-hudi-scan"
EVIDENCE.mkdir(parents=True, exist_ok=True)

PROBES = {
    # ── all jars relevant to Hudi or Hive client on each host ─────────────
    "10_spark_jars":       "ls /opt/spark-3.4.4-bin-hadoop3/jars/ | grep -iE 'hudi|hive|hadoop|thrift|libfb' | sort",
    "11_spark_extra_jars": "find /opt/spark-3.4.4-bin-hadoop3 -maxdepth 4 -name '*hudi*' -o -name '*hive-metastore*' 2>/dev/null | head -30",
    "12_extra_jars_dirs":  "ls -la /opt/spark-3.4.4-bin-hadoop3/extra-jars/ 2>/dev/null; ls -la /opt/spark-3.4.4-bin-hadoop3/jars-extra/ 2>/dev/null",
    "13_hive_lib":         "ls /opt/apache-hive-4.0.1-bin/lib/ | grep -iE 'hudi|hive-metastore|hive-exec|libthrift' | sort | head -30",
    "14_shared_libs":      "ls /opt/shared-libs/ 2>/dev/null",
    "15_hadoop_lib":       "find /etc/hadoop-3.4.1/share/hadoop -maxdepth 3 -name '*hudi*' 2>/dev/null | head -10",
    "16_libfb303":         "find /opt /usr -maxdepth 6 -name 'libfb303*.jar' 2>/dev/null | head -5",

    # ── ingestion scripts + airflow dags ──────────────────────────────────
    "20_airflow_dags_dir": "ls -la /home/hadoop/airflow/dags 2>/dev/null; ls -la /root/airflow/dags 2>/dev/null",
    "21_find_ingest_py":   "find /home/hadoop /root /opt -maxdepth 8 -name 'IngestMultiple*.py' -o -name 'ingest*hudi*.py' -o -name '*ingest*Csv*.py' 2>/dev/null | head -10",
    "22_dag_files":        "ls /home/hadoop/airflow/dags/*.py 2>/dev/null; ls /root/airflow/dags/*.py 2>/dev/null",
    "23_dag_bash_calls":   "grep -rnE 'spark-submit|bash_command|BashOperator' /home/hadoop/airflow/dags/ 2>/dev/null | head -30",

    # ── hudi conf, hive sync mode, metastore client ──────────────────────
    "30_hudi_defaults":    "cat /etc/hudi/conf/hudi-defaults.conf 2>/dev/null; ls -la /etc/hudi 2>/dev/null",
    "31_hive_site_for_sync": "cat /opt/apache-hive-4.0.1-bin/conf/hive-site.xml 2>/dev/null | grep -iE 'metastore.client|metastore.uri|metastore.warehouse|hive.metastore.use|hive.execution|hive.server2' | head -30",
    "32_spark_conf_for_hive": "cat /opt/spark-3.4.4-bin-hadoop3/conf/spark-defaults.conf 2>/dev/null | grep -iE 'hive|metastore|hudi|catalog'",
    "33_spark_hive_site":   "ls -la /opt/spark-3.4.4-bin-hadoop3/conf/hive-site.xml 2>/dev/null; head -50 /opt/spark-3.4.4-bin-hadoop3/conf/hive-site.xml 2>/dev/null",
    "34_spark_hadoop_conf": "ls -la /opt/spark-3.4.4-bin-hadoop3/conf/ 2>/dev/null",

    # ── what HMS thrift API is exposed (does live Hive 4 still have get_table?) ──
    "40_hms_jar_inspect":  "jar tf /opt/apache-hive-4.0.1-bin/lib/hive-metastore-*.jar 2>&1 | grep -iE 'ThriftHiveMetastore\\$' | head -3 || true",

    # ── live spark-submit history (find a recent successful invocation) ──
    "50_spark_history":    "ls /opt/spark-3.4.4-bin-hadoop3/work-dir 2>/dev/null; find /tmp /var/log -maxdepth 4 -name 'spark-submit*.log' -mtime -30 2>/dev/null | head -5",
    "51_airflow_recent_logs": "ls -la /home/hadoop/airflow/logs/scheduler/ 2>/dev/null | head -5; find /home/hadoop/airflow/logs -name '*.log' -mtime -7 2>/dev/null | head -10",
    "52_recent_ingest_log": "find /home/hadoop/airflow/logs -name '*ingest*' -type f 2>/dev/null | head -5; find /home/hadoop/airflow/logs -name 'attempt=*.log' -mtime -14 2>/dev/null | head -5",

    # ── classpath actually used by the ingest spark-submit ───────────────
    "60_running_spark_submits": "ps -eo pid,user,cmd | grep -E '[s]park-submit' | head -5",
    "61_resolved_classpath":    "find /home/hadoop/airflow -maxdepth 6 -name 'bootstrap*.sh' -o -name '*.sh' 2>/dev/null | xargs grep -l 'spark-submit' 2>/dev/null | head -5",
}

# Pick the right host. The ingestion ran from sdpdevafw01 (Airflow) but spark
# itself runs on sdpdevstg01 / dn01 etc.  We need:
#   - stg01 for the actual spark+hudi jars + hive client
#   - afw01 for airflow DAGs that call spark-submit
HOSTS = [
    {"name": "stg01", "host": "65.21.246.208",  "user": "hadoop"},
    {"name": "afw01", "host": "135.181.157.21", "user": "hadoop"},  # we know root failed earlier, hadoop worked
    {"name": "nn01",  "host": "37.27.255.200",  "user": "hadoop"},
]


def run(c, cmd, t=60):
    try:
        _, so, se = c.exec_command(cmd, timeout=t)
        return so.channel.recv_exit_status(), so.read().decode(errors="replace"), se.read().decode(errors="replace")
    except Exception as e:
        return -1, "", f"EXC: {e}"


def main():
    for spec in HOSTS:
        out_dir = EVIDENCE / spec["name"]
        out_dir.mkdir(exist_ok=True)
        c = paramiko.SSHClient()
        c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        print(f"\n=== {spec['name']} ({spec['host']}) ===", flush=True)
        try:
            c.connect(spec["host"], username=spec["user"], password=PASSWORD,
                      timeout=20, look_for_keys=False, allow_agent=False)
        except Exception as e:
            print(f"  ! connect failed: {e}")
            continue
        for k, cmd in sorted(PROBES.items()):
            rc, out, err = run(c, cmd)
            (out_dir / f"{k}.txt").write_text(f"$ {cmd}\nrc={rc}\n--- stdout ---\n{out}\n--- stderr ---\n{err}\n")
            mark = "+" if rc == 0 else ("?" if rc == -1 else "!")
            print(f"  [{mark}] {k}: rc={rc} bytes={len(out)}", flush=True)
        c.close()

if __name__ == "__main__":
    main()
