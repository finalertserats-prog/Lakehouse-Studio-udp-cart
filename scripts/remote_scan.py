#!/usr/bin/env python3
"""Remote audit of the Enterprise On-Prem Datalake via paramiko.

Reads HOSTS table below, runs a battery of read-only commands per role,
and dumps stdout/stderr to evidence/enterprise-onprem-scan/<host>/<probe>.txt.
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import paramiko

PASSWORD = os.environ.get("LHS_SCAN_PASSWORD", "")
EVIDENCE_ROOT = Path(__file__).resolve().parent.parent / "evidence" / "enterprise-onprem-scan"

HOSTS = [
    {"name": "nn01",  "host": "37.27.255.200",  "user": "hadoop", "fqdn": "sdpdevnn01.techsophy.com"},
    {"name": "stg01", "host": "65.21.246.208",  "user": "hadoop", "fqdn": "sdpdevstg01.techsophy.com"},
    {"name": "dn01",  "host": "157.180.25.254", "user": "hadoop", "fqdn": "sdpdevdn01.techsophy.com"},
    {"name": "afw01", "host": "135.181.157.21", "user": "root",   "fqdn": "sdpdevafw01.techsophy.com"},
]

# Probes common to every host
COMMON_PROBES = {
    "00_uname":          "uname -a; cat /etc/os-release 2>/dev/null",
    "01_hostname":       "hostname -f; hostname -I",
    "02_uptime":         "uptime; who",
    "03_cpu":            "lscpu | head -25",
    "04_memory":         "free -h; cat /proc/meminfo | head -5",
    "05_disk":           "df -h --output=source,fstype,size,used,avail,pcent,target | grep -vE '^(tmpfs|devtmpfs|udev)'",
    "06_listening_ports": "ss -tlnp 2>/dev/null || netstat -tlnp 2>/dev/null",
    "07_systemd_units":  "systemctl list-units --type=service --state=running --no-pager 2>/dev/null | head -60",
    "08_running_java":   "ps -eo pid,user,cmd | grep -E '[j]ava' | head -40",
    "09_opt_listing":    "ls -la /opt/ 2>/dev/null",
    "10_usr_lib_listing": "ls -la /usr/lib/ 2>/dev/null | grep -iE 'hadoop|hive|spark|tez|ranger|trino|starrocks|airflow|loki|grafana|prometh'",
    "11_etc_listing":    "ls -la /etc/ 2>/dev/null | grep -iE 'hadoop|hive|spark|tez|ranger|trino|starrocks|airflow|loki|grafana|prometh'",
    "12_env":            "env | grep -iE 'HADOOP|HIVE|SPARK|TEZ|RANGER|TRINO|JAVA|PATH' | head -30",
    "13_java_version":   "java -version 2>&1; which java",
    "14_user":           "id; groups",
}

# Per-role probes (additional)
ROLE_PROBES = {
    "nn01": {
        "20_hadoop_conf_listing":     "ls -la /etc/hadoop-3.4.1/ 2>/dev/null; ls -la /etc/hadoop/ 2>/dev/null",
        "21_core_site":               "cat /etc/hadoop-3.4.1/core-site.xml 2>/dev/null || cat /etc/hadoop/conf/core-site.xml 2>/dev/null",
        "22_hdfs_site":               "cat /etc/hadoop-3.4.1/hdfs-site.xml 2>/dev/null || cat /etc/hadoop/conf/hdfs-site.xml 2>/dev/null",
        "23_yarn_site":               "cat /etc/hadoop-3.4.1/yarn-site.xml 2>/dev/null || cat /etc/hadoop/conf/yarn-site.xml 2>/dev/null",
        "24_mapred_site":             "cat /etc/hadoop-3.4.1/mapred-site.xml 2>/dev/null || cat /etc/hadoop/conf/mapred-site.xml 2>/dev/null",
        "25_workers":                 "cat /etc/hadoop-3.4.1/workers 2>/dev/null || cat /etc/hadoop/conf/workers 2>/dev/null",
        "30_ranger_listing":          "ls -la /usr/lib/ranger/ranger-3.0.0-SNAPSHOT-admin/ 2>/dev/null; ls /usr/lib/ranger/ranger-3.0.0-SNAPSHOT-admin/ews/webapp/WEB-INF/classes/conf/ 2>/dev/null",
        "31_ranger_install_props":    "cat /usr/lib/ranger/ranger-3.0.0-SNAPSHOT-admin/install.properties 2>/dev/null | grep -vE 'PASSWORD|password|SECRET|secret' | head -80",
        "32_ranger_admin_status":     "systemctl status ranger-admin --no-pager 2>/dev/null | head -20",
        "33_starrocks_fe":            "ls /opt/StarRocks-4.0.0-rc01-ubuntu-amd64/ 2>/dev/null; cat /opt/StarRocks-4.0.0-rc01-ubuntu-amd64/fe/conf/fe.conf 2>/dev/null | grep -vE 'password|PASSWORD' | head -40",
        "40_prometheus_conf":         "cat /opt/prometheus-3.4.1.linux-amd64/prometheus.yml 2>/dev/null | head -80; ls /opt/prometheus-3.4.1.linux-amd64/ 2>/dev/null",
        "41_grafana_listing":         "ls /opt/grafana-v12.0.1/ 2>/dev/null; cat /opt/grafana-v12.0.1/conf/grafana.ini 2>/dev/null | grep -vE 'password|PASSWORD|secret_key' | head -40",
        "42_loki_listing":            "ls /opt/loki-promtail/ 2>/dev/null",
        "50_hdfs_report":             "/etc/hadoop-3.4.1/bin/hdfs dfsadmin -report 2>&1 | head -120 || /opt/hadoop/bin/hdfs dfsadmin -report 2>&1 | head -120",
        "51_yarn_nodes":              "/etc/hadoop-3.4.1/bin/yarn node -list 2>&1 | head -30 || /opt/hadoop/bin/yarn node -list 2>&1 | head -30",
        "52_hdfs_fs_root":            "/etc/hadoop-3.4.1/bin/hdfs dfs -ls / 2>&1 | head -30 || hdfs dfs -ls / 2>&1 | head -30",
    },
    "stg01": {
        "20_hive_listing":            "ls -la /opt/apache-hive-4.0.1-bin/ 2>/dev/null; ls /opt/apache-hive-4.0.1-bin/conf/ 2>/dev/null",
        "21_hive_site":               "cat /opt/apache-hive-4.0.1-bin/conf/hive-site.xml 2>/dev/null | grep -vE 'password|Password|PASSWORD' | head -150",
        "22_tez_site":                "cat /opt/apache-tez-0.10.4-bin/conf/tez-site.xml 2>/dev/null | head -80",
        "23_spark_defaults":          "cat /opt/spark-3.4.4-bin-hadoop3/conf/spark-defaults.conf 2>/dev/null | head -80",
        "24_spark_env":               "cat /opt/spark-3.4.4-bin-hadoop3/conf/spark-env.sh 2>/dev/null | head -60",
        "30_trino_479":               "ls /opt/trino-server-479/ 2>/dev/null; cat /opt/trino-server-479/etc/config.properties 2>/dev/null; cat /opt/trino-server-479/etc/node.properties 2>/dev/null",
        "31_trino_480":               "ls /opt/trino-server-480/ 2>/dev/null; cat /opt/trino-server-480/etc/config.properties 2>/dev/null; cat /opt/trino-server-480/etc/node.properties 2>/dev/null",
        "32_trino_catalogs_480":      "ls /opt/trino-server-480/etc/catalog/ 2>/dev/null; for f in /opt/trino-server-480/etc/catalog/*.properties; do echo \"---$f---\"; cat \"$f\" | grep -vE 'password|Password|PASSWORD'; done 2>/dev/null",
        "33_starrocks_dir":           "ls /opt/StarRocks-4.0.0-rc01-ubuntu-amd64/ 2>/dev/null; cat /opt/StarRocks-4.0.0-rc01-ubuntu-amd64/fe/conf/fe.conf 2>/dev/null | head -40; cat /opt/StarRocks-4.0.0-rc01-ubuntu-amd64/be/conf/be.conf 2>/dev/null | head -40",
        "40_running_services":       "systemctl list-units --type=service --state=running --no-pager 2>/dev/null | grep -iE 'hive|trino|starrocks|spark|tez'",
        "41_postgres":               "systemctl status postgresql --no-pager 2>/dev/null | head -10; ss -tlnp 2>/dev/null | grep -E ':5432|:6432'",
        "42_pgbouncer":              "ls /etc/pgbouncer/ 2>/dev/null; cat /etc/pgbouncer/pgbouncer.ini 2>/dev/null | grep -vE 'password|auth_file' | head -40",
        "50_hive_metastore_check":    "ss -tlnp 2>/dev/null | grep -E ':9083|:10000|:10002'",
        "51_trino_health":            "curl -s -o /dev/null -w 'trino-480: %{http_code}\\n' http://localhost:8080/v1/info 2>&1; curl -s http://localhost:8080/v1/info 2>&1 | head -20",
        "52_starrocks_fe_state":      "curl -s http://localhost:8030/api/show_proc?path=/frontends 2>&1 | head -20",
    },
    "dn01": {
        "20_hadoop_conf_listing":     "ls -la /etc/hadoop-3.4.1/ 2>/dev/null",
        "21_core_site":               "cat /etc/hadoop-3.4.1/core-site.xml 2>/dev/null",
        "22_hdfs_site":               "cat /etc/hadoop-3.4.1/hdfs-site.xml 2>/dev/null",
        "23_yarn_site":               "cat /etc/hadoop-3.4.1/yarn-site.xml 2>/dev/null",
        "30_dfs_data_dirs":           "df -h | grep -E 'data|dfs'; ls /data 2>/dev/null; ls /hadoop 2>/dev/null",
        "31_datanode_processes":      "ps -eo pid,user,cmd | grep -iE '[d]atanode|[n]odemanager' ",
        "32_yarn_local_dirs":         "ls /var/lib/hadoop-yarn 2>/dev/null; ls -la /opt/yarn 2>/dev/null",
    },
    "afw01": {
        "20_airflow_version":         "airflow version 2>/dev/null || /root/airflow/.venv/bin/airflow version 2>/dev/null || pip show apache-airflow 2>/dev/null | head -5",
        "21_airflow_dir":             "ls -la /root/airflow/ 2>/dev/null",
        "22_airflow_cfg":             "cat /root/airflow/airflow.cfg 2>/dev/null | grep -vE 'password|fernet_key|secret_key' | head -120",
        "23_airflow_dags":            "ls -la /root/airflow/dags/ 2>/dev/null | head -40",
        "24_airflow_processes":       "ps -eo pid,user,cmd | grep -iE '[a]irflow|[p]ostgres'",
        "25_airflow_services":        "systemctl list-units --type=service --no-pager 2>/dev/null | grep -iE 'airflow'",
        "26_airflow_db":              "ss -tlnp 2>/dev/null | grep -E ':5432|:8080'",
    },
}


def run(client: paramiko.SSHClient, cmd: str, timeout: int = 60) -> tuple[int, str, str]:
    try:
        stdin, stdout, stderr = client.exec_command(cmd, timeout=timeout)
        out = stdout.read().decode(errors="replace")
        err = stderr.read().decode(errors="replace")
        rc = stdout.channel.recv_exit_status()
        return rc, out, err
    except Exception as exc:  # noqa: BLE001
        return -1, "", f"EXC: {exc}"


def scan_host(spec: dict) -> dict:
    name = spec["name"]
    out_dir = EVIDENCE_ROOT / name
    out_dir.mkdir(parents=True, exist_ok=True)

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    report = {"name": name, "host": spec["host"], "fqdn": spec["fqdn"], "status": "unknown", "probes": 0}
    print(f"\n=== {name} ({spec['fqdn']} / {spec['host']}) as {spec['user']} ===", flush=True)
    try:
        client.connect(
            hostname=spec["host"],
            username=spec["user"],
            password=PASSWORD,
            timeout=20,
            banner_timeout=20,
            auth_timeout=20,
            look_for_keys=False,
            allow_agent=False,
        )
    except Exception as exc:  # noqa: BLE001
        report["status"] = f"connect-failed: {exc}"
        (out_dir / "_CONNECT_ERROR.txt").write_text(str(exc))
        print(f"  ! connect failed: {exc}", flush=True)
        return report

    report["status"] = "connected"
    probes = {**COMMON_PROBES, **ROLE_PROBES.get(name, {})}
    for key, cmd in sorted(probes.items()):
        rc, out, err = run(client, cmd, timeout=90)
        body = f"$ {cmd}\nrc={rc}\n--- stdout ---\n{out}\n--- stderr ---\n{err}\n"
        (out_dir / f"{key}.txt").write_text(body)
        report["probes"] += 1
        marker = "+" if rc == 0 else ("?" if rc == -1 else "!")
        print(f"  [{marker}] {key}: rc={rc} bytes={len(out)}", flush=True)

    client.close()
    return report


def main() -> int:
    if not PASSWORD:
        print("Set LHS_SCAN_PASSWORD env var.", file=sys.stderr)
        return 2
    EVIDENCE_ROOT.mkdir(parents=True, exist_ok=True)
    started = time.time()
    summary = []
    for spec in HOSTS:
        summary.append(scan_host(spec))
    elapsed = time.time() - started

    summary_path = EVIDENCE_ROOT / "_summary.txt"
    with summary_path.open("w") as fh:
        fh.write(f"Scan elapsed: {elapsed:.1f}s\n\n")
        for r in summary:
            fh.write(f"{r['name']:<6} {r['host']:<16} {r['fqdn']:<32} status={r['status']} probes={r['probes']}\n")
    print(f"\nSummary written to {summary_path}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
