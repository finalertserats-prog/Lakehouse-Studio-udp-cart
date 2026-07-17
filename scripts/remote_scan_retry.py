#!/usr/bin/env python3
"""Targeted retry after the first scan revealed:
- Hadoop configs live at /etc/hadoop-3.4.1/etc/hadoop/, not /etc/hadoop-3.4.1/.
- Trino 480 dir owned by trino user; hadoop user cannot read etc/. Use sudo -n.
- Trino 479 dir is empty (post-upgrade).
- afw01 root auth failed; try hadoop user there too.
"""

from __future__ import annotations
import os, sys
from pathlib import Path
import paramiko

PASSWORD = os.environ.get("LHS_SCAN_PASSWORD", "")
EVIDENCE_ROOT = Path(__file__).resolve().parent.parent / "evidence" / "enterprise-onprem-scan"

# Each entry: (out_dir, host, user, probes_dict)
HADOOP_CONF_PROBES = {
    "21b_core_site":   "cat /etc/hadoop-3.4.1/etc/hadoop/core-site.xml",
    "22b_hdfs_site":   "cat /etc/hadoop-3.4.1/etc/hadoop/hdfs-site.xml",
    "23b_yarn_site":   "cat /etc/hadoop-3.4.1/etc/hadoop/yarn-site.xml",
    "24b_mapred_site": "cat /etc/hadoop-3.4.1/etc/hadoop/mapred-site.xml",
    "25b_workers":     "cat /etc/hadoop-3.4.1/etc/hadoop/workers",
    "26b_hadoop_env":  "cat /etc/hadoop-3.4.1/etc/hadoop/hadoop-env.sh 2>/dev/null | grep -vE '^#|^$' | head -40",
    "27b_capacity_scheduler": "cat /etc/hadoop-3.4.1/etc/hadoop/capacity-scheduler.xml 2>/dev/null | head -120",
    "28b_log4j":       "cat /etc/hadoop-3.4.1/etc/hadoop/log4j.properties 2>/dev/null | head -40",
}

TARGETS = [
    {"name": "nn01",  "host": "37.27.255.200",  "user": "hadoop", "probes": {
        **HADOOP_CONF_PROBES,
        "60_starrocks_listing": "ls -la /opt/StarRocks-4.0.0-rc01-ubuntu-amd64/; ls /opt/StarRocks-4.0.0-rc01-ubuntu-amd64/fe/conf/ 2>/dev/null; ls /opt/StarRocks-4.0.0-rc01-ubuntu-amd64/be/conf/ 2>/dev/null",
        "61_starrocks_fe_conf": "cat /opt/StarRocks-4.0.0-rc01-ubuntu-amd64/fe/conf/fe.conf 2>/dev/null | grep -vE '^#|^$' | head -60",
        "62_starrocks_be_conf": "cat /opt/StarRocks-4.0.0-rc01-ubuntu-amd64/be/conf/be.conf 2>/dev/null | grep -vE '^#|^$' | head -60",
        "63_prometheus_full":   "cat /opt/prometheus-3.4.1.linux-amd64/prometheus.yml 2>/dev/null",
        "64_loki_config":       "ls /opt/loki-promtail/; cat /opt/loki-promtail/loki-config.yaml 2>/dev/null | head -80; cat /opt/loki-promtail/promtail-config.yaml 2>/dev/null | head -60",
        "65_grafana_datasources": "ls /opt/grafana-v12.0.1/conf/provisioning/ 2>/dev/null; cat /opt/grafana-v12.0.1/conf/provisioning/datasources/*.yaml 2>/dev/null | head -40",
        "66_ranger_extras":     "ls /usr/lib/ranger/; cat /usr/lib/ranger/ranger-3.0.0-SNAPSHOT-admin/ews/webapp/WEB-INF/classes/conf/ranger-admin-site.xml 2>/dev/null | head -100",
        "70_node_exporter":     "ps -eo pid,user,cmd | grep [n]ode_exporter",
        "71_promtail":          "ps -eo pid,user,cmd | grep [p]romtail",
    }},
    {"name": "stg01", "host": "65.21.246.208", "user": "hadoop", "probes": {
        "40_trino_480_etc_ls":   "sudo -n ls -la /opt/trino-server-480/etc/ 2>&1; ls /opt/trino-server-480/etc/ 2>&1",
        "41_trino_480_config":   "sudo -n cat /opt/trino-server-480/etc/config.properties 2>&1; sudo -n cat /opt/trino-server-480/etc/node.properties 2>&1; sudo -n cat /opt/trino-server-480/etc/jvm.config 2>&1",
        "42_trino_480_catalogs": "sudo -n ls /opt/trino-server-480/etc/catalog/ 2>&1; for f in /opt/trino-server-480/etc/catalog/*.properties; do echo \"---$f---\"; sudo -n cat \"$f\" 2>&1; done",
        "43_trino_user_check":   "id trino 2>&1; sudo -n -u trino whoami 2>&1",
        "44_trino_proc_args":    "ps -eo pid,user,cmd | grep [t]rino",
        "50_starrocks_proc":     "ps -eo pid,user,cmd | grep -iE 'starrocks|palo'",
        "51_hive_running_proc":  "ps -eo pid,user,cmd | grep -iE 'hive|metastore'",
        "52_metastore_jdbc_url": "grep -A1 javax.jdo.option.ConnectionURL /opt/apache-hive-4.0.1-bin/conf/hive-site.xml 2>/dev/null",
        "53_pg_in_docker":       "docker ps 2>&1 | head -20; ss -tlnp 2>/dev/null | grep -E ':5432|:6432'",
    }},
    {"name": "dn01",  "host": "157.180.25.254", "user": "hadoop", "probes": {
        **HADOOP_CONF_PROBES,
        "60_dfs_data_layout":    "ls /data/ 2>/dev/null; ls /hadoop/ 2>/dev/null; ls /var/lib/hadoop* 2>/dev/null; mount | grep -E 'data|hadoop'",
        "61_datanode_args":      "ps -eo pid,user,cmd | grep -iE '[d]atanode|[n]odemanager' | head -5",
    }},
]

# afw01 was root@. Try hadoop user since root failed.
AFW01_TARGETS = [
    {"name": "afw01_as_hadoop", "host": "135.181.157.21", "user": "hadoop", "probes": {
        "00_user":           "id; pwd; ls /home 2>/dev/null",
        "01_uname":          "uname -a; cat /etc/os-release 2>/dev/null",
        "02_listening":      "ss -tlnp 2>/dev/null",
        "03_services":       "systemctl list-units --type=service --state=running --no-pager 2>/dev/null | grep -iE 'airflow|postgres|celery|redis' ",
        "04_airflow_ps":     "ps -eo pid,user,cmd | grep -iE '[a]irflow|[p]ostgres|[r]edis|[c]elery' | head -30",
        "05_airflow_dir":    "ls -la /home/hadoop/airflow 2>/dev/null; ls -la /opt/airflow 2>/dev/null; ls -la /root/airflow 2>/dev/null",
        "06_airflow_ver":    "airflow version 2>&1 | tail -10; which airflow",
        "07_curl_webui":     "curl -s -o /dev/null -w 'webui http=%{http_code} time=%{time_total}\\n' http://localhost:8080/health 2>&1",
    }},
]


def run(client, cmd, timeout=90):
    try:
        _, stdout, stderr = client.exec_command(cmd, timeout=timeout)
        out = stdout.read().decode(errors="replace")
        err = stderr.read().decode(errors="replace")
        rc = stdout.channel.recv_exit_status()
        return rc, out, err
    except Exception as exc:
        return -1, "", f"EXC: {exc}"


def scan(spec):
    out_dir = EVIDENCE_ROOT / spec["name"]
    out_dir.mkdir(parents=True, exist_ok=True)
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    print(f"\n=== {spec['name']} ({spec['host']}) as {spec['user']} ===", flush=True)
    try:
        client.connect(spec["host"], username=spec["user"], password=PASSWORD,
                       timeout=20, look_for_keys=False, allow_agent=False)
    except Exception as exc:
        print(f"  ! connect failed: {exc}")
        (out_dir / f"_CONNECT_ERROR_{spec['user']}.txt").write_text(str(exc))
        return
    for key, cmd in sorted(spec["probes"].items()):
        rc, out, err = run(client, cmd)
        (out_dir / f"{key}.txt").write_text(f"$ {cmd}\nrc={rc}\n--- stdout ---\n{out}\n--- stderr ---\n{err}\n")
        marker = "+" if rc == 0 else ("?" if rc == -1 else "!")
        print(f"  [{marker}] {key}: rc={rc} bytes={len(out)}", flush=True)
    client.close()


def main():
    if not PASSWORD:
        print("set LHS_SCAN_PASSWORD", file=sys.stderr); return 2
    for t in TARGETS + AFW01_TARGETS:
        scan(t)
    return 0

if __name__ == "__main__":
    sys.exit(main())
