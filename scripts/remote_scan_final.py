#!/usr/bin/env python3
"""Final sweep: observability stack on nn01, airflow web on afw01, dn01 mounts."""
import os, sys
from pathlib import Path
import paramiko

PASSWORD = os.environ["LHS_SCAN_PASSWORD"]
EVIDENCE = Path(__file__).resolve().parent.parent / "evidence" / "enterprise-onprem-scan"

TARGETS = [
    {"name": "nn01", "host": "37.27.255.200", "user": "hadoop", "probes": {
        "80_prom_running":   "pgrep -af prometheus 2>&1 | head -5; ss -tlnp 2>/dev/null | grep -E ':9090|:3000|:3100' ",
        "81_grafana_running":"pgrep -af grafana 2>&1 | head -5",
        "82_loki_running":   "pgrep -af loki 2>&1 | head -5; pgrep -af promtail 2>&1 | head -5",
        "83_ranger_pid":     "pgrep -af ranger 2>&1 | head -5",
        "84_starrocks_be":   "pgrep -af starrocks 2>&1 | head -5",
        "85_node_ports":     "ss -tlnp 2>/dev/null | grep -E 'LISTEN.+(:8030|:9090|:3000|:3100|:6080|:6182|:9100)' ",
        "86_hdfs_https_curl":"curl -sk -o /dev/null -w 'nn-ui=%{http_code}\\n' https://localhost:9871 2>&1",
        "87_yarn_rm_ui":     "curl -s -o /dev/null -w 'rm-ui=%{http_code}\\n' http://localhost:8090 2>&1",
        "88_mr_history":     "curl -s -o /dev/null -w 'mr-hist=%{http_code}\\n' http://localhost:19890 2>&1",
        "89_starrocks_fe_ui_remote": "curl -s -o /dev/null -w 'sr-fe-stg=%{http_code}\\n' http://sdpdevstg01.techsophy.com:8030 2>&1",
        "90_ranger_admin_port": "curl -s -o /dev/null -w 'ranger=%{http_code}\\n' http://localhost:6080 2>&1; curl -sk -o /dev/null -w 'ranger-https=%{http_code}\\n' https://localhost:6182 2>&1",
    }},
    {"name": "stg01", "host": "65.21.246.208", "user": "hadoop", "probes": {
        "60_trino_anywhere": "pgrep -af trino 2>&1 | head -5; ss -tlnp 2>/dev/null | grep -E ':8080|:18080'",
        "61_starrocks_proc": "pgrep -af StarRocksFE 2>&1; pgrep -af starrocks_be 2>&1",
        "62_hms_remote_test":"curl -s --connect-timeout 5 -o /dev/null -w 'hs2-local=%{http_code}\\n' http://localhost:10002 2>&1",
        "63_metastore_pg":   "ss -tlnp 2>/dev/null | grep -E ':5432|:6432'",
        "64_systemd_hive":   "systemctl list-units --type=service --no-pager 2>/dev/null | grep -iE 'hive|trino|metastore|starrocks|pgbouncer|postgres'",
    }},
    {"name": "dn01", "host": "157.180.25.254", "user": "hadoop", "probes": {
        "70_dfs_data_dir":   "ls -la /mnt/dfs/ 2>&1; ls /mnt/dfs/data/current 2>/dev/null | head -10; mount | grep -E '/mnt|/data'",
        "71_dn_block_dirs":  "du -sh /mnt/dfs/data 2>/dev/null; ls /mnt/dfs/data/current/BP-* 2>/dev/null | head -5",
        "72_nm_local_dirs":  "ls /mnt/yarn 2>/dev/null; ls /tmp/hadoop* 2>/dev/null | head -10; find / -maxdepth 3 -name 'nm-local-dir' 2>/dev/null",
    }},
    {"name": "afw01_as_hadoop", "host": "135.181.157.21", "user": "hadoop", "probes": {
        "10_curl_web_full":  "curl -s --connect-timeout 5 -o /dev/null -w 'web-root=%{http_code}\\n' http://localhost:8080/ 2>&1; curl -s --connect-timeout 5 -o /dev/null -w 'web-login=%{http_code}\\n' http://localhost:8080/login 2>&1; curl -s --connect-timeout 5 -o /dev/null -w 'web-api=%{http_code}\\n' http://localhost:8080/api/v1/health 2>&1",
        "11_airflow_root_dir":"sudo -n ls /root/airflow 2>&1 | head; ls /root/airflow 2>&1 | head",
        "12_dag_dir_guess":  "find /opt /home -maxdepth 4 -name 'dags' -type d 2>/dev/null | head -5; find /opt /home -maxdepth 4 -name 'airflow.cfg' 2>/dev/null | head -5",
        "13_curl_internal":  "curl -s --connect-timeout 3 -o /dev/null -w 'rabbit=%{http_code}\\n' http://localhost:15672 2>&1; curl -s --connect-timeout 3 -o /dev/null -w 'pg-admin=%{http_code}\\n' http://localhost:5050 2>&1",
        "14_remote_targets": "curl -s --connect-timeout 5 -o /dev/null -w 'nn-rpc-ok-check\\n' http://sdpdevnn01.techsophy.com:9870 2>&1; nc -z -w3 sdpdevnn01.techsophy.com 9820 && echo 'NN-RPC=OK' || echo 'NN-RPC=FAIL'; nc -z -w3 sdpdevstg01.techsophy.com 9083 && echo 'HMS=OK' || echo 'HMS=FAIL'; nc -z -w3 sdpdevstg01.techsophy.com 8030 && echo 'SR-FE=OK' || echo 'SR-FE=FAIL'",
    }},
]

def main():
    for spec in TARGETS:
        out_dir = EVIDENCE / spec["name"]
        c = paramiko.SSHClient()
        c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        print(f"\n=== {spec['name']} ===", flush=True)
        try:
            c.connect(spec["host"], username=spec["user"], password=PASSWORD,
                      timeout=20, look_for_keys=False, allow_agent=False)
        except Exception as e:
            print(f"  ! connect: {e}")
            continue
        for k, cmd in sorted(spec["probes"].items()):
            try:
                _, so, se = c.exec_command(cmd, timeout=30)
                out = so.read().decode(errors="replace")
                err = se.read().decode(errors="replace")
                rc = so.channel.recv_exit_status()
                (out_dir / f"{k}.txt").write_text(f"$ {cmd}\nrc={rc}\n--- stdout ---\n{out}\n--- stderr ---\n{err}\n")
                print(f"  [{'+'if rc==0 else'!'}] {k}: rc={rc} bytes={len(out)}", flush=True)
            except Exception as e:
                print(f"  [?] {k}: {e}")
        c.close()

if __name__ == "__main__":
    sys.exit(main() or 0)
