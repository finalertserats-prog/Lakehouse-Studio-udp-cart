"""Turn raw install failures into actionable diagnoses.

The runner produces a stream of stdout/stderr lines plus a final exit code.
Given the failed step and recent log tail, this module classifies the
failure into a known category and returns:
  - title          (one-line summary)
  - why            (why it happened, plain English)
  - fix            (concrete remediation, executable where possible)
  - retryable      (bool — is it safe to retry the step after the fix?)
  - category       (port_conflict | disk_full | oom | image_pull | daemon_down |
                    perm_denied | network | unknown)
"""
from __future__ import annotations
import re
from typing import Optional


_PATTERNS: list[dict] = [
    {
        "category": "port_conflict",
        "match": re.compile(r"(?:address already in use|bind.*address.*in use|port is already allocated)", re.IGNORECASE),
        "extract_port": re.compile(r"(?:port|0\.0\.0\.0):(\d{2,5})"),
        "title": "Port conflict",
        "why_tmpl": "Another process is already listening on port {port}, so Docker couldn't bind it.",
        "fix_tmpl": "Find what's using it and stop it:\n  Linux:  sudo lsof -i :{port}    # or ss -tlnp '( sport = :{port} )'\n  Windows: netstat -ano | findstr :{port}\nThen `taskkill /F /PID <pid>` (Windows) or `kill <pid>` (Linux). Re-run the failed step.",
        "retryable": True,
    },
    {
        "category": "disk_full",
        "match": re.compile(r"no space left on device", re.IGNORECASE),
        "title": "Disk full",
        "why": "The Docker storage volume ran out of space — usually image layers + container writable layers.",
        "fix": "Free space: `docker system prune -a --volumes` (DESTROYS unused images/volumes). Or move Docker's data root to a bigger disk. Re-run after disk has 50+ GB free.",
        "retryable": True,
    },
    {
        "category": "oom",
        "match": re.compile(r"(?:OOMKilled|out of memory|killed.*signal 9|exit code 137)", re.IGNORECASE),
        "title": "Out of memory (OOMKilled)",
        "why": "The container exceeded available RAM and the Linux OOM killer terminated it. StarRocks BE is the usual suspect on smaller VPSes.",
        "fix": "Move to a larger VPS (≥16 GB RAM recommended), or reduce StarRocks BE memory in the compose file. Re-run after upgrading.",
        "retryable": True,
    },
    {
        "category": "image_pull",
        "match": re.compile(r"(?:pull access denied|manifest unknown|toomanyrequests|repository does not exist|i/o timeout.*registry|net/http: TLS handshake timeout)", re.IGNORECASE),
        "title": "Image pull failed",
        "why": "Docker couldn't pull one of the component images — network issue, registry rate limit, or wrong tag.",
        "fix": "Check internet from the host: `curl -I https://registry-1.docker.io`. If you're rate-limited, log in: `docker login`. If the tag is wrong, check stacks/udp-local-v0.2.yaml versions. Re-run.",
        "retryable": True,
    },
    {
        "category": "daemon_down",
        "match": re.compile(r"(?:cannot connect to the Docker daemon|Is the docker daemon running|dockerDesktopLinuxEngine|Got permission denied while trying to connect to the Docker daemon socket)", re.IGNORECASE),
        "title": "Docker daemon not reachable",
        "why": "The Docker CLI is installed but the daemon isn't running (Docker Desktop stopped, or systemd service is down), or the current user can't access the socket.",
        "fix": "Linux: `sudo systemctl start docker && sudo usermod -aG docker $USER && newgrp docker`.\nWindows/Mac: launch Docker Desktop and wait for the whale icon. Re-run inspection.",
        "retryable": True,
    },
    {
        "category": "perm_denied",
        # Tighter: only match when paired with EACCES, a path, or the install dir,
        # to avoid false-positives on benign log mentions.
        "match": re.compile(
            r"(?:EACCES\b|"
            r"permission denied\s*(?:while|on|to|for|writing|reading|opening|accessing|:)|"
            r"open\s+\S+:\s*permission denied|"
            r"mkdir\s+\S+:\s*permission denied)",
            re.IGNORECASE,
        ),
        "title": "Permission denied",
        "why": "The Studio process can't read/write something it needs — usually the install directory or a Docker socket.",
        "fix": "Make sure the install directory is owned by the user running Studio. On Linux, ensure the user is in the `docker` group (`groups` should list it). Re-run after fixing.",
        "retryable": True,
    },
    {
        "category": "network",
        # Tighter: require a transport context (dial/curl/Get/host:port) — bare
        # 'connection refused' during container warm-up is normal and not a failure.
        "match": re.compile(
            r"(?:"
            r"dial (?:tcp|udp) .*: connection refused|"
            r"Get \".*\": .*connection refused|"
            r"curl: \(7\) .*connection refused|"
            r"connection timed out after \d+|"
            r"temporary failure in name resolution|"
            r"no route to host"
            r")",
            re.IGNORECASE,
        ),
        "title": "Network unreachable",
        "why": "A container or the install script couldn't reach a required endpoint — DNS, the docker registry, or an inter-container service.",
        "fix": "Verify outbound connectivity: `curl -v https://registry-1.docker.io`. If you're behind a corporate proxy, configure Docker's daemon with `HTTP_PROXY/HTTPS_PROXY`. Re-run.",
        "retryable": True,
    },
    {
        "category": "git_clone",
        # Tighter: require a git-specific signature (no bare 'Could not resolve host').
        "match": re.compile(
            r"(?:fatal: unable to access 'https?://[^']+'|"
            r"fatal: repository '.*' not found|"
            r"fatal: could not read from remote repository)",
            re.IGNORECASE,
        ),
        "title": "Git clone failed",
        "why": "Studio couldn't fetch the UDP repository from GitHub — most likely no internet or DNS.",
        "fix": "Test from the host: `git ls-remote https://github.com/finalertserats-prog/Unified-Data-Plug.git`. Fix connectivity, then retry.",
        "retryable": True,
    },
    {
        "category": "compose_invalid",
        "match": re.compile(r"(?:yaml: line|services\..*: must be a mapping|invalid compose)", re.IGNORECASE),
        "title": "Invalid docker-compose.yml",
        "why": "The compose file failed to parse. Usually means the UDP clone got truncated or you're on a Compose v1 that doesn't understand v2 features.",
        "fix": "Make sure you have Docker Compose v2 (`docker compose version`). Re-clone the UDP repo (delete the install dir, retry).",
        "retryable": True,
    },
    {
        "category": "starrocks_be",
        "match": re.compile(r"(?:starrocks-be.*(?:unhealthy|exited)|udp-starrocks-be.*Restarting|Backend not in alive)", re.IGNORECASE),
        "title": "StarRocks Backend won't start",
        "why": "Usually means the BE container's priority_networks setting doesn't match the Docker bridge subnet on this host.",
        "fix": "Check the BE container logs: `docker logs udp-starrocks-be`. If you see 'no available network', edit docker-compose.yml's BE command to use the right subnet (default is 172.16.0.0/12). Re-run start.",
        "retryable": True,
    },
]


def explain(failed_step: Optional[str], log_tail: list[str], exit_code: Optional[int] = None) -> Optional[dict]:
    """Look at the last N log lines and try to classify the failure.

    Returns None if no pattern matched (caller should fall back to generic
    'check the logs' messaging)."""
    blob = "\n".join(log_tail[-200:])  # last 200 lines is plenty

    for pat in _PATTERNS:
        m = pat["match"].search(blob)
        if not m:
            continue
        # Pattern matched — fill the template
        port = None
        if pat.get("extract_port"):
            pm = pat["extract_port"].search(blob)
            if pm:
                port = pm.group(1)
        title = pat["title"]
        if pat.get("why_tmpl") and not port:
            # Port-conflict template needs a port; without one, fall back to generic.
            why = "Another process is already listening on a port the stack needs."
            fix = ("Find what's using the conflicting port:\n"
                   "  Linux:  sudo lsof -i -P -n | grep LISTEN | grep -E '9000|9001|8181|8030|9030|8888'\n"
                   "  Windows: netstat -ano | findstr LISTENING\n"
                   "Stop the offending process and re-run.")
        else:
            why = pat.get("why") or pat["why_tmpl"].format(port=port)
            fix = pat.get("fix") or pat["fix_tmpl"].format(port=port)
        return {
            "category": pat["category"],
            "title": title,
            "why": why,
            "fix": fix,
            "retryable": bool(pat.get("retryable", True)),
            "failed_step": failed_step,
            "exit_code": exit_code,
        }

    # Nothing matched — generic fallback
    return {
        "category": "unknown",
        "title": "Install failed",
        "why": f"The step '{failed_step or 'unknown'}' exited with code {exit_code}, but the failure didn't match any known pattern.",
        "fix": "Check the full logs in the install panel. If you're stuck, open an issue with the install_id and the log tail attached.",
        "retryable": True,
        "failed_step": failed_step,
        "exit_code": exit_code,
    }
