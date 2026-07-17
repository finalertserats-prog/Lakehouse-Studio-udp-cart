"""Pre-baked demo queries Studio can run against a deployed UDP stack.

We intentionally do NOT support free-form SQL in v0.1 — the surface area is
too easy to abuse. A small map of canned queries keyed by id lets the UI
offer "Run demo query" without becoming a SQL injection vector.
"""
from __future__ import annotations
import asyncio
import shutil
from typing import Any


DEMO_QUERIES: dict[str, dict[str, str]] = {
    "udp-customer-summary": {
        "label": "Top demo customers (analytics view)",
        "description": "Reads app_analytics.demo_customer_summary — the curated table built by UDP's bootstrap. Verifies StarRocks can serve queries from Iceberg via Lakekeeper.",
        "sql": (
            "SELECT * FROM app_analytics.demo_customer_summary "
            "ORDER BY total_amount DESC LIMIT 10;"
        ),
        "container": "udp-starrocks-fe",
    },
    "udp-catalogs": {
        "label": "List catalogs and databases",
        "description": "Quick StarRocks sanity check — what catalogs and databases are visible.",
        "sql": "SHOW CATALOGS; SHOW DATABASES;",
        "container": "udp-starrocks-fe",
    },
    "udp-row-count": {
        "label": "Row count: demo_customer_summary",
        "description": "Single-cell row count.",
        "sql": "SELECT COUNT(*) AS rows FROM app_analytics.demo_customer_summary;",
        "container": "udp-starrocks-fe",
    },
}


def list_queries() -> list[dict[str, str]]:
    return [{"id": k, "label": v["label"], "description": v["description"]} for k, v in DEMO_QUERIES.items()]


async def run_demo_query(query_id: str, install_dir=None, timeout: int = 30) -> dict[str, Any]:
    if query_id not in DEMO_QUERIES:
        raise KeyError(f"unknown query id: {query_id}")
    q = DEMO_QUERIES[query_id]
    if shutil.which("docker") is None:
        return {"error": "docker CLI not on PATH (this Studio host can't reach docker)"}

    # Resolve THIS install's StarRocks container (udp-/sl-/<project>- prefixes)
    # instead of assuming the UDP container name; fall back to the canned name.
    from .sql_editor import resolve_container
    container = await resolve_container(install_dir, "starrocks-fe") or q["container"]

    # mysql client lives inside the starrocks-fe container; use docker exec.
    # --batch + --raw produces tab-delimited output with a header row.
    cmd = [
        "docker", "exec", "-i", container,
        "mysql", "-h", "127.0.0.1", "-P", "9030", "-u", "root",
        "--batch", "--raw", "-e", q["sql"],
    ]

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout_b, stderr_b = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        return {"error": f"query timed out after {timeout}s"}

    out = stdout_b.decode("utf-8", errors="replace")
    err = stderr_b.decode("utf-8", errors="replace")

    if proc.returncode != 0:
        # Common case: container not running, or analytics view not yet created
        return {"error": err.strip() or out.strip() or f"mysql exited {proc.returncode}"}

    rows: list[list[str]] = []
    columns: list[str] = []
    lines = [ln for ln in out.splitlines() if ln.strip()]
    if lines:
        columns = lines[0].split("\t")
        for ln in lines[1:]:
            rows.append(ln.split("\t"))

    return {
        "query_id": query_id,
        "sql": q["sql"],
        "columns": columns,
        "rows": rows,
        "row_count": len(rows),
        "stderr": err.strip() if err.strip() else None,
    }
