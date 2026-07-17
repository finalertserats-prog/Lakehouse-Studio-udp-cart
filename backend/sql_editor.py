"""Sandboxed free-form SQL editor.

Accepts user-supplied SQL but only runs read-only statements
(SELECT/SHOW/DESCRIBE/EXPLAIN/WITH). Anything else is rejected before
docker exec is invoked. Same wire format as demo_query.run_demo_query so
the frontend can render results identically.
"""
from __future__ import annotations
import asyncio
import json
import re
import shutil
from pathlib import Path
from typing import Any


MAX_SQL_BYTES = 10_000
DEFAULT_TIMEOUT_SEC = 30

# Statements whose leading keyword is in this set are allowed.
_ALLOWED = {"SELECT", "SHOW", "DESCRIBE", "DESC", "EXPLAIN", "WITH"}

# Anything matching any of these inside the SQL (after we've stripped strings
# and comments) is rejected even if the first keyword looks safe. These are
# the destructive verbs.
_FORBIDDEN_KEYWORDS = {
    "INSERT", "UPDATE", "DELETE", "DROP", "TRUNCATE", "CREATE", "ALTER",
    "GRANT", "REVOKE", "RENAME", "ADMIN", "USE", "SET", "CALL", "LOAD",
    "INSTALL", "REPLACE", "MERGE", "COPY", "IMPORT", "EXPORT", "BACKUP",
    "RESTORE", "ANALYZE",
}


def _strip_comments_and_strings(sql: str) -> str:
    """Remove SQL comments and string literals so keyword scans don't fire
    on substrings inside e.g. 'this DROP is part of a name'."""
    out = []
    i = 0
    n = len(sql)
    while i < n:
        c = sql[i]
        # Line comment -- ... \n
        if c == "-" and i + 1 < n and sql[i + 1] == "-":
            nl = sql.find("\n", i)
            if nl == -1: break
            i = nl + 1
            continue
        # Block comment /* ... */
        if c == "/" and i + 1 < n and sql[i + 1] == "*":
            end = sql.find("*/", i + 2)
            if end == -1: break
            i = end + 2
            continue
        # String literal '...' (handles doubled '' inside)
        if c == "'":
            i += 1
            while i < n:
                if sql[i] == "'":
                    if i + 1 < n and sql[i + 1] == "'":  # escaped quote
                        i += 2; continue
                    i += 1; break
                i += 1
            out.append(" ")
            continue
        # Backtick-quoted identifier
        if c == "`":
            i += 1
            while i < n and sql[i] != "`":
                i += 1
            i += 1
            out.append(" ")
            continue
        # Double-quoted identifier
        if c == '"':
            i += 1
            while i < n and sql[i] != '"':
                i += 1
            i += 1
            out.append(" ")
            continue
        out.append(c)
        i += 1
    return "".join(out)


def _split_statements(sql_clean: str) -> list[str]:
    """Split on ';' (string literals already stripped). Trim + drop empties."""
    return [s.strip() for s in sql_clean.split(";") if s.strip()]


def _first_keyword(stmt: str) -> str:
    m = re.match(r"\s*([A-Za-z]+)", stmt)
    return m.group(1).upper() if m else ""


def validate_sql(sql: str) -> tuple[bool, str]:
    """Return (ok, reason). Caller must NOT execute if ok is False."""
    if not isinstance(sql, str):
        return False, "sql must be a string"
    raw = sql.strip().rstrip(";").strip()
    if not raw:
        return False, "sql is empty"
    if len(raw.encode("utf-8", "replace")) > MAX_SQL_BYTES:
        return False, f"sql exceeds max length ({MAX_SQL_BYTES} bytes)"

    cleaned = _strip_comments_and_strings(raw)
    statements = _split_statements(cleaned)
    if not statements:
        return False, "no statements parsed (only comments?)"

    # Each statement must start with an allowed keyword
    for stmt in statements:
        kw = _first_keyword(stmt)
        if not kw:
            return False, f"could not parse statement keyword: {stmt[:60]!r}"
        if kw not in _ALLOWED:
            return False, f"statement starts with '{kw}', which is not in the allow-list ({sorted(_ALLOWED)})"

    # And no destructive keyword anywhere (as a whole word, case-insensitive)
    upper_blob = " " + re.sub(r"\s+", " ", cleaned.upper()) + " "
    for kw in _FORBIDDEN_KEYWORDS:
        if re.search(rf"(?<![A-Z_]){re.escape(kw)}(?![A-Z_])", upper_blob):
            return False, f"contains forbidden keyword: {kw}"

    return True, ""


async def resolve_container(install_dir: "Path | str | None", name_contains: str,
                            timeout: float = 10.0) -> str | None:
    """Resolve the running container Name for the compose service whose name
    matches `name_contains`, scoped to this install's compose project.

    Generic across container-naming schemes — udp-starrocks-fe (UDP base),
    sl-starrocks-fe (streaming), <project>-starrocks-fe-1 (compose default) —
    so post-install actions work on every stack, not just the UDP one.
    Returns None if docker is unavailable or no matching container exists.
    """
    if not install_dir:
        return None
    d = Path(install_dir)
    if shutil.which("docker") is None or not d.exists():
        return None
    try:
        proc = await asyncio.create_subprocess_exec(
            "docker", "compose", "ps", "--format", "json", "--all",
            cwd=str(d),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        out_b, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except Exception:
        return None
    text = out_b.decode("utf-8", "replace").strip()
    if not text:
        return None
    rows: list = []
    if text.startswith("["):
        try:
            rows = json.loads(text)
        except json.JSONDecodeError:
            rows = []
    else:
        for ln in text.splitlines():
            ln = ln.strip()
            if not ln:
                continue
            try:
                rows.append(json.loads(ln))
            except json.JSONDecodeError:
                continue
    fallback: str | None = None
    for row in rows:
        if not isinstance(row, dict):
            continue
        svc = row.get("Service") or ""
        nm = row.get("Name") or ""
        if name_contains in svc or name_contains in nm:
            if (row.get("State") or "").lower().startswith("run"):
                return nm or None
            fallback = fallback or (nm or None)
    return fallback


async def run_user_sql(sql: str, install_dir: "Path | str | None" = None,
                       timeout: int = DEFAULT_TIMEOUT_SEC) -> dict[str, Any]:
    ok, why = validate_sql(sql)
    if not ok:
        return {"sql": sql, "error": f"rejected: {why}"}

    if shutil.which("docker") is None:
        return {"sql": sql, "error": "docker CLI not on PATH (this Studio host can't reach docker)"}

    # Resolve THIS install's StarRocks container rather than assuming the UDP
    # container name. Falls back to udp-starrocks-fe for older udp installs
    # where compose-ps resolution isn't available.
    container = await resolve_container(install_dir, "starrocks-fe") or "udp-starrocks-fe"
    cmd = [
        "docker", "exec", "-i", container,
        "mysql", "-h", "127.0.0.1", "-P", "9030", "-u", "root",
        "--batch", "--raw", "-e", sql,
    ]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
    except (FileNotFoundError, NotImplementedError, OSError) as e:
        return {"sql": sql, "error": f"failed to spawn subprocess: {e}"}

    try:
        stdout_b, stderr_b = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        try: proc.kill()
        except ProcessLookupError: pass
        try: await asyncio.wait_for(proc.wait(), timeout=5)
        except asyncio.TimeoutError: pass
        return {"sql": sql, "error": f"query timed out after {timeout}s"}

    out = stdout_b.decode("utf-8", "replace")
    err = stderr_b.decode("utf-8", "replace")

    if proc.returncode != 0:
        raw = err.strip() or out.strip() or f"mysql exited {proc.returncode}"
        if "No such container" in raw:
            raw = (f"StarRocks isn't running for this install (container '{container}' not found). "
                   "Start the stack before running SQL.")
        return {"sql": sql, "error": raw}

    # Skip MySQL warning lines before parsing
    lines = [ln for ln in out.splitlines()
             if ln.strip()
             and not ln.lower().startswith("warning:")
             and not ln.lower().startswith("mysql:")]

    columns: list[str] = []
    rows: list[list[str]] = []
    if lines:
        columns = lines[0].split("\t")
        for ln in lines[1:]:
            rows.append(ln.split("\t"))

    return {
        "sql": sql,
        "columns": columns,
        "rows": rows,
        "row_count": len(rows),
        "stderr": err.strip() if err.strip() else None,
    }
