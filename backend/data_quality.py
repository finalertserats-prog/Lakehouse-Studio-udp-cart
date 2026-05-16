"""Data Quality checks for the Lakehouse Studio.

Lets operators register lightweight assertions against tables in the
deployed Iceberg/StarRocks stack (e.g. "column X must be non-null",
"row count must be >= N") and execute them on demand. Pure read-only:
every check translates to a SELECT and is routed through the existing
`run_user_sql` sandbox in `sql_editor`, which only permits
SELECT/SHOW/DESCRIBE/EXPLAIN/WITH.

Persistence: WORK_DIR/dq_checks.json + WORK_DIR/dq_results.json, both
using the same debounced atomic-write pattern as state.py /
data_sources.py so we don't stall on Windows AV/OneDrive.

SQL-injection guard: namespace, table, and column identifiers are
validated against the same `_IDENT_RE` (`^[A-Za-z0-9_.\\-]{1,128}$`)
that `table_explorer._validate_ident` uses. Anything else is rejected
at the boundary so the strict allowlist regex is the moat — that lets
us safely interpolate identifiers into SQL strings (the mysql-cli
docker-exec path doesn't support parameter binding cleanly).
"""
from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field

from .config import WORK_DIR
from .sql_editor import run_user_sql


log = logging.getLogger("lhs.data_quality")


# ---------- Identifier guard (mirrors table_explorer._IDENT_RE exactly) ----------

_IDENT_RE = re.compile(r"^[A-Za-z0-9_.\-]{1,128}$")


def _validate_ident(value: Optional[str], field: str) -> str:
    """Reject namespace / table / column identifiers that don't match the
    sane allowlist. Catches SQL injection (quotes, semicolons, parens, spaces),
    path traversal (..), and absurd lengths before they hit the SQL template.
    """
    if value is None or not isinstance(value, str) or not _IDENT_RE.match(value):
        raise ValueError(
            f"{field} {value!r}: must match [A-Za-z0-9_.-]{{1,128}}"
        )
    return value


# ---------- Models ----------

CheckKind = Literal[
    "no_nulls",
    "no_dups",
    "positive",
    "valid_date",
    "row_count_min",
    "row_count_max",
]

ResultStatus = Literal["passed", "failed", "error"]

_KINDS_REQUIRING_COLUMN = frozenset({"no_nulls", "no_dups", "positive", "valid_date"})
_KINDS_REQUIRING_EXPECTED = frozenset({"row_count_min", "row_count_max"})


class DQCheck(BaseModel):
    check_id: str
    install_id: str
    namespace: str
    table: str
    kind: CheckKind
    column: Optional[str] = None
    expected: Optional[float] = None
    enabled: bool = True
    created_at: float


class DQResult(BaseModel):
    result_id: str
    check_id: str
    ran_at: float
    status: ResultStatus
    message: str
    observed: dict[str, Any] = Field(default_factory=dict)


# ---------- Persistence: debounced atomic write (mirrors data_sources.py) ----------

_DQ_CHECKS_FILE = WORK_DIR / "dq_checks.json"
_DQ_RESULTS_FILE = WORK_DIR / "dq_results.json"

_DQ_LOCK = threading.RLock()
_DQ_CHECKS: dict[str, dict[str, Any]] = {}
_DQ_RESULTS: dict[str, dict[str, Any]] = {}
_DQ_CHECKS_DIRTY = False
_DQ_RESULTS_DIRTY = False
_DQ_CHECKS_FLUSH_TIMER: Optional[threading.Timer] = None
_DQ_RESULTS_FLUSH_TIMER: Optional[threading.Timer] = None
_DQ_WRITE_DEBOUNCE_SEC = 0.25


def _atomic_write(path: Path, data: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(data, encoding="utf-8")
    for _ in range(5):
        try:
            os.replace(tmp, path)
            return
        except PermissionError:
            time.sleep(0.1)
    # Last-ditch: leave tmp so data isn't lost.


def _write_checks_now_locked() -> None:
    global _DQ_CHECKS_DIRTY
    _atomic_write(_DQ_CHECKS_FILE, json.dumps(_DQ_CHECKS, indent=2))
    _DQ_CHECKS_DIRTY = False


def _write_results_now_locked() -> None:
    global _DQ_RESULTS_DIRTY
    _atomic_write(_DQ_RESULTS_FILE, json.dumps(_DQ_RESULTS, indent=2))
    _DQ_RESULTS_DIRTY = False


def _flush_checks_from_timer() -> None:
    global _DQ_CHECKS_FLUSH_TIMER
    with _DQ_LOCK:
        _DQ_CHECKS_FLUSH_TIMER = None
        if _DQ_CHECKS_DIRTY:
            try:
                _write_checks_now_locked()
            except Exception:
                log.exception("dq_checks flush failed")


def _flush_results_from_timer() -> None:
    global _DQ_RESULTS_FLUSH_TIMER
    with _DQ_LOCK:
        _DQ_RESULTS_FLUSH_TIMER = None
        if _DQ_RESULTS_DIRTY:
            try:
                _write_results_now_locked()
            except Exception:
                log.exception("dq_results flush failed")


def _persist_checks_locked(*, force: bool = False) -> None:
    global _DQ_CHECKS_DIRTY, _DQ_CHECKS_FLUSH_TIMER
    _DQ_CHECKS_DIRTY = True
    if force:
        if _DQ_CHECKS_FLUSH_TIMER is not None:
            _DQ_CHECKS_FLUSH_TIMER.cancel()
            _DQ_CHECKS_FLUSH_TIMER = None
        _write_checks_now_locked()
        return
    if _DQ_CHECKS_FLUSH_TIMER is None:
        _DQ_CHECKS_FLUSH_TIMER = threading.Timer(
            _DQ_WRITE_DEBOUNCE_SEC, _flush_checks_from_timer
        )
        _DQ_CHECKS_FLUSH_TIMER.daemon = True
        _DQ_CHECKS_FLUSH_TIMER.start()


def _persist_results_locked(*, force: bool = False) -> None:
    global _DQ_RESULTS_DIRTY, _DQ_RESULTS_FLUSH_TIMER
    _DQ_RESULTS_DIRTY = True
    if force:
        if _DQ_RESULTS_FLUSH_TIMER is not None:
            _DQ_RESULTS_FLUSH_TIMER.cancel()
            _DQ_RESULTS_FLUSH_TIMER = None
        _write_results_now_locked()
        return
    if _DQ_RESULTS_FLUSH_TIMER is None:
        _DQ_RESULTS_FLUSH_TIMER = threading.Timer(
            _DQ_WRITE_DEBOUNCE_SEC, _flush_results_from_timer
        )
        _DQ_RESULTS_FLUSH_TIMER.daemon = True
        _DQ_RESULTS_FLUSH_TIMER.start()


def _load_checks() -> None:
    if not _DQ_CHECKS_FILE.exists():
        return
    try:
        raw = json.loads(_DQ_CHECKS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return
    if not isinstance(raw, dict):
        return
    for cid, data in raw.items():
        if not isinstance(data, dict):
            continue
        try:
            DQCheck(**data)  # validate shape; drop malformed rows
        except Exception:
            continue
        _DQ_CHECKS[cid] = data


def _load_results() -> None:
    if not _DQ_RESULTS_FILE.exists():
        return
    try:
        raw = json.loads(_DQ_RESULTS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return
    if not isinstance(raw, dict):
        return
    for rid, data in raw.items():
        if not isinstance(data, dict):
            continue
        try:
            DQResult(**data)
        except Exception:
            continue
        _DQ_RESULTS[rid] = data


_load_checks()
_load_results()


# ---------- Validation ----------

def _validate_create_body(body: dict[str, Any]) -> dict[str, Any]:
    """Validate the create-request shape + the kind/column/expected matrix.

    Raises ValueError on any rejection. Returns a normalized dict the caller
    can feed into DQCheck(**...).
    """
    if not isinstance(body, dict):
        raise ValueError("body must be an object")

    namespace = body.get("namespace")
    table = body.get("table")
    kind = body.get("kind")
    column = body.get("column")
    expected = body.get("expected")
    enabled = body.get("enabled", True)

    if kind not in (
        "no_nulls", "no_dups", "positive", "valid_date",
        "row_count_min", "row_count_max",
    ):
        raise ValueError(f"kind {kind!r}: must be one of "
                         "no_nulls|no_dups|positive|valid_date|"
                         "row_count_min|row_count_max")

    _validate_ident(namespace, "namespace")
    _validate_ident(table, "table")

    if kind in _KINDS_REQUIRING_COLUMN:
        if column is None or column == "":
            raise ValueError(f"kind {kind!r} requires a column")
        _validate_ident(column, "column")
    else:
        # column not required, but if supplied still must pass the guard
        if column is not None and column != "":
            _validate_ident(column, "column")
        else:
            column = None

    if kind in _KINDS_REQUIRING_EXPECTED:
        if expected is None:
            raise ValueError(f"kind {kind!r} requires a numeric 'expected' threshold")
        try:
            expected = float(expected)
        except (TypeError, ValueError) as e:
            raise ValueError(
                f"kind {kind!r}: 'expected' must be numeric ({e})"
            ) from e
    else:
        # For non row-count kinds, 'expected' is ignored but normalized to None
        expected = None

    if not isinstance(enabled, bool):
        raise ValueError("enabled must be a boolean")

    return {
        "namespace": namespace,
        "table": table,
        "kind": kind,
        "column": column,
        "expected": expected,
        "enabled": enabled,
    }


# ---------- SQL builders (parameterized via strict allowlist regex) ----------

def _qualified(namespace: str, table: str) -> str:
    return f"{namespace}.{table}"


def _build_sql(check: DQCheck) -> str:
    """Build the read-only SELECT for a check. All identifiers have already
    been validated against `_IDENT_RE` at create time, so direct
    interpolation is safe in this controlled context."""
    qt = _qualified(check.namespace, check.table)
    kind = check.kind
    col = check.column

    if kind == "no_nulls":
        return f"SELECT COUNT(*) FROM {qt} WHERE {col} IS NULL"
    if kind == "no_dups":
        return (
            f"SELECT COUNT(*) FROM ("
            f"SELECT {col}, COUNT(*) c FROM {qt} GROUP BY {col} HAVING c > 1"
            f") t"
        )
    if kind == "positive":
        return f"SELECT COUNT(*) FROM {qt} WHERE {col} <= 0"
    if kind == "valid_date":
        return (
            f"SELECT COUNT(*) FROM {qt} "
            f"WHERE {col} IS NULL "
            f"OR {col} > CURRENT_TIMESTAMP() "
            f"OR {col} < '1900-01-01'"
        )
    if kind in ("row_count_min", "row_count_max"):
        return f"SELECT COUNT(*) FROM {qt}"
    raise ValueError(f"unsupported kind {kind!r}")  # defensive — validate_body caught it


def _interpret(check: DQCheck, observed_value: Optional[float]) -> tuple[ResultStatus, str]:
    """Decide passed/failed/error given the observed COUNT(*) and the check's
    intent. Returns (status, message)."""
    if observed_value is None:
        return "error", "query returned no rows"

    kind = check.kind
    if kind in ("no_nulls", "no_dups", "positive", "valid_date"):
        if observed_value == 0:
            return "passed", f"observed {int(observed_value)} violations (expected 0)"
        return "failed", f"observed {int(observed_value)} violations (expected 0)"

    if kind == "row_count_min":
        threshold = check.expected or 0
        if observed_value >= threshold:
            return "passed", (
                f"observed {int(observed_value)} rows (>= {threshold:g})"
            )
        return "failed", (
            f"observed {int(observed_value)} rows (< {threshold:g})"
        )

    if kind == "row_count_max":
        threshold = check.expected or 0
        if observed_value <= threshold:
            return "passed", (
                f"observed {int(observed_value)} rows (<= {threshold:g})"
            )
        return "failed", (
            f"observed {int(observed_value)} rows (> {threshold:g})"
        )

    return "error", f"unsupported kind {kind!r}"


# ---------- Public CRUD ----------

async def create_check(install_id: str, body: dict[str, Any]) -> DQCheck:
    """Validate + persist a new DQCheck for the given install."""
    _validate_ident(install_id, "install_id")
    normalized = _validate_create_body(body)

    now = time.time()
    check_id = uuid.uuid4().hex
    check = DQCheck(
        check_id=check_id,
        install_id=install_id,
        namespace=normalized["namespace"],
        table=normalized["table"],
        kind=normalized["kind"],
        column=normalized["column"],
        expected=normalized["expected"],
        enabled=normalized["enabled"],
        created_at=now,
    )
    with _DQ_LOCK:
        _DQ_CHECKS[check_id] = check.model_dump()
        _persist_checks_locked(force=True)
    return check


async def list_checks(install_id: str) -> list[DQCheck]:
    with _DQ_LOCK:
        out: list[DQCheck] = []
        for data in _DQ_CHECKS.values():
            if data.get("install_id") != install_id:
                continue
            try:
                out.append(DQCheck(**data))
            except Exception:
                continue
        return sorted(out, key=lambda c: c.created_at, reverse=True)


async def get_check(check_id: str) -> Optional[DQCheck]:
    with _DQ_LOCK:
        data = _DQ_CHECKS.get(check_id)
        if not data:
            return None
        try:
            return DQCheck(**data)
        except Exception:
            return None


async def delete_check(check_id: str) -> None:
    """Remove a check. Raises KeyError if it does not exist."""
    with _DQ_LOCK:
        if check_id not in _DQ_CHECKS:
            raise KeyError(check_id)
        _DQ_CHECKS.pop(check_id, None)
        _persist_checks_locked(force=True)


# ---------- Run ----------

def _parse_count(raw_result: dict[str, Any]) -> Optional[float]:
    """Pull the single COUNT(*) cell out of a run_user_sql response."""
    rows = raw_result.get("rows") or []
    if not rows or not rows[0]:
        return None
    try:
        return float(rows[0][0])
    except (TypeError, ValueError):
        return None


async def run_check(check_id: str) -> DQResult:
    check = await get_check(check_id)
    if check is None:
        raise KeyError(check_id)

    sql = _build_sql(check)
    ran_at = time.time()
    result_id = uuid.uuid4().hex

    try:
        raw = await run_user_sql(sql)
    except Exception as e:
        result = DQResult(
            result_id=result_id,
            check_id=check_id,
            ran_at=ran_at,
            status="error",
            message=f"sql execution raised: {type(e).__name__}: {e}",
            observed={"sql": sql},
        )
        _persist_result(result)
        return result

    if raw.get("error"):
        result = DQResult(
            result_id=result_id,
            check_id=check_id,
            ran_at=ran_at,
            status="error",
            message=str(raw.get("error")),
            observed={"sql": sql, "raw_error": raw.get("error")},
        )
        _persist_result(result)
        return result

    observed_value = _parse_count(raw)
    status, message = _interpret(check, observed_value)
    result = DQResult(
        result_id=result_id,
        check_id=check_id,
        ran_at=ran_at,
        status=status,
        message=message,
        observed={
            "sql": sql,
            "value": observed_value,
            "expected": check.expected,
            "columns": raw.get("columns") or [],
            "row_count": raw.get("row_count", 0),
        },
    )
    _persist_result(result)
    return result


def _persist_result(result: DQResult) -> None:
    with _DQ_LOCK:
        _DQ_RESULTS[result.result_id] = result.model_dump()
        _persist_results_locked(force=True)


async def list_results(
    install_id: str,
    check_id: Optional[str] = None,
    limit: int = 20,
) -> list[DQResult]:
    """Return recent results for an install, optionally filtered by check_id.

    A result is "for an install" when its check_id resolves to a check that
    belongs to that install. Results from deleted checks are excluded.
    """
    if limit < 1:
        limit = 1
    if limit > 1000:
        limit = 1000

    with _DQ_LOCK:
        check_ids_for_install = {
            cid for cid, data in _DQ_CHECKS.items()
            if data.get("install_id") == install_id
        }
        out: list[DQResult] = []
        for data in _DQ_RESULTS.values():
            rcid = data.get("check_id")
            if rcid not in check_ids_for_install:
                continue
            if check_id is not None and rcid != check_id:
                continue
            try:
                out.append(DQResult(**data))
            except Exception:
                continue
        out.sort(key=lambda r: r.ran_at, reverse=True)
        return out[:limit]
