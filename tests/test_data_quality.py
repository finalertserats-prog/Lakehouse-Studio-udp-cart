"""Unit tests for backend.data_quality.

Hermetic — no docker, no StarRocks. The `run_check` tests monkey-patch
`run_user_sql` to return canned envelopes so we exercise the SQL
template, the result interpretation, and the persistence path without
ever spawning a subprocess.

Tests use UUID-prefixed install ids to avoid colliding with on-disk
state from other suites (the WORK_DIR is shared across the suite).
"""
from __future__ import annotations

import asyncio
import uuid

import pytest

from backend import data_quality as dq_mod


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _new_install_id() -> str:
    return f"inst_{uuid.uuid4().hex[:10]}"


def _base_body(**overrides):
    body = {
        "namespace": "iceberg.demo",
        "table": "orders",
        "kind": "no_nulls",
        "column": "order_id",
        "expected": None,
        "enabled": True,
    }
    body.update(overrides)
    return body


def _cleanup_check(check_id: str) -> None:
    try:
        asyncio.run(dq_mod.delete_check(check_id))
    except KeyError:
        pass


# ---------------------------------------------------------------------------
# 1. no_nulls requires a column
# ---------------------------------------------------------------------------

def test_create_no_nulls_requires_column():
    install_id = _new_install_id()
    body = _base_body(kind="no_nulls", column=None)
    with pytest.raises(ValueError, match="requires a column"):
        asyncio.run(dq_mod.create_check(install_id, body))


# ---------------------------------------------------------------------------
# 2. row_count_min requires an `expected` threshold
# ---------------------------------------------------------------------------

def test_create_row_count_min_requires_expected():
    install_id = _new_install_id()
    body = _base_body(kind="row_count_min", column=None, expected=None)
    with pytest.raises(ValueError, match="requires a numeric 'expected'"):
        asyncio.run(dq_mod.create_check(install_id, body))


# ---------------------------------------------------------------------------
# 3. SQL injection in column field is rejected
# ---------------------------------------------------------------------------

def test_create_rejects_sql_injection_in_column():
    install_id = _new_install_id()
    # Classic SQLi payload: tries to close the identifier and inject DROP.
    body = _base_body(column="order_id; DROP TABLE orders --")
    with pytest.raises(ValueError, match=r"column.*must match"):
        asyncio.run(dq_mod.create_check(install_id, body))

    # Spaces, quotes, parens — all rejected by the strict allowlist.
    for evil in ["o'r", "a b", "x)y", "1=1", "a/b"]:
        body = _base_body(column=evil)
        with pytest.raises(ValueError, match=r"must match"):
            asyncio.run(dq_mod.create_check(install_id, body))


# ---------------------------------------------------------------------------
# 4. run_check for no_nulls builds the expected SQL (mock run_user_sql)
# ---------------------------------------------------------------------------

def test_run_check_no_nulls_builds_expected_sql(monkeypatch):
    install_id = _new_install_id()
    body = _base_body(kind="no_nulls", column="email", namespace="iceberg.users",
                      table="customers")
    check = asyncio.run(dq_mod.create_check(install_id, body))

    captured = {}

    async def fake_run_user_sql(sql, timeout=30):
        captured["sql"] = sql
        return {
            "sql": sql,
            "columns": ["COUNT(*)"],
            "rows": [["0"]],
            "row_count": 1,
            "stderr": None,
        }

    monkeypatch.setattr(dq_mod, "run_user_sql", fake_run_user_sql)

    result = asyncio.run(dq_mod.run_check(check.check_id))

    assert captured["sql"] == (
        "SELECT COUNT(*) FROM iceberg.users.customers WHERE email IS NULL"
    )
    assert result.status == "passed"
    assert result.check_id == check.check_id
    assert result.observed.get("value") == 0.0

    _cleanup_check(check.check_id)


# ---------------------------------------------------------------------------
# 5. run_check persists the DQResult and surfaces it via list_results
# ---------------------------------------------------------------------------

def test_run_check_persists_result_and_returns_it(monkeypatch):
    install_id = _new_install_id()
    body = _base_body(kind="row_count_min", column=None, expected=100,
                      namespace="iceberg.demo", table="events")
    check = asyncio.run(dq_mod.create_check(install_id, body))

    async def fake_run_user_sql(sql, timeout=30):
        # 250 rows observed: >= 100 expected -> passed
        return {
            "sql": sql,
            "columns": ["COUNT(*)"],
            "rows": [["250"]],
            "row_count": 1,
            "stderr": None,
        }

    monkeypatch.setattr(dq_mod, "run_user_sql", fake_run_user_sql)

    result = asyncio.run(dq_mod.run_check(check.check_id))
    assert result.status == "passed"
    assert result.observed.get("value") == 250.0
    assert result.observed.get("expected") == 100.0

    listed = asyncio.run(dq_mod.list_results(install_id))
    assert any(r.result_id == result.result_id for r in listed), \
        "freshly-run result must appear in list_results for the install"

    # Also exercise the failing path so we know interpretation is wired up.
    async def fake_low(sql, timeout=30):
        return {
            "sql": sql,
            "columns": ["COUNT(*)"],
            "rows": [["5"]],
            "row_count": 1,
            "stderr": None,
        }

    monkeypatch.setattr(dq_mod, "run_user_sql", fake_low)
    failing = asyncio.run(dq_mod.run_check(check.check_id))
    assert failing.status == "failed"
    assert "5" in failing.message

    _cleanup_check(check.check_id)


# ---------------------------------------------------------------------------
# 6. list_checks filters by install_id
# ---------------------------------------------------------------------------

def test_list_checks_filters_by_install_id():
    install_a = _new_install_id()
    install_b = _new_install_id()

    body_a = _base_body(kind="positive", column="price", namespace="iceberg.a",
                        table="orders")
    body_b = _base_body(kind="no_dups", column="user_id", namespace="iceberg.b",
                        table="users")

    a = asyncio.run(dq_mod.create_check(install_a, body_a))
    b = asyncio.run(dq_mod.create_check(install_b, body_b))

    list_a = asyncio.run(dq_mod.list_checks(install_a))
    list_b = asyncio.run(dq_mod.list_checks(install_b))

    a_ids = {c.check_id for c in list_a}
    b_ids = {c.check_id for c in list_b}

    assert a.check_id in a_ids
    assert a.check_id not in b_ids
    assert b.check_id in b_ids
    assert b.check_id not in a_ids

    _cleanup_check(a.check_id)
    _cleanup_check(b.check_id)


# ---------------------------------------------------------------------------
# 7. Bonus: SQL builders for the other kinds emit the documented templates
# ---------------------------------------------------------------------------

def test_sql_builders_per_kind():
    # Build a fake DQCheck for each kind and assert the SQL string shape.
    common = dict(install_id="inst_test", namespace="db.s", table="t",
                  enabled=True, created_at=0.0)

    no_nulls = dq_mod.DQCheck(check_id="c1", kind="no_nulls", column="c",
                              expected=None, **common)
    assert dq_mod._build_sql(no_nulls) == \
        "SELECT COUNT(*) FROM db.s.t WHERE c IS NULL"

    no_dups = dq_mod.DQCheck(check_id="c2", kind="no_dups", column="c",
                             expected=None, **common)
    assert dq_mod._build_sql(no_dups) == (
        "SELECT COUNT(*) FROM ("
        "SELECT c, COUNT(*) c FROM db.s.t GROUP BY c HAVING c > 1) t"
    )

    positive = dq_mod.DQCheck(check_id="c3", kind="positive", column="c",
                              expected=None, **common)
    assert dq_mod._build_sql(positive) == \
        "SELECT COUNT(*) FROM db.s.t WHERE c <= 0"

    valid_date = dq_mod.DQCheck(check_id="c4", kind="valid_date", column="c",
                                expected=None, **common)
    assert dq_mod._build_sql(valid_date) == (
        "SELECT COUNT(*) FROM db.s.t "
        "WHERE c IS NULL OR c > CURRENT_TIMESTAMP() OR c < '1900-01-01'"
    )

    row_min = dq_mod.DQCheck(check_id="c5", kind="row_count_min", column=None,
                             expected=10.0, **common)
    assert dq_mod._build_sql(row_min) == "SELECT COUNT(*) FROM db.s.t"

    row_max = dq_mod.DQCheck(check_id="c6", kind="row_count_max", column=None,
                             expected=100.0, **common)
    assert dq_mod._build_sql(row_max) == "SELECT COUNT(*) FROM db.s.t"
