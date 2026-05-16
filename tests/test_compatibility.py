"""Unit tests for backend.compatibility.

These tests are hermetic — no live Docker calls, no network. Any code path
that would shell out to `docker manifest inspect` is either short-circuited
by patching `shutil.which` to return None, or by patching
`asyncio.create_subprocess_exec` to return a stub process.

Fixtures live at:
  stacks/compatibility/udp-local-v0.2.lock.yaml
  stacks/compatibility/udp-local-v0.2.upgrades.yaml
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from backend import compatibility


STACK_ID = "udp-local-v0.2"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _clear_caches():
    """Each test gets a fresh view of the on-disk lock/upgrades files.

    `load_lock` and `load_upgrades` use `lru_cache`, which would leak state
    between tests if a test ever needed to swap fixtures.
    """
    compatibility.load_lock.cache_clear()
    compatibility.load_upgrades.cache_clear()
    yield
    compatibility.load_lock.cache_clear()
    compatibility.load_upgrades.cache_clear()


def _make_fake_proc(returncode: int, stdout: bytes, stderr: bytes) -> MagicMock:
    """Build a stub asyncio subprocess object compatible with the code under test."""
    proc = MagicMock()
    proc.returncode = returncode
    proc.communicate = AsyncMock(return_value=(stdout, stderr))
    proc.wait = AsyncMock(return_value=returncode)
    proc.kill = MagicMock()
    return proc


def _live_catalog_components() -> list[dict]:
    """A catalog snapshot that matches the lock exactly — drift-free."""
    return [
        {"id": "minio",         "image": "minio/minio:RELEASE.2025-04-22T22-12-26Z"},
        {"id": "minio-client",  "image": "minio/mc:RELEASE.2025-04-16T18-13-26Z"},
        {"id": "iceberg-rest",  "image": "tabulario/iceberg-rest:1.6.0"},
        {"id": "spark",         "image": "tabulario/spark-iceberg:3.5.5_1.8.1"},
        {"id": "starrocks-fe",  "image": "starrocks/fe-ubuntu:3.3.12"},
        {"id": "starrocks-be",  "image": "starrocks/be-ubuntu:3.3.12"},
    ]


# ---------------------------------------------------------------------------
# 1. load_lock returns a dict with version_id
# ---------------------------------------------------------------------------

def test_load_lock_returns_dict_with_version_id():
    lock = compatibility.load_lock(STACK_ID)
    assert isinstance(lock, dict)
    assert "version_id" in lock
    assert lock["version_id"] == "0.2.0"


# ---------------------------------------------------------------------------
# 2. load_lock returns None for unknown stack
# ---------------------------------------------------------------------------

def test_load_lock_returns_none_for_unknown_stack():
    assert compatibility.load_lock("nonexistent") is None


# ---------------------------------------------------------------------------
# 3. list_locks includes the certified stack
# ---------------------------------------------------------------------------

def test_list_locks_includes_udp_local_v02():
    locks = compatibility.list_locks()
    assert STACK_ID in locks


# ---------------------------------------------------------------------------
# 4. validate_against_catalog returns [] when catalog matches lock
# ---------------------------------------------------------------------------

def test_validate_against_catalog_clean_when_aligned():
    problems = compatibility.validate_against_catalog(STACK_ID, _live_catalog_components())
    assert problems == []


# ---------------------------------------------------------------------------
# 5. _safe_image_ref accepts well-formed refs
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(("image", "tag"), [
    ("minio/minio", "RELEASE.2025-04-22T22-12-26Z"),
    ("starrocks/fe-ubuntu", "3.3.13"),
])
def test_safe_image_ref_accepts_valid_refs(image, tag):
    assert compatibility._safe_image_ref(image, tag) is True


# ---------------------------------------------------------------------------
# 6. _safe_image_ref rejects malformed / dangerous refs
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(("image", "tag"), [
    ("--rm", "1.0"),
    ("img", "../../etc/passwd"),
    ("img; rm -rf /", "1.0"),
    ("", "1.0"),
    ("img", ""),
])
def test_safe_image_ref_rejects_bad_refs(image, tag):
    assert compatibility._safe_image_ref(image, tag) is False


# ---------------------------------------------------------------------------
# 7. load_upgrades returns a dict with candidates list
# ---------------------------------------------------------------------------

def test_load_upgrades_returns_dict_with_candidates():
    upgrades = compatibility.load_upgrades(STACK_ID)
    assert isinstance(upgrades, dict)
    assert "candidates" in upgrades
    assert isinstance(upgrades["candidates"], list)
    assert len(upgrades["candidates"]) >= 1


# ---------------------------------------------------------------------------
# 8. list_upgrade_candidates surfaces a starrocks-fe candidate that differs
#    from the currently locked tag.
# ---------------------------------------------------------------------------

def test_list_upgrade_candidates_includes_starrocks_fe_bump():
    candidates = compatibility.list_upgrade_candidates(STACK_ID)
    fe_entries = [c for c in candidates if c["component_id"] == "starrocks-fe"]
    assert fe_entries, "expected at least one starrocks-fe upgrade candidate"

    bumps = [c for c in fe_entries if c["candidate_tag"] != c["current_tag"]]
    assert bumps, "expected at least one starrocks-fe candidate whose tag differs from current"
    # And verify the current_tag was correctly hydrated from the lock.
    assert bumps[0]["current_tag"] == "3.3.12"


# ---------------------------------------------------------------------------
# 9. simulate_upgrade flags the known-incompatible spark tag as fail
#    even though the registry would say it exists.
# ---------------------------------------------------------------------------

def test_simulate_upgrade_flags_known_incompatible_spark_tag():
    # The registry path WILL be hit because shutil.which("docker") is truthy
    # in many dev envs. Patch both shutil.which (force-truthy) AND
    # asyncio.create_subprocess_exec so no real docker call escapes.
    fake_proc = _make_fake_proc(
        returncode=0,
        stdout=b'{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.v2+json"}',
        stderr=b"",
    )
    with patch.object(compatibility.shutil, "which", return_value="/usr/bin/docker"), \
         patch.object(compatibility.asyncio, "create_subprocess_exec",
                      AsyncMock(return_value=fake_proc)) as mocked_exec:
        result = asyncio.run(
            compatibility.simulate_upgrade(STACK_ID, {"spark": "3.5.1_1.5.2"})
        )

    assert result["verdict"] == "fail"
    assert result["incompatible_hits"], "expected a known-incompatible hit for spark:3.5.1_1.5.2"
    matched = [hit["matched"] for hit in result["incompatible_hits"]]
    assert "spark:3.5.1_1.5.2" in matched
    # Sanity: we mocked create_subprocess_exec but never let it shell out.
    # It may or may not have been called depending on which branches ran —
    # the important thing is that the real one was patched.
    assert isinstance(mocked_exec, AsyncMock)


# ---------------------------------------------------------------------------
# 10. simulate_upgrade on a benign starrocks-fe bump returns pass or unknown,
#     with no incompatible hits. Registry call mocked to "available".
# ---------------------------------------------------------------------------

def test_simulate_upgrade_starrocks_fe_minor_bump_is_not_incompatible():
    fake_proc = _make_fake_proc(
        returncode=0,
        stdout=b'{"schemaVersion":2}',
        stderr=b"",
    )
    with patch.object(compatibility.shutil, "which", return_value="/usr/bin/docker"), \
         patch.object(compatibility.asyncio, "create_subprocess_exec",
                      AsyncMock(return_value=fake_proc)):
        result = asyncio.run(
            compatibility.simulate_upgrade(STACK_ID, {"starrocks-fe": "3.3.13"})
        )

    assert result["incompatible_hits"] == []
    assert result["verdict"] in ("unknown", "pass")


# ---------------------------------------------------------------------------
# 11. simulate_upgrade rejects an unknown component_id with an error
# ---------------------------------------------------------------------------

def test_simulate_upgrade_rejects_unknown_component():
    with patch.object(compatibility.shutil, "which", return_value=None):
        result = asyncio.run(
            compatibility.simulate_upgrade(STACK_ID, {"unknown-component": "1.0"})
        )

    assert result["verdict"] == "fail"
    assert "error" in result
    assert "unknown-component" in result["error"]


# ---------------------------------------------------------------------------
# 12. lock_summary includes an evidence array with at least one entry
# ---------------------------------------------------------------------------

def test_lock_summary_includes_evidence_entries():
    summary = compatibility.lock_summary(STACK_ID)
    assert summary is not None
    assert "evidence" in summary
    assert isinstance(summary["evidence"], list)
    assert len(summary["evidence"]) >= 1
    # Sanity-check the first evidence entry shape.
    first = summary["evidence"][0]
    assert "id" in first
    assert "result" in first


# ---------------------------------------------------------------------------
# 13. lock_summary reports components_pinned == 6
# ---------------------------------------------------------------------------

def test_lock_summary_components_pinned_is_six():
    summary = compatibility.lock_summary(STACK_ID)
    assert summary is not None
    assert summary["components_pinned"] == 6


# ---------------------------------------------------------------------------
# 14. precheck_image_availability returns ok=False when docker is missing
# ---------------------------------------------------------------------------

def test_precheck_image_availability_reports_missing_docker():
    with patch.object(compatibility.shutil, "which", return_value=None):
        result = asyncio.run(compatibility.precheck_image_availability(STACK_ID))

    assert result["ok"] is False
    assert "docker" in result.get("error", "").lower()
    assert result["checks"] == []


# ---------------------------------------------------------------------------
# 15. validate_against_catalog flags a tag drift between catalog and lock
# ---------------------------------------------------------------------------

def test_validate_against_catalog_detects_tag_drift():
    drifted = [
        # Wrong tag for starrocks-fe — catalog has 3.3.99 but lock has 3.3.12.
        {"id": "starrocks-fe", "image": "starrocks/fe-ubuntu:3.3.99"},
    ]
    problems = compatibility.validate_against_catalog(STACK_ID, drifted)
    assert problems, "expected at least one drift problem reported"
    # Confirm the drift message identifies the component and both tags.
    joined = " | ".join(problems)
    assert "starrocks-fe" in joined
    assert "3.3.99" in joined
    assert "3.3.12" in joined


# ---------------------------------------------------------------------------
# Bonus sanity: load_upgrades returns None for an unknown stack
# (Not in the required 15, but cheap insurance against silent regressions.)
# ---------------------------------------------------------------------------

def test_load_upgrades_returns_none_for_unknown_stack():
    assert compatibility.load_upgrades("nonexistent") is None
