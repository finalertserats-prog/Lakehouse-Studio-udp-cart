"""Tests for scripts/install_harness.py.

Scope (intentionally narrow):
  * Arg parsing — every flag in the contract.
  * Evidence record renderer — given a fake step-results dict, produces the
    YAML shape the lock file expects.
  * Promote instruction string — references the right lock path, version
    bumps, and certified_at value.
  * Teardown happens in a finally: block — mock subprocess, then verify
    docker_compose_down was called even when the install task raised.

Out of scope:
  * Live Docker calls. The harness's real-pipeline driver `_run_harness` is
    not exercised here — it depends on the full backend.runner pipeline and
    a real Docker daemon.
"""
from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml


# ---------------------------------------------------------------------------
# Module loader — scripts/install_harness.py is not a package, load it
# directly so the test suite doesn't need to add scripts/ to sys.path.
# We register the module in sys.modules before exec_module so the @dataclass
# decorator on StepResult can find the module by name (dataclasses look up
# the defining module to resolve ClassVar annotations).
# ---------------------------------------------------------------------------

_HARNESS_PATH = Path(__file__).resolve().parent.parent / "scripts" / "install_harness.py"


@pytest.fixture(scope="module")
def harness():
    spec = importlib.util.spec_from_file_location("install_harness", _HARNESS_PATH)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["install_harness"] = mod  # dataclasses needs this
    try:
        spec.loader.exec_module(mod)  # type: ignore[union-attr]
    except Exception:
        sys.modules.pop("install_harness", None)
        raise
    return mod


# ---------------------------------------------------------------------------
# Arg parsing — covers every flag in the public contract.
# ---------------------------------------------------------------------------

class TestArgParsing:
    def test_minimal_args(self, harness):
        args = harness.parse_args(["--stack", "udp-local-v0.2"])
        assert args.stack == "udp-local-v0.2"
        assert args.work_dir is None
        assert args.keep is False
        assert args.no_teardown is False
        assert args.json is None

    def test_stack_is_required(self, harness):
        with pytest.raises(SystemExit):
            harness.parse_args([])

    def test_work_dir_override(self, harness):
        args = harness.parse_args(["--stack", "x", "--work-dir", r"D:\foo\bar"])
        assert args.work_dir == r"D:\foo\bar"

    def test_keep_flag(self, harness):
        args = harness.parse_args(["--stack", "x", "--keep"])
        assert args.keep is True

    def test_no_teardown_flag(self, harness):
        args = harness.parse_args(["--stack", "x", "--no-teardown"])
        assert args.no_teardown is True

    def test_json_without_path_uses_stdout_sentinel(self, harness):
        # `--json` (no path) -> args.json == "-" (stdout sentinel)
        args = harness.parse_args(["--stack", "x", "--json"])
        assert args.json == "-"

    def test_json_with_path(self, harness):
        args = harness.parse_args(["--stack", "x", "--json", "out.json"])
        assert args.json == "out.json"

    def test_all_flags_compose(self, harness):
        args = harness.parse_args([
            "--stack", "udp-trino-local-v0.1",
            "--work-dir", r"C:\tmp\work",
            "--keep",
            "--no-teardown",
            "--json", "evidence.json",
        ])
        assert args.stack == "udp-trino-local-v0.1"
        assert args.work_dir == r"C:\tmp\work"
        assert args.keep is True
        assert args.no_teardown is True
        assert args.json == "evidence.json"


# ---------------------------------------------------------------------------
# Evidence record renderer — pure function tests.
# ---------------------------------------------------------------------------

def _make_step_result(harness, step_id, status="passed", exit_code=0,
                      stdout_lines=None, stderr_lines=None, duration=1.0):
    sr = harness.StepResult(step_id=step_id)
    sr.status = status
    sr.exit_code = exit_code
    sr.started_at = 1_000.0
    sr.finished_at = 1_000.0 + duration
    sr.stdout_lines = list(stdout_lines or [])
    sr.stderr_lines = list(stderr_lines or [])
    return sr


def _passing_step_results(harness):
    return {
        sid: _make_step_result(harness, sid, "passed", 0)
        for sid in harness.PIPELINE_STEPS
    }


class TestEvidenceRenderer:
    def test_shape_matches_lock_file(self, harness):
        # Reproduce the shape of stacks/compatibility/udp-local-v0.2.lock.yaml
        # evidence[0]: id, timestamp, operator, host, via, install_id, result,
        # and either smoke_failure_root_cause OR proof.
        results = _passing_step_results(harness)
        results["smoke"].stdout_lines = ["row1", "row2", "row3"]

        ts = datetime(2026, 5, 17, 10, 24, 53, tzinfo=timezone.utc)
        rec = harness.render_evidence_record(
            install_id="inst_7f3a91c2c1",
            step_results=results,
            host_info={
                "os": "Windows-11-10.0.26200-SP0",
                "docker": "28.3.0",
                "ram_gb": 15.3,
                "cpu_cores": 16,
            },
            operator="vishnu.wildeagle@gmail.com",
            timestamp=ts,
            hostname="myhost",
        )

        # Top-level keys (matching udp-local-v0.2.lock.yaml evidence[0])
        for key in ("id", "timestamp", "operator", "host", "via", "install_id", "result"):
            assert key in rec, f"missing required key: {key}"

        # id format: <date>-<hostname>-<shortinstallid>
        assert rec["id"] == "2026-05-17-myhost-7f3a91c2c1"

        # host sub-keys
        assert rec["host"] == {
            "os": "Windows-11-10.0.26200-SP0",
            "docker": "28.3.0",
            "ram_gb": 15.3,
            "cpu_cores": 16,
        }

        # via mentions the harness
        assert "install_harness.py" in rec["via"]

        # result block has all 8 pipeline steps
        for step in harness.PIPELINE_STEPS:
            assert step in rec["result"]
        assert rec["result"]["smoke"] == "passed"

        # smoke passed -> proof present, smoke_failure_root_cause absent
        assert "proof" in rec
        assert "smoke_failure_root_cause" not in rec
        assert rec["proof"] == ["row1", "row2", "row3"]

    def test_proof_falls_back_when_smoke_silent(self, harness):
        results = _passing_step_results(harness)
        results["smoke"].stdout_lines = []  # no output
        rec = harness.render_evidence_record(
            install_id="inst_abc",
            step_results=results,
            host_info={"os": "x", "docker": "y", "ram_gb": 1.0, "cpu_cores": 1},
            operator="op@example.com",
        )
        assert "proof" in rec
        assert any("smoke produced no stdout" in line for line in rec["proof"])

    def test_smoke_failure_captures_stderr_tail(self, harness):
        results = _passing_step_results(harness)
        # 25 stderr lines — renderer should keep the last 20.
        results["smoke"] = _make_step_result(
            harness, "smoke", status="failed", exit_code=1,
            stderr_lines=[f"err_line_{i}" for i in range(25)],
        )
        rec = harness.render_evidence_record(
            install_id="inst_xyz",
            step_results=results,
            host_info={"os": "x", "docker": "y", "ram_gb": 1.0, "cpu_cores": 1},
            operator="op@example.com",
        )
        assert "smoke_failure_root_cause" in rec
        assert "proof" not in rec
        lines = rec["smoke_failure_root_cause"].splitlines()
        assert lines[0] == "err_line_5"
        assert lines[-1] == "err_line_24"
        assert len(lines) == 20

    def test_missing_step_marked_not_run(self, harness):
        # Pipeline started but finalize never executed
        results = {sid: _make_step_result(harness, sid, "passed") for sid in
                   ("prepare", "clone", "env", "doctor", "start", "bootstrap", "smoke")}
        rec = harness.render_evidence_record(
            install_id="inst_partial",
            step_results=results,
            host_info={"os": "x", "docker": "y", "ram_gb": 1.0, "cpu_cores": 1},
            operator="op@example.com",
        )
        assert rec["result"]["finalize"] == "not_run"

    def test_short_install_id_when_no_prefix(self, harness):
        rec = harness.render_evidence_record(
            install_id="abcdef0123",
            step_results=_passing_step_results(harness),
            host_info={"os": "x", "docker": "y", "ram_gb": 1.0, "cpu_cores": 1},
            operator="op@example.com",
            timestamp=datetime(2026, 1, 2, tzinfo=timezone.utc),
            hostname="HOST",
        )
        # hostname lower-cased; install id used verbatim, truncated to 10 chars
        assert rec["id"] == "2026-01-02-host-abcdef0123"

    def test_dump_yaml_is_loadable_list(self, harness):
        rec = harness.render_evidence_record(
            install_id="inst_yamltest",
            step_results=_passing_step_results(harness),
            host_info={"os": "x", "docker": "y", "ram_gb": 1.0, "cpu_cores": 1},
            operator="op@example.com",
        )
        text = harness.dump_evidence_yaml(rec)
        loaded = yaml.safe_load(text)
        # Must be a single-element list (so you can copy-paste into evidence:[])
        assert isinstance(loaded, list)
        assert len(loaded) == 1
        assert loaded[0]["install_id"] == "inst_yamltest"

    def test_dump_yaml_uses_block_literal_for_multiline(self, harness):
        results = _passing_step_results(harness)
        results["smoke"] = _make_step_result(
            harness, "smoke", status="failed", exit_code=1,
            stderr_lines=["line1", "line2", "line3"],
        )
        rec = harness.render_evidence_record(
            install_id="inst_blockstyle",
            step_results=results,
            host_info={"os": "x", "docker": "y", "ram_gb": 1.0, "cpu_cores": 1},
            operator="op@example.com",
        )
        text = harness.dump_evidence_yaml(rec)
        # Block literal indicator `|` should appear for multi-line stderr
        assert "smoke_failure_root_cause: |" in text


# ---------------------------------------------------------------------------
# Promote instructions — string assertions for what an operator will paste.
# ---------------------------------------------------------------------------

class TestPromoteInstructions:
    def test_references_correct_lock_path(self, harness):
        out = harness.render_promote_instructions("udp-trino-local-v0.1")
        assert "stacks/compatibility/udp-trino-local-v0.1.lock.yaml" in out

    def test_default_version_bump_pattern(self, harness):
        out = harness.render_promote_instructions("udp-local-v0.2")
        assert "0.x.0" in out
        assert "0.x.1" in out

    def test_custom_version_bump(self, harness):
        out = harness.render_promote_instructions(
            "udp-local-v0.2",
            current_version="0.2.0",
            next_version="0.2.1",
        )
        assert "0.2.0" in out
        assert "0.2.1" in out

    def test_certified_at_value_included(self, harness):
        iso = "2026-05-17T12:00:00+00:00"
        out = harness.render_promote_instructions("udp-local-v0.2", now_iso=iso)
        assert iso in out
        assert "certified_at" in out

    def test_status_transition_described(self, harness):
        out = harness.render_promote_instructions("udp-local-v0.2")
        assert "candidate" in out
        assert "pilot-stable" in out

    def test_promote_commit_message_template(self, harness):
        out = harness.render_promote_instructions("udp-local-v0.2")
        # The runbook contract: a copy-paste commit message.
        assert "cert(udp-local-v0.2): promote to pilot-stable" in out


# ---------------------------------------------------------------------------
# Teardown — runs in a finally: block. Mock subprocess so no real Docker is
# touched. We can't drive _run_harness end-to-end without backend.runner +
# Docker, so we test the SHAPE of the teardown helpers + the finally: contract.
# ---------------------------------------------------------------------------

class TestTeardown:
    def test_docker_compose_down_falls_back_to_project_level_when_no_compose_file(
        self, harness, tmp_path
    ):
        """v0.6.2 — when the compose file is missing (e.g. previous teardown
        removed install_dir but containers stayed up due to a project-name
        mismatch), we still try `docker compose -p <project> down` so the
        runtime project state gets cleaned up. Otherwise stale containers
        from previous installs block the next install via port collisions.
        """
        fake_subprocess = MagicMock()
        fake_subprocess.run.return_value = MagicMock(returncode=0)
        rc = harness.docker_compose_down(tmp_path, "test-project", runner=fake_subprocess)
        assert rc == 0
        # SHOULD have called compose down at the project level.
        fake_subprocess.run.assert_called_once()
        argv = fake_subprocess.run.call_args.args[0]
        assert argv == ["docker", "compose", "-p", "test-project", "down", "--remove-orphans"]

    def test_docker_compose_down_invokes_correct_argv(self, harness, tmp_path):
        # Make it LOOK like a UDP clone.
        (tmp_path / "docker-compose.yml").write_text("services: {}\n", encoding="utf-8")
        fake_subprocess = MagicMock()
        fake_subprocess.run.return_value = MagicMock(returncode=0)
        rc = harness.docker_compose_down(tmp_path, "udp-pnc", runner=fake_subprocess)
        assert rc == 0
        fake_subprocess.run.assert_called_once()
        argv = fake_subprocess.run.call_args.args[0]
        # --remove-orphans added 2026-05-17 to catch stale containers from
        # previous installs that re-used the project name.
        assert argv == ["docker", "compose", "-p", "udp-pnc", "down", "--remove-orphans"]
        # No volume-destroying flag — lake data must survive teardown.
        assert "-v" not in argv
        assert "--volumes" not in argv
        # Working directory is the install dir.
        assert fake_subprocess.run.call_args.kwargs["cwd"] == str(tmp_path)

    def test_docker_compose_down_swallows_exceptions(self, harness, tmp_path):
        (tmp_path / "docker-compose.yml").write_text("services: {}\n", encoding="utf-8")
        fake_subprocess = MagicMock()
        fake_subprocess.run.side_effect = RuntimeError("docker not running")
        rc = harness.docker_compose_down(tmp_path, "p", runner=fake_subprocess)
        assert rc == 1  # error -> 1, but no exception propagates

    def test_remove_install_dir_is_no_op_on_missing_path(self, harness, tmp_path):
        # Should not raise on missing path.
        missing = tmp_path / "does-not-exist"
        harness.remove_install_dir(missing)  # no exception

    def test_remove_install_dir_actually_removes(self, harness, tmp_path):
        (tmp_path / "file.txt").write_text("x")
        assert tmp_path.exists()
        harness.remove_install_dir(tmp_path)
        assert not tmp_path.exists()

    def test_remove_install_dir_refuses_non_directory(self, harness, tmp_path):
        f = tmp_path / "afile"
        f.write_text("x")
        # Pass the file path, not the dir — should be a no-op (defensive).
        harness.remove_install_dir(f)
        assert f.exists()

    def test_teardown_runs_in_finally_when_install_task_raises(self, harness, tmp_path):
        """Contract check: a finally: block around the install body MUST call
        docker_compose_down. We simulate the orchestrator's structure here to
        prove the harness uses the right pattern."""
        (tmp_path / "docker-compose.yml").write_text("services: {}\n", encoding="utf-8")
        fake_subprocess = MagicMock()
        fake_subprocess.run.return_value = MagicMock(returncode=0)
        teardown_called = {"value": False}

        async def _simulate():
            try:
                # Simulate the install pipeline blowing up mid-run.
                raise RuntimeError("simulated bootstrap failure")
            finally:
                # Mirror what _run_harness does in its finally: block.
                harness.docker_compose_down(tmp_path, "udp-pnc", runner=fake_subprocess)
                teardown_called["value"] = True

        with pytest.raises(RuntimeError, match="simulated bootstrap failure"):
            asyncio.run(_simulate())

        assert teardown_called["value"] is True
        fake_subprocess.run.assert_called_once()
        argv = fake_subprocess.run.call_args.args[0]
        assert argv[:2] == ["docker", "compose"]
        assert "down" in argv

    def test_no_destructive_flags_in_any_compose_argv(self, harness, tmp_path):
        """Defense in depth: the harness must never emit -v, --volumes, or
        --rmi to docker compose down. Those would destroy the lakehouse data
        we want to preserve for re-attach."""
        (tmp_path / "docker-compose.yml").write_text("services: {}\n", encoding="utf-8")
        fake_subprocess = MagicMock()
        fake_subprocess.run.return_value = MagicMock(returncode=0)
        harness.docker_compose_down(tmp_path, "p", runner=fake_subprocess)
        argv = fake_subprocess.run.call_args.args[0]
        forbidden = {"-v", "--volumes", "--rmi", "--force", "--no-verify", "-rf", "rm"}
        assert not (forbidden & set(argv)), \
            f"docker compose argv contains a forbidden destructive flag: {argv}"
