#!/usr/bin/env python3
"""Lakehouse Studio install harness.

Drives a stack manifest end-to-end (clone -> env -> doctor -> start -> bootstrap
-> smoke -> finalize) outside the FastAPI app, captures every log line via the
event bus, then renders a YAML evidence record whose shape matches the existing
``evidence[0]`` entry in ``stacks/compatibility/<stack>.lock.yaml``.

The harness is the mechanical tool behind the "install 6 stacks, certify the
ones that pass" workflow. It does not promote the stack itself — it prints a
ready-to-paste YAML block plus the exact steps an operator runs to promote.

Usage::

    python scripts/install_harness.py --stack <stack_id> [--work-dir DIR] \\
        [--keep] [--no-teardown] [--json [PATH]]

Safety:
  * Never passes --no-verify, --force, or rm -rf.
  * On any failure the last 30 lines of the failing step's stderr are printed.
  * SIGINT (Ctrl-C) routes through a finally: block that runs
    ``docker compose down`` if the start step actually launched containers,
    so orphan containers never survive a kill.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import shutil
import signal
import socket
import subprocess
import sys
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import yaml

# Make ``backend.*`` importable when the harness is invoked from any CWD.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# Allow --work-dir to take effect by setting the env var BEFORE backend.config
# is first imported. argparse runs here, then the imports happen at module top
# of ``main()`` — see _maybe_apply_work_dir_env().
_HARNESS_VERSION = "Lakehouse Studio v0.6.1"
_DEFAULT_WORK_DIR = r"D:\Projects\ClaudeCode\PNC"


# ---------------------------------------------------------------------------
# Lightweight argument parsing — extracted so tests can call it directly.
# ---------------------------------------------------------------------------

def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="install_harness.py",
        description="Lakehouse Studio install harness — drives a stack end-to-end "
                    "and captures an evidence YAML record ready for promotion.",
    )
    p.add_argument(
        "--stack", required=True,
        help="Stack id (e.g. udp-local-v0.2). Must match a manifest in stacks/.",
    )
    p.add_argument(
        "--work-dir", default=None,
        help=f"Override WORK_DIR (default: {_DEFAULT_WORK_DIR}).",
    )
    p.add_argument(
        "--keep", action="store_true",
        help="After success, leave containers running (default: docker compose down).",
    )
    p.add_argument(
        "--no-teardown", action="store_true",
        help="After success or failure, do NOT delete install_dir or run "
             "docker compose down.",
    )
    p.add_argument(
        "--json", nargs="?", const="-", default=None, metavar="PATH",
        help="Emit the evidence record as JSON. With a PATH argument the JSON "
             "is written to that file; with no argument it goes to stdout. The "
             "human-readable YAML block is always printed to stderr.",
    )
    return p


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    return build_arg_parser().parse_args(argv)


# ---------------------------------------------------------------------------
# Apply --work-dir env override before backend.config is imported.
# ---------------------------------------------------------------------------

def _maybe_apply_work_dir_env(work_dir: Optional[str]) -> None:
    chosen = work_dir or os.environ.get("LHS_WORK_DIR") or _DEFAULT_WORK_DIR
    os.environ["LHS_WORK_DIR"] = str(Path(chosen).expanduser())


# ---------------------------------------------------------------------------
# Step result container — also used by tests to drive the evidence renderer.
# ---------------------------------------------------------------------------

# Canonical step order matches backend.runner.UDPRunner._PIPELINE.
PIPELINE_STEPS: tuple[str, ...] = (
    "prepare", "clone", "env", "doctor", "start",
    "bootstrap", "smoke", "finalize",
)


@dataclass
class StepResult:
    step_id: str
    status: str = "pending"          # pending|running|passed|failed|skipped
    exit_code: Optional[int] = None
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    stdout_lines: list[str] = field(default_factory=list)
    stderr_lines: list[str] = field(default_factory=list)

    @property
    def duration_sec(self) -> Optional[float]:
        if self.started_at is None or self.finished_at is None:
            return None
        return round(self.finished_at - self.started_at, 1)


# ---------------------------------------------------------------------------
# Host fact collection — for the evidence record's "host" block.
# ---------------------------------------------------------------------------

def _git_operator() -> str:
    """Return the operator email. Falls back to the OS username if git is absent."""
    for args in (["git", "config", "user.email"], ["git", "config", "--global", "user.email"]):
        try:
            r = subprocess.run(args, capture_output=True, text=True, timeout=5)
            email = (r.stdout or "").strip()
            if email:
                return email
        except Exception:
            pass
    return os.environ.get("USERNAME") or os.environ.get("USER") or "unknown"


def _docker_version() -> str:
    try:
        r = subprocess.run(
            ["docker", "--version"], capture_output=True, text=True, timeout=5,
        )
        line = (r.stdout or "").strip()
        # "Docker version 28.3.0, build 1234" -> "28.3.0"
        if line.lower().startswith("docker version "):
            rest = line[len("docker version "):]
            return rest.split(",", 1)[0].strip()
        return line or "unknown"
    except Exception:
        return "unknown"


def _ram_gb() -> float:
    try:
        import psutil  # already a runtime dep of the studio
        return round(psutil.virtual_memory().total / 1024 / 1024 / 1024, 1)
    except Exception:
        return 0.0


def collect_host_info() -> dict[str, Any]:
    return {
        "os": platform.platform(),
        "docker": _docker_version(),
        "ram_gb": _ram_gb(),
        "cpu_cores": os.cpu_count() or 0,
    }


# ---------------------------------------------------------------------------
# Evidence record renderer — pure function, no I/O, tested directly.
# ---------------------------------------------------------------------------

def render_evidence_record(
    *,
    install_id: str,
    step_results: dict[str, StepResult],
    host_info: dict[str, Any],
    operator: str,
    timestamp: Optional[datetime] = None,
    via: str = f"install_harness.py ({_HARNESS_VERSION})",
    hostname: Optional[str] = None,
    proof_lines: Optional[list[str]] = None,
) -> dict[str, Any]:
    """Render an evidence dict matching the shape in udp-local-v0.2.lock.yaml's
    evidence[0]. Caller passes the *outcome* of each step in step_results.

    Returns a dict (not YAML) so callers can json.dump it. Use dump_evidence_yaml()
    to format it for paste into a lock file.
    """
    ts = (timestamp or datetime.now(timezone.utc)).isoformat()
    date_part = (timestamp or datetime.now(timezone.utc)).strftime("%Y-%m-%d")
    host_short = (hostname or socket.gethostname() or "host").lower()
    # short install id: strip the "inst_" prefix if present
    short_install = install_id.split("_", 1)[-1][:10] if install_id else "noid"
    record_id = f"{date_part}-{host_short}-{short_install}"

    # Result block — one entry per pipeline step. Always emit all 8 keys.
    result_block: dict[str, str] = {}
    for step_id in PIPELINE_STEPS:
        sr = step_results.get(step_id)
        result_block[step_id] = sr.status if sr else "not_run"

    record: dict[str, Any] = {
        "id": record_id,
        "timestamp": ts,
        "operator": operator,
        "host": {
            "os": host_info.get("os", "unknown"),
            "docker": host_info.get("docker", "unknown"),
            "ram_gb": host_info.get("ram_gb", 0.0),
            "cpu_cores": host_info.get("cpu_cores", 0),
        },
        "via": via,
        "install_id": install_id,
        "result": result_block,
    }

    smoke = step_results.get("smoke")
    if smoke and smoke.status == "passed":
        # Proof lines: explicit ones from caller, else last 20 lines of smoke stdout.
        if proof_lines is not None:
            record["proof"] = list(proof_lines)
        else:
            tail = smoke.stdout_lines[-20:] if smoke.stdout_lines else []
            record["proof"] = tail or ["(smoke produced no stdout — see full-log.txt)"]
    elif smoke and smoke.status not in ("passed", "not_run", "skipped"):
        # Failure case — capture last 20 lines of stderr (or stdout if stderr empty).
        tail_src = smoke.stderr_lines or smoke.stdout_lines
        tail = "\n".join(tail_src[-20:]) if tail_src else "(no captured output)"
        record["smoke_failure_root_cause"] = tail

    return record


def dump_evidence_yaml(record: dict[str, Any]) -> str:
    """Serialize the evidence record as a YAML list item, ready for paste."""
    # Use a tiny wrapper: yaml.safe_dump of [record] then indent — keeps block
    # style and forces multi-line strings into the literal style so the
    # smoke_failure_root_cause block reads naturally.
    class _LiteralStr(str):
        pass

    def _literal_representer(dumper, data):
        return dumper.represent_scalar("tag:yaml.org,2002:str", str(data), style="|")

    yaml.add_representer(_LiteralStr, _literal_representer)

    # Coerce multi-line strings to LiteralStr so they emit as block literals.
    def _coerce(node: Any) -> Any:
        if isinstance(node, dict):
            return {k: _coerce(v) for k, v in node.items()}
        if isinstance(node, list):
            return [_coerce(v) for v in node]
        if isinstance(node, str) and "\n" in node:
            return _LiteralStr(node)
        return node

    coerced = _coerce(record)
    return yaml.dump([coerced], sort_keys=False, default_flow_style=False, allow_unicode=True)


# ---------------------------------------------------------------------------
# Promote instruction — pure string, tested directly.
# ---------------------------------------------------------------------------

def render_promote_instructions(
    stack_id: str,
    *,
    current_version: str = "0.x.0",
    next_version: str = "0.x.1",
    now_iso: Optional[str] = None,
) -> str:
    """The exact promotion runbook the operator follows when smoke passes."""
    if now_iso is None:
        now_iso = datetime.now(timezone.utc).isoformat()
    lock_path = f"stacks/compatibility/{stack_id}.lock.yaml"
    return (
        f"To promote {stack_id} to pilot-stable:\n"
        f"  1. Append the YAML block above to {lock_path}'s evidence[] array.\n"
        f"  2. Change `status: candidate` -> `status: pilot-stable` in the same file.\n"
        f"  3. Bump `version_id: {current_version}` -> `version_id: {next_version}`.\n"
        f"  4. Set `certified_at: {now_iso}`.\n"
        f"  5. Commit with message: `cert({stack_id}): promote to pilot-stable`.\n"
    )


# ---------------------------------------------------------------------------
# Teardown helpers — split out so the test suite can mock subprocess cleanly.
# ---------------------------------------------------------------------------

def docker_compose_down(install_dir: Path, project_name: str, *,
                        runner=subprocess) -> int:
    """Run ``docker compose -p <project> down --remove-orphans`` from install_dir.
    Returns rc.

    Volumes survive by default — we want lake data to live for re-attach.
    --remove-orphans catches stale containers from previous installs that
    re-used the same project name (e.g. when a teardown failed mid-flight
    and the next install re-uses the install_dir).
    """
    if not install_dir.exists() or not (install_dir / "docker-compose.yml").exists():
        # Fall back to project-level down even without a compose file on
        # disk — compose tracks projects in Docker even after the source
        # compose.yml is deleted (as long as containers exist).
        try:
            completed = runner.run(
                ["docker", "compose", "-p", project_name, "down", "--remove-orphans"],
                capture_output=True, text=True, timeout=180,
            )
            return completed.returncode
        except Exception:
            return 0
    try:
        completed = runner.run(
            ["docker", "compose", "-p", project_name, "down", "--remove-orphans"],
            cwd=str(install_dir),
            capture_output=True,
            text=True,
            timeout=180,
        )
        return completed.returncode
    except Exception:
        return 1


def remove_install_dir(install_dir: Path) -> None:
    """Remove ONLY the install staging area. Docker volumes survive because
    they live under Docker's data root, not inside install_dir."""
    if not install_dir.exists():
        return
    # Be defensive: refuse to nuke anything that isn't a directory we just made.
    if not install_dir.is_dir():
        return
    shutil.rmtree(install_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Event capture — subscribes to the runner's event bus and shoves every line
# into the matching StepResult container.
# ---------------------------------------------------------------------------

class _EventCapturer:
    """Drain ``bus.subscribe(install_id)`` until the install reaches a terminal
    state, mirroring every log line into ``self.results``."""

    def __init__(self, install_id: str):
        self.install_id = install_id
        self.results: dict[str, StepResult] = {
            sid: StepResult(step_id=sid) for sid in PIPELINE_STEPS
        }
        self.terminal_state: Optional[str] = None
        self._task: Optional[asyncio.Task] = None
        self._done = asyncio.Event()

    async def start(self) -> None:
        from backend.events import bus
        queue = await bus.subscribe(self.install_id)

        async def _drain() -> None:
            try:
                while True:
                    evt = await queue.get()
                    kind = evt.kind
                    step = evt.step
                    if kind == "step_start" and step:
                        sr = self.results.setdefault(step, StepResult(step_id=step))
                        sr.status = "running"
                        sr.started_at = evt.ts
                    elif kind == "step_end" and step:
                        sr = self.results.setdefault(step, StepResult(step_id=step))
                        sr.finished_at = evt.ts
                        # Translate runner's success/failed/skipped vocab to
                        # evidence vocab (passed/failed/skipped).
                        runner_status = evt.status or ""
                        if runner_status == "success":
                            sr.status = "passed"
                        elif runner_status == "failed":
                            sr.status = "failed"
                        elif runner_status == "skipped":
                            sr.status = "skipped"
                        else:
                            sr.status = runner_status or sr.status
                        payload = evt.payload or {}
                        ec = payload.get("exit_code")
                        if ec is not None:
                            sr.exit_code = ec
                    elif kind == "log" and step:
                        sr = self.results.setdefault(step, StepResult(step_id=step))
                        line = evt.line or ""
                        if evt.stream == "stderr":
                            sr.stderr_lines.append(line)
                        else:
                            sr.stdout_lines.append(line)
                    elif kind == "state":
                        if evt.status in ("READY", "FAILED", "STOPPED", "CLEANED"):
                            self.terminal_state = evt.status
                            # Don't break — let the runner emit any trailing
                            # logs first. The orchestrator signals completion
                            # via _done once runner.run() returns.
            except asyncio.CancelledError:
                return

        self._task = asyncio.create_task(_drain(), name=f"harness-capture-{self.install_id}")

    async def stop(self) -> None:
        if self._task is None:
            return
        # Give the bus a beat to flush trailing events the runner just emitted.
        await asyncio.sleep(0.1)
        self._task.cancel()
        try:
            await self._task
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Summary table printing.
# ---------------------------------------------------------------------------

def _format_duration(d: Optional[float]) -> str:
    if d is None:
        return "—"
    if d < 60:
        return f"{d:.1f}s"
    minutes = int(d // 60)
    seconds = int(d - minutes * 60)
    return f"{minutes}m{seconds:02d}s"


def print_summary_table(results: dict[str, StepResult], *, stream=None) -> None:
    stream = stream or sys.stdout
    rows = [(sid, results.get(sid, StepResult(step_id=sid))) for sid in PIPELINE_STEPS]
    print("", file=stream)
    print(f"{'STEP':<11} | {'STATUS':<8} | {'DURATION':<8} | EXIT", file=stream)
    print("-" * 44, file=stream)
    for sid, sr in rows:
        print(
            f"{sid:<11} | {sr.status:<8} | {_format_duration(sr.duration_sec):<8} | "
            f"{sr.exit_code if sr.exit_code is not None else '—'}",
            file=stream,
        )
    print("", file=stream)


def print_failure_tail(results: dict[str, StepResult], *, stream=None,
                       n: int = 30) -> Optional[str]:
    """Print the last n lines of stdout+stderr from the first failed step.
    Returns the failing step id (or None)."""
    stream = stream or sys.stderr
    for sid in PIPELINE_STEPS:
        sr = results.get(sid)
        if sr and sr.status == "failed":
            print(f"\n=== last {n} lines from failed step '{sid}' ===", file=stream)
            combined = []
            for line in sr.stdout_lines:
                combined.append(("stdout", line))
            for line in sr.stderr_lines:
                combined.append(("stderr", line))
            # Keep insertion order; just take the tail.
            tail = combined[-n:] if len(combined) > n else combined
            if not tail:
                print("(no captured output)", file=stream)
            else:
                for stream_kind, line in tail:
                    print(f"[{stream_kind}] {line}", file=stream)
            print("=== end failure tail ===\n", file=stream)
            return sid
    return None


# ---------------------------------------------------------------------------
# Main orchestrator.
# ---------------------------------------------------------------------------

async def _run_harness(args: argparse.Namespace) -> int:
    # Late imports — these MUST come after _maybe_apply_work_dir_env so
    # backend.config picks up the overridden WORK_DIR.
    from backend.config import WORK_DIR
    from backend.events import bus  # noqa: F401  (touch module to init)
    from backend.runner import UDPRunner, make_steps
    from backend.stack_manifest import load_manifest
    from backend.state import store

    # ---- 1. Load manifest ----
    try:
        stack = load_manifest(args.stack)
    except KeyError as e:
        print(f"[harness] {e}", file=sys.stderr)
        return 2

    # ---- 2. Compute install_dir under WORK_DIR ----
    repo_dirname = (stack.repository.get("install_dir") or stack.id).strip()
    install_dir = (WORK_DIR / repo_dirname).resolve()
    install_dir.parent.mkdir(parents=True, exist_ok=True)

    host = "127.0.0.1"
    print(f"[harness] stack       = {stack.id}", file=sys.stderr)
    print(f"[harness] work_dir    = {WORK_DIR}", file=sys.stderr)
    print(f"[harness] install_dir = {install_dir}", file=sys.stderr)
    print(f"[harness] host        = {host}", file=sys.stderr)
    print(f"[harness] keep        = {args.keep}", file=sys.stderr)
    print(f"[harness] teardown    = {not args.no_teardown}", file=sys.stderr)

    # ---- 3. Create InstallRecord ----
    steps = make_steps(stack)
    create_kwargs: dict[str, Any] = dict(
        stack_id=stack.id,
        host=host,
        install_dir=str(install_dir),
        steps=steps,
    )
    # The InstallRecord model accepts environment=testing only if it's in the
    # EnvironmentTier literal — currently dev|staging|prod. Pass it only when
    # the model truly accepts it so we don't break on older revs.
    try:
        from backend.models import EnvironmentTier  # noqa: F401
        import typing
        allowed = typing.get_args(EnvironmentTier)
        if "testing" in allowed:
            create_kwargs["environment"] = "testing"
        elif "dev" in allowed:
            # Closest analogue — keeps install isolated from prod-tier installs
            # in single-host multi-tier setups.
            create_kwargs["environment"] = "dev"
    except Exception:
        pass

    record = store.create(**create_kwargs)
    install_id = record.install_id
    print(f"[harness] install_id  = {install_id}", file=sys.stderr)

    # ---- 4. Subscribe to event bus BEFORE kicking off ----
    capturer = _EventCapturer(install_id)
    await capturer.start()

    # Project name MUST match what `docker compose up` actually used.
    # The runner doesn't pass `-p`, so compose auto-derives the project
    # name from the install_dir basename (sanitized — lowercase, no spaces).
    # Using stack.env_defaults["UDP_PROJECT_NAME"] is WRONG because that
    # env var is only an env-default exposed to the containers; it never
    # makes it into compose's project naming. Bug found 2026-05-17 when
    # the udp-local-v0.2 teardown was a no-op because it called
    # `docker compose -p unified-data-plug down` for containers actually
    # running under project `udp` (= install_dir name).
    project_name = install_dir.name.lower().replace(" ", "_").replace("/", "_")

    teardown_done = False

    async def _teardown(reason: str) -> None:
        nonlocal teardown_done
        if teardown_done:
            return
        teardown_done = True
        if args.no_teardown:
            print(f"[harness] teardown skipped ({reason}; --no-teardown set)",
                  file=sys.stderr)
            return
        # Bring down compose containers. Volumes survive (no -v flag).
        print(f"[harness] teardown: docker compose -p {project_name} down ({reason})",
              file=sys.stderr)
        rc = docker_compose_down(install_dir, project_name)
        if rc != 0:
            print(f"[harness] docker compose down exit={rc} (continuing)",
                  file=sys.stderr)
        # Remove install_dir AFTER teardown so a partial clone is also cleaned.
        # Skip if --keep set (keeps the install live for inspection).
        if not args.keep:
            print(f"[harness] removing install_dir {install_dir}", file=sys.stderr)
            remove_install_dir(install_dir)

    # ---- 5. Run the pipeline ----
    runner = UDPRunner(stack, install_id, host, install_dir)
    start_ts = time.time()

    try:
        try:
            await runner.run(env_overrides={})
        except asyncio.CancelledError:
            print("[harness] cancelled — running teardown", file=sys.stderr)
            await _teardown("cancelled")
            raise
    finally:
        # Always stop the capturer so we drain trailing events first.
        await capturer.stop()
        results = capturer.results
        terminal = capturer.terminal_state

        # ---- 6. Print step summary ----
        elapsed = round(time.time() - start_ts, 1)
        print(f"[harness] pipeline finished in {_format_duration(elapsed)} "
              f"(terminal state: {terminal or 'unknown'})", file=sys.stderr)
        print_summary_table(results, stream=sys.stderr)

        # ---- 7. If failure: print last 30 lines of the failing step ----
        if terminal == "FAILED" or any(sr.status == "failed" for sr in results.values()):
            print_failure_tail(results, stream=sys.stderr, n=30)

        # ---- 8. Build + emit the evidence record ----
        smoke_passed = results.get("smoke") and results["smoke"].status == "passed"

        evidence = render_evidence_record(
            install_id=install_id,
            step_results=results,
            host_info=collect_host_info(),
            operator=_git_operator(),
        )

        yaml_block = dump_evidence_yaml(evidence)

        # YAML always goes to stderr so stdout stays clean for --json piping.
        print("\n=== EVIDENCE YAML (paste into lock file's evidence[]) ===",
              file=sys.stderr)
        print(yaml_block, file=sys.stderr)
        print("=== end evidence ===\n", file=sys.stderr)

        # JSON output target: stdout ("-") or file path.
        if args.json is not None:
            payload = json.dumps(evidence, indent=2, default=str)
            if args.json == "-":
                print(payload)
            else:
                Path(args.json).write_text(payload + "\n", encoding="utf-8")
                print(f"[harness] evidence JSON written to {args.json}", file=sys.stderr)

        # ---- 9. Print promote instructions only on smoke pass ----
        if smoke_passed:
            print(render_promote_instructions(stack.id), file=sys.stderr)
        else:
            print("[harness] smoke did NOT pass — not promoting. "
                  "Investigate the failure tail above, then re-run.",
                  file=sys.stderr)

        # ---- 10. Teardown (success path) ----
        # On --keep: leave containers up but still write evidence + skip rmtree.
        if not teardown_done:
            if args.keep:
                print(f"[harness] --keep set: leaving containers running at {install_dir}",
                      file=sys.stderr)
                # Still flush state.
                try:
                    store.flush()
                except Exception:
                    pass
            else:
                await _teardown("end-of-run")

        # Always flush state on exit.
        try:
            store.flush()
        except Exception:
            pass

    # Exit code: 0 if smoke passed, otherwise the position of the failing step
    # (so CI can distinguish "infra didn't start" from "smoke failed").
    if smoke_passed:
        return 0
    # Map first-failure-step to a small int (1..7) for caller diagnosis.
    for i, sid in enumerate(PIPELINE_STEPS, start=1):
        sr = results.get(sid)
        if sr and sr.status == "failed":
            return i
    return 1


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    _maybe_apply_work_dir_env(args.work_dir)

    # Hook SIGINT so Ctrl-C cancels the pipeline (the finally: block then
    # routes through _teardown). On Windows asyncio's default handler raises
    # KeyboardInterrupt which we let propagate into the runner's CancelledError.
    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        task = loop.create_task(_run_harness(args))

        def _on_signal(_signum, _frame):  # pragma: no cover - signal path
            task.cancel()

        try:
            signal.signal(signal.SIGINT, _on_signal)
        except (ValueError, AttributeError):
            # Not on main thread or platform doesn't support — Ctrl-C will
            # still raise KeyboardInterrupt which loop.run_until_complete
            # propagates.
            pass

        try:
            return loop.run_until_complete(task)
        except KeyboardInterrupt:
            task.cancel()
            try:
                return loop.run_until_complete(task)
            except Exception:
                return 130
        except asyncio.CancelledError:
            return 130
    finally:
        try:
            loop.close()
        except Exception:
            pass


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
