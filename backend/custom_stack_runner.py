"""custom_stack_runner.py — end-to-end orchestrator for custom lakehouse stacks.

Builds and starts an arbitrary component selection via:
  1. AI version research (version_fetcher + compat_ai picks best compatible set)
  2. Dependency resolution
  3. docker-compose.yml generation (stack_composer)
  4. AI config file generation (ai_configurator)
  5. Write all files to disk
  6. docker-compose up -d
  7. Health / connectivity verification
  8. Connection summary emission
"""
from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from pathlib import Path
from typing import AsyncGenerator

from .component_registry import COMPONENTS, resolve_dependencies, get_live_versions
from . import stack_composer, ai_configurator, version_fetcher, compat_ai, ai_safety
from .config import WORK_DIR
from .state import store
from .models import StepStatus

_CUSTOM_JOBS: dict[str, "CustomStackJob"] = {}

# Phase display order
_PHASES = ["versions", "resolve", "compose", "ai_config", "write_files", "start", "post_cfg", "verify", "summary"]

# Human titles for the Install History step list (mirrors manifest-based installs).
_PHASE_TITLES = {
    "versions":    "Resolve versions",
    "resolve":     "Resolve dependencies",
    "compose":     "Generate docker-compose",
    "ai_config":   "Generate service configs",
    "write_files": "Write config files",
    "start":       "Start stack (docker compose up)",
    "post_cfg":    "Post-start configuration",
    "verify":      "Verify health",
    "summary":     "Capture outputs",
}

# Custom-build phase → InstallState so the Install History pill matches the
# manifest-based lifecycle. Any non-terminal state keeps the record in
# RUNNING_STATES, which correctly blocks uninstall until the build settles.
_PHASE_TO_STATE = {
    "versions":    "READY_TO_INSTALL",
    "resolve":     "READY_TO_INSTALL",
    "compose":     "WRITING_ENV",
    "ai_config":   "WRITING_ENV",
    "write_files": "WRITING_ENV",
    "start":       "STARTING_STACK",
    "post_cfg":    "BOOTSTRAPPING",
    "verify":      "SMOKE_TESTING",
    "summary":     "SMOKE_TESTING",
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def start_custom_build(
    selected: list[str],
    version_overrides: dict[str, str] | None = None,
    stack_name: str | None = None,
    include_experimental: bool = False,
    target: dict | None = None,
) -> str:
    """Create a new build job and return its job_id. Fires off background task.

    target (optional): {"mode": "local"|"remote", "host": ..., "ssh_user": ...,
      "ssh_port": 22, "ssh_key_path": ..., "ssh_password": ..., "install_dir": ...}
    When mode == "remote", the stack is built locally then rsynced + run on the
    remote host over SSH.
    """
    job_id = str(uuid.uuid4())
    job = CustomStackJob(
        job_id=job_id,
        selected=selected,
        version_overrides=version_overrides or {},
        stack_name=_safe_name(stack_name or "custom-lakehouse"),
        include_experimental=include_experimental,
        target=target or {"mode": "local"},
    )
    _CUSTOM_JOBS[job_id] = job
    asyncio.get_event_loop().create_task(job.run())
    return job_id


def get_job(job_id: str) -> "CustomStackJob | None":
    return _CUSTOM_JOBS.get(job_id)


def _safe_name(name: str) -> str:
    import re
    return re.sub(r"[^a-z0-9_-]", "-", name.lower())[:48]


# ---------------------------------------------------------------------------
# Job class
# ---------------------------------------------------------------------------

class CustomStackJob:
    def __init__(
        self,
        job_id: str,
        selected: list[str],
        version_overrides: dict[str, str],
        stack_name: str,
        include_experimental: bool = False,
        target: dict | None = None,
    ):
        self.job_id = job_id
        self.selected = selected
        self.version_overrides = version_overrides
        self.stack_name = stack_name
        self.include_experimental = include_experimental
        self.target = target or {"mode": "local"}
        self.is_remote = (self.target.get("mode") == "remote")

        self.state: str = "pending"
        self.current_phase: str | None = None
        self.phase_states: dict[str, str] = {p: "pending" for p in _PHASES}
        self.resolved: list[str] = []
        self.auto_added: list[str] = []
        self.compose_yaml: str = ""
        self.connection_info: dict = {}
        self.pipeline_example: str = ""
        self.config_plan: dict = {}
        self.error: str | None = None
        # Known up-front (compose phase re-affirms it); needed now so the
        # Install History record + uninstall can point at the compose dir.
        self.install_dir: Path | None = WORK_DIR / self.stack_name

        self._events: asyncio.Queue = asyncio.Queue()
        self._done = False

        # ── Register in Install History (state.store) ───────────────────────
        # Quick Install and custom builds previously ran entirely off the
        # in-memory job and never appeared in Install History, so their
        # containers could only be removed by hand. Create a real InstallRecord
        # (marked custom_build so uninstall uses `docker compose down`, not the
        # manifest-based `./udp clean`) and keep its state in sync below.
        self.store_install_id: str | None = None
        try:
            rec = store.create(
                stack_id=self.stack_name,
                host=(self.target.get("host") if self.is_remote else "localhost"),
                install_dir=str(self.install_dir),
                steps=[StepStatus(id=p, title=_PHASE_TITLES.get(p, p)) for p in _PHASES],
                lake_name=self.stack_name,
                cart=list(self.selected),
            )
            self.store_install_id = rec.install_id
            store.set_outputs(rec.install_id, {"custom_build": True})
        except Exception:
            # Never let history bookkeeping break an install.
            self.store_install_id = None

    # ── Install History sync ─────────────────────────────────────────────────

    def _store_state(self, state: str, error: str | None = None) -> None:
        if self.store_install_id:
            try:
                store.update_state(self.store_install_id, state, error=error)  # type: ignore[arg-type]
            except Exception:
                pass

    def _store_step(self, phase: str, status: str) -> None:
        if self.store_install_id:
            try:
                store.update_step(self.store_install_id, phase, status=status,
                                  finished_at=(time.time() if status in ("success", "failed") else None),
                                  started_at=(time.time() if status == "running" else None))
            except Exception:
                pass

    # ── event helpers ───────────────────────────────────────────────────────

    def _emit(self, kind: str, **kwargs) -> None:
        self._events.put_nowait({"kind": kind, "ts": time.time(), **kwargs})

    def _log(self, line: str) -> None:
        self._emit("log", line=line)

    def _phase_start(self, phase: str) -> None:
        self.current_phase = phase
        self.phase_states[phase] = "active"
        self._store_step(phase, "running")
        self._store_state(_PHASE_TO_STATE.get(phase, "STARTING_STACK"))
        self._emit("phase_start", phase=phase)

    def _phase_end(self, phase: str, ok: bool = True) -> None:
        self.phase_states[phase] = "done" if ok else "failed"
        self._store_step(phase, "success" if ok else "failed")
        self._emit("phase_end", phase=phase, ok=ok)

    async def stream_events(self) -> AsyncGenerator[str, None]:
        """Async generator of SSE data lines."""
        while True:
            try:
                event = await asyncio.wait_for(self._events.get(), timeout=0.5)
                yield f"data: {json.dumps(event)}\n\n"
            except asyncio.TimeoutError:
                if self._done:
                    break
                yield ": heartbeat\n\n"

    # ── main orchestrator ───────────────────────────────────────────────────

    async def run(self) -> None:
        self.state = "running"
        try:
            await self._run_phases()
            # All phases done — mark the Install History record READY and
            # persist the endpoint URLs so it renders like a normal install.
            self._store_state("READY")
        except Exception as exc:
            self.state = "failed"
            self.error = str(exc)
            self._emit("error", message=str(exc))
            if self.current_phase:
                self._phase_end(self.current_phase, ok=False)
            self._store_state("FAILED", error=str(exc))
        finally:
            self._done = True
            self._emit("done", state=self.state)

    async def _run_phases(self) -> None:

        # ── 0: versions — AI picks latest compatible set ────────────────────
        self._phase_start("versions")
        await self._phase_version_research()
        self._phase_end("versions")

        # ── 1: resolve ──────────────────────────────────────────────────────
        self._phase_start("resolve")
        self.resolved = resolve_dependencies(self.selected)
        self.auto_added = [c for c in self.resolved if c not in self.selected]
        self._log(f"Resolved {len(self.resolved)} components")
        if self.auto_added:
            self._log(f"Auto-added: {', '.join(self.auto_added)}")
        self._emit("resolved", resolved=self.resolved, auto_added=self.auto_added)
        self._phase_end("resolve")

        # ── 2: compose ─────────────────────────────────────────────────────
        self._phase_start("compose")
        self.install_dir = WORK_DIR / self.stack_name
        plan = stack_composer.write_compose(
            self.install_dir, self.selected, self.version_overrides,
            include_experimental=self.include_experimental,
        )
        self.compose_yaml = plan["compose_yaml"]
        vols = plan["volumes"]
        # Update resolved to what actually went into the compose
        self.resolved = plan["resolved"]
        self.auto_added = plan["auto_added"]
        self._log(f"Generated docker-compose.yml — {len(self.resolved)} services")
        self._log(f"Named volumes: {', '.join(vols) if vols else 'none'}")
        for w in plan.get("warnings", []):
            self._log(f"  ⚠ {w}")
        if plan.get("skipped_experimental"):
            skipped = plan["skipped_experimental"]
            self._log(f"Skipped {len(skipped)} experimental component(s): {', '.join(skipped)}")
            self._emit("skipped_experimental", components=skipped)
        self._emit("compose_ready", compose_yaml=self.compose_yaml)
        self._phase_end("compose")

        # ── 3: ai_config ────────────────────────────────────────────────────
        self._phase_start("ai_config")
        self._log("Asking AI to generate all service configuration files …")
        loop = asyncio.get_event_loop()
        config_plan: dict = await loop.run_in_executor(
            None,
            ai_configurator.generate_configs,
            self.resolved,
            self.version_overrides,
        )
        self.config_plan = config_plan
        self.connection_info = config_plan.get("connection_info", {})
        self.pipeline_example = config_plan.get("pipeline_example", "")
        non_empty = [k for k, v in config_plan.items() if v]
        self._log(f"AI returned: {', '.join(non_empty)}")
        self._phase_end("ai_config")

        # ── 4: write_files ──────────────────────────────────────────────────
        self._phase_start("write_files")
        written = ai_configurator.write_configs(
            self.install_dir, config_plan, self.resolved
        )
        for f in written:
            self._log(f"  wrote: {f}")
        self._log(f"Wrote {len(written)} config files to {self.install_dir}")
        self._phase_end("write_files")

        # ── 4b: post-write patches ──────────────────────────────────────────
        self._apply_post_write_patches()

        # ── 5: start ────────────────────────────────────────────────────────
        self._phase_start("start")
        self._log(f"docker-compose up -d  [{self.install_dir}]")
        # Stop any containers from a previous stack that share our udp-* names
        await self._cleanup_conflicting_containers()
        # Check if any service has a build: section (custom images that need local build)
        build_services = [
            cid for cid in self.resolved
            if COMPONENTS.get(cid, {}).get("build_dockerfile")
        ]
        if build_services:
            self._log(f"Building custom images locally: {', '.join(build_services)} (this may take 3-5 min on first run)")
        ok = await self._docker_compose_up()
        if not ok:
            raise RuntimeError("docker-compose up failed — see logs above")
        self._phase_end("start")

        # ── 6: post_cfg ─────────────────────────────────────────────────────
        self._phase_start("post_cfg")
        await self._phase_post_cfg()
        self._phase_end("post_cfg")

        # ── 7: verify ───────────────────────────────────────────────────────
        self._phase_start("verify")
        await self._verify_health()
        self._phase_end("verify")

        # ── 8: summary ──────────────────────────────────────────────────────
        self._phase_start("summary")
        # Remote installs render URLs against the remote host, not localhost.
        host = (self.target.get("host") if self.is_remote else None) \
            or os.environ.get("LAKEHOUSE_HOST", "localhost")
        conn = {k: v.replace("HOST", host) for k, v in self.connection_info.items()}
        # Override with actual host-port mappings from component registry
        _PORT_MAP = {
            "airflow":          ("Airflow",              f"http://{host}:8090"),
            "superset":         ("Superset",             f"http://{host}:8089"),
            "trino":            ("Trino",                f"http://{host}:8285"),
            "trino-enterprise": ("Trino Enterprise",     f"http://{host}:8285"),
            "hadoop-yarn":      ("YARN ResourceManager", f"http://{host}:8188"),
            "hdfs":             ("HDFS NameNode",        f"http://{host}:9870"),
            "starrocks":        ("StarRocks",            f"http://{host}:8040"),
            "minio":            ("MinIO",                f"http://{host}:9001"),
            "spark-hudi":       ("Spark UI",             f"http://{host}:8888"),
            "spark-delta":      ("Spark UI",             f"http://{host}:8888"),
            "spark-iceberg":    ("Spark UI",             f"http://{host}:8888"),
            "hive-metastore":   ("Hive Metastore",       f"thrift://{host}:9083"),
            "hive":             ("HiveServer2 JDBC",     f"jdbc:hive2://{host}:10000/default"),
            "pgbouncer":        ("PgBouncer",            f"postgresql://{host}:5433"),
            "postgres":         ("PostgreSQL",           f"postgresql://{host}:5533"),
            "grafana":          ("Grafana",              f"http://{host}:3010"),
            "ranger":           ("Ranger",               f"http://{host}:6080"),
        }
        conn = {}
        for cid in self.resolved:
            if cid in _PORT_MAP:
                label, url = _PORT_MAP[cid]
                conn[label] = url
        self._emit(
            "connection_info",
            info=conn,
            pipeline_example=self.pipeline_example,
        )
        # Persist endpoints to the Install History record (keep the custom_build
        # marker so uninstall keeps using compose-down).
        if self.store_install_id:
            try:
                store.set_outputs(self.store_install_id, {
                    "custom_build": True,
                    "urls": {label: {"url": url, "label": label} for label, url in conn.items()},
                    "pipeline_example": self.pipeline_example,
                })
            except Exception:
                pass
        self._emit("provision_complete", stack_name=self.stack_name)
        self._log("Stack is ready!")
        self.state = "done"
        self._phase_end("summary")

    # ── version research ────────────────────────────────────────────────────

    async def _phase_version_research(self) -> None:
        """Fetch live versions from registries + call AI to pick compatible set.

        Populates self.version_overrides with AI-selected versions for every
        component that doesn't already have a user override.  Falls back to
        the registry default_version if fetching or AI fails.
        """
        loop = asyncio.get_event_loop()

        # Step 1: resolve deps early so we know all components we need versions for
        pre_resolved = resolve_dependencies(self.selected)

        # Step 2: fetch live versions for every resolved component (in thread pool)
        self._log("Fetching latest versions from upstream registries…")
        available: dict[str, list[str]] = {}

        def _fetch_all() -> dict[str, list[str]]:
            result: dict[str, list[str]] = {}
            for cid in pre_resolved:
                versions = version_fetcher.get_versions(cid)
                good = [v["version"] for v in versions if not v.get("error") and v.get("version")]
                if good:
                    result[cid] = good
            return result

        available = await loop.run_in_executor(None, _fetch_all)

        fetched_count = sum(1 for v in available.values() if v)
        self._log(f"Fetched versions for {fetched_count}/{len(pre_resolved)} components")

        # Step 3: pick the anchor — primary compute engine if present
        _ANCHOR_PRIORITY = [
            "spark-hudi", "spark-delta", "spark-iceberg", "spark",
            "trino-enterprise", "trino", "starrocks",
            "flink", "kafka", "airflow",
        ]
        anchor_id = next((c for c in _ANCHOR_PRIORITY if c in pre_resolved), None)
        if anchor_id is None:
            anchor_id = pre_resolved[0] if pre_resolved else None

        if anchor_id and available.get(anchor_id):
            anchor_version = available[anchor_id][0]  # latest available
            self._log(f"AI version research: anchor = {anchor_id} {anchor_version}")

            def _do_research() -> dict:
                return compat_ai.research_compat(
                    anchor_id=anchor_id,
                    anchor_version=anchor_version,
                    available_versions=available,
                )

            research = await loop.run_in_executor(None, _do_research)

            if research.get("error"):
                self._log(f"  ⚠ AI research error: {research['error']} — using registry defaults")
            else:
                ai_versions: dict[str, str] = research.get("compat", {})
                cached = research.get("cached", False)
                self._log(f"  AI recommended {len(ai_versions)} versions {'(cached)' if cached else ''}")
                for cid, ver in ai_versions.items():
                    if cid not in self.version_overrides:   # don't override user's explicit picks
                        self.version_overrides[cid] = ver
                        self._log(f"    {cid}: {ver}")
        else:
            self._log("No anchor component with known version — using registry defaults")

        # Step 4: fill in any remaining components using live fetcher fallback
        for cid in pre_resolved:
            if cid not in self.version_overrides:
                if available.get(cid):
                    self.version_overrides[cid] = available[cid][0]
                else:
                    fallback = COMPONENTS.get(cid, {}).get("default_version", "latest")
                    self.version_overrides[cid] = fallback

        self._emit("versions_ready", versions=self.version_overrides)

    # ── remote SSH helpers ───────────────────────────────────────────────────

    def _ssh_opts(self) -> list[str]:
        t = self.target
        opts = [
            "-o", "StrictHostKeyChecking=no",
            "-o", "ConnectTimeout=30",
            "-o", "ServerAliveInterval=30",
            "-p", str(t.get("ssh_port", 22)),
        ]
        if t.get("ssh_key_path"):
            opts += ["-i", t["ssh_key_path"]]
        return opts

    def _ssh_dest(self) -> str:
        return f'{self.target.get("ssh_user")}@{self.target.get("host")}'

    def _sshpass(self) -> list[str]:
        import shutil as _sh
        pw = self.target.get("ssh_password")
        if not pw:
            return []
        if not _sh.which("sshpass"):
            raise RuntimeError("sshpass not installed (needed for SSH password auth). Run: sudo apt-get install -y sshpass")
        return ["sshpass", "-p", pw]

    def _remote_dir(self) -> str:
        d = self.target.get("install_dir") or f"lakehouse-studio/{self.stack_name}"
        return d

    def _wrap_remote(self, remote_cmd: str) -> list[str]:
        """Wrap a shell command so it runs on the remote host inside remote_dir."""
        full = f"cd {self._remote_dir()} 2>/dev/null; {remote_cmd}"
        return self._sshpass() + ["ssh"] + self._ssh_opts() + [self._ssh_dest(), full]

    async def _exec_stream(self, argv: list[str]) -> int:
        """Run argv, streaming stdout/stderr lines to the job log. Returns rc."""
        proc = await asyncio.create_subprocess_exec(
            *argv,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        assert proc.stdout is not None
        async for raw_line in proc.stdout:
            self._log(raw_line.decode(errors="replace").rstrip())
        await proc.wait()
        return proc.returncode or 0

    async def _exec_capture(self, argv: list[str]) -> tuple[int, str]:
        """Run argv, capture combined output. Returns (rc, output)."""
        proc = await asyncio.create_subprocess_exec(
            *argv,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        out, _ = await proc.communicate()
        return (proc.returncode or 0), out.decode(errors="replace")

    async def _sync_to_remote(self) -> bool:
        """rsync the local install_dir to the remote host's remote_dir."""
        opts = self._ssh_opts()
        if self.target.get("ssh_password") and __import__("shutil").which("sshpass"):
            ssh_cmd = "sshpass -p " + self.target["ssh_password"] + " ssh " + " ".join(opts)
        else:
            ssh_cmd = "ssh " + " ".join(opts)
        # ensure remote dir exists
        mk = self._sshpass() + ["ssh"] + opts + [self._ssh_dest(), f"mkdir -p {self._remote_dir()}"]
        rc, out = await self._exec_capture(mk)
        if rc != 0:
            self._log(f"  ⚠ could not create remote dir: {out[:200]}")
            return False
        src = str(self.install_dir).rstrip("/") + "/"
        dst = f"{self._ssh_dest()}:{self._remote_dir()}/"
        self._log(f"  rsync → {dst}")
        rc = await self._exec_stream(
            ["rsync", "-az", "--delete", "-e", ssh_cmd, src, dst]
        )
        return rc == 0

    # ── docker helpers (target-aware) ─────────────────────────────────────────

    async def _cleanup_conflicting_containers(self) -> None:
        """Stop and remove containers from previous stacks that share udp-* names.

        All stacks use the same container_name scheme (udp-minio, udp-postgres, …).
        If a previous stack is still running, docker compose up crashes with a
        'container name already in use' error.  This pre-flight cleans those up.
        """
        expected_names = {f"udp-{cid}" for cid in self.resolved}

        # List all containers (running or stopped) by name — local or remote.
        if self.is_remote:
            _, out = await self._exec_capture(
                self._wrap_remote("docker ps -a --format '{{.Names}}'"))
        else:
            _, out = await self._exec_capture(
                ["docker", "ps", "-a", "--format", "{{.Names}}"])
        existing = set(out.splitlines())

        conflicts = sorted(expected_names & existing)
        if not conflicts:
            return

        self._log(f"Stopping {len(conflicts)} pre-existing container(s): {', '.join(conflicts)}")
        names = " ".join(conflicts)
        if self.is_remote:
            await self._exec_capture(self._wrap_remote(f"docker rm -f {names}"))
        else:
            await self._exec_capture(["docker", "rm", "-f", *conflicts])
        self._log("  previous containers removed — clean slate for new stack")

    async def _docker_compose_up(self) -> bool:
        has_builds = any(
            COMPONENTS.get(cid, {}).get("build_dockerfile")
            for cid in self.resolved
        )
        build_flag = " --build" if has_builds else ""

        if self.is_remote:
            # Push the staged stack to the remote host, then run compose there.
            if has_builds:
                self._log("  ⚠ stack has locally-built images; remote build context is not "
                          "synced — remote install supports pull-only stacks. Continuing (pull).")
            self._log(f"  remote install on {self.target.get('host')} → {self._remote_dir()}")
            if not await self._sync_to_remote():
                self._log("  ✗ rsync to remote failed")
                return False
            remote_cmd = f"docker compose -f docker-compose.yml up -d --remove-orphans{build_flag}"
            self._log(f"$ [remote] {remote_cmd}")
            rc = await self._exec_stream(self._wrap_remote(remote_cmd))
            return rc == 0

        cmd = [
            "docker", "compose",
            "-f", str(self.install_dir / "docker-compose.yml"),
            "up", "-d", "--remove-orphans",
        ]
        if has_builds:
            cmd.append("--build")
        self._log(f"$ {' '.join(cmd)}")
        rc = await self._exec_stream(cmd)
        return rc == 0

    def _apply_post_write_patches(self) -> None:
        """Deterministic fixes applied after AI writes files — no AI call needed.

        Covers things that AI gets wrong consistently:
          - Airflow needs command: standalone + correct DB hostname + migration env
          - pgbouncer port mapping 5433:5432 + pg_isready healthcheck
          - YARN ResourceManager must bind webapp to 0.0.0.0:8088
          - HiveServer2 must use hiveserver2-site.xml (embedded MySQL, not HMS Thrift)
          - Trino hive catalog must use new s3.* properties (Trino 400+)
          - HMS metastore-site.xml must use &amp; not bare & in JDBC URL
        """
        import re as _re, shutil as _sh, pathlib as _pl

        d = self.install_dir

        # ── docker-compose.yml patches ──────────────────────────────────────
        compose_path = d / "docker-compose.yml"
        if compose_path.exists():
            txt = compose_path.read_text()

            # Pin known-good image tags — version research sometimes picks a
            # library version (e.g. Iceberg 1.11.0) that has no matching Docker
            # image tag. Force these images to tags that actually exist.
            _IMAGE_PINS = {
                "tabulario/iceberg-rest":  "1.6.0",
                "tabulario/spark-iceberg": "3.5.5_1.8.1",
                "trinodb/trino":           "481",
                "apache/hive":             "4.0.1",
                "apache/hadoop":           "3.4.1",
                "starrocks/allin1-ubuntu": "4.0.10",
                "apache/airflow":          "3.2.2",
                "projectnessie/nessie":    "0.99.0",
                "apache/flink":            "1.20.0",
                "flink":                   "1.20-scala_2.12",
                "confluentinc/cp-kafka":   "7.8.0",
                "confluentinc/cp-zookeeper": "7.8.0",
                "wbaa/rokku-dev-apache-ranger": "latest",
            }
            for img, tag in _IMAGE_PINS.items():
                txt = _re.sub(
                    rf"image:\s*{_re.escape(img)}:[^\n]+",
                    f"image: {img}:{tag}", txt)

            # Airflow: add standalone command
            if "command: standalone" not in txt:
                txt = _re.sub(
                    r'(  airflow:.*?restart: unless-stopped\n)(    networks:)',
                    r'\1    command: standalone\n\2', txt, flags=_re.DOTALL)

            # Airflow: fix DB URL to use container hostname
            txt = txt.replace(
                "@postgres/${POSTGRES_DB", "@udp-postgres/${POSTGRES_DB")

            # Airflow: add migration env vars
            if "_AIRFLOW_DB_MIGRATE" not in txt:
                txt = txt.replace(
                    "      AIRFLOW_UID: '50000'",
                    "      AIRFLOW_UID: '50000'\n"
                    "      _AIRFLOW_DB_MIGRATE: 'true'\n"
                    "      _AIRFLOW_WWW_USER_CREATE: 'true'\n"
                    "      _AIRFLOW_WWW_USER_USERNAME: admin\n"
                    "      _AIRFLOW_WWW_USER_PASSWORD: admin")

            # pgbouncer: fix port mapping
            txt = txt.replace("- 5433:5433", "- 5433:5432")
            # pgbouncer: fix healthcheck
            txt = _re.sub(
                r"psql -h localhost -p 54\d+ -U \S+ -c 'SELECT 1'[^\n]*",
                "pg_isready -h localhost -p 5432", txt)

            # Hive: pin to 4.0.1 and use correct volumes + env
            if "apache/hive:" in txt:
                txt = _re.sub(r"image: apache/hive:[0-9.]+",
                              "image: apache/hive:4.0.1", txt)
                # Replace hive volumes block
                txt = _re.sub(
                    r'(  hive:.*?    volumes:\n)(?:    - [^\n]+\n)+(    healthcheck:)',
                    r'\1'
                    r'    - ./config/hive/hiveserver2-site.xml:/opt/hive/conf/hiveserver2-site.xml:ro\n'
                    r'    - ./config/hadoop/core-site.xml:/opt/hive/conf/core-site.xml\n'
                    r'    - ./hive-jars/mysql-connector-java.jar:/opt/hive/lib/mysql-connector-java.jar:ro\n'
                    r'\2',
                    txt, flags=_re.DOTALL)
                # Add SKIP_SCHEMA_INIT if missing
                if "SKIP_SCHEMA_INIT" not in txt:
                    txt = _re.sub(
                        r'(  hive:.*?    environment:\n      SERVICE_NAME: hiveserver2\n)',
                        r"\1      SKIP_SCHEMA_INIT: 'true'\n      IS_RESUME: 'true'\n",
                        txt, flags=_re.DOTALL)

            # HMS healthcheck: schema init takes ~60s on fresh MySQL volume.
            # Use a line-by-line approach — DOTALL regex crosses service boundaries.
            lines = txt.splitlines()
            in_hms = False
            hc_lines_left = 0
            for i, line in enumerate(lines):
                if line.startswith("  hive-metastore:"):
                    in_hms = True
                elif in_hms and line.startswith("  ") and not line.startswith("    "):
                    in_hms = False  # entered next top-level service
                if in_hms and "healthcheck:" in line:
                    hc_lines_left = 8  # scan next 8 lines for timing props
                if in_hms and hc_lines_left > 0:
                    hc_lines_left -= 1
                    if "retries:" in line:
                        lines[i] = _re.sub(r'retries: \d+', 'retries: 30', line)
                    if "start_period:" in line:
                        lines[i] = _re.sub(r'start_period: \S+', 'start_period: 90s', line)
            txt = "\n".join(lines)

            # Ranger pre-provision: the rokku Ranger image's setup connects
            # directly as the 'ranger' app user and expects its DB + role to
            # already exist (its companion postgres normally provisions them).
            # Mount a postgres initdb script that creates the ranger role + db
            # on first init — runs before Ranger's setup connects.
            if "ranger-admin" in self.resolved and "postgres" in self.resolved:
                plines = txt.splitlines()
                in_pg = False
                for i, line in enumerate(plines):
                    if line.startswith("  postgres:"):
                        in_pg = True
                    elif in_pg and line.startswith("  ") and not line.startswith("    "):
                        in_pg = False
                    if in_pg and line.strip() == "volumes:":
                        plines.insert(
                            i + 1,
                            "    - ./config/postgres/init-ranger.sql:"
                            "/docker-entrypoint-initdb.d/10-ranger.sql:ro")
                        break
                txt = "\n".join(plines)
                pg_init = d / "config/postgres/init-ranger.sql"
                pg_init.parent.mkdir(parents=True, exist_ok=True)
                pg_init.write_text(
                    "DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE "
                    "rolname='ranger') THEN CREATE ROLE ranger LOGIN PASSWORD "
                    "'security'; END IF; END $$;\n"
                    "SELECT 'CREATE DATABASE ranger OWNER ranger' WHERE NOT EXISTS "
                    "(SELECT FROM pg_database WHERE datname='ranger')\\gexec\n")
                self._log("  wrote postgres init-ranger.sql (ranger role+db)")

            compose_path.write_text(txt)
            self._log("  patched docker-compose.yml (airflow/pgbouncer/hive/yarn)")

        # ── yarn-site.xml: bind RM webapp to 0.0.0.0 ───────────────────────
        yarn_xml = d / "config/hadoop/yarn-site.xml"
        if yarn_xml.exists():
            y = yarn_xml.read_text()
            if "webapp.address" not in y:
                y = y.replace(
                    "</configuration>",
                    "  <property>\n"
                    "    <name>yarn.resourcemanager.webapp.address</name>\n"
                    "    <value>0.0.0.0:8088</value>\n"
                    "  </property>\n</configuration>")
                yarn_xml.write_text(y)
                self._log("  patched yarn-site.xml (webapp.address=0.0.0.0:8088)")

        # ── hive-metastore-site.xml: XML-escape & in JDBC URL ───────────────
        hms_xml = d / "hive-metastore-site.xml"
        if hms_xml.exists():
            h = hms_xml.read_text()
            if "ConnectionDriverName" not in h:
                # AI generated incomplete XML — rewrite with all 4 required JDO props
                h = (
                    '<?xml version="1.0" encoding="UTF-8" standalone="no"?>\n'
                    '<configuration>\n'
                    '  <property><name>javax.jdo.option.ConnectionURL</name>'
                    '<value>jdbc:mysql://udp-mysql-hms:3306/metastore?createDatabaseIfNotExist=true'
                    '&amp;useSSL=false&amp;allowPublicKeyRetrieval=true</value></property>\n'
                    '  <property><name>javax.jdo.option.ConnectionDriverName</name>'
                    '<value>com.mysql.cj.jdbc.Driver</value></property>\n'
                    '  <property><name>javax.jdo.option.ConnectionUserName</name>'
                    '<value>hive</value></property>\n'
                    '  <property><name>javax.jdo.option.ConnectionPassword</name>'
                    '<value>hive_password_pilot</value></property>\n'
                    '  <property><name>metastore.thrift.uris</name>'
                    '<value>thrift://localhost:9083</value></property>\n'
                    '  <property><name>metastore.warehouse.dir</name>'
                    '<value>hdfs://udp-hdfs:8020/warehouse</value></property>\n'
                    '  <property><name>metastore.expression.proxy</name>'
                    '<value>org.apache.hadoop.hive.metastore.DefaultPartitionExpressionProxy</value></property>\n'
                    '  <property><name>metastore.task.threads.always</name>'
                    '<value>org.apache.hadoop.hive.metastore.events.EventCleanerTask</value></property>\n'
                    '</configuration>\n'
                )
            else:
                import re as re2
                h = re2.sub(r'&(?![a-zA-Z#][a-zA-Z0-9#]*;)', '&amp;', h)
                if "expression.proxy" not in h:
                    h = h.replace(
                        "</configuration>",
                        '  <property><name>metastore.expression.proxy</name>'
                        '<value>org.apache.hadoop.hive.metastore.DefaultPartitionExpressionProxy</value></property>\n'
                        '  <property><name>metastore.task.threads.always</name>'
                        '<value>org.apache.hadoop.hive.metastore.events.EventCleanerTask</value></property>\n'
                        '</configuration>')
            hms_xml.write_text(h)
            self._log("  patched hive-metastore-site.xml")

        # ── hiveserver2-site.xml: embedded MySQL mode ───────────────────────
        hs2_xml = d / "config/hive/hiveserver2-site.xml"
        hs2_xml.parent.mkdir(parents=True, exist_ok=True)
        hs2_xml.write_text(
            '<?xml version="1.0" encoding="UTF-8" standalone="no"?>\n'
            '<configuration>\n'
            '  <property><name>hive.metastore.uris</name><value></value></property>\n'
            '  <property><name>javax.jdo.option.ConnectionURL</name>'
            '<value>jdbc:mysql://udp-mysql-hms:3306/metastore?createDatabaseIfNotExist=true'
            '&amp;useSSL=false&amp;allowPublicKeyRetrieval=true</value></property>\n'
            '  <property><name>javax.jdo.option.ConnectionDriverName</name>'
            '<value>com.mysql.cj.jdbc.Driver</value></property>\n'
            '  <property><name>javax.jdo.option.ConnectionUserName</name>'
            '<value>hive</value></property>\n'
            '  <property><name>javax.jdo.option.ConnectionPassword</name>'
            '<value>hive_password_pilot</value></property>\n'
            '  <property><name>hive.metastore.schema.verification</name><value>false</value></property>\n'
            '  <property><name>hive.server2.thrift.port</name><value>10000</value></property>\n'
            '  <property><name>hive.server2.authentication</name><value>NONE</value></property>\n'
            '  <property><name>hive.server2.enable.doAs</name><value>false</value></property>\n'
            '  <property><name>hive.server2.webui.enabled</name><value>false</value></property>\n'
            '  <property><name>hive.metastore.warehouse.dir</name>'
            '<value>hdfs://udp-hdfs:8020/warehouse</value></property>\n'
            '  <property><name>hive.metastore.event.db.notification.api.auth</name><value>false</value></property>\n'
            '  <property><name>hive.exec.scratchdir</name><value>/tmp/hive</value></property>\n'
            '  <property><name>hive.execution.engine</name><value>mr</value></property>\n'
            '  <property><name>hive.server2.tez.initialize.default.sessions</name><value>false</value></property>\n'
            '  <property><name>hive.materializedview.rewriting</name><value>false</value></property>\n'
            '  <property><name>hive.metastore.transactional.event.listeners</name><value></value></property>\n'
            '  <property><name>metastore.transactional.event.listeners</name><value></value></property>\n'
            '</configuration>\n'
        )
        self._log("  wrote hiveserver2-site.xml (embedded MySQL mode)")

        # ── config/hive/hive-site.xml for Spark — must be a FILE not a dir ────
        # Docker creates a directory when the mount target doesn't exist.
        # Spark variants always mount this file; write HMS pointer only when
        # HMS is in the stack, else a minimal empty config (Iceberg/Nessie
        # stacks don't use HMS but the mount target must still exist).
        resolved = set(self.resolved or [])
        has_hms_ = "hive-metastore" in resolved
        hive_site = d / "config/hive/hive-site.xml"
        if hive_site.is_dir():
            import shutil as _shutil
            _shutil.rmtree(hive_site)
        hive_site.parent.mkdir(parents=True, exist_ok=True)
        if has_hms_:
            hive_site.write_text(
                '<?xml version="1.0" encoding="UTF-8"?>\n'
                '<configuration>\n'
                '  <property><name>hive.metastore.uris</name>'
                '<value>thrift://udp-hive-metastore:9083</value></property>\n'
                '  <property><name>hive.metastore.warehouse.dir</name>'
                '<value>hdfs://udp-hdfs:8020/warehouse</value></property>\n'
                '</configuration>\n'
            )
            self._log("  wrote config/hive/hive-site.xml (Spark HMS pointer)")
        elif not hive_site.exists() or hive_site.stat().st_size == 0:
            hive_site.write_text(
                '<?xml version="1.0" encoding="UTF-8"?>\n<configuration>\n</configuration>\n'
            )
            self._log("  wrote config/hive/hive-site.xml (empty — no HMS in stack)")

        # ── Spark + Iceberg REST catalog: deterministic spark-defaults.conf ──
        # AI-generated spark configs are unreliable; write the proven config.
        if "spark-iceberg" in resolved and "iceberg-rest" in resolved:
            spark_conf = d / "config/spark/spark-defaults.conf"
            spark_conf.parent.mkdir(parents=True, exist_ok=True)
            spark_conf.write_text(
                "spark.sql.extensions org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions\n"
                "spark.sql.catalog.iceberg org.apache.iceberg.spark.SparkCatalog\n"
                "spark.sql.catalog.iceberg.type rest\n"
                "spark.sql.catalog.iceberg.uri http://udp-iceberg-rest:8181\n"
                "spark.sql.catalog.iceberg.io-impl org.apache.iceberg.aws.s3.S3FileIO\n"
                "spark.sql.catalog.iceberg.warehouse s3://warehouse\n"
                "spark.sql.catalog.iceberg.s3.endpoint http://udp-minio:9000\n"
                "spark.sql.catalog.iceberg.s3.path-style-access true\n"
                "spark.sql.catalog.iceberg.s3.region us-east-1\n"
                "spark.sql.catalog.iceberg.client.region us-east-1\n"
                "spark.sql.defaultCatalog iceberg\n"
                "spark.hadoop.fs.s3a.endpoint http://udp-minio:9000\n"
                "spark.hadoop.fs.s3a.access.key admin\n"
                "spark.hadoop.fs.s3a.secret.key udp_admin_12345\n"
                "spark.hadoop.fs.s3a.path.style.access true\n"
                "spark.hadoop.fs.s3a.impl org.apache.hadoop.fs.s3a.S3AFileSystem\n"
            )
            self._log("  wrote spark-defaults.conf (Iceberg REST + MinIO)")

        # ── Spark + Nessie catalog: deterministic spark-defaults.conf ────────
        if "spark-iceberg" in resolved and "nessie" in resolved:
            spark_conf = d / "config/spark/spark-defaults.conf"
            spark_conf.parent.mkdir(parents=True, exist_ok=True)
            spark_conf.write_text(
                "spark.sql.extensions org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions\n"
                "spark.sql.catalog.nessie org.apache.iceberg.spark.SparkCatalog\n"
                "spark.sql.catalog.nessie.catalog-impl org.apache.iceberg.nessie.NessieCatalog\n"
                "spark.sql.catalog.nessie.uri http://udp-nessie:19120/api/v1\n"
                "spark.sql.catalog.nessie.ref main\n"
                "spark.sql.catalog.nessie.io-impl org.apache.iceberg.aws.s3.S3FileIO\n"
                "spark.sql.catalog.nessie.warehouse s3://warehouse\n"
                "spark.sql.catalog.nessie.s3.endpoint http://udp-minio:9000\n"
                "spark.sql.catalog.nessie.s3.path-style-access true\n"
                "spark.sql.catalog.nessie.s3.region us-east-1\n"
                "spark.sql.catalog.nessie.client.region us-east-1\n"
                "spark.sql.defaultCatalog nessie\n"
                "spark.hadoop.fs.s3a.endpoint http://udp-minio:9000\n"
                "spark.hadoop.fs.s3a.access.key admin\n"
                "spark.hadoop.fs.s3a.secret.key udp_admin_12345\n"
                "spark.hadoop.fs.s3a.path.style.access true\n"
                "spark.hadoop.fs.s3a.impl org.apache.hadoop.fs.s3a.S3AFileSystem\n"
            )
            self._log("  wrote spark-defaults.conf (Nessie + MinIO)")

        # ── Trino iceberg catalog (REST) — deterministic ────────────────────
        if "trino" in resolved and "iceberg-rest" in resolved:
            ice_props = d / "config/trino/catalog/iceberg.properties"
            ice_props.parent.mkdir(parents=True, exist_ok=True)
            ice_props.write_text(
                "connector.name=iceberg\n"
                "iceberg.catalog.type=rest\n"
                "iceberg.rest-catalog.uri=http://udp-iceberg-rest:8181\n"
                "iceberg.rest-catalog.warehouse=s3://warehouse\n"
                "fs.native-s3.enabled=true\n"
                "s3.endpoint=http://udp-minio:9000\n"
                "s3.aws-access-key=admin\n"
                "s3.aws-secret-key=udp_admin_12345\n"
                "s3.path-style-access=true\n"
                "s3.region=us-east-1\n"
            )
            self._log("  wrote trino iceberg.properties (REST catalog)")

        # ── Trino iceberg catalog (Nessie) — deterministic ──────────────────
        if "trino" in resolved and "nessie" in resolved:
            nes_props = d / "config/trino/catalog/iceberg.properties"
            nes_props.parent.mkdir(parents=True, exist_ok=True)
            nes_props.write_text(
                "connector.name=iceberg\n"
                "iceberg.catalog.type=nessie\n"
                "iceberg.nessie-catalog.uri=http://udp-nessie:19120/api/v1\n"
                "iceberg.nessie-catalog.default-warehouse-dir=s3://warehouse\n"
                "fs.native-s3.enabled=true\n"
                "s3.endpoint=http://udp-minio:9000\n"
                "s3.aws-access-key=admin\n"
                "s3.aws-secret-key=udp_admin_12345\n"
                "s3.path-style-access=true\n"
                "s3.region=us-east-1\n"
            )
            self._log("  wrote trino iceberg.properties (Nessie catalog)")

        # ── MySQL JDBC driver for Hive ───────────────────────────────────────
        jar_dest = d / "hive-jars/mysql-connector-java.jar"
        if not jar_dest.exists():
            jar_dest.parent.mkdir(parents=True, exist_ok=True)
            import subprocess
            subprocess.run(
                ["docker", "cp",
                 "udp-hive-metastore:/opt/apache-hive-metastore-3.0.0-bin/lib/mysql-connector-java-8.0.19.jar",
                 str(jar_dest)],
                capture_output=True)
            if jar_dest.exists():
                self._log("  copied MySQL driver for Hive")

        # ── Trino hive catalog: new s3.* props (Trino 400+) ─────────────────
        hive_props = d / "config/trino/catalog/hive.properties"
        if "trino" in resolved and not has_hms_ and hive_props.exists():
            # No HMS in stack — a hive catalog would crash Trino on startup.
            hive_props.unlink()
            self._log("  removed stray trino hive.properties (no HMS in stack)")
        elif hive_props.exists() and "hive.s3.aws-access-key" in hive_props.read_text():
            hive_props.write_text(
                "connector.name=hive\n"
                "hive.metastore.uri=thrift://udp-hive-metastore:9083\n"
                "hive.metastore.thrift.retries=5\n"
                "fs.native-s3.enabled=true\n"
                "s3.endpoint=http://udp-minio:9000\n"
                "s3.aws-access-key=admin\n"
                "s3.aws-secret-key=udp_admin_12345\n"
                "s3.path-style-access=true\n"
            )
            self._log("  patched trino hive.properties (s3.* props for Trino 400+)")

    async def _phase_post_cfg(self) -> None:
        """Run post-startup initialization commands generated by AI.

        Handles things like:
          - Hive Metastore schema initialization (schematool -initSchema)
          - HDFS directory creation
          - Airflow DB migration + admin user creation
          - Superset DB init + admin user creation
        """
        commands = list(self.config_plan.get("post_start_commands", []))

        guaranteed = []

        # Always run HDFS directory setup when HDFS is in the stack
        if "hdfs" in self.resolved:
            guaranteed.append({
                "description": "HDFS directory setup & permissions",
                "container": "udp-hdfs",
                "command": (
                    "hdfs dfs -mkdir -p /user/root /user/spark /user/hive /tmp /warehouse "
                    "/apps/tez /mr-history/done /mr-history/tmp && "
                    "hdfs dfs -chmod -R 1777 /tmp && "
                    "hdfs dfs -chmod 777 /warehouse && "
                    "hdfs dfs -chmod 755 /apps && "
                    "echo 'HDFS dirs ready'"
                ),
            })

        # Always upgrade HMS schema from 3.0 -> 4.x when hive is in the stack.
        # bitsondatadev/hive-metastore initialises schema at 3.0; Hive 4.x needs 4.0.0.
        if "hive" in self.resolved and "hive-metastore" in self.resolved:
            guaranteed.append({
                "description": "Upgrade HMS schema 3.0 → 4.0.0 (required for Hive 4.x)",
                "container": "udp-hive",
                "command": (
                    "/opt/hive/bin/schematool "
                    "-dbType mysql "
                    "-url 'jdbc:mysql://udp-mysql-hms:3306/metastore?useSSL=false&allowPublicKeyRetrieval=true' "
                    "-driver com.mysql.cj.jdbc.Driver "
                    "-userName hive "
                    "-passWord hive_password_pilot "
                    "-upgradeSchemaFrom 3.0.0 2>&1 | tail -3 || true"
                ),
            })

        # OpenLineage (Marquez): its bundled marquez.dev.yml HARDCODES db/user/
        # password = marquez (only POSTGRES_HOST/PORT come from env). The shared
        # udp-postgres only has the 'lakehouse' role/db, so Marquez crash-loops on
        # "password authentication failed for user marquez". Create the role + db
        # it expects; the container's restart policy then self-recovers.
        if "openlineage" in self.resolved and "postgres" in self.resolved:
            guaranteed.append({
                "description": "Bootstrap marquez role + database in postgres",
                "container": "udp-postgres",
                "command": (
                    "psql -U lakehouse -d lakehouse -v ON_ERROR_STOP=0 -c "
                    "\"DO \\$\\$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE "
                    "rolname='marquez') THEN CREATE ROLE marquez LOGIN PASSWORD "
                    "'marquez'; END IF; END \\$\\$;\" && "
                    "psql -U lakehouse -d lakehouse -tc \"SELECT 1 FROM pg_database "
                    "WHERE datname='marquez'\" | grep -q 1 || "
                    "psql -U lakehouse -d lakehouse -c \"CREATE DATABASE marquez "
                    "OWNER marquez;\"; "
                    "echo 'marquez db ready' || true"
                ),
            })

        # StarRocks + Iceberg REST: create external iceberg_catalog so StarRocks
        # can read Iceberg tables. Runs from inside the StarRocks container.
        if "starrocks" in self.resolved and "iceberg-rest" in self.resolved:
            guaranteed.append({
                "description": "Create StarRocks iceberg_catalog (REST)",
                "container": "udp-starrocks",
                "command": (
                    "mysql -h 127.0.0.1 -P 9030 -u root --connect-timeout=30 -e \""
                    "CREATE EXTERNAL CATALOG IF NOT EXISTS iceberg_catalog PROPERTIES ("
                    "'type'='iceberg',"
                    "'iceberg.catalog.type'='rest',"
                    "'iceberg.catalog.uri'='http://udp-iceberg-rest:8181',"
                    "'iceberg.catalog.warehouse'='s3://warehouse',"
                    "'aws.s3.endpoint'='http://udp-minio:9000',"
                    "'aws.s3.access_key'='admin',"
                    "'aws.s3.secret_key'='udp_admin_12345',"
                    "'aws.s3.enable_path_style_access'='true'"
                    ");\" 2>&1 | tail -3 || true"
                ),
            })

        # ── Demo pipeline: generate CSV → land → Spark ingest to Iceberg.
        # Works for both the REST catalog (name 'iceberg') and Nessie ('nessie').
        if "spark-iceberg" in self.resolved and (
            "iceberg-rest" in self.resolved or "nessie" in self.resolved):
            import base64 as _b64
            cat = "nessie" if "nessie" in self.resolved else "iceberg"
            seed_py = (
                "import random\n"
                "from pyspark.sql import SparkSession\n"
                "spark = SparkSession.builder.appName('DemoSeed').getOrCreate()\n"
                "spark.sparkContext.setLogLevel('WARN')\n"
                "random.seed(42)\n"
                "products=['Laptop','Phone','Tablet','Monitor','Keyboard','Mouse','Headset','Webcam','SSD','USB Hub']\n"
                "customers=['Alice','Bob','Charlie','Diana','Eve','Frank','Grace','Hank','Ivy','Jack']\n"
                "statuses=['shipped','pending','delivered','returned']\n"
                "rows=[]\n"
                "for i in range(1,201):\n"
                "    q=random.randint(1,10); p=round(random.uniform(10,999),2)\n"
                "    rows.append((i, random.choice(customers), random.choice(products), q, round(q*p,2), random.choice(statuses)))\n"
                "cols=['order_id','customer','product','quantity','total','status']\n"
                "df=spark.createDataFrame(rows, cols)\n"
                "df.write.mode('overwrite').option('header','true').csv('file:///tmp/raw/orders')\n"
                "print('LANDED raw CSV -> /tmp/raw/orders')\n"
                "raw=spark.read.option('header','true').option('inferSchema','true').csv('file:///tmp/raw/orders')\n"
                f"spark.sql('CREATE NAMESPACE IF NOT EXISTS {cat}.demo')\n"
                f"raw.writeTo('{cat}.demo.orders').createOrReplace()\n"
                f"print('INGESTED', spark.table('{cat}.demo.orders').count(), 'rows -> {cat}.demo.orders')\n"
                "spark.stop()\n"
            )
            b64 = _b64.b64encode(seed_py.encode()).decode()
            guaranteed.append({
                "description": f"Demo pipeline: CSV → Spark → Iceberg ({cat}.demo.orders)",
                "container": "udp-spark-iceberg",
                "command": (
                    f"echo {b64} | base64 -d > /tmp/demo_seed.py && "
                    "/opt/spark/bin/spark-submit --master 'local[2]' /tmp/demo_seed.py 2>&1 "
                    "| grep -E 'LANDED|INGESTED|Error|Exception' | tail -8"
                ),
            })
            # Verify StarRocks can read the freshly-seeded table
            if "starrocks" in self.resolved:
                guaranteed.append({
                    "description": "Verify StarRocks reads iceberg_catalog.demo.orders",
                    "container": "udp-starrocks",
                    "command": (
                        "mysql -h 127.0.0.1 -P 9030 -u root --connect-timeout=30 -e \""
                        "SELECT COUNT(*) AS demo_orders_rows FROM iceberg_catalog.demo.orders;"
                        "SELECT * FROM iceberg_catalog.demo.orders ORDER BY order_id LIMIT 5;"
                        "\" 2>&1 | tail -12 || true"
                    ),
                })

        # Prepend guaranteed steps before AI-generated commands
        commands = guaranteed + commands

        if not commands:
            self._log("No post-start commands — skipping initialization")
            return

        self._log(f"Waiting 45s for services to stabilize before post-start init…")
        await asyncio.sleep(45)

        self._log(f"Running {len(commands)} post-start initialization command(s)…")
        for item in commands:
            if not isinstance(item, dict):
                continue
            desc = item.get("description", "init step")
            container = item.get("container", "")
            command = item.get("command", "")
            if not container or not command:
                self._log(f"  ⚠ Skipping malformed command: {item}")
                continue

            self._log(f"  → [{desc}]")
            self._log(f"    docker exec {container} {command[:80]}")
            if self.is_remote:
                # Run `docker exec <container> /bin/sh -c '<command>'` on the remote.
                import shlex as _shlex
                remote_cmd = f"docker exec {container} /bin/sh -c {_shlex.quote(command)}"
                rc, output = await self._exec_capture(self._wrap_remote(remote_cmd))
                output = output.strip()
            else:
                cmd = ["docker", "exec", container, "/bin/sh", "-c", command]
                rc, output = await self._exec_capture(cmd)
                output = output.strip()
            if rc == 0:
                self._log(f"    ✓ done")
            else:
                # Don't fail the entire stack on post-init errors — some
                # init commands are idempotent and may return non-zero on re-runs
                self._log(f"    ⚠ exited {rc} (may be OK if already initialized)")
                if output:
                    for line in output.splitlines()[-5:]:
                        self._log(f"      {line}")

    async def _verify_health(self) -> None:
        # docker compose ps — basic container state (local or remote)
        if self.is_remote:
            _, out = await self._exec_capture(
                self._wrap_remote("docker compose -f docker-compose.yml ps"))
        else:
            _, out = await self._exec_capture([
                "docker", "compose",
                "-f", str(self.install_dir / "docker-compose.yml"),
                "ps",
            ])
        for line in out.splitlines():
            self._log(line)

        # Connectivity checks from AI plan
        checks = self.config_plan.get("connectivity_checks", [])
        if not checks:
            return

        self._log(f"Running {len(checks)} connectivity check(s)…")
        passed = 0
        for check in checks:
            if not isinstance(check, dict):
                continue
            name = check.get("name", "check")
            command = check.get("command", "")
            if not command:
                continue
            # connectivity_checks come from the AI config plan — untrusted.
            # Gate each command before it reaches a (local or remote) shell.
            safe, reason = ai_safety.vet_provisioning_command(command)
            if not safe:
                self._log(f"  ⛔ {name}: refused unsafe check ({reason})")
                continue
            if self.is_remote:
                rc, _out2 = await self._exec_capture(self._wrap_remote(command))
                out2 = _out2.encode()
            else:
                proc2 = await asyncio.create_subprocess_shell(
                    command,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.STDOUT,
                )
                assert proc2.stdout is not None
                out2, _ = await proc2.communicate()
                rc = proc2.returncode
            if rc == 0:
                passed += 1
                self._log(f"  ✓ {name}")
            else:
                detail = out2.decode().strip()
                self._log(f"  ✗ {name}")
                if detail:
                    self._log(f"    {detail[:120]}")

        self._log(f"Connectivity: {passed}/{len(checks)} checks passed")
