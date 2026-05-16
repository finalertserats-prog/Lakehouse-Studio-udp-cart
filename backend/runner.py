from __future__ import annotations
import asyncio
import os
import platform
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

from .config import WORK_DIR
from .events import bus
from .models import LogEvent, StepStatus
from .redact import redact, sanitize_env_overrides, quote_env_value, SECRET_KEYS
from .stack_manifest import StackManifest
from .state import store


_STUDIO_BOOTSTRAP_SH = r"""#!/usr/bin/env bash
# Studio-owned bootstrap. Replaces UDP's scripts/bootstrap.sh because that
# script hard-requires hive-metastore which Studio's v0.3 pilot deliberately
# doesn't ship. This version uses only MinIO + Iceberg-REST + Spark + StarRocks.
set -euo pipefail

# Prevent Git Bash on Windows from converting Unix-style /home/... paths
# into C:/Program Files/Git/home/... before passing them to docker exec.
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

echo "[studio-bootstrap] waiting for Iceberg REST..."
for i in $(seq 1 60); do
  if curl -fsS http://localhost:8181/v1/config >/dev/null 2>&1; then
    echo "  iceberg-rest OK"; break
  fi
  echo "  ($i/60) iceberg-rest not ready yet"; sleep 2
done

echo "[studio-bootstrap] waiting for StarRocks FE..."
for i in $(seq 1 60); do
  if docker exec udp-starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root -e "SELECT 1" >/dev/null 2>&1; then
    echo "  starrocks-fe OK"; break
  fi
  echo "  ($i/60) starrocks-fe not ready yet"; sleep 5
done

echo "[studio-bootstrap] registering StarRocks backend..."
docker exec udp-starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root -e \
  "ALTER SYSTEM ADD BACKEND 'starrocks-be:9050';" 2>&1 | grep -v "already exists" || true

echo "[studio-bootstrap] running Spark bootstrap job (REST-catalog)..."
# Use double-leading-slash on the path so Git Bash on Windows definitively
# doesn't path-convert it. Linux/macOS bash treats // as / so this is safe.
docker exec udp-spark spark-submit //home/iceberg/jobs/bootstrap_demo_lake.py

echo "[studio-bootstrap] creating StarRocks REST catalog (3.3.12+ props)..."
# StarRocks 3.3.12+ fixed PR #55416 — Iceberg REST catalog properties now
# correctly propagate to the S3 FileIO. Required additions vs earlier 3.3.x:
#   - iceberg.catalog.warehouse: explicit warehouse path
#   - iceberg.catalog.vended-credentials-enabled=false: MinIO can't vend
#   - aws.s3.enable_ssl=false: plain HTTP MinIO
docker exec -i udp-starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root <<'SQL'
DROP CATALOG IF EXISTS iceberg_rest_catalog;
CREATE EXTERNAL CATALOG iceberg_rest_catalog
PROPERTIES (
    "type" = "iceberg",
    "iceberg.catalog.type" = "rest",
    "iceberg.catalog.uri" = "http://iceberg-rest:8181",
    "iceberg.catalog.warehouse" = "s3://datalake/warehouse",
    "iceberg.catalog.vended-credentials-enabled" = "false",
    "aws.s3.endpoint" = "http://minio:9000",
    "aws.s3.enable_ssl" = "false",
    "aws.s3.enable_path_style_access" = "true",
    "aws.s3.region" = "us-east-1",
    "aws.s3.access_key" = "admin",
    "aws.s3.secret_key" = "udp_admin_12345"
);
SQL

echo "[studio-bootstrap] creating app_analytics views (REST-backed)..."
docker exec -i udp-starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root <<'SQL'
CREATE DATABASE IF NOT EXISTS app_analytics;
DROP VIEW IF EXISTS app_analytics.demo_customer_summary;
CREATE VIEW app_analytics.demo_customer_summary AS
SELECT region, customer_count, total_order_amount, curated_timestamp
FROM iceberg_rest_catalog.curated.demo_customer_summary;
SQL

echo "[studio-bootstrap] complete"
"""


_STUDIO_SMOKE_SH = r"""#!/usr/bin/env bash
# Studio-owned smoke test. Replaces UDP's scripts/smoke-test.sh because that
# script also hard-requires hive-metastore. Validates the same things:
#   - Iceberg raw + curated tables readable from Spark (via REST catalog)
#   - StarRocks can SHOW CATALOGS, SHOW DATABASES, and query the
#     app_analytics view
set -euo pipefail

export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

echo "[studio-smoke] checking Iceberg REST..."
curl -fsS http://localhost:8181/v1/config >/dev/null || { echo "iceberg-rest unreachable"; exit 1; }
echo "  iceberg-rest OK"

echo "[studio-smoke] checking StarRocks FE..."
docker exec udp-starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root -e "SELECT 1" >/dev/null || { echo "starrocks-fe unreachable"; exit 1; }
echo "  starrocks-fe OK"

echo "[studio-smoke] running Spark Iceberg smoke job..."
docker exec udp-spark spark-submit //home/iceberg/jobs/smoke_test_iceberg.py

echo "[studio-smoke] StarRocks queries..."
docker exec udp-starrocks-fe mysql -h 127.0.0.1 -P 9030 -u root -e \
  "SHOW CATALOGS; SHOW DATABASES; SELECT COUNT(*) AS customer_summary_rows FROM app_analytics.demo_customer_summary;"

echo "[studio-smoke] passed"
"""


def _build_steps(stack: StackManifest) -> list[StepStatus]:
    return [
        StepStatus(id="prepare", title="Prepare workspace"),
        StepStatus(id="clone", title="Clone UDP repository"),
        StepStatus(id="env", title="Write .env file"),
        StepStatus(id="doctor", title="Run doctor checks"),
        StepStatus(id="start", title="Start stack (docker compose up)"),
        StepStatus(id="bootstrap", title="Bootstrap demo lakehouse"),
        StepStatus(id="smoke", title="Run smoke tests"),
        StepStatus(id="finalize", title="Capture outputs"),
    ]


def _bash_executable() -> str:
    bash = shutil.which("bash")
    if not bash:
        raise RuntimeError(
            "bash not found in PATH. Install Git Bash (Windows) or any POSIX bash."
        )
    return bash


def _to_posix_path(p: Path) -> str:
    """On Windows, bash needs /c/Users/... not C:\\Users\\...

    Guards: only handle absolute drive-letter paths (C:\\…). Refuses UNC
    (\\\\server\\share) and long-path-prefixed (\\\\?\\) paths; falls back to
    the raw string for non-Windows.
    """
    if platform.system() != "Windows":
        return str(p)
    s = str(Path(p).resolve())
    # UNC / long-path / weird: bail out by returning the original string.
    # Bash inside Git for Windows can usually handle forward-slashed paths.
    if s.startswith("\\\\") or len(s) < 3 or s[1] != ":":
        return s.replace("\\", "/")
    drive = s[0].lower()
    rest = s[2:].replace("\\", "/")
    if not rest.startswith("/"):
        rest = "/" + rest
    return f"/{drive}{rest}"


# Env vars to pass to child subprocesses. Keep the surface small; explicitly
# drop credentials present in the parent process env (CI tokens, AWS keys, etc.).
_ENV_ALLOW = {
    "PATH", "HOME", "USER", "USERNAME", "USERPROFILE", "LANG", "LC_ALL", "TZ",
    "TMP", "TEMP", "TMPDIR",
    # Docker on Windows / WSL
    "DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH",
    # MSYS / Git Bash
    "MSYSTEM", "MSYS", "MSYSTEM_PREFIX", "MINGW_PREFIX",
    # Locale needed by docker compose
    "COLUMNS", "LINES", "TERM",
    # systemroot is needed for various Windows shell utilities
    "SYSTEMROOT", "SYSTEMDRIVE", "COMSPEC", "WINDIR", "PROGRAMFILES", "PROGRAMFILES(X86)",
}


def _build_subprocess_env() -> dict[str, str]:
    src = os.environ
    out = {k: v for k, v in src.items() if k in _ENV_ALLOW or k.startswith("LHS_")}
    out["PYTHONUNBUFFERED"] = "1"
    out["GIT_TERMINAL_PROMPT"] = "0"
    # docker compose v2 needs HOME
    out.setdefault("HOME", src.get("HOME", src.get("USERPROFILE", "")))
    return out


class UDPRunner:
    def __init__(self, stack: StackManifest, install_id: str, host: str, install_dir: Path):
        self.stack = stack
        self.install_id = install_id
        self.host = host
        self.install_dir = install_dir
        self._proc: Optional[asyncio.subprocess.Process] = None
        self._cancel = False

    # ---------- event helpers ----------

    def _emit(self, kind: str, **kwargs) -> None:
        evt = LogEvent(install_id=self.install_id, ts=time.time(), kind=kind, **kwargs)  # type: ignore[arg-type]
        bus.publish_nowait(evt)

    def _step_start(self, step_id: str) -> None:
        store.update_step(self.install_id, step_id, status="running", started_at=time.time())
        self._emit("step_start", step=step_id, status="running")

    def _step_end(self, step_id: str, success: bool, exit_code: int = 0, message: Optional[str] = None) -> None:
        status = "success" if success else "failed"
        store.update_step(
            self.install_id, step_id,
            status=status, finished_at=time.time(),
            exit_code=exit_code, message=message,
        )
        self._emit("step_end", step=step_id, status=status, payload={"exit_code": exit_code, "message": message})

    def _log(self, step_id: str, stream: str, line: str) -> None:
        self._emit("log", step=step_id, stream=stream, line=redact(line))  # type: ignore[arg-type]

    def _set_state(self, state: str) -> None:
        store.update_state(self.install_id, state)  # type: ignore[arg-type]
        self._emit("state", status=state)

    # ---------- subprocess plumbing ----------

    async def _run_bash(self, step_id: str, argv: list[str], cwd: Path, timeout: int) -> int:
        """Run a command under bash so UDP's shell scripts work cross-platform."""
        bash = _bash_executable()
        posix_cwd = _to_posix_path(cwd)
        quoted = " ".join(self._sh_quote(a) for a in argv)
        cmd_str = f"cd {self._sh_quote(posix_cwd)} && {quoted}"

        # Redact the echoed command in case argv contains a credential.
        self._log(step_id, "stdout", redact(f"$ {cmd_str}"))

        env = _build_subprocess_env()

        proc = await asyncio.create_subprocess_exec(
            bash, "-c", cmd_str,  # no -l: don't source user profile
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
        self._proc = proc

        async def _drain(stream: asyncio.StreamReader, kind: str) -> None:
            try:
                while True:
                    raw = await stream.readline()
                    if not raw:
                        return
                    try:
                        text = raw.decode("utf-8", errors="replace").rstrip("\r\n")
                    except Exception:
                        text = repr(raw)
                    self._log(step_id, kind, text)
            except asyncio.CancelledError:
                return

        drain_out = asyncio.create_task(_drain(proc.stdout, "stdout"))  # type: ignore[arg-type]
        drain_err = asyncio.create_task(_drain(proc.stderr, "stderr"))  # type: ignore[arg-type]

        timed_out = False
        try:
            await asyncio.wait_for(proc.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            timed_out = True
            self._log(step_id, "stderr", f"[timeout after {timeout}s; killing]")
            try:
                proc.kill()
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(proc.wait(), timeout=10)
            except asyncio.TimeoutError:
                pass
        finally:
            # Always drain to EOF, even on timeout or cancel.
            for t in (drain_out, drain_err):
                try:
                    await asyncio.wait_for(t, timeout=5)
                except asyncio.TimeoutError:
                    t.cancel()
                except Exception:
                    pass
            if self._proc is proc:
                self._proc = None

        if timed_out:
            return 124
        rc = proc.returncode
        return rc if rc is not None else 1

    @staticmethod
    def _sh_quote(s: str) -> str:
        if not s or any(c in s for c in " \t\"'\\$`!|&;()<>*?[]{}"):
            return "'" + s.replace("'", "'\\''") + "'"
        return s

    async def cancel(self) -> None:
        self._cancel = True
        if self._proc is not None:
            try:
                self._proc.kill()
            except ProcessLookupError:
                pass

    # ---------- pipeline steps ----------

    async def _step_prepare(self) -> bool:
        self._step_start("prepare")
        try:
            self.install_dir.parent.mkdir(parents=True, exist_ok=True)
            self._log("prepare", "stdout", f"workspace: {self.install_dir}")
            self._step_end("prepare", True)
            return True
        except Exception as e:
            self._step_end("prepare", False, message=str(e))
            return False

    async def _step_clone(self) -> bool:
        self._step_start("clone")
        repo = self.stack.repository
        url = repo.get("url")
        ref = repo.get("ref", "main")
        if (self.install_dir / ".git").exists():
            self._log("clone", "stdout", f"existing repo at {self.install_dir}, pulling latest")
            rc = await self._run_bash("clone", ["git", "fetch", "origin", ref], self.install_dir, timeout=120)
            if rc != 0:
                self._step_end("clone", False, exit_code=rc, message="git fetch failed")
                return False
            rc = await self._run_bash("clone", ["git", "checkout", ref], self.install_dir, timeout=60)
            if rc != 0:
                self._step_end("clone", False, exit_code=rc, message="git checkout failed")
                return False
            rc = await self._run_bash("clone", ["git", "reset", "--hard", f"origin/{ref}"], self.install_dir, timeout=60)
            ok = rc == 0
            self._step_end("clone", ok, exit_code=rc)
            return ok
        # Clone fresh into install_dir.parent then move; simpler: clone directly into install_dir
        self.install_dir.parent.mkdir(parents=True, exist_ok=True)
        rc = await self._run_bash(
            "clone",
            ["git", "clone", "--branch", ref, "--depth", "1", url, _to_posix_path(self.install_dir)],
            cwd=self.install_dir.parent,
            timeout=300,
        )
        ok = rc == 0
        self._step_end("clone", ok, exit_code=rc)
        return ok

    # Defaults for env vars referenced by UDP's compose but tied to optional
    # services (hms / ranger) we don't ship in the v0.3 cart. Setting them to
    # empty strings silences the "variable is not set" warnings; the services
    # themselves either get a docker-compose profile gate (in UDP) or fail
    # quietly without breaking the services we DO want.
    _SAFE_DEFAULTS = {
        "HMS_DB_NAME": "metastore",
        "HMS_DB_USER": "hive",
        "HMS_DB_PASSWORD": "hive",
        "RANGER_DB_NAME": "ranger",
        "RANGER_DB_USER": "ranger",
        "RANGER_DB_PASSWORD": "ranger",
    }

    def _patch_compose_images(self) -> None:
        """Rewrite the cloned UDP docker-compose.yml so it matches our cart:
          1. Update every `image: <repo>:<tag>` to the catalog's pinned tag
             (UDP upstream can drift; this keeps installs reproducible)
          2. Strip `depends_on` edges pointing at services that aren't in
             our cart (UDP includes enterprise services like hive-metastore
             and ranger that we don't ship; their dep edges would force
             docker compose to bring them up even when we don't ask for them)
        Idempotent — running twice is a no-op. Logs every change."""
        import re
        compose_path = self.install_dir / "docker-compose.yml"
        if not compose_path.exists():
            return
        text = compose_path.read_text(encoding="utf-8")
        original = text

        # ---- (1) image tag rewrites ----
        image_replacements: list[tuple[str, str]] = []
        for comp in self.stack.components:
            image = comp.get("image")
            if not image or ":" not in image:
                continue
            repo, _new_tag = image.rsplit(":", 1)
            pattern = re.compile(
                rf"^(\s*image:\s*){re.escape(repo)}:[^\s#]+",
                re.MULTILINE,
            )
            new_text, n = pattern.subn(rf"\g<1>{image}", text)
            if n:
                image_replacements.append((repo, image))
                text = new_text

        # ---- (2) prune depends_on entries for services not in our cart ----
        wanted_services = {c.get("service_name") for c in self.stack.components if c.get("service_name")}
        start_cmd = self.stack.data.get("commands", {}).get("start", {}) or {}
        wanted_services.update(start_cmd.get("extra_services") or [])

        dep_removals: list[str] = []
        # Match a single `<svc>:\n      condition: service_<state>\n` block inside a depends_on:
        dep_block_re = re.compile(
            r"^(?P<indent> {6,})(?P<svc>[a-z][a-z0-9_-]*):\n"
            r"\s+condition:\s*service_(?:healthy|started|completed_successfully)\s*\n",
            re.MULTILINE,
        )
        def _maybe_strip(m: re.Match) -> str:
            svc = m.group("svc")
            if svc in wanted_services or svc in ("create-bucket",):
                return m.group(0)
            dep_removals.append(svc)
            return ""
        text = dep_block_re.sub(_maybe_strip, text)

        # Remove `depends_on:` lines whose children were ALL pruned.
        text = re.sub(
            r"^(?P<indent> {4,})depends_on:\s*\n(?=(?P=indent)[a-z]|^[a-z])",
            "",
            text,
            flags=re.MULTILINE,
        )

        # Patch StarRocks FE startup with:
        #   - priority_networks (FE refuses leader election on Docker Desktop
        #     without it because the IP changes between restarts)
        #   - AWS_REGION / AWS_ENDPOINT_URL_S3 / etc env vars (empirically
        #     needed even on 3.3.12 — catalog property propagation doesn't
        #     fully cover the SDK default-credentials/region/endpoint chain
        #     when querying Iceberg-on-MinIO)
        # Same env vars also injected into BE in _patch_compose_be (below).
        fe_old = r"/opt/starrocks/fe/bin/start_fe.sh --daemon"
        fe_new = (
            r'echo "priority_networks = 172.16.0.0/12" >> /opt/starrocks/fe/conf/fe.conf'
            '\n        export AWS_REGION=us-east-1'
            '\n        export AWS_ACCESS_KEY_ID=admin'
            '\n        export AWS_SECRET_ACCESS_KEY=udp_admin_12345'
            '\n        export AWS_ENDPOINT_URL_S3=http://minio:9000'
            '\n        export AWS_S3_US_EAST_1_REGIONAL_ENDPOINT=regional'
            '\n        /opt/starrocks/fe/bin/start_fe.sh --daemon'
        )
        if fe_old in text and fe_new not in text:
            text = text.replace(fe_old, fe_new, 1)

        # Same env-var injection for BE (it needs the SDK config too for any
        # actual S3 read during query execution).
        be_old = r"/opt/starrocks/be/bin/start_be.sh --daemon"
        be_new = (
            r'echo "priority_networks = 172.16.0.0/12" >> /opt/starrocks/be/conf/be.conf'
            '\n        export AWS_REGION=us-east-1'
            '\n        export AWS_ACCESS_KEY_ID=admin'
            '\n        export AWS_SECRET_ACCESS_KEY=udp_admin_12345'
            '\n        export AWS_ENDPOINT_URL_S3=http://minio:9000'
            '\n        export AWS_S3_US_EAST_1_REGIONAL_ENDPOINT=regional'
            '\n        /opt/starrocks/be/bin/start_be.sh --daemon'
        )
        # Note: UDP's compose already has `echo "priority_networks..." >> be.conf`
        # before `start_be.sh --daemon`. To avoid double-prepending, match the
        # original line WITHOUT our priority_networks prefix.
        be_existing_re = re.compile(
            r'echo "priority_networks = 172\.16\.0\.0/12" >> /opt/starrocks/be/conf/be\.conf\s*\n\s*'
            r'/opt/starrocks/be/bin/start_be\.sh --daemon'
        )
        if be_existing_re.search(text):
            text = be_existing_re.sub(be_new, text, count=1)

        # Downgrade `condition: service_healthy` → `condition: service_started`.
        # Several UDP images ship broken healthchecks (iceberg-rest's check
        # calls `wget` which isn't in the image, starrocks-fe takes minutes
        # to pass on first boot). Downgrading lets `docker compose up -d`
        # return after services START rather than waiting for healthchecks
        # that may never pass. The bootstrap step has its own wait-for
        # logic so we don't lose the readiness guarantee.
        text, healthy_to_started = re.subn(
            r"condition:\s*service_healthy",
            "condition: service_started",
            text,
        )

        if text != original:
            compose_path.write_text(text, encoding="utf-8")
            for repo, image in image_replacements:
                self._log("env", "stdout", f"compose image: {repo} -> {image}")
            if dep_removals:
                # Dedupe and report
                seen = []
                for d in dep_removals:
                    if d not in seen: seen.append(d)
                self._log("env", "stdout",
                          f"compose deps pruned (not in cart): {', '.join(seen)}")
            if healthy_to_started:
                self._log("env", "stdout",
                          f"compose: downgraded {healthy_to_started} 'service_healthy' deps to 'service_started' (UDP upstream healthchecks unreliable; bootstrap step has its own readiness gate)")

    def _patch_spark_defaults(self) -> None:
        """Repoint Spark's default `udp` catalog from hive-metastore to
        iceberg-REST. UDP's spark-defaults.conf configures `udp` for HMS
        and `udp_rest` for REST in parallel; we replace the HMS lines so
        the bootstrap job (hardcoded to use `udp`) runs against REST."""
        cfg = self.install_dir / "config" / "spark" / "spark-defaults.conf"
        if not cfg.exists():
            return
        text = cfg.read_text(encoding="utf-8")
        original = text
        # Replace the 3 hive-specific lines for the `udp` catalog
        replacements = [
            ("spark.sql.catalog.udp.type=hive", "spark.sql.catalog.udp.type=rest"),
            ("spark.sql.catalog.udp.uri=thrift://hive-metastore:9083",
             "spark.sql.catalog.udp.uri=http://iceberg-rest:8181"),
            ("spark.sql.catalog.udp.warehouse=s3a://datalake/warehouse",
             "spark.sql.catalog.udp.warehouse=s3://datalake/warehouse"),
        ]
        for old, new in replacements:
            text = text.replace(old, new)
        if text != original:
            cfg.write_text(text, encoding="utf-8")
            self._log("env", "stdout", "spark-defaults.conf: repointed 'udp' catalog from HMS to REST")

    def _write_studio_bootstrap(self) -> None:
        """Drop Studio-owned bootstrap + smoke scripts into the install dir's
        scripts/ directory. Replace UDP's equivalents which hard-require
        hive-metastore. Studio's scripts use ONLY the services we ship:
        MinIO + Iceberg REST + Spark + StarRocks."""
        scripts_dir = self.install_dir / "scripts"
        scripts_dir.mkdir(parents=True, exist_ok=True)
        for name, body in (("lhs-bootstrap.sh", _STUDIO_BOOTSTRAP_SH),
                           ("lhs-smoke.sh",     _STUDIO_SMOKE_SH)):
            path = scripts_dir / name
            path.write_text(body, encoding="utf-8")
            try:
                path.chmod(0o755)
            except Exception:
                pass

    async def _step_env(self, overrides: dict[str, str]) -> bool:
        self._step_start("env")
        env_path = self.install_dir / ".env"

        # ---- patch the cloned UDP repo with our catalog's pinned image versions ----
        # UDP's docker-compose.yml may carry stale image tags upstream
        # (caught in pilot: tabulario/spark-iceberg:3.5.1_1.5.2 was removed
        # from Docker Hub). We override every image to whatever the catalog
        # currently certifies, so an out-of-date UDP clone still installs
        # cleanly. Bonus: this is what makes the cart's component versions
        # actually mean something (closes part of the Gemini "guided
        # illusion" gap for image tags specifically).
        try:
            self._patch_compose_images()
        except Exception as e:
            self._log("env", "stderr", f"compose image patch warning: {e}")

        # Sanitize user overrides; reject anything dangerous outright.
        clean_overrides, rejections = sanitize_env_overrides(overrides)
        for r in rejections:
            self._log("env", "stderr", f"rejected override {r}")
        if rejections and not clean_overrides:
            # If everything was rejected and nothing came through, still proceed
            # with defaults — but tell the user.
            pass

        # Defaults are trusted (from the manifest), but quote them too for safety.
        # _SAFE_DEFAULTS supplies dummy values for env vars referenced by
        # optional UDP services we don't ship (hms/ranger) — silences
        # docker-compose's "variable not set" warnings on every command.
        merged: dict[str, str] = {**self._SAFE_DEFAULTS, **self.stack.env_defaults, **clean_overrides}

        # Patch Spark's catalog config: swap the default `udp` catalog from
        # hive-metastore-backed to iceberg-REST-backed so the Spark bootstrap
        # job works without hive-metastore. UDP ships a parallel `udp_rest`
        # catalog already configured for REST — we redirect `udp` at the same
        # endpoint so the bootstrap job (which hardcodes catalog name `udp`)
        # runs unmodified.
        try:
            self._patch_spark_defaults()
        except Exception as e:
            self._log("env", "stderr", f"spark-defaults patch warning: {e}")

        # Write Studio's own bootstrap script that uses REST catalog only.
        # The manifest's `bootstrap` command points at this script via
        # `./scripts/lhs-bootstrap.sh`.
        try:
            self._write_studio_bootstrap()
        except Exception as e:
            self._log("env", "stderr", f"studio bootstrap write warning: {e}")

        # Make UDP scripts executable. On Windows chmod is a near-noop, but on
        # Linux/macOS it matters. Don't swallow surprising errors silently.
        try:
            for name in ("udp",):
                p = self.install_dir / name
                if p.exists():
                    p.chmod(p.stat().st_mode | 0o111)
            scripts_dir = self.install_dir / "scripts"
            if scripts_dir.is_dir():
                for p in scripts_dir.glob("*.sh"):
                    p.chmod(p.stat().st_mode | 0o111)
        except Exception as e:
            self._log("env", "stderr", f"chmod warning: {e}")

        try:
            lines = [f"{k}={quote_env_value(v)}" for k, v in merged.items()]
            env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            try:
                env_path.chmod(0o600)
            except Exception:
                pass
            # Echo redacted preview line-by-line.
            for k, v in merged.items():
                is_secret = (
                    k in SECRET_KEYS
                    or "PASSWORD" in k.upper()
                    or "SECRET" in k.upper()
                    or "TOKEN" in k.upper()
                )
                shown = ("********" if v else "(empty)") if is_secret else v
                self._log("env", "stdout", f"{k}={shown}")
            self._step_end("env", True)
            return True
        except Exception as e:
            self._step_end("env", False, message=str(e))
            return False

    async def _step_cmd(self, step_id: str, cmd_name: str) -> bool:
        self._step_start(step_id)
        try:
            spec = self.stack.command(cmd_name)
        except KeyError as e:
            self._step_end(step_id, False, message=str(e))
            return False

        # Special command type: docker_compose_up with explicit service list
        # built from the stack's components. Lets us skip enterprise services
        # that UDP's compose includes by default (hive-metastore, ranger).
        if spec.get("type") == "docker_compose_up":
            services: list[str] = []
            for comp in self.stack.components:
                sn = comp.get("service_name")
                if sn:
                    services.append(sn)
            services.extend(spec.get("extra_services") or [])
            if not services:
                self._step_end(step_id, False, message="no services to start (cart empty?)")
                return False
            argv = ["docker", "compose", "up", "-d"] + services
        else:
            argv = list(spec["argv"])

        rc = await self._run_bash(step_id, argv, self.install_dir, int(spec.get("timeout", 600)))
        ok = rc == 0
        self._step_end(step_id, ok, exit_code=rc)
        return ok

    async def _step_finalize(self) -> bool:
        self._step_start("finalize")
        urls = self.stack.output_urls(self.host)
        conns = self.stack.output_connections(self.host)
        outputs = {"urls": urls, "connections": conns}
        store.set_outputs(self.install_id, outputs)
        self._emit("result", payload=outputs)
        # Capture evidence: result.json, system-info.json, full-log.txt
        evidence_ok = True
        try:
            from .evidence import capture
            rec = store.get(self.install_id)
            if rec:
                out_dir = capture(rec)
                outputs["evidence_dir"] = str(out_dir)
                store.set_outputs(self.install_id, outputs)
                self._log("finalize", "stdout", f"evidence captured: {out_dir}")
        except Exception as e:
            evidence_ok = False
            self._log("finalize", "stderr", f"evidence capture failed: {e}")
        # Step is success only if evidence wrote cleanly; stack is still READY either way.
        self._step_end("finalize", evidence_ok,
                       message=None if evidence_ok else "evidence capture failed (stack is still READY)")
        return evidence_ok

    # ---------- top-level orchestration ----------

    # Ordered sequence: (step_id, state_to_enter_before_running, callable_factory).
    # Each callable_factory takes the runner + overrides and returns a coroutine.
    _PIPELINE: list[tuple[str, str]] = [
        ("prepare",   "CLONING_REPO"),
        ("clone",     "CLONING_REPO"),
        ("env",       "WRITING_ENV"),
        ("doctor",    "RUNNING_DOCTOR"),
        ("start",     "STARTING_STACK"),
        ("bootstrap", "BOOTSTRAPPING"),
        ("smoke",     "SMOKE_TESTING"),
        ("finalize",  "READY"),
    ]

    # Steps the user is allowed to Skip (the install can still complete).
    SKIPPABLE = frozenset({"smoke", "finalize"})

    async def _execute_step(self, step_id: str, env_overrides: dict[str, str]) -> bool:
        """Dispatch a single step. Used by both initial run and retry."""
        if step_id == "prepare":   return await self._step_prepare()
        if step_id == "clone":     return await self._step_clone()
        if step_id == "env":       return await self._step_env(env_overrides)
        if step_id == "doctor":    return await self._step_cmd("doctor", "doctor")
        if step_id == "start":     return await self._step_cmd("start", "start")
        if step_id == "bootstrap": return await self._step_cmd("bootstrap", "bootstrap")
        if step_id == "smoke":     return await self._step_cmd("smoke", "smoke")
        if step_id == "finalize":  return await self._step_finalize()
        raise ValueError(f"unknown step: {step_id}")

    def _step_index(self, step_id: str) -> int:
        for i, (sid, _) in enumerate(self._PIPELINE):
            if sid == step_id: return i
        return -1

    async def run(self, env_overrides: dict[str, str], *, start_at: str = "prepare") -> None:
        """Run the pipeline starting at `start_at` (default = beginning).

        On the first run this drives all steps. On a Retry, the caller passes
        the failed step id as start_at; on Skip, the caller passes the NEXT
        step id; rollback runs ./udp clean instead.
        """
        try:
            self._set_state("INSPECTING")  # caller did the inspection already
            self._set_state("READY_TO_INSTALL")

            start_idx = self._step_index(start_at)
            if start_idx < 0:
                return self._fail(f"unknown start step: {start_at}")

            for step_id, state in self._PIPELINE[start_idx:]:
                if self._cancel:
                    return self._fail("cancelled")
                # Don't downgrade state — but READY is the terminal of finalize
                if state != "READY":
                    self._set_state(state)
                ok = await self._execute_step(step_id, env_overrides)
                if not ok:
                    # finalize failing means evidence didn't write, stack is still up
                    if step_id == "finalize":
                        self._set_state("READY")
                        self._emit("state", status="READY")
                        return
                    return self._fail(f"{step_id} failed")

            self._set_state("READY")
            self._emit("state", status="READY")
        except asyncio.CancelledError:
            self._fail("cancelled")
        except Exception as e:
            self._fail(f"unexpected: {e}")

    def _fail(self, msg: str) -> None:
        store.update_state(self.install_id, "FAILED", error=msg)
        self._emit("state", status="FAILED", payload={"error": msg})
        self._emit("error", line=msg)


def make_steps(stack: StackManifest) -> list[StepStatus]:
    return _build_steps(stack)


def next_step_id(stack: StackManifest, step_id: str) -> str | None:
    pipeline = [sid for sid, _ in UDPRunner._PIPELINE]
    try:
        i = pipeline.index(step_id)
    except ValueError:
        return None
    return pipeline[i + 1] if i + 1 < len(pipeline) else None


async def retry_install(stack: StackManifest, install_id: str, host: str, install_dir: Path,
                        env_overrides: dict[str, str], start_at: str) -> None:
    """Resume a failed install from `start_at`. Resets the chosen step (and
    everything after) to pending before re-running so the UI updates cleanly.
    """
    rec = store.get(install_id)
    if not rec:
        return
    # Reset everything from start_at onward to pending
    pipeline = [sid for sid, _ in UDPRunner._PIPELINE]
    if start_at not in pipeline:
        return
    cutover = pipeline.index(start_at)
    for s in rec.steps:
        if s.id in pipeline and pipeline.index(s.id) >= cutover:
            s.status = "pending"
            s.started_at = None
            s.finished_at = None
            s.exit_code = None
            s.message = None
    rec.error = None
    store._persist()
    runner = UDPRunner(stack, install_id, host, install_dir)
    await runner.run(env_overrides, start_at=start_at)


def mark_step_skipped(install_id: str, step_id: str) -> str | None:
    """Mark a step as skipped (only allowed for SKIPPABLE steps). Return the next step id, or None."""
    if step_id not in UDPRunner.SKIPPABLE:
        return None
    rec = store.get(install_id)
    if not rec:
        return None
    for s in rec.steps:
        if s.id == step_id:
            s.status = "skipped"
            s.message = "user-skipped"
            break
    store._persist()
    return next_step_id_for(step_id)


def next_step_id_for(step_id: str) -> str | None:
    pipeline = [sid for sid, _ in UDPRunner._PIPELINE]
    try:
        i = pipeline.index(step_id)
    except ValueError:
        return None
    return pipeline[i + 1] if i + 1 < len(pipeline) else None


async def run_command(install_id: str, install_dir: Path, host: str, stack: StackManifest, cmd_name: str) -> int:
    """One-shot command for stop/clean/status, with logs piped through the event bus."""
    runner = UDPRunner(stack, install_id, host, install_dir)
    runner._step_start(cmd_name)
    try:
        spec = stack.command(cmd_name)
    except KeyError as e:
        runner._step_end(cmd_name, False, message=str(e))
        return 1
    rc = await runner._run_bash(cmd_name, list(spec["argv"]), install_dir, int(spec.get("timeout", 300)))
    runner._step_end(cmd_name, rc == 0, exit_code=rc)
    return rc
