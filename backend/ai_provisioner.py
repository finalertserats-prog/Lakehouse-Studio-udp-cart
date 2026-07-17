"""AI-powered end-to-end lakehouse provisioner.

Phases:
  1. research    – AI recommends compatible versions (compat_ai)
  2. gen_config  – AI generates all configs (LiteLLM)
  3. install     – Full docker-compose install (UDPRunner)
  4. post_cfg    – Inject Trino catalogs + extra config post-start
  5. verify      – Connectivity checks via docker exec
  6. summary     – Connection details + pipeline example
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any, Callable

from dotenv import load_dotenv

load_dotenv()

import litellm

from . import compat_ai
from .stack_manifest import load_manifest, StackManifest

log = logging.getLogger("lhs.ai_provisioner")
litellm.suppress_debug_info = True

_BASE_URL = os.environ.get("LITELLM_BASE_URL", "")
_API_KEY  = os.environ.get("LITELLM_API_KEY", "")
_MODEL    = os.environ.get("LITELLM_MODEL", "gpt-4o-mini")

# Global job registry
_PROV_JOBS: dict[str, "ProvisionJob"] = {}

PHASES = [
    ("research",    "AI: Research Compatible Versions"),
    ("gen_config",  "AI: Generate All Configurations"),
    ("install",     "Install Stack (docker compose)"),
    ("post_cfg",    "AI: Apply Post-Start Configs"),
    ("verify",      "AI: Verify Service Connectivity"),
    ("summary",     "Lakehouse Ready — Connection Details"),
]

# ---------------------------------------------------------------------------
# Topology helpers
# ---------------------------------------------------------------------------

def _build_topology(stack: StackManifest, selected_versions: dict[str, str]) -> dict:
    services = []
    for comp in stack.components:
        svc = {
            "id":        comp["id"],
            "name":      comp.get("name", comp["id"]),
            "container": f"udp-{comp.get('service_name', comp['id'])}",
            "version":   selected_versions.get(comp["id"]) or comp.get("version", "latest"),
            "ports":     comp.get("ports", []),
            "category":  comp.get("category", ""),
        }
        services.append(svc)
    return {"stack_id": stack.id, "stack_name": stack.name, "services": services}


def _extract_addon_version(composite: str) -> str:
    """'3.5.5_1.2.0' → '1.2.0'. Handles plain version strings too."""
    parts = composite.split("_", 1)
    return parts[1] if len(parts) == 2 else composite


# ---------------------------------------------------------------------------
# LLM config generation
# ---------------------------------------------------------------------------

_STACK_HINTS: dict[str, str] = {
    "hudi-hms-spark-local-v0.1": (
        "Apache Hudi on Spark with Hive Metastore (MySQL-backed). "
        "Spark writes Hudi COPY_ON_WRITE tables to MinIO s3a://datalake/warehouse. "
        "spark-hudi component version is composite Spark_Hudi (e.g. 3.5.5_1.2.0). "
        "Maven: org.apache.hudi:hudi-spark3.5-bundle_2.12:{hudi_ver}. "
        "No Trino in this stack — Spark-only read/write."
    ),
    "delta-hms-spark-trino-local-v0.1": (
        "Delta Lake on Spark with Hive Metastore (MySQL-backed) + Trino query engine. "
        "Delta JARs are PRE-BAKED into the lakehousestudio/spark-delta image — do NOT add spark.jars.packages for delta. "
        "spark-delta version is composite Spark_Delta (e.g. 3.5.5_3.3.2). "
        "Trino needs a delta_lake catalog pointing at HMS thrift://hive-metastore:9083. "
        "Include trino_catalog_files.delta.properties."
    ),
    "iceberg-nessie-trino-local-v0.1": (
        "Apache Iceberg on Spark with Nessie REST catalog + Trino query engine. "
        "Nessie REST for Iceberg: http://nessie:19120/iceberg/main. "
        "Trino needs iceberg catalog pointing at Nessie REST. "
        "Maven: org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{iceberg_ver} "
        "and org.apache.iceberg:iceberg-aws-bundle:{iceberg_ver}. "
        "Include trino_catalog_files.iceberg.properties."
    ),
    "udp-trino-local-v0.1": (
        "Iceberg REST catalog (iceberg-rest:8181) + Trino. No HMS. "
        "Trino iceberg catalog points at http://iceberg-rest:8181. "
        "Include trino_catalog_files.iceberg.properties."
    ),
    "iceberg-polaris-spark-local-v0.1": (
        "Apache Iceberg with Apache Polaris catalog + Spark. "
        "Polaris REST: http://polaris:8181. "
        "Spark connects to Polaris for catalog management. "
        "Maven: org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{iceberg_ver}."
    ),
}


def _build_config_prompt(
    stack: StackManifest,
    topology: dict,
    selected_versions: dict[str, str],
) -> str:
    ver_lines = "\n".join(f"  {k}: {v}" for k, v in sorted(selected_versions.items()))
    svc_lines = "\n".join(
        f"  {s['name']} — container={s['container']}, ports={s['ports']}"
        for s in topology["services"]
    )

    hudi_ver    = _extract_addon_version(selected_versions.get("spark-hudi",    "3.5.5_1.2.0"))
    delta_ver   = _extract_addon_version(selected_versions.get("spark-delta",   "3.5.5_3.3.2"))
    iceberg_ver = _extract_addon_version(selected_versions.get("spark-iceberg", "3.5.5_1.8.1"))
    trino_ver   = selected_versions.get("trino", "481")
    hint        = _STACK_HINTS.get(stack.id, "")

    return f"""You are an expert data lakehouse engineer.

Configure this stack: {stack.name}  (id: {stack.id})
Context: {hint}

Selected versions:
{ver_lines}

Docker service topology (services communicate by container name on the shared docker network):
{svc_lines}

Fixed credentials (do not change):
  MinIO endpoint:     http://minio:9000
  MinIO access_key:   admin
  MinIO secret_key:   udp_admin_12345
  MinIO bucket:       datalake
  MySQL HMS host:     mysql-hms:3306
  MySQL db/user/pass: metastore / hive / hive_password_pilot
  HMS Thrift URI:     thrift://hive-metastore:9083
  Nessie REST:        http://nessie:19120
  Iceberg REST:       http://iceberg-rest:8181
  Polaris REST:       http://polaris:8181

Version hints for Spark 3.5 / Scala 2.12 Maven artifacts:
  Hudi {hudi_ver}:    org.apache.hudi:hudi-spark3.5-bundle_2.12:{hudi_ver}
  Delta {delta_ver}:  io.delta:delta-spark_2.12:{delta_ver}
  Iceberg {iceberg_ver}: org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{iceberg_ver}
  Iceberg AWS:        org.apache.iceberg:iceberg-aws-bundle:{iceberg_ver}
  Hadoop-AWS (S3A):   org.apache.hadoop:hadoop-aws:3.3.4
  Trino {trino_ver} does NOT use spark packages — it is configured via catalog .properties files only.

Generate ALL configuration. Return ONLY valid JSON, no markdown fences:
{{
  "spark_defaults_append": "newline-separated key=value lines to APPEND to spark-defaults.conf",
  "trino_catalog_files": {{
    "iceberg.properties": "complete file content",
    "delta.properties":   "complete file content"
  }},
  "post_start_commands": [
    {{"desc": "...", "cmd": "shell command using docker exec"}}
  ],
  "connectivity_checks": [
    {{"name": "MinIO",       "cmd": "curl -fsS http://localhost:9000/minio/health/live >/dev/null"}},
    {{"name": "HMS Thrift",  "cmd": "docker exec udp-hive-metastore bash -c 'echo > /dev/tcp/127.0.0.1/9083'"}}
  ],
  "connection_info": {{
    "spark_master_ui":  "http://HOST:8888",
    "minio_console":    "http://HOST:9001",
    "minio_endpoint":   "http://HOST:9000",
    "trino_ui":         "http://HOST:8080",
    "hms_thrift":       "thrift://HOST:9083"
  }},
  "pipeline_example": "brief PySpark or SQL snippet showing how to write/read the first table",
  "reasoning": "two-sentence explanation of key version and config decisions"
}}

Rules:
- spark_defaults_append MUST include: s3a filesystem config (endpoint/creds/impl), catalog extensions class, warehouse path
- Only include trino_catalog_files keys for catalogs this stack actually has
- Only include HMS connectivity_checks if this stack has hive-metastore
- post_start_commands: only ADDITIONAL steps NOT already handled by the standard bootstrap scripts
- For Delta: do NOT include spark.jars.packages for delta — JARs are baked into the image
- For connection_info: only include URLs for services this stack exposes
- CRITICAL JSON FORMAT: NEVER use triple-quotes (\"\"\" or ''') inside JSON values. Use \\n for newlines. Use SINGLE QUOTES for all Python/SQL strings inside pipeline_example (e.g. appName('test') NOT appName(\"test\")).
"""


def _call_llm(prompt: str) -> dict:
    kwargs: dict[str, Any] = {
        "model":       _MODEL,
        "messages":    [{"role": "user", "content": prompt}],
        "temperature": 0.1,
        "max_tokens":  2000,
    }
    if _BASE_URL:
        kwargs["base_url"] = _BASE_URL
    if _API_KEY:
        kwargs["api_key"] = _API_KEY

    response = litellm.completion(**kwargs)
    raw = response.choices[0].message.content.strip()
    # Strip markdown fences
    raw = re.sub(r'^```(?:json)?\s*', '', raw)
    raw = re.sub(r'\s*```$', '', raw.rstrip())
    # Replace triple double-quotes (Python docstring style) with triple single-quotes
    # — LLM often generates """SQL""" inside JSON strings which is invalid JSON
    raw = raw.replace('"""', "'''")
    # Fix remaining bare unescaped double-quotes inside string values:
    # pattern: a non-escaped " that follows a non-" non-\ char inside a string value
    # Safest approach: use a lenient JSON parser fallback
    raw = re.sub(r',\s*([}\]])', r'\1', raw)  # trailing commas
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        # Fallback: extract just the known safe fields via regex
        result: dict[str, Any] = {}
        for field in ("spark_defaults_append", "reasoning"):
            m = re.search(rf'"{field}"\s*:\s*"((?:[^"\\]|\\.)*)"', raw, re.DOTALL)
            if m:
                result[field] = m.group(1).replace("\\n", "\n")
        for field in ("trino_catalog_files", "post_start_commands",
                      "connectivity_checks", "connection_info"):
            # Try to parse each field individually
            m = re.search(rf'"{field}"\s*:\s*(\{{[^}}]*\}}|\[[^\]]*\])', raw, re.DOTALL)
            if m:
                try:
                    result[field] = json.loads(m.group(1))
                except Exception:
                    pass
        if not result:
            raise  # re-raise original if nothing recovered
        return result


# ---------------------------------------------------------------------------
# Config application helpers
# ---------------------------------------------------------------------------

def _apply_spark_defaults(install_dir: Path, lines: str) -> None:
    cfg = install_dir / "config" / "spark" / "spark-defaults.conf"
    if not cfg.exists():
        log.warning("spark-defaults.conf not found at %s — skipping", cfg)
        return
    existing = cfg.read_text(encoding="utf-8")
    additions = []
    for line in lines.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key = line.split("=", 1)[0].strip()
        if key and key not in existing:
            additions.append(line)
    if additions:
        cfg.write_text(
            existing.rstrip() + "\n\n# --- AI Provisioner additions ---\n" + "\n".join(additions) + "\n",
            encoding="utf-8",
        )
        log.info("spark-defaults: appended %d AI-generated lines", len(additions))


_VERSION_PATCHES = [
    # Hudi: hudi-spark3.5-bundle_2.12:0.15.0 → correct version
    (re.compile(r'(hudi-spark3\.\d-bundle_2\.12:)[\d.]+'), "hudi-spark-bundle"),
    # Delta JAR filenames
    (re.compile(r'(delta-spark_2\.12[-:])([\d.]+)(\.jar)'), "delta-spark-jar"),
    (re.compile(r'(delta-storage-)([\d.]+)(\.jar)'), "delta-storage-jar"),
    # Iceberg
    (re.compile(r'(iceberg-spark-runtime-3\.5_2\.12:)[\d.]+'), "iceberg-runtime"),
    (re.compile(r'(iceberg-aws-bundle:)[\d.]+'), "iceberg-aws"),
]


def _patch_script_versions(install_dir: Path, stack_id: str, selected_versions: dict[str, str]) -> None:
    """Patch hardcoded version strings in bootstrap/smoke scripts to match cart selections."""
    hudi_ver    = _extract_addon_version(selected_versions.get("spark-hudi",    ""))
    delta_ver   = _extract_addon_version(selected_versions.get("spark-delta",   ""))
    iceberg_ver = _extract_addon_version(selected_versions.get("spark-iceberg", ""))

    scripts_dir = install_dir / "scripts"
    if not scripts_dir.is_dir():
        return

    for script_path in scripts_dir.glob("*.sh"):
        text = script_path.read_text(encoding="utf-8")
        original = text

        if hudi_ver:
            text = re.sub(
                r'hudi-spark3\.\d-bundle_2\.12:[\d.]+',
                f'hudi-spark3.5-bundle_2.12:{hudi_ver}',
                text,
            )
        if delta_ver:
            text = re.sub(
                r'delta-spark_2\.12-([\d.]+)(\.jar)',
                lambda m, v=delta_ver: f'delta-spark_2.12-{v}{m.group(2)}',
                text,
            )
            text = re.sub(
                r'delta-storage-([\d.]+)(\.jar)',
                lambda m, v=delta_ver: f'delta-storage-{v}{m.group(2)}',
                text,
            )
            text = re.sub(
                r'io\.delta:delta-spark_2\.12:[\d.]+',
                f'io.delta:delta-spark_2.12:{delta_ver}',
                text,
            )
        if iceberg_ver:
            text = re.sub(
                r'iceberg-spark-runtime-3\.5_2\.12:[\d.]+',
                f'iceberg-spark-runtime-3.5_2.12:{iceberg_ver}',
                text,
            )
            text = re.sub(
                r'iceberg-aws-bundle:[\d.]+',
                f'iceberg-aws-bundle:{iceberg_ver}',
                text,
            )

        if text != original:
            script_path.write_text(text, encoding="utf-8")
            log.info("patched version strings in %s", script_path.name)


def _run_post_start_commands(commands: list[dict], emit_log: Callable) -> None:
    for item in commands:
        desc = item.get("desc", "")
        cmd  = item.get("cmd", "")
        if not cmd:
            continue
        emit_log(f"  running: {desc or cmd}")
        try:
            result = subprocess.run(
                cmd, shell=True, capture_output=True, text=True, timeout=60
            )
            if result.returncode == 0:
                emit_log(f"  ✓ {desc or 'done'}")
            else:
                emit_log(f"  ⚠ {desc}: {result.stderr.strip()[:200]}")
        except subprocess.TimeoutExpired:
            emit_log(f"  ⚠ {desc}: timed out after 60s")
        except Exception as e:
            emit_log(f"  ⚠ {desc}: {e}")


def _inject_trino_catalogs(catalog_files: dict[str, str], emit_log: Callable) -> None:
    for name, content in catalog_files.items():
        if not name.endswith(".properties"):
            name = name + ".properties"
        emit_log(f"  writing Trino catalog: {name}")
        try:
            escaped = content.replace("'", "'\"'\"'")
            cmd = (
                f"docker exec udp-trino mkdir -p /data/trino/etc/catalog && "
                f"docker exec -i udp-trino bash -c 'cat > /data/trino/etc/catalog/{name}' "
                f"<<'__TRINOEOF__'\n{content}\n__TRINOEOF__"
            )
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30)
            if result.returncode == 0:
                emit_log(f"  ✓ {name} written")
            else:
                emit_log(f"  ⚠ {name}: {result.stderr.strip()[:200]}")
        except Exception as e:
            emit_log(f"  ⚠ {name}: {e}")

    if catalog_files:
        emit_log("  restarting Trino to load new catalogs…")
        try:
            subprocess.run("docker restart udp-trino", shell=True, timeout=30)
            time.sleep(8)
            emit_log("  ✓ Trino restarted")
        except Exception as e:
            emit_log(f"  ⚠ Trino restart: {e}")


def _run_connectivity_checks(checks: list[dict], emit_log: Callable) -> list[dict]:
    results = []
    for check in checks:
        name = check.get("name", "?")
        cmd  = check.get("cmd", "")
        if not cmd:
            continue
        try:
            r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=15)
            ok = r.returncode == 0
        except Exception:
            ok = False
        icon = "✓" if ok else "✗"
        emit_log(f"  {icon} {name}")
        results.append({"name": name, "ok": ok})
    return results


# ---------------------------------------------------------------------------
# ProvisionJob — tracks phases + streams SSE
# ---------------------------------------------------------------------------

class ProvisionJob:
    def __init__(
        self,
        job_id:           str,
        stack_id:         str,
        cart_selections:  dict[str, str],
        install_options:  dict,
    ):
        self.job_id          = job_id
        self.stack_id        = stack_id
        self.cart_selections = dict(cart_selections)
        self.install_options = dict(install_options)
        self.state           = "pending"
        self.current_phase   = ""
        self.phase_states: dict[str, str] = {p[0]: "pending" for p in PHASES}
        self._events: asyncio.Queue = asyncio.Queue()
        self._config_plan: dict = {}
        self._connection_info: dict = {}
        self._pipeline_example: str = ""
        self._reasoning: str = ""

    # --- event plumbing ---

    def _emit(self, kind: str, **payload) -> None:
        self._events.put_nowait({"kind": kind, "ts": time.time(), **payload})

    def _log(self, line: str) -> None:
        self._emit("log", line=line)

    def _phase_start(self, phase_id: str) -> None:
        self.current_phase = phase_id
        self.phase_states[phase_id] = "active"
        label = next((p[1] for p in PHASES if p[0] == phase_id), phase_id)
        self._emit("phase_start", phase=phase_id, label=label)
        self._log(f"\n▶ {label}")

    def _phase_end(self, phase_id: str, success: bool, message: str = "") -> None:
        self.phase_states[phase_id] = "done" if success else "failed"
        self._emit("phase_end", phase=phase_id, success=success, message=message)
        if not success and message:
            self._log(f"✗ {message}")

    async def stream_events(self):
        """Async generator — yields raw SSE data strings."""
        while True:
            try:
                event = await asyncio.wait_for(self._events.get(), timeout=30)
            except asyncio.TimeoutError:
                yield "data: {\"kind\":\"heartbeat\"}\n\n"
                if self.state in ("done", "failed"):
                    break
                continue
            yield f"data: {json.dumps(event)}\n\n"
            if event.get("kind") == "provision_complete":
                break

    # --- phase implementations ---

    async def _phase_research(self) -> dict[str, str]:
        """Call compat_ai to get recommended versions for all components."""
        self._phase_start("research")
        try:
            stack = load_manifest(self.stack_id)
            anchor_id = self.cart_selections.get("_anchor", "")
            anchor_ver = ""

            # Pick the most meaningful anchor component
            for pref in ("spark-hudi", "spark-delta", "spark-iceberg", "spark", "hdfs"):
                if pref in self.cart_selections:
                    anchor_id = pref
                    anchor_ver = self.cart_selections[pref]
                    break

            if not anchor_id or not anchor_ver:
                self._log("  no clear anchor — using cart selections as-is")
                self._phase_end("research", True)
                return self.cart_selections

            self._log(f"  anchor: {anchor_id} @ {anchor_ver}")

            from . import version_fetcher
            loop = asyncio.get_event_loop()
            all_cids = version_fetcher.list_registered_components()

            async def _fetch(cid: str) -> tuple[str, list[str]]:
                vs = await loop.run_in_executor(
                    None, lambda c=cid: version_fetcher.get_versions(c)
                )
                return cid, [v["version"] for v in vs if not v.get("error")]

            pairs = await asyncio.gather(*[_fetch(c) for c in all_cids])
            available = dict(pairs)

            result = await loop.run_in_executor(
                None,
                lambda: compat_ai.research_compat(anchor_id, anchor_ver, available),
            )

            if result.get("error"):
                self._log(f"  ⚠ AI research warning: {result['error']} — using cart versions")
                self._phase_end("research", True)
                return self.cart_selections

            merged = {**self.cart_selections, **result.get("compat", {})}
            for cid, ver in result.get("compat", {}).items():
                self._log(f"  AI → {cid}: {ver}")
            self._phase_end("research", True)
            return merged

        except Exception as exc:
            self._log(f"  research error: {exc} — continuing with cart versions")
            self._phase_end("research", True)
            return self.cart_selections

    async def _phase_gen_config(self, selected_versions: dict[str, str]) -> dict:
        """Ask LLM to generate spark-defaults, Trino catalogs, post-start commands, etc."""
        self._phase_start("gen_config")
        try:
            stack = load_manifest(self.stack_id)
            topology = _build_topology(stack, selected_versions)
            self._log(f"  services: {[s['name'] for s in topology['services']]}")
            self._log("  calling LLM for config generation…")

            loop = asyncio.get_event_loop()
            prompt = _build_config_prompt(stack, topology, selected_versions)
            plan = await loop.run_in_executor(None, lambda: _call_llm(prompt))

            self._log(f"  reasoning: {plan.get('reasoning', '—')}")
            if plan.get("spark_defaults_append"):
                lines = [l for l in plan["spark_defaults_append"].splitlines() if l.strip() and not l.startswith("#")]
                self._log(f"  spark-defaults additions: {len(lines)} lines")
            if plan.get("trino_catalog_files"):
                self._log(f"  Trino catalogs: {list(plan['trino_catalog_files'].keys())}")
            if plan.get("post_start_commands"):
                self._log(f"  post-start commands: {len(plan['post_start_commands'])}")

            self._config_plan = plan
            self._connection_info = plan.get("connection_info", {})
            self._pipeline_example = plan.get("pipeline_example", "")
            self._reasoning = plan.get("reasoning", "")
            self._phase_end("gen_config", True)
            return plan

        except Exception as exc:
            self._log(f"  config generation error: {exc}")
            self._phase_end("gen_config", False, str(exc))
            return {}

    async def _phase_install(self, selected_versions: dict[str, str], config_plan: dict) -> bool:
        """Run the full UDPRunner install, forwarding all log events."""
        self._phase_start("install")
        try:
            from .runner import UDPRunner
            from .stack_manifest import load_manifest as lm
            from .state import store
            from .config import WORK_DIR

            stack = lm(self.stack_id)
            install_id = f"inst_{uuid.uuid4().hex[:10]}"
            host = self.install_options.get("host", "localhost")
            repo_dir = stack.repository.get("install_dir") or "udp"
            raw_install = self.install_options.get("install_dir") or str(WORK_DIR / repo_dir)
            install_dir = Path(raw_install)

            env_overrides: dict[str, str] = {}
            for cid, ver in selected_versions.items():
                # Carry version selections as LHS_ env vars so runner can use them
                env_key = f"LHS_VERSION_{cid.upper().replace('-', '_')}"
                env_overrides[env_key] = ver

            for k, v in self.install_options.get("env_overrides", {}).items():
                env_overrides[k] = v

            # post_env_hook: inject AI-generated spark-defaults + patch script versions
            async def _post_env_hook(idir: Path) -> None:
                if config_plan.get("spark_defaults_append"):
                    self._log("  [AI] patching spark-defaults.conf…")
                    _apply_spark_defaults(idir, config_plan["spark_defaults_append"])
                self._log("  [AI] patching bootstrap script versions…")
                _patch_script_versions(idir, self.stack_id, selected_versions)

            class _ForwardingRunner(UDPRunner):
                def _emit(self_r, kind: str, **kwargs):
                    super()._emit(kind, **kwargs)
                    if kind == "log":
                        self._log(kwargs.get("line", ""))
                    elif kind in ("step_start", "step_end", "state"):
                        self._emit(f"install_{kind}", **kwargs)

            runner = _ForwardingRunner(stack, install_id, host, install_dir)
            await runner.run(env_overrides, post_env_hook=_post_env_hook)

            # Check if install succeeded
            try:
                final_state = store.get_state(install_id)
                success = final_state in ("READY", "DONE")
            except Exception:
                success = True  # assume ok if state not tracked

            self._phase_end("install", success, "" if success else "Install step failed — check install log")
            return success

        except Exception as exc:
            self._log(f"  install error: {exc}")
            self._phase_end("install", False, str(exc))
            return False

    async def _phase_post_cfg(self, config_plan: dict) -> None:
        self._phase_start("post_cfg")
        try:
            trino_catalogs = config_plan.get("trino_catalog_files", {})
            if trino_catalogs:
                self._log("  injecting Trino catalog files…")
                _inject_trino_catalogs(trino_catalogs, self._log)
            else:
                self._log("  no Trino catalog injection needed for this stack")

            post_cmds = config_plan.get("post_start_commands", [])
            if post_cmds:
                self._log("  running post-start commands…")
                _run_post_start_commands(post_cmds, self._log)

            self._phase_end("post_cfg", True)
        except Exception as exc:
            self._log(f"  post-config warning: {exc}")
            self._phase_end("post_cfg", True)  # non-fatal

    async def _phase_verify(self, config_plan: dict) -> None:
        self._phase_start("verify")
        checks = config_plan.get("connectivity_checks", [])
        if not checks:
            self._log("  no connectivity checks defined — skipping")
        else:
            self._log(f"  running {len(checks)} connectivity checks…")
            loop = asyncio.get_event_loop()
            results = await loop.run_in_executor(
                None, lambda: _run_connectivity_checks(checks, self._log)
            )
            passed = sum(1 for r in results if r["ok"])
            self._log(f"  {passed}/{len(results)} checks passed")
        self._phase_end("verify", True)

    async def _phase_summary(self, host: str) -> None:
        self._phase_start("summary")
        info = {}
        for key, url_template in self._connection_info.items():
            info[key] = url_template.replace("HOST", host)

        self._emit("connection_info",
                   info=info,
                   pipeline_example=self._pipeline_example,
                   reasoning=self._reasoning)

        self._log("\n🎉 Your lakehouse is ready!")
        if info:
            self._log("\n  Service endpoints:")
            for key, url in info.items():
                self._log(f"    {key}: {url}")
        if self._pipeline_example:
            self._log("\n  Pipeline starter:")
            for ln in self._pipeline_example.splitlines()[:10]:
                self._log(f"    {ln}")

        self._phase_end("summary", True)
        self.state = "done"
        self._emit("provision_complete", connection_info=info)

    # --- main orchestrator ---

    async def run(self) -> None:
        self.state = "running"
        self._emit("provision_started", stack_id=self.stack_id, phases=[p[0] for p in PHASES])
        host = self.install_options.get("host", "localhost")

        try:
            selected_versions = await self._phase_research()
            config_plan = await self._phase_gen_config(selected_versions)

            if not config_plan and not self._config_plan:
                self._log("Config generation failed — aborting provision")
                self.state = "failed"
                self._emit("provision_complete", error="config generation failed")
                return

            ok = await self._phase_install(selected_versions, config_plan)
            if not ok:
                self.state = "failed"
                self._emit("provision_complete", error="install failed")
                return

            await self._phase_post_cfg(config_plan)
            await self._phase_verify(config_plan)
            await self._phase_summary(host)

        except Exception as exc:
            self._log(f"Provision failed: {exc}")
            self.state = "failed"
            self._emit("provision_complete", error=str(exc))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def start_provision(
    stack_id: str,
    cart_selections: dict[str, str],
    install_options: dict | None = None,
) -> str:
    job_id = f"prov_{uuid.uuid4().hex[:10]}"
    job = ProvisionJob(
        job_id=job_id,
        stack_id=stack_id,
        cart_selections=cart_selections,
        install_options=install_options or {},
    )
    _PROV_JOBS[job_id] = job
    asyncio.create_task(job.run())
    return job_id


def get_job(job_id: str) -> ProvisionJob | None:
    return _PROV_JOBS.get(job_id)


def list_jobs() -> list[dict]:
    return [
        {
            "job_id":        j.job_id,
            "stack_id":      j.stack_id,
            "state":         j.state,
            "current_phase": j.current_phase,
            "phase_states":  j.phase_states,
        }
        for j in _PROV_JOBS.values()
    ]
