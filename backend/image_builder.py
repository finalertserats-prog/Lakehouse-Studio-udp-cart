"""LLM-driven pipeline for custom Studio Spark images.

Flow per image:
  1. research()   — LLM picks best addon version (Hudi/Delta) for the current Spark base
  2. validate()   — HEAD-check the Maven jar URL
  3. patch()      — Rewrite Dockerfile + stack YAML with new version
  4. build()      — docker build (async, streams to job log)
  5. push()       — docker push (async, streams to job log)
  6. update_yaml()— bump version + image in the stack YAML file

Endpoints in main.py use:
  POST /api/image-build/research   — step 1, returns recommendation
  POST /api/image-build/start      — steps 2-6, returns job_id
  GET  /api/image-build/stream/{job_id} — SSE log stream
  GET  /api/image-build/status/{job_id} — current job state
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import time
import uuid
from pathlib import Path
from typing import Any

import httpx
import litellm
from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).parent.parent

_BASE_URL = os.environ.get("LITELLM_BASE_URL", "")
_API_KEY  = os.environ.get("LITELLM_API_KEY", "")
_MODEL    = os.environ.get("LITELLM_MODEL", "gpt-4o-mini")


# ── Image configurations ───────────────────────────────────────────────────────

IMAGES: dict[str, dict[str, Any]] = {
    "spark-hudi": {
        "repo":        "lakehousestudio/spark-hudi",
        "dockerfile":  ROOT / "scripts/images/Dockerfile.spark-hudi",
        "addon_id":    "hudi",
        "addon_name":  "Apache Hudi",
        # Maven artifact used in the Dockerfile ADD line (Scala 2.12 build)
        "maven_artifact": "org.apache.hudi:hudi-spark3.5-bundle_2.12",
        "ver_re":      re.compile(
            r"(https://repo1\.maven\.org/maven2/org/apache/hudi/"
            r"hudi-spark3\.5-bundle_2\.12/)([\d.]+)"
            r"(/hudi-spark3\.5-bundle_2\.12-)([\d.]+)(\.jar)"
        ),
        "maven_url": lambda v: (
            "https://repo1.maven.org/maven2/org/apache/hudi/"
            f"hudi-spark3.5-bundle_2.12/{v}/hudi-spark3.5-bundle_2.12-{v}.jar"
        ),
        "stack_yaml":  ROOT / "stacks/hudi-hms-spark-local-v0.1.yaml",
        "yaml_comp_id": "spark-hudi",
    },
    "spark-delta": {
        "repo":        "lakehousestudio/spark-delta",
        "dockerfile":  ROOT / "scripts/images/Dockerfile.spark-delta",
        "addon_id":    "delta",
        "addon_name":  "Delta Lake",
        # Delta 4.x dropped the Scala 2.12 build — delta-spark_2.12 tops out at 3.x
        "maven_artifact": "io.delta:delta-spark_2.12 (Scala 2.12 build — only exists up to 3.x)",
        "ver_re":      re.compile(
            r"(https://repo1\.maven\.org/maven2/io/delta/"
            r"delta-spark_2\.12/)([\d.]+)"
            r"(/delta-spark_2\.12-)([\d.]+)(\.jar)"
        ),
        "maven_url": lambda v: (
            "https://repo1.maven.org/maven2/io/delta/"
            f"delta-spark_2.12/{v}/delta-spark_2.12-{v}.jar"
        ),
        "stack_yaml":  ROOT / "stacks/delta-hms-spark-trino-local-v0.1.yaml",
        "yaml_comp_id": "spark-delta",
    },
}


# ── Job tracking ───────────────────────────────────────────────────────────────

_JOBS: dict[str, dict[str, Any]] = {}


def new_job() -> str:
    job_id = uuid.uuid4().hex[:12]
    _JOBS[job_id] = {
        "status": "pending",
        "lines": [],
        "result": None,
        "created_at": time.time(),
    }
    return job_id


def get_job(job_id: str) -> dict[str, Any] | None:
    return _JOBS.get(job_id)


def _log(job_id: str, line: str) -> None:
    job = _JOBS.get(job_id)
    if job is not None:
        job["lines"].append(line)


# ── Step 1: LLM research ───────────────────────────────────────────────────────

def _spark_ver_from_dockerfile(df: Path) -> str:
    m = re.search(r"FROM\s+\S+:(\d+\.\d+\.\d+)", df.read_text())
    return m.group(1) if m else "3.5.5"


def _current_addon_ver(image_id: str) -> str:
    cfg = IMAGES[image_id]
    m = cfg["ver_re"].search(cfg["dockerfile"].read_text())
    return m.group(2) if m else "unknown"


def research_addon_version(image_id: str, available: list[str]) -> dict[str, Any]:
    """Ask the LLM which addon version to bundle. Validates against available list."""
    cfg = IMAGES[image_id]
    spark_ver = _spark_ver_from_dockerfile(cfg["dockerfile"])
    current = _current_addon_ver(image_id)

    if not available:
        return {"error": "No addon versions available from version_fetcher", "spark_ver": spark_ver}

    maven_artifact = cfg.get("maven_artifact", cfg["addon_name"])
    prompt = (
        f"You are an expert in Apache Spark and open-source lakehouse compatibility.\n\n"
        f"Task: choose the SINGLE best {cfg['addon_name']} version to bundle with Spark {spark_ver}.\n\n"
        f"IMPORTANT Maven constraint: we use the artifact `{maven_artifact}`.\n"
        f"Only pick versions where this exact artifact exists on Maven Central.\n\n"
        f"Available {cfg['addon_name']} versions (newest first):\n"
        f"{', '.join(available[:20])}\n\n"
        f"Rules:\n"
        f"- Must be officially supported on Spark {spark_ver}\n"
        f"- Must have the Maven artifact listed above published for this version\n"
        f"- Prefer newest stable (no RC, alpha, beta suffixes)\n\n"
        f"Return ONLY valid JSON (no markdown, no extra text):\n"
        f'{{ "addon_version": "X.Y.Z", "reason": "one-line reason" }}'
    )

    try:
        kwargs: dict[str, Any] = {
            "model": _MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.1,
            "max_tokens": 200,
        }
        if _BASE_URL:
            kwargs["base_url"] = _BASE_URL
        if _API_KEY:
            kwargs["api_key"] = _API_KEY

        raw = litellm.completion(**kwargs).choices[0].message.content.strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
        data = json.loads(raw)
        addon_ver: str = data.get("addon_version", "").strip()
        reason: str    = data.get("reason", "")

        # Validate: must be in the available list (exact or prefix match)
        if addon_ver not in available:
            for v in available:
                if v.startswith(addon_ver.rsplit(".", 1)[0] + "."):
                    addon_ver = v
                    break
            else:
                return {
                    "error": f"LLM suggested {addon_ver!r} but it is not in available versions list",
                    "llm_suggestion": addon_ver,
                    "available_sample": available[:5],
                    "spark_ver": spark_ver,
                }

        tag = f"{spark_ver}_{addon_ver}"
        return {
            "image_id":    image_id,
            "spark_ver":   spark_ver,
            "addon_ver":   addon_ver,
            "current_addon": current,
            "tag":         tag,
            "full_image":  f"{cfg['repo']}:{tag}",
            "reason":      reason,
        }
    except Exception as exc:
        return {"error": str(exc), "spark_ver": spark_ver, "image_id": image_id}


# ── Step 2: Validate Maven URL ────────────────────────────────────────────────

def validate_maven(image_id: str, addon_ver: str) -> tuple[bool, str]:
    url = IMAGES[image_id]["maven_url"](addon_ver)
    try:
        r = httpx.head(url, timeout=12, follow_redirects=True)
        return r.status_code == 200, url
    except Exception as exc:
        return False, str(exc)


# ── Step 3: Patch Dockerfile ──────────────────────────────────────────────────

def _patch_dockerfile_content(image_id: str, spark_ver: str, addon_ver: str) -> str:
    cfg = IMAGES[image_id]
    text = cfg["dockerfile"].read_text()

    # Replace version in the primary ADD line (both path and filename)
    def _repl(m: re.Match) -> str:
        return f"{m.group(1)}{addon_ver}{m.group(3)}{addon_ver}{m.group(5)}"
    text = cfg["ver_re"].sub(_repl, text)

    # delta-storage must match delta-spark version
    if image_id == "spark-delta":
        text = re.sub(
            r"(https://repo1\.maven\.org/maven2/io/delta/delta-storage/)([\d.]+)"
            r"(/delta-storage-)([\d.]+)(\.jar)",
            lambda m: f"{m.group(1)}{addon_ver}{m.group(3)}{addon_ver}{m.group(5)}",
            text,
        )

    # Update bake-recipe tag in header comment (e.g. lakehousestudio/spark-hudi:3.5.0_0.15.0)
    text = re.sub(
        rf"({re.escape(cfg['repo'])}:)[\d._]+",
        rf"\g<1>{spark_ver}_{addon_ver}",
        text,
    )

    return text


def _patch_stack_yaml(image_id: str, spark_ver: str, addon_ver: str) -> None:
    cfg = IMAGES[image_id]
    yaml_path: Path = cfg["stack_yaml"]
    comp_id = cfg["yaml_comp_id"]
    tag = f"{spark_ver}_{addon_ver}"
    full_image = f"{cfg['repo']}:{tag}"

    text = yaml_path.read_text()

    # Replace version: <old_tag> under the component block
    text = re.sub(
        rf"(- id: {re.escape(comp_id)}.*?version: )([^\n]+)",
        rf"\g<1>{tag}",
        text,
        flags=re.DOTALL,
        count=1,
    )
    # Replace image: lakehousestudio/<repo>:<old_tag>
    text = re.sub(
        rf"(image: {re.escape(cfg['repo'])}):[^\n]+",
        rf"\g<1>:{tag}",
        text,
        count=1,
    )
    yaml_path.write_text(text)


# ── Steps 4+5: Async docker build / push ─────────────────────────────────────

async def _stream_cmd(cmd: list[str], job_id: str) -> int:
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        cwd=str(ROOT),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    assert proc.stdout is not None
    while True:
        line_bytes = await proc.stdout.readline()
        if not line_bytes:
            break
        _log(job_id, line_bytes.decode(errors="replace").rstrip())
    await proc.wait()
    return proc.returncode or 0


# ── Full pipeline ─────────────────────────────────────────────────────────────

async def run_pipeline(image_id: str, job_id: str, research: dict[str, Any]) -> None:
    """Async pipeline. Updates _JOBS[job_id] throughout."""
    job = _JOBS[job_id]
    cfg = IMAGES[image_id]
    spark_ver:  str = research["spark_ver"]
    addon_ver:  str = research["addon_ver"]
    tag:        str = research["tag"]
    full_image: str = research["full_image"]

    try:
        # 1. Validate Maven URL (with auto-fallback to next available version) ----
        job["status"] = "validating"
        _log(job_id, f"[validate] Checking Maven artifact for {cfg['addon_name']} {addon_ver}…")
        ok, url_or_err = await asyncio.get_event_loop().run_in_executor(
            None, lambda: validate_maven(image_id, addon_ver)
        )
        if not ok:
            _log(job_id, f"[validate] ✗ {url_or_err} — trying fallback versions…")
            # Walk the available list (ordered newest-first from version_fetcher)
            # to find the first one with a real Maven jar
            from . import version_fetcher as _vf
            raw = _vf.get_versions(cfg["addon_id"])
            candidates = [v["version"] for v in raw if not v.get("error") and v["version"] != addon_ver]
            found = False
            for candidate in candidates:
                _log(job_id, f"[validate] trying {candidate}…")
                ok2, url2 = await asyncio.get_event_loop().run_in_executor(
                    None, lambda c=candidate: validate_maven(image_id, c)
                )
                if ok2:
                    addon_ver = candidate
                    tag = f"{spark_ver}_{addon_ver}"
                    full_image = f"{cfg['repo']}:{tag}"
                    url_or_err = url2
                    found = True
                    _log(job_id, f"[validate] ✓ fallback selected: {addon_ver}")
                    break
            if not found:
                raise RuntimeError(f"No valid Maven artifact found for any {cfg['addon_name']} version")
        _log(job_id, f"[validate] ✓ {url_or_err}")

        # 2. Patch Dockerfile + stack YAML ----------------------------------------
        job["status"] = "patching"
        _log(job_id, f"[patch] Updating {cfg['dockerfile'].name} → Spark {spark_ver} + {cfg['addon_name']} {addon_ver}…")
        new_content = _patch_dockerfile_content(image_id, spark_ver, addon_ver)
        cfg["dockerfile"].write_text(new_content)
        _patch_stack_yaml(image_id, spark_ver, addon_ver)
        _log(job_id, f"[patch] ✓ Dockerfile and stack YAML updated")

        # 3. Docker build ---------------------------------------------------------
        job["status"] = "building"
        build_cmd = [
            "docker", "build",
            "-f", str(cfg["dockerfile"]),
            "-t", full_image,
            ".",
        ]
        _log(job_id, f"[build] {' '.join(build_cmd)}")
        rc = await _stream_cmd(build_cmd, job_id)
        if rc != 0:
            raise RuntimeError(f"docker build failed (exit {rc})")
        _log(job_id, f"[build] ✓ Built: {full_image}")

        # 4. Docker push ----------------------------------------------------------
        job["status"] = "pushing"
        _log(job_id, f"[push] docker push {full_image}")
        rc = await _stream_cmd(["docker", "push", full_image], job_id)
        if rc != 0:
            raise RuntimeError(f"docker push failed (exit {rc})")
        _log(job_id, f"[push] ✓ Pushed: {full_image}")

        job["status"] = "done"
        job["result"] = {"image": full_image, "tag": tag, "image_id": image_id}
        _log(job_id, f"[done] ✓ {full_image} is live — version picker will show it on next refresh")

    except Exception as exc:
        job["status"] = "error"
        job["result"] = {"error": str(exc)}
        _log(job_id, f"[error] ✗ {exc}")
