"""GitOps export — pack a deployed stack as a portable tarball.

Bundles the post-patched compose file, the scrubbed .env, Studio-owned
scripts, the manifest, the lock file, and a README into a single
.tar.gz. User downloads it from the success screen and runs
`docker compose up -d` on any host with Docker.

NEVER includes:
- Real MinIO / StarRocks passwords (rewritten to `<rotate-me>` in .env)
- Auth tokens, API keys, anything matching the redact module's secret list
- Backup tarballs, ingest staging files, runtime state from work/
"""
from __future__ import annotations
import io
import re
import tarfile
import time
from pathlib import Path
from typing import Iterable

from .config import ROOT
from .stack_manifest import load_manifest


# Keys whose value we replace with `<rotate-me>` in the exported .env
# Conservative pattern: anything obviously credential-shaped.
_SECRET_KEY_RE = re.compile(
    r"^(.*(?:PASSWORD|SECRET|TOKEN|KEY|CREDENTIAL)[^=]*)=(.*)$",
    re.IGNORECASE,
)


def _scrub_env(text: str) -> str:
    out_lines: list[str] = []
    for line in text.splitlines():
        m = _SECRET_KEY_RE.match(line.strip())
        if m and m.group(2).strip() not in ("", "<rotate-me>"):
            out_lines.append(f"{m.group(1)}=<rotate-me>")
        else:
            out_lines.append(line)
    return "\n".join(out_lines) + ("\n" if text.endswith("\n") else "")


def _readme(install_id: str, stack_id: str, lake_name: str | None) -> str:
    return f"""# Lakehouse export — {lake_name or stack_id}

Exported from Lakehouse Studio install `{install_id}` on
{time.strftime("%Y-%m-%d %H:%M:%S %Z")}.

## Contents

- `docker-compose.yml` — the patched compose file Studio ran
- `.env` — environment with SECRETS REPLACED by `<rotate-me>` placeholders
- `scripts/` — Studio-owned bootstrap + smoke scripts
- `stack-manifest.yaml` — the certified stack manifest
- `stack-lock.yaml` — compatibility lock file with evidence

## Bring it up on a fresh host

1. Rotate every `<rotate-me>` value in `.env` to real secrets.
2. `docker compose pull`
3. `docker compose up -d`
4. Wait for services to come up (use `docker compose ps`).
5. Run the included bootstrap script if you want demo data:
   `bash scripts/lhs-bootstrap.sh`

## Caveats

- The compose file is patched for the certified versions in the lock —
  do NOT mutate image tags without re-validating against the matrix.
- This is a *snapshot* of one install. For ongoing deployments, treat
  the lock file as the source of truth and re-export after upgrades.
"""


def _safe_members(root: Path) -> Iterable[tuple[Path, str]]:
    """Yield (path, arcname) pairs for files we ship in the bundle."""
    # docker-compose.yml + .env at the top
    compose = root / "docker-compose.yml"
    env = root / ".env"
    if compose.exists():
        yield compose, "docker-compose.yml"
    if env.exists():
        yield env, ".env"
    # scripts/ subdirectory recursively
    scripts = root / "scripts"
    if scripts.is_dir():
        for f in scripts.rglob("*"):
            if f.is_file():
                yield f, f"scripts/{f.relative_to(scripts).as_posix()}"


def build_export(install_id: str, install_dir: Path, stack_id: str,
                 lake_name: str | None = None) -> tuple[bytes, str]:
    """Build the tarball in memory. Returns (bytes, filename)."""
    fname = f"lakehouse-{lake_name or stack_id}-{install_id}.tar.gz"
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        # docker-compose.yml + scripts (verbatim)
        for path, arc in _safe_members(install_dir):
            if arc == ".env":
                # scrub before writing
                scrubbed = _scrub_env(path.read_text(encoding="utf-8", errors="replace"))
                data = scrubbed.encode("utf-8")
                info = tarfile.TarInfo(name=".env")
                info.size = len(data); info.mtime = int(time.time())
                tar.addfile(info, io.BytesIO(data))
            else:
                tar.add(str(path), arcname=arc, recursive=False)

        # Stack manifest + lock file (from Studio's own dir, not the cloned UDP repo)
        manifest_path = ROOT / "stacks" / f"{stack_id}.yaml"
        lock_path = ROOT / "stacks" / "compatibility" / f"{stack_id}.lock.yaml"
        for src, arc in [(manifest_path, "stack-manifest.yaml"),
                         (lock_path, "stack-lock.yaml")]:
            if src.exists():
                data = src.read_bytes()
                info = tarfile.TarInfo(name=arc)
                info.size = len(data); info.mtime = int(time.time())
                tar.addfile(info, io.BytesIO(data))

        # README
        readme_data = _readme(install_id, stack_id, lake_name).encode("utf-8")
        info = tarfile.TarInfo(name="README.md")
        info.size = len(readme_data); info.mtime = int(time.time())
        tar.addfile(info, io.BytesIO(readme_data))

    return buf.getvalue(), fname
