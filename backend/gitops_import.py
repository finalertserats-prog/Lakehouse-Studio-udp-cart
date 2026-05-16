"""GitOps import — materialize an exported tarball as a new install dir.

Inverse of :mod:`backend.gitops_export`. Operator uploads the .tar.gz that
`build_export()` produced on another Studio instance; we validate it
in-memory, then unpack it into a fresh install_dir on disk. The result
sits in state ``DRAFT`` — caller decides whether to kick off a real install.

Safety invariants:
- Tarball is validated BEFORE any disk write (members listed, manifest
  parsed, stack_id checked against the local catalog).
- Safe-extract: any member with an absolute path, ``..`` segment, or
  symlink target is rejected outright. We do NOT trust the upload.
- Target directory must be empty or non-existent. We never overwrite an
  existing install_dir during import.
- `.env` placeholder check: by default, importing a tarball whose `.env`
  still has `<rotate-me>` values for any secret key REJECTS. Operator
  opts into the permissive shape only when they're knowingly re-importing
  a snapshot they'll edit before installing.
"""
from __future__ import annotations
import io
import logging
import re
import tarfile
from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel, Field

from .stack_manifest import load_manifest


log = logging.getLogger("lhs.gitops_import")


# Required tarball members — must mirror what build_export() always writes.
REQUIRED_MEMBERS: tuple[str, ...] = (
    "docker-compose.yml",
    ".env",
    "stack-manifest.yaml",
    "stack-lock.yaml",
    "README.md",
)

# Placeholder marker written by gitops_export._scrub_env when masking secrets.
_PLACEHOLDER_TOKEN = "<rotate-me>"

# Mirrors _SECRET_KEY_RE from gitops_export — same pattern means an env line
# whose value is `<rotate-me>` is an unresolved placeholder for a secret.
_SECRET_KEY_RE = re.compile(
    r"^(.*(?:PASSWORD|SECRET|TOKEN|KEY|CREDENTIAL)[^=]*)=(.*)$",
    re.IGNORECASE,
)

# Soft cap on tarball size (uncompressed). Defense against zip-bomb uploads.
# A realistic full install dir is well under 1 GB; 2 GB is generous.
_MAX_UNCOMPRESSED_BYTES = 2 * 1024 * 1024 * 1024


class ImportError(ValueError):
    """The uploaded tarball is malformed, unsafe, or references unknown stacks."""


class ImportPlan(BaseModel):
    stack_id: str
    lake_name: Optional[str] = None
    udp_project_name: Optional[str] = None
    udp_env: Optional[str] = None
    file_inventory: list[str] = Field(default_factory=list)
    unresolved_placeholders: list[str] = Field(default_factory=list)
    has_scripts: bool = False
    uncompressed_bytes: int = 0


class ImportResult(BaseModel):
    install_dir: str
    stack_id: str
    plan: ImportPlan


# --------------------------------------------------------------------------- #
# Validation                                                                  #
# --------------------------------------------------------------------------- #


def _safe_open(data: bytes) -> tarfile.TarFile:
    """Open the upload as a gz tarball. Raises ImportError on a bad header."""
    try:
        return tarfile.open(fileobj=io.BytesIO(data), mode="r:gz")
    except tarfile.ReadError as e:
        raise ImportError(f"upload is not a valid gzipped tarball: {e}") from e
    except (OSError, EOFError) as e:
        raise ImportError(f"upload could not be opened: {type(e).__name__}: {e}") from e


def _check_member_safety(member: tarfile.TarInfo) -> None:
    """Reject members that would break out of the target dir."""
    name = member.name
    if not name or name.startswith("/") or name.startswith("\\"):
        raise ImportError(f"unsafe absolute path in tarball: {name!r}")
    if re.match(r"^[A-Za-z]:", name):
        raise ImportError(f"unsafe drive-qualified path in tarball: {name!r}")
    # Windows NTFS alternate data streams use `name:stream` syntax. Anything
    # else with a `:` is also suspicious — drive letters were handled above.
    if ":" in name:
        raise ImportError(f"unsafe colon in tarball member name (ADS or drive): {name!r}")
    # Windows UNC paths.
    if name.startswith("\\\\") or name.startswith("//"):
        raise ImportError(f"unsafe UNC path in tarball: {name!r}")
    # Reject any '..' segment after normalization
    parts = re.split(r"[\\/]+", name)
    if any(p == ".." for p in parts):
        raise ImportError(f"unsafe path traversal in tarball: {name!r}")
    if member.issym() or member.islnk():
        raise ImportError(f"symlink/hardlink not allowed in tarball: {name!r}")
    if member.isdev() or member.isfifo() or member.ischr() or member.isblk():
        raise ImportError(f"special device entry not allowed in tarball: {name!r}")


def _read_member_bytes(tar: tarfile.TarFile, member_name: str) -> Optional[bytes]:
    """Extract one member into memory. Returns None if missing."""
    try:
        member = tar.getmember(member_name)
    except KeyError:
        return None
    f = tar.extractfile(member)
    if f is None:
        return None
    return f.read()


def _extract_env_metadata(env_text: str) -> tuple[Optional[str], Optional[str], Optional[str], list[str]]:
    """Pull (lake_name, udp_project_name, udp_env, unresolved_secret_keys) out of an .env."""
    lake_name: Optional[str] = None
    project: Optional[str] = None
    env_val: Optional[str] = None
    unresolved: list[str] = []
    for raw_line in env_text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        if key == "LAKE_NAME" or key == "LHS_LAKE_NAME":
            lake_name = value or None
        elif key == "UDP_PROJECT_NAME":
            project = value or None
        elif key == "UDP_ENV":
            env_val = value or None
        m = _SECRET_KEY_RE.match(line)
        if m and m.group(2).strip() == _PLACEHOLDER_TOKEN:
            unresolved.append(m.group(1).strip())
    return lake_name, project, env_val, unresolved


def validate_tarball(data: bytes) -> ImportPlan:
    """Validate the upload IN MEMORY. Never touches disk. Raises ImportError
    on any structural problem; returns an ImportPlan otherwise."""
    if not data:
        raise ImportError("upload is empty")

    tar = _safe_open(data)
    try:
        # Enumerate members and accumulate size; reject unsafe members eagerly.
        inventory: list[str] = []
        uncompressed = 0
        has_scripts = False
        for member in tar:
            _check_member_safety(member)
            inventory.append(member.name)
            uncompressed += int(getattr(member, "size", 0) or 0)
            if uncompressed > _MAX_UNCOMPRESSED_BYTES:
                raise ImportError(
                    f"tarball uncompressed size exceeds cap "
                    f"({_MAX_UNCOMPRESSED_BYTES} bytes) — refusing import"
                )
            if member.name == "scripts" or member.name.startswith("scripts/"):
                has_scripts = True

        # Required members must all be present AND be regular files.
        # A `.env` directory would bypass the placeholder check below; a
        # `stack-manifest.yaml` directory would also defeat manifest parsing.
        missing = [m for m in REQUIRED_MEMBERS if m not in inventory]
        if missing:
            raise ImportError(
                f"tarball missing required members: {missing}"
            )
        for required in REQUIRED_MEMBERS:
            try:
                req_member = tar.getmember(required)
            except KeyError:
                continue  # impossible — covered by the missing-check above
            if not req_member.isfile():
                raise ImportError(
                    f"required member {required!r} must be a regular file, "
                    f"not a {req_member.type!r} entry"
                )

        # Parse the embedded manifest. Stack id MUST be present in the local
        # catalog — otherwise the import is unactionable.
        manifest_bytes = _read_member_bytes(tar, "stack-manifest.yaml") or b""
        try:
            manifest_doc = yaml.safe_load(manifest_bytes.decode("utf-8", errors="replace"))
        except yaml.YAMLError as e:
            raise ImportError(f"stack-manifest.yaml does not parse: {e}") from e
        if not isinstance(manifest_doc, dict):
            raise ImportError("stack-manifest.yaml must be a YAML mapping")
        stack_id = manifest_doc.get("id")
        if not isinstance(stack_id, str) or not stack_id:
            raise ImportError("stack-manifest.yaml missing string 'id'")
        try:
            load_manifest(stack_id)  # round-trip against the local catalog
        except KeyError as e:
            raise ImportError(
                f"stack '{stack_id}' from tarball is not in the local catalog "
                f"({e}) — refusing import"
            ) from e

        # Pull operator-visible details out of the bundled .env.
        env_bytes = _read_member_bytes(tar, ".env") or b""
        env_text = env_bytes.decode("utf-8", errors="replace")
        lake_name, project, env_val, unresolved = _extract_env_metadata(env_text)

        return ImportPlan(
            stack_id=stack_id,
            lake_name=lake_name,
            udp_project_name=project,
            udp_env=env_val,
            file_inventory=inventory,
            unresolved_placeholders=unresolved,
            has_scripts=has_scripts,
            uncompressed_bytes=uncompressed,
        )
    finally:
        tar.close()


# --------------------------------------------------------------------------- #
# Materialize                                                                 #
# --------------------------------------------------------------------------- #


def _empty_or_missing(path: Path) -> bool:
    if not path.exists():
        return True
    if not path.is_dir():
        return False
    return next(path.iterdir(), None) is None


def materialize_import(
    data: bytes,
    target_dir: Path,
    *,
    allow_placeholders: bool = False,
) -> ImportResult:
    """Validate, then extract the tarball into ``target_dir``. The target
    must be empty or non-existent — we never overwrite. Returns an
    ImportResult describing what landed on disk."""
    plan = validate_tarball(data)
    if plan.unresolved_placeholders and not allow_placeholders:
        raise ImportError(
            f"tarball .env still has <rotate-me> values for "
            f"{plan.unresolved_placeholders} — pass allow_placeholders=True "
            "to import anyway (operator must rotate secrets before install)"
        )
    raw_target = Path(target_dir)
    # Reject symlinks/junctions BEFORE resolving — `_empty_or_missing` on a
    # symlink that points to an empty dir would otherwise pass and we'd
    # extract into the link target, escaping the intended import root.
    # `is_symlink()` handles POSIX symlinks + Windows symlinks; NTFS junctions
    # are a known gap that requires a deeper Windows-specific reparse-point
    # check — flagged as a follow-up if anyone runs into it.
    if raw_target.is_symlink():
        raise ImportError(
            f"target directory {raw_target} is a symlink — refusing import"
        )
    target_dir = raw_target.resolve()
    if target_dir.exists():
        if not target_dir.is_dir():
            raise ImportError(
                f"target {target_dir} exists and is not a directory"
            )
        if next(target_dir.iterdir(), None) is not None:
            raise ImportError(
                f"target directory {target_dir} is not empty — refusing import"
            )
    else:
        # Atomic create-or-fail closes the TOCTOU window between the
        # empty-check above and the extractall below: if another process
        # races us to create the path, this raises instead of silently
        # extracting into their directory.
        target_dir.mkdir(parents=True, exist_ok=False)

    tar = _safe_open(data)
    try:
        # We already checked safety in validate_tarball; re-check on extract
        # because we're trusting the same buffer end-to-end.
        for member in tar:
            _check_member_safety(member)
        # Re-open: iterating consumed the tar above.
        tar.close()
        tar = _safe_open(data)
        # Python 3.12+ accepts a `filter` kwarg; older versions don't. Try the
        # newer signature first and fall back so we run on either.
        try:
            tar.extractall(str(target_dir), filter="data")  # type: ignore[call-arg]
        except TypeError:
            tar.extractall(str(target_dir))
    finally:
        tar.close()

    return ImportResult(
        install_dir=str(target_dir),
        stack_id=plan.stack_id,
        plan=plan,
    )
