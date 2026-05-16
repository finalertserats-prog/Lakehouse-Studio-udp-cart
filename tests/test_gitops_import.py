from __future__ import annotations

import io
import tarfile
import time

import pytest

from backend import gitops_import


def _tarball(members: dict[str, bytes], *, link: str | None = None) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for name, data in members.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(data)
            info.mtime = int(time.time())
            tar.addfile(info, io.BytesIO(data))
        if link is not None:
            info = tarfile.TarInfo(name=link)
            info.type = tarfile.SYMTYPE
            info.linkname = "../escape"
            info.mtime = int(time.time())
            tar.addfile(info)
    return buf.getvalue()


def _bundle(
    *,
    stack_id: str = "udp-local-v0.2",
    env: str = "LAKE_NAME=demo-lake\nUDP_PROJECT_NAME=demo\nUDP_ENV=dev\nMINIO_PASSWORD=<rotate-me>\n",
    extra: dict[str, bytes] | None = None,
) -> bytes:
    members = {
        "docker-compose.yml": b"services: {}\n",
        ".env": env.encode("utf-8"),
        "stack-manifest.yaml": f"id: {stack_id}\nname: Test Stack\n".encode("utf-8"),
        "stack-lock.yaml": b"stack_id: udp-local-v0.2\n",
        "README.md": b"# exported\n",
    }
    if extra:
        members.update(extra)
    return _tarball(members)


def _tarball_with_dir_env() -> bytes:
    """Build a tarball where `.env` is a directory entry, not a file.

    This is the Codex-flagged bypass: a tarball with `.env` as a directory
    has no body, so the placeholder check sees empty bytes and passes —
    even though a real install couldn't read it. Required-file-type check
    is supposed to reject this at validation time.
    """
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for name, data in {
            "docker-compose.yml": b"services: {}\n",
            "stack-manifest.yaml": b"id: udp-local-v0.2\nname: T\n",
            "stack-lock.yaml": b"stack_id: udp-local-v0.2\n",
            "README.md": b"# x\n",
        }.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(data)
            info.mtime = int(time.time())
            tar.addfile(info, io.BytesIO(data))
        # .env as a directory — the bypass shape
        dir_info = tarfile.TarInfo(name=".env")
        dir_info.type = tarfile.DIRTYPE
        dir_info.mtime = int(time.time())
        tar.addfile(dir_info)
    return buf.getvalue()


def _tarball_with_ads_member() -> bytes:
    """Tarball with a Windows alternate-data-stream member name."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for name, data in {
            "docker-compose.yml": b"services: {}\n",
            ".env": b"X=1\n",
            "stack-manifest.yaml": b"id: udp-local-v0.2\nname: T\n",
            "stack-lock.yaml": b"stack_id: udp-local-v0.2\n",
            "README.md": b"# x\n",
            "docker-compose.yml:evil": b"pwned\n",
        }.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(data)
            info.mtime = int(time.time())
            tar.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def test_rejects_env_as_directory():
    """Codex-flagged: a tarball whose .env is a directory bypasses the
    `<rotate-me>` placeholder check because the body is empty."""
    data = _tarball_with_dir_env()
    with pytest.raises(gitops_import.ImportError, match="regular file"):
        gitops_import.validate_tarball(data)


def test_rejects_windows_ads_member_name():
    """Codex-flagged: member names with `:` (NTFS alternate data streams)
    can write hidden disk state on Windows. Reject at validation."""
    data = _tarball_with_ads_member()
    with pytest.raises(gitops_import.ImportError, match="colon"):
        gitops_import.validate_tarball(data)


def test_materialize_rejects_symlink_target_dir(tmp_path):
    """Codex-flagged: `target_dir` as a symlink to an empty directory
    passes the empty-check and extracts into the link target — escaping
    the intended import root. Reject any symlink target outright."""
    real = tmp_path / "actual"
    real.mkdir()
    link = tmp_path / "link"
    try:
        link.symlink_to(real, target_is_directory=True)
    except (OSError, NotImplementedError):
        pytest.skip("symlink creation not permitted on this platform")
    with pytest.raises(gitops_import.ImportError, match="symlink"):
        gitops_import.materialize_import(_bundle(), link, allow_placeholders=True)


def test_validate_tarball_returns_import_plan_for_export_shape():
    plan = gitops_import.validate_tarball(
        _bundle(extra={"scripts/lhs-bootstrap.sh": b"#!/usr/bin/env bash\n"})
    )

    assert plan.stack_id == "udp-local-v0.2"
    assert plan.lake_name == "demo-lake"
    assert plan.udp_project_name == "demo"
    assert plan.udp_env == "dev"
    assert plan.has_scripts is True
    assert "docker-compose.yml" in plan.file_inventory
    assert plan.unresolved_placeholders == ["MINIO_PASSWORD"]
    assert plan.uncompressed_bytes > 0


def test_materialize_import_requires_explicit_placeholder_opt_in(tmp_path):
    data = _bundle()

    with pytest.raises(gitops_import.ImportError, match="rotate-me"):
        gitops_import.materialize_import(data, tmp_path / "imported")

    result = gitops_import.materialize_import(
        data,
        tmp_path / "imported",
        allow_placeholders=True,
    )

    assert result.stack_id == "udp-local-v0.2"
    assert (tmp_path / "imported" / "docker-compose.yml").read_text() == "services: {}\n"
    assert "<rotate-me>" in (tmp_path / "imported" / ".env").read_text()


@pytest.mark.parametrize(
    "bad_name",
    [
        "../escape.txt",
        "safe/../../escape.txt",
        "/absolute.txt",
        r"C:\absolute-ish.txt",
    ],
)
def test_validate_tarball_rejects_unsafe_member_paths(bad_name):
    with pytest.raises(gitops_import.ImportError, match="unsafe"):
        gitops_import.validate_tarball(_bundle(extra={bad_name: b"nope"}))


def test_validate_tarball_rejects_links():
    with pytest.raises(gitops_import.ImportError, match="link"):
        gitops_import.validate_tarball(_tarball({
            "docker-compose.yml": b"services: {}\n",
            ".env": b"LAKE_NAME=demo\n",
            "stack-manifest.yaml": b"id: udp-local-v0.2\n",
            "stack-lock.yaml": b"stack_id: udp-local-v0.2\n",
            "README.md": b"# exported\n",
        }, link="scripts/bootstrap-link"))


def test_validate_tarball_rejects_missing_required_member():
    data = _tarball({
        "docker-compose.yml": b"services: {}\n",
        ".env": b"LAKE_NAME=demo\n",
        "stack-manifest.yaml": b"id: udp-local-v0.2\n",
        "README.md": b"# exported\n",
    })

    with pytest.raises(gitops_import.ImportError, match="missing required"):
        gitops_import.validate_tarball(data)


def test_validate_tarball_rejects_unknown_stack_id():
    with pytest.raises(gitops_import.ImportError, match="not in the local catalog"):
        gitops_import.validate_tarball(_bundle(stack_id="does-not-exist"))


def test_materialize_import_refuses_non_empty_target(tmp_path):
    target = tmp_path / "imported"
    target.mkdir()
    existing = target / "keep.txt"
    existing.write_text("do not overwrite", encoding="utf-8")

    with pytest.raises(gitops_import.ImportError, match="not empty"):
        gitops_import.materialize_import(
            _bundle(env="LAKE_NAME=demo\nMINIO_PASSWORD=real-secret\n"),
            target,
        )

    assert existing.read_text(encoding="utf-8") == "do not overwrite"
