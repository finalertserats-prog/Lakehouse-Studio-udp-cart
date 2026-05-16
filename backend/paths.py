"""install_dir validation. Refuses anything that could clobber arbitrary state."""
from __future__ import annotations
import platform
from pathlib import Path


# Refuse to install into these (case-insensitive, exact match on resolved path).
_FORBIDDEN_WINDOWS = (
    r"c:\windows", r"c:\program files", r"c:\program files (x86)",
    r"c:\users", r"c:\\",
)
_FORBIDDEN_POSIX = ("/", "/etc", "/var", "/usr", "/bin", "/sbin", "/boot", "/root", "/home")


class InstallDirError(ValueError):
    pass


def validate_install_dir(raw: str) -> Path:
    """Resolve & validate the user-provided install_dir. Raises InstallDirError on rejection."""
    if not raw or not raw.strip():
        raise InstallDirError("install_dir is empty")

    p = Path(raw).expanduser()
    if not p.is_absolute():
        raise InstallDirError(
            f"install_dir must be absolute (got {raw!r}). "
            "Example on Windows: D:\\udp-pilot. Example on Linux: /opt/lakehouse/udp."
        )

    try:
        resolved = p.resolve()
    except Exception as e:
        raise InstallDirError(f"could not resolve {raw!r}: {e}")

    sys_low = str(resolved).lower()
    if platform.system() == "Windows":
        for f in _FORBIDDEN_WINDOWS:
            if sys_low == f or sys_low.startswith(f + "\\"):
                if sys_low == r"c:\users":
                    continue  # allow subpaths of c:\users
                raise InstallDirError(
                    f"install_dir {resolved} is inside a protected system path"
                )
    else:
        if sys_low in _FORBIDDEN_POSIX:
            raise InstallDirError(
                f"install_dir {resolved} is a protected system path"
            )
        for f in _FORBIDDEN_POSIX:
            if f != "/" and (sys_low == f or sys_low.startswith(f + "/")):
                # /home/* is fine, /usr/* is not. Refuse anything *exactly under* a forbidden root.
                if sys_low.startswith("/home/"):
                    continue
                raise InstallDirError(
                    f"install_dir {resolved} is inside a protected system path ({f})"
                )

    # If the path already exists and is not empty, require it to look like a prior UDP clone.
    if resolved.exists():
        if not resolved.is_dir():
            raise InstallDirError(f"install_dir {resolved} exists but is not a directory")
        try:
            entries = list(resolved.iterdir())
        except PermissionError as e:
            raise InstallDirError(f"cannot read install_dir {resolved}: {e}")
        if entries:
            looks_like_udp = (resolved / ".git").exists() and (resolved / "udp").exists()
            if not looks_like_udp:
                names = ", ".join(sorted(e.name for e in entries[:8]))
                raise InstallDirError(
                    f"install_dir {resolved} is non-empty and does not look like a UDP clone. "
                    f"Contains: {names}. Refusing to overwrite. "
                    f"Pick an empty directory or delete the contents first."
                )

    return resolved
