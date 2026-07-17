"""pyarrow_fs_shim — drop-in replacement for `pyarrow.fs` HDFS bits.

Why this exists
    The apache/spark docker image doesn't ship libhdfs.so in a state pyarrow
    can dlopen.  On live the script just uses `pyarrow.fs.HadoopFileSystem`
    which calls into libhdfs (JNI → JVM → Hadoop client).  In this docker
    we route the same API through WebHDFS REST so the ingest script can run
    unchanged except for `import pyarrow.fs as fs` → `import pyarrow_fs_shim as fs`.

Exposes
    FileType.File / .Directory / .NotFound          (the values the script reads)
    FileInfo(path, type, base_name)
    FileSelector(base_dir, recursive=False)
    HadoopFileSystem.from_uri(uri)
        .get_file_info(path_or_selector) → FileInfo | list[FileInfo]
        .open_input_stream(path) → context manager yielding bytes-like reader
        .delete_file(path)
        .delete_dir(path)
        .delete_dir_contents(path)
"""

from __future__ import annotations
import enum
import io
from dataclasses import dataclass
from typing import List, Union
from urllib.parse import urlparse, quote

import requests


class FileType(enum.Enum):
    NotFound  = 0
    Unknown   = 1
    File      = 2
    Directory = 3


@dataclass
class FileInfo:
    path: str
    type: FileType
    base_name: str = ""


@dataclass
class FileSelector:
    base_dir: str
    recursive: bool = False
    allow_not_found: bool = True


class _Reader(io.RawIOBase):
    """Wraps bytes as a readable, context-manager-friendly stream."""
    def __init__(self, data: bytes):
        self._buf = io.BytesIO(data)
    def read(self, n=-1):
        return self._buf.read(n)
    def readable(self):
        return True
    def close(self):
        self._buf.close()
    def __enter__(self):
        return self._buf
    def __exit__(self, *exc):
        self._buf.close()
        return False


class HadoopFileSystem:
    """WebHDFS-backed stand-in for pyarrow.fs.HadoopFileSystem."""

    def __init__(self, host: str, port: int, user: str = "hadoop"):
        self._base = f"http://{host}:{port}/webhdfs/v1"
        self._user = user

    @classmethod
    def from_uri(cls, uri: str) -> "HadoopFileSystem":
        # uri like hdfs://namenode:9820 — WebHDFS lives on namenode:9870 by default
        u = urlparse(uri)
        # WebHDFS port is 9870 in our stack; let it be overridden via WEBHDFS_HOST_PORT env if needed.
        import os
        webhdfs_host = os.environ.get("WEBHDFS_HOST", u.hostname or "namenode")
        webhdfs_port = int(os.environ.get("WEBHDFS_PORT", "9870"))
        return cls(webhdfs_host, webhdfs_port, user=os.environ.get("HDFS_USER", "hadoop"))

    # ── internal HTTP ────────────────────────────────────────────────────
    def _url(self, path: str, op: str, **params) -> str:
        if not path.startswith("/"):
            path = "/" + path
        q = "&".join(f"{k}={quote(str(v), safe='')}" for k, v in params.items())
        return f"{self._base}{path}?op={op}&user.name={self._user}" + (f"&{q}" if q else "")

    # ── public API ───────────────────────────────────────────────────────
    def get_file_info(
        self, target: Union[str, FileSelector]
    ) -> Union[FileInfo, List[FileInfo]]:
        if isinstance(target, FileSelector):
            r = requests.get(self._url(target.base_dir, "LISTSTATUS"), timeout=30)
            if r.status_code == 404:
                if target.allow_not_found:
                    return []
                raise FileNotFoundError(target.base_dir)
            r.raise_for_status()
            entries = r.json().get("FileStatuses", {}).get("FileStatus", [])
            out = []
            for e in entries:
                full = f"{target.base_dir.rstrip('/')}/{e['pathSuffix']}"
                t = FileType.Directory if e["type"] == "DIRECTORY" else FileType.File
                out.append(FileInfo(path=full, type=t, base_name=e["pathSuffix"]))
                if target.recursive and t == FileType.Directory:
                    out.extend(self.get_file_info(FileSelector(full, recursive=True)))
            return out

        # single path
        path = target
        r = requests.get(self._url(path, "GETFILESTATUS"), timeout=30)
        if r.status_code == 404:
            return FileInfo(path=path, type=FileType.NotFound, base_name=path.rsplit("/", 1)[-1])
        r.raise_for_status()
        st = r.json().get("FileStatus", {})
        t = FileType.Directory if st.get("type") == "DIRECTORY" else FileType.File
        return FileInfo(path=path, type=t, base_name=path.rsplit("/", 1)[-1])

    def open_input_stream(self, path: str):
        r = requests.get(self._url(path, "OPEN"), timeout=120, allow_redirects=True)
        r.raise_for_status()
        return _Reader(r.content)

    def delete_file(self, path: str) -> None:
        r = requests.delete(self._url(path, "DELETE", recursive="false"), timeout=30)
        if r.status_code == 404:
            raise FileNotFoundError(path)
        r.raise_for_status()
        if not r.json().get("boolean"):
            raise FileNotFoundError(path)

    def delete_dir(self, path: str) -> None:
        r = requests.delete(self._url(path, "DELETE", recursive="true"), timeout=60)
        r.raise_for_status()

    def delete_dir_contents(self, path: str) -> None:
        # Best-effort: list children and delete each
        for fi in self.get_file_info(FileSelector(path, recursive=False)):
            if fi.type == FileType.Directory:
                self.delete_dir(fi.path)
            else:
                try:
                    self.delete_file(fi.path)
                except FileNotFoundError:
                    pass
