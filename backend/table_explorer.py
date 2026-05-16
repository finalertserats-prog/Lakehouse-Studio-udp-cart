"""Read-only Iceberg REST catalog client.

Talks to the Iceberg REST catalog (default: http://localhost:8181) that ships
with the UDP stack. Pure read path — never writes. Used by Table Explorer
to enumerate namespaces, tables, and surface per-table schema/snapshot info.

All calls are short-timeout (5s) and gracefully report errors via dict
return shape so a flaky catalog does not 500 the UI.
"""
from __future__ import annotations
import time
from typing import Any, Optional

import httpx


DEFAULT_REST_BASE_URL = "http://localhost:8181"
_REQUEST_TIMEOUT_SEC = 5.0

# Simple TTL cache: {(fn_name, *args): (expires_at, value)}
_CACHE: dict[tuple, tuple[float, Any]] = {}
_CACHE_TTL_SEC = 30.0


def _cache_get(key: tuple) -> Optional[Any]:
    hit = _CACHE.get(key)
    if not hit:
        return None
    expires_at, value = hit
    if time.monotonic() > expires_at:
        _CACHE.pop(key, None)
        return None
    return value


def _cache_set(key: tuple, value: Any) -> None:
    _CACHE[(key)] = (time.monotonic() + _CACHE_TTL_SEC, value)


def _invalidate_cache() -> None:
    """Test/utility hook — clears the in-process cache."""
    _CACHE.clear()


async def _get_json(url: str) -> Any:
    async with httpx.AsyncClient(timeout=_REQUEST_TIMEOUT_SEC) as client:
        r = await client.get(url)
        r.raise_for_status()
        return r.json()


async def list_namespaces(rest_base_url: str = DEFAULT_REST_BASE_URL) -> list[str]:
    """Return a flat list of namespace strings (joined with '.' for nested)."""
    key = ("list_namespaces", rest_base_url)
    cached = _cache_get(key)
    if cached is not None:
        return cached

    data = await _get_json(f"{rest_base_url.rstrip('/')}/v1/namespaces")
    # Response shape: {"namespaces": [["ns1"], ["ns2", "sub"]]}
    raw = data.get("namespaces") or []
    out: list[str] = []
    for ns in raw:
        if isinstance(ns, list) and ns:
            out.append(".".join(str(p) for p in ns))
        elif isinstance(ns, str):
            out.append(ns)
    _cache_set(key, out)
    return out


async def list_tables(namespace: str, rest_base_url: str = DEFAULT_REST_BASE_URL) -> list[dict]:
    """List tables in a namespace. Returns [{namespace, name}]."""
    key = ("list_tables", rest_base_url, namespace)
    cached = _cache_get(key)
    if cached is not None:
        return cached

    # REST spec: namespace path component is the multipart namespace joined with '%1F' (unit-separator),
    # but in practice the UDP catalog uses '.'-separated paths.
    ns_path = namespace.replace(".", "%1F")
    url = f"{rest_base_url.rstrip('/')}/v1/namespaces/{ns_path}/tables"
    data = await _get_json(url)
    identifiers = data.get("identifiers") or []
    out: list[dict] = []
    for ident in identifiers:
        ns = ident.get("namespace") or []
        name = ident.get("name")
        if not name:
            continue
        ns_str = ".".join(str(p) for p in ns) if isinstance(ns, list) else str(ns)
        out.append({"namespace": ns_str or namespace, "name": name})
    _cache_set(key, out)
    return out


def _extract_schema(meta: dict) -> list[dict]:
    """Pull the current schema's column list from an Iceberg table metadata blob."""
    schemas = meta.get("schemas") or []
    current_id = meta.get("current-schema-id")
    schema: Optional[dict] = None
    if schemas and current_id is not None:
        schema = next((s for s in schemas if s.get("schema-id") == current_id), None)
    if schema is None:
        # Fallback: top-level "schema" field on older spec
        schema = meta.get("schema") or (schemas[0] if schemas else None)
    if not schema:
        return []
    fields = schema.get("fields") or []
    cols: list[dict] = []
    for f in fields:
        cols.append({
            "id": f.get("id"),
            "name": f.get("name"),
            "type": f.get("type"),
            "required": bool(f.get("required", False)),
            "doc": f.get("doc"),
        })
    return cols


def _extract_snapshot_info(meta: dict) -> tuple[Optional[int], Optional[int]]:
    """Return (row_count_approx, last_updated_ms) from the current snapshot."""
    current_snap_id = meta.get("current-snapshot-id")
    snapshots = meta.get("snapshots") or []
    if current_snap_id is None or not snapshots:
        return None, meta.get("last-updated-ms")
    snap = next((s for s in snapshots if s.get("snapshot-id") == current_snap_id), None)
    if not snap:
        return None, meta.get("last-updated-ms")
    summary = snap.get("summary") or {}
    row_count: Optional[int] = None
    raw = summary.get("total-records")
    if raw is not None:
        try:
            row_count = int(raw)
        except (TypeError, ValueError):
            row_count = None
    return row_count, snap.get("timestamp-ms") or meta.get("last-updated-ms")


async def get_table_info(
    namespace: str, name: str, rest_base_url: str = DEFAULT_REST_BASE_URL
) -> dict:
    """Fetch a single table's metadata. No cache — callers may want fresh data."""
    ns_path = namespace.replace(".", "%1F")
    url = f"{rest_base_url.rstrip('/')}/v1/namespaces/{ns_path}/tables/{name}"
    data = await _get_json(url)
    meta = data.get("metadata") or {}
    row_count, last_updated_ms = _extract_snapshot_info(meta)
    return {
        "namespace": namespace,
        "name": name,
        "location": meta.get("location") or data.get("metadata-location"),
        "schema": _extract_schema(meta),
        "row_count_approx": row_count,
        "last_updated_ms": last_updated_ms,
        "format_version": meta.get("format-version"),
        "partition_specs": meta.get("partition-specs") or [],
    }
