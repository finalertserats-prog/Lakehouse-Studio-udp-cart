"""Dynamic version discovery for lakehouse components.

Fetches available versions from official sources:
  - Apache Software Foundation downloads directory (downloads.apache.org)
  - GitHub Releases API
  - Docker Hub tags API

Results are cached in-memory for _TTL_SECONDS (default 1 hour).
On fetch failure the error is returned as a single entry so the UI
can surface it gracefully rather than crashing.
"""
from __future__ import annotations

import os
import re
import time
from typing import Any

import httpx

# ── TTL cache ─────────────────────────────────────────────────────────────────
_CACHE: dict[str, tuple[float, list[dict[str, Any]]]] = {}
_TTL_SECONDS = 3600  # 1 hour


def _cached(key: str) -> list[dict[str, Any]] | None:
    entry = _CACHE.get(key)
    if entry and (time.time() - entry[0]) < _TTL_SECONDS:
        return entry[1]
    return None


def _store(key: str, data: list[dict[str, Any]]) -> None:
    _CACHE[key] = (time.time(), data)


def get_cached_at(component_id: str) -> float | None:
    entry = _CACHE.get(component_id)
    return entry[0] if entry else None


def clear_cache(component_id: str | None = None) -> None:
    if component_id:
        _CACHE.pop(component_id, None)
    else:
        _CACHE.clear()


# ── Version tuple for sorting ──────────────────────────────────────────────────

def _vtuple(version: str) -> tuple[int, ...]:
    """Convert a version string to an integer tuple for sorting."""
    nums = re.findall(r'\d+', version.split('_')[0].split('+')[0])
    return tuple(int(n) for n in nums[:5])


# ── Source fetch functions ────────────────────────────────────────────────────

def _fetch_apache_dir(
    subpath: str,
    prefix: str,
    filter_re: str | None = None,
    max_versions: int = 25,
) -> list[dict[str, Any]]:
    """Parse an Apache downloads.apache.org directory listing.

    subpath: e.g. "hadoop/common" → https://downloads.apache.org/hadoop/common/
    prefix:  e.g. "hadoop-" — stripped to get the bare version string
    filter_re: optional regex applied to the bare version (e.g. r"^3\\.")
    """
    url = f"https://downloads.apache.org/{subpath}/"
    resp = httpx.get(url, timeout=15, follow_redirects=True)
    resp.raise_for_status()

    # Apache directory listing format:
    #   <a href="hadoop-3.5.0/">hadoop-3.5.0/</a>  2026-04-02 02:01   -
    dir_pat = re.compile(
        rf'<a\s+href="({re.escape(prefix)}[^/"]+)/"',
        re.IGNORECASE,
    )
    # Optional date that follows the link on the same line
    date_pat = re.compile(r'(\d{4}-\d{2}-\d{2})')

    filt = re.compile(filter_re) if filter_re else None
    seen: set[str] = set()
    results: list[dict[str, Any]] = []

    for m in dir_pat.finditer(resp.text):
        dirname = m.group(1)
        version = dirname[len(prefix):]
        if not re.match(r'^\d', version):
            continue
        if filt and not filt.match(version):
            continue
        if version in seen:
            continue
        seen.add(version)

        # Look for a date on the same line (within 120 chars after the match)
        line_tail = resp.text[m.end(): m.end() + 120]
        date_m = date_pat.search(line_tail)
        entry: dict[str, Any] = {
            "version": version,
            "label": version,
            "source": "apache",
        }
        if date_m:
            entry["release_date"] = date_m.group(1)
        results.append(entry)

    results.sort(key=lambda x: _vtuple(x["version"]), reverse=True)
    return results[:max_versions]


def _fetch_github_releases(
    owner: str,
    repo: str,
    strip_prefixes: list[str] | None = None,
    filter_re: str | None = None,
    max_releases: int = 30,
) -> list[dict[str, Any]]:
    """Fetch stable releases from the GitHub Releases API."""
    url = f"https://api.github.com/repos/{owner}/{repo}/releases"
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    resp = httpx.get(url, params={"per_page": max_releases}, headers=headers, timeout=15)
    resp.raise_for_status()

    prefixes = strip_prefixes or ["v"]
    filt = re.compile(filter_re) if filter_re else None
    seen: set[str] = set()
    results: list[dict[str, Any]] = []

    for rel in resp.json():
        if rel.get("draft") or rel.get("prerelease"):
            continue
        tag = rel.get("tag_name", "")
        version = tag
        for pfx in prefixes + ["v", "release-", "rel/"]:
            if version.startswith(pfx):
                version = version[len(pfx):]
                break
        if not re.match(r'^\d', version):
            continue
        if filt and not filt.match(version):
            continue
        if version in seen:
            continue
        seen.add(version)
        results.append({
            "version": version,
            "label": version,
            "release_date": (rel.get("published_at") or "")[:10],
            "source": "github",
        })

    return results


def _fetch_dockerhub_tags(
    image: str,
    pattern: str | None = None,
    max_tags: int = 50,
) -> list[dict[str, Any]]:
    """Fetch image tags from Docker Hub."""
    owner, repo = image.split("/", 1) if "/" in image else ("library", image)
    url = f"https://hub.docker.com/v2/repositories/{owner}/{repo}/tags/"
    resp = httpx.get(
        url,
        params={"page_size": max_tags, "ordering": "last_updated"},
        timeout=15,
    )
    if resp.status_code == 404:
        return []   # repo not yet created / no tags pushed — treat as empty
    resp.raise_for_status()

    filt = re.compile(pattern) if pattern else None
    seen: set[str] = set()
    results: list[dict[str, Any]] = []

    for tag in resp.json().get("results", []):
        name = tag.get("name", "")
        if name in ("latest", "edge", "main", "master"):
            continue
        if filt and not filt.match(name):
            continue
        if name in seen:
            continue
        seen.add(name)

        # Parse compound tags like "3.5.5_1.8.1" (spark_iceberg)
        entry: dict[str, Any] = {
            "version": name,
            "label": name,
            "release_date": (tag.get("last_updated") or "")[:10],
            "source": "dockerhub",
        }
        if "_" in name and all(re.match(r'^\d+\.\d+', p) for p in name.split("_")):
            parts = name.split("_")
            entry["base_version"] = parts[0]  # e.g. "3.5.5" (Spark version)
            entry["addon_version"] = parts[1]  # e.g. "1.8.1" (Iceberg version)
        results.append(entry)

    # Keep sorted: newest first (already from API), but normalise
    results.sort(key=lambda x: _vtuple(x["version"]), reverse=True)
    return results


# ── Per-component source registry ────────────────────────────────────────────
# Maps component_id → zero-arg callable that returns list[dict].
# Multiple IDs can share the same callable (aliases).

_SOURCES: dict[str, Any] = {}


def _reg(comp_ids: list[str] | str, fn: Any) -> None:
    ids = [comp_ids] if isinstance(comp_ids, str) else comp_ids
    for cid in ids:
        _SOURCES[cid] = fn


# ── Apache HDFS / Hadoop ──────────────────────────────────────────────────────
_reg(
    ["hdfs", "hadoop-yarn"],
    lambda: _fetch_apache_dir("hadoop/common", "hadoop-", r"^3\."),
)

# ── Hive Metastore ────────────────────────────────────────────────────────────
# bitsondatadev/hive-metastore only has a "latest" tag on Docker Hub.
# Return a single static entry so the UI shows a version and the compose
# always resolves to the correct tag — never pulls a non-existent semver tag.
_reg(
    ["hive-metastore", "hive-metastore-hadoop"],
    lambda: [{"version": "latest", "label": "latest (bitsondatadev)", "source": "dockerhub"}],
)

# ── Apache Hive (HiveServer2) ─────────────────────────────────────────────────
_reg(
    ["hive"],
    lambda: _fetch_dockerhub_tags("apache/hive", pattern=r"^\d+\.\d+"),
)

# ── Apache Spark (Docker Hub — tabulario/spark-iceberg compound tags) ─────────
# Tags like "3.5.5_1.8.1" (Spark_Iceberg). Pure Spark versions without Iceberg
# (e.g. "3.4.4") are also listed for the YARN/enterprise variant.
_reg(
    ["spark-iceberg", "spark-hadoop"],
    lambda: _fetch_dockerhub_tags(
        "tabulario/spark-iceberg",
        pattern=r"^\d+\.\d+",  # starts with digit.digit
    ),
)

# ── Custom Studio Spark images ─────────────────────────────────────────────────
# lakehousestudio/spark-hudi and spark-delta are built + pushed by the image
# build pipeline (backend/image_builder.py). Before any image is pushed,
# _fetch_dockerhub_tags returns [] (404 → empty handled above) — version
# picker shows an empty list and a "Build Image" button.
_reg(
    ["spark-hudi"],
    lambda: _fetch_dockerhub_tags(
        "lakehousestudio/spark-hudi",
        pattern=r"^\d+\.\d+",
    ),
)
_reg(
    ["spark-delta"],
    lambda: _fetch_dockerhub_tags(
        "lakehousestudio/spark-delta",
        pattern=r"^\d+\.\d+",
    ),
)

# ── Apache TEZ ────────────────────────────────────────────────────────────────
_reg(
    ["tez"],
    lambda: _fetch_github_releases(
        "apache", "tez",
        strip_prefixes=["rel/", "release-"],
        filter_re=r"^0\.1",
        max_releases=20,
    ),
)

# ── Apache Iceberg ────────────────────────────────────────────────────────────
_reg(
    ["iceberg", "iceberg-rest"],
    lambda: _fetch_github_releases(
        "apache", "iceberg",
        strip_prefixes=["apache-iceberg-"],
        max_releases=30,
    ),
)

# ── Apache Hudi ───────────────────────────────────────────────────────────────
_reg(
    ["hudi", "hudi-v1"],
    lambda: _fetch_github_releases(
        "apache", "hudi",
        strip_prefixes=["release-"],
        filter_re=r"^(0\.\d+|1\.)",
        max_releases=20,
    ),
)

# ── Delta Lake ────────────────────────────────────────────────────────────────
_reg(
    ["delta"],
    lambda: _fetch_github_releases("delta-io", "delta", max_releases=20),
)

# ── Trino ─────────────────────────────────────────────────────────────────────
# Tags are plain numbers: 475, 476, ...
_reg(
    ["trino", "trino-enterprise"],
    lambda: _fetch_github_releases(
        "trinodb", "trino",
        strip_prefixes=[],
        filter_re=r"^\d{3}$",
        max_releases=30,
    ),
)

# ── StarRocks ─────────────────────────────────────────────────────────────────
_reg(
    ["starrocks"],
    lambda: _fetch_github_releases(
        "StarRocks", "starrocks",
        filter_re=r"^[34]\.",
        max_releases=20,
    ),
)

# ── MinIO ─────────────────────────────────────────────────────────────────────
# MinIO Docker Hub tags are RELEASE.YYYY-MM-DDTHH-MM-SSZ — fetch them directly
# from Docker Hub so version strings are always valid Docker image tags.
_reg(
    ["minio"],
    # Match only standard RELEASE.YYYY-MM-DDTHH-MM-SSZ tags; skip -cpuv1 / -arm variants
    lambda: _fetch_dockerhub_tags(
        "minio/minio",
        pattern=r"^RELEASE\.\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}Z$",
    ),
)

# ── Apache Kafka ──────────────────────────────────────────────────────────────
_reg(
    ["kafka"],
    lambda: _fetch_apache_dir("kafka", "", filter_re=r"^\d+\.\d"),
)

# ── Project Nessie ────────────────────────────────────────────────────────────
_reg(
    ["nessie"],
    lambda: _fetch_github_releases(
        "projectnessie", "nessie",
        strip_prefixes=["nessie-"],
        max_releases=20,
    ),
)

# ── Apache Polaris ────────────────────────────────────────────────────────────
_reg(
    ["polaris"],
    lambda: _fetch_github_releases("apache", "polaris", max_releases=15),
)

# ── Apache Airflow ────────────────────────────────────────────────────────────
_reg(
    ["airflow"],
    lambda: _fetch_github_releases(
        "apache", "airflow",
        filter_re=r"^[23]\.",
        max_releases=20,
    ),
)

# ── Dagster ───────────────────────────────────────────────────────────────────
_reg(
    ["dagster"],
    lambda: _fetch_github_releases(
        "dagster-io", "dagster",
        filter_re=r"^1\.",
        max_releases=15,
    ),
)

# ── Apache Superset ───────────────────────────────────────────────────────────
_reg(
    ["superset"],
    lambda: _fetch_github_releases(
        "apache", "superset",
        filter_re=r"^[34]\.",
        max_releases=15,
    ),
)

# ── Prometheus ────────────────────────────────────────────────────────────────
_reg(
    ["prometheus"],
    lambda: _fetch_github_releases("prometheus", "prometheus", max_releases=15),
)

# ── Grafana ───────────────────────────────────────────────────────────────────
_reg(
    ["grafana"],
    lambda: _fetch_github_releases(
        "grafana", "grafana",
        filter_re=r"^(10|11)\.",
        max_releases=15,
    ),
)

# ── Grafana Loki ──────────────────────────────────────────────────────────────
_reg(
    ["loki"],
    lambda: _fetch_github_releases(
        "grafana", "loki",
        filter_re=r"^[23]\.",
        max_releases=15,
    ),
)

# ── Apache Ranger ─────────────────────────────────────────────────────────────
_reg(
    ["ranger-admin"],
    lambda: _fetch_github_releases(
        "apache", "ranger",
        strip_prefixes=["ranger-"],
        max_releases=15,
    ),
)

# ── Apache YARN (same releases as HDFS — both ship in Hadoop) ─────────────────
_reg(
    ["hadoop-yarn", "hadoop-yarn-nm"],
    lambda: _fetch_apache_dir("hadoop/common", "hadoop-", r"^3\."),
)

# ── Trino Enterprise (same releases as Trino) ─────────────────────────────────
_reg(
    ["trino-enterprise"],
    lambda: _fetch_github_releases(
        "trinodb", "trino",
        strip_prefixes=[],
        filter_re=r"^\d{3}$",
        max_releases=30,
    ),
)

# ── Apache Polaris ────────────────────────────────────────────────────────────
_reg(
    ["polaris"],
    lambda: _fetch_github_releases("apache", "polaris", max_releases=15),
)

# ── Debezium ──────────────────────────────────────────────────────────────────
_reg(
    ["debezium"],
    lambda: _fetch_github_releases(
        "debezium", "debezium",
        filter_re=r"^[23]\.",
        max_releases=15,
    ),
)

# ── OpenLineage / Marquez ──────────────────────────────────────────────────────
_reg(
    ["openlineage"],
    lambda: _fetch_github_releases(
        "MarquezProject", "marquez",
        max_releases=15,
    ),
)

# ── PgBouncer ─────────────────────────────────────────────────────────────────
# GitHub releases use "pgbouncer_1_25_2" tags (underscores) which don't match
# Docker Hub tags (edoburu/pgbouncer uses "1.23.1" dot notation).
# Fetch from Docker Hub instead to get valid image tags directly.
_reg(
    ["pgbouncer"],
    lambda: _fetch_dockerhub_tags("edoburu/pgbouncer", pattern=r"^\d+\.\d+"),
)


# ── Public API ────────────────────────────────────────────────────────────────

def get_versions(component_id: str, force_refresh: bool = False) -> list[dict[str, Any]]:
    """Return available versions for a component (1-hour TTL cache).

    Returns an empty list if no source is registered for component_id.
    Returns a single ``{"error": True}`` entry if the fetch fails.
    """
    if not force_refresh:
        cached = _cached(component_id)
        if cached is not None:
            return cached

    fn = _SOURCES.get(component_id)
    if fn is None:
        return []

    try:
        versions = fn()
    except Exception as exc:
        return [{"version": "error", "label": f"Fetch failed: {exc}", "error": True}]

    _store(component_id, versions)
    return versions


def list_registered_components() -> list[str]:
    """Return all component IDs that have a registered version source."""
    return sorted(_SOURCES.keys())
