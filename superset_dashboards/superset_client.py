"""Superset REST API client — read-only dashboard inventory puller.

Given a running Superset base URL plus credentials, this module logs in,
lists every dashboard the account can see, and enriches each row with the
signals we care about for a dashboard register:

  - owner(s)        from the dashboard's `owners`
  - team            best-effort: custom tags -> roles -> owner (Superset has
                    no first-class "team" field, so we surface the closest
                    proxy and let the human confirm)
  - frequency       real scheduled cadence from Alerts & Reports (crontab),
                    falling back to the dashboard's auto-refresh interval
  - disposition     always "Superset" here (this tool only reads Superset);
                    Excel-based dashboards are added to the register by hand

Everything is GET-only. No dashboard is modified. The client degrades
gracefully: if the account lacks permission on /report/ or a detail call
fails, that signal is left blank rather than aborting the whole pull.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from typing import Any, Optional

import httpx


# Auto-refresh interval (seconds) -> human label, mirroring Superset's own
# refresh-interval dropdown so the "frequency" column reads sensibly.
_REFRESH_LABELS = {
    0: "",
    10: "Every 10 seconds",
    30: "Every 30 seconds",
    60: "Every minute",
    300: "Every 5 minutes",
    1800: "Every 30 minutes",
    3600: "Every hour",
    21600: "Every 6 hours",
    43200: "Every 12 hours",
    86400: "Every 24 hours",
}


class SupersetError(RuntimeError):
    """Raised for auth / connectivity problems, with a user-facing message."""


@dataclass
class DashboardRow:
    id: int
    title: str
    owners: str
    team: str
    frequency: str
    disposition: str
    status: str
    tags: str
    roles: str
    last_modified: str
    last_modified_by: str
    created_on: str
    url: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class FetchResult:
    base_url: str
    version: str
    count: int
    dashboards: list[DashboardRow] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def _full_name(person: dict[str, Any]) -> str:
    first = (person or {}).get("first_name") or ""
    last = (person or {}).get("last_name") or ""
    name = f"{first} {last}".strip()
    return name or (person or {}).get("username") or ""


class SupersetClient:
    """Thin, read-only wrapper over the Superset REST API (v1)."""

    def __init__(self, base_url: str, timeout: float = 30.0, verify: bool = True):
        self.base_url = base_url.rstrip("/")
        self._http = httpx.Client(
            base_url=self.base_url,
            timeout=timeout,
            verify=verify,
            follow_redirects=True,
            headers={"Accept": "application/json"},
        )
        self._token: Optional[str] = None

    # -- lifecycle -------------------------------------------------------

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "SupersetClient":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- auth ------------------------------------------------------------

    def login(self, username: str, password: str, provider: str = "db") -> None:
        """Obtain a JWT access token via /api/v1/security/login."""
        try:
            resp = self._http.post(
                "/api/v1/security/login",
                json={
                    "username": username,
                    "password": password,
                    "provider": provider,
                    "refresh": True,
                },
            )
        except httpx.HTTPError as exc:
            raise SupersetError(
                f"Could not reach Superset at {self.base_url}: {exc}"
            ) from exc

        if resp.status_code == 401:
            raise SupersetError("Login failed: bad username or password.")
        if resp.status_code == 404:
            raise SupersetError(
                "Login endpoint not found — is this really a Superset URL? "
                "(expected /api/v1/security/login)"
            )
        if resp.status_code >= 400:
            raise SupersetError(
                f"Login failed (HTTP {resp.status_code}): {resp.text[:300]}"
            )

        token = resp.json().get("access_token")
        if not token:
            raise SupersetError("Login succeeded but no access_token was returned.")
        self._token = token
        self._http.headers["Authorization"] = f"Bearer {token}"

    # -- low-level GET ---------------------------------------------------

    def _get(self, path: str, params: Optional[dict] = None) -> httpx.Response:
        resp = self._http.get(path, params=params)
        return resp

    def version(self) -> str:
        try:
            resp = self._get("/api/v1/menu/")  # cheap authed probe; not for version
        except httpx.HTTPError:
            return "unknown"
        # Superset exposes version at /version but it's not always enabled;
        # keep this best-effort and non-fatal.
        try:
            v = self._http.get("/api/v1/openapi/v1/version")
            if v.status_code == 200:
                return v.json().get("version", "unknown")
        except httpx.HTTPError:
            pass
        return "unknown"

    # -- reports (real scheduled frequency) ------------------------------

    def _report_frequency_map(self, warnings: list[str]) -> dict[int, str]:
        """Map dashboard_id -> human crontab from Alerts & Reports.

        Requires can_read on ReportSchedule. If forbidden, we warn and
        return an empty map rather than failing the whole pull.
        """
        freq: dict[int, str] = {}
        page = 0
        while True:
            q = f"(page:{page},page_size:100)"
            resp = self._get("/api/v1/report/", params={"q": q})
            if resp.status_code == 403:
                warnings.append(
                    "This account cannot read Alerts & Reports, so scheduled "
                    "email/report frequency is blank. Grant 'can read on "
                    "ReportSchedule' to populate it."
                )
                break
            if resp.status_code >= 400:
                warnings.append(
                    f"Could not read report schedules (HTTP {resp.status_code}); "
                    "frequency falls back to dashboard auto-refresh."
                )
                break
            body = resp.json()
            for rep in body.get("result", []):
                dash = rep.get("dashboard") or {}
                did = dash.get("id")
                if did is None:
                    continue
                label = rep.get("crontab_humanized") or rep.get("crontab") or ""
                if rep.get("active") is False and label:
                    label = f"{label} (paused)"
                if label:
                    freq.setdefault(did, label)
            total = body.get("count", 0)
            page += 1
            if page * 100 >= total or not body.get("result"):
                break
        return freq

    # -- dashboards ------------------------------------------------------

    def _refresh_frequency(self, dash: dict[str, Any]) -> str:
        """Pull auto-refresh interval out of json_metadata, if present."""
        meta_raw = dash.get("json_metadata")
        if not meta_raw:
            return ""
        try:
            meta = json.loads(meta_raw)
        except (json.JSONDecodeError, TypeError):
            return ""
        secs = meta.get("refresh_frequency")
        if not secs:
            return ""
        return _REFRESH_LABELS.get(secs, f"Auto-refresh every {secs}s")

    def _team_of(self, dash: dict[str, Any], owners: str) -> str:
        """Best-effort team: custom tags -> roles -> owner name."""
        tags = [
            t.get("name", "")
            for t in dash.get("tags", [])
            # tag type 1 == custom tag in Superset; owner/type tags are auto
            if t.get("type") in (1, "custom", "TagType.custom") and t.get("name")
        ]
        if tags:
            return ", ".join(tags)
        roles = [r.get("name", "") for r in dash.get("roles", []) if r.get("name")]
        if roles:
            return ", ".join(roles)
        return owners or ""

    def fetch_dashboards(self) -> FetchResult:
        """List all visible dashboards and build enriched rows."""
        if not self._token:
            raise SupersetError("Not authenticated — call login() first.")

        warnings: list[str] = []
        freq_map = self._report_frequency_map(warnings)

        rows: list[DashboardRow] = []
        page = 0
        total = 0
        while True:
            q = (
                f"(order_column:changed_on_delta_humanized,"
                f"order_direction:desc,page:{page},page_size:100)"
            )
            resp = self._get("/api/v1/dashboard/", params={"q": q})
            if resp.status_code == 403:
                raise SupersetError(
                    "This account cannot read dashboards (need 'can read on "
                    "Dashboard')."
                )
            if resp.status_code >= 400:
                raise SupersetError(
                    f"Listing dashboards failed (HTTP {resp.status_code}): "
                    f"{resp.text[:300]}"
                )
            body = resp.json()
            total = body.get("count", 0)
            results = body.get("result", [])
            for d in results:
                owners = ", ".join(_full_name(o) for o in d.get("owners", [])) or ""
                tags = ", ".join(
                    t.get("name", "") for t in d.get("tags", []) if t.get("name")
                )
                roles = ", ".join(
                    r.get("name", "") for r in d.get("roles", []) if r.get("name")
                )
                did = d.get("id")
                frequency = freq_map.get(did) or self._refresh_frequency(d)
                status = "Published" if d.get("published") else "Draft"
                url = d.get("url") or ""
                if url and url.startswith("/"):
                    url = f"{self.base_url}{url}"
                rows.append(
                    DashboardRow(
                        id=did,
                        title=d.get("dashboard_title") or "(untitled)",
                        owners=owners,
                        team=self._team_of(d, owners),
                        frequency=frequency,
                        disposition="Superset",
                        status=status,
                        tags=tags,
                        roles=roles,
                        last_modified=d.get("changed_on_delta_humanized") or "",
                        last_modified_by=_full_name(d.get("changed_by") or {}),
                        created_on=d.get("created_on_delta_humanized") or "",
                        url=url,
                    )
                )
            page += 1
            if not results or page * 100 >= total:
                break

        return FetchResult(
            base_url=self.base_url,
            version=self.version(),
            count=total or len(rows),
            dashboards=rows,
            warnings=warnings,
        )
