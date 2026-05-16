"""HTTP client used by every `lks` command.

Wraps httpx with:
  - Bearer auth from --token or env LHS_TOKEN
  - URL joining anchored at --server
  - Friendly error rendering (404 / 409 / 503 → exit 1, never a stack trace)
  - One injection point for tests: `make_client(transport=...)`
"""
from __future__ import annotations

import os
from typing import Any, Optional
from urllib.parse import urljoin

import click
import httpx

DEFAULT_SERVER = "http://127.0.0.1:7878"
DEFAULT_TIMEOUT = 30.0


class ApiError(click.ClickException):
    """Surfaces backend errors as a Click error so exit code is 1 and the
    message lands on stderr without a traceback."""

    def __init__(self, status: int, message: str):
        super().__init__(f"HTTP {status}: {message}")
        self.exit_code = 1


def make_client(
    server: str,
    token: Optional[str],
    transport: Optional[httpx.BaseTransport] = None,
) -> httpx.Client:
    """Build the httpx.Client used for a single CLI invocation.

    The `transport` parameter is the ONLY test seam — tests pass
    `httpx.MockTransport(handler)` to intercept all calls without touching
    a real socket.
    """
    headers: dict[str, str] = {"Accept": "application/json", "User-Agent": "lks/0.1.0"}
    if token:
        # Server accepts both `Authorization: Bearer ...` and `X-Studio-Token`.
        # Send the bearer form — it's the standard one.
        headers["Authorization"] = f"Bearer {token}"
    return httpx.Client(
        base_url=server.rstrip("/"),
        timeout=DEFAULT_TIMEOUT,
        headers=headers,
        transport=transport,
    )


def resolve_token(token_flag: Optional[str]) -> Optional[str]:
    """--token wins, then LHS_TOKEN env var. Empty string is treated as unset."""
    if token_flag:
        return token_flag
    env = os.environ.get("LHS_TOKEN")
    return env if env else None


def request(
    client: httpx.Client,
    method: str,
    path: str,
    *,
    json_body: Optional[dict[str, Any]] = None,
    params: Optional[dict[str, Any]] = None,
) -> Any:
    """Issue an HTTP request and return the decoded JSON body.

    On non-2xx, raises ApiError with the server's `detail` (or the raw text
    if the body isn't JSON). NetworkError / TimeoutException also surface
    as a single-line ApiError — never a stack trace.
    """
    try:
        response = client.request(method, _join(client.base_url, path), json=json_body, params=params)
    except httpx.TimeoutException as e:
        raise ApiError(0, f"request timed out: {e}")
    except httpx.HTTPError as e:
        raise ApiError(0, f"network error: {type(e).__name__}: {e}")

    if response.status_code >= 400:
        detail = _extract_detail(response)
        raise ApiError(response.status_code, detail)

    if not response.content:
        return None
    ctype = response.headers.get("content-type", "")
    if "application/json" in ctype:
        return response.json()
    return response.text


def download(client: httpx.Client, path: str, dest: str) -> int:
    """Stream a binary body to `dest`. Returns total bytes written.

    Used for `lks export` — the install export endpoint returns a gzip
    tarball, NOT JSON.
    """
    try:
        with client.stream("GET", _join(client.base_url, path)) as response:
            if response.status_code >= 400:
                response.read()
                raise ApiError(response.status_code, _extract_detail(response))
            written = 0
            with open(dest, "wb") as fh:
                for chunk in response.iter_bytes():
                    fh.write(chunk)
                    written += len(chunk)
            return written
    except httpx.HTTPError as e:
        raise ApiError(0, f"download failed: {type(e).__name__}: {e}")


def _join(base: Any, path: str) -> str:
    """Join base + path, tolerating leading slashes on either side."""
    base_str = str(base).rstrip("/")
    return f"{base_str}/{path.lstrip('/')}"


def _extract_detail(response: httpx.Response) -> str:
    """Best-effort extraction of FastAPI's `{"detail": ...}` envelope."""
    try:
        payload = response.json()
    except Exception:
        return (response.text or response.reason_phrase or "request failed").strip()
    if isinstance(payload, dict) and "detail" in payload:
        d = payload["detail"]
        return d if isinstance(d, str) else str(d)
    return str(payload)
