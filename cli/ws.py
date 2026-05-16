"""WebSocket log streamer for `lks install logs --follow`.

Uses httpx's WS support if available; otherwise falls back to the stdlib
`websockets` package only when --follow is requested. Without --follow,
this module is unused — the history endpoint is served via plain HTTP.
"""
from __future__ import annotations

import asyncio
import json
from typing import Optional
from urllib.parse import urlparse, urlunparse

from cli.render import echo, error


def http_to_ws(url: str) -> str:
    """Convert http(s) URL to ws(s) URL preserving host, port, path, query."""
    parsed = urlparse(url)
    scheme = "wss" if parsed.scheme == "https" else "ws"
    return urlunparse((scheme, parsed.netloc, parsed.path, parsed.params, parsed.query, parsed.fragment))


async def _stream(url: str, token: Optional[str]) -> int:
    """Connect to a Studio logs WS and pretty-print events until EOF.

    Returns 0 on clean close, 1 on connection / protocol failure.
    """
    try:
        import websockets  # type: ignore
    except ImportError:
        error(
            "the 'websockets' package is required for --follow. "
            "Install it with: pip install websockets"
        )
        return 1

    extra_headers = []
    if token:
        extra_headers.append(("Authorization", f"Bearer {token}"))

    try:
        async with websockets.connect(url, additional_headers=extra_headers or None) as ws:
            async for raw in ws:
                try:
                    evt = json.loads(raw)
                except Exception:
                    echo(str(raw))
                    continue
                echo(_format_event(evt))
    except Exception as e:
        error(f"websocket failed: {type(e).__name__}: {e}")
        return 1
    return 0


def _format_event(evt: dict) -> str:
    """Single-line render for one log event.

    Format: `[kind|stream] step: line`. Falls back gracefully if any field
    is missing — the bus shape evolves and we don't want a missing key to
    nuke a multi-hour install tail.
    """
    kind = evt.get("kind", "log")
    stream = evt.get("stream") or ""
    step = evt.get("step") or ""
    line = evt.get("line") or evt.get("status") or ""
    prefix = kind if not stream else f"{kind}|{stream}"
    if step:
        return f"[{prefix}] {step}: {line}"
    return f"[{prefix}] {line}"


def follow_logs(server: str, install_id: str, token: Optional[str]) -> int:
    """Synchronous wrapper used by the Click command."""
    ws_url = http_to_ws(f"{server.rstrip('/')}/api/installs/{install_id}/logs")
    try:
        return asyncio.run(_stream(ws_url, token))
    except KeyboardInterrupt:
        return 0
