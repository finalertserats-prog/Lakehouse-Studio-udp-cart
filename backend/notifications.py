"""Notifications: pluggable multi-channel event dispatch.

Pure additive scaffold (v0.4.0). The dispatcher owns drivers (toast / email /
slack), reads channel/rule config from `work/notifications.yaml`, and dedups
events by (install_id, event_type) within a severity-derived TTL window.

v0.4.1: wire dispatch calls from runner.py state transitions and smoke-step
exceptions — minimal call sites: post-finalize -> install_completed,
post-smoke-fail -> smoke_failed. Runner is FROZEN in v0.4.0; do not edit.
"""
from __future__ import annotations
import asyncio
import logging
import os
import smtplib
import time
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Literal, Optional, Protocol

import httpx
import yaml
from pydantic import BaseModel, Field

from .config import WORK_DIR
from .events import bus
from .models import LogEvent
from .redact import redact


log = logging.getLogger("lhs.notify")


Severity = Literal["info", "warn", "critical"]


# TTL per severity. Critical events repeat fastest (5min), info slowest (1h).
_DEDUP_TTL: dict[str, float] = {"critical": 300.0, "warn": 900.0, "info": 3600.0}

_CONFIG_FILENAME = "notifications.yaml"
_POLL_INTERVAL_SEC = 5.0
_HTTP_TIMEOUT_SEC = 10.0
_SMTP_TIMEOUT_SEC = 10.0


class NotifyEvent(BaseModel):
    event_type: str
    severity: Severity
    install_id: Optional[str] = None
    title: str
    body: str
    links: dict[str, str] = Field(default_factory=dict)
    ts: float


class Driver(Protocol):
    async def send(self, event: NotifyEvent) -> None: ...


def _resolve_indirection(value: Any) -> tuple[Optional[str], Optional[str]]:
    """Resolve ${ENV} or keyring:name indirections. Returns (resolved, error).

    Plaintext non-empty strings are rejected here so they cannot silently
    propagate into log lines or webhook bodies. The dispatcher refuses to
    register a driver whose secret field failed to resolve.
    """
    if value is None or value == "":
        return "", None
    if not isinstance(value, str):
        return None, f"expected string, got {type(value).__name__}"
    if value.startswith("${") and value.endswith("}"):
        env_key = value[2:-1].strip()
        resolved = os.environ.get(env_key)
        if not resolved:
            return None, f"env var {env_key} not set"
        return resolved, None
    if value.startswith("keyring:"):
        # Keyring support is opt-in; if the lib isn't present we surface a
        # clear reason instead of trying to import at module load time.
        try:
            import keyring  # type: ignore
        except ImportError:
            return None, "keyring indirection used but 'keyring' package not installed"
        name = value.split(":", 1)[1].strip()
        resolved = keyring.get_password("lakehouse-studio", name)
        if not resolved:
            return None, f"keyring entry 'lakehouse-studio/{name}' not found"
        return resolved, None
    return None, "plaintext secrets rejected; use ${ENV} or keyring:name indirection"


class ToastDriver:
    """Republishes the event onto the install's bus as a LogEvent.

    Uses kind="log" + step="notify" so the existing WebSocket consumer needs
    no schema change. The payload carries the structured event for the UI.
    """

    async def send(self, event: NotifyEvent) -> None:
        if not event.install_id:
            # Toasts without an install scope have no bus to land on; drop.
            return
        bus.publish_nowait(LogEvent(
            install_id=event.install_id,
            ts=event.ts,
            kind="log",
            stream="stdout",
            step="notify",
            line=f"[{event.severity}] {event.title}: {redact(event.body)}",
            payload={
                "event_type": event.event_type,
                "severity": event.severity,
                "title": event.title,
                "body": redact(event.body),
                "links": event.links,
            },
        ))


class EmailDriver:
    """SMTP send. Synchronous smtplib wrapped via asyncio.to_thread."""

    def __init__(self, *, smtp_host: str, smtp_port: int, username: str,
                 password: str, sender: str, recipients: list[str]):
        self.smtp_host = smtp_host
        self.smtp_port = int(smtp_port)
        self.username = username
        self.password = password
        self.sender = sender
        self.recipients = recipients

    def _send_sync(self, event: NotifyEvent) -> None:
        msg = EmailMessage()
        msg["Subject"] = f"[lakehouse-studio][{event.severity}] {event.title}"
        msg["From"] = self.sender
        msg["To"] = ", ".join(self.recipients)
        body = redact(event.body)
        if event.links:
            body += "\n\nLinks:\n" + "\n".join(f"  {k}: {v}" for k, v in event.links.items())
        if event.install_id:
            body += f"\n\nInstall: {event.install_id}"
        msg.set_content(body)
        with smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=_SMTP_TIMEOUT_SEC) as s:
            s.ehlo()
            try:
                s.starttls()
                s.ehlo()
            except smtplib.SMTPException:
                # Server doesn't support STARTTLS — proceed unencrypted only
                # if the user explicitly configured a non-default port (587
                # implies STARTTLS, so we refuse to silently downgrade).
                if self.smtp_port == 587:
                    raise
            if self.username:
                s.login(self.username, self.password)
            s.send_message(msg)

    async def send(self, event: NotifyEvent) -> None:
        if not self.recipients:
            raise RuntimeError("email driver has no recipients configured")
        # Refuse to attempt SMTP AUTH with an empty resolved secret — that's
        # an unset ${ENV} or keyring miss, which would emit a weak/null login
        # attempt the server logs as an auth failure. Fail loudly here instead.
        if self.username and not self.password:
            raise RuntimeError(
                "email driver has username but no resolved secret — "
                "check ${ENV} indirection or keyring entry"
            )
        await asyncio.to_thread(self._send_sync, event)


class SlackDriver:
    """POSTs to a Slack-compatible webhook URL."""

    def __init__(self, *, webhook_url: str):
        self.webhook_url = webhook_url

    async def send(self, event: NotifyEvent) -> None:
        text = f"*[{event.severity.upper()}] {event.title}*\n{redact(event.body)}"
        if event.install_id:
            text += f"\n_install_: `{event.install_id}`"
        if event.links:
            text += "\n" + " | ".join(f"<{v}|{k}>" for k, v in event.links.items())
        async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT_SEC) as client:
            r = await client.post(self.webhook_url, json={"text": text})
            r.raise_for_status()


class EventDispatcher:
    """Owns drivers + rules + dedup state, with mtime-poll config reload.

    Drivers are registered by name (toast/email/slack). `dispatch` looks up
    the rule list for an event_type and sends through each enabled channel.
    Dedup is keyed by (install_id, event_type) with TTL by severity.
    """

    def __init__(self, *, config_path: Optional[Path] = None):
        self.config_path = config_path or (WORK_DIR / _CONFIG_FILENAME)
        self._drivers: dict[str, Driver] = {}
        self._channel_cfg: dict[str, dict[str, Any]] = {}
        self._rules: dict[str, list[str]] = {}
        self._dedup: dict[tuple[Optional[str], str], float] = {}
        self._mtime: float = 0.0
        self._lock = asyncio.Lock()
        self._poll_task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        # Toast is always available — it just republishes onto the in-process
        # bus, no external dependencies.
        self._drivers["toast"] = ToastDriver()
        self._channel_cfg["toast"] = {"enabled": True}

    def register_driver(self, name: str, driver: Driver) -> None:
        self._drivers[name] = driver

    def get_public_config(self) -> dict[str, Any]:
        """Config view safe to serve over the API — secrets replaced with marker."""
        out_channels: dict[str, Any] = {}
        for name, cfg in self._channel_cfg.items():
            scrub = dict(cfg)
            for key in list(scrub.keys()):
                kl = key.lower()
                if "password" in kl or "webhook" in kl or "token" in kl:
                    val = scrub.get(key)
                    if val:
                        scrub[key] = "<configured>"
            out_channels[name] = scrub
        return {"channels": out_channels, "rules": dict(self._rules)}

    async def dispatch(self, event: NotifyEvent) -> None:
        """Fan out event to all channels mapped to its event_type."""
        await self._maybe_reload()
        channels = self._rules.get(event.event_type, [])
        if not channels:
            return
        key = (event.install_id, event.event_type)
        ttl = _DEDUP_TTL.get(event.severity, _DEDUP_TTL["info"])
        last = self._dedup.get(key, 0.0)
        if event.ts - last < ttl:
            return
        self._dedup[key] = event.ts
        # Prune ancient dedup entries opportunistically so the table stays small.
        cutoff = event.ts - max(_DEDUP_TTL.values())
        for k in list(self._dedup.keys()):
            if self._dedup[k] < cutoff:
                self._dedup.pop(k, None)
        for ch in channels:
            cfg = self._channel_cfg.get(ch, {})
            if not cfg.get("enabled"):
                continue
            driver = self._drivers.get(ch)
            if driver is None:
                log.warning("notify: rule references unknown channel %r", ch)
                continue
            try:
                await driver.send(event)
            except Exception as e:
                log.exception("notify: channel %r send failed: %s", ch, e)

    async def send_through(self, channel: str, event: NotifyEvent) -> tuple[bool, str]:
        """Send a one-off through a single channel, bypassing rules + dedup.

        Used by /api/notifications/test. Returns (ok, detail).
        """
        await self._maybe_reload()
        cfg = self._channel_cfg.get(channel)
        if not cfg:
            return False, f"channel '{channel}' not configured"
        if not cfg.get("enabled"):
            return False, f"channel '{channel}' is disabled in config"
        driver = self._drivers.get(channel)
        if driver is None:
            return False, f"channel '{channel}' has no driver (config error)"
        try:
            await driver.send(event)
            return True, "sent"
        except Exception as e:
            log.exception("notify-test: channel %r failed", channel)
            return False, f"{type(e).__name__}: {e}"

    # ---- config loading ----

    def _load_config_sync(self) -> Optional[dict[str, Any]]:
        if not self.config_path.exists():
            return None
        try:
            raw = yaml.safe_load(self.config_path.read_text(encoding="utf-8"))
        except yaml.YAMLError as e:
            log.error("notify config parse error: %s", e)
            return None
        if not isinstance(raw, dict):
            log.error("notify config root must be a mapping, got %s", type(raw).__name__)
            return None
        return raw

    def _apply_config(self, raw: dict[str, Any]) -> None:
        channels = raw.get("channels") or {}
        rules = raw.get("rules") or {}
        if not isinstance(channels, dict) or not isinstance(rules, dict):
            log.error("notify config: 'channels' and 'rules' must be mappings")
            return

        new_drivers: dict[str, Driver] = {"toast": ToastDriver()}
        new_cfg: dict[str, dict[str, Any]] = {}

        for name, ccfg in channels.items():
            if not isinstance(ccfg, dict):
                log.warning("notify config: channel %r ignored (not a mapping)", name)
                continue
            new_cfg[name] = dict(ccfg)
            if not ccfg.get("enabled"):
                continue
            try:
                if name == "toast":
                    pass  # always-on, already registered
                elif name == "slack":
                    url, err = _resolve_indirection(ccfg.get("webhook_url"))
                    if err or not url:
                        log.error("notify slack disabled: %s", err or "no webhook_url")
                        new_cfg[name]["enabled"] = False
                        continue
                    new_drivers["slack"] = SlackDriver(webhook_url=url)
                elif name == "email":
                    pw, err = _resolve_indirection(ccfg.get("password"))
                    if err:
                        log.error("notify email disabled: password: %s", err)
                        new_cfg[name]["enabled"] = False
                        continue
                    recipients = ccfg.get("to") or []
                    if not isinstance(recipients, list) or not recipients:
                        log.error("notify email disabled: 'to' must be a non-empty list")
                        new_cfg[name]["enabled"] = False
                        continue
                    new_drivers["email"] = EmailDriver(
                        smtp_host=str(ccfg.get("smtp_host", "")),
                        smtp_port=int(ccfg.get("smtp_port", 587)),
                        username=str(ccfg.get("username", "")),
                        password=pw or "",
                        sender=str(ccfg.get("from", "")),
                        recipients=[str(r) for r in recipients],
                    )
                else:
                    log.warning("notify config: unknown channel %r ignored", name)
            except Exception as e:
                log.exception("notify: failed to build driver %r: %s", name, e)
                new_cfg[name]["enabled"] = False

        clean_rules: dict[str, list[str]] = {}
        for evt, chans in rules.items():
            if not isinstance(chans, list):
                log.warning("notify config: rule %r ignored (value must be a list)", evt)
                continue
            clean_rules[str(evt)] = [str(c) for c in chans]

        # Replace atomically so dispatch() never sees a half-applied config.
        self._drivers = new_drivers
        self._channel_cfg = new_cfg
        self._rules = clean_rules
        log.info("notify config loaded: %d channels, %d rules",
                 sum(1 for c in new_cfg.values() if c.get("enabled")), len(clean_rules))

    async def _maybe_reload(self) -> None:
        async with self._lock:
            try:
                st = self.config_path.stat()
            except FileNotFoundError:
                if self._mtime != 0.0:
                    log.info("notify config removed; reverting to toast-only")
                    self._drivers = {"toast": ToastDriver()}
                    self._channel_cfg = {"toast": {"enabled": True}}
                    self._rules = {}
                    self._mtime = 0.0
                return
            if st.st_mtime == self._mtime:
                return
            raw = self._load_config_sync()
            if raw is not None:
                self._apply_config(raw)
            self._mtime = st.st_mtime

    async def start(self) -> None:
        await self._maybe_reload()
        if self._poll_task is None or self._poll_task.done():
            self._stop.clear()
            self._poll_task = asyncio.create_task(self._poll_loop(), name="notify-config-poll")

    async def stop(self) -> None:
        self._stop.set()
        if self._poll_task is not None:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except (asyncio.CancelledError, Exception):
                pass
            self._poll_task = None

    async def _poll_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=_POLL_INTERVAL_SEC)
                return
            except asyncio.TimeoutError:
                pass
            try:
                await self._maybe_reload()
            except Exception:
                log.exception("notify config poll failed")


_dispatcher: Optional[EventDispatcher] = None


def get_dispatcher() -> EventDispatcher:
    """Lazy global accessor. Routes + tests use this rather than constructing."""
    global _dispatcher
    if _dispatcher is None:
        _dispatcher = EventDispatcher()
    return _dispatcher


def load_config() -> Optional[dict[str, Any]]:
    """Re-read the YAML from disk without going through the dispatcher.

    Exposed for diagnostics / tests; the dispatcher polls mtime on its own.
    """
    d = get_dispatcher()
    return d._load_config_sync()


async def notify(
    install_id: Optional[str],
    event_type: str,
    severity: Severity,
    title: str,
    body: str,
    links: Optional[dict[str, str]] = None,
) -> None:
    """Top-level helper. Build a NotifyEvent and dispatch through the global bus."""
    evt = NotifyEvent(
        event_type=event_type,
        severity=severity,
        install_id=install_id,
        title=title,
        body=body,
        links=links or {},
        ts=time.time(),
    )
    await get_dispatcher().dispatch(evt)
