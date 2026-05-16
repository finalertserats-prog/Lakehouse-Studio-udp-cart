from __future__ import annotations
import asyncio
import threading
from collections import defaultdict, deque
from typing import Any

from .models import LogEvent


class EventBus:
    """Per-install event bus: bounded history + live async subscribers.

    The bus is accessed from both sync (subprocess drain callbacks scheduled
    on the loop) and async (FastAPI handlers) code, so we guard the history
    deque with a threading.Lock. Subscriber list mutations sit behind the
    asyncio.Lock so subscribe/unsubscribe don't race with publish.
    """

    def __init__(self, history_size: int = 5000):
        self._history: dict[str, deque[LogEvent]] = defaultdict(lambda: deque(maxlen=history_size))
        self._subscribers: dict[str, set[asyncio.Queue]] = defaultdict(set)
        self._sub_lock = asyncio.Lock()
        self._hist_lock = threading.Lock()
        self._dropped: dict[str, int] = defaultdict(int)

    def _append_history(self, event: LogEvent) -> None:
        with self._hist_lock:
            self._history[event.install_id].append(event)

    def _fanout(self, event: LogEvent) -> None:
        # Snapshot subscriber set under lock-free read of dict (defaultdict reads are atomic in CPython).
        queues = list(self._subscribers.get(event.install_id, ()))
        for q in queues:
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                self._dropped[event.install_id] += 1
                # Best-effort signal that we dropped — try once with a sentinel; if full, give up.
                try:
                    q.put_nowait(LogEvent(
                        install_id=event.install_id,
                        ts=event.ts,
                        kind="log",
                        stream="stderr",
                        line=f"[lakehouse-studio: subscriber slow — dropped {self._dropped[event.install_id]} events]",
                    ))
                except asyncio.QueueFull:
                    pass

    async def publish(self, event: LogEvent) -> None:
        self._append_history(event)
        self._fanout(event)

    def publish_nowait(self, event: LogEvent) -> None:
        """Fire-and-forget from sync or async code. Thread-safe."""
        self._append_history(event)
        self._fanout(event)

    def history_snapshot(self, install_id: str) -> tuple[list[LogEvent], int]:
        """Return (events, next_index). The next_index marks where new events
        published after this call will land — callers use it to dedupe live
        events against the replayed history.
        """
        with self._hist_lock:
            dq = self._history.get(install_id)
            if not dq:
                return [], 0
            snapshot = list(dq)
            return snapshot, len(snapshot)

    # Backward-compat name used by evidence.py
    def history(self, install_id: str) -> list[LogEvent]:
        events, _ = self.history_snapshot(install_id)
        return events

    async def subscribe(self, install_id: str) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=10000)
        async with self._sub_lock:
            self._subscribers[install_id].add(q)
        return q

    async def unsubscribe(self, install_id: str, q: asyncio.Queue) -> None:
        async with self._sub_lock:
            self._subscribers[install_id].discard(q)


bus = EventBus()
