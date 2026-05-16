from __future__ import annotations
import asyncio
from collections import defaultdict, deque
from typing import Any

from .models import LogEvent


class EventBus:
    """Per-install event bus: keeps a bounded history + live subscribers."""

    def __init__(self, history_size: int = 5000):
        self._history: dict[str, deque[LogEvent]] = defaultdict(lambda: deque(maxlen=history_size))
        self._subscribers: dict[str, set[asyncio.Queue]] = defaultdict(set)
        self._lock = asyncio.Lock()

    async def publish(self, event: LogEvent) -> None:
        self._history[event.install_id].append(event)
        async with self._lock:
            queues = list(self._subscribers.get(event.install_id, ()))
        for q in queues:
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                pass

    def publish_nowait(self, event: LogEvent) -> None:
        """Fire-and-forget from sync code: appends to history; live delivery happens via subscribers."""
        self._history[event.install_id].append(event)
        for q in list(self._subscribers.get(event.install_id, ())):
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                pass

    def history(self, install_id: str) -> list[LogEvent]:
        return list(self._history.get(install_id, ()))

    async def subscribe(self, install_id: str) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=10000)
        async with self._lock:
            self._subscribers[install_id].add(q)
        return q

    async def unsubscribe(self, install_id: str, q: asyncio.Queue) -> None:
        async with self._lock:
            self._subscribers[install_id].discard(q)


bus = EventBus()
