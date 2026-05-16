from __future__ import annotations
import asyncio
import itertools
import threading
from collections import defaultdict, deque
from typing import Optional

from .models import LogEvent


class EventBus:
    """Per-install event bus with bounded history + live async subscribers.

    Each event gets a monotonic per-install sequence number assigned at publish
    time. The WebSocket handler sends `seq` on every event; on reconnect the
    client passes `last_seq` and we replay only events with `seq > last_seq`,
    or send a `reset` marker if last_seq is older than what we still have
    buffered. This replaces the prior dedup-by-tuple set which leaked memory.
    """

    def __init__(self, history_size: int = 5000):
        self._history: dict[str, deque[LogEvent]] = defaultdict(lambda: deque(maxlen=history_size))
        self._next_seq: dict[str, itertools.count] = defaultdict(lambda: itertools.count(1))
        self._subscribers: dict[str, set[asyncio.Queue]] = defaultdict(set)
        self._sub_lock = asyncio.Lock()
        self._hist_lock = threading.Lock()
        self._dropped: dict[str, int] = defaultdict(int)

    def _assign_seq(self, event: LogEvent) -> LogEvent:
        # Assign monotonic seq under the history lock so seq matches insertion order.
        if event.seq is None:
            event.seq = next(self._next_seq[event.install_id])
        return event

    def _append_history(self, event: LogEvent) -> None:
        with self._hist_lock:
            self._assign_seq(event)
            self._history[event.install_id].append(event)

    def _fanout(self, event: LogEvent) -> None:
        queues = list(self._subscribers.get(event.install_id, ()))
        for q in queues:
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                self._dropped[event.install_id] += 1
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

    def history_snapshot(self, install_id: str, *, since_seq: int = 0) -> tuple[list[LogEvent], int, bool]:
        """Return (events, next_seq_hint, reset_needed).

        `since_seq` is the last seq the client has seen; events with
        seq > since_seq are returned. `reset_needed` is True when
        since_seq is older than the oldest event still buffered (the
        client must clear local state and accept the full history)."""
        with self._hist_lock:
            dq = self._history.get(install_id)
            if not dq:
                return [], 0, False
            snapshot = list(dq)
            oldest_seq = snapshot[0].seq or 0
            newest_seq = snapshot[-1].seq or 0
            reset_needed = since_seq > 0 and since_seq < oldest_seq
            if since_seq == 0 or reset_needed:
                return snapshot, newest_seq, reset_needed
            filtered = [e for e in snapshot if (e.seq or 0) > since_seq]
            return filtered, newest_seq, False

    # Backward-compat name used by evidence.py
    def history(self, install_id: str) -> list[LogEvent]:
        events, _, _ = self.history_snapshot(install_id)
        return events

    async def subscribe(self, install_id: str) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=10000)
        async with self._sub_lock:
            self._subscribers[install_id].add(q)
        return q

    async def unsubscribe(self, install_id: str, q: asyncio.Queue) -> None:
        async with self._sub_lock:
            subs = self._subscribers.get(install_id)
            if subs is None:
                return
            subs.discard(q)
            if not subs:
                # Reap empty subscriber sets; the history dict stays so a
                # reconnecting client can replay.
                del self._subscribers[install_id]


bus = EventBus()
