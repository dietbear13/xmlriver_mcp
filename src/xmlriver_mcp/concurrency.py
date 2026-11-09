"""Bounded workers and adaptive, process-local HTTP admission control."""

import asyncio
import time
from collections import deque
from contextlib import asynccontextmanager
from contextvars import ContextVar

from .errors import XmlRiverError

# Context follows a worker across its HTTP retries, without mixing other workers' counts.
attempt_scopes: ContextVar[tuple] = ContextVar("xmlriver_attempt_scopes", default=())


@asynccontextmanager
async def attempt_scope(limit=None):
    stats = {"http_attempts": 0, "limit": limit}
    token = attempt_scopes.set((*attempt_scopes.get(), stats))
    try:
        yield stats
    finally:
        attempt_scopes.reset(token)


def count_attempt():
    scopes = attempt_scopes.get()
    check_stopped()
    # No await between checking and reserving: atomic within the event loop.
    for stats in scopes:
        stats["http_attempts"] += 1


def check_stopped():
    for stats in attempt_scopes.get():
        if stats.get("fatal"):
            raise stats["fatal"]
        if stats["limit"] is not None and stats["http_attempts"] >= stats["limit"]:
            raise XmlRiverError(kind="budget")


def notify_attempt():
    for stats in attempt_scopes.get():
        callback = stats.get("on_attempt")
        if callback:
            callback()


class AdaptiveGate:
    def __init__(self, maximum=9, rate_max=9, rate_window=1.0):
        if not 1 <= maximum <= 9 or rate_max < 1 or rate_window <= 0:
            raise ValueError("Invalid concurrency/rate limits")
        self.maximum = self.limit = maximum
        self.rate_max, self.rate_window = rate_max, rate_window
        self.active = 0
        self.cooldown_until = 0.0
        self.successes = 0
        self.starts: deque[float] = deque()
        self.condition = asyncio.Condition()

    async def acquire(self):
        async with self.condition:
            while True:
                check_stopped()
                now = time.monotonic()
                while self.starts and now - self.starts[0] >= self.rate_window:
                    self.starts.popleft()
                delay = max(0.0, self.cooldown_until - now)
                if len(self.starts) >= self.rate_max:
                    delay = max(delay, self.starts[0] + self.rate_window - now)
                if self.active < self.limit and delay <= 0:
                    self.active += 1
                    self.starts.append(now)
                    return
                if delay > 0:
                    try:
                        await asyncio.wait_for(self.condition.wait(), delay)
                    except TimeoutError:
                        pass
                else:
                    await self.condition.wait()

    async def release(self):
        async with self.condition:
            self.active -= 1
            self.condition.notify_all()

    async def failed(self, error, delay):
        async with self.condition:
            if error.kind in {"auth", "balance", "ip"}:
                for stats in attempt_scopes.get():
                    stats["fatal"] = error
            if error.kind == "temp":
                self.successes = 0
                self.cooldown_until = max(self.cooldown_until, time.monotonic() + delay)
                if error.code in {"110", "111", "115", "429"}:
                    self.limit = max(1, self.limit // 2)
            self.condition.notify_all()

    async def succeeded(self):
        async with self.condition:
            if time.monotonic() >= self.cooldown_until:
                self.successes += 1
                if self.successes >= 20 and self.limit < self.maximum:
                    self.limit += 1
                    self.successes = 0
                    self.condition.notify_all()


async def workers(items, concurrency, process, stopped):
    """Keep only N tasks alive, and await their cleanup before returning/cancelling."""
    iterator = iter(enumerate(items))

    async def worker():
        while not stopped():
            item = next(iterator, None)
            if item is None:
                return
            await process(*item)

    tasks = [asyncio.create_task(worker()) for _ in range(min(concurrency, len(items)))]
    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
