"""
Async TTL cache with single-flight fetching
===========================================
Provider calls are blocking (yfinance, requests), so they run in worker
threads. This cache makes sure that when five panels ask for NVDA's profile
at once, only one request goes out, and everyone awaits the same result.

If a refresh fails and an expired value is still around, the stale value is
returned instead of an error (a slightly old quote beats an empty panel).
"""
import asyncio
import logging
import time
from typing import Any, Callable, Dict, Tuple

logger = logging.getLogger("stock_oracle.terminal")


class AsyncTTLCache:
    def __init__(self, max_items: int = 4000):
        self._data: Dict[Any, Tuple[float, Any]] = {}   # key -> (expires_at, value)
        self._inflight: Dict[Any, asyncio.Future] = {}
        self._max = max_items

    def peek(self, key) -> Any:
        """Return a cached value (even if expired) or None."""
        hit = self._data.get(key)
        return hit[1] if hit else None

    def put(self, key, value, ttl: float):
        self._data[key] = (time.monotonic() + ttl, value)
        self._evict()

    def invalidate(self, key):
        self._data.pop(key, None)

    async def get(self, key, ttl: float, fn: Callable, *args, **kwargs) -> Any:
        hit = self._data.get(key)
        if hit and hit[0] > time.monotonic():
            return hit[1]

        fut = self._inflight.get(key)
        if fut is None:
            fut = asyncio.ensure_future(asyncio.to_thread(fn, *args, **kwargs))
            self._inflight[key] = fut

            def _done(f: asyncio.Future, key=key):
                self._inflight.pop(key, None)
                if f.cancelled():
                    return
                if f.exception() is None:
                    self.put(key, f.result(), ttl)

            fut.add_done_callback(_done)

        try:
            # shield: a client disconnecting must not cancel the shared fetch
            return await asyncio.shield(fut)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            stale = self._data.get(key)
            if stale is not None:
                logger.debug(f"cache: serving stale {key!r} after error: {e}")
                return stale[1]
            raise

    def _evict(self):
        if len(self._data) <= self._max:
            return
        # Drop the entries closest to expiry (oldest) first
        for key, _ in sorted(self._data.items(), key=lambda kv: kv[1][0])[: len(self._data) - self._max]:
            self._data.pop(key, None)
