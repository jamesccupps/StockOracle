"""
Base Collector
==============
All signal collectors inherit from this base class.
Provides caching, rate limiting, error handling, and a standard interface.
"""
import json
import time
import hashlib
import logging
import requests
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from stock_oracle.config import CACHE_DIR, CACHE_TTL_HOURS, REQUEST_DELAY, MAX_RETRIES

logger = logging.getLogger("stock_oracle")


class SignalResult:
    """Standardized result from any collector."""

    def __init__(
        self,
        collector_name: str,
        ticker: str,
        signal_value: float,        # -1.0 (very bearish) to +1.0 (very bullish)
        confidence: float,           # 0.0 to 1.0
        raw_data: Any = None,
        details: str = "",
        timestamp: Optional[datetime] = None,
        **kwargs,                    # Accept extra keys gracefully (e.g. from cache)
    ):
        self.collector_name = collector_name
        self.ticker = ticker
        self.signal_value = max(-1.0, min(1.0, signal_value))
        self.confidence = max(0.0, min(1.0, confidence))
        self.raw_data = raw_data
        self.details = details
        self.timestamp = timestamp or datetime.now(timezone.utc)

    def to_dict(self) -> Dict:
        result = {
            "collector": self.collector_name,
            "ticker": self.ticker,
            "signal": self.signal_value,
            "confidence": self.confidence,
            "details": self.details,
            "timestamp": self.timestamp.isoformat() if isinstance(self.timestamp, datetime) else str(self.timestamp),
        }
        # Include raw_data if it's serializable (needed for GUI deep dive)
        if self.raw_data is not None:
            try:
                import json
                json.dumps(self.raw_data, default=str)  # Test serializability
                result["raw_data"] = self.raw_data
            except (TypeError, ValueError):
                pass  # Skip non-serializable raw_data
        return result

    @classmethod
    def from_dict(cls, d: Dict) -> "SignalResult":
        """Reconstruct from to_dict() output or cache data."""
        sr = cls(
            collector_name=d.get("collector", d.get("collector_name", "unknown")),
            ticker=d.get("ticker", ""),
            signal_value=d.get("signal", d.get("signal_value", 0.0)),
            confidence=d.get("confidence", 0.0),
            details=d.get("details", ""),
        )
        sr.raw_data = d.get("raw_data")
        return sr

    def __repr__(self):
        direction = "BULL" if self.signal_value > 0 else "BEAR" if self.signal_value < 0 else "NEUTRAL"
        return (
            f"<Signal {self.collector_name} | {self.ticker} | "
            f"{direction} {self.signal_value:+.2f} | conf={self.confidence:.0%}>"
        )


class BaseCollector(ABC):
    """
    Base class for all data collectors.

    Each collector must implement:
      - name: str property
      - collect(ticker) -> SignalResult

    Features:
      - Automatic caching with session-aware TTL
      - Circuit breaker: if a collector fails 3 times in a row,
        it's disabled for 10 minutes to avoid wasting time
    """

    # Shared across all instances: tracks which collectors are broken
    _failure_counts: Dict[str, int] = {}
    _failure_until: Dict[str, float] = {}  # timestamp when to retry
    FAILURE_THRESHOLD = 2     # failures before circuit opens
    FAILURE_COOLDOWN = 600    # seconds to wait before retrying (10 min)

    def __init__(self):
        self._last_request_time = 0
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        })

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique name for this collector."""
        pass

    @abstractmethod
    def collect(self, ticker: str) -> SignalResult:
        """Collect signal data for a given ticker."""
        pass

    # ── Caching ────────────────────────────────────────────────

    def _cache_key(self, *args) -> str:
        raw = f"{self.name}:{'|'.join(str(a) for a in args)}"
        return hashlib.md5(raw.encode()).hexdigest()

    def _get_cached(self, *args) -> Optional[Any]:
        key = self._cache_key(*args)
        cache_file = CACHE_DIR / f"{key}.json"
        if cache_file.exists():
            try:
                data = json.loads(cache_file.read_text())
                cached_at = datetime.fromisoformat(data["_cached_at"])
                if cached_at.tzinfo is None:
                    cached_at = cached_at.replace(tzinfo=timezone.utc)

                # Adaptive TTL: shorter during market hours for fresh data
                ttl = CACHE_TTL_HOURS
                try:
                    from stock_oracle.collectors.finnhub_collector import get_market_session
                    session = get_market_session()
                    if session.get("is_open"):
                        ttl = 0.083  # ~5 minutes during market hours
                    elif session.get("is_extended"):
                        ttl = 0.133  # ~8 minutes during pre/after hours
                    # Off-hours: use default (4 hours)
                except Exception:
                    pass

                if datetime.now(timezone.utc) - cached_at < timedelta(hours=ttl):
                    return data["payload"]
            except (KeyError, ValueError, json.JSONDecodeError):
                pass
        return None

    def _set_cache(self, payload: Any, *args):
        key = self._cache_key(*args)
        cache_file = CACHE_DIR / f"{key}.json"
        data = {
            "_cached_at": datetime.now(timezone.utc).isoformat(),
            "payload": payload,
        }
        cache_file.write_text(json.dumps(data, default=str))

    # ── Rate-limited requests ──────────────────────────────────

    # Host-level failure tracking (shared across all collectors)
    _host_failures: Dict[str, int] = {}
    _host_until: Dict[str, float] = {}

    def _request(self, url: str, params: Dict = None, headers: Dict = None) -> Optional[requests.Response]:
        # Check host-level circuit breaker
        from urllib.parse import urlparse
        host = urlparse(url).hostname or ""

        # Permanent disable: if a host has failed 10+ times, give up for this session
        if BaseCollector._host_failures.get(host, 0) >= 10:
            return None  # Permanently dead this session

        until = BaseCollector._host_until.get(host, 0)
        if time.time() < until:
            return None  # Host is in cooldown

        elapsed = time.time() - self._last_request_time
        if elapsed < REQUEST_DELAY:
            time.sleep(REQUEST_DELAY - elapsed)

        for attempt in range(MAX_RETRIES):
            try:
                self._last_request_time = time.time()
                resp = self._session.get(url, params=params, headers=headers, timeout=8)
                if resp.status_code == 200:
                    BaseCollector._host_failures[host] = 0  # Reset on success
                    return resp
                elif resp.status_code == 429:
                    wait = 2 ** attempt * 3
                    logger.warning(f"{self.name}: Rate limited, waiting {wait}s")
                    time.sleep(wait)
                    continue
                else:
                    # Track persistent failures (403, 404, 500, etc.)
                    count = BaseCollector._host_failures.get(host, 0) + 1
                    BaseCollector._host_failures[host] = count
                    if count >= 10:
                        logger.warning(f"{self.name}: host {host} permanently disabled this session ({count} errors)")
                    elif count >= 3:
                        # Escalating cooldown: 10min, 20min, 30min...
                        cooldown = min(600 * (count // 3), 3600)
                        BaseCollector._host_until[host] = time.time() + cooldown
                        if count == 3:
                            logger.warning(f"{self.name}: host {host} disabled for {cooldown//60}min after {count} errors")
                    else:
                        logger.warning(f"{self.name}: HTTP {resp.status_code} from {url}")
                    return None  # Don't return error responses — callers may not check status
            except requests.RequestException as e:
                count = BaseCollector._host_failures.get(host, 0) + 1
                BaseCollector._host_failures[host] = count
                if count >= 10:
                    logger.warning(f"{self.name}: host {host} permanently disabled this session ({count} errors)")
                elif count >= 2:
                    cooldown = min(600 * (count // 2), 3600)
                    BaseCollector._host_until[host] = time.time() + cooldown
                    if count <= 3:
                        logger.warning(f"{self.name}: host {host} disabled for {cooldown//60}min ({e})")
                else:
                    logger.error(f"{self.name}: Request error: {e}")
                time.sleep(1)

        return None

    # ── Utility ────────────────────────────────────────────────

    def _neutral_signal(self, ticker: str, reason: str = "No data") -> SignalResult:
        return SignalResult(
            collector_name=self.name,
            ticker=ticker,
            signal_value=0.0,
            confidence=0.0,
            details=reason,
        )

    def _safe_collect(self, ticker: str) -> SignalResult:
        """
        Wrapper that catches exceptions, returns neutral on failure,
        and implements a circuit breaker for persistently broken collectors.
        """
        # Circuit breaker: skip if this collector is in cooldown
        until = BaseCollector._failure_until.get(self.name, 0)
        if time.time() < until:
            return self._neutral_signal(ticker, f"Skipped (cooldown until retry)")

        try:
            result = self.collect(ticker)
            # Success: reset failure count
            BaseCollector._failure_counts[self.name] = 0
            return result
        except Exception as e:
            # Track failures
            count = BaseCollector._failure_counts.get(self.name, 0) + 1
            BaseCollector._failure_counts[self.name] = count

            if count >= self.FAILURE_THRESHOLD:
                BaseCollector._failure_until[self.name] = time.time() + self.FAILURE_COOLDOWN
                logger.warning(f"{self.name}: circuit breaker OPEN after {count} failures, "
                               f"retry in {self.FAILURE_COOLDOWN}s")
            else:
                logger.error(f"{self.name} failed for {ticker}: {e}")

            return self._neutral_signal(ticker, f"Error: {e}")
