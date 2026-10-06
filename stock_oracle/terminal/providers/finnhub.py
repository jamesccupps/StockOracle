"""
Finnhub provider (free key)
===========================
Real-time quotes, company news, market news, analyst recommendation trends,
peers and EPS surprises.

The free tier allows 60 calls/minute per key, and the GUI's collectors use
the same key, so this process limits itself (TERMINAL_FINNHUB_RPM, default 30).
When the budget is spent, calls raise RateLimited and the router falls back
to Yahoo instead of waiting.
"""
import logging
import os
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import requests

from stock_oracle.terminal import settings, symbols as S

logger = logging.getLogger("stock_oracle.terminal")

BASE_URL = os.environ.get("FINNHUB_BASE_URL", "https://finnhub.io/api/v1")


class RateLimited(Exception):
    pass


class FinnhubError(Exception):
    pass


class _TokenBucket:
    def __init__(self):
        self._lock = threading.Lock()
        self._tokens = None
        self._last = time.monotonic()

    def take(self, wait: float = 2.0) -> bool:
        deadline = time.monotonic() + wait
        while True:
            rpm = settings.finnhub_rpm()
            with self._lock:
                now = time.monotonic()
                if self._tokens is None:
                    self._tokens = float(rpm)
                self._tokens = min(float(rpm), self._tokens + (now - self._last) * rpm / 60.0)
                self._last = now
                if self._tokens >= 1:
                    self._tokens -= 1
                    return True
                need = (1 - self._tokens) * 60.0 / rpm
            if time.monotonic() + need > deadline:
                return False
            time.sleep(need)


_bucket = _TokenBucket()
_session = requests.Session()


def available() -> bool:
    return bool(settings.finnhub_key())


def _get(path: str, params: Dict, wait: float = 2.0):
    key = settings.finnhub_key()
    if not key:
        raise FinnhubError("no Finnhub key")
    if not _bucket.take(wait):
        raise RateLimited("terminal Finnhub budget exhausted")
    resp = _session.get(f"{BASE_URL}{path}", params={**params, "token": key}, timeout=10)
    if resp.status_code == 429:
        raise RateLimited("Finnhub returned 429")
    if resp.status_code in (401, 403):
        raise FinnhubError(f"Finnhub rejected the request ({resp.status_code}) — "
                           "check the key, or this endpoint needs a paid plan")
    if resp.status_code != 200:
        raise FinnhubError(f"Finnhub HTTP {resp.status_code}")
    return resp.json()


def _fsym(sym: str) -> str:
    f = S.to_finnhub(sym)
    if not f:
        raise FinnhubError(f"{sym} is not covered by Finnhub's free tier")
    return f


# ── quotes ───────────────────────────────────────────────────

def quote(sym: str) -> Optional[Dict]:
    d = _get("/quote", {"symbol": _fsym(sym)})
    price = d.get("c") or 0
    if not price:
        return None
    prev = d.get("pc") or None
    return {
        "symbol": sym,
        "price": float(price),
        "prev_close": float(prev) if prev else None,
        "open": d.get("o") or None,
        "high": d.get("h") or None,
        "low": d.get("l") or None,
        "volume": None,  # /quote has no volume; the Yahoo poll fills it in
        "change": d.get("d"),
        "change_pct": d.get("dp"),
        "ext_price": None,
        "ext_change_pct": None,
        "ext_session": None,
        "updated": float(d.get("t") or time.time()),
        "src": "finnhub",
    }


# ── company data ─────────────────────────────────────────────

def profile(sym: str) -> Dict:
    return _get("/stock/profile2", {"symbol": _fsym(sym)}) or {}


def peers(sym: str) -> List[str]:
    data = _get("/stock/peers", {"symbol": _fsym(sym)}) or []
    return [S.normalize(p) for p in data if isinstance(p, str)]


def recommendation_trend(sym: str) -> List[Dict]:
    data = _get("/stock/recommendation", {"symbol": _fsym(sym)}) or []
    out = []
    for r in data[:6]:
        out.append({
            "period": r.get("period", ""),
            "strong_buy": r.get("strongBuy", 0),
            "buy": r.get("buy", 0),
            "hold": r.get("hold", 0),
            "sell": r.get("sell", 0),
            "strong_sell": r.get("strongSell", 0),
        })
    return out


def eps_surprises(sym: str) -> List[Dict]:
    data = _get("/stock/earnings", {"symbol": _fsym(sym)}) or []
    return [{
        "date": r.get("period", ""),
        "eps_est": r.get("estimate"),
        "eps_actual": r.get("actual"),
        "surprise_pct": r.get("surprisePercent"),
    } for r in data]


def earnings_calendar(sym: str) -> Optional[Dict]:
    today = datetime.now()
    data = _get("/calendar/earnings", {
        "symbol": _fsym(sym),
        "from": today.strftime("%Y-%m-%d"),
        "to": (today + timedelta(days=120)).strftime("%Y-%m-%d"),
    }) or {}
    rows = sorted(data.get("earningsCalendar") or [], key=lambda r: r.get("date", ""))
    if not rows:
        return None
    r = rows[0]
    return {
        "date": r.get("date", ""),
        "hour": {"bmo": "before open", "amc": "after close", "dmh": "during hours"}.get(r.get("hour", ""), ""),
        "eps_avg": r.get("epsEstimate"),
        "rev_avg": r.get("revenueEstimate"),
        "quarter": r.get("quarter"),
        "year": r.get("year"),
    }


# ── news ─────────────────────────────────────────────────────

def _article(item: Dict, ticker: str = "") -> Dict:
    return {
        "headline": item.get("headline", ""),
        "summary": item.get("summary", ""),
        "source": item.get("source", ""),
        "url": item.get("url", ""),
        "timestamp": int(item.get("datetime") or 0),
        "related": [ticker] if ticker else ([item["related"]] if item.get("related") else []),
    }


def company_news(sym: str, days: int = 7, limit: int = 40) -> List[Dict]:
    today = datetime.now()
    data = _get("/company-news", {
        "symbol": _fsym(sym),
        "from": (today - timedelta(days=days)).strftime("%Y-%m-%d"),
        "to": today.strftime("%Y-%m-%d"),
    }) or []
    arts = [_article(a, sym) for a in data if a.get("headline")]
    arts.sort(key=lambda a: a["timestamp"], reverse=True)
    return arts[:limit]


def market_news(category: str = "general", limit: int = 40) -> List[Dict]:
    data = _get("/news", {"category": category}) or []
    arts = [_article(a) for a in data if a.get("headline")]
    arts.sort(key=lambda a: a["timestamp"], reverse=True)
    return arts[:limit]


def earnings_calendar_range(days: int = 90) -> List[Dict]:
    """All scheduled earnings in the next `days` days (one call, free tier)."""
    today = datetime.now()
    data = _get("/calendar/earnings", {
        "from": today.strftime("%Y-%m-%d"),
        "to": (today + timedelta(days=days)).strftime("%Y-%m-%d"),
    }, wait=5.0) or {}
    return data.get("earningsCalendar") or []
