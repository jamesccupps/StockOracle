"""
Ticker Resolver
===============
Automatically resolves ANY stock ticker to its company name.
No more maintaining a manual dictionary.

Uses yfinance for lookup, caches results to disk so each
ticker is only looked up once.

Usage:
    from stock_oracle.utils.ticker_resolver import resolve_name

    name = resolve_name("LUNR")   # -> "Intuitive Machines, Inc."
    name = resolve_name("AAPL")   # -> "Apple Inc."
    name = resolve_name("XYZFAKE")  # -> "XYZFAKE" (falls back to ticker)
"""
import json
import logging
import threading
from pathlib import Path
from typing import Optional

from stock_oracle.config import CACHE_DIR

logger = logging.getLogger("stock_oracle")

NAMES_CACHE_FILE = CACHE_DIR / "company_names.json"

# In-memory cache (loaded from disk on first call)
_name_cache: Optional[dict] = None
_resolve_lock = threading.Lock()


def _load_cache() -> dict:
    """Load the name cache from disk."""
    global _name_cache
    if _name_cache is not None:
        return _name_cache

    if NAMES_CACHE_FILE.exists():
        try:
            _name_cache = json.loads(NAMES_CACHE_FILE.read_text())
        except Exception:
            _name_cache = {}
    else:
        _name_cache = {}

    return _name_cache


def _save_cache():
    """Persist name cache to disk."""
    if _name_cache is not None:
        NAMES_CACHE_FILE.write_text(json.dumps(_name_cache, indent=2))


def _lookup_yfinance(ticker: str) -> Optional[str]:
    """Look up company name via yfinance."""
    try:
        import yfinance as yf
        stock = yf.Ticker(ticker)
        info = stock.info
        # Try multiple fields — yfinance is inconsistent about which one is populated
        name = (
            info.get("shortName")
            or info.get("longName")
            or info.get("displayName")
        )
        if name:
            # Clean up suffixes that hurt search quality
            for suffix in [", Inc.", " Inc.", " Inc", " Corp.", " Corp",
                           " Corporation", " Ltd.", " Ltd", " Limited",
                           " Holdings", " Group", " plc", " PLC",
                           " Incorporated", " Co.", " Co", " SE",
                           " N.V.", " S.A.", " AG", " NV"]:
                if name.endswith(suffix):
                    name = name[:-len(suffix)].strip()
                    break
            return name
    except Exception as e:
        logger.debug(f"yfinance lookup failed for {ticker}: {e}")
    return None


def _lookup_sec(ticker: str) -> Optional[str]:
    """Fallback: look up company name from SEC EDGAR."""
    try:
        import requests
        url = "https://www.sec.gov/files/company_tickers.json"
        headers = {"User-Agent": "StockOracle research@stockoracle.local"}
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            for entry in data.values():
                if entry.get("ticker", "").upper() == ticker.upper():
                    name = entry.get("title", "")
                    if name:
                        # SEC names are ALL CAPS, title-case them
                        return name.title()
    except Exception as e:
        logger.debug(f"SEC lookup failed for {ticker}: {e}")
    return None


def resolve_name(ticker: str) -> str:
    """
    Resolve a ticker symbol to a human-readable company name.
    Cached permanently after first lookup.

    Returns the ticker itself as fallback if lookup fails.
    """
    ticker = ticker.upper()
    cache = _load_cache()

    # Check cache first (no lock needed for reads)
    if ticker in cache:
        return cache[ticker]

    # Lock to prevent parallel threads from all doing the same lookup
    with _resolve_lock:
        # Double-check after acquiring lock
        if ticker in cache:
            return cache[ticker]

        # Try yfinance first (best quality names)
        name = _lookup_yfinance(ticker)

        # Fallback to SEC EDGAR
        if not name:
            name = _lookup_sec(ticker)

        # Final fallback: just use the ticker
        if not name:
            name = ticker

        # Cache it
        cache[ticker] = name
        _name_cache[ticker] = name
        _save_cache()

        logger.info(f"Resolved {ticker} -> {name}")
        return name


def resolve_batch(tickers: list) -> dict:
    """Resolve multiple tickers at once. Returns {ticker: name} dict."""
    return {t: resolve_name(t) for t in tickers}


def get_cached_names() -> dict:
    """Return all cached ticker->name mappings."""
    return dict(_load_cache())
