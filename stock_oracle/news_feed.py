"""
News Feed Module
==================
Pulls company news from Finnhub and general market news.
Caches results to avoid hitting rate limits.

Usage:
    feed = NewsFeed(api_key="your_finnhub_key")
    articles = feed.get_news("AAPL", days=3)
    market_news = feed.get_market_news()
"""
import json
import logging
import time
import requests
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger("stock_oracle")

CACHE_TTL = 600  # Cache news for 10 minutes


class NewsFeed:
    """Fetches and caches stock news from Finnhub."""

    COMPANY_NEWS_URL = "https://finnhub.io/api/v1/company-news"
    MARKET_NEWS_URL = "https://finnhub.io/api/v1/news"

    def __init__(self, api_key: str = ""):
        self.api_key = api_key
        self._cache: Dict[str, Dict] = {}  # {ticker: {timestamp, articles}}
        self._market_cache: Optional[Dict] = None

    def get_news(self, ticker: str, days: int = 3,
                 max_articles: int = 15) -> List[Dict]:
        """
        Get recent news for a ticker.

        Returns list of:
        {
            "headline": str,
            "summary": str,
            "source": str,
            "url": str,
            "timestamp": int (unix),
            "datetime": str (human readable),
            "age": str ("2h ago", "1d ago"),
            "ticker": str,
        }
        """
        if not self.api_key:
            return []

        # Check cache
        cached = self._cache.get(ticker)
        if cached and time.time() - cached["fetched_at"] < CACHE_TTL:
            return cached["articles"][:max_articles]

        try:
            today = datetime.now().strftime("%Y-%m-%d")
            from_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

            resp = requests.get(self.COMPANY_NEWS_URL, params={
                "symbol": ticker.upper(),
                "from": from_date,
                "to": today,
                "token": self.api_key,
            }, timeout=10)

            if resp.status_code != 200:
                logger.debug(f"News fetch failed for {ticker}: {resp.status_code}")
                return []

            raw = resp.json()
            if not isinstance(raw, list):
                return []

            articles = []
            for item in raw[:max_articles]:
                ts = item.get("datetime", 0)
                articles.append({
                    "headline": item.get("headline", ""),
                    "summary": item.get("summary", ""),
                    "source": item.get("source", ""),
                    "url": item.get("url", ""),
                    "timestamp": ts,
                    "datetime": self._format_datetime(ts),
                    "age": self._format_age(ts),
                    "ticker": ticker,
                    "image": item.get("image", ""),
                    "category": item.get("category", ""),
                })

            self._cache[ticker] = {
                "fetched_at": time.time(),
                "articles": articles,
            }
            return articles[:max_articles]

        except Exception as e:
            logger.debug(f"News fetch error for {ticker}: {e}")
            return []

    def get_market_news(self, max_articles: int = 20) -> List[Dict]:
        """Get general market/financial news."""
        if not self.api_key:
            return []

        if (self._market_cache and
                time.time() - self._market_cache["fetched_at"] < CACHE_TTL):
            return self._market_cache["articles"][:max_articles]

        try:
            resp = requests.get(self.MARKET_NEWS_URL, params={
                "category": "general",
                "token": self.api_key,
            }, timeout=10)

            if resp.status_code != 200:
                return []

            raw = resp.json()
            articles = []
            for item in raw[:max_articles]:
                ts = item.get("datetime", 0)
                articles.append({
                    "headline": item.get("headline", ""),
                    "summary": item.get("summary", ""),
                    "source": item.get("source", ""),
                    "url": item.get("url", ""),
                    "timestamp": ts,
                    "datetime": self._format_datetime(ts),
                    "age": self._format_age(ts),
                    "ticker": "",
                    "category": item.get("category", ""),
                })

            self._market_cache = {
                "fetched_at": time.time(),
                "articles": articles,
            }
            return articles[:max_articles]

        except Exception as e:
            logger.debug(f"Market news fetch error: {e}")
            return []

    def get_watchlist_news(self, tickers: List[str], days: int = 2,
                           max_per_ticker: int = 5,
                           max_total: int = 50) -> List[Dict]:
        """
        Get combined news for all tickers, sorted by recency.
        """
        all_articles = []
        for ticker in tickers:
            articles = self.get_news(ticker, days=days,
                                     max_articles=max_per_ticker)
            all_articles.extend(articles)

        # Deduplicate by headline (same article can appear for related tickers)
        seen = set()
        unique = []
        for a in all_articles:
            key = a["headline"][:80]
            if key not in seen:
                seen.add(key)
                unique.append(a)

        # Sort by timestamp descending (newest first)
        unique.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
        return unique[:max_total]

    def clear_cache(self):
        """Clear all cached news."""
        self._cache.clear()
        self._market_cache = None

    @staticmethod
    def _format_datetime(ts: int) -> str:
        if not ts:
            return ""
        try:
            dt = datetime.fromtimestamp(ts)
            return dt.strftime("%b %d, %I:%M %p")
        except Exception:
            return ""

    @staticmethod
    def _format_age(ts: int) -> str:
        if not ts:
            return ""
        try:
            age = time.time() - ts
            if age < 3600:
                return f"{int(age / 60)}m ago"
            elif age < 86400:
                return f"{int(age / 3600)}h ago"
            elif age < 604800:
                return f"{int(age / 86400)}d ago"
            else:
                return f"{int(age / 604800)}w ago"
        except Exception:
            return ""
