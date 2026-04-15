"""
Real-Time News Signal Collector
==================================
Pulls recent company news from Finnhub and converts it into a
trading signal that actually changes between scans.

Key differences from the old news_sentiment collector:
  - Uses Finnhub API (fast, reliable, real financial news)
  - 15-minute cache instead of 4 hours — catches breaking news
  - Recency weighting: articles from last 2 hours count 3x more
  - News velocity detection: sudden spike in articles = event
  - Financial-specific sentiment with context awareness
  - Returns different signals per scan as news flow changes
"""
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from stock_oracle.collectors.base import BaseCollector, SignalResult

logger = logging.getLogger("stock_oracle")

# Short cache — news changes fast
NEWS_CACHE_TTL = 900  # 15 minutes (vs 4 hours for old collector)


class RealtimeNewsCollector(BaseCollector):
    """
    Real-time news sentiment from Finnhub.
    Updates every 15 minutes, weights recent articles heavily.
    """

    COMPANY_NEWS_URL = "https://finnhub.io/api/v1/company-news"

    # ── Sentiment lexicon (financial-specific) ──────────────────
    STRONG_POSITIVE = {
        "surge", "soar", "rocket", "skyrocket", "breakout", "all-time high",
        "record high", "blowout", "crushes", "smashes", "tops estimates",
        "beats expectations", "massive contract", "major deal", "approved",
        "fda approval", "contract win", "awarded", "partnership",
        "acquisition", "merger", "buyout offer", "price target raised",
        "upgraded", "double upgraded", "strong buy", "outperform",
    }
    MODERATE_POSITIVE = {
        "rises", "gains", "climbs", "advances", "rally", "jumps",
        "beats", "tops", "exceeds", "growth", "revenue up", "profit up",
        "expansion", "launches", "innovation", "breakthrough",
        "bullish", "optimistic", "positive", "upbeat", "confident",
        "momentum", "recovery", "rebounds", "bounces", "turnaround",
        "raised guidance", "upside", "catalyst", "opportunity",
    }
    STRONG_NEGATIVE = {
        "crash", "plunge", "collapse", "tank", "plummet", "freefall",
        "bankruptcy", "fraud", "sec investigation", "fbi", "indicted",
        "recall", "massive loss", "catastrophic", "default", "delisted",
        "halted", "suspended", "liquidation", "misses badly",
        "slashes guidance", "warns", "profit warning",
    }
    MODERATE_NEGATIVE = {
        "falls", "drops", "declines", "slips", "sinks", "slides",
        "misses", "below estimates", "disappoints", "weak", "concern",
        "downgrade", "sell rating", "underperform", "cut",
        "layoffs", "restructuring", "lawsuit", "litigation",
        "investigation", "subpoena", "bearish", "pessimistic",
        "headwinds", "pressure", "risk", "uncertainty", "downturn",
        "lowers guidance", "reduced forecast", "price target cut",
    }

    @property
    def name(self) -> str:
        return "realtime_news"

    def collect(self, ticker: str) -> SignalResult:
        # Read API key dynamically
        import stock_oracle.config as cfg
        api_key = cfg.FINNHUB_API_KEY
        if not api_key:
            return self._neutral_signal(ticker, "No Finnhub API key")

        # Use short-lived cache
        cache_key = f"rt_news_{ticker}"
        cached = self._get_timed_cache(cache_key)
        if cached:
            return SignalResult.from_dict(cached)

        # Fetch recent news (last 3 days)
        articles = self._fetch_news(ticker, api_key, days=3)

        if not articles:
            return self._neutral_signal(ticker, "No recent news")

        # Score each article with recency weighting
        now = time.time()
        scored_articles = []
        for article in articles:
            headline = article.get("headline", "")
            summary = article.get("summary", "")
            ts = article.get("datetime", 0)
            source = article.get("source", "")
            age_hours = (now - ts) / 3600 if ts else 999

            # Sentiment score from headline + summary
            sentiment = self._score_sentiment(headline, summary)

            # Recency weight: last 2 hours = 3x, last 6 hours = 2x, last 24h = 1x, older = 0.5x
            if age_hours < 2:
                recency_weight = 3.0
            elif age_hours < 6:
                recency_weight = 2.0
            elif age_hours < 24:
                recency_weight = 1.0
            else:
                recency_weight = 0.5

            # Source credibility weight
            premium_sources = {"Reuters", "Bloomberg", "CNBC", "WSJ",
                               "Financial Times", "MarketWatch", "Barrons"}
            source_weight = 1.3 if source in premium_sources else 1.0

            weighted_score = sentiment * recency_weight * source_weight

            scored_articles.append({
                "headline": headline[:100],
                "sentiment": round(sentiment, 3),
                "age_hours": round(age_hours, 1),
                "recency_weight": recency_weight,
                "weighted_score": round(weighted_score, 3),
                "source": source,
            })

        # Compute final signal
        total_weight = sum(abs(a["weighted_score"]) + 0.1 for a in scored_articles)
        if total_weight > 0:
            weighted_sentiment = sum(a["weighted_score"] for a in scored_articles) / total_weight
        else:
            weighted_sentiment = 0

        # ── News velocity: many articles in short time = breaking event ──
        recent_2h = sum(1 for a in scored_articles if a["age_hours"] < 2)
        recent_6h = sum(1 for a in scored_articles if a["age_hours"] < 6)
        total_articles = len(scored_articles)

        velocity_mult = 1.0
        if recent_2h >= 5:
            velocity_mult = 1.5  # Breaking news — high article volume
        elif recent_6h >= 10:
            velocity_mult = 1.3  # Active news day

        # Final signal
        signal = weighted_sentiment * velocity_mult
        signal = max(-0.6, min(0.6, signal))  # Cap at ±0.6

        # Confidence based on article count + recency
        if recent_2h >= 3:
            confidence = 0.75  # Fresh breaking news
        elif recent_6h >= 5:
            confidence = 0.65  # Active recent coverage
        elif total_articles >= 5:
            confidence = 0.50  # Decent coverage
        else:
            confidence = 0.35  # Sparse coverage

        # Build detail string
        top_articles = sorted(scored_articles, key=lambda x: abs(x["weighted_score"]),
                              reverse=True)[:3]
        detail_headlines = [f"{'+'if a['sentiment']>0 else ''}{a['sentiment']:.1f} "
                           f"({a['age_hours']:.0f}h) {a['headline'][:50]}"
                           for a in top_articles]
        detail_str = (
            f"{total_articles} articles ({recent_2h} <2h, {recent_6h} <6h) | "
            f"sentiment={weighted_sentiment:+.3f} vel={velocity_mult:.1f}x"
        )

        result = SignalResult(
            collector_name=self.name,
            ticker=ticker,
            signal_value=signal,
            confidence=confidence,
            raw_data={
                "article_count": total_articles,
                "recent_2h": recent_2h,
                "recent_6h": recent_6h,
                "weighted_sentiment": round(weighted_sentiment, 4),
                "velocity_mult": velocity_mult,
                "top_articles": top_articles[:5],
                "signal": round(signal, 4),
            },
            details=detail_str,
        )

        self._set_timed_cache(result.to_dict(), cache_key)
        return result

    def _fetch_news(self, ticker: str, api_key: str, days: int = 3) -> List[Dict]:
        """Fetch recent news from Finnhub."""
        today = datetime.now().strftime("%Y-%m-%d")
        from_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

        resp = self._request(self.COMPANY_NEWS_URL, params={
            "symbol": ticker.upper(),
            "from": from_date,
            "to": today,
            "token": api_key,
        })

        if resp and resp.status_code == 200:
            try:
                data = resp.json()
                if isinstance(data, list):
                    return data[:50]  # Cap at 50 articles
            except Exception:
                pass
        return []

    def _score_sentiment(self, headline: str, summary: str = "") -> float:
        """
        Score headline + summary sentiment.
        Returns -1.0 to +1.0.
        """
        text = (headline + " " + summary).lower()

        strong_pos = sum(1 for phrase in self.STRONG_POSITIVE if phrase in text)
        mod_pos = sum(1 for phrase in self.MODERATE_POSITIVE if phrase in text)
        strong_neg = sum(1 for phrase in self.STRONG_NEGATIVE if phrase in text)
        mod_neg = sum(1 for phrase in self.MODERATE_NEGATIVE if phrase in text)

        # Weighted: strong words count 2x
        pos_score = strong_pos * 2 + mod_pos
        neg_score = strong_neg * 2 + mod_neg

        total = pos_score + neg_score
        if total == 0:
            return 0.0

        return (pos_score - neg_score) / total

    # ── Short-lived cache (15 min instead of 4 hours) ──────────

    _timed_cache: Dict = {}

    def _get_timed_cache(self, key: str) -> Optional[Dict]:
        entry = self._timed_cache.get(key)
        if entry and time.time() - entry["time"] < NEWS_CACHE_TTL:
            return entry["data"]
        return None

    def _set_timed_cache(self, data: Dict, key: str):
        self._timed_cache[key] = {"data": data, "time": time.time()}
