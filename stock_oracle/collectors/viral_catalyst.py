"""
Viral Catalyst Collector
=========================
Detects viral executive moments and brand events that move stocks.

Examples this catches:
  - McDonald's CEO eating a burger on TikTok (MCD +3%)
  - Elon Musk tweeting about Dogecoin (TSLA volatility spike)
  - CEO doing AMA on Reddit (brand sentiment shift)
  - Executive caught on hot mic (negative viral moment)
  - Company product going viral on social media

How it works:
  We can't monitor TikTok/Instagram directly, but viral moments
  create a measurable ripple across detectable channels:
    1. Google Trends spike for executive name or brand
    2. Reddit discussion volume spike
    3. HackerNews/news coverage of the moment
    4. Wikipedia pageview spike for the executive

  When multiple channels spike simultaneously for the same entity,
  that's a viral catalyst event.

Signal interpretation:
  - Positive viral (CEO humanizing brand, product demos) = bullish
  - Negative viral (scandals, gaffes, controversies) = bearish
  - Neutral viral (just attention, no clear sentiment) = slight bullish
    (attention usually helps stock price short-term)
"""
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from stock_oracle.collectors.base import BaseCollector, SignalResult
from stock_oracle.utils.ticker_resolver import resolve_name

logger = logging.getLogger("stock_oracle")


# Map tickers to their key executives and brand terms to watch
EXECUTIVE_MAP = {
    "AAPL": {"execs": ["Tim Cook", "Apple CEO"], "brand": ["iPhone", "Apple Vision Pro", "Apple"]},
    "TSLA": {"execs": ["Elon Musk", "Tesla CEO"], "brand": ["Tesla", "Cybertruck", "Model Y"]},
    "MSFT": {"execs": ["Satya Nadella", "Microsoft CEO"], "brand": ["Microsoft", "Copilot", "Xbox"]},
    "GOOGL": {"execs": ["Sundar Pichai", "Google CEO"], "brand": ["Google", "Gemini AI", "YouTube"]},
    "AMZN": {"execs": ["Andy Jassy", "Amazon CEO"], "brand": ["Amazon", "AWS", "Prime"]},
    "META": {"execs": ["Mark Zuckerberg", "Meta CEO", "Zuck"], "brand": ["Meta", "Instagram", "WhatsApp", "Threads"]},
    "NVDA": {"execs": ["Jensen Huang", "NVIDIA CEO"], "brand": ["NVIDIA", "GeForce", "CUDA"]},
    "AMD": {"execs": ["Lisa Su", "AMD CEO"], "brand": ["AMD", "Ryzen", "EPYC"]},
    "NFLX": {"execs": ["Ted Sarandos", "Netflix CEO"], "brand": ["Netflix"]},
    "DIS": {"execs": ["Bob Iger", "Disney CEO"], "brand": ["Disney", "Disney+", "Marvel"]},
    "MCD": {"execs": ["Chris Kempczinski", "McDonald's CEO"], "brand": ["McDonald's", "McDonalds", "Big Mac"]},
    "NKE": {"execs": ["Elliott Hill", "Nike CEO"], "brand": ["Nike", "Air Jordan", "Just Do It"]},
    "SBUX": {"execs": ["Brian Niccol", "Starbucks CEO"], "brand": ["Starbucks"]},
    "WMT": {"execs": ["Doug McMillon", "Walmart CEO"], "brand": ["Walmart"]},
    "COST": {"execs": ["Ron Vachris", "Costco CEO"], "brand": ["Costco", "Kirkland"]},
    "BA": {"execs": ["Kelly Ortberg", "Boeing CEO"], "brand": ["Boeing", "737 MAX"]},
    "LUNR": {"execs": ["Steve Altemus", "Intuitive Machines CEO"], "brand": ["Intuitive Machines", "Odysseus", "moon lander"]},
    "RKLB": {"execs": ["Peter Beck", "Rocket Lab CEO"], "brand": ["Rocket Lab", "Electron", "Neutron"]},
    "PLTR": {"execs": ["Alex Karp", "Palantir CEO"], "brand": ["Palantir", "Gotham", "Foundry"]},
    "COIN": {"execs": ["Brian Armstrong", "Coinbase CEO"], "brand": ["Coinbase"]},
    "GME": {"execs": ["Ryan Cohen", "GameStop CEO"], "brand": ["GameStop"]},
    "RIVN": {"execs": ["RJ Scaringe", "Rivian CEO"], "brand": ["Rivian", "R1T", "R1S"]},
}


class ViralCatalystCollector(BaseCollector):
    """
    Detects viral moments involving company executives or brand events.
    Combines Google Trends spikes, social buzz, and news to identify
    when a stock is getting unusual attention from viral content.
    """

    @property
    def name(self) -> str:
        return "viral_catalyst"

    def collect(self, ticker: str) -> SignalResult:
        cached = self._get_cached(ticker, "viral")
        if cached:
            return SignalResult.from_dict(cached)

        ticker_upper = ticker.upper()

        # Get executive/brand info
        info = EXECUTIVE_MAP.get(ticker_upper)
        if not info:
            # Try to build a basic entry from ticker resolver
            company = resolve_name(ticker)
            info = {
                "execs": [f"{company} CEO"],
                "brand": [company],
            }

        # Check multiple channels for unusual activity
        exec_buzz = self._check_executive_buzz(info["execs"])
        brand_buzz = self._check_brand_buzz(info["brand"], ticker_upper)
        trend_spike = self._check_trend_spike(info["execs"] + info["brand"][:2])

        # Combine signals
        total_buzz = exec_buzz["score"] + brand_buzz["score"] + trend_spike["score"]
        is_viral = total_buzz > 0.3  # Threshold for "something is happening"

        if not is_viral:
            result = SignalResult(
                collector_name=self.name,
                ticker=ticker,
                signal_value=0.0,
                confidence=0.15,
                raw_data={
                    "exec_buzz": exec_buzz,
                    "brand_buzz": brand_buzz,
                    "trend_spike": trend_spike,
                    "is_viral": False,
                },
                details=f"No viral activity detected",
            )
            self._set_cache(result.to_dict(), ticker, "viral")
            return result

        # Determine sentiment of the viral moment
        sentiment = (
            exec_buzz.get("sentiment", 0) * 0.4 +
            brand_buzz.get("sentiment", 0) * 0.4 +
            trend_spike.get("sentiment", 0) * 0.2
        )

        # Viral attention is usually net positive short-term
        # (even "bad" viral can boost stock if it increases brand awareness)
        signal = sentiment * 0.6 + 0.1  # Slight positive bias for attention

        confidence = min(0.7, total_buzz)

        details_parts = []
        if exec_buzz["score"] > 0.1:
            details_parts.append(f"Exec buzz: {exec_buzz['top_exec']}")
        if brand_buzz["score"] > 0.1:
            details_parts.append(f"Brand viral: {brand_buzz.get('top_term', '?')}")
        if trend_spike["score"] > 0.1:
            details_parts.append(f"Trending: {trend_spike.get('spike_term', '?')}")

        result = SignalResult(
            collector_name=self.name,
            ticker=ticker,
            signal_value=max(-1.0, min(1.0, signal)),
            confidence=confidence,
            raw_data={
                "exec_buzz": exec_buzz,
                "brand_buzz": brand_buzz,
                "trend_spike": trend_spike,
                "is_viral": True,
                "total_buzz": round(total_buzz, 3),
                "sentiment": round(sentiment, 3),
            },
            details=" | ".join(details_parts) if details_parts else f"Viral activity detected (buzz={total_buzz:.2f})",
        )

        self._set_cache(result.to_dict(), ticker, "viral")
        return result

    def _check_executive_buzz(self, execs: List[str]) -> Dict:
        """
        Check if executives are being discussed unusually on
        HackerNews and Google News (proxy for social media virality).
        """
        total_mentions = 0
        sentiment_sum = 0
        top_exec = ""
        best_score = 0

        for exec_name in execs[:3]:  # Check top 3 exec names
            # Check HackerNews for executive mentions
            hn_url = "https://hn.algolia.com/api/v1/search_by_date"
            params = {
                "query": exec_name,
                "tags": "story",
                "numericFilters": f"created_at_i>{int((datetime.now() - timedelta(days=3)).timestamp())}",
                "hitsPerPage": 10,
            }
            resp = self._request(hn_url, params=params)
            if resp and resp.status_code == 200:
                try:
                    data = resp.json()
                    hits = data.get("hits", [])
                    mentions = len(hits)
                    total_mentions += mentions

                    if mentions > best_score:
                        best_score = mentions
                        top_exec = exec_name

                    # Analyze sentiment from titles
                    for hit in hits:
                        title = hit.get("title", "").lower()
                        points = hit.get("points", 0)
                        # Positive signals
                        if any(w in title for w in ["launch", "announce", "record", "growth",
                                                      "impressive", "amazing", "innovation", "win"]):
                            sentiment_sum += 0.3 * (1 + points / 100)
                        # Negative signals
                        elif any(w in title for w in ["scandal", "fired", "resign", "lawsuit",
                                                        "crash", "fail", "controversy", "accused",
                                                        "fraud", "layoff", "cut"]):
                            sentiment_sum -= 0.4 * (1 + points / 100)
                        else:
                            sentiment_sum += 0.05  # Neutral attention is slightly positive

                except Exception:
                    pass

        # Score: 0 = no buzz, 0.5 = moderate, 1.0 = very high
        score = min(1.0, total_mentions / 8)  # 8+ mentions in 3 days = max buzz
        sentiment = max(-1.0, min(1.0, sentiment_sum / max(total_mentions, 1)))

        return {
            "score": round(score, 3),
            "mentions": total_mentions,
            "sentiment": round(sentiment, 3),
            "top_exec": top_exec,
        }

    def _check_brand_buzz(self, brand_terms: List[str], ticker: str) -> Dict:
        """
        Check if brand terms are spiking on Google News.
        A sudden increase in "McDonald's" articles about something
        OTHER than earnings = viral moment.
        """
        total_articles = 0
        viral_articles = 0
        sentiment_sum = 0
        top_term = ""

        for term in brand_terms[:2]:
            # Use Google News RSS (no API key needed)
            url = f"https://news.google.com/rss/search?q={term}+viral+OR+trending+OR+video+OR+social+media&hl=en-US&gl=US&ceid=US:en"
            resp = self._request(url)
            if resp and resp.status_code == 200:
                try:
                    content = resp.text
                    # Count items in RSS
                    items = content.count("<item>")
                    total_articles += items

                    if items > 0 and not top_term:
                        top_term = term

                    # Check for viral indicators in titles
                    viral_keywords = ["viral", "trending", "video", "tiktok", "instagram",
                                     "social media", "clip", "meme", "goes viral", "internet"]
                    for kw in viral_keywords:
                        if kw in content.lower():
                            viral_articles += 1

                    # Sentiment from content
                    positive_words = ["love", "amazing", "great", "best", "incredible", "wow"]
                    negative_words = ["boycott", "scandal", "outrage", "backlash", "controversy", "worst"]
                    for pw in positive_words:
                        sentiment_sum += content.lower().count(pw) * 0.1
                    for nw in negative_words:
                        sentiment_sum -= content.lower().count(nw) * 0.15

                except Exception:
                    pass

        score = min(1.0, viral_articles / 5)  # 5+ viral articles = max
        sentiment = max(-1.0, min(1.0, sentiment_sum / max(total_articles, 1)))

        return {
            "score": round(score, 3),
            "total_articles": total_articles,
            "viral_articles": viral_articles,
            "sentiment": round(sentiment, 3),
            "top_term": top_term,
        }

    def _check_trend_spike(self, terms: List[str]) -> Dict:
        """
        Check Google Trends autocomplete for sudden interest spikes.
        If "McDonald's CEO" suddenly appears in autocomplete suggestions,
        something viral is happening.
        """
        spike_detected = False
        spike_term = ""
        score = 0.0
        sentiment = 0.0

        for term in terms[:3]:
            url = "https://suggestqueries.google.com/complete/search"
            params = {
                "client": "firefox",
                "q": term,
            }
            resp = self._request(url, params=params)
            if resp and resp.status_code == 200:
                try:
                    data = resp.json()
                    suggestions = data[1] if len(data) > 1 else []

                    # Check if suggestions indicate viral activity
                    viral_indicators = ["video", "viral", "tiktok", "meme",
                                        "clip", "interview", "tweet", "response",
                                        "reaction", "controversy", "apology"]

                    for sug in suggestions:
                        sug_lower = sug.lower()
                        for vi in viral_indicators:
                            if vi in sug_lower:
                                spike_detected = True
                                spike_term = sug
                                score = max(score, 0.3)

                                # Sentiment from suggestion
                                if any(p in sug_lower for p in ["love", "amazing", "best", "great"]):
                                    sentiment += 0.3
                                elif any(n in sug_lower for n in ["scandal", "fired", "boycott", "controversy"]):
                                    sentiment -= 0.4
                                else:
                                    sentiment += 0.05  # Neutral viral
                                break

                except Exception:
                    pass

        return {
            "score": round(min(1.0, score), 3),
            "spike_detected": spike_detected,
            "spike_term": spike_term,
            "sentiment": round(max(-1.0, min(1.0, sentiment)), 3),
        }
