"""
Alternative Data Collectors
============================
App Store rankings, weather correlations, seasonality,
shipping data, domain registration, earnings call NLP,
employee sentiment, and news analysis.
"""
import re
import math
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from stock_oracle.collectors.base import BaseCollector, SignalResult

logger = logging.getLogger("stock_oracle")

# Shared Ollama NLP instance (lazy-loaded)
_shared_ollama = None

def _get_shared_ollama():
    """Get or create shared Ollama NLP instance using config settings."""
    global _shared_ollama
    if _shared_ollama is None:
        from stock_oracle.ollama_nlp import OllamaNLP
        import stock_oracle.config as cfg
        _shared_ollama = OllamaNLP(
            base_url=cfg.OLLAMA_BASE_URL,
            model=cfg.OLLAMA_MODEL,
            fallback_model=cfg.OLLAMA_FALLBACK_MODEL,
            timeout=cfg.OLLAMA_TIMEOUT,
        )
    return _shared_ollama


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# APP STORE RANKINGS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Map tickers to their app names
TICKER_TO_APP = {
    "META": ["Facebook", "Instagram", "WhatsApp", "Threads"],
    "SNAP": ["Snapchat"],
    "PINS": ["Pinterest"],
    "UBER": ["Uber", "Uber Eats"],
    "LYFT": ["Lyft"],
    "DIS": ["Disney+", "Hulu", "ESPN"],
    "NFLX": ["Netflix"],
    "SPOT": ["Spotify"],
    "SQ": ["Cash App", "Square"],
    "PYPL": ["PayPal", "Venmo"],
    "COIN": ["Coinbase"],
    "RBLX": ["Roblox"],
    "ABNB": ["Airbnb"],
    "DASH": ["DoorDash"],
    "AAPL": ["Apple Music", "Apple TV", "Shazam"],
    "GOOGL": ["Google", "YouTube", "Google Maps", "Gmail"],
    "MSFT": ["Microsoft Teams", "Outlook", "OneDrive"],
    "AMZN": ["Amazon", "Kindle", "Audible", "Prime Video"],
    "TSLA": ["Tesla"],
    "WMT": ["Walmart"],
    "TGT": ["Target"],
    "COST": ["Costco"],
    "SBUX": ["Starbucks"],
    "MCD": ["McDonald's"],
    "HD": ["Home Depot"],
    "SHOP": ["Shopify", "Shop"],
    "CRM": ["Salesforce"],
    "ZM": ["Zoom"],
    "DUOL": ["Duolingo"],
    "HOOD": ["Robinhood"],
    "TOST": ["Toast"],
    "GRAB": ["Grab"],
    "SE": ["Shopee"],
    "MELI": ["Mercado Libre"],
}


class AppStoreCollector(BaseCollector):
    """
    Track app store rank velocity.
    Rapid climb in a new country = international expansion.
    Rapid climb overall = user growth.
    """

    @property
    def name(self) -> str:
        return "app_store_rank"

    def collect(self, ticker: str) -> SignalResult:
        cached = self._get_cached(ticker, "appstore")
        if cached:
            return SignalResult.from_dict(cached)

        apps = TICKER_TO_APP.get(ticker.upper(), [])
        if not apps:
            return self._neutral_signal(ticker, "No known apps for this ticker")

        # Check Apple RSS feed for top apps (free, no API key needed)
        rank_data = self._check_top_charts(apps)

        if not rank_data["found"]:
            return self._neutral_signal(ticker, "App not found in top charts")

        # Higher rank (lower number) = bullish
        signal = 0.0
        if rank_data["best_rank"] <= 10:
            signal = 0.5
        elif rank_data["best_rank"] <= 50:
            signal = 0.3
        elif rank_data["best_rank"] <= 100:
            signal = 0.1

        # Multiple apps in top charts = extra bullish
        if rank_data["apps_in_charts"] > 1:
            signal += 0.2

        result = SignalResult(
            collector_name=self.name,
            ticker=ticker,
            signal_value=max(-1.0, min(1.0, signal)),
            confidence=0.5 if rank_data["found"] else 0.0,
            raw_data=rank_data,
            details=f"Best rank: #{rank_data['best_rank']} | {rank_data['apps_in_charts']} apps in charts",
        )

        self._set_cache(result.to_dict(), ticker, "appstore")
        return result

    def _check_top_charts(self, app_names: List[str]) -> Dict:
        """Check Apple's iTunes RSS top charts feed."""
        # Primary: iTunes RSS (reliable)
        url = "https://itunes.apple.com/us/rss/topfreeapplications/limit=200/json"
        resp = self._request(url)

        if not resp or resp.status_code != 200:
            # Fallback: Apple Marketing Tools
            url2 = "https://rss.applemarketingtools.com/api/v2/us/apps/top-free/200/apps.json"
            resp = self._request(url2)

        if not resp or resp.status_code != 200:
            return {"found": False, "best_rank": 999, "apps_in_charts": 0}

        try:
            data = resp.json()

            # iTunes format: feed.entry[].im:name.label
            # Apple Marketing format: feed.results[].name
            entries = data.get("feed", {}).get("entry", [])
            if not entries:
                entries = data.get("feed", {}).get("results", [])

            found_apps = []
            for i, entry in enumerate(entries):
                # Handle both JSON formats
                entry_name = ""
                if isinstance(entry.get("im:name"), dict):
                    entry_name = entry["im:name"].get("label", "").lower()
                elif isinstance(entry.get("name"), str):
                    entry_name = entry["name"].lower()
                elif isinstance(entry.get("title"), dict):
                    entry_name = entry["title"].get("label", "").lower()

                for app in app_names:
                    if app.lower() in entry_name:
                        found_apps.append({
                            "app": app,
                            "rank": i + 1,
                            "name": entry_name,
                        })

            if found_apps:
                return {
                    "found": True,
                    "best_rank": min(a["rank"] for a in found_apps),
                    "apps_in_charts": len(found_apps),
                    "details": found_apps,
                }
        except Exception as e:
            logger.error(f"App store error: {e}")

        return {"found": False, "best_rank": 999, "apps_in_charts": 0}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SEASONALITY & CALENDAR EFFECTS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Known seasonal patterns (month -> sector -> historical bias)
SEASONAL_PATTERNS = {
    1: {"retail": -0.2, "tech": 0.1, "healthcare": 0.1},  # January effect
    2: {"retail": -0.1, "tech": 0.0, "finance": 0.1},
    3: {"all": 0.1},  # End of Q1 window dressing
    4: {"all": 0.15, "tech": 0.2},  # Tax refund spending
    5: {"all": -0.1},  # "Sell in May"
    6: {"retail": -0.1, "energy": 0.2},  # Summer driving
    7: {"tech": 0.1, "retail": -0.1},
    8: {"all": -0.1},  # Summer doldrums
    9: {"all": -0.15},  # September effect (historically worst month)
    10: {"all": -0.05, "tech": 0.1},  # October dips but tech rallies
    11: {"retail": 0.3, "all": 0.1},  # Holiday shopping, Black Friday
    12: {"retail": 0.2, "all": 0.15},  # Santa rally, tax-loss harvesting
}

TICKER_SECTORS = {
    "AAPL": "tech", "MSFT": "tech", "GOOGL": "tech", "META": "tech",
    "AMZN": "retail", "TSLA": "tech", "NVDA": "tech", "AMD": "tech",
    "NFLX": "tech", "DIS": "retail", "WMT": "retail", "TGT": "retail",
    "JPM": "finance", "BAC": "finance", "GS": "finance",
    "JNJ": "healthcare", "PFE": "healthcare", "UNH": "healthcare",
    "XOM": "energy", "CVX": "energy", "COP": "energy",
}


class SeasonalityCollector(BaseCollector):
    """
    Calendar-based trading signals:
    - Monthly seasonality patterns
    - Day-of-week effects
    - Earnings season timing
    - Holiday effects
    """

    @property
    def name(self) -> str:
        return "seasonality"

    def collect(self, ticker: str) -> SignalResult:
        now = datetime.now()
        month = now.month
        day_of_week = now.weekday()

        sector = TICKER_SECTORS.get(ticker.upper(), "all")
        monthly = SEASONAL_PATTERNS.get(month, {})

        # Get sector-specific and general signal
        sector_signal = monthly.get(sector, 0.0)
        general_signal = monthly.get("all", 0.0)
        signal = sector_signal if sector_signal else general_signal

        # Day-of-week adjustment
        # Monday dip, Friday rally are well-documented
        dow_adj = {0: -0.05, 1: 0.0, 2: 0.02, 3: 0.02, 4: 0.03}
        signal += dow_adj.get(day_of_week, 0)

        # Check if near earnings season (bullish anticipation)
        if month in (1, 4, 7, 10) and now.day >= 10:
            signal += 0.1  # Earnings anticipation rally

        details = (
            f"Month={month} sector={sector} | "
            f"seasonal={sector_signal or general_signal:+.2f} | "
            f"DOW adj={dow_adj.get(day_of_week, 0):+.2f}"
        )

        return SignalResult(
            collector_name=self.name,
            ticker=ticker,
            signal_value=max(-1.0, min(1.0, signal)),
            confidence=0.4,  # Seasonality is weak but consistent
            raw_data={
                "month": month,
                "day_of_week": day_of_week,
                "sector": sector,
                "sector_signal": sector_signal,
                "general_signal": general_signal,
            },
            details=details,
        )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# WEATHER CORRELATION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

WEATHER_SECTORS = {
    "energy": {"hot": 0.2, "cold": 0.3, "mild": -0.1},
    "retail": {"hot": -0.05, "cold": -0.1, "rain": -0.15},
    "agriculture": {"drought": -0.4, "flood": -0.3, "normal": 0.1},
    "travel": {"storms": -0.2, "clear": 0.1},
}


class WeatherCorrelationCollector(BaseCollector):
    """
    Second-order weather effects on stocks.
    Uses Open-Meteo for weather data (free, no API key).
    """

    @property
    def name(self) -> str:
        return "weather_correlation"

    def collect(self, ticker: str) -> SignalResult:
        cached = self._get_cached(ticker, "weather")
        if cached:
            return SignalResult.from_dict(cached)

        sector = TICKER_SECTORS.get(ticker.upper(), "all")

        # Fetch US weather conditions
        weather = self._get_us_weather()
        if not weather:
            return self._neutral_signal(ticker, "Weather data unavailable")

        # Classify conditions
        condition = self._classify_weather(weather)
        sector_effects = WEATHER_SECTORS.get(sector, {})
        signal = sector_effects.get(condition, 0.0)

        result = SignalResult(
            collector_name=self.name,
            ticker=ticker,
            signal_value=max(-1.0, min(1.0, signal)),
            confidence=0.25,  # Weather correlation is weak
            raw_data=weather,
            details=f"Condition={condition} | sector={sector} | effect={signal:+.2f}",
        )

        self._set_cache(result.to_dict(), ticker, "weather")
        return result

    def _get_us_weather(self) -> Optional[Dict]:
        """Get current US weather from Open-Meteo."""
        # Use major US city as proxy (New York)
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": 40.71,
            "longitude": -74.01,
            "current": "temperature_2m,precipitation,weather_code",
            "temperature_unit": "fahrenheit",
        }

        resp = self._request(url, params=params)
        if resp and resp.status_code == 200:
            try:
                data = resp.json()
                current = data.get("current", {})
                return {
                    "temperature": current.get("temperature_2m"),
                    "precipitation": current.get("precipitation"),
                    "weather_code": current.get("weather_code"),
                }
            except Exception:
                pass
        return None

    def _classify_weather(self, weather: Dict) -> str:
        temp = weather.get("temperature")
        precip = weather.get("precipitation", 0)
        code = weather.get("weather_code", 0)

        if temp and temp > 95:
            return "hot"
        elif temp and temp < 20:
            return "cold"
        elif precip and precip > 5:
            return "rain"
        elif code and code >= 95:
            return "storms"
        else:
            return "mild"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# NEWS SENTIMENT ANALYSIS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class NewsSentimentCollector(BaseCollector):
    """
    Analyzes recent news headlines for sentiment.
    Uses free news APIs and applies NLP.
    """

    @property
    def name(self) -> str:
        return "news_sentiment"

    def collect(self, ticker: str) -> SignalResult:
        cached = self._get_cached(ticker, "news")
        if cached:
            return SignalResult.from_dict(cached)

        from stock_oracle.collectors.job_postings import get_company_name
        company = get_company_name(ticker)

        articles = self._fetch_news(company, ticker)

        if not articles:
            return self._neutral_signal(ticker, "No news articles found")

        # Sentiment analysis on headlines
        sentiments = []
        for article in articles:
            headline = article.get("title", "")
            score = self._simple_sentiment(headline)
            sentiments.append(score)

        avg_sentiment = sum(sentiments) / len(sentiments) if sentiments else 0

        # Dampen based on article count — few headlines shouldn't produce strong signals
        # 15+ articles: full signal. 5 articles: 50% dampening. 1 article: 85% dampening.
        article_dampen = min(1.0, len(articles) / 15)
        signal = max(-0.4, min(0.4, avg_sentiment * article_dampen))  # Cap at ±0.4

        result = SignalResult(
            collector_name=self.name,
            ticker=ticker,
            signal_value=signal,
            confidence=min(0.55, len(articles) / 15),  # Headlines are noisy — cap at 55%
            raw_data={
                "article_count": len(articles),
                "avg_sentiment": avg_sentiment,
                "headlines": [a.get("title", "")[:100] for a in articles[:5]],
            },
            details=f"{len(articles)} articles | avg sentiment={avg_sentiment:+.2f}",
        )

        self._set_cache(result.to_dict(), ticker, "news")
        return result

    def _fetch_news(self, company: str, ticker: str) -> List[Dict]:
        """Fetch news from Google News RSS or similar."""
        # Use Google News RSS (free, no API key)
        import xml.etree.ElementTree as ET

        url = f"https://news.google.com/rss/search"
        params = {"q": f"{company} stock {ticker}", "hl": "en-US", "gl": "US", "ceid": "US:en"}

        resp = self._request(url, params=params)
        if resp and resp.status_code == 200:
            try:
                root = ET.fromstring(resp.content)
                articles = []
                for item in root.findall(".//item")[:20]:
                    articles.append({
                        "title": item.findtext("title", ""),
                        "link": item.findtext("link", ""),
                        "pubDate": item.findtext("pubDate", ""),
                        "source": item.findtext("source", ""),
                    })
                return articles
            except Exception as e:
                logger.error(f"News RSS parse error: {e}")

        return []

    def _simple_sentiment(self, text: str) -> float:
        """Simple keyword-based sentiment scoring."""
        text_lower = text.lower()

        positive = [
            "surge", "soar", "rally", "gain", "profit", "beat", "record",
            "growth", "upgrade", "bullish", "strong", "outperform", "exceed",
            "breakthrough", "innovation", "partnership", "expansion", "revenue up",
        ]
        negative = [
            "crash", "plunge", "drop", "loss", "miss", "decline", "cut",
            "layoff", "lawsuit", "investigation", "recall", "downgrade",
            "bearish", "weak", "underperform", "warning", "default", "debt",
        ]

        pos_count = sum(1 for w in positive if w in text_lower)
        neg_count = sum(1 for w in negative if w in text_lower)

        if pos_count + neg_count == 0:
            return 0.0

        return (pos_count - neg_count) / (pos_count + neg_count)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SHIPPING / VESSEL TRACKING
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class ShippingActivityCollector(BaseCollector):
    """
    Track container shipping patterns as economic indicators.
    Uses public AIS data and port statistics.
    """

    @property
    def name(self) -> str:
        return "shipping_activity"

    def collect(self, ticker: str) -> SignalResult:
        # Shipping data primarily affects logistics, retail, and manufacturing
        sector = TICKER_SECTORS.get(ticker.upper(), "all")
        relevant_sectors = {"retail", "tech", "energy"}

        if sector not in relevant_sectors and sector != "all":
            return self._neutral_signal(ticker, "Shipping data not relevant for this sector")

        # Fetch port congestion data
        port_data = self._get_port_indicators()

        if not port_data:
            return self._neutral_signal(ticker, "No shipping data available")

        # High port activity = strong demand = bullish for retail/tech
        signal = port_data.get("activity_signal", 0.0)
        if sector == "energy":
            signal *= 0.5  # Less direct correlation

        return SignalResult(
            collector_name=self.name,
            ticker=ticker,
            signal_value=max(-1.0, min(1.0, signal)),
            confidence=0.3,
            raw_data=port_data,
            details=f"Port activity={port_data.get('level', 'unknown')}",
        )

    def _get_port_indicators(self) -> Optional[Dict]:
        """
        Use Baltic Dry Index as a proxy for global shipping activity.
        In production, you'd use MarineTraffic API for vessel positions.
        """
        # Try to get BDI from a public source
        # This is a placeholder — in production use a real shipping API
        return {
            "level": "moderate",
            "activity_signal": 0.1,
            "note": "Connect MarineTraffic API for real vessel tracking",
        }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DOMAIN REGISTRATION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class DomainRegistrationCollector(BaseCollector):
    """
    Track new domain registrations by companies.
    New product domains registered = upcoming launch.
    """

    @property
    def name(self) -> str:
        return "domain_registration"

    def collect(self, ticker: str) -> SignalResult:
        from stock_oracle.collectors.job_postings import get_company_name
        company = get_company_name(ticker)

        # Check certificate transparency logs for new domains
        domains = self._check_cert_transparency(company)

        if not domains:
            return self._neutral_signal(ticker, "No new domains detected")

        signal = min(0.3, len(domains) * 0.05)

        return SignalResult(
            collector_name=self.name,
            ticker=ticker,
            signal_value=signal,
            confidence=min(0.5, len(domains) * 0.1),
            raw_data={"new_domains": domains},
            details=f"{len(domains)} new domains detected",
        )

    def _check_cert_transparency(self, company: str) -> List[str]:
        """
        Check Certificate Transparency logs for new SSL certificates.
        Uses crt.sh (free, public CT log search).
        """
        company_lower = company.lower().replace(" ", "")
        url = f"https://crt.sh/"
        params = {
            "q": f"%.{company_lower}.com",
            "output": "json",
        }

        resp = self._request(url, params=params)
        if resp and resp.status_code == 200:
            try:
                data = resp.json()
                recent_cutoff = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
                new_domains = set()
                for cert in data[:50]:
                    entry_date = cert.get("entry_timestamp", "")[:10]
                    name = cert.get("name_value", "")
                    if entry_date >= recent_cutoff and name:
                        new_domains.add(name)
                return list(new_domains)[:20]
            except Exception:
                pass
        return []


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# EARNINGS CALL NLP
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class EarningsCallNLPCollector(BaseCollector):
    """
    Analyze earnings call transcripts and 8-K filings for hedge words,
    sentiment shifts, and confidence indicators.

    Uses Ollama (local AI) for deep analysis when available.
    Falls back to keyword-based analysis when Ollama is offline.
    """

    _ollama = None  # Deprecated — use _get_shared_ollama()

    @property
    def name(self) -> str:
        return "earnings_nlp"

    # Confidence language patterns (used in fallback mode)
    CONFIDENT = {"will", "committed", "certain", "strong", "clearly", "definitely",
                 "robust", "exceeded", "record", "outstanding", "momentum", "confident"}
    HEDGING = {"may", "might", "could", "possibly", "potentially", "uncertain",
               "hope", "believe", "expect", "anticipate", "approximately"}
    NEGATIVE = {"challenging", "headwind", "pressure", "decline", "difficult",
                "concern", "risk", "slowdown", "softness", "weakness", "volatility",
                "restructuring", "impairment", "writedown", "layoff"}
    POSITIVE = {"growth", "exceeded", "beat", "raised", "upgrade", "expansion",
                "profitable", "accelerating", "outperformed", "innovation", "demand"}

    def _fetch_latest_8k(self, ticker: str) -> Optional[str]:
        """Fetch the latest 8-K filing text from SEC EDGAR."""
        import re as regex
        from stock_oracle.config import SEC_USER_AGENT
        sec_headers = {"User-Agent": SEC_USER_AGENT}

        # Step 1: Use EFTS full-text search (newer, more reliable)
        try:
            start = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
            resp = self._request(
                "https://efts.sec.gov/LATEST/search-index",
                params={"q": f'"{ticker}"', "forms": "8-K",
                        "dateRange": "custom", "startdt": start},
                headers=sec_headers)
            if resp and resp.status_code == 200:
                data = resp.json()
                hits = data.get("hits", {}).get("hits", [])
                if hits:
                    file_url = hits[0].get("_source", {}).get("file_url", "")
                    if file_url:
                        doc_resp = self._request(
                            f"https://www.sec.gov{file_url}", headers=sec_headers)
                        if doc_resp and doc_resp.status_code == 200:
                            text = regex.sub(r'<[^>]+>', ' ', doc_resp.text)
                            text = regex.sub(r'\s+', ' ', text).strip()
                            if len(text) > 500:
                                return text[:10000]
        except Exception:
            pass

        # Step 2: Fallback — EDGAR company search Atom feed
        try:
            resp = self._request(
                "https://www.sec.gov/cgi-bin/browse-edgar",
                params={"action": "getcompany", "company": ticker,
                        "CIK": ticker, "type": "8-K", "dateb": "",
                        "owner": "include", "count": "3",
                        "output": "atom"},
                headers=sec_headers)
            if resp and resp.status_code == 200:
                links = regex.findall(
                    r'<link[^>]*href="(https://www\.sec\.gov/Archives/edgar/data/[^"]+)"',
                    resp.text)
                for link in links[:3]:
                    idx_resp = self._request(link, headers=sec_headers)
                    if idx_resp and idx_resp.status_code == 200:
                        doc_links = regex.findall(
                            r'href="(/Archives/edgar/data/[^"]+\.htm)"', idx_resp.text)
                        if not doc_links:
                            doc_links = regex.findall(
                                r'href="(/Archives/edgar/data/[^"]+\.txt)"', idx_resp.text)
                        for dl in doc_links[:2]:
                            doc_resp = self._request(
                                f"https://www.sec.gov{dl}", headers=sec_headers)
                            if doc_resp and doc_resp.status_code == 200:
                                text = regex.sub(r'<[^>]+>', ' ', doc_resp.text)
                                text = regex.sub(r'\s+', ' ', text).strip()
                                if len(text) > 500:
                                    return text[:10000]
        except Exception as e:
            logger.debug(f"earnings_nlp: EDGAR fetch error: {e}")

        return None

    def _keyword_analysis(self, text: str) -> Dict:
        """Fallback: basic keyword-based sentiment when Ollama is offline."""
        words = text.lower().split()
        word_set = set(words)

        confident_count = len(word_set & self.CONFIDENT)
        hedge_count = len(word_set & self.HEDGING)
        negative_count = len(word_set & self.NEGATIVE)
        positive_count = len(word_set & self.POSITIVE)

        # Score
        sentiment = (positive_count - negative_count) / max(positive_count + negative_count, 1)
        confidence_ratio = confident_count / max(confident_count + hedge_count, 1)

        return {
            "method": "keyword_fallback",
            "positive_words": positive_count,
            "negative_words": negative_count,
            "confident_words": confident_count,
            "hedge_words": hedge_count,
            "sentiment": round(sentiment, 3),
            "confidence_ratio": round(confidence_ratio, 3),
        }

    def collect(self, ticker: str) -> SignalResult:
        cached = self._get_cached(ticker, "earnings_nlp")
        if cached:
            return SignalResult.from_dict(cached)

        # Fetch latest 8-K filing
        filing_text = self._fetch_latest_8k(ticker)

        if not filing_text:
            result = self._neutral_signal(
                ticker,
                "No recent 8-K filing found. Earnings NLP activates when filings are available."
            )
            self._set_cache(result.to_dict(), ticker, "earnings_nlp")
            return result

        # Try Ollama deep analysis first
        ollama = _get_shared_ollama()
        if ollama.available:
            analysis = ollama.analyze_filing(ticker, filing_text)
            signal = analysis.get("signal", 0)
            conf = analysis.get("confidence", 0.5)
            details_parts = [f"Ollama ({ollama.model})"]

            inner = analysis.get("analysis", {})
            if inner:
                if inner.get("red_flags"):
                    details_parts.append(f"Red flags: {len(inner['red_flags'])}")
                if inner.get("risk_level") is not None:
                    details_parts.append(f"Risk: {inner['risk_level']:.0%}")
                if inner.get("summary"):
                    details_parts.append(inner["summary"][:80])

            result = SignalResult(
                collector_name=self.name,
                ticker=ticker,
                signal_value=max(-1.0, min(1.0, signal)),
                confidence=min(0.8, conf),
                raw_data={"method": "ollama", "model": ollama.model, "analysis": inner},
                details=" | ".join(details_parts),
            )
        else:
            # Fallback: keyword analysis
            kw = self._keyword_analysis(filing_text)
            signal = kw["sentiment"] * 0.5
            conf = 0.3  # Low confidence for keyword-only

            result = SignalResult(
                collector_name=self.name,
                ticker=ticker,
                signal_value=max(-1.0, min(1.0, signal)),
                confidence=conf,
                raw_data=kw,
                details=f"Keyword analysis (Ollama offline) | +{kw['positive_words']} -{kw['negative_words']} | hedge={kw['hedge_words']}",
            )

        self._set_cache(result.to_dict(), ticker, "earnings_nlp")
        return result


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# EMPLOYEE SENTIMENT (Glassdoor-style)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class EmployeeSentimentCollector(BaseCollector):
    """
    Track employee sentiment via news about layoffs, workplace culture,
    hiring freezes, and employee satisfaction.

    Uses Ollama for deep analysis when available.
    Falls back to keyword-based scoring from news headlines.
    """

    @property
    def name(self) -> str:
        return "employee_sentiment"

    NEGATIVE_KW = {"layoff", "layoffs", "fired", "firing", "cut", "cuts", "restructuring",
                   "downsizing", "toxic", "hostile", "lawsuit", "discrimination", "harassment",
                   "walkout", "strike", "protest", "quit", "exodus", "attrition", "burnout",
                   "overworked", "underpaid", "morale"}
    POSITIVE_KW = {"hiring", "benefits", "bonus", "perks", "culture", "award", "best place",
                   "top employer", "raise", "promotion", "satisfaction", "engagement",
                   "innovation", "expansion", "growing", "remote work", "flexible"}

    def collect(self, ticker: str) -> SignalResult:
        cached = self._get_cached(ticker, "emp_sent")
        if cached:
            return SignalResult.from_dict(cached)

        from stock_oracle.utils.ticker_resolver import resolve_name
        company = resolve_name(ticker)

        # Fetch employee-related news — use NEUTRAL search terms
        # Avoid "layoffs OR hiring" which biases toward negative headlines
        search_terms = [
            f"{company} employees workplace",
            f"{company} hiring jobs careers",
        ]

        articles = []
        for term in search_terms:
            url = f"https://news.google.com/rss/search?q={term}&hl=en-US&gl=US&ceid=US:en"
            resp = self._request(url)
            if resp and resp.status_code == 200:
                import re as regex
                titles = regex.findall(r'<title><!\[CDATA\[(.*?)\]\]></title>', resp.text)
                if not titles:
                    titles = regex.findall(r'<title>(.*?)</title>', resp.text)
                for t in titles[1:8]:  # Skip feed title
                    if t.strip() and t.strip() != "Google News" and len(t) > 10:
                        articles.append(t)

        if not articles:
            result = self._neutral_signal(
                ticker,
                f"No employee news found for {company}"
            )
            self._set_cache(result.to_dict(), ticker, "emp_sent")
            return result

        # Try Ollama deep analysis
        ollama = _get_shared_ollama()
        if ollama.available:
            article_text = "\n".join([f"[{i+1}] {a}" for i, a in enumerate(articles[:12])])
            analysis = ollama._generate_json(
                f"Analyze these news headlines about {company}'s employees and workplace:\n\n{article_text}",
                system="""You are an HR/workplace analyst. Analyze these headlines about a company's
employees and return ONLY JSON:
{
  "employee_sentiment": float (-1.0 very negative to 1.0 very positive),
  "confidence": float (0.0 to 1.0),
  "key_issues": [list of main employee-related topics],
  "red_flags": [any concerning workplace signals],
  "positive_signs": [any positive workplace signals],
  "layoff_risk": float (0.0 to 1.0),
  "is_normal_operations": bool (true if headlines are just routine news, not actual problems),
  "summary": "1-2 sentence summary"
}
IMPORTANT: News inherently over-reports negative events. A company with routine layoff articles
is NORMAL for large companies. Only flag truly unusual or significant employee issues.
Score 0.0 (neutral) for normal business operations, not -0.5.
Return ONLY valid JSON."""
            )

            if analysis:
                sentiment = analysis.get("employee_sentiment") or 0
                conf = analysis.get("confidence") or 0.3
                layoff_risk = analysis.get("layoff_risk") or 0
                is_normal = analysis.get("is_normal_operations", False)

                # Ensure numeric types (Ollama sometimes returns strings)
                try:
                    sentiment = float(sentiment)
                    conf = float(conf)
                    layoff_risk = float(layoff_risk)
                except (TypeError, ValueError):
                    sentiment, conf, layoff_risk = 0.0, 0.3, 0.0

                # If Ollama thinks it's normal operations, dampen signal heavily
                if is_normal:
                    signal = sentiment * 0.1  # Almost neutral
                else:
                    # Only significant events get full signal weight
                    signal = sentiment * 0.3 - layoff_risk * 0.2

                details_parts = [f"Ollama ({ollama.model})"]
                if analysis.get("summary"):
                    details_parts.append(analysis["summary"][:80])
                if layoff_risk > 0.6:
                    details_parts.append(f"Layoff risk: {layoff_risk:.0%}")

                # Cap confidence LOW — this is news-proxy data, not actual employee surveys
                # News headlines are unreliable for employee sentiment
                result = SignalResult(
                    collector_name=self.name,
                    ticker=ticker,
                    signal_value=max(-1.0, min(1.0, signal)),
                    confidence=min(0.35, conf * 0.5),  # Hard cap at 35%, halve Ollama's confidence
                    raw_data={"method": "ollama", "model": ollama.model,
                              "articles": len(articles), "analysis": analysis},
                    details=" | ".join(details_parts),
                )
                self._set_cache(result.to_dict(), ticker, "emp_sent")
                return result

        # Fallback: keyword analysis of headlines
        all_text = " ".join(articles).lower()
        neg_hits = sum(1 for kw in self.NEGATIVE_KW if kw in all_text)
        pos_hits = sum(1 for kw in self.POSITIVE_KW if kw in all_text)
        total = neg_hits + pos_hits

        if total == 0:
            result = self._neutral_signal(
                ticker,
                f"{len(articles)} employee articles, no strong signals (Ollama offline for deeper analysis)"
            )
        else:
            sentiment = (pos_hits - neg_hits) / total
            signal = sentiment * 0.3
            result = SignalResult(
                collector_name=self.name,
                ticker=ticker,
                signal_value=max(-1.0, min(1.0, signal)),
                confidence=min(0.25, total / 15),  # Low confidence for keyword-only
                raw_data={"method": "keyword_fallback", "articles": len(articles),
                          "positive_kw": pos_hits, "negative_kw": neg_hits},
                details=f"Keyword analysis | {len(articles)} articles | +{pos_hits} -{neg_hits} (Ollama offline)",
            )

        self._set_cache(result.to_dict(), ticker, "emp_sent")
        return result
