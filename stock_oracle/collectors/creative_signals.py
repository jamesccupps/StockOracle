"""
Creative Alternative Data Collectors
=====================================
The weird, out-of-the-box signals that sound insane but have
actual causal logic behind them.

The thesis: markets are driven by human behavior, and human behavior
leaves traces EVERYWHERE. The trick is finding the traces that
lead the stock price by days or weeks.
"""
import re
import json
import math
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from stock_oracle.collectors.base import BaseCollector, SignalResult

logger = logging.getLogger("stock_oracle")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 1. WAFFLE HOUSE INDEX
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# FEMA literally uses Waffle House closures to gauge disaster severity.
# If Waffle Houses close, the regional economy is in serious trouble.
# Track Google searches for "Waffle House closed" as a proxy for
# regional economic disruption. Bearish for regional retail/insurance.

class WaffleHouseIndexCollector(BaseCollector):
    """
    The Waffle House Index: FEMA's real disaster severity gauge.
    Waffle House never closes unless things are truly catastrophic.
    Spike in "Waffle House closed" searches = regional economic disruption.
    Affects: insurance (TRV, ALL), regional retail, disaster recovery stocks.
    """

    @property
    def name(self) -> str:
        return "waffle_house_index"

    def collect(self, ticker: str) -> SignalResult:
        # Track via Google Trends proxy — search interest for disaster terms
        disaster_signal = self._check_disaster_indicators()

        if not disaster_signal["active"]:
            return self._neutral_signal(ticker, "No disaster indicators detected")

        # Disasters are bearish for most stocks, bullish for rebuilding
        DISASTER_BENEFICIARIES = {"HD", "LOW", "GE", "CAT", "DE", "SHW", "MLM"}
        DISASTER_LOSERS = {"TRV", "ALL", "PGR", "DIS", "MAR", "HLT"}

        signal = 0.0
        if ticker.upper() in DISASTER_BENEFICIARIES:
            signal = 0.3  # Rebuilding demand
        elif ticker.upper() in DISASTER_LOSERS:
            signal = -0.3  # Insurance claims, tourism drop

        return SignalResult(
            collector_name=self.name,
            ticker=ticker,
            signal_value=signal,
            confidence=disaster_signal.get("confidence", 0.3),
            raw_data=disaster_signal,
            details=f"Disaster level: {disaster_signal.get('level', 'none')}",
        )

    def _check_disaster_indicators(self) -> Dict:
        """Check NWS alerts and FEMA disaster declarations."""
        # Primary: NWS active alerts count (reliable, no auth needed)
        nws_url = "https://api.weather.gov/alerts/active/count"
        resp = self._request(nws_url)
        if resp and resp.status_code == 200:
            try:
                data = resp.json()
                total = data.get("total", 0)
                land = data.get("land", 0)
                regions = data.get("regions", {})
                if land > 200:
                    return {"active": True, "level": "high", "count": land,
                            "total_alerts": total, "confidence": 0.5,
                            "source": "nws_alerts", "top_regions": dict(list(regions.items())[:5])}
                elif land > 50:
                    return {"active": True, "level": "moderate", "count": land,
                            "total_alerts": total, "confidence": 0.3,
                            "source": "nws_alerts"}
                else:
                    return {"active": False, "level": "low", "count": land,
                            "total_alerts": total, "confidence": 0.25,
                            "source": "nws_alerts"}
            except Exception:
                pass

        # Fallback: FEMA OpenFEMA API
        fema_url = "https://www.fema.gov/api/open/v2/DisasterDeclarations"
        params = {
            "$top": "10",
            "$orderby": "declarationDate desc",
        }
        resp = self._request(fema_url, params=params)
        if resp and resp.status_code == 200:
            try:
                data = resp.json()
                declarations = data.get("DisasterDeclarations", [])
                if len(declarations) > 5:
                    return {"active": True, "level": "high", "count": len(declarations), "confidence": 0.5}
                elif len(declarations) > 2:
                    return {"active": True, "level": "moderate", "count": len(declarations), "confidence": 0.3}
            except Exception:
                pass

        return {"active": False, "level": "none", "count": 0, "confidence": 0}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 2. CEO FLIGHT TRACKER (Private jet movements)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Before M&A deals, executives fly to meet each other.
# ADS-B Exchange tracks private jets publicly.
# CEO flying to competitor HQ on a weekend = something is happening.
# This is 100% legal public data (ADS-B transponders are required by law).

class ExecutiveTravelCollector(BaseCollector):
    """
    Track corporate jet movements via ADS-B public data.
    Unusual executive travel patterns precede M&A announcements
    by days or weeks. Weekend flights to competitor cities are
    especially suspicious.

    Data source: ADS-B Exchange (adsbexchange.com) — public transponder data.
    In production: cross-reference tail numbers from FAA registry
    with corporate jet ownership filings.
    """

    # Known corporate HQ cities (simplified)
    TECH_HUBS = {"San Francisco", "San Jose", "Seattle", "Cupertino", "Mountain View",
                 "Redmond", "Austin", "New York"}
    FINANCE_HUBS = {"New York", "Charlotte", "Chicago", "Boston"}

    @property
    def name(self) -> str:
        return "executive_travel"

    def collect(self, ticker: str) -> SignalResult:
        # Check for unusual corporate jet activity
        # In production: query ADS-B Exchange API with known tail numbers
        # For now: use as a framework placeholder with the logic built out

        return self._neutral_signal(
            ticker,
            "Connect ADS-B Exchange API + FAA registry for jet tracking. "
            "Framework detects: weekend flights, competitor-city visits, "
            "unusual frequency spikes, multi-stop deal tours."
        )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 3. GITHUB COMMIT VELOCITY
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Public repos tell you what engineers are actually building.
# Spike in commits to a company's open-source repos = shipping soon.
# Drop in commits = team disruption or pivot.
# Also: competitor repos gaining stars = market share threat.

TICKER_TO_GITHUB = {
    "META": ["facebook", "facebookresearch", "facebookincubator"],
    "GOOGL": ["google", "googlecloudplatform", "tensorflow"],
    "MSFT": ["microsoft", "azure", "dotnet"],
    "AMZN": ["aws", "amzn", "amazon-science"],
    "AAPL": ["apple"],
    "TSLA": ["teslamotors"],
    "NVDA": ["nvidia", "NVIDIA"],
    "AMD": ["amd", "GPUOpen-LibrariesAndSDKs"],
    "NFLX": ["netflix"],
    "UBER": ["uber"],
    "COIN": ["coinbase"],
    "CRM": ["salesforce", "forcedotcom"],
    "SHOP": ["shopify"],
}


class GitHubVelocityCollector(BaseCollector):
    """
    Track open-source commit velocity as a product development signal.
    Companies ship code before they ship announcements.

    Signals:
    - Commit acceleration = product launch incoming (bullish)
    - Commit drop-off = team disruption (bearish)
    - New repos in hot areas (AI, blockchain) = strategic pivot
    - Star velocity on key repos = developer mindshare
    """

    @property
    def name(self) -> str:
        return "github_velocity"

    def collect(self, ticker: str) -> SignalResult:
        cached = self._get_cached(ticker, "github")
        if cached:
            return SignalResult.from_dict(cached)

        orgs = TICKER_TO_GITHUB.get(ticker.upper(), [])
        if not orgs:
            return self._neutral_signal(ticker, "No known GitHub orgs")

        total_commits_recent = 0
        total_stars = 0
        new_repos = 0
        hot_repos = []

        for org in orgs:
            data = self._fetch_org_activity(org)
            total_commits_recent += data.get("recent_commits", 0)
            total_stars += data.get("total_stars", 0)
            new_repos += data.get("new_repos", 0)
            hot_repos.extend(data.get("hot_repos", []))

        if total_commits_recent == 0 and total_stars == 0:
            return self._neutral_signal(ticker, "No GitHub activity data")

        # High recent commits = actively shipping = bullish
        signal = 0.0
        if total_commits_recent > 100:
            signal += 0.2
        if new_repos > 3:
            signal += 0.15  # Expanding into new areas
        if total_stars > 10000:
            signal += 0.1  # Strong developer mindshare

        result = SignalResult(
            collector_name=self.name,
            ticker=ticker,
            signal_value=max(-1.0, min(1.0, signal)),
            confidence=min(0.6, total_commits_recent / 200),
            raw_data={
                "recent_commits": total_commits_recent,
                "total_stars": total_stars,
                "new_repos": new_repos,
                "hot_repos": hot_repos[:5],
            },
            details=f"{total_commits_recent} commits | {total_stars} stars | {new_repos} new repos",
        )

        self._set_cache(result.to_dict(), ticker, "github")
        return result

    def _fetch_org_activity(self, org: str) -> Dict:
        """Fetch GitHub org activity. Uses token if configured to avoid rate limits."""
        import stock_oracle.config as cfg

        url = f"https://api.github.com/orgs/{org}/events"
        params = {"per_page": 100}
        headers = {"Accept": "application/vnd.github.v3+json"}
        if cfg.GITHUB_TOKEN:
            headers["Authorization"] = f"Bearer {cfg.GITHUB_TOKEN}"

        resp = self._request(url, params=params, headers=headers)
        if not resp or resp.status_code != 200:
            return {"recent_commits": 0, "total_stars": 0, "new_repos": 0}

        try:
            events = resp.json()
            push_events = [e for e in events if e.get("type") == "PushEvent"]
            create_events = [e for e in events if e.get("type") == "CreateEvent"
                           and e.get("payload", {}).get("ref_type") == "repository"]

            commit_count = sum(
                len(e.get("payload", {}).get("commits", []))
                for e in push_events
            )

            return {
                "recent_commits": commit_count,
                "total_stars": 0,  # Need separate call per repo
                "new_repos": len(create_events),
                "hot_repos": [e.get("repo", {}).get("name", "") for e in push_events[:5]],
            }
        except Exception:
            return {"recent_commits": 0, "total_stars": 0, "new_repos": 0}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 4. GOOGLE TRENDS VELOCITY (Search interest = demand signal)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# When people start googling a product MORE, sales follow.
# When people start googling "cancel [subscription]", churn follows.
# Track the ACCELERATION of search interest, not the raw volume.

class GoogleTrendsCollector(BaseCollector):
    """
    Google search interest as a leading demand indicator.

    Key patterns:
    - "[product] buy" increasing = demand growth
    - "[company] layoffs" spiking = trouble ahead
    - "[company] cancel" or "[company] alternative" = churn signal
    - "[product] vs [competitor]" = competitive pressure
    - "[company] stock" itself trending = retail attention incoming

    Uses SerpAPI/Google Trends via public endpoints.
    """

    BEARISH_MODIFIERS = ["cancel", "layoffs", "lawsuit", "class action",
                         "scam", "hack", "breach", "recall", "alternative to",
                         "crash", "drop", "sell", "overvalued", "short", "puts",
                         "decline", "bankruptcy", "fraud", "investigation"]
    BULLISH_MODIFIERS = ["buy", "review", "how to use", "vs", "worth it",
                         "preorder", "waitlist", "sold out",
                         "calls", "undervalued", "growth", "invest",
                         "dividend", "bullish", "beat", "upgrade", "price target"]

    @property
    def name(self) -> str:
        return "google_trends"

    def collect(self, ticker: str) -> SignalResult:
        cached = self._get_cached(ticker, "gtrends")
        if cached:
            return SignalResult.from_dict(cached)

        from stock_oracle.collectors.job_postings import get_company_name
        company = get_company_name(ticker)

        # Search for both company name and "[ticker] stock" for better finance coverage
        suggestions = self._get_autocomplete(f"{company}")
        suggestions += self._get_autocomplete(f"{ticker} stock")

        # Deduplicate
        seen = set()
        unique = []
        for s in suggestions:
            sl = s.lower()
            if sl not in seen:
                seen.add(sl)
                unique.append(s)
        suggestions = unique

        if not suggestions:
            return self._neutral_signal(ticker, "No Google Trends data")

        bull_count = 0
        bear_count = 0
        for suggestion in suggestions:
            s_lower = suggestion.lower()
            if any(mod in s_lower for mod in self.BEARISH_MODIFIERS):
                bear_count += 1
            if any(mod in s_lower for mod in self.BULLISH_MODIFIERS):
                bull_count += 1

        total = bull_count + bear_count
        if total == 0:
            return self._neutral_signal(ticker, "Neutral search patterns")

        signal = (bull_count - bear_count) / total * 0.4
        confidence = min(0.5, total / 10)

        result = SignalResult(
            collector_name=self.name,
            ticker=ticker,
            signal_value=max(-1.0, min(1.0, signal)),
            confidence=confidence,
            raw_data={
                "suggestions": suggestions[:10],
                "bullish_terms": bull_count,
                "bearish_terms": bear_count,
            },
            details=f"{bull_count} bullish / {bear_count} bearish search terms",
        )

        self._set_cache(result.to_dict(), ticker, "gtrends")
        return result

    def _get_autocomplete(self, query: str) -> List[str]:
        """Get Google autocomplete suggestions (free, no API key)."""
        url = "https://suggestqueries.google.com/complete/search"
        params = {
            "client": "firefox",
            "q": query,
        }
        resp = self._request(url, params=params)
        if resp and resp.status_code == 200:
            try:
                data = resp.json()
                return data[1] if len(data) > 1 else []
            except Exception:
                pass
        return []


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 5. THE PIZZA / CARDBOARD INDEX
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Cardboard box production is a leading indicator of economic activity.
# Everything ships in a box. More boxes = more stuff being made.
# The corrugated cardboard index is tracked by the Fibre Box Association.
# Pizza delivery volume correlates with late-night tech work (crunch time).
# UPS/FedEx package volume spikes precede retail earnings beats.

class CardboardIndexCollector(BaseCollector):
    """
    The Cardboard Box Index: a classic leading economic indicator.
    Industrial production of corrugated packaging leads GDP by ~2 quarters.

    Extended thesis:
    - Shipping box demand -> retail/ecommerce health
    - Pizza delivery searches near tech campuses -> product launch crunch
    - UPS/FedEx volume data -> holiday sales preview
    - Packaging material prices (kraft paper, resin) -> manufacturing costs

    Uses FRED data for industrial production as a proxy.
    """

    @property
    def name(self) -> str:
        return "cardboard_index"

    def collect(self, ticker: str) -> SignalResult:
        cached = self._get_cached(ticker, "cardboard")
        if cached:
            return SignalResult.from_dict(cached)

        # Use FRED industrial production data as proxy
        ip_data = self._get_industrial_production()

        if not ip_data:
            return self._neutral_signal(ticker, "No industrial production data")

        # Rising IP = economy expanding = generally bullish
        signal = ip_data.get("trend", 0)

        # Retail and logistics stocks are most directly affected
        LOGISTICS_TICKERS = {"UPS", "FDX", "AMZN", "WMT", "TGT", "COST", "HD", "LOW"}
        if ticker.upper() in LOGISTICS_TICKERS:
            signal *= 1.5  # Amplify for directly affected stocks

        result = SignalResult(
            collector_name=self.name,
            ticker=ticker,
            signal_value=max(-1.0, min(1.0, signal)),
            confidence=0.4,
            raw_data=ip_data,
            details=f"Industrial production trend: {ip_data.get('trend', 0):+.2f}",
        )

        self._set_cache(result.to_dict(), ticker, "cardboard")
        return result

    def _get_industrial_production(self) -> Optional[Dict]:
        """Get industrial production index from FRED."""
        from stock_oracle.config import FRED_API_KEY

        if FRED_API_KEY:
            url = "https://api.stlouisfed.org/fred/series/observations"
            params = {
                "series_id": "IPMAN",  # Manufacturing industrial production
                "api_key": FRED_API_KEY,
                "file_type": "json",
                "sort_order": "desc",
                "limit": 12,
            }
            resp = self._request(url, params=params)
            if resp and resp.status_code == 200:
                try:
                    data = resp.json()
                    obs = data.get("observations", [])
                    values = [float(o["value"]) for o in obs if o["value"] != "."]
                    if len(values) >= 3:
                        recent_avg = sum(values[:3]) / 3
                        older_avg = sum(values[3:6]) / 3 if len(values) >= 6 else recent_avg
                        trend = (recent_avg - older_avg) / older_avg if older_avg else 0
                        return {"trend": max(-0.5, min(0.5, trend * 5)), "latest": values[0]}
                except Exception:
                    pass

        # Fallback: check recent ISM PMI-like data via public sources
        return {"trend": 0.05, "latest": None, "note": "Set FRED_API_KEY for real data"}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 6. LINKEDIN EMPLOYEE FLOW TRACKER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# WHERE employees go when they leave tells you where the smart
# money (career capital) is flowing. If NVIDIA engineers are all
# going to a startup, that startup is probably building something big.
# If executives are fleeing, something is wrong internally.

class TalentFlowCollector(BaseCollector):
    """
    Track talent migration patterns between companies.

    Signals:
    - Net talent inflow from top companies = bullish
    - Executive departures clustering = bearish
    - Engineers moving from BigTech to target = innovation signal
    - Recruiting from specific competitor = competitive move

    Uses HN "Who's Hiring"/"Who Wants to Be Hired" + public data.
    """

    @property
    def name(self) -> str:
        return "talent_flow"

    def collect(self, ticker: str) -> SignalResult:
        from stock_oracle.collectors.job_postings import get_company_name
        company = get_company_name(ticker)

        # Search HN for people mentioning moving to/from this company
        flow_data = self._check_talent_signals(company)

        if not flow_data.get("mentions"):
            return self._neutral_signal(ticker, "No talent flow data")

        signal = flow_data.get("net_flow", 0) * 0.3
        # Confidence from directional matches, not just total mentions
        directional = flow_data.get("joining", 0) + flow_data.get("leaving", 0)
        confidence = min(0.35, directional / 10)  # Need 10+ directional matches for 35%
        return SignalResult(
            collector_name=self.name,
            ticker=ticker,
            signal_value=max(-1.0, min(1.0, signal)),
            confidence=confidence,
            raw_data=flow_data,
            details=f"{flow_data['mentions']} talent mentions | flow={flow_data['net_flow']:+.1f}",
        )

    def _check_talent_signals(self, company: str) -> Dict:
        """Check HN hiring threads for talent flow mentions."""
        url = "https://hn.algolia.com/api/v1/search"
        params = {
            "query": f"hiring {company}",
            "tags": "comment",
            "numericFilters": f"created_at_i>{int(datetime.now().timestamp()) - 60*86400}",
            "hitsPerPage": 20,
        }
        resp = self._request(url, params=params)
        if resp and resp.status_code == 200:
            try:
                data = resp.json()
                hits = data.get("hits", [])

                if not hits:
                    return {"mentions": 0, "net_flow": 0}

                # Analyze comment text for direction signals
                joining = 0
                leaving = 0
                neutral = 0
                for hit in hits:
                    text = (hit.get("comment_text") or hit.get("story_text") or "").lower()
                    if any(w in text for w in ["joining", "just joined", "starting at",
                                                "hired at", "offer from", "accepted"]):
                        joining += 1
                    elif any(w in text for w in ["leaving", "left ", "quit", "departed",
                                                  "laid off", "layoff", "firing"]):
                        leaving += 1
                    else:
                        neutral += 1

                total = joining + leaving + neutral
                if total == 0:
                    return {"mentions": len(hits), "net_flow": 0}

                # Net flow: positive = more joining than leaving
                directional_total = joining + leaving
                if directional_total >= 3:
                    # Enough directional evidence to produce a real signal
                    net_flow = (joining - leaving) / directional_total
                elif directional_total > 0:
                    # Few directional matches — dampen heavily
                    net_flow = (joining - leaving) / directional_total * 0.3
                else:
                    # No direction words found — treat as mild positive
                    # (companies being mentioned in hiring context is slightly bullish)
                    net_flow = min(0.15, len(hits) / 100)

                return {
                    "mentions": len(hits),
                    "joining": joining,
                    "leaving": leaving,
                    "neutral_mentions": neutral,
                    "net_flow": round(net_flow, 2),
                }
            except Exception:
                pass
        return {"mentions": 0, "net_flow": 0}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 7. SATELLITE PROXY — NIGHTTIME LIGHT INTENSITY
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Nighttime light intensity from satellite imagery correlates with
# economic activity. Factories running 24/7 = production ramp.
# Oil fields lighting up = extraction increasing.
# Cities getting brighter = economic growth.
# NASA publishes this data for free (VIIRS).

class NighttimeLightsCollector(BaseCollector):
    """
    Satellite nighttime light intensity as an economic activity proxy.
    Brighter industrial zones = more production.
    NASA VIIRS day-night band data is publicly available.

    In production: query NASA FIRMS/VIIRS API and cross-reference
    with known factory/warehouse/datacenter locations for specific companies.
    """

    @property
    def name(self) -> str:
        return "satellite_lights"

    def collect(self, ticker: str) -> SignalResult:
        # Check NASA FIRMS for fire/thermal anomalies near industrial sites
        # This is a real signal — data centers generate heat signatures
        fires = self._check_thermal_anomalies()

        return self._neutral_signal(
            ticker,
            f"Satellite proxy: {fires.get('active_fires', 0)} thermal anomalies. "
            "Connect NASA VIIRS/FIRMS API for production-zone light tracking."
        )

    def _check_thermal_anomalies(self) -> Dict:
        """Check NASA FIRMS for thermal anomalies (proxy for industrial activity)."""
        # NASA FIRMS provides near-real-time fire/thermal data
        # In production, you'd filter by lat/lon of known industrial sites
        url = "https://firms.modaps.eosdis.nasa.gov/api/area/csv/OPEN_API_KEY/VIIRS_SNPP_NRT/usa/1"
        # This needs a NASA API key (free: https://api.nasa.gov/)
        return {"active_fires": 0, "note": "Get free NASA API key at api.nasa.gov"}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 8. WIKIPEDIA PAGEVIEW VELOCITY
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Wikipedia pageview spikes precede stock moves by 1-3 days.
# When normies start reading the Wikipedia page for a company,
# retail buying follows. This is peer-reviewed research
# (Moat et al., "Quantifying Trading Behavior in Financial Markets
# Using Google Trends", Scientific Reports, 2013).

class WikipediaVelocityCollector(BaseCollector):
    """
    Wikipedia pageview acceleration as a retail attention indicator.
    Peer-reviewed: pageview spikes precede stock price moves.

    API: Wikimedia REST API (free, no key, generous rate limits).
    """

    @property
    def name(self) -> str:
        return "wikipedia_velocity"

    def collect(self, ticker: str) -> SignalResult:
        cached = self._get_cached(ticker, "wiki")
        if cached:
            return SignalResult.from_dict(cached)

        from stock_oracle.collectors.job_postings import get_company_name
        company = get_company_name(ticker)

        views = self._get_pageviews(company)
        if not views:
            return self._neutral_signal(ticker, "No Wikipedia data")

        # Calculate velocity (recent vs older period)
        if len(views) >= 14:
            recent = sum(views[:7])
            older = sum(views[7:14])
            velocity = (recent - older) / max(older, 1)
        else:
            velocity = 0

        # High velocity = incoming retail attention
        signal = max(-1.0, min(1.0, velocity * 0.5))

        result = SignalResult(
            collector_name=self.name,
            ticker=ticker,
            signal_value=signal,
            confidence=min(0.5, abs(velocity)),
            raw_data={
                "daily_views": views[:14],
                "velocity": round(velocity, 3),
                "total_recent": sum(views[:7]) if len(views) >= 7 else 0,
            },
            details=f"Wiki velocity: {velocity:+.1%} | {sum(views[:7]) if len(views) >= 7 else 0} views/wk",
        )

        self._set_cache(result.to_dict(), ticker, "wiki")
        return result

    def _get_pageviews(self, article_title: str) -> List[int]:
        """Get daily Wikipedia pageviews for the last 30 days."""
        # First, resolve the actual Wikipedia article title via search
        resolved = self._resolve_article_title(article_title)
        if not resolved:
            return []

        title = resolved.replace(" ", "_")
        end = datetime.now()
        start = end - timedelta(days=30)

        url = (
            f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
            f"en.wikipedia/all-access/all-agents/{title}/daily/"
            f"{start.strftime('%Y%m%d')}/{end.strftime('%Y%m%d')}"
        )
        resp = self._request(url)
        if resp and resp.status_code == 200:
            try:
                data = resp.json()
                items = data.get("items", [])
                return [item.get("views", 0) for item in reversed(items)]
            except Exception:
                pass
        return []

    def _resolve_article_title(self, company_name: str) -> Optional[str]:
        """
        Resolve a company name to the actual Wikipedia article title.
        'Apple' -> 'Apple Inc.'
        'Intuitive Machines' -> 'Intuitive Machines'
        """
        url = "https://en.wikipedia.org/w/api.php"
        params = {
            "action": "query",
            "list": "search",
            "srsearch": f"{company_name} company stock",
            "srlimit": 3,
            "format": "json",
        }
        resp = self._request(url, params=params)
        if resp and resp.status_code == 200:
            try:
                results = resp.json().get("query", {}).get("search", [])
                if results:
                    return results[0].get("title", company_name)
            except Exception:
                pass
        return company_name


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 9. ENERGY PRICE CASCADE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Energy prices cascade through EVERYTHING with a lag:
#   Nat gas up -> electricity up -> data center costs up -> cloud margins down
#   Oil up -> shipping up -> retail costs up -> consumer spending down
#   Electricity up -> EV charging costs up -> EV adoption slows
# Track energy futures to predict second-order effects 2-6 weeks out.

class EnergyCascadeCollector(BaseCollector):
    """
    Energy price cascading effects on downstream industries.
    Natural gas, oil, and electricity prices ripple through
    the entire economy with predictable lag times.
    """

    # Which sectors are hurt vs helped by energy price moves
    ENERGY_UP_BEARISH = {"AMZN", "GOOGL", "MSFT", "META", "NFLX",  # Cloud/datacenter costs
                         "WMT", "TGT", "COST",  # Shipping/logistics costs
                         "UAL", "DAL", "LUV",  # Jet fuel
                         "UBER", "LYFT"}  # Driver costs
    ENERGY_UP_BULLISH = {"XOM", "CVX", "COP", "SLB", "HAL",  # Oil producers
                         "LNG", "TELL",  # LNG
                         "FSLR", "ENPH", "SEDG"}  # Solar alternatives

    @property
    def name(self) -> str:
        return "energy_cascade"

    def collect(self, ticker: str) -> SignalResult:
        cached = self._get_cached(ticker, "energy")
        if cached:
            return SignalResult.from_dict(cached)

        energy_data = self._get_energy_prices()
        if not energy_data:
            return self._neutral_signal(ticker, "No energy price data")

        oil_trend = energy_data.get("oil_trend", 0)

        # Energy cascade is a secondary/indirect effect — keep signal modest
        signal = 0.0
        if ticker.upper() in self.ENERGY_UP_BEARISH:
            signal = -oil_trend * 0.2  # Moderate impact (was 0.4)
        elif ticker.upper() in self.ENERGY_UP_BULLISH:
            signal = oil_trend * 0.2

        # Confidence should be low — this is an indirect correlation, not a direct cause
        confidence = 0.25 if signal != 0 else 0.15

        result = SignalResult(
            collector_name=self.name,
            ticker=ticker,
            signal_value=max(-1.0, min(1.0, signal)),
            confidence=confidence,
            raw_data=energy_data,
            details=f"Oil trend: {oil_trend:+.2f} | cascade effect: {signal:+.2f}",
        )

        self._set_cache(result.to_dict(), ticker, "energy")
        return result

    def _get_energy_prices(self) -> Optional[Dict]:
        """Get energy price trends. Uses Yahoo Finance for crude oil."""
        try:
            import yfinance as yf
            oil = yf.Ticker("CL=F")
            hist = oil.history(period="3mo")
            if not hist.empty:
                closes = hist["Close"].tolist()
                ma20 = sum(closes[-20:]) / 20
                ma50 = sum(closes[-50:]) / min(50, len(closes))
                trend = (ma20 - ma50) / ma50 if ma50 else 0
                return {
                    "oil_price": closes[-1],
                    "oil_trend": max(-1, min(1, trend * 2)),  # Moderate amplification
                    "oil_ma20": ma20,
                }
        except Exception:
            pass
        return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 10. SOCIAL SENTIMENT BEYOND REDDIT
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# YouTube comment sentiment on earnings call videos
# TikTok stock mentions (retail frenzy indicator)
# Blind (anonymous employee app) sentiment
# Hacker News discussion quality (smart money signal)

class HackerNewsSentimentCollector(BaseCollector):
    """
    Hacker News sentiment — the "smart money" retail signal.
    HN commenters skew technical and informed. When HN turns
    bearish on a tech company, it often leads the market by weeks.

    Free API via Algolia: hn.algolia.com/api
    """

    @property
    def name(self) -> str:
        return "hackernews_sentiment"

    def collect(self, ticker: str) -> SignalResult:
        cached = self._get_cached(ticker, "hn")
        if cached:
            return SignalResult.from_dict(cached)

        from stock_oracle.collectors.job_postings import get_company_name
        company = get_company_name(ticker)

        comments = self._fetch_hn_discussion(company)
        if not comments:
            return self._neutral_signal(ticker, "No HN discussion found")

        # Analyze comment sentiment
        sentiments = [self._score_comment(c) for c in comments]
        avg = sum(sentiments) / len(sentiments) if sentiments else 0

        # HN skews negative, so calibrate
        adjusted = avg + 0.1  # Slight positive adjustment

        result = SignalResult(
            collector_name=self.name,
            ticker=ticker,
            signal_value=max(-1.0, min(1.0, adjusted)),
            confidence=min(0.5, len(comments) / 20),
            raw_data={
                "comment_count": len(comments),
                "avg_sentiment": round(avg, 3),
                "top_comments": [c[:100] for c in comments[:3]],
            },
            details=f"{len(comments)} HN comments | sentiment={avg:+.2f}",
        )

        self._set_cache(result.to_dict(), ticker, "hn")
        return result

    def _fetch_hn_discussion(self, company: str) -> List[str]:
        url = "https://hn.algolia.com/api/v1/search"
        params = {
            "query": company,
            "tags": "(story,comment)",
            "numericFilters": f"created_at_i>{int(datetime.now().timestamp()) - 30*86400}",
            "hitsPerPage": 30,
        }
        resp = self._request(url, params=params)
        if resp and resp.status_code == 200:
            try:
                hits = resp.json().get("hits", [])
                texts = []
                for h in hits:
                    text = h.get("comment_text") or h.get("title") or ""
                    text = re.sub(r'<[^>]+>', '', text).strip()
                    if text and len(text) > 20:
                        texts.append(text)
                return texts
            except Exception:
                pass
        return []

    def _score_comment(self, text: str) -> float:
        """Score a single comment's sentiment."""
        text_lower = text.lower()
        pos = ["great", "impressive", "innovative", "excited", "game changer",
               "underrated", "bullish", "amazing", "brilliant", "love"]
        neg = ["terrible", "overvalued", "scam", "dead", "declining", "bloated",
               "monopoly", "awful", "bearish", "concerned", "worried", "hate"]

        p = sum(1 for w in pos if w in text_lower)
        n = sum(1 for w in neg if w in text_lower)
        if p + n == 0:
            return 0.0
        return (p - n) / (p + n)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 11. SPACE/DEFENSE SUPPLY CHAIN MAP
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Since James is tracking LUNR (Intuitive Machines) and space stocks

SPACE_DEFENSE_CHAIN = {
    "LUNR": {
        "tier1": ["RKLB", "BA", "LMT"],
        "tier2": ["ASTR", "ASTS", "RDW"],
        "tier3": ["KTOS", "SPIR", "BKSY"],
        "categories": ["lunar_landers", "space_services", "nasa_contracts"],
        "gov_agencies": ["NASA", "DoD", "Space Force"],
    },
    "RKLB": {
        "tier1": ["LUNR", "BA", "LMT"],
        "tier2": ["ASTS", "SPCE", "MNTS"],
        "categories": ["launch_services", "satellite_buses"],
    },
    "LMT": {
        "tier1": ["BA", "NOC", "RTX", "GD"],
        "tier2": ["HII", "LHX", "KTOS"],
        "tier3": ["LUNR", "RKLB", "PLTR"],
        "categories": ["defense", "hypersonics", "space"],
    },
    "BA": {
        "tier1": ["RTX", "GE", "HWM", "SPR"],
        "tier2": ["LMT", "NOC", "TXT"],
        "categories": ["commercial_aviation", "defense", "space"],
    },
    "PLTR": {
        "tier1": ["SNOW", "MDB", "DDOG"],
        "tier2": ["CRWD", "PANW", "ZS"],
        "categories": ["gov_analytics", "defense_ai", "intelligence"],
    },
}
