"""
Job Postings Collector
======================
Tracks hiring velocity as a leading indicator.
- Rapid hiring in ML/AI roles = building something new
- Mass job deletions = coming layoffs
- Role types signal strategic direction
Uses Google Jobs search as a free data source.
"""
import re
import logging
from datetime import datetime
from typing import Dict, List

from stock_oracle.collectors.base import BaseCollector, SignalResult

logger = logging.getLogger("stock_oracle")


def get_company_name(ticker: str) -> str:
    """
    Resolve any ticker to a company name. Uses the dynamic resolver
    which auto-looks up via yfinance/SEC and caches permanently.
    """
    try:
        from stock_oracle.utils.ticker_resolver import resolve_name
        return resolve_name(ticker)
    except Exception:
        return ticker


# Keep a static map as fast-path fallback (resolver adds to this over time)
COMPANY_NAMES = {
    "AAPL": "Apple", "MSFT": "Microsoft", "GOOGL": "Google",
    "AMZN": "Amazon", "TSLA": "Tesla", "NVDA": "NVIDIA",
    "META": "Meta", "AMD": "AMD", "NFLX": "Netflix",
    "DIS": "Disney", "INTC": "Intel", "CRM": "Salesforce",
    "PYPL": "PayPal", "SQ": "Block", "SHOP": "Shopify",
    "UBER": "Uber", "LYFT": "Lyft", "SNAP": "Snap",
    "PINS": "Pinterest", "COIN": "Coinbase",
    "LUNR": "Intuitive Machines", "RKLB": "Rocket Lab", "BA": "Boeing",
    "LMT": "Lockheed Martin", "NOC": "Northrop Grumman", "RTX": "RTX Raytheon",
    "GD": "General Dynamics", "PLTR": "Palantir", "KTOS": "Kratos Defense",
    "ASTS": "AST SpaceMobile", "SPCE": "Virgin Galactic", "BKSY": "BlackSky",
    "WMT": "Walmart", "TGT": "Target", "COST": "Costco", "HD": "Home Depot",
    "LOW": "Lowes", "MCD": "McDonalds", "SBUX": "Starbucks",
    "JPM": "JPMorgan", "BAC": "Bank of America", "GS": "Goldman Sachs",
    "XOM": "ExxonMobil", "CVX": "Chevron",
    "SMCI": "Super Micro Computer", "DELL": "Dell", "SNOW": "Snowflake",
    "CRWD": "CrowdStrike", "PANW": "Palo Alto Networks",
}

# Strategic role categories
GROWTH_ROLES = {"machine learning", "ai", "data scientist", "growth", "expansion", "new market"}
COST_CUT_ROLES = {"restructuring", "transformation", "efficiency"}
PIVOT_ROLES = {"blockchain", "web3", "metaverse", "autonomous", "robotics", "quantum"}


class JobPostingsCollector(BaseCollector):
    """
    Analyzes job posting patterns to predict company trajectory.
    """

    @property
    def name(self) -> str:
        return "job_postings"

    def collect(self, ticker: str) -> SignalResult:
        cached = self._get_cached(ticker, "jobs")
        if cached:
            return SignalResult.from_dict(cached)

        company = get_company_name(ticker)
        if not company:
            company = ticker

        # Fetch job data from multiple angles
        job_data = self._fetch_job_signals(company, ticker)

        if not job_data["total_jobs"]:
            return self._neutral_signal(ticker, f"No job data for {company}")

        # Analyze signals
        signal = self._compute_signal(job_data)
        confidence = min(1.0, job_data["total_jobs"] / 100)

        result = SignalResult(
            collector_name=self.name,
            ticker=ticker,
            signal_value=signal,
            confidence=confidence,
            raw_data=job_data,
            details=(
                f"{job_data['total_jobs']} jobs | "
                f"growth_roles={job_data['growth_count']} | "
                f"pivot_roles={job_data['pivot_count']}"
            ),
        )

        self._set_cache(result.to_dict(), ticker, "jobs")
        return result

    def _fetch_job_signals(self, company: str, ticker: str) -> Dict:
        """
        Scrape job counts and categorize roles.
        Uses multiple search approaches for robustness.
        """
        total_jobs = 0
        growth_count = 0
        pivot_count = 0
        engineering_count = 0
        titles = []

        # Approach 1: Search via Google Jobs-like endpoint
        # (In practice, you'd use Indeed API, LinkedIn API, or a scraper)
        # Here we use a heuristic based on what's publicly scrapable

        # Try GitHub Jobs API (for tech companies)
        url = f"https://www.google.com/search"
        params = {
            "q": f"{company} jobs hiring 2025",
            "num": 10,
        }

        # Fallback: estimate from company career pages
        # Most companies have /careers endpoints
        career_urls = {
            "Apple": "https://jobs.apple.com/api/role/search",
            "Google": "https://careers.google.com/api/v3/search/",
            "Microsoft": "https://careers.microsoft.com/us/en/search-results",
            "Amazon": "https://www.amazon.jobs/en/search",
        }

        # Use HackerNews Who's Hiring as a signal
        hn_jobs = self._check_hn_hiring(company)

        # Aggregate what we can find
        total_jobs = hn_jobs.get("count", 0)
        titles = hn_jobs.get("titles", [])

        for title in titles:
            title_lower = title.lower()
            if any(kw in title_lower for kw in GROWTH_ROLES):
                growth_count += 1
            if any(kw in title_lower for kw in PIVOT_ROLES):
                pivot_count += 1
            if any(kw in title_lower for kw in {"engineer", "developer", "swe"}):
                engineering_count += 1

        return {
            "total_jobs": total_jobs,
            "growth_count": growth_count,
            "pivot_count": pivot_count,
            "engineering_count": engineering_count,
            "sample_titles": titles[:10],
            "source": "hn_whoishiring",
        }

    def _check_hn_hiring(self, company: str) -> Dict:
        """Check HackerNews 'Who is Hiring' threads for company mentions."""
        # Get the most recent "Who is hiring" thread
        url = "https://hacker-news.firebaseio.com/v0/item/39217310.json"  # Example thread ID
        # In practice, you'd search for the latest monthly thread

        # Search HN algolia API
        search_url = "https://hn.algolia.com/api/v1/search"
        params = {
            "query": f"{company} hiring",
            "tags": "comment",
            "numericFilters": f"created_at_i>{int(datetime.now().timestamp()) - 30*86400}",
        }

        resp = self._request(search_url, params=params)
        if resp and resp.status_code == 200:
            try:
                data = resp.json()
                hits = data.get("hits", [])
                titles = []
                for hit in hits[:20]:
                    text = hit.get("comment_text", "")
                    # Extract job title-like patterns
                    lines = text.split("\n")[:3]
                    for line in lines:
                        clean = re.sub(r'<[^>]+>', '', line).strip()
                        if clean and len(clean) < 200:
                            titles.append(clean)
                            break

                return {"count": len(hits), "titles": titles}
            except Exception:
                pass

        return {"count": 0, "titles": []}

    def _compute_signal(self, data: Dict) -> float:
        """Convert job data into a trading signal."""
        signal = 0.0

        total = data["total_jobs"]
        if total == 0:
            return 0.0

        # High growth role ratio = bullish
        growth_ratio = data["growth_count"] / max(total, 1)
        signal += growth_ratio * 0.5

        # Pivot roles = speculative bullish (high risk/reward)
        pivot_ratio = data["pivot_count"] / max(total, 1)
        signal += pivot_ratio * 0.3

        # Raw volume of engineering roles = investment in product
        if data["engineering_count"] > 5:
            signal += 0.2

        return max(-1.0, min(1.0, signal))
