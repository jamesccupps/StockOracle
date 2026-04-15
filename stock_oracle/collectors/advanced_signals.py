"""
Supply Chain Cascade Collector
==============================
When a big company announces something, trace the supply chain
downstream to find tier-2/3 suppliers that will benefit.
"""
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List

from stock_oracle.collectors.base import BaseCollector, SignalResult

logger = logging.getLogger("stock_oracle")

# Known supply chain relationships (this would be a much larger DB in production)
SUPPLY_CHAIN_MAP = {
    "AAPL": {
        "tier1": ["TSM", "QCOM", "AVGO", "TXN", "SWKS"],
        "tier2": ["LRCX", "AMAT", "KLAC", "ASML"],
        "tier3": ["CREE", "II-VI", "MKSI"],
        "categories": ["semiconductors", "displays", "sensors", "assembly"],
    },
    "TSLA": {
        "tier1": ["PCRFY", "ALB", "SQM", "LTHM"],
        "tier2": ["X", "NUE", "STLD", "FCX"],
        "tier3": ["LAC", "PLL", "MP"],
        "categories": ["batteries", "steel", "lithium", "rare_earth"],
    },
    "AMZN": {
        "tier1": ["UPS", "FDX", "RIVN"],
        "tier2": ["PCAR", "PLUG", "GFL"],
        "tier3": ["RKLB", "RR"],
        "categories": ["logistics", "delivery", "cloud_hardware"],
    },
    "MSFT": {
        "tier1": ["NVDA", "AMD", "INTC"],
        "tier2": ["DELL", "HPE", "SMCI"],
        "tier3": ["ANET", "FFIV"],
        "categories": ["cloud", "ai_chips", "servers"],
    },
    "NVDA": {
        "tier1": ["TSM", "ASML", "AVGO"],
        "tier2": ["AMAT", "LRCX", "KLAC"],
        "tier3": ["CREE", "MKSI", "ONTO"],
        "categories": ["fab", "lithography", "packaging"],
    },
    # Space & Defense
    "LUNR": {
        "tier1": ["RKLB", "BA", "LMT"],
        "tier2": ["ASTS", "RDW", "SPCE"],
        "tier3": ["KTOS", "SPIR", "BKSY"],
        "categories": ["lunar_landers", "space_services", "nasa_contracts"],
    },
    "RKLB": {
        "tier1": ["LUNR", "BA", "LMT", "ASTS"],
        "tier2": ["MNTS", "SPCE", "BKSY"],
        "tier3": ["SPIR", "RDW"],
        "categories": ["launch_services", "satellite_buses", "space_systems"],
    },
    "LMT": {
        "tier1": ["BA", "NOC", "RTX", "GD"],
        "tier2": ["HII", "LHX", "KTOS"],
        "tier3": ["LUNR", "RKLB", "PLTR"],
        "categories": ["defense", "hypersonics", "space", "f35"],
    },
    "PLTR": {
        "tier1": ["SNOW", "MDB", "CRWD"],
        "tier2": ["DDOG", "PANW", "ZS"],
        "tier3": ["AI", "BBAI", "SAIC"],
        "categories": ["gov_analytics", "defense_ai", "intelligence"],
    },
}


class SupplyChainCollector(BaseCollector):
    """
    Detects supply chain cascade opportunities.
    When a major company has positive/negative news,
    trace to suppliers that haven't moved yet.
    """

    @property
    def name(self) -> str:
        return "supply_chain"

    def collect(self, ticker: str) -> SignalResult:
        cached = self._get_cached(ticker, "supply")
        if cached:
            return SignalResult.from_dict(cached)

        # Check if this ticker IS a supplier to a major company
        parent_signals = self._check_parent_momentum(ticker)

        # Check if this ticker HAS known suppliers
        downstream = SUPPLY_CHAIN_MAP.get(ticker.upper(), {})

        if not parent_signals and not downstream:
            return self._neutral_signal(ticker, "No supply chain data")

        # If ticker is a supplier, check parent companies for momentum
        signal = 0.0
        details_parts = []

        if parent_signals:
            for parent, strength in parent_signals.items():
                signal += strength * 0.3
                details_parts.append(f"{parent}={strength:+.2f}")

        # If ticker has suppliers, that data goes to the downstream tickers
        if downstream:
            details_parts.append(f"Has {len(downstream.get('tier1', []))} T1 suppliers")

        signal = max(-1.0, min(1.0, signal))
        confidence = min(1.0, len(parent_signals) * 0.3) if parent_signals else 0.2

        result = SignalResult(
            collector_name=self.name,
            ticker=ticker,
            signal_value=signal,
            confidence=confidence,
            raw_data={
                "parent_signals": parent_signals,
                "downstream_tickers": downstream,
            },
            details=" | ".join(details_parts) if details_parts else "No cascade signal",
        )

        self._set_cache(result.to_dict(), ticker, "supply")
        return result

    def _check_parent_momentum(self, ticker: str) -> Dict[str, float]:
        """Check if any parent companies have recent momentum."""
        parents = {}
        ticker_upper = ticker.upper()

        for parent, chain in SUPPLY_CHAIN_MAP.items():
            all_suppliers = (
                chain.get("tier1", []) +
                chain.get("tier2", []) +
                chain.get("tier3", [])
            )
            if ticker_upper in [s.upper() for s in all_suppliers]:
                # Determine tier (closer = stronger signal)
                if ticker_upper in [s.upper() for s in chain.get("tier1", [])]:
                    parents[parent] = 0.6
                elif ticker_upper in [s.upper() for s in chain.get("tier2", [])]:
                    parents[parent] = 0.4
                else:
                    parents[parent] = 0.2

        return parents

    def get_cascade_targets(self, ticker: str) -> List[Dict]:
        """Get all suppliers that would be affected by this ticker's moves."""
        chain = SUPPLY_CHAIN_MAP.get(ticker.upper(), {})
        targets = []
        for tier_name in ["tier1", "tier2", "tier3"]:
            for supplier in chain.get(tier_name, []):
                targets.append({
                    "ticker": supplier,
                    "tier": tier_name,
                    "delay_days": {"tier1": 1, "tier2": 3, "tier3": 5}.get(tier_name, 3),
                })
        return targets


class GovernmentContractsCollector(BaseCollector):
    """
    Tracks government contract awards from USAspending.gov.
    A small contractor winning a big contract = very bullish.
    """

    @property
    def name(self) -> str:
        return "gov_contracts"

    def collect(self, ticker: str) -> SignalResult:
        cached = self._get_cached(ticker, "gov")
        if cached:
            return SignalResult.from_dict(cached)

        from stock_oracle.collectors.job_postings import get_company_name
        company = get_company_name(ticker)

        contracts = self._fetch_contracts(company)

        if not contracts:
            return self._neutral_signal(ticker, "No government contracts found")

        total_value = sum(c.get("value", 0) for c in contracts)
        recent_count = len(contracts)

        # Large new contracts = bullish
        signal = 0.0
        if total_value > 100_000_000:
            signal = 0.5
        elif total_value > 10_000_000:
            signal = 0.3
        elif total_value > 1_000_000:
            signal = 0.1

        confidence = min(1.0, recent_count / 5)

        result = SignalResult(
            collector_name=self.name,
            ticker=ticker,
            signal_value=signal,
            confidence=confidence,
            raw_data={"contracts": contracts, "total_value": total_value},
            details=f"{recent_count} contracts | ${total_value:,.0f} total",
        )

        self._set_cache(result.to_dict(), ticker, "gov")
        return result

    def _fetch_contracts(self, company: str) -> List[Dict]:
        """Fetch from USAspending.gov API."""
        url = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
        # The API uses POST for searches
        import json

        payload = {
            "filters": {
                "keywords": [company],
                "time_period": [
                    {
                        "start_date": (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d"),
                        "end_date": datetime.now().strftime("%Y-%m-%d"),
                    }
                ],
            },
            "fields": [
                "Award ID", "Recipient Name", "Award Amount",
                "Start Date", "End Date", "Awarding Agency",
            ],
            "limit": 10,
            "page": 1,
            "sort": "Award Amount",
            "order": "desc",
        }

        try:
            resp = self._session.post(url, json=payload, timeout=15,
                                      headers={"Content-Type": "application/json"})
            if resp.status_code == 200:
                data = resp.json()
                results = data.get("results", [])
                return [
                    {
                        "id": r.get("Award ID"),
                        "recipient": r.get("Recipient Name"),
                        "value": float(r.get("Award Amount", 0) or 0),
                        "agency": r.get("Awarding Agency"),
                        "start_date": r.get("Start Date"),
                    }
                    for r in results
                ]
        except Exception as e:
            logger.error(f"USAspending error: {e}")

        return []


class PatentActivityCollector(BaseCollector):
    """
    Tracks USPTO patent filings.
    Sudden burst of patents in a new area = company pivoting.
    """

    @property
    def name(self) -> str:
        return "patent_activity"

    def collect(self, ticker: str) -> SignalResult:
        cached = self._get_cached(ticker, "patents")
        if cached:
            return SignalResult.from_dict(cached)

        from stock_oracle.collectors.job_postings import get_company_name
        company = get_company_name(ticker)

        patent_data = self._fetch_patents(company)

        if not patent_data["count"]:
            return self._neutral_signal(ticker, "No patent data")

        # More recent patents = more innovation investment = bullish
        signal = min(1.0, patent_data["count"] / 50) * 0.5

        # Check for new technology areas
        if patent_data.get("new_categories"):
            signal += 0.3  # Entering new areas is very bullish

        result = SignalResult(
            collector_name=self.name,
            ticker=ticker,
            signal_value=max(-1.0, min(1.0, signal)),
            confidence=min(1.0, patent_data["count"] / 20),
            raw_data=patent_data,
            details=f"{patent_data['count']} patents | categories={patent_data.get('categories', [])}",
        )

        self._set_cache(result.to_dict(), ticker, "patents")
        return result

    def _fetch_patents(self, company: str) -> Dict:
        """Search for patents. Tries multiple sources since PatentsView API is defunct."""

        # Approach 1: USPTO full-text search (PEDS)
        patents_found = self._search_uspto_peds(company)
        if patents_found["count"] > 0:
            return patents_found

        # Approach 2: Google Scholar patent search via Serpapi-style
        # Use Google search as a last resort
        patents_found = self._search_google_patents(company)
        if patents_found["count"] > 0:
            return patents_found

        return {"count": 0, "patents": [], "categories": [], "new_categories": False}

    def _search_uspto_peds(self, company: str) -> Dict:
        """Search USPTO Patent Examination Data System."""
        url = "https://ped.uspto.gov/api/queries"
        payload = {
            "searchText": f"firstNamedApplicant:({company})",
            "fq": [f"appFilingDate:[{(datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')} TO *]"],
            "fl": "patentTitle,appFilingDate,patentNumber",
            "mm": "90%",
            "start": 0,
            "rows": 20,
        }
        try:
            resp = self._session.post(url, json=payload, timeout=15,
                                      headers={"Content-Type": "application/json"})
            if resp and resp.status_code == 200:
                data = resp.json()
                docs = data.get("queryResults", {}).get("searchResponse", {}).get("response", {}).get("docs", [])
                categories = set()
                for p in docs:
                    title = (p.get("patentTitle") or "").lower()
                    for cat in ["artificial intelligence", "machine learning", "blockchain",
                                "autonomous", "quantum", "battery", "solar", "5g", "iot"]:
                        if cat in title:
                            categories.add(cat)
                return {
                    "count": len(docs),
                    "patents": [{"title": d.get("patentTitle"), "date": d.get("appFilingDate")} for d in docs[:10]],
                    "categories": list(categories),
                    "new_categories": len(categories) > 0,
                    "source": "uspto_peds",
                }
        except Exception as e:
            logger.debug(f"USPTO PEDS error: {e}")
        return {"count": 0, "patents": [], "categories": []}

    def _search_google_patents(self, company: str) -> Dict:
        """Fallback: estimate patent activity via Google search."""
        url = "https://www.googleapis.com/customsearch/v1"
        # This requires a Google API key — degrade gracefully without one
        return {"count": 0, "patents": [], "categories": [], "source": "none",
                "note": "PatentsView API is defunct. Connect USPTO PEDS or Google Patents API."}


class CongressionalTradesCollector(BaseCollector):
    """
    Track congressional stock trades (public data).
    Some members statistically outperform the market.
    """

    @property
    def name(self) -> str:
        return "insider_trades"

    def collect(self, ticker: str) -> SignalResult:
        cached = self._get_cached(ticker, "congress")
        if cached:
            return SignalResult.from_dict(cached)

        trades = self._fetch_congressional_trades(ticker)

        if not trades:
            return self._neutral_signal(ticker, "No congressional trades found")

        # Count buys vs sells
        buys = sum(1 for t in trades if t.get("type") == "purchase")
        sells = sum(1 for t in trades if t.get("type") == "sale")

        if buys + sells == 0:
            return self._neutral_signal(ticker, "No clear trade direction")

        # Net buy ratio
        net_ratio = (buys - sells) / (buys + sells)
        signal = net_ratio * 0.6  # Don't overweight this

        result = SignalResult(
            collector_name=self.name,
            ticker=ticker,
            signal_value=max(-1.0, min(1.0, signal)),
            confidence=min(1.0, (buys + sells) / 10),
            raw_data={
                "trades": trades,
                "buys": buys,
                "sells": sells,
                "net_ratio": net_ratio,
            },
            details=f"{buys} buys / {sells} sells | net={net_ratio:+.2f}",
        )

        self._set_cache(result.to_dict(), ticker, "congress")
        return result

    def _fetch_congressional_trades(self, ticker: str) -> List[Dict]:
        """
        Fetch from public congressional trading data.
        Tries multiple sources since the original S3 bucket is often blocked.
        """

        # Source 1: Senate Stock Watcher
        trades = self._fetch_senate_watcher(ticker)
        if trades:
            return trades

        # Source 2: House Stock Watcher (original source, may be blocked)
        trades = self._fetch_house_watcher(ticker)
        if trades:
            return trades

        # Source 3: SEC EDGAR Form 4 as a proxy for insider trading
        # (not congressional, but insider transactions are also public)
        trades = self._fetch_sec_insider_proxy(ticker)
        return trades

    def _fetch_senate_watcher(self, ticker: str) -> List[Dict]:
        """Try senate-stock-watcher-data S3 bucket."""
        url = "https://senate-stock-watcher-data.s3-us-west-2.amazonaws.com/aggregate/all_transactions.json"
        resp = self._request(url)
        if resp and resp.status_code == 200:
            try:
                all_trades = resp.json()
                cutoff = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
                return [
                    {
                        "representative": t.get("senator") or t.get("first_name", "") + " " + t.get("last_name", ""),
                        "type": t.get("type", "").lower(),
                        "amount": t.get("amount"),
                        "date": t.get("transaction_date"),
                        "ticker": t.get("ticker"),
                    }
                    for t in all_trades
                    if t.get("ticker", "").upper() == ticker.upper()
                    and t.get("transaction_date", "") >= cutoff
                ][:20]
            except Exception:
                pass
        return []

    def _fetch_house_watcher(self, ticker: str) -> List[Dict]:
        """Try house-stock-watcher-data S3 bucket."""
        url = "https://house-stock-watcher-data.s3-us-west-2.amazonaws.com/data/all_transactions.json"
        resp = self._request(url)
        if resp and resp.status_code == 200:
            try:
                all_trades = resp.json()
                cutoff = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
                return [
                    {
                        "representative": t.get("representative"),
                        "type": t.get("type", "").lower(),
                        "amount": t.get("amount"),
                        "date": t.get("transaction_date"),
                        "ticker": t.get("ticker"),
                    }
                    for t in all_trades
                    if t.get("ticker", "").upper() == ticker.upper()
                    and t.get("transaction_date", "") >= cutoff
                ][:20]
            except Exception:
                pass
        return []

    def _fetch_sec_insider_proxy(self, ticker: str) -> List[Dict]:
        """
        Fallback: use SEC EDGAR full-text search for Form 4 mentions.
        Not a perfect replacement but gives insider trading signal.
        """
        from stock_oracle.config import SEC_USER_AGENT
        url = "https://efts.sec.gov/LATEST/search-index"
        params = {
            "q": f'"{ticker}" AND "form 4"',
            "dateRange": "custom",
            "startdt": (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d"),
            "enddt": datetime.now().strftime("%Y-%m-%d"),
        }
        headers = {"User-Agent": SEC_USER_AGENT}
        resp = self._request(url, params=params, headers=headers)
        if resp and resp.status_code == 200:
            try:
                data = resp.json()
                total = data.get("hits", {}).get("total", {}).get("value", 0)
                if total > 0:
                    # Rough heuristic: more Form 4 filings = more insider activity
                    return [{"type": "insider_filing", "count": total, "source": "sec_edgar"}]
            except Exception:
                pass
        return []
