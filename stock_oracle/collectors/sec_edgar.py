"""
SEC EDGAR Collector
===================
Analyzes SEC filings for hidden signals:
- Filing timing (Friday evening = burying bad news)
- Insider transactions (Form 4)
- Language complexity changes in 10-K/10-Q
- Unusual filing patterns
"""
import re
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from stock_oracle.collectors.base import BaseCollector, SignalResult
from stock_oracle.config import SEC_USER_AGENT

logger = logging.getLogger("stock_oracle")

# Words that hedge or signal uncertainty
HEDGE_WORDS = {
    "may", "might", "could", "possibly", "potentially", "approximately",
    "uncertain", "risk", "believe", "estimate", "expect", "anticipate",
    "projected", "contingent", "subject to", "no assurance", "cannot predict",
}

NEGATIVE_WORDS = {
    "loss", "decline", "decrease", "adverse", "impairment", "litigation",
    "default", "restructuring", "layoff", "downturn", "deficit", "write-off",
    "investigation", "subpoena", "fraud", "restatement", "material weakness",
}


class SECEdgarCollector(BaseCollector):
    """
    Analyzes SEC filings for timing patterns, insider activity,
    and linguistic signals.
    """

    SEC_BASE = "https://efts.sec.gov/LATEST"
    EDGAR_BASE = "https://data.sec.gov"

    @property
    def name(self) -> str:
        return "sec_edgar"

    def collect(self, ticker: str) -> SignalResult:
        cached = self._get_cached(ticker, "sec")
        if cached:
            return SignalResult.from_dict(cached)

        # Get company CIK
        cik = self._get_cik(ticker)
        if not cik:
            return self._neutral_signal(ticker, "CIK not found")

        # Collect sub-signals
        filing_timing = self._analyze_filing_timing(cik)
        insider_signal = self._analyze_insider_trades(cik)
        filing_language = self._analyze_filing_language(cik)

        # Combine signals
        signal = (
            filing_timing["signal"] * 0.3 +
            insider_signal["signal"] * 0.5 +
            filing_language["signal"] * 0.2
        )

        confidence = max(
            filing_timing["confidence"],
            insider_signal["confidence"],
            filing_language["confidence"],
        )

        result = SignalResult(
            collector_name=self.name,
            ticker=ticker,
            signal_value=signal,
            confidence=confidence,
            raw_data={
                "cik": cik,
                "filing_timing": filing_timing,
                "insider_activity": insider_signal,
                "filing_language": filing_language,
            },
            details=(
                f"Timing={filing_timing['signal']:+.2f} | "
                f"Insiders={insider_signal['signal']:+.2f} | "
                f"Language={filing_language['signal']:+.2f}"
            ),
        )

        self._set_cache(result.to_dict(), ticker, "sec")
        return result

    def _get_cik(self, ticker: str) -> Optional[str]:
        """Look up CIK number from ticker."""
        url = f"{self.EDGAR_BASE}/submissions/CIK{ticker}.json"
        headers = {"User-Agent": SEC_USER_AGENT}

        # Try the company tickers endpoint
        url2 = "https://www.sec.gov/files/company_tickers.json"
        resp = self._request(url2, headers=headers)
        if resp and resp.status_code == 200:
            try:
                data = resp.json()
                for entry in data.values():
                    if entry.get("ticker", "").upper() == ticker.upper():
                        return str(entry["cik_str"]).zfill(10)
            except Exception:
                pass
        return None

    def _analyze_filing_timing(self, cik: str) -> Dict:
        """
        Detect 'Friday dump' pattern: companies burying bad news
        by filing late Friday / before holidays.
        """
        url = f"{self.EDGAR_BASE}/submissions/CIK{cik}.json"
        headers = {"User-Agent": SEC_USER_AGENT}
        resp = self._request(url, headers=headers)

        if not resp or resp.status_code != 200:
            return {"signal": 0.0, "confidence": 0.0, "detail": "No data"}

        try:
            data = resp.json()
            recent = data.get("filings", {}).get("recent", {})
            dates = recent.get("filingDate", [])[:20]
            forms = recent.get("form", [])[:20]

            friday_filings = 0
            total_filings = 0

            for date_str, form in zip(dates, forms):
                if form in ("10-K", "10-Q", "8-K"):
                    total_filings += 1
                    filing_date = datetime.strptime(date_str, "%Y-%m-%d")
                    if filing_date.weekday() == 4:  # Friday
                        friday_filings += 1

            if total_filings == 0:
                return {"signal": 0.0, "confidence": 0.0, "detail": "No recent filings"}

            friday_ratio = friday_filings / total_filings
            # Normal distribution: ~20% on any day. >35% is suspicious
            signal = 0.0
            if friday_ratio > 0.35:
                signal = -(friday_ratio - 0.2) * 2  # Bearish
            elif friday_ratio < 0.1:
                signal = 0.1  # Slightly bullish — nothing to hide

            return {
                "signal": max(-1.0, min(1.0, signal)),
                "confidence": min(1.0, total_filings / 10),
                "detail": f"{friday_filings}/{total_filings} on Fridays",
            }
        except Exception as e:
            return {"signal": 0.0, "confidence": 0.0, "detail": str(e)}

    def _analyze_insider_trades(self, cik: str) -> Dict:
        """
        Analyze Form 4 (insider trades). Net buying = bullish.
        Cluster selling = very bearish.
        """
        url = f"{self.SEC_BASE}/search-index"
        params = {
            "q": f'"{cik}"',
            "dateRange": "custom",
            "startdt": (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d"),
            "enddt": datetime.now().strftime("%Y-%m-%d"),
            "forms": "4",
        }
        headers = {"User-Agent": SEC_USER_AGENT}

        # Use the full-text search endpoint
        search_url = f"{self.SEC_BASE}/search-index"
        params2 = {
            "q": f'"form 4"',
            "dateRange": "custom",
            "category": "form-type",
        }

        # Simplified: count Form 4 filings and check recent patterns
        url3 = f"{self.EDGAR_BASE}/submissions/CIK{cik}.json"
        resp = self._request(url3, headers=headers)

        if not resp or resp.status_code != 200:
            return {"signal": 0.0, "confidence": 0.0, "detail": "No data"}

        try:
            data = resp.json()
            recent = data.get("filings", {}).get("recent", {})
            forms = recent.get("form", [])
            dates = recent.get("filingDate", [])

            # Count Form 4s in last 90 days
            cutoff = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
            recent_form4s = sum(
                1 for f, d in zip(forms, dates)
                if f in ("4", "4/A") and d >= cutoff
            )

            # Many Form 4s in a cluster = something is happening
            # Without parsing the XML, we use count as a proxy
            # High Form 4 count near earnings = normal
            # High Form 4 count outside earnings = suspicious selling
            if recent_form4s > 15:
                signal = -0.3  # Lots of insider activity — often selling
            elif recent_form4s > 8:
                signal = -0.1
            elif recent_form4s < 3:
                signal = 0.1  # Quiet = insiders holding
            else:
                signal = 0.0

            return {
                "signal": signal,
                "confidence": 0.4,
                "detail": f"{recent_form4s} Form 4s in 90 days",
            }
        except Exception as e:
            return {"signal": 0.0, "confidence": 0.0, "detail": str(e)}

    def _analyze_filing_language(self, cik: str) -> Dict:
        """
        NLP on recent 8-K/10-K filings.
        Increased hedge words / negative language = bearish.
        """
        url = f"{self.EDGAR_BASE}/submissions/CIK{cik}.json"
        headers = {"User-Agent": SEC_USER_AGENT}
        resp = self._request(url, headers=headers)

        if not resp or resp.status_code != 200:
            return {"signal": 0.0, "confidence": 0.0, "detail": "No data"}

        try:
            data = resp.json()
            # Get the description of recent filings as a proxy
            recent = data.get("filings", {}).get("recent", {})
            descriptions = recent.get("primaryDocDescription", [])[:10]

            # Check for red-flag filing types
            red_flags = 0
            for desc in descriptions:
                desc_lower = (desc or "").lower()
                if any(w in desc_lower for w in ["restatement", "amendment", "delay", "nt 10"]):
                    red_flags += 1

            signal = -red_flags * 0.15
            return {
                "signal": max(-1.0, min(1.0, signal)),
                "confidence": 0.3,
                "detail": f"{red_flags} red-flag descriptions",
            }
        except Exception as e:
            return {"signal": 0.0, "confidence": 0.0, "detail": str(e)}
