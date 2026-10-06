"""
SEC EDGAR filings — no key required
===================================
CF  company filings: 8-K (with item descriptions), 10-Q/10-K, Form 4 insider
    reports, Form 144 planned sales, 13D/13G 5% holders, S-1/S-3/424B
    registrations and prospectuses (share offerings = dilution risk), proxies.

SEC asks every client to identify itself with a contact email in the
User-Agent. This uses SEC_USER_AGENT from Settings (the same value Oracle's
SEC collector uses).
"""
import json
import logging
import threading
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Optional

import requests

import stock_oracle.config as cfg
from stock_oracle.terminal import settings

logger = logging.getLogger("stock_oracle.terminal")

TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
ARCHIVE = "https://www.sec.gov/Archives/edgar/data/{cik}/{acc}"
TICKER_CACHE = Path(cfg.CACHE_DIR) / "sec_company_tickers.json"

FORMS = {
    "10-K": "Annual report", "10-K/A": "Annual report (amended)",
    "10-Q": "Quarterly report", "10-Q/A": "Quarterly report (amended)",
    "8-K": "Current report", "8-K/A": "Current report (amended)",
    "6-K": "Foreign issuer report", "20-F": "Foreign annual report",
    "4": "Insider transaction", "3": "Initial insider holdings", "5": "Annual insider report",
    "144": "Planned insider sale",
    "SC 13D": "Activist 5%+ stake", "SC 13D/A": "Activist stake update",
    "SC 13G": "Passive 5%+ stake", "SC 13G/A": "Passive stake update",
    "SCHEDULE 13D": "Activist 5%+ stake", "SCHEDULE 13D/A": "Activist stake update",
    "SCHEDULE 13G": "Passive 5%+ stake", "SCHEDULE 13G/A": "Passive stake update",
    "DEF 14A": "Proxy statement", "DEFA14A": "Proxy material", "PRE 14A": "Preliminary proxy",
    "S-1": "IPO / share registration", "S-1/A": "Share registration (amended)",
    "S-3": "Shelf registration", "S-3/A": "Shelf registration (amended)", "S-3ASR": "Automatic shelf",
    "S-8": "Employee stock plan shares", "424B1": "Prospectus", "424B2": "Prospectus",
    "424B3": "Prospectus", "424B4": "Prospectus (priced offering)", "424B5": "Prospectus supplement (offering)",
    "424B7": "Prospectus (selling holders)", "FWP": "Offering free-writing prospectus",
    "11-K": "Employee plan annual report", "SD": "Conflict minerals",
    "CORRESP": "Letter to SEC", "UPLOAD": "SEC comment letter", "ARS": "Annual report to holders",
    "NT 10-K": "Late annual report notice", "NT 10-Q": "Late quarterly report notice",
    "25-NSE": "Delisting notice", "15-12G": "Deregistration",
}

ITEMS_8K = {
    "1.01": "Material agreement", "1.02": "Agreement terminated", "1.03": "Bankruptcy",
    "1.05": "Cybersecurity incident", "2.01": "Acquisition or disposal", "2.02": "Earnings results",
    "2.03": "New debt obligation", "2.04": "Debt acceleration", "2.05": "Restructuring costs",
    "2.06": "Impairment", "3.01": "Listing deficiency / delisting", "3.02": "Unregistered share sale",
    "3.03": "Shareholder rights changed", "4.01": "Auditor change", "4.02": "Financials no longer reliable",
    "5.01": "Change in control", "5.02": "Officer/director change", "5.03": "Bylaws amended",
    "5.07": "Shareholder vote results", "7.01": "Reg FD disclosure", "8.01": "Other events",
    "9.01": "Exhibits",
}

# Forms that signal new shares coming to market
DILUTION_FORMS = {"S-1", "S-1/A", "S-3", "S-3/A", "S-3ASR", "424B1", "424B2", "424B3", "424B4",
                  "424B5", "424B7", "FWP"}
INSIDER_FORMS = {"3", "4", "5", "144"}
OWNERSHIP_FORMS = {"SC 13D", "SC 13D/A", "SC 13G", "SC 13G/A", "SCHEDULE 13D",
                   "SCHEDULE 13D/A", "SCHEDULE 13G", "SCHEDULE 13G/A"}

_lock = threading.Lock()
_map: Dict[str, Dict] = {}
_map_loaded = 0.0
_session = requests.Session()


def user_agent() -> str:
    return settings.get("SEC_USER_AGENT", cfg.SEC_USER_AGENT) or cfg.SEC_USER_AGENT


def ua_is_placeholder() -> bool:
    return "your@email.com" in user_agent() or "@" not in user_agent()


def _get(url: str):
    resp = _session.get(url, headers={"User-Agent": user_agent(), "Accept-Encoding": "gzip, deflate"},
                        timeout=20)
    if resp.status_code == 403:
        raise RuntimeError("SEC refused the request (403). Set SEC_USER_AGENT in Settings to "
                           "'StockOracle you@yourdomain.com' — SEC requires a real contact email.")
    resp.raise_for_status()
    return resp.json()


def _ticker_map() -> Dict[str, Dict]:
    global _map, _map_loaded
    with _lock:
        if _map and time.time() - _map_loaded < 7 * 86400:
            return _map
        data = None
        try:
            if TICKER_CACHE.exists() and time.time() - TICKER_CACHE.stat().st_mtime < 7 * 86400:
                data = json.loads(TICKER_CACHE.read_text(encoding="utf-8"))
        except Exception:
            data = None
        if data is None:
            data = _get(TICKERS_URL)
            try:
                TICKER_CACHE.write_text(json.dumps(data), encoding="utf-8")
            except Exception:
                pass
        _map = {str(v["ticker"]).upper(): {"cik": int(v["cik_str"]), "name": v.get("title", "")}
                for v in data.values()}
        _map_loaded = time.time()
        return _map


def lookup(sym: str) -> Optional[Dict]:
    m = _ticker_map()
    return m.get(sym) or m.get(sym.replace("-", ".")) or m.get(sym.replace("-", ""))


def filings(sym: str, limit: int = 80) -> Dict:
    ent = lookup(sym)
    if not ent:
        raise LookupError(f"{sym} isn't an SEC registrant (ETFs, funds and foreign listings often aren't)")
    cik = ent["cik"]
    data = _get(SUBMISSIONS_URL.format(cik=f"{cik:010d}"))
    recent = (data.get("filings") or {}).get("recent") or {}
    n = len(recent.get("accessionNumber", []))
    rows = []
    cutoff_90 = (date.today() - timedelta(days=90)).isoformat()
    counts = {"insider": 0, "dilution": 0, "ownership": 0, "8k": 0}
    for i in range(n):
        form = recent["form"][i]
        filed = recent["filingDate"][i]
        acc = recent["accessionNumber"][i]
        acc_nodash = acc.replace("-", "")
        base = ARCHIVE.format(cik=cik, acc=acc_nodash)
        primary = recent.get("primaryDocument", [""] * n)[i]
        items = [x.strip() for x in (recent.get("items", [""] * n)[i] or "").split(",") if x.strip()]
        if filed >= cutoff_90:
            if form in INSIDER_FORMS:
                counts["insider"] += 1
            if form in DILUTION_FORMS:
                counts["dilution"] += 1
            if form in OWNERSHIP_FORMS:
                counts["ownership"] += 1
            if form.startswith("8-K"):
                counts["8k"] += 1
        if len(rows) < limit:
            rows.append({
                "form": form,
                "description": FORMS.get(form, recent.get("primaryDocDescription", [""] * n)[i] or ""),
                "filed": filed,
                "report_date": recent.get("reportDate", [""] * n)[i],
                "accepted": recent.get("acceptanceDateTime", [""] * n)[i],
                "items": [{"code": c, "label": ITEMS_8K.get(c, "")} for c in items],
                "category": ("insider" if form in INSIDER_FORMS else
                             "dilution" if form in DILUTION_FORMS else
                             "ownership" if form in OWNERSHIP_FORMS else
                             "periodic" if form.split("/")[0] in ("10-K", "10-Q", "20-F", "6-K") else
                             "event" if form.startswith("8-K") else "other"),
                "url": f"{base}/{primary}" if primary else f"{base}/{acc}-index.htm",
                "index_url": f"{base}/{acc}-index.htm",
            })
    return {
        "symbol": sym,
        "cik": cik,
        "name": data.get("name") or ent["name"],
        "sic": data.get("sicDescription", ""),
        "fiscal_year_end": data.get("fiscalYearEnd", ""),
        "last_90d": counts,
        "rows": rows,
        "edgar_url": f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type=&dateb=&owner=include&count=40",
        "src": "SEC EDGAR",
    }
