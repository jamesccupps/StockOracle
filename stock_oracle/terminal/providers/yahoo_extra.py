"""
Yahoo Finance — ownership, estimates, corporate actions, funds, screens
=======================================================================
EEO   consensus estimates and how they've been revised (7/30/60/90 days)
HDS   major holders, top institutions and funds
INS   insider transactions (Form 4 data, parsed) and 6-month net activity
DVD   dividend history, growth, splits
SI    short interest (bi-monthly exchange data) — FINRA daily volume is added by the router
HOLD  ETF top holdings, sector weights, expense ratio
MOST  market movers and other Yahoo predefined screens
"""
import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict

from stock_oracle.terminal.providers.yahoo import _info, _int, _num, _require

logger = logging.getLogger("stock_oracle.terminal")

try:
    import yfinance as yf
except ImportError:  # pragma: no cover
    yf = None

PERIODS = {"0q": "Current quarter", "+1q": "Next quarter", "0y": "Current fiscal year",
           "+1y": "Next fiscal year", "-5y": "Past 5 years (per yr)", "+5y": "Next 5 years (per yr)"}


def _safe(obj, attr):
    try:
        return getattr(obj, attr)
    except Exception as e:
        logger.debug(f"yahoo {attr} failed: {e}")
        return None


def _date(v) -> str:
    if v is None:
        return ""
    if hasattr(v, "strftime"):
        return v.strftime("%Y-%m-%d")
    if isinstance(v, (int, float)) and v > 10_000_000:
        return datetime.fromtimestamp(v, tz=timezone.utc).strftime("%Y-%m-%d")
    return str(v)[:10]


# ── estimates & revisions ────────────────────────────────────

def estimates(sym: str) -> Dict:
    _require()
    t = yf.Ticker(sym)
    out = {"symbol": sym, "earnings": [], "revenue": [], "trend": [], "revisions": [],
           "growth": [], "src": "yahoo"}

    def rows(df):
        if df is None or getattr(df, "empty", True):
            return []
        return [(str(idx), r) for idx, r in df.iterrows()]

    for period, r in rows(_safe(t, "earnings_estimate")):
        out["earnings"].append({"period": period, "label": PERIODS.get(period, period),
                                "avg": _num(r.get("avg")), "low": _num(r.get("low")),
                                "high": _num(r.get("high")), "year_ago": _num(r.get("yearAgoEps")),
                                "analysts": _int(r.get("numberOfAnalysts")), "growth": _num(r.get("growth"))})
    for period, r in rows(_safe(t, "revenue_estimate")):
        out["revenue"].append({"period": period, "label": PERIODS.get(period, period),
                               "avg": _num(r.get("avg")), "low": _num(r.get("low")),
                               "high": _num(r.get("high")), "year_ago": _num(r.get("yearAgoRevenue")),
                               "analysts": _int(r.get("numberOfAnalysts")), "growth": _num(r.get("growth"))})
    for period, r in rows(_safe(t, "eps_trend")):
        cur, d30, d90 = _num(r.get("current")), _num(r.get("30daysAgo")), _num(r.get("90daysAgo"))
        out["trend"].append({
            "period": period, "label": PERIODS.get(period, period), "current": cur,
            "d7": _num(r.get("7daysAgo")), "d30": d30, "d60": _num(r.get("60daysAgo")), "d90": d90,
            "chg_30d_pct": ((cur - d30) / abs(d30) * 100) if (cur is not None and d30) else None,
            "chg_90d_pct": ((cur - d90) / abs(d90) * 100) if (cur is not None and d90) else None,
        })
    for period, r in rows(_safe(t, "eps_revisions")):
        up30, dn30 = _int(r.get("upLast30days")) or 0, _int(r.get("downLast30days")) or 0
        out["revisions"].append({
            "period": period, "label": PERIODS.get(period, period),
            "up_7d": _int(r.get("upLast7days")) or 0, "down_7d": _int(r.get("downLast7Days")) or 0,
            "up_30d": up30, "down_30d": dn30,
            "net_30d": up30 - dn30,
        })
    for period, r in rows(_safe(t, "growth_estimates")):
        out["growth"].append({"period": period, "label": PERIODS.get(period, period),
                              "stock": _num(r.get("stockTrend")), "index": _num(r.get("indexTrend"))})

    # One-line read for the dossier/ASK: direction of current-FY EPS revisions
    fy = next((x for x in out["trend"] if x["period"] == "0y"), None)
    rv = next((x for x in out["revisions"] if x["period"] == "0y"), None)
    out["summary"] = {
        "fy_eps_chg_30d_pct": fy["chg_30d_pct"] if fy else None,
        "fy_eps_chg_90d_pct": fy["chg_90d_pct"] if fy else None,
        "fy_net_revisions_30d": rv["net_30d"] if rv else None,
    }
    if not any(out[k] for k in ("earnings", "revenue", "trend")):
        raise LookupError(f"No analyst estimates for {sym}")
    return out


# ── ownership ────────────────────────────────────────────────

def holders(sym: str) -> Dict:
    _require()
    t = yf.Ticker(sym)
    major = {}
    mh = _safe(t, "major_holders")
    if mh is not None and not getattr(mh, "empty", True):
        try:
            col = mh.columns[0]
            major = {str(k): _num(v) for k, v in mh[col].items()}
        except Exception:
            major = {}

    def table(df):
        out = []
        if df is None or getattr(df, "empty", True):
            return out
        for r in df.to_dict("records"):
            out.append({
                "holder": r.get("Holder", ""),
                "date": _date(r.get("Date Reported")),
                "pct": _num(r.get("pctHeld")),
                "shares": _num(r.get("Shares")),
                "value": _num(r.get("Value")),
                "pct_change": _num(r.get("pctChange")),
            })
        return out

    inst = table(_safe(t, "institutional_holders"))
    funds = table(_safe(t, "mutualfund_holders"))
    if not major and not inst and not funds:
        raise LookupError(f"No holder data for {sym}")
    return {
        "symbol": sym,
        "insiders_pct": major.get("insidersPercentHeld"),
        "institutions_pct": major.get("institutionsPercentHeld"),
        "institutions_float_pct": major.get("institutionsFloatPercentHeld"),
        "institutions_count": _int(major.get("institutionsCount")),
        "institutions": inst,
        "funds": funds,
        "src": "yahoo (13F filings)",
    }


def _insider_kind(text: str) -> str:
    t = (text or "").lower()
    if "sale" in t:
        return "Sale"
    if "purchase" in t or "buy" in t:
        return "Buy"
    if "award" in t or "grant" in t:
        return "Award"
    if "exercise" in t or "conversion" in t:
        return "Exercise"
    if "gift" in t:
        return "Gift"
    return "Other" if t else "Filed"


def insiders(sym: str) -> Dict:
    _require()
    t = yf.Ticker(sym)
    rows = []
    df = _safe(t, "insider_transactions")
    if df is not None and not getattr(df, "empty", True):
        for r in df.head(60).to_dict("records"):
            kind = _insider_kind(r.get("Text", ""))
            rows.append({
                "date": _date(r.get("Start Date")),
                "insider": str(r.get("Insider", "")).title(),
                "position": r.get("Position", ""),
                "kind": kind,
                "text": r.get("Text", "") or r.get("Transaction", ""),
                "shares": _num(r.get("Shares")),
                "value": _num(r.get("Value")),
                "ownership": "Direct" if r.get("Ownership") == "D" else "Indirect" if r.get("Ownership") == "I" else "",
            })
    # Open-market buys/sells only — awards and exercises aren't conviction signals
    cutoff = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
    agg = defaultdict(lambda: {"count": 0, "shares": 0.0, "value": 0.0})
    for r in rows:
        if r["date"] >= cutoff and r["kind"] in ("Buy", "Sale"):
            a = agg[r["kind"]]
            a["count"] += 1
            a["shares"] += r["shares"] or 0
            a["value"] += r["value"] or 0
    six_month = []
    ip = _safe(t, "insider_purchases")
    if ip is not None and not getattr(ip, "empty", True):
        try:
            label_col = ip.columns[0]
            for r in ip.to_dict("records"):
                six_month.append({"label": str(r.get(label_col, "")), "shares": _num(r.get("Shares")),
                                  "transactions": _int(r.get("Trans"))})
        except Exception:
            pass
    if not rows and not six_month:
        raise LookupError(f"No insider data for {sym}")
    buys, sells = agg["Buy"], agg["Sale"]
    return {
        "symbol": sym,
        "rows": rows,
        "last_90d": {"buys": buys["count"], "buy_value": buys["value"],
                     "sells": sells["count"], "sell_value": sells["value"],
                     "net_value": buys["value"] - sells["value"]},
        "six_month": six_month,
        "src": "yahoo (SEC Form 4)",
    }


# ── corporate actions ────────────────────────────────────────

def dividends(sym: str) -> Dict:
    _require()
    t = yf.Ticker(sym)
    info = _info(sym)
    payments = []
    divs = _safe(t, "dividends")
    if divs is not None and len(divs):
        for ts, amt in divs.items():
            payments.append({"date": _date(ts), "amount": _num(amt)})
    payments.reverse()                      # newest first
    by_year = defaultdict(float)
    for p in payments:
        by_year[p["date"][:4]] += p["amount"] or 0
    this_year = str(datetime.now().year)
    full_years = sorted(y for y in by_year if y != this_year)
    annual = [{"year": y, "total": round(by_year[y], 4)} for y in sorted(by_year, reverse=True)[:12]]
    cagr5 = None
    if len(full_years) >= 6:
        a, b = by_year[full_years[-6]], by_year[full_years[-1]]
        if a > 0 and b > 0:
            cagr5 = ((b / a) ** (1 / 5) - 1) * 100
    splits = []
    sp = _safe(t, "splits")
    if sp is not None and len(sp):
        for ts, ratio in sp.items():
            r = _num(ratio)
            splits.append({"date": _date(ts), "ratio": r,
                           "label": (f"{r:g}-for-1" if r and r >= 1 else f"1-for-{1 / r:g} (reverse)") if r else ""})
        splits.reverse()
    price = _num(info.get("regularMarketPrice") or info.get("currentPrice") or info.get("navPrice"))
    rate = _num(info.get("dividendRate") or info.get("trailingAnnualDividendRate")) or None
    ttm = sum(p["amount"] or 0 for p in payments
              if p["date"] >= (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d"))
    return {
        "symbol": sym,
        "price": price,
        "annual_rate": rate,
        "ttm_paid": round(ttm, 4) if ttm else None,
        "yield_pct": (rate / price * 100) if (rate and price) else ((ttm / price * 100) if (ttm and price) else None),
        "payout_ratio": _num(info.get("payoutRatio")),
        "five_year_avg_yield": _num(info.get("fiveYearAvgDividendYield")),
        "ex_date": _date(info.get("exDividendDate")),
        "growth_5y_cagr_pct": cagr5,
        "payments": payments[:40],
        "annual": annual,
        "splits": splits,
        "src": "yahoo",
    }


def short_interest(sym: str) -> Dict:
    info = _info(sym)
    shares = _num(info.get("sharesShort"))
    prior = _num(info.get("sharesShortPriorMonth"))
    return {
        "symbol": sym,
        "shares_short": shares,
        "prior_month": prior,
        "change_pct": ((shares - prior) / prior * 100) if (shares and prior) else None,
        "pct_float": _num(info.get("shortPercentOfFloat")),
        "pct_shares_out": _num(info.get("sharesPercentSharesOut")),
        "days_to_cover": _num(info.get("shortRatio")),
        "as_of": _date(info.get("dateShortInterest")),
        "float": _num(info.get("floatShares")),
        "avg_volume": _num(info.get("averageVolume")),
        "src": "yahoo (exchange short interest, reported twice monthly)",
    }


# ── funds ────────────────────────────────────────────────────

def etf(sym: str) -> Dict:
    _require()
    t = yf.Ticker(sym)
    fd = _safe(t, "funds_data")
    if fd is None:
        raise LookupError(f"{sym} isn't a fund")
    top = []
    th = _safe(fd, "top_holdings")
    if th is not None and not getattr(th, "empty", True):
        for s, r in th.iterrows():
            top.append({"symbol": str(s), "name": r.get("Name", ""), "pct": _num(r.get("Holding Percent"))})
    sectors = {k: _num(v) for k, v in (_safe(fd, "sector_weightings") or {}).items()}
    assets = {k: _num(v) for k, v in (_safe(fd, "asset_classes") or {}).items()}
    overview = _safe(fd, "fund_overview") or {}
    ops = {}
    fo = _safe(fd, "fund_operations")
    if fo is not None and not getattr(fo, "empty", True):
        try:
            col = fo.columns[0]
            ops = {str(k): _num(v) for k, v in fo[col].items()}
        except Exception:
            ops = {}
    eq = {}
    eh = _safe(fd, "equity_holdings")
    if eh is not None and not getattr(eh, "empty", True):
        try:
            col = eh.columns[0]
            for k, v in eh[col].items():
                v = _num(v)
                # Yahoo stores fund valuation as yields (E/P, B/P...); flip to ratios
                if str(k).startswith("Price/") and v:
                    v = 1 / v
                eq[str(k)] = v
        except Exception:
            eq = {}
    if not top and not sectors:
        raise LookupError(f"No fund holdings data for {sym}")
    return {
        "symbol": sym,
        "category": overview.get("categoryName", ""),
        "family": overview.get("family", ""),
        "legal_type": overview.get("legalType", ""),
        "expense_ratio": ops.get("Annual Report Expense Ratio"),
        "turnover": ops.get("Annual Holdings Turnover"),
        "net_assets": ops.get("Total Net Assets"),
        "top_holdings": top,
        "top10_pct": sum(h["pct"] or 0 for h in top) or None,
        "sectors": dict(sorted(sectors.items(), key=lambda kv: -(kv[1] or 0))),
        "asset_classes": assets,
        "equity_stats": eq,
        "src": "yahoo",
    }


# ── screens ──────────────────────────────────────────────────

SCREENS = {
    "GAINERS": ("day_gainers", "Top gainers"),
    "LOSERS": ("day_losers", "Top losers"),
    "ACTIVE": ("most_actives", "Most active"),
    "SHORTED": ("most_shorted_stocks", "Most shorted"),
    "SMALLCAP": ("small_cap_gainers", "Small-cap gainers"),
    "VALUE": ("undervalued_large_caps", "Undervalued large caps"),
    "GROWTH": ("growth_technology_stocks", "Growth tech"),
}


def screen(code: str, count: int = 30) -> Dict:
    _require()
    code = code.upper()
    if code not in SCREENS:
        code = "GAINERS"
    name, title = SCREENS[code]
    res = yf.screen(name, count=count) or {}
    rows = []
    for q in res.get("quotes", []):
        rows.append({
            "symbol": q.get("symbol", ""),
            "name": q.get("shortName") or q.get("longName", ""),
            "price": _num(q.get("regularMarketPrice")),
            "change_pct": _num(q.get("regularMarketChangePercent")),
            "volume": _num(q.get("regularMarketVolume")),
            "avg_volume": _num(q.get("averageDailyVolume3Month")),
            "market_cap": _num(q.get("marketCap")),
            "pe": _num(q.get("trailingPE")),
            "short_pct_float": _num(q.get("shortPercentOfFloat")),
        })
    return {"code": code, "title": title, "rows": rows,
            "screens": {k: v[1] for k, v in SCREENS.items()}, "src": "yahoo screener"}
