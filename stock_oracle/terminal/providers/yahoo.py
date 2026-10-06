"""
Yahoo Finance provider (via yfinance) — no key required
=======================================================
Covers every asset type the terminal shows: quotes, intraday/daily history,
company profile, fundamentals, analyst data, earnings, options chains, news
and the cross-asset market groups.

All functions are blocking; the server calls them from worker threads.
yf.download() keeps module-level state and is not safe to run concurrently,
so every call to it goes through _DOWNLOAD_LOCK.
"""
import logging
import math
import threading
from datetime import datetime, time as dtime, timezone
from typing import Dict, List, Optional

from stock_oracle.terminal import symbols as S

logger = logging.getLogger("stock_oracle.terminal")

try:
    import yfinance as yf
    import pandas as pd
    logging.getLogger("yfinance").setLevel(logging.CRITICAL)
    HAS_YF = True
except ImportError:  # pragma: no cover
    HAS_YF = False

_DOWNLOAD_LOCK = threading.Lock()

# range -> (yfinance period, interval)
RANGES = {
    "1D": ("1d", "1m"),
    "5D": ("5d", "5m"),
    "1M": ("1mo", "30m"),
    "3M": ("3mo", "1h"),
    "6M": ("6mo", "1d"),
    "YTD": ("ytd", "1d"),
    "1Y": ("1y", "1d"),
    "2Y": ("2y", "1d"),
    "5Y": ("5y", "1wk"),
    "10Y": ("10y", "1wk"),
    "MAX": ("max", "1mo"),
}


# ── helpers ──────────────────────────────────────────────────

def _num(v) -> Optional[float]:
    """float or None (handles NaN, None, strings)."""
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(f) or math.isinf(f) else f


def _int(v) -> Optional[int]:
    f = _num(v)
    return int(f) if f is not None else None


def _require():
    if not HAS_YF:
        raise RuntimeError("yfinance is not installed (pip install yfinance)")


def _download(tickers, **kw):
    _require()
    kw.setdefault("progress", False)
    kw.setdefault("auto_adjust", False)
    kw.setdefault("group_by", "ticker")
    kw.setdefault("threads", True)
    with _DOWNLOAD_LOCK:
        return yf.download(tickers, **kw)


def _frame(df, sym):
    """Pull one symbol's OHLCV frame out of a yf.download result."""
    if df is None or getattr(df, "empty", True):
        return None
    if isinstance(df.columns, pd.MultiIndex):
        lv0 = df.columns.get_level_values(0)
        if sym in lv0:
            return df[sym]
        lv1 = df.columns.get_level_values(1)
        if sym in lv1:
            return df.xs(sym, axis=1, level=1)
        return None
    return df


def _wall_clock_epoch(ts) -> int:
    """Epoch seconds shifted to the exchange's local wall clock.
    The chart library labels times in UTC, so shifting makes an intraday
    NYSE chart read 09:30-16:00 instead of 13:30-20:00."""
    off = ts.utcoffset()
    return int(ts.timestamp()) + (int(off.total_seconds()) if off else 0)


# ── quotes ───────────────────────────────────────────────────

def quotes(symbols: List[str], session: str = "regular") -> Dict[str, Dict]:
    """Batch snapshot quotes. Extended-hours prices are added for equities
    during pre-market and after-hours."""
    symbols = [s for s in dict.fromkeys(symbols) if S.is_valid(s)]
    if not symbols:
        return {}
    df = _download(symbols, period="5d", interval="1d")
    out: Dict[str, Dict] = {}
    now = datetime.now(timezone.utc).timestamp()
    for sym in symbols:
        f = _frame(df, sym)
        if f is None or f.empty or "Close" not in f:
            continue
        f = f.dropna(subset=["Close"])
        if f.empty:
            continue
        last = f.iloc[-1]
        prev = f.iloc[-2] if len(f) >= 2 else None
        price = _num(last["Close"])
        prev_close = _num(prev["Close"]) if prev is not None else None
        chg = (price - prev_close) if (price is not None and prev_close) else None
        out[sym] = {
            "symbol": sym,
            "price": price,
            "prev_close": prev_close,
            "open": _num(last.get("Open")),
            "high": _num(last.get("High")),
            "low": _num(last.get("Low")),
            "volume": _int(last.get("Volume")),
            "change": chg,
            "change_pct": (chg / prev_close * 100) if (chg is not None and prev_close) else None,
            "date": str(f.index[-1].date()) if hasattr(f.index[-1], "date") else "",
            "ext_price": None,
            "ext_change_pct": None,
            "ext_session": None,
            "updated": now,
            "src": "yahoo",
        }

    if session in ("pre_market", "after_hours"):
        eq = [s for s in out if S.is_equity(s)]
        if eq:
            try:
                _add_extended(out, eq, session)
            except Exception as e:
                logger.debug(f"yahoo extended-hours lookup failed: {e}")
    return out


def _add_extended(out: Dict[str, Dict], eq: List[str], session: str):
    df = _download(eq, period="1d", interval="1m", prepost=True)
    for sym in eq:
        f = _frame(df, sym)
        if f is None or f.empty:
            continue
        f = f.dropna(subset=["Close"])
        if f.empty:
            continue
        ts = f.index[-1]
        try:
            local = ts.tz_convert("America/New_York").time()
        except Exception:
            continue
        in_ext = local >= dtime(16, 0) if session == "after_hours" else local < dtime(9, 30)
        if not in_ext:
            continue
        ext = _num(f["Close"].iloc[-1])
        q = out[sym]
        # Extended moves are measured from the last regular-session close.
        # Pre-market: that's the latest daily bar, unless Yahoo has already
        # opened today's bar, in which case it's the bar before.
        today = datetime.now(timezone.utc).astimezone(ts.tz).date().isoformat()
        base = q["prev_close"] if (session == "pre_market" and q.get("date") == today) else q["price"]
        q["ext_price"] = ext
        q["ext_session"] = "post" if session == "after_hours" else "pre"
        q["ext_change_pct"] = ((ext - base) / base * 100) if (ext and base) else None


# ── history ──────────────────────────────────────────────────

def history(sym: str, rng: str = "6M", prepost: bool = False, daily: bool = False) -> Dict:
    """OHLCV bars. `daily` forces end-of-day bars (weekly beyond two years),
    for tables like HP that want one row per session."""
    _require()
    rng = rng.upper()
    if rng not in RANGES:
        rng = "6M"
    period, interval = RANGES[rng]
    if daily:
        interval = "1wk" if rng in ("5Y", "10Y", "MAX") else "1d"
    intraday = interval.endswith(("m", "h"))
    t = yf.Ticker(sym)
    hist = t.history(period=period, interval=interval, auto_adjust=False,
                     prepost=prepost and intraday, actions=False)
    candles = []
    if hist is not None and not hist.empty:
        hist = hist.dropna(subset=["Open", "High", "Low", "Close"])
        for ts, row in hist.iterrows():
            candles.append({
                "time": _wall_clock_epoch(ts) if intraday else ts.strftime("%Y-%m-%d"),
                "open": round(float(row["Open"]), 6),
                "high": round(float(row["High"]), 6),
                "low": round(float(row["Low"]), 6),
                "close": round(float(row["Close"]), 6),
                "volume": int(row["Volume"]) if not math.isnan(row["Volume"]) else 0,
            })
    prev_close = None
    if rng == "1D":
        try:
            prev_close = _num(t.fast_info.previous_close)
        except Exception:
            prev_close = None
    return {
        "symbol": sym,
        "range": rng,
        "interval": interval,
        "intraday": intraday,
        "prev_close": prev_close,
        "candles": candles,
        "src": "yahoo",
    }


# ── profile / description ───────────────────────────────────

def _info(sym: str) -> Dict:
    _require()
    try:
        return yf.Ticker(sym).info or {}
    except Exception as e:
        logger.debug(f"yahoo info failed for {sym}: {e}")
        return {}


def profile(sym: str) -> Dict:
    i = _info(sym)
    price = _num(i.get("regularMarketPrice") or i.get("currentPrice") or i.get("navPrice"))
    div_rate = _num(i.get("dividendRate") or i.get("trailingAnnualDividendRate"))
    div_yield = (div_rate / price * 100) if (div_rate and price) else None
    if div_yield is None and _num(i.get("yield")) is not None:   # funds report yield directly
        div_yield = _num(i.get("yield")) * 100
    officers = [
        {"name": o.get("name", ""), "title": o.get("title", "")}
        for o in (i.get("companyOfficers") or [])[:5]
    ]
    loc = ", ".join(x for x in [i.get("city"), i.get("state"), i.get("country")] if x)
    return {
        "symbol": sym,
        "name": i.get("longName") or i.get("shortName") or sym,
        "type": i.get("quoteType", ""),
        "exchange": i.get("fullExchangeName") or i.get("exchange", ""),
        "currency": i.get("currency", "USD"),
        "sector": i.get("sector") or i.get("category") or "",
        "industry": i.get("industry") or i.get("fundFamily") or "",
        "location": loc,
        "website": i.get("website", ""),
        "employees": _int(i.get("fullTimeEmployees")),
        "summary": i.get("longBusinessSummary") or i.get("description") or "",
        "officers": officers,
        "stats": {
            "price": price,
            "market_cap": _num(i.get("marketCap")),
            "enterprise_value": _num(i.get("enterpriseValue")),
            "total_assets": _num(i.get("totalAssets")),
            "shares_out": _num(i.get("sharesOutstanding")),
            "float": _num(i.get("floatShares")),
            "beta": _num(i.get("beta") or i.get("beta3Year")),
            "pe_ttm": _num(i.get("trailingPE")),
            "pe_fwd": _num(i.get("forwardPE")),
            "peg": _num(i.get("trailingPegRatio") or i.get("pegRatio")),
            "ps_ttm": _num(i.get("priceToSalesTrailing12Months")),
            "pb": _num(i.get("priceToBook")),
            "eps_ttm": _num(i.get("trailingEps")),
            "eps_fwd": _num(i.get("forwardEps")),
            "div_rate": div_rate,
            "div_yield_pct": div_yield,
            "payout_ratio": _num(i.get("payoutRatio")),
            "expense_ratio": _num(i.get("netExpenseRatio") or i.get("annualReportExpenseRatio")),
            "hi_52w": _num(i.get("fiftyTwoWeekHigh")),
            "lo_52w": _num(i.get("fiftyTwoWeekLow")),
            "ma_50": _num(i.get("fiftyDayAverage")),
            "ma_200": _num(i.get("twoHundredDayAverage")),
            "avg_vol_3m": _num(i.get("averageVolume")),
            "avg_vol_10d": _num(i.get("averageDailyVolume10Day")),
            "short_pct_float": _num(i.get("shortPercentOfFloat")),
            "short_ratio": _num(i.get("shortRatio")),
            "target_mean": _num(i.get("targetMeanPrice")),
            "target_high": _num(i.get("targetHighPrice")),
            "target_low": _num(i.get("targetLowPrice")),
            "rec_key": i.get("recommendationKey", ""),
            "num_analysts": _int(i.get("numberOfAnalystOpinions")),
        },
        "src": "yahoo",
    }


# ── fundamentals ─────────────────────────────────────────────

_IS_ROWS = [("Total Revenue", "Revenue"), ("Gross Profit", "Gross Profit"),
            ("Operating Income", "Operating Income"), ("EBITDA", "EBITDA"),
            ("Net Income", "Net Income"), ("Diluted EPS", "Diluted EPS")]
_CF_ROWS = [("Operating Cash Flow", "Operating Cash Flow"), ("Free Cash Flow", "Free Cash Flow")]
_BS_ROWS = [("Cash And Cash Equivalents", "Cash"), ("Total Debt", "Total Debt"),
            ("Stockholders Equity", "Shareholders' Equity")]


def _statement_block(frames, max_periods: int) -> Dict:
    """Merge income/cash-flow/balance frames into one period-aligned table."""
    periods: List = []
    for df, _ in frames:
        if df is not None and not df.empty:
            for c in df.columns:
                if c not in periods:
                    periods.append(c)
    periods = sorted(periods, reverse=True)[:max_periods]
    rows = []
    for df, spec in frames:
        if df is None or df.empty:
            continue
        for key, label in spec:
            if key in df.index:
                vals = [(_num(df.at[key, p]) if p in df.columns else None) for p in periods]
                if any(v is not None for v in vals):
                    rows.append({"label": label, "values": vals})
    labels = [p.strftime("%Y-%m-%d") if hasattr(p, "strftime") else str(p) for p in periods]
    return {"periods": labels, "rows": rows}


def fundamentals(sym: str) -> Dict:
    _require()
    t = yf.Ticker(sym)
    i = _info(sym)

    def safe(attr):
        try:
            return getattr(t, attr)
        except Exception:
            return None

    annual = _statement_block([(safe("income_stmt"), _IS_ROWS),
                               (safe("cashflow"), _CF_ROWS),
                               (safe("balance_sheet"), _BS_ROWS)], 5)
    quarterly = _statement_block([(safe("quarterly_income_stmt"), _IS_ROWS),
                                  (safe("quarterly_cashflow"), _CF_ROWS),
                                  (safe("quarterly_balance_sheet"), _BS_ROWS)], 6)
    ratios = {
        "revenue_ttm": _num(i.get("totalRevenue")),
        "ebitda_ttm": _num(i.get("ebitda")),
        "gross_margin": _num(i.get("grossMargins")),
        "operating_margin": _num(i.get("operatingMargins")),
        "profit_margin": _num(i.get("profitMargins")),
        "roe": _num(i.get("returnOnEquity")),
        "roa": _num(i.get("returnOnAssets")),
        "revenue_growth": _num(i.get("revenueGrowth")),
        "earnings_growth": _num(i.get("earningsGrowth")),
        "debt_to_equity": _num(i.get("debtToEquity")),
        "current_ratio": _num(i.get("currentRatio")),
        "quick_ratio": _num(i.get("quickRatio")),
        "total_cash": _num(i.get("totalCash")),
        "total_debt": _num(i.get("totalDebt")),
        "free_cashflow": _num(i.get("freeCashflow")),
        "operating_cashflow": _num(i.get("operatingCashflow")),
        "pe_ttm": _num(i.get("trailingPE")),
        "pe_fwd": _num(i.get("forwardPE")),
        "ev_ebitda": _num(i.get("enterpriseToEbitda")),
        "ev_revenue": _num(i.get("enterpriseToRevenue")),
    }
    return {"symbol": sym, "currency": i.get("financialCurrency") or i.get("currency", "USD"),
            "ratios": ratios, "annual": annual, "quarterly": quarterly, "src": "yahoo"}


# ── analysts ─────────────────────────────────────────────────

def analysts(sym: str) -> Dict:
    _require()
    t = yf.Ticker(sym)
    i = _info(sym)
    trend = []
    try:
        rec = t.recommendations
        if rec is not None and not rec.empty:
            for r in rec.to_dict("records"):
                trend.append({
                    "period": str(r.get("period", "")),
                    "strong_buy": _int(r.get("strongBuy")) or 0,
                    "buy": _int(r.get("buy")) or 0,
                    "hold": _int(r.get("hold")) or 0,
                    "sell": _int(r.get("sell")) or 0,
                    "strong_sell": _int(r.get("strongSell")) or 0,
                })
    except Exception as e:
        logger.debug(f"yahoo recommendations failed for {sym}: {e}")
    actions = []
    try:
        ud = t.upgrades_downgrades
        if ud is not None and not ud.empty:
            for r in ud.reset_index().head(25).to_dict("records"):
                gd = r.get("GradeDate")
                actions.append({
                    "date": gd.strftime("%Y-%m-%d") if hasattr(gd, "strftime") else str(gd)[:10],
                    "firm": r.get("Firm", ""),
                    "action": r.get("Action", ""),
                    "from": r.get("FromGrade", ""),
                    "to": r.get("ToGrade", ""),
                    "target": _num(r.get("currentPriceTarget")),
                    "prior_target": _num(r.get("priorPriceTarget")),
                })
    except Exception as e:
        logger.debug(f"yahoo upgrades/downgrades failed for {sym}: {e}")
    return {
        "symbol": sym,
        "trend": trend,
        "actions": actions,
        "targets": {
            "mean": _num(i.get("targetMeanPrice")),
            "median": _num(i.get("targetMedianPrice")),
            "high": _num(i.get("targetHighPrice")),
            "low": _num(i.get("targetLowPrice")),
            "price": _num(i.get("regularMarketPrice") or i.get("currentPrice")),
            "rec_key": i.get("recommendationKey", ""),
            "num_analysts": _int(i.get("numberOfAnalystOpinions")),
        },
        "src": "yahoo",
    }


# ── earnings ─────────────────────────────────────────────────

def earnings(sym: str) -> Dict:
    _require()
    t = yf.Ticker(sym)
    nxt = {}
    try:
        cal = t.calendar or {}
        dates = cal.get("Earnings Date") or []
        nxt = {
            "date": str(dates[0]) if dates else "",
            "eps_avg": _num(cal.get("Earnings Average")),
            "eps_low": _num(cal.get("Earnings Low")),
            "eps_high": _num(cal.get("Earnings High")),
            "rev_avg": _num(cal.get("Revenue Average")),
            "rev_low": _num(cal.get("Revenue Low")),
            "rev_high": _num(cal.get("Revenue High")),
            "ex_dividend": str(cal.get("Ex-Dividend Date") or ""),
            "dividend_date": str(cal.get("Dividend Date") or ""),
        }
    except Exception as e:
        logger.debug(f"yahoo calendar failed for {sym}: {e}")
    history_rows = []
    try:
        ed = t.earnings_dates
        if ed is not None and not ed.empty:
            for ts, r in ed.head(12).iterrows():
                history_rows.append({
                    "date": ts.strftime("%Y-%m-%d"),
                    "eps_est": _num(r.get("EPS Estimate")),
                    "eps_actual": _num(r.get("Reported EPS")),
                    "surprise_pct": _num(r.get("Surprise(%)")),
                })
    except Exception as e:
        logger.debug(f"yahoo earnings_dates failed for {sym}: {e}")
    return {"symbol": sym, "next": nxt, "history": history_rows, "src": "yahoo"}


# ── options ──────────────────────────────────────────────────

def options(sym: str, expiry: Optional[str] = None, width: int = 15) -> Dict:
    """Option chain for one expiry, trimmed to `width` strikes either side of spot."""
    _require()
    t = yf.Ticker(sym)
    expiries = list(t.options or [])
    if not expiries:
        return {"symbol": sym, "expiries": [], "expiry": None, "spot": None,
                "calls": [], "puts": [], "totals": {}, "src": "yahoo"}
    if expiry not in expiries:
        expiry = expiries[0]
    chain = t.option_chain(expiry)
    try:
        spot = _num(t.fast_info.last_price)
    except Exception:
        spot = None

    def rows(df):
        if df is None or df.empty:
            return []
        recs = []
        for r in df.to_dict("records"):
            recs.append({
                "strike": _num(r.get("strike")),
                "last": _num(r.get("lastPrice")),
                "bid": _num(r.get("bid")),
                "ask": _num(r.get("ask")),
                "change_pct": _num(r.get("percentChange")),
                "volume": _int(r.get("volume")) or 0,
                "oi": _int(r.get("openInterest")) or 0,
                "iv": _num(r.get("impliedVolatility")),
                "itm": bool(r.get("inTheMoney")),
            })
        return recs

    calls, puts = rows(chain.calls), rows(chain.puts)
    totals = {
        "call_volume": sum(r["volume"] for r in calls),
        "put_volume": sum(r["volume"] for r in puts),
        "call_oi": sum(r["oi"] for r in calls),
        "put_oi": sum(r["oi"] for r in puts),
    }
    totals["pc_volume"] = (totals["put_volume"] / totals["call_volume"]) if totals["call_volume"] else None
    totals["pc_oi"] = (totals["put_oi"] / totals["call_oi"]) if totals["call_oi"] else None

    if spot:
        strikes = sorted({r["strike"] for r in calls + puts if r["strike"] is not None})
        if strikes:
            atm = min(range(len(strikes)), key=lambda k: abs(strikes[k] - spot))
            keep = set(strikes[max(0, atm - width): atm + width + 1])
            calls = [r for r in calls if r["strike"] in keep]
            puts = [r for r in puts if r["strike"] in keep]
    return {"symbol": sym, "expiries": expiries, "expiry": expiry, "spot": spot,
            "calls": calls, "puts": puts, "totals": totals, "src": "yahoo"}


# ── news (fallback when there's no Finnhub key) ──────────────

def search(query: str, quotes: int = 10, news_count: int = 0):
    """yf.Search across versions (the quotes-count argument was renamed)."""
    _require()
    try:
        return yf.Search(query, max_results=max(quotes, 1), news_count=news_count)
    except TypeError:
        return yf.Search(query, quotes_count=quotes, news_count=news_count)


def symbol_search(query: str) -> List[Dict]:
    res = search(query, quotes=10).quotes or []
    return [{"symbol": r.get("symbol", ""), "name": r.get("shortname") or r.get("longname", ""),
             "type": r.get("quoteType", ""), "exchange": r.get("exchDisp", "")}
            for r in res if r.get("symbol")]


def news(query: str, count: int = 25) -> List[Dict]:
    _require()
    try:
        items = search(query, quotes=1, news_count=count).news or []
    except Exception as e:
        logger.debug(f"yahoo news failed for {query}: {e}")
        return []
    out = []
    for n in items:
        ts = _int(n.get("providerPublishTime")) or 0
        out.append({
            "headline": n.get("title", ""),
            "summary": "",
            "source": n.get("publisher", ""),
            "url": n.get("link", ""),
            "timestamp": ts,
            "related": n.get("relatedTickers") or [],
        })
    out.sort(key=lambda a: a["timestamp"], reverse=True)
    return out


# ── market groups (indices, sectors, rates, FX, commodities, crypto) ──

def market_batch(symbols: List[str]) -> Dict[str, Dict]:
    df = _download(symbols, period="3mo", interval="1d")
    out = {}
    for sym in symbols:
        f = _frame(df, sym)
        if f is None or f.empty:
            continue
        closes = f["Close"].dropna()
        if closes.empty:
            continue
        vals = [float(v) for v in closes.tolist()]
        last = vals[-1]
        prev = vals[-2] if len(vals) >= 2 else None

        def back(n):
            return vals[-1 - n] if len(vals) > n else None

        def pct(ref):
            return ((last - ref) / ref * 100) if ref else None

        out[sym] = {
            "symbol": sym,
            "price": last,
            "change": (last - prev) if prev else None,
            "change_pct": pct(prev),
            "chg_5d_pct": pct(back(5)),
            "chg_1m_pct": pct(back(21)),
            "spark": [round(v, 6) for v in vals[-30:]],
            "date": str(closes.index[-1].date()) if hasattr(closes.index[-1], "date") else "",
        }
    return out
