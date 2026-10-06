"""
Data router
===========
One async interface over the providers. Picks Finnhub where it adds
something (real-time quote, company news, peers) and Yahoo everywhere else
or whenever Finnhub is missing, rate-limited or failing.

Every method is cached (see cache.py for the single-flight behaviour).
"""
import asyncio
import logging
from typing import Dict, List, Optional

from stock_oracle.terminal import market, settings, symbols as S
from stock_oracle.terminal.cache import AsyncTTLCache
from stock_oracle.terminal.providers import finnhub, finra, fred, sec, yahoo, yahoo_extra

logger = logging.getLogger("stock_oracle.terminal")

HISTORY_TTL = {"1D": 20, "5D": 120, "1M": 600, "3M": 900}


class ProviderError(Exception):
    pass


class DataRouter:
    def __init__(self):
        self.cache = AsyncTTLCache()

    async def _get(self, key, ttl, fn, *args):
        """Cached call that turns 'not found' into a ProviderError (404)."""
        try:
            return await self.cache.get(key, ttl, fn, *args)
        except LookupError as e:
            raise ProviderError(str(e))

    # ── quotes ───────────────────────────────────────────────

    async def quotes(self, syms: List[str], session: str) -> Dict[str, Dict]:
        """Fresh batch snapshot from Yahoo (used by the quote hub's poll)."""
        if not syms:
            return {}
        return await asyncio.to_thread(yahoo.quotes, syms, session)

    async def quote_realtime(self, sym: str) -> Optional[Dict]:
        """Single real-time quote from Finnhub, or None if unavailable."""
        if not finnhub.available() or not S.to_finnhub(sym):
            return None
        try:
            return await asyncio.to_thread(finnhub.quote, sym)
        except (finnhub.RateLimited, finnhub.FinnhubError) as e:
            logger.debug(f"finnhub quote {sym}: {e}")
            return None

    # ── charts ───────────────────────────────────────────────

    async def history(self, sym: str, rng: str, daily: bool = False) -> Dict:
        rng = rng.upper()
        ttl = HISTORY_TTL.get(rng, 3600) if not daily else 900
        prepost = settings.include_prepost()
        data = await self.cache.get(("hist", sym, rng, prepost, daily), ttl, yahoo.history, sym, rng, prepost, daily)
        if not data["candles"]:
            raise ProviderError(f"No price history for {sym}")
        return data

    # ── company ──────────────────────────────────────────────

    async def profile(self, sym: str) -> Dict:
        prof = await self.cache.get(("profile", sym), 6 * 3600, yahoo.profile, sym)
        if finnhub.available() and S.to_finnhub(sym):
            try:
                fp = await self.cache.get(("fh_profile", sym), 24 * 3600, finnhub.profile, sym)
                if fp:
                    prof = dict(prof)
                    prof["logo"] = fp.get("logo", "")
                    prof["ipo"] = fp.get("ipo", "")
                    prof["website"] = prof.get("website") or fp.get("weburl", "")
                    if not prof.get("industry"):
                        prof["industry"] = fp.get("finnhubIndustry", "")
            except (finnhub.RateLimited, finnhub.FinnhubError):
                pass
        if prof.get("name") == sym and not prof["stats"].get("price") and not prof.get("summary"):
            raise ProviderError(f"Unknown security: {sym}")
        return prof

    async def fundamentals(self, sym: str) -> Dict:
        if not S.is_equity(sym):
            raise ProviderError(f"Fundamentals apply to stocks and ETFs, not {sym}")
        return await self.cache.get(("fa", sym), 12 * 3600, yahoo.fundamentals, sym)

    async def analysts(self, sym: str) -> Dict:
        if not S.is_equity(sym):
            raise ProviderError(f"No analyst coverage data for {sym}")
        data = await self.cache.get(("anr", sym), 6 * 3600, yahoo.analysts, sym)
        if not data["trend"] and finnhub.available():
            try:
                trend = await self.cache.get(("fh_rec", sym), 6 * 3600, finnhub.recommendation_trend, sym)
                data = {**data, "trend": trend, "src": "yahoo+finnhub"}
            except (finnhub.RateLimited, finnhub.FinnhubError):
                pass
        return data

    async def earnings(self, sym: str) -> Dict:
        if not S.is_equity(sym):
            raise ProviderError(f"No earnings data for {sym}")
        data = await self.cache.get(("ee", sym), 6 * 3600, yahoo.earnings, sym)
        if finnhub.available():
            data = dict(data)
            try:
                cal = await self.cache.get(("fh_cal", sym), 6 * 3600, finnhub.earnings_calendar, sym)
                if cal:
                    nxt = dict(data.get("next") or {})
                    if not nxt.get("date"):
                        nxt["date"] = cal["date"]
                    nxt["hour"] = cal.get("hour", "")
                    if nxt.get("eps_avg") is None:
                        nxt["eps_avg"] = cal.get("eps_avg")
                    data["next"] = nxt
                if not data.get("history"):
                    data["history"] = await self.cache.get(("fh_eps", sym), 6 * 3600,
                                                           finnhub.eps_surprises, sym)
            except (finnhub.RateLimited, finnhub.FinnhubError):
                pass
        return data

    async def options(self, sym: str, expiry: Optional[str]) -> Dict:
        if not S.is_equity(sym):
            raise ProviderError(f"No listed options data for {sym}")
        data = await self.cache.get(("omon", sym, expiry or ""), 120, yahoo.options, sym, expiry)
        if not data["expiries"]:
            raise ProviderError(f"No options listed for {sym}")
        return data

    # ── news ─────────────────────────────────────────────────

    async def news(self, sym: str) -> Dict:
        if finnhub.available() and S.to_finnhub(sym):
            try:
                arts = await self.cache.get(("fh_news", sym), 300, finnhub.company_news, sym)
                if arts:
                    return {"symbol": sym, "articles": arts, "src": "finnhub"}
            except (finnhub.RateLimited, finnhub.FinnhubError) as e:
                logger.debug(f"finnhub news {sym}: {e}")
        arts = await self.cache.get(("yh_news", sym), 300, yahoo.news, sym)
        return {"symbol": sym, "articles": arts, "src": "yahoo"}

    async def market_news(self) -> Dict:
        if finnhub.available():
            try:
                arts = await self.cache.get(("fh_mkt_news",), 300, finnhub.market_news)
                if arts:
                    return {"symbol": "", "articles": arts, "src": "finnhub"}
            except (finnhub.RateLimited, finnhub.FinnhubError) as e:
                logger.debug(f"finnhub market news: {e}")
        arts = await self.cache.get(("yh_mkt_news",), 300, yahoo.news, "stock market today")
        return {"symbol": "", "articles": arts, "src": "yahoo"}

    # ── peers / relative value ───────────────────────────────

    async def peers(self, sym: str, watchlist: List[str]) -> Dict:
        if not S.is_equity(sym):
            raise ProviderError(f"No peer group for {sym}")
        names, src = [], "finnhub"
        if finnhub.available():
            try:
                names = await self.cache.get(("fh_peers", sym), 24 * 3600, finnhub.peers, sym)
            except (finnhub.RateLimited, finnhub.FinnhubError):
                names = []
        if not names:
            # No key: compare against watchlist names in the same sector
            src = "watchlist-sector"
            base = await self.profile(sym)
            sector = base.get("sector")
            others = [t for t in watchlist if t != sym and S.is_equity(t)][:30]
            profs = await asyncio.gather(*[self._safe_profile(t) for t in others])
            names = [sym] + [p["symbol"] for p in profs if p and sector and p.get("sector") == sector]
        names = [n for n in dict.fromkeys([sym] + names) if S.is_valid(n)][:12]
        profs = await asyncio.gather(*[self._safe_profile(n) for n in names])
        rows = []
        for p in profs:
            if not p:
                continue
            st = p["stats"]
            rows.append({
                "symbol": p["symbol"], "name": p["name"], "price": st.get("price"),
                "market_cap": st.get("market_cap"), "pe_ttm": st.get("pe_ttm"),
                "pe_fwd": st.get("pe_fwd"), "ps_ttm": st.get("ps_ttm"), "pb": st.get("pb"),
                "div_yield_pct": st.get("div_yield_pct"), "beta": st.get("beta"),
            })
        fa = await asyncio.gather(*[self._safe_fa(r["symbol"]) for r in rows])
        for r, f in zip(rows, fa):
            ratios = (f or {}).get("ratios", {})
            r["revenue_growth"] = ratios.get("revenue_growth")
            r["gross_margin"] = ratios.get("gross_margin")
            r["profit_margin"] = ratios.get("profit_margin")
        return {"symbol": sym, "rows": rows, "src": src}

    async def _safe_profile(self, sym):
        try:
            return await self.profile(sym)
        except Exception:
            return None

    async def _safe_fa(self, sym):
        try:
            return await self.cache.get(("fa", sym), 12 * 3600, yahoo.fundamentals, sym)
        except Exception:
            return None

    # ── market monitors ──────────────────────────────────────

    async def market_group(self, code: str) -> Dict:
        code = market.resolve(code)
        if code not in market.GROUPS:
            raise ProviderError(f"Unknown market group {code}")
        title, members = market.GROUPS[code]
        syms = [s for s, _ in members]
        data = await self.cache.get(("mkt", code), 60, yahoo.market_batch, syms)
        rows = []
        for s, label in members:
            d = data.get(s)
            if d:
                rows.append({**d, "label": label, "is_yield": s in market.YIELD_SYMBOLS})
        return {"code": code, "title": title, "rows": rows, "src": "yahoo"}

    # ── ownership, estimates, corporate actions ─────────────

    def _equity_only(self, sym: str, what: str):
        if not S.is_equity(sym):
            raise ProviderError(f"{what} applies to stocks and ETFs, not {sym}")

    async def estimates(self, sym: str) -> Dict:
        self._equity_only(sym, "Estimates")
        return await self._get(("eeo", sym), 6 * 3600, yahoo_extra.estimates, sym)

    async def holders(self, sym: str) -> Dict:
        self._equity_only(sym, "Holder data")
        return await self._get(("hds", sym), 12 * 3600, yahoo_extra.holders, sym)

    async def insiders(self, sym: str) -> Dict:
        self._equity_only(sym, "Insider data")
        return await self._get(("ins", sym), 3 * 3600, yahoo_extra.insiders, sym)

    async def dividends(self, sym: str) -> Dict:
        self._equity_only(sym, "Dividend history")
        return await self._get(("dvd", sym), 12 * 3600, yahoo_extra.dividends, sym)

    async def short_interest(self, sym: str) -> Dict:
        self._equity_only(sym, "Short interest")
        si, daily = await asyncio.gather(
            self._get(("si", sym), 6 * 3600, yahoo_extra.short_interest, sym),
            self._get(("finra", sym), 3 * 3600, finra.short_volume, sym),
            return_exceptions=True)
        if isinstance(si, Exception) and isinstance(daily, Exception):
            raise ProviderError(f"No short data for {sym}")
        return {"symbol": sym,
                "interest": None if isinstance(si, Exception) else si,
                "daily_volume": None if isinstance(daily, Exception) else daily}

    async def etf(self, sym: str) -> Dict:
        self._equity_only(sym, "Fund holdings")
        return await self._get(("etf", sym), 24 * 3600, yahoo_extra.etf, sym)

    async def filings(self, sym: str) -> Dict:
        self._equity_only(sym, "SEC filings")
        data = await self._get(("cf", sym), 900, sec.filings, sym)
        return {**data, "ua_placeholder": sec.ua_is_placeholder()}

    async def screen(self, code: str) -> Dict:
        return await self._get(("most", code.upper()), 120, yahoo_extra.screen, code)

    # ── macro (FRED) ─────────────────────────────────────────

    async def macro(self) -> Dict:
        return await self._get(("eco",), 3 * 3600, fred.macro)

    async def curve(self) -> Dict:
        return await self._get(("curve",), 3600, fred.curve)

    # ── calendars ────────────────────────────────────────────

    async def earnings_calendar(self, watchlist: List[str]) -> Dict:
        from datetime import date
        eq = [t for t in watchlist if S.is_equity(t)]
        rows, src = [], "yahoo"
        if finnhub.available():
            try:
                cal = await self.cache.get(("fh_cal_all",), 6 * 3600, finnhub.earnings_calendar_range)
                wanted = {S.to_finnhub(t): t for t in eq}
                for r in cal:
                    t = wanted.get(r.get("symbol"))
                    if t:
                        rows.append({"symbol": t, "date": r.get("date", ""),
                                     "hour": {"bmo": "before open", "amc": "after close",
                                              "dmh": "during hours"}.get(r.get("hour", ""), ""),
                                     "eps_avg": r.get("epsEstimate"), "rev_avg": r.get("revenueEstimate")})
                src = "finnhub"
            except (finnhub.RateLimited, finnhub.FinnhubError):
                rows = []
        if not rows:
            sem = asyncio.Semaphore(6)

            async def one(t):
                async with sem:
                    try:
                        e = await self.earnings(t)
                        n = e.get("next") or {}
                        if n.get("date"):
                            return {"symbol": t, "date": n["date"][:10], "hour": n.get("hour", ""),
                                    "eps_avg": n.get("eps_avg"), "rev_avg": n.get("rev_avg")}
                    except Exception:
                        return None
            rows = [r for r in await asyncio.gather(*[one(t) for t in eq]) if r]
            src = "yahoo"
        today = date.today()
        for r in rows:
            try:
                r["days"] = (date.fromisoformat(r["date"][:10]) - today).days
            except ValueError:
                r["days"] = None
        rows = sorted((r for r in rows if r["days"] is None or r["days"] >= 0),
                      key=lambda r: (r["days"] is None, r["days"] or 0, r["symbol"]))
        no_date = sorted(set(eq) - {r["symbol"] for r in rows})
        return {"rows": rows, "no_date": no_date, "src": src}
