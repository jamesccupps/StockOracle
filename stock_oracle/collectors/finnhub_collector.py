"""
Finnhub Real-Time Collector
============================
Uses Finnhub REST API for REAL real-time stock quotes.
Includes after-hours and pre-market data detection.

Get your free API key at: https://finnhub.io/register
"""
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from stock_oracle.collectors.base import BaseCollector, SignalResult
from stock_oracle.config import FINNHUB_API_KEY

logger = logging.getLogger("stock_oracle")


def get_market_session() -> Dict:
    """
    Determine current US stock market session.
    All times in Eastern Time.

    Returns:
        session: 'pre_market' | 'regular' | 'after_hours' | 'closed'
        is_open: bool (regular hours)
        is_extended: bool (pre or after hours trading available)
    """
    import zoneinfo
    try:
        et = zoneinfo.ZoneInfo("America/New_York")
    except Exception:
        # Fallback: estimate ET as UTC-4 or UTC-5
        et = timezone(timedelta(hours=-4))

    now = datetime.now(et)
    weekday = now.weekday()  # 0=Mon, 6=Sun
    hour = now.hour
    minute = now.minute
    time_val = hour * 100 + minute

    # Weekend
    if weekday >= 5:
        return {"session": "closed", "is_open": False, "is_extended": False,
                "detail": "Weekend"}

    # Pre-market: 4:00 AM - 9:30 AM ET
    if 400 <= time_val < 930:
        return {"session": "pre_market", "is_open": False, "is_extended": True,
                "detail": f"Pre-market ({now.strftime('%I:%M %p')} ET)"}

    # Regular hours: 9:30 AM - 4:00 PM ET
    if 930 <= time_val < 1600:
        return {"session": "regular", "is_open": True, "is_extended": False,
                "detail": f"Market open ({now.strftime('%I:%M %p')} ET)"}

    # After hours: 4:00 PM - 8:00 PM ET
    if 1600 <= time_val < 2000:
        return {"session": "after_hours", "is_open": False, "is_extended": True,
                "detail": f"After hours ({now.strftime('%I:%M %p')} ET)"}

    # Closed: 8:00 PM - 4:00 AM ET
    return {"session": "closed", "is_open": False, "is_extended": False,
            "detail": f"Market closed ({now.strftime('%I:%M %p')} ET)"}


class FinnhubCollector(BaseCollector):
    """
    Real-time stock data from Finnhub.
    Free tier: 60 calls/minute, real-time US stock quotes.

    Provides:
    - Real-time price (including pre-market and after-hours)
    - Market session awareness (regular, pre-market, after-hours, closed)
    - Analyst recommendation consensus
    - Insider transaction tracking
    - After-hours move detection
    """

    QUOTE_URL = "https://finnhub.io/api/v1/quote"
    RECOMMENDATION_URL = "https://finnhub.io/api/v1/stock/recommendation"
    INSIDER_URL = "https://finnhub.io/api/v1/stock/insider-transactions"

    @property
    def name(self) -> str:
        return "finnhub_realtime"

    def collect(self, ticker: str) -> SignalResult:
        # Read key dynamically so GUI settings changes take effect immediately
        import stock_oracle.config as cfg
        api_key = cfg.FINNHUB_API_KEY or FINNHUB_API_KEY
        if not api_key:
            return self._neutral_signal(ticker,
                "No Finnhub API key. Get free key at finnhub.io and add in Settings.")

        cached = self._get_cached(ticker, "finnhub")
        if cached:
            return SignalResult.from_dict(cached)

        # Determine market session
        session = get_market_session()

        # Get real-time quote from Finnhub
        quote = self._get_quote(ticker, api_key)
        if not quote or not quote.get("c"):
            return self._neutral_signal(ticker, "Finnhub quote unavailable")

        # Get analyst recommendations
        recs = self._get_recommendations(ticker, api_key)

        # Get insider transactions
        insider_sig = self._get_insider_signal(ticker, api_key)

        # Base prices from Finnhub
        price = quote["c"]        # Current price (regular session only on free tier)
        open_price = quote["o"]   # Today's open
        prev_close = quote["pc"]  # Previous close
        high = quote["h"]
        low = quote["l"]

        # During extended hours, Finnhub free tier returns stale regular-session price.
        # Use multiple yfinance methods to get actual pre/post market price.
        extended_price = None
        extended_change = None
        if session["session"] in ("pre_market", "after_hours"):
            try:
                import yfinance as yf
                stock = yf.Ticker(ticker)

                # Method 1: .info fields (most specific but often None)
                try:
                    info = stock.info
                    market_state = info.get("marketState", "")

                    if market_state == "PRE" and info.get("preMarketPrice"):
                        extended_price = info["preMarketPrice"]
                        extended_change = info.get("preMarketChangePercent", 0) / 100
                    elif market_state in ("POST", "POSTPOST") and info.get("postMarketPrice"):
                        extended_price = info["postMarketPrice"]
                        extended_change = info.get("postMarketChangePercent", 0) / 100
                    elif info.get("currentPrice"):
                        cp = info["currentPrice"]
                        if abs(cp - price) > 0.01:
                            extended_price = cp

                    # Grab regular session reference prices
                    reg_price = info.get("regularMarketPrice", price)
                    reg_prev = info.get("regularMarketPreviousClose", prev_close)
                    if reg_prev and reg_prev > 0:
                        prev_close = reg_prev
                    if reg_price and reg_price > 0:
                        price = reg_price
                except Exception:
                    pass

                # Method 2: fast_info.last_price (updates during extended hours)
                if not extended_price:
                    try:
                        fi = stock.fast_info
                        last = fi.last_price
                        if last and last > 0 and abs(last - price) > 0.01:
                            extended_price = float(last)
                            if not prev_close:
                                prev_close = fi.previous_close or price
                    except Exception:
                        pass

                # Method 3: history with prepost=True (actual extended hours candles)
                if not extended_price:
                    try:
                        hist = stock.history(period="1d", prepost=True)
                        if hist is not None and not hist.empty:
                            last_close = float(hist["Close"].iloc[-1])
                            if abs(last_close - price) > 0.01:
                                extended_price = last_close
                    except Exception:
                        pass

            except Exception as e:
                logger.debug(f"finnhub: yfinance extended hours lookup failed: {e}")

        # Use extended price if available
        display_price = extended_price if extended_price else price
        ref_price = prev_close if prev_close > 0 else price

        # ── Compute three distinct change metrics ──
        # 1. regular_change: today's regular session move (prev_close → regular close)
        regular_change = (price - ref_price) / ref_price if ref_price > 0 else 0

        # 2. extended_change: AH/PM move from regular close (NOT from prev_close)
        #    This is what the "AH" tag should show — the actual after-hours delta
        extended_session_change = 0.0
        if extended_price and price > 0:
            extended_session_change = (extended_price - price) / price
        elif extended_change is not None and extended_price:
            # yfinance provided the AH % change directly, but it's from prev_close
            # Re-derive from regular close if we can
            if price > 0:
                extended_session_change = (extended_price - price) / price
            else:
                extended_session_change = extended_change

        # 3. daily_change: total change from prev_close (for signal computation)
        daily_change = (display_price - ref_price) / ref_price if ref_price > 0 else 0

        # Legacy: after_hours_move kept for backward compat but now means total AH from prev
        after_hours_move = daily_change if session["session"] in ("after_hours", "pre_market") else 0.0

        # Where in today's range are we? (0 = at low, 1 = at high)
        day_range = high - low if high != low else 1
        range_position = (display_price - low) / day_range if day_range > 0 else 0.5
        range_position = max(0.0, min(1.0, range_position))

        # Signal from price action
        price_signal = daily_change * 2

        # After-hours moves: be more cautious, not more aggressive.
        # Thin volume means moves are less reliable, not more significant.
        # Use extended_session_change (from regular close) not daily_change (from prev)
        if session["session"] in ("after_hours", "pre_market"):
            if abs(extended_session_change) > 0.03:  # Only amplify moves > 3%
                price_signal = extended_session_change * 1.5  # Mild amplification
            elif abs(extended_session_change) > 0.005:
                price_signal = extended_session_change  # Pass through as-is
            else:
                price_signal = regular_change * 0.5  # Mostly use regular change, dampened

        # Signal from analyst recommendations
        rec_signal = recs.get("signal", 0)

        # Combine
        signal = (
            price_signal * 0.4 +
            rec_signal * 0.3 +
            insider_sig * 0.3
        )

        # Reduce confidence during closed hours (stale data)
        confidence = 0.75
        if session["session"] == "regular":
            confidence = 0.85  # Highest during market hours
        elif session["session"] in ("after_hours", "pre_market"):
            confidence = 0.65  # Extended hours = thinner volume
        elif session["session"] == "closed":
            confidence = 0.50  # Weekend/overnight = stale

        # Build details string
        session_tag = {
            "regular": "LIVE",
            "pre_market": "PRE",
            "after_hours": "AH",
            "closed": "CLOSED",
        }.get(session["session"], "?")

        ah_str = ""
        if session["session"] in ("after_hours", "pre_market") and extended_price:
            ah_str = f" | AH: {extended_session_change:+.2%} (from close ${price:.2f})"

        result = SignalResult(
            collector_name=self.name,
            ticker=ticker,
            signal_value=max(-1.0, min(1.0, signal)),
            confidence=confidence,
            raw_data={
                "price": display_price,
                "current_price": display_price,  # For oracle price extraction
                "regular_price": price,
                "extended_price": extended_price,
                "open": open_price,
                "prev_close": prev_close,
                "high": high,
                "low": low,
                "daily_change": round(daily_change, 4),
                "regular_change": round(regular_change, 4),
                "extended_change": round(extended_session_change, 4),
                "range_position": round(range_position, 2),
                "after_hours_move": round(after_hours_move, 4),
                "session": session,
                "analyst_recs": recs,
                "realtime": True,
                "price_source": "yfinance_extended" if extended_price else "finnhub",
            },
            details=(
                f"[{session_tag}] ${display_price:.2f} ({daily_change:+.2%}) | "
                f"range: {range_position:.0%} | "
                f"recs: {rec_signal:+.2f}{ah_str}"
            ),
        )

        self._set_cache(result.to_dict(), ticker, "finnhub")
        return result

    def _get_quote(self, ticker: str, api_key: str) -> Optional[Dict]:
        """Get real-time quote."""
        resp = self._request(self.QUOTE_URL, params={
            "symbol": ticker.upper(),
            "token": api_key,
        })
        if resp and resp.status_code == 200:
            try:
                data = resp.json()
                if data.get("c", 0) > 0:
                    return data
            except Exception:
                pass
        return None

    def _get_recommendations(self, ticker: str, api_key: str) -> Dict:
        """
        Get analyst recommendation trends.
        Returns buy/sell/hold counts and a composite signal.
        """
        resp = self._request(self.RECOMMENDATION_URL, params={
            "symbol": ticker.upper(),
            "token": api_key,
        })
        if resp and resp.status_code == 200:
            try:
                data = resp.json()
                if data and len(data) > 0:
                    latest = data[0]
                    buy = latest.get("buy", 0) + latest.get("strongBuy", 0)
                    sell = latest.get("sell", 0) + latest.get("strongSell", 0)
                    hold = latest.get("hold", 0)
                    total = buy + sell + hold

                    if total > 0:
                        signal = (buy - sell) / total * 0.5
                        return {
                            "buy": buy,
                            "sell": sell,
                            "hold": hold,
                            "signal": round(signal, 3),
                            "period": latest.get("period", ""),
                        }
            except Exception:
                pass
        return {"buy": 0, "sell": 0, "hold": 0, "signal": 0}

    def _get_insider_signal(self, ticker: str, api_key: str) -> float:
        """
        Get recent insider transactions.
        Net buying = bullish, net selling = bearish.
        """
        resp = self._request(self.INSIDER_URL, params={
            "symbol": ticker.upper(),
            "token": api_key,
        })
        if resp and resp.status_code == 200:
            try:
                data = resp.json()
                transactions = data.get("data", [])

                if not transactions:
                    return 0.0

                # Only look at last 90 days
                cutoff = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
                recent = [t for t in transactions
                          if t.get("transactionDate", "") >= cutoff]

                if not recent:
                    return 0.0

                # Count buy vs sell volume
                buy_shares = sum(
                    abs(t.get("share", 0))
                    for t in recent
                    if t.get("transactionCode") in ("P", "A", "M")  # Purchase, Award, Exercise
                )
                sell_shares = sum(
                    abs(t.get("share", 0))
                    for t in recent
                    if t.get("transactionCode") in ("S", "F")  # Sale, Tax withholding
                )

                total = buy_shares + sell_shares
                if total == 0:
                    return 0.0

                return round((buy_shares - sell_shares) / total * 0.4, 3)

            except Exception:
                pass
        return 0.0
