"""
New Creative Indicators
========================
Four additional signal collectors that add real analytical value:

1. Fear & Greed Proxy — VIX + put/call + breadth + safe haven flows
2. Dividend vs Treasury — Yield gap analysis for income stocks  
3. Momentum Quality — Rate-of-change consistency + trend strength
4. Insider Buy/Sell Ratio — SEC Form 4 buy vs sell activity
"""
import logging
import numpy as np
from datetime import datetime, timezone
from typing import Dict, List, Optional

from stock_oracle.collectors.base import BaseCollector, SignalResult
from stock_oracle.config import SEC_USER_AGENT

logger = logging.getLogger("stock_oracle")


class FearGreedProxyCollector(BaseCollector):
    """
    Market Fear & Greed proxy using:
    - VIX level and direction (fear gauge)
    - Market breadth (advancing vs declining via S&P ETF momentum)
    - Safe haven flows (gold vs S&P ratio)
    - Junk bond spread proxy (HYG vs TLT ratio)
    
    This is a MARKET-LEVEL signal — same for all stocks.
    It measures whether the environment favors risk-on (bullish) or risk-off (bearish).
    """

    @property
    def name(self):
        return "fear_greed_proxy"

    def collect(self, ticker: str) -> SignalResult:
        cached = self._get_cached(ticker, "fear_greed")
        if cached:
            return SignalResult.from_dict(cached)

        try:
            import yfinance as yf
        except ImportError:
            return self._neutral_signal(ticker, "yfinance not available")

        signals = []
        raw = {}

        # 1. VIX — below 15 = complacent/bullish, above 25 = fearful/bearish
        try:
            vix = yf.Ticker("^VIX").history(period="5d")
            if vix is not None and not vix.empty:
                vix_close = float(vix["Close"].iloc[-1])
                raw["vix"] = round(vix_close, 2)

                if vix_close < 15:
                    signals.append(0.3)   # Low fear = bullish
                elif vix_close < 20:
                    signals.append(0.1)
                elif vix_close < 25:
                    signals.append(-0.1)
                elif vix_close < 35:
                    signals.append(-0.3)  # High fear = bearish
                else:
                    signals.append(-0.5)  # Extreme fear

                # VIX direction (5-day)
                if len(vix) >= 5:
                    vix_5d_ago = float(vix["Close"].iloc[0])
                    vix_change = (vix_close - vix_5d_ago) / vix_5d_ago
                    raw["vix_5d_change"] = round(vix_change, 4)
                    if vix_change > 0.15:
                        signals.append(-0.2)  # VIX rising fast = fear increasing
                    elif vix_change < -0.15:
                        signals.append(0.2)   # VIX falling fast = fear decreasing
        except Exception:
            pass

        # 2. Market breadth proxy — RSP (equal weight S&P) vs SPY (cap weight)
        # If equal weight outperforms, breadth is good (bullish)
        try:
            spy = yf.Ticker("SPY").history(period="20d")
            rsp = yf.Ticker("RSP").history(period="20d")
            if (spy is not None and not spy.empty and len(spy) >= 10 and
                rsp is not None and not rsp.empty and len(rsp) >= 10):

                spy_ret = (float(spy["Close"].iloc[-1]) - float(spy["Close"].iloc[-10])) / float(spy["Close"].iloc[-10])
                rsp_ret = (float(rsp["Close"].iloc[-1]) - float(rsp["Close"].iloc[-10])) / float(rsp["Close"].iloc[-10])
                breadth = rsp_ret - spy_ret  # Positive = broad participation
                raw["breadth_spread"] = round(breadth, 4)
                signals.append(max(-0.3, min(0.3, breadth * 10)))
        except Exception:
            pass

        # 3. Safe haven flow — Gold (GLD) vs S&P (SPY) relative strength
        try:
            gld = yf.Ticker("GLD").history(period="20d")
            if (gld is not None and not gld.empty and len(gld) >= 10 and
                spy is not None and not spy.empty and len(spy) >= 10):

                gld_ret = (float(gld["Close"].iloc[-1]) - float(gld["Close"].iloc[-10])) / float(gld["Close"].iloc[-10])
                spy_ret_10 = (float(spy["Close"].iloc[-1]) - float(spy["Close"].iloc[-10])) / float(spy["Close"].iloc[-10])
                haven_flow = gld_ret - spy_ret_10  # Positive = money flowing to gold (fear)
                raw["safe_haven_flow"] = round(haven_flow, 4)
                signals.append(max(-0.3, min(0.3, -haven_flow * 8)))  # Invert: gold up = bearish for stocks
        except Exception:
            pass

        if not signals:
            return self._neutral_signal(ticker, "Could not compute fear/greed proxy")

        avg_signal = sum(signals) / len(signals)
        signal = max(-1.0, min(1.0, avg_signal))

        # Classify
        if signal > 0.15:
            mood = "Greed (risk-on)"
        elif signal > 0.05:
            mood = "Mild optimism"
        elif signal > -0.05:
            mood = "Neutral"
        elif signal > -0.15:
            mood = "Mild fear"
        else:
            mood = "Fear (risk-off)"

        raw["mood"] = mood
        raw["components"] = len(signals)

        result = SignalResult(
            collector_name=self.name,
            ticker=ticker,
            signal_value=signal,
            confidence=min(0.55, len(signals) * 0.15),  # More components = more confident
            raw_data=raw,
            details=f"VIX={raw.get('vix', '?')} | {mood}",
        )
        self._set_cache(result.to_dict(), ticker, "fear_greed")
        return result


class DividendVsTreasuryCollector(BaseCollector):
    """
    Dividend yield vs 10-year Treasury yield.
    
    When a stock's dividend yield is significantly higher than Treasury yields,
    it's potentially undervalued for income investors (bullish).
    When Treasury yields are much higher, money rotates out of equities (bearish).
    
    Most useful for: dividend stocks, REITs, utilities, ETFs like SCHD/VYM/NOBL.
    For growth stocks with no dividend, returns neutral.
    """

    @property
    def name(self):
        return "dividend_vs_treasury"

    def collect(self, ticker: str) -> SignalResult:
        cached = self._get_cached(ticker, "div_treasury")
        if cached:
            return SignalResult.from_dict(cached)

        try:
            import yfinance as yf
        except ImportError:
            return self._neutral_signal(ticker, "yfinance not available")

        raw = {}

        # Get stock's dividend yield
        try:
            stock = yf.Ticker(ticker)
            info = stock.info or {}

            # yfinance is inconsistent: dividendYield can be 0.42 (meaning 0.42%)
            # or trailingAnnualDividendYield is 0.0041 (meaning 0.41%)
            # Prefer trailingAnnualDividendYield (consistent ratio format)
            div_yield = info.get("trailingAnnualDividendYield") or 0
            if not div_yield:
                raw_dy = info.get("dividendYield") or 0
                # If > 0.20, it's almost certainly in percent (e.g., 3.3 = 3.3%, 0.42 = 0.42%)
                # No real stock has a 20%+ dividend yield expressed as a ratio
                if raw_dy > 0.20:
                    div_yield = raw_dy / 100.0
                else:
                    div_yield = raw_dy

            raw["dividend_yield"] = round(div_yield * 100, 2) if div_yield else 0
        except Exception:
            return self._neutral_signal(ticker, "Could not get dividend data")

        if not div_yield or div_yield < 0.005:
            return SignalResult(
                collector_name=self.name,
                ticker=ticker,
                signal_value=0.0,
                confidence=0.0,
                raw_data={"dividend_yield": 0, "note": "No meaningful dividend"},
                details="No dividend — signal N/A",
            )

        # Get 10-year Treasury yield proxy (^TNX)
        try:
            tnx = yf.Ticker("^TNX").history(period="5d")
            if tnx is not None and not tnx.empty:
                treasury_yield = float(tnx["Close"].iloc[-1]) / 100  # TNX is in percent
                raw["treasury_10y"] = round(treasury_yield * 100, 2)
            else:
                return self._neutral_signal(ticker, "Could not get Treasury yield")
        except Exception:
            return self._neutral_signal(ticker, "Treasury data error")

        # Yield gap: stock dividend yield minus Treasury yield
        yield_gap = div_yield - treasury_yield
        raw["yield_gap_pct"] = round(yield_gap * 100, 2)

        # Signal: positive gap = stock pays more than Treasuries = attractive
        if yield_gap > 0.02:
            signal = 0.3    # Stock yields 2%+ more than Treasuries — very attractive
        elif yield_gap > 0.01:
            signal = 0.15
        elif yield_gap > -0.01:
            signal = 0.0    # Roughly equal
        elif yield_gap > -0.02:
            signal = -0.15  # Treasuries more attractive
        else:
            signal = -0.3   # Treasuries much more attractive

        confidence = min(0.5, 0.3 + abs(yield_gap) * 5)

        result = SignalResult(
            collector_name=self.name,
            ticker=ticker,
            signal_value=signal,
            confidence=confidence,
            raw_data=raw,
            details=f"Div={raw['dividend_yield']:.1f}% vs T10Y={raw['treasury_10y']:.1f}% (gap={raw['yield_gap_pct']:+.1f}%)",
        )
        self._set_cache(result.to_dict(), ticker, "div_treasury")
        return result


class MomentumQualityCollector(BaseCollector):
    """
    Momentum Quality Score — not just "is price going up?" but HOW.
    
    Measures:
    - Trend consistency: What % of recent days closed in the trend direction?
    - Rate of change acceleration: Is momentum increasing or fading?
    - Volume confirmation: Is volume supporting the move?
    - Drawdown from recent high: How much has it pulled back?
    
    High-quality momentum = consistent uptrend with increasing volume and minimal drawdown.
    Low-quality = choppy, divergent volume, big drawdowns despite uptrend.
    """

    @property
    def name(self):
        return "momentum_quality"

    def collect(self, ticker: str) -> SignalResult:
        cached = self._get_cached(ticker, "momentum_q")
        if cached:
            return SignalResult.from_dict(cached)

        try:
            import yfinance as yf
        except ImportError:
            return self._neutral_signal(ticker, "yfinance not available")

        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="60d")
            if hist is None or hist.empty or len(hist) < 30:
                return self._neutral_signal(ticker, "Insufficient price history")

            closes = hist["Close"].values.astype(float)
            volumes = hist["Volume"].values.astype(float)
            highs = hist["High"].values.astype(float)
        except Exception as e:
            return self._neutral_signal(ticker, f"Data error: {e}")

        raw = {}

        # 1. Trend consistency: % of days that closed higher than previous
        daily_returns = np.diff(closes[-20:])
        up_days = sum(1 for r in daily_returns if r > 0)
        down_days = sum(1 for r in daily_returns if r < 0)
        consistency = up_days / len(daily_returns) if daily_returns.size > 0 else 0.5
        raw["up_day_pct"] = round(consistency * 100, 1)

        # 2. Rate of change acceleration
        roc_10 = (closes[-1] - closes[-10]) / closes[-10] if len(closes) >= 10 and closes[-10] != 0 else 0
        roc_20 = (closes[-1] - closes[-20]) / closes[-20] if len(closes) >= 20 and closes[-20] != 0 else 0
        # If short-term ROC > long-term ROC, momentum is accelerating
        acceleration = roc_10 - (roc_20 / 2)  # Compare 10d to half of 20d
        # Guard against NaN
        if np.isnan(roc_10): roc_10 = 0
        if np.isnan(roc_20): roc_20 = 0
        if np.isnan(acceleration): acceleration = 0
        raw["roc_10d"] = round(roc_10 * 100, 2)
        raw["roc_20d"] = round(roc_20 * 100, 2)
        raw["acceleration"] = round(acceleration * 100, 2)

        # 3. Volume trend: is volume increasing on up days?
        vol_up = []
        vol_down = []
        for i in range(1, min(20, len(closes))):
            if closes[-i] > closes[-i-1]:
                vol_up.append(volumes[-i])
            else:
                vol_down.append(volumes[-i])

        avg_vol_up = np.mean(vol_up) if vol_up else 0
        avg_vol_down = np.mean(vol_down) if vol_down else 1
        vol_confirmation = (avg_vol_up / max(avg_vol_down, 1)) - 1  # >0 = volume supports uptrend
        if np.isnan(vol_confirmation): vol_confirmation = 0
        raw["volume_confirmation"] = round(vol_confirmation, 3)

        # 4. Drawdown from 20-day high
        high_20d = float(np.nanmax(highs[-20:])) if len(highs) >= 20 else float(highs[-1])
        drawdown = (closes[-1] - high_20d) / high_20d if high_20d > 0 else 0
        if np.isnan(drawdown): drawdown = 0
        raw["drawdown_pct"] = round(drawdown * 100, 2)

        # Composite score
        # Each component contributes to a -1 to +1 signal
        trend_score = (consistency - 0.5) * 2  # 0.5 = neutral, 0.7 = +0.4
        accel_score = max(-0.5, min(0.5, acceleration * 15))
        vol_score = max(-0.3, min(0.3, vol_confirmation * 0.3))
        dd_score = max(-0.3, min(0.1, drawdown * 3))  # Penalize drawdowns more

        composite = trend_score * 0.35 + accel_score * 0.30 + vol_score * 0.20 + dd_score * 0.15
        signal = max(-1.0, min(1.0, composite))

        # Quality label
        if signal > 0.2:
            quality = "Strong momentum"
        elif signal > 0.05:
            quality = "Moderate momentum"
        elif signal > -0.05:
            quality = "No clear momentum"
        elif signal > -0.2:
            quality = "Fading momentum"
        else:
            quality = "Negative momentum"

        raw["quality"] = quality

        confidence = min(0.7, 0.4 + abs(signal) * 0.8)

        result = SignalResult(
            collector_name=self.name,
            ticker=ticker,
            signal_value=signal,
            confidence=confidence,
            raw_data=raw,
            details=f"{quality} | Up={raw['up_day_pct']:.0f}% | Accel={raw['acceleration']:+.1f}% | DD={raw['drawdown_pct']:.1f}%",
        )
        self._set_cache(result.to_dict(), ticker, "momentum_q")
        return result


class InsiderRatioCollector(BaseCollector):
    """
    SEC Form 4 Insider Buy/Sell Ratio.
    
    Scrapes SEC EDGAR for recent Form 4 filings (insider transactions).
    A high buy/sell ratio = insiders are buying = bullish signal.
    Heavy selling without corresponding buys = bearish.
    
    This is one of the most studied predictive signals in academic finance.
    Insider buying is a stronger signal than selling (executives sell for many
    non-informational reasons like diversification, tax planning, etc.)
    """

    @property
    def name(self):
        return "insider_ratio"

    def collect(self, ticker: str) -> SignalResult:
        cached = self._get_cached(ticker, "insider_ratio")
        if cached:
            return SignalResult.from_dict(cached)

        raw = {"buys": 0, "sells": 0, "buy_value": 0, "sell_value": 0,
               "buy_shares": 0, "sell_shares": 0, "net_shares": 0}

        # Use SEC EDGAR full-text search for Form 4 filings
        try:
            headers = {"User-Agent": SEC_USER_AGENT}
            url = "https://efts.sec.gov/LATEST/search-index"
            from datetime import timedelta
            params = {
                "q": f'"{ticker}"',
                "forms": "4",
                "dateRange": "custom",
                "startdt": (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d"),
                "enddt": datetime.now().strftime("%Y-%m-%d"),
            }
            resp = self._request(url, params=params, headers=headers)

            if resp and resp.status_code == 200:
                data = resp.json()
                hits = data.get("hits", {}).get("hits", [])
                raw["form4_count"] = len(hits)

                # Can't parse individual buy/sell from search results alone
                # Use filing count as a proxy — lots of filings = lots of insider activity
                if len(hits) > 20:
                    raw["note"] = "High insider activity (20+ Form 4s in 90 days)"
                elif len(hits) > 10:
                    raw["note"] = "Moderate insider activity"
                else:
                    raw["note"] = "Low insider activity"
        except Exception:
            pass

        # Primary source: yfinance insider_purchases summary table
        # This is the same data Finviz shows — clean buy/sell/net summary
        try:
            import yfinance as yf
            stock = yf.Ticker(ticker)

            # insider_purchases is a summary table with Purchases/Sales/Net rows
            purch = getattr(stock, 'insider_purchases', None)
            if purch is not None and not purch.empty:
                for _, row in purch.iterrows():
                    label = str(row.get("Insider Purchases Last 6m", "")).strip()
                    trans = row.get("Trans")
                    shares = row.get("Shares", 0)

                    # Guard against pandas NA/NaN
                    try:
                        trans_int = int(trans) if trans is not None and str(trans) not in ('', '<NA>', 'nan', 'None') else 0
                    except (ValueError, TypeError):
                        trans_int = 0
                    try:
                        shares_int = int(shares) if shares is not None and str(shares) not in ('', '<NA>', 'nan', 'None') else 0
                    except (ValueError, TypeError):
                        shares_int = 0

                    if label == "Purchases" and trans_int > 0:
                        raw["buys"] = trans_int
                        raw["buy_shares"] = shares_int
                    elif label == "Sales" and trans_int > 0:
                        raw["sells"] = trans_int
                        raw["sell_shares"] = shares_int
                    elif label == "Net Shares Purchased (Sold)":
                        raw["net_shares"] = shares_int
                    elif label == "Total Insider Shares Held":
                        raw["total_insider_shares"] = shares_int

                raw["source"] = "yfinance_insider_purchases"

            # Fallback: parse insider_transactions Text field
            if raw["buys"] == 0 and raw["sells"] == 0:
                txns = getattr(stock, 'insider_transactions', None)
                if txns is not None and not txns.empty:
                    for _, row in txns.iterrows():
                        text = str(row.get("Text", "") or "").lower()
                        transaction = str(row.get("Transaction", "") or "").lower()
                        combined = text + " " + transaction

                        if any(w in combined for w in ["purchase", "buy", "acquisition"]):
                            raw["buys"] += 1
                            val = row.get("Value", 0)
                            if val and not (isinstance(val, float) and val != val):  # NaN check
                                raw["buy_value"] += abs(float(val))
                        elif any(w in combined for w in ["sale at price", "sell"]):
                            raw["sells"] += 1
                            val = row.get("Value", 0)
                            if val and not (isinstance(val, float) and val != val):
                                raw["sell_value"] += abs(float(val))
                        # Skip: Stock Award(Grant), Conversion, Gift — these aren't buy/sell signals

                    raw["source"] = "yfinance_insider_transactions"

        except Exception:
            pass

        buys = raw["buys"]
        sells = raw["sells"]
        total = buys + sells

        if total == 0:
            return SignalResult(
                collector_name=self.name,
                ticker=ticker,
                signal_value=0.0,
                confidence=0.1,
                raw_data=raw,
                details="No insider transaction data available",
            )

        # Buy/sell ratio signal
        # Key insight: executive selling is ROUTINE (diversification, tax planning, 
        # scheduled 10b5-1 plans). Only insider BUYING is a strong signal.
        # But NOW we have real transaction counts from the summary table.
        ratio = buys / max(sells, 1)
        raw["buy_sell_ratio"] = round(ratio, 2)

        # Net shares direction
        net_shares = raw.get("net_shares", 0)

        signal = 0.0

        # Use both transaction count AND ratio to determine signal
        if buys >= 5 and ratio > 2.0:
            signal = 0.4    # Heavy buying, ratio strongly favors buys
        elif buys >= 5 and ratio > 1.0:
            signal = 0.3    # Many buys, net buyer
        elif buys >= 5 and ratio > 0.3:
            signal = 0.1    # Many buys but also heavy selling
        elif buys >= 3 and ratio > 1.0:
            signal = 0.25   # Multiple insiders buying more than selling
        elif buys >= 3:
            signal = 0.1    # Some buying but selling dominates
        elif buys >= 1 and ratio > 0.5:
            signal = 0.1    # Some buying
        elif buys >= 1:
            signal = 0.0    # Token buys amid heavy selling = neutral
        elif sells > 10 and buys == 0:
            signal = -0.15  # Heavy selling with zero buys
        elif sells > 3 and buys == 0:
            signal = -0.05  # Moderate selling, no buys

        # Net shares adjustment
        if net_shares > 0 and buys > 0:
            signal += 0.05  # Net accumulation bonus
        elif net_shares < 0 and sells > buys * 2:
            signal -= 0.1   # Significant net distribution

        signal = max(-1.0, min(1.0, signal))

        # Confidence scales with data quality
        confidence = min(0.65, 0.2 + min(total, 20) * 0.02 + (0.1 if buys > 0 else 0))

        # Build detail string
        net_str = ""
        if net_shares:
            net_str = f" Net:{net_shares:+,}"

        result = SignalResult(
            collector_name=self.name,
            ticker=ticker,
            signal_value=signal,
            confidence=confidence,
            raw_data=raw,
            details=f"Buys={buys} Sells={sells} Ratio={raw['buy_sell_ratio']:.1f}{net_str}",
        )
        self._set_cache(result.to_dict(), ticker, "insider_ratio")
        return result


class MarketPulseCollector(BaseCollector):
    """
    Global market pulse — monitors broad market health and major events.
    
    Checks:
    - S&P 500 intraday performance (are most stocks up or down?)
    - Treasury yield movement (risk-on vs risk-off rotation)
    - Dollar strength (DXY proxy via UUP)
    - Global market indicators (EFA for international)
    
    This is a MARKET-LEVEL signal applied to all stocks.
    Unlike fear_greed_proxy which uses VIX/breadth, this focuses on
    actual price action of broad market ETFs RIGHT NOW.
    """

    @property
    def name(self):
        return "market_pulse"

    def collect(self, ticker: str) -> SignalResult:
        cached = self._get_cached(ticker, "market_pulse")
        if cached:
            return SignalResult.from_dict(cached)

        try:
            import yfinance as yf
        except ImportError:
            return self._neutral_signal(ticker, "yfinance not available")

        signals = []
        raw = {}

        # 1. S&P 500 intraday direction
        try:
            spy = yf.Ticker("SPY").history(period="2d")
            if spy is not None and not spy.empty and len(spy) >= 2:
                today = float(spy["Close"].iloc[-1])
                yesterday = float(spy["Close"].iloc[-2])
                spy_chg = (today - yesterday) / yesterday
                raw["spy_change"] = round(spy_chg * 100, 3)

                if spy_chg > 0.01:
                    signals.append(0.3)
                elif spy_chg > 0.003:
                    signals.append(0.15)
                elif spy_chg < -0.01:
                    signals.append(-0.3)
                elif spy_chg < -0.003:
                    signals.append(-0.15)
                else:
                    signals.append(0.0)
        except Exception:
            pass

        # 2. Treasury yield direction (TLT as proxy — inverse of yield)
        try:
            tlt = yf.Ticker("TLT").history(period="5d")
            if tlt is not None and not tlt.empty and len(tlt) >= 2:
                tlt_chg = (float(tlt["Close"].iloc[-1]) - float(tlt["Close"].iloc[-2])) / float(tlt["Close"].iloc[-2])
                raw["tlt_change"] = round(tlt_chg * 100, 3)
                # TLT up = yields falling = bullish for stocks
                if tlt_chg > 0.005:
                    signals.append(0.15)
                elif tlt_chg < -0.005:
                    signals.append(-0.15)
        except Exception:
            pass

        # 3. Dollar strength (UUP ETF)
        try:
            uup = yf.Ticker("UUP").history(period="5d")
            if uup is not None and not uup.empty and len(uup) >= 2:
                uup_chg = (float(uup["Close"].iloc[-1]) - float(uup["Close"].iloc[-2])) / float(uup["Close"].iloc[-2])
                raw["dollar_change"] = round(uup_chg * 100, 3)
                # Strong dollar = headwind for multinational stocks
                if uup_chg > 0.003:
                    signals.append(-0.1)
                elif uup_chg < -0.003:
                    signals.append(0.1)
        except Exception:
            pass

        # 4. International markets (EFA)
        try:
            efa = yf.Ticker("EFA").history(period="2d")
            if efa is not None and not efa.empty and len(efa) >= 2:
                efa_chg = (float(efa["Close"].iloc[-1]) - float(efa["Close"].iloc[-2])) / float(efa["Close"].iloc[-2])
                raw["intl_change"] = round(efa_chg * 100, 3)
                if efa_chg > 0.005:
                    signals.append(0.1)
                elif efa_chg < -0.005:
                    signals.append(-0.1)
        except Exception:
            pass

        # 5. NASDAQ Composite (QQQ as proxy — tech-heavy)
        try:
            qqq = yf.Ticker("QQQ").history(period="2d")
            if qqq is not None and not qqq.empty and len(qqq) >= 2:
                qqq_chg = (float(qqq["Close"].iloc[-1]) - float(qqq["Close"].iloc[-2])) / float(qqq["Close"].iloc[-2])
                raw["nasdaq_change"] = round(qqq_chg * 100, 3)
                if qqq_chg > 0.01:
                    signals.append(0.2)
                elif qqq_chg > 0.003:
                    signals.append(0.1)
                elif qqq_chg < -0.01:
                    signals.append(-0.2)
                elif qqq_chg < -0.003:
                    signals.append(-0.1)
        except Exception:
            pass

        # 6. Dow Jones (DIA as proxy — blue chips/industrials)
        try:
            dia = yf.Ticker("DIA").history(period="2d")
            if dia is not None and not dia.empty and len(dia) >= 2:
                dia_chg = (float(dia["Close"].iloc[-1]) - float(dia["Close"].iloc[-2])) / float(dia["Close"].iloc[-2])
                raw["dow_change"] = round(dia_chg * 100, 3)
        except Exception:
            pass

        # 7. Russell 2000 (IWM as proxy — small caps)
        try:
            iwm = yf.Ticker("IWM").history(period="2d")
            if iwm is not None and not iwm.empty and len(iwm) >= 2:
                iwm_chg = (float(iwm["Close"].iloc[-1]) - float(iwm["Close"].iloc[-2])) / float(iwm["Close"].iloc[-2])
                raw["russell_change"] = round(iwm_chg * 100, 3)
                # Small cap divergence from large cap is a breadth signal
                spy_chg = raw.get("spy_change", 0) / 100
                if iwm_chg > spy_chg + 0.005:
                    signals.append(0.1)   # Small caps outperforming = risk-on breadth
                elif iwm_chg < spy_chg - 0.005:
                    signals.append(-0.1)  # Small caps underperforming = narrowing leadership
        except Exception:
            pass

        if not signals:
            return self._neutral_signal(ticker, "Could not compute market pulse")

        avg = sum(signals) / len(signals)
        signal = max(-1.0, min(1.0, avg))

        # Classify
        if signal > 0.1:
            pulse = "Risk-on (broad rally)"
        elif signal > 0.03:
            pulse = "Mildly positive"
        elif signal > -0.03:
            pulse = "Mixed"
        elif signal > -0.1:
            pulse = "Mildly negative"
        else:
            pulse = "Risk-off (broad selloff)"

        raw["pulse"] = pulse
        raw["components"] = len(signals)

        # Build detail string with all available indexes
        detail_parts = []
        if "spy_change" in raw:
            detail_parts.append(f"S&P:{raw['spy_change']:+.2f}%")
        if "nasdaq_change" in raw:
            detail_parts.append(f"NDQ:{raw['nasdaq_change']:+.2f}%")
        if "dow_change" in raw:
            detail_parts.append(f"DOW:{raw['dow_change']:+.2f}%")
        if "russell_change" in raw:
            detail_parts.append(f"RUS:{raw['russell_change']:+.2f}%")
        detail_parts.append(pulse)

        result = SignalResult(
            collector_name=self.name,
            ticker=ticker,
            signal_value=signal,
            confidence=min(0.65, len(signals) * 0.1),
            raw_data=raw,
            details=" | ".join(detail_parts),
        )
        self._set_cache(result.to_dict(), ticker, "market_pulse")
        return result
