"""
Analysis Collectors
====================
Real financial analysis collectors using yfinance data.
No additional API keys required.

1. TechnicalAnalysisCollector  - RSI, MACD, Bollinger Bands, MA crossovers, volume
2. FundamentalAnalysisCollector - P/E, margins, growth, debt, cash flow
3. AnalystConsensusCollector    - Price targets, buy/hold/sell ratings
4. OptionsFlowCollector         - Put/call ratio, unusual activity
5. ShortInterestCollector       - Short interest % and trends
"""
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

import numpy as np

from stock_oracle.collectors.base import BaseCollector, SignalResult

logger = logging.getLogger("stock_oracle")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 1. TECHNICAL ANALYSIS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TechnicalAnalysisCollector(BaseCollector):
    """
    Computes RSI, MACD, Bollinger Band position, MA crossovers,
    and volume trends from yfinance price data.

    This is pure math on price/volume — the backbone of most
    short-term trading signals.
    """

    @property
    def name(self) -> str:
        return "technical_analysis"

    def collect(self, ticker: str) -> SignalResult:
        cached = self._get_cached(ticker, "tech_analysis")
        if cached:
            return SignalResult.from_dict(cached)

        try:
            import yfinance as yf
            stock = yf.Ticker(ticker)
            hist = stock.history(period="6mo")

            if hist.empty or len(hist) < 50:
                return self._neutral_signal(ticker, "Insufficient price history")

            closes = hist["Close"].values
            volumes = hist["Volume"].values
            highs = hist["High"].values
            lows = hist["Low"].values

            indicators = {}
            signals = []

            # ── RSI (14-period) ──
            rsi = self._compute_rsi(closes, 14)
            indicators["rsi"] = round(rsi, 1)
            if rsi < 30:
                signals.append(("RSI oversold", 0.4))
            elif rsi < 40:
                signals.append(("RSI low", 0.15))
            elif rsi > 70:
                signals.append(("RSI overbought", -0.4))
            elif rsi > 60:
                signals.append(("RSI high", -0.15))
            else:
                signals.append(("RSI neutral", 0.0))

            # ── MACD (12,26,9) ──
            macd_line, signal_line, histogram = self._compute_macd(closes)
            indicators["macd_histogram"] = round(histogram, 4)
            # Bullish: MACD crosses above signal (histogram turns positive)
            if len(closes) >= 27:
                prev_hist = self._compute_macd(closes[:-1])[2]
                if histogram > 0 and prev_hist <= 0:
                    signals.append(("MACD bullish cross", 0.35))
                elif histogram < 0 and prev_hist >= 0:
                    signals.append(("MACD bearish cross", -0.35))
                elif histogram > 0:
                    signals.append(("MACD positive", 0.1))
                elif histogram < 0:
                    signals.append(("MACD negative", -0.1))

            # ── Bollinger Bands (20,2) ──
            bb_pos = self._bollinger_position(closes, 20, 2)
            indicators["bollinger_position"] = round(bb_pos, 3)
            # bb_pos: 0=at lower band, 0.5=at middle, 1=at upper band
            if bb_pos < 0.05:
                signals.append(("Below lower Bollinger", 0.35))
            elif bb_pos < 0.2:
                signals.append(("Near lower Bollinger", 0.15))
            elif bb_pos > 0.95:
                signals.append(("Above upper Bollinger", -0.35))
            elif bb_pos > 0.8:
                signals.append(("Near upper Bollinger", -0.15))

            # ── Moving Average Crossovers ──
            ma20 = np.mean(closes[-20:])
            ma50 = np.mean(closes[-50:])
            indicators["ma20"] = round(float(ma20), 2)
            indicators["ma50"] = round(float(ma50), 2)
            indicators["price_vs_ma20_pct"] = round((closes[-1] - ma20) / ma20 * 100, 2)

            if len(closes) >= 200:
                ma200 = np.mean(closes[-200:])
                indicators["ma200"] = round(float(ma200), 2)
                # Golden cross: 50 > 200 (bullish)
                # Death cross: 50 < 200 (bearish)
                if ma50 > ma200:
                    prev_ma50 = np.mean(closes[-51:-1])
                    if prev_ma50 <= ma200:
                        signals.append(("Golden cross forming", 0.4))
                    else:
                        signals.append(("Above 200 MA", 0.1))
                else:
                    prev_ma50 = np.mean(closes[-51:-1])
                    if prev_ma50 >= ma200:
                        signals.append(("Death cross forming", -0.4))
                    else:
                        signals.append(("Below 200 MA", -0.1))

            # Price vs 20 MA
            pct_from_ma20 = (closes[-1] - ma20) / ma20
            if pct_from_ma20 > 0.05:
                signals.append(("Extended above MA20", -0.1))
            elif pct_from_ma20 < -0.05:
                signals.append(("Depressed below MA20", 0.1))

            # ── Volume Analysis ──
            if len(volumes) >= 20:
                avg_vol_20 = np.mean(volumes[-20:])
                avg_vol_5 = np.mean(volumes[-5:])
                vol_ratio = avg_vol_5 / avg_vol_20 if avg_vol_20 > 0 else 1.0
                indicators["volume_ratio_5d_vs_20d"] = round(vol_ratio, 2)

                # Rising volume + rising price = bullish confirmation
                price_change_5d = (closes[-1] - closes[-5]) / closes[-5] if closes[-5] > 0 else 0
                if vol_ratio > 1.5 and price_change_5d > 0.02:
                    signals.append(("Volume surge + price up", 0.25))
                elif vol_ratio > 1.5 and price_change_5d < -0.02:
                    signals.append(("Volume surge + price down", -0.25))
                elif vol_ratio < 0.6:
                    signals.append(("Declining volume", 0.0))

            # ── Support/Resistance ──
            recent_high = float(np.max(highs[-20:]))
            recent_low = float(np.min(lows[-20:]))
            price = closes[-1]
            price_range = recent_high - recent_low
            if price_range > 0:
                position_in_range = (price - recent_low) / price_range
                indicators["range_position"] = round(position_in_range, 3)
                if position_in_range > 0.9:
                    signals.append(("Near resistance", -0.1))
                elif position_in_range < 0.1:
                    signals.append(("Near support", 0.1))

            # ── Combine all technical signals ──
            if not signals:
                return self._neutral_signal(ticker, "No clear technical signals")

            total_signal = sum(s[1] for s in signals) / max(len(signals), 1)
            # Stronger signals when multiple technicals agree
            agreement = sum(1 for s in signals if (s[1] > 0) == (total_signal > 0)) / len(signals)
            confidence = min(0.80, 0.50 + agreement * 0.30)

            detail_parts = [f"{s[0]}" for s in signals if abs(s[1]) > 0.05]
            detail_str = f"RSI={rsi:.0f} | MACD={'+'if histogram>0 else ''}{histogram:.3f} | " \
                         f"BB={bb_pos:.2f} | {', '.join(detail_parts[:3])}"

            result = SignalResult(
                collector_name=self.name,
                ticker=ticker,
                signal_value=max(-1.0, min(1.0, total_signal)),
                confidence=confidence,
                raw_data={"indicators": indicators, "signals": [(s[0], s[1]) for s in signals]},
                details=detail_str,
            )
            self._set_cache(result.to_dict(), ticker, "tech_analysis")
            return result

        except Exception as e:
            logger.debug(f"technical_analysis: Error for {ticker}: {e}")
            return self._neutral_signal(ticker, f"Technical analysis error: {str(e)[:60]}")

    @staticmethod
    def _compute_rsi(closes: np.ndarray, period: int = 14) -> float:
        """Relative Strength Index."""
        if len(closes) < period + 1:
            return 50.0
        deltas = np.diff(closes)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        avg_gain = np.mean(gains[-period:])
        avg_loss = np.mean(losses[-period:])
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    @staticmethod
    def _compute_macd(closes: np.ndarray, fast=12, slow=26, signal_period=9):
        """MACD line, signal line, histogram."""
        if len(closes) < slow + signal_period:
            return 0.0, 0.0, 0.0

        def ema(data, period):
            multiplier = 2 / (period + 1)
            result = [float(data[0])]
            for i in range(1, len(data)):
                result.append((float(data[i]) - result[-1]) * multiplier + result[-1])
            return np.array(result)

        ema_fast = ema(closes, fast)
        ema_slow = ema(closes, slow)
        macd_line = ema_fast - ema_slow
        signal_line = ema(macd_line[slow-1:], signal_period)

        if len(signal_line) == 0:
            return 0.0, 0.0, 0.0

        histogram = float(macd_line[-1] - signal_line[-1])
        return float(macd_line[-1]), float(signal_line[-1]), histogram

    @staticmethod
    def _bollinger_position(closes: np.ndarray, period=20, std_mult=2) -> float:
        """Returns position within Bollinger Bands: 0=lower, 0.5=middle, 1=upper."""
        if len(closes) < period:
            return 0.5
        ma = np.mean(closes[-period:])
        std = np.std(closes[-period:])
        if std == 0:
            return 0.5
        upper = ma + std_mult * std
        lower = ma - std_mult * std
        band_width = upper - lower
        if band_width == 0:
            return 0.5
        return max(0.0, min(1.0, (closes[-1] - lower) / band_width))


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 2. FUNDAMENTAL ANALYSIS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class FundamentalAnalysisCollector(BaseCollector):
    """
    Evaluates P/E ratio, profit margins, revenue growth,
    debt-to-equity, and free cash flow from yfinance data.

    Produces a composite fundamental health score.
    """

    @property
    def name(self) -> str:
        return "fundamental_analysis"

    # Rough sector average P/E ratios for comparison
    SECTOR_PE = {
        "Technology": 30, "Communication Services": 22,
        "Consumer Cyclical": 25, "Consumer Defensive": 22,
        "Healthcare": 20, "Financial Services": 14,
        "Industrials": 20, "Energy": 12,
        "Real Estate": 35, "Utilities": 18,
        "Basic Materials": 15,
    }

    def collect(self, ticker: str) -> SignalResult:
        cached = self._get_cached(ticker, "fundamentals")
        if cached:
            return SignalResult.from_dict(cached)

        try:
            import yfinance as yf
            stock = yf.Ticker(ticker)
            info = stock.info

            if not info or "symbol" not in info:
                return self._neutral_signal(ticker, "No fundamental data available")

            scores = []
            details = {}

            # ── P/E Ratio ──
            pe = info.get("trailingPE") or info.get("forwardPE")
            if pe and pe > 0:
                sector = info.get("sector", "Technology")
                sector_pe = self.SECTOR_PE.get(sector, 22)
                pe_ratio = pe / sector_pe
                details["pe"] = round(pe, 1)
                details["sector_avg_pe"] = sector_pe

                if pe_ratio < 0.6:
                    scores.append(("Significantly undervalued (P/E)", 0.35))
                elif pe_ratio < 0.85:
                    scores.append(("Undervalued (P/E)", 0.15))
                elif pe_ratio > 2.0:
                    scores.append(("Very overvalued (P/E)", -0.3))
                elif pe_ratio > 1.3:
                    scores.append(("Overvalued (P/E)", -0.15))
                else:
                    scores.append(("Fair value (P/E)", 0.0))

            # ── PEG Ratio (P/E to Growth) ──
            peg = info.get("pegRatio")
            if peg and peg > 0:
                details["peg"] = round(peg, 2)
                if peg < 1.0:
                    scores.append(("Attractive PEG", 0.2))
                elif peg > 2.5:
                    scores.append(("Expensive PEG", -0.2))

            # ── Profit Margins ──
            margin = info.get("profitMargins")
            if margin is not None:
                details["profit_margin"] = round(margin * 100, 1)
                if margin > 0.25:
                    scores.append(("Strong margins", 0.2))
                elif margin > 0.10:
                    scores.append(("Healthy margins", 0.05))
                elif margin < 0:
                    scores.append(("Negative margins", -0.25))
                elif margin < 0.05:
                    scores.append(("Thin margins", -0.1))

            # ── Revenue Growth ──
            rev_growth = info.get("revenueGrowth")
            if rev_growth is not None:
                details["revenue_growth"] = round(rev_growth * 100, 1)
                if rev_growth > 0.25:
                    scores.append(("Strong growth", 0.3))
                elif rev_growth > 0.10:
                    scores.append(("Solid growth", 0.15))
                elif rev_growth > 0:
                    scores.append(("Modest growth", 0.05))
                elif rev_growth > -0.10:
                    scores.append(("Slight decline", -0.1))
                else:
                    scores.append(("Revenue declining", -0.25))

            # ── Earnings Growth ──
            earn_growth = info.get("earningsGrowth")
            if earn_growth is not None:
                details["earnings_growth"] = round(earn_growth * 100, 1)
                if earn_growth > 0.30:
                    scores.append(("Strong earnings growth", 0.25))
                elif earn_growth > 0.10:
                    scores.append(("Good earnings growth", 0.1))
                elif earn_growth < -0.20:
                    scores.append(("Earnings declining", -0.2))

            # ── Debt-to-Equity ──
            debt_eq = info.get("debtToEquity")
            if debt_eq is not None:
                details["debt_to_equity"] = round(debt_eq, 1)
                # Convert from percentage to ratio if needed
                de_ratio = debt_eq / 100 if debt_eq > 10 else debt_eq
                if de_ratio < 0.3:
                    scores.append(("Low debt", 0.15))
                elif de_ratio > 2.0:
                    scores.append(("High debt", -0.2))
                elif de_ratio > 1.0:
                    scores.append(("Moderate debt", -0.05))

            # ── Free Cash Flow ──
            fcf = info.get("freeCashflow")
            market_cap = info.get("marketCap")
            if fcf and market_cap and market_cap > 0:
                fcf_yield = fcf / market_cap
                details["fcf_yield"] = round(fcf_yield * 100, 2)
                if fcf_yield > 0.08:
                    scores.append(("Excellent FCF yield", 0.25))
                elif fcf_yield > 0.04:
                    scores.append(("Good FCF yield", 0.1))
                elif fcf_yield < 0:
                    scores.append(("Negative FCF", -0.2))

            # ── Return on Equity ──
            roe = info.get("returnOnEquity")
            if roe is not None:
                details["roe"] = round(roe * 100, 1)
                if roe > 0.25:
                    scores.append(("Excellent ROE", 0.15))
                elif roe > 0.15:
                    scores.append(("Good ROE", 0.05))
                elif roe < 0:
                    scores.append(("Negative ROE", -0.15))

            if not scores:
                return self._neutral_signal(ticker, "No fundamental metrics available (likely ETF)")

            total_signal = sum(s[1] for s in scores) / max(len(scores), 1)
            # More metrics = more confidence
            confidence = min(0.70, 0.30 + len(scores) * 0.05)

            detail_parts = []
            if "pe" in details:
                detail_parts.append(f"P/E={details['pe']}")
            if "profit_margin" in details:
                detail_parts.append(f"Margin={details['profit_margin']}%")
            if "revenue_growth" in details:
                detail_parts.append(f"RevGr={details['revenue_growth']}%")
            if "debt_to_equity" in details:
                detail_parts.append(f"D/E={details['debt_to_equity']}")

            result = SignalResult(
                collector_name=self.name,
                ticker=ticker,
                signal_value=max(-1.0, min(1.0, total_signal)),
                confidence=confidence,
                raw_data={"metrics": details, "scores": [(s[0], s[1]) for s in scores]},
                details=" | ".join(detail_parts[:4]),
            )
            self._set_cache(result.to_dict(), ticker, "fundamentals")
            return result

        except Exception as e:
            logger.debug(f"fundamental_analysis: Error for {ticker}: {e}")
            return self._neutral_signal(ticker, f"Fundamental analysis error: {str(e)[:60]}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 3. ANALYST CONSENSUS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class AnalystConsensusCollector(BaseCollector):
    """
    Aggregates analyst price targets and buy/hold/sell
    recommendations from yfinance.

    The gap between current price and mean target price is
    one of the strongest predictive signals available.
    """

    @property
    def name(self) -> str:
        return "analyst_consensus"

    def collect(self, ticker: str) -> SignalResult:
        cached = self._get_cached(ticker, "analyst")
        if cached:
            return SignalResult.from_dict(cached)

        try:
            import yfinance as yf
            stock = yf.Ticker(ticker)
            info = stock.info

            if not info:
                return self._neutral_signal(ticker, "No analyst data")

            signals = []
            details = {}

            # ── Price Target vs Current ──
            current = info.get("currentPrice") or info.get("regularMarketPrice")
            target_mean = info.get("targetMeanPrice")
            target_low = info.get("targetLowPrice")
            target_high = info.get("targetHighPrice")
            num_analysts = info.get("numberOfAnalystOpinions", 0)

            if current and target_mean and current > 0:
                upside = (target_mean - current) / current
                details["current_price"] = round(current, 2)
                details["target_mean"] = round(target_mean, 2)
                details["target_upside_pct"] = round(upside * 100, 1)
                details["num_analysts"] = num_analysts

                if upside > 0.30:
                    signals.append(("Strong upside to target", 0.4))
                elif upside > 0.15:
                    signals.append(("Moderate upside", 0.25))
                elif upside > 0.05:
                    signals.append(("Slight upside", 0.1))
                elif upside > -0.05:
                    signals.append(("Near target", 0.0))
                elif upside > -0.15:
                    signals.append(("Slight downside risk", -0.15))
                else:
                    signals.append(("Below target significantly", -0.3))

                # Target spread — wide spread = less consensus
                if target_low and target_high and target_high > 0:
                    spread = (target_high - target_low) / current
                    details["target_spread_pct"] = round(spread * 100, 1)

            # ── Recommendation Trend ──
            rec = info.get("recommendationKey", "").lower()
            rec_mean = info.get("recommendationMean")
            details["recommendation"] = rec
            details["recommendation_mean"] = rec_mean

            if rec_mean:
                # 1=Strong Buy, 2=Buy, 3=Hold, 4=Sell, 5=Strong Sell
                if rec_mean < 1.8:
                    signals.append(("Strong Buy consensus", 0.3))
                elif rec_mean < 2.3:
                    signals.append(("Buy consensus", 0.15))
                elif rec_mean < 2.8:
                    signals.append(("Moderate Buy", 0.05))
                elif rec_mean < 3.5:
                    signals.append(("Hold consensus", -0.05))
                else:
                    signals.append(("Sell consensus", -0.25))

            if not signals:
                return self._neutral_signal(ticker, "No analyst coverage")

            total_signal = sum(s[1] for s in signals) / max(len(signals), 1)

            # More analysts = more confidence, but cap at 60%
            # Analysts are often wrong, but collectively they have information value
            analyst_factor = min(1.0, (num_analysts or 1) / 15)
            confidence = min(0.60, 0.25 + analyst_factor * 0.35)

            detail_str = ""
            if "target_upside_pct" in details:
                detail_str += f"Target: ${details.get('target_mean',0)} ({details['target_upside_pct']:+.1f}%)"
            if rec:
                detail_str += f" | Rec: {rec}"
            if num_analysts:
                detail_str += f" | {num_analysts} analysts"

            result = SignalResult(
                collector_name=self.name,
                ticker=ticker,
                signal_value=max(-1.0, min(1.0, total_signal)),
                confidence=confidence,
                raw_data=details,
                details=detail_str,
            )
            self._set_cache(result.to_dict(), ticker, "analyst")
            return result

        except Exception as e:
            logger.debug(f"analyst_consensus: Error for {ticker}: {e}")
            return self._neutral_signal(ticker, f"Analyst data error: {str(e)[:60]}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 4. OPTIONS FLOW
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class OptionsFlowCollector(BaseCollector):
    """
    Analyzes put/call ratio, implied volatility,
    and unusual options activity from yfinance options chains.

    Smart money often moves in options before stock price follows.
    """

    @property
    def name(self) -> str:
        return "options_flow"

    def collect(self, ticker: str) -> SignalResult:
        cached = self._get_cached(ticker, "options")
        if cached:
            return SignalResult.from_dict(cached)

        try:
            import yfinance as yf
            stock = yf.Ticker(ticker)

            # Get nearest expiration
            expirations = stock.options
            if not expirations:
                return self._neutral_signal(ticker, "No options data (likely ETF or small-cap)")

            # Use the nearest 2 expirations for most relevant data
            signals = []
            details = {}
            total_call_vol = 0
            total_put_vol = 0
            total_call_oi = 0
            total_put_oi = 0

            for exp in expirations[:2]:
                try:
                    chain = stock.option_chain(exp)
                    calls = chain.calls
                    puts = chain.puts

                    if not calls.empty:
                        total_call_vol += calls["volume"].sum() if "volume" in calls else 0
                        total_call_oi += calls["openInterest"].sum() if "openInterest" in calls else 0
                    if not puts.empty:
                        total_put_vol += puts["volume"].sum() if "volume" in puts else 0
                        total_put_oi += puts["openInterest"].sum() if "openInterest" in puts else 0
                except Exception:
                    continue

            # ── Put/Call Ratio (volume) ──
            if total_call_vol > 0:
                pcr_vol = total_put_vol / total_call_vol
                details["put_call_ratio_volume"] = round(pcr_vol, 3)

                # PCR interpretation is contrarian:
                # High PCR (>1.2) = lots of puts = bearish sentiment = often contrarian bullish
                # Low PCR (<0.5) = lots of calls = bullish sentiment = often contrarian bearish
                # But extreme readings are more reliable directionally
                if pcr_vol > 1.5:
                    signals.append(("Extreme put buying (fear)", 0.2))  # Contrarian bullish
                elif pcr_vol > 1.0:
                    signals.append(("Elevated puts", 0.1))
                elif pcr_vol < 0.4:
                    signals.append(("Extreme call buying (greed)", -0.15))  # Contrarian bearish
                elif pcr_vol < 0.7:
                    signals.append(("Call-heavy flow", -0.05))
                else:
                    signals.append(("Balanced options flow", 0.0))

            # ── Put/Call Ratio (open interest) ──
            if total_call_oi > 0:
                pcr_oi = total_put_oi / total_call_oi
                details["put_call_ratio_oi"] = round(pcr_oi, 3)

                if pcr_oi > 1.3:
                    signals.append(("High put OI (hedging)", 0.1))
                elif pcr_oi < 0.5:
                    signals.append(("High call OI (bullish bets)", 0.05))

            # ── Total Volume (activity level) ──
            total_vol = total_call_vol + total_put_vol
            details["total_options_volume"] = int(total_vol)

            if not signals:
                return self._neutral_signal(ticker, "No meaningful options signals")

            total_signal = sum(s[1] for s in signals) / max(len(signals), 1)
            # Options are noisy, keep confidence moderate
            confidence = min(0.50, 0.25 + (0.15 if total_vol > 10000 else 0.0))

            detail_parts = []
            if "put_call_ratio_volume" in details:
                detail_parts.append(f"P/C Vol={details['put_call_ratio_volume']:.2f}")
            if "put_call_ratio_oi" in details:
                detail_parts.append(f"P/C OI={details['put_call_ratio_oi']:.2f}")
            detail_parts.append(f"Total Vol={total_vol:,}")

            result = SignalResult(
                collector_name=self.name,
                ticker=ticker,
                signal_value=max(-1.0, min(1.0, total_signal)),
                confidence=confidence,
                raw_data=details,
                details=" | ".join(detail_parts),
            )
            self._set_cache(result.to_dict(), ticker, "options")
            return result

        except Exception as e:
            logger.debug(f"options_flow: Error for {ticker}: {e}")
            return self._neutral_signal(ticker, f"Options analysis error: {str(e)[:60]}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 5. SHORT INTEREST
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class ShortInterestCollector(BaseCollector):
    """
    Tracks short interest as % of float and short ratio.

    High short interest can indicate:
    - Bearish sentiment (negative)
    - Potential short squeeze (contrarian bullish)
    """

    @property
    def name(self) -> str:
        return "short_interest"

    def collect(self, ticker: str) -> SignalResult:
        cached = self._get_cached(ticker, "short_int")
        if cached:
            return SignalResult.from_dict(cached)

        try:
            import yfinance as yf
            stock = yf.Ticker(ticker)
            info = stock.info

            if not info:
                return self._neutral_signal(ticker, "No short interest data")

            short_pct = info.get("shortPercentOfFloat")
            short_ratio = info.get("shortRatio")  # Days to cover
            shares_short = info.get("sharesShort")
            shares_short_prev = info.get("sharesShortPriorMonth")

            if short_pct is None and short_ratio is None:
                return self._neutral_signal(ticker, "No short interest data (likely ETF)")

            signals = []
            details = {}

            if short_pct is not None:
                details["short_pct_float"] = round(short_pct * 100, 2)

                if short_pct > 0.20:
                    # Very high short interest — squeeze potential
                    signals.append(("Extreme short interest (squeeze risk)", 0.15))
                elif short_pct > 0.10:
                    signals.append(("High short interest", -0.1))
                elif short_pct > 0.05:
                    signals.append(("Moderate short interest", -0.05))
                else:
                    signals.append(("Low short interest", 0.05))

            if short_ratio is not None:
                details["days_to_cover"] = round(short_ratio, 1)
                if short_ratio > 7:
                    signals.append(("Very high days-to-cover", 0.1))  # Squeeze risk
                elif short_ratio > 4:
                    signals.append(("Elevated days-to-cover", 0.0))

            # Short interest trend
            if shares_short and shares_short_prev and shares_short_prev > 0:
                change = (shares_short - shares_short_prev) / shares_short_prev
                details["short_change_monthly"] = round(change * 100, 1)
                if change > 0.10:
                    signals.append(("Short interest rising", -0.15))
                elif change < -0.10:
                    signals.append(("Short interest declining", 0.15))

            if not signals:
                return self._neutral_signal(ticker, "No meaningful short interest signals")

            total_signal = sum(s[1] for s in signals) / max(len(signals), 1)
            confidence = min(0.55, 0.30 + len(signals) * 0.08)

            detail_parts = []
            if "short_pct_float" in details:
                detail_parts.append(f"Short={details['short_pct_float']:.1f}%")
            if "days_to_cover" in details:
                detail_parts.append(f"DTC={details['days_to_cover']:.1f}")
            if "short_change_monthly" in details:
                detail_parts.append(f"MoM={details['short_change_monthly']:+.1f}%")

            result = SignalResult(
                collector_name=self.name,
                ticker=ticker,
                signal_value=max(-1.0, min(1.0, total_signal)),
                confidence=confidence,
                raw_data=details,
                details=" | ".join(detail_parts),
            )
            self._set_cache(result.to_dict(), ticker, "short_int")
            return result

        except Exception as e:
            logger.debug(f"short_interest: Error for {ticker}: {e}")
            return self._neutral_signal(ticker, f"Short interest error: {str(e)[:60]}")
