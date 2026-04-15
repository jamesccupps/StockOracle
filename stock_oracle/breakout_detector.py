"""
Breakout Detector
==================
Scores each ticker on how likely it is to break out upward.
Combines multiple technical patterns into a 0-100 breakout score.

Key patterns detected:
  1. Bollinger Squeeze → expansion (volatility compression releasing)
  2. Volume accumulation (rising volume during consolidation)
  3. 52-week high proximity (within striking distance with momentum)
  4. RSI momentum buildup (40-65 range and accelerating)
  5. MACD crossover or histogram expansion
  6. Price above key MAs with MA alignment (20 > 50 > 200)
  7. Range compression → expansion (ATR narrowing then widening)
  8. Relative strength vs SPY (outperforming the market)

Usage:
    detector = BreakoutDetector()
    scores = detector.scan(["AAPL", "TSLA", "LUNR", "RKLB"])
    # Returns sorted list: [(ticker, score, details), ...]
"""
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import numpy as np

logger = logging.getLogger("stock_oracle")


class BreakoutDetector:
    """
    Scans stocks for breakout potential.
    Each signal contributes 0-15 points to a composite 0-100 score.
    """

    # Score thresholds for classification
    STRONG_BREAKOUT = 70    # 70+ = strong breakout setup
    MODERATE_BREAKOUT = 50  # 50-69 = building momentum
    WEAK_SIGNAL = 30        # 30-49 = some positive signs
    # Below 30 = no breakout signal

    def __init__(self):
        self._spy_data = None  # Cache SPY data for relative strength

    def scan(self, tickers: List[str], results: Dict = None) -> List[Dict]:
        """
        Score all tickers for breakout potential.

        Args:
            tickers: List of stock symbols to scan
            results: Optional current oracle results dict (for cached signal data)

        Returns:
            List of dicts sorted by score descending:
            [{ticker, score, grade, signals, details}, ...]
        """
        scores = []

        # Pre-fetch SPY for relative strength comparison
        self._load_spy_data()

        for ticker in tickers:
            try:
                result = self._analyze_ticker(ticker, results)
                scores.append(result)
            except Exception as e:
                logger.debug(f"Breakout scan error for {ticker}: {e}")
                scores.append({
                    "ticker": ticker,
                    "score": 0,
                    "grade": "N/A",
                    "signals": [],
                    "details": f"Error: {e}",
                })

        # Sort by score descending
        scores.sort(key=lambda x: x["score"], reverse=True)
        return scores

    def _analyze_ticker(self, ticker: str, results: Dict = None) -> Dict:
        """Compute breakout score for a single ticker."""
        import yfinance as yf

        stock = yf.Ticker(ticker)
        hist = stock.history(period="6mo")

        if hist.empty or len(hist) < 50:
            return {"ticker": ticker, "score": 0, "grade": "N/A",
                    "signals": [], "details": "Insufficient data",
                    "timeframe": "", "timeframe_days": 0}

        closes = hist["Close"].values.astype(float)
        highs = hist["High"].values.astype(float)
        lows = hist["Low"].values.astype(float)
        volumes = hist["Volume"].values.astype(float)
        price = closes[-1]

        # Signals now include timeframe: (name, points, detail_str, timeframe)
        # timeframe: "SHORT" (1-3d), "MEDIUM" (1-2wk), "LONG" (weeks-months)
        signals = []
        total_score = 0

        # ── 1. Bollinger Squeeze Detection (0-15 points) — MEDIUM ──
        bb_score, bb_detail = self._bollinger_squeeze(closes)
        signals.append(("BB Squeeze", bb_score, bb_detail, "MEDIUM"))
        total_score += bb_score

        # ── 2. Volume Accumulation (0-15 points) — SHORT/MEDIUM ──
        vol_score, vol_detail, vol_tf = self._volume_pattern(closes, volumes)
        signals.append(("Volume", vol_score, vol_detail, vol_tf))
        total_score += vol_score

        # ── 3. 52-Week High Proximity (0-15 points) — LONG ──
        company_info = {}
        try:
            info = stock.info
            week52_high = info.get("fiftyTwoWeekHigh", 0)
            # Grab company description while we have info
            company_info = {
                "name": info.get("longName") or info.get("shortName", ticker),
                "sector": info.get("sector", ""),
                "industry": info.get("industry", ""),
                "summary": info.get("longBusinessSummary", ""),
                "market_cap": info.get("marketCap", 0),
                "employees": info.get("fullTimeEmployees", 0),
                "52w_high": info.get("fiftyTwoWeekHigh", 0),
                "52w_low": info.get("fiftyTwoWeekLow", 0),
            }
        except Exception:
            week52_high = float(np.max(highs))
        hi_score, hi_detail = self._near_high(price, week52_high, volumes)
        signals.append(("52W High", hi_score, hi_detail, "LONG"))
        total_score += hi_score

        # ── 4. RSI Momentum (0-12 points) — SHORT ──
        rsi_score, rsi_detail = self._rsi_momentum(closes)
        signals.append(("RSI", rsi_score, rsi_detail, "SHORT"))
        total_score += rsi_score

        # ── 5. MACD Signal (0-12 points) — SHORT ──
        macd_score, macd_detail = self._macd_signal(closes)
        signals.append(("MACD", macd_score, macd_detail, "SHORT"))
        total_score += macd_score

        # ── 6. MA Alignment (0-12 points) — MEDIUM/LONG ──
        ma_score, ma_detail, ma_tf = self._ma_alignment(closes)
        signals.append(("MA Align", ma_score, ma_detail, ma_tf))
        total_score += ma_score

        # ── 7. Range Compression (0-10 points) — MEDIUM ──
        atr_score, atr_detail, atr_tf = self._range_compression(highs, lows, closes)
        signals.append(("Range", atr_score, atr_detail, atr_tf))
        total_score += atr_score

        # ── 8. Relative Strength vs SPY (0-9 points) — LONG ──
        rs_score, rs_detail = self._relative_strength(closes)
        signals.append(("Rel Str", rs_score, rs_detail, "LONG"))
        total_score += rs_score

        # Clamp to 100
        total_score = min(100, max(0, total_score))

        # Grade
        if total_score >= self.STRONG_BREAKOUT:
            grade = "STRONG"
        elif total_score >= self.MODERATE_BREAKOUT:
            grade = "BUILDING"
        elif total_score >= self.WEAK_SIGNAL:
            grade = "EARLY"
        else:
            grade = "NONE"

        # ── Estimate breakout timeframe ──
        # Weight each active signal's timeframe by its point contribution
        tf_weights = {"SHORT": 0, "MEDIUM": 0, "LONG": 0}
        tf_days = {"SHORT": 2, "MEDIUM": 7, "LONG": 21}
        for name, pts, detail, tf in signals:
            if pts > 0 and tf in tf_weights:
                tf_weights[tf] += pts

        total_pts = sum(tf_weights.values())
        if total_pts > 0:
            # Weighted average timeframe in days
            est_days = sum(tf_days[tf] * w for tf, w in tf_weights.items()) / total_pts
            est_days = round(est_days)

            # Determine dominant timeframe
            dominant_tf = max(tf_weights, key=tf_weights.get)
            dominant_pct = tf_weights[dominant_tf] / total_pts

            # Build human-readable timeframe string
            if est_days <= 3:
                timeframe_str = f"~{est_days}d (quick pop)"
            elif est_days <= 7:
                timeframe_str = f"~{est_days}d (swing)"
            elif est_days <= 14:
                timeframe_str = f"~{est_days}d (1-2 weeks)"
            else:
                weeks = round(est_days / 7)
                timeframe_str = f"~{weeks}wk (position)"

            # If one timeframe dominates >65%, label it clearly
            if dominant_pct > 0.65:
                tf_labels = {"SHORT": "short-term", "MEDIUM": "swing", "LONG": "longer-term"}
                timeframe_str += f" — mostly {tf_labels[dominant_tf]}"
        else:
            est_days = 0
            timeframe_str = ""

        # Build summary
        top_signals = sorted(signals, key=lambda x: x[1], reverse=True)
        detail_parts = [f"{name}: {detail}" for name, pts, detail, tf in top_signals if pts > 0]
        details = " | ".join(detail_parts) if detail_parts else "No breakout signals"

        # Build short company description
        short_desc = ""
        if company_info:
            name = company_info.get("name", ticker)
            sector = company_info.get("sector", "")
            industry = company_info.get("industry", "")
            full_summary = company_info.get("summary", "")
            mcap = company_info.get("market_cap", 0)

            # Market cap label
            if mcap >= 200_000_000_000:
                cap_label = "mega-cap"
            elif mcap >= 10_000_000_000:
                cap_label = "large-cap"
            elif mcap >= 2_000_000_000:
                cap_label = "mid-cap"
            elif mcap >= 300_000_000:
                cap_label = "small-cap"
            elif mcap > 0:
                cap_label = "micro-cap"
            else:
                cap_label = ""

            # Format market cap
            if mcap >= 1_000_000_000:
                mcap_str = f"${mcap/1_000_000_000:.1f}B"
            elif mcap >= 1_000_000:
                mcap_str = f"${mcap/1_000_000:.0f}M"
            else:
                mcap_str = ""

            # Build one-liner: "Amprius Technologies — small-cap ($420M) | Technology | Batteries"
            parts = [name]
            if cap_label and mcap_str:
                parts.append(f"{cap_label} ({mcap_str})")
            if sector:
                parts.append(sector)
            if industry and industry != sector:
                parts.append(industry)
            short_desc = " — ".join(parts[:2])
            if sector or industry:
                short_desc += " | " + " · ".join(
                    [s for s in [sector, industry] if s and s != sector or s == sector][:2]
                )

            # Truncate full summary to ~200 chars for display
            if full_summary and len(full_summary) > 200:
                # Cut at last sentence boundary within 200 chars
                cut = full_summary[:200]
                last_period = cut.rfind(".")
                if last_period > 80:
                    full_summary_short = cut[:last_period + 1]
                else:
                    full_summary_short = cut + "..."
            else:
                full_summary_short = full_summary

            company_info["short_desc"] = short_desc
            company_info["summary_short"] = full_summary_short

        return {
            "ticker": ticker,
            "score": round(total_score),
            "grade": grade,
            "price": round(price, 2),
            "signals": [(name, pts, detail, tf) for name, pts, detail, tf in signals],
            "details": details,
            "timeframe": timeframe_str,
            "timeframe_days": est_days,
            "timeframe_weights": tf_weights,
            "company": company_info,
        }

    # ── Individual Signal Scorers ─────────────────────────────

    def _bollinger_squeeze(self, closes: np.ndarray) -> Tuple[int, str]:
        """
        Detect Bollinger Band squeeze (width narrowing then expanding).
        A squeeze followed by upward expansion is a classic breakout signal.
        """
        if len(closes) < 30:
            return 0, "insufficient data"

        # Compute BB width over time
        period = 20
        widths = []
        for i in range(period, len(closes)):
            window = closes[i-period:i]
            sma = np.mean(window)
            std = np.std(window)
            width = (2 * std) / sma if sma > 0 else 0  # Normalized BB width
            widths.append(width)

        if len(widths) < 10:
            return 0, "insufficient data"

        current_width = widths[-1]
        avg_width = np.mean(widths)
        min_width_20d = min(widths[-20:]) if len(widths) >= 20 else min(widths)
        price = closes[-1]
        sma = np.mean(closes[-period:])

        # Squeeze: current width near recent minimum AND price above SMA
        squeeze_ratio = current_width / avg_width if avg_width > 0 else 1

        if squeeze_ratio < 0.5 and price > sma:
            # Tight squeeze + price above mid = strong setup
            return 15, f"tight squeeze ({squeeze_ratio:.2f}x avg), price above mid"
        elif squeeze_ratio < 0.65 and price > sma:
            return 10, f"squeeze forming ({squeeze_ratio:.2f}x avg)"
        elif squeeze_ratio < 0.75 and price > sma:
            return 5, f"mild compression ({squeeze_ratio:.2f}x avg)"
        elif current_width > avg_width * 1.3 and price > sma:
            # Just broke out of squeeze — expansion phase
            # Check if width was compressed recently
            if min(widths[-5:]) < avg_width * 0.7:
                return 12, "squeeze breakout in progress"
            return 3, f"wide bands, uptrend ({squeeze_ratio:.2f}x avg)"
        return 0, f"no squeeze ({squeeze_ratio:.2f}x avg)"

    def _volume_pattern(self, closes: np.ndarray, volumes: np.ndarray
                        ) -> Tuple[int, str, str]:
        """
        Volume accumulation: rising volume with price holding or climbing.
        Distribution: rising volume with price falling (bearish — no points).
        Returns (score, detail, timeframe).
        """
        if len(volumes) < 20:
            return 0, "insufficient data", "MEDIUM"

        avg_vol_20 = np.mean(volumes[-20:])
        avg_vol_5 = np.mean(volumes[-5:])
        avg_vol_2 = np.mean(volumes[-2:])
        vol_ratio = avg_vol_5 / avg_vol_20 if avg_vol_20 > 0 else 1

        # Price change over same periods
        pct_5d = (closes[-1] - closes[-5]) / closes[-5] if closes[-5] > 0 else 0
        pct_2d = (closes[-1] - closes[-2]) / closes[-2] if closes[-2] > 0 else 0

        # Best: volume surging with price rising
        if vol_ratio > 2.0 and pct_5d > 0.02:
            return 15, f"volume surge {vol_ratio:.1f}x + price up {pct_5d:+.1%}", "SHORT"
        elif vol_ratio > 1.5 and pct_5d > 0.01:
            return 12, f"strong accumulation {vol_ratio:.1f}x + up {pct_5d:+.1%}", "SHORT"
        elif vol_ratio > 1.3 and pct_5d > 0:
            return 8, f"building volume {vol_ratio:.1f}x", "MEDIUM"
        elif avg_vol_2 > avg_vol_20 * 1.8 and pct_2d > 0.01:
            return 10, f"recent volume spike {avg_vol_2/avg_vol_20:.1f}x", "SHORT"
        elif vol_ratio > 1.0 and pct_5d > 0:
            return 4, f"volume confirming ({vol_ratio:.1f}x)", "MEDIUM"
        elif vol_ratio < 0.6 and abs(pct_5d) < 0.02:
            return 3, f"quiet consolidation (vol={vol_ratio:.1f}x)", "MEDIUM"
        return 0, f"no accumulation pattern ({vol_ratio:.1f}x)", "MEDIUM"

    def _near_high(self, price: float, week52_high: float,
                   volumes: np.ndarray) -> Tuple[int, str]:
        """
        Proximity to 52-week high. Stocks that break to new highs
        with volume tend to continue (momentum effect).
        """
        if week52_high <= 0 or price <= 0:
            return 0, "no 52w data"

        pct_from_high = (week52_high - price) / week52_high

        if pct_from_high < 0:
            # ABOVE 52-week high — already breaking out
            return 15, f"NEW 52W HIGH! ({abs(pct_from_high):.1%} above)"
        elif pct_from_high < 0.02:
            return 13, f"within 2% of 52w high (${week52_high:.2f})"
        elif pct_from_high < 0.05:
            return 10, f"within 5% of 52w high (${week52_high:.2f})"
        elif pct_from_high < 0.10:
            return 6, f"within 10% of 52w high"
        elif pct_from_high < 0.20:
            return 2, f"{pct_from_high:.0%} below 52w high"
        return 0, f"{pct_from_high:.0%} below 52w high"

    def _rsi_momentum(self, closes: np.ndarray) -> Tuple[int, str]:
        """
        RSI in the sweet spot (45-65) and accelerating = breakout fuel.
        Below 30 = might bounce but not a breakout.
        Above 70 = already extended, risky entry.
        """
        if len(closes) < 20:
            return 0, "insufficient data"

        rsi = self._compute_rsi(closes, 14)
        rsi_prev = self._compute_rsi(closes[:-1], 14)
        rsi_accel = rsi - rsi_prev

        if 50 <= rsi <= 65 and rsi_accel > 0:
            # Sweet spot — momentum building but not overbought
            pts = 12 if rsi_accel > 3 else 8 if rsi_accel > 1 else 5
            return pts, f"RSI {rsi:.0f} accelerating (+{rsi_accel:.1f})"
        elif 40 <= rsi < 50 and rsi_accel > 2:
            return 6, f"RSI {rsi:.0f} recovering fast (+{rsi_accel:.1f})"
        elif 65 < rsi <= 75 and rsi_accel > 0:
            # Strong but getting extended
            return 4, f"RSI {rsi:.0f} strong (watch overbought)"
        elif rsi < 35 and rsi_accel > 0:
            # Bounce from oversold — could be start of move
            return 3, f"RSI {rsi:.0f} bouncing from oversold"
        elif rsi > 75:
            return 0, f"RSI {rsi:.0f} overbought — extended"
        return 0, f"RSI {rsi:.0f} (neutral)"

    def _macd_signal(self, closes: np.ndarray) -> Tuple[int, str]:
        """
        MACD histogram turning positive or expanding = momentum confirmation.
        """
        if len(closes) < 30:
            return 0, "insufficient data"

        hist_now = self._compute_macd_hist(closes)
        hist_prev = self._compute_macd_hist(closes[:-1])
        hist_prev2 = self._compute_macd_hist(closes[:-2]) if len(closes) > 28 else hist_prev

        # Bullish crossover (histogram crosses zero)
        if hist_now > 0 and hist_prev <= 0:
            return 12, "bullish crossover!"
        elif hist_now > 0 and hist_now > hist_prev > 0:
            # Expanding positive histogram — momentum building
            expansion = hist_now - hist_prev
            if expansion > abs(hist_prev) * 0.5:
                return 10, f"histogram expanding strongly (+{hist_now:.3f})"
            return 6, f"histogram expanding (+{hist_now:.3f})"
        elif hist_now > hist_prev and hist_prev > hist_prev2 and hist_now > -0.1:
            # Three consecutive higher histograms — turning bullish
            return 5, f"momentum building (hist {hist_now:.3f})"
        elif hist_now > 0:
            return 3, f"MACD positive ({hist_now:.3f})"
        return 0, f"MACD bearish ({hist_now:.3f})"

    def _ma_alignment(self, closes: np.ndarray) -> Tuple[int, str, str]:
        """
        Moving average alignment: price > 20MA > 50MA = uptrend.
        Best breakouts happen when MAs align and price pulls back to test.
        Returns (score, detail, timeframe).
        """
        if len(closes) < 50:
            return 0, "insufficient data", "MEDIUM"

        price = closes[-1]
        ma20 = np.mean(closes[-20:])
        ma50 = np.mean(closes[-50:])

        above_20 = price > ma20
        above_50 = price > ma50
        ma20_above_50 = ma20 > ma50

        # Check for pullback to MA (price was higher, came back near MA)
        recent_high = np.max(closes[-10:])
        pullback_to_ma20 = (price - ma20) / ma20 if ma20 > 0 else 0

        if above_20 and above_50 and ma20_above_50:
            if 0 < pullback_to_ma20 < 0.01:
                return 12, f"perfect pullback to 20MA — BUY zone", "MEDIUM"
            elif pullback_to_ma20 < 0.03:
                return 10, f"uptrend + near 20MA support", "MEDIUM"
            return 7, f"bullish alignment (price>20>50)", "LONG"
        elif above_50 and not above_20 and ma20_above_50:
            return 5, f"pullback — testing 20MA from below", "MEDIUM"
        elif above_20 and not above_50:
            return 3, f"above 20MA, below 50MA — recovery", "LONG"
        elif above_50 and not ma20_above_50:
            return 2, f"above 50MA but MAs crossing", "LONG"
        return 0, f"below key MAs", "LONG"

    def _range_compression(self, highs: np.ndarray, lows: np.ndarray,
                           closes: np.ndarray) -> Tuple[int, str, str]:
        """
        ATR compression followed by expansion = breakout signal.
        Returns (score, detail, timeframe).
        """
        if len(closes) < 20:
            return 0, "insufficient data", "MEDIUM"

        # Compute ATR over time
        atrs = []
        for i in range(1, len(closes)):
            tr = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i-1]),
                abs(lows[i] - closes[i-1])
            )
            atrs.append(tr)

        if len(atrs) < 20:
            return 0, "insufficient data", "MEDIUM"

        # Normalize ATR by price
        norm_atrs = [a / closes[i+1] if closes[i+1] > 0 else 0
                     for i, a in enumerate(atrs)]

        recent_atr = np.mean(norm_atrs[-5:])
        avg_atr = np.mean(norm_atrs[-20:])
        long_atr = np.mean(norm_atrs) if len(norm_atrs) > 30 else avg_atr

        compression = recent_atr / avg_atr if avg_atr > 0 else 1

        # Last 2 days expanding from compression
        last2_atr = np.mean(norm_atrs[-2:])
        last5_atr = np.mean(norm_atrs[-5:])
        expanding = last2_atr > last5_atr * 1.2

        price_up = closes[-1] > closes[-3] if len(closes) > 3 else False

        if compression < 0.6 and not expanding:
            # Still compressed — setup forming but not triggered yet
            return 7, f"range compressed ({compression:.2f}x avg) — coiling", "MEDIUM"
        elif compression < 0.8 and expanding and price_up:
            # Compression breaking upward — imminent
            return 10, f"range expanding UP from squeeze", "SHORT"
        elif expanding and price_up and compression < 1.2:
            return 5, f"range expanding with upside", "SHORT"
        return 0, f"no compression ({compression:.2f}x avg)", "MEDIUM"

    def _relative_strength(self, closes: np.ndarray) -> Tuple[int, str]:
        """
        Relative strength vs SPY. Stocks outperforming the market
        tend to continue outperforming (momentum factor).
        """
        if self._spy_data is None or len(closes) < 20:
            return 0, "no SPY data"

        spy = self._spy_data
        # Align lengths
        min_len = min(len(closes), len(spy))
        if min_len < 20:
            return 0, "insufficient overlap"

        stock_return_20d = (closes[-1] - closes[-min_len]) / closes[-min_len]
        spy_return_20d = (spy[-1] - spy[-min_len]) / spy[-min_len]
        relative = stock_return_20d - spy_return_20d

        stock_5d = (closes[-1] - closes[-5]) / closes[-5] if len(closes) >= 5 else 0
        spy_5d = (spy[-1] - spy[-5]) / spy[-5] if len(spy) >= 5 else 0
        rel_5d = stock_5d - spy_5d

        if relative > 0.10 and rel_5d > 0.02:
            return 9, f"strong RS ({relative:+.1%} vs SPY, accelerating)"
        elif relative > 0.05 and rel_5d > 0:
            return 7, f"outperforming SPY ({relative:+.1%})"
        elif relative > 0.02:
            return 4, f"slight RS ({relative:+.1%} vs SPY)"
        elif relative > 0:
            return 2, f"in-line with SPY ({relative:+.1%})"
        return 0, f"underperforming SPY ({relative:+.1%})"

    # ── Utility Functions ─────────────────────────────────────

    def _load_spy_data(self):
        """Pre-fetch SPY data for relative strength comparison."""
        try:
            if self._spy_data is None:
                import yfinance as yf
                spy = yf.Ticker("SPY")
                hist = spy.history(period="6mo")
                if not hist.empty:
                    self._spy_data = hist["Close"].values.astype(float)
        except Exception:
            self._spy_data = None

    @staticmethod
    def _compute_rsi(closes: np.ndarray, period: int = 14) -> float:
        if len(closes) < period + 1:
            return 50.0
        deltas = np.diff(closes)
        gains = np.maximum(deltas, 0)
        losses = np.abs(np.minimum(deltas, 0))

        avg_gain = np.mean(gains[-period:])
        avg_loss = np.mean(losses[-period:])

        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    @staticmethod
    def _compute_macd_hist(closes: np.ndarray) -> float:
        if len(closes) < 26:
            return 0.0
        ema12 = BreakoutDetector._ema(closes, 12)
        ema26 = BreakoutDetector._ema(closes, 26)
        macd_line = ema12 - ema26
        # Signal line would need full history; approximate with recent
        return macd_line  # Simplified — histogram ≈ MACD line strength

    @staticmethod
    def _ema(data: np.ndarray, period: int) -> float:
        if len(data) < period:
            return float(data[-1])
        multiplier = 2 / (period + 1)
        ema = float(data[0])
        for val in data[1:]:
            ema = (float(val) - ema) * multiplier + ema
        return ema
