"""
Market Regime Detector
========================
Detects whether the broad market is in a selloff, rally, or ranging
condition by analyzing SPY, VIX proxy, and sector breadth.

This is critical for 5-day predictions: when 99% of stocks are falling,
individual stock signals don't matter — everything goes down together.

Regimes:
  SELLOFF   — broad market declining, high correlation, risk-off
  RALLY     — broad market rising, risk-on, momentum up
  VOLATILE  — large swings both directions, high VIX
  RANGING   — sideways, normal conditions

Usage:
    detector = MarketRegimeDetector()
    regime = detector.detect()
    # regime = {"regime": "SELLOFF", "confidence": 0.85,
    #           "market_return_5d": -0.034, "breadth": 0.12, ...}
"""
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Optional

import numpy as np

logger = logging.getLogger("stock_oracle")

# Cache TTL: recompute regime every 10 minutes
REGIME_CACHE_TTL = 600


class MarketRegimeDetector:
    """
    Detects broad market regime from SPY price action and breadth.
    """

    def __init__(self):
        self._cache: Optional[Dict] = None
        self._cache_time: float = 0

    def detect(self) -> Dict:
        """
        Detect current market regime.
        Returns dict with regime classification and supporting data.
        """
        # Use cache if fresh
        if self._cache and time.time() - self._cache_time < REGIME_CACHE_TTL:
            return self._cache

        try:
            result = self._compute_regime()
            self._cache = result
            self._cache_time = time.time()
            return result
        except Exception as e:
            logger.debug(f"Regime detection error: {e}")
            return {
                "regime": "UNKNOWN",
                "confidence": 0,
                "bias": 0,
                "detail": f"Detection failed: {e}",
            }

    def _compute_regime(self) -> Dict:
        """Core regime computation from market data."""
        import yfinance as yf

        # Fetch SPY (broad market proxy)
        spy = yf.Ticker("SPY")
        hist = spy.history(period="1mo")

        if hist.empty or len(hist) < 10:
            return {"regime": "UNKNOWN", "confidence": 0, "bias": 0,
                    "detail": "Insufficient SPY data"}

        closes = hist["Close"].values.astype(float)
        volumes = hist["Volume"].values.astype(float)

        # ── Multi-timeframe returns ──
        ret_1d = (closes[-1] - closes[-2]) / closes[-2] if len(closes) >= 2 else 0
        ret_3d = (closes[-1] - closes[-3]) / closes[-3] if len(closes) >= 3 else 0
        ret_5d = (closes[-1] - closes[-5]) / closes[-5] if len(closes) >= 5 else 0
        ret_10d = (closes[-1] - closes[-10]) / closes[-10] if len(closes) >= 10 else 0

        # ── Volatility (rolling 5-day std of returns) ──
        if len(closes) >= 6:
            daily_returns = np.diff(closes[-6:]) / closes[-6:-1]
            volatility = float(np.std(daily_returns))
        else:
            volatility = 0.01

        # ── Trend strength (price vs moving averages) ──
        ma5 = np.mean(closes[-5:]) if len(closes) >= 5 else closes[-1]
        ma10 = np.mean(closes[-10:]) if len(closes) >= 10 else closes[-1]
        ma20 = np.mean(closes[-20:]) if len(closes) >= 20 else closes[-1]
        price = closes[-1]

        above_ma5 = price > ma5
        above_ma10 = price > ma10
        above_ma20 = price > ma20
        ma_score = sum([above_ma5, above_ma10, above_ma20])  # 0-3

        # ── Breadth: fetch a few sector ETFs to see if decline is broad ──
        breadth_up = 0
        breadth_total = 0
        sector_etfs = ["XLK", "XLF", "XLE", "XLV", "XLI", "XLC", "XLY", "XLP", "XLU"]

        for etf in sector_etfs:
            try:
                h = yf.Ticker(etf).history(period="6d")
                if h is not None and len(h) >= 2:
                    breadth_total += 1
                    chg = (h["Close"].iloc[-1] - h["Close"].iloc[-2]) / h["Close"].iloc[-2]
                    if chg > 0.001:
                        breadth_up += 1
            except Exception:
                pass

        breadth_ratio = breadth_up / max(breadth_total, 1)

        # ── 5-day breadth (for 5-day prediction window) ──
        breadth_5d_up = 0
        breadth_5d_total = 0
        for etf in sector_etfs:
            try:
                h = yf.Ticker(etf).history(period="6d")
                if h is not None and len(h) >= 5:
                    breadth_5d_total += 1
                    chg = (h["Close"].iloc[-1] - h["Close"].iloc[-5]) / h["Close"].iloc[-5]
                    if chg > 0:
                        breadth_5d_up += 1
            except Exception:
                pass

        breadth_5d_ratio = breadth_5d_up / max(breadth_5d_total, 1)

        # ── Classify regime ──
        score = 0  # Negative = bearish, positive = bullish

        # Multi-timeframe momentum
        if ret_5d < -0.03:
            score -= 3  # Strong 5d decline
        elif ret_5d < -0.015:
            score -= 2
        elif ret_5d < -0.005:
            score -= 1
        elif ret_5d > 0.03:
            score += 3
        elif ret_5d > 0.015:
            score += 2
        elif ret_5d > 0.005:
            score += 1

        if ret_10d < -0.05:
            score -= 2
        elif ret_10d < -0.02:
            score -= 1
        elif ret_10d > 0.05:
            score += 2
        elif ret_10d > 0.02:
            score += 1

        # MA alignment
        if ma_score == 0:
            score -= 2  # Below all MAs
        elif ma_score == 3:
            score += 2  # Above all MAs

        # Breadth
        if breadth_ratio < 0.25:
            score -= 2  # Very few sectors up today
        elif breadth_ratio > 0.75:
            score += 2

        if breadth_5d_ratio < 0.25:
            score -= 2  # Almost no sectors up over 5 days
        elif breadth_5d_ratio > 0.75:
            score += 2

        # High volatility indicator
        is_volatile = volatility > 0.015  # >1.5% daily std

        # Determine regime
        if score <= -5:
            regime = "SELLOFF"
            confidence = min(1.0, abs(score) / 8)
        elif score <= -2:
            regime = "DECLINING"
            confidence = min(1.0, abs(score) / 6)
        elif score >= 5:
            regime = "RALLY"
            confidence = min(1.0, score / 8)
        elif score >= 2:
            regime = "RISING"
            confidence = min(1.0, score / 6)
        elif is_volatile:
            regime = "VOLATILE"
            confidence = 0.6
        else:
            regime = "RANGING"
            confidence = 0.5

        # Prediction bias: how much to shift all predictions
        # In a selloff, even "neutral" signals should lean bearish
        if regime == "SELLOFF":
            bias = -0.06 * confidence  # Strong bearish bias
        elif regime == "DECLINING":
            bias = -0.03 * confidence
        elif regime == "RALLY":
            bias = +0.06 * confidence
        elif regime == "RISING":
            bias = +0.03 * confidence
        else:
            bias = 0.0

        result = {
            "regime": regime,
            "confidence": round(confidence, 3),
            "score": score,
            "bias": round(bias, 4),
            "spy_1d": round(ret_1d, 4),
            "spy_3d": round(ret_3d, 4),
            "spy_5d": round(ret_5d, 4),
            "spy_10d": round(ret_10d, 4),
            "spy_price": round(price, 2),
            "volatility": round(volatility, 4),
            "ma_score": ma_score,
            "breadth_1d": round(breadth_ratio, 2),
            "breadth_5d": round(breadth_5d_ratio, 2),
            "detail": (
                f"{regime} (score={score}, SPY 5d={ret_5d:+.1%}, "
                f"breadth={breadth_5d_ratio:.0%}, vol={volatility:.2%})"
            ),
        }

        logger.info(f"Market regime: {result['detail']}")
        return result

    def get_prediction_bias(self) -> float:
        """
        Get the signal bias to apply to all predictions.
        Negative = bearish shift, positive = bullish shift.
        """
        regime = self.detect()
        return regime.get("bias", 0)

    def should_force_bearish(self) -> bool:
        """True if the market is in a strong enough selloff to override neutral."""
        regime = self.detect()
        return regime.get("regime") == "SELLOFF" and regime.get("confidence", 0) > 0.6
