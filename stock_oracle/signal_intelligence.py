"""
Signal Intelligence Module
============================
Learns signal behavior over time and adjusts predictions accordingly.

Key capabilities:
1. STALE SIGNAL DETECTION — collectors that return the same value every scan
   get their weight suppressed. If insider_ratio is always +0.45 for AAPL,
   it's a company characteristic, not a trading signal.

2. VOLATILITY-ADAPTIVE THRESHOLDS — LUNR (1% avg move) needs a much higher
   signal threshold for a conviction call than BND (0.06% avg move).

3. SIGNAL DETRENDING — subtracts each collector's running mean per ticker,
   so only CHANGES in signal value contribute to predictions.

4. DYNAMIC CONFIDENCE — based on how many non-stale, changing signals agree,
   not the raw number of collectors.

Persistence: Saves learned stats to disk so knowledge accumulates across sessions.
"""
import json
import logging
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger("stock_oracle")

INTELLIGENCE_FILE = Path("stock_oracle/data/signal_intelligence.json")

# ── Configuration ─────────────────────────────────────────────
# How many consecutive identical readings before a signal is "stale"
STALE_REPEAT_THRESHOLD = 3
# Weight multiplier for stale signals (0.0 = fully suppressed)
STALE_WEIGHT_MULT = 0.05
# Minimum number of unique values to consider a signal "dynamic"
MIN_UNIQUE_FOR_DYNAMIC = 4
# Exponential moving average decay for volatility tracking
VOLATILITY_EMA_ALPHA = 0.1
# Base threshold for BULL/BEAR classification
BASE_CONVICTION_THRESHOLD = 0.06
# Volatility scaling: threshold = BASE + vol_mult * ticker_volatility
VOLATILITY_THRESHOLD_MULT = 0.3
# Max threshold (don't set so high we never make calls)
MAX_CONVICTION_THRESHOLD = 0.25
# Min threshold (don't go below this even for very stable ETFs)
MIN_CONVICTION_THRESHOLD = 0.04
# Detrending: how much of the signal is "change from mean" vs raw
# 0.0 = all raw, 1.0 = all detrended
DETREND_RATIO = 0.3
# Minimum non-stale dynamic signals required to make a conviction call
# If fewer than this are dynamic, force NEUTRAL regardless of signal
MIN_DYNAMIC_FOR_CONVICTION = 5


class SignalIntelligence:
    """
    Learns which signals actually change intraday and adjusts
    prediction weights accordingly.
    """

    def __init__(self):
        # Per-ticker per-collector signal history (in-memory, current session)
        # {ticker: {collector: [val1, val2, val3, ...]}}
        self._signal_history: Dict[str, Dict[str, List[float]]] = defaultdict(
            lambda: defaultdict(list)
        )

        # Per-ticker per-collector running mean (for detrending)
        # {ticker: {collector: mean_value}}
        self._signal_means: Dict[str, Dict[str, float]] = defaultdict(dict)

        # Per-ticker volatility (EMA of absolute price changes)
        # {ticker: volatility_pct}
        self._volatility: Dict[str, float] = {}

        # Per-ticker price history (for volatility calc)
        self._price_history: Dict[str, List[float]] = defaultdict(list)

        # Per-ticker conviction thresholds (derived from volatility)
        self._thresholds: Dict[str, float] = {}

        # Per-collector staleness flags
        # {ticker: {collector: is_stale}}
        self._staleness: Dict[str, Dict[str, bool]] = defaultdict(dict)

        # Accumulated stats for persistence
        self._stats = {
            "total_scans": 0,
            "stale_detections": 0,
            "conviction_calls_made": 0,
            "conviction_calls_correct": 0,
        }

        # Current market session
        self._current_session = "regular"

        # Load persisted intelligence
        self._load()

    # ── Core API ──────────────────────────────────────────────

    def update(self, ticker: str, signals: List[Dict], price: float,
               market_session: str = "regular"):
        """
        Called after each scan. Updates signal history, staleness,
        and volatility for this ticker.

        Args:
            ticker: Stock symbol
            signals: List of signal dicts [{collector, signal, confidence}, ...]
            price: Current price
            market_session: 'regular', 'pre_market', 'after_hours', or 'closed'
        """
        self._current_session = market_session

        # Update price history and volatility
        # During extended hours, many prices are stale — only update vol
        # if price actually changed
        self._price_history[ticker].append(price)
        if len(self._price_history[ticker]) > 2:
            prices = self._price_history[ticker]
            pct_change = abs(prices[-1] - prices[-2]) / prices[-2]
            # Skip vol update on zero moves (stale extended hours price)
            if pct_change > 0.00001:
                old_vol = self._volatility.get(ticker, pct_change)
                self._volatility[ticker] = (
                    VOLATILITY_EMA_ALPHA * pct_change +
                    (1 - VOLATILITY_EMA_ALPHA) * old_vol
                )
        # Keep last 100 prices in memory
        if len(self._price_history[ticker]) > 100:
            self._price_history[ticker] = self._price_history[ticker][-100:]

        # Update conviction threshold based on volatility + market session
        vol = self._volatility.get(ticker, 0.002)
        raw_threshold = BASE_CONVICTION_THRESHOLD + VOLATILITY_THRESHOLD_MULT * vol

        # Extended hours: raise threshold significantly
        # After-hours and pre-market have thin volume, stale prices,
        # and wider spreads — require much stronger signals for conviction
        if market_session in ("after_hours", "pre_market"):
            raw_threshold *= 1.5  # 50% higher bar during extended hours
        elif market_session == "closed":
            raw_threshold *= 2.0  # Double bar when market closed

        self._thresholds[ticker] = max(
            MIN_CONVICTION_THRESHOLD,
            min(MAX_CONVICTION_THRESHOLD, raw_threshold)
        )

        # Update signal history and staleness
        for sig in signals:
            collector = sig.get("collector", "")
            value = round(sig.get("signal", 0), 4)
            confidence = sig.get("confidence", 0)

            if confidence < 0.01:
                continue

            history = self._signal_history[ticker][collector]
            history.append(value)

            # Keep last 50 values
            if len(history) > 50:
                self._signal_history[ticker][collector] = history[-50:]
                history = self._signal_history[ticker][collector]

            # Update running mean
            self._signal_means[ticker][collector] = sum(history) / len(history)

            # Detect staleness: same value N times in a row
            if len(history) >= STALE_REPEAT_THRESHOLD:
                recent = history[-STALE_REPEAT_THRESHOLD:]
                is_stale = len(set(recent)) == 1
                # Also stale if <MIN_UNIQUE unique values ever
                unique_vals = len(set(history))
                if unique_vals < MIN_UNIQUE_FOR_DYNAMIC and len(history) >= 10:
                    is_stale = True

                was_stale = self._staleness[ticker].get(collector, False)
                self._staleness[ticker][collector] = is_stale

                if is_stale and not was_stale:
                    self._stats["stale_detections"] += 1
                    logger.debug(
                        f"Signal intelligence: {collector} for {ticker} marked STALE "
                        f"(value={value:+.4f}, {unique_vals} unique in {len(history)} scans)"
                    )

        self._stats["total_scans"] += 1

    def get_adjusted_signals(self, ticker: str, signals: List[Dict]) -> List[Dict]:
        """
        Returns signals with staleness-adjusted weights and detrended values.
        Adds metadata to each signal dict (non-destructive — adds new keys).

        Returns a NEW list of signal dicts with added keys:
          - _stale: bool
          - _detrended_signal: float (signal minus per-ticker mean)
          - _weight_mult: float (1.0 for dynamic, STALE_WEIGHT_MULT for stale)
          - _freshness: float (0.0 stale → 1.0 very dynamic)
        """
        adjusted = []
        for sig in signals:
            collector = sig.get("collector", "")
            value = sig.get("signal", 0)
            confidence = sig.get("confidence", 0)

            # Copy signal dict and add intelligence metadata
            out = dict(sig)

            is_stale = self._staleness.get(ticker, {}).get(collector, False)
            mean = self._signal_means.get(ticker, {}).get(collector, 0)
            history = self._signal_history.get(ticker, {}).get(collector, [])

            # Detrended signal: how much this reading differs from the average
            detrended = value - mean

            # Freshness: how variable is this collector for this ticker?
            if len(history) >= 5:
                unique_ratio = len(set(round(v, 4) for v in history)) / len(history)
            else:
                unique_ratio = 0.5  # Unknown — neutral

            # Weight multiplier
            if is_stale:
                weight_mult = STALE_WEIGHT_MULT
            else:
                weight_mult = min(1.0, 0.3 + 0.7 * unique_ratio)

            out["_stale"] = is_stale
            out["_detrended_signal"] = round(detrended, 6)
            out["_detrend_ratio"] = DETREND_RATIO
            out["_weight_mult"] = round(weight_mult, 4)
            out["_freshness"] = round(unique_ratio, 4)

            adjusted.append(out)

        return adjusted

    def get_conviction_threshold(self, ticker: str) -> float:
        """
        Get the volatility-adaptive conviction threshold for a ticker.
        Higher volatility → higher threshold needed for BULL/BEAR call.
        """
        return self._thresholds.get(ticker, BASE_CONVICTION_THRESHOLD)

    def get_min_dynamic_for_conviction(self) -> int:
        """Minimum non-stale signals required before making a conviction call."""
        return MIN_DYNAMIC_FOR_CONVICTION

    def get_market_session(self) -> str:
        """Get current market session as last seen by update()."""
        return self._current_session

    def get_session_confidence_mult(self) -> float:
        """
        Confidence multiplier based on market session.
        Regular hours: 1.0 (full confidence in data quality)
        Pre-market/After-hours: 0.6 (thin volume, stale prices, wider spreads)
        Closed: 0.3 (data is completely stale)
        """
        return {
            "regular": 1.0,
            "pre_market": 0.6,
            "after_hours": 0.6,
            "closed": 0.3,
        }.get(self._current_session, 0.5)

    def get_volatility(self, ticker: str) -> float:
        """Get current volatility estimate for a ticker (as pct, e.g. 0.003 = 0.3%)."""
        return self._volatility.get(ticker, 0.002)

    def get_dynamic_signal_count(self, ticker: str) -> int:
        """How many non-stale collectors are active for this ticker."""
        staleness = self._staleness.get(ticker, {})
        return sum(1 for v in staleness.values() if not v)

    def get_stale_collectors(self, ticker: str) -> List[str]:
        """List collectors flagged as stale for this ticker."""
        return [c for c, stale in self._staleness.get(ticker, {}).items() if stale]

    def get_status(self) -> Dict:
        """Get intelligence status for display / Claude advisor."""
        total_staleness = {}
        for ticker, colls in self._staleness.items():
            for coll, is_stale in colls.items():
                if coll not in total_staleness:
                    total_staleness[coll] = {"stale": 0, "dynamic": 0}
                if is_stale:
                    total_staleness[coll]["stale"] += 1
                else:
                    total_staleness[coll]["dynamic"] += 1

        stale_summary = {
            c: f"{s['stale']}/{s['stale']+s['dynamic']} tickers stale"
            for c, s in total_staleness.items()
            if s["stale"] > s["dynamic"]  # Mostly stale
        }

        vol_summary = {
            t: f"{v*100:.3f}%"
            for t, v in sorted(self._volatility.items())
        }

        threshold_summary = {
            t: f"±{v:.4f}"
            for t, v in sorted(self._thresholds.items())
        }

        return {
            "total_scans": self._stats["total_scans"],
            "stale_detections": self._stats["stale_detections"],
            "mostly_stale_collectors": stale_summary,
            "volatilities": vol_summary,
            "thresholds": threshold_summary,
            "conviction_calls": self._stats["conviction_calls_made"],
            "conviction_accuracy": (
                round(self._stats["conviction_calls_correct"] /
                      max(self._stats["conviction_calls_made"], 1) * 100, 1)
            ),
        }

    def record_conviction_result(self, correct: bool):
        """Track accuracy of conviction (non-NEUTRAL) calls."""
        self._stats["conviction_calls_made"] += 1
        if correct:
            self._stats["conviction_calls_correct"] += 1

    # ── Persistence ───────────────────────────────────────────

    def _load(self):
        """Load persisted intelligence from disk."""
        try:
            if INTELLIGENCE_FILE.exists():
                with open(INTELLIGENCE_FILE) as f:
                    data = json.load(f)
                self._volatility = data.get("volatility", {})
                self._thresholds = data.get("thresholds", {})
                self._stats = data.get("stats", self._stats)
                # Reconstruct signal means from persisted data
                self._signal_means = defaultdict(dict)
                for ticker, colls in data.get("signal_means", {}).items():
                    for coll, mean in colls.items():
                        self._signal_means[ticker][coll] = mean
                logger.info(
                    f"Signal intelligence loaded: {len(self._volatility)} tickers, "
                    f"{self._stats['total_scans']} historical scans"
                )
        except Exception as e:
            logger.warning(f"Could not load signal intelligence: {e}")

    def save(self):
        """Persist learned intelligence to disk."""
        try:
            INTELLIGENCE_FILE.parent.mkdir(parents=True, exist_ok=True)
            data = {
                "last_saved": datetime.now(timezone.utc).isoformat(),
                "volatility": self._volatility,
                "thresholds": self._thresholds,
                "signal_means": dict(self._signal_means),
                "stats": self._stats,
            }
            with open(INTELLIGENCE_FILE, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save signal intelligence: {e}")
