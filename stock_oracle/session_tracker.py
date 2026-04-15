"""
Intraday Session Tracker
=========================
Tracks predictions throughout a monitoring session, verifies short-term
outcomes (2-3 scans later), and builds intraday trend data.

Works alongside PredictionTracker (5-day horizon) to give both:
  - Real-time feedback: "Was my prediction from 10 minutes ago right?"
  - Trend tracking: "Is AAPL getting more bullish or bearish over this session?"

Each monitoring scan creates a snapshot. After N scans, we can verify
whether earlier predictions were directionally correct in the short term.

Storage:
  data/sessions/session_YYYYMMDD_HHMMSS.jsonl  — scan snapshots
  data/sessions/intraday_verified.jsonl         — verified intraday predictions
"""
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from stock_oracle.config import DATA_DIR

logger = logging.getLogger("stock_oracle")

SESSIONS_DIR = DATA_DIR / "sessions"
SESSIONS_DIR.mkdir(exist_ok=True)

INTRADAY_VERIFIED_FILE = SESSIONS_DIR / "intraday_verified.jsonl"

# How many scans back to verify (e.g., 3 scans at 300s = 15 min)
VERIFY_SCANS_BACK = 3
# Intraday movement threshold (±0.3% is meaningful for 15-min window)
INTRADAY_THRESHOLD = 0.003


class SessionTracker:
    """
    Tracks a single monitoring session. Lives in memory during monitoring,
    persists snapshots to disk for ML training.
    """

    def __init__(self):
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.session_file = SESSIONS_DIR / f"session_{self.session_id}.jsonl"
        self.scan_number = 0

        # In-memory history: ticker -> list of scan snapshots
        self.history: Dict[str, List[Dict]] = {}

        # Intraday accuracy tracking
        self.intraday_stats = {
            "verified": 0, "correct": 0, "directional": 0, "total_scans": 0,
        }

        # Per-ticker trend data
        self.trends: Dict[str, Dict] = {}

    def record_scan(self, results: Dict[str, Dict]):
        """
        Record an entire scan's results. Called after each monitoring cycle.
        
        results: {ticker: analysis_result_dict, ...}
        """
        self.scan_number += 1
        self.intraday_stats["total_scans"] = self.scan_number
        timestamp = datetime.now(timezone.utc).isoformat()

        for ticker, result in results.items():
            price = self._extract_price(result)
            signal = result.get("signal", 0)
            prediction = result.get("prediction", "NEUTRAL")
            confidence = result.get("confidence", 0)
            core = result.get("weighted_prediction", {}).get("core_analysis_score", 0)

            # Save full signals for ML training (compact: just collector/signal/confidence)
            compact_signals = [
                {"collector": s.get("collector", ""), "signal": s.get("signal", 0),
                 "confidence": s.get("confidence", 0)}
                for s in result.get("signals", [])
            ]

            snapshot = {
                "scan": self.scan_number,
                "timestamp": timestamp,
                "ticker": ticker,
                "price": price,
                "signal": signal,
                "prediction": prediction,
                "confidence": confidence,
                "core_conviction": core,
                "conviction_threshold": result.get("conviction_threshold", 0.06),
                "dynamic_signals": result.get("dynamic_signals", 0),
                "stale_signals": result.get("stale_signals", 0),
                "volatility": result.get("volatility", 0),
                "market_session": result.get("market_session", "regular"),
                "market_regime": result.get("market_regime", ""),
                "regime_bias": result.get("regime_bias", 0),
                "signals": compact_signals,  # Full signal vector for ML
            }

            # Append to in-memory history
            if ticker not in self.history:
                self.history[ticker] = []
            self.history[ticker].append(snapshot)

            # Trim to last 50 scans to prevent unbounded memory growth
            # (~8 hours at 300s intervals). Disk file retains everything.
            if len(self.history[ticker]) > 50:
                self.history[ticker] = self.history[ticker][-50:]

            # Update trend
            self._update_trend(ticker)

            # Save snapshot to disk (full signals included)
            try:
                SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
                with open(self.session_file, "a", encoding="utf-8") as f:
                    f.write(json.dumps(snapshot, default=str) + "\n")
            except Exception:
                pass

        # After recording, verify predictions from N scans ago
        if self.scan_number >= VERIFY_SCANS_BACK + 1:
            self._verify_intraday()

    def _extract_price(self, result: Dict) -> float:
        """Get price from result signals."""
        for s in result.get("signals", []):
            if s.get("collector") in ("finnhub_realtime", "yahoo_finance"):
                raw = s.get("raw_data") or {}
                if isinstance(raw, dict) and raw.get("price"):
                    return float(raw["price"])
        price_data = result.get("price_data", [])
        if price_data:
            return float(price_data[-1].get("close", 0))
        return 0.0

    def _update_trend(self, ticker: str):
        """
        Compute intraday trend for a ticker based on scan history.
        
        Looks at:
        - Signal direction over last N scans
        - Price movement
        - Whether signal is strengthening or weakening
        """
        history = self.history.get(ticker, [])
        if len(history) < 2:
            self.trends[ticker] = {
                "direction": "new",
                "signal_trend": 0,
                "price_trend": 0,
                "scans": len(history),
                "arrow": "--",
                "detail": "First scan",
            }
            return

        current = history[-1]
        recent = history[-min(5, len(history)):]  # Last 5 scans

        # Signal trend: is signal getting more bullish or bearish?
        signals = [s["signal"] for s in recent]
        if len(signals) >= 2:
            signal_slope = (signals[-1] - signals[0]) / len(signals)
        else:
            signal_slope = 0

        # Price trend
        prices = [s["price"] for s in recent if s["price"] > 0]
        if len(prices) >= 2:
            price_change = (prices[-1] - prices[0]) / prices[0]
        else:
            price_change = 0

        # Conviction trend
        convictions = [s.get("core_conviction", 0) for s in recent]
        conv_change = convictions[-1] - convictions[0] if len(convictions) >= 2 else 0

        # Classify trend
        if signal_slope > 0.005:
            if current["signal"] > 0:
                direction = "strengthening_bull"
                arrow = "++ "
            else:
                direction = "recovering"
                arrow = "+ "
        elif signal_slope < -0.005:
            if current["signal"] < 0:
                direction = "strengthening_bear"
                arrow = "-- "
            else:
                direction = "weakening"
                arrow = "- "
        else:
            direction = "stable"
            arrow = "= "

        # Detail string for GUI
        detail_parts = []
        if abs(price_change) > 0.001:
            detail_parts.append(f"Price {price_change:+.2%}")
        if abs(signal_slope) > 0.001:
            detail_parts.append(f"Sig {signal_slope:+.4f}/scan")
        if abs(conv_change) > 0.05:
            detail_parts.append(f"Conv {conv_change:+.0%}")

        self.trends[ticker] = {
            "direction": direction,
            "signal_trend": round(signal_slope, 4),
            "price_trend": round(price_change, 4),
            "price_change_pct": round(price_change * 100, 2),
            "conv_change": round(conv_change, 3),
            "scans": len(history),
            "arrow": arrow,
            "detail": " | ".join(detail_parts) if detail_parts else "Stable",
            "signals_history": signals[-10:],  # Last 10 for sparkline
            "prices_history": prices[-10:],
        }

    def _verify_intraday(self):
        """
        Verify predictions from VERIFY_SCANS_BACK scans ago.
        Uses short-term price movement as ground truth.
        """
        for ticker, history in self.history.items():
            if len(history) < VERIFY_SCANS_BACK + 1:
                continue

            # Get the prediction from N scans ago
            old = history[-(VERIFY_SCANS_BACK + 1)]
            current = history[-1]

            old_price = old.get("price", 0)
            new_price = current.get("price", 0)

            if old_price <= 0 or new_price <= 0:
                continue

            pct_change = (new_price - old_price) / old_price
            prediction = old.get("prediction", "NEUTRAL")
            signal = old.get("signal", 0)

            # Session-aware threshold: after-hours moves are noisier
            # and often just bid/ask bounce, not real direction
            session = old.get("market_session", "regular")
            if session in ("after_hours", "pre_market"):
                threshold = INTRADAY_THRESHOLD * 2  # ±0.6% for extended hours
            elif session == "closed":
                threshold = INTRADAY_THRESHOLD * 3  # ±0.9% when closed
            else:
                threshold = INTRADAY_THRESHOLD  # ±0.3% regular hours

            # Determine actual short-term outcome
            if pct_change > threshold:
                actual = "BULLISH"
            elif pct_change < -threshold:
                actual = "BEARISH"
            else:
                actual = "NEUTRAL"

            exact = prediction == actual
            directional = (
                (prediction == "BULLISH" and pct_change > 0) or
                (prediction == "BEARISH" and pct_change < 0) or
                (prediction == "NEUTRAL" and abs(pct_change) < threshold * 2)
            )

            self.intraday_stats["verified"] += 1
            if exact:
                self.intraday_stats["correct"] += 1
            if directional:
                self.intraday_stats["directional"] += 1

            # Save to disk for ML training
            verified = {
                "ticker": ticker,
                "scan_from": old["scan"],
                "scan_to": current["scan"],
                "timestamp_from": old["timestamp"],
                "timestamp_to": current["timestamp"],
                "prediction": prediction,
                "signal": signal,
                "confidence": old.get("confidence", 0),
                "conviction_threshold": old.get("conviction_threshold", 0.06),
                "dynamic_signals": old.get("dynamic_signals", 0),
                "stale_signals": old.get("stale_signals", 0),
                "volatility": old.get("volatility", 0),
                "market_session": session,
                "price_from": old_price,
                "price_to": new_price,
                "pct_change": round(pct_change, 6),
                "actual_outcome": actual,
                "correct": exact,
                "directional_correct": directional,
                "horizon_type": "intraday",
                "scans_apart": VERIFY_SCANS_BACK,
                "signals": old.get("signals", []),  # Full signal vector for ML
            }

            try:
                with open(INTRADAY_VERIFIED_FILE, "a", encoding="utf-8") as f:
                    f.write(json.dumps(verified, default=str) + "\n")
            except Exception:
                pass

    # ── Public API for GUI ─────────────────────────────────────

    def get_trend(self, ticker: str) -> Dict:
        """Get current intraday trend for a ticker."""
        return self.trends.get(ticker, {
            "direction": "unknown",
            "arrow": "?",
            "detail": "No data",
            "scans": 0,
        })

    def get_all_trends(self) -> Dict[str, Dict]:
        """Get trends for all tracked tickers."""
        return dict(self.trends)

    def get_session_stats(self) -> Dict:
        """Get session-level statistics."""
        verified = self.intraday_stats["verified"]
        correct = self.intraday_stats["correct"]
        directional = self.intraday_stats["directional"]

        return {
            "session_id": self.session_id,
            "total_scans": self.scan_number,
            "tickers_tracked": len(self.history),
            "intraday_verified": verified,
            "intraday_accuracy": round(correct / verified * 100, 1) if verified > 0 else 0,
            "intraday_directional": round(directional / verified * 100, 1) if verified > 0 else 0,
            "total_snapshots": sum(len(h) for h in self.history.values()),
        }

    def get_ticker_history(self, ticker: str) -> List[Dict]:
        """Get full scan history for a ticker (for charts/sparklines)."""
        return self.history.get(ticker, [])

    @staticmethod
    def get_intraday_training_data() -> List[Dict]:
        """
        Load verified intraday predictions for ML training.
        Each record now includes the full 37-signal vector from the scan
        that made the prediction, plus the actual short-term outcome.
        
        These have tighter thresholds (±0.3% vs ±2% for 5-day) since
        the verification window is only ~15 minutes. ML training uses
        the actual_outcome label which maps to the same BULLISH/NEUTRAL/BEARISH
        classes as the 5-day data.
        """
        if not INTRADAY_VERIFIED_FILE.exists():
            return []

        data = []
        skipped = 0
        with open(INTRADAY_VERIFIED_FILE, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    v = json.loads(line.strip())
                    signals = v.get("signals", [])

                    # Only include samples that have signals
                    if not signals:
                        skipped += 1
                        continue

                    data.append({
                        "signals": signals,
                        "price_history": [],  # Not needed — signals encode price info
                        "outcome": v.get("actual_outcome", "NEUTRAL"),
                    })
                except Exception:
                    continue

        if skipped > 0:
            logger.info(f"Intraday training: {len(data)} usable, {skipped} skipped (no signals)")
        return data

    @staticmethod
    def get_intraday_accuracy_summary() -> Dict:
        """Load aggregate intraday accuracy from all sessions."""
        if not INTRADAY_VERIFIED_FILE.exists():
            return {"verified": 0, "accuracy": 0, "directional": 0}

        verified = 0
        correct = 0
        directional = 0

        try:
            with open(INTRADAY_VERIFIED_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        v = json.loads(line.strip())
                        verified += 1
                        if v.get("correct"):
                            correct += 1
                        if v.get("directional_correct"):
                            directional += 1
                    except Exception:
                        continue
        except Exception:
            pass

        return {
            "verified": verified,
            "accuracy": round(correct / verified * 100, 1) if verified > 0 else 0,
            "directional": round(directional / verified * 100, 1) if verified > 0 else 0,
        }
