"""
Prediction Tracker & Feedback Loop
====================================
Records every prediction, automatically verifies outcomes after N days,
scores accuracy, and feeds verified data back into ML training.

This is what makes the ML actually LEARN from its mistakes.

Flow:
  1. After each analysis, record_prediction() saves the prediction + signals
  2. Background timer calls verify_pending() every hour
  3. verify_pending() checks predictions from N days ago against actual prices
  4. Verified outcomes are scored and saved to verified_predictions.jsonl
  5. On next ML retrain, get_verified_training_data() provides real labeled samples
  6. GUI shows accuracy scorecard via get_accuracy_stats()

Storage:
  data/predictions/pending_YYYYMMDD.jsonl   — predictions awaiting verification
  data/predictions/verified.jsonl            — predictions with confirmed outcomes
  data/predictions/accuracy_log.json         — rolling accuracy statistics
"""
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from stock_oracle.config import DATA_DIR, PREDICTION_HORIZON_DAYS

logger = logging.getLogger("stock_oracle")

PREDICTIONS_DIR = DATA_DIR / "predictions"
PREDICTIONS_DIR.mkdir(exist_ok=True)

VERIFIED_FILE = PREDICTIONS_DIR / "verified.jsonl"
ACCURACY_FILE = PREDICTIONS_DIR / "accuracy_log.json"


class PredictionTracker:
    """
    Records predictions and verifies them against actual outcomes.
    """

    def __init__(self, horizon_days: int = None):
        self.horizon = horizon_days or PREDICTION_HORIZON_DAYS  # Default 5 days
        # Track which tickers have been recorded today to avoid monitoring floods
        # (monitoring re-analyzes every 5 min — we only need one 5-day prediction per ticker per day)
        self._recorded_today: Dict[str, str] = {}  # ticker -> date_str

    # ── Recording ──────────────────────────────────────────────

    def record_prediction(self, result: Dict):
        """
        Save a prediction for later verification.
        Called automatically after every analyze() call.
        Only records one prediction per ticker per day to avoid
        monitoring floods (27 tickers × 100+ scans/day = too much).
        """
        ticker = result.get("ticker", "")
        if not ticker:
            return

        # Deduplicate: one 5-day prediction per ticker per day
        date_str = datetime.now().strftime("%Y%m%d")
        prev_date = self._recorded_today.get(ticker)
        if prev_date == date_str:
            return  # Already recorded today
        self._recorded_today[ticker] = date_str

        record = {
            "ticker": ticker,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "prediction": result.get("prediction", "NEUTRAL"),
            "signal": result.get("signal", 0),
            "confidence": result.get("confidence", 0),
            "method": result.get("method", ""),
            "price_at_prediction": self._extract_price(result),
            "core_conviction": result.get("weighted_prediction", {}).get("core_analysis_score", 0),
            "horizon_days": self.horizon,
            "verify_after": (datetime.now(timezone.utc) + timedelta(days=self.horizon)).isoformat(),
            # Signal intelligence metadata
            "conviction_threshold": result.get("conviction_threshold", 0),
            "market_session": result.get("market_session", ""),
            "dynamic_signals": result.get("dynamic_signals", 0),
            "stale_signals": result.get("stale_signals", 0),
            "volatility": result.get("volatility", 0),
            # Market regime
            "market_regime": result.get("market_regime", ""),
            "regime_bias": result.get("regime_bias", 0),
            "regime_detail": result.get("regime_detail", ""),
            # Save full signals for ML retraining
            "signals": result.get("signals", []),
            # Don't save raw_data (too large) — just signal + confidence per collector
            "signal_summary": {
                s.get("collector", "?"): {
                    "signal": s.get("signal", 0),
                    "confidence": s.get("confidence", 0),
                }
                for s in result.get("signals", [])
            },
        }

        # Save to date-specific pending file
        date_str = datetime.now().strftime("%Y%m%d")
        pending_file = PREDICTIONS_DIR / f"pending_{date_str}.jsonl"

        try:
            PREDICTIONS_DIR.mkdir(parents=True, exist_ok=True)
            with open(pending_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, default=str) + "\n")
        except Exception as e:
            logger.error(f"Failed to record prediction for {ticker}: {e}")

    def _extract_price(self, result: Dict) -> float:
        """Get the current price from the result signals."""
        for s in result.get("signals", []):
            if s.get("collector") == "finnhub_realtime":
                raw = s.get("raw_data") or {}
                if isinstance(raw, dict) and raw.get("price"):
                    return float(raw["price"])
            if s.get("collector") == "yahoo_finance":
                raw = s.get("raw_data") or {}
                if isinstance(raw, dict) and raw.get("price"):
                    return float(raw["price"])

        # Fallback: try price_data
        price_data = result.get("price_data", [])
        if price_data:
            return float(price_data[-1].get("close", 0))

        return 0.0

    # ── Verification ───────────────────────────────────────────

    def verify_pending(self) -> Dict:
        """
        Check all pending predictions whose horizon has passed.
        Fetches actual prices and scores each prediction.
        
        Returns: {"verified": N, "correct": N, "wrong": N, "skipped": N}
        """
        try:
            import yfinance as yf
        except ImportError:
            logger.warning("yfinance needed for verification")
            return {"verified": 0, "error": "yfinance not available"}

        now = datetime.now(timezone.utc)
        stats = {"verified": 0, "correct": 0, "wrong": 0, "skipped": 0}

        # Find all pending files
        pending_files = sorted(PREDICTIONS_DIR.glob("pending_*.jsonl"))

        for pending_file in pending_files:
            remaining = []  # Predictions not yet ready to verify
            
            try:
                with open(pending_file, "r", encoding="utf-8") as f:
                    lines = f.readlines()
            except Exception:
                continue

            for line in lines:
                try:
                    record = json.loads(line.strip())
                except Exception:
                    continue

                # Check if verification time has passed
                verify_after = datetime.fromisoformat(record.get("verify_after", "2099-01-01"))
                if hasattr(verify_after, 'tzinfo') and verify_after.tzinfo is None:
                    verify_after = verify_after.replace(tzinfo=timezone.utc)

                if now < verify_after:
                    remaining.append(line)  # Not ready yet
                    continue

                # Ready to verify — fetch actual price
                ticker = record.get("ticker", "")
                pred_price = record.get("price_at_prediction", 0)

                if not ticker or pred_price <= 0:
                    stats["skipped"] += 1
                    continue

                try:
                    actual_price = self._get_current_price(ticker, yf)
                    if actual_price <= 0:
                        remaining.append(line)  # Retry later
                        stats["skipped"] += 1
                        continue

                    # Score the prediction
                    verified = self._score_prediction(record, actual_price)
                    self._save_verified(verified)
                    
                    stats["verified"] += 1
                    if verified["correct"]:
                        stats["correct"] += 1
                    else:
                        stats["wrong"] += 1

                except Exception as e:
                    logger.error(f"Verification error for {ticker}: {e}")
                    remaining.append(line)  # Retry later
                    stats["skipped"] += 1

            # Rewrite pending file with only unverified predictions
            if remaining:
                with open(pending_file, "w", encoding="utf-8") as f:
                    f.writelines(remaining)
            else:
                # All verified — delete the pending file
                try:
                    pending_file.unlink()
                except Exception:
                    pass

        # Update accuracy stats
        self._update_accuracy_stats(stats)

        if stats["verified"] > 0:
            logger.info(
                f"Verified {stats['verified']} predictions: "
                f"{stats['correct']} correct, {stats['wrong']} wrong"
            )

        return stats

    def _get_current_price(self, ticker: str, yf) -> float:
        """Fetch current price for verification."""
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="2d")
            if hist is not None and not hist.empty:
                return float(hist["Close"].iloc[-1])
        except Exception:
            pass
        return 0.0

    def _score_prediction(self, record: Dict, actual_price: float) -> Dict:
        """
        Score a prediction against actual outcome.
        
        A prediction is "correct" if:
        - BULLISH and price went up >1%
        - BEARISH and price went down >1%
        - NEUTRAL and price stayed within ±2%
        """
        pred_price = record["price_at_prediction"]
        pct_change = (actual_price - pred_price) / pred_price
        prediction = record["prediction"]

        # Determine actual outcome (same thresholds as training)
        if pct_change > 0.02:
            actual_outcome = "BULLISH"
        elif pct_change < -0.02:
            actual_outcome = "BEARISH"
        else:
            actual_outcome = "NEUTRAL"

        # Was the prediction correct?
        # Strict: exact match
        exact_match = prediction == actual_outcome

        # Directional: got the direction right (BULLISH and went up, BEARISH and went down)
        directional_match = (
            (prediction == "BULLISH" and pct_change > 0.01) or
            (prediction == "BEARISH" and pct_change < -0.01) or
            (prediction == "NEUTRAL" and abs(pct_change) < 0.02)
        )

        # Signal quality: how much did signal predict the actual move?
        signal = record.get("signal", 0)
        signal_error = abs(signal - pct_change)  # Lower = better

        verified = {
            **record,  # Include all original data
            "actual_price": round(actual_price, 2),
            "pct_change": round(pct_change, 4),
            "actual_outcome": actual_outcome,
            "correct": exact_match,
            "directional_correct": directional_match,
            "signal_error": round(signal_error, 4),
            "verified_at": datetime.now(timezone.utc).isoformat(),
        }

        return verified

    def _save_verified(self, verified: Dict):
        """Append a verified prediction to the verified file."""
        try:
            with open(VERIFIED_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(verified, default=str) + "\n")
        except Exception as e:
            logger.error(f"Failed to save verified prediction: {e}")

    # ── Accuracy Stats ─────────────────────────────────────────

    def _update_accuracy_stats(self, new_stats: Dict):
        """Update rolling accuracy statistics."""
        existing = self._load_accuracy_stats()
        
        existing["total_verified"] = existing.get("total_verified", 0) + new_stats.get("verified", 0)
        existing["total_correct"] = existing.get("total_correct", 0) + new_stats.get("correct", 0)
        existing["total_wrong"] = existing.get("total_wrong", 0) + new_stats.get("wrong", 0)
        existing["last_verified"] = datetime.now(timezone.utc).isoformat()

        if existing["total_verified"] > 0:
            existing["accuracy_pct"] = round(
                existing["total_correct"] / existing["total_verified"] * 100, 1
            )
        else:
            existing["accuracy_pct"] = 0

        try:
            ACCURACY_FILE.write_text(json.dumps(existing, indent=2))
        except Exception:
            pass

    def _load_accuracy_stats(self) -> Dict:
        """Load existing accuracy stats."""
        if ACCURACY_FILE.exists():
            try:
                return json.loads(ACCURACY_FILE.read_text())
            except Exception:
                pass
        return {
            "total_verified": 0, "total_correct": 0, "total_wrong": 0,
            "accuracy_pct": 0, "last_verified": None,
        }

    def get_accuracy_stats(self) -> Dict:
        """
        Get comprehensive accuracy statistics for the GUI scorecard.
        """
        base = self._load_accuracy_stats()

        # Count pending predictions
        pending_count = 0
        for f in PREDICTIONS_DIR.glob("pending_*.jsonl"):
            try:
                pending_count += sum(1 for _ in open(f))
            except Exception:
                pass

        base["pending_verification"] = pending_count

        # Per-ticker accuracy from verified file
        ticker_stats = {}
        prediction_type_stats = {"BULLISH": {"correct": 0, "total": 0},
                                  "BEARISH": {"correct": 0, "total": 0},
                                  "NEUTRAL": {"correct": 0, "total": 0}}
        directional_correct = 0
        directional_total = 0
        recent_predictions = []

        if VERIFIED_FILE.exists():
            try:
                with open(VERIFIED_FILE, "r", encoding="utf-8") as f:
                    for line in f:
                        try:
                            v = json.loads(line.strip())
                            ticker = v.get("ticker", "?")
                            pred = v.get("prediction", "NEUTRAL")

                            # Per-ticker
                            if ticker not in ticker_stats:
                                ticker_stats[ticker] = {"correct": 0, "total": 0, "directional": 0}
                            ticker_stats[ticker]["total"] += 1
                            if v.get("correct"):
                                ticker_stats[ticker]["correct"] += 1
                            if v.get("directional_correct"):
                                ticker_stats[ticker]["directional"] += 1

                            # Per-prediction-type
                            if pred in prediction_type_stats:
                                prediction_type_stats[pred]["total"] += 1
                                if v.get("correct"):
                                    prediction_type_stats[pred]["correct"] += 1

                            # Directional
                            directional_total += 1
                            if v.get("directional_correct"):
                                directional_correct += 1

                            # Recent (last 20)
                            recent_predictions.append({
                                "ticker": ticker,
                                "prediction": pred,
                                "signal": v.get("signal", 0),
                                "price_at": v.get("price_at_prediction", 0),
                                "actual_price": v.get("actual_price", 0),
                                "pct_change": v.get("pct_change", 0),
                                "correct": v.get("correct", False),
                                "directional": v.get("directional_correct", False),
                                "date": v.get("timestamp", "")[:10],
                            })

                        except Exception:
                            continue
            except Exception:
                pass

        # Compute per-ticker accuracy
        ticker_accuracy = {}
        for t, s in ticker_stats.items():
            if s["total"] > 0:
                ticker_accuracy[t] = {
                    "accuracy": round(s["correct"] / s["total"] * 100, 1),
                    "directional": round(s["directional"] / s["total"] * 100, 1),
                    "total": s["total"],
                }

        # Sort by accuracy for best/worst
        sorted_tickers = sorted(ticker_accuracy.items(), key=lambda x: x[1]["accuracy"], reverse=True)

        base["ticker_accuracy"] = ticker_accuracy
        base["best_tickers"] = sorted_tickers[:5] if sorted_tickers else []
        base["worst_tickers"] = sorted_tickers[-5:] if len(sorted_tickers) > 5 else []
        base["prediction_type_stats"] = prediction_type_stats
        base["directional_accuracy"] = (
            round(directional_correct / directional_total * 100, 1)
            if directional_total > 0 else 0
        )
        base["recent_predictions"] = recent_predictions[-20:]  # Last 20

        return base

    # ── ML Training Data ───────────────────────────────────────

    def get_verified_training_data(self) -> List[Dict]:
        """
        Convert verified predictions into ML training samples.
        These are REAL outcomes, not synthetic historical data — 
        they should produce better ML models over time.
        """
        if not VERIFIED_FILE.exists():
            return []

        training_data = []
        with open(VERIFIED_FILE, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    v = json.loads(line.strip())
                    
                    # Convert to the format StockPredictor.train() expects
                    sample = {
                        "signals": v.get("signals", []),
                        "price_history": [],  # We don't save full history, but signals encode price info
                        "outcome": v.get("actual_outcome", "NEUTRAL"),
                    }
                    training_data.append(sample)
                except Exception:
                    continue

        return training_data

    def get_pending_count(self) -> int:
        """Quick count of pending predictions."""
        count = 0
        for f in PREDICTIONS_DIR.glob("pending_*.jsonl"):
            try:
                count += sum(1 for _ in open(f))
            except Exception:
                pass
        return count

    def get_verified_count(self) -> int:
        """Quick count of verified predictions."""
        if not VERIFIED_FILE.exists():
            return 0
        try:
            return sum(1 for _ in open(VERIFIED_FILE))
        except Exception:
            return 0
