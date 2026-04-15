"""
ML Pipeline
============
Combines all collector signals into features, trains ensemble models,
and produces final predictions with confidence scores.
"""
import json
import logging
import pickle
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np

from stock_oracle.config import (
    MODEL_DIR, PREDICTION_HORIZON_DAYS, TRAIN_TEST_SPLIT,
    LOOKBACK_DAYS, SIGNAL_WEIGHTS, ENSEMBLE_MODELS,
)

logger = logging.getLogger("stock_oracle")

# Try to import ML libraries
try:
    from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
    from sklearn.neural_network import MLPClassifier
    from sklearn.preprocessing import StandardScaler
    from sklearn.model_selection import TimeSeriesSplit
    from sklearn.metrics import accuracy_score, classification_report
    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False
    logger.warning("scikit-learn not installed. ML features limited to weighted average.")


class FeatureEngine:
    """
    Converts raw collector signals into ML-ready feature vectors.
    Always produces the same number of features regardless of how
    many collectors returned data — this is critical for ML training.
    """

    # Canonical list of all collector names — order matters for consistency
    CANONICAL_COLLECTORS = [
        "yahoo_finance", "finnhub_realtime", "viral_catalyst",
        # Real analysis (high signal)
        "technical_analysis", "fundamental_analysis",
        "analyst_consensus", "options_flow", "short_interest",
        # New analytical indicators
        "fear_greed_proxy", "dividend_vs_treasury",
        "momentum_quality", "insider_ratio",
        "market_pulse",
        # Social / sentiment
        "reddit_sentiment", "news_sentiment",
        "hackernews_sentiment", "sec_edgar", "insider_trades",
        # Corporate
        "job_postings", "supply_chain", "gov_contracts",
        "patent_activity", "github_velocity", "app_store_rank",
        # Alternative / macro
        "google_trends", "wikipedia_velocity", "seasonality",
        "weather_correlation", "energy_cascade", "cardboard_index",
        "waffle_house_index", "talent_flow", "shipping_activity",
        "domain_registration", "earnings_nlp", "employee_sentiment",
        # Cross-stock
        "cross_stock", "earnings_contagion",
        # Historical-only signals
        "cross_stock_sector", "cross_stock_diverge", "cross_stock_etf",
        "volume_anomaly",
    ]

    def __init__(self):
        self.feature_names = []

    def build_features(self, signals: List[Dict], price_history: List[Dict] = None) -> np.ndarray:
        """
        Build a fixed-size feature vector from collector signals.
        Missing collectors get zeros. This ensures every training sample
        and every prediction has the same number of features.
        """
        features = []
        self.feature_names = []

        # Build a lookup from collector name -> signal data
        signal_map = {}
        for s in signals:
            name = s.get("collector", "unknown")
            signal_map[name] = s

        # ── Fixed collector features (3 per collector) ─────────
        for cname in self.CANONICAL_COLLECTORS:
            s = signal_map.get(cname, {})
            sig_val = float(s.get("signal", 0.0))
            conf_val = float(s.get("confidence", 0.0))

            features.append(sig_val)
            self.feature_names.append(f"{cname}_signal")

            features.append(conf_val)
            self.feature_names.append(f"{cname}_confidence")

            features.append(sig_val * conf_val)
            self.feature_names.append(f"{cname}_weighted")

        # ── Price-derived features (ALWAYS added, 0 if no data) ──
        returns_5d = 0.0
        returns_20d = 0.0
        vol_20d = 0.0
        price_vs_ma20 = 0.0
        price_vs_ma50 = 0.0
        ma20_vs_ma50 = 0.0
        volume_ratio = 1.0

        if price_history and len(price_history) >= 20:
            closes = [d["close"] for d in price_history if d.get("close")]
            volumes = [d["volume"] for d in price_history if d.get("volume")]

            if len(closes) >= 20:
                returns_5d = (closes[-1] - closes[-5]) / closes[-5] if len(closes) >= 5 else 0
                returns_20d = (closes[-1] - closes[-20]) / closes[-20]

                daily_returns = [(closes[i] - closes[i-1]) / closes[i-1]
                                 for i in range(1, len(closes))]
                vol_20d = float(np.std(daily_returns[-20:])) if len(daily_returns) >= 20 else 0

                ma20 = np.mean(closes[-20:])
                price_vs_ma20 = (closes[-1] - ma20) / ma20

                if len(closes) >= 50:
                    ma50 = np.mean(closes[-50:])
                    price_vs_ma50 = (closes[-1] - ma50) / ma50
                    ma20_vs_ma50 = (ma20 - ma50) / ma50

            if volumes and len(volumes) >= 20:
                avg_vol = np.mean(volumes[-20:])
                if avg_vol > 0:
                    volume_ratio = volumes[-1] / avg_vol

        features.extend([returns_5d, returns_20d, vol_20d,
                         price_vs_ma20, price_vs_ma50, ma20_vs_ma50, volume_ratio])
        self.feature_names.extend(["returns_5d", "returns_20d", "volatility_20d",
                                    "price_vs_ma20", "price_vs_ma50",
                                    "ma20_vs_ma50", "volume_ratio"])

        # ── Cross-signal features (ALWAYS added) ─────────────────
        signal_values = [s.get("signal", 0) for s in signals if s.get("confidence", 0) > 0.2]
        agreement = 0.0
        divergence = 0.0
        weighted_avg = 0.0

        if signal_values:
            bullish = sum(1 for v in signal_values if v > 0.1)
            bearish = sum(1 for v in signal_values if v < -0.1)
            total = len(signal_values)

            agreement = max(bullish, bearish) / total if total > 0 else 0
            divergence = float(np.std(signal_values))

            weights = [s.get("confidence", 0) for s in signals]
            if sum(weights) > 0:
                weighted_avg = sum(v * w for v, w in zip(signal_values, weights)) / sum(weights)

        features.extend([agreement, divergence, weighted_avg])
        self.feature_names.extend(["signal_agreement", "signal_divergence", "weighted_avg_signal"])

        # ── Calendar features ──────────────────────────────────
        now = datetime.now()
        features.append(now.month / 12.0)
        self.feature_names.append("month_normalized")
        features.append(now.weekday() / 6.0)
        self.feature_names.append("weekday_normalized")
        features.append((now.month - 1) // 3 / 3.0)
        self.feature_names.append("quarter_normalized")

        # Market session: 0=closed, 0.33=pre-market, 0.66=after-hours, 1.0=open
        session_val = 0.0
        try:
            from stock_oracle.collectors.finnhub_collector import get_market_session
            session = get_market_session()
            session_map = {"closed": 0.0, "pre_market": 0.33, "after_hours": 0.66, "regular": 1.0}
            session_val = session_map.get(session.get("session", "closed"), 0.0)
        except Exception:
            pass
        features.append(session_val)
        self.feature_names.append("market_session")

        # ── After-hours / extended-hours features ─────────────────
        ah_move = 0.0
        daily_change = 0.0
        for sig in signals:
            if sig.get("collector") == "finnhub_realtime":
                raw = sig.get("raw_data") or {}
                if isinstance(raw, dict):
                    ah_move = raw.get("after_hours_move", 0.0) or 0.0
                    daily_change = raw.get("daily_change", 0.0) or 0.0
                break
        features.append(float(ah_move))
        self.feature_names.append("after_hours_move")
        features.append(float(daily_change))
        self.feature_names.append("intraday_change")

        # ── Broad market index features (from market_pulse) ──────
        spy_chg = 0.0
        nasdaq_chg = 0.0
        dow_chg = 0.0
        russell_chg = 0.0
        tlt_chg = 0.0
        for sig in signals:
            if sig.get("collector") == "market_pulse":
                raw = sig.get("raw_data") or {}
                if isinstance(raw, dict):
                    spy_chg = (raw.get("spy_change", 0) or 0) / 100.0
                    nasdaq_chg = (raw.get("nasdaq_change", 0) or 0) / 100.0
                    dow_chg = (raw.get("dow_change", 0) or 0) / 100.0
                    russell_chg = (raw.get("russell_change", 0) or 0) / 100.0
                    tlt_chg = (raw.get("tlt_change", 0) or 0) / 100.0
                break
        features.extend([spy_chg, nasdaq_chg, dow_chg, russell_chg, tlt_chg])
        self.feature_names.extend([
            "market_spy_change", "market_nasdaq_change",
            "market_dow_change", "market_russell_change",
            "market_tlt_change",
        ])

        return np.array(features, dtype=np.float64)


class StockPredictor:
    """
    Ensemble ML model for stock prediction.

    Combines:
    - Random Forest (captures non-linear patterns)
    - Gradient Boosting (sequential error correction)
    - Neural Network (complex interactions)

    Falls back to weighted average if sklearn unavailable.
    """

    def __init__(self):
        self.feature_engine = FeatureEngine()
        self.scaler = StandardScaler() if HAS_SKLEARN else None
        self.models = {}
        self.is_trained = False
        self._init_models()

    def _init_models(self):
        if not HAS_SKLEARN:
            return

        self.models = {
            "random_forest": RandomForestClassifier(
                n_estimators=200,
                max_depth=10,
                min_samples_leaf=5,
                random_state=42,
                class_weight="balanced",
            ),
            "gradient_boost": GradientBoostingClassifier(
                n_estimators=150,
                max_depth=5,
                learning_rate=0.1,
                random_state=42,
            ),
            "neural_net": MLPClassifier(
                hidden_layer_sizes=(128, 64, 32),
                activation="relu",
                max_iter=500,
                random_state=42,
                early_stopping=True,
            ),
        }

    # ── Cross-market tier classification (from 2 sessions: up + down) ──
    # Tier 1: BIDIRECTIONAL — accurate in both up AND down markets (2x weight)
    TIER1_COLLECTORS = {
        "dividend_vs_treasury",   # 91.7% down, 55.0% up
        "employee_sentiment",     # 63.2% down, 66.7% up
        "wikipedia_velocity",     # 72.0% down, 56.0% up
    }
    # Tier 2: Useful — either one-directional but paired with opposite,
    # or moderate accuracy. Bull-biased and bear-biased collectors
    # are BOTH in Tier 2 so they balance each other. (1x weight)
    TIER2_COLLECTORS = {
        # Bear-biased (good at detecting downturns)
        "momentum_quality", "sec_edgar", "energy_cascade",
        # Bull-biased (good at detecting rallies)
        "technical_analysis", "analyst_consensus", "news_sentiment",
        "hackernews_sentiment", "supply_chain",
        # Mixed/balanced
        "fundamental_analysis", "short_interest", "finnhub_realtime",
        "cross_stock", "insider_ratio", "earnings_nlp",
        "earnings_contagion", "market_pulse", "insider_trades",
        # Real-time news (dynamic, recency-weighted)
        "realtime_news",
    }
    # Tier 3: Low accuracy in both directions or macro-only (0.5x weight)
    TIER3_COLLECTORS = {
        "fear_greed_proxy", "reddit_sentiment", "options_flow",
        "talent_flow", "yahoo_finance", "google_trends",
        "viral_catalyst", "seasonality", "shipping_activity",
        "app_store_rank", "cardboard_index", "waffle_house_index",
        "weather_correlation", "domain_registration", "job_postings",
        "patent_activity", "gov_contracts", "github_velocity",
        "sec_filing_timing",
    }

    def predict_weighted(self, signals: List[Dict],
                         conviction_threshold: float = 0.10,
                         min_dynamic_for_conviction: int = 5) -> Dict:
        """
        Intelligence-aware weighted prediction.

        Uses signal intelligence metadata when available:
          - _stale signals get suppressed (weight × 0.05)
          - _detrended_signal used for dynamic signals (change from mean)
          - _weight_mult applied on top of tier weighting
          - conviction_threshold adapts to ticker volatility
          - min_dynamic_for_conviction: won't call BULL/BEAR unless this many
            non-stale collectors are active (prevents noise-driven calls)

        Falls back to raw signals gracefully if intelligence hasn't run yet.
        """
        total_weight = 0
        weighted_signal = 0

        # Track dynamic (non-stale) signals separately for conviction scoring
        core_signals = []
        dynamic_count = 0
        stale_count = 0

        for signal in signals:
            collector = signal.get("collector", "")
            base_weight = SIGNAL_WEIGHTS.get(collector, 0.03)
            confidence = signal.get("confidence", 0)
            raw_value = signal.get("signal", 0)

            # Skip effectively dead signals
            if confidence < 0.01:
                continue

            # ── Intelligence adjustments ──
            is_stale = signal.get("_stale", False)
            weight_mult = signal.get("_weight_mult", 1.0)
            detrended = signal.get("_detrended_signal", None)

            # For stale signals: use raw value but heavily suppressed
            # For dynamic signals: blend detrended + raw per intelligence config
            if is_stale:
                value = raw_value
                stale_count += 1
            elif detrended is not None:
                detrend_ratio = signal.get("_detrend_ratio", 0.3)
                value = detrended * detrend_ratio + raw_value * (1 - detrend_ratio)
                dynamic_count += 1
            else:
                value = raw_value
                dynamic_count += 1

            # Apply tier multiplier
            if collector in self.TIER1_COLLECTORS:
                tier_mult = 2.0
                if confidence >= 0.1 and not is_stale:
                    core_signals.append(value)
            elif collector in self.TIER2_COLLECTORS:
                tier_mult = 1.0
                if confidence >= 0.4 and not is_stale:
                    core_signals.append(value)
            else:
                tier_mult = 0.5

            # Final weight: base × confidence × tier × intelligence
            weight = base_weight * confidence * tier_mult * weight_mult
            weighted_signal += value * weight
            total_weight += weight

        if total_weight == 0:
            return {
                "prediction": "NEUTRAL",
                "signal": 0.0,
                "confidence": 0.0,
                "method": "weighted_average",
            }

        final_signal = weighted_signal / total_weight

        # ── Classification with adaptive threshold ──
        # Require minimum dynamic signals for conviction call
        # If mostly stale signals, force NEUTRAL — not enough real-time
        # information to justify a directional call
        if dynamic_count < min_dynamic_for_conviction:
            prediction = "NEUTRAL"
        elif final_signal > conviction_threshold:
            prediction = "BULLISH"
        elif final_signal < -conviction_threshold:
            prediction = "BEARISH"
        else:
            prediction = "NEUTRAL"

        # ── Confidence scoring ──
        # Only non-stale signals count for agreement
        non_stale = [s for s in signals
                     if s.get("confidence", 0) > 0.15 and not s.get("_stale", False)]
        if non_stale:
            direction = final_signal > 0
            agreement = sum(1 for s in non_stale
                           if (s.get("signal", 0) > 0) == direction) / len(non_stale)
        else:
            # Fall back to all signals
            signal_values = [s.get("signal", 0) for s in signals if s.get("confidence", 0) > 0.15]
            if signal_values:
                direction = final_signal > 0
                agreement = sum(1 for v in signal_values
                               if (v > 0) == direction) / len(signal_values)
            else:
                agreement = 0.5

        # Core analysis conviction — only dynamic Tier 1/2 signals
        core_conviction = 0.5
        if core_signals:
            direction = final_signal > 0
            core_agree = sum(1 for v in core_signals if (v > 0) == direction)
            core_conviction = core_agree / len(core_signals)

        # Penalize confidence when mostly stale signals drove the prediction
        total_active = dynamic_count + stale_count
        dynamic_ratio = dynamic_count / max(total_active, 1)
        # If 80%+ of signals are stale, confidence gets cut
        staleness_penalty = min(1.0, 0.4 + 0.6 * dynamic_ratio)

        # Blend: 60% core conviction, 40% broad agreement, then staleness penalty
        confidence = (core_conviction * 0.6 + agreement * 0.4) * staleness_penalty

        return {
            "prediction": prediction,
            "signal": round(final_signal, 4),
            "confidence": round(confidence, 4),
            "method": "weighted_average",
            "core_analysis_score": round(core_conviction, 3),
            "conviction_threshold": round(conviction_threshold, 4),
            "dynamic_signals": dynamic_count,
            "stale_signals": stale_count,
            "signal_breakdown": {
                s.get("collector", "?"): {
                    "signal": s.get("signal", 0),
                    "confidence": s.get("confidence", 0),
                    "weight": SIGNAL_WEIGHTS.get(s.get("collector", ""), 0.03),
                    "tier": 1 if s.get("collector","") in self.TIER1_COLLECTORS
                            else 2 if s.get("collector","") in self.TIER2_COLLECTORS
                            else 3,
                    "stale": s.get("_stale", False),
                    "detrended": s.get("_detrended_signal", None),
                }
                for s in signals
            },
        }

    def predict_ml(self, signals: List[Dict], price_history: List[Dict] = None) -> Dict:
        """
        ML ensemble prediction.
        If not trained, falls back to weighted average.
        """
        if not HAS_SKLEARN or not self.is_trained:
            result = self.predict_weighted(signals)
            result["method"] = "weighted_average (ML not trained)"
            return result

        # Build features
        features = self.feature_engine.build_features(signals, price_history)

        # Clean NaN/inf — same as training pipeline
        features = np.nan_to_num(features, nan=0.0, posinf=1.0, neginf=-1.0)

        # Check for feature count mismatch (model was trained with different collector set)
        try:
            expected = self.scaler.n_features_in_
            actual = features.shape[0]
            if actual != expected:
                logger.warning(
                    f"ML model expects {expected} features but got {actual}. "
                    f"Retrain with: python -m stock_oracle.historical_trainer --train"
                )
                result = self.predict_weighted(signals)
                result["method"] = f"weighted_average (model stale: {expected} vs {actual} features)"
                return result

            features_scaled = self.scaler.transform(features.reshape(1, -1))
        except Exception as e:
            logger.warning(f"ML scaling error: {e}")
            result = self.predict_weighted(signals)
            result["method"] = "weighted_average (ML scaling error)"
            return result

        # Get predictions from each model
        predictions = {}
        probabilities = {}

        for name, model in self.models.items():
            try:
                pred = model.predict(features_scaled)[0]
                prob = model.predict_proba(features_scaled)[0]
                predictions[name] = pred
                probabilities[name] = prob.tolist()
            except Exception as e:
                logger.error(f"Model {name} prediction error: {e}")

        if not predictions:
            return self.predict_weighted(signals)

        # Ensemble vote
        votes = list(predictions.values())
        from collections import Counter
        vote_counts = Counter(votes)
        ensemble_pred = vote_counts.most_common(1)[0][0]

        # Average probabilities
        avg_probs = np.mean(list(probabilities.values()), axis=0)

        pred_map = {0: "BEARISH", 1: "NEUTRAL", 2: "BULLISH"}
        signal_map = {0: -0.5, 1: 0.0, 2: 0.5}

        return {
            "prediction": pred_map.get(ensemble_pred, "NEUTRAL"),
            "signal": signal_map.get(ensemble_pred, 0.0),
            "confidence": round(float(max(avg_probs)), 4),
            "method": "ml_ensemble",
            "model_votes": {k: pred_map.get(v, "?") for k, v in predictions.items()},
            "probabilities": {
                "bearish": round(float(avg_probs[0]), 4) if len(avg_probs) > 0 else 0,
                "neutral": round(float(avg_probs[1]), 4) if len(avg_probs) > 1 else 0,
                "bullish": round(float(avg_probs[2]), 4) if len(avg_probs) > 2 else 0,
            },
            "feature_importance": self._get_feature_importance(),
        }

    def train(self, training_data: List[Dict]):
        """
        Train on historical signals + outcomes.

        training_data format:
        [
            {
                "signals": [...],           # Collector signals at time T
                "price_history": [...],     # Price history at time T
                "outcome": "BULLISH",       # Actual result N days later
            },
            ...
        ]
        """
        if not HAS_SKLEARN:
            logger.warning("scikit-learn required for training")
            return

        if len(training_data) < 50:
            logger.warning(f"Need at least 50 samples, got {len(training_data)}")
            return

        # Build feature matrix
        X = []
        y = []
        outcome_map = {"BEARISH": 0, "NEUTRAL": 1, "BULLISH": 2}

        for sample in training_data:
            features = self.feature_engine.build_features(
                sample["signals"],
                sample.get("price_history"),
            )
            X.append(features)
            y.append(outcome_map.get(sample["outcome"], 1))

        X = np.array(X)
        y = np.array(y)

        # Handle NaN/inf
        X = np.nan_to_num(X, nan=0.0, posinf=1.0, neginf=-1.0)

        # Scale features
        X = self.scaler.fit_transform(X)

        # Time-series cross-validation
        tscv = TimeSeriesSplit(n_splits=5)

        for name, model in self.models.items():
            scores = []
            for train_idx, val_idx in tscv.split(X):
                X_train, X_val = X[train_idx], X[val_idx]
                y_train, y_val = y[train_idx], y[val_idx]

                model.fit(X_train, y_train)
                score = model.score(X_val, y_val)
                scores.append(score)

            avg_score = np.mean(scores)
            logger.info(f"Model {name}: CV accuracy = {avg_score:.3f}")

            # Final fit on all data
            model.fit(X, y)

        self.is_trained = True
        self._save_models()
        logger.info("All models trained and saved")

    def _get_feature_importance(self) -> Dict:
        """Get feature importance from Random Forest."""
        if "random_forest" not in self.models:
            return {}

        rf = self.models["random_forest"]
        if not hasattr(rf, "feature_importances_"):
            return {}

        importances = rf.feature_importances_
        names = self.feature_engine.feature_names

        if len(importances) != len(names):
            return {}

        paired = sorted(zip(names, importances), key=lambda x: x[1], reverse=True)
        return {name: round(float(imp), 4) for name, imp in paired[:15]}

    def _save_models(self):
        """Save trained models to disk."""
        save_path = MODEL_DIR / "ensemble_models.pkl"
        with open(save_path, "wb") as f:
            pickle.dump({
                "models": self.models,
                "scaler": self.scaler,
                "feature_names": self.feature_engine.feature_names,
                "trained_at": datetime.now(timezone.utc).isoformat(),
            }, f)
        logger.info(f"Models saved to {save_path}")

    def load_models(self) -> bool:
        """Load previously trained models."""
        save_path = MODEL_DIR / "ensemble_models.pkl"
        if not save_path.exists():
            return False

        try:
            with open(save_path, "rb") as f:
                data = pickle.load(f)

            self.models = data["models"]
            self.scaler = data["scaler"]
            self.feature_engine.feature_names = data["feature_names"]
            self.is_trained = True
            logger.info(f"Models loaded (trained {data.get('trained_at', 'unknown')})")
            return True
        except Exception as e:
            logger.error(f"Failed to load models: {e}")
            return False


class Backtester:
    """
    Backtest strategies against historical data.
    """

    def __init__(self):
        self.results = []

    def run(
        self,
        predictions: List[Dict],
        price_history: List[Dict],
        initial_capital: float = 10000.0,
    ) -> Dict:
        """
        Simulate trading based on predictions.

        predictions: list of {date, prediction, signal, confidence}
        price_history: list of {date, open, high, low, close, volume}
        """
        capital = initial_capital
        position = 0  # Number of shares
        trades = []
        equity_curve = []

        price_by_date = {p["date"][:10]: p for p in price_history}

        for pred in predictions:
            date = pred["date"][:10]
            price_data = price_by_date.get(date)
            if not price_data:
                continue

            price = price_data["close"]
            current_equity = capital + (position * price)
            equity_curve.append({"date": date, "equity": current_equity})

            # Trading logic
            if pred["prediction"] == "BULLISH" and pred["confidence"] > 0.5:
                if position == 0:
                    # Buy with available capital
                    shares = int(capital * 0.95 / price)
                    if shares > 0:
                        position = shares
                        capital -= shares * price
                        trades.append({
                            "date": date, "action": "BUY",
                            "shares": shares, "price": price,
                        })

            elif pred["prediction"] == "BEARISH" and pred["confidence"] > 0.5:
                if position > 0:
                    # Sell all
                    capital += position * price
                    trades.append({
                        "date": date, "action": "SELL",
                        "shares": position, "price": price,
                    })
                    position = 0

        # Final equity
        if price_history:
            final_price = price_history[-1]["close"]
            final_equity = capital + (position * final_price)
        else:
            final_equity = capital

        total_return = (final_equity - initial_capital) / initial_capital
        num_trades = len(trades)
        winning_trades = sum(
            1 for i in range(0, len(trades) - 1, 2)
            if i + 1 < len(trades) and trades[i+1]["price"] > trades[i]["price"]
        )

        return {
            "initial_capital": initial_capital,
            "final_equity": round(final_equity, 2),
            "total_return": round(total_return * 100, 2),
            "num_trades": num_trades,
            "win_rate": round(winning_trades / max(num_trades // 2, 1) * 100, 1),
            "trades": trades,
            "equity_curve": equity_curve,
        }
