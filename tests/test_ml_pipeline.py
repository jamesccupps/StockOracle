"""ML pipeline: feature correctness and an honest cross-validation score."""
import logging
import re

import numpy as np
import pytest

from stock_oracle.ml import pipeline
from stock_oracle.ml.pipeline import FeatureEngine, StockPredictor


def _weighted_avg(signals):
    fe = FeatureEngine()
    vec = fe.build_features(signals)
    return float(vec[fe.feature_names.index("weighted_avg_signal")])


def test_weighted_avg_pairs_each_value_with_its_own_confidence():
    signals = [
        {"collector": "yahoo_finance", "signal": 0.8, "confidence": 0.1},    # below 0.2: excluded
        {"collector": "sec_edgar", "signal": -0.5, "confidence": 0.9},
        {"collector": "reddit_sentiment", "signal": -0.5, "confidence": 0.3},
    ]
    # Only the two confident signals: (-0.5*0.9 + -0.5*0.3) / (0.9+0.3) = -0.5
    assert _weighted_avg(signals) == pytest.approx(-0.5)


def _random_samples(rng, n):
    names = FeatureEngine.CANONICAL_COLLECTORS[:20]
    return [{"signals": [{"collector": c, "signal": float(rng.uniform(-1, 1)),
                          "confidence": float(rng.uniform(0.3, 1))} for c in names],
             "outcome": rng.choice(["BULLISH", "NEUTRAL", "BEARISH"])}
            for _ in range(n)]


def test_cv_is_not_inflated_by_repeated_rows(tmp_path, monkeypatch, caplog):
    # Labels are pure noise, so honest CV accuracy sits near chance (~1/3).
    # Built the way gui.py merges sources: historical + verified * 3.
    monkeypatch.setattr(pipeline, "MODEL_DIR", tmp_path)
    rng = np.random.default_rng(7)
    hist, verified = _random_samples(rng, 300), _random_samples(rng, 120)
    combined = hist + verified * 3

    p = StockPredictor()
    p.models = {"random_forest": p.models["random_forest"]}   # keep the test quick
    with caplog.at_level(logging.INFO, logger="stock_oracle"):
        p.train(combined)

    text = caplog.text
    assert "240 repeats excluded" in text
    rf = float(re.search(r"Model random_forest: CV accuracy = ([0-9.]+)", text).group(1))
    base = float(re.search(r"Baseline \(majority class\): CV accuracy = ([0-9.]+)", text).group(1))
    assert rf < 0.5, f"CV {rf:.3f} on noise labels means repeated rows leaked into validation"
    assert 0.2 < base < 0.5
    assert p.is_trained and (tmp_path / "ensemble_models.pkl").exists()
