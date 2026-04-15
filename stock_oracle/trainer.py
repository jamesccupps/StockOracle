"""
Training Data Logger
====================
Automatically logs every analysis with a timestamp.
After N days, checks what the stock actually did and labels the outcome.
This builds the training dataset for the ML ensemble over time.

Run daily: python -m stock_oracle.trainer --log
Run weekly: python -m stock_oracle.trainer --label
Train when ready: python -m stock_oracle.trainer --train
"""
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List

from stock_oracle.config import DATA_DIR, PREDICTION_HORIZON_DAYS, WATCHLIST

logger = logging.getLogger("stock_oracle")

TRAINING_DIR = DATA_DIR / "training"
TRAINING_DIR.mkdir(exist_ok=True)
LOG_FILE = TRAINING_DIR / "signal_log.jsonl"
LABELED_FILE = TRAINING_DIR / "labeled_data.jsonl"


def log_analysis(result: Dict):
    """
    Append a single analysis result to the signal log.
    Called automatically after every oracle.analyze() call.
    """
    record = {
        "logged_at": datetime.now(timezone.utc).isoformat(),
        "ticker": result.get("ticker"),
        "signals": result.get("signals", []),
        "prediction": result.get("prediction"),
        "signal": result.get("signal"),
        "confidence": result.get("confidence"),
        "price": None,
    }

    # Extract current price from price_data
    price_data = result.get("price_data", [])
    if price_data:
        record["price"] = price_data[-1].get("close")

    with open(LOG_FILE, "a") as f:
        f.write(json.dumps(record, default=str) + "\n")

    logger.info(f"Logged training data for {record['ticker']}")


def label_outcomes():
    """
    Go through old signal logs and label what actually happened.
    Checks if stock went up or down N days after the signal.
    """
    if not LOG_FILE.exists():
        logger.warning("No signal log found. Run analyses first.")
        return 0

    try:
        import yfinance as yf
    except ImportError:
        logger.error("yfinance required for labeling")
        return 0

    labeled_count = 0
    cutoff = datetime.now(timezone.utc) - timedelta(days=PREDICTION_HORIZON_DAYS + 1)

    # Read existing labeled tickers to avoid re-labeling
    already_labeled = set()
    if LABELED_FILE.exists():
        with open(LABELED_FILE) as f:
            for line in f:
                try:
                    rec = json.loads(line)
                    key = f"{rec['ticker']}_{rec['logged_at']}"
                    already_labeled.add(key)
                except Exception:
                    continue

    with open(LOG_FILE) as f:
        for line in f:
            try:
                record = json.loads(line)
                logged_at = datetime.fromisoformat(record["logged_at"])
                ticker = record["ticker"]
                key = f"{ticker}_{record['logged_at']}"

                # Skip if already labeled or too recent
                if key in already_labeled:
                    continue
                if logged_at > cutoff:
                    continue

                # Get the price N days after the signal
                signal_price = record.get("price")
                if not signal_price:
                    continue

                # Fetch what happened
                stock = yf.Ticker(ticker)
                target_date = logged_at + timedelta(days=PREDICTION_HORIZON_DAYS)
                hist = stock.history(
                    start=target_date.strftime("%Y-%m-%d"),
                    end=(target_date + timedelta(days=3)).strftime("%Y-%m-%d"),
                )

                if hist.empty:
                    continue

                future_price = float(hist["Close"].iloc[0])
                pct_change = (future_price - signal_price) / signal_price

                # Label the outcome
                if pct_change > 0.02:
                    outcome = "BULLISH"
                elif pct_change < -0.02:
                    outcome = "BEARISH"
                else:
                    outcome = "NEUTRAL"

                labeled_record = {
                    **record,
                    "future_price": future_price,
                    "pct_change": round(pct_change, 4),
                    "outcome": outcome,
                    "horizon_days": PREDICTION_HORIZON_DAYS,
                }

                with open(LABELED_FILE, "a") as lf:
                    lf.write(json.dumps(labeled_record, default=str) + "\n")

                labeled_count += 1
                logger.info(
                    f"Labeled {ticker}: {outcome} ({pct_change:+.2%}) "
                    f"${signal_price:.2f} -> ${future_price:.2f}"
                )

            except Exception as e:
                continue

    logger.info(f"Labeled {labeled_count} new training samples")
    return labeled_count


def get_training_data() -> List[Dict]:
    """Load labeled data formatted for ML training."""
    if not LABELED_FILE.exists():
        return []

    data = []
    with open(LABELED_FILE) as f:
        for line in f:
            try:
                record = json.loads(line)
                data.append({
                    "signals": record["signals"],
                    "price_history": [],  # Could add if we stored it
                    "outcome": record["outcome"],
                })
            except Exception:
                continue

    return data


def get_stats() -> Dict:
    """Get training data statistics."""
    total_logged = 0
    total_labeled = 0

    if LOG_FILE.exists():
        with open(LOG_FILE) as f:
            total_logged = sum(1 for _ in f)

    if LABELED_FILE.exists():
        with open(LABELED_FILE) as f:
            for line in f:
                total_labeled += 1

    return {
        "total_logged": total_logged,
        "total_labeled": total_labeled,
        "ready_to_train": total_labeled >= 50,
        "samples_needed": max(0, 50 - total_labeled),
        "log_file": str(LOG_FILE),
        "labeled_file": str(LABELED_FILE),
    }


if __name__ == "__main__":
    import argparse
    import sys

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser(description="Stock Oracle ML Training Manager")
    parser.add_argument("--log", action="store_true", help="Run analysis and log signals")
    parser.add_argument("--label", action="store_true", help="Label old signals with outcomes")
    parser.add_argument("--train", action="store_true", help="Train ML models on labeled data")
    parser.add_argument("--stats", action="store_true", help="Show training data statistics")
    args = parser.parse_args()

    if args.stats or not any([args.log, args.label, args.train]):
        stats = get_stats()
        print(f"\n  Training Data Stats")
        print(f"  Logged analyses:  {stats['total_logged']}")
        print(f"  Labeled samples:  {stats['total_labeled']}")
        print(f"  Ready to train:   {'Yes' if stats['ready_to_train'] else 'No'}")
        if not stats['ready_to_train']:
            print(f"  Samples needed:   {stats['samples_needed']} more")
        print()

    if args.log:
        from stock_oracle.oracle import StockOracle
        oracle = StockOracle()
        for ticker in WATCHLIST:
            result = oracle.analyze(ticker, verbose=False)
            log_analysis(result)
        print(f"Logged {len(WATCHLIST)} analyses")

    if args.label:
        count = label_outcomes()
        print(f"Labeled {count} new samples")

    if args.train:
        data = get_training_data()
        if len(data) < 50:
            print(f"Need at least 50 labeled samples (have {len(data)})")
            sys.exit(1)

        from stock_oracle.ml.pipeline import StockPredictor
        predictor = StockPredictor()
        predictor.train(data)
        print("Models trained and saved!")
