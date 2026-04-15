"""
Historical Training Data Generator
====================================
Instead of waiting weeks to collect forward-looking training data,
this module generates training samples from HISTORICAL data.

It replays what the signals would have looked like on past dates
and labels them with what actually happened to the stock.

This gives you hundreds or thousands of training samples immediately.

Signals that CAN be backfilled from historical data:
  - Yahoo Finance (price, RSI, volume, momentum) -- YES, full history
  - Seasonality (calendar math) -- YES, trivially
  - Energy cascade (oil price history) -- YES
  - Cross-stock (sector correlation, lead-lag) -- YES
  - Supply chain (peer price movements) -- YES
  - Weather (Open-Meteo historical) -- YES

Signals that CANNOT be backfilled:
  - Reddit/HN/News sentiment -- no historical archive of sentiment
  - SEC filings -- would need historical filing data
  - Job postings, patents, etc. -- point-in-time only

Strategy: Train on the signals we CAN backfill (price-based + calendar),
then gradually improve as forward-collected sentiment data accumulates.

Usage:
    python -m stock_oracle.historical_trainer --generate AAPL TSLA NVDA --days 365
    python -m stock_oracle.historical_trainer --train
"""
import json
import logging
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np

from stock_oracle.config import (
    DATA_DIR, PREDICTION_HORIZON_DAYS, WATCHLIST,
)

logger = logging.getLogger("stock_oracle")

HIST_TRAINING_DIR = DATA_DIR / "historical_training"
HIST_TRAINING_DIR.mkdir(exist_ok=True)
HIST_DATA_FILE = HIST_TRAINING_DIR / "historical_samples.jsonl"

try:
    import yfinance as yf
    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False
    logger.warning(
        "yfinance not installed for this Python version. "
        "Fix: py -3.13 -m pip install yfinance  "
        "(Python 3.14 may not be compatible)"
    )

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False


def diagnose_yfinance(ticker: str = "AAPL") -> Dict:
    """
    Run diagnostics on yfinance to check if it works.
    Call this from the GUI to see what's going wrong.
    """
    results = {"ok": False, "messages": []}

    if not HAS_YFINANCE:
        results["messages"].append("yfinance not installed: pip install yfinance")
        return results

    results["messages"].append(f"yfinance version: {yf.__version__}")

    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period="5d")

        results["messages"].append(f"history() returned: {type(hist).__name__}, shape={hist.shape if hist is not None else 'None'}")

        if hist is None or hist.empty:
            results["messages"].append("ERROR: yfinance returned empty data")
            # Try with auto_adjust=False
            hist2 = stock.history(period="5d", auto_adjust=False)
            results["messages"].append(f"Retry with auto_adjust=False: shape={hist2.shape if hist2 is not None else 'None'}")
            return results

        results["messages"].append(f"Columns: {list(hist.columns)}")
        col_type = type(hist.columns).__name__
        results["messages"].append(f"Column type: {col_type}")

        if hasattr(hist.columns, 'nlevels'):
            results["messages"].append(f"Column levels: {hist.columns.nlevels}")

        # Try to extract Close
        try:
            if hasattr(hist.columns, 'nlevels') and hist.columns.nlevels > 1:
                close = hist["Close"].iloc[:, 0] if isinstance(hist["Close"], pd.DataFrame) else hist["Close"]
            else:
                close = hist["Close"]
            results["messages"].append(f"Close prices: {close.tolist()}")
            results["ok"] = True
        except Exception as e:
            results["messages"].append(f"Error extracting Close: {e}")

    except Exception as e:
        results["messages"].append(f"ERROR: {type(e).__name__}: {e}")

    return results


def _extract_column(hist, col_name: str, ticker: str = None):
    """
    Safely extract a column from yfinance DataFrame,
    handling both single-level and MultiIndex columns.
    """
    try:
        # Try direct access first (single-level columns)
        if col_name in hist.columns:
            return hist[col_name].values.tolist()
    except Exception:
        pass

    try:
        # MultiIndex: ('Close', 'AAPL') format
        if hasattr(hist.columns, 'nlevels') and hist.columns.nlevels > 1:
            if ticker and (col_name, ticker) in hist.columns:
                return hist[(col_name, ticker)].values.tolist()
            # Try getting the first sub-column
            sub = hist[col_name]
            if hasattr(sub, 'iloc') and hasattr(sub, 'shape') and len(sub.shape) > 1:
                return sub.iloc[:, 0].values.tolist()
            return sub.values.tolist()
    except Exception:
        pass

    try:
        # Last resort: find any column containing the name
        for col in hist.columns:
            col_str = str(col)
            if col_name.lower() in col_str.lower():
                return hist[col].values.tolist()
    except Exception:
        pass

    return None


def generate_historical_samples(
    ticker: str,
    days_back: int = 365,
    horizon: int = None,
) -> int:
    """
    Generate training samples from historical price data.
    For each trading day in the past, compute what the signals
    would have been, then label with the actual outcome.

    Returns the number of samples generated.
    """
    if not HAS_YFINANCE:
        logger.error("yfinance required - pip install yfinance")
        return 0

    horizon = horizon or PREDICTION_HORIZON_DAYS

    logger.info(f"Generating historical samples for {ticker} ({days_back} days back, {horizon}d horizon)")

    # Pull extended history — try multiple approaches
    hist = None
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back + horizon + 90)

    try:
        stock = yf.Ticker(ticker)

        # Approach 1: explicit dates
        logger.info(f"  Fetching {ticker} history...")
        hist = stock.history(start=start_date.strftime("%Y-%m-%d"),
                             end=end_date.strftime("%Y-%m-%d"))

        if hist is None or hist.empty:
            # Approach 2: period string
            logger.info(f"  Date range returned empty, trying period='2y'...")
            hist = stock.history(period="2y")

        if hist is None or hist.empty:
            # Approach 3: with auto_adjust=False
            logger.info(f"  Still empty, trying auto_adjust=False...")
            hist = stock.history(period="1y", auto_adjust=False)

    except Exception as e:
        logger.error(f"  yfinance error for {ticker}: {type(e).__name__}: {e}")
        return 0

    if hist is None or hist.empty:
        logger.warning(f"  All fetch attempts returned empty for {ticker}")
        return 0

    logger.info(f"  Got {len(hist)} rows, columns: {list(hist.columns)[:6]}")

    if len(hist) < 60:
        logger.warning(f"  Not enough history for {ticker} ({len(hist)} days, need 60+)")
        return 0

    # Extract columns using robust helper
    closes = _extract_column(hist, "Close", ticker)
    volumes = _extract_column(hist, "Volume", ticker)

    if closes is None or len(closes) == 0:
        logger.error(f"  Could not extract Close prices for {ticker}")
        logger.error(f"  Available columns: {list(hist.columns)}")
        return 0

    if volumes is None:
        volumes = [0] * len(closes)

    # Filter out None/NaN values
    clean_data = []
    for i in range(len(closes)):
        c = closes[i]
        v = volumes[i] if i < len(volumes) else 0
        if c is not None and not (isinstance(c, float) and math.isnan(c)):
            clean_data.append((c, v))

    if len(clean_data) < 60:
        logger.warning(f"  Only {len(clean_data)} valid prices after cleaning (need 60+)")
        return 0

    closes = [d[0] for d in clean_data]
    volumes = [d[1] or 0 for d in clean_data]
    dates = [hist.index[i].strftime("%Y-%m-%d") for i in range(min(len(hist), len(clean_data)))]

    logger.info(f"  {len(closes)} valid prices: ${closes[0]:.2f} to ${closes[-1]:.2f}")

    # Pull sector peer data for cross-stock signals
    from stock_oracle.collectors.cross_stock import SECTOR_PEERS, SECTOR_ETFS
    sector = None
    peers = []
    for s, p in SECTOR_PEERS.items():
        if ticker in p:
            sector = s
            peers = [x for x in p if x != ticker][:4]
            break

    peer_closes = {}
    etf_closes = []
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    if peers:
        for peer in peers:
            try:
                ph = yf.Ticker(peer).history(start=start_str, end=end_str)
                if ph is not None and not ph.empty and len(ph) > 50:
                    pc = _extract_column(ph, "Close", peer)
                    if pc and len(pc) > 50:
                        peer_closes[peer] = pc
            except Exception:
                pass

    etf_sym = SECTOR_ETFS.get(sector)
    if etf_sym:
        try:
            eh = yf.Ticker(etf_sym).history(start=start_str, end=end_str)
            if eh is not None and not eh.empty and len(eh) > 50:
                ec = _extract_column(eh, "Close", etf_sym)
                if ec:
                    etf_closes = ec
        except Exception:
            pass

    # Also pull oil for energy cascade
    oil_closes = []
    try:
        oh = yf.Ticker("CL=F").history(start=start_str, end=end_str)
        if oh is not None and not oh.empty and len(oh) > 50:
            oc = _extract_column(oh, "Close", "CL=F")
            if oc:
                oil_closes = oc
    except Exception:
        pass

    samples_written = 0

    # Slide a window through history
    # Start from day 50 (need lookback) to len - horizon (need future price)
    start_idx = 50
    end_idx = len(closes) - horizon

    if end_idx <= start_idx:
        logger.warning(f"Not enough trading days for {ticker} after lookback window")
        return 0

    with open(HIST_DATA_FILE, "a") as f:
        for i in range(start_idx, end_idx):
            try:
                # ── Build features for day i ──────────────
                signals = []

                # 1. Price-based signal (RSI, momentum, volume)
                window = closes[i-60:i+1]
                vol_window = volumes[i-60:i+1]
                price_signal = _compute_price_signal(window, vol_window)
                signals.append(price_signal)

                # 2. Seasonality
                date_obj = datetime.strptime(dates[i], "%Y-%m-%d")
                season_signal = _compute_seasonality(date_obj, sector or "all")
                signals.append(season_signal)

                # 3. Cross-stock: sector momentum
                sector_signal = _compute_sector_momentum(
                    i, closes, peer_closes, ticker
                )
                signals.append(sector_signal)

                # 4. Cross-stock: pair divergence
                diverge_signal = _compute_divergence(
                    i, closes, peer_closes, ticker
                )
                signals.append(diverge_signal)

                # 5. ETF flow
                if etf_closes and len(etf_closes) > i:
                    etf_signal = _compute_etf_signal(i, etf_closes)
                    signals.append(etf_signal)

                # 6. Energy cascade
                if oil_closes and len(oil_closes) > i:
                    energy_signal = _compute_energy_signal(i, oil_closes, ticker)
                    signals.append(energy_signal)

                # 7. Volume anomaly (separate from price)
                vol_signal = _compute_volume_signal(i, vol_window)
                signals.append(vol_signal)

                # 8. Technical analysis (RSI, MACD, Bollinger, MA crossovers)
                tech_signal = _compute_technical_analysis(i, closes, volumes)
                signals.append(tech_signal)

                # 9. Momentum quality (trend consistency, acceleration, volume confirmation)
                mq_signal = _compute_momentum_quality(i, closes, volumes)
                signals.append(mq_signal)

                # ── Label the outcome ─────────────────────
                current_price = closes[i]
                future_price = closes[i + horizon]
                pct_change = (future_price - current_price) / current_price

                if pct_change > 0.02:
                    outcome = "BULLISH"
                elif pct_change < -0.02:
                    outcome = "BEARISH"
                else:
                    outcome = "NEUTRAL"

                # ── Write sample ──────────────────────────
                sample = {
                    "ticker": ticker,
                    "date": dates[i],
                    "signals": signals,
                    "price": round(current_price, 2),
                    "future_price": round(future_price, 2),
                    "pct_change": round(pct_change, 4),
                    "outcome": outcome,
                    "horizon_days": horizon,
                    "source": "historical_backfill",
                }

                f.write(json.dumps(sample, default=str) + "\n")
                samples_written += 1

            except Exception as e:
                continue

    logger.info(f"Generated {samples_written} historical samples for {ticker}")
    return samples_written


def _compute_price_signal(closes: list, volumes: list) -> Dict:
    """Compute RSI + momentum + volume signal from price window."""
    if len(closes) < 20:
        return {"collector": "yahoo_finance", "signal": 0, "confidence": 0}

    current = closes[-1]

    # Momentum: 20-day vs 50-day MA
    ma20 = np.mean(closes[-20:])
    ma50 = np.mean(closes[-50:]) if len(closes) >= 50 else ma20
    momentum = (ma20 - ma50) / ma50 if ma50 > 0 else 0

    # RSI (14-day)
    deltas = np.diff(closes[-15:])
    gains = np.mean([d for d in deltas if d > 0]) if any(d > 0 for d in deltas) else 0
    losses = np.mean([abs(d) for d in deltas if d < 0]) if any(d < 0 for d in deltas) else 0.001
    rs = gains / losses if losses > 0 else 1
    rsi = 100 - (100 / (1 + rs))

    rsi_signal = 0.0
    if rsi > 70:
        rsi_signal = -(rsi - 70) / 30
    elif rsi < 30:
        rsi_signal = (30 - rsi) / 30

    # Volume ratio
    vol_avg = np.mean(volumes[-20:]) if len(volumes) >= 20 else np.mean(volumes)
    vol_ratio = volumes[-1] / vol_avg if vol_avg > 0 else 1.0

    signal = (momentum * 0.4) + (rsi_signal * 0.3)
    if vol_ratio > 2.0:
        signal *= 1.2

    return {
        "collector": "yahoo_finance",
        "signal": round(max(-1.0, min(1.0, signal)), 4),
        "confidence": 0.7,
    }


def _compute_seasonality(date_obj: datetime, sector: str) -> Dict:
    """Compute seasonality signal for a given date."""
    from stock_oracle.collectors.alt_data import SEASONAL_PATTERNS

    month = date_obj.month
    dow = date_obj.weekday()
    monthly = SEASONAL_PATTERNS.get(month, {})

    sector_signal = monthly.get(sector, 0.0)
    general_signal = monthly.get("all", 0.0)
    signal = sector_signal if sector_signal else general_signal

    dow_adj = {0: -0.05, 1: 0.0, 2: 0.02, 3: 0.02, 4: 0.03}
    signal += dow_adj.get(dow, 0)

    return {
        "collector": "seasonality",
        "signal": round(max(-1.0, min(1.0, signal)), 4),
        "confidence": 0.4,
    }


def _compute_sector_momentum(idx: int, my_closes: list,
                              peer_closes: Dict[str, list], ticker: str) -> Dict:
    """Compute sector momentum at a historical point."""
    if not peer_closes:
        return {"collector": "cross_stock_sector", "signal": 0, "confidence": 0}

    peer_returns = []
    for peer, pcloses in peer_closes.items():
        if len(pcloses) > idx and idx >= 5:
            ret = (pcloses[idx] - pcloses[idx-5]) / pcloses[idx-5]
            peer_returns.append(ret)

    if not peer_returns:
        return {"collector": "cross_stock_sector", "signal": 0, "confidence": 0}

    sector_avg = np.mean(peer_returns)
    my_ret = (my_closes[idx] - my_closes[idx-5]) / my_closes[idx-5] if idx >= 5 else 0
    gap = sector_avg - my_ret

    return {
        "collector": "cross_stock_sector",
        "signal": round(max(-1.0, min(1.0, float(gap) * 5)), 4),
        "confidence": round(min(0.6, len(peer_returns) / 4), 2),
    }


def _compute_divergence(idx: int, my_closes: list,
                         peer_closes: Dict[str, list], ticker: str) -> Dict:
    """Compute pair divergence at a historical point."""
    if not peer_closes or idx < 20:
        return {"collector": "cross_stock_diverge", "signal": 0, "confidence": 0}

    my_ret = (my_closes[idx] - my_closes[idx-20]) / my_closes[idx-20]
    peer_rets = []
    for peer, pcloses in peer_closes.items():
        if len(pcloses) > idx and idx >= 20:
            ret = (pcloses[idx] - pcloses[idx-20]) / pcloses[idx-20]
            peer_rets.append(ret)

    if not peer_rets:
        return {"collector": "cross_stock_diverge", "signal": 0, "confidence": 0}

    peer_avg = np.mean(peer_rets)
    divergence = peer_avg - my_ret

    return {
        "collector": "cross_stock_diverge",
        "signal": round(max(-1.0, min(1.0, float(divergence) * 3)), 4),
        "confidence": round(min(0.5, abs(float(divergence)) * 5), 2),
    }


def _compute_etf_signal(idx: int, etf_closes: list) -> Dict:
    """Compute ETF flow signal at a historical point."""
    if idx < 5 or len(etf_closes) <= idx:
        return {"collector": "cross_stock_etf", "signal": 0, "confidence": 0}

    ret = (etf_closes[idx] - etf_closes[idx-5]) / etf_closes[idx-5]
    signal = max(-1.0, min(1.0, ret * 3))

    return {
        "collector": "cross_stock_etf",
        "signal": round(signal, 4),
        "confidence": 0.4,
    }


def _compute_energy_signal(idx: int, oil_closes: list, ticker: str) -> Dict:
    """Compute energy cascade at a historical point."""
    if idx < 50 or len(oil_closes) <= idx:
        return {"collector": "energy_cascade", "signal": 0, "confidence": 0}

    from stock_oracle.collectors.creative_signals import EnergyCascadeCollector
    ma20 = np.mean(oil_closes[idx-20:idx])
    ma50 = np.mean(oil_closes[idx-50:idx])
    trend = (ma20 - ma50) / ma50 if ma50 > 0 else 0

    signal = 0.0
    ticker_upper = ticker.upper()
    if ticker_upper in EnergyCascadeCollector.ENERGY_UP_BEARISH:
        signal = -trend * 0.4
    elif ticker_upper in EnergyCascadeCollector.ENERGY_UP_BULLISH:
        signal = trend * 0.4

    return {
        "collector": "energy_cascade",
        "signal": round(max(-1.0, min(1.0, signal)), 4),
        "confidence": 0.45,
    }


def _compute_volume_signal(idx: int, volumes: list) -> Dict:
    """Compute volume anomaly signal."""
    if idx < 20 or len(volumes) <= idx:
        return {"collector": "volume_anomaly", "signal": 0, "confidence": 0}

    avg = np.mean(volumes[idx-20:idx])
    if avg == 0:
        return {"collector": "volume_anomaly", "signal": 0, "confidence": 0}

    ratio = volumes[idx] / avg
    signal = 0
    if ratio > 3.0:
        signal = 0.15
    elif ratio > 2.0:
        signal = 0.08
    elif ratio < 0.3:
        signal = -0.05

    return {
        "collector": "volume_anomaly",
        "signal": round(signal, 4),
        "confidence": round(min(0.4, ratio / 5), 2),
    }


def _compute_technical_analysis(idx: int, closes: list, volumes: list,
                                  highs: list = None, lows: list = None) -> Dict:
    """
    Historical backfill of technical_analysis collector.
    Computes RSI, MACD, Bollinger position, MA crossovers from historical window.
    """
    if idx < 50 or len(closes) <= idx:
        return {"collector": "technical_analysis", "signal": 0, "confidence": 0}

    window = np.array(closes[:idx+1], dtype=float)
    signals = []

    # RSI (14-period)
    if len(window) >= 15:
        deltas = np.diff(window[-15:])
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        avg_gain = np.mean(gains)
        avg_loss = np.mean(losses)
        if avg_loss == 0:
            rsi = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))

        if rsi < 30:
            signals.append(0.4)
        elif rsi < 40:
            signals.append(0.15)
        elif rsi > 70:
            signals.append(-0.4)
        elif rsi > 60:
            signals.append(-0.15)
        else:
            signals.append(0.0)

    # MACD (12,26,9)
    if len(window) >= 35:
        def ema(data, period):
            multiplier = 2 / (period + 1)
            result = [float(data[0])]
            for i in range(1, len(data)):
                result.append((float(data[i]) - result[-1]) * multiplier + result[-1])
            return np.array(result)

        ema12 = ema(window, 12)
        ema26 = ema(window, 26)
        macd_line = ema12 - ema26
        signal_line = ema(macd_line[25:], 9)
        if len(signal_line) > 1:
            hist_now = macd_line[-1] - signal_line[-1]
            hist_prev = macd_line[-2] - signal_line[-2] if len(signal_line) > 1 else hist_now
            if hist_now > 0 and hist_prev <= 0:
                signals.append(0.35)  # Bullish cross
            elif hist_now < 0 and hist_prev >= 0:
                signals.append(-0.35)  # Bearish cross
            elif hist_now > 0:
                signals.append(0.1)
            elif hist_now < 0:
                signals.append(-0.1)

    # Bollinger Bands (20,2)
    if len(window) >= 20:
        ma = np.mean(window[-20:])
        std = np.std(window[-20:])
        if std > 0:
            upper = ma + 2 * std
            lower = ma - 2 * std
            bb_pos = (window[-1] - lower) / (upper - lower)
            bb_pos = max(0, min(1, bb_pos))
            if bb_pos < 0.05:
                signals.append(0.35)
            elif bb_pos < 0.2:
                signals.append(0.15)
            elif bb_pos > 0.95:
                signals.append(-0.35)
            elif bb_pos > 0.8:
                signals.append(-0.15)

    # MA crossover (50 vs 200 if available)
    if len(window) >= 200:
        ma50 = np.mean(window[-50:])
        ma200 = np.mean(window[-200:])
        if ma50 > ma200:
            prev_ma50 = np.mean(window[-51:-1])
            if prev_ma50 <= ma200:
                signals.append(0.4)  # Golden cross
            else:
                signals.append(0.1)
        else:
            prev_ma50 = np.mean(window[-51:-1])
            if prev_ma50 >= ma200:
                signals.append(-0.4)  # Death cross
            else:
                signals.append(-0.1)

    if not signals:
        return {"collector": "technical_analysis", "signal": 0, "confidence": 0}

    total = sum(signals) / len(signals)
    agreement = sum(1 for s in signals if (s > 0) == (total > 0)) / len(signals)
    conf = min(0.80, 0.50 + agreement * 0.30)

    return {
        "collector": "technical_analysis",
        "signal": round(max(-1.0, min(1.0, total)), 4),
        "confidence": round(conf, 2),
    }


def _compute_momentum_quality(idx: int, closes: list, volumes: list) -> Dict:
    """
    Historical backfill of momentum_quality collector.
    Measures trend consistency, acceleration, volume confirmation, drawdown.
    """
    if idx < 30 or len(closes) <= idx:
        return {"collector": "momentum_quality", "signal": 0, "confidence": 0}

    window = closes[max(0, idx-20):idx+1]
    vol_window = volumes[max(0, idx-20):idx+1]

    if len(window) < 10:
        return {"collector": "momentum_quality", "signal": 0, "confidence": 0}

    # 1. Trend consistency: % of days that closed higher
    daily_returns = [window[i] - window[i-1] for i in range(1, len(window))]
    up_days = sum(1 for r in daily_returns if r > 0)
    consistency = up_days / len(daily_returns) if daily_returns else 0.5

    # 2. Rate of change acceleration
    roc_10 = (window[-1] - window[-min(10, len(window))]) / window[-min(10, len(window))] if window[-min(10, len(window))] != 0 else 0
    roc_20 = (window[-1] - window[0]) / window[0] if window[0] != 0 else 0
    acceleration = roc_10 - (roc_20 / 2)

    # 3. Volume confirmation
    vol_up, vol_down = [], []
    for i in range(1, len(window)):
        if window[i] > window[i-1] and i < len(vol_window):
            vol_up.append(vol_window[i])
        elif i < len(vol_window):
            vol_down.append(vol_window[i])
    avg_vu = np.mean(vol_up) if vol_up else 0
    avg_vd = np.mean(vol_down) if vol_down else 1
    vol_conf = (avg_vu / max(avg_vd, 1)) - 1

    # 4. Drawdown from window high
    high_val = max(window)
    drawdown = (window[-1] - high_val) / high_val if high_val > 0 else 0

    # Composite
    trend_score = (consistency - 0.5) * 2
    accel_score = max(-0.5, min(0.5, acceleration * 15))
    vol_score = max(-0.3, min(0.3, vol_conf * 0.3))
    dd_score = max(-0.3, min(0.1, drawdown * 3))

    composite = trend_score * 0.35 + accel_score * 0.30 + vol_score * 0.20 + dd_score * 0.15
    signal = max(-1.0, min(1.0, composite))
    confidence = min(0.7, 0.4 + abs(signal) * 0.8)

    return {
        "collector": "momentum_quality",
        "signal": round(signal, 4),
        "confidence": round(confidence, 2),
    }


def load_historical_training_data() -> List[Dict]:
    """Load all historical training samples."""
    if not HIST_DATA_FILE.exists():
        return []

    data = []
    with open(HIST_DATA_FILE) as f:
        for line in f:
            try:
                record = json.loads(line)
                data.append({
                    "signals": record["signals"],
                    "price_history": [],
                    "outcome": record["outcome"],
                })
            except Exception:
                continue
    return data


def get_historical_stats() -> Dict:
    """Get statistics on historical training data."""
    empty = {
        "total_samples": 0,
        "tickers": [],
        "outcomes": {"BULLISH": 0, "BEARISH": 0, "NEUTRAL": 0},
        "ready_to_train": False,
        "balance": {"BULLISH": "0%", "BEARISH": "0%", "NEUTRAL": "0%"},
    }

    if not HIST_DATA_FILE.exists():
        return empty

    tickers = set()
    outcomes = {"BULLISH": 0, "BEARISH": 0, "NEUTRAL": 0}
    total = 0

    with open(HIST_DATA_FILE) as f:
        for line in f:
            try:
                record = json.loads(line)
                tickers.add(record["ticker"])
                outcomes[record["outcome"]] = outcomes.get(record["outcome"], 0) + 1
                total += 1
            except Exception:
                continue

    return {
        "total_samples": total,
        "tickers": sorted(tickers),
        "outcomes": outcomes,
        "ready_to_train": total >= 50,
        "balance": {k: f"{v/max(total,1):.0%}" for k, v in outcomes.items()},
    }


if __name__ == "__main__":
    import argparse
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="Historical Training Data Generator")
    parser.add_argument("--generate", nargs="*", help="Tickers to generate data for")
    parser.add_argument("--days", type=int, default=365, help="Days of history (default: 365)")
    parser.add_argument("--train", action="store_true", help="Train ML on historical data")
    parser.add_argument("--stats", action="store_true", help="Show data statistics")
    parser.add_argument("--clear", action="store_true", help="Clear historical data")
    args = parser.parse_args()

    if args.clear:
        if HIST_DATA_FILE.exists():
            HIST_DATA_FILE.unlink()
            print("Cleared historical training data")
        sys.exit(0)

    if args.generate is not None:
        tickers = args.generate if args.generate else WATCHLIST
        total = 0
        for ticker in tickers:
            count = generate_historical_samples(ticker, days_back=args.days)
            total += count
        print(f"\nGenerated {total} total samples across {len(tickers)} tickers")

    if args.stats or (not args.generate and not args.train):
        stats = get_historical_stats()
        print(f"\n  Historical Training Data")
        print(f"  Total samples:  {stats['total_samples']}")
        print(f"  Tickers:        {', '.join(stats['tickers']) if stats['tickers'] else 'none'}")
        print(f"  Outcomes:       {stats['outcomes']}")
        print(f"  Balance:        {stats.get('balance', {})}")
        print(f"  Ready to train: {'Yes' if stats['ready_to_train'] else 'No'}")
        print()

    if args.train:
        data = load_historical_training_data()
        if len(data) < 50:
            print(f"Need at least 50 samples (have {len(data)})")
            print("Run: python -m stock_oracle.historical_trainer --generate AAPL TSLA NVDA")
            sys.exit(1)

        sys.path.insert(0, ".")
        from stock_oracle.ml.pipeline import StockPredictor
        predictor = StockPredictor()
        predictor.train(data)
        print(f"Models trained on {len(data)} historical samples!")
