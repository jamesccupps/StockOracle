"""
Yahoo Finance Collector
=======================
Fetches historical price data, volume, and basic fundamentals.
This is the foundation — all other signals are overlaid on price action.
"""
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import json

from stock_oracle.collectors.base import BaseCollector, SignalResult

logger = logging.getLogger("stock_oracle")

# We use yfinance if available, otherwise fall back to Yahoo's public API
try:
    import yfinance as yf
    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False


class YahooFinanceCollector(BaseCollector):
    """Collects price data, volume anomalies, and basic technical signals."""

    @property
    def name(self) -> str:
        return "yahoo_finance"

    def collect(self, ticker: str) -> SignalResult:
        cached = self._get_cached(ticker, "price")
        if cached:
            return SignalResult.from_dict(cached)

        if HAS_YFINANCE:
            return self._collect_yfinance(ticker)
        else:
            return self._collect_api(ticker)

    def _collect_yfinance(self, ticker: str) -> SignalResult:
        stock = yf.Ticker(ticker)
        hist = stock.history(period="6mo")

        if hist.empty:
            return self._neutral_signal(ticker, "No price data available")

        # Calculate signals
        close = hist["Close"]
        volume = hist["Volume"]

        # Momentum: 20-day vs 50-day moving average crossover
        ma20 = close.rolling(20).mean()
        ma50 = close.rolling(50).mean()
        momentum = 0.0
        if len(ma20.dropna()) > 0 and len(ma50.dropna()) > 0:
            momentum = (ma20.iloc[-1] - ma50.iloc[-1]) / ma50.iloc[-1]

        # Volume anomaly: current vs 20-day average
        vol_avg = volume.rolling(20).mean()
        vol_ratio = volume.iloc[-1] / vol_avg.iloc[-1] if vol_avg.iloc[-1] > 0 else 1.0

        # RSI (14-day)
        delta = close.diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = (-delta.clip(upper=0)).rolling(14).mean()
        rs = gain.iloc[-1] / loss.iloc[-1] if loss.iloc[-1] > 0 else 1.0
        rsi = 100 - (100 / (1 + rs))

        # Convert RSI to signal (-1 to 1)
        # RSI > 70 = overbought (bearish), RSI < 30 = oversold (bullish)
        rsi_signal = 0.0
        if rsi > 70:
            rsi_signal = -(rsi - 70) / 30  # -0.0 to -1.0
        elif rsi < 30:
            rsi_signal = (30 - rsi) / 30   # 0.0 to 1.0

        # Composite signal
        signal = (momentum * 0.4) + (rsi_signal * 0.3)
        if vol_ratio > 2.0:
            signal *= 1.2  # High volume amplifies the signal

        signal = max(-1.0, min(1.0, signal))

        result = SignalResult(
            collector_name=self.name,
            ticker=ticker,
            signal_value=signal,
            confidence=0.45,  # Lower — technical_analysis does RSI/MACD better
            raw_data={
                "price": float(close.iloc[-1]),
                "ma20": float(ma20.iloc[-1]) if len(ma20.dropna()) > 0 else None,
                "ma50": float(ma50.iloc[-1]) if len(ma50.dropna()) > 0 else None,
                "rsi": float(rsi),
                "volume_ratio": float(vol_ratio),
                "momentum": float(momentum),
                "prices_30d": [float(x) for x in close.tail(30).tolist()],
                "volumes_30d": [float(x) for x in volume.tail(30).tolist()],
            },
            details=f"RSI={rsi:.0f} | Vol={vol_ratio:.1f}x avg | Mom={momentum:+.2%}",
        )

        self._set_cache(result.to_dict(), ticker, "price")
        return result

    def _collect_api(self, ticker: str) -> SignalResult:
        """Fallback: use Yahoo's chart API directly."""
        end = int(datetime.now().timestamp())
        start = int((datetime.now() - timedelta(days=180)).timestamp())

        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        params = {
            "period1": start,
            "period2": end,
            "interval": "1d",
        }

        resp = self._request(url, params=params)
        if not resp or resp.status_code != 200:
            return self._neutral_signal(ticker, "Yahoo API unavailable")

        try:
            data = resp.json()
            result = data["chart"]["result"][0]
            closes = result["indicators"]["quote"][0]["close"]
            volumes = result["indicators"]["quote"][0]["volume"]

            # Simple momentum calculation
            valid_closes = [c for c in closes if c is not None]
            if len(valid_closes) < 50:
                return self._neutral_signal(ticker, "Insufficient price history")

            current = valid_closes[-1]
            avg_20 = sum(valid_closes[-20:]) / 20
            avg_50 = sum(valid_closes[-50:]) / 50
            momentum = (avg_20 - avg_50) / avg_50

            signal = max(-1.0, min(1.0, momentum * 5))

            return SignalResult(
                collector_name=self.name,
                ticker=ticker,
                signal_value=signal,
                confidence=0.6,
                raw_data={
                    "price": current,
                    "momentum": momentum,
                    "prices_30d": valid_closes[-30:],
                },
                details=f"Price=${current:.2f} | Mom={momentum:+.2%}",
            )
        except (KeyError, IndexError, json.JSONDecodeError) as e:
            return self._neutral_signal(ticker, f"Parse error: {e}")

    def get_price_history(self, ticker: str, days: int = 90) -> List[Dict]:
        """Get raw price history for ML features."""
        if HAS_YFINANCE:
            stock = yf.Ticker(ticker)
            hist = stock.history(period=f"{days}d")
            return [
                {
                    "date": idx.isoformat(),
                    "open": float(row["Open"]),
                    "high": float(row["High"]),
                    "low": float(row["Low"]),
                    "close": float(row["Close"]),
                    "volume": int(row["Volume"]),
                }
                for idx, row in hist.iterrows()
            ]
        return []
