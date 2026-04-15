"""
Backtesting Engine
==================
Historical simulation of trading strategies with:
- Walk-forward optimization
- Monte Carlo randomization
- Risk-adjusted performance metrics (Sharpe, Sortino, max drawdown)
- Comparison vs buy-and-hold benchmark
"""
import math
import logging
import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np

logger = logging.getLogger("stock_oracle")

try:
    import yfinance as yf
    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False


class Trade:
    """Single trade record."""
    def __init__(self, date: str, action: str, symbol: str, shares: int,
                 price: float, signal: float = 0, reason: str = ""):
        self.date = date
        self.action = action  # BUY / SELL
        self.symbol = symbol
        self.shares = shares
        self.price = price
        self.signal = signal
        self.reason = reason
        self.pnl = 0.0  # Filled on sell

    def to_dict(self) -> Dict:
        return {
            "date": self.date, "action": self.action, "symbol": self.symbol,
            "shares": self.shares, "price": round(self.price, 2),
            "signal": round(self.signal, 4), "pnl": round(self.pnl, 2),
            "reason": self.reason,
        }


class BacktestEngine:
    """
    Full backtesting engine.

    Usage:
        engine = BacktestEngine()

        # Load historical data
        engine.load_data("AAPL", period="2y")

        # Define strategy
        result = engine.run(
            strategy="oracle_signals",  # or "momentum", "mean_reversion", etc.
            initial_capital=10000,
            position_size=0.95,
            stop_loss=0.05,
            take_profit=0.15,
        )

        # Monte Carlo analysis
        mc = engine.monte_carlo(n_simulations=1000)
    """

    def __init__(self):
        self.price_data: Dict[str, List[Dict]] = {}
        self.trades: List[Trade] = []
        self.equity_curve: List[Dict] = []

    def load_data(self, symbol: str, period: str = "2y") -> bool:
        """Load historical price data."""
        if not HAS_YFINANCE:
            logger.error("yfinance required for backtesting")
            return False

        try:
            stock = yf.Ticker(symbol)
            hist = stock.history(period=period)
            self.price_data[symbol] = [
                {
                    "date": idx.strftime("%Y-%m-%d"),
                    "open": float(row["Open"]),
                    "high": float(row["High"]),
                    "low": float(row["Low"]),
                    "close": float(row["Close"]),
                    "volume": int(row["Volume"]),
                }
                for idx, row in hist.iterrows()
            ]
            logger.info(f"Loaded {len(self.price_data[symbol])} days for {symbol}")
            return True
        except Exception as e:
            logger.error(f"Failed to load {symbol}: {e}")
            return False

    def run(
        self,
        symbol: str,
        signals: List[Dict] = None,
        initial_capital: float = 10000.0,
        position_size: float = 0.95,
        stop_loss: float = 0.05,
        take_profit: float = 0.15,
        signal_threshold: float = 0.12,
        confidence_threshold: float = 0.4,
    ) -> Dict:
        """
        Run backtest simulation.

        signals: list of {"date": "2024-01-15", "signal": 0.35, "confidence": 0.7}
            If None, uses moving average crossover as default strategy.
        """
        prices = self.price_data.get(symbol, [])
        if not prices:
            return {"error": f"No price data for {symbol}"}

        # Build signal map
        signal_map = {}
        if signals:
            for s in signals:
                signal_map[s["date"]] = s
        else:
            # Default: 20/50 MA crossover strategy
            signal_map = self._generate_ma_signals(prices)

        # Simulation state
        capital = initial_capital
        position = 0
        entry_price = 0.0
        self.trades = []
        self.equity_curve = []

        for i, day in enumerate(prices):
            date = day["date"]
            price = day["close"]
            equity = capital + (position * price)

            self.equity_curve.append({
                "date": date,
                "equity": round(equity, 2),
                "price": round(price, 2),
                "position": position,
            })

            sig = signal_map.get(date, {})
            sig_val = sig.get("signal", 0)
            sig_conf = sig.get("confidence", 0)

            # ── Entry logic ────────────────────────────────
            if position == 0:
                if sig_val > signal_threshold and sig_conf > confidence_threshold:
                    shares = int(capital * position_size / price)
                    if shares > 0:
                        position = shares
                        entry_price = price
                        capital -= shares * price
                        self.trades.append(Trade(
                            date=date, action="BUY", symbol=symbol,
                            shares=shares, price=price, signal=sig_val,
                            reason=f"Signal {sig_val:+.3f} > {signal_threshold}",
                        ))

            # ── Exit logic ─────────────────────────────────
            elif position > 0:
                pnl_pct = (price - entry_price) / entry_price
                should_sell = False
                reason = ""

                # Stop loss
                if pnl_pct <= -stop_loss:
                    should_sell = True
                    reason = f"Stop loss ({pnl_pct:+.1%})"

                # Take profit
                elif pnl_pct >= take_profit:
                    should_sell = True
                    reason = f"Take profit ({pnl_pct:+.1%})"

                # Signal reversal
                elif sig_val < -signal_threshold and sig_conf > confidence_threshold:
                    should_sell = True
                    reason = f"Signal reversal ({sig_val:+.3f})"

                if should_sell:
                    pnl = (price - entry_price) * position
                    capital += position * price
                    trade = Trade(
                        date=date, action="SELL", symbol=symbol,
                        shares=position, price=price, signal=sig_val,
                        reason=reason,
                    )
                    trade.pnl = pnl
                    self.trades.append(trade)
                    position = 0
                    entry_price = 0

        # Close any open position at end
        if position > 0:
            final_price = prices[-1]["close"]
            pnl = (final_price - entry_price) * position
            capital += position * final_price
            trade = Trade(
                date=prices[-1]["date"], action="SELL", symbol=symbol,
                shares=position, price=final_price,
                reason="End of backtest",
            )
            trade.pnl = pnl
            self.trades.append(trade)

        # Calculate metrics
        final_equity = capital
        metrics = self._compute_metrics(initial_capital, final_equity, prices, symbol)

        return {
            "symbol": symbol,
            "period": f"{prices[0]['date']} to {prices[-1]['date']}",
            "initial_capital": initial_capital,
            "final_equity": round(final_equity, 2),
            **metrics,
            "trades": [t.to_dict() for t in self.trades],
            "equity_curve": self.equity_curve,
        }

    def _generate_ma_signals(self, prices: List[Dict]) -> Dict:
        """Generate signals from 20/50 MA crossover."""
        closes = [d["close"] for d in prices]
        signals = {}

        for i in range(50, len(prices)):
            ma20 = np.mean(closes[i-20:i])
            ma50 = np.mean(closes[i-50:i])
            momentum = (ma20 - ma50) / ma50

            signals[prices[i]["date"]] = {
                "signal": max(-1, min(1, momentum * 5)),
                "confidence": 0.6,
            }

        return signals

    def _compute_metrics(self, initial: float, final: float,
                         prices: List[Dict], symbol: str) -> Dict:
        """Compute comprehensive performance metrics."""
        # Basic returns
        total_return = (final - initial) / initial
        days = len(prices)
        annual_return = (1 + total_return) ** (252 / max(days, 1)) - 1

        # Buy and hold benchmark
        bnh_return = (prices[-1]["close"] - prices[0]["close"]) / prices[0]["close"]
        bnh_annual = (1 + bnh_return) ** (252 / max(days, 1)) - 1

        # Equity curve analysis
        equities = [e["equity"] for e in self.equity_curve]
        if not equities:
            equities = [initial]

        # Daily returns
        daily_returns = []
        for i in range(1, len(equities)):
            if equities[i-1] > 0:
                daily_returns.append((equities[i] - equities[i-1]) / equities[i-1])

        daily_returns = np.array(daily_returns) if daily_returns else np.array([0])

        # Volatility
        volatility = float(np.std(daily_returns) * math.sqrt(252))

        # Sharpe ratio (assuming 4.5% risk-free rate)
        risk_free = 0.045
        sharpe = (annual_return - risk_free) / volatility if volatility > 0 else 0

        # Sortino ratio (downside deviation only)
        downside = daily_returns[daily_returns < 0]
        downside_std = float(np.std(downside) * math.sqrt(252)) if len(downside) > 0 else 0.001
        sortino = (annual_return - risk_free) / downside_std

        # Max drawdown
        peak = equities[0]
        max_dd = 0
        for eq in equities:
            peak = max(peak, eq)
            dd = (peak - eq) / peak
            max_dd = max(max_dd, dd)

        # Trade analysis
        sell_trades = [t for t in self.trades if t.action == "SELL"]
        winning = [t for t in sell_trades if t.pnl > 0]
        losing = [t for t in sell_trades if t.pnl < 0]

        avg_win = np.mean([t.pnl for t in winning]) if winning else 0
        avg_loss = np.mean([abs(t.pnl) for t in losing]) if losing else 0
        profit_factor = (sum(t.pnl for t in winning) / max(sum(abs(t.pnl) for t in losing), 0.01)) if sell_trades else 0

        return {
            "total_return_pct": round(total_return * 100, 2),
            "annual_return_pct": round(annual_return * 100, 2),
            "benchmark_return_pct": round(bnh_return * 100, 2),
            "benchmark_annual_pct": round(bnh_annual * 100, 2),
            "alpha_pct": round((annual_return - bnh_annual) * 100, 2),
            "volatility_pct": round(volatility * 100, 2),
            "sharpe_ratio": round(sharpe, 3),
            "sortino_ratio": round(sortino, 3),
            "max_drawdown_pct": round(max_dd * 100, 2),
            "total_trades": len(sell_trades),
            "win_rate_pct": round(len(winning) / max(len(sell_trades), 1) * 100, 1),
            "avg_win": round(float(avg_win), 2),
            "avg_loss": round(float(avg_loss), 2),
            "profit_factor": round(float(profit_factor), 3),
            "best_trade": round(max((t.pnl for t in sell_trades), default=0), 2),
            "worst_trade": round(min((t.pnl for t in sell_trades), default=0), 2),
        }

    def monte_carlo(
        self,
        n_simulations: int = 1000,
        initial_capital: float = 10000.0,
        days: int = 252,
    ) -> Dict:
        """
        Monte Carlo simulation using historical daily returns.
        Generates probability distribution of outcomes.
        """
        if not self.equity_curve:
            return {"error": "Run a backtest first"}

        equities = [e["equity"] for e in self.equity_curve]
        daily_returns = []
        for i in range(1, len(equities)):
            if equities[i-1] > 0:
                daily_returns.append((equities[i] - equities[i-1]) / equities[i-1])

        if not daily_returns:
            return {"error": "Not enough equity data"}

        daily_returns = np.array(daily_returns)

        # Run simulations
        final_equities = []
        max_drawdowns = []

        for _ in range(n_simulations):
            # Random sample with replacement from historical returns
            sampled = np.random.choice(daily_returns, size=days, replace=True)
            equity = initial_capital
            peak = equity
            max_dd = 0

            for r in sampled:
                equity *= (1 + r)
                peak = max(peak, equity)
                dd = (peak - equity) / peak
                max_dd = max(max_dd, dd)

            final_equities.append(equity)
            max_drawdowns.append(max_dd)

        final_equities = np.array(final_equities)
        max_drawdowns = np.array(max_drawdowns)

        percentiles = [5, 10, 25, 50, 75, 90, 95]
        equity_pcts = {f"p{p}": round(float(np.percentile(final_equities, p)), 2) for p in percentiles}
        dd_pcts = {f"p{p}": round(float(np.percentile(max_drawdowns, p)) * 100, 2) for p in percentiles}

        return {
            "simulations": n_simulations,
            "days": days,
            "initial_capital": initial_capital,
            "mean_final_equity": round(float(np.mean(final_equities)), 2),
            "median_final_equity": round(float(np.median(final_equities)), 2),
            "std_final_equity": round(float(np.std(final_equities)), 2),
            "prob_profit": round(float(np.mean(final_equities > initial_capital)) * 100, 1),
            "prob_double": round(float(np.mean(final_equities > initial_capital * 2)) * 100, 1),
            "prob_loss_20pct": round(float(np.mean(final_equities < initial_capital * 0.8)) * 100, 1),
            "equity_percentiles": equity_pcts,
            "max_drawdown_percentiles": dd_pcts,
            "best_case": round(float(np.max(final_equities)), 2),
            "worst_case": round(float(np.min(final_equities)), 2),
        }

    def walk_forward(
        self,
        symbol: str,
        train_window: int = 120,
        test_window: int = 30,
        step: int = 30,
    ) -> Dict:
        """
        Walk-forward analysis: train on window, test on next window, step forward.
        Prevents look-ahead bias in backtesting.
        """
        prices = self.price_data.get(symbol, [])
        if len(prices) < train_window + test_window:
            return {"error": "Not enough data for walk-forward"}

        windows = []
        i = 0

        while i + train_window + test_window <= len(prices):
            train_prices = prices[i:i+train_window]
            test_prices = prices[i+train_window:i+train_window+test_window]

            # Generate signals from training period
            train_signals = self._generate_ma_signals(train_prices)

            # Test on out-of-sample data
            self.price_data[f"{symbol}_test"] = test_prices
            result = self.run(
                f"{symbol}_test",
                signals=[{"date": d, **s} for d, s in train_signals.items()],
            )

            windows.append({
                "train_period": f"{train_prices[0]['date']} to {train_prices[-1]['date']}",
                "test_period": f"{test_prices[0]['date']} to {test_prices[-1]['date']}",
                "return_pct": result.get("total_return_pct", 0),
                "trades": result.get("total_trades", 0),
            })

            # Clean up
            del self.price_data[f"{symbol}_test"]
            i += step

        avg_return = np.mean([w["return_pct"] for w in windows])
        consistency = sum(1 for w in windows if w["return_pct"] > 0) / max(len(windows), 1)

        return {
            "symbol": symbol,
            "windows": len(windows),
            "train_window": train_window,
            "test_window": test_window,
            "avg_return_pct": round(float(avg_return), 2),
            "consistency_pct": round(consistency * 100, 1),
            "window_details": windows,
        }
