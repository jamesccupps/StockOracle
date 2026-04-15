"""
Stock Oracle — Main Orchestrator
=================================
Coordinates all collectors, ML pipeline, and produces
final predictions with visualization-ready output.

Usage:
    from stock_oracle.oracle import StockOracle

    oracle = StockOracle()
    result = oracle.analyze("AAPL")
    print(result)

    # Analyze entire watchlist
    results = oracle.analyze_watchlist()
"""
import json
import logging
import sys
import io
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

# Setup logging — force UTF-8 on Windows to avoid cp1252 crashes
_stdout_handler = logging.StreamHandler(
    io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    if hasattr(sys.stdout, "buffer") else sys.stdout
)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    handlers=[
        _stdout_handler,
        logging.FileHandler("stock_oracle.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("stock_oracle")

from stock_oracle.config import WATCHLIST, DATA_DIR
from stock_oracle.collectors.base import SignalResult
from stock_oracle.collectors.yahoo_finance import YahooFinanceCollector
from stock_oracle.collectors.reddit_sentiment import RedditSentimentCollector
from stock_oracle.collectors.sec_edgar import SECEdgarCollector
from stock_oracle.collectors.job_postings import JobPostingsCollector
from stock_oracle.collectors.advanced_signals import (
    SupplyChainCollector,
    GovernmentContractsCollector,
    PatentActivityCollector,
    CongressionalTradesCollector,
)
from stock_oracle.collectors.alt_data import (
    AppStoreCollector,
    SeasonalityCollector,
    WeatherCorrelationCollector,
    NewsSentimentCollector,
    ShippingActivityCollector,
    DomainRegistrationCollector,
    EarningsCallNLPCollector,
    EmployeeSentimentCollector,
)
from stock_oracle.collectors.creative_signals import (
    WaffleHouseIndexCollector,
    GitHubVelocityCollector,
    GoogleTrendsCollector,
    CardboardIndexCollector,
    WikipediaVelocityCollector,
    EnergyCascadeCollector,
    HackerNewsSentimentCollector,
    TalentFlowCollector,
)
from stock_oracle.collectors.cross_stock import (
    CrossStockCollector,
    EarningsContagionCollector,
)
from stock_oracle.collectors.analysis import (
    TechnicalAnalysisCollector,
    FundamentalAnalysisCollector,
    AnalystConsensusCollector,
    OptionsFlowCollector,
    ShortInterestCollector,
)
from stock_oracle.collectors.new_indicators import (
    FearGreedProxyCollector,
    DividendVsTreasuryCollector,
    MomentumQualityCollector,
    InsiderRatioCollector,
    MarketPulseCollector,
)
from stock_oracle.collectors.finnhub_collector import FinnhubCollector
from stock_oracle.collectors.viral_catalyst import ViralCatalystCollector
from stock_oracle.collectors.realtime_news import RealtimeNewsCollector
from stock_oracle.ml.pipeline import StockPredictor, Backtester
from stock_oracle.signal_intelligence import SignalIntelligence
from stock_oracle.market_regime import MarketRegimeDetector


class StockOracle:
    """
    The main oracle. Runs all collectors, combines signals,
    and produces ML-enhanced predictions.
    """

    def __init__(self, use_ml: bool = True, parallel: bool = True):
        self.use_ml = use_ml
        self.parallel = parallel
        self.skip_slow = False  # Set True during monitoring to skip Ollama collectors

        # Signal intelligence — learns stale signals, volatility, thresholds
        self.intelligence = SignalIntelligence()

        # Market regime — detects broad selloffs, rallies, etc.
        self.regime_detector = MarketRegimeDetector()

        # Collectors that use Ollama (5-7s each) — skippable during monitoring
        self.SLOW_COLLECTORS = {"employee_sentiment", "earnings_nlp"}

        # Initialize all collectors
        self.collectors = [
            # Core market data
            YahooFinanceCollector(),
            FinnhubCollector(),  # Real-time prices (needs API key in Settings)
            ViralCatalystCollector(),  # Viral executive/brand moments
            # Technical & fundamental analysis (yfinance — no API key)
            TechnicalAnalysisCollector(),  # RSI, MACD, Bollinger, MA crossovers
            FundamentalAnalysisCollector(),  # P/E, margins, growth, debt
            AnalystConsensusCollector(),  # Price targets, buy/hold/sell
            OptionsFlowCollector(),  # Put/call ratio, unusual activity
            ShortInterestCollector(),  # Short interest % and trends
            # Social / sentiment
            RedditSentimentCollector(),
            NewsSentimentCollector(),
            HackerNewsSentimentCollector(),
            # Regulatory / insider
            SECEdgarCollector(),
            CongressionalTradesCollector(),
            # Corporate activity
            JobPostingsCollector(),
            SupplyChainCollector(),
            GovernmentContractsCollector(),
            PatentActivityCollector(),
            GitHubVelocityCollector(),
            # Consumer / demand signals
            AppStoreCollector(),
            GoogleTrendsCollector(),
            WikipediaVelocityCollector(),
            # Macro / alternative
            SeasonalityCollector(),
            WeatherCorrelationCollector(),
            EnergyCascadeCollector(),
            CardboardIndexCollector(),
            WaffleHouseIndexCollector(),
            TalentFlowCollector(),
            # Infrastructure (need API keys)
            ShippingActivityCollector(),
            DomainRegistrationCollector(),
            EarningsCallNLPCollector(),
            EmployeeSentimentCollector(),
            # Cross-stock correlation
            CrossStockCollector(),
            EarningsContagionCollector(),
            # New analytical indicators
            FearGreedProxyCollector(),       # VIX + breadth + safe haven
            DividendVsTreasuryCollector(),   # Yield gap analysis
            MomentumQualityCollector(),      # Trend quality scoring
            InsiderRatioCollector(),         # SEC Form 4 buy/sell ratio
            MarketPulseCollector(),          # Broad market health (SPY, TLT, DXY, EFA)
            RealtimeNewsCollector(),         # Real-time Finnhub news (15min cache, recency weighted)
        ]

        # Initialize ML
        self.predictor = StockPredictor()
        self.predictor.load_models()
        self.backtester = Backtester()

        # Initialize prediction tracker (feedback loop)
        from stock_oracle.prediction_tracker import PredictionTracker
        self.tracker = PredictionTracker()

        # Pre-disable known-dead hosts to avoid wasting scan time
        # crt.sh: chronically times out (8s per ticker × 27 tickers = 216s wasted)
        # Senate/House S3: permanently 403 (datasets went private)
        from stock_oracle.collectors.base import BaseCollector
        for dead_host in [
            "crt.sh",
            "senate-stock-watcher-data.s3-us-west-2.amazonaws.com",
            "house-stock-watcher-data.s3-us-west-2.amazonaws.com",
        ]:
            BaseCollector._host_failures[dead_host] = 10  # Permanently disabled

        logger.info(f"StockOracle initialized with {len(self.collectors)} collectors")

    def analyze(self, ticker: str, verbose: bool = False) -> Dict:
        """
        Run full analysis on a single ticker.
        Returns a comprehensive prediction with signal breakdown.
        """
        logger.info(f"--- Analyzing {ticker} ---")
        start_time = datetime.now()

        # Auto-reload ML models if trained since last check
        if self.use_ml and not self.predictor.is_trained:
            self.predictor.load_models()

        # Collect all signals
        signals = self._collect_all(ticker)

        # Get price history for ML features
        price_history = []
        yahoo = self.collectors[0]  # YahooFinanceCollector is always first
        if hasattr(yahoo, "get_price_history"):
            price_history = yahoo.get_price_history(ticker)

        # ── Signal Intelligence ──
        # Detect market session
        try:
            from stock_oracle.collectors.finnhub_collector import get_market_session
            market_session_info = get_market_session()
            market_session = market_session_info.get("session", "regular")
        except Exception:
            market_session = "regular"

        # Extract current price for volatility tracking
        current_price = 0
        for s in signals:
            if s.collector_name in ("finnhub_realtime", "yahoo_finance"):
                raw = getattr(s, "raw_data", {}) or {}
                if isinstance(raw, dict):
                    current_price = raw.get("current_price", 0) or raw.get("price", 0)
                    if current_price:
                        break
        if not current_price and price_history:
            current_price = price_history[-1].get("close", 0)

        # Update intelligence with this scan's signals + market session
        signal_dicts = [s.to_dict() for s in signals]
        if current_price > 0:
            self.intelligence.update(ticker, signal_dicts, current_price,
                                     market_session=market_session)

        # Get intelligence-adjusted signals (staleness, detrending, freshness)
        adjusted_signals = self.intelligence.get_adjusted_signals(ticker, signal_dicts)

        # Get volatility-adaptive threshold for this ticker
        conviction_threshold = self.intelligence.get_conviction_threshold(ticker)
        min_dynamic = self.intelligence.get_min_dynamic_for_conviction()

        # Generate predictions using adjusted signals
        weighted_pred = self.predictor.predict_weighted(
            adjusted_signals,
            conviction_threshold=conviction_threshold,
            min_dynamic_for_conviction=min_dynamic,
        )

        ml_pred = None
        if self.use_ml:
            ml_pred = self.predictor.predict_ml(
                signal_dicts,  # ML uses raw signals — it learns its own patterns
                price_history,
            )

        # ── Blend predictions ──
        # Weighted analysis gets 60% (real data with intelligence),
        # ML gets 40% (learned patterns from historical data).

        # Get market regime (cached 10min, shared across tickers)
        regime = self.regime_detector.detect()
        regime_bias = regime.get("bias", 0)

        if ml_pred and ml_pred.get("method") == "ml_ensemble":
            # Extract ML's continuous signal from probabilities
            probs = ml_pred.get("probabilities", {})
            ml_signal = (
                probs.get("bullish", 0.33) * 0.5 +
                probs.get("neutral", 0.34) * 0.0 +
                probs.get("bearish", 0.33) * -0.5
            )

            # Blend: 60% weighted analysis, 40% ML
            weighted_signal = weighted_pred.get("signal", 0)
            blended_signal = weighted_signal * 0.6 + ml_signal * 0.4

            # Apply market regime bias
            # In a selloff, this shifts everything bearish; in a rally, bullish
            blended_signal += regime_bias

            # Classification using adaptive threshold (not fixed 0.06)
            # Also enforce min dynamic signals gate
            dyn_count = weighted_pred.get("dynamic_signals", min_dynamic)
            if dyn_count < min_dynamic:
                # Exception: in a strong selloff, allow BEARISH even without
                # many dynamic signals — the macro is the signal
                if regime.get("regime") == "SELLOFF" and blended_signal < -conviction_threshold:
                    blended_prediction = "BEARISH"
                else:
                    blended_prediction = "NEUTRAL"
            elif blended_signal > conviction_threshold:
                blended_prediction = "BULLISH"
            elif blended_signal < -conviction_threshold:
                blended_prediction = "BEARISH"
            else:
                # In a selloff, NEUTRAL signals that are slightly negative
                # should be pushed to BEARISH (the market drags everything down)
                if (regime.get("regime") in ("SELLOFF", "DECLINING")
                        and blended_signal < 0
                        and abs(blended_signal) > conviction_threshold * 0.5):
                    blended_prediction = "BEARISH"
                elif (regime.get("regime") in ("RALLY", "RISING")
                        and blended_signal > 0
                        and abs(blended_signal) > conviction_threshold * 0.5):
                    blended_prediction = "BULLISH"
                else:
                    blended_prediction = "NEUTRAL"

            # Confidence: blend both, penalize disagreement
            weighted_conf = weighted_pred.get("confidence", 0.5)
            ml_conf = ml_pred.get("confidence", 0.33)
            agree = (weighted_pred.get("prediction") == ml_pred.get("prediction"))
            conf_penalty = 1.0 if agree else 0.8
            # Apply market session confidence penalty
            session_mult = self.intelligence.get_session_confidence_mult()
            blended_confidence = (weighted_conf * 0.6 + ml_conf * 0.4) * conf_penalty * session_mult

            primary_pred = {
                "prediction": blended_prediction,
                "signal": round(blended_signal, 4),
                "confidence": round(blended_confidence, 4),
                "method": "blended (60% analysis + 40% ML)",
            }
        else:
            primary_pred = weighted_pred
            # Apply regime bias to weighted-only prediction too
            if regime_bias != 0:
                primary_pred = dict(primary_pred)
                adj_signal = primary_pred.get("signal", 0) + regime_bias
                primary_pred["signal"] = round(adj_signal, 4)
                # Reclassify with adjusted signal
                if adj_signal > conviction_threshold:
                    primary_pred["prediction"] = "BULLISH"
                elif adj_signal < -conviction_threshold:
                    primary_pred["prediction"] = "BEARISH"
                elif (regime.get("regime") in ("SELLOFF", "DECLINING")
                        and adj_signal < 0
                        and abs(adj_signal) > conviction_threshold * 0.5):
                    primary_pred["prediction"] = "BEARISH"
                else:
                    primary_pred["prediction"] = "NEUTRAL"

            # Apply session confidence to weighted-only prediction too
            session_mult = self.intelligence.get_session_confidence_mult()
            if session_mult < 1.0:
                primary_pred = dict(primary_pred)
                primary_pred["confidence"] = round(
                    primary_pred.get("confidence", 0.5) * session_mult, 4
                )

        # Build result
        elapsed = (datetime.now() - start_time).total_seconds()

        # Intelligence metadata for this ticker
        stale_collectors = self.intelligence.get_stale_collectors(ticker)
        dynamic_count = self.intelligence.get_dynamic_signal_count(ticker)
        volatility = self.intelligence.get_volatility(ticker)

        result = {
            "ticker": ticker,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "elapsed_seconds": round(elapsed, 2),

            # Primary prediction (blended)
            "prediction": primary_pred["prediction"],
            "signal": primary_pred["signal"],
            "confidence": primary_pred["confidence"],
            "method": primary_pred["method"],

            # Detailed breakdown
            "weighted_prediction": weighted_pred,
            "ml_prediction": ml_pred,

            # Individual signals (raw for compatibility)
            "signals": [s.to_dict() for s in signals],
            "signal_summary": self._summarize_signals(signals),

            # Price context
            "price": current_price,
            "price_data": price_history[-5:] if price_history else [],

            # Signal intelligence
            "conviction_threshold": round(conviction_threshold, 4),
            "volatility": round(volatility, 6),
            "dynamic_signals": dynamic_count,
            "stale_signals": len(stale_collectors),
            "market_session": market_session,

            # Market regime
            "market_regime": regime.get("regime", "UNKNOWN"),
            "regime_bias": regime_bias,
            "regime_detail": regime.get("detail", ""),
        }

        if verbose:
            self._print_analysis(result)

        # Auto-log for ML training
        try:
            from stock_oracle.trainer import log_analysis
            log_analysis(result)
        except Exception:
            pass  # Training logger is optional

        # Record prediction for verification and feedback loop
        try:
            self.tracker.record_prediction(result)
        except Exception:
            pass  # Tracker is optional

        # Save intelligence periodically (every 27 tickers ≈ once per scan cycle)
        if self.intelligence._stats["total_scans"] % 27 == 0:
            try:
                self.intelligence.save()
            except Exception:
                pass

        return result

    def analyze_watchlist(self, tickers: List[str] = None, verbose: bool = True) -> List[Dict]:
        """Analyze all tickers in the watchlist."""
        tickers = tickers or WATCHLIST
        results = []

        for ticker in tickers:
            try:
                result = self.analyze(ticker, verbose=verbose)
                results.append(result)
            except Exception as e:
                logger.error(f"Failed to analyze {ticker}: {e}")
                results.append({
                    "ticker": ticker,
                    "error": str(e),
                    "prediction": "ERROR",
                })

        # Sort by signal strength
        results.sort(key=lambda r: abs(r.get("signal", 0)), reverse=True)

        # Save results
        self._save_results(results)

        if verbose:
            self._print_watchlist_summary(results)

        return results

    def _collect_all(self, ticker: str) -> List[SignalResult]:
        """Run all collectors (parallel or sequential)."""
        signals = []

        # Filter out slow collectors during monitoring
        active_collectors = self.collectors
        if self.skip_slow:
            active_collectors = [c for c in self.collectors
                                 if c.name not in self.SLOW_COLLECTORS]

        if self.parallel:
            with ThreadPoolExecutor(max_workers=8) as executor:
                futures = {
                    executor.submit(c._safe_collect, ticker): c.name
                    for c in active_collectors
                }
                for future in as_completed(futures):
                    name = futures[future]
                    try:
                        result = future.result(timeout=30)
                        signals.append(result)
                        if result.confidence > 0:
                            logger.info(f"  {result}")
                    except Exception as e:
                        logger.error(f"  {name} timed out: {e}")
        else:
            for collector in self.collectors:
                result = collector._safe_collect(ticker)
                signals.append(result)
                if result.confidence > 0:
                    logger.info(f"  {result}")

        return signals

    def _summarize_signals(self, signals: List[SignalResult]) -> Dict:
        """Create a human-readable signal summary."""
        active = [s for s in signals if s.confidence > 0.1]
        bullish = [s for s in active if s.signal_value > 0.1]
        bearish = [s for s in active if s.signal_value < -0.1]
        neutral = [s for s in active if -0.1 <= s.signal_value <= 0.1]

        return {
            "total_signals": len(signals),
            "active_signals": len(active),
            "bullish_count": len(bullish),
            "bearish_count": len(bearish),
            "neutral_count": len(neutral),
            "strongest_bull": max(
                (s.to_dict() for s in bullish),
                key=lambda x: x["signal"],
                default=None,
            ),
            "strongest_bear": min(
                (s.to_dict() for s in bearish),
                key=lambda x: x["signal"],
                default=None,
            ),
            "consensus": (
                "BULLISH" if len(bullish) > len(bearish) + 2
                else "BEARISH" if len(bearish) > len(bullish) + 2
                else "MIXED"
            ),
        }

    def _print_analysis(self, result: Dict):
        """Pretty-print analysis results."""
        t = result["ticker"]
        pred = result["prediction"]
        sig = result["signal"]
        conf = result["confidence"]

        # Color-coded output
        color = {"BULLISH": "\033[92m", "BEARISH": "\033[91m", "NEUTRAL": "\033[93m"}
        reset = "\033[0m"
        c = color.get(pred, "\033[93m")

        print(f"\n{'='*60}")
        print(f"  {t}  |  {c}{pred}{reset}  |  Signal: {sig:+.4f}  |  Conf: {conf:.0%}")
        print(f"{'='*60}")

        summary = result.get("signal_summary", {})
        print(f"  Signals: {summary.get('bullish_count', 0)} bull / "
              f"{summary.get('bearish_count', 0)} bear / "
              f"{summary.get('neutral_count', 0)} neutral")
        print(f"  Consensus: {summary.get('consensus', '?')}")

        # Print active signals
        for sig_data in result.get("signals", []):
            if sig_data.get("confidence", 0) > 0.1:
                s = sig_data["signal"]
                icon = "[+]" if s > 0.1 else "[-]" if s < -0.1 else "[ ]"
                print(f"  {icon} {sig_data['collector']:24s} | "
                      f"sig={s:+.2f} conf={sig_data['confidence']:.0%} | "
                      f"{sig_data.get('details', '')[:50]}")

        # Price context
        if result.get("price_data"):
            latest = result["price_data"][-1]
            print(f"\n  Latest price: ${latest.get('close', 0):.2f}")

        print(f"  Elapsed: {result.get('elapsed_seconds', 0)}s\n")

    def _print_watchlist_summary(self, results: List[Dict]):
        """Print a summary table of all watchlist results."""
        print(f"\n{'='*70}")
        print(f"  STOCK ORACLE — WATCHLIST SUMMARY")
        print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*70}")
        print(f"  {'Ticker':<8} {'Prediction':<12} {'Signal':>8} {'Confidence':>12} {'Consensus'}")
        print(f"  {'-'*8} {'-'*12} {'-'*8} {'-'*12} {'-'*10}")

        for r in results:
            if r.get("error"):
                print(f"  {r['ticker']:<8} ERROR: {r['error'][:40]}")
                continue

            pred = r.get("prediction", "?")
            sig = r.get("signal", 0)
            conf = r.get("confidence", 0)
            consensus = r.get("signal_summary", {}).get("consensus", "?")

            color = {"BULLISH": "\033[92m", "BEARISH": "\033[91m"}.get(pred, "\033[93m")
            reset = "\033[0m"

            print(f"  {r['ticker']:<8} {color}{pred:<12}{reset} {sig:>+8.4f} "
                  f"{conf:>11.0%} {consensus}")

        print(f"{'='*70}\n")

    def _save_results(self, results: List[Dict]):
        """Save analysis results to JSON."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = DATA_DIR / f"analysis_{timestamp}.json"
        with open(output_file, "w") as f:
            json.dump(results, f, indent=2, default=str)
        logger.info(f"Results saved to {output_file}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CLI Entry Point
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main():
    """Command-line interface for Stock Oracle."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Stock Oracle — Multi-Signal Stock Prediction",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m stock_oracle.oracle AAPL              # Analyze single stock
  python -m stock_oracle.oracle AAPL TSLA NVDA    # Analyze multiple
  python -m stock_oracle.oracle --watchlist        # Analyze default watchlist
  python -m stock_oracle.oracle --all              # Verbose all signals
        """,
    )
    parser.add_argument("tickers", nargs="*", help="Stock ticker(s) to analyze")
    parser.add_argument("--watchlist", "-w", action="store_true", help="Analyze full watchlist")
    parser.add_argument("--all", "-a", action="store_true", help="Verbose output")
    parser.add_argument("--no-ml", action="store_true", help="Skip ML predictions")
    parser.add_argument("--sequential", "-s", action="store_true", help="Run collectors sequentially")

    args = parser.parse_args()

    oracle = StockOracle(
        use_ml=not args.no_ml,
        parallel=not args.sequential,
    )

    if args.watchlist or not args.tickers:
        oracle.analyze_watchlist(verbose=True)
    else:
        for ticker in args.tickers:
            oracle.analyze(ticker.upper(), verbose=True)


if __name__ == "__main__":
    main()
