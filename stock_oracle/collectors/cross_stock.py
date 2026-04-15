"""
Cross-Stock Correlation Engine
===============================
Stocks don't move in isolation. This module analyzes how stocks
affect each other and finds tradeable patterns in those relationships.

Signals:
  1. Sector momentum    — if your sector is ripping/dumping, you follow
  2. Lead-lag pairs     — stock A moves, stock B follows 1-3 days later
  3. Pair divergence    — correlated stocks diverge = mean reversion opportunity
  4. Earnings contagion — one company's earnings predict the sector
  5. ETF flow effects   — money into/out of sector ETFs drags components
  6. Correlation regime  — detect when correlations break (regime change)
"""
import logging
import math
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np

from stock_oracle.collectors.base import BaseCollector, SignalResult

logger = logging.getLogger("stock_oracle")

try:
    import yfinance as yf
    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False


# ── Sector & ETF Mappings ──────────────────────────────────────

SECTOR_MAP = {
    # Tech / Semiconductors
    "AAPL": "tech", "MSFT": "tech", "GOOGL": "tech", "META": "tech",
    "NVDA": "semis", "AMD": "semis", "INTC": "semis", "TSM": "semis",
    "AVGO": "semis", "QCOM": "semis", "MU": "semis", "MRVL": "semis",
    "AMZN": "tech", "NFLX": "tech", "CRM": "tech", "ORCL": "tech",
    "ADBE": "tech", "NOW": "tech", "SNOW": "tech", "PLTR": "tech",
    # Space & Defense
    "LUNR": "space", "RKLB": "space", "ASTS": "space", "SPCE": "space",
    "BA": "defense", "LMT": "defense", "NOC": "defense", "RTX": "defense",
    "GD": "defense", "KTOS": "defense",
    # EV & Auto
    "TSLA": "ev", "RIVN": "ev", "LCID": "ev", "NIO": "ev",
    "F": "auto", "GM": "auto", "TM": "auto",
    # Finance
    "JPM": "finance", "BAC": "finance", "GS": "finance", "MS": "finance",
    "WFC": "finance", "C": "finance", "SCHW": "finance",
    # Energy
    "XOM": "energy", "CVX": "energy", "COP": "energy", "SLB": "energy",
    # Retail
    "WMT": "retail", "TGT": "retail", "COST": "retail", "HD": "retail",
    "LOW": "retail", "AMZN": "retail",
    # Pharma / Biotech
    "JNJ": "pharma", "PFE": "pharma", "MRK": "pharma", "ABBV": "pharma",
    "LLY": "pharma", "UNH": "pharma",
    # AI plays (cross-sector, but trade as a group)
    "NVDA": "ai", "AMD": "ai", "MSFT": "ai", "GOOGL": "ai",
    "PLTR": "ai", "SMCI": "ai", "META": "ai",
}

# Stocks within each sector that tend to move together
SECTOR_PEERS = {
    "semis": ["NVDA", "AMD", "INTC", "TSM", "AVGO", "QCOM", "MU", "MRVL"],
    "tech": ["AAPL", "MSFT", "GOOGL", "META", "AMZN", "NFLX", "CRM"],
    "space": ["LUNR", "RKLB", "ASTS", "BKSY", "SPCE", "SPIR"],
    "defense": ["LMT", "NOC", "RTX", "GD", "BA", "HII", "LHX"],
    "ev": ["TSLA", "RIVN", "LCID", "NIO", "XPEV", "LI"],
    "finance": ["JPM", "BAC", "GS", "MS", "WFC", "C"],
    "energy": ["XOM", "CVX", "COP", "SLB", "EOG", "PXD"],
    "retail": ["WMT", "TGT", "COST", "HD", "LOW"],
    "pharma": ["JNJ", "PFE", "MRK", "ABBV", "LLY"],
    "ai": ["NVDA", "AMD", "MSFT", "GOOGL", "PLTR", "SMCI", "META"],
}

# Sector ETFs — track money flow
SECTOR_ETFS = {
    "semis": "SMH",    # VanEck Semiconductor
    "tech": "QQQ",     # Nasdaq 100
    "defense": "ITA",  # iShares Aerospace & Defense
    "finance": "XLF",  # Financial Select
    "energy": "XLE",   # Energy Select
    "retail": "XRT",   # S&P Retail
    "pharma": "XLV",   # Health Care Select
    "ev": "DRIV",      # Global X Autonomous & EV
    "space": "UFO",    # Procure Space ETF
    "ai": "BOTZ",      # Global X Robotics & AI
}

# Known lead-lag relationships
# Format: (leader, follower, typical_lag_days, correlation_strength)
LEAD_LAG_PAIRS = [
    ("NVDA", "AMD", 1, 0.75),      # NVDA leads AMD by ~1 day
    ("NVDA", "TSM", 1, 0.70),      # NVDA leads its fab
    ("NVDA", "SMCI", 1, 0.65),     # NVDA leads server builders
    ("AAPL", "QCOM", 1, 0.60),     # Apple leads its chip suppliers
    ("AAPL", "TSM", 2, 0.55),      # Apple leads its fab
    ("TSLA", "RIVN", 1, 0.70),     # Tesla leads EV sector
    ("TSLA", "NIO", 1, 0.55),      # Tesla leads Chinese EVs
    ("JPM", "BAC", 1, 0.80),       # JPMorgan leads banks
    ("JPM", "GS", 1, 0.75),
    ("XOM", "CVX", 1, 0.85),       # Oil majors move together, XOM slightly leads
    ("LMT", "NOC", 1, 0.70),       # Lockheed leads defense
    ("LMT", "LUNR", 2, 0.40),      # Defense primes lead space subcontractors
    ("RKLB", "LUNR", 1, 0.55),     # Space stocks correlate
    ("WMT", "TGT", 1, 0.65),       # Walmart leads Target
    ("META", "SNAP", 1, 0.60),     # Meta leads social media
    ("MSFT", "CRM", 1, 0.55),      # Microsoft leads enterprise SaaS
]


class CrossStockCollector(BaseCollector):
    """
    Analyzes cross-stock relationships to generate signals.

    Combines:
    - Sector momentum (is the sector moving?)
    - Lead-lag signals (did a leader stock just move?)
    - Pair divergence (is this stock lagging its peers?)
    - ETF flow direction (money flowing into/out of sector?)
    """

    @property
    def name(self) -> str:
        return "cross_stock"

    def collect(self, ticker: str) -> SignalResult:
        if not HAS_YFINANCE:
            return self._neutral_signal(ticker, "yfinance required for cross-stock analysis")

        cached = self._get_cached(ticker, "cross")
        if cached:
            return SignalResult.from_dict(cached)

        ticker = ticker.upper()

        # Get all sub-signals
        sector_sig = self._sector_momentum(ticker)
        leadlag_sig = self._lead_lag_signal(ticker)
        divergence_sig = self._pair_divergence(ticker)
        etf_sig = self._etf_flow_signal(ticker)

        # Combine with weights
        signal = (
            sector_sig.get("signal", 0) * 0.30 +
            leadlag_sig.get("signal", 0) * 0.30 +
            divergence_sig.get("signal", 0) * 0.25 +
            etf_sig.get("signal", 0) * 0.15
        )

        confidence = max(
            sector_sig.get("confidence", 0),
            leadlag_sig.get("confidence", 0),
            divergence_sig.get("confidence", 0),
            etf_sig.get("confidence", 0),
        )

        details_parts = []
        if sector_sig.get("signal"): details_parts.append(f"sector={sector_sig['signal']:+.2f}")
        if leadlag_sig.get("signal"): details_parts.append(f"lead-lag={leadlag_sig['signal']:+.2f}")
        if divergence_sig.get("signal"): details_parts.append(f"diverge={divergence_sig['signal']:+.2f}")
        if etf_sig.get("signal"): details_parts.append(f"etf={etf_sig['signal']:+.2f}")

        result = SignalResult(
            collector_name=self.name,
            ticker=ticker,
            signal_value=max(-1.0, min(1.0, signal)),
            confidence=confidence,
            raw_data={
                "sector": sector_sig,
                "lead_lag": leadlag_sig,
                "divergence": divergence_sig,
                "etf_flow": etf_sig,
            },
            details=" | ".join(details_parts) if details_parts else "No cross-stock signals",
        )

        self._set_cache(result.to_dict(), ticker, "cross")
        return result

    def _get_returns(self, symbol: str, days: int = 30) -> Optional[np.ndarray]:
        """Get daily returns for a symbol."""
        try:
            stock = yf.Ticker(symbol)
            hist = stock.history(period=f"{days + 5}d")
            if len(hist) < 5:
                return None
            closes = hist["Close"].values
            # Guard against NaN in price data
            if np.any(np.isnan(closes)):
                closes = closes[~np.isnan(closes)]
                if len(closes) < 5:
                    return None
            returns = np.diff(closes) / closes[:-1]
            # Remove any NaN/Inf returns
            returns = returns[np.isfinite(returns)]
            if len(returns) == 0:
                return None
            return returns[-days:] if len(returns) >= days else returns
        except Exception:
            return None

    # ── 1. Sector Momentum ─────────────────────────────────────

    def _sector_momentum(self, ticker: str) -> Dict:
        """
        Is the sector moving? If peers are all up, you'll likely follow.
        If you're the only one down while peers are up, you might catch up.
        """
        # Find which sectors this ticker belongs to
        sectors = [s for s, peers in SECTOR_PEERS.items()
                   if ticker in peers]

        if not sectors:
            return {"signal": 0, "confidence": 0, "detail": "Not in any sector group"}

        sector = sectors[0]
        peers = [p for p in SECTOR_PEERS[sector] if p != ticker][:5]

        if not peers:
            return {"signal": 0, "confidence": 0}

        # Get recent returns for peers
        peer_returns = []
        for peer in peers:
            ret = self._get_returns(peer, days=5)
            if ret is not None and len(ret) > 0:
                # Total 5-day return
                total = float(np.prod(1 + ret) - 1)
                if np.isfinite(total):
                    peer_returns.append(total)

        if not peer_returns:
            return {"signal": 0, "confidence": 0}

        # Sector average momentum
        sector_avg = float(np.mean(peer_returns))
        if not np.isfinite(sector_avg):
            return {"signal": 0, "confidence": 0}

        # Get ticker's own return
        my_ret = self._get_returns(ticker, days=5)
        if my_ret is None or len(my_ret) == 0:
            # No data for ticker, just use sector direction
            signal = max(-1.0, min(1.0, sector_avg * 1.5))  # Was *3
            return {
                "signal": signal,
                "confidence": min(0.5, len(peer_returns) / 5),
                "sector": sector,
                "sector_return_5d": round(float(sector_avg), 4),
                "peers_sampled": len(peer_returns),
            }

        my_total = float(np.prod(1 + my_ret) - 1)
        if not np.isfinite(my_total):
            signal = max(-1.0, min(1.0, float(sector_avg) * 1.5))
            return {
                "signal": signal,
                "confidence": min(0.5, len(peer_returns) / 5),
                "sector": sector,
                "sector_return_5d": round(float(sector_avg), 4),
                "peers_sampled": len(peer_returns),
            }

        # If sector is up but I'm down, I might catch up (bullish)
        # If sector is down but I'm up, I might fall (bearish)
        gap = sector_avg - my_total

        signal = max(-1.0, min(1.0, gap * 2))  # Was *5 — too aggressive

        return {
            "signal": signal,
            "confidence": min(0.6, len(peer_returns) / 4),
            "sector": sector,
            "sector_return_5d": round(float(sector_avg), 4),
            "ticker_return_5d": round(my_total, 4),
            "gap": round(float(gap), 4),
            "peers_sampled": len(peer_returns),
            "detail": f"{sector} avg={sector_avg:+.1%} vs {ticker}={my_total:+.1%}",
        }

    # ── 2. Lead-Lag Signals ────────────────────────────────────

    def _lead_lag_signal(self, ticker: str) -> Dict:
        """
        Check if any known leader stocks have recently moved.
        If NVDA dropped 5% yesterday and you hold AMD, that's bearish for AMD today.
        """
        # Find pairs where this ticker is the follower
        relevant_pairs = [
            (leader, lag, corr) for leader, follower, lag, corr in LEAD_LAG_PAIRS
            if follower == ticker
        ]

        if not relevant_pairs:
            # Also check if this ticker IS a leader (helpful context)
            is_leader_for = [
                follower for leader, follower, lag, corr in LEAD_LAG_PAIRS
                if leader == ticker
            ]
            detail = f"Leads: {', '.join(is_leader_for[:3])}" if is_leader_for else "No lead-lag pairs"
            return {"signal": 0, "confidence": 0, "detail": detail}

        signals = []
        for leader, lag, corr in relevant_pairs:
            ret = self._get_returns(leader, days=lag + 2)
            if ret is not None and len(ret) >= lag:
                # Get the leader's return from `lag` days ago
                leader_return = float(ret[-lag])
                # Scale by correlation strength
                expected_effect = leader_return * corr
                signals.append({
                    "leader": leader,
                    "leader_return": round(leader_return, 4),
                    "lag_days": lag,
                    "correlation": corr,
                    "expected_effect": round(expected_effect, 4),
                })

        if not signals:
            return {"signal": 0, "confidence": 0}

        # Average expected effect from all leaders
        avg_effect = np.mean([s["expected_effect"] for s in signals])
        signal = max(-1.0, min(1.0, avg_effect * 10))

        return {
            "signal": signal,
            "confidence": min(0.6, len(signals) * 0.2),
            "pairs": signals,
            "detail": f"{len(signals)} leaders: {', '.join(s['leader'] for s in signals)}",
        }

    # ── 3. Pair Divergence ─────────────────────────────────────

    def _pair_divergence(self, ticker: str) -> Dict:
        """
        Find normally-correlated stocks that have diverged.
        If NVDA is up 10% this month but AMD is flat, AMD might catch up.
        This is the basis of pair trading / mean reversion.
        """
        sectors = [s for s, peers in SECTOR_PEERS.items() if ticker in peers]
        if not sectors:
            return {"signal": 0, "confidence": 0}

        sector = sectors[0]
        peers = [p for p in SECTOR_PEERS[sector] if p != ticker][:4]

        my_ret = self._get_returns(ticker, days=20)
        if my_ret is None or len(my_ret) < 10:
            return {"signal": 0, "confidence": 0}

        my_total = float(np.prod(1 + my_ret) - 1)
        if not np.isfinite(my_total):
            return {"signal": 0, "confidence": 0}

        peer_totals = []
        for peer in peers:
            ret = self._get_returns(peer, days=20)
            if ret is not None and len(ret) >= 10:
                pt = float(np.prod(1 + ret) - 1)
                if np.isfinite(pt):
                    peer_totals.append(pt)

        if not peer_totals:
            return {"signal": 0, "confidence": 0}

        peer_avg = float(np.mean(peer_totals))
        if not np.isfinite(peer_avg):
            return {"signal": 0, "confidence": 0}
        divergence = peer_avg - my_total

        # Large divergence = mean reversion opportunity
        # Positive divergence = peers did better = I should catch up (bullish)
        # Negative divergence = I outperformed = might pull back (bearish)
        signal = max(-1.0, min(1.0, divergence * 1.5))  # Was *3 — too aggressive

        return {
            "signal": signal,
            "confidence": min(0.5, abs(divergence) * 5),
            "ticker_20d": round(my_total, 4),
            "peer_avg_20d": round(float(peer_avg), 4),
            "divergence": round(float(divergence), 4),
            "detail": f"20d: {ticker}={my_total:+.1%} vs peers={peer_avg:+.1%}",
        }

    # ── 4. ETF Flow Signal ─────────────────────────────────────

    def _etf_flow_signal(self, ticker: str) -> Dict:
        """
        Track sector ETF momentum as a proxy for institutional money flow.
        If SMH (semiconductor ETF) is surging, all semis benefit.
        Volume spikes in sector ETFs = big money moving.
        """
        sectors = [s for s, peers in SECTOR_PEERS.items() if ticker in peers]
        if not sectors:
            return {"signal": 0, "confidence": 0}

        sector = sectors[0]
        etf = SECTOR_ETFS.get(sector)
        if not etf:
            return {"signal": 0, "confidence": 0}

        try:
            etf_stock = yf.Ticker(etf)
            hist = etf_stock.history(period="30d")
            if len(hist) < 10:
                return {"signal": 0, "confidence": 0}

            closes = hist["Close"].values
            volumes = hist["Volume"].values

            # ETF momentum (5-day return)
            etf_return_5d = (closes[-1] - closes[-5]) / closes[-5]

            # Volume spike (today vs 20-day avg)
            vol_avg = np.mean(volumes[-20:])
            vol_ratio = volumes[-1] / vol_avg if vol_avg > 0 else 1.0

            # Strong ETF momentum + high volume = institutional conviction
            signal = etf_return_5d * 1.5  # Was *3 — too aggressive, clamped at ±1.0
            if vol_ratio > 1.5:
                signal *= 1.2  # Volume confirms the move

            signal = max(-1.0, min(1.0, signal))

            return {
                "signal": signal,
                "confidence": 0.4,
                "etf": etf,
                "etf_return_5d": round(float(etf_return_5d), 4),
                "volume_ratio": round(float(vol_ratio), 2),
                "detail": f"{etf} 5d={etf_return_5d:+.1%} vol={vol_ratio:.1f}x",
            }
        except Exception as e:
            return {"signal": 0, "confidence": 0, "detail": str(e)}


class EarningsContagionCollector(BaseCollector):
    """
    When one company in a sector reports earnings, it predicts
    the entire sector's direction. AMD's earnings predict NVDA.
    WMT's earnings predict TGT. JPM's earnings predict BAC.

    This collector checks if any sector peer recently had an
    earnings surprise and propagates the signal.
    """

    # Known earnings leaders (report first in their sector)
    EARNINGS_LEADERS = {
        "semis": ["TSM", "ASML"],           # Fabs report before chip designers
        "finance": ["JPM", "GS"],           # Big banks report first
        "retail": ["WMT", "COST"],          # Walmart/Costco set the tone
        "tech": ["MSFT", "GOOGL", "META"],  # Mega-cap tech reports and moves sector
        "defense": ["LMT", "RTX"],
    }

    @property
    def name(self) -> str:
        return "earnings_contagion"

    def collect(self, ticker: str) -> SignalResult:
        if not HAS_YFINANCE:
            return self._neutral_signal(ticker, "yfinance required")

        cached = self._get_cached(ticker, "earnings_contagion")
        if cached:
            return SignalResult.from_dict(cached)

        ticker = ticker.upper()
        sectors = [s for s, peers in SECTOR_PEERS.items() if ticker in peers]

        if not sectors:
            return self._neutral_signal(ticker, "Not in a tracked sector")

        sector = sectors[0]
        leaders = self.EARNINGS_LEADERS.get(sector, [])

        if not leaders or ticker in leaders:
            return self._neutral_signal(ticker, "No earnings leader data or ticker is the leader")

        # Check if any sector leader had a big move recently (earnings reaction proxy)
        contagion_signals = []
        for leader in leaders:
            try:
                stock = yf.Ticker(leader)
                hist = stock.history(period="10d")
                if len(hist) < 3:
                    continue

                closes = hist["Close"].values
                volumes = hist["Volume"].values

                # Look for a big single-day move (>3%) with high volume
                for i in range(1, min(5, len(closes))):
                    daily_return = (closes[i] - closes[i-1]) / closes[i-1]
                    vol_avg = np.mean(volumes) if len(volumes) > 0 else 1
                    vol_ratio = volumes[i] / vol_avg if vol_avg > 0 else 1

                    if abs(daily_return) > 0.03 and vol_ratio > 1.3:
                        contagion_signals.append({
                            "leader": leader,
                            "move": round(float(daily_return), 4),
                            "days_ago": len(closes) - 1 - i,
                            "volume_spike": round(float(vol_ratio), 2),
                        })
                        break  # Only care about the most recent big move
            except Exception:
                continue

        if not contagion_signals:
            return self._neutral_signal(ticker, "No recent earnings moves in sector leaders")

        # Average the contagion effect
        avg_move = np.mean([s["move"] for s in contagion_signals])
        # Contagion effect is typically 30-60% of the leader's move
        signal = max(-1.0, min(1.0, avg_move * 0.5 * 5))

        leaders_str = ", ".join(f"{s['leader']}={s['move']:+.1%}" for s in contagion_signals)

        result = SignalResult(
            collector_name=self.name,
            ticker=ticker,
            signal_value=signal,
            confidence=min(0.5, len(contagion_signals) * 0.25),
            raw_data={"contagion_signals": contagion_signals, "sector": sector},
            details=f"Sector {sector} leaders: {leaders_str}",
        )

        self._set_cache(result.to_dict(), ticker, "earnings_contagion")
        return result


def get_correlation_matrix(tickers: List[str], days: int = 60) -> Optional[Dict]:
    """
    Build a correlation matrix for a list of tickers.
    Useful for portfolio analysis and pair identification.

    Returns dict with 'matrix', 'tickers', and 'strongest_pairs'.
    """
    if not HAS_YFINANCE:
        return None

    import yfinance as yf

    try:
        data = yf.download(tickers, period=f"{days + 5}d", progress=False)
        if data.empty:
            return None

        closes = data["Close"]
        returns = closes.pct_change().dropna()

        if len(returns) < 10:
            return None

        corr = returns.corr()

        # Find strongest pairs
        pairs = []
        tickers_list = list(corr.columns)
        for i in range(len(tickers_list)):
            for j in range(i + 1, len(tickers_list)):
                c = corr.iloc[i, j]
                if not math.isnan(c):
                    pairs.append({
                        "pair": (tickers_list[i], tickers_list[j]),
                        "correlation": round(float(c), 4),
                    })

        pairs.sort(key=lambda x: abs(x["correlation"]), reverse=True)

        return {
            "matrix": {t: {t2: round(float(corr.loc[t, t2]), 4)
                          for t2 in tickers_list}
                       for t in tickers_list},
            "tickers": tickers_list,
            "strongest_pairs": pairs[:10],
            "weakest_pairs": pairs[-5:],
        }
    except Exception as e:
        logger.error(f"Correlation matrix error: {e}")
        return None
