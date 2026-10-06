"""
Oracle bridge
=============
Everything the terminal shows that comes from Stock Oracle itself:

  * ORC   — full analysis for a ticker (all collectors + ML + intelligence + regime),
            run on demand, one at a time (StockOracle.analyze isn't built for
            concurrent calls; the GUI runs it sequentially too)
  * W     — latest verdict per watchlist ticker, from this process's runs or,
            if the GUI is monitoring, from its newest session file — so the
            terminal shows the GUI's scans without re-running them
  * ACC   — 5-day and intraday accuracy (PredictionTracker / SessionTracker)
  * REG   — market regime (MarketRegimeDetector)
  * BRK   — breakout scanner (BreakoutDetector)
  * ASK   — Claude advisor with whatever is on screen as context (ClaudeAdvisor,
            same key, model, monthly cap and spend log as the GUI)

Progress and results are pushed to browsers through `notify` (the quote hub's
broadcast), so a long analysis never blocks a request.
"""
import asyncio
import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, List, Optional

from stock_oracle.terminal import settings
from stock_oracle.terminal import symbols as S
from stock_oracle.terminal.cache import AsyncTTLCache
from stock_oracle.terminal.util import plain

logger = logging.getLogger("stock_oracle.terminal")


def _iso_age(ts: str) -> Optional[float]:
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return max(0.0, time.time() - dt.timestamp())
    except Exception:
        return None


class OracleBridge:
    def __init__(self, notify: Callable[[Dict], None]):
        self.notify = notify
        self._oracle = None
        self._oracle_lock = threading.Lock()
        self._exec = ThreadPoolExecutor(max_workers=1, thread_name_prefix="oracle")
        self.results: Dict[str, Dict] = {}     # ticker -> terminal-run result (trimmed)
        self.queued: Dict[str, str] = {}       # ticker -> "queued" | "running"
        self.errors: Dict[str, str] = {}
        self.scan_state: Dict = {"active": False, "done": 0, "total": 0}
        self.cache = AsyncTTLCache(max_items=200)
        self._session_cache = {"key": None, "data": {}}
        self._regime_detector = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._scan_task: Optional[asyncio.Task] = None

    # ── engine ───────────────────────────────────────────────

    def _get_oracle(self):
        with self._oracle_lock:
            if self._oracle is None:
                logger.info("terminal: loading Oracle engine...")
                from stock_oracle.oracle import StockOracle
                self._oracle = StockOracle(use_ml=True, parallel=True)
            return self._oracle

    @property
    def loaded(self) -> bool:
        return self._oracle is not None

    def _notify_threadsafe(self, msg: Dict):
        if self._loop and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self.notify, msg)

    # ── analysis ─────────────────────────────────────────────

    async def run(self, ticker: str, fast: bool = False) -> Dict:
        """Queue an analysis. Returns immediately; the result arrives as an
        {"t": "oracle"} message and via get()."""
        self._loop = asyncio.get_running_loop()
        if not S.is_equity(ticker):
            raise ValueError(f"Oracle analyzes stocks and ETFs, not {ticker}")
        if ticker in self.queued:
            return {"ticker": ticker, "state": self.queued[ticker]}
        self.queued[ticker] = "queued"
        self.errors.pop(ticker, None)
        self.notify({"t": "oracle", "s": ticker, "state": "queued"})
        fut = self._loop.run_in_executor(self._exec, self._analyze_job, ticker, fast)
        fut.add_done_callback(lambda f: None if f.cancelled() else f.exception())
        return {"ticker": ticker, "state": "queued"}

    def _analyze_job(self, ticker: str, fast: bool):
        self.queued[ticker] = "running"
        self._notify_threadsafe({"t": "oracle", "s": ticker, "state": "running"})
        try:
            oracle = self._get_oracle()
            oracle.skip_slow = fast
            result = oracle.analyze(ticker, verbose=False)
            trimmed = plain(self._trim(result, oracle))
            self.results[ticker] = trimmed
            self.cache.invalidate(("acc",))
            self._notify_threadsafe({"t": "oracle", "s": ticker, "state": "done",
                                     "summary": self.summary(trimmed)})
        except Exception as e:
            logger.exception(f"terminal: Oracle analysis failed for {ticker}")
            self.errors[ticker] = str(e)
            self._notify_threadsafe({"t": "oracle", "s": ticker, "state": "error", "error": str(e)})
        finally:
            self.queued.pop(ticker, None)

    async def scan(self, tickers: List[str]) -> Dict:
        """Analyze a list of tickers in the background (fast mode, like the
        GUI's monitor: skips the slow Ollama collectors)."""
        if self.scan_state["active"]:
            return dict(self.scan_state)
        tickers = [t for t in tickers if S.is_equity(t)]
        self.scan_state = {"active": True, "done": 0, "total": len(tickers)}
        self.notify({"t": "scan", **self.scan_state})

        async def runner():
            try:
                for t in tickers:
                    if t not in self.queued:
                        await self.run(t, fast=True)
                    while t in self.queued:
                        await asyncio.sleep(0.5)
                    self.scan_state["done"] += 1
                    self.notify({"t": "scan", **self.scan_state})
            finally:
                self.scan_state["active"] = False
                self.notify({"t": "scan", **self.scan_state})

        self._scan_task = asyncio.create_task(runner())
        return dict(self.scan_state)

    def _trim(self, result: Dict, oracle) -> Dict:
        """Drop bulky raw_data, add weights, staleness and the narrative."""
        import stock_oracle.config as cfg
        try:
            from stock_oracle.narrative import generate_narrative
            narrative = generate_narrative(result)
        except Exception as e:
            narrative = ""
            logger.debug(f"narrative failed: {e}")
        try:
            stale = set(oracle.intelligence.get_stale_collectors(result["ticker"]))
        except Exception:
            stale = set()
        signals = []
        for s in result.get("signals", []):
            name = s.get("collector", "")
            signals.append({
                "collector": name,
                "signal": s.get("signal", 0),
                "confidence": s.get("confidence", 0),
                "details": (s.get("details") or "")[:240],
                "weight": cfg.SIGNAL_WEIGHTS.get(name, 0),
                "stale": name in stale,
            })
        signals.sort(key=lambda s: abs(s["signal"]) * max(s["weight"], 0.01) * s["confidence"], reverse=True)
        ml = result.get("ml_prediction") or {}
        wp = result.get("weighted_prediction") or {}
        keep = {k: result.get(k) for k in (
            "ticker", "timestamp", "elapsed_seconds", "prediction", "signal", "confidence",
            "method", "price", "conviction_threshold", "volatility", "dynamic_signals",
            "stale_signals", "market_session", "market_regime", "regime_bias", "regime_detail",
            "signal_summary")}
        summ = keep.get("signal_summary") or {}
        for k in ("strongest_bull", "strongest_bear"):
            if isinstance(summ.get(k), dict):
                summ[k] = {kk: summ[k].get(kk) for kk in ("collector", "signal", "confidence", "details")}
        keep.update({
            "signals": signals,
            "weighted": {k: wp.get(k) for k in ("prediction", "signal", "confidence",
                                                 "core_analysis_score", "dynamic_signals")},
            "ml": {"prediction": ml.get("prediction"), "confidence": ml.get("confidence"),
                   "method": ml.get("method"), "probabilities": ml.get("probabilities")} if ml else None,
            "narrative": narrative,
            "source": "terminal",
        })
        return keep

    # ── reading results ──────────────────────────────────────

    def _session_snapshots(self) -> Dict[str, Dict]:
        """Latest per-ticker snapshot from the GUI's newest monitoring session."""
        try:
            from stock_oracle.session_tracker import SESSIONS_DIR
            files = sorted(SESSIONS_DIR.glob("session_*.jsonl"),
                           key=lambda p: p.stat().st_mtime, reverse=True)
        except Exception:
            return {}
        if not files:
            return {}
        newest = files[0]
        st = newest.stat()
        key = (str(newest), st.st_mtime, st.st_size)
        if self._session_cache["key"] == key:
            return self._session_cache["data"]
        data: Dict[str, Dict] = {}
        try:
            with open(newest, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        snap = json.loads(line)
                    except Exception:
                        continue
                    t = snap.get("ticker")
                    if t:
                        data[t] = snap
        except Exception as e:
            logger.debug(f"session read failed: {e}")
        data = plain(data)
        for snap in data.values():
            snap["source"] = f"gui session {newest.stem.replace('session_', '')}"
        self._session_cache = {"key": key, "data": data}
        return data

    def best(self, ticker: str) -> Optional[Dict]:
        """Newest available result for a ticker (terminal run or GUI session)."""
        mine = self.results.get(ticker)
        theirs = self._session_snapshots().get(ticker)
        if mine and theirs:
            a, b = _iso_age(mine.get("timestamp", "")), _iso_age(theirs.get("timestamp", ""))
            if a is not None and b is not None and b < a:
                return theirs
            return mine
        return mine or theirs

    @staticmethod
    def summary(r: Dict) -> Dict:
        return {
            "ticker": r.get("ticker"),
            "prediction": r.get("prediction"),
            "signal": r.get("signal"),
            "confidence": r.get("confidence"),
            "price": r.get("price"),
            "timestamp": r.get("timestamp"),
            "age": _iso_age(r.get("timestamp", "")),
            "threshold": r.get("conviction_threshold"),
            "regime": r.get("market_regime"),
            "source": r.get("source", "terminal"),
        }

    def verdicts(self, tickers: List[str]) -> Dict[str, Dict]:
        out = {}
        for t in tickers:
            r = self.best(t)
            if r:
                out[t] = self.summary(r)
            if t in self.queued:
                out.setdefault(t, {"ticker": t})["state"] = self.queued[t]
        return out

    async def detail(self, ticker: str) -> Dict:
        r = self.best(ticker)
        acc = await self.accuracy()
        return {
            "ticker": ticker,
            "result": r,
            "state": self.queued.get(ticker),
            "error": self.errors.get(ticker),
            "age": _iso_age(r.get("timestamp", "")) if r else None,
            "accuracy": {
                "five_day": acc["by_ticker_5d"].get(ticker),
                "intraday": acc["by_ticker_intraday"].get(ticker),
            },
            "engine_loaded": self.loaded,
        }

    # ── accuracy ─────────────────────────────────────────────

    async def accuracy(self) -> Dict:
        return await self.cache.get(("acc",), 120, self._accuracy_sync)

    def _accuracy_sync(self) -> Dict:
        from stock_oracle.prediction_tracker import PredictionTracker
        from stock_oracle.session_tracker import SessionTracker, INTRADAY_VERIFIED_FILE
        five = PredictionTracker().get_accuracy_stats()
        intraday = SessionTracker.get_intraday_accuracy_summary()
        per_t: Dict[str, Dict] = {}
        by_pred: Dict[str, Dict] = {}
        if Path(INTRADAY_VERIFIED_FILE).exists():
            with open(INTRADAY_VERIFIED_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        v = json.loads(line)
                    except Exception:
                        continue
                    t = v.get("ticker", "?")
                    p = per_t.setdefault(t, {"total": 0, "correct": 0, "directional": 0})
                    p["total"] += 1
                    p["correct"] += bool(v.get("correct"))
                    p["directional"] += bool(v.get("directional_correct"))
                    k = by_pred.setdefault(v.get("prediction", "?"), {"total": 0, "correct": 0})
                    k["total"] += 1
                    k["correct"] += bool(v.get("correct"))
        for p in per_t.values():
            p["accuracy"] = round(p["correct"] / p["total"] * 100, 1) if p["total"] else 0
            p["directional_pct"] = round(p["directional"] / p["total"] * 100, 1) if p["total"] else 0
        return plain({
            "five_day": five,
            "intraday": {**intraday, "by_prediction": by_pred},
            "by_ticker_5d": five.get("ticker_accuracy", {}),
            "by_ticker_intraday": per_t,
        })

    # ── regime, breakouts ────────────────────────────────────

    def _regime_sync(self) -> Dict:
        if self._regime_detector is None:
            from stock_oracle.market_regime import MarketRegimeDetector
            self._regime_detector = MarketRegimeDetector()
        return plain(self._regime_detector.detect())

    async def regime(self) -> Dict:
        return await self.cache.get(("regime",), 60, self._regime_sync)

    @staticmethod
    def _breakouts_sync(tickers: List[str]) -> List[Dict]:
        from stock_oracle.breakout_detector import BreakoutDetector
        rows = BreakoutDetector().scan(tickers)
        for r in rows:
            comp = r.get("company") or {}
            r["company"] = {k: comp.get(k) for k in ("name", "sector", "industry", "short_desc", "market_cap")}
            r.pop("timeframe_weights", None)
        return plain(rows)

    async def breakouts(self, tickers: List[str]) -> List[Dict]:
        eq = tuple(sorted(t for t in tickers if S.is_equity(t)))
        return await self.cache.get(("brk", eq), 900, self._breakouts_sync, list(eq))

    # ── Claude ───────────────────────────────────────────────

    def _advisor(self):
        key = settings.anthropic_key()
        if not key:
            return None
        from stock_oracle.claude_advisor import ClaudeAdvisor, DEFAULT_MODEL, DEFAULT_MONTHLY_CAP
        model = settings.get("CLAUDE_MODEL", DEFAULT_MODEL) or DEFAULT_MODEL
        cap = settings.get_float("CLAUDE_MONTHLY_CAP", DEFAULT_MONTHLY_CAP)
        return ClaudeAdvisor(api_key=key, model=model, monthly_cap=cap)

    def advisor_status(self) -> Dict:
        adv = self._advisor()
        if not adv:
            return {"api_key_set": False}
        return adv.get_status()

    async def ask(self, question: str, focus: Optional[str], screen: Dict, watchlist: List[str],
                  background: str = "") -> Dict:
        adv = self._advisor()
        if not adv:
            raise ValueError("No Anthropic API key. Add ANTHROPIC_API_KEY in the GUI's Settings (or stock_oracle/.env).")
        ok, reason = adv.is_available()
        if not ok:
            raise ValueError(f"Claude advisor unavailable: {reason}")

        verdicts = {t: v for t, v in self.verdicts(watchlist).items() if v.get("prediction")}
        context = self._ask_context(focus, screen)
        if background:
            context += ("\n\nData pulled live for this question (quote it; say when something "
                        "isn't covered rather than guessing):\n" + background)

        text = await asyncio.to_thread(adv.ask_question, question, verdicts, context)
        if text is None:
            raise ValueError("Claude didn't answer (API error or the monthly cap would be exceeded). "
                             "See stock_oracle.log for details.")
        return {"answer": text, "status": adv.get_status()}

    def _ask_context(self, focus: Optional[str], screen: Dict) -> str:
        lines = [f"The user is in the Stock Oracle Terminal. Time: "
                 f"{datetime.now().strftime('%Y-%m-%d %H:%M')} local."]
        panels = screen.get("panels") or []
        if panels:
            lines.append("Panels on screen: " + ", ".join(f"{p.get('fn')} {p.get('sym') or ''}".strip()
                                                         for p in panels[:6]))
        if focus:
            q = screen.get("quote") or {}
            if q.get("p") is not None:
                lines.append(f"{focus} last {q.get('p')} ({(q.get('cp') or 0):+.2f}% today).")
            r = self.best(focus)
            if r:
                lines.append(
                    f"Oracle on {focus}: {r.get('prediction')} signal {r.get('signal', 0):+.3f}, "
                    f"confidence {r.get('confidence', 0):.0%}, threshold ±{r.get('conviction_threshold') or 0:.3f}, "
                    f"as of {r.get('timestamp', '')[:16]} ({r.get('source', 'terminal')}).")
                sigs = [s for s in r.get("signals", []) if s.get("confidence", 0) > 0.2]
                top = sorted(sigs, key=lambda s: abs(s.get("signal", 0)), reverse=True)[:8]
                if top:
                    lines.append("Top collector signals: " + "; ".join(
                        f"{s['collector']} {s['signal']:+.2f}" + (" (stale)" if s.get("stale") else "")
                        for s in top))
                if r.get("narrative"):
                    lines.append("Oracle narrative: " + r["narrative"][:800])
        extra = (screen.get("text") or "").strip()
        if extra:
            lines.append("Visible panel data:\n" + extra[:3000])
        return "\n".join(lines)
