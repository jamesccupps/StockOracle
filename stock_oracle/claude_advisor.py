"""
Claude Advisor — AI Meta-Layer for Stock Oracle
=================================================
Periodically sends signal data to Claude API for deeper analysis.
Claude can spot patterns, bias shifts, and collector issues that
the local ML misses.

Modes:
  - Hourly check-in: compact signal summary → weight tweaks + alerts
  - End-of-session: full verified data → accuracy report + config changes
  - On-demand: "Ask Claude" button with custom question + context

Cost safeguards:
  - Hard monthly spending cap (default $10)
  - Per-call token limits
  - Usage tracking with persistent log
  - Auto-disables when approaching limit
  - Haiku by default (cheap)
  - Spend is reserved before each call and shared across processes

Requires: pip install anthropic
API key: console.anthropic.com
"""
import json
import logging
import os
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import stock_oracle.config as cfg

logger = logging.getLogger("stock_oracle")

# ── Cost tracking ──────────────────────────────────────────────
# First-party API prices per 1M tokens, as of 2026-10-06.
MODEL_COSTS = {
    "claude-fable-5-1":          {"input": 10.00, "output": 50.00},
    "claude-fable-5":            {"input": 10.00, "output": 50.00},
    "claude-opus-5-5":           {"input": 4.00,  "output": 20.00},
    "claude-opus-5":             {"input": 5.00,  "output": 25.00},
    "claude-opus-4-8":           {"input": 5.00,  "output": 25.00},
    "claude-sonnet-5-5":         {"input": 2.00,  "output": 10.00},
    "claude-sonnet-5":           {"input": 2.00,  "output": 10.00},
    "claude-sonnet-4-6":         {"input": 3.00,  "output": 15.00},
    "claude-haiku-5-5":          {"input": 0.10,  "output": 0.50},   # prompts <=100K (we cap at 12K)
    "claude-haiku-4-5":          {"input": 1.00,  "output": 5.00},
    "claude-haiku-4-5-20251001": {"input": 1.00,  "output": 5.00},
    "claude-sonnet-4-20250514":  {"input": 3.00,  "output": 15.00},  # deprecated
}
DEFAULT_MODEL = "claude-haiku-4-5-20251001"
# Unknown model: assume the most expensive tier so the cap can only undershoot
FALLBACK_INPUT_COST = 10.00
FALLBACK_OUTPUT_COST = 50.00

# Safety limits
DEFAULT_MONTHLY_CAP = 10.00        # Hard cap in dollars
WARNING_THRESHOLD_PCT = 0.80       # Warn at 80% of cap
PER_CALL_MAX_INPUT_TOKENS = 12000  # Won't send more than this
PER_CALL_MAX_OUTPUT_TOKENS = 4000  # Won't request more than this
MIN_BALANCE_FOR_CALL = 0.50        # Stop if less than $0.50 remaining
RESERVATION_TTL = 900              # seconds; drops holds left by a crashed process

# Paths: config.DATA_DIR, not a cwd-relative path, so the GUI, the terminal
# and the frozen build all charge the same budget.
DATA_DIR = Path(cfg.DATA_DIR)
USAGE_FILE = DATA_DIR / "claude_usage.json"
LOCK_FILE = DATA_DIR / "claude_usage.lock"

_thread_lock = threading.RLock()


class UsageFileError(RuntimeError):
    """claude_usage.json exists but can't be parsed; spending is refused."""


@contextmanager
def _usage_lock(timeout: float = 10.0):
    """Exclusive lock across threads and processes (GUI + terminal) for one read-modify-write."""
    with _thread_lock:
        LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(LOCK_FILE, "a+b") as fh:
            if os.name == "nt":
                import msvcrt
                deadline = time.monotonic() + timeout
                while True:
                    try:
                        fh.seek(0)
                        msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
                        break
                    except OSError:
                        if time.monotonic() > deadline:
                            raise TimeoutError(f"{LOCK_FILE} held for more than {timeout}s")
                        time.sleep(0.02)
                try:
                    yield
                finally:
                    fh.seek(0)
                    msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(fh, fcntl.LOCK_EX)
                try:
                    yield
                finally:
                    fcntl.flock(fh, fcntl.LOCK_UN)


def _price(model: str) -> Tuple[float, float]:
    c = MODEL_COSTS.get(model, {})
    return c.get("input", FALLBACK_INPUT_COST), c.get("output", FALLBACK_OUTPUT_COST)


def estimate_cost(input_tokens: int, output_tokens: int, model: str) -> float:
    pin, pout = _price(model)
    return input_tokens / 1_000_000 * pin + output_tokens / 1_000_000 * pout


class SpendingTracker:
    """
    Monthly spend cap shared by every advisor instance, thread and process.

    The file is the source of truth and is re-read under an exclusive lock for
    every check and every update, so concurrent callers can't overwrite each
    other's spend. A call first reserves its worst-case cost, which makes
    concurrent calls see each other before any of them is billed. It then
    settles to the actual cost, or releases the hold if it fails.
    """

    def __init__(self, monthly_cap: float = DEFAULT_MONTHLY_CAP):
        self.monthly_cap = monthly_cap
        self.error = ""
        self.current_month = datetime.now().strftime("%Y-%m")
        self.total_spent = 0.0
        self.reserved = 0.0
        self.total_calls = 0
        self.total_input_tokens = 0
        self.total_output_tokens = 0
        self.call_log: List[Dict] = []
        try:
            with _usage_lock():
                self._sync(self._read())
        except (UsageFileError, OSError) as e:
            self.error = str(e)
            logger.error(f"Claude advisor: {e}")

    # ── file I/O (caller holds the lock) ───────────────────────

    def _read(self) -> Dict:
        month = datetime.now().strftime("%Y-%m")
        fresh = {"month": month, "total_spent": 0.0, "total_calls": 0,
                 "total_input_tokens": 0, "total_output_tokens": 0,
                 "call_log": [], "reservations": {}}
        if not USAGE_FILE.exists():
            return fresh
        try:
            data = json.loads(USAGE_FILE.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                raise ValueError("top level is not an object")
            float(data.get("total_spent", 0.0))
        except (OSError, ValueError, TypeError) as e:
            # Fail closed: resetting to $0 here would silently void the cap
            raise UsageFileError(
                f"{USAGE_FILE} is unreadable ({e}). Claude calls are blocked until "
                f"it is fixed or deleted.") from None
        if data.get("month") != month:
            logger.info(f"Claude advisor: new month ({month}), resetting usage")
            return fresh
        for k, v in fresh.items():
            data.setdefault(k, v)
        now = time.time()
        data["reservations"] = {rid: r for rid, r in (data.get("reservations") or {}).items()
                                if now - float(r.get("at", 0)) < RESERVATION_TTL}
        return data

    def _write(self, data: Dict):
        data["call_log"] = data["call_log"][-100:]
        data["monthly_cap"] = self.monthly_cap
        data["total_spent"] = round(data["total_spent"], 6)
        tmp = USAGE_FILE.with_name(f"{USAGE_FILE.stem}.{os.getpid()}.tmp")
        tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
        os.replace(tmp, USAGE_FILE)

    def _sync(self, data: Dict):
        self.current_month = data["month"]
        self.total_spent = float(data["total_spent"])
        self.reserved = sum(float(r.get("amount", 0)) for r in data["reservations"].values())
        self.total_calls = data["total_calls"]
        self.total_input_tokens = data["total_input_tokens"]
        self.total_output_tokens = data["total_output_tokens"]
        self.call_log = data["call_log"]
        self.error = ""

    def _check(self, data: Dict, estimated_cost: float) -> Tuple[bool, str]:
        held = sum(float(r.get("amount", 0)) for r in data["reservations"].values())
        remaining = self.monthly_cap - data["total_spent"] - held
        if remaining < MIN_BALANCE_FOR_CALL:
            return False, (f"Monthly spending cap reached. Spent ${data['total_spent']:.2f} "
                           f"of ${self.monthly_cap:.2f} limit"
                           + (" (plus calls in flight)" if held else "")
                           + ". Resets next month.")
        if estimated_cost > remaining:
            return False, (f"This call would cost up to ${estimated_cost:.4f} but only "
                           f"${remaining:.2f} remains in the monthly budget.")
        return True, f"OK (est ${estimated_cost:.4f}, ${remaining:.2f} remaining)"

    # ── public API ─────────────────────────────────────────────

    def can_afford(self, estimated_input_tokens: int = 4000,
                   estimated_output_tokens: int = 1000,
                   model: str = DEFAULT_MODEL) -> Tuple[bool, str]:
        """Read-only check. Use reserve() before an actual call."""
        try:
            with _usage_lock():
                data = self._read()
                self._sync(data)
                return self._check(data, estimate_cost(estimated_input_tokens,
                                                       estimated_output_tokens, model))
        except (UsageFileError, OSError) as e:
            self.error = str(e)
            return False, str(e)

    def reserve(self, estimated_input_tokens: int, estimated_output_tokens: int,
                model: str) -> Tuple[Optional[str], str]:
        """Hold the worst-case cost of a call. Returns (reservation_id or None, reason)."""
        est = estimate_cost(estimated_input_tokens, estimated_output_tokens, model)
        try:
            with _usage_lock():
                data = self._read()
                ok, reason = self._check(data, est)
                if not ok:
                    self._sync(data)
                    return None, reason
                rid = uuid.uuid4().hex
                data["reservations"][rid] = {"amount": round(est, 6), "at": time.time(),
                                             "pid": os.getpid()}
                self._write(data)
                self._sync(data)
                return rid, reason
        except (UsageFileError, OSError) as e:
            self.error = str(e)
            return None, str(e)

    def release(self, reservation_id: Optional[str]):
        """Drop a hold for a call that never completed."""
        if not reservation_id:
            return
        try:
            with _usage_lock():
                data = self._read()
                if data["reservations"].pop(reservation_id, None) is not None:
                    self._write(data)
                self._sync(data)
        except (UsageFileError, OSError) as e:
            logger.error(f"Claude advisor: could not release reservation: {e}")

    def record_call(self, input_tokens: int, output_tokens: int,
                    model: str, purpose: str, reservation_id: Optional[str] = None) -> float:
        """Charge a completed call (and drop its reservation). Returns its cost."""
        cost = estimate_cost(input_tokens, output_tokens, model)
        try:
            with _usage_lock():
                data = self._read()
                data["reservations"].pop(reservation_id, None)
                data["total_spent"] += cost
                data["total_calls"] += 1
                data["total_input_tokens"] += input_tokens
                data["total_output_tokens"] += output_tokens
                data["call_log"].append({
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "model": model,
                    "purpose": purpose,
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                    "cost": round(cost, 6),
                })
                self._write(data)
                self._sync(data)
        except (UsageFileError, OSError) as e:
            # The call was billed; make the lost record loud
            self.error = str(e)
            logger.error(f"Claude advisor: ${cost:.4f} call NOT recorded against the cap: {e}")
            return cost

        pct = self.total_spent / self.monthly_cap if self.monthly_cap > 0 else 1.0
        if pct >= WARNING_THRESHOLD_PCT:
            logger.warning(
                f"Claude advisor: ${self.total_spent:.2f} of "
                f"${self.monthly_cap:.2f} monthly cap used ({pct:.0%})"
            )
        return cost

    def get_status(self) -> Dict:
        """Current usage for display (as of the last read)."""
        remaining = self.monthly_cap - self.total_spent - self.reserved
        pct = self.total_spent / self.monthly_cap if self.monthly_cap > 0 else 0
        status = {
            "month": self.current_month,
            "spent": round(self.total_spent, 4),
            "cap": self.monthly_cap,
            "remaining": round(remaining, 4),
            "pct_used": round(pct * 100, 1),
            "calls": self.total_calls,
            "input_tokens": self.total_input_tokens,
            "output_tokens": self.total_output_tokens,
            "enabled": remaining >= MIN_BALANCE_FOR_CALL and not self.error,
        }
        if self.error:
            status["error"] = self.error
        return status


class ClaudeAdvisor:
    """
    Main advisor class. Sends data to Claude, gets analysis back.
    """

    def __init__(self, api_key: str = None, model: str = DEFAULT_MODEL,
                 monthly_cap: float = DEFAULT_MONTHLY_CAP):
        self.api_key = api_key
        self.model = model
        self.tracker = SpendingTracker(monthly_cap)
        self._client = None

    def _get_client(self):
        """Lazy-init the Anthropic client."""
        if self._client is None:
            if not self.api_key:
                raise ValueError("No Anthropic API key configured. Add it in Settings.")
            try:
                import anthropic
                self._client = anthropic.Anthropic(api_key=self.api_key)
            except ImportError:
                raise ImportError(
                    "anthropic package not installed. "
                    "Run: pip install anthropic"
                )
        return self._client

    def is_available(self) -> Tuple[bool, str]:
        """Check if the advisor is configured and has budget."""
        if not self.api_key:
            return False, "No API key configured"

        ok, reason = self.tracker.can_afford()
        if not ok:
            return False, reason

        return True, f"Ready (${self.tracker.get_status()['remaining']:.2f} remaining)"

    def _call_api(self, system_prompt: str, user_message: str,
                  purpose: str, max_output: int = None) -> Optional[str]:
        """
        Make a guarded API call.
        Returns the response text, or None if blocked/failed.
        """
        max_output = max_output or PER_CALL_MAX_OUTPUT_TOKENS

        # Estimate input tokens (~4 chars per token)
        est_input = len(system_prompt + user_message) // 4
        est_input = min(est_input, PER_CALL_MAX_INPUT_TOKENS)

        # Hold the worst-case cost before calling, so concurrent calls can't
        # all pass the check and overshoot the cap together
        rid, reason = self.tracker.reserve(est_input, max_output, self.model)
        if not rid:
            logger.warning(f"Claude advisor blocked: {reason}")
            return None

        try:
            client = self._get_client()

            # Truncate input if too long
            if len(user_message) > PER_CALL_MAX_INPUT_TOKENS * 4:
                user_message = user_message[:PER_CALL_MAX_INPUT_TOKENS * 4]
                user_message += "\n\n[Message truncated for cost control]"

            response = client.messages.create(
                model=self.model,
                max_tokens=max_output,
                system=system_prompt,
                messages=[{"role": "user", "content": user_message}],
            )

            # Record actual usage
            input_tokens = response.usage.input_tokens
            output_tokens = response.usage.output_tokens
            cost = self.tracker.record_call(
                input_tokens, output_tokens, self.model, purpose, reservation_id=rid
            )
            rid = None

            # Current models think by default, so content can open with a
            # thinking block; join the text blocks instead of taking [0]
            text = "".join(getattr(b, "text", "") for b in (response.content or [])
                           if getattr(b, "type", "") == "text")
            logger.info(
                f"Claude advisor ({purpose}): {input_tokens}+{output_tokens} tokens, "
                f"${cost:.4f}"
            )
            return text

        except Exception as e:
            logger.error(f"Claude advisor error: {e}")
            return None
        finally:
            self.tracker.release(rid)

    # ── Advisor Modes ──────────────────────────────────────────

    def hourly_checkin(self, results: Dict, session_stats: Dict,
                       recent_verified: List[Dict] = None) -> Optional[Dict]:
        """
        Hourly check-in during monitoring.
        Sends signal summary + recent accuracy, gets weight tweaks + detailed analysis back.
        """
        # Build current weight context for Claude
        import stock_oracle.config as cfg
        weight_str = ", ".join(f"{k}:{v:.3f}" for k, v in sorted(cfg.SIGNAL_WEIGHTS.items()) if v > 0)

        system = (
            "You are a quantitative trading advisor analyzing real-time stock prediction data. "
            "You are part of Stock Oracle, a multi-collector signal aggregation system that monitors "
            "individual stocks and ETFs with ~40 seconds between scans and verifies intraday predictions "
            "3 scans back (~2 minutes). "
            "The system now has SIGNAL INTELLIGENCE: stale signals (collectors returning constant values) "
            "are suppressed, conviction thresholds adapt to each ticker's volatility (high-vol stocks "
            "like LUNR need stronger signals), and signals are detrended (change from baseline matters "
            "more than absolute level). During after-hours and pre-market, thresholds are raised 50% "
            "and confidence is penalized 40% because prices are stale and volume is thin. "
            "Each ticker line shows: thresh=adaptive threshold, "
            "vol=recent volatility, dyn=dynamic signal count, stale=stale signal count. "
            "Respond with a JSON object containing: "
            '{"weight_adjustments": {"collector_name": new_weight, ...}, '
            '"alerts": ["string", ...], '
            '"analysis": "detailed multi-paragraph analysis of current market behavior, '
            'collector performance, pattern observations, and what to watch for. '
            'Be specific — name tickers, collectors, and signal values. '
            'Discuss which conviction calls are working vs failing and why. '
            'Comment on the intelligence system — are thresholds appropriate? '
            'Are the right signals being suppressed as stale? '
            'Identify any emerging trends, sector rotations, or anomalies. '
            'This is the primary advisory output shown to the trader so make it thorough and actionable."} '
            "RULES FOR WEIGHT ADJUSTMENTS: "
            "1. Each adjustment is CLAMPED to ±0.03 from current weight per check-in. "
            "   Example: if momentum_quality is currently 0.07, you can suggest 0.04-0.10. "
            "2. Valid range: 0.01 to 0.15. "
            "3. Collectors at weight 0 are DEAD (broken APIs) and cannot be changed. "
            "4. Only suggest changes if you see clear evidence of bias or failure. "
            "5. Suggest the DESIRED weight, not a delta. The system handles clamping. "
            f"CURRENT WEIGHTS: {weight_str}"
        )

        # Build compact signal summary — include all tickers
        ticker_summaries = []
        for ticker, result in list(results.items())[:27]:  # All watchlist tickers
            sig = result.get("signal", 0)
            pred = result.get("prediction", "?")
            conf = result.get("confidence", 0)
            price = result.get("price", 0)
            ct = result.get("conviction_threshold", 0.06)
            dyn = result.get("dynamic_signals", 0)
            stale = result.get("stale_signals", 0)
            vol = result.get("volatility", 0)
            top_signals = sorted(
                [s for s in result.get("signals", []) if s.get("confidence", 0) > 0.2],
                key=lambda s: abs(s.get("signal", 0)), reverse=True
            )[:5]
            sig_str = ", ".join(
                f"{s['collector']}:{s['signal']:+.2f}"
                for s in top_signals
            )
            ticker_summaries.append(
                f"{ticker}: {pred} ({sig:+.3f}, conf={conf:.2f}, ${price:.2f}, "
                f"thresh=±{ct:.3f}, vol={vol*100:.2f}%, dyn={dyn}/stale={stale}) [{sig_str}]"
            )

        # Recent accuracy breakdown if available
        accuracy_str = ""
        if recent_verified:
            correct = sum(1 for v in recent_verified if v.get("directional_correct"))
            total = len(recent_verified)
            accuracy_str = f"\nRecent accuracy (last {total}): {correct}/{total} = {correct/total*100:.0f}% directional"

            # Per-prediction breakdown
            from collections import Counter
            pred_counts = Counter()
            pred_correct = Counter()
            for v in recent_verified:
                p = v.get("prediction", "?")
                pred_counts[p] += 1
                if v.get("directional_correct"):
                    pred_correct[p] += 1
            for p in ["BULLISH", "BEARISH", "NEUTRAL"]:
                if pred_counts[p]:
                    accuracy_str += (
                        f"\n  {p}: {pred_correct[p]}/{pred_counts[p]} = "
                        f"{pred_correct[p]/pred_counts[p]*100:.0f}%"
                    )

            # Per-collector accuracy from recent verified
            collector_hits = {}
            collector_total = {}
            for v in recent_verified:
                actual_dir = 1 if v.get("pct_change", 0) > 0.001 else (-1 if v.get("pct_change", 0) < -0.001 else 0)
                for s in v.get("signals", []):
                    name = s.get("collector", "")
                    sig_val = s.get("signal", 0)
                    collector_total[name] = collector_total.get(name, 0) + 1
                    if (sig_val > 0.02 and actual_dir > 0) or (sig_val < -0.02 and actual_dir < 0):
                        collector_hits[name] = collector_hits.get(name, 0) + 1
            if collector_total:
                accuracy_str += "\n\nCollector directional accuracy (recent):"
                sorted_colls = sorted(collector_total.items(),
                    key=lambda x: collector_hits.get(x[0], 0) / max(x[1], 1), reverse=True)
                for name, total_c in sorted_colls[:10]:
                    hits = collector_hits.get(name, 0)
                    accuracy_str += f"\n  {name}: {hits}/{total_c} = {hits/total_c*100:.0f}%"

        # Detect current market session and regime
        market_session_str = ""
        try:
            from stock_oracle.collectors.finnhub_collector import get_market_session
            ms = get_market_session()
            market_session_str = (
                f"\nMarket session: {ms.get('session', '?')} — {ms.get('detail', '')}"
            )
        except Exception:
            pass

        regime_str = ""
        try:
            from stock_oracle.market_regime import MarketRegimeDetector
            detector = MarketRegimeDetector()
            regime = detector.detect()
            regime_str = (
                f"\nMarket regime: {regime.get('regime', '?')} "
                f"(bias={regime.get('bias', 0):+.4f}, SPY 5d={regime.get('spy_5d', 0):+.1%}, "
                f"breadth={regime.get('breadth_5d', 0):.0%}) — {regime.get('detail', '')}"
            )
        except Exception:
            pass

        user_msg = (
            f"Session stats: {json.dumps(session_stats)}\n"
            f"{market_session_str}"
            f"{regime_str}"
            f"{accuracy_str}\n\n"
            f"Current predictions:\n" +
            "\n".join(ticker_summaries)
        )

        response = self._call_api(system, user_msg, "hourly_checkin", max_output=2000)
        if not response:
            return None

        # Parse JSON response
        try:
            # Strip markdown fences if present
            clean = response.strip()
            if clean.startswith("```"):
                clean = clean.split("\n", 1)[1] if "\n" in clean else clean[3:]
            if clean.endswith("```"):
                clean = clean[:-3]
            clean = clean.strip()
            if clean.startswith("json"):
                clean = clean[4:].strip()
            return json.loads(clean)
        except Exception:
            # If Claude didn't return clean JSON, wrap the text
            return {"notes": response, "weight_adjustments": {}, "alerts": []}

    def end_of_session(self, verified_data: List[Dict],
                       session_stats: Dict) -> Optional[Dict]:
        """
        Deep end-of-session analysis.
        Sends full verified data, gets comprehensive report.
        """
        system = (
            "You are a quantitative trading advisor reviewing a completed monitoring session "
            "for Stock Oracle, a multi-collector signal aggregation system. "
            "Provide a thorough, actionable post-session report. "
            "Respond with a JSON object containing: "
            '{"accuracy_report": {"collector_name": {"accuracy": pct, "calls": n, "bias": "bull/bear/neutral"}, ...}, '
            '"weight_recommendations": {"collector_name": new_weight, ...}, '
            '"pattern_notes": ["string", ...], '
            '"overall_grade": "A/B/C/D/F", '
            '"summary": "Multi-paragraph detailed analysis. Cover: '
            '(1) Overall session performance with specific numbers. '
            '(2) Which tickers the system predicted well vs poorly and why. '
            '(3) Which collectors contributed useful signal vs added noise. '
            '(4) Any bullish/bearish bias issues — were conviction calls justified? '
            '(5) Specific actionable recommendations for the next session. '
            '(6) Market regime observations (trending, choppy, flat) and how the system handled it. '
            'Be specific — name tickers, percentages, and signal values. '
            'This is the primary session debrief the trader reads."}'
        )

        # Compact verified data — more records, include per-record signals
        compact = []
        for v in verified_data[-200:]:  # Cap at 200 most recent
            entry = {
                "ticker": v["ticker"],
                "pred": v["prediction"],
                "actual": v.get("actual_outcome", "?"),
                "pct": round(v.get("pct_change", 0) * 100, 3),
                "correct": v.get("directional_correct", False),
                "sig": round(v.get("signal", 0), 4),
                "conf": round(v.get("confidence", 0), 3),
            }
            # Include top 3 signals if available
            signals = v.get("signals", [])
            if signals:
                top3 = sorted(signals, key=lambda s: abs(s.get("signal", 0)), reverse=True)[:3]
                entry["top_sigs"] = [
                    {"c": s["collector"][:15], "s": round(s.get("signal", 0), 3)}
                    for s in top3
                ]
            compact.append(entry)

        # Add aggregate stats
        from collections import Counter
        pred_dist = Counter(v["prediction"] for v in verified_data)
        actual_dist = Counter(v.get("actual_outcome", "?") for v in verified_data)

        user_msg = (
            f"Session stats: {json.dumps(session_stats)}\n"
            f"Total verified: {len(verified_data)}\n"
            f"Prediction distribution: {dict(pred_dist)}\n"
            f"Actual outcome distribution: {dict(actual_dist)}\n\n"
            f"Verified predictions (last {len(compact)} records):\n"
            f"{json.dumps(compact, separators=(',', ':'))}"
        )

        return self._call_api(system, user_msg, "end_of_session", max_output=3000)

    def ask_question(self, question: str, results: Dict = None,
                     context: str = "") -> Optional[str]:
        """
        On-demand question with optional current data context.
        """
        system = (
            "You are a quantitative trading advisor for Stock Oracle, "
            "a 38-collector stock prediction system. Answer concisely and practically. "
            "If the question involves specific tickers, reference the provided signal data."
        )

        # Add current data context if available
        data_context = ""
        if results:
            for ticker, result in list(results.items())[:27]:
                sig = result.get("signal", 0)
                pred = result.get("prediction", "?")
                conf = result.get("confidence", 0)
                price = result.get("price", 0)
                data_context += f"  {ticker}: {pred} ({sig:+.3f}, conf={conf:.2f}, ${price:.2f})\n"

        user_msg = question
        if data_context:
            user_msg = f"Current predictions:\n{data_context}\n\nQuestion: {question}"
        if context:
            user_msg = f"{context}\n\n{user_msg}"

        return self._call_api(system, user_msg, "ask_question", max_output=2000)

    def get_status(self) -> Dict:
        """Get advisor status for display."""
        status = self.tracker.get_status()
        status["api_key_set"] = bool(self.api_key)
        status["model"] = self.model
        return status


def load_advisor_from_settings() -> Optional[ClaudeAdvisor]:
    """
    Load advisor using settings from the GUI settings file.
    Returns None if not configured.
    """
    settings_file = DATA_DIR / "settings.json"
    if not settings_file.exists():
        return None

    try:
        with open(settings_file) as f:
            settings = json.load(f)

        api_key = settings.get("anthropic_api_key", "")
        if not api_key:
            return None

        model = settings.get("claude_model", DEFAULT_MODEL)
        cap = settings.get("claude_monthly_cap", DEFAULT_MONTHLY_CAP)

        return ClaudeAdvisor(api_key=api_key, model=model, monthly_cap=cap)

    except Exception:
        return None
