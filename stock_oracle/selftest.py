"""
Collector self-test
===================
Runs every Oracle collector on a few tickers with live data and reports
which ones produce a signal on this machine with these keys.

    python -m stock_oracle.selftest                  # AAPL, MSFT, SPY
    python -m stock_oracle.selftest NVDA LUNR SCHD   # your own tickers
    python -m stock_oracle.selftest --cached         # allow cached results

LIVE   gave a signal (confidence above 0) for at least one ticker
IDLE   ran but gave nothing: no key, no data for these tickers, or a source
       that has stopped answering (the details and warnings say which)
ERROR  raised an exception (the collector's own failure, not a data gap)

By default it uses an empty, throwaway cache so every source is really
called; the app's own cache is not read or changed, and nothing is written
to the app's log. Exit code 1 if any collector raised.
"""
import argparse
import logging
import sys
import tempfile
import time
from collections import Counter
from pathlib import Path


class _Collect(logging.Handler):
    """Counts warnings and errors so they can be listed once at the end."""

    def __init__(self):
        super().__init__(logging.WARNING)
        self.seen = Counter()

    def emit(self, record):
        self.seen[record.getMessage()[:160]] += 1


def classify(results) -> str:
    """LIVE / IDLE / ERROR for one collector's SignalResults."""
    if any((r.details or "").startswith("Error:") for r in results):
        return "ERROR"
    return "LIVE" if any(r.confidence > 0 for r in results) else "IDLE"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python -m stock_oracle.selftest")
    ap.add_argument("tickers", nargs="*", default=["AAPL", "MSFT", "SPY"])
    ap.add_argument("--cached", action="store_true",
                    help="use the app's cache (faster; may show results up to 4 hours old)")
    args = ap.parse_args(argv)
    tickers = [t.upper() for t in args.tickers]
    if hasattr(sys.stdout, "reconfigure"):
        # A headline with a character the console's code page lacks
        # shouldn't end the run
        sys.stdout.reconfigure(errors="replace")

    # Before anything imports oracle.py: its logging.basicConfig then does
    # nothing, so nothing goes to the console mid-table or to the app's log
    # file. Warnings are counted and listed at the end instead.
    collected = _Collect()
    logging.basicConfig(level=logging.WARNING, handlers=[collected])
    root = logging.getLogger()
    if collected not in root.handlers:
        root.addHandler(collected)
    try:
        return _run(args, tickers, collected)
    finally:
        root.removeHandler(collected)


def _run(args, tickers, collected) -> int:
    from stock_oracle.collectors import base
    if not args.cached:
        base.CACHE_DIR = Path(tempfile.mkdtemp(prefix="stock_oracle_selftest_"))

    from stock_oracle import config as cfg
    from stock_oracle.oracle import StockOracle
    oracle = StockOracle(use_ml=False, parallel=False)   # the app's collector list

    source = "app cache" if args.cached else "live data, fresh cache"
    print(f"\n  Stock Oracle collector self-test ({', '.join(tickers)}), {source}\n")
    counts = Counter()
    for c in oracle.collectors:
        results, secs = [], 0.0
        for t in tickers:
            t0 = time.time()
            results.append(c._safe_collect(t))
            secs += time.time() - t0
        status = classify(results)
        counts[status] += 1
        active = sum(1 for r in results if r.confidence > 0)
        if status == "ERROR":
            shown = next(r for r in results if (r.details or "").startswith("Error:"))
        else:
            shown = next((r for r in results if r.confidence > 0), results[0])
        print(f"  {status:5}  {c.name:22} {active}/{len(tickers)} {secs:6.1f}s  "
              f"{shown.ticker}: {(shown.details or '')[:80]}")

    sec_contact = "@" in cfg.SEC_USER_AGENT and "your@email.com" not in cfg.SEC_USER_AGENT
    keys = {"finnhub": cfg.FINNHUB_API_KEY, "fred": cfg.FRED_API_KEY, "github": cfg.GITHUB_TOKEN,
            "reddit": cfg.REDDIT_CLIENT_ID and cfg.REDDIT_CLIENT_SECRET, "SEC contact": sec_contact}
    print(f"\n  {counts['LIVE']} live, {counts['IDLE']} idle, {counts['ERROR']} errors.  Keys: "
          + ", ".join(f"{k} {'yes' if v else 'no'}" for k, v in keys.items()))
    if collected.seen:
        print(f"\n  Warnings and errors logged ({sum(collected.seen.values())}):")
        for msg, n in collected.seen.most_common(15):
            print(f"  {n:4}x  {msg}")
    print()
    return 1 if counts["ERROR"] else 0


if __name__ == "__main__":
    sys.exit(main())
