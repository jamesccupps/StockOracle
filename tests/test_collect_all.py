"""StockOracle._collect_all: a stuck collector can't hold up a scan."""
import threading
import time

from stock_oracle.collectors.base import SignalResult
from stock_oracle.oracle import StockOracle


class FakeCollector:
    def __init__(self, name, delay=0.0, release=None):
        self.name, self.delay, self.release = name, delay, release
        self.calls = 0

    def _safe_collect(self, ticker):
        self.calls += 1
        if self.release is not None:
            self.release.wait(10)          # "stuck" until the test lets it go
        time.sleep(self.delay)
        return SignalResult(self.name, ticker, 0.1, 0.5)


def _oracle(collectors, parallel=True, skip_slow=False):
    o = StockOracle.__new__(StockOracle)    # no network, models or files
    o.collectors, o.parallel, o.skip_slow = collectors, parallel, skip_slow
    o.SLOW_COLLECTORS = {"employee_sentiment", "earnings_nlp"}
    return o


def test_stuck_collector_is_skipped_after_the_deadline():
    release = threading.Event()
    o = _oracle([FakeCollector("fast"), FakeCollector("stuck", release=release)])
    o.COLLECT_TIMEOUT = 0.5
    t0 = time.time()
    try:
        signals = o._collect_all("EXMP")
        elapsed = time.time() - t0
    finally:
        release.set()
    assert elapsed < 3
    assert [s.collector_name for s in signals] == ["fast"]


def test_all_collectors_answer_within_the_deadline():
    o = _oracle([FakeCollector(f"c{i}", delay=0.05) for i in range(12)])
    names = sorted(s.collector_name for s in o._collect_all("EXMP"))
    assert names == sorted(f"c{i}" for i in range(12))


def test_sequential_mode_honors_skip_slow():
    slow = FakeCollector("employee_sentiment")
    o = _oracle([FakeCollector("fast"), slow], parallel=False, skip_slow=True)
    assert [s.collector_name for s in o._collect_all("EXMP")] == ["fast"]
    assert slow.calls == 0
