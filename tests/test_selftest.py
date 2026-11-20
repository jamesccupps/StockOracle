"""python -m stock_oracle.selftest reports each collector as LIVE, IDLE or ERROR."""
import logging

import pytest

from stock_oracle import oracle as oracle_mod
from stock_oracle import selftest
from stock_oracle.collectors import base
from stock_oracle.collectors.base import BaseCollector, SignalResult


class Fake(BaseCollector):
    def __init__(self, name, conf=0.0, fail=False):
        super().__init__()
        self._name, self.conf, self.fail = name, conf, fail
        self.tickers = []

    @property
    def name(self):
        return self._name

    def collect(self, ticker):
        self.tickers.append(ticker)
        if self.fail:
            raise RuntimeError("source changed its format")
        logging.getLogger("stock_oracle").warning(f"{self._name}: HTTP 400 from example.test")
        return SignalResult(self._name, ticker, 0.2, self.conf, details=f"{self._name} details")


@pytest.fixture
def fakes(monkeypatch, tmp_path):
    monkeypatch.setattr(base, "CACHE_DIR", tmp_path)          # restored afterwards
    monkeypatch.setattr(BaseCollector, "_failure_counts", {})
    monkeypatch.setattr(BaseCollector, "_failure_until", {})
    collectors = [Fake("live_one", conf=0.5), Fake("idle_one"), Fake("broken_one", fail=True)]

    class FakeOracle:
        def __init__(self, use_ml=True, parallel=True):
            self.collectors = collectors
    monkeypatch.setattr(oracle_mod, "StockOracle", FakeOracle)
    return collectors


def test_report(fakes, capsys):
    rc = selftest.main(["exmp", "exmq"])
    out = capsys.readouterr().out
    assert rc == 1                                             # a collector raised
    assert "LIVE   live_one" in out and "2/2" in out
    assert "IDLE   idle_one" in out
    assert "ERROR  broken_one" in out and "source changed its format" in out
    assert "1 live, 1 idle, 1 errors" in out
    assert "HTTP 400 from example.test" in out                 # warnings listed at the end
    assert fakes[0].tickers == ["EXMP", "EXMQ"]


def test_fresh_cache_by_default(fakes, tmp_path):
    selftest.main(["EXMP"])
    assert base.CACHE_DIR != tmp_path and "stock_oracle_selftest_" in str(base.CACHE_DIR)


def test_cached_keeps_the_app_cache(fakes, tmp_path):
    selftest.main(["EXMP", "--cached"])
    assert base.CACHE_DIR == tmp_path


def test_log_handler_removed(fakes):
    before = list(logging.getLogger().handlers)
    selftest.main(["EXMP"])
    assert logging.getLogger().handlers == before


def test_classify():
    r = lambda conf, details="": SignalResult("c", "T", 0.1, conf, details=details)  # noqa: E731
    assert selftest.classify([r(0), r(0.3)]) == "LIVE"
    assert selftest.classify([r(0), r(0)]) == "IDLE"
    assert selftest.classify([r(0.3), r(0, "Error: boom")]) == "ERROR"
