"""Market-wide inputs are fetched once for the watchlist, not once per ticker."""
import sys
import types
from collections import Counter

import pandas as pd
import pytest

from stock_oracle.collectors import base
from stock_oracle.collectors.creative_signals import EnergyCascadeCollector
from stock_oracle.collectors.new_indicators import (
    DividendVsTreasuryCollector, FearGreedProxyCollector, MarketPulseCollector)

TICKERS = ["AAA", "BBB", "CCC"]


@pytest.fixture
def history_calls(tmp_path, monkeypatch):
    monkeypatch.setattr(base, "CACHE_DIR", tmp_path)
    calls = Counter()

    class FakeTicker:
        def __init__(self, sym):
            self.sym = sym
            self.info = {"trailingAnnualDividendYield": 0.03}    # 3% for every stock

        def history(self, period="1mo", **kw):
            calls[self.sym] += 1
            closes = [100 + i * 0.5 for i in range(60)]
            if self.sym == "^TNX":
                closes = [4.5] * 5
            return pd.DataFrame({"Close": closes, "Volume": [1_000_000] * len(closes)})

    monkeypatch.setitem(sys.modules, "yfinance", types.SimpleNamespace(Ticker=FakeTicker))
    return calls


def test_each_market_series_is_fetched_once(history_calls):
    collectors = [FearGreedProxyCollector(), MarketPulseCollector(),
                  DividendVsTreasuryCollector(), EnergyCascadeCollector()]
    market = lambda: {s: n for s, n in history_calls.items() if s not in TICKERS}  # noqa: E731
    results = {TICKERS[0]: [c.collect(TICKERS[0]) for c in collectors]}
    first = market()
    assert first, "no market series were requested"
    results.update({t: [c.collect(t) for c in collectors] for t in TICKERS[1:]})
    # The other tickers reuse the first one's market data
    assert market() == first

    for t, rs in results.items():
        assert all(r.ticker == t for r in rs)
    # Market-wide scores are the same for every ticker
    for i in (0, 1):
        assert len({results[t][i].signal_value for t in TICKERS}) == 1
    # The dividend side is still per ticker: 3% against a 4.5% 10-year
    div = results["BBB"][2]
    assert div.raw_data["treasury_10y"] == 4.5 and div.raw_data["dividend_yield"] == 3.0
