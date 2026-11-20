"""insider_ratio counts open-market trades only, not grants, gifts or exercises."""
import sys
import types

import pandas as pd
import pytest

from stock_oracle.collectors import base
from stock_oracle.collectors import new_indicators as ni

NOW = pd.Timestamp("2026-11-20")
RECENT = pd.Timestamp.now() - pd.Timedelta(days=10)   # the collector uses today's date


def _txns(rows):
    # The columns and text yfinance 1.8 returns for insider_transactions
    return pd.DataFrame(
        [{"Shares": sh, "Value": val, "URL": "", "Text": text, "Insider": "A. Person",
          "Position": "Officer", "Transaction": "", "Start Date": pd.Timestamp(date),
          "Ownership": "D"} for text, sh, val, date in rows])


# A mega-cap's typical six months: grants and gifts, steady selling, no buys
ROUTINE = [
    ("Stock Award(Grant) at price 0.00 per share.", 40000, 0, "2026-09-04"),
    ("Stock Award(Grant) at price 0.00 per share.", 125, 0, "2026-09-04"),
    ("Stock Gift at price 0.00 per share.", 4704, 0, "2026-11-06"),
    ("Sale at price 531.00 per share.", 5000, 2655000, "2026-11-10"),
    ("Sale at price 544.24 - 544.95 per share.", 7305, 3976633, "2026-11-09"),
    ("Conversion of Exercise of derivative security at price 11.50 per share.", 9000, 0, "2026-10-01"),
    (None, 150000, float("nan"), "2026-09-21"),
    ("", 150000, float("nan"), "2026-09-21"),
]


def test_only_open_market_lines_count():
    out = ni.open_market_trades(_txns(ROUTINE), now=NOW)
    assert out["buys"] == 0
    assert out["sells"] == 2 and out["sell_shares"] == 12305
    assert out["sell_value"] == pytest.approx(2655000 + 3976633)
    assert out["skipped"] == 6
    assert out["net_shares"] == -12305


def test_old_rows_are_outside_the_window():
    rows = ROUTINE + [("Purchase at price 397.35 per share.", 1000, 397350, "2025-01-15")]
    assert ni.open_market_trades(_txns(rows), now=NOW)["buys"] == 0


def test_purchases_count():
    rows = ROUTINE + [("Purchase at price 397.35 per share.", 1000, 397350, "2026-10-15")]
    out = ni.open_market_trades(_txns(rows), now=NOW)
    assert out["buys"] == 1 and out["buy_shares"] == 1000 and out["buy_value"] == 397350


@pytest.fixture
def collector(tmp_path, monkeypatch):
    monkeypatch.setattr(base, "CACHE_DIR", tmp_path)
    c = ni.InsiderRatioCollector()
    monkeypatch.setattr(c, "_request", lambda *a, **k: None)     # no SEC search
    return c


def _fake_yfinance(monkeypatch, txns, summary=None):
    ticker = types.SimpleNamespace(insider_transactions=txns, insider_purchases=summary)
    monkeypatch.setitem(sys.modules, "yfinance",
                        types.SimpleNamespace(Ticker=lambda sym: ticker))


def test_grants_and_selling_are_not_read_as_buying(collector, monkeypatch):
    # Yahoo's own summary for the same six months calls the grants "Purchases"
    # and says insiders bought more than they sold; the collector used to
    # believe it and score this bullish
    summary = pd.DataFrame({
        "Insider Purchases Last 6m": ["Purchases", "Sales", "Net Shares Purchased (Sold)"],
        "Shares": [518348.0, 260692.0, 257656.0], "Trans": [17, 11, 28]})
    rows = [(t, s, v, RECENT) for t, s, v, _ in ROUTINE]
    _fake_yfinance(monkeypatch, _txns(rows), summary)
    r = collector.collect("EXMP")
    assert r.signal_value <= 0
    assert r.raw_data["buys"] == 0 and r.raw_data["sells"] == 2


def test_open_market_buying_is_bullish(collector, monkeypatch):
    rows = [("Purchase at price 20.00 per share.", 5000, 100000, RECENT)] * 5
    rows += [("Sale at price 21.00 per share.", 1000, 21000, RECENT)]
    _fake_yfinance(monkeypatch, _txns(rows))
    r = collector.collect("EXMP")
    assert r.signal_value > 0
    assert r.raw_data["buys"] == 5 and r.raw_data["sells"] == 1
