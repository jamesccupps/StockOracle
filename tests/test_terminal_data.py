"""Terminal data normalisation."""
import pytest

from stock_oracle.terminal.providers.yahoo import _expense_ratio


@pytest.mark.parametrize("info,expected", [
    ({"netExpenseRatio": 0.0945}, 0.000945),                  # SPY, Yahoo reports percent
    ({"netExpenseRatio": 0.75}, 0.0075),                      # ARKK
    ({"annualReportExpenseRatio": 0.0009}, 0.0009),           # legacy field, already a fraction
    ({"netExpenseRatio": None, "annualReportExpenseRatio": 0.002}, 0.002),
    ({}, None),
    ({"netExpenseRatio": float("nan")}, None),
])
def test_expense_ratio_is_a_fraction(info, expected):
    got = _expense_ratio(info)
    assert got == pytest.approx(expected) if expected is not None else got is None


def test_repolled_quote_with_only_new_timestamp_is_not_a_change():
    from stock_oracle.terminal.quotes import _same
    a = {"symbol": "SPY", "price": 777.22, "prev_close": 779.09, "updated": 1000.0, "src": "yahoo"}
    b = dict(a, updated=1015.0)
    assert _same(a, b)
    assert not _same(a, dict(b, price=777.30))
    assert not _same(a, dict(b, ext_price=776.90))      # extended-hours moves still go out
