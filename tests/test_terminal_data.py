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
