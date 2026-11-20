"""sec_edgar no longer scores the number of Form 4 filings."""
import json
from datetime import datetime, timedelta

import pytest

from stock_oracle.collectors import base
from stock_oracle.collectors import sec_edgar


def _submissions(n_form4):
    # A large company's quarter: many Form 4s (grants, exercises), periodic
    # reports filed midweek, nothing amended or delayed
    today = datetime.now()
    wednesday = today - timedelta(days=(today.weekday() - 2) % 7 or 7)
    forms, dates, descs = [], [], []
    for i in range(6):
        forms.append("8-K" if i % 2 else "10-Q")
        dates.append((wednesday - timedelta(weeks=i)).strftime("%Y-%m-%d"))
        descs.append("8-K" if i % 2 else "10-Q")
    for i in range(n_form4):
        forms.append("4")
        dates.append((today - timedelta(days=1 + i)).strftime("%Y-%m-%d"))
        descs.append("FORM 4")
    return {"filings": {"recent": {"form": forms, "filingDate": dates, "primaryDocDescription": descs}}}


class FakeResp:
    status_code = 200

    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


@pytest.fixture
def run(tmp_path, monkeypatch):
    monkeypatch.setattr(base, "CACHE_DIR", tmp_path)

    def go(n_form4):
        c = sec_edgar.SECEdgarCollector()
        monkeypatch.setattr(c, "_get_cik", lambda t: "0001234567")
        monkeypatch.setattr(c, "_request",
                            lambda url, params=None, headers=None: FakeResp(_submissions(n_form4)))
        return c.collect(f"EX{n_form4}")
    return go


def test_many_form4s_are_not_bearish(run):
    busy, quiet = run(40), run(1)
    # Same filings apart from the Form 4 count: same score
    assert busy.signal_value == pytest.approx(quiet.signal_value)
    assert busy.signal_value >= 0
    assert busy.raw_data["insider_activity"]["form4_count"] == 40
    assert "Form 4s=40" in busy.details
