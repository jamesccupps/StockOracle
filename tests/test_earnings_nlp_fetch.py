"""earnings_nlp finds the company's own earnings release through EDGAR's JSON endpoints."""
import json
from datetime import datetime, timedelta

import pytest

from stock_oracle.collectors import alt_data
from stock_oracle.collectors import base

TODAY = datetime.now()
D = lambda days: (TODAY - timedelta(days=days)).strftime("%Y-%m-%d")    # noqa: E731

RELEASE = ("<html><body><p>Example Corp reports third quarter results. Revenue grew and "
           "demand was strong; margins expanded.</p>" + "<p>Detail.</p>" * 200 + "</body></html>")

INDEX = """<table class="tableFile">
<tr><td scope="row">1</td><td scope="row">8-K</td>
<td scope="row"><a href="/ix?doc=/Archives/edgar/data/1234567/000123456726000088/exmp-8k.htm">exmp-8k.htm</a></td>
<td scope="row">8-K</td></tr>
<tr><td scope="row">2</td><td scope="row">EX-99.1</td>
<td scope="row"><a href="/Archives/edgar/data/1234567/000123456726000088/q3pr.htm">q3pr.htm</a></td>
<td scope="row">EX-99.1</td></tr>
</table>"""


class FakeResp:
    status_code = 200

    def __init__(self, payload):
        self._payload = payload
        self.text = payload if isinstance(payload, str) else json.dumps(payload)

    def json(self):
        return self._payload if not isinstance(self._payload, str) else json.loads(self._payload)


@pytest.fixture
def collector(tmp_path, monkeypatch):
    monkeypatch.setattr(base, "CACHE_DIR", tmp_path)
    monkeypatch.setattr(alt_data.EarningsCallNLPCollector, "_cik_map", {}, raising=False)
    c = alt_data.EarningsCallNLPCollector()
    c.requested = []
    submissions = {"filings": {"recent": {
        # Newest first, as EDGAR lists them
        "form":            ["8-K", "4", "8-K", "8-K"],
        "filingDate":      [D(3), D(5), D(20), D(200)],
        "accessionNumber": ["0001234567-26-000099", "0001234567-26-000098",
                            "0001234567-26-000088", "0001234567-26-000010"],
        "items":           ["8.01", "", "2.02,9.01", "2.02"],
        "primaryDocument": ["exmp-other.htm", "form4.xml", "exmp-8k.htm", "old.htm"],
    }}}
    routes = {
        "https://www.sec.gov/files/company_tickers.json":
            {"0": {"cik_str": 1234567, "ticker": "EXMP", "title": "Example Corp"}},
        "https://data.sec.gov/submissions/CIK0001234567.json": submissions,
        "https://www.sec.gov/Archives/edgar/data/1234567/000123456726000088/0001234567-26-000088-index.htm": INDEX,
        "https://www.sec.gov/Archives/edgar/data/1234567/000123456726000088/q3pr.htm": RELEASE,
    }

    def fake_request(url, params=None, headers=None):
        c.requested.append(url)
        assert headers and "User-Agent" in headers
        return FakeResp(routes[url]) if url in routes else None
    monkeypatch.setattr(c, "_request", fake_request)
    return c


def test_reads_the_earnings_release_exhibit(collector):
    text = collector._fetch_latest_8k("EXMP")
    assert text and text.startswith("Example Corp reports third quarter results")
    # The newer 8.01 filing is passed over for the earnings release (2.02),
    # and the 200-day-old one is outside the window
    assert not any("000099" in u or "000010" in u for u in collector.requested)


def test_no_full_text_search_or_browse_edgar(collector):
    collector._fetch_latest_8k("EXMP")
    assert not any("efts.sec.gov" in u or "browse-edgar" in u for u in collector.requested)


def test_unknown_ticker_is_none(collector):
    assert collector._fetch_latest_8k("NOPE") is None


def test_ticker_map_is_fetched_once(collector):
    collector._fetch_latest_8k("EXMP")
    collector._fetch_latest_8k("NOPE")
    assert collector.requested.count("https://www.sec.gov/files/company_tickers.json") == 1


def test_collect_scores_the_release(collector, monkeypatch):
    monkeypatch.setattr(alt_data, "_get_shared_ollama", lambda: type("O", (), {"available": False})())
    r = collector.collect("EXMP")
    assert r.confidence > 0 and "Keyword analysis" in r.details
