"""API keys stay out of URLs, log lines and HTTP error bodies."""
import logging

import pytest
import requests

from stock_oracle.utils.redact import redact

SECRET = "SECRETKEY_DO_NOT_LEAK"


@pytest.mark.parametrize("text", [
    f"Max retries exceeded with url: /api/v1/quote?symbol=AAPL&token={SECRET} (Caused by ...)",
    f"/fred/series/observations?series_id=IPMAN&api_key={SECRET}&file_type=json",
    f"GET https://x.test/?apikey={SECRET}",
    f"'https://x.test/a?TOKEN={SECRET}'",
])
def test_redact_strips_secret_params(text):
    out = redact(text)
    assert SECRET not in out
    assert "***" in out


def test_redact_leaves_other_params():
    assert redact("/quote?symbol=AAPL&from=2026-01-01") == "/quote?symbol=AAPL&from=2026-01-01"


# ── base collector: requests exceptions are logged without the key ──

def test_base_collector_logs_redacted(caplog, monkeypatch):
    from stock_oracle.collectors import base

    class Dummy(base.BaseCollector):
        name = "dummy"

        def collect(self, ticker):
            return None

    def boom(*a, **kw):
        raise requests.ConnectionError(
            f"HTTPSConnectionPool(host='api.test', port=443): Max retries exceeded with "
            f"url: /series?series_id=X&api_key={SECRET}")

    monkeypatch.setattr(base.time, "sleep", lambda s: None)
    monkeypatch.setattr(base.BaseCollector, "_host_failures", {})
    monkeypatch.setattr(base.BaseCollector, "_host_until", {})
    c = Dummy()
    monkeypatch.setattr(c._session, "get", boom)
    with caplog.at_level(logging.DEBUG, logger="stock_oracle"):
        assert c._request("https://api.test/series", params={"api_key": SECRET}) is None
    assert caplog.records, "expected the request error to be logged"
    assert SECRET not in caplog.text


# ── Finnhub key travels in the X-Finnhub-Token header ───────────

def test_collector_sends_key_in_header(monkeypatch):
    from stock_oracle.collectors.finnhub_collector import FinnhubCollector
    seen = []
    c = FinnhubCollector()
    monkeypatch.setattr(c, "_request", lambda url, params=None, headers=None: seen.append((params, headers)))
    c._get_quote("AAPL", SECRET)
    c._get_recommendations("AAPL", SECRET)
    c._get_insider_signal("AAPL", SECRET)
    assert len(seen) == 3
    for params, headers in seen:
        assert SECRET not in str(params)
        assert headers == {"X-Finnhub-Token": SECRET}


def test_terminal_provider_sends_key_in_header(monkeypatch):
    from stock_oracle.terminal.providers import finnhub
    seen = {}

    class Resp:
        status_code = 200

        def json(self):
            return {"c": 1.0}

    def fake_get(url, params=None, headers=None, timeout=None):
        seen.update(url=url, params=params, headers=headers)
        return Resp()

    monkeypatch.setattr(finnhub.settings, "finnhub_key", lambda: SECRET)
    monkeypatch.setattr(finnhub._session, "get", fake_get)
    assert finnhub._get("/quote", {"symbol": "AAPL"}) == {"c": 1.0}
    assert SECRET not in seen["url"] and SECRET not in str(seen["params"])
    assert seen["headers"] == {"X-Finnhub-Token": SECRET}


def test_terminal_network_error_is_clean_finnhub_error(monkeypatch):
    from stock_oracle.terminal.providers import finnhub

    def boom(*a, **kw):
        raise requests.ConnectionError(f"Max retries exceeded with url: /api/v1/quote?token={SECRET}")

    monkeypatch.setattr(finnhub.settings, "finnhub_key", lambda: SECRET)
    monkeypatch.setattr(finnhub._session, "get", boom)
    with pytest.raises(finnhub.FinnhubError) as exc:
        finnhub._get("/quote", {"symbol": "AAPL"})
    assert SECRET not in str(exc.value)
    assert exc.value.__cause__ is None and exc.value.__suppress_context__


def test_terminal_falls_back_to_yahoo_when_finnhub_unreachable(monkeypatch):
    from fastapi.testclient import TestClient
    from stock_oracle.terminal.providers import finnhub, yahoo
    from stock_oracle.terminal.server import create_app

    def boom(*a, **kw):
        raise requests.ConnectionError(f"url: /api/v1/company-news?token={SECRET}")

    monkeypatch.setattr(finnhub.settings, "finnhub_key", lambda: SECRET)
    monkeypatch.setattr(finnhub._session, "get", boom)
    monkeypatch.setattr(yahoo, "news", lambda q, *a, **kw: [{"headline": "from yahoo"}])
    with TestClient(create_app()) as c:
        r = c.get("/api/news/AAPL")
    assert r.status_code == 200
    assert r.json()["src"] == "yahoo"
    assert SECRET not in r.text
