"""Small fixes from the 2026-11-15 review."""
import json

from stock_oracle.utils import ticker_resolver as tr


def test_failed_name_lookup_is_not_cached(tmp_path, monkeypatch):
    monkeypatch.setattr(tr, "NAMES_CACHE_FILE", tmp_path / "company_names.json")
    monkeypatch.setattr(tr, "_name_cache", {})
    monkeypatch.setattr(tr, "_lookup_yfinance", lambda t: None)
    monkeypatch.setattr(tr, "_lookup_sec", lambda t: None)
    assert tr.resolve_name("ZZZQ") == "ZZZQ"
    assert "ZZZQ" not in tr._load_cache()
    # next time the lookup works, the real name is used and cached
    monkeypatch.setattr(tr, "_lookup_yfinance", lambda t: "Zed Corp")
    assert tr.resolve_name("ZZZQ") == "Zed Corp"
    assert json.loads((tmp_path / "company_names.json").read_text())["ZZZQ"] == "Zed Corp"
