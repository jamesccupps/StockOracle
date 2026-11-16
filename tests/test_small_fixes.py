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


def test_launcher_dev_paths_match_config(monkeypatch):
    import os
    import sys
    from pathlib import Path
    import stock_oracle.config as cfg
    from stock_oracle import launcher
    made = []
    monkeypatch.setattr(os, "makedirs", lambda p, exist_ok=False: made.append(p))
    monkeypatch.delattr(sys, "frozen", raising=False)
    app_dir, data_dir = launcher.get_app_dirs()
    pkg = Path(launcher.__file__).resolve().parent
    assert Path(data_dir) == pkg / "data" == Path(cfg.BASE_DIR) / "data"
    assert Path(app_dir) == pkg.parent                     # where "import stock_oracle" resolves
    assert "stock_oracle" + os.sep + "stock_oracle" not in data_dir


def test_gui_watchlist_loader_rejects_non_lists(tmp_path, monkeypatch):
    import queue
    import types
    from stock_oracle import gui
    monkeypatch.setattr(gui, "DATA_DIR", tmp_path)
    f = tmp_path / "gui_watchlist.json"
    for bad in ('{"AAPL": 1}', '"AAPL"', '[1, 2]', "[]", "not json"):
        f.write_text(bad)
        fake = types.SimpleNamespace(watchlist=["DEFAULT"], msg_queue=queue.Queue())
        gui.StockOracleGUI._load_watchlist(fake)
        assert fake.watchlist == ["DEFAULT"], bad
        assert fake.msg_queue.get_nowait()[0] == "log"
    f.write_text('["nvda", " schd "]')
    fake = types.SimpleNamespace(watchlist=["DEFAULT"], msg_queue=queue.Queue())
    gui.StockOracleGUI._load_watchlist(fake)
    assert fake.watchlist == ["NVDA", "SCHD"]


def test_env_file_location(tmp_path):
    from stock_oracle.config import _resolve_env_file
    pkg, data = tmp_path / "pkg", tmp_path / "appdata"
    pkg.mkdir()
    data.mkdir()
    # source checkout: next to the code, as the README says
    assert _resolve_env_file(pkg, data, data_override=False) == pkg / ".env"
    # frozen build (data folder override): in the data folder
    assert _resolve_env_file(pkg, data, data_override=True) == data / ".env"
    # an older build's .env inside the program folder is moved there once
    (pkg / ".env").write_text("FINNHUB_API_KEY=old\n")
    assert _resolve_env_file(pkg, data, data_override=True) == data / ".env"
    assert (data / ".env").read_text() == "FINNHUB_API_KEY=old\n"
    assert not (pkg / ".env").exists()
    # never overwrites an existing data-folder .env
    (pkg / ".env").write_text("FINNHUB_API_KEY=stale\n")
    assert _resolve_env_file(pkg, data, data_override=True) == data / ".env"
    assert (data / ".env").read_text() == "FINNHUB_API_KEY=old\n"


def test_every_env_reader_uses_config_env_file():
    from pathlib import Path
    pkg = Path(__file__).resolve().parent.parent / "stock_oracle"
    for f in ("gui.py", "setup_wizard.py", "terminal/settings.py"):
        assert '/ ".env"' not in (pkg / f).read_text(encoding="utf-8"), f
