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
