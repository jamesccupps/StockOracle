"""An .env saved with a UTF-8 byte-order mark must not lose its first key."""
from pathlib import Path

import pytest

BOM_ENV = b"\xef\xbb\xbfFINNHUB_API_KEY=fh_bom\r\nFRED_API_KEY=fred_key\r\n"
PKG = Path(__file__).resolve().parent.parent / "stock_oracle"


@pytest.fixture
def bom_env(tmp_path):
    p = tmp_path / ".env"
    p.write_bytes(BOM_ENV)
    return p


def test_read_env_strips_bom(bom_env):
    from stock_oracle.utils.envfile import read_env
    assert read_env(bom_env) == {"FINNHUB_API_KEY": "fh_bom", "FRED_API_KEY": "fred_key"}


def test_terminal_settings_strips_bom(bom_env, monkeypatch):
    from stock_oracle.terminal import settings
    monkeypatch.setattr(settings, "ENV_FILE", bom_env)
    monkeypatch.setattr(settings, "_env_mtime", -1.0)
    assert settings._read_env_file()["FINNHUB_API_KEY"] == "fh_bom"


def test_save_env_does_not_duplicate_the_bom_key(bom_env):
    from stock_oracle.utils.envfile import read_env, save_env
    save_env(bom_env, {"FINNHUB_API_KEY": "fh_new"})
    text = bom_env.read_text(encoding="utf-8")
    assert "﻿" not in text
    assert text.count("FINNHUB_API_KEY=") == 1
    assert read_env(bom_env) == {"FINNHUB_API_KEY": "fh_new", "FRED_API_KEY": "fred_key"}


def test_config_reads_env_as_utf8_sig():
    # config.py parses .env at import time from a fixed path, so check the source
    src = (PKG / "config.py").read_text(encoding="utf-8")
    assert '_env_file.read_text(encoding="utf-8-sig"' in src
