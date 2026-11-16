"""First-run wizard must not wipe an .env the user already filled in."""
import tkinter as tk

import pytest

from stock_oracle import setup_wizard as sw
from stock_oracle.utils.envfile import read_env

EXISTING = (
    "FINNHUB_API_KEY=fh_existing\n"
    "ANTHROPIC_API_KEY=sk-ant-existing\n"
    "TERMINAL_STREAM=alpaca\n"
    "RH_PASSWORD=old-broker-login\n"
)


@pytest.fixture
def wizard(tmp_path, monkeypatch):
    base, data = tmp_path / "pkg", tmp_path / "data"
    base.mkdir()
    (base / ".env").write_text(EXISTING, encoding="utf-8")
    monkeypatch.setattr(sw, "BASE_DIR", base)
    monkeypatch.setattr(sw, "ENV_FILE", base / ".env")
    monkeypatch.setattr(sw, "DATA_DIR", data)
    monkeypatch.setattr(sw, "SETUP_FLAG", data / ".setup_complete")
    try:
        w = sw.SetupWizard()
    except tk.TclError as e:          # no display (headless runner)
        pytest.skip(f"Tk unavailable: {e}")
    yield w, base, data
    try:
        w._close()
    except tk.TclError:
        pass                          # already closed by the test (Skip)


def test_wizard_starts_from_existing_env(wizard):
    w, _, _ = wizard
    assert w.settings["FINNHUB_API_KEY"] == "fh_existing"
    assert w.settings["TERMINAL_STREAM"] == "alpaca"


def test_skip_writes_nothing_but_the_done_flag(wizard, monkeypatch):
    w, base, data = wizard
    before = (base / ".env").read_bytes()
    monkeypatch.setattr(sw.messagebox, "askyesno", lambda *a, **k: True)
    w._skip()
    assert (base / ".env").read_bytes() == before
    assert (data / ".setup_complete").exists()
    assert w.completed


def test_saving_merges_instead_of_overwriting(wizard):
    w, base, _ = wizard
    w.settings["FINNHUB_API_KEY"] = "fh_new"        # changed in the wizard
    w.settings["NEWS_API_KEY"] = ""                 # box left empty
    w._save_all_settings()
    env = read_env(base / ".env")
    assert env["FINNHUB_API_KEY"] == "fh_new"
    assert env["ANTHROPIC_API_KEY"] == "sk-ant-existing"
    assert env["TERMINAL_STREAM"] == "alpaca"        # not a wizard field
    assert "RH_PASSWORD" not in env                  # retired broker login dropped
    assert "NEWS_API_KEY" not in env


def test_watchlist_goes_to_the_file_the_app_reads(wizard):
    import json
    from stock_oracle.terminal import settings as terminal_settings
    w, _, data = wizard
    w.watchlist = ["NVDA", "SCHD"]
    w._save_all_settings()
    assert json.loads((data / "gui_watchlist.json").read_text()) == ["NVDA", "SCHD"]
    assert not (data / "watchlist.json").exists()
    # same file name the GUI and the terminal use
    assert terminal_settings.WATCHLIST_FILE.name == sw.WATCHLIST_FILE_NAME
    gui_src = (sw.Path(sw.__file__).parent / "gui.py").read_text(encoding="utf-8")
    assert f'DATA_DIR / "{sw.WATCHLIST_FILE_NAME}"' in gui_src


def test_wizard_starts_from_existing_watchlist(tmp_path, monkeypatch):
    (tmp_path / "gui_watchlist.json").write_text('["LUNR", "RKLB"]')
    monkeypatch.setattr(sw, "DATA_DIR", tmp_path)
    assert sw._existing_watchlist() == ["LUNR", "RKLB"]
    (tmp_path / "gui_watchlist.json").write_text('{"not": "a list"}')
    assert sw._existing_watchlist() == []
