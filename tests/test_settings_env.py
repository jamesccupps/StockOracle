"""GUI Settings save keeps keys it doesn't manage and drops retired broker logins."""
from stock_oracle.gui import merge_env

EXISTING = """# Stock Oracle Settings
FINNHUB_API_KEY=oldkey
TERMINAL_STREAM=alpaca
TERMINAL_TOKEN=keepme
ALPACA_USE_SIP=1
RH_PASSWORD=hunter2
RH_TOTP_SECRET=JBSWY3DPEHPK3PXP
NEWS_API_KEY=cleared-in-dialog
"""


def _keys(text):
    return {l.split("=", 1)[0]: l.split("=", 1)[1] for l in text.splitlines()
            if "=" in l and not l.startswith("#")}


def test_dialog_fields_win_and_unmanaged_keys_survive():
    out = _keys(merge_env(EXISTING, {"FINNHUB_API_KEY": "newkey", "NEWS_API_KEY": ""}))
    assert out["FINNHUB_API_KEY"] == "newkey"
    assert "NEWS_API_KEY" not in out                 # cleared in the dialog
    assert out["TERMINAL_STREAM"] == "alpaca"
    assert out["TERMINAL_TOKEN"] == "keepme"
    assert out["ALPACA_USE_SIP"] == "1"


def test_retired_broker_credentials_are_dropped():
    out = merge_env(EXISTING, {"FINNHUB_API_KEY": "k"})
    assert "RH_PASSWORD" not in out and "hunter2" not in out
    assert "RH_TOTP_SECRET" not in out


def test_empty_file():
    assert _keys(merge_env("", {"A": "1", "B": ""})) == {"A": "1"}


import pytest  # noqa: E402


@pytest.mark.parametrize("url,opens", [
    ("https://www.reuters.com/markets/x", True),
    ("http://example.com/a?b=c", True),
    ("file:///C:/Windows/System32/calc.exe", False),
    (r"\\attacker\share\x.exe", False),
    ("javascript:alert(1)", False),
    ("ms-settings:", False),
    ("", False),
])
def test_news_links_only_open_http(monkeypatch, url, opens):
    import webbrowser
    from stock_oracle.gui import open_news_url
    opened = []
    monkeypatch.setattr(webbrowser, "open", lambda u: opened.append(u))
    assert open_news_url(url) is opens
    assert bool(opened) is opens


def test_no_cwd_relative_data_paths():
    # A frozen build (or any launch outside the repo root) reads/writes the
    # wrong files if a module hard-codes Path("stock_oracle/...")
    import pathlib
    import re
    pkg = pathlib.Path(__file__).resolve().parent.parent / "stock_oracle"
    hits = [f"{p.relative_to(pkg)}:{i}" for p in pkg.rglob("*.py")
            for i, line in enumerate(p.read_text(encoding="utf-8").splitlines(), 1)
            if re.search(r"""Path\(\s*["']stock_oracle[/\\]""", line)]
    assert not hits, hits


def test_intelligence_and_advisor_paths_follow_data_dir():
    import stock_oracle.config as cfg
    from stock_oracle import claude_advisor, signal_intelligence
    assert signal_intelligence.INTELLIGENCE_FILE.parent == cfg.DATA_DIR
    assert claude_advisor.USAGE_FILE.parent == cfg.DATA_DIR
