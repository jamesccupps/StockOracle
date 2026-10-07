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
