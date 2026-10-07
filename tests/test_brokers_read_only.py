"""brokers.py is read-only: no code path can submit, modify or cancel an order."""
import inspect
import re
from pathlib import Path

from stock_oracle import brokers

SRC = Path(brokers.__file__).read_text(encoding="utf-8")

# SDK calls that move money: Webull TradeClient.order.*, robin_stocks order_*/cancel_*
FORBIDDEN_CALLS = re.compile(
    r"\.(place_order|replace_order|modify_order|cancel_order|order_buy_\w+|order_sell_\w+|"
    r"order_option_\w+|order_crypto\w*|cancel_\w+_order|cancel_all_\w+)\s*\(")


def test_no_order_calls_in_source():
    assert not FORBIDDEN_CALLS.findall(SRC)


def test_no_order_methods_on_connectors():
    for cls in (brokers.WebullConnector, brokers.RobinhoodConnector, brokers.DualBrokerManager):
        names = {n for n, _ in inspect.getmembers(cls, inspect.isfunction)}
        assert not names & {"place_order", "cancel_order", "trade", "modify_order"}, cls.__name__


def test_settings_dialog_no_longer_collects_broker_logins():
    gui = (Path(brokers.__file__).parent / "gui.py").read_text(encoding="utf-8")
    for key in ("RH_EMAIL", "RH_PASSWORD", "RH_TOTP_SECRET", "WEBULL_APP_KEY", "WEBULL_APP_SECRET"):
        assert not re.search(rf'add_field\(\s*"{key}"', gui), key
