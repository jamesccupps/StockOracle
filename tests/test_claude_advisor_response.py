"""Advisor reads the answer from text blocks, not content[0] (which can be a thinking block)."""
import types

import pytest
from anthropic.types import TextBlock, ThinkingBlock

from stock_oracle import claude_advisor as ca


@pytest.fixture(autouse=True)
def isolated_usage(tmp_path, monkeypatch):
    monkeypatch.setattr(ca, "USAGE_FILE", tmp_path / "claude_usage.json")
    monkeypatch.setattr(ca, "LOCK_FILE", tmp_path / "claude_usage.lock")


def _client(content):
    resp = types.SimpleNamespace(usage=types.SimpleNamespace(input_tokens=100, output_tokens=50),
                                 content=content)
    return types.SimpleNamespace(messages=types.SimpleNamespace(create=lambda **kw: resp))


@pytest.mark.parametrize("content,expected", [
    ([ThinkingBlock(type="thinking", thinking="", signature="sig"),
      TextBlock(type="text", text="the answer")], "the answer"),
    ([TextBlock(type="text", text="part 1 "), TextBlock(type="text", text="part 2")], "part 1 part 2"),
    ([], ""),
])
def test_text_extracted_from_text_blocks(content, expected):
    adv = ca.ClaudeAdvisor(api_key="x", model="claude-opus-5-5", monthly_cap=10.0)
    adv._client = _client(content)
    assert adv._call_api("s", "q", "t", 500) == expected


def test_sdk_retries_are_off():
    # The spend tracker reserves one call; an SDK-internal retry could be billed twice
    adv = ca.ClaudeAdvisor(api_key="sk-ant-test", monthly_cap=10.0)
    assert adv._get_client().max_retries == 0


def test_gui_checkin_is_paced_by_the_clock_not_scan_count(monkeypatch):
    import types
    from stock_oracle import gui
    clock = [1000.0]
    monkeypatch.setattr(gui.time, "monotonic", lambda: clock[0])
    fake = types.SimpleNamespace(_last_claude_checkin=1000.0)
    due = lambda: gui.StockOracleGUI._claude_checkin_due(fake)
    # 40 s scans for 29 minutes: no check-in (scan-count pacing fired every 6 scans = 4 min)
    for _ in range(int(29 * 60 / 40)):
        clock[0] += 40
        assert not due()
    clock[0] = 1000.0 + 30 * 60
    assert due()                    # 30 min reached
    assert not due()                # and the clock restarted


def test_structured_calls_get_room_and_cutoffs_are_loud(monkeypatch, caplog):
    import logging
    from stock_oracle import market_regime
    monkeypatch.setattr(market_regime.MarketRegimeDetector, "detect", lambda self, *a, **k: {})  # offline
    adv = ca.ClaudeAdvisor(api_key="x", monthly_cap=10.0)
    asked = []
    resp = types.SimpleNamespace(usage=types.SimpleNamespace(input_tokens=10, output_tokens=8000),
                                 stop_reason="max_tokens",
                                 content=[TextBlock(type="text", text='{"weight_adjustments": {')])
    adv._client = types.SimpleNamespace(messages=types.SimpleNamespace(
        create=lambda **kw: asked.append(kw["max_tokens"]) or resp))
    with caplog.at_level(logging.WARNING, logger="stock_oracle"):
        out = adv.hourly_checkin({}, {}, [])
    # 27 tickers measured ~3,800 output tokens; the old 2,000 cut every check-in off
    assert asked == [ca.STRUCTURED_MAX_OUTPUT_TOKENS] and ca.STRUCTURED_MAX_OUTPUT_TOKENS >= 8000
    assert "cut off" in caplog.text
    assert out["weight_adjustments"] == {}            # unparseable reply applies nothing


def test_haiku_5_5_is_sent_with_thinking_off_and_others_unchanged():
    sent = {}
    resp = types.SimpleNamespace(usage=types.SimpleNamespace(input_tokens=1, output_tokens=1),
                                 stop_reason="end_turn", content=[TextBlock(type="text", text="ok")])
    for model in (ca.DEFAULT_MODEL, "claude-haiku-4-5-20251001", "claude-opus-5-5"):
        adv = ca.ClaudeAdvisor(api_key="x", model=model, monthly_cap=10.0)
        adv._client = types.SimpleNamespace(messages=types.SimpleNamespace(
            create=lambda **kw: sent.__setitem__(kw["model"], kw) or resp))
        assert adv._call_api("s", "q", "t", 100) == "ok"
    assert ca.DEFAULT_MODEL == "claude-haiku-5-5"
    assert sent["claude-haiku-5-5"]["thinking"] == {"type": "disabled"}
    assert "thinking" not in sent["claude-haiku-4-5-20251001"]
    assert "thinking" not in sent["claude-opus-5-5"]       # can't be disabled there: never sent
