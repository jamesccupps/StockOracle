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
