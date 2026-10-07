"""Claude monthly cap holds under concurrency (threads and processes) and fails closed."""
import json
import multiprocessing as mp
import threading
import time
import types

import pytest

from stock_oracle import claude_advisor as ca

IN_TOK, OUT_TOK = 10_000, 2_000          # per fake call: $0.02 on Haiku 4.5
CALL_COST = ca.estimate_cost(IN_TOK, OUT_TOK, ca.DEFAULT_MODEL)


class FakeClient:
    def __init__(self, fail=False, latency=0.2):
        self.fail, self.latency = fail, latency
        self.calls = 0
        self._lock = threading.Lock()
        self.messages = self

    def create(self, **kw):
        time.sleep(self.latency)
        if self.fail:
            raise RuntimeError("API down")
        with self._lock:
            self.calls += 1
        return types.SimpleNamespace(
            usage=types.SimpleNamespace(input_tokens=IN_TOK, output_tokens=OUT_TOK),
            content=[types.SimpleNamespace(type="text", text="ok")])


@pytest.fixture(autouse=True)
def isolated_usage(tmp_path, monkeypatch):
    monkeypatch.setattr(ca, "USAGE_FILE", tmp_path / "claude_usage.json")
    monkeypatch.setattr(ca, "LOCK_FILE", tmp_path / "claude_usage.lock")
    return tmp_path


def _advisor(cap, client):
    adv = ca.ClaudeAdvisor(api_key="x", monthly_cap=cap)
    adv._client = client
    return adv


def test_concurrent_threads_cannot_overshoot_or_lose_spend():
    cap = 0.60
    client = FakeClient()
    threads = [threading.Thread(target=lambda: _advisor(cap, client)._call_api("s", "q", "t", 2000))
               for _ in range(40)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    recorded = json.loads(ca.USAGE_FILE.read_text())
    true_spend = client.calls * CALL_COST
    assert recorded["total_calls"] == client.calls
    assert recorded["total_spent"] == pytest.approx(true_spend)
    assert true_spend <= cap
    assert recorded["reservations"] == {}


def _proc_worker(usage_dir, n, cap, out_q):
    from pathlib import Path
    from stock_oracle import claude_advisor as mod
    mod.USAGE_FILE = Path(usage_dir) / "claude_usage.json"
    mod.LOCK_FILE = Path(usage_dir) / "claude_usage.lock"
    client = FakeClient(latency=0.05)
    for _ in range(n):
        adv = mod.ClaudeAdvisor(api_key="x", monthly_cap=cap)
        adv._client = client
        adv._call_api("s", "q", "t", 2000)
    out_q.put(client.calls)


def test_two_processes_share_one_budget(isolated_usage):
    # The GUI and the terminal are separate processes writing the same file
    ctx = mp.get_context("spawn")
    q = ctx.Queue()
    procs = [ctx.Process(target=_proc_worker, args=(str(isolated_usage), 15, 100.0, q))
             for _ in range(4)]
    for p in procs:
        p.start()
    calls = sum(q.get(timeout=120) for _ in procs)
    for p in procs:
        p.join(timeout=30)
    recorded = json.loads(ca.USAGE_FILE.read_text())
    assert calls == 60
    assert recorded["total_calls"] == 60
    assert recorded["total_spent"] == pytest.approx(60 * CALL_COST)


def test_failed_call_releases_its_reservation():
    adv = _advisor(10.0, FakeClient(fail=True, latency=0))
    assert adv._call_api("s", "q", "t", 2000) is None
    data = json.loads(ca.USAGE_FILE.read_text())
    assert data["reservations"] == {} and data["total_spent"] == 0


def test_corrupt_usage_file_fails_closed():
    ca.USAGE_FILE.write_text("{ not json")
    adv = _advisor(10.0, FakeClient(latency=0))
    ok, reason = adv.is_available()
    assert not ok and "unreadable" in reason
    assert adv._call_api("s", "q", "t", 2000) is None
    assert adv._client.calls == 0
    status = adv.get_status()
    assert status["enabled"] is False and "error" in status
    assert ca.USAGE_FILE.read_text() == "{ not json"      # left for the user to inspect


def test_unknown_model_priced_at_most_expensive_tier():
    top_in = max(c["input"] for c in ca.MODEL_COSTS.values())
    top_out = max(c["output"] for c in ca.MODEL_COSTS.values())
    assert ca.estimate_cost(1_000_000, 1_000_000, "claude-some-future-model") == top_in + top_out


@pytest.mark.parametrize("model", ["claude-opus-5-5", "claude-sonnet-5-5", "claude-haiku-5-5",
                                   "claude-fable-5-1", ca.DEFAULT_MODEL])
def test_current_models_are_priced(model):
    assert model in ca.MODEL_COSTS


def test_stale_reservation_from_crashed_process_expires():
    t = ca.SpendingTracker(monthly_cap=1.0)
    rid, _ = t.reserve(10_000, 100_000, ca.DEFAULT_MODEL)     # ~$0.51 hold
    assert rid
    ok, _ = ca.SpendingTracker(monthly_cap=1.0).can_afford(1000, 1000, ca.DEFAULT_MODEL)
    assert not ok                                               # the hold counts
    data = json.loads(ca.USAGE_FILE.read_text())
    data["reservations"][rid]["at"] -= ca.RESERVATION_TTL + 1
    ca.USAGE_FILE.write_text(json.dumps(data))
    ok, _ = ca.SpendingTracker(monthly_cap=1.0).can_afford(1000, 1000, ca.DEFAULT_MODEL)
    assert ok


def test_new_month_resets_spend():
    ca.USAGE_FILE.write_text(json.dumps({"month": "1999-01", "total_spent": 9.99}))
    assert ca.SpendingTracker(monthly_cap=10.0).get_status()["spent"] == 0
