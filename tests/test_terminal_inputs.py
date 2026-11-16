"""Terminal routes reject malformed input with 400 instead of 500 or junk."""
import pytest
from fastapi.testclient import TestClient

from stock_oracle.terminal import server, settings

H = {"x-terminal-token": "tok"}


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "WATCHLIST_FILE", tmp_path / "gui_watchlist.json")
    asked = []

    async def fake_ask(self, question, focus, screen, watchlist, background=""):
        asked.append((question, focus, screen))
        return {"answer": "ok"}

    async def no_data(*a, **k):
        return None

    monkeypatch.setattr(server.OracleBridge, "ask", fake_ask)
    monkeypatch.setattr(server.dossier, "market", no_data)
    monkeypatch.setattr(server.dossier, "security", no_data)
    monkeypatch.setattr(server.dossier, "market_text", lambda d: "")
    with TestClient(server.create_app(token="tok")) as c:
        c.asked = asked
        yield c


@pytest.mark.parametrize("body", [
    {"set": "AAPL"},                       # a string was split into A, A, P, L
    {"add": "NVDA"},
    {"remove": [1, 2]},
    {"set": ["AAPL", None]},
    {"set": [f"T{i}" for i in range(201)]},
])
def test_watchlist_rejects_bad_shapes(client, body):
    assert client.post("/api/watchlist", json=body, headers=H).status_code == 400


def test_watchlist_total_is_capped(client):
    client.post("/api/watchlist", json={"set": [f"A{i}" for i in range(200)]}, headers=H)
    assert client.post("/api/watchlist", json={"add": ["ZZZ"]}, headers=H).status_code == 400


def test_watchlist_normal_edit(client):
    r = client.post("/api/watchlist", json={"set": ["AAPL", "MSFT"], "add": ["NVDA"], "remove": ["MSFT"]},
                    headers=H)
    assert r.status_code == 200 and r.json()["tickers"] == ["AAPL", "NVDA"]


@pytest.mark.parametrize("body", [
    {"question": 123},
    {"question": ["why"]},
    {"question": "x" * 2001},
    {"question": "why", "symbol": 5},
    {"question": "why", "screen": ["not", "an", "object"]},
])
def test_ask_rejects_bad_shapes(client, body):
    assert client.post("/api/ask", json=body, headers=H).status_code == 400
    assert client.asked == []


def test_ask_cleans_screen_before_it_reaches_the_prompt(client):
    screen = {"panels": [{"fn": "DES", "sym": "AAPL"}, "junk", 7, {"fn": ["x"]}],
              "text": "y" * 5000, "quote": {"cp": "not a number"}}
    r = client.post("/api/ask", json={"question": "why?", "screen": screen}, headers=H)
    assert r.status_code == 200
    _, _, sent = client.asked[0]
    assert sent["panels"] == [{"fn": "DES", "sym": "AAPL"}, {"fn": "['x']", "sym": ""}]
    assert len(sent["text"]) == 3000
    assert "quote" not in sent                       # only the server's own quote is used


def test_ws_ignores_non_object_messages(client):
    with client.websocket_connect("/ws", headers={"host": "testserver", "x-terminal-token": "tok"}) as ws:
        assert ws.receive_json()["t"] == "status"
        for junk in ("[1, 2]", "42", '"text"', '{"op": "sub", "symbols": "AAPL"}'):
            ws.send_text(junk)
        ws.send_text('{"op": "ping"}')
        while True:
            msg = ws.receive_json()
            if msg["t"] == "pong":
                break
