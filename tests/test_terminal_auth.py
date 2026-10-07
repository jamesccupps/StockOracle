"""Terminal auth: token required everywhere, Host pinned on loopback, WS Origin checked."""
import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from stock_oracle.terminal import settings
from stock_oracle.terminal.server import _host_only, create_app

TOK = "tok_abc123"
LOCAL = "http://127.0.0.1:8765"


@pytest.fixture(scope="module")
def app():
    return create_app(token=TOK, trusted_hosts=settings.LOOPBACK_HOSTS)


@pytest.fixture
def client(app):
    with TestClient(app, base_url=LOCAL) as c:
        yield c


def test_no_token_rejected(client):
    assert client.get("/api/watchlist").status_code == 401
    assert client.get("/").status_code == 401
    assert client.get("/static/js/app.js").status_code == 401


def test_cross_site_form_post_rejected(client):
    # A plain <form method=POST> from another site: no preflight, no token
    r = client.post("/api/oracle/AAPL/run?fast=true",
                    headers={"Origin": "https://evil.example",
                             "Content-Type": "application/x-www-form-urlencoded"})
    assert r.status_code == 401


@pytest.mark.parametrize("host", ["evil.example", "evil.example:8765", "127.0.0.1.evil.example"])
def test_rebinding_host_rejected_even_with_token(client, host):
    assert client.get(f"/api/watchlist?token={TOK}", headers={"Host": host}).status_code == 421


@pytest.mark.parametrize("host", ["127.0.0.1:8765", "localhost:8765", "[::1]:8765", "LOCALHOST"])
def test_loopback_hosts_allowed(client, host):
    assert client.get(f"/api/watchlist?token={TOK}", headers={"Host": host}).status_code == 200


def test_non_ascii_token_is_401_not_500(client):
    assert client.get("/api/watchlist?token=t%C3%B6k").status_code == 401


def test_token_sets_strict_httponly_cookie(client):
    r = client.get(f"/?token={TOK}")
    assert r.status_code == 200
    cookie = r.headers["set-cookie"].lower()
    assert "so_terminal=" in cookie and "httponly" in cookie and "samesite=strict" in cookie
    assert client.get("/api/watchlist").status_code == 200   # cookie alone now works


def test_header_token_accepted(client):
    assert client.get("/api/watchlist", headers={"x-terminal-token": TOK}).status_code == 200


# Starlette's websocket test client ignores base_url (Host: testserver) and the
# cookie jar, so Host and Cookie are set explicitly.
WS_OK = {"host": "127.0.0.1:8765", "cookie": f"so_terminal={TOK}"}


def test_ws_same_origin_connects(client):
    with client.websocket_connect("/ws", headers={**WS_OK, "origin": LOCAL}) as ws:
        assert ws.receive_json()["t"] == "status"


def test_ws_without_origin_connects(client):
    with client.websocket_connect("/ws", headers=WS_OK) as ws:
        assert ws.receive_json()["t"] == "status"


@pytest.mark.parametrize("headers", [
    {**WS_OK, "origin": "https://evil.example"},
    {"host": "evil.example:8765", "cookie": f"so_terminal={TOK}", "origin": "http://evil.example:8765"},
])
def test_ws_foreign_origin_or_host_rejected(client, headers):
    with pytest.raises(WebSocketDisconnect):
        with client.websocket_connect("/ws", headers=headers) as ws:
            ws.receive_json()


def test_ws_without_token_closes_4401(client):
    # 4401 (not 1006) is what tells the browser client to stop reconnecting
    with pytest.raises(WebSocketDisconnect) as exc:
        with client.websocket_connect("/ws", headers={"host": "127.0.0.1:8765", "origin": LOCAL}) as ws:
            ws.receive_json()
    assert exc.value.code == 4401


def test_lan_mode_skips_host_check_but_needs_token():
    lan = create_app(token=TOK, trusted_hosts=None)
    with TestClient(lan) as c:
        assert c.get("/api/watchlist", headers={"Host": "lan-host:8765"}).status_code == 401
        assert c.get(f"/api/watchlist?token={TOK}",
                     headers={"Host": "lan-host:8765"}).status_code == 200


@pytest.mark.parametrize("raw,expected", [
    ("127.0.0.1:8765", "127.0.0.1"), ("[::1]:8765", "[::1]"), ("[::1]", "[::1]"),
    ("LocalHost", "localhost"), ("", ""),
])
def test_host_only(raw, expected):
    assert _host_only(raw) == expected


def test_access_token_persists(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "TOKEN_FILE", tmp_path / "terminal_token.txt")
    monkeypatch.setattr(settings, "get", lambda key, default="": default)
    first = settings.access_token()
    assert len(first) >= 20
    assert settings.access_token() == first
    assert (tmp_path / "terminal_token.txt").read_text().strip() == first


def test_configured_token_wins(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "TOKEN_FILE", tmp_path / "terminal_token.txt")
    monkeypatch.setattr(settings, "get", lambda key, default="": "mine" if key == "TERMINAL_TOKEN" else default)
    assert settings.access_token() == "mine"
    assert not (tmp_path / "terminal_token.txt").exists()


def test_static_assets_revalidate(client):
    r = client.get(f"/static/js/app.js?token={TOK}")
    assert r.status_code == 200 and r.headers["cache-control"] == "no-cache"
    etag = r.headers["etag"]
    r2 = client.get("/static/js/app.js", headers={"If-None-Match": etag})
    assert r2.status_code == 304
