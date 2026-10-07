"""
Terminal web server
===================
FastAPI app: REST endpoints for each terminal function, /ws for live quotes
and Oracle progress, and the static front end.

Every request must carry the access token once as ?token=..., after which
a SameSite=Strict cookie is set. That holds on localhost too: without it any
web page could POST to 127.0.0.1 (CSRF) or reach it through DNS rebinding.
When bound to loopback the Host header is also checked against
settings.LOOPBACK_HOSTS. create_app(token=None) is for in-process tests only.
"""
import asyncio
import json
import logging
import secrets
from contextlib import asynccontextmanager
from typing import Dict, Optional
from urllib.parse import urlsplit

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles

from stock_oracle.terminal import __version__, dossier, market, settings, symbols as S
from stock_oracle.terminal.oracle_bridge import OracleBridge
from stock_oracle.terminal.providers import DataRouter, ProviderError
from stock_oracle.terminal.quotes import Client, QuoteHub

logger = logging.getLogger("stock_oracle.terminal")

COOKIE = "so_terminal"


def _sym(raw: str) -> str:
    sym = S.normalize(raw)
    if not S.is_valid(sym):
        raise HTTPException(400, f"Not a valid symbol: {raw!r}")
    return sym


def _host_only(host_header: str) -> str:
    """'127.0.0.1:8765' -> '127.0.0.1', '[::1]:8765' -> '[::1]'."""
    h = (host_header or "").strip().lower()
    if h.startswith("["):
        return h[:h.find("]") + 1] if "]" in h else h
    return h.split(":", 1)[0]


def create_app(token: Optional[str] = None,
               trusted_hosts: Optional[frozenset] = None) -> FastAPI:
    router = DataRouter()
    hub = QuoteHub(router)
    bridge = OracleBridge(notify=hub.broadcast)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await hub.start()
        yield
        await hub.stop()

    app = FastAPI(title="Stock Oracle Terminal", version=__version__, lifespan=lifespan,
                  docs_url=None, redoc_url=None, openapi_url=None)
    app.state.router, app.state.hub, app.state.bridge = router, hub, bridge
    app.state.token = token

    # ── auth ─────────────────────────────────────────────────

    def _host_ok(headers) -> bool:
        return trusted_hosts is None or _host_only(headers.get("host", "")) in trusted_hosts

    def _authorized(cookies, query, headers) -> bool:
        if not token:
            return True
        supplied = query.get("token") or cookies.get(COOKIE) or headers.get("x-terminal-token")
        # bytes: compare_digest raises TypeError on non-ASCII str
        return bool(supplied) and secrets.compare_digest(str(supplied).encode(), token.encode())

    @app.middleware("http")
    async def auth_mw(request: Request, call_next):
        if not _host_ok(request.headers):
            return JSONResponse({"detail": "Host not allowed."}, 421)
        if not _authorized(request.cookies, request.query_params, request.headers):
            return JSONResponse({"detail": "Missing or wrong terminal token. Open the URL printed "
                                           "in the console (it ends with ?token=...)."}, 401)
        response = await call_next(request)
        if token and request.query_params.get("token"):
            response.set_cookie(COOKIE, token, httponly=True, samesite="strict", max_age=90 * 86400)
        return response

    # ── errors ───────────────────────────────────────────────

    @app.exception_handler(ProviderError)
    async def provider_error(_, exc: ProviderError):
        return JSONResponse({"detail": str(exc)}, 404)

    @app.exception_handler(ValueError)
    async def value_error(_, exc: ValueError):
        return JSONResponse({"detail": str(exc)}, 400)

    async def guarded(coro):
        try:
            return await coro
        except (HTTPException, ProviderError, ValueError):
            raise
        except Exception as e:
            logger.warning(f"terminal: upstream error: {e!r}")
            raise HTTPException(502, f"Data source error: {e}")

    # ── front end ────────────────────────────────────────────

    @app.get("/", include_in_schema=False)
    async def index():
        return FileResponse(settings.STATIC_DIR / "index.html",
                            headers={"Cache-Control": "no-store"})

    app.mount("/static", StaticFiles(directory=settings.STATIC_DIR), name="static")

    @app.get("/favicon.ico", include_in_schema=False)
    async def favicon():
        return Response(status_code=204)

    # ── status / watchlist ───────────────────────────────────

    @app.get("/api/status")
    async def status():
        return {**hub.status(), "version": __version__, "engine_loaded": bridge.loaded,
                "advisor": bridge.advisor_status(), "scan": bridge.scan_state,
                "tape": [{"s": s, "label": l} for s, l in market.TAPE]}

    @app.get("/api/watchlist")
    async def get_watchlist():
        return {"tickers": settings.load_watchlist()}

    @app.post("/api/watchlist")
    async def edit_watchlist(body: Dict):
        current = settings.load_watchlist()
        if "set" in body:
            current = [_sym(t) for t in body["set"]]
        for t in body.get("add", []):
            sym = _sym(t)
            if sym not in current:
                current.append(sym)
        drop = {_sym(t) for t in body.get("remove", [])}
        current = [t for t in current if t not in drop]
        return {"tickers": settings.save_watchlist(current)}

    # ── market data ──────────────────────────────────────────

    @app.get("/api/quote/{sym}")
    async def quote(sym: str):
        sym = _sym(sym)
        q = hub.get(sym)
        if q is None:
            q = await router.quote_realtime(sym)
        if q is None:
            fresh = await guarded(router.quotes([sym], hub.status()["session"]))
            q = fresh.get(sym)
        if q is None:
            raise HTTPException(404, f"No quote for {sym}")
        return q

    @app.get("/api/history/{sym}")
    async def history(sym: str, range: str = "6M", daily: bool = False):
        return await guarded(router.history(_sym(sym), range, daily))

    @app.get("/api/des/{sym}")
    async def des(sym: str):
        return await guarded(router.profile(_sym(sym)))

    @app.get("/api/fa/{sym}")
    async def fa(sym: str):
        return await guarded(router.fundamentals(_sym(sym)))

    @app.get("/api/anr/{sym}")
    async def anr(sym: str):
        return await guarded(router.analysts(_sym(sym)))

    @app.get("/api/ee/{sym}")
    async def ee(sym: str):
        return await guarded(router.earnings(_sym(sym)))

    @app.get("/api/omon/{sym}")
    async def omon(sym: str, expiry: Optional[str] = None):
        return await guarded(router.options(_sym(sym), expiry))

    @app.get("/api/news")
    async def market_news():
        return await guarded(router.market_news())

    @app.get("/api/news/{sym}")
    async def news(sym: str):
        return await guarded(router.news(_sym(sym)))

    @app.get("/api/peers/{sym}")
    async def peers(sym: str):
        return await guarded(router.peers(_sym(sym), settings.load_watchlist()))

    @app.get("/api/market/{code}")
    async def market_group(code: str):
        return await guarded(router.market_group(code))

    @app.get("/api/search")
    async def search(q: str):
        q = q.strip()
        if not q:
            return {"results": []}

        from stock_oracle.terminal.providers import yahoo
        rows = await guarded(router.cache.get(("search", q.lower()), 3600, yahoo.symbol_search, q))
        return {"results": rows}

    # ── Oracle ───────────────────────────────────────────────

    @app.get("/api/oracle")
    async def oracle_verdicts(tickers: Optional[str] = None):
        tl = [_sym(t) for t in tickers.split(",") if t.strip()] if tickers else settings.load_watchlist()
        return {"verdicts": bridge.verdicts(tl), "scan": bridge.scan_state}

    @app.get("/api/oracle/{sym}")
    async def oracle_detail(sym: str):
        return await bridge.detail(_sym(sym))

    @app.post("/api/oracle/{sym}/run")
    async def oracle_run(sym: str, fast: bool = False):
        return await bridge.run(_sym(sym), fast=fast)

    @app.post("/api/scan")
    async def oracle_scan(body: Optional[Dict] = None):
        tickers = [_sym(t) for t in (body or {}).get("tickers", [])] or settings.load_watchlist()
        return await bridge.scan(tickers)

    @app.get("/api/regime")
    async def regime():
        return await guarded(bridge.regime())

    @app.get("/api/brk")
    async def breakouts(tickers: Optional[str] = None):
        tl = [_sym(t) for t in tickers.split(",") if t.strip()] if tickers else settings.load_watchlist()
        return {"rows": await guarded(bridge.breakouts(tl))}

    @app.get("/api/acc")
    async def accuracy():
        return await guarded(bridge.accuracy())

    @app.post("/api/ask")
    async def ask(body: Dict):
        question = (body.get("question") or "").strip()
        if not question:
            raise HTTPException(400, "Ask a question, e.g.  ASK why is NVDA bearish today?")
        focus = body.get("symbol")
        focus = _sym(focus) if focus else None
        screen = body.get("screen") or {}
        if focus and hub.get(focus):
            from stock_oracle.terminal.quotes import _compact
            screen["quote"] = _compact(hub.get(focus))
        # Ground the answer: live dossier for the focused security + market backdrop
        sec_d, mkt_d = await asyncio.gather(
            dossier.security(router, bridge, hub, focus) if focus else asyncio.sleep(0, None),
            dossier.market(router, bridge))
        background = "\n\n".join(x for x in (
            dossier.security_text(sec_d) if sec_d else "", dossier.market_text(mkt_d)) if x)
        return await guarded(bridge.ask(question, focus, screen, settings.load_watchlist(), background))

    # ── deep data ────────────────────────────────────────────

    @app.get("/api/brief")
    async def brief_market():
        return await dossier.market(router, bridge)

    @app.get("/api/brief/{sym}")
    async def brief(sym: str):
        return await guarded(dossier.security(router, bridge, hub, _sym(sym)))

    @app.get("/api/eeo/{sym}")
    async def eeo(sym: str):
        return await guarded(router.estimates(_sym(sym)))

    @app.get("/api/hds/{sym}")
    async def hds(sym: str):
        return await guarded(router.holders(_sym(sym)))

    @app.get("/api/ins/{sym}")
    async def ins(sym: str):
        return await guarded(router.insiders(_sym(sym)))

    @app.get("/api/dvd/{sym}")
    async def dvd(sym: str):
        return await guarded(router.dividends(_sym(sym)))

    @app.get("/api/si/{sym}")
    async def si(sym: str):
        return await guarded(router.short_interest(_sym(sym)))

    @app.get("/api/hold/{sym}")
    async def hold(sym: str):
        return await guarded(router.etf(_sym(sym)))

    @app.get("/api/cf/{sym}")
    async def cf(sym: str):
        return await guarded(router.filings(_sym(sym)))

    @app.get("/api/most")
    async def most(code: str = "GAINERS"):
        return await guarded(router.screen(code))

    @app.get("/api/eco")
    async def eco():
        return await guarded(router.macro())

    @app.get("/api/curve")
    async def curve():
        return await guarded(router.curve())

    @app.get("/api/evts")
    async def evts():
        return await guarded(router.earnings_calendar(settings.load_watchlist()))

    # ── websocket ────────────────────────────────────────────

    @app.websocket("/ws")
    async def ws_endpoint(ws: WebSocket):
        # Browsers always send Origin on websocket handshakes and CORS doesn't
        # apply to them, so a foreign page is caught here.
        origin = ws.headers.get("origin")
        if not _host_ok(ws.headers) or (origin and urlsplit(origin).netloc.lower()
                                        != ws.headers.get("host", "").lower()):
            await ws.close()          # before accept() -> HTTP 403
            return
        if not _authorized(ws.cookies, ws.query_params, ws.headers):
            # Accept first: a close before accept() reaches the browser as 1006,
            # and only 4401 tells the client to stop reconnecting.
            await ws.accept()
            await ws.close(code=4401)
            return
        await ws.accept()
        client = Client(ws)
        hub.register(client)
        writer = asyncio.create_task(client.writer())
        try:
            while True:
                raw = await ws.receive_text()
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                op = msg.get("op")
                if op == "sub":
                    hub.set_subs(client, msg.get("symbols") or [])
                elif op == "ping":
                    client.push({"t": "pong"})
        except WebSocketDisconnect:
            pass
        except Exception as e:
            logger.debug(f"ws closed: {e!r}")
        finally:
            writer.cancel()
            hub.unregister(client)

    return app
