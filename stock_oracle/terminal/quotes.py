"""
Live quote hub
==============
Keeps one snapshot per symbol and pushes changes to every browser that is
watching that symbol.

Sources, merged into the same snapshot:
  * Websocket trades — Finnhub (free key, up to 50 symbols) or Alpaca
    (IEX feed on the free plan, 30 symbols). Uses Oracle's realtime.py clients.
  * A Yahoo batch poll — fills in day open/high/low/volume/previous close,
    covers whatever the stream can't (indices, FX, futures, symbols beyond
    the stream cap) and is the only source when no key is configured.
    Runs every TERMINAL_POLL_SECONDS without a stream, every 60s with one.

Browsers tell the hub what they're showing ({"op": "sub", "symbols": [...]})
and receive batched {"t": "quotes"} messages a few times per second.
"""
import asyncio
import itertools
import json
import logging
import time
from collections import Counter, deque
from typing import Dict, Iterable, List, Optional, Set

from stock_oracle.terminal import market, settings, symbols as S
from stock_oracle.terminal.util import plain

logger = logging.getLogger("stock_oracle.terminal")

FLUSH_SECONDS = 0.4
MAX_SUBS_PER_CLIENT = 300
LIVE_PRICE_GRACE = 15  # seconds a streamed price wins over a polled one


def market_session() -> Dict:
    try:
        from stock_oracle.collectors.finnhub_collector import get_market_session
        return get_market_session()
    except Exception:
        return {"session": "regular", "is_open": True, "detail": ""}


class Client:
    """One connected browser tab."""
    _ids = itertools.count(1)

    def __init__(self, ws):
        self.id = next(self._ids)
        self.ws = ws
        self.subs: Set[str] = set()
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=1000)

    def push(self, msg: Dict):
        try:
            self.queue.put_nowait(msg)
        except asyncio.QueueFull:
            # Slow consumer: drop the oldest message rather than block the hub
            try:
                self.queue.get_nowait()
                self.queue.put_nowait(msg)
            except Exception:
                pass

    async def writer(self):
        while True:
            msg = await self.queue.get()
            try:
                text = json.dumps(msg, separators=(",", ":"), allow_nan=False)
            except (TypeError, ValueError):
                text = json.dumps(plain(msg), separators=(",", ":"))
            await self.ws.send_text(text)


class _TickSink:
    """Adapter so realtime.py providers can hand ticks to the hub from their thread."""

    def __init__(self, hub: "QuoteHub", from_stream_symbol):
        self.hub = hub
        self.map = from_stream_symbol

    def add_tick(self, tick):
        self.hub.on_tick_threadsafe(self.map(tick.symbol), tick)


def _compact(q: Dict) -> Dict:
    return {
        "s": q["symbol"], "p": q.get("price"), "pc": q.get("prev_close"),
        "c": q.get("change"), "cp": q.get("change_pct"),
        "o": q.get("open"), "h": q.get("high"), "l": q.get("low"), "v": q.get("volume"),
        "xp": q.get("ext_price"), "xcp": q.get("ext_change_pct"), "xs": q.get("ext_session"),
        "b": q.get("bid"), "a": q.get("ask"),
        "u": q.get("updated"), "src": q.get("src"), "live": bool(q.get("live_at")),
    }


def _same(a: Dict, b: Dict) -> bool:
    """Equal apart from the fetch time. Re-sending an unchanged quote on every
    poll (the "u" stamp always differs) is what fed the charts flat bars."""
    ca, cb = _compact(a), _compact(b)
    ca.pop("u")
    cb.pop("u")
    return ca == cb


def _recompute(q: Dict):
    p, pc = q.get("price"), q.get("prev_close")
    if p is not None and pc:
        q["change"] = p - pc
        q["change_pct"] = (p - pc) / pc * 100


class QuoteHub:
    def __init__(self, router):
        self.router = router
        self.snap: Dict[str, Dict] = {}
        self.clients: Dict[int, Client] = {}
        self._dirty: Set[str] = set()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._tasks: List[asyncio.Task] = []
        self._bg: Set[asyncio.Task] = set()
        self._wake = asyncio.Event()
        self._session = market_session()
        self._session_at = 0.0

        self._stream = None
        self._stream_kind = "poll"
        self._stream_key = None
        self._stream_syms: Set[str] = set()      # internal symbols currently streamed
        self._stream_max = 0
        self._to_stream = None
        self._tick_times: deque = deque(maxlen=5000)
        self.last_poll_at = 0.0
        self.last_poll_error = ""

    # ── lifecycle ────────────────────────────────────────────

    async def start(self):
        self._loop = asyncio.get_running_loop()
        self._ensure_stream()
        self._tasks = [
            asyncio.create_task(self._flush_loop(), name="quote-flush"),
            asyncio.create_task(self._poll_loop(), name="quote-poll"),
            asyncio.create_task(self._status_loop(), name="quote-status"),
        ]

    async def stop(self):
        for t in self._tasks:
            t.cancel()
        self._stop_stream()

    # ── clients ──────────────────────────────────────────────

    def register(self, client: Client):
        self.clients[client.id] = client
        client.push(self.status())

    def unregister(self, client: Client):
        self.clients.pop(client.id, None)
        self._sync_stream()

    def set_subs(self, client: Client, syms: Iterable[str]):
        clean = []
        for s in syms:
            s = S.normalize(str(s))
            if S.is_valid(s):
                clean.append(s)
        client.subs = set(clean[:MAX_SUBS_PER_CLIENT])
        # Send what we already have right away; fetch the rest promptly
        known = [_compact(self.snap[s]) for s in client.subs if s in self.snap]
        if known:
            client.push({"t": "quotes", "d": known})
        missing = [s for s in client.subs if s not in self.snap]
        if missing:
            task = asyncio.create_task(self._prime(missing))
            self._bg.add(task)
            task.add_done_callback(self._bg.discard)
        self._sync_stream()

    def broadcast(self, msg: Dict):
        for c in list(self.clients.values()):
            c.push(msg)

    def all_symbols(self) -> Set[str]:
        syms = {s for s, _ in market.TAPE}
        for c in self.clients.values():
            syms |= c.subs
        return syms

    def get(self, sym: str) -> Optional[Dict]:
        return self.snap.get(sym)

    # ── streaming ────────────────────────────────────────────

    def _ensure_stream(self):
        """Start, stop or restart the websocket stream to match current keys."""
        mode = settings.stream_mode()
        fk, ak = settings.finnhub_key(), settings.alpaca_keys()
        want, key = "poll", None
        if mode in ("auto", "finnhub") and fk:
            want, key = "finnhub", fk
        elif mode in ("auto", "alpaca") and ak:
            want, key = "alpaca", ak
        if want == self._stream_kind and key == self._stream_key:
            return
        self._stop_stream()
        if want == "poll":
            return
        try:
            from stock_oracle import realtime
            if want == "finnhub":
                sink = _TickSink(self, S.from_finnhub_stream)
                self._stream = realtime.FinnhubRealtime(api_key=key, buffer=sink)
                self._to_stream, self._stream_max = S.to_finnhub_stream, 50
            else:
                import stock_oracle.config as cfg
                sink = _TickSink(self, S.from_alpaca)
                use_sip = settings.get("ALPACA_USE_SIP", "0").lower() in ("1", "true") or cfg.ALPACA_USE_SIP
                self._stream = realtime.AlpacaRealtime(key[0], key[1], buffer=sink,
                                                       use_sip=use_sip, channels=("trades",))
                self._to_stream = S.to_alpaca
                self._stream_max = 500 if use_sip else 30
            self._stream_kind, self._stream_key = want, key
            self._stream_syms = set()
            self._stream.start()
            self._sync_stream()
            logger.info(f"terminal: live stream via {want}")
        except Exception as e:
            logger.error(f"terminal: could not start {want} stream: {e}")
            self._stream, self._stream_kind, self._stream_key = None, "poll", None

    def _stop_stream(self):
        if self._stream:
            try:
                self._stream.stop()
            except Exception:
                pass
        self._stream, self._stream_kind, self._stream_key = None, "poll", None
        self._stream_syms = set()

    def _sync_stream(self):
        if not self._stream:
            return
        # Most-watched symbols first when we have more than the stream allows
        counts = Counter()
        for c in self.clients.values():
            counts.update(c.subs)
        for s, _ in market.TAPE:
            counts[s] += 0
        ordered = [s for s, _ in counts.most_common() if self._to_stream(s)]
        desired = set(ordered[: self._stream_max])
        add, drop = desired - self._stream_syms, self._stream_syms - desired
        try:
            if drop:
                self._stream.unsubscribe([self._to_stream(s) for s in drop])
            if add:
                self._stream.subscribe([self._to_stream(s) for s in add])
        except Exception as e:
            logger.debug(f"stream resubscribe failed: {e}")
        self._stream_syms = desired

    def stream_connected(self) -> bool:
        return bool(self._stream and getattr(self._stream, "connected", False))

    def on_tick_threadsafe(self, sym: str, tick):
        loop = self._loop
        if loop is None or loop.is_closed():
            return
        loop.call_soon_threadsafe(self._apply_tick, sym, float(tick.price), int(tick.volume or 0),
                                  float(tick.timestamp or time.time()), float(tick.bid or 0),
                                  float(tick.ask or 0), tick.source)

    def _apply_tick(self, sym, price, vol, ts, bid, ask, source):
        now = time.time()
        self._tick_times.append(now)
        q = self.snap.get(sym)
        if q is None:
            q = {"symbol": sym, "price": None, "prev_close": None, "open": None,
                 "high": None, "low": None, "volume": None, "change": None,
                 "change_pct": None, "ext_price": None, "ext_change_pct": None,
                 "ext_session": None}
            self.snap[sym] = q
        if bid and ask:  # quote update, not a trade
            q["bid"], q["ask"] = bid, ask
        elif price > 0:
            sess = self._session.get("session", "regular")
            if S.is_equity(sym) and sess in ("pre_market", "after_hours"):
                base = q.get("price")
                q["ext_price"] = price
                q["ext_session"] = "pre" if sess == "pre_market" else "post"
                q["ext_change_pct"] = ((price - base) / base * 100) if base else None
            else:
                q["price"] = price
                if q.get("high") is not None and price > q["high"]:
                    q["high"] = price
                if q.get("low") is not None and price < q["low"]:
                    q["low"] = price
                if q.get("volume") is not None:
                    q["volume"] += vol
                _recompute(q)
            q["live_at"] = now
        q["updated"] = ts
        q["src"] = source
        self._dirty.add(sym)

    # ── polling ──────────────────────────────────────────────

    def _session_now(self) -> Dict:
        if time.time() - self._session_at > 20:
            self._session = market_session()
            self._session_at = time.time()
        return self._session

    def _poll_interval(self) -> float:
        sess = self._session_now().get("session", "regular")
        if sess == "closed":
            return 120.0
        if self.stream_connected():
            return 60.0
        return float(settings.poll_seconds())

    async def _prime(self, syms: List[str]):
        """First quote for newly watched symbols: Finnhub REST if it covers them
        (real-time, cheap), otherwise wake the Yahoo poll."""
        need_poll = False
        for s in syms[:10]:
            q = await self.router.quote_realtime(s)
            if q and s not in self.snap:
                self.snap[s] = q
                self._dirty.add(s)
            elif s not in self.snap:
                need_poll = True
        if need_poll or len(syms) > 10:
            self._wake.set()

    async def _poll_loop(self):
        while True:
            try:
                await self.poll_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.last_poll_error = str(e)
                logger.debug(f"quote poll failed: {e}")
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self._poll_interval())
            except asyncio.TimeoutError:
                pass
            self._wake.clear()

    async def poll_once(self):
        syms = sorted(self.all_symbols())
        if not syms:
            return
        sess = self._session_now().get("session", "regular")
        fresh: Dict[str, Dict] = {}
        for i in range(0, len(syms), 80):
            fresh.update(await self.router.quotes(syms[i:i + 80], sess))
        now = time.time()
        for sym, nq in fresh.items():
            old = self.snap.get(sym)
            if old and old.get("live_at", 0) > now - LIVE_PRICE_GRACE:
                # A streamed trade is fresher than the poll; keep its price,
                # take day stats from the poll.
                nq["price"] = old.get("price") if old.get("price") is not None else nq["price"]
                if old.get("ext_price") is not None:
                    nq["ext_price"] = old["ext_price"]
                    nq["ext_change_pct"] = old.get("ext_change_pct")
                    nq["ext_session"] = old.get("ext_session")
                nq["live_at"] = old["live_at"]
                nq["src"] = old.get("src", nq.get("src"))
                _recompute(nq)
            if old:
                for k in ("bid", "ask"):
                    if k in old:
                        nq[k] = old[k]
            if old is None or not _same(old, nq):
                self.snap[sym] = nq
                self._dirty.add(sym)
        self.last_poll_at = now
        self.last_poll_error = ""

    # ── fan-out ──────────────────────────────────────────────

    async def _flush_loop(self):
        while True:
            await asyncio.sleep(FLUSH_SECONDS)
            if not self._dirty:
                continue
            dirty, self._dirty = self._dirty, set()
            for c in list(self.clients.values()):
                rows = [_compact(self.snap[s]) for s in dirty & c.subs if s in self.snap]
                if rows:
                    c.push({"t": "quotes", "d": rows})

    def status(self) -> Dict:
        sess = self._session_now()
        now = time.time()
        recent = sum(1 for t in self._tick_times if t > now - 10)
        return {
            "t": "status",
            "session": sess.get("session"),
            "session_detail": sess.get("detail", ""),
            "stream": self._stream_kind,
            "connected": self.stream_connected(),
            "streaming": len(self._stream_syms),
            "stream_max": self._stream_max,
            "tps": round(recent / 10, 1),
            "poll_seconds": self._poll_interval(),
            "last_poll": self.last_poll_at,
            "poll_error": self.last_poll_error,
            "keys": settings.key_status(),
            "clients": len(self.clients),
            "time": now,
        }

    async def _status_loop(self):
        while True:
            await asyncio.sleep(5)
            try:
                self._ensure_stream()
            except Exception as e:
                logger.debug(f"stream check failed: {e}")
            self.broadcast(self.status())
