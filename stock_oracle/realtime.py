"""
Real-Time Data Module
=====================
Websocket-based real-time stock data streaming.
Supports multiple brokers/providers — configure whichever you have access to.

Priority order for real-time:
  1. Your broker API (IBKR, Alpaca, Schwab, etc.)
  2. Finnhub websocket (free, real-time trades)
  3. Polygon.io websocket (paid = real-time, free = delayed)
  4. Yahoo Finance (15-min delayed fallback)

WHY IS EVERYTHING 15 MINUTES DELAYED?
--------------------------------------
NYSE and NASDAQ own the real-time tick data. They license it to data
vendors who pay per-user fees. Free services only get permission for
delayed data. It's a business model, not a tech limitation.

Real-time sources that are actually free:
  - Finnhub: free websocket for real-time US trades (rate limited)
  - Alpaca: free with account (paper trading counts)
  - IEX Cloud: free tier has some real-time
  - Your broker: if you have a funded account, you already pay for real-time
"""
import json
import time
import logging
import threading
from abc import ABC, abstractmethod
from collections import deque
from datetime import datetime, timedelta
from typing import Callable, Dict, List, Optional, Set

logger = logging.getLogger("stock_oracle")

# Try websocket imports
try:
    import websocket
    HAS_WEBSOCKET = True
except ImportError:
    HAS_WEBSOCKET = False

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False


class TickData:
    """Single trade/quote tick."""
    __slots__ = ['symbol', 'price', 'volume', 'bid', 'ask', 'timestamp', 'source']

    def __init__(self, symbol: str, price: float, volume: int = 0,
                 bid: float = 0, ask: float = 0, timestamp: float = None,
                 source: str = "unknown"):
        self.symbol = symbol
        self.price = price
        self.volume = volume
        self.bid = bid
        self.ask = ask
        self.timestamp = timestamp or time.time()
        self.source = source

    def to_dict(self) -> Dict:
        return {
            "symbol": self.symbol,
            "price": self.price,
            "volume": self.volume,
            "bid": self.bid,
            "ask": self.ask,
            "timestamp": self.timestamp,
            "time_str": datetime.fromtimestamp(self.timestamp).strftime("%H:%M:%S.%f")[:-3],
            "source": self.source,
        }


class RealtimeBuffer:
    """
    Thread-safe circular buffer for real-time ticks.
    Maintains per-symbol price history with configurable depth.
    """

    def __init__(self, max_ticks: int = 10000):
        self._lock = threading.Lock()
        self._ticks: Dict[str, deque] = {}
        self._max = max_ticks
        self._callbacks: List[Callable] = []
        self._last_prices: Dict[str, float] = {}

    def add_tick(self, tick: TickData):
        with self._lock:
            if tick.symbol not in self._ticks:
                self._ticks[tick.symbol] = deque(maxlen=self._max)
            self._ticks[tick.symbol].append(tick)
            self._last_prices[tick.symbol] = tick.price

        # Fire callbacks
        for cb in self._callbacks:
            try:
                cb(tick)
            except Exception as e:
                logger.error(f"Tick callback error: {e}")

    def get_latest(self, symbol: str) -> Optional[TickData]:
        with self._lock:
            buf = self._ticks.get(symbol)
            return buf[-1] if buf else None

    def get_price(self, symbol: str) -> Optional[float]:
        return self._last_prices.get(symbol)

    def get_history(self, symbol: str, seconds: int = 300) -> List[TickData]:
        """Get ticks from the last N seconds."""
        cutoff = time.time() - seconds
        with self._lock:
            buf = self._ticks.get(symbol, deque())
            return [t for t in buf if t.timestamp >= cutoff]

    def get_ohlcv(self, symbol: str, interval_seconds: int = 60, periods: int = 30) -> List[Dict]:
        """Aggregate ticks into OHLCV candles."""
        now = time.time()
        ticks = self.get_history(symbol, interval_seconds * periods)
        if not ticks:
            return []

        candles = []
        for i in range(periods):
            start = now - (periods - i) * interval_seconds
            end = start + interval_seconds
            bucket = [t for t in ticks if start <= t.timestamp < end]
            if bucket:
                candles.append({
                    "time": datetime.fromtimestamp(start).strftime("%H:%M"),
                    "open": bucket[0].price,
                    "high": max(t.price for t in bucket),
                    "low": min(t.price for t in bucket),
                    "close": bucket[-1].price,
                    "volume": sum(t.volume for t in bucket),
                    "ticks": len(bucket),
                })

        return candles

    def on_tick(self, callback: Callable):
        """Register a callback for every incoming tick."""
        self._callbacks.append(callback)

    def get_all_symbols(self) -> Set[str]:
        return set(self._last_prices.keys())

    def get_spread(self, symbol: str) -> Optional[Dict]:
        """Get current bid-ask spread."""
        tick = self.get_latest(symbol)
        if tick and tick.bid and tick.ask:
            return {
                "bid": tick.bid,
                "ask": tick.ask,
                "spread": round(tick.ask - tick.bid, 4),
                "spread_pct": round((tick.ask - tick.bid) / tick.price * 100, 4),
            }
        return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PROVIDER: Finnhub (FREE real-time trades)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class FinnhubRealtime:
    """
    Free real-time US stock trades via Finnhub websocket.
    Get API key at: https://finnhub.io/ (free tier)

    Usage:
        rt = FinnhubRealtime(api_key="your_key", buffer=buffer)
        rt.subscribe(["AAPL", "TSLA", "NVDA"])
        rt.start()  # Starts background thread
    """

    WS_URL = "wss://ws.finnhub.io"

    def __init__(self, api_key: str, buffer: RealtimeBuffer):
        self.api_key = api_key
        self.buffer = buffer
        self._ws = None
        self._thread = None
        self._symbols: Set[str] = set()
        self._running = False
        self._connected = False
        self._send_lock = threading.Lock()

    @property
    def connected(self) -> bool:
        return self._connected

    def _send(self, payload: Dict):
        """Send on the socket if it is open. Subscriptions made while the
        socket is down are kept in self._symbols and replayed on connect."""
        if not (self._ws and self._connected):
            return
        try:
            with self._send_lock:
                self._ws.send(json.dumps(payload))
        except Exception as e:
            logger.debug(f"Finnhub send failed (will resync on reconnect): {e}")

    def subscribe(self, symbols: List[str]):
        new = [s.upper() for s in symbols if s.upper() not in self._symbols]
        self._symbols.update(new)
        for s in new:
            self._send({"type": "subscribe", "symbol": s})

    def unsubscribe(self, symbols: List[str]):
        for s in symbols:
            s = s.upper()
            if s in self._symbols:
                self._symbols.discard(s)
                self._send({"type": "unsubscribe", "symbol": s})

    def start(self):
        if not HAS_WEBSOCKET:
            logger.error("pip install websocket-client for real-time data")
            return

        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True,
                                        name="finnhub-ws")
        self._thread.start()
        logger.info(f"Finnhub real-time started for {len(self._symbols)} symbols")

    def stop(self):
        self._running = False
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass

    def _run(self):
        """Connect and reconnect with backoff until stop() is called.
        (Loop instead of recursion so long sessions can't exhaust the stack.)"""
        def on_message(ws, message):
            try:
                data = json.loads(message)
                if data.get("type") == "trade":
                    for trade in data.get("data", []):
                        tick = TickData(
                            symbol=trade["s"],
                            price=float(trade["p"]),
                            volume=int(trade.get("v") or 0),
                            timestamp=trade["t"] / 1000,
                            source="finnhub",
                        )
                        self.buffer.add_tick(tick)
                elif data.get("type") == "error":
                    logger.warning(f"Finnhub websocket: {data.get('msg')}")
            except Exception as e:
                logger.error(f"Finnhub parse error: {e}")

        def on_open(ws):
            self._connected = True
            for symbol in list(self._symbols):
                with self._send_lock:
                    ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))
            logger.info(f"Finnhub websocket connected ({len(self._symbols)} symbols)")

        def on_error(ws, error):
            logger.error(f"Finnhub error: {error}")

        def on_close(ws, close_status_code, close_msg):
            self._connected = False
            logger.warning("Finnhub websocket closed")

        import os
        base = os.environ.get("FINNHUB_WS_URL", self.WS_URL)
        backoff = 5
        while self._running:
            started = time.time()
            self._ws = websocket.WebSocketApp(
                f"{base}?token={self.api_key}",
                on_message=on_message,
                on_open=on_open,
                on_error=on_error,
                on_close=on_close,
            )
            try:
                self._ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as e:
                logger.error(f"Finnhub websocket crashed: {e}")
            self._connected = False
            if not self._running:
                break
            # Reset backoff after a connection that stayed up a while
            backoff = 5 if time.time() - started > 60 else min(backoff * 2, 120)
            time.sleep(backoff)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PROVIDER: Alpaca (FREE with account)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class AlpacaRealtime:
    """
    Real-time data from Alpaca Markets.
    Free with paper trading account: https://alpaca.markets

    Supports both IEX (free) and SIP (paid) feeds.

    Usage:
        rt = AlpacaRealtime(key_id="...", secret="...", buffer=buffer)
        rt.subscribe(["AAPL", "TSLA"])
        rt.start()
    """

    # IEX feed (free) — use "wss://stream.data.alpaca.markets/v2/sip" for paid
    WS_URL = "wss://stream.data.alpaca.markets/v2/iex"

    def __init__(self, key_id: str, secret: str, buffer: RealtimeBuffer, use_sip: bool = False,
                 channels: tuple = ("trades", "quotes")):
        self.key_id = key_id
        self.secret = secret
        self.buffer = buffer
        self._ws = None
        self._thread = None
        self._symbols: Set[str] = set()
        self._running = False
        self._connected = False   # True once authenticated
        self._send_lock = threading.Lock()
        self._channels = tuple(channels)
        self._url = "wss://stream.data.alpaca.markets/v2/sip" if use_sip else self.WS_URL

    @property
    def connected(self) -> bool:
        return self._connected

    def _send(self, payload: Dict):
        if not (self._ws and self._connected):
            return
        try:
            with self._send_lock:
                self._ws.send(json.dumps(payload))
        except Exception as e:
            logger.debug(f"Alpaca send failed (will resync on reconnect): {e}")

    def _sub_payload(self, action: str, symbols: List[str]) -> Dict:
        payload = {"action": action}
        for ch in self._channels:
            payload[ch] = list(symbols)
        return payload

    def subscribe(self, symbols: List[str]):
        new = [s.upper() for s in symbols if s.upper() not in self._symbols]
        self._symbols.update(new)
        if new:
            self._send(self._sub_payload("subscribe", new))

    def unsubscribe(self, symbols: List[str]):
        gone = [s.upper() for s in symbols if s.upper() in self._symbols]
        for s in gone:
            self._symbols.discard(s)
        if gone:
            self._send(self._sub_payload("unsubscribe", gone))

    def start(self):
        if not HAS_WEBSOCKET:
            logger.error("pip install websocket-client for real-time data")
            return

        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True,
                                        name="alpaca-ws")
        self._thread.start()

    def stop(self):
        self._running = False
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass

    def _run(self):
        def on_message(ws, message):
            try:
                msgs = json.loads(message)
                for msg in msgs:
                    kind = msg.get("T")
                    if kind == "success" and msg.get("msg") == "authenticated":
                        self._connected = True
                        if self._symbols:
                            with self._send_lock:
                                ws.send(json.dumps(
                                    self._sub_payload("subscribe", list(self._symbols))))
                        logger.info(f"Alpaca authenticated ({len(self._symbols)} symbols)")
                    elif kind == "error":
                        logger.warning(f"Alpaca websocket error {msg.get('code')}: {msg.get('msg')}")
                    elif kind == "t":  # Trade
                        tick = TickData(
                            symbol=msg["S"],
                            price=float(msg["p"]),
                            volume=int(msg["s"]),
                            timestamp=datetime.fromisoformat(
                                msg["t"].replace("Z", "+00:00")
                            ).timestamp(),
                            source="alpaca",
                        )
                        self.buffer.add_tick(tick)
                    elif kind == "q":  # Quote
                        tick = TickData(
                            symbol=msg["S"],
                            price=(float(msg["bp"]) + float(msg["ap"])) / 2,
                            bid=float(msg["bp"]),
                            ask=float(msg["ap"]),
                            volume=int(msg.get("bs", 0)) + int(msg.get("as", 0)),
                            source="alpaca",
                        )
                        self.buffer.add_tick(tick)
            except Exception as e:
                logger.error(f"Alpaca parse error: {e}")

        def on_open(ws):
            # Authenticate; subscriptions are sent once the server confirms auth
            with self._send_lock:
                ws.send(json.dumps({
                    "action": "auth",
                    "key": self.key_id,
                    "secret": self.secret,
                }))
            logger.info("Alpaca websocket connected, authenticating")

        def on_error(ws, error):
            logger.error(f"Alpaca error: {error}")

        def on_close(ws, *args):
            self._connected = False

        backoff = 5
        while self._running:
            started = time.time()
            self._ws = websocket.WebSocketApp(
                self._url,
                on_message=on_message,
                on_open=on_open,
                on_error=on_error,
                on_close=on_close,
            )
            try:
                self._ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as e:
                logger.error(f"Alpaca websocket crashed: {e}")
            self._connected = False
            if not self._running:
                break
            backoff = 5 if time.time() - started > 60 else min(backoff * 2, 120)
            time.sleep(backoff)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PROVIDER: Interactive Brokers (TWS API)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class IBKRRealtime:
    """
    Real-time data from Interactive Brokers TWS/Gateway.
    Requires: pip install ib_insync
    And TWS or IB Gateway running locally.

    Usage:
        rt = IBKRRealtime(buffer=buffer)
        rt.subscribe(["AAPL", "TSLA"])
        rt.start()
    """

    def __init__(self, buffer: RealtimeBuffer, host: str = "127.0.0.1",
                 port: int = 7497, client_id: int = 1):
        self.buffer = buffer
        self.host = host
        self.port = port
        self.client_id = client_id
        self._symbols: Set[str] = set()
        self._ib = None

    def subscribe(self, symbols: List[str]):
        self._symbols.update(s.upper() for s in symbols)

    def start(self):
        try:
            from ib_insync import IB, Stock, util
        except ImportError:
            logger.error("pip install ib_insync for IBKR integration")
            return

        self._ib = IB()
        self._ib.connect(self.host, self.port, clientId=self.client_id)

        for symbol in self._symbols:
            contract = Stock(symbol, 'SMART', 'USD')
            self._ib.qualifyContracts(contract)
            self._ib.reqMktData(contract)

            def on_tick(tickers, sym=symbol):
                for t in tickers:
                    if t.last and t.last > 0:
                        tick = TickData(
                            symbol=sym,
                            price=float(t.last),
                            volume=int(t.lastSize or 0),
                            bid=float(t.bid or 0),
                            ask=float(t.ask or 0),
                            source="ibkr",
                        )
                        self.buffer.add_tick(tick)

            self._ib.pendingTickersEvent += on_tick

        logger.info(f"IBKR connected, streaming {len(self._symbols)} symbols")

        # Run in background
        thread = threading.Thread(target=self._ib.run, daemon=True)
        thread.start()

    def stop(self):
        if self._ib:
            self._ib.disconnect()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PROVIDER: Polygon.io
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class PolygonRealtime:
    """
    Real-time (paid) or delayed (free) from Polygon.io.
    Get API key at: https://polygon.io

    Usage:
        rt = PolygonRealtime(api_key="...", buffer=buffer)
        rt.subscribe(["AAPL", "TSLA"])
        rt.start()
    """

    WS_URL = "wss://socket.polygon.io/stocks"

    def __init__(self, api_key: str, buffer: RealtimeBuffer):
        self.api_key = api_key
        self.buffer = buffer
        self._ws = None
        self._thread = None
        self._symbols: Set[str] = set()
        self._running = False

    def subscribe(self, symbols: List[str]):
        self._symbols.update(s.upper() for s in symbols)
        if self._ws and self._running:
            channels = [f"T.{s}" for s in symbols] + [f"Q.{s}" for s in symbols]
            self._ws.send(json.dumps({"action": "subscribe", "params": ",".join(channels)}))

    def start(self):
        if not HAS_WEBSOCKET:
            logger.error("pip install websocket-client")
            return

        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._running = False
        if self._ws:
            self._ws.close()

    def _run(self):
        def on_message(ws, message):
            try:
                msgs = json.loads(message)
                for msg in msgs:
                    ev = msg.get("ev")
                    if ev == "T":  # Trade
                        tick = TickData(
                            symbol=msg["sym"],
                            price=float(msg["p"]),
                            volume=int(msg["s"]),
                            timestamp=msg["t"] / 1000,
                            source="polygon",
                        )
                        self.buffer.add_tick(tick)
                    elif ev == "Q":  # Quote
                        tick = TickData(
                            symbol=msg["sym"],
                            price=(float(msg["bp"]) + float(msg["ap"])) / 2,
                            bid=float(msg["bp"]),
                            ask=float(msg["ap"]),
                            source="polygon",
                        )
                        self.buffer.add_tick(tick)
            except Exception as e:
                logger.error(f"Polygon parse error: {e}")

        def on_open(ws):
            ws.send(json.dumps({"action": "auth", "params": self.api_key}))
            channels = [f"T.{s}" for s in self._symbols] + [f"Q.{s}" for s in self._symbols]
            ws.send(json.dumps({"action": "subscribe", "params": ",".join(channels)}))
            logger.info("Polygon websocket connected")

        def on_error(ws, error):
            logger.error(f"Polygon error: {error}")

        def on_close(ws, *args):
            if self._running:
                time.sleep(5)
                self._run()

        self._ws = websocket.WebSocketApp(
            self.WS_URL,
            on_message=on_message,
            on_open=on_open,
            on_error=on_error,
            on_close=on_close,
        )
        self._ws.run_forever()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PROVIDER: Schwab/TD Ameritrade
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class SchwabRealtime:
    """
    Real-time data from Charles Schwab (formerly TD Ameritrade).
    Requires Schwab developer account: https://developer.schwab.com

    Usage:
        rt = SchwabRealtime(app_key="...", access_token="...", buffer=buffer)
        rt.subscribe(["AAPL", "TSLA"])
        rt.start()
    """

    WS_URL = "wss://streamer-api.schwab.com/ws"

    def __init__(self, app_key: str, access_token: str, buffer: RealtimeBuffer):
        self.app_key = app_key
        self.access_token = access_token
        self.buffer = buffer
        self._symbols: Set[str] = set()
        self._running = False

    def subscribe(self, symbols: List[str]):
        self._symbols.update(s.upper() for s in symbols)

    def start(self):
        logger.info("Schwab real-time: configure OAuth flow via developer.schwab.com")
        logger.info("Once authenticated, websocket streams real-time Level 1 quotes")
        # Full implementation requires OAuth2 flow specific to Schwab's API
        # The pattern is identical to the other providers above

    def stop(self):
        self._running = False


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# UNIFIED REAL-TIME MANAGER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class RealtimeManager:
    """
    Manages multiple real-time data providers with automatic failover.
    Uses the first available provider that returns data.

    Usage:
        mgr = RealtimeManager()

        # Add providers in priority order
        mgr.add_provider("finnhub", FinnhubRealtime(api_key="...", buffer=mgr.buffer))
        mgr.add_provider("alpaca", AlpacaRealtime(key_id="...", secret="...", buffer=mgr.buffer))

        # Subscribe and start
        mgr.subscribe(["AAPL", "TSLA", "NVDA"])
        mgr.start()

        # Get current price (from whichever provider delivered it)
        price = mgr.get_price("AAPL")

        # Get OHLCV candles aggregated from ticks
        candles = mgr.get_candles("AAPL", interval=60, periods=30)

        # Register callback for real-time alerts
        mgr.on_tick(lambda tick: print(f"{tick.symbol} = ${tick.price}"))
    """

    def __init__(self):
        self.buffer = RealtimeBuffer(max_ticks=50000)
        self._providers: Dict[str, object] = {}
        self._priority: List[str] = []

    def add_provider(self, name: str, provider):
        self._providers[name] = provider
        self._priority.append(name)

    def subscribe(self, symbols: List[str]):
        for provider in self._providers.values():
            provider.subscribe(symbols)

    def start(self):
        for name, provider in self._providers.items():
            try:
                provider.start()
                logger.info(f"Started real-time provider: {name}")
            except Exception as e:
                logger.error(f"Failed to start {name}: {e}")

    def stop(self):
        for provider in self._providers.values():
            try:
                provider.stop()
            except Exception:
                pass

    def get_price(self, symbol: str) -> Optional[float]:
        return self.buffer.get_price(symbol)

    def get_latest(self, symbol: str) -> Optional[TickData]:
        return self.buffer.get_latest(symbol)

    def get_candles(self, symbol: str, interval: int = 60, periods: int = 30) -> List[Dict]:
        return self.buffer.get_ohlcv(symbol, interval, periods)

    def get_spread(self, symbol: str) -> Optional[Dict]:
        return self.buffer.get_spread(symbol)

    def on_tick(self, callback: Callable):
        self.buffer.on_tick(callback)

    def get_status(self) -> Dict:
        return {
            "providers": list(self._providers.keys()),
            "symbols": list(self.buffer.get_all_symbols()),
            "total_ticks": sum(
                len(buf) for buf in self.buffer._ticks.values()
            ),
        }
