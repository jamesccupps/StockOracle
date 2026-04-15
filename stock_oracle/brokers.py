"""
Broker Connectors: Webull + Robinhood
======================================
Real-time data and trading integration for James's brokerage accounts.

WEBULL (Primary — Official API)
  - Real-time quotes via MQTT (true real-time, not 15-min delayed)
  - Snapshots, candlestick history, instrument lookup via HTTP
  - Full trading: place/modify/cancel orders
  - Apply at: Webull website > Account Center > API Management
  - SDK: pip install webull-sdk  (or webullsdkmdata + webullsdkcore)

ROBINHOOD (Secondary — Unofficial via robin_stocks)
  - Real-time quotes via polling (not websocket, but fast)
  - Portfolio, positions, order history
  - Full trading: stocks, options, crypto
  - Library: pip install robin-stocks
  - Requires 2FA/TOTP setup for automated login

STRATEGY:
  Use Webull MQTT for real-time price streaming (true tick data).
  Use Robinhood for portfolio monitoring + trade execution on RH account.
  Both feed into the RealtimeBuffer for the Oracle to consume.
"""
import json
import time
import logging
import threading
from datetime import datetime
from typing import Dict, List, Optional, Set

from stock_oracle.realtime import TickData, RealtimeBuffer

logger = logging.getLogger("stock_oracle")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# WEBULL — Official OpenAPI (MQTT real-time)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class WebullConnector:
    """
    Real-time market data and trading via Webull's official OpenAPI.

    Setup:
      1. Log into Webull website
      2. Go to Account Center > API Management > My Application
      3. Apply for API access (1-3 day review)
      4. Create an App to get app_key + app_secret
      5. pip install webull-sdk

    Usage:
        wb = WebullConnector(
            app_key="your_app_key",
            app_secret="your_app_secret",
            buffer=realtime_buffer,
        )
        wb.subscribe(["AAPL", "TSLA", "NVDA"])
        wb.start_streaming()

        # Get a snapshot
        snap = wb.get_snapshot("AAPL")

        # Get candlestick history
        bars = wb.get_bars("AAPL", timespan="M5", count=100)

        # Place a trade
        wb.place_order("AAPL", side="BUY", qty=10, order_type="MARKET")
    """

    API_ENDPOINT = "https://api.webull.com"

    def __init__(self, app_key: str, app_secret: str, buffer: RealtimeBuffer,
                 region: str = "us"):
        self.app_key = app_key
        self.app_secret = app_secret
        self.buffer = buffer
        self.region = region
        self._mqtt_client = None
        self._api_client = None
        self._data_client = None
        self._trade_client = None
        self._symbols: Set[str] = set()
        self._running = False

        self._init_clients()

    def _init_clients(self):
        """Initialize Webull SDK clients."""
        try:
            from webullsdkcore.client import ApiClient
            from webullsdkmdata.quotes.subscribe.default_client import DefaultQuotesClient

            self._api_client = ApiClient(self.app_key, self.app_secret, self.region)
            logger.info("Webull API client initialized")

        except ImportError:
            logger.warning(
                "Webull SDK not installed. Install with:\n"
                "  pip install webull-sdk\n"
                "Or manually:\n"
                "  pip install webullsdkcore webullsdkmdata webullsdktrade"
            )

    # ── Real-time Streaming (MQTT) ─────────────────────────────

    def subscribe(self, symbols: List[str]):
        """Add symbols to the real-time subscription."""
        self._symbols.update(s.upper() for s in symbols)

    def start_streaming(self):
        """Start MQTT real-time data streaming."""
        try:
            from webullsdkmdata.common.category import Category
            from webullsdkmdata.common.subscribe_type import SubscribeType
            from webullsdkmdata.quotes.subscribe.default_client import DefaultQuotesClient
        except ImportError:
            logger.error("webullsdkmdata not installed")
            return

        def on_message(client, userdata, message):
            """Handle incoming MQTT messages."""
            try:
                payload = json.loads(message.payload)
                symbol = payload.get("symbol", "")
                price = float(payload.get("price", 0) or payload.get("close", 0) or 0)
                volume = int(payload.get("volume", 0) or 0)
                bid = float(payload.get("bid", 0) or 0)
                ask = float(payload.get("ask", 0) or 0)

                if price > 0:
                    tick = TickData(
                        symbol=symbol,
                        price=price,
                        volume=volume,
                        bid=bid,
                        ask=ask,
                        source="webull",
                    )
                    self.buffer.add_tick(tick)
            except Exception as e:
                logger.error(f"Webull MQTT parse error: {e}")

        self._running = True

        for symbol in self._symbols:
            try:
                client = DefaultQuotesClient(
                    self.app_key, self.app_secret, self.region
                )
                client.init_default_settings(
                    symbol,
                    Category.US_STOCK.name,
                    SubscribeType.SNAPSHOT.name,
                )
                client.on_message = on_message

                thread = threading.Thread(
                    target=client.connect_and_loop_forever,
                    daemon=True,
                    name=f"webull-{symbol}",
                )
                thread.start()
                logger.info(f"Webull MQTT streaming: {symbol}")
            except Exception as e:
                logger.error(f"Webull stream error for {symbol}: {e}")

    def stop_streaming(self):
        self._running = False

    # ── HTTP API: Snapshots & History ──────────────────────────

    def get_snapshot(self, symbol: str) -> Optional[Dict]:
        """
        Get current real-time snapshot for a symbol.
        Returns: price, bid, ask, volume, high, low, open, close, change.
        """
        try:
            from webullsdkcore.client import ApiClient
            from webullsdkmdata.common.category import Category
            from webullsdkmdata.data_client import DataClient

            data_client = DataClient(self._api_client)
            resp = data_client.market_data.get_snapshot(
                symbol,
                Category.US_STOCK.name,
                extend_hour_required=True,
                overnight_required=True,
            )
            if resp.status_code == 200:
                data = resp.json()
                # Also feed into our buffer
                if data.get("close") or data.get("price"):
                    tick = TickData(
                        symbol=symbol,
                        price=float(data.get("close") or data.get("price", 0)),
                        volume=int(data.get("volume", 0)),
                        bid=float(data.get("bid", 0)),
                        ask=float(data.get("ask", 0)),
                        source="webull_snapshot",
                    )
                    self.buffer.add_tick(tick)
                return data
        except Exception as e:
            logger.error(f"Webull snapshot error: {e}")
        return None

    def get_bars(self, symbol: str, timespan: str = "M5", count: int = 100) -> List[Dict]:
        """
        Get historical candlestick bars.
        Timespans: M1 (1min), M5, M15, M30, H1, H2, H4, D1, W1, MN1
        """
        try:
            from webullsdkmdata.common.category import Category
            from webullsdkmdata.common.timespan import Timespan
            from webullsdkmdata.data_client import DataClient

            data_client = DataClient(self._api_client)
            resp = data_client.market_data.get_history_bars(
                symbol, Category.US_STOCK.name, timespan, count
            )
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            logger.error(f"Webull bars error: {e}")
        return []

    # ── Trading ────────────────────────────────────────────────

    def get_account_info(self) -> Optional[Dict]:
        """Get Webull account balance and positions."""
        try:
            from webullsdktrade.trade_client import TradeClient
            trade_client = TradeClient(self._api_client)
            accounts = trade_client.account_v2.get_account_list()
            if accounts.status_code == 200:
                return accounts.json()
        except Exception as e:
            logger.error(f"Webull account error: {e}")
        return None

    def place_order(self, symbol: str, side: str = "BUY", qty: int = 1,
                    order_type: str = "MARKET", limit_price: float = None) -> Optional[Dict]:
        """
        Place a trade on Webull.

        Args:
            symbol: Stock ticker (e.g. "AAPL")
            side: "BUY" or "SELL"
            qty: Number of shares
            order_type: "MARKET", "LIMIT", "STOP", "STOP_LIMIT"
            limit_price: Required for LIMIT and STOP_LIMIT orders

        Returns:
            Order confirmation dict or None on failure

        WARNING: This places REAL orders with REAL money.
        Test with paper trading first!
        """
        logger.warning(f"⚠ PLACING {'PAPER ' if False else ''}ORDER: {side} {qty} {symbol} @ {order_type}")

        try:
            from webullsdktrade.trade_client import TradeClient
            import uuid

            trade_client = TradeClient(self._api_client)
            accounts = trade_client.account_v2.get_account_list()
            if accounts.status_code != 200:
                return None

            account_id = accounts.json()[0].get("accountId")
            client_order_id = str(uuid.uuid4())

            # Build order params based on type
            order_params = {
                "clientOrderId": client_order_id,
                "symbol": symbol,
                "side": side,
                "orderType": order_type,
                "qty": str(qty),
                "timeInForce": "DAY",
            }
            if limit_price and order_type in ("LIMIT", "STOP_LIMIT"):
                order_params["limitPrice"] = str(limit_price)

            resp = trade_client.order.place_order(account_id, order_params)
            if resp.status_code == 200:
                result = resp.json()
                logger.info(f"Order placed: {result}")
                return result
            else:
                logger.error(f"Order failed: {resp.status_code} {resp.text}")
        except Exception as e:
            logger.error(f"Webull order error: {e}")
        return None

    def get_status(self) -> Dict:
        return {
            "provider": "webull",
            "connected": self._api_client is not None,
            "streaming": self._running,
            "symbols": list(self._symbols),
            "type": "official_api",
        }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ROBINHOOD — Unofficial via robin_stocks
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class RobinhoodConnector:
    """
    Real-time quotes and trading via Robinhood (unofficial API).

    Setup:
      1. pip install robin-stocks pyotp
      2. Enable 2FA on Robinhood with an authenticator app
      3. Save the TOTP secret key for automated login

    Usage:
        rh = RobinhoodConnector(
            email="your@email.com",
            password="your_password",
            totp_secret="your_2fa_secret",  # Optional but recommended
            buffer=realtime_buffer,
        )
        rh.login()
        rh.subscribe(["AAPL", "TSLA"])
        rh.start_polling()

        # Get current price
        price = rh.get_price("AAPL")

        # Get portfolio
        portfolio = rh.get_portfolio()

        # Place a trade
        rh.place_order("AAPL", side="BUY", qty=1)

    NOTE: robin_stocks is unofficial. Robinhood can change their internal
    API at any time, which may temporarily break functionality.
    """

    def __init__(self, email: str, password: str, buffer: RealtimeBuffer,
                 totp_secret: str = None):
        self.email = email
        self.password = password
        self.totp_secret = totp_secret
        self.buffer = buffer
        self._logged_in = False
        self._symbols: Set[str] = set()
        self._running = False
        self._poll_interval = 5  # seconds between quote fetches
        self._rh = None

        self._import_robin()

    def _import_robin(self):
        try:
            import robin_stocks.robinhood as rh
            self._rh = rh
            logger.info("robin_stocks loaded")
        except ImportError:
            logger.warning(
                "robin_stocks not installed. Install with:\n"
                "  pip install robin-stocks pyotp"
            )

    # ── Authentication ─────────────────────────────────────────

    def login(self) -> bool:
        """
        Log into Robinhood.
        Supports 2FA via TOTP (recommended for automated use).
        """
        if not self._rh:
            return False

        try:
            if self.totp_secret:
                import pyotp
                totp = pyotp.TOTP(self.totp_secret).now()
                login = self._rh.login(
                    self.email, self.password,
                    mfa_code=totp,
                    store_session=True,
                )
            else:
                login = self._rh.login(
                    self.email, self.password,
                    store_session=True,
                )

            self._logged_in = login is not None
            if self._logged_in:
                logger.info("Robinhood login successful")
            else:
                logger.error("Robinhood login failed")
            return self._logged_in

        except Exception as e:
            logger.error(f"Robinhood login error: {e}")
            return False

    def logout(self):
        if self._rh and self._logged_in:
            self._rh.logout()
            self._logged_in = False

    # ── Real-time Polling ──────────────────────────────────────

    def subscribe(self, symbols: List[str]):
        self._symbols.update(s.upper() for s in symbols)

    def start_polling(self, interval: float = 5.0):
        """
        Start polling Robinhood for real-time quotes.
        Robinhood doesn't offer websockets, so we poll at intervals.
        For most trading strategies, 5-second updates are sufficient.
        """
        self._poll_interval = interval
        self._running = True

        thread = threading.Thread(target=self._poll_loop, daemon=True, name="rh-poller")
        thread.start()
        logger.info(f"Robinhood polling started ({interval}s interval, {len(self._symbols)} symbols)")

    def stop_polling(self):
        self._running = False

    def _poll_loop(self):
        while self._running:
            try:
                if not self._logged_in:
                    time.sleep(10)
                    continue

                for symbol in list(self._symbols):
                    try:
                        prices = self._rh.get_latest_price(symbol)
                        if prices and prices[0]:
                            price = float(prices[0])
                            quote = self._rh.get_quotes(symbol)

                            bid = 0
                            ask = 0
                            volume = 0
                            if quote and len(quote) > 0 and quote[0]:
                                q = quote[0]
                                bid = float(q.get("bid_price", 0) or 0)
                                ask = float(q.get("ask_price", 0) or 0)
                                volume = int(float(q.get("last_trade_price_amount", 0) or 0))

                            tick = TickData(
                                symbol=symbol,
                                price=price,
                                volume=volume,
                                bid=bid,
                                ask=ask,
                                source="robinhood",
                            )
                            self.buffer.add_tick(tick)

                    except Exception as e:
                        logger.error(f"RH quote error for {symbol}: {e}")

                time.sleep(self._poll_interval)

            except Exception as e:
                logger.error(f"RH poll loop error: {e}")
                time.sleep(30)

    # ── Quotes & Data ──────────────────────────────────────────

    def get_price(self, symbol: str) -> Optional[float]:
        """Get current price for a symbol."""
        if not self._rh or not self._logged_in:
            return None
        try:
            prices = self._rh.get_latest_price(symbol)
            return float(prices[0]) if prices and prices[0] else None
        except Exception:
            return None

    def get_quote(self, symbol: str) -> Optional[Dict]:
        """Get full quote data."""
        if not self._rh:
            return None
        try:
            quotes = self._rh.get_quotes(symbol)
            return quotes[0] if quotes else None
        except Exception:
            return None

    def get_fundamentals(self, symbol: str) -> Optional[Dict]:
        """Get fundamental data (PE ratio, market cap, etc.)."""
        if not self._rh:
            return None
        try:
            return self._rh.get_fundamentals(symbol)[0]
        except Exception:
            return None

    def get_historicals(self, symbol: str, interval: str = "day",
                        span: str = "year") -> List[Dict]:
        """
        Get historical price data.
        interval: 5minute, 10minute, hour, day, week
        span: day, week, month, 3month, year, 5year
        """
        if not self._rh:
            return []
        try:
            return self._rh.get_stock_historicals(symbol, interval=interval, span=span) or []
        except Exception:
            return []

    # ── Portfolio & Account ────────────────────────────────────

    def get_portfolio(self) -> Optional[Dict]:
        """Get current portfolio summary."""
        if not self._rh or not self._logged_in:
            return None
        try:
            profile = self._rh.load_portfolio_profile()
            positions = self._rh.get_current_positions()

            holdings = []
            for pos in positions:
                try:
                    name = self._rh.get_name_by_url(pos.get("instrument", ""))
                    holdings.append({
                        "name": name,
                        "quantity": float(pos.get("quantity", 0)),
                        "avg_cost": float(pos.get("average_buy_price", 0)),
                        "current_price": float(pos.get("last_trade_price", 0) or 0),
                    })
                except Exception:
                    continue

            return {
                "equity": float(profile.get("equity", 0) or 0),
                "cash": float(profile.get("withdrawable_amount", 0) or 0),
                "market_value": float(profile.get("market_value", 0) or 0),
                "holdings": holdings,
            }
        except Exception as e:
            logger.error(f"RH portfolio error: {e}")
            return None

    def get_dividends(self) -> List[Dict]:
        """Get dividend history."""
        if not self._rh:
            return []
        try:
            return self._rh.get_dividends() or []
        except Exception:
            return []

    # ── Trading ────────────────────────────────────────────────

    def place_order(self, symbol: str, side: str = "BUY", qty: int = 1,
                    order_type: str = "MARKET", limit_price: float = None,
                    time_in_force: str = "gfd") -> Optional[Dict]:
        """
        Place a trade on Robinhood.

        Args:
            symbol: Ticker
            side: "BUY" or "SELL"
            qty: Number of shares (supports fractional)
            order_type: "MARKET" or "LIMIT"
            limit_price: Required for LIMIT orders
            time_in_force: "gfd" (good for day), "gtc" (good til cancelled)

        WARNING: REAL orders, REAL money. No confirmation prompt.
        """
        if not self._rh or not self._logged_in:
            logger.error("Not logged into Robinhood")
            return None

        logger.warning(f"⚠ ROBINHOOD ORDER: {side} {qty} {symbol} @ {order_type}")

        try:
            if side.upper() == "BUY":
                if order_type.upper() == "MARKET":
                    result = self._rh.order_buy_market(
                        symbol, qty, timeInForce=time_in_force
                    )
                elif order_type.upper() == "LIMIT" and limit_price:
                    result = self._rh.order_buy_limit(
                        symbol, qty, limit_price, timeInForce=time_in_force
                    )
                else:
                    logger.error(f"Invalid order type: {order_type}")
                    return None

            elif side.upper() == "SELL":
                if order_type.upper() == "MARKET":
                    result = self._rh.order_sell_market(
                        symbol, qty, timeInForce=time_in_force
                    )
                elif order_type.upper() == "LIMIT" and limit_price:
                    result = self._rh.order_sell_limit(
                        symbol, qty, limit_price, timeInForce=time_in_force
                    )
                else:
                    return None
            else:
                return None

            if result:
                logger.info(f"RH order result: {result.get('id', 'unknown')}")
            return result

        except Exception as e:
            logger.error(f"RH order error: {e}")
            return None

    def cancel_order(self, order_id: str) -> bool:
        """Cancel an open order."""
        if not self._rh:
            return False
        try:
            result = self._rh.cancel_stock_order(order_id)
            return result is not None
        except Exception:
            return False

    def get_open_orders(self) -> List[Dict]:
        """Get all open/pending orders."""
        if not self._rh:
            return []
        try:
            return self._rh.get_all_open_stock_orders() or []
        except Exception:
            return []

    def get_status(self) -> Dict:
        return {
            "provider": "robinhood",
            "connected": self._logged_in,
            "streaming": self._running,
            "poll_interval": self._poll_interval,
            "symbols": list(self._symbols),
            "type": "unofficial_api",
        }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DUAL BROKER MANAGER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class DualBrokerManager:
    """
    Manages both Webull and Robinhood simultaneously.

    Strategy:
      - Webull MQTT for real-time price data (faster, official)
      - Robinhood for portfolio monitoring and secondary quotes
      - Trade execution on whichever account you choose
      - Both feed into a single RealtimeBuffer for the Oracle

    Usage:
        mgr = DualBrokerManager(
            webull_key="...", webull_secret="...",
            rh_email="...", rh_password="...", rh_totp="...",
        )
        mgr.start(["AAPL", "TSLA", "NVDA", "MSFT"])

        # Get best available price (Webull first, RH fallback)
        price = mgr.get_price("AAPL")

        # Get combined portfolio across both brokers
        portfolio = mgr.get_combined_portfolio()

        # Execute trade on a specific broker
        mgr.trade("AAPL", "BUY", 10, broker="webull")
    """

    def __init__(
        self,
        webull_key: str = "",
        webull_secret: str = "",
        rh_email: str = "",
        rh_password: str = "",
        rh_totp: str = "",
    ):
        self.buffer = RealtimeBuffer(max_ticks=50000)

        self.webull = None
        self.robinhood = None

        if webull_key and webull_secret:
            self.webull = WebullConnector(
                app_key=webull_key,
                app_secret=webull_secret,
                buffer=self.buffer,
            )

        if rh_email and rh_password:
            self.robinhood = RobinhoodConnector(
                email=rh_email,
                password=rh_password,
                totp_secret=rh_totp,
                buffer=self.buffer,
            )

    def start(self, symbols: List[str]):
        """Start both brokers with the given symbol list."""
        if self.webull:
            self.webull.subscribe(symbols)
            self.webull.start_streaming()
            logger.info("Webull streaming started (real-time MQTT)")

        if self.robinhood:
            if self.robinhood.login():
                self.robinhood.subscribe(symbols)
                self.robinhood.start_polling(interval=5.0)
                logger.info("Robinhood polling started (5s interval)")

    def stop(self):
        if self.webull:
            self.webull.stop_streaming()
        if self.robinhood:
            self.robinhood.stop_polling()
            self.robinhood.logout()

    def get_price(self, symbol: str) -> Optional[float]:
        """Get best available price (Webull real-time > RH > buffer)."""
        # Try buffer first (has latest from whichever source)
        price = self.buffer.get_price(symbol)
        if price:
            return price

        # Fallback to direct API calls
        if self.webull:
            snap = self.webull.get_snapshot(symbol)
            if snap:
                return float(snap.get("close", 0) or snap.get("price", 0))

        if self.robinhood:
            return self.robinhood.get_price(symbol)

        return None

    def get_combined_portfolio(self) -> Dict:
        """Get combined portfolio across both brokers."""
        result = {
            "total_equity": 0,
            "webull": None,
            "robinhood": None,
        }

        if self.webull:
            wb_info = self.webull.get_account_info()
            if wb_info:
                result["webull"] = wb_info

        if self.robinhood:
            rh_info = self.robinhood.get_portfolio()
            if rh_info:
                result["robinhood"] = rh_info
                result["total_equity"] += rh_info.get("equity", 0)

        return result

    def trade(self, symbol: str, side: str, qty: int,
              broker: str = "webull", **kwargs) -> Optional[Dict]:
        """Execute a trade on a specific broker."""
        if broker == "webull" and self.webull:
            return self.webull.place_order(symbol, side, qty, **kwargs)
        elif broker == "robinhood" and self.robinhood:
            return self.robinhood.place_order(symbol, side, qty, **kwargs)
        else:
            logger.error(f"Broker '{broker}' not available")
            return None

    def get_status(self) -> Dict:
        return {
            "webull": self.webull.get_status() if self.webull else "not configured",
            "robinhood": self.robinhood.get_status() if self.robinhood else "not configured",
            "buffer_symbols": list(self.buffer.get_all_symbols()),
            "buffer_ticks": sum(len(b) for b in self.buffer._ticks.values()),
        }
