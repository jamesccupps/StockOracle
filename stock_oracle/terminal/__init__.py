"""
Stock Oracle Terminal
=====================
A Bloomberg-style, keyboard-driven market terminal that runs in the browser
on top of the Stock Oracle engine.

    python -m stock_oracle.terminal            # http://127.0.0.1:8765
    python -m stock_oracle.terminal --lan      # reachable over Tailscale/LAN (token protected)

Layout of this package:
    settings.py       keys and options (reads the same .env the GUI writes)
    symbols.py        ticker normalization (Yahoo vs Finnhub formats)
    cache.py          async TTL cache with single-flight fetches
    providers/        market data: Yahoo (no key), Finnhub (free key), routing
    market.py         index / sector / rates / FX / commodity / crypto groups
    quotes.py         live quote hub: websocket stream + polling + fan-out
    oracle_bridge.py  Oracle analysis, accuracy, regime, breakouts, Claude ASK
    server.py         FastAPI app (REST + /ws)
    static/           the browser front end
    selftest.py       end-to-end check of every endpoint
"""

__version__ = "1.0.0"
