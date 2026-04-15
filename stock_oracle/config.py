"""
Stock Oracle Configuration
==========================
"""
import os
import sys
from pathlib import Path

BASE_DIR = Path(__file__).parent

# Support portable data directory (used by PyInstaller frozen builds)
# When frozen, data goes to %APPDATA%/StockOracle so it persists across updates
_data_override = os.environ.get("STOCK_ORACLE_DATA_DIR")
if _data_override:
    DATA_DIR = Path(_data_override)
    CACHE_DIR = DATA_DIR / "cache"
    MODEL_DIR = DATA_DIR / "models"
else:
    DATA_DIR = BASE_DIR / "data"
    CACHE_DIR = BASE_DIR / "cache"
    MODEL_DIR = BASE_DIR / "models"

for d in [DATA_DIR, CACHE_DIR, MODEL_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ── Load .env file (saved by GUI Settings) ─────────────────────
_env_file = BASE_DIR / ".env"
if _env_file.exists():
    try:
        for _line in _env_file.read_text().splitlines():
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _key, _, _val = _line.partition("=")
                _key = _key.strip()
                _val = _val.strip().strip('"').strip("'")
                if _val and _key not in os.environ:
                    os.environ[_key] = _val
    except Exception:
        pass

WATCHLIST = [
    # Individual stocks
    "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META", "AMD",
    "INTC", "LUNR", "RKLB", "ASTS", "RDW", "OPTT", "PIII", "PLUG",
    # ETFs
    "NOBL", "RDVY", "SCHD", "VIG", "VYM", "IVV", "BND", "VONG", "SPMO", "VWO", "IJR",
]

# API Keys (all optional)
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID", "")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET", "")
REDDIT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
ALPHA_VANTAGE_KEY = os.getenv("ALPHA_VANTAGE_KEY", "")
FRED_API_KEY = os.getenv("FRED_API_KEY", "")
NEWS_API_KEY = os.getenv("NEWS_API_KEY", "")
SEC_USER_AGENT = os.getenv("SEC_USER_AGENT", "StockOracle your@email.com")

# Real-Time Providers
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "")
ALPACA_KEY_ID = os.getenv("ALPACA_KEY_ID", "")
ALPACA_SECRET = os.getenv("ALPACA_SECRET", "")
ALPACA_USE_SIP = False
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "")
IBKR_HOST = os.getenv("IBKR_HOST", "127.0.0.1")
IBKR_PORT = int(os.getenv("IBKR_PORT", "7497"))
SCHWAB_APP_KEY = os.getenv("SCHWAB_APP_KEY", "")
SCHWAB_ACCESS_TOKEN = os.getenv("SCHWAB_ACCESS_TOKEN", "")

# Ollama (Local AI)
OLLAMA_BASE_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:14b")
OLLAMA_FALLBACK_MODEL = "qwen2.5:7b"
OLLAMA_TIMEOUT = 120

# GitHub (optional — avoids 403 rate limits)
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")

# Collector Settings
CACHE_TTL_HOURS = 4
REQUEST_DELAY = 0.2
MAX_RETRIES = 1  # Don't retry — broken endpoints won't fix themselves
REDDIT_SUBREDDITS = [
    "wallstreetbets","stocks","investing","stockmarket",
    "options","SecurityAnalysis","ValueInvesting","pennystocks","Daytrading",
]

# ML Settings
PREDICTION_HORIZON_DAYS = 5
TRAIN_TEST_SPLIT = 0.8
LOOKBACK_DAYS = 60
ENSEMBLE_MODELS = ["random_forest","gradient_boost","neural_net"]

SIGNAL_WEIGHTS = {
    # ══════════════════════════════════════════════════════════════
    # Weights calibrated from TWO sessions: down market + up market
    # Strategy: bidirectional > balanced bull+bear > mixed > bad
    # ══════════════════════════════════════════════════════════════

    # ── BIDIRECTIONAL — accurate in BOTH up and down markets ──
    # Highest weight — the only collectors that reliably detect
    # direction regardless of market conditions.
    "dividend_vs_treasury":.12,  # 91.7% down, 55.0% up — best overall
    "employee_sentiment":.10,    # 63.2% down, 66.7% up — consistent both
    "wikipedia_velocity":.08,    # 72.0% down, 56.0% up

    # ── BEAR-BIASED — detect downturns well, miss rallies ──
    "momentum_quality":.07,      # 76.3% down, 42.6% up
    "sec_edgar":.05,             # 69.7% down, 36.4% up
    "energy_cascade":.03,        # 62.5% down, 37.5% up

    # ── BULL-BIASED — detect rallies, miss downturns ──
    # Raised back to BALANCE the bear-biased collectors.
    # Without these, the system has a bearish bias in up markets.
    "technical_analysis":.06,    # 17.9% down, 57.9% up
    "analyst_consensus":.05,     # 34.5% down, 60.7% up
    "news_sentiment":.05,        # 26.9% down, 62.5% up
    "hackernews_sentiment":.04,  # 35.0% down, 65.0% up
    "supply_chain":.04,          # 35.4% down, 81.2% up — best bull detector
    "seasonality":.03,           # 23.5% down, 59.3% up
    "viral_catalyst":.03,        # 23.5% down, 59.3% up
    "shipping_activity":.02,     # 23.5% down, 59.3% up
    "google_trends":.02,         # 11.4% down, 55.0% up

    # ── MIXED — roughly 50% both directions ──
    "cross_stock":.05,           # 40.0% down, 60.0% up
    "short_interest":.05,        # 43.3% down, 60.0% up
    "finnhub_realtime":.05,      # 47.0% down, 50.0% up — essential price
    "insider_ratio":.05,         # 40.9% down, 53.8% up
    "fundamental_analysis":.06,  # 39.7% down, 47.6% up — essential context
    "earnings_contagion":.04,    # 50.0% both
    "earnings_nlp":.04,          # 38.9% down, 66.7% up — small sample
    "app_store_rank":.02,        # 37.5% down, 62.5% up — small sample
    "reddit_sentiment":.03,      # Noisy proxy

    # ── REAL-TIME NEWS — dynamic signal, updates every 15 min ──
    "realtime_news":.10,         # Finnhub news with recency weighting — NEW, high weight

    # ── CONTEXT PROVIDERS — low accuracy but useful dashboard/ML data ──
    "market_pulse":.04,          # 0% down, 40.7% up — provides index data
    "fear_greed_proxy":.03,      # 30.8% down — provides VIX for dashboard
    "yahoo_finance":.03,         # 11.1% down, 53.3% up — price context

    # ── BAD BOTH — minimal weight ──
    "talent_flow":.01,           # 29.2% down, 25.0% up — worst performer
    "options_flow":.02,          # 23.8% down, 35.7% up

    # ── MACRO — same for all tickers ──
    "cardboard_index":.01, "waffle_house_index":.01,
    "weather_correlation":.01, "job_postings":.02, "gov_contracts":.01,

    # ── DEAD APIS — zero weight ──
    "domain_registration":0, "patent_activity":0,
    "github_velocity":.03, "insider_trades":0,

    # ── Legacy ──
    "sec_filing_timing":.02,
}

# Backtesting
BACKTEST_DEFAULT_CAPITAL = 10000.0
BACKTEST_POSITION_SIZE = 0.95
BACKTEST_STOP_LOSS = 0.05
BACKTEST_TAKE_PROFIT = 0.15
BACKTEST_SIGNAL_THRESHOLD = 0.12
BACKTEST_CONFIDENCE_THRESHOLD = 0.4

# Dashboard
DASHBOARD_HOST = "0.0.0.0"
DASHBOARD_PORT = 5000

# UI Colors (shared between gui.py and setup_wizard.py)
BG_DARK     = "#0d1117"
BG_PANEL    = "#161b22"
BG_CARD     = "#1c2333"
BG_INPUT    = "#0d1117"
FG_PRIMARY  = "#e6edf3"
FG_SECONDARY= "#8b949e"
FG_DIM      = "#484f58"
GREEN       = "#3fb950"
RED         = "#f85149"
AMBER       = "#d29922"
BLUE        = "#58a6ff"
PURPLE      = "#bc8cff"
BORDER      = "#30363d"
ACCENT      = "#1f6feb"
