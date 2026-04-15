# Stock Oracle

A multi-signal stock prediction and monitoring system that combines 39 data collectors, machine learning, and optional AI analysis to generate real-time predictions.

![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue)
![License: MIT](https://img.shields.io/badge/license-MIT-green)
![Platform: Windows](https://img.shields.io/badge/platform-Windows-lightgrey)

## Features

- **39 Signal Collectors** — Technical analysis, news sentiment, SEC filings, Reddit/HackerNews sentiment, analyst ratings, insider trades, macro indicators, and more
- **Real-Time Monitoring** — Continuous scanning with live prices, intraday trend tracking, and automatic prediction verification
- **Machine Learning** — Ensemble ML (Random Forest, Gradient Boost, Neural Net) that trains on verified predictions and improves over time
- **Signal Intelligence** — Automatically detects and suppresses stale/constant signals, adapts conviction thresholds per ticker volatility, and adjusts for market hours vs after-hours
- **Market Regime Detection** — Identifies broad market selloffs/rallies from SPY + sector breadth and shifts all predictions accordingly
- **Breakout Scanner** — Scores stocks 0-100 on breakout probability using 8 technical patterns with estimated timeframes
- **Real-Time News Feed** — Finnhub-powered news aggregation with recency weighting (breaking news counts 3x)
- **Claude AI Advisor** — Optional Anthropic API integration for hourly weight adjustments, pattern detection, and session reviews
- **News Feed** — Per-stock and watchlist-wide news with clickable article links
- **Built-in Help Guide** — 7-tab help system explaining every feature

## Quick Start

### Option 1: One-Click Install (Windows)

1. Download or clone this repository
2. Double-click `INSTALL.bat`
3. Follow the prompts — it installs Python dependencies and creates shortcuts
4. Launch from Desktop shortcut or Start Menu

### Option 2: Manual Setup

```bash
git clone https://github.com/jamesccupps/StockOracle.git
cd StockOracle
pip install -r stock_oracle/requirements.txt
python -m stock_oracle
```

### Option 3: Build Standalone .exe

```bash
# After cloning and installing dependencies:
BUILD.bat
# Output: dist/StockOracle/StockOracle.exe (no Python needed to run)
```

## Configuration

1. Copy `.env.example` to `stock_oracle/.env`
2. Add your API keys (all optional, but Finnhub is recommended for real-time prices):

| Key | Source | Cost | What it enables |
|-----|--------|------|-----------------|
| Finnhub | [finnhub.io](https://finnhub.io/register) | Free | Real-time prices, company news |
| Anthropic | [console.anthropic.com](https://console.anthropic.com) | ~$5/mo | Claude AI advisor |
| FRED | [fred.stlouisfed.org](https://fred.stlouisfed.org) | Free | Economic indicators |
| News API | [newsapi.org](https://newsapi.org) | Free | News sentiment |
| Reddit | [reddit.com/prefs/apps](https://www.reddit.com/prefs/apps) | Free | Social sentiment |
| SEC EDGAR | Just your email | Free | Filing analysis |
| GitHub | [github.com/settings/tokens](https://github.com/settings/tokens) | Free | Avoids rate limits |

Or skip all of this — the Settings dialog in the app lets you add keys through the GUI.

## How It Works

### Signal Collection
Each scan pulls data from 39 collectors spanning technical indicators (RSI, MACD, Bollinger Bands), fundamental analysis (P/E, margins), news sentiment, social media, SEC filings, analyst ratings, and alternative data sources. Signals are combined using tiered weighting with intelligence adjustments.

### Signal Intelligence
The system learns which collectors actually produce changing signals vs static noise. Collectors that return the same value every scan (like cached analyst ratings) get suppressed. Only dynamic, real-time signals drive conviction calls.

### Market Regime
Before predicting individual stocks, the system checks SPY, sector ETFs, and market breadth to detect broad selloffs or rallies. In a selloff, all predictions shift bearish — because when the whole market drops, individual stock signals don't matter much.

### Prediction Verification
Every prediction is recorded and verified against actual price movement (intraday: 3 scans later, 5-day: after horizon passes). Verified outcomes feed back into ML training, creating a learning loop.

### Breakout Scanner
Scores stocks on 8 technical breakout patterns (Bollinger squeeze, volume accumulation, 52-week high proximity, RSI momentum, MACD crossover, MA alignment, range compression, relative strength) with estimated timeframes.

## Project Structure

```
StockOracle/
├── INSTALL.bat                    # One-click installer
├── START.bat                      # Quick launcher
├── BUILD.bat                      # PyInstaller build script
├── .env.example                   # API key template
├── stock_oracle/
│   ├── __main__.py                # Entry point
│   ├── config.py                  # Configuration & signal weights
│   ├── oracle.py                  # Main orchestrator
│   ├── gui.py                     # Desktop GUI (tkinter)
│   ├── signal_intelligence.py     # Stale signal detection & adaptive thresholds
│   ├── market_regime.py           # Broad market selloff/rally detection
│   ├── breakout_detector.py       # Breakout probability scanner
│   ├── news_feed.py               # News aggregation & display
│   ├── claude_advisor.py          # Anthropic Claude AI integration
│   ├── session_tracker.py         # Intraday monitoring & verification
│   ├── prediction_tracker.py      # 5-day prediction recording & scoring
│   ├── narrative.py               # Human-readable prediction summaries
│   ├── ml/
│   │   └── pipeline.py            # ML ensemble (RF, GBM, NN)
│   ├── collectors/
│   │   ├── base.py                # Base collector with caching
│   │   ├── yahoo_finance.py       # Price data & technicals
│   │   ├── finnhub_collector.py   # Real-time quotes & analyst data
│   │   ├── realtime_news.py       # Breaking news signal (15min cache)
│   │   ├── analysis.py            # RSI, MACD, Bollinger, MA crossovers
│   │   ├── advanced_signals.py    # Supply chain, patents, app store
│   │   ├── alt_data.py            # News, shipping, sentiment
│   │   ├── creative_signals.py    # Wikipedia, energy, cardboard index
│   │   ├── new_indicators.py      # Fear/greed, momentum, insider ratio
│   │   ├── cross_stock.py         # Peer correlation & earnings contagion
│   │   └── ...
│   └── data/                      # Generated at runtime (gitignored)
│       ├── predictions/           # Pending & verified predictions
│       ├── sessions/              # Monitoring session data
│       └── settings.json          # GUI-saved settings
```

## Watchlist

Edit the `WATCHLIST` in `config.py` or add/remove tickers through the GUI. The default list includes major tech, space, and ETF tickers. The system handles 50-60 tickers comfortably.

## Disclaimer

**This is a research and educational tool. It is NOT financial advice.** No algorithm can predict the stock market with certainty. Past performance does not guarantee future results. Always do your own research and consult a financial advisor before making investment decisions. Use at your own risk.

## License

MIT — see [LICENSE](LICENSE)
