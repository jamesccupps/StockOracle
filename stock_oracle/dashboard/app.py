"""
Stock Oracle Dashboard
======================
Flask-based web dashboard serving analysis results.
Run: python -m stock_oracle.dashboard.app
"""
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, jsonify, request, render_template_string
from flask_cors import CORS

from stock_oracle.oracle import StockOracle
from stock_oracle.config import DASHBOARD_HOST, DASHBOARD_PORT, WATCHLIST, DATA_DIR

logger = logging.getLogger("stock_oracle")

app = Flask(__name__)
CORS(app)

# Global oracle instance
oracle = None


def get_oracle():
    global oracle
    if oracle is None:
        oracle = StockOracle(use_ml=True, parallel=True)
    return oracle


# ── API Endpoints ──────────────────────────────────────────────

@app.route("/api/analyze/<ticker>")
def api_analyze(ticker):
    """Analyze a single stock."""
    try:
        result = get_oracle().analyze(ticker.upper())
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/watchlist")
def api_watchlist():
    """Analyze the full watchlist."""
    try:
        tickers = request.args.get("tickers", "").split(",")
        tickers = [t.strip().upper() for t in tickers if t.strip()] or WATCHLIST
        results = get_oracle().analyze_watchlist(tickers=tickers, verbose=False)
        return jsonify({"results": results, "timestamp": datetime.now(timezone.utc).isoformat()})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/signals/<ticker>")
def api_signals(ticker):
    """Get detailed signal breakdown for a ticker."""
    try:
        result = get_oracle().analyze(ticker.upper())
        return jsonify({
            "ticker": ticker.upper(),
            "signals": result.get("signals", []),
            "summary": result.get("signal_summary", {}),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/history")
def api_history():
    """Get past analysis results."""
    files = sorted(DATA_DIR.glob("analysis_*.json"), reverse=True)[:10]
    history = []
    for f in files:
        try:
            data = json.loads(f.read_text())
            history.append({
                "filename": f.name,
                "timestamp": f.name.replace("analysis_", "").replace(".json", ""),
                "ticker_count": len(data) if isinstance(data, list) else 1,
            })
        except Exception:
            continue
    return jsonify({"history": history})


@app.route("/api/config")
def api_config():
    """Get current configuration."""
    from stock_oracle.config import SIGNAL_WEIGHTS, WATCHLIST
    return jsonify({
        "watchlist": WATCHLIST,
        "signal_weights": SIGNAL_WEIGHTS,
        "collectors": [c.name for c in get_oracle().collectors],
    })


# ── Web UI ─────────────────────────────────────────────────────

DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Stock Oracle Dashboard</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'SF Mono', 'Fira Code', monospace;
            background: #0a0a0f;
            color: #e0e0e0;
            min-height: 100vh;
        }
        .header {
            background: linear-gradient(135deg, #0f0f1a 0%, #1a1a2e 100%);
            padding: 20px 30px;
            border-bottom: 1px solid #2a2a3e;
            display: flex;
            align-items: center;
            justify-content: space-between;
        }
        .header h1 {
            font-size: 24px;
            background: linear-gradient(90deg, #00ff88, #00aaff);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }
        .controls {
            display: flex;
            gap: 10px;
            align-items: center;
        }
        .controls input {
            background: #1a1a2e;
            border: 1px solid #3a3a5e;
            color: #e0e0e0;
            padding: 8px 16px;
            border-radius: 6px;
            font-family: inherit;
        }
        .controls button {
            background: linear-gradient(135deg, #00ff88, #00aaff);
            color: #0a0a0f;
            border: none;
            padding: 8px 20px;
            border-radius: 6px;
            font-weight: bold;
            cursor: pointer;
        }
        .controls button:hover { opacity: 0.9; }
        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(320px, 1fr));
            gap: 16px;
            padding: 20px 30px;
        }
        .card {
            background: #12121e;
            border: 1px solid #2a2a3e;
            border-radius: 12px;
            padding: 20px;
            transition: all 0.2s;
        }
        .card:hover {
            border-color: #4a4a6e;
            transform: translateY(-2px);
        }
        .card-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 16px;
        }
        .ticker { font-size: 22px; font-weight: bold; }
        .prediction {
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 12px;
            font-weight: bold;
        }
        .bull { background: #00ff8820; color: #00ff88; border: 1px solid #00ff8840; }
        .bear { background: #ff444420; color: #ff4444; border: 1px solid #ff444440; }
        .neutral { background: #ffaa0020; color: #ffaa00; border: 1px solid #ffaa0040; }
        .signal-bar {
            height: 8px;
            background: #1a1a2e;
            border-radius: 4px;
            margin: 12px 0;
            position: relative;
            overflow: hidden;
        }
        .signal-fill {
            height: 100%;
            border-radius: 4px;
            transition: width 0.5s;
        }
        .signals-list { font-size: 12px; color: #8888aa; }
        .signal-item {
            display: flex;
            justify-content: space-between;
            padding: 4px 0;
            border-bottom: 1px solid #1a1a2e;
        }
        .loading {
            text-align: center;
            padding: 60px;
            color: #4a4a6e;
            font-size: 18px;
        }
        .spinner {
            display: inline-block;
            width: 40px;
            height: 40px;
            border: 3px solid #2a2a3e;
            border-top-color: #00ff88;
            border-radius: 50%;
            animation: spin 0.8s linear infinite;
            margin-bottom: 16px;
        }
        @keyframes spin { to { transform: rotate(360deg); } }
    </style>
</head>
<body>
    <div class="header">
        <h1>⟐ STOCK ORACLE</h1>
        <div class="controls">
            <input type="text" id="tickerInput" placeholder="AAPL, TSLA, NVDA..."
                   value="{{ watchlist }}" />
            <button onclick="analyze()">ANALYZE</button>
        </div>
    </div>
    <div id="content" class="grid"></div>

    <script>
        async function analyze() {
            const input = document.getElementById('tickerInput').value;
            const content = document.getElementById('content');
            content.innerHTML = '<div class="loading"><div class="spinner"></div><br>Collecting signals across 16 data sources...</div>';

            try {
                const resp = await fetch(`/api/watchlist?tickers=${encodeURIComponent(input)}`);
                const data = await resp.json();
                renderResults(data.results);
            } catch (e) {
                content.innerHTML = `<div class="loading">Error: ${e.message}</div>`;
            }
        }

        function renderResults(results) {
            const content = document.getElementById('content');
            content.innerHTML = results.map(r => {
                if (r.error) return `<div class="card"><div class="ticker">${r.ticker}</div><div style="color:#ff4444">Error: ${r.error}</div></div>`;

                const pred = r.prediction || 'NEUTRAL';
                const cls = pred === 'BULLISH' ? 'bull' : pred === 'BEARISH' ? 'bear' : 'neutral';
                const signal = r.signal || 0;
                const conf = r.confidence || 0;
                const pct = Math.abs(signal) * 100;
                const dir = signal >= 0 ? 'right' : 'left';
                const color = signal > 0 ? '#00ff88' : signal < 0 ? '#ff4444' : '#ffaa00';

                const signals = (r.signals || [])
                    .filter(s => s.confidence > 0.1)
                    .sort((a, b) => Math.abs(b.signal) - Math.abs(a.signal))
                    .slice(0, 6)
                    .map(s => {
                        const icon = s.signal > 0.1 ? '🟢' : s.signal < -0.1 ? '🔴' : '⚪';
                        return `<div class="signal-item">
                            <span>${icon} ${s.collector}</span>
                            <span>${s.signal > 0 ? '+' : ''}${s.signal.toFixed(2)}</span>
                        </div>`;
                    }).join('');

                return `
                    <div class="card">
                        <div class="card-header">
                            <span class="ticker">${r.ticker}</span>
                            <span class="prediction ${cls}">${pred}</span>
                        </div>
                        <div style="font-size:13px;color:#8888aa">
                            Signal: <span style="color:${color}">${signal > 0 ? '+' : ''}${signal.toFixed(4)}</span>
                            &nbsp;│&nbsp; Confidence: ${(conf * 100).toFixed(0)}%
                        </div>
                        <div class="signal-bar">
                            <div class="signal-fill" style="width:${pct}%;background:${color};margin-left:${signal < 0 ? (50 - pct) + '%' : '50%'}"></div>
                        </div>
                        <div class="signals-list">${signals}</div>
                    </div>
                `;
            }).join('');
        }

        // Auto-analyze on load
        window.addEventListener('load', () => setTimeout(analyze, 500));
    </script>
</body>
</html>
"""


@app.route("/")
def dashboard():
    return render_template_string(DASHBOARD_HTML, watchlist=",".join(WATCHLIST))


if __name__ == "__main__":
    print(f"\n  Stock Oracle Dashboard")
    print(f"  http://localhost:{DASHBOARD_PORT}\n")
    app.run(
        host=DASHBOARD_HOST,
        port=DASHBOARD_PORT,
        debug=True,
    )
