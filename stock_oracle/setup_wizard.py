"""
Stock Oracle — First Run Setup Wizard
=======================================
Guided setup that runs on first launch only.
Walks through: API keys → Ollama → Watchlist → Generate History → Train ML → First Scan

After completion, writes a .setup_complete flag so it doesn't run again.
"""
import json
import os
import threading
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from stock_oracle.config import (
    DATA_DIR, WATCHLIST, FINNHUB_API_KEY,
    BG_DARK, BG_PANEL, BG_CARD, BG_INPUT,
    FG_PRIMARY, FG_SECONDARY, FG_DIM,
    GREEN, RED, AMBER, BLUE, PURPLE, ACCENT, BORDER,
)

SETUP_FLAG = DATA_DIR / ".setup_complete"


def needs_setup() -> bool:
    """Check if first-run setup is needed."""
    return not SETUP_FLAG.exists()


def mark_setup_complete():
    """Mark setup as done so wizard doesn't run again."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    SETUP_FLAG.write_text(datetime.now().isoformat())


class SetupWizard:
    """
    Multi-step first-run wizard.

    Steps:
      1. Welcome
      2. API Keys (Finnhub, Anthropic, Ollama)
      3. Watchlist editor
      4. Generate History + Train ML
      5. Done → launch main app
    """

    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Stock Oracle — First Time Setup")
        self.root.geometry("700x600")
        self.root.configure(bg=BG_DARK)
        self.root.resizable(False, False)

        try:
            import ctypes
            ctypes.windll.shcore.SetProcessDpiAwareness(1)
        except Exception:
            pass

        self.settings: Dict[str, str] = {}
        self.watchlist: List[str] = list(WATCHLIST)
        self.current_step = 0
        self.completed = False

        # Main container
        self.header = tk.Frame(self.root, bg=BG_PANEL, padx=20, pady=12)
        self.header.pack(fill="x")

        self.title_label = tk.Label(self.header, text="", bg=BG_PANEL, fg=FG_PRIMARY,
                                     font=("Segoe UI", 16, "bold"))
        self.title_label.pack(side="left")

        self.step_label = tk.Label(self.header, text="", bg=BG_PANEL, fg=FG_DIM,
                                    font=("Segoe UI", 10))
        self.step_label.pack(side="right")

        self.content = tk.Frame(self.root, bg=BG_DARK)
        self.content.pack(fill="both", expand=True, padx=20, pady=10)

        # Bottom nav
        self.nav = tk.Frame(self.root, bg=BG_DARK, padx=20, pady=12)
        self.nav.pack(fill="x")

        self.back_btn = tk.Button(self.nav, text="← Back", bg=BG_CARD, fg=FG_PRIMARY,
                                   font=("Segoe UI", 10), borderwidth=0, padx=16, pady=6,
                                   cursor="hand2", command=self._prev_step)
        self.back_btn.pack(side="left")

        self.next_btn = tk.Button(self.nav, text="Next →", bg=ACCENT, fg="white",
                                   font=("Segoe UI", 10, "bold"), borderwidth=0,
                                   padx=20, pady=6, cursor="hand2",
                                   command=self._next_step)
        self.next_btn.pack(side="right")

        self.skip_btn = tk.Button(self.nav, text="Skip Setup", bg=BG_DARK, fg=FG_DIM,
                                   font=("Segoe UI", 9), borderwidth=0, padx=12,
                                   cursor="hand2", command=self._skip)
        self.skip_btn.pack(side="right", padx=(0, 12))

        self.steps = [
            self._step_welcome,
            self._step_api_keys,
            self._step_ollama,
            self._step_watchlist,
            self._step_train,
            self._step_done,
        ]

        self._show_step(0)

    def _clear_content(self):
        for w in self.content.winfo_children():
            w.destroy()

    def _show_step(self, idx):
        self.current_step = idx
        self._clear_content()
        self.step_label.configure(text=f"Step {idx + 1} of {len(self.steps)}")

        # Nav button visibility
        self.back_btn.pack_forget()
        if idx > 0:
            self.back_btn.pack(side="left")

        if idx == len(self.steps) - 1:
            self.next_btn.configure(text="Launch Stock Oracle")
            self.skip_btn.pack_forget()
        elif idx == len(self.steps) - 2:
            self.next_btn.configure(text="Generate & Train")
        else:
            self.next_btn.configure(text="Next →")

        self.steps[idx]()

    def _next_step(self):
        # Save current step data
        self._save_current_step()

        if self.current_step == len(self.steps) - 2:
            # Training step — run it
            self._run_training()
            return

        if self.current_step >= len(self.steps) - 1:
            # Done — launch main app
            self._finish()
            return

        self._show_step(self.current_step + 1)

    def _prev_step(self):
        if self.current_step > 0:
            self._show_step(self.current_step - 1)

    def _skip(self):
        if messagebox.askyesno("Skip Setup",
                "Skip the setup wizard?\n\n"
                "You can configure settings later from the Settings menu.\n"
                "The app will work with default settings but some features "
                "may be limited without API keys."):
            self._finish()

    def _save_current_step(self):
        """Collect data from current step's entry fields."""
        for key, var in getattr(self, '_current_entries', {}).items():
            try:
                val = var.get().strip()
                if val:
                    self.settings[key] = val
            except Exception:
                pass  # Widget may be destroyed

        # Save watchlist from text widget (before it gets destroyed by step change)
        text = getattr(self, 'watchlist_text', None)
        if text:
            try:
                raw = text.get("1.0", "end").strip()
                tickers = [t.strip().upper() for t in raw.replace("\n", ",").split(",") if t.strip()]
                if tickers:
                    self.watchlist = tickers
            except Exception:
                pass  # Widget already destroyed — watchlist was saved earlier

    # ── STEP 1: Welcome ──────────────────────────────────────

    def _step_welcome(self):
        self._current_entries = {}
        self.title_label.configure(text="Welcome to Stock Oracle")

        msg = tk.Label(self.content, bg=BG_DARK, fg=FG_PRIMARY,
                        font=("Segoe UI", 11), justify="left", wraplength=620,
                        text=(
            "Stock Oracle is a multi-signal stock prediction system that combines "
            "38 data collectors, machine learning, and optional AI analysis to "
            "generate predictions for your watchlist.\n\n"
            "This setup will walk you through:\n\n"
            "  1. API Keys — connect to real-time data sources\n"
            "  2. Local AI — configure Ollama for NLP analysis\n"
            "  3. Watchlist — choose which stocks to track\n"
            "  4. Training — generate historical data and train the ML model\n\n"
            "Most API keys are free. The app works without them but "
            "more keys = more data = better predictions.\n\n"
            "This wizard only runs once. You can change everything later in Settings."
        ))
        msg.pack(pady=20, anchor="w")

    # ── STEP 2: API Keys ─────────────────────────────────────

    def _step_api_keys(self):
        self.title_label.configure(text="API Keys")
        self._current_entries = {}

        canvas = tk.Canvas(self.content, bg=BG_DARK, highlightthickness=0)
        scrollbar = ttk.Scrollbar(self.content, orient="vertical", command=canvas.yview)
        inner = tk.Frame(canvas, bg=BG_DARK)
        inner.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=inner, anchor="nw", width=630)
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        def add_key(key, label, hint, required=False, show=None):
            frame = tk.Frame(inner, bg=BG_DARK)
            frame.pack(fill="x", pady=3)

            color = GREEN if required else FG_PRIMARY
            tk.Label(frame, text=label, bg=BG_DARK, fg=color,
                      font=("Segoe UI", 10), width=24, anchor="w").pack(side="left")

            var = tk.StringVar(value=self.settings.get(key, ""))
            entry = tk.Entry(frame, textvariable=var, bg=BG_INPUT, fg=FG_PRIMARY,
                              font=("Consolas", 10), insertbackground=FG_PRIMARY,
                              borderwidth=1, relief="solid", show=show)
            entry.pack(side="left", fill="x", expand=True)
            self._current_entries[key] = var

            tk.Label(inner, text=f"     {hint}", bg=BG_DARK, fg=FG_DIM,
                      font=("Segoe UI", 8)).pack(anchor="w")

        # Section: Essential
        tk.Label(inner, text="Essential (Recommended)", bg=BG_DARK, fg=BLUE,
                  font=("Segoe UI", 11, "bold")).pack(anchor="w", pady=(8, 4))

        add_key("FINNHUB_API_KEY", "Finnhub API Key",
                "FREE at finnhub.io/register — real-time stock quotes", required=True)

        add_key("ANTHROPIC_API_KEY", "Anthropic API Key",
                "console.anthropic.com — enables Claude AI advisor (~$0.50/month)", show="*")

        add_key("CLAUDE_MONTHLY_CAP", "Claude Monthly Cap ($)",
                "Hard spending limit for Claude API (default: 10)")

        # Section: Free Data
        tk.Label(inner, text="Free Data APIs (Optional)", bg=BG_DARK, fg=BLUE,
                  font=("Segoe UI", 11, "bold")).pack(anchor="w", pady=(16, 4))

        add_key("SEC_USER_AGENT", "SEC EDGAR Email",
                "Required format: 'StockOracle your@email.com' — SEC filing data")

        add_key("FRED_API_KEY", "FRED API Key",
                "FREE at fred.stlouisfed.org — economic indicators")

        add_key("REDDIT_CLIENT_ID", "Reddit Client ID",
                "FREE at reddit.com/prefs/apps — social sentiment")
        add_key("REDDIT_CLIENT_SECRET", "Reddit Secret", "", show="*")

        add_key("GITHUB_TOKEN", "GitHub Token",
                "FREE at github.com/settings/tokens — avoids rate limits", show="*")

        # Section: Brokers
        tk.Label(inner, text="Broker APIs (Optional)", bg=BG_DARK, fg=BLUE,
                  font=("Segoe UI", 11, "bold")).pack(anchor="w", pady=(16, 4))

        add_key("ALPACA_KEY_ID", "Alpaca Key ID",
                "FREE at alpaca.markets — paper trading + data")
        add_key("ALPACA_SECRET", "Alpaca Secret", "", show="*")

        # Default Finnhub key hint
        if not self.settings.get("FINNHUB_API_KEY") and FINNHUB_API_KEY:
            tk.Label(inner, text=f"     A default Finnhub key is included but may be rate-limited. "
                                  f"Get your own free key for best results.",
                      bg=BG_DARK, fg=AMBER, font=("Segoe UI", 8)).pack(anchor="w", pady=(8, 0))

    # ── STEP 3: Ollama ───────────────────────────────────────

    def _step_ollama(self):
        self.title_label.configure(text="Local AI (Ollama)")
        self._current_entries = {}

        tk.Label(self.content, bg=BG_DARK, fg=FG_PRIMARY,
                  font=("Segoe UI", 11), justify="left", wraplength=620,
                  text=(
            "Stock Oracle uses Ollama for local NLP analysis of earnings calls "
            "and employee sentiment. This runs on your GPU and costs nothing.\n\n"
            "If you don't have Ollama installed, these two collectors will be "
            "skipped automatically — everything else works fine."
        )).pack(pady=(10, 16), anchor="w")

        frame = tk.Frame(self.content, bg=BG_DARK)
        frame.pack(fill="x", pady=4)
        tk.Label(frame, text="Ollama URL:", bg=BG_DARK, fg=FG_PRIMARY,
                  font=("Segoe UI", 10), width=20, anchor="w").pack(side="left")
        url_var = tk.StringVar(value=self.settings.get("OLLAMA_URL", "http://localhost:11434"))
        tk.Entry(frame, textvariable=url_var, bg=BG_INPUT, fg=FG_PRIMARY,
                  font=("Consolas", 10), insertbackground=FG_PRIMARY,
                  borderwidth=1, relief="solid").pack(side="left", fill="x", expand=True)
        self._current_entries["OLLAMA_URL"] = url_var

        frame2 = tk.Frame(self.content, bg=BG_DARK)
        frame2.pack(fill="x", pady=4)
        tk.Label(frame2, text="Ollama Model:", bg=BG_DARK, fg=FG_PRIMARY,
                  font=("Segoe UI", 10), width=20, anchor="w").pack(side="left")
        model_var = tk.StringVar(value=self.settings.get("OLLAMA_MODEL", "qwen2.5:14b"))
        tk.Entry(frame2, textvariable=model_var, bg=BG_INPUT, fg=FG_PRIMARY,
                  font=("Consolas", 10), insertbackground=FG_PRIMARY,
                  borderwidth=1, relief="solid").pack(side="left", fill="x", expand=True)
        self._current_entries["OLLAMA_MODEL"] = model_var

        # Test connection button
        self.ollama_status = tk.Label(self.content, text="", bg=BG_DARK, fg=FG_DIM,
                                       font=("Consolas", 9))
        self.ollama_status.pack(pady=(8, 0), anchor="w")

        def test_ollama():
            url = url_var.get().strip().rstrip("/")
            self.ollama_status.configure(text="Testing...", fg=AMBER)
            self.root.update()
            try:
                import requests
                resp = requests.get(f"{url}/api/tags", timeout=5)
                if resp.status_code == 200:
                    models = [m["name"] for m in resp.json().get("models", [])]
                    target = model_var.get().strip()
                    if any(target in m for m in models):
                        self.ollama_status.configure(
                            text=f"Connected! Model '{target}' found. "
                                 f"{len(models)} models available.",
                            fg=GREEN)
                    else:
                        self.ollama_status.configure(
                            text=f"Connected but '{target}' not found. "
                                 f"Available: {', '.join(models[:5])}",
                            fg=AMBER)
                else:
                    self.ollama_status.configure(
                        text=f"HTTP {resp.status_code} — Ollama may not be running",
                        fg=RED)
            except Exception as e:
                self.ollama_status.configure(
                    text=f"Cannot connect: {str(e)[:60]}. Ollama collectors will be skipped.",
                    fg=RED)

        tk.Button(self.content, text="  Test Connection  ", bg=ACCENT, fg="white",
                   font=("Segoe UI", 10, "bold"), borderwidth=0, padx=12, pady=4,
                   cursor="hand2", command=test_ollama).pack(pady=(12, 0), anchor="w")

        tk.Label(self.content, bg=BG_DARK, fg=FG_DIM, font=("Segoe UI", 9),
                  text="Install: ollama.com → ollama pull qwen2.5:14b").pack(
                      pady=(16, 0), anchor="w")

    # ── STEP 4: Watchlist ────────────────────────────────────

    def _step_watchlist(self):
        self.title_label.configure(text="Watchlist")
        self._current_entries = {}

        tk.Label(self.content, bg=BG_DARK, fg=FG_PRIMARY,
                  font=("Segoe UI", 11), justify="left", wraplength=620,
                  text=(
            "Choose which stocks to monitor. You can add or remove tickers "
            "at any time from the main app. The default list includes tech "
            "stocks, space companies, and dividend ETFs."
        )).pack(pady=(10, 12), anchor="w")

        # Editable text area with current watchlist
        self.watchlist_text = tk.Text(self.content, bg=BG_INPUT, fg=FG_PRIMARY,
                                       font=("Consolas", 11), height=8,
                                       insertbackground=FG_PRIMARY, wrap="word",
                                       borderwidth=1, relief="solid")
        self.watchlist_text.pack(fill="x", pady=4)
        self.watchlist_text.insert("1.0", ", ".join(self.watchlist))

        tk.Label(self.content, bg=BG_DARK, fg=FG_DIM, font=("Segoe UI", 9),
                  text="Enter tickers separated by commas. Example: AAPL, MSFT, TSLA, NVDA"
                  ).pack(anchor="w", pady=(2, 0))

        # Preset buttons
        presets = tk.Frame(self.content, bg=BG_DARK)
        presets.pack(fill="x", pady=(12, 0))
        tk.Label(presets, text="Presets:", bg=BG_DARK, fg=FG_SECONDARY,
                  font=("Segoe UI", 9)).pack(side="left", padx=(0, 8))

        def set_preset(tickers):
            self.watchlist_text.delete("1.0", "end")
            self.watchlist_text.insert("1.0", ", ".join(tickers))

        tk.Button(presets, text="Tech", bg=BG_CARD, fg=FG_PRIMARY,
                   font=("Segoe UI", 8), borderwidth=0, padx=8,
                   command=lambda: set_preset(
                       ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META", "AMD", "INTC"])
                   ).pack(side="left", padx=2)
        tk.Button(presets, text="Space", bg=BG_CARD, fg=FG_PRIMARY,
                   font=("Segoe UI", 8), borderwidth=0, padx=8,
                   command=lambda: set_preset(
                       ["LUNR", "RKLB", "ASTS", "RDW", "ASTR", "SPCE", "BA", "LMT"])
                   ).pack(side="left", padx=2)
        tk.Button(presets, text="Dividend ETFs", bg=BG_CARD, fg=FG_PRIMARY,
                   font=("Segoe UI", 8), borderwidth=0, padx=8,
                   command=lambda: set_preset(
                       ["NOBL", "RDVY", "SCHD", "VIG", "VYM", "IVV", "BND", "VONG", "SPMO", "VWO", "IJR"])
                   ).pack(side="left", padx=2)
        tk.Button(presets, text="Full Default", bg=BG_CARD, fg=FG_PRIMARY,
                   font=("Segoe UI", 8), borderwidth=0, padx=8,
                   command=lambda: set_preset(list(WATCHLIST))
                   ).pack(side="left", padx=2)

        count_label = tk.Label(self.content, bg=BG_DARK, fg=FG_DIM,
                                font=("Consolas", 9))
        count_label.pack(anchor="w", pady=(8, 0))

        def update_count(event=None):
            text = self.watchlist_text.get("1.0", "end").strip()
            tickers = [t.strip().upper() for t in text.replace("\n", ",").split(",") if t.strip()]
            count_label.configure(text=f"{len(tickers)} tickers | "
                                       f"Est. scan time: ~{len(tickers) * 3}s (quick) / "
                                       f"~{len(tickers) * 6}s (full)")

        self.watchlist_text.bind("<KeyRelease>", update_count)
        update_count()

    # ── STEP 5: Train ────────────────────────────────────────

    def _step_train(self):
        self.title_label.configure(text="Generate History & Train ML")
        self._current_entries = {}

        tk.Label(self.content, bg=BG_DARK, fg=FG_PRIMARY,
                  font=("Segoe UI", 11), justify="left", wraplength=620,
                  text=(
            "Click 'Generate & Train' to download historical price data "
            "and train the machine learning model. This takes 1-3 minutes "
            "depending on how many tickers you have.\n\n"
            "The ML model learns from historical price patterns and improves "
            "over time as it collects verified predictions."
        )).pack(pady=(10, 16), anchor="w")

        self.train_log = scrolledtext.ScrolledText(
            self.content, bg="#0d1117", fg=FG_PRIMARY, font=("Consolas", 9),
            wrap="word", borderwidth=0, height=16)
        self.train_log.pack(fill="both", expand=True, pady=4)

        self.train_progress = tk.Label(self.content, text="Ready",
                                        bg=BG_DARK, fg=FG_DIM,
                                        font=("Consolas", 10))
        self.train_progress.pack(anchor="w", pady=(4, 0))

    def _run_training(self):
        """Execute history generation, ML training, and first analysis."""
        self._save_all_settings()

        # Watchlist was already saved by _save_current_step before we got here

        self.next_btn.configure(state="disabled", text="Working...")
        self.back_btn.configure(state="disabled")

        def log(msg):
            self.train_log.insert("end", f"[{datetime.now().strftime('%H:%M:%S')}] {msg}\n")
            self.train_log.see("end")
            self.root.update_idletasks()

        def run():
            try:
                # Apply settings to environment
                for key, val in self.settings.items():
                    if val:
                        os.environ[key] = val

                # Step 1: Generate historical data
                log("Generating historical price data...")
                self.train_progress.configure(text="Downloading price history...", fg=AMBER)

                from stock_oracle.historical_trainer import generate_historical_samples
                total_samples = 0
                for i, ticker in enumerate(self.watchlist):
                    try:
                        n = generate_historical_samples(ticker, days_back=180)
                        total_samples += n
                        log(f"  {ticker}: {n} samples")
                    except Exception as e:
                        log(f"  {ticker}: error ({e})")
                    self.train_progress.configure(
                        text=f"History: {i+1}/{len(self.watchlist)} tickers...", fg=AMBER)

                log(f"Total: {total_samples} historical samples")

                # Step 2: Train ML
                if total_samples >= 50:
                    log("Training ML models...")
                    self.train_progress.configure(text="Training ML...", fg=AMBER)

                    from stock_oracle.historical_trainer import load_historical_training_data
                    from stock_oracle.ml.pipeline import StockPredictor
                    data = load_historical_training_data()
                    predictor = StockPredictor()
                    predictor.train(data)
                    log(f"ML trained on {len(data)} samples — 3 models saved")
                else:
                    log(f"Only {total_samples} samples — need 50+ for ML training")
                    log("ML will train automatically during monitoring")

                # Step 3: Initial analysis (first 5 tickers as a quick test)
                log("Running initial analysis...")
                self.train_progress.configure(text="Analyzing...", fg=AMBER)

                from stock_oracle.oracle import StockOracle
                oracle = StockOracle()
                for ticker in self.watchlist[:5]:
                    try:
                        r = oracle.analyze(ticker, verbose=False)
                        pred = r.get("prediction", "?")
                        sig = r.get("signal", 0)
                        log(f"  {ticker}: {pred} (signal: {sig:+.4f})")
                    except Exception as e:
                        log(f"  {ticker}: error ({e})")

                if len(self.watchlist) > 5:
                    log(f"  ... +{len(self.watchlist)-5} more will analyze on first monitoring scan")

                log("")
                log("Setup complete! Click 'Launch Stock Oracle' to start.")
                self.train_progress.configure(text="Done!", fg=GREEN)

            except Exception as e:
                log(f"Error: {e}")
                self.train_progress.configure(text=f"Error: {e}", fg=RED)

            # Re-enable nav
            self.next_btn.configure(state="normal", text="Launch Stock Oracle")
            self.back_btn.configure(state="normal")

            # Move to done step
            self.current_step = len(self.steps) - 1

        threading.Thread(target=run, daemon=True).start()

    # ── STEP 6: Done ─────────────────────────────────────────

    def _step_done(self):
        self.title_label.configure(text="Ready to Go!")
        self._current_entries = {}

        tk.Label(self.content, bg=BG_DARK, fg=GREEN,
                  font=("Segoe UI", 14, "bold"),
                  text="Setup Complete").pack(pady=(30, 16))

        features = []
        if self.settings.get("FINNHUB_API_KEY"):
            features.append("Real-time quotes (Finnhub)")
        if self.settings.get("ANTHROPIC_API_KEY"):
            features.append("Claude AI advisor")
        if self.settings.get("OLLAMA_URL"):
            features.append("Local NLP (Ollama)")
        features.append(f"{len(self.watchlist)} tickers in watchlist")
        features.append("Auto-monitoring during market hours")
        features.append("System tray background mode")

        for f in features:
            tk.Label(self.content, bg=BG_DARK, fg=FG_PRIMARY,
                      font=("Segoe UI", 10), text=f"  ✓  {f}").pack(anchor="w", padx=40, pady=2)

        tk.Label(self.content, bg=BG_DARK, fg=FG_DIM,
                  font=("Segoe UI", 9), wraplength=500, justify="left",
                  text=("\nThe app will auto-start monitoring when the market is open. "
                        "Closing the window minimizes to the system tray — "
                        "use the Quit button to fully exit.\n\n"
                        "All settings can be changed later from the Settings menu.")
                  ).pack(pady=(20, 0), anchor="w", padx=40)

    # ── Save / Finish ────────────────────────────────────────

    def _save_all_settings(self):
        """Write settings to .env file."""
        env_path = Path("stock_oracle/.env")
        lines = [
            "# Stock Oracle Settings",
            f"# Generated by Setup Wizard: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
        ]
        for key, value in sorted(self.settings.items()):
            if value:
                lines.append(f"{key}={value}")

        env_path.write_text("\n".join(lines) + "\n")

        # Also save watchlist
        wl_path = DATA_DIR / "watchlist.json"
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        with open(wl_path, "w") as f:
            json.dump(self.watchlist, f)

    def _finish(self):
        """Mark setup complete and close wizard."""
        self._save_current_step()
        self._save_all_settings()

        # Watchlist already saved into self.watchlist by _save_current_step()
        # and written to disk by _save_all_settings() — no need to read widget

        mark_setup_complete()
        self.completed = True
        self.root.destroy()

    def run(self) -> bool:
        """Run the wizard. Returns True if completed (launch main app)."""
        self.root.mainloop()
        return self.completed
