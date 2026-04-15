"""
Stock Oracle GUI
================
Single desktop application for everything:
  - Add/remove stocks to watch
  - Live monitoring with auto-refresh
  - Signal breakdown per stock
  - ML training controls
  - Real-time price updates
  - Alert notifications

Launch:  python -m stock_oracle.gui
"""
import json
import threading
import time
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional
import queue
import sys
import os

# Ensure the parent directory is in path
sys.path.insert(0, str(Path(__file__).parent.parent))

from stock_oracle.config import (
    WATCHLIST, DATA_DIR, CACHE_DIR, MODEL_DIR, PREDICTION_HORIZON_DAYS,
    BG_DARK, BG_PANEL, BG_CARD, BG_INPUT,
    FG_PRIMARY, FG_SECONDARY, FG_DIM,
    GREEN, RED, AMBER, BLUE, PURPLE, BORDER, ACCENT,
)


class StockOracleGUI:
    """Main application window."""

    def __init__(self):
        # ── Windows DPI awareness — MUST be before any tkinter calls ──
        try:
            import ctypes
            ctypes.windll.shcore.SetProcessDpiAwareness(1)  # Per-monitor DPI aware
        except Exception:
            try:
                import ctypes
                ctypes.windll.user32.SetProcessDPIAware()  # Fallback for older Windows
            except Exception:
                pass

        self.root = tk.Tk()
        self.root.title("Stock Oracle")
        self.root.geometry("1400x900")
        self.root.configure(bg=BG_DARK)
        self.root.minsize(1000, 700)

        # Apply DPI scaling to tkinter
        try:
            self.root.tk.call('tk', 'scaling', self.root.winfo_fpixels('1i') / 72.0)
        except Exception:
            pass

        # State
        self.watchlist: List[str] = list(WATCHLIST)
        self.results: Dict[str, Dict] = {}
        self.monitoring = False
        self.monitor_interval = 300  # seconds (5 min default)
        self._scanning = False  # Prevent concurrent scan runs
        self._force_full_scan = False  # Set by Analyze All to interrupt countdown
        self.msg_queue = queue.Queue()
        self.oracle = None
        self._oracle_lock = threading.Lock()
        self._tray_icon = None
        self._quitting = False

        # Load saved watchlist
        self._load_watchlist()

        # Build UI
        self._build_styles()
        self._build_layout()
        self._populate_watchlist()

        # Start queue processor
        self.root.after(100, self._process_queue)

        # Log startup
        self._log("Stock Oracle GUI started")
        self._log(f"Watchlist: {', '.join(self.watchlist)}")
        self._check_ml_status()

    def _build_styles(self):
        style = ttk.Style()
        style.theme_use("clam")

        style.configure("Dark.TFrame", background=BG_DARK)
        style.configure("Panel.TFrame", background=BG_PANEL)
        style.configure("Card.TFrame", background=BG_CARD)

        style.configure("Dark.TLabel", background=BG_DARK, foreground=FG_PRIMARY,
                         font=("Segoe UI", 10))
        style.configure("Panel.TLabel", background=BG_PANEL, foreground=FG_PRIMARY,
                         font=("Segoe UI", 10))
        style.configure("Header.TLabel", background=BG_DARK, foreground=FG_PRIMARY,
                         font=("Segoe UI", 16, "bold"))
        style.configure("SubHeader.TLabel", background=BG_PANEL, foreground=FG_SECONDARY,
                         font=("Segoe UI", 9))
        style.configure("Ticker.TLabel", background=BG_CARD, foreground=FG_PRIMARY,
                         font=("Consolas", 14, "bold"))
        style.configure("Signal.TLabel", background=BG_CARD, foreground=FG_PRIMARY,
                         font=("Consolas", 11))

        style.configure("Accent.TButton", background=ACCENT, foreground="white",
                         font=("Segoe UI", 10, "bold"), padding=(12, 6))
        style.map("Accent.TButton", background=[("active", "#1a5cc7")])

        style.configure("Dark.TButton", background=BG_CARD, foreground=FG_PRIMARY,
                         font=("Segoe UI", 9), padding=(8, 4))
        style.map("Dark.TButton", background=[("active", "#2d333b")])

        style.configure("Green.TButton", background="#238636", foreground="white",
                         font=("Segoe UI", 9, "bold"), padding=(8, 4))
        style.configure("Red.TButton", background="#da3633", foreground="white",
                         font=("Segoe UI", 9, "bold"), padding=(8, 4))

        style.configure("Dark.TEntry", fieldbackground=BG_INPUT, foreground=FG_PRIMARY,
                         insertcolor=FG_PRIMARY)

    def _build_layout(self):
        # ── Top Bar ────────────────────────────────────────────
        top = ttk.Frame(self.root, style="Dark.TFrame")
        top.pack(fill="x", padx=16, pady=(12, 0))

        ttk.Label(top, text="Stock Oracle", style="Header.TLabel").pack(side="left")

        # Market session indicator
        self.market_label = tk.Label(top, text="", bg=BG_DARK, fg=FG_SECONDARY,
                                      font=("Segoe UI", 10))
        self.market_label.pack(side="left", padx=(12, 0))
        self._update_market_session()

        # Update market session every 30 seconds
        def _market_tick():
            self._update_market_session()
            self.root.after(30000, _market_tick)
        self.root.after(30000, _market_tick)

        # Monitor controls (right side of top bar)
        ctrl = ttk.Frame(top, style="Dark.TFrame")
        ctrl.pack(side="right")

        self.monitor_btn = ttk.Button(ctrl, text="Start Monitoring",
                                       style="Green.TButton", command=self._toggle_monitor)
        self.monitor_btn.pack(side="right", padx=(8, 0))

        ttk.Button(ctrl, text="Settings", style="Dark.TButton",
                    command=self._open_settings).pack(side="right", padx=(8, 0))
        ttk.Button(ctrl, text="News", style="Dark.TButton",
                    command=self._open_news_feed).pack(side="right", padx=(4, 0))
        ttk.Button(ctrl, text="Help", style="Dark.TButton",
                    command=self._open_help).pack(side="right", padx=(4, 0))
        ttk.Button(ctrl, text="Quit", style="Dark.TButton",
                    command=self._real_quit).pack(side="right", padx=(4, 0))

        ttk.Label(ctrl, text="Interval (sec):", style="Dark.TLabel").pack(side="right", padx=(8, 2))
        self.interval_var = tk.StringVar(value="300")
        interval_entry = ttk.Entry(ctrl, textvariable=self.interval_var, width=5,
                                    style="Dark.TEntry")
        interval_entry.pack(side="right")

        self.status_label = ttk.Label(ctrl, text="Idle", style="Dark.TLabel",
                                       foreground=FG_SECONDARY)
        self.status_label.pack(side="right", padx=(0, 16))

        # ── Main Content (3 columns) ──────────────────────────
        main = ttk.Frame(self.root, style="Dark.TFrame")
        main.pack(fill="both", expand=True, padx=16, pady=12)
        main.columnconfigure(1, weight=1)
        main.rowconfigure(0, weight=1)

        # Left: Watchlist
        self._build_watchlist_panel(main)

        # Center: Results
        self._build_results_panel(main)

        # Right: Details + Controls
        self._build_details_panel(main)

    # ── LEFT: Watchlist Panel ──────────────────────────────────

    def _build_watchlist_panel(self, parent):
        panel = ttk.Frame(parent, style="Panel.TFrame", width=200)
        panel.grid(row=0, column=0, sticky="nsew", padx=(0, 8))
        panel.grid_propagate(False)

        # Header
        hdr = ttk.Frame(panel, style="Panel.TFrame")
        hdr.pack(fill="x", padx=8, pady=(8, 4))
        ttk.Label(hdr, text="Watchlist", style="Panel.TLabel",
                   font=("Segoe UI", 12, "bold")).pack(side="left")

        # Add ticker input
        add_frame = ttk.Frame(panel, style="Panel.TFrame")
        add_frame.pack(fill="x", padx=8, pady=4)

        self.ticker_entry = ttk.Entry(add_frame, style="Dark.TEntry", width=10)
        self.ticker_entry.pack(side="left", fill="x", expand=True, padx=(0, 4))
        self.ticker_entry.bind("<Return>", lambda e: self._add_ticker())

        ttk.Button(add_frame, text="Add", style="Green.TButton",
                    command=self._add_ticker).pack(side="right")

        # Scrollable ticker list
        list_frame = ttk.Frame(panel, style="Panel.TFrame")
        list_frame.pack(fill="both", expand=True, padx=4, pady=4)

        self.watchlist_listbox = tk.Listbox(
            list_frame, bg=BG_DARK, fg=FG_PRIMARY, font=("Consolas", 12),
            selectbackground=ACCENT, selectforeground="white",
            borderwidth=0, highlightthickness=0, activestyle="none",
        )
        self.watchlist_listbox.pack(fill="both", expand=True, padx=4)
        self.watchlist_listbox.bind("<<ListboxSelect>>", self._on_ticker_select)

        # Buttons
        btn_frame = ttk.Frame(panel, style="Panel.TFrame")
        btn_frame.pack(fill="x", padx=8, pady=(0, 8))

        ttk.Button(btn_frame, text="Remove", style="Red.TButton",
                    command=self._remove_ticker).pack(side="left")
        ttk.Button(btn_frame, text="Analyze", style="Accent.TButton",
                    command=self._analyze_selected).pack(side="right")
        ttk.Button(btn_frame, text="Analyze All", style="Dark.TButton",
                    command=self._analyze_all).pack(side="right", padx=4)

    # ── CENTER: Results Panel ──────────────────────────────────

    def _build_results_panel(self, parent):
        panel = ttk.Frame(parent, style="Dark.TFrame")
        panel.grid(row=0, column=1, sticky="nsew", padx=4)

        # ── Sort bar ──
        sort_bar = tk.Frame(panel, bg=BG_PANEL, padx=8, pady=4)
        sort_bar.pack(fill="x")

        tk.Label(sort_bar, text="Sort:", bg=BG_PANEL, fg=FG_SECONDARY,
                  font=("Consolas", 9)).pack(side="left")

        self.sort_var = tk.StringVar(value="signal")
        for label, val in [("Signal", "signal"), ("Conviction", "conviction"),
                           ("Change", "change"), ("Name", "name")]:
            rb = tk.Radiobutton(sort_bar, text=label, variable=self.sort_var, value=val,
                                 bg=BG_PANEL, fg=FG_SECONDARY, selectcolor=BG_DARK,
                                 activebackground=BG_PANEL, activeforeground=ACCENT,
                                 font=("Consolas", 9), indicatoron=0, padx=6, pady=2,
                                 borderwidth=0, highlightthickness=0,
                                 command=self._resort_cards)
            rb.pack(side="left", padx=2)

        # Quick Scan button
        tk.Button(sort_bar, text="Quick Scan", bg=ACCENT, fg="white",
                   font=("Segoe UI", 8, "bold"), borderwidth=0, padx=8, pady=2,
                   cursor="hand2", command=self._quick_scan_all).pack(side="right")

        # Breakout Scan button
        tk.Button(sort_bar, text="Breakout Scan", bg="#e6a817", fg="black",
                   font=("Segoe UI", 8, "bold"), borderwidth=0, padx=8, pady=2,
                   cursor="hand2", command=self._breakout_scan).pack(side="right", padx=(0, 6))

        # ── Summary bar ──
        self.summary_bar = tk.Frame(panel, bg=BG_DARK, padx=8, pady=2)
        self.summary_bar.pack(fill="x")
        self.summary_label = tk.Label(self.summary_bar, text="", bg=BG_DARK,
                                       fg=FG_DIM, font=("Consolas", 9))
        self.summary_label.pack(side="left")

        # ── Market Overview bar ──
        self.market_bar = tk.Frame(panel, bg="#1a1f2e", padx=8, pady=4)
        self.market_bar.pack(fill="x", pady=(2, 0))
        self.market_overview_label = tk.Label(self.market_bar, text="Market: loading...",
                                      bg="#1a1f2e", fg=FG_DIM, font=("Consolas", 9),
                                      anchor="w")
        self.market_overview_label.pack(fill="x")

        # Results scrollable canvas
        canvas = tk.Canvas(panel, bg=BG_DARK, highlightthickness=0)
        scrollbar = ttk.Scrollbar(panel, orient="vertical", command=canvas.yview)
        self.results_frame = ttk.Frame(canvas, style="Dark.TFrame")

        self.results_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        canvas.create_window((0, 0), window=self.results_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        # Bind mousewheel
        def _on_mousewheel(event):
            canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        canvas.bind_all("<MouseWheel>", _on_mousewheel)

        self.result_cards = {}

    # ── RIGHT: Details + Controls Panel ────────────────────────

    def _build_details_panel(self, parent):
        panel = ttk.Frame(parent, style="Panel.TFrame", width=320)
        panel.grid(row=0, column=2, sticky="nsew", padx=(8, 0))
        panel.grid_propagate(False)

        # Details header
        ttk.Label(panel, text="Signal Details", style="Panel.TLabel",
                   font=("Segoe UI", 12, "bold")).pack(padx=8, pady=(8, 4), anchor="w")

        # Signal detail area
        self.detail_text = scrolledtext.ScrolledText(
            panel, bg=BG_DARK, fg=FG_PRIMARY, font=("Consolas", 9),
            wrap="word", borderwidth=0, highlightthickness=0, height=15,
        )
        self.detail_text.pack(fill="both", expand=True, padx=8, pady=4)

        # ML section
        ml_frame = ttk.Frame(panel, style="Panel.TFrame")
        ml_frame.pack(fill="x", padx=8, pady=(8, 4))

        ttk.Label(ml_frame, text="Machine Learning", style="Panel.TLabel",
                   font=("Segoe UI", 11, "bold")).pack(anchor="w")

        self.ml_status_label = ttk.Label(ml_frame, text="Not trained",
                                          style="SubHeader.TLabel")
        self.ml_status_label.pack(anchor="w", pady=2)

        ml_btns = ttk.Frame(ml_frame, style="Panel.TFrame")
        ml_btns.pack(fill="x", pady=4)

        ttk.Button(ml_btns, text="Generate History", style="Dark.TButton",
                    command=self._generate_historical).pack(side="left", padx=(0, 4))
        ttk.Button(ml_btns, text="Train ML", style="Accent.TButton",
                    command=self._train_ml).pack(side="left", padx=4)
        ttk.Button(ml_btns, text="Export CSV", style="Dark.TButton",
                    command=self._export_csv).pack(side="right")

        # ── Accuracy Scorecard ──
        acc_frame = ttk.Frame(panel, style="Panel.TFrame")
        acc_frame.pack(fill="x", padx=8, pady=(8, 4))

        acc_header = ttk.Frame(acc_frame, style="Panel.TFrame")
        acc_header.pack(fill="x")
        ttk.Label(acc_header, text="Prediction Accuracy", style="Panel.TLabel",
                   font=("Segoe UI", 11, "bold")).pack(side="left")
        ttk.Button(acc_header, text="Verify Now", style="Dark.TButton",
                    command=self._verify_predictions).pack(side="right")

        self.accuracy_label = tk.Label(acc_frame, text="No verified predictions yet",
                                        bg=BG_PANEL, fg=FG_SECONDARY,
                                        font=("Consolas", 9), anchor="w", justify="left")
        self.accuracy_label.pack(fill="x", pady=2)

        self.accuracy_detail = tk.Label(acc_frame, text="",
                                         bg=BG_PANEL, fg=FG_DIM,
                                         font=("Consolas", 8), anchor="w", justify="left")
        self.accuracy_detail.pack(fill="x")

        # Auto-verify predictions every 30 minutes
        self._update_accuracy_display()
        def _verify_tick():
            self._auto_verify()
            self.root.after(1800000, _verify_tick)  # 30 min
        self.root.after(60000, _verify_tick)  # First check after 1 min

        # ── Claude Advisor Section ──
        advisor_frame = ttk.Frame(panel, style="Panel.TFrame")
        advisor_frame.pack(fill="x", padx=8, pady=(8, 4))

        advisor_header = ttk.Frame(advisor_frame, style="Panel.TFrame")
        advisor_header.pack(fill="x")
        ttk.Label(advisor_header, text="Claude Advisor", style="Panel.TLabel",
                   font=("Segoe UI", 11, "bold")).pack(side="left")

        self.advisor_status_label = tk.Label(advisor_frame, text="Not configured — add API key in Settings",
                                              bg=BG_PANEL, fg=FG_DIM,
                                              font=("Consolas", 8), anchor="w")
        self.advisor_status_label.pack(fill="x", pady=2)

        advisor_btns = ttk.Frame(advisor_frame, style="Panel.TFrame")
        advisor_btns.pack(fill="x", pady=2)
        ttk.Button(advisor_btns, text="Ask Claude", style="Accent.TButton",
                    command=self._ask_claude).pack(side="left", padx=(0, 4))
        ttk.Button(advisor_btns, text="Session Review", style="Dark.TButton",
                    command=self._claude_session_review).pack(side="left", padx=4)

        self._update_advisor_status()

        # Log area
        ttk.Label(panel, text="Activity Log", style="Panel.TLabel",
                   font=("Segoe UI", 11, "bold")).pack(padx=8, pady=(8, 2), anchor="w")

        self.log_text = scrolledtext.ScrolledText(
            panel, bg=BG_DARK, fg=FG_SECONDARY, font=("Consolas", 8),
            wrap="word", borderwidth=0, highlightthickness=0, height=10,
        )
        self.log_text.pack(fill="both", expand=True, padx=8, pady=(0, 8))

    # ── Watchlist Management ───────────────────────────────────

    def _get_live_price(self, result: Dict) -> Dict:
        """Extract live price info from Finnhub or Yahoo signal."""
        price = 0.0
        change = 0.0
        regular_change = 0.0
        extended_change = 0.0
        session = ""

        # Get market session ONCE (same for all stocks)
        try:
            from stock_oracle.collectors.finnhub_collector import get_market_session
            ms = get_market_session()
            session = {"regular": "LIVE", "pre_market": "PRE",
                       "after_hours": "AH", "closed": ""}.get(
                ms.get("session", ""), "")
        except Exception:
            pass

        # Try Finnhub first (real-time)
        for s in result.get("signals", []):
            if s.get("collector") == "finnhub_realtime":
                raw = s.get("raw_data") if isinstance(s.get("raw_data"), dict) else {}
                if raw.get("price"):
                    price = raw["price"]
                    # During AH/PRE: show the AH-specific change (from regular close)
                    # During regular/closed: show daily change (from prev close)
                    regular_change = raw.get("regular_change", raw.get("daily_change", 0))
                    extended_change = raw.get("extended_change", 0)

                    if session in ("AH", "PRE") and raw.get("extended_price"):
                        change = extended_change  # AH delta from regular close
                    else:
                        change = raw.get("daily_change", 0)  # Total from prev close

                    return {"price": price, "change": change,
                            "regular_change": regular_change,
                            "extended_change": extended_change,
                            "session": session}

        # Fallback to Yahoo price_data
        price_data = result.get("price_data", [])
        if price_data:
            latest = price_data[-1]
            price = latest.get("close", 0)
            if len(price_data) >= 2:
                prev = price_data[-2].get("close", price)
                change = (price - prev) / prev if prev > 0 else 0

        # Also check Yahoo signal raw_data
        if not price:
            for s in result.get("signals", []):
                if s.get("collector") == "yahoo_finance":
                    raw = s.get("raw_data") if isinstance(s.get("raw_data"), dict) else {}
                    if raw.get("price"):
                        price = raw["price"]

        return {"price": price, "change": change,
                "regular_change": regular_change,
                "extended_change": extended_change,
                "session": session}

    def _get_intraday_prediction(self, ticker: str, trend: Dict, result: Dict) -> tuple:
        """
        Determine intraday prediction from session trend data.
        Returns (label, color) for the intraday badge.
        
        Separate from the 5-day prediction. Looks at:
        - Signal direction over the monitoring session
        - Price movement within the session
        - Whether momentum is building or fading
        """
        signal_trend = trend.get("signal_trend", 0)   # Signal change per scan
        price_pct = trend.get("price_change_pct", 0)   # Price % change over session
        direction = trend.get("direction", "stable")
        current_signal = result.get("signal", 0)

        # Score based on multiple factors
        score = 0.0

        # 1. Price direction within session (strongest weight)
        if abs(price_pct) > 0.1:
            score += price_pct * 2.0  # Scale price % into score

        # 2. Signal trend (is analysis getting more bullish/bearish?)
        score += signal_trend * 50  # Amplify small per-scan changes

        # 3. Current signal leans
        score += current_signal * 0.3

        # 4. Direction classification bonus
        if "strengthening_bull" in direction:
            score += 0.15
        elif "strengthening_bear" in direction:
            score -= 0.15
        elif "weakening" in direction:
            score -= 0.05
        elif "recovering" in direction:
            score += 0.05

        # Classify
        if score > 0.15:
            return ("RISING", GREEN)
        elif score < -0.15:
            return ("FALLING", RED)
        elif score > 0.05:
            return ("LEAN UP", "#2d8659")   # Muted green
        elif score < -0.05:
            return ("LEAN DN", "#a63d40")   # Muted red
        else:
            return ("FLAT", "#666666")

    def _populate_watchlist(self):
        self.watchlist_listbox.delete(0, tk.END)
        for ticker in self.watchlist:
            display = f"  {ticker}"
            if ticker in self.results:
                r = self.results[ticker]
                pred = r.get("prediction", "?")
                pinfo = self._get_live_price(r)
                price = pinfo["price"]
                change = pinfo["change"]
                session = pinfo["session"]

                if price > 0:
                    sess_str = f" {session}" if session else ""
                    # During AH/PRE: show regular + AH change
                    if session in ("AH", "PRE") and pinfo.get("regular_change"):
                        reg_str = f"{pinfo['regular_change']:+.1%}"
                        ext_str = f"{pinfo['extended_change']:+.1%}" if pinfo.get("extended_change") else ""
                        display = f"  {ticker:5s} ${price:<8.2f} {reg_str} {ext_str}{sess_str}"
                    else:
                        chg_str = f"{change:+.1%}" if change else ""
                        display = f"  {ticker:5s} ${price:<8.2f} {chg_str:>6s}{sess_str}"
                else:
                    icon = "+" if r.get("signal", 0) > 0.1 else "-" if r.get("signal", 0) < -0.1 else " "
                    display = f"{icon} {ticker:6s} {pred:8s} {r.get('signal',0):+.3f}"

            self.watchlist_listbox.insert(tk.END, display)

            # Color code by price change direction
            idx = self.watchlist.index(ticker)
            if ticker in self.results:
                pinfo = self._get_live_price(self.results[ticker])
                if pinfo["change"] > 0.001:
                    self.watchlist_listbox.itemconfig(idx, fg=GREEN)
                elif pinfo["change"] < -0.001:
                    self.watchlist_listbox.itemconfig(idx, fg=RED)
                else:
                    pred = self.results[ticker].get("prediction", "")
                    if pred == "BULLISH":
                        self.watchlist_listbox.itemconfig(idx, fg=GREEN)
                    elif pred == "BEARISH":
                        self.watchlist_listbox.itemconfig(idx, fg=RED)
                    else:
                        self.watchlist_listbox.itemconfig(idx, fg=AMBER)

    def _add_ticker(self):
        ticker = self.ticker_entry.get().strip().upper()
        if not ticker:
            return
        if ticker in self.watchlist:
            self._log(f"{ticker} already in watchlist")
            return

        self.watchlist.append(ticker)
        self._save_watchlist()
        self._populate_watchlist()
        self.ticker_entry.delete(0, tk.END)
        self._log(f"Added {ticker} to watchlist")

        # Auto-analyze the new ticker
        self._analyze_ticker(ticker)

    def _remove_ticker(self):
        sel = self.watchlist_listbox.curselection()
        if not sel:
            return
        idx = sel[0]
        ticker = self.watchlist[idx]
        self.watchlist.remove(ticker)
        self.results.pop(ticker, None)
        self._save_watchlist()
        self._populate_watchlist()
        self._log(f"Removed {ticker}")

    def _on_ticker_select(self, event):
        sel = self.watchlist_listbox.curselection()
        if not sel:
            return
        ticker = self.watchlist[sel[0]]
        self._show_details(ticker)

    def _save_watchlist(self):
        path = DATA_DIR / "gui_watchlist.json"
        path.parent.mkdir(exist_ok=True)
        path.write_text(json.dumps(self.watchlist))

    def _load_watchlist(self):
        path = DATA_DIR / "gui_watchlist.json"
        if path.exists():
            try:
                self.watchlist = json.loads(path.read_text())
            except Exception:
                pass

    # ── Analysis ───────────────────────────────────────────────

    def _get_oracle(self):
        """Lazy-load oracle (heavy import)."""
        with self._oracle_lock:
            if self.oracle is None:
                self._log("Loading Oracle engine...")
                from stock_oracle.oracle import StockOracle
                self.oracle = StockOracle(use_ml=True, parallel=True)
                self._log(f"Oracle ready ({len(self.oracle.collectors)} collectors)")
            return self.oracle

    def _analyze_selected(self):
        sel = self.watchlist_listbox.curselection()
        if not sel:
            return
        ticker = self.watchlist[sel[0]]
        self._analyze_ticker(ticker)

    def _analyze_all(self):
        """Analyze entire watchlist in background."""
        # During monitoring: interrupt countdown, force a full scan
        if self.monitoring:
            if self._scanning:
                self._log("Scan in progress — full scan will run next")
            else:
                self._log("Interrupting countdown — full scan starting")
            self._force_full_scan = True
            return

        # Not monitoring: run standalone full scan
        if self._scanning:
            self._log("Scan already in progress, skipping")
            return
        self._scanning = True
        self._log(f"Analyzing {len(self.watchlist)} tickers...")
        self.status_label.configure(text="Analyzing...", foreground=AMBER)

        def run():
            try:
                oracle = self._get_oracle()
                oracle.skip_slow = False  # Full analysis uses all collectors
                for i, ticker in enumerate(self.watchlist):
                    try:
                        self.msg_queue.put(("status", f"Analyzing {ticker} ({i+1}/{len(self.watchlist)})"))
                        result = oracle.analyze(ticker, verbose=False)
                        self.msg_queue.put(("result", ticker, result))
                    except Exception as e:
                        self.msg_queue.put(("log", f"Error analyzing {ticker}: {e}"))

                self.msg_queue.put(("status", f"Done - {datetime.now().strftime('%H:%M:%S')}"))
                self.msg_queue.put(("log", f"Analysis complete for {len(self.watchlist)} tickers"))
                self.msg_queue.put(("refresh", None))
            finally:
                self._scanning = False

        threading.Thread(target=run, daemon=True).start()

    def _analyze_ticker(self, ticker: str):
        """Analyze a single ticker in background."""
        self._log(f"Analyzing {ticker}...")
        self.status_label.configure(text=f"Analyzing {ticker}...", foreground=AMBER)

        def run():
            try:
                oracle = self._get_oracle()
                result = oracle.analyze(ticker, verbose=False)
                self.msg_queue.put(("result", ticker, result))
                self.msg_queue.put(("status", f"Done - {datetime.now().strftime('%H:%M:%S')}"))
            except Exception as e:
                self.msg_queue.put(("log", f"Error: {e}"))
                self.msg_queue.put(("status", "Error"))

        threading.Thread(target=run, daemon=True).start()

    def _resort_cards(self):
        """Re-sort result cards based on selected sort order."""
        if not self.results:
            return

        sort_key = self.sort_var.get()
        tickers = list(self.results.keys())

        if sort_key == "signal":
            tickers.sort(key=lambda t: self.results[t].get("signal", 0), reverse=True)
        elif sort_key == "conviction":
            tickers.sort(key=lambda t: self.results[t].get("weighted_prediction", {}).get("core_analysis_score", 0), reverse=True)
        elif sort_key == "change":
            tickers.sort(key=lambda t: self._get_live_price(self.results[t]).get("change", 0), reverse=True)
        elif sort_key == "name":
            tickers.sort()

        # Rebuild all cards in new order
        for card in self.result_cards.values():
            card.pack_forget()
        for ticker in tickers:
            if ticker in self.result_cards:
                self.result_cards[ticker].pack(fill="x", padx=4, pady=3)

    def _breakout_scan(self):
        """Scan all watchlist tickers for breakout potential."""
        self._log("Breakout scan starting...")
        self.status_label.configure(text="Breakout Scan...", foreground=AMBER)

        def run():
            try:
                from stock_oracle.breakout_detector import BreakoutDetector
                detector = BreakoutDetector()
                scores = detector.scan(self.watchlist, self.results)
                self.msg_queue.put(("_breakout_results", scores))
            except Exception as e:
                self.msg_queue.put(("log", f"Breakout scan error: {e}"))
                self.msg_queue.put(("status", "Breakout scan failed"))

        threading.Thread(target=run, daemon=True).start()

    def _show_breakout_results(self, scores: List[Dict]):
        """Display breakout scan results in a popup window with timeframe info."""
        self.status_label.configure(text="Breakout scan complete", foreground=GREEN)
        self._log(f"Breakout scan: {len(scores)} tickers scored")

        # Log top results to main log
        top = [s for s in scores if s["score"] >= 30]
        if top:
            for s in top[:5]:
                tf = s.get("timeframe", "")
                tf_str = f" [{tf}]" if tf else ""
                self._log(f"  Breakout: {s['ticker']} = {s['score']} {s['grade']}{tf_str}")
        else:
            self._log("  No breakout setups detected (all scores <30)")

        popup = tk.Toplevel(self.root)
        popup.title("Breakout Scanner")
        popup.geometry("850x650")
        popup.configure(bg=BG_DARK)
        popup.transient(self.root)

        # Store scores for filtering
        popup._all_scores = scores

        # Header
        hdr = tk.Frame(popup, bg=BG_DARK)
        hdr.pack(fill="x", padx=12, pady=(12, 4))
        tk.Label(hdr, text="Breakout Scanner", bg=BG_DARK, fg=ACCENT,
                 font=("Segoe UI", 16, "bold")).pack(side="left")
        timestamp = datetime.now().strftime("%H:%M:%S")
        tk.Label(hdr, text=f"  scanned {timestamp}", bg=BG_DARK, fg=FG_DIM,
                 font=("Segoe UI", 9)).pack(side="left", padx=(8, 0))

        # Legend + timeframe filter
        filter_frame = tk.Frame(popup, bg=BG_DARK)
        filter_frame.pack(fill="x", padx=12, pady=(0, 4))

        tk.Label(filter_frame, text="Grade:", bg=BG_DARK, fg=FG_DIM,
                 font=("Consolas", 8)).pack(side="left")
        for label, color in [("STRONG 70+", GREEN), ("BUILDING 50+", "#e6a817"),
                              ("EARLY 30+", AMBER)]:
            tk.Label(filter_frame, text=f" {label} ", bg=color, fg="white",
                     font=("Consolas", 8, "bold"), padx=3).pack(side="left", padx=1)

        # Timeframe filter buttons
        tf_frame = tk.Frame(popup, bg=BG_DARK)
        tf_frame.pack(fill="x", padx=12, pady=(0, 8))
        tk.Label(tf_frame, text="Filter:", bg=BG_DARK, fg=FG_DIM,
                 font=("Consolas", 9)).pack(side="left")

        # Results container (rebuilt on filter change)
        results_container = tk.Frame(popup, bg=BG_DARK)
        results_container.pack(fill="both", expand=True, padx=12, pady=(0, 8))

        def rebuild_results(filter_tf="ALL"):
            """Rebuild results list with optional timeframe filter."""
            # Clear existing
            for w in results_container.winfo_children():
                w.destroy()

            # Filter scores
            if filter_tf == "ALL":
                filtered = popup._all_scores
            else:
                tf_day_ranges = {
                    "SHORT": (0, 4),
                    "MEDIUM": (4, 14),
                    "LONG": (14, 999),
                }
                lo, hi = tf_day_ranges.get(filter_tf, (0, 999))
                filtered = [s for s in popup._all_scores
                           if lo <= s.get("timeframe_days", 0) < hi and s["score"] > 0]

            # Canvas + scrollbar
            canvas = tk.Canvas(results_container, bg=BG_DARK, highlightthickness=0)
            scrollbar = ttk.Scrollbar(results_container, orient="vertical",
                                       command=canvas.yview)
            inner = tk.Frame(canvas, bg=BG_DARK)
            inner.bind("<Configure>",
                        lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
            canvas.create_window((0, 0), window=inner, anchor="nw")
            canvas.configure(yscrollcommand=scrollbar.set)
            canvas.pack(side="left", fill="both", expand=True)
            scrollbar.pack(side="right", fill="y")

            # Populate
            for i, item in enumerate(filtered):
                ticker = item["ticker"]
                score = item["score"]
                grade = item["grade"]
                price = item.get("price", 0)
                signals = item.get("signals", [])
                timeframe = item.get("timeframe", "")
                tf_weights = item.get("timeframe_weights", {})

                # Grade color
                if grade == "STRONG":
                    grade_color = GREEN
                elif grade == "BUILDING":
                    grade_color = "#e6a817"
                elif grade == "EARLY":
                    grade_color = AMBER
                else:
                    grade_color = FG_DIM

                # Row frame
                row = tk.Frame(inner, bg=BG_CARD, padx=10, pady=6,
                               highlightbackground=BORDER, highlightthickness=1)
                row.pack(fill="x", pady=2)

                # Top line: rank + ticker + price + score + timeframe
                top_row = tk.Frame(row, bg=BG_CARD)
                top_row.pack(fill="x")

                # Left: rank + ticker + price
                tk.Label(top_row, text=f"#{i+1}", bg=BG_CARD, fg=FG_DIM,
                         font=("Consolas", 10)).pack(side="left")
                tk.Label(top_row, text=f"  {ticker:5s}", bg=BG_CARD, fg=FG_PRIMARY,
                         font=("Consolas", 13, "bold")).pack(side="left")
                if price > 0:
                    tk.Label(top_row, text=f"  ${price:.2f}", bg=BG_CARD, fg=FG_SECONDARY,
                             font=("Consolas", 11)).pack(side="left")

                # Score badge
                tk.Label(top_row, text=f"  {score}", bg=grade_color,
                         fg="white" if grade != "NONE" else BG_DARK,
                         font=("Segoe UI", 12, "bold"), padx=8, pady=1
                         ).pack(side="left", padx=(8, 4))
                tk.Label(top_row, text=grade, bg=BG_CARD, fg=grade_color,
                         font=("Segoe UI", 9, "bold")).pack(side="left")

                # Timeframe badge (right side)
                if timeframe and score > 0:
                    # Color based on speed
                    tf_days = item.get("timeframe_days", 0)
                    if tf_days <= 3:
                        tf_color = "#ff6b6b"   # Red-ish = urgent/fast
                    elif tf_days <= 7:
                        tf_color = "#e6a817"   # Gold = swing
                    elif tf_days <= 14:
                        tf_color = BLUE        # Blue = medium
                    else:
                        tf_color = "#888888"   # Gray = slow

                    tk.Label(top_row, text=f" {timeframe} ", bg=tf_color,
                             fg="white", font=("Segoe UI", 9, "bold"),
                             padx=6, pady=1).pack(side="right", padx=(4, 0))

                # Score bar
                bar_frame = tk.Frame(top_row, bg="#1a1f2e", height=8, width=100)
                bar_frame.pack(side="right", padx=(8, 4))
                bar_frame.pack_propagate(False)
                bar_w = max(2, score)
                tk.Frame(bar_frame, bg=grade_color, width=bar_w).place(
                    x=0, y=0, width=bar_w, relheight=1.0)

                # Company description line
                company = item.get("company", {})
                if company:
                    desc_frame = tk.Frame(row, bg=BG_CARD)
                    desc_frame.pack(fill="x", pady=(2, 0))

                    short_desc = company.get("short_desc", "")
                    if short_desc:
                        tk.Label(desc_frame, text=short_desc, bg=BG_CARD,
                                 fg=FG_DIM, font=("Segoe UI", 8),
                                 anchor="w", wraplength=700).pack(anchor="w")

                    # Show summary on click
                    summary_short = company.get("summary_short", "")
                    if summary_short:
                        summary_label = tk.Label(
                            desc_frame, text="", bg=BG_CARD,
                            fg=FG_SECONDARY, font=("Segoe UI", 8),
                            anchor="w", wraplength=700, justify="left"
                        )
                        summary_label.pack(anchor="w")
                        summary_label._visible = False
                        summary_label._full_text = summary_short

                        def toggle_summary(lbl=summary_label):
                            if lbl._visible:
                                lbl.configure(text="")
                                lbl._visible = False
                            else:
                                lbl.configure(text=lbl._full_text)
                                lbl._visible = True

                        # Click row to toggle summary
                        row.bind("<Button-1>", lambda e, fn=toggle_summary: fn())
                        for child in top_row.winfo_children():
                            child.bind("<Button-1>", lambda e, fn=toggle_summary: fn())
                        desc_frame.bind("<Button-1>", lambda e, fn=toggle_summary: fn())

                # Signal breakdown
                if score > 0 and signals:
                    sig_frame = tk.Frame(inner, bg=BG_PANEL, padx=12, pady=4)
                    sig_frame.pack(fill="x", pady=(0, 2))

                    for sig_item in sorted(signals, key=lambda x: -x[1]):
                        # Handle both old (name, pts, detail) and new (name, pts, detail, tf) format
                        name = sig_item[0]
                        pts = sig_item[1]
                        detail = sig_item[2]
                        tf = sig_item[3] if len(sig_item) > 3 else ""

                        if pts > 0:
                            tag_color = GREEN if pts >= 10 else "#e6a817" if pts >= 5 else FG_DIM
                            tf_tag = f" [{tf}]" if tf else ""
                            tag_text = f"{name}: {pts}pts{tf_tag} — {detail}"
                            tk.Label(sig_frame, text=tag_text, bg=BG_PANEL,
                                     fg=tag_color, font=("Consolas", 8),
                                     anchor="w").pack(anchor="w")

        # Create filter buttons
        for tf_label, tf_key, tf_desc in [
            ("All", "ALL", ""),
            ("Short (1-3d)", "SHORT", "quick pops"),
            ("Swing (1-2wk)", "MEDIUM", "swing trades"),
            ("Position (wks+)", "LONG", "longer holds"),
        ]:
            tk.Button(tf_frame, text=tf_label, bg=BG_CARD, fg=FG_PRIMARY,
                       font=("Segoe UI", 8, "bold"), borderwidth=0, padx=8, pady=2,
                       cursor="hand2",
                       command=lambda k=tf_key: rebuild_results(k)
                       ).pack(side="left", padx=2)

        # Initial build — show all
        rebuild_results("ALL")

        # Close button
        tk.Button(popup, text="Close", bg=BG_CARD, fg=FG_PRIMARY,
                   font=("Segoe UI", 10), borderwidth=0, padx=16, pady=4,
                   cursor="hand2", command=popup.destroy
                   ).pack(pady=(0, 12))

    def _quick_scan_all(self):
        """Run only the most accurate + essential collectors (fast ~3s per ticker)."""
        self._log(f"Quick scanning {len(self.watchlist)} tickers (proven collectors)...")
        self.status_label.configure(text="Quick Scan...", foreground=AMBER)

        def run():
            from stock_oracle.collectors.yahoo_finance import YahooFinanceCollector
            from stock_oracle.collectors.finnhub_collector import FinnhubCollector
            from stock_oracle.collectors.analysis import (
                TechnicalAnalysisCollector, FundamentalAnalysisCollector,
                ShortInterestCollector,
            )
            from stock_oracle.collectors.new_indicators import (
                MomentumQualityCollector, DividendVsTreasuryCollector,
            )
            from stock_oracle.collectors.sec_edgar import SECEdgarCollector
            from stock_oracle.ml.pipeline import StockPredictor

            # Mix of: essential price data + data-proven accurate collectors
            quick_collectors = [
                YahooFinanceCollector(),       # Price data (essential)
                FinnhubCollector(),            # Real-time price (essential)
                TechnicalAnalysisCollector(),  # RSI/MACD context (demoted but fast)
                FundamentalAnalysisCollector(),# P/E context
                ShortInterestCollector(),      # 43% accuracy
                MomentumQualityCollector(),    # 76% accuracy — Tier 1
                DividendVsTreasuryCollector(), # 92% accuracy — Tier 1
                SECEdgarCollector(),           # 70% accuracy — Tier 1
            ]
            predictor = StockPredictor()

            for i, ticker in enumerate(self.watchlist):
                try:
                    self.msg_queue.put(("status", f"Quick: {ticker} ({i+1}/{len(self.watchlist)})"))
                    signals = []
                    for c in quick_collectors:
                        try:
                            sig = c.collect(ticker)
                            signals.append(sig)
                        except Exception:
                            pass

                    sig_dicts = [s.to_dict() for s in signals]
                    wp = predictor.predict_weighted(sig_dicts)

                    # Build a lightweight result compatible with _update_card
                    result = {
                        "ticker": ticker,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "prediction": wp["prediction"],
                        "signal": wp["signal"],
                        "confidence": wp["confidence"],
                        "method": "quick_scan (Tier 1 only)",
                        "weighted_prediction": wp,
                        "ml_prediction": None,
                        "signals": sig_dicts,
                        "signal_summary": {
                            "bullish_count": sum(1 for s in sig_dicts if s.get("signal", 0) > 0.05),
                            "bearish_count": sum(1 for s in sig_dicts if s.get("signal", 0) < -0.05),
                            "neutral_count": sum(1 for s in sig_dicts if abs(s.get("signal", 0)) <= 0.05),
                        },
                        "price_data": [],
                    }

                    self.msg_queue.put(("result", ticker, result))
                except Exception as e:
                    self.msg_queue.put(("log", f"Quick scan error {ticker}: {e}"))

            self.msg_queue.put(("status", f"Quick Done - {datetime.now().strftime('%H:%M:%S')}"))
            self.msg_queue.put(("log", f"Quick scan complete ({len(self.watchlist)} tickers)"))
            self.msg_queue.put(("refresh", None))

        threading.Thread(target=run, daemon=True).start()

    def _update_summary(self):
        """Update the summary bar with overall portfolio view."""
        if not self.results:
            self.summary_label.configure(text="No results yet")
            return

        bullish = sum(1 for r in self.results.values() if r.get("prediction") == "BULLISH")
        bearish = sum(1 for r in self.results.values() if r.get("prediction") == "BEARISH")
        neutral = sum(1 for r in self.results.values() if r.get("prediction") == "NEUTRAL")
        total = len(self.results)

        # Average conviction
        convictions = [r.get("weighted_prediction", {}).get("core_analysis_score", 0)
                       for r in self.results.values()]
        avg_conv = sum(convictions) / len(convictions) if convictions else 0

        # Top picks
        top = sorted(self.results.items(), key=lambda x: x[1].get("signal", 0), reverse=True)
        top_pick = top[0][0] if top and top[0][1].get("signal", 0) > 0 else "none"

        text = (f"{bullish} bull / {bearish} bear / {neutral} neutral  |  "
                f"Avg conv: {avg_conv:.0%}  |  Top: {top_pick}")

        # Add intraday prediction counts and session info during monitoring
        if hasattr(self, 'session_tracker') and self.session_tracker and self.session_tracker.scan_number >= 2:
            sess = self.session_tracker.get_session_stats()
            rising = 0
            falling = 0
            flat = 0
            for ticker, result in self.results.items():
                trend = self.session_tracker.get_trend(ticker)
                if trend.get("scans", 0) >= 2:
                    intra_pred, _ = self._get_intraday_prediction(ticker, trend, result)
                    if intra_pred in ("RISING", "LEAN UP"):
                        rising += 1
                    elif intra_pred in ("FALLING", "LEAN DN"):
                        falling += 1
                    else:
                        flat += 1
            text += f"  |  Intra: {rising}↑ {falling}↓ {flat}→"
            text += f"  |  Scan #{sess['total_scans']}"
            if sess["intraday_verified"] > 0:
                text += f" ({sess['intraday_directional']:.0f}% dir)"
        elif hasattr(self, 'session_tracker') and self.session_tracker and self.session_tracker.scan_number > 0:
            text += f"  |  Scan #{self.session_tracker.scan_number}"

        self.summary_label.configure(text=text)
        self._update_market_overview()

    def _update_market_overview(self):
        """Update the market overview bar with broad market indicators."""
        if not self.results:
            return

        try:
            # Pull market-level data from fear_greed_proxy (same for all tickers)
            any_result = next(iter(self.results.values()))
            fg = None
            for s in any_result.get("signals", []):
                if s.get("collector") == "fear_greed_proxy":
                    fg = s.get("raw_data") if isinstance(s.get("raw_data"), dict) else {}
                    break

            parts = []

            # VIX
            if fg and fg.get("vix"):
                vix = fg["vix"]
                vix_chg = fg.get("vix_5d_change", 0)
                if vix > 30:
                    vix_desc = "HIGH FEAR"
                elif vix > 25:
                    vix_desc = "Elevated"
                elif vix > 20:
                    vix_desc = "Cautious"
                elif vix > 15:
                    vix_desc = "Normal"
                else:
                    vix_desc = "Calm"
                chg_str = f" ({vix_chg:+.0%} 5d)" if vix_chg else ""
                parts.append(f"VIX:{vix:.0f} {vix_desc}{chg_str}")

            # Breadth
            if fg and "breadth_spread" in fg:
                breadth = fg["breadth_spread"]
                if breadth > 0.01:
                    parts.append("Breadth:Wide")
                elif breadth < -0.01:
                    parts.append("Breadth:Narrow")
                else:
                    parts.append("Breadth:Normal")

            # Safe haven flow
            if fg and "safe_haven_flow" in fg:
                haven = fg["safe_haven_flow"]
                if haven > 0.02:
                    parts.append("Gold>Stocks")
                elif haven < -0.02:
                    parts.append("Stocks>Gold")

            # Market Pulse (SPY, NASDAQ, DOW, Russell, TLT, Dollar)
            mp = None
            for s in any_result.get("signals", []):
                if s.get("collector") == "market_pulse":
                    mp = s.get("raw_data") if isinstance(s.get("raw_data"), dict) else {}
                    break
            if mp:
                # Major indexes
                for key, label in [("spy_change", "S&P"), ("nasdaq_change", "NDQ"),
                                    ("dow_change", "DOW"), ("russell_change", "RUS")]:
                    val = mp.get(key)
                    if val is not None:
                        parts.append(f"{label}:{val:+.2f}%")

                # Bonds
                tlt_chg = mp.get("tlt_change")
                if tlt_chg is not None:
                    parts.append(f"Bonds:{'up' if tlt_chg > 0.1 else 'dn' if tlt_chg < -0.1 else 'flat'}")

                pulse = mp.get("pulse", "")
                if pulse:
                    parts.append(pulse)

            # Portfolio-level signals from our analysis
            avg_sig = sum(r.get("signal", 0) for r in self.results.values()) / len(self.results)
            avg_momentum = 0
            momentum_count = 0
            for r in self.results.values():
                for s in r.get("signals", []):
                    if s.get("collector") == "momentum_quality" and s.get("confidence", 0) > 0.3:
                        avg_momentum += s.get("signal", 0)
                        momentum_count += 1
            if momentum_count > 0:
                avg_momentum /= momentum_count
                if avg_momentum > 0.05:
                    parts.append("Momentum:Positive")
                elif avg_momentum < -0.05:
                    parts.append("Momentum:Fading")
                else:
                    parts.append("Momentum:Flat")

            # Overall portfolio lean
            if avg_sig > 0.03:
                parts.append(f"Portfolio:+{avg_sig:.3f}")
            elif avg_sig < -0.03:
                parts.append(f"Portfolio:{avg_sig:.3f}")
            else:
                parts.append(f"Portfolio:Neutral")

            # Market regime (from any recent result)
            regime = any_result.get("market_regime", "")
            regime_bias = any_result.get("regime_bias", 0)
            if regime and regime != "UNKNOWN":
                parts.append(f"Regime:{regime} ({regime_bias:+.3f})")

            market_text = "  |  ".join(parts) if parts else "Market data loading..."

            # Color based on overall mood
            if fg and fg.get("mood"):
                mood = fg["mood"]
                if "Greed" in mood or "optimism" in mood:
                    color = GREEN
                elif "Fear" in mood or "risk-off" in mood:
                    color = RED
                else:
                    color = FG_DIM
            else:
                color = FG_DIM

            self.market_overview_label.configure(text=market_text, fg=color)

        except Exception:
            pass

    # ── Monitoring Loop ────────────────────────────────────────

    def _toggle_monitor(self):
        if self.monitoring:
            self.monitoring = False
            self.monitor_btn.configure(text="Start Monitoring", style="Green.TButton")
            self.status_label.configure(text="Stopped", foreground=FG_SECONDARY)
            if hasattr(self, 'session_tracker') and self.session_tracker:
                stats = self.session_tracker.get_session_stats()
                self._log(f"Session ended: {stats['total_scans']} scans, "
                          f"{stats['intraday_verified']} verified "
                          f"({stats['intraday_accuracy']:.0f}% accurate)")
            # Save signal intelligence learned this session
            if self.oracle and hasattr(self.oracle, 'intelligence'):
                try:
                    self.oracle.intelligence.save()
                    intel_status = self.oracle.intelligence.get_status()
                    self._log(f"Signal intelligence saved: "
                              f"{len(intel_status.get('mostly_stale_collectors', {}))} mostly-stale collectors, "
                              f"{intel_status['total_scans']} total scans learned")
                except Exception:
                    pass
            self._log("Monitoring stopped")
        else:
            try:
                self.monitor_interval = int(self.interval_var.get())
            except ValueError:
                self.monitor_interval = 300

            self.monitoring = True
            self.monitor_btn.configure(text="Stop Monitoring", style="Red.TButton")
            self._log(f"Monitoring started ({self.monitor_interval}s interval)")

            # Create a new session tracker for this monitoring session
            from stock_oracle.session_tracker import SessionTracker
            self.session_tracker = SessionTracker()
            self._log(f"Session tracker started (ID: {self.session_tracker.session_id})")

            threading.Thread(target=self._monitor_loop, daemon=True).start()

    def _monitor_loop(self):
        """Continuously analyze watchlist."""
        while self.monitoring:
            # Wait for any active scan to finish before starting monitor cycle
            while self._scanning and self.monitoring:
                time.sleep(2)

            if not self.monitoring:
                break

            self._scanning = True
            self.msg_queue.put(("status", "Scanning..."))
            try:
                oracle = self._get_oracle()

                scan_num = self.session_tracker.scan_number if hasattr(self, 'session_tracker') and self.session_tracker else 0

                # Decide whether to run Ollama collectors this scan:
                # - Forced full scan (Analyze All button): always full
                # - Every 3rd scan: full (Ollama refreshes ~every 15 min)
                # - Otherwise: fast mode (skip Ollama)
                if self._force_full_scan:
                    oracle.skip_slow = False
                    self._force_full_scan = False
                    self.msg_queue.put(("log", "Full scan (all collectors including Ollama)"))
                elif scan_num % 3 == 0:
                    oracle.skip_slow = False
                    self.msg_queue.put(("log", f"Scan #{scan_num}: full mode (Ollama refresh)"))
                else:
                    oracle.skip_slow = True

                for ticker in list(self.watchlist):
                    if not self.monitoring:
                        break
                    try:
                        self.msg_queue.put(("status", f"Scanning {ticker}..."))
                        result = oracle.analyze(ticker, verbose=False)

                        # Check for signal changes BEFORE updating results
                        old = self.results.get(ticker, {})
                        old_sig = old.get("signal", 0)
                        new_sig = result.get("signal", 0)
                        old_pred = old.get("prediction", "")
                        new_pred = result.get("prediction", "")

                        # Alert: Prediction flipped (BULLISH->BEARISH or vice versa)
                        if old_pred and new_pred and old_pred != new_pred:
                            if old_pred in ("BULLISH","BEARISH") and new_pred in ("BULLISH","BEARISH"):
                                self.msg_queue.put((
                                    "alert",
                                    f"{ticker} FLIPPED {old_pred} -> {new_pred}"
                                ))
                            elif old_pred != "NEUTRAL" or new_pred != "NEUTRAL":
                                self.msg_queue.put((
                                    "alert",
                                    f"{ticker} prediction changed: {old_pred} -> {new_pred}"
                                ))

                        # Alert: Significant signal shift
                        if abs(new_sig - old_sig) > 0.08:
                            direction = "BULLISH" if new_sig > old_sig else "BEARISH"
                            self.msg_queue.put((
                                "alert",
                                f"{ticker} signal shifted {direction} "
                                f"({old_sig:+.3f} -> {new_sig:+.3f})"
                            ))

                        # Alert: Core conviction dropped significantly
                        old_wp = old.get("weighted_prediction", {})
                        new_wp = result.get("weighted_prediction", {})
                        old_core = old_wp.get("core_analysis_score", 0)
                        new_core = new_wp.get("core_analysis_score", 0)
                        if old_core > 0 and (old_core - new_core) > 0.25:
                            self.msg_queue.put((
                                "alert",
                                f"{ticker} conviction dropped {old_core:.0%} -> {new_core:.0%}"
                            ))

                        # Alert: Big price move (>2% between scans)
                        old_price = self._get_live_price(old).get("price", 0) if old else 0
                        new_price = self._get_live_price(result).get("price", 0)
                        if old_price > 0 and new_price > 0:
                            pct_move = (new_price - old_price) / old_price
                            if abs(pct_move) > 0.02:
                                self.msg_queue.put((
                                    "alert",
                                    f"{ticker} moved {pct_move:+.1%} "
                                    f"(${old_price:.2f} -> ${new_price:.2f})"
                                ))

                        self.msg_queue.put(("result", ticker, result))

                    except Exception as e:
                        self.msg_queue.put(("log", f"Monitor error {ticker}: {e}"))

            finally:
                self._scanning = False

            # Record scan to session tracker for intraday trend/feedback
            if hasattr(self, 'session_tracker') and self.session_tracker:
                try:
                    self.session_tracker.record_scan(dict(self.results))
                    stats = self.session_tracker.get_session_stats()

                    # Log intraday accuracy after enough scans
                    if stats["intraday_verified"] > 0 and self.session_tracker.scan_number % 3 == 0:
                        self.msg_queue.put(("log",
                            f"Intraday: {stats['intraday_accuracy']:.0f}% exact, "
                            f"{stats['intraday_directional']:.0f}% directional "
                            f"({stats['intraday_verified']} verified)"))

                    # Auto-retrain ML every ~1 hour (12 scans at 300s)
                    # Only if we have enough intraday verified data to be meaningful
                    scan_num = self.session_tracker.scan_number
                    if (scan_num > 0 and scan_num % 12 == 0
                            and stats["intraday_verified"] >= 20):
                        self.msg_queue.put(("log",
                            f"Auto-retrain triggered (scan #{scan_num}, "
                            f"{stats['intraday_verified']} intraday samples)"))
                        self._auto_retrain()

                    # Claude advisor check-in every ~30 min (6 scans at 300s)
                    if scan_num > 0 and scan_num % 6 == 0:
                        try:
                            self._claude_hourly_checkin()
                        except Exception:
                            pass  # Advisor is optional

                except Exception as e:
                    self.msg_queue.put(("log", f"Session tracker error: {e}"))

            self.msg_queue.put(("refresh", None))
            timestamp = datetime.now().strftime("%H:%M:%S")
            scan_num = self.session_tracker.scan_number if hasattr(self, 'session_tracker') and self.session_tracker else "?"
            self.msg_queue.put(("log", f"Scan #{scan_num} complete at {timestamp}"))

            # Countdown with live status updates — can be interrupted by Analyze All
            for remaining in range(self.monitor_interval, 0, -1):
                if not self.monitoring:
                    break
                if self._force_full_scan:
                    self.msg_queue.put(("log", "Countdown interrupted — full scan requested"))
                    break
                if remaining % 10 == 0 or remaining <= 5:
                    self.msg_queue.put(("status", f"Next scan in {remaining}s"))
                time.sleep(1)

    # ── Queue Processor ────────────────────────────────────────

    def _process_queue(self):
        """Process messages from background threads."""
        try:
            while not self.msg_queue.empty():
                msg = self.msg_queue.get_nowait()

                if msg[0] == "result":
                    _, ticker, result = msg
                    self.results[ticker] = result
                    self._update_card(ticker, result)
                    self._populate_watchlist()
                    self._update_summary()

                elif msg[0] == "status":
                    self.status_label.configure(
                        text=msg[1],
                        foreground=GREEN if "Done" in msg[1] else
                                   AMBER if "..." in msg[1] else FG_SECONDARY
                    )

                elif msg[0] == "log":
                    self._log(msg[1])

                elif msg[0] == "alert":
                    self._log(f"*** {msg[1]} ***")
                    # Flash the window title
                    self.root.title(f"ALERT - Stock Oracle - {msg[1]}")
                    self.root.after(5000, lambda: self.root.title("Stock Oracle"))

                elif msg[0] == "refresh":
                    self._populate_watchlist()
                    self._update_summary()
                    self._resort_cards()
                    self._update_accuracy_display()

                elif msg[0] == "update_accuracy":
                    self._update_accuracy_display()

                elif msg[0] == "_claude_response":
                    _, dialog, text_widget, answer = msg
                    try:
                        text_widget.delete("1.0", "end")
                        text_widget.insert("end", answer or "No response")
                    except Exception:
                        pass  # Dialog may have been closed

                elif msg[0] == "_claude_analysis_popup":
                    self._show_claude_analysis_popup(msg[1])

                elif msg[0] == "_breakout_results":
                    self._show_breakout_results(msg[1])

                elif msg[0] == "_news_feed_results":
                    self._show_news_feed(msg[1], msg[2])

        except Exception:
            pass

        self.root.after(200, self._process_queue)

    # ── Result Cards ───────────────────────────────────────────

    def _update_card(self, ticker: str, result: Dict):
        """Create or update a result card with live price and analysis summary."""
        if ticker in self.result_cards:
            self.result_cards[ticker].destroy()

        pred = result.get("prediction", "?")
        sig = result.get("signal", 0)
        conf = result.get("confidence", 0)
        pinfo = self._get_live_price(result)
        price = pinfo["price"]
        change = pinfo["change"]
        session = pinfo["session"]

        # Extract key analysis data for the card
        signals_list = result.get("signals", [])
        wp = result.get("weighted_prediction", {})
        core_score = wp.get("core_analysis_score", 0)

        def _card_sig(name):
            return next((s for s in signals_list if s.get("collector") == name), {})

        analyst = _card_sig("analyst_consensus")
        analyst_raw = analyst.get("raw_data", {}) if isinstance(analyst.get("raw_data"), dict) else {}
        tech = _card_sig("technical_analysis")
        tech_raw = tech.get("raw_data", {}) if isinstance(tech.get("raw_data"), dict) else {}
        indicators = tech_raw.get("indicators", {})

        # Card frame
        card = tk.Frame(self.results_frame, bg=BG_CARD, padx=12, pady=8,
                         highlightbackground=BORDER, highlightthickness=1)
        card.pack(fill="x", padx=4, pady=3)
        card.bind("<Button-1>", lambda e, t=ticker: self._show_details(t))
        card.bind("<Double-Button-1>", lambda e, t=ticker: self._open_deep_dive(t))

        # Row 1: Ticker + Price + Change + Session tag
        top = tk.Frame(card, bg=BG_CARD)
        top.pack(fill="x")

        tk.Label(top, text=ticker, bg=BG_CARD, fg=FG_PRIMARY,
                  font=("Consolas", 14, "bold")).pack(side="left")

        if price > 0:
            tk.Label(top, text=f"  ${price:.2f}", bg=BG_CARD, fg=FG_PRIMARY,
                      font=("Consolas", 14)).pack(side="left")

            # During AH/PRE: show regular session change + AH delta separately
            if session in ("AH", "PRE") and pinfo.get("regular_change"):
                reg_chg = pinfo.get("regular_change", 0)
                ext_chg = pinfo.get("extended_change", 0)

                # Regular session change (dimmed)
                reg_color = GREEN if reg_chg > 0.001 else RED if reg_chg < -0.001 else FG_DIM
                tk.Label(top, text=f"  {reg_chg:+.1%}", bg=BG_CARD, fg=reg_color,
                          font=("Consolas", 10)).pack(side="left")

                # AH delta (brighter, with session tag)
                ext_color = GREEN if ext_chg > 0.001 else RED if ext_chg < -0.001 else FG_SECONDARY
                sess_color = AMBER if session == "PRE" else PURPLE
                tk.Label(top, text=f"  {ext_chg:+.1%}", bg=BG_CARD, fg=ext_color,
                          font=("Consolas", 12, "bold")).pack(side="left")
                tk.Label(top, text=f" {session}", bg=BG_CARD, fg=sess_color,
                          font=("Consolas", 9)).pack(side="left", padx=(2, 0))
            else:
                # Regular hours or no extended data: show single change
                chg_color = GREEN if change > 0.001 else RED if change < -0.001 else FG_SECONDARY
                chg_text = f"  {change:+.2%}"
                tk.Label(top, text=chg_text, bg=BG_CARD, fg=chg_color,
                          font=("Consolas", 12)).pack(side="left")

                if session:
                    sess_color = GREEN if session == "LIVE" else AMBER if session == "PRE" else PURPLE
                    tk.Label(top, text=f"  {session}", bg=BG_CARD, fg=sess_color,
                              font=("Consolas", 9)).pack(side="left", padx=(4, 0))

        # Prediction badges (right side)
        # Intraday badge (only during monitoring with 2+ scans)
        if hasattr(self, 'session_tracker') and self.session_tracker:
            trend = self.session_tracker.get_trend(ticker)
            if trend.get("scans", 0) >= 2:
                intra_pred, intra_color = self._get_intraday_prediction(ticker, trend, result)
                intra_badge = tk.Label(top, text=f" {intra_pred} ",
                                        bg=intra_color, fg="white",
                                        font=("Segoe UI", 8, "bold"), padx=4, pady=1)
                intra_badge.pack(side="right", padx=(2, 0))
                tk.Label(top, text="INTRA", bg=BG_CARD, fg=FG_DIM,
                          font=("Consolas", 7)).pack(side="right")

        # 5-day prediction badge
        badge_color = GREEN if pred == "BULLISH" else RED if pred == "BEARISH" else AMBER
        badge = tk.Label(top, text=f" {pred} ", bg=badge_color, fg="white",
                          font=("Segoe UI", 9, "bold"), padx=8, pady=1)
        badge.pack(side="right", padx=(0, 2))
        tk.Label(top, text="5D", bg=BG_CARD, fg=FG_DIM,
                  font=("Consolas", 7)).pack(side="right")

        # Signal bar
        bar_frame = tk.Frame(card, bg="#1a1f2e", height=6)
        bar_frame.pack(fill="x", pady=(6, 2))
        bar_frame.pack_propagate(False)

        bar_width = abs(sig) * 100
        bar_color = GREEN if sig > 0 else RED if sig < 0 else AMBER
        bar = tk.Frame(bar_frame, bg=bar_color, width=max(2, int(bar_width)))
        bar.place(x=150 if sig >= 0 else 150 - int(bar_width), y=0,
                   width=max(2, int(bar_width)), relheight=1.0)

        # Row 2: Key analysis metrics
        analysis_row = tk.Frame(card, bg=BG_CARD)
        analysis_row.pack(fill="x", pady=(2, 0))

        # RSI indicator
        rsi = indicators.get("rsi")
        if rsi is not None:
            rsi_color = GREEN if rsi < 35 else RED if rsi > 65 else FG_DIM
            tk.Label(analysis_row, text=f"RSI:{rsi:.0f}", bg=BG_CARD, fg=rsi_color,
                      font=("Consolas", 9)).pack(side="left")

        # Analyst target
        target_upside = analyst_raw.get("target_upside_pct")
        if target_upside is not None:
            tgt_color = GREEN if target_upside > 10 else RED if target_upside < -5 else FG_SECONDARY
            tk.Label(analysis_row, text=f"Tgt:{target_upside:+.0f}%", bg=BG_CARD, fg=tgt_color,
                      font=("Consolas", 9)).pack(side="left", padx=(8, 0))

        # Core conviction
        conv_color = GREEN if core_score > 0.65 else RED if core_score < 0.35 else AMBER
        tk.Label(analysis_row, text=f"Conv:{core_score:.0%}", bg=BG_CARD, fg=conv_color,
                  font=("Consolas", 9)).pack(side="left", padx=(8, 0))

        # Intraday trend arrow (from session tracker)
        if hasattr(self, 'session_tracker') and self.session_tracker:
            trend = self.session_tracker.get_trend(ticker)
            if trend.get("scans", 0) >= 2:
                arrow = trend.get("arrow", "")
                direction = trend.get("direction", "stable")
                trend_color = (GREEN if "bull" in direction or "recovering" in direction
                               else RED if "bear" in direction or "weakening" in direction
                               else FG_DIM)
                price_pct = trend.get("price_change_pct", 0)
                trend_text = f"{arrow}{price_pct:+.2f}%"
                tk.Label(analysis_row, text=trend_text, bg=BG_CARD, fg=trend_color,
                          font=("Consolas", 9, "bold")).pack(side="left", padx=(8, 0))

        # Active signals count (right side)
        active = len([s for s in signals_list if s.get("confidence", 0) > 0.1])
        total = len(signals_list)
        tk.Label(analysis_row, text=f"{active}/{total} signals", bg=BG_CARD, fg=FG_DIM,
                  font=("Consolas", 9)).pack(side="right")

        method = result.get("method", "")
        if "ml_ensemble" in method:
            tk.Label(analysis_row, text="ML", bg=BG_CARD, fg=PURPLE,
                      font=("Consolas", 9, "bold")).pack(side="right", padx=8)

        self.result_cards[ticker] = card

    def _show_details(self, ticker: str):
        """Show analysis summary + signal breakdown for a ticker."""
        result = self.results.get(ticker)
        if not result:
            self.detail_text.delete("1.0", tk.END)
            self.detail_text.insert("1.0", f"No analysis data for {ticker}\n\nClick 'Analyze' to run.")
            return

        self.detail_text.delete("1.0", tk.END)

        # Configure tags
        self.detail_text.tag_configure("header", foreground=FG_PRIMARY,
                                        font=("Consolas", 12, "bold"))
        self.detail_text.tag_configure("sub", foreground=ACCENT,
                                        font=("Consolas", 10, "bold"))
        self.detail_text.tag_configure("bull", foreground=GREEN)
        self.detail_text.tag_configure("bear", foreground=RED)
        self.detail_text.tag_configure("neutral", foreground=AMBER)
        self.detail_text.tag_configure("dim", foreground=FG_DIM)
        self.detail_text.tag_configure("good", foreground=GREEN, font=("Consolas", 10, "bold"))
        self.detail_text.tag_configure("bad", foreground=RED, font=("Consolas", 10, "bold"))

        # Header with live price
        pred = result.get("prediction", "?")
        sig = result.get("signal", 0)
        conf = result.get("confidence", 0)
        pinfo = self._get_live_price(result)
        wp = result.get("weighted_prediction", {})
        core = wp.get("core_analysis_score", 0)

        self.detail_text.insert(tk.END, f"  {ticker}", "header")
        pred_tag = "bull" if pred == "BULLISH" else "bear" if pred == "BEARISH" else "neutral"
        self.detail_text.insert(tk.END, f"  {pred}\n", pred_tag)

        if pinfo["price"] > 0:
            chg = pinfo["change"]
            sess = f" [{pinfo['session']}]" if pinfo["session"] else ""
            chg_tag = "bull" if chg > 0.001 else "bear" if chg < -0.001 else "neutral"
            self.detail_text.insert(tk.END, f"  ${pinfo['price']:.2f}", "header")
            self.detail_text.insert(tk.END, f"  {chg:+.2%}{sess}\n", chg_tag)

        conv_tag = "good" if core > 0.65 else "bad" if core < 0.35 else "neutral"
        self.detail_text.insert(tk.END, f"  Sig: {sig:+.4f}  Conf: {conf:.0%}  Core: ", "dim")
        self.detail_text.insert(tk.END, f"{core:.0%}\n", conv_tag)

        # Intraday prediction (during monitoring)
        if hasattr(self, 'session_tracker') and self.session_tracker:
            trend = self.session_tracker.get_trend(ticker)
            if trend.get("scans", 0) >= 2:
                intra_pred, _ = self._get_intraday_prediction(ticker, trend, result)
                intra_tag = "bull" if intra_pred in ("RISING", "LEAN UP") else "bear" if intra_pred in ("FALLING", "LEAN DN") else "neutral"
                price_pct = trend.get("price_change_pct", 0)
                sig_trend = trend.get("signal_trend", 0)
                self.detail_text.insert(tk.END, f"  Intraday: ", "dim")
                self.detail_text.insert(tk.END, f"{intra_pred}", intra_tag)
                self.detail_text.insert(tk.END, f"  (sess: {price_pct:+.2f}%  sig: {sig_trend:+.4f}/scan  {trend.get('scans',0)} scans)\n", "dim")

        self.detail_text.insert(tk.END, f"  {'- '*22}\n\n")

        # Company info (cached, fetched lazily)
        if not hasattr(self, '_company_cache'):
            self._company_cache = {}

        if ticker not in self._company_cache:
            try:
                import yfinance as yf
                stock = yf.Ticker(ticker)
                info = stock.info
                self._company_cache[ticker] = {
                    "name": info.get("longName") or info.get("shortName", ""),
                    "sector": info.get("sector", ""),
                    "industry": info.get("industry", ""),
                    "summary": info.get("longBusinessSummary", ""),
                    "market_cap": info.get("marketCap", 0),
                }
            except Exception:
                self._company_cache[ticker] = {}

        co = self._company_cache.get(ticker, {})
        if co.get("name"):
            self.detail_text.tag_configure("company", foreground=ACCENT,
                                            font=("Segoe UI", 9))
            self.detail_text.tag_configure("co_dim", foreground=FG_DIM,
                                            font=("Segoe UI", 8))

            co_name = co["name"]
            sector = co.get("sector", "")
            industry = co.get("industry", "")
            mcap = co.get("market_cap", 0)

            mcap_str = ""
            if mcap >= 1_000_000_000:
                mcap_str = f"${mcap/1_000_000_000:.1f}B"
            elif mcap >= 1_000_000:
                mcap_str = f"${mcap/1_000_000:.0f}M"

            co_line = f"  {co_name}"
            if mcap_str:
                co_line += f"  ({mcap_str})"
            self.detail_text.insert(tk.END, co_line + "\n", "company")

            if sector or industry:
                self.detail_text.insert(tk.END, f"  {sector}", "co_dim")
                if industry and industry != sector:
                    self.detail_text.insert(tk.END, f" · {industry}", "co_dim")
                self.detail_text.insert(tk.END, "\n", "co_dim")

            co_summary = co.get("summary", "")
            if co_summary:
                short_s = co_summary[:200]
                last_p = short_s.rfind(".")
                if last_p > 80:
                    short_s = short_s[:last_p + 1]
                else:
                    short_s = short_s + "..."
                self.detail_text.insert(tk.END, f"  {short_s}\n", "co_dim")

            self.detail_text.insert(tk.END, "\n")

        # Narrative summary
        try:
            from stock_oracle.narrative import generate_narrative
            narrative = generate_narrative(result)
            self.detail_text.tag_configure("narrative", foreground="#c8d0e0",
                                            font=("Segoe UI", 9))
            # Show first 3 paragraphs as a condensed summary
            paras = [p.strip() for p in narrative.split("\n\n") if p.strip()]
            for para in paras[:4]:
                self.detail_text.insert(tk.END, f"  {para}\n\n", "narrative")
            self.detail_text.insert(tk.END, f"  {'- '*22}\n\n")
        except Exception:
            pass

        # Recent news for this ticker
        try:
            news_feed = self._get_news_feed()
            if news_feed:
                articles = news_feed.get_news(ticker, days=3, max_articles=5)
                if articles:
                    self.detail_text.tag_configure("news_head", foreground=ACCENT,
                                                    font=("Segoe UI", 9, "bold"))
                    self.detail_text.tag_configure("news_source", foreground=FG_DIM,
                                                    font=("Segoe UI", 8))
                    self.detail_text.tag_configure("news_link", foreground=BLUE,
                                                    font=("Segoe UI", 8, "underline"))

                    self.detail_text.insert(tk.END, f"  Recent News\n", "sub")
                    for article in articles[:5]:
                        headline = article.get("headline", "")[:80]
                        source = article.get("source", "")
                        age = article.get("age", "")
                        url = article.get("url", "")

                        self.detail_text.insert(tk.END, f"  • {headline}\n", "news_head")
                        self.detail_text.insert(tk.END, f"    {source} · {age}", "news_source")

                        if url:
                            tag_name = f"link_{hash(url) % 100000}"
                            self.detail_text.tag_configure(tag_name, foreground=BLUE,
                                                            font=("Segoe UI", 8, "underline"))
                            self.detail_text.tag_bind(tag_name, "<Button-1>",
                                lambda e, u=url: __import__("webbrowser").open(u))
                            self.detail_text.tag_bind(tag_name, "<Enter>",
                                lambda e: self.detail_text.configure(cursor="hand2"))
                            self.detail_text.tag_bind(tag_name, "<Leave>",
                                lambda e: self.detail_text.configure(cursor=""))
                            self.detail_text.insert(tk.END, f"  [open]", tag_name)

                        self.detail_text.insert(tk.END, "\n")

                    self.detail_text.insert(tk.END, f"\n  {'- '*22}\n\n")
        except Exception:
            pass

        # Key analysis signals
        signals = result.get("signals", [])
        def _sig(name):
            return next((s for s in signals if s.get("collector") == name), {})

        # Technical
        tech = _sig("technical_analysis")
        tech_raw = tech.get("raw_data", {}) if isinstance(tech.get("raw_data"), dict) else {}
        ind = tech_raw.get("indicators", {})
        if ind:
            self.detail_text.insert(tk.END, f"  Technical\n", "sub")
            rsi = ind.get("rsi")
            if rsi is not None:
                rsi_tag = "good" if rsi < 35 else "bad" if rsi > 65 else "dim"
                self.detail_text.insert(tk.END, f"    RSI={rsi:.0f}", rsi_tag)
            macd_h = ind.get("macd_histogram", 0)
            m_tag = "good" if macd_h > 0 else "bad"
            self.detail_text.insert(tk.END, f"  MACD={'+'if macd_h>0 else ''}{macd_h:.3f}", m_tag)
            bb = ind.get("bollinger_position")
            if bb is not None:
                self.detail_text.insert(tk.END, f"  BB={bb:.2f}", "dim")
            self.detail_text.insert(tk.END, "\n")

        # Analyst
        analyst = _sig("analyst_consensus")
        ar = analyst.get("raw_data", {}) if isinstance(analyst.get("raw_data"), dict) else {}
        if ar.get("target_mean"):
            self.detail_text.insert(tk.END, f"  Analyst\n", "sub")
            upside = ar.get("target_upside_pct", 0)
            up_tag = "good" if upside > 10 else "bad" if upside < -5 else "dim"
            rec = ar.get("recommendation", "").upper()
            self.detail_text.insert(tk.END, f"    Target ${ar['target_mean']:.2f} (", "dim")
            self.detail_text.insert(tk.END, f"{upside:+.1f}%", up_tag)
            self.detail_text.insert(tk.END, f") {rec}\n", "dim")

        # Fundamentals
        fund = _sig("fundamental_analysis")
        fr = fund.get("raw_data", {}) if isinstance(fund.get("raw_data"), dict) else {}
        metrics = fr.get("metrics", {})
        if metrics:
            self.detail_text.insert(tk.END, f"  Fundamentals\n", "sub")
            parts = []
            if "pe" in metrics:
                parts.append(f"P/E={metrics['pe']}")
            if "profit_margin" in metrics:
                parts.append(f"Margin={metrics['profit_margin']}%")
            if "revenue_growth" in metrics:
                rg = metrics["revenue_growth"]
                parts.append(f"RevGr={rg:+.1f}%")
            self.detail_text.insert(tk.END, f"    {' | '.join(parts[:3])}\n", "dim")

        # Short interest
        short = _sig("short_interest")
        sr = short.get("raw_data", {}) if isinstance(short.get("raw_data"), dict) else {}
        if sr.get("short_pct_float"):
            sp = sr["short_pct_float"]
            sp_tag = "bad" if sp > 10 else "good" if sp < 3 else "dim"
            self.detail_text.insert(tk.END, f"  Short: ", "sub")
            self.detail_text.insert(tk.END, f"{sp:.1f}%", sp_tag)
            if "short_change_monthly" in sr:
                sc = sr["short_change_monthly"]
                sc_tag = "good" if sc < -5 else "bad" if sc > 5 else "dim"
                self.detail_text.insert(tk.END, f" (MoM {sc:+.1f}%)", sc_tag)
            self.detail_text.insert(tk.END, "\n")

        self.detail_text.insert(tk.END, f"\n  {'- '*22}\n")

        # Signal list (condensed)
        active = sorted(
            [s for s in signals if s.get("confidence", 0) > 0.1],
            key=lambda x: abs(x["signal"]),
            reverse=True,
        )

        self.detail_text.insert(tk.END, f"  Signals ({len(active)} active):\n\n")
        for s in active[:12]:
            sv = s["signal"]
            icon = "+" if sv > 0.1 else "-" if sv < -0.1 else " "
            color_tag = "bull" if sv > 0.1 else "bear" if sv < -0.1 else "neutral"
            self.detail_text.insert(tk.END,
                f"  {icon} {s['collector']:22s} {sv:+.3f} {s['confidence']:.0%}\n", color_tag)

        remaining = len(active) - 12
        if remaining > 0:
            self.detail_text.insert(tk.END, f"  ... +{remaining} more\n", "dim")

        self.detail_text.insert(tk.END, "\n")
        btn = tk.Button(self.detail_text, text="  Open Deep Dive  ",
                         bg=ACCENT, fg="white", font=("Segoe UI", 10, "bold"),
                         borderwidth=0, padx=12, pady=4, cursor="hand2",
                         command=lambda t=ticker: self._open_deep_dive(t))
        self.detail_text.window_create(tk.END, window=btn)
        self.detail_text.insert(tk.END, "  (or double-click card)\n", "dim")

    # ── Deep Dive Window ───────────────────────────────────────

    def _open_deep_dive(self, ticker: str):
        """Open a full deep-dive analysis window for a ticker."""
        result = self.results.get(ticker)
        if not result:
            return

        win = tk.Toplevel(self.root)
        win.title(f"Deep Dive - {ticker}")
        win.geometry("920x780")
        win.configure(bg=BG_DARK)

        # ── Header ─────────────────────────────────────────────
        hdr = tk.Frame(win, bg=BG_PANEL, padx=16, pady=10)
        hdr.pack(fill="x")

        pred = result.get("prediction", "?")
        pinfo = self._get_live_price(result)
        sig = result.get("signal", 0)
        conf = result.get("confidence", 0)
        method = result.get("method", "?")

        tk.Label(hdr, text=ticker, bg=BG_PANEL, fg=FG_PRIMARY,
                  font=("Consolas", 20, "bold")).pack(side="left")

        if pinfo["price"] > 0:
            tk.Label(hdr, text=f"  ${pinfo['price']:.2f}", bg=BG_PANEL, fg=FG_PRIMARY,
                      font=("Consolas", 18)).pack(side="left")
            chg_color = GREEN if pinfo["change"] > 0 else RED if pinfo["change"] < 0 else FG_SECONDARY
            tk.Label(hdr, text=f"  {pinfo['change']:+.2%}", bg=BG_PANEL, fg=chg_color,
                      font=("Consolas", 16)).pack(side="left")
            if pinfo["session"]:
                tk.Label(hdr, text=f"  {pinfo['session']}", bg=BG_PANEL, fg=AMBER,
                          font=("Consolas", 12)).pack(side="left")

        badge_color = GREEN if pred == "BULLISH" else RED if pred == "BEARISH" else AMBER
        tk.Label(hdr, text=f"  {pred}  ", bg=badge_color, fg="white",
                  font=("Segoe UI", 12, "bold"), padx=12, pady=4).pack(side="right")

        if "ml_ensemble" in method:
            tk.Label(hdr, text="ML", bg=BG_PANEL, fg=PURPLE,
                      font=("Consolas", 11, "bold")).pack(side="right", padx=8)

        # Sub-header
        sub = tk.Frame(win, bg=BG_PANEL, padx=16)
        sub.pack(fill="x", pady=(0, 6))
        summary = result.get("signal_summary", {})
        tk.Label(sub, text=f"Signal: {sig:+.4f}  |  Conf: {conf:.0%}  |  {method}  |  Bull:{summary.get('bullish_count',0)} Bear:{summary.get('bearish_count',0)} Neutral:{summary.get('neutral_count',0)}",
                  bg=BG_PANEL, fg=FG_SECONDARY, font=("Consolas", 10)).pack(side="left")

        # ── Tab buttons ────────────────────────────────────────
        tab_bar = tk.Frame(win, bg=BG_DARK)
        tab_bar.pack(fill="x", padx=8, pady=(8, 0))

        # Content area
        content = tk.Frame(win, bg=BG_DARK)
        content.pack(fill="both", expand=True, padx=8, pady=(0, 8))

        # Create all tab content frames
        tabs = {}
        current_tab = [None]
        tab_buttons = {}

        def show_tab(name):
            for n, f in tabs.items():
                f.pack_forget()
            tabs[name].pack(fill="both", expand=True)
            current_tab[0] = name
            for n, b in tab_buttons.items():
                if n == name:
                    b.configure(bg=ACCENT, fg="white")
                else:
                    b.configure(bg=BG_CARD, fg=FG_SECONDARY)

        for tab_name in ["Analysis", "All Signals", "ML Analysis", "Price Data", "Raw JSON"]:
            btn = tk.Button(tab_bar, text=f"  {tab_name}  ", bg=BG_CARD, fg=FG_SECONDARY,
                             font=("Segoe UI", 10, "bold"), borderwidth=0, padx=12, pady=6,
                             cursor="hand2", activebackground=ACCENT, activeforeground="white",
                             command=lambda n=tab_name: show_tab(n))
            btn.pack(side="left", padx=(0, 2))
            tab_buttons[tab_name] = btn
            tabs[tab_name] = tk.Frame(content, bg=BG_DARK)

        signals = result.get("signals", [])

        # ── TAB: Analysis (new — shows core financial analysis) ──
        t0 = scrolledtext.ScrolledText(
            tabs["Analysis"], bg=BG_DARK, fg=FG_PRIMARY, font=("Consolas", 10),
            wrap="word", borderwidth=0, highlightthickness=0,
        )
        t0.pack(fill="both", expand=True)
        for w in [t0]:
            w.tag_configure("hdr", foreground=BLUE, font=("Consolas", 12, "bold"))
            w.tag_configure("sub", foreground=ACCENT, font=("Consolas", 11, "bold"))
            w.tag_configure("bull", foreground=GREEN)
            w.tag_configure("bear", foreground=RED)
            w.tag_configure("neutral", foreground=AMBER)
            w.tag_configure("dim", foreground=FG_DIM)
            w.tag_configure("key", foreground=PURPLE)
            w.tag_configure("val", foreground=FG_PRIMARY)
            w.tag_configure("good", foreground=GREEN, font=("Consolas", 10, "bold"))
            w.tag_configure("bad", foreground=RED, font=("Consolas", 10, "bold"))

        # Helper to get signal dict by collector name
        def _sig(name):
            return next((s for s in signals if s.get("collector") == name), {})

        # ── Overall Verdict ──
        wp = result.get("weighted_prediction", {})
        pred = wp.get("prediction", "NEUTRAL")
        pred_tag = "bull" if pred == "BULLISH" else "bear" if pred == "BEARISH" else "neutral"
        t0.insert(tk.END, f" STOCK ANALYSIS: {ticker}\n", "hdr")
        t0.insert(tk.END, f" {'='*60}\n")
        t0.insert(tk.END, f" Overall: ", "dim")
        t0.insert(tk.END, f"{pred}", pred_tag)
        t0.insert(tk.END, f"  (signal {wp.get('signal',0):+.3f}, confidence {wp.get('confidence',0):.0%})\n\n", "dim")

        # ── Narrative Summary ──
        try:
            from stock_oracle.narrative import generate_narrative
            narrative = generate_narrative(result)
            t0.tag_configure("narrative", foreground="#c8d0e0", font=("Segoe UI", 10))
            t0.insert(tk.END, f" WHY THIS PREDICTION\n", "sub")
            t0.insert(tk.END, f" {'-'*40}\n", "dim")
            for para in narrative.split("\n\n"):
                t0.insert(tk.END, f" {para.strip()}\n\n", "narrative")
            t0.insert(tk.END, f" {'='*60}\n\n", "dim")
        except Exception:
            pass

        # ── Technical Analysis ──
        tech = _sig("technical_analysis")
        tech_raw = tech.get("raw_data", {}) if isinstance(tech.get("raw_data"), dict) else {}
        indicators = tech_raw.get("indicators", {})
        tech_signals = tech_raw.get("signals", [])

        t0.insert(tk.END, f" TECHNICAL ANALYSIS", "sub")
        tv = tech.get("signal", 0)
        ttag = "good" if tv > 0.05 else "bad" if tv < -0.05 else "neutral"
        t0.insert(tk.END, f"  {tv:+.2f}\n", ttag)

        if indicators:
            rsi = indicators.get("rsi", "?")
            rsi_tag = "good" if isinstance(rsi, (int, float)) and rsi < 40 else "bad" if isinstance(rsi, (int, float)) and rsi > 60 else "val"
            t0.insert(tk.END, f"   RSI(14): ", "key")
            t0.insert(tk.END, f"{rsi}", rsi_tag)
            if isinstance(rsi, (int, float)):
                if rsi < 30: t0.insert(tk.END, "  OVERSOLD", "good")
                elif rsi < 40: t0.insert(tk.END, "  low", "good")
                elif rsi > 70: t0.insert(tk.END, "  OVERBOUGHT", "bad")
                elif rsi > 60: t0.insert(tk.END, "  high", "bad")
            t0.insert(tk.END, "\n")

            macd_h = indicators.get("macd_histogram", 0)
            macd_tag = "good" if macd_h > 0 else "bad"
            t0.insert(tk.END, f"   MACD:    ", "key")
            t0.insert(tk.END, f"{'+'if macd_h>0 else ''}{macd_h:.3f}\n", macd_tag)

            bb = indicators.get("bollinger_position", 0.5)
            bb_tag = "good" if bb < 0.2 else "bad" if bb > 0.8 else "val"
            t0.insert(tk.END, f"   BB Pos:  ", "key")
            t0.insert(tk.END, f"{bb:.2f}", bb_tag)
            t0.insert(tk.END, f"  (0=lower band, 1=upper)\n", "dim")

            if "ma20" in indicators:
                pct = indicators.get("price_vs_ma20_pct", 0)
                t0.insert(tk.END, f"   MA20:    ${indicators['ma20']:.2f}  ({pct:+.1f}%)\n", "val")
            if "ma50" in indicators:
                t0.insert(tk.END, f"   MA50:    ${indicators['ma50']:.2f}\n", "val")
            if "ma200" in indicators:
                t0.insert(tk.END, f"   MA200:   ${indicators['ma200']:.2f}\n", "val")
            if "volume_ratio_5d_vs_20d" in indicators:
                vr = indicators["volume_ratio_5d_vs_20d"]
                vr_tag = "good" if vr > 1.3 else "bad" if vr < 0.7 else "val"
                t0.insert(tk.END, f"   Vol 5d/20d: ", "key")
                t0.insert(tk.END, f"{vr:.2f}x\n", vr_tag)

        if tech_signals:
            t0.insert(tk.END, f"   Signals: ", "dim")
            parts = [s[0] for s in tech_signals if abs(s[1]) > 0.05]
            t0.insert(tk.END, f"{', '.join(parts[:4])}\n", "val")
        t0.insert(tk.END, "\n")

        # ── Fundamental Analysis ──
        fund = _sig("fundamental_analysis")
        fund_raw = fund.get("raw_data", {}) if isinstance(fund.get("raw_data"), dict) else {}
        metrics = fund_raw.get("metrics", {})
        fund_scores = fund_raw.get("scores", [])

        t0.insert(tk.END, f" FUNDAMENTALS", "sub")
        fv = fund.get("signal", 0)
        ftag = "good" if fv > 0.05 else "bad" if fv < -0.05 else "neutral"
        t0.insert(tk.END, f"  {fv:+.2f}\n", ftag)

        metric_display = [
            ("pe", "P/E Ratio", None),
            ("peg", "PEG Ratio", lambda v: "good" if v < 1.0 else "bad" if v > 2.5 else "val"),
            ("profit_margin", "Profit Margin", lambda v: "good" if v > 15 else "bad" if v < 0 else "val"),
            ("revenue_growth", "Revenue Growth", lambda v: "good" if v > 10 else "bad" if v < 0 else "val"),
            ("earnings_growth", "Earnings Growth", lambda v: "good" if v > 10 else "bad" if v < -10 else "val"),
            ("debt_to_equity", "Debt/Equity", lambda v: "good" if v < 50 else "bad" if v > 200 else "val"),
            ("fcf_yield", "FCF Yield", lambda v: "good" if v > 4 else "bad" if v < 0 else "val"),
            ("roe", "ROE", lambda v: "good" if v > 20 else "bad" if v < 0 else "val"),
        ]

        for key, label, tag_fn in metric_display:
            if key in metrics:
                v = metrics[key]
                tag = tag_fn(v) if tag_fn else "val"
                suffix = "%" if key in ("profit_margin", "revenue_growth", "earnings_growth", "fcf_yield", "roe") else ""
                suffix = "x" if key == "peg" else suffix
                t0.insert(tk.END, f"   {label:18s} ", "key")
                t0.insert(tk.END, f"{v}{suffix}\n", tag)

        if fund_scores:
            t0.insert(tk.END, f"   Highlights: ", "dim")
            parts = [s[0] for s in fund_scores if abs(s[1]) > 0.05]
            t0.insert(tk.END, f"{', '.join(parts[:3])}\n", "val")
        t0.insert(tk.END, "\n")

        # ── Analyst Consensus ──
        analyst = _sig("analyst_consensus")
        analyst_raw = analyst.get("raw_data", {}) if isinstance(analyst.get("raw_data"), dict) else {}

        t0.insert(tk.END, f" ANALYST CONSENSUS", "sub")
        av = analyst.get("signal", 0)
        atag = "good" if av > 0.05 else "bad" if av < -0.05 else "neutral"
        t0.insert(tk.END, f"  {av:+.2f}\n", atag)

        if analyst_raw:
            if "current_price" in analyst_raw:
                t0.insert(tk.END, f"   Current:  ", "key")
                t0.insert(tk.END, f"${analyst_raw['current_price']:.2f}\n", "val")
            if "target_mean" in analyst_raw:
                upside = analyst_raw.get("target_upside_pct", 0)
                up_tag = "good" if upside > 10 else "bad" if upside < -5 else "val"
                t0.insert(tk.END, f"   Target:   ", "key")
                t0.insert(tk.END, f"${analyst_raw['target_mean']:.2f} ({upside:+.1f}%)\n", up_tag)
            if "recommendation" in analyst_raw:
                rec = analyst_raw["recommendation"]
                rec_tag = "good" if rec in ("buy","strong_buy") else "bad" if rec in ("sell","strong_sell") else "val"
                t0.insert(tk.END, f"   Rating:   ", "key")
                t0.insert(tk.END, f"{rec.upper()}", rec_tag)
                if "num_analysts" in analyst_raw:
                    t0.insert(tk.END, f"  ({analyst_raw['num_analysts']} analysts)", "dim")
                t0.insert(tk.END, "\n")
        t0.insert(tk.END, "\n")

        # ── Options & Short Interest ──
        opts = _sig("options_flow")
        opts_raw = opts.get("raw_data", {}) if isinstance(opts.get("raw_data"), dict) else {}
        short = _sig("short_interest")
        short_raw = short.get("raw_data", {}) if isinstance(short.get("raw_data"), dict) else {}

        t0.insert(tk.END, f" OPTIONS & SHORT INTEREST\n", "sub")
        if opts_raw:
            if "put_call_ratio_volume" in opts_raw:
                pcr = opts_raw["put_call_ratio_volume"]
                pcr_tag = "good" if pcr > 1.2 else "bad" if pcr < 0.5 else "val"
                t0.insert(tk.END, f"   Put/Call (vol): ", "key")
                t0.insert(tk.END, f"{pcr:.2f}", pcr_tag)
                if pcr > 1.3: t0.insert(tk.END, "  (fear/contrarian bullish)", "dim")
                elif pcr < 0.5: t0.insert(tk.END, "  (greed/contrarian bearish)", "dim")
                t0.insert(tk.END, "\n")
            if "total_options_volume" in opts_raw:
                t0.insert(tk.END, f"   Options Vol:    ", "key")
                t0.insert(tk.END, f"{opts_raw['total_options_volume']:,}\n", "val")

        if short_raw:
            if "short_pct_float" in short_raw:
                sp = short_raw["short_pct_float"]
                sp_tag = "bad" if sp > 10 else "good" if sp < 3 else "val"
                t0.insert(tk.END, f"   Short % Float:  ", "key")
                t0.insert(tk.END, f"{sp:.1f}%\n", sp_tag)
            if "days_to_cover" in short_raw:
                t0.insert(tk.END, f"   Days to Cover:  ", "key")
                t0.insert(tk.END, f"{short_raw['days_to_cover']:.1f}\n", "val")
            if "short_change_monthly" in short_raw:
                sc = short_raw["short_change_monthly"]
                sc_tag = "good" if sc < -5 else "bad" if sc > 5 else "val"
                t0.insert(tk.END, f"   Short MoM:      ", "key")
                t0.insert(tk.END, f"{sc:+.1f}%\n", sc_tag)
        t0.insert(tk.END, "\n")

        # ── Sentiment Summary ──
        t0.insert(tk.END, f" SENTIMENT SUMMARY\n", "sub")
        sentiment_collectors = ["news_sentiment", "reddit_sentiment", "hackernews_sentiment",
                                 "employee_sentiment", "viral_catalyst"]
        for sname in sentiment_collectors:
            s = _sig(sname)
            sv = s.get("signal", 0)
            cv = s.get("confidence", 0)
            if cv > 0.05:
                stag = "good" if sv > 0.05 else "bad" if sv < -0.05 else "dim"
                t0.insert(tk.END, f"   {sname:24s} ", "key")
                t0.insert(tk.END, f"{sv:+.3f} ({cv:.0%})\n", stag)

        # Show default tab
        show_tab("Analysis")

        # ── TAB: All Signals ───────────────────────────────────
        t1 = scrolledtext.ScrolledText(
            tabs["All Signals"], bg=BG_DARK, fg=FG_PRIMARY, font=("Consolas", 10),
            wrap="word", borderwidth=0, highlightthickness=0,
        )
        t1.pack(fill="both", expand=True)

        for w in [t1]:
            w.tag_configure("hdr", foreground=BLUE, font=("Consolas", 11, "bold"))
            w.tag_configure("bull", foreground=GREEN)
            w.tag_configure("bear", foreground=RED)
            w.tag_configure("neutral", foreground=AMBER)
            w.tag_configure("dim", foreground=FG_DIM)
            w.tag_configure("key", foreground=BLUE)
            w.tag_configure("val", foreground=FG_PRIMARY)

        all_sorted = sorted(signals, key=lambda x: abs(x.get("signal", 0)), reverse=True)
        t1.insert(tk.END, f" ALL {len(signals)} COLLECTORS\n", "hdr")
        t1.insert(tk.END, f" {'='*70}\n\n")

        for s in all_sorted:
            sv = s.get("signal", 0)
            cv = s.get("confidence", 0)
            name = s.get("collector", "?")
            details = s.get("details", "")

            if cv <= 0.1:
                icon, tag = "[OFF]", "dim"
            elif sv > 0.1:
                icon, tag = "[BUY]", "bull"
            elif sv < -0.1:
                icon, tag = "[SELL]", "bear"
            else:
                icon, tag = "[HOLD]", "neutral"

            t1.insert(tk.END, f" {icon} ", tag)
            t1.insert(tk.END, f"{name}\n", "hdr" if cv > 0.1 else "dim")
            t1.insert(tk.END, f"     Signal: ", "dim")
            t1.insert(tk.END, f"{sv:+.4f}", tag)
            t1.insert(tk.END, f"  |  Confidence: ", "dim")
            t1.insert(tk.END, f"{cv:.0%}\n", "val")
            if details:
                t1.insert(tk.END, f"     {details}\n", "val")

            raw = s.get("raw_data")
            if raw and isinstance(raw, dict) and cv > 0.1:
                t1.insert(tk.END, f"     Raw data:\n", "dim")
                for k, v in raw.items():
                    if isinstance(v, list) and len(v) > 5:
                        t1.insert(tk.END, f"       {k}: ", "key")
                        t1.insert(tk.END, f"[{len(v)} items]\n", "dim")
                    elif isinstance(v, dict):
                        t1.insert(tk.END, f"       {k}:\n", "key")
                        for k2, v2 in v.items():
                            t1.insert(tk.END, f"         {k2}: ", "dim")
                            t1.insert(tk.END, f"{v2}\n", "val")
                    else:
                        t1.insert(tk.END, f"       {k}: ", "key")
                        t1.insert(tk.END, f"{v}\n", "val")

            t1.insert(tk.END, f"     {'- '*35}\n\n", "dim")

        # ── TAB: ML Analysis ───────────────────────────────────
        t2 = scrolledtext.ScrolledText(
            tabs["ML Analysis"], bg=BG_DARK, fg=FG_PRIMARY, font=("Consolas", 10),
            wrap="word", borderwidth=0, highlightthickness=0,
        )
        t2.pack(fill="both", expand=True)
        for w in [t2]:
            w.tag_configure("hdr", foreground=BLUE, font=("Consolas", 11, "bold"))
            w.tag_configure("bull", foreground=GREEN)
            w.tag_configure("bear", foreground=RED)
            w.tag_configure("neutral", foreground=AMBER)
            w.tag_configure("dim", foreground=FG_DIM)
            w.tag_configure("key", foreground=PURPLE)
            w.tag_configure("val", foreground=FG_PRIMARY)

        wp = result.get("weighted_prediction", {})
        t2.insert(tk.END, f" TIERED WEIGHTED PREDICTION\n", "hdr")
        t2.insert(tk.END, f" {'='*50}\n")
        wp_tag = "bull" if wp.get("prediction") == "BULLISH" else "bear" if wp.get("prediction") == "BEARISH" else "neutral"
        t2.insert(tk.END, f" Prediction: ", "dim")
        t2.insert(tk.END, f"{wp.get('prediction', '?')}\n", wp_tag)
        t2.insert(tk.END, f" Signal: {wp.get('signal', 0):+.4f}  |  Confidence: {wp.get('confidence', 0):.0%}", "val")
        core = wp.get("core_analysis_score", 0)
        core_tag = "bull" if core > 0.6 else "bear" if core < 0.4 else "neutral"
        t2.insert(tk.END, f"  |  Core Conviction: ", "dim")
        t2.insert(tk.END, f"{core:.0%}\n\n", core_tag)

        breakdown = wp.get("signal_breakdown", {})
        if breakdown:
            tier_labels = {1: "T1 Analysis", 2: "T2 Data", 3: "T3 Proxy"}
            # Group by tier
            for tier_num in [1, 2, 3]:
                tier_items = [(sname, info) for sname, info in breakdown.items()
                              if info.get("tier") == tier_num and info.get("confidence", 0) > 0.01]
                if not tier_items:
                    continue
                tier_items.sort(key=lambda x: abs(x[1].get("signal",0)*x[1].get("weight",0)*x[1].get("confidence",0)), reverse=True)
                mult = {1: "2x", 2: "1x", 3: "0.5x"}[tier_num]
                t2.insert(tk.END, f" {tier_labels[tier_num]} (weight {mult}):\n", "hdr")
                for sname, info in tier_items:
                    sv = info.get("signal", 0)
                    w = info.get("weight", 0)
                    c = info.get("confidence", 0)
                    contrib = sv * w * c * {1:2.0, 2:1.0, 3:0.5}[tier_num]
                    ctag = "bull" if contrib > 0.003 else "bear" if contrib < -0.003 else "dim"
                    t2.insert(tk.END, f"   {sname:24s} ", "dim")
                    t2.insert(tk.END, f"sig={sv:+.3f} x wt={w:.2f} x conf={c:.0%}", "val")
                    t2.insert(tk.END, f" = {contrib:+.4f}\n", ctag)
                t2.insert(tk.END, "\n")

        ml_pred = result.get("ml_prediction", {})
        if ml_pred:
            t2.insert(tk.END, f"\n\n ML ENSEMBLE PREDICTION\n", "hdr")
            t2.insert(tk.END, f" {'='*50}\n")
            ml_tag = "bull" if ml_pred.get("prediction") == "BULLISH" else "bear" if ml_pred.get("prediction") == "BEARISH" else "neutral"
            t2.insert(tk.END, f" Prediction: ", "dim")
            t2.insert(tk.END, f"{ml_pred.get('prediction', '?')}\n", ml_tag)
            t2.insert(tk.END, f" Confidence: {ml_pred.get('confidence', 0):.0%}\n\n", "val")

            votes = ml_pred.get("model_votes", {})
            if votes:
                t2.insert(tk.END, f" Model votes:\n", "dim")
                for model, vote in votes.items():
                    vtag = "bull" if vote == "BULLISH" else "bear" if vote == "BEARISH" else "neutral"
                    t2.insert(tk.END, f"   {model:20s} -> ", "dim")
                    t2.insert(tk.END, f"{vote}\n", vtag)

            probs = ml_pred.get("probabilities", {})
            if probs:
                t2.insert(tk.END, f"\n Probabilities:\n", "dim")
                for label, prob in probs.items():
                    bar = "#" * int(float(prob) * 40)
                    ptag = "bull" if label == "bullish" else "bear" if label == "bearish" else "neutral"
                    t2.insert(tk.END, f"   {label:10s} ", "dim")
                    t2.insert(tk.END, f"{float(prob):.1%} {bar}\n", ptag)

            importance = ml_pred.get("feature_importance", {})
            if importance:
                t2.insert(tk.END, f"\n Top predictive features:\n", "dim")
                for fname, score in list(importance.items())[:15]:
                    bar = "#" * int(float(score) * 100)
                    t2.insert(tk.END, f"   {fname:35s} ", "key")
                    t2.insert(tk.END, f"{float(score):.4f} {bar}\n", "val")

        # ── TAB: Price Data ────────────────────────────────────
        t3 = scrolledtext.ScrolledText(
            tabs["Price Data"], bg=BG_DARK, fg=FG_PRIMARY, font=("Consolas", 10),
            wrap="word", borderwidth=0, highlightthickness=0,
        )
        t3.pack(fill="both", expand=True)
        for w in [t3]:
            w.tag_configure("hdr", foreground=BLUE, font=("Consolas", 11, "bold"))
            w.tag_configure("bull", foreground=GREEN)
            w.tag_configure("bear", foreground=RED)
            w.tag_configure("dim", foreground=FG_DIM)
            w.tag_configure("val", foreground=FG_PRIMARY)
            w.tag_configure("key", foreground=AMBER)

        fh = next((s for s in signals if s.get("collector") == "finnhub_realtime"), {})
        fh_raw = fh.get("raw_data", {}) if isinstance(fh.get("raw_data"), dict) else {}
        if fh_raw:
            t3.insert(tk.END, f" REAL-TIME (Finnhub)\n", "hdr")
            t3.insert(tk.END, f" {'='*50}\n")
            t3.insert(tk.END, f" Price:      ", "dim")
            t3.insert(tk.END, f"${fh_raw.get('price', 0):.2f}\n", "val")
            t3.insert(tk.END, f" Open:       ${fh_raw.get('open', 0):.2f}\n", "val")
            t3.insert(tk.END, f" Prev Close: ${fh_raw.get('prev_close', 0):.2f}\n", "val")
            t3.insert(tk.END, f" High:       ${fh_raw.get('high', 0):.2f}\n", "val")
            t3.insert(tk.END, f" Low:        ${fh_raw.get('low', 0):.2f}\n", "val")
            dc = fh_raw.get('daily_change', 0)
            t3.insert(tk.END, f" Day Change: ", "dim")
            t3.insert(tk.END, f"{dc:+.2%}\n", "bull" if dc > 0 else "bear")
            t3.insert(tk.END, f" Range Pos:  {fh_raw.get('range_position', 0):.0%}\n", "val")
            ah = fh_raw.get('after_hours_move', 0)
            if abs(ah) > 0.001:
                t3.insert(tk.END, f" AH Move:    ", "dim")
                t3.insert(tk.END, f"{ah:+.2%}\n", "bull" if ah > 0 else "bear")
            sess = fh_raw.get('session', {})
            if sess:
                t3.insert(tk.END, f" Session:    {sess.get('detail', '?')}\n\n", "key")

            recs = fh_raw.get('analyst_recs', {})
            if recs and recs.get('buy', 0) + recs.get('sell', 0) + recs.get('hold', 0) > 0:
                t3.insert(tk.END, f" ANALYST RECOMMENDATIONS\n", "hdr")
                t3.insert(tk.END, f" {'='*50}\n")
                t3.insert(tk.END, f" Buy:  {recs.get('buy', 0):3d}  ", "bull")
                t3.insert(tk.END, f"{'#' * recs.get('buy', 0)}\n", "bull")
                t3.insert(tk.END, f" Hold: {recs.get('hold', 0):3d}  ", "dim")
                t3.insert(tk.END, f"{'#' * recs.get('hold', 0)}\n", "dim")
                t3.insert(tk.END, f" Sell: {recs.get('sell', 0):3d}  ", "bear")
                t3.insert(tk.END, f"{'#' * recs.get('sell', 0)}\n\n", "bear")

        yf_sig = next((s for s in signals if s.get("collector") == "yahoo_finance"), {})
        yf_raw = yf_sig.get("raw_data", {}) if isinstance(yf_sig.get("raw_data"), dict) else {}
        if yf_raw:
            t3.insert(tk.END, f" TECHNICAL INDICATORS (Yahoo)\n", "hdr")
            t3.insert(tk.END, f" {'='*50}\n")
            rsi = yf_raw.get("rsi", 0)
            rsi_tag = "bear" if rsi > 70 else "bull" if rsi < 30 else "val"
            rsi_label = " (overbought)" if rsi > 70 else " (oversold)" if rsi < 30 else ""
            t3.insert(tk.END, f" RSI (14):   ", "dim")
            t3.insert(tk.END, f"{rsi:.0f}{rsi_label}\n", rsi_tag)
            t3.insert(tk.END, f" MA20:       ${yf_raw.get('ma20', 0):.2f}\n", "val")
            t3.insert(tk.END, f" MA50:       ${yf_raw.get('ma50', 0):.2f}\n", "val")
            t3.insert(tk.END, f" Momentum:   {yf_raw.get('momentum', 0):+.2%}\n", "val")
            t3.insert(tk.END, f" Volume:     {yf_raw.get('volume_ratio', 0):.1f}x avg\n\n", "val")

        cs_sig = next((s for s in signals if s.get("collector") == "cross_stock"), {})
        cs_raw = cs_sig.get("raw_data", {}) if isinstance(cs_sig.get("raw_data"), dict) else {}
        if cs_raw:
            t3.insert(tk.END, f" CROSS-STOCK ANALYSIS\n", "hdr")
            t3.insert(tk.END, f" {'='*50}\n")
            sect = cs_raw.get("sector", {})
            if sect:
                t3.insert(tk.END, f" Sector: {sect.get('sector', '?')}\n", "key")
                t3.insert(tk.END, f"   Sector 5d return: {sect.get('sector_return_5d', 0):+.2%}\n", "val")
                tr = sect.get('ticker_return_5d')
                if tr is not None:
                    t3.insert(tk.END, f"   {ticker} 5d return: {tr:+.2%}\n", "val")
                    gap = sect.get('gap', 0)
                    gap_tag = "bull" if gap > 0.01 else "bear" if gap < -0.01 else "dim"
                    label = " (catch-up opportunity)" if gap > 0.01 else " (pullback risk)" if gap < -0.01 else ""
                    t3.insert(tk.END, f"   Gap: {gap:+.2%}{label}\n", gap_tag)

            ll = cs_raw.get("lead_lag", {})
            pairs = ll.get("pairs", [])
            if pairs:
                t3.insert(tk.END, f"\n Lead-lag signals:\n", "dim")
                for p in pairs:
                    t3.insert(tk.END, f"   {p['leader']:6s} moved ", "dim")
                    t3.insert(tk.END, f"{p['leader_return']:+.2%}", "bull" if p['leader_return'] > 0 else "bear")
                    t3.insert(tk.END, f" ({p['lag_days']}d lag, {p['correlation']:.0%} corr)\n", "dim")

            div = cs_raw.get("divergence", {})
            if div and div.get("divergence"):
                t3.insert(tk.END, f"\n Pair divergence (20d):\n", "dim")
                t3.insert(tk.END, f"   {ticker}: {div.get('ticker_20d', 0):+.2%}  vs  Peers: {div.get('peer_avg_20d', 0):+.2%}\n", "val")
                dv = div.get("divergence", 0)
                label = " -> mean reversion buy" if dv > 0.02 else " -> overextended" if dv < -0.02 else ""
                t3.insert(tk.END, f"   Divergence: {dv:+.2%}{label}\n", "bull" if dv > 0.02 else "bear" if dv < -0.02 else "dim")

            etf = cs_raw.get("etf_flow", {})
            if etf and etf.get("etf"):
                t3.insert(tk.END, f"\n ETF flow ({etf['etf']}):\n", "dim")
                t3.insert(tk.END, f"   5d return: {etf.get('etf_return_5d', 0):+.2%}  |  Volume: {etf.get('volume_ratio', 0):.1f}x avg\n", "val")

        # ── TAB: Raw JSON ──────────────────────────────────────
        t4 = scrolledtext.ScrolledText(
            tabs["Raw JSON"], bg=BG_DARK, fg=FG_SECONDARY, font=("Consolas", 9),
            wrap="word", borderwidth=0, highlightthickness=0,
        )
        t4.pack(fill="both", expand=True)
        try:
            raw_json = json.dumps(result, indent=2, default=str)
            t4.insert(tk.END, raw_json)
        except Exception as e:
            t4.insert(tk.END, f"Error serializing: {e}")

        # Show first tab
        # Default tab already set to "Analysis" above

    # ── ML Controls ────────────────────────────────────────────

    def _check_ml_status(self):
        """Check if ML models are trained."""
        model_path = MODEL_DIR / "ensemble_models.pkl"
        if model_path.exists():
            import os
            size = os.path.getsize(model_path)
            self.ml_status_label.configure(
                text=f"Trained ({size/1024:.0f} KB)", foreground=GREEN)
        else:
            self.ml_status_label.configure(
                text="Not trained - generate history first", foreground=AMBER)

    def _generate_historical(self):
        """Generate historical training data."""
        if not self.watchlist:
            messagebox.showwarning("No tickers", "Add tickers to your watchlist first.")
            return

        self._log(f"Generating historical data for {len(self.watchlist)} tickers...")
        self.status_label.configure(text="Generating training data...", foreground=AMBER)

        def run():
            try:
                from stock_oracle.historical_trainer import (
                    generate_historical_samples, get_historical_stats,
                    diagnose_yfinance,
                )

                self.msg_queue.put(("log", "Running yfinance diagnostic..."))
                diag = diagnose_yfinance("AAPL")
                for msg in diag.get("messages", []):
                    self.msg_queue.put(("log", f"  DIAG: {msg}"))

                if not diag.get("ok"):
                    self.msg_queue.put(("log", "yfinance diagnostic FAILED"))
                    self.msg_queue.put(("log", "Try: pip install --upgrade yfinance"))
                    self.msg_queue.put(("status", "yfinance error"))
                    return

                self.msg_queue.put(("log", "yfinance OK, generating history..."))

                import logging
                class GUILogHandler(logging.Handler):
                    def __init__(self, q):
                        super().__init__()
                        self.q = q
                    def emit(self, record):
                        try:
                            self.q.put(("log", f"  {record.getMessage()}"))
                        except Exception:
                            pass

                gui_handler = GUILogHandler(self.msg_queue)
                gui_handler.setLevel(logging.INFO)
                logging.getLogger("stock_oracle").addHandler(gui_handler)

                total = 0
                for i, ticker in enumerate(self.watchlist):
                    self.msg_queue.put(("status", f"History: {ticker} ({i+1}/{len(self.watchlist)})"))
                    try:
                        count = generate_historical_samples(ticker, days_back=365)
                        total += count
                        self.msg_queue.put(("log", f"  {ticker}: {count} samples"))
                    except Exception as e:
                        import traceback
                        self.msg_queue.put(("log", f"  {ticker}: FAILED - {e}"))

                logging.getLogger("stock_oracle").removeHandler(gui_handler)

                stats = get_historical_stats()
                outcomes = stats.get("outcomes", {})
                self.msg_queue.put(("log",
                    f"Done! {stats.get('total_samples', 0)} total samples. "
                    f"Outcomes: {outcomes}"
                ))
                self.msg_queue.put(("status",
                    f"History ready ({stats.get('total_samples', 0)} samples)"))
            except Exception as e:
                import traceback
                self.msg_queue.put(("log", f"Error generating history: {e}"))
                self.msg_queue.put(("log", traceback.format_exc()))
                self.msg_queue.put(("status", "Error"))

        threading.Thread(target=run, daemon=True).start()

    def _train_ml(self):
        """Train ML models."""
        self._log("Training ML models...")
        self.status_label.configure(text="Training ML...", foreground=AMBER)

        def run():
            try:
                from stock_oracle.historical_trainer import load_historical_training_data
                from stock_oracle.prediction_tracker import PredictionTracker
                from stock_oracle.session_tracker import SessionTracker
                from stock_oracle.ml.pipeline import StockPredictor

                # 1. Historical (synthetic) training data — bulk baseline
                hist_data = load_historical_training_data()

                # 2. Verified 5-day predictions (REAL outcomes — highest quality)
                tracker = PredictionTracker()
                verified_5d = tracker.get_verified_training_data()

                # 3. Verified intraday predictions (shorter horizon, more samples)
                verified_intraday = SessionTracker.get_intraday_training_data()

                # Merge with quality weighting:
                #   Historical (synthetic): 1x — bulk baseline
                #   5-day verified (real):  3x — real outcomes, best quality
                #   Intraday verified:      2x — real but shorter horizon
                combined = (hist_data +
                            verified_5d * 3 +
                            verified_intraday * 2)

                if len(combined) < 50:
                    self.msg_queue.put(("log",
                        f"Need 50+ samples (have {len(hist_data)} historical + "
                        f"{len(verified_5d)} 5-day + {len(verified_intraday)} intraday). "
                        f"Generate history first."))
                    self.msg_queue.put(("status", "Need more data"))
                    return

                predictor = StockPredictor()
                predictor.train(combined)

                self.msg_queue.put(("log",
                    f"ML trained on {len(combined)} samples: "
                    f"{len(hist_data)} hist + {len(verified_5d)} 5d(x3) + "
                    f"{len(verified_intraday)} intraday(x2)"))
                self.msg_queue.put(("status", "ML trained"))

                with self._oracle_lock:
                    self.oracle = None
                self.msg_queue.put(("log", "Oracle will reload with ML on next analysis"))
                self.root.after(100, self._check_ml_status)

            except Exception as e:
                self.msg_queue.put(("log", f"Training error: {e}"))
                self.msg_queue.put(("status", "Training failed"))

        threading.Thread(target=run, daemon=True).start()

    def _export_csv(self):
        """Export current results to CSV file."""
        if not self.results:
            self._log("No results to export")
            return

        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filepath = DATA_DIR / f"export_{timestamp}.csv"

            lines = ["Ticker,Prediction,Intraday,Signal,Confidence,Core Conviction,"
                     "Price,Change %,RSI,Analyst Target,Target Upside %,"
                     "P/E,Margin %,Rev Growth %,Short %,Method"]

            for ticker, result in sorted(self.results.items()):
                pred = result.get("prediction", "")
                sig = result.get("signal", 0)
                conf = result.get("confidence", 0)
                wp = result.get("weighted_prediction", {})
                core = wp.get("core_analysis_score", 0)
                pinfo = self._get_live_price(result)
                method = result.get("method", "")

                # Intraday prediction
                intra_pred = ""
                if hasattr(self, 'session_tracker') and self.session_tracker:
                    trend = self.session_tracker.get_trend(ticker)
                    if trend.get("scans", 0) >= 2:
                        intra_pred, _ = self._get_intraday_prediction(ticker, trend, result)

                # Extract analysis data
                signals = result.get("signals", [])
                def _s(name):
                    return next((s for s in signals if s.get("collector") == name), {})

                tech = _s("technical_analysis")
                tech_ind = (tech.get("raw_data", {}) or {}).get("indicators", {})
                rsi = tech_ind.get("rsi", "")

                analyst = _s("analyst_consensus")
                ar = analyst.get("raw_data", {}) or {}
                target = ar.get("target_mean", "")
                upside = ar.get("target_upside_pct", "")

                fund = _s("fundamental_analysis")
                fm = (fund.get("raw_data", {}) or {}).get("metrics", {})
                pe = fm.get("pe", "")
                margin = fm.get("profit_margin", "")
                rev_gr = fm.get("revenue_growth", "")

                short = _s("short_interest")
                sr = short.get("raw_data", {}) or {}
                short_pct = sr.get("short_pct_float", "")

                line = (f"{ticker},{pred},{intra_pred},{sig:+.4f},{conf:.4f},{core:.3f},"
                        f"{pinfo['price']:.2f},{pinfo['change']:.4f},"
                        f"{rsi},{target},{upside},"
                        f"{pe},{margin},{rev_gr},{short_pct},{method}")
                lines.append(line)

            filepath.write_text("\n".join(lines), encoding="utf-8")
            self._log(f"Exported {len(self.results)} results to {filepath.name}")

            # Try to open the file
            try:
                import subprocess
                subprocess.Popen(["notepad.exe", str(filepath)])
            except Exception:
                pass

        except Exception as e:
            self._log(f"Export error: {e}")

    # ── Prediction Verification & Accuracy ─────────────────────

    def _verify_predictions(self):
        """Manually trigger prediction verification."""
        self._log("Verifying pending predictions...")
        self.status_label.configure(text="Verifying...", foreground=AMBER)

        def run():
            try:
                from stock_oracle.prediction_tracker import PredictionTracker
                tracker = PredictionTracker()
                stats = tracker.verify_pending()

                verified = stats.get("verified", 0)
                correct = stats.get("correct", 0)
                wrong = stats.get("wrong", 0)
                skipped = stats.get("skipped", 0)

                if verified > 0:
                    self.msg_queue.put(("log",
                        f"Verified {verified} predictions: "
                        f"{correct} correct, {wrong} wrong, {skipped} skipped"))
                else:
                    pending = tracker.get_pending_count()
                    if pending > 0:
                        self.msg_queue.put(("log",
                            f"No predictions ready to verify yet ({pending} pending, "
                            f"need {tracker.horizon} days to pass)"))
                    else:
                        self.msg_queue.put(("log",
                            "No pending predictions. Run Analyze All first."))

                self.msg_queue.put(("status", f"Verified {verified}"))
                self.msg_queue.put(("update_accuracy", None))

            except Exception as e:
                self.msg_queue.put(("log", f"Verification error: {e}"))
                self.msg_queue.put(("status", "Verify failed"))

        threading.Thread(target=run, daemon=True).start()

    def _auto_verify(self):
        """Background auto-verification — runs silently."""
        def run():
            try:
                from stock_oracle.prediction_tracker import PredictionTracker
                tracker = PredictionTracker()
                stats = tracker.verify_pending()
                if stats.get("verified", 0) > 0:
                    self.msg_queue.put(("log",
                        f"Auto-verified {stats['verified']} predictions "
                        f"({stats['correct']} correct, {stats['wrong']} wrong)"))
                    self.msg_queue.put(("update_accuracy", None))
            except Exception:
                pass

        threading.Thread(target=run, daemon=True).start()

    def _auto_retrain(self):
        """
        Background ML retrain during monitoring.
        Merges historical + 5-day verified + intraday verified data,
        retrains the model, and hot-swaps it into the running oracle.
        """
        def run():
            try:
                from stock_oracle.historical_trainer import load_historical_training_data
                from stock_oracle.prediction_tracker import PredictionTracker
                from stock_oracle.session_tracker import SessionTracker
                from stock_oracle.ml.pipeline import StockPredictor

                hist_data = load_historical_training_data()
                verified_5d = PredictionTracker().get_verified_training_data()
                verified_intraday = SessionTracker.get_intraday_training_data()

                combined = (hist_data +
                            verified_5d * 3 +
                            verified_intraday * 2)

                if len(combined) < 50:
                    return  # Not enough data, skip silently

                predictor = StockPredictor()
                predictor.train(combined)

                # Hot-swap: reload oracle with new models on next scan
                with self._oracle_lock:
                    if self.oracle:
                        self.oracle.predictor.load_models()
                        self.msg_queue.put(("log",
                            f"Auto-retrain complete: {len(combined)} samples "
                            f"({len(hist_data)}h + {len(verified_5d)}v5d + "
                            f"{len(verified_intraday)}vid). Models hot-swapped."))
                    else:
                        self.msg_queue.put(("log", "Auto-retrain complete, will load on next scan"))

                self.msg_queue.put(("update_accuracy", None))

            except Exception as e:
                self.msg_queue.put(("log", f"Auto-retrain error: {e}"))

        threading.Thread(target=run, daemon=True).start()

    def _update_accuracy_display(self):
        """Refresh the accuracy scorecard in the GUI."""
        try:
            from stock_oracle.prediction_tracker import PredictionTracker
            from stock_oracle.session_tracker import SessionTracker
            tracker = PredictionTracker()
            stats = tracker.get_accuracy_stats()

            total = stats.get("total_verified", 0)
            pending = stats.get("pending_verification", 0)

            # Get intraday stats
            intraday_text = ""
            if hasattr(self, 'session_tracker') and self.session_tracker:
                sess = self.session_tracker.get_session_stats()
                if sess["intraday_verified"] > 0:
                    intraday_text = (f"Session: {sess['intraday_accuracy']:.0f}% exact, "
                                     f"{sess['intraday_directional']:.0f}% dir "
                                     f"({sess['intraday_verified']} checks, scan #{sess['total_scans']})")
                elif sess["total_scans"] > 0:
                    intraday_text = f"Session: scan #{sess['total_scans']} (verifying after scan #{sess['total_scans'] + 3})"

            hist_intraday = SessionTracker.get_intraday_accuracy_summary()
            if hist_intraday["verified"] > 0:
                if intraday_text:
                    intraday_text += f"\nAll intraday: {hist_intraday['directional']:.0f}% dir ({hist_intraday['verified']})"
                else:
                    intraday_text = f"Intraday: {hist_intraday['directional']:.0f}% directional ({hist_intraday['verified']} verified)"

            if total == 0:
                if pending > 0 or intraday_text:
                    main_text = intraday_text if intraday_text else f"Tracking {pending} (verify in {PREDICTION_HORIZON_DAYS}d)"
                    self.accuracy_label.configure(text=main_text, fg=AMBER)
                    self.accuracy_detail.configure(text=f"{pending} pending 5-day verification" if pending else "")
                else:
                    self.accuracy_label.configure(
                        text="No verified predictions yet",
                        fg=FG_SECONDARY)
                    self.accuracy_detail.configure(text="Run Analyze All to start tracking")
                return

            accuracy = stats.get("accuracy_pct", 0)
            directional = stats.get("directional_accuracy", 0)
            correct = stats.get("total_correct", 0)

            color = GREEN if accuracy >= 60 else AMBER if accuracy >= 45 else RED

            main_line = (f"5-Day: {accuracy:.0f}% exact, {directional:.0f}% dir "
                         f"({correct}/{total})  |  {pending} pending")
            if intraday_text:
                main_line += f"\n{intraday_text}"

            self.accuracy_label.configure(text=main_line, fg=color)

            # Detail line: per-type breakdown
            type_stats = stats.get("prediction_type_stats", {})
            parts = []
            for pred_type in ["BULLISH", "BEARISH", "NEUTRAL"]:
                ts = type_stats.get(pred_type, {})
                t_total = ts.get("total", 0)
                t_correct = ts.get("correct", 0)
                if t_total > 0:
                    t_pct = round(t_correct / t_total * 100)
                    parts.append(f"{pred_type[:4]}={t_pct}%({t_correct}/{t_total})")

            detail = "  ".join(parts)

            best = stats.get("best_tickers", [])
            worst = stats.get("worst_tickers", [])
            if best:
                t, s = best[0]
                detail += f"  |  Best: {t} {s['accuracy']:.0f}%"
            if worst and len(worst) > 0:
                t, s = worst[-1]
                if s["accuracy"] < accuracy:
                    detail += f"  Worst: {t} {s['accuracy']:.0f}%"

            self.accuracy_detail.configure(text=detail)

        except Exception:
            pass

    # ── Market Session ─────────────────────────────────────────

    def _update_market_session(self):
        """Update the market session indicator in the top bar."""
        try:
            from stock_oracle.collectors.finnhub_collector import get_market_session
            session = get_market_session()

            text = session.get("detail", "Unknown")
            sess_type = session.get("session", "closed")

            if sess_type == "regular":
                self.market_label.configure(text=f"  {text}", fg=GREEN)
            elif sess_type == "pre_market":
                self.market_label.configure(text=f"  {text}", fg=AMBER)
            elif sess_type == "after_hours":
                self.market_label.configure(text=f"  {text}", fg=PURPLE)
            else:
                self.market_label.configure(text=f"  {text}", fg=FG_DIM)
        except Exception:
            self.market_label.configure(text="", fg=FG_DIM)

    # ── News Feed ────────────────────────────────────────────────

    def _get_news_feed(self):
        """Get or create the news feed instance."""
        if not hasattr(self, '_news_feed_instance'):
            self._news_feed_instance = None
        if self._news_feed_instance is None:
            try:
                from stock_oracle.news_feed import NewsFeed
                # Get API key from settings
                settings = self._load_settings()
                import stock_oracle.config as cfg
                api_key = settings.get("FINNHUB_API_KEY", "") or cfg.FINNHUB_API_KEY
                if api_key:
                    self._news_feed_instance = NewsFeed(api_key=api_key)
            except Exception:
                pass
        return self._news_feed_instance

    def _open_news_feed(self):
        """Open a news feed popup showing recent news for all watchlist stocks."""
        self._log("Loading news feed...")

        def run():
            feed = self._get_news_feed()
            if not feed:
                self.msg_queue.put(("log", "News feed: no Finnhub API key. Add one in Settings."))
                return

            # Get market news + per-ticker news
            market_news = feed.get_market_news(max_articles=10)
            watchlist_news = feed.get_watchlist_news(
                self.watchlist, days=2, max_per_ticker=5, max_total=50
            )
            self.msg_queue.put(("_news_feed_results", market_news, watchlist_news))

        threading.Thread(target=run, daemon=True).start()

    def _show_news_feed(self, market_news: list, watchlist_news: list):
        """Display the news feed popup."""
        self._log(f"News feed: {len(watchlist_news)} watchlist + {len(market_news)} market articles")

        popup = tk.Toplevel(self.root)
        popup.title("Stock Oracle — News Feed")
        popup.geometry("800x650")
        popup.configure(bg=BG_DARK)
        popup.transient(self.root)

        # Header
        hdr = tk.Frame(popup, bg=BG_DARK)
        hdr.pack(fill="x", padx=12, pady=(12, 4))
        tk.Label(hdr, text="News Feed", bg=BG_DARK, fg=ACCENT,
                 font=("Segoe UI", 16, "bold")).pack(side="left")
        tk.Label(hdr, text=f"  {len(watchlist_news)} articles",
                 bg=BG_DARK, fg=FG_DIM,
                 font=("Segoe UI", 9)).pack(side="left", padx=(8, 0))

        # Filter bar
        filter_frame = tk.Frame(popup, bg=BG_DARK)
        filter_frame.pack(fill="x", padx=12, pady=(0, 8))

        # Results container
        results_container = tk.Frame(popup, bg=BG_DARK)
        results_container.pack(fill="both", expand=True, padx=12, pady=(0, 8))

        all_news = {"Market": market_news, "Watchlist": watchlist_news}

        # Per-ticker filter
        ticker_counts = {}
        for a in watchlist_news:
            t = a.get("ticker", "")
            if t:
                ticker_counts[t] = ticker_counts.get(t, 0) + 1

        def rebuild_news(filter_ticker="ALL"):
            for w in results_container.winfo_children():
                w.destroy()

            canvas = tk.Canvas(results_container, bg=BG_DARK, highlightthickness=0)
            scrollbar = ttk.Scrollbar(results_container, orient="vertical",
                                       command=canvas.yview)
            inner = tk.Frame(canvas, bg=BG_DARK)
            inner.bind("<Configure>",
                        lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
            canvas.create_window((0, 0), window=inner, anchor="nw")
            canvas.configure(yscrollcommand=scrollbar.set)
            canvas.pack(side="left", fill="both", expand=True)
            scrollbar.pack(side="right", fill="y")

            # Bind mousewheel
            def _mw(event):
                canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
            canvas.bind_all("<MouseWheel>", _mw)

            # Choose which articles to show
            if filter_ticker == "ALL":
                articles = watchlist_news
            elif filter_ticker == "MARKET":
                articles = market_news
            else:
                articles = [a for a in watchlist_news
                           if a.get("ticker") == filter_ticker]

            for article in articles:
                headline = article.get("headline", "")
                summary = article.get("summary", "")
                source = article.get("source", "")
                age = article.get("age", "")
                url = article.get("url", "")
                ticker = article.get("ticker", "")

                # Article card
                card = tk.Frame(inner, bg=BG_CARD, padx=10, pady=8,
                               highlightbackground=BORDER, highlightthickness=1)
                card.pack(fill="x", pady=2)

                # Top row: ticker badge + headline
                top_row = tk.Frame(card, bg=BG_CARD)
                top_row.pack(fill="x")

                if ticker:
                    tk.Label(top_row, text=f" {ticker} ", bg=ACCENT, fg="white",
                             font=("Consolas", 8, "bold"), padx=4
                             ).pack(side="left", padx=(0, 6))

                tk.Label(top_row, text=headline, bg=BG_CARD, fg=FG_PRIMARY,
                         font=("Segoe UI", 10, "bold"), anchor="w",
                         wraplength=650, justify="left"
                         ).pack(side="left", fill="x", expand=True)

                # Source and age
                meta = tk.Frame(card, bg=BG_CARD)
                meta.pack(fill="x", pady=(2, 0))

                tk.Label(meta, text=f"{source} · {age}", bg=BG_CARD, fg=FG_DIM,
                         font=("Segoe UI", 8)).pack(side="left")

                if url:
                    link_btn = tk.Label(meta, text="Read →", bg=BG_CARD,
                                         fg=BLUE, font=("Segoe UI", 8, "underline"),
                                         cursor="hand2")
                    link_btn.pack(side="right")
                    link_btn.bind("<Button-1>",
                                  lambda e, u=url: __import__("webbrowser").open(u))

                # Summary (truncated)
                if summary:
                    short_summary = summary[:200]
                    if len(summary) > 200:
                        last_period = short_summary.rfind(".")
                        if last_period > 80:
                            short_summary = short_summary[:last_period + 1]
                        else:
                            short_summary += "..."
                    tk.Label(card, text=short_summary, bg=BG_CARD,
                             fg=FG_SECONDARY, font=("Segoe UI", 9),
                             anchor="w", wraplength=700, justify="left"
                             ).pack(fill="x", pady=(4, 0))

        # Build filter buttons
        tk.Label(filter_frame, text="Show:", bg=BG_DARK, fg=FG_DIM,
                 font=("Consolas", 9)).pack(side="left")

        for label, key in [("All Watchlist", "ALL"), ("Market", "MARKET")]:
            tk.Button(filter_frame, text=label, bg=BG_CARD, fg=FG_PRIMARY,
                       font=("Segoe UI", 8, "bold"), borderwidth=0, padx=8, pady=2,
                       cursor="hand2",
                       command=lambda k=key: rebuild_news(k)
                       ).pack(side="left", padx=2)

        # Add per-ticker buttons for top tickers
        sorted_tickers = sorted(ticker_counts.items(), key=lambda x: -x[1])
        for ticker, count in sorted_tickers[:10]:
            tk.Button(filter_frame, text=f"{ticker} ({count})", bg=BG_CARD,
                       fg=FG_PRIMARY, font=("Segoe UI", 7), borderwidth=0,
                       padx=6, pady=2, cursor="hand2",
                       command=lambda t=ticker: rebuild_news(t)
                       ).pack(side="left", padx=1)

        # Initial view
        rebuild_news("ALL")

        # Close button
        tk.Button(popup, text="Close", bg=BG_CARD, fg=FG_PRIMARY,
                   font=("Segoe UI", 10), borderwidth=0, padx=16, pady=4,
                   cursor="hand2", command=popup.destroy
                   ).pack(pady=(0, 12))

    # ── Help Guide ──────────────────────────────────────────────

    def _open_help(self):
        """Open the built-in help guide."""
        win = tk.Toplevel(self.root)
        win.title("Stock Oracle — Help & Guide")
        win.geometry("750x650")
        win.configure(bg=BG_DARK)
        win.transient(self.root)

        # Header
        hdr = tk.Frame(win, bg=BG_PANEL, padx=16, pady=10)
        hdr.pack(fill="x")
        tk.Label(hdr, text="Stock Oracle Guide", bg=BG_PANEL, fg=FG_PRIMARY,
                 font=("Segoe UI", 16, "bold")).pack(side="left")

        # Tab-like buttons
        tab_frame = tk.Frame(win, bg=BG_DARK)
        tab_frame.pack(fill="x", padx=12, pady=(8, 0))

        content = scrolledtext.ScrolledText(
            win, bg=BG_DARK, fg=FG_PRIMARY, font=("Segoe UI", 10),
            wrap="word", borderwidth=0, highlightthickness=0,
            insertbackground=BG_DARK, padx=16, pady=12,
        )
        content.pack(fill="both", expand=True, padx=12, pady=(4, 12))
        content.tag_configure("h1", font=("Segoe UI", 14, "bold"), foreground=ACCENT)
        content.tag_configure("h2", font=("Segoe UI", 12, "bold"), foreground=BLUE)
        content.tag_configure("bold", font=("Segoe UI", 10, "bold"), foreground=FG_PRIMARY)
        content.tag_configure("dim", foreground=FG_DIM, font=("Segoe UI", 9))
        content.tag_configure("green", foreground=GREEN)
        content.tag_configure("amber", foreground=AMBER)

        tabs = {
            "Getting Started": self._help_getting_started,
            "Main Interface": self._help_interface,
            "Monitoring": self._help_monitoring,
            "Breakout Scanner": self._help_breakout,
            "News Feed": self._help_news,
            "Claude AI Advisor": self._help_claude,
            "Settings & API Keys": self._help_settings,
            "Tips & FAQ": self._help_faq,
        }

        def show_tab(tab_name):
            content.configure(state="normal")
            content.delete("1.0", "end")
            tabs[tab_name](content)
            content.configure(state="disabled")
            # Update button styling
            for btn in tab_btns:
                if btn.cget("text") == tab_name:
                    btn.configure(bg=ACCENT, fg="white")
                else:
                    btn.configure(bg=BG_CARD, fg=FG_PRIMARY)

        tab_btns = []
        for name in tabs:
            btn = tk.Button(tab_frame, text=name, bg=BG_CARD, fg=FG_PRIMARY,
                            font=("Segoe UI", 8, "bold"), borderwidth=0,
                            padx=8, pady=3, cursor="hand2",
                            command=lambda n=name: show_tab(n))
            btn.pack(side="left", padx=1)
            tab_btns.append(btn)

        show_tab("Getting Started")

    def _help_getting_started(self, t):
        t.insert("end", "Getting Started\n\n", "h1")
        t.insert("end", "What is Stock Oracle?\n", "h2")
        t.insert("end",
            "Stock Oracle is a stock prediction and monitoring system that combines "
            "38 different data collectors (technical indicators, news sentiment, SEC filings, "
            "social media, analyst ratings, and more), machine learning, and optional AI analysis "
            "to generate predictions for stocks you're interested in.\n\n"
            "It's designed as a research and experimentation tool — it tracks its own accuracy "
            "and learns from its mistakes over time.\n\n")

        t.insert("end", "Quick Start (5 minutes)\n", "h2")
        t.insert("end", "1. ", "bold")
        t.insert("end", "Add a Finnhub API key (free)\n", "green")
        t.insert("end",
            "   Click Settings → paste your key from finnhub.io/register\n"
            "   This gives you real-time stock prices. Without it, prices are 15min delayed.\n\n")

        t.insert("end", "2. ", "bold")
        t.insert("end", "Click \"Analyze All\" to run your first scan\n", "green")
        t.insert("end",
            "   This takes ~30 seconds. It pulls data from all 38 collectors for every stock\n"
            "   in your watchlist and generates predictions.\n\n")

        t.insert("end", "3. ", "bold")
        t.insert("end", "Click \"Start Monitoring\" for continuous tracking\n", "green")
        t.insert("end",
            "   The system scans every 5 minutes (configurable), tracks price changes,\n"
            "   verifies its predictions, and learns which signals actually work.\n\n")

        t.insert("end", "4. ", "bold")
        t.insert("end", "Try the Breakout Scanner\n", "green")
        t.insert("end",
            "   Click the gold \"Breakout Scan\" button to find which stocks on your\n"
            "   watchlist are most likely to make a big move. Click any result to see\n"
            "   what the company does.\n\n")

        t.insert("end", "Optional: Add more API keys in Settings for better data. ", "dim")
        t.insert("end", "The more data sources connected, the better the predictions.\n\n", "dim")

    def _help_interface(self, t):
        t.insert("end", "Main Interface\n\n", "h1")

        t.insert("end", "Left Panel — Watchlist\n", "h2")
        t.insert("end",
            "Shows all stocks you're tracking with live prices and daily changes. "
            "Type a ticker in the box at top and click + to add. "
            "Click any stock to see its signal details on the right.\n\n")

        t.insert("end", "Center — Stock Cards\n", "h2")
        t.insert("end",
            "Each card shows:\n"
            "  • Ticker, price, and daily change (green = up, red = down)\n"
            "  • 5D badge — the 5-day prediction (BULLISH / BEARISH / NEUTRAL)\n"
            "  • INTRA badge — real-time intraday trend (appears during monitoring)\n"
            "  • Signal bar — visual strength of the prediction signal\n"
            "  • Key metrics from the top collectors\n\n"
            "Click a card once for signal details. Double-click for a deep dive analysis.\n\n"
            "During after-hours (AH) or pre-market (PRE), the card shows two percentages:\n"
            "the regular session change (dimmed) and the AH/PRE change (bold).\n\n")

        t.insert("end", "Right Panel — Details & Controls\n", "h2")
        t.insert("end",
            "  • Signal Details — breakdown of what each collector found\n"
            "  • Company info — name, sector, industry, description\n"
            "  • Machine Learning — train the ML model on historical data\n"
            "  • Prediction Accuracy — how well the system is actually performing\n"
            "  • Claude Advisor — AI-powered analysis (needs API key)\n"
            "  • Activity Log — everything the system is doing\n\n")

        t.insert("end", "Sort & Filter\n", "h2")
        t.insert("end",
            "Use the sort buttons (Signal, Name, Change) to reorder cards. "
            "\"Quick Scan\" runs only the fastest collectors for a rapid update. "
            "\"Breakout Scan\" scores all stocks for breakout potential.\n\n")

    def _help_monitoring(self, t):
        t.insert("end", "Monitoring Mode\n\n", "h1")

        t.insert("end", "How it works\n", "h2")
        t.insert("end",
            "Click \"Start Monitoring\" to enter continuous mode. The system will:\n\n"
            "  1. Scan all watchlist stocks every N seconds (default 300 = 5 min)\n"
            "  2. Track price changes and signal trends over the session\n"
            "  3. Verify predictions from 3 scans ago against actual price movement\n"
            "  4. Run Claude AI check-ins (if configured) for weight adjustments\n"
            "  5. Auto-retrain the ML model when enough verified data accumulates\n\n")

        t.insert("end", "Signal Intelligence\n", "h2")
        t.insert("end",
            "The system learns during monitoring:\n\n"
            "  • Stale signals — collectors that return the same value every scan get\n"
            "    suppressed (many return static company data, not real-time signals)\n"
            "  • Volatility tracking — adapts conviction thresholds per ticker\n"
            "    (volatile stocks like LUNR need stronger signals for a BULL/BEAR call)\n"
            "  • Market session awareness — raises thresholds 50% during after-hours\n"
            "    and 100% when the market is closed (stale prices, thin volume)\n\n")

        t.insert("end", "What the accuracy numbers mean\n", "h2")
        t.insert("end",
            "  • Directional — did price move in the predicted direction?\n"
            "  • Exact — did the prediction match the actual outcome label?\n"
            "  • NEUTRAL accuracy is high because most stocks don't move much\n"
            "    intraday. The real test is BULL/BEAR call accuracy.\n\n")

    def _help_breakout(self, t):
        t.insert("end", "Breakout Scanner\n\n", "h1")

        t.insert("end",
            "The breakout scanner scores each stock 0-100 on how likely it is to\n"
            "break out upward. It checks 8 technical patterns:\n\n")

        signals = [
            ("BB Squeeze (15pts)", "SHORT/MED",
             "Bollinger Bands narrowing then expanding — compressed energy releasing"),
            ("Volume (15pts)", "SHORT/MED",
             "Rising volume with price climbing = institutional accumulation"),
            ("52W High (15pts)", "LONG",
             "Near or at 52-week high with momentum — breakout to new highs"),
            ("RSI Momentum (12pts)", "SHORT",
             "RSI 50-65 and accelerating — building power without being overbought"),
            ("MACD (12pts)", "SHORT",
             "Histogram crossing zero or expanding positive — momentum shift"),
            ("MA Alignment (12pts)", "MED/LONG",
             "Price > 20MA > 50MA — healthy uptrend with pullback to support"),
            ("Range (10pts)", "SHORT/MED",
             "ATR compressed then expanding upward — range breakout"),
            ("Rel Strength (9pts)", "LONG",
             "Outperforming SPY — sector/stock leadership"),
        ]

        for name, tf, desc in signals:
            t.insert("end", f"  {name}", "bold")
            t.insert("end", f" [{tf}]\n", "amber")
            t.insert("end", f"    {desc}\n\n", "dim")

        t.insert("end", "Grades\n", "h2")
        t.insert("end", "  STRONG (70+)", "green")
        t.insert("end", " — multiple strong signals aligning, high probability setup\n")
        t.insert("end", "  BUILDING (50-69)", "amber")
        t.insert("end", " — momentum building, watch for confirmation\n")
        t.insert("end", "  EARLY (30-49) — some positive signs, not yet confirmed\n")
        t.insert("end", "  NONE (<30) — no breakout setup detected\n\n")

        t.insert("end", "Timeframe Estimate\n", "h2")
        t.insert("end",
            "Each result shows an estimated timeframe (e.g. \"~3d quick pop\" or\n"
            "\"~2wk swing\") based on which signals are driving the score.\n"
            "Use the filter buttons to show only Short, Swing, or Position setups.\n"
            "Click any result to see what the company does.\n\n")

    def _help_news(self, t):
        t.insert("end", "News Feed\n\n", "h1")

        t.insert("end", "Two ways to see news:\n\n", "h2")
        t.insert("end", "1. Click any stock card", "bold")
        t.insert("end",
            " — the detail panel on the right shows the 5 most recent\n"
            "   headlines for that ticker with source, age, and a clickable link\n"
            "   to read the full article in your browser.\n\n")

        t.insert("end", "2. Click the \"News\" button", "bold")
        t.insert("end",
            " in the top bar — opens a full news feed popup with\n"
            "   articles from all your watchlist stocks plus general market news.\n"
            "   Use the filter buttons to show all, market-only, or a specific ticker.\n\n")

        t.insert("end", "Data source\n", "h2")
        t.insert("end",
            "News comes from Finnhub's company news API. It aggregates articles\n"
            "from Yahoo Finance, Reuters, MarketWatch, Seeking Alpha, Benzinga,\n"
            "and other major financial news sources. Requires a Finnhub API key\n"
            "(free at finnhub.io). Articles are cached for 10 minutes to avoid\n"
            "hitting rate limits.\n\n")

        t.insert("end", "The news feed does NOT influence predictions — it's purely ", "dim")
        t.insert("end", "informational so you can see what's driving price moves.\n\n", "dim")

    def _help_claude(self, t):
        t.insert("end", "Claude AI Advisor\n\n", "h1")

        t.insert("end", "What it does\n", "h2")
        t.insert("end",
            "During monitoring, Claude analyzes your signal data and provides:\n\n"
            "  • Weight adjustments — which collectors to trust more or less\n"
            "  • Pattern detection — trends, sector rotations, bias issues\n"
            "  • Alerts — notable market conditions or system issues\n"
            "  • Session reviews — end-of-day report card with recommendations\n\n")

        t.insert("end", "Cost\n", "h2")
        t.insert("end",
            "Uses Claude Haiku (cheapest model) at ~$0.009 per check-in.\n"
            "At hourly check-ins during market hours, that's roughly $5/month.\n"
            "There's a hard monthly spending cap (default $10) that auto-stops calls.\n\n")

        t.insert("end", "Setup\n", "h2")
        t.insert("end",
            "  1. Get an API key at console.anthropic.com\n"
            "  2. Add credit ($5-10 is plenty for months of use)\n"
            "  3. Paste the key in Settings → Anthropic API Key\n"
            "  4. Claude activates automatically during monitoring\n\n")

        t.insert("end", "\"Ask Claude\" lets you ask questions about your current predictions ", "dim")
        t.insert("end", "anytime. \"Session Review\" sends all verified data for a comprehensive analysis.\n\n", "dim")

    def _help_settings(self, t):
        t.insert("end", "Settings & API Keys\n\n", "h1")

        t.insert("end", "Essential (Recommended)\n", "h2")
        t.insert("end", "  Finnhub API Key", "bold")
        t.insert("end", " — FREE at finnhub.io/register\n"
                 "    Gives real-time stock prices. Without it, prices are ~15min delayed.\n\n")
        t.insert("end", "  Anthropic API Key", "bold")
        t.insert("end", " — console.anthropic.com\n"
                 "    Enables Claude AI advisor during monitoring. Costs ~$5/month.\n\n")

        t.insert("end", "Free Data APIs (Optional)\n", "h2")
        t.insert("end",
            "  • SEC EDGAR Email — format: 'StockOracle you@email.com'\n"
            "  • FRED API Key — free at fred.stlouisfed.org (economic data)\n"
            "  • Reddit Client ID/Secret — free at reddit.com/prefs/apps\n"
            "  • GitHub Token — free at github.com/settings/tokens\n\n"
            "Each additional key enables more data collectors, which improves\n"
            "prediction quality. The app works without any of them though.\n\n")

        t.insert("end", "Watchlist\n", "h2")
        t.insert("end",
            "Add tickers using the + button in the left panel. The default list\n"
            "includes major tech stocks, space companies, and ETFs. Add whatever\n"
            "you want to track — the system handles up to 50-60 tickers comfortably.\n\n")

        t.insert("end", "Interval\n", "h2")
        t.insert("end",
            "The scan interval (seconds) controls how often monitoring re-scans.\n"
            "Default is 300 (5 minutes). Lower = more data but higher API usage.\n"
            "Don't go below 120s or you'll hit rate limits on free APIs.\n\n")

    def _help_faq(self, t):
        t.insert("end", "Tips & FAQ\n\n", "h1")

        t.insert("end", "Why are most predictions NEUTRAL?\n", "h2")
        t.insert("end",
            "Because most stocks don't move significantly in any 5-minute window.\n"
            "NEUTRAL is the correct call ~70% of the time. The system is cautious —\n"
            "it only calls BULLISH or BEARISH when multiple dynamic signals agree\n"
            "above the volatility-adaptive threshold. Fewer conviction calls but\n"
            "higher quality ones.\n\n")

        t.insert("end", "What's Signal Intelligence?\n", "h2")
        t.insert("end",
            "Many collectors (analyst consensus, insider ratio, Reddit sentiment)\n"
            "return the SAME value every scan because they're pulling static company\n"
            "data, not real-time signals. Signal Intelligence detects these \"stale\"\n"
            "signals and suppresses their weight so they can't drive false\n"
            "conviction calls. Only signals that actually change between scans\n"
            "can trigger BULLISH/BEARISH predictions.\n\n")

        t.insert("end", "How does ML training work?\n", "h2")
        t.insert("end",
            "Click \"Generate History\" to create training data from 6 months of\n"
            "price history, then \"Train ML\" to build the model. During monitoring,\n"
            "the system also collects intraday verification data and periodically\n"
            "retrains. The ML blends with the weighted analysis (60/40 split).\n\n")

        t.insert("end", "Can I use this for real trading?\n", "h2")
        t.insert("end",
            "This is a research and experimentation tool, not financial advice.\n"
            "No algorithm can predict the stock market with certainty. The breakout\n"
            "scanner and predictions are educational tools — always do your own\n"
            "research and understand the risks before investing real money.\n\n")

        t.insert("end", "Where is my data stored?\n", "h2")
        t.insert("end",
            "If running from source: stock_oracle/data/\n"
            "If running the installer: %APPDATA%\\StockOracle\\\n"
            "This includes settings, predictions, sessions, and ML models.\n"
            "Updating the app never deletes your data.\n\n")

        t.insert("end", "Keyboard shortcut\n", "h2")
        t.insert("end",
            "Double-click any stock card for a deep-dive analysis with full\n"
            "signal breakdown, narrative summary, and technical details.\n\n")

    # ── Settings Dialog ─────────────────────────────────────────

    def _open_settings(self):
        """Open settings window for API keys and preferences."""
        win = tk.Toplevel(self.root)
        win.title("Settings")
        win.geometry("600x700")
        win.configure(bg=BG_DARK)
        win.transient(self.root)
        win.grab_set()

        # Load current settings
        settings = self._load_settings()

        # Scrollable content
        canvas = tk.Canvas(win, bg=BG_DARK, highlightthickness=0)
        scrollbar = ttk.Scrollbar(win, orient="vertical", command=canvas.yview)
        content = tk.Frame(canvas, bg=BG_DARK)
        content.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=content, anchor="nw", width=570)
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.pack(side="left", fill="both", expand=True, padx=8, pady=8)
        scrollbar.pack(side="right", fill="y")

        entries = {}

        def add_section(title):
            tk.Label(content, text=title, bg=BG_DARK, fg=BLUE,
                      font=("Segoe UI", 12, "bold")).pack(anchor="w", padx=12, pady=(16, 4))
            sep = tk.Frame(content, bg=BORDER, height=1)
            sep.pack(fill="x", padx=12, pady=(0, 8))

        def add_field(key, label, hint="", show=None):
            frame = tk.Frame(content, bg=BG_DARK)
            frame.pack(fill="x", padx=12, pady=3)

            tk.Label(frame, text=label, bg=BG_DARK, fg=FG_PRIMARY,
                      font=("Segoe UI", 10), width=22, anchor="w").pack(side="left")

            var = tk.StringVar(value=settings.get(key, ""))
            entry = tk.Entry(frame, textvariable=var, bg=BG_INPUT, fg=FG_PRIMARY,
                              font=("Consolas", 10), insertbackground=FG_PRIMARY,
                              borderwidth=1, relief="solid", show=show)
            entry.pack(side="left", fill="x", expand=True, padx=(4, 0))
            entries[key] = var

            if hint:
                tk.Label(content, text=f"     {hint}", bg=BG_DARK, fg=FG_DIM,
                          font=("Segoe UI", 8)).pack(anchor="w", padx=12)

        # ── API Keys ──────────────────────────────────

        add_section("Real-Time Data (pick one)")

        add_field("FINNHUB_API_KEY", "Finnhub API Key",
                   "Free at finnhub.io - real-time stock trades")
        add_field("ALPACA_KEY_ID", "Alpaca Key ID",
                   "Free at alpaca.markets - real-time + paper trading")
        add_field("ALPACA_SECRET", "Alpaca Secret", show="*")
        add_field("POLYGON_API_KEY", "Polygon.io API Key",
                   "Free tier = delayed, paid = real-time")

        add_section("Broker APIs")

        add_field("WEBULL_APP_KEY", "Webull App Key",
                   "Apply at Webull website > Account > API Management")
        add_field("WEBULL_APP_SECRET", "Webull App Secret", show="*")
        add_field("RH_EMAIL", "Robinhood Email")
        add_field("RH_PASSWORD", "Robinhood Password", show="*")
        add_field("RH_TOTP_SECRET", "Robinhood 2FA Secret",
                   "TOTP secret from authenticator app setup", show="*")

        add_section("Free Data APIs (recommended)")

        add_field("FRED_API_KEY", "FRED API Key",
                   "Free at fred.stlouisfed.org - economic indicators (cardboard index)")
        add_field("NEWS_API_KEY", "NewsAPI Key",
                   "Free at newsapi.org - enhanced news sentiment")
        add_field("SEC_USER_AGENT", "SEC EDGAR Email",
                   "Required format: 'StockOracle your@email.com'")
        add_field("REDDIT_CLIENT_ID", "Reddit Client ID",
                   "Free at reddit.com/prefs/apps - better Reddit access")
        add_field("REDDIT_CLIENT_SECRET", "Reddit Client Secret", show="*")
        add_field("GITHUB_TOKEN", "GitHub Token",
                   "Free at github.com/settings/tokens - avoids rate limits")

        add_section("Local AI (Ollama)")

        add_field("OLLAMA_URL", "Ollama URL",
                   "Default: http://localhost:11434")
        add_field("OLLAMA_MODEL", "Ollama Model",
                   "Default: qwen2.5:14b (for earnings call NLP)")

        add_section("Analysis Settings")

        add_field("PREDICTION_HORIZON_DAYS", "Prediction Horizon (days)",
                   "How many days ahead to predict (default: 5)")
        add_field("MONITOR_INTERVAL", "Monitor Interval (sec)",
                   "How often to re-scan in monitoring mode (default: 300)")
        add_field("BACKTEST_STOP_LOSS", "Stop Loss %",
                   "As decimal: 0.05 = 5% (default: 0.05)")
        add_field("BACKTEST_TAKE_PROFIT", "Take Profit %",
                   "As decimal: 0.15 = 15% (default: 0.15)")

        add_section("Claude Advisor (AI Meta-Layer)")

        add_field("ANTHROPIC_API_KEY", "Anthropic API Key",
                   "Get at console.anthropic.com — enables AI advisor during monitoring", show="*")
        add_field("CLAUDE_MONTHLY_CAP", "Monthly Spending Cap ($)",
                   "Hard limit on API costs per month (default: 10.00)")
        add_field("CLAUDE_MODEL", "Claude Model",
                   "claude-haiku-4-5-20251001 (cheap) or claude-sonnet-4-20250514 (smart)")

        # Show current Claude usage
        try:
            from stock_oracle.claude_advisor import SpendingTracker
            usage = SpendingTracker().get_status()
            usage_frame = tk.Frame(content, bg=BG_PANEL, padx=8, pady=6)
            usage_frame.pack(fill="x", padx=12, pady=(4, 0))
            pct = usage['pct_used']
            color = GREEN if pct < 50 else AMBER if pct < 80 else RED
            tk.Label(usage_frame, bg=BG_PANEL, fg=color, font=("Consolas", 9),
                      text=f"This month: ${usage['spent']:.4f} / ${usage['cap']:.2f} "
                           f"({pct:.1f}%) | {usage['calls']} calls").pack(anchor="w")
        except Exception:
            pass

        add_section("Startup & Automation")

        add_field("AUTO_MONITOR", "Auto-Monitor",
                   "1 = auto-start monitoring during market hours, 0 = manual only")

        # Run on Windows startup toggle
        startup_frame = tk.Frame(content, bg=BG_DARK)
        startup_frame.pack(fill="x", padx=12, pady=(8, 0))

        def toggle_startup():
            ok, msg = StockOracleGUI.install_startup_shortcut()
            if ok:
                startup_status.configure(text=f"Enabled: {msg}", fg=GREEN)
            else:
                startup_status.configure(text=f"Failed: {msg}", fg=RED)

        def remove_startup():
            ok, msg = StockOracleGUI.remove_startup_shortcut()
            startup_status.configure(text=msg, fg=FG_DIM)

        tk.Button(startup_frame, text="Run on Windows Startup", bg=ACCENT, fg="white",
                   font=("Segoe UI", 9, "bold"), borderwidth=0, padx=8, pady=3,
                   cursor="hand2", command=toggle_startup).pack(side="left", padx=(0, 8))
        tk.Button(startup_frame, text="Remove", bg=BG_CARD, fg=FG_PRIMARY,
                   font=("Segoe UI", 9), borderwidth=0, padx=8, pady=3,
                   cursor="hand2", command=remove_startup).pack(side="left")

        startup_status = tk.Label(content, text="", bg=BG_DARK, fg=FG_DIM,
                                   font=("Consolas", 8))
        startup_status.pack(anchor="w", padx=12, pady=(2, 0))

        # Check if startup shortcut exists
        try:
            startup_path = os.path.join(
                os.environ.get("APPDATA", ""),
                r"Microsoft\Windows\Start Menu\Programs\Startup",
                "StockOracle.bat"
            )
            if os.path.exists(startup_path):
                startup_status.configure(text="Currently enabled (runs on Windows login)", fg=GREEN)
            else:
                startup_status.configure(text="Not enabled — app only runs when manually started", fg=FG_DIM)
        except Exception:
            pass

        # ── Buttons ───────────────────────────────────

        btn_frame = tk.Frame(content, bg=BG_DARK)
        btn_frame.pack(fill="x", padx=12, pady=(20, 12))

        def save():
            new_settings = {k: v.get().strip() for k, v in entries.items()}
            self._save_settings(new_settings)
            self._apply_settings(new_settings)
            self._log("Settings saved")

            # Force oracle reload so it picks up new keys
            with self._oracle_lock:
                self.oracle = None
            self._log("Oracle will reload with new settings on next analysis")

            win.destroy()

        def show_status():
            """Show which APIs are configured."""
            lines = []
            for key, var in entries.items():
                val = var.get().strip()
                if key in ("RH_PASSWORD", "WEBULL_APP_SECRET", "ALPACA_SECRET",
                           "REDDIT_CLIENT_SECRET", "RH_TOTP_SECRET", "GITHUB_TOKEN"):
                    status = "Set" if val else "Not set"
                else:
                    status = val[:20] + "..." if len(val) > 20 else val if val else "Not set"
                icon = "+" if val else "-"
                lines.append(f"[{icon}] {key}: {status}")
            messagebox.showinfo("API Status", "\n".join(lines))

        ttk.Button(btn_frame, text="Save", style="Green.TButton",
                    command=save).pack(side="right", padx=4)
        ttk.Button(btn_frame, text="Cancel", style="Dark.TButton",
                    command=win.destroy).pack(side="right", padx=4)
        ttk.Button(btn_frame, text="Check Status", style="Dark.TButton",
                    command=show_status).pack(side="left")

    def _load_settings(self) -> Dict:
        """Load settings from .env file."""
        settings = {}
        env_path = Path(__file__).parent / ".env"

        # Load from .env file
        if env_path.exists():
            for line in env_path.read_text().splitlines():
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, _, value = line.partition("=")
                    settings[key.strip()] = value.strip().strip('"').strip("'")

        # Also check os.environ as fallback
        for key in [
            "FINNHUB_API_KEY", "ALPACA_KEY_ID", "ALPACA_SECRET",
            "POLYGON_API_KEY", "WEBULL_APP_KEY", "WEBULL_APP_SECRET",
            "RH_EMAIL", "RH_PASSWORD", "RH_TOTP_SECRET",
            "FRED_API_KEY", "NEWS_API_KEY", "SEC_USER_AGENT",
            "REDDIT_CLIENT_ID", "REDDIT_CLIENT_SECRET",
            "GITHUB_TOKEN",
            "OLLAMA_URL", "OLLAMA_MODEL",
            "PREDICTION_HORIZON_DAYS", "MONITOR_INTERVAL",
            "BACKTEST_STOP_LOSS", "BACKTEST_TAKE_PROFIT",
        ]:
            if key not in settings and key in os.environ:
                settings[key] = os.environ[key]

        # Defaults
        settings.setdefault("OLLAMA_URL", "http://localhost:11434")
        settings.setdefault("OLLAMA_MODEL", "qwen2.5:14b")
        settings.setdefault("SEC_USER_AGENT", "StockOracle your@email.com")
        settings.setdefault("PREDICTION_HORIZON_DAYS", "5")
        settings.setdefault("MONITOR_INTERVAL", "300")
        settings.setdefault("BACKTEST_STOP_LOSS", "0.05")
        settings.setdefault("BACKTEST_TAKE_PROFIT", "0.15")

        return settings

    def _save_settings(self, settings: Dict):
        """Save settings to .env file and apply to environment."""
        env_path = Path(__file__).parent / ".env"
        lines = [
            "# Stock Oracle Settings",
            "# Generated by GUI - do not edit manually while app is running",
            f"# Last saved: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
        ]

        for key, value in sorted(settings.items()):
            if value:
                # Mask passwords in the file comments
                lines.append(f"{key}={value}")

        env_path.write_text("\n".join(lines) + "\n")
        self._log(f"Settings saved to {env_path}")

    def _apply_settings(self, settings: Dict):
        """Apply settings to environment variables and config module."""
        for key, value in settings.items():
            if value:
                os.environ[key] = value

        # Update config module directly
        import stock_oracle.config as cfg

        if settings.get("FINNHUB_API_KEY"):
            cfg.FINNHUB_API_KEY = settings["FINNHUB_API_KEY"]
        if settings.get("ALPACA_KEY_ID"):
            cfg.ALPACA_KEY_ID = settings["ALPACA_KEY_ID"]
        if settings.get("ALPACA_SECRET"):
            cfg.ALPACA_SECRET = settings["ALPACA_SECRET"]
        if settings.get("POLYGON_API_KEY"):
            cfg.POLYGON_API_KEY = settings["POLYGON_API_KEY"]
        if settings.get("FRED_API_KEY"):
            cfg.FRED_API_KEY = settings["FRED_API_KEY"]
        if settings.get("NEWS_API_KEY"):
            cfg.NEWS_API_KEY = settings["NEWS_API_KEY"]
        if settings.get("SEC_USER_AGENT"):
            cfg.SEC_USER_AGENT = settings["SEC_USER_AGENT"]
        if settings.get("REDDIT_CLIENT_ID"):
            cfg.REDDIT_CLIENT_ID = settings["REDDIT_CLIENT_ID"]
        if settings.get("REDDIT_CLIENT_SECRET"):
            cfg.REDDIT_CLIENT_SECRET = settings["REDDIT_CLIENT_SECRET"]
        if settings.get("GITHUB_TOKEN"):
            cfg.GITHUB_TOKEN = settings["GITHUB_TOKEN"]
        if settings.get("OLLAMA_URL"):
            cfg.OLLAMA_BASE_URL = settings["OLLAMA_URL"]
        if settings.get("OLLAMA_MODEL"):
            cfg.OLLAMA_MODEL = settings["OLLAMA_MODEL"]
        # Reset shared Ollama instance so it reconnects with new settings
        try:
            from stock_oracle.collectors.alt_data import _shared_ollama
            import stock_oracle.collectors.alt_data as alt_mod
            alt_mod._shared_ollama = None
        except Exception:
            pass
        if settings.get("PREDICTION_HORIZON_DAYS"):
            try:
                cfg.PREDICTION_HORIZON_DAYS = int(settings["PREDICTION_HORIZON_DAYS"])
            except ValueError:
                pass
        if settings.get("MONITOR_INTERVAL"):
            try:
                self.monitor_interval = int(settings["MONITOR_INTERVAL"])
                self.interval_var.set(settings["MONITOR_INTERVAL"])
            except ValueError:
                pass
        if settings.get("BACKTEST_STOP_LOSS"):
            try:
                cfg.BACKTEST_STOP_LOSS = float(settings["BACKTEST_STOP_LOSS"])
            except ValueError:
                pass
        if settings.get("BACKTEST_TAKE_PROFIT"):
            try:
                cfg.BACKTEST_TAKE_PROFIT = float(settings["BACKTEST_TAKE_PROFIT"])
            except ValueError:
                pass

        # Count configured APIs
        api_count = sum(1 for k in [
            "FINNHUB_API_KEY", "ALPACA_KEY_ID", "POLYGON_API_KEY",
            "WEBULL_APP_KEY", "FRED_API_KEY", "NEWS_API_KEY",
            "REDDIT_CLIENT_ID",
        ] if settings.get(k))
        self._log(f"Applied {api_count} API keys + settings")

    # ── Claude Advisor ──────────────────────────────────────────

    def _get_advisor(self):
        """Get or create Claude advisor from settings."""
        try:
            settings = self._load_settings()
            api_key = settings.get("ANTHROPIC_API_KEY", "")
            if not api_key:
                return None

            from stock_oracle.claude_advisor import ClaudeAdvisor, DEFAULT_MODEL, DEFAULT_MONTHLY_CAP
            model = settings.get("CLAUDE_MODEL", DEFAULT_MODEL) or DEFAULT_MODEL
            try:
                cap = float(settings.get("CLAUDE_MONTHLY_CAP", DEFAULT_MONTHLY_CAP))
            except (ValueError, TypeError):
                cap = DEFAULT_MONTHLY_CAP

            return ClaudeAdvisor(api_key=api_key, model=model, monthly_cap=cap)
        except Exception:
            return None

    def _update_advisor_status(self):
        """Update the advisor status label."""
        try:
            advisor = self._get_advisor()
            if advisor:
                ok, reason = advisor.is_available()
                status = advisor.get_status()
                if ok:
                    self.advisor_status_label.configure(
                        text=f"Ready | ${status['spent']:.3f} / ${status['cap']:.2f} "
                             f"({status['pct_used']:.0f}%) | {status['calls']} calls this month",
                        fg=GREEN)
                else:
                    self.advisor_status_label.configure(text=reason, fg=AMBER)
            else:
                self.advisor_status_label.configure(
                    text="Not configured — add API key in Settings", fg=FG_DIM)
        except Exception:
            pass

    def _show_claude_analysis_popup(self, analysis_text: str):
        """Show Claude's full analysis in a scrollable popup window."""
        popup = tk.Toplevel(self.root)
        popup.title("Claude Advisor — Analysis")
        popup.geometry("700x500")
        popup.configure(bg=BG_DARK)
        popup.transient(self.root)

        # Header
        tk.Label(popup, text="Claude Advisor Analysis",
                 bg=BG_DARK, fg=ACCENT, font=("Segoe UI", 14, "bold")
                 ).pack(padx=12, pady=(12, 4), anchor="w")

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        tk.Label(popup, text=f"Generated: {timestamp}",
                 bg=BG_DARK, fg=FG_DIM, font=("Segoe UI", 9)
                 ).pack(padx=12, anchor="w")

        # Scrollable text area
        text_frame = tk.Frame(popup, bg=BG_DARK)
        text_frame.pack(fill="both", expand=True, padx=12, pady=8)

        text_widget = scrolledtext.ScrolledText(
            text_frame, bg=BG_PANEL, fg=FG_PRIMARY,
            font=("Consolas", 10), wrap="word",
            insertbackground=FG_PRIMARY, borderwidth=0,
            highlightthickness=1, highlightbackground=BORDER,
        )
        text_widget.pack(fill="both", expand=True)
        text_widget.insert("1.0", analysis_text)
        text_widget.configure(state="disabled")

        # Buttons
        btn_frame = tk.Frame(popup, bg=BG_DARK)
        btn_frame.pack(fill="x", padx=12, pady=(0, 12))

        def copy_to_clipboard():
            self.root.clipboard_clear()
            self.root.clipboard_append(analysis_text)

        ttk.Button(btn_frame, text="Copy", style="Dark.TButton",
                   command=copy_to_clipboard).pack(side="left", padx=(0, 8))
        ttk.Button(btn_frame, text="Close", style="Dark.TButton",
                   command=popup.destroy).pack(side="right")

    def _ask_claude(self):
        """Open dialog to ask Claude a question about current predictions."""
        advisor = self._get_advisor()
        if not advisor:
            from tkinter import messagebox
            messagebox.showinfo("Claude Advisor",
                "No Anthropic API key configured.\n\n"
                "Go to Settings and add your API key from console.anthropic.com")
            return

        ok, reason = advisor.is_available()
        if not ok:
            from tkinter import messagebox
            messagebox.showwarning("Claude Advisor", reason)
            return

        # Simple input dialog
        dialog = tk.Toplevel(self.root)
        dialog.title("Ask Claude")
        dialog.geometry("600x500")
        dialog.configure(bg=BG_DARK)
        dialog.transient(self.root)

        tk.Label(dialog, text="Ask Claude about your predictions:",
                  bg=BG_DARK, fg=FG_PRIMARY, font=("Segoe UI", 11, "bold")
                  ).pack(padx=12, pady=(12, 4), anchor="w")

        status = advisor.get_status()
        tk.Label(dialog, text=f"Budget: ${status['remaining']:.2f} remaining | "
                              f"Est. cost: ~$0.005-0.012 per question (Haiku)",
                  bg=BG_DARK, fg=FG_DIM, font=("Consolas", 8)).pack(padx=12, anchor="w")

        question_text = tk.Text(dialog, bg=BG_INPUT, fg=FG_PRIMARY,
                                 font=("Consolas", 10), height=4,
                                 insertbackground=FG_PRIMARY, wrap="word")
        question_text.pack(fill="x", padx=12, pady=8)
        question_text.insert("1.0", "Which tickers look strongest right now and why?")
        question_text.focus()

        response_text = scrolledtext.ScrolledText(
            dialog, bg=BG_DARK, fg=FG_PRIMARY, font=("Consolas", 10),
            wrap="word", borderwidth=0, height=15)
        response_text.pack(fill="both", expand=True, padx=12, pady=(0, 8))

        def send():
            q = question_text.get("1.0", "end").strip()
            if not q:
                return
            response_text.delete("1.0", "end")
            response_text.insert("end", "Thinking...\n", )

            def run():
                try:
                    answer = advisor.ask_question(q, self.results)
                    self.msg_queue.put(("_claude_response", dialog, response_text, answer))
                    self._update_advisor_status()
                except Exception as e:
                    self.msg_queue.put(("_claude_response", dialog, response_text,
                                        f"Error: {e}"))

            threading.Thread(target=run, daemon=True).start()

        tk.Button(dialog, text="  Ask  ", bg=ACCENT, fg="white",
                   font=("Segoe UI", 10, "bold"), borderwidth=0,
                   cursor="hand2", command=send).pack(pady=(0, 12))

    def _claude_session_review(self):
        """Send end-of-session data to Claude for analysis."""
        advisor = self._get_advisor()
        if not advisor:
            from tkinter import messagebox
            messagebox.showinfo("Claude Advisor",
                "No Anthropic API key configured.\nGo to Settings to add it.")
            return

        ok, reason = advisor.is_available()
        if not ok:
            from tkinter import messagebox
            messagebox.showwarning("Claude Advisor", reason)
            return

        # Load intraday verified data
        from pathlib import Path
        verified_file = Path("stock_oracle/data/sessions/intraday_verified.jsonl")
        if not verified_file.exists():
            self._log("No intraday verified data for session review")
            return

        verified = []
        try:
            for line in open(verified_file):
                verified.append(json.loads(line.strip()))
        except Exception:
            self._log("Error reading verified data")
            return

        if len(verified) < 5:
            self._log(f"Only {len(verified)} verified samples — need at least 5 for review")
            return

        self._log(f"Sending {len(verified)} verified samples to Claude for review...")

        session_stats = {}
        if hasattr(self, 'session_tracker') and self.session_tracker:
            session_stats = self.session_tracker.get_session_stats()

        def run():
            try:
                response = advisor.end_of_session(verified, session_stats)
                if response:
                    # Try to parse as JSON for structured display
                    display_text = response
                    if isinstance(response, str):
                        try:
                            clean = response.strip()
                            if clean.startswith("```"):
                                clean = clean.split("\n", 1)[1] if "\n" in clean else clean[3:]
                            if clean.endswith("```"):
                                clean = clean[:-3]
                            clean = clean.strip()
                            if clean.startswith("json"):
                                clean = clean[4:].strip()
                            parsed = json.loads(clean)
                            # Build readable text from JSON
                            parts = []
                            grade = parsed.get("overall_grade", "?")
                            parts.append(f"Overall Grade: {grade}")
                            summary = parsed.get("summary", "")
                            if summary:
                                parts.append(f"\n{summary}")
                            patterns = parsed.get("pattern_notes", [])
                            if patterns:
                                parts.append("\nPattern Notes:")
                                for p in patterns:
                                    parts.append(f"  • {p}")
                            weight_recs = parsed.get("weight_recommendations", {})
                            if weight_recs:
                                parts.append("\nWeight Recommendations:")
                                for k, v in weight_recs.items():
                                    parts.append(f"  {k}: {v}")
                            display_text = "\n".join(parts)
                        except (json.JSONDecodeError, Exception):
                            pass  # Use raw response

                    preview = display_text[:300] + "..." if len(display_text) > 300 else display_text
                    self.msg_queue.put(("log", f"Claude session review: {preview}"))
                    if len(display_text) > 300:
                        self.msg_queue.put(("_claude_analysis_popup", display_text))
                else:
                    self.msg_queue.put(("log", "Claude session review: no response (budget limit?)"))
                self._update_advisor_status()
            except Exception as e:
                self.msg_queue.put(("log", f"Claude review error: {e}"))

        threading.Thread(target=run, daemon=True).start()

    def _claude_hourly_checkin(self):
        """Automatic hourly check-in during monitoring."""
        advisor = self._get_advisor()
        if not advisor:
            return

        ok, _ = advisor.is_available()
        if not ok:
            return

        if not self.results:
            return

        self._log("Claude advisor: hourly check-in...")

        session_stats = {}
        if hasattr(self, 'session_tracker') and self.session_tracker:
            session_stats = self.session_tracker.get_session_stats()

        # Get recent verified data
        recent_verified = []
        try:
            from pathlib import Path
            vf = Path("stock_oracle/data/sessions/intraday_verified.jsonl")
            if vf.exists():
                for line in open(vf):
                    recent_verified.append(json.loads(line.strip()))
                recent_verified = recent_verified[-150:]  # Last 150
        except Exception:
            pass

        def run():
            try:
                result = advisor.hourly_checkin(self.results, session_stats, recent_verified)
                if result:
                    # Support both old "notes" and new "analysis" keys
                    notes = ""
                    if isinstance(result, dict):
                        notes = result.get("analysis", "") or result.get("notes", "")
                    else:
                        notes = str(result)
                    alerts = result.get("alerts", []) if isinstance(result, dict) else []
                    weight_adj = result.get("weight_adjustments", {}) if isinstance(result, dict) else {}

                    if alerts:
                        for alert in alerts[:5]:
                            self.msg_queue.put(("log", f"Claude ALERT: {alert}"))

                    # Auto-apply weight adjustments with guardrails
                    if weight_adj and isinstance(weight_adj, dict):
                        self._apply_weight_adjustments(weight_adj)

                    if notes:
                        # Show a short preview in the log
                        preview = notes[:300] + "..." if len(notes) > 300 else notes
                        self.msg_queue.put(("log", f"Claude analysis: {preview}"))
                        # If full analysis is longer, show in popup
                        if len(notes) > 300:
                            self.msg_queue.put(("_claude_analysis_popup", notes))

                self._update_advisor_status()
            except Exception as e:
                self.msg_queue.put(("log", f"Claude check-in error: {e}"))

        threading.Thread(target=run, daemon=True).start()

    def _apply_weight_adjustments(self, adjustments: Dict):
        """
        Auto-apply Claude's weight suggestions with safety guardrails.

        Rules:
        - Collector must exist in SIGNAL_WEIGHTS
        - New weight must be between 0.01 and 0.15
        - Max change per adjustment: ±0.03
        - Changes are session-only (don't overwrite config file)
        - Everything is logged
        """
        import stock_oracle.config as cfg

        applied = []
        rejected = []

        for collector, new_weight in adjustments.items():
            try:
                new_weight = float(new_weight)
            except (ValueError, TypeError):
                rejected.append(f"{collector}: invalid value '{new_weight}'")
                continue

            # Must be a known collector
            if collector not in cfg.SIGNAL_WEIGHTS:
                rejected.append(f"{collector}: unknown collector")
                continue

            old_weight = cfg.SIGNAL_WEIGHTS[collector]

            # Dead collectors stay dead
            if old_weight == 0:
                rejected.append(f"{collector}: dead collector (weight=0)")
                continue

            # Clamp to valid range
            new_weight = max(0.01, min(0.15, new_weight))

            # Max ±0.03 change per check-in
            delta = new_weight - old_weight
            if abs(delta) > 0.03:
                new_weight = old_weight + (0.03 if delta > 0 else -0.03)
                new_weight = max(0.01, min(0.15, new_weight))

            delta = new_weight - old_weight
            if abs(delta) < 0.001:
                continue  # No meaningful change

            # Apply
            cfg.SIGNAL_WEIGHTS[collector] = round(new_weight, 3)
            applied.append(f"{collector} {old_weight:.3f}→{new_weight:.3f}")

        if applied:
            self.msg_queue.put(("log",
                f"Claude auto-adjusted weights: {', '.join(applied)}"))
        if rejected:
            self.msg_queue.put(("log",
                f"Claude adjustments rejected: {', '.join(rejected[:3])}"))

    # ── Startup Settings Load ──────────────────────────────────

    def _load_startup_settings(self):
        """Load and apply settings on startup."""
        settings = self._load_settings()
        self._apply_settings(settings)

    # ── Logging ────────────────────────────────────────────────

    def _log(self, message: str):
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_text.insert(tk.END, f"[{timestamp}] {message}\n")
        self.log_text.see(tk.END)

    # ── Run ────────────────────────────────────────────────────

    def run(self):
        # Load settings on startup
        self._load_startup_settings()

        # Override close button — minimize to tray instead of quit
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

        # Setup system tray icon (if pystray available)
        self._setup_tray()

        # Auto-start monitoring if market is open or in extended hours
        self.root.after(3000, self._auto_start_monitoring)

        # Periodic check: start/stop monitoring based on market hours
        self._schedule_market_check()

        self.root.mainloop()

        # Cleanup tray icon on exit
        if self._tray_icon:
            try:
                self._tray_icon.stop()
            except Exception:
                pass

    def _on_close(self):
        """Close button minimizes to tray/taskbar instead of quitting."""
        if self._quitting:
            self._real_quit()
            return

        if self._tray_icon:
            # Has tray icon — hide window completely
            self.root.withdraw()
            self._log("Minimized to system tray (right-click tray icon to restore)")
        else:
            # No tray icon — just minimize to taskbar
            self.root.iconify()
            self._log("Minimized to taskbar")

    def _real_quit(self):
        """Actually exit the application."""
        self._quitting = True
        self.monitoring = False

        if self._tray_icon:
            try:
                self._tray_icon.stop()
            except Exception:
                pass

        try:
            self.root.destroy()
        except Exception:
            pass

    def _setup_tray(self):
        """Setup system tray icon using pystray (optional dependency)."""
        try:
            import pystray
            from PIL import Image, ImageDraw

            # Create a simple icon (green circle on dark background)
            img = Image.new('RGB', (64, 64), '#1a1f2e')
            draw = ImageDraw.Draw(img)
            draw.ellipse([16, 16, 48, 48], fill='#22c55e')
            draw.text((22, 22), "SO", fill='white')

            def on_open(icon, item):
                self.root.after(0, self._show_window)

            def on_toggle_monitor(icon, item):
                self.root.after(0, self._toggle_monitor)

            def on_quit(icon, item):
                self.root.after(0, self._real_quit)

            menu = pystray.Menu(
                pystray.MenuItem("Open Stock Oracle", on_open, default=True),
                pystray.MenuItem("Toggle Monitoring", on_toggle_monitor),
                pystray.Menu.SEPARATOR,
                pystray.MenuItem("Quit", on_quit),
            )

            self._tray_icon = pystray.Icon(
                "stock_oracle", img, "Stock Oracle", menu
            )

            # Run tray icon in background thread
            threading.Thread(target=self._tray_icon.run, daemon=True).start()
            self._log("System tray icon active")

        except ImportError:
            self._log("System tray: install pystray and Pillow for tray icon "
                      "(pip install pystray Pillow)")
        except Exception as e:
            self._log(f"System tray setup failed: {e}")

    def _show_window(self):
        """Restore window from tray/taskbar."""
        self.root.deiconify()
        self.root.lift()
        self.root.focus_force()

    def _auto_start_monitoring(self):
        """Auto-start monitoring if market is in a trading session."""
        if self.monitoring:
            return  # Already monitoring

        settings = self._load_settings()
        if settings.get("AUTO_MONITOR", "1") == "0":
            return  # Disabled in settings

        try:
            from stock_oracle.collectors.finnhub_collector import get_market_session
            session = get_market_session()
            sess_type = session.get("session", "closed")

            if sess_type in ("regular", "pre_market", "after_hours"):
                self._log(f"Market is {sess_type} — auto-starting monitoring")
                if not self.monitoring:
                    self._toggle_monitor()
            else:
                self._log(f"Market closed — monitoring will auto-start when market opens")
        except Exception as e:
            self._log(f"Auto-monitor check failed: {e}")

    def _schedule_market_check(self):
        """Check market session every 5 minutes for auto-start/stop."""
        def check():
            if self._quitting:
                return

            try:
                from stock_oracle.collectors.finnhub_collector import get_market_session
                session = get_market_session()
                sess_type = session.get("session", "closed")

                settings = self._load_settings()
                auto_enabled = settings.get("AUTO_MONITOR", "1") != "0"

                if auto_enabled:
                    if sess_type in ("regular", "pre_market", "after_hours"):
                        if not self.monitoring:
                            self._log(f"Market session: {sess_type} — starting monitoring")
                            self._toggle_monitor()
                    elif sess_type == "closed":
                        if self.monitoring:
                            self._log("Market closed — stopping monitoring")
                            self._toggle_monitor()
            except Exception:
                pass

            # Schedule next check in 5 minutes
            if not self._quitting:
                self.root.after(300000, check)

        # First check after 10 minutes (give startup time)
        self.root.after(600000, check)

    # ── Windows Startup ────────────────────────────────────────

    @staticmethod
    def install_startup_shortcut():
        """
        Create a Windows Start Menu shortcut to launch on login.
        Puts a .bat shortcut in the Startup folder.
        """
        try:
            import winreg
            import subprocess

            # Get Windows Startup folder
            startup_folder = os.path.join(
                os.environ.get("APPDATA", ""),
                r"Microsoft\Windows\Start Menu\Programs\Startup"
            )

            if not os.path.exists(startup_folder):
                return False, f"Startup folder not found: {startup_folder}"

            # Find our START.bat
            app_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            start_bat = os.path.join(app_dir, "START.bat")

            if not os.path.exists(start_bat):
                return False, f"START.bat not found at {start_bat}"

            # Create a small launcher .bat in the Startup folder
            shortcut_path = os.path.join(startup_folder, "StockOracle.bat")
            with open(shortcut_path, "w") as f:
                f.write(f'@echo off\ncd /d "{app_dir}"\nstart "" "{start_bat}"\n')

            return True, f"Startup shortcut created at {shortcut_path}"

        except Exception as e:
            return False, str(e)

    @staticmethod
    def remove_startup_shortcut():
        """Remove the Windows startup shortcut."""
        try:
            startup_folder = os.path.join(
                os.environ.get("APPDATA", ""),
                r"Microsoft\Windows\Start Menu\Programs\Startup"
            )
            shortcut_path = os.path.join(startup_folder, "StockOracle.bat")
            if os.path.exists(shortcut_path):
                os.remove(shortcut_path)
                return True, "Startup shortcut removed"
            return True, "No shortcut to remove"
        except Exception as e:
            return False, str(e)


def main():
    # Check if first-run setup is needed
    try:
        from stock_oracle.setup_wizard import needs_setup, SetupWizard
        if needs_setup():
            wizard = SetupWizard()
            completed = wizard.run()
            if not completed:
                return  # User closed wizard without completing
    except Exception as e:
        # If wizard fails, just launch main app
        print(f"Setup wizard error: {e}")

    app = StockOracleGUI()
    app.run()


if __name__ == "__main__":
    main()
