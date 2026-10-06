"""
Terminal settings
=================
Keys come from the same stock_oracle/.env file the GUI's Settings dialog
writes, so anything configured there works here too. The file is re-read
when it changes, so keys added in the GUI take effect without a restart.

Terminal-only options (all optional, set in .env or the environment):
    TERMINAL_STREAM          auto | finnhub | alpaca | off   (default auto)
    TERMINAL_FINNHUB_RPM     Finnhub REST calls/minute this process may use (default 30;
                             the free tier is 60/min shared with the GUI's collectors)
    TERMINAL_POLL_SECONDS    quote refresh interval when nothing is streaming (default 15)
    TERMINAL_PREPOST         1 = include pre/after-hours bars on intraday charts (default 0)
    TERMINAL_TOKEN           access token required when serving beyond localhost
"""
import json
import os
import threading
from pathlib import Path
from typing import Dict, List, Optional

import stock_oracle.config as cfg

PACKAGE_DIR = Path(cfg.BASE_DIR)
REPO_DIR = PACKAGE_DIR.parent
ENV_FILE = PACKAGE_DIR / ".env"
STATIC_DIR = Path(__file__).parent / "static"

# Shared with the GUI so both apps edit one watchlist
WATCHLIST_FILE = Path(cfg.DATA_DIR) / "gui_watchlist.json"

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8765

_env_lock = threading.Lock()
_env_cache: Dict[str, str] = {}
_env_mtime: float = -1.0


def _read_env_file() -> Dict[str, str]:
    global _env_cache, _env_mtime
    with _env_lock:
        try:
            mtime = ENV_FILE.stat().st_mtime
        except OSError:
            _env_cache, _env_mtime = {}, -1.0
            return _env_cache
        if mtime != _env_mtime:
            values = {}
            try:
                for line in ENV_FILE.read_text(encoding="utf-8", errors="replace").splitlines():
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        key, _, val = line.partition("=")
                        val = val.strip().strip('"').strip("'")
                        if val:
                            values[key.strip()] = val
            except Exception:
                values = {}
            _env_cache, _env_mtime = values, mtime
        return _env_cache


def get(key: str, default: str = "") -> str:
    """Setting lookup: .env file first (matches the GUI), then environment."""
    val = _read_env_file().get(key)
    if val:
        return val
    return os.environ.get(key, default) or default


def get_int(key: str, default: int) -> int:
    try:
        return int(float(get(key, str(default))))
    except (TypeError, ValueError):
        return default


def get_float(key: str, default: float) -> float:
    try:
        return float(get(key, str(default)))
    except (TypeError, ValueError):
        return default


def _placeholder(val: str) -> bool:
    return not val or val.lower().startswith("your_")


def finnhub_key() -> str:
    key = get("FINNHUB_API_KEY")
    return "" if _placeholder(key) else key


def alpaca_keys() -> Optional[tuple]:
    kid, sec = get("ALPACA_KEY_ID"), get("ALPACA_SECRET")
    if _placeholder(kid) or _placeholder(sec):
        return None
    return kid, sec


def anthropic_key() -> str:
    key = get("ANTHROPIC_API_KEY")
    return "" if _placeholder(key) else key


def stream_mode() -> str:
    return get("TERMINAL_STREAM", "auto").lower()


def finnhub_rpm() -> int:
    return max(5, min(60, get_int("TERMINAL_FINNHUB_RPM", 30)))


def poll_seconds() -> int:
    return max(5, get_int("TERMINAL_POLL_SECONDS", 15))


def include_prepost() -> bool:
    return get("TERMINAL_PREPOST", "0").lower() in ("1", "true", "yes")


def key_status() -> Dict[str, bool]:
    return {
        "finnhub": bool(finnhub_key()),
        "alpaca": alpaca_keys() is not None,
        "anthropic": bool(anthropic_key()),
    }


# ── Watchlist (shared with the GUI) ──────────────────────────

_wl_lock = threading.Lock()


def load_watchlist() -> List[str]:
    with _wl_lock:
        if WATCHLIST_FILE.exists():
            try:
                data = json.loads(WATCHLIST_FILE.read_text(encoding="utf-8"))
                if isinstance(data, list) and data:
                    return [str(t).upper() for t in data if str(t).strip()]
            except Exception:
                pass
        return list(cfg.WATCHLIST)


def save_watchlist(tickers: List[str]) -> List[str]:
    seen, clean = set(), []
    for t in tickers:
        t = str(t).upper().strip()
        if t and t not in seen:
            seen.add(t)
            clean.append(t)
    with _wl_lock:
        WATCHLIST_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = WATCHLIST_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(clean), encoding="utf-8")
        tmp.replace(WATCHLIST_FILE)
    return clean
