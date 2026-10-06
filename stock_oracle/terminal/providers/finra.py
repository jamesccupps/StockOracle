"""
FINRA daily short-sale volume — no key required
===============================================
FINRA publishes one file per trading day (~6 pm ET) with short-sale volume
for every symbol traded off-exchange (dark pools, wholesalers via the FINRA
trade reporting facilities). The short share of that volume is a widely
watched flow/sentiment gauge. It is *not* short interest, and it only covers
off-exchange volume, which is typically 40-50% of a stock's total.

Parsed days are cached on disk (cache/finra/), so only new days download.
"""
import json
import logging
import threading
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

import stock_oracle.config as cfg

logger = logging.getLogger("stock_oracle.terminal")

URL = "https://cdn.finra.org/equity/regsho/daily/CNMSshvol{d}.txt"
CACHE = Path(cfg.CACHE_DIR) / "finra"
_mem: Dict[str, Optional[Dict[str, Tuple[float, float]]]] = {}
_lock = threading.Lock()
_session = requests.Session()
_session.headers["User-Agent"] = "StockOracle-Terminal"


def _load_day(d: date) -> Optional[Dict[str, Tuple[float, float]]]:
    key = d.strftime("%Y%m%d")
    with _lock:
        if key in _mem:
            return _mem[key]
    path = CACHE / f"{key}.json"
    data = None
    if path.exists():
        try:
            data = {k: tuple(v) for k, v in json.loads(path.read_text()).items()}
        except Exception:
            data = None
    if data is None:
        try:
            resp = _session.get(URL.format(d=key), timeout=20)
        except requests.RequestException as e:
            logger.debug(f"finra {key}: {e}")
            return None
        if resp.status_code != 200 or "|" not in resp.text[:200]:
            # Holiday, weekend or not published yet. Remember misses for
            # past days only (today's file may appear later).
            if d < date.today():
                with _lock:
                    _mem[key] = None
            return None
        data = {}
        for line in resp.text.splitlines()[1:]:
            parts = line.split("|")
            if len(parts) >= 5 and parts[1]:
                try:
                    data[parts[1]] = (round(float(parts[2])), round(float(parts[4])))
                except ValueError:
                    continue
        try:
            CACHE.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(data))
            _prune()
        except Exception:
            pass
    with _lock:
        _mem[key] = data
    return data


def _prune(keep_days: int = 90):
    cutoff = time.time() - keep_days * 86400
    for p in CACHE.glob("*.json"):
        try:
            if p.stat().st_mtime < cutoff:
                p.unlink()
        except Exception:
            pass


def _trading_days(n: int) -> List[date]:
    days, d = [], date.today()
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d)
        d -= timedelta(days=1)
    return days


def short_volume(sym: str, sessions: int = 20) -> Dict:
    variants = [sym, sym.replace("-", "."), sym.replace("-", "/"), sym.replace("-", "")]
    rows = []
    for d in _trading_days(sessions + 8):           # extra days absorb holidays
        day = _load_day(d)
        if not day:
            continue
        hit = next((day[v] for v in variants if v in day), None)
        if hit and hit[1] > 0:
            rows.append({"date": d.isoformat(), "short": hit[0], "total": hit[1],
                         "ratio": round(hit[0] / hit[1] * 100, 1)})
        if len(rows) >= sessions:
            break
    rows.reverse()

    def avg(rs):
        tot = sum(r["total"] for r in rs)
        return round(sum(r["short"] for r in rs) / tot * 100, 1) if tot else None

    return {
        "symbol": sym,
        "rows": rows,
        "avg_5d": avg(rows[-5:]),
        "avg_20d": avg(rows),
        "src": "FINRA RegSHO daily (off-exchange volume only)",
    }
