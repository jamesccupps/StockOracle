"""
FRED macro data (St. Louis Fed) — no key required
=================================================
Uses FRED's public CSV download (fredgraph.csv), which needs no API key and
accepts many series in one request.

ECO   macro dashboard: inflation, labor, growth, policy rate, credit, housing,
      sentiment, financial stress
GC    Treasury yield curve: today vs one month and one year ago
"""
import csv
import io
import zipfile
import logging
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import requests

logger = logging.getLogger("stock_oracle.terminal")

CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"
# Default requests User-Agent on purpose: FRED's edge stalls unfamiliar agents.
_session = requests.Session()

# (series id, label, group, transform, unit)
#   transform: level | yoy (percent change vs 12 months earlier) | diff (change vs prior obs)
MACRO: List[Tuple[str, str, str, str, str]] = [
    ("CPIAUCSL", "CPI inflation", "Inflation", "yoy", "%"),
    ("CPILFESL", "Core CPI", "Inflation", "yoy", "%"),
    ("PCEPILFE", "Core PCE (Fed target)", "Inflation", "yoy", "%"),
    ("T5YIE", "5y breakeven inflation", "Inflation", "level", "%"),
    ("UNRATE", "Unemployment rate", "Labor", "level", "%"),
    ("PAYEMS", "Nonfarm payrolls, monthly change", "Labor", "diff", "k"),
    ("ICSA", "Initial jobless claims", "Labor", "level", "count"),
    ("A191RL1Q225SBEA", "Real GDP growth (annualized)", "Growth", "level", "%"),
    ("INDPRO", "Industrial production", "Growth", "yoy", "%"),
    ("RSAFS", "Retail sales", "Growth", "yoy", "%"),
    ("UMCSENT", "Consumer sentiment (UMich)", "Growth", "level", "idx"),
    ("DFF", "Fed funds effective", "Policy & rates", "level", "%"),
    ("DGS2", "2-year Treasury", "Policy & rates", "level", "%"),
    ("DGS10", "10-year Treasury", "Policy & rates", "level", "%"),
    ("T10Y2Y", "10y minus 2y spread", "Policy & rates", "level", "%"),
    ("T10Y3M", "10y minus 3m spread", "Policy & rates", "level", "%"),
    ("MORTGAGE30US", "30-year mortgage rate", "Policy & rates", "level", "%"),
    ("BAMLH0A0HYM2", "High-yield credit spread", "Credit & stress", "level", "%"),
    ("BAMLC0A0CM", "Investment-grade credit spread", "Credit & stress", "level", "%"),
    ("STLFSI4", "St. Louis Fed financial stress", "Credit & stress", "level", "idx"),
    ("NFCI", "Chicago Fed financial conditions", "Credit & stress", "level", "idx"),
    ("M2SL", "M2 money supply", "Credit & stress", "yoy", "%"),
    ("HOUST", "Housing starts (annualized)", "Housing", "level", "k"),
    ("CSUSHPINSA", "Case-Shiller home prices", "Housing", "yoy", "%"),
]

CURVE: List[Tuple[str, str, float]] = [
    ("DGS1MO", "1M", 1 / 12), ("DGS3MO", "3M", 0.25), ("DGS6MO", "6M", 0.5),
    ("DGS1", "1Y", 1), ("DGS2", "2Y", 2), ("DGS3", "3Y", 3), ("DGS5", "5Y", 5),
    ("DGS7", "7Y", 7), ("DGS10", "10Y", 10), ("DGS20", "20Y", 20), ("DGS30", "30Y", 30),
]


def _fetch(ids: List[str], since: date) -> Dict[str, List[Tuple[str, float]]]:
    """series id -> [(YYYY-MM-DD, value), ...] oldest first, blanks dropped."""
    params = {"id": ",".join(ids), "cosd": ",".join([since.isoformat()] * len(ids))}
    resp = _session.get(CSV_URL, params=params, timeout=20)
    resp.raise_for_status()
    out: Dict[str, List[Tuple[str, float]]] = {}
    if resp.content[:2] == b"PK":
        # Mixed frequencies (daily + monthly...) come back as a zip, one CSV each
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            for name in zf.namelist():
                if name.lower().endswith(".csv"):
                    _parse_csv(zf.read(name).decode("utf-8", "replace"), out)
    else:
        _parse_csv(resp.text, out)
    return out


def _parse_csv(text: str, out: Dict[str, List[Tuple[str, float]]]):
    reader = csv.reader(text.splitlines())
    header = next(reader, [])
    cols = header[1:]
    for c in cols:
        out.setdefault(c, [])
    for row in reader:
        if not row:
            continue
        d = row[0]
        for c, v in zip(cols, row[1:]):
            v = v.strip()
            if v and v != ".":
                try:
                    out[c].append((d, float(v)))
                except ValueError:
                    pass


def _value_on_or_before(obs: List[Tuple[str, float]], target: str) -> Optional[Tuple[str, float]]:
    best = None
    for d, v in obs:
        if d <= target:
            best = (d, v)
        else:
            break
    return best


def _yoy(obs: List[Tuple[str, float]], idx: int) -> Optional[float]:
    d, v = obs[idx]
    dt = datetime.strptime(d, "%Y-%m-%d").date()
    try:
        target = dt.replace(year=dt.year - 1).isoformat()
    except ValueError:  # Feb 29
        target = (dt - timedelta(days=365)).isoformat()
    prev = _value_on_or_before(obs[: idx + 1], target)
    if not prev or prev[0] < (dt - timedelta(days=380)).isoformat() or not prev[1]:
        return None
    return (v / prev[1] - 1) * 100


def _fetch_chunked(ids: List[str], since: date, size: int = 4) -> Dict[str, List[Tuple[str, float]]]:
    """FRED's CSV endpoint slows sharply with many series per request, so ask
    for a few at a time in parallel and keep whatever arrives."""
    from concurrent.futures import ThreadPoolExecutor
    chunks = [ids[i:i + size] for i in range(0, len(ids), size)]
    out: Dict[str, List[Tuple[str, float]]] = {}
    errors = []
    with ThreadPoolExecutor(max_workers=6) as ex:
        for res in ex.map(lambda c: _safe_fetch(c, since, errors), chunks):
            out.update(res)
    if not out and errors:
        raise RuntimeError(f"FRED unavailable: {errors[0]}")
    return out


def _safe_fetch(ids, since, errors):
    try:
        return _fetch(ids, since)
    except Exception as e:
        errors.append(str(e))
        logger.debug(f"FRED {ids}: {e}")
        return {}


def macro() -> Dict:
    since = date.today() - timedelta(days=3 * 365)
    data = _fetch_chunked([s for s, *_ in MACRO], since)
    rows = []
    for sid, label, group, transform, unit in MACRO:
        obs = data.get(sid) or []
        if not obs:
            continue
        series: List[Tuple[str, float]] = []
        for i in range(len(obs)):
            if transform == "yoy":
                val = _yoy(obs, i)
            elif transform == "diff":
                val = obs[i][1] - obs[i - 1][1] if i > 0 else None
            else:
                val = obs[i][1]
            if val is not None:
                series.append((obs[i][0], val))
        if not series:
            continue
        last_d, last_v = series[-1]
        prev_v = series[-2][1] if len(series) >= 2 else None
        yr_ago = _value_on_or_before(series, (datetime.strptime(last_d, "%Y-%m-%d").date()
                                              - timedelta(days=365)).isoformat())
        rows.append({
            "id": sid, "label": label, "group": group, "unit": unit, "transform": transform,
            "date": last_d, "value": round(last_v, 4),
            "prior": round(prev_v, 4) if prev_v is not None else None,
            "year_ago": round(yr_ago[1], 4) if yr_ago else None,
            "spark": [round(v, 4) for _, v in series[-36:]],
            "url": f"https://fred.stlouisfed.org/series/{sid}",
        })
    return {"rows": rows, "src": "FRED"}


def curve() -> Dict:
    since = date.today() - timedelta(days=400)
    data = _fetch_chunked([s for s, *_ in CURVE], since, size=6)
    latest_dates = [obs[-1][0] for obs in data.values() if obs]
    if not latest_dates:
        raise RuntimeError("FRED returned no Treasury data")
    as_of = max(latest_dates)
    as_of_dt = datetime.strptime(as_of, "%Y-%m-%d").date()
    marks = {
        "now": as_of,
        "1m": (as_of_dt - timedelta(days=30)).isoformat(),
        "1y": (as_of_dt - timedelta(days=365)).isoformat(),
    }
    points = []
    for sid, tenor, years in CURVE:
        obs = data.get(sid) or []
        p = {"id": sid, "tenor": tenor, "years": years}
        for k, target in marks.items():
            hit = _value_on_or_before(obs, target)
            p[k] = hit[1] if hit else None
        points.append(p)
    get = {p["tenor"]: p["now"] for p in points}
    spreads = {
        "2s10s": round((get["10Y"] - get["2Y"]) * 100) if get.get("10Y") is not None and get.get("2Y") is not None else None,
        "3m10y": round((get["10Y"] - get["3M"]) * 100) if get.get("10Y") is not None and get.get("3M") is not None else None,
        "5s30s": round((get["30Y"] - get["5Y"]) * 100) if get.get("30Y") is not None and get.get("5Y") is not None else None,
    }
    return {"as_of": as_of, "marks": marks, "points": points, "spreads_bp": spreads, "src": "FRED"}
