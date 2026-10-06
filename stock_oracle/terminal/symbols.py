"""
Symbol handling
===============
Internally every symbol uses Yahoo's format, since Yahoo covers the most
asset types without a key:

    equities   AAPL, BRK-B          indices    ^GSPC, ^VIX
    FX         EURUSD=X             futures    GC=F, CL=F
    crypto     BTC-USD

Finnhub uses BRK.B for share classes and exchange-prefixed symbols for
crypto (BINANCE:BTCUSDT); conversions live here.
"""
import re
from typing import Optional

_VALID = re.compile(r"^\^?[A-Z0-9][A-Z0-9.\-=]{0,14}$")
_SHARE_CLASS = re.compile(r"^([A-Z]{1,5})[./]([A-Z])$")
_CRYPTO = re.compile(r"^([A-Z0-9]{2,10})-USD$")

# Bloomberg-ish suffixes people type out of habit: "AAPL US", "AAPL US EQUITY"
_SUFFIXES = (" US EQUITY", " EQUITY", " US")


def normalize(raw: str) -> str:
    """Clean user input into the internal (Yahoo) symbol format."""
    s = (raw or "").strip().upper()
    if s.startswith("$"):
        s = s[1:]
    for suf in _SUFFIXES:
        if s.endswith(suf):
            s = s[: -len(suf)]
            break
    s = s.strip()
    m = _SHARE_CLASS.match(s)
    if m:
        s = f"{m.group(1)}-{m.group(2)}"
    return s


def is_valid(sym: str) -> bool:
    return bool(sym) and bool(_VALID.match(sym))


def is_index(sym: str) -> bool:
    return sym.startswith("^")


def is_fx(sym: str) -> bool:
    return sym.endswith("=X")


def is_future(sym: str) -> bool:
    return sym.endswith("=F")


def is_crypto(sym: str) -> bool:
    return bool(_CRYPTO.match(sym))


def is_equity(sym: str) -> bool:
    """Stocks and ETFs — the things Oracle, fundamentals and options apply to."""
    return not (is_index(sym) or is_fx(sym) or is_future(sym) or is_crypto(sym))


def to_finnhub(sym: str) -> Optional[str]:
    """Finnhub REST symbol, or None when the free tier doesn't cover it
    (non-US listings like NVDA.TO and index symbols like DX-Y.NYB)."""
    if not is_equity(sym) or "." in sym:
        return None
    m = re.match(r"^([A-Z]{1,5})-([A-Z])$", sym)
    if m:
        return f"{m.group(1)}.{m.group(2)}"
    return sym


def to_finnhub_stream(sym: str) -> Optional[str]:
    """Finnhub websocket symbol (equities plus Binance crypto pairs)."""
    m = _CRYPTO.match(sym)
    if m:
        return f"BINANCE:{m.group(1)}USDT"
    return to_finnhub(sym)


def from_finnhub_stream(fsym: str) -> str:
    if fsym.startswith("BINANCE:") and fsym.endswith("USDT"):
        return f"{fsym[8:-4]}-USD"
    m = re.match(r"^([A-Z]{1,5})\.([A-Z])$", fsym)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    return fsym


def to_alpaca(sym: str) -> Optional[str]:
    if not is_equity(sym):
        return None
    m = re.match(r"^([A-Z]{1,5})-([A-Z])$", sym)
    return f"{m.group(1)}.{m.group(2)}" if m else sym


def from_alpaca(asym: str) -> str:
    m = re.match(r"^([A-Z]{1,5})\.([A-Z])$", asym)
    return f"{m.group(1)}-{m.group(2)}" if m else asym
