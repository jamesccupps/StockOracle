"""
Cross-asset market monitors
===========================
Symbol groups behind the market-wide functions (Yahoo symbols, no key needed).
"""
from typing import Dict, List, Tuple

# group code -> (title, [(symbol, label), ...])
GROUPS: Dict[str, Tuple[str, List[Tuple[str, str]]]] = {
    "WEI": ("World Equity Indices", [
        ("^GSPC", "S&P 500"), ("^NDX", "Nasdaq 100"), ("^DJI", "Dow Jones"),
        ("^RUT", "Russell 2000"), ("^VIX", "CBOE VIX"),
        ("^GSPTSE", "S&P/TSX"), ("^BVSP", "Bovespa"),
        ("^STOXX50E", "Euro Stoxx 50"), ("^FTSE", "FTSE 100"), ("^GDAXI", "DAX"),
        ("^FCHI", "CAC 40"), ("^N225", "Nikkei 225"), ("^HSI", "Hang Seng"),
        ("000001.SS", "Shanghai Comp"), ("^KS11", "KOSPI"), ("^AXJO", "ASX 200"),
    ]),
    "SECT": ("US Sectors (SPDR ETFs)", [
        ("XLK", "Technology"), ("XLC", "Communication"), ("XLY", "Cons Discretionary"),
        ("XLF", "Financials"), ("XLV", "Health Care"), ("XLI", "Industrials"),
        ("XLE", "Energy"), ("XLB", "Materials"), ("XLP", "Cons Staples"),
        ("XLU", "Utilities"), ("XLRE", "Real Estate"), ("SPY", "S&P 500 ETF"),
    ]),
    "GOVT": ("US Treasury Yields", [
        ("^IRX", "13-Week Bill"), ("^FVX", "5-Year Note"),
        ("^TNX", "10-Year Note"), ("^TYX", "30-Year Bond"),
        ("TLT", "20+ Yr Treasury ETF"), ("SHY", "1-3 Yr Treasury ETF"),
    ]),
    "FX": ("Currencies", [
        ("DX-Y.NYB", "US Dollar Index"), ("EURUSD=X", "EUR/USD"), ("USDJPY=X", "USD/JPY"),
        ("GBPUSD=X", "GBP/USD"), ("USDCAD=X", "USD/CAD"), ("USDCHF=X", "USD/CHF"),
        ("AUDUSD=X", "AUD/USD"), ("USDCNY=X", "USD/CNY"), ("USDMXN=X", "USD/MXN"),
    ]),
    "CMDTY": ("Commodities (front-month futures)", [
        ("CL=F", "WTI Crude"), ("BZ=F", "Brent Crude"), ("NG=F", "Natural Gas"),
        ("RB=F", "RBOB Gasoline"), ("GC=F", "Gold"), ("SI=F", "Silver"),
        ("HG=F", "Copper"), ("PL=F", "Platinum"), ("ZC=F", "Corn"),
        ("ZW=F", "Wheat"), ("ZS=F", "Soybeans"), ("LE=F", "Live Cattle"),
    ]),
    "CRYPTO": ("Crypto", [
        ("BTC-USD", "Bitcoin"), ("ETH-USD", "Ether"), ("SOL-USD", "Solana"),
        ("XRP-USD", "XRP"), ("DOGE-USD", "Dogecoin"), ("ADA-USD", "Cardano"),
    ]),
}

# Aliases people reach for out of Bloomberg habit
ALIASES = {"WB": "GOVT", "YCRV": "GOVT", "BTMM": "GOVT", "FXC": "FX", "WCRS": "FX",
           "GLCO": "CMDTY", "CMD": "CMDTY", "XBTC": "CRYPTO", "CRYP": "CRYPTO",
           "IMAP": "SECT", "SECTORS": "SECT"}

# The always-on strip under the command line
TAPE: List[Tuple[str, str]] = [
    ("^GSPC", "SPX"), ("^NDX", "NDX"), ("^DJI", "INDU"), ("^RUT", "RTY"),
    ("^VIX", "VIX"), ("^TNX", "UST10Y"), ("DX-Y.NYB", "DXY"), ("CL=F", "WTI"),
    ("GC=F", "GOLD"), ("BTC-USD", "BTC"),
]

# Yield symbols quote in percent, so their changes read in basis points
YIELD_SYMBOLS = {"^IRX", "^FVX", "^TNX", "^TYX"}


def resolve(code: str) -> str:
    code = code.upper()
    return ALIASES.get(code, code)
