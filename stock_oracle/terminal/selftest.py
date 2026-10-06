"""
Terminal self-test
==================
Calls every terminal endpoint in-process with live data and reports what
works on this machine with these keys.

    python -m stock_oracle.terminal.selftest              # AAPL, data endpoints only
    python -m stock_oracle.terminal.selftest NVDA --oracle  # also run a full Oracle analysis
    python -m stock_oracle.terminal.selftest --ask          # also ask Claude one question (costs ~$0.01)
"""
import argparse
import os
import sys
import time


def main():
    ap = argparse.ArgumentParser(prog="python -m stock_oracle.terminal.selftest")
    ap.add_argument("symbol", nargs="?", default="AAPL")
    ap.add_argument("--etf", default="SCHD", help="ETF to test fund holdings with")
    ap.add_argument("--oracle", action="store_true", help="run a full Oracle analysis (10-60s)")
    ap.add_argument("--ask", action="store_true", help="send one ASK question to Claude")
    args = ap.parse_args()

    from stock_oracle.terminal import settings
    os.chdir(settings.REPO_DIR)
    import logging
    logging.disable(logging.INFO)
    from fastapi.testclient import TestClient
    from stock_oracle.terminal.server import create_app

    sym, etf = args.symbol.upper(), args.etf.upper()
    checks = [
        ("Status", "/api/status", None),
        ("Watchlist", "/api/watchlist", lambda d: f"{len(d['tickers'])} tickers"),
        ("Quote", f"/api/quote/{sym}", lambda d: f"{d['price']} via {d['src']}"),
        ("Chart 1D", f"/api/history/{sym}?range=1D", lambda d: f"{len(d['candles'])} bars"),
        ("Chart 1Y", f"/api/history/{sym}?range=1Y", lambda d: f"{len(d['candles'])} bars"),
        ("DES profile", f"/api/des/{sym}", lambda d: d["name"]),
        ("FA statements", f"/api/fa/{sym}", lambda d: f"{len(d['annual']['rows'])} rows"),
        ("EEO estimates", f"/api/eeo/{sym}", lambda d: f"net FY revisions 30d {d['summary']['fy_net_revisions_30d']}"),
        ("ANR analysts", f"/api/anr/{sym}", lambda d: f"target {d['targets']['mean']}"),
        ("EE earnings", f"/api/ee/{sym}", lambda d: f"next {d['next'].get('date')}"),
        ("INS insiders", f"/api/ins/{sym}", lambda d: f"{len(d['rows'])} transactions"),
        ("HDS holders", f"/api/hds/{sym}", lambda d: f"{len(d['institutions'])} institutions"),
        ("CF SEC filings", f"/api/cf/{sym}", lambda d: f"{len(d['rows'])} filings"
         + (" (set SEC_USER_AGENT!)" if d.get("ua_placeholder") else "")),
        ("SI short data", f"/api/si/{sym}", lambda d: f"FINRA days {len((d.get('daily_volume') or {}).get('rows', []))}"),
        ("DVD dividends", f"/api/dvd/{sym}", lambda d: f"yield {d['yield_pct']}"),
        ("HOLD ETF", f"/api/hold/{etf}", lambda d: f"{len(d['top_holdings'])} holdings"),
        ("OMON options", f"/api/omon/{sym}", lambda d: f"{len(d['calls'])} call strikes"),
        ("N news", f"/api/news/{sym}", lambda d: f"{len(d['articles'])} articles via {d['src']}"),
        ("TOP news", "/api/news", lambda d: f"{len(d['articles'])} articles via {d['src']}"),
        ("RV peers", f"/api/peers/{sym}", lambda d: f"{len(d['rows'])} peers via {d['src']}"),
        ("WEI indices", "/api/market/WEI", lambda d: f"{len(d['rows'])} rows"),
        ("ECO macro", "/api/eco", lambda d: f"{len(d['rows'])} series"),
        ("GC curve", "/api/curve", lambda d: f"2s10s {d['spreads_bp']['2s10s']}bp"),
        ("MOST movers", "/api/most?code=GAINERS", lambda d: f"{len(d['rows'])} rows"),
        ("EVTS calendar", "/api/evts", lambda d: f"{len(d['rows'])} upcoming"),
        ("REG regime", "/api/regime", lambda d: d.get("detail", "")),
        ("ACC accuracy", "/api/acc", lambda d: f"{d['five_day'].get('total_verified', 0)} verified"),
        ("BRIEF dossier", f"/api/brief/{sym}", lambda d: f"{len(d)} sections"),
        ("BRK breakouts", f"/api/brk?tickers={sym}", lambda d: f"score {d['rows'][0]['score'] if d['rows'] else '-'}"),
    ]

    ok = fail = 0
    print(f"\n  Stock Oracle Terminal self-test ({sym}, ETF {etf})\n")
    with TestClient(create_app()) as c:
        for name, path, describe in checks:
            t0 = time.time()
            try:
                r = c.get(path)
                dt = time.time() - t0
                if r.status_code == 200:
                    info = describe(r.json()) if describe else ""
                    print(f"  PASS  {name:16} {dt:5.1f}s  {info}")
                    ok += 1
                else:
                    print(f"  FAIL  {name:16} {dt:5.1f}s  HTTP {r.status_code}: {r.json().get('detail', '')[:120]}")
                    fail += 1
            except Exception as e:
                print(f"  FAIL  {name:16} {time.time() - t0:5.1f}s  {e!r}"[:200])
                fail += 1

        if args.oracle:
            t0 = time.time()
            c.post(f"/api/oracle/{sym}/run")
            while time.time() - t0 < 240:
                time.sleep(2)
                d = c.get(f"/api/oracle/{sym}").json()
                if not d.get("state"):
                    break
            d = c.get(f"/api/oracle/{sym}").json()
            r = d.get("result")
            if r and r.get("source") == "terminal":
                print(f"  PASS  {'ORC analysis':16} {time.time() - t0:5.1f}s  {r['prediction']} {r['signal']:+.4f} "
                      f"({len(r['signals'])} collectors)")
                ok += 1
            else:
                print(f"  FAIL  {'ORC analysis':16} {time.time() - t0:5.1f}s  {d.get('error') or 'no result'}")
                fail += 1

        if args.ask:
            t0 = time.time()
            r = c.post("/api/ask", json={"question": f"In two sentences, what stands out in {sym}'s data today?",
                                         "symbol": sym})
            if r.status_code == 200:
                print(f"  PASS  {'ASK Claude':16} {time.time() - t0:5.1f}s  {r.json()['answer'][:100]}...")
                ok += 1
            else:
                print(f"  FAIL  {'ASK Claude':16} {time.time() - t0:5.1f}s  {r.json().get('detail')}")
                fail += 1

    keys = settings.key_status()
    print(f"\n  {ok} passed, {fail} failed.  Keys: " + ", ".join(f"{k} {'yes' if v else 'no'}" for k, v in keys.items()))
    return 1 if fail else 0


if __name__ == "__main__":
    sys.exit(main())
