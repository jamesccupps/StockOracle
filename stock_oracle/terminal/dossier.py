"""
Dossiers
========
Pull every source together into one view.

BRIEF <ticker>   one page per security: price, Oracle verdict, valuation,
                 estimate revisions, insider activity, short data, SEC filing
                 flags, next earnings, analyst targets, dividend, headlines
MACRO (inside ASK and BRIEF)  regime, yield curve, inflation, labor, credit

The same dossiers are written out as text and handed to Claude with every
ASK, so answers are grounded in the data rather than the model's memory.
"""
import asyncio
import logging
import time
from typing import Dict

from stock_oracle.terminal import symbols as S

logger = logging.getLogger("stock_oracle.terminal")

PART_TIMEOUT = 20


async def _part(coro):
    try:
        return await asyncio.wait_for(coro, PART_TIMEOUT)
    except Exception as e:
        logger.debug(f"dossier part failed: {e!r}")
        return None


async def security(router, bridge, hub, sym: str) -> Dict:
    eq = S.is_equity(sym)
    parts = {
        "profile": router.profile(sym),
        "news": router.news(sym),
    }
    if eq:
        parts.update({
            "estimates": router.estimates(sym),
            "insiders": router.insiders(sym),
            "short": router.short_interest(sym),
            "filings": router.filings(sym),
            "earnings": router.earnings(sym),
            "analysts": router.analysts(sym),
            "dividends": router.dividends(sym),
        })
    keys = list(parts)
    results = await asyncio.gather(*[_part(parts[k]) for k in keys])
    d = dict(zip(keys, results))
    q = hub.get(sym)
    if q is None:
        q = await _part(router.quote_realtime(sym))
    if q is None:
        fresh = await _part(router.quotes([sym], "regular"))
        q = (fresh or {}).get(sym)
    d["quote"] = q
    d["oracle"] = bridge.best(sym) if eq else None
    d["symbol"] = sym
    d["generated"] = time.time()
    return _shape(d)


def _shape(d: Dict) -> Dict:
    """Reduce each source to the handful of facts that matter."""
    out = {"symbol": d["symbol"], "generated": d["generated"]}
    p = d.get("profile") or {}
    st = p.get("stats") or {}
    out["name"] = p.get("name") or d["symbol"]
    out["sector"] = " / ".join(x for x in (p.get("sector"), p.get("industry")) if x)
    out["summary"] = (p.get("summary") or "")[:600]
    q = d.get("quote") or {}
    out["price"] = {"last": q.get("price"), "change_pct": q.get("change_pct"),
                    "ext_price": q.get("ext_price"), "ext_change_pct": q.get("ext_change_pct"),
                    "hi_52w": st.get("hi_52w"), "lo_52w": st.get("lo_52w"),
                    "ma_50": st.get("ma_50"), "ma_200": st.get("ma_200")}
    out["valuation"] = {k: st.get(k) for k in ("market_cap", "pe_ttm", "pe_fwd", "peg", "ps_ttm",
                                               "pb", "beta", "div_yield_pct")}
    o = d.get("oracle")
    if o:
        top = sorted([s for s in o.get("signals", []) if (s.get("confidence") or 0) > 0.2],
                     key=lambda s: abs(s.get("signal") or 0), reverse=True)[:6]
        out["oracle"] = {"prediction": o.get("prediction"), "signal": o.get("signal"),
                         "confidence": o.get("confidence"), "threshold": o.get("conviction_threshold"),
                         "timestamp": o.get("timestamp"), "source": o.get("source"),
                         "regime": o.get("market_regime"),
                         "top": [{"collector": s["collector"], "signal": s.get("signal")} for s in top]}
    e = d.get("estimates") or {}
    if e:
        out["estimates"] = e.get("summary")
        fy = next((x for x in e.get("earnings", []) if x["period"] == "0y"), None)
        if fy:
            out["estimates"] = {**(out["estimates"] or {}), "fy_eps": fy.get("avg"),
                                "fy_growth": fy.get("growth"), "analysts": fy.get("analysts")}
    ins = d.get("insiders") or {}
    if ins:
        out["insiders"] = ins.get("last_90d")
    sh = d.get("short") or {}
    if sh:
        si = sh.get("interest") or {}
        dv = sh.get("daily_volume") or {}
        out["short"] = {"pct_float": si.get("pct_float"), "days_to_cover": si.get("days_to_cover"),
                        "change_pct": si.get("change_pct"), "as_of": si.get("as_of"),
                        "offexch_short_ratio_5d": dv.get("avg_5d"), "offexch_short_ratio_20d": dv.get("avg_20d")}
    f = d.get("filings") or {}
    if f:
        notable = [r for r in f.get("rows", []) if r["category"] in ("event", "dilution", "ownership", "periodic")][:6]
        out["filings"] = {"last_90d": f.get("last_90d"),
                          "recent": [{"form": r["form"], "filed": r["filed"], "what": r["description"],
                                      "items": [i["label"] for i in r.get("items", []) if i["code"] != "9.01"]}
                                     for r in notable]}
    er = d.get("earnings") or {}
    if er:
        hist = er.get("history") or []
        beats = [h for h in hist if h.get("surprise_pct") is not None][:4]
        out["earnings"] = {"next": (er.get("next") or {}).get("date"),
                           "hour": (er.get("next") or {}).get("hour", ""),
                           "eps_est": (er.get("next") or {}).get("eps_avg"),
                           "last_surprises_pct": [round(h["surprise_pct"], 1) for h in beats]}
    a = d.get("analysts") or {}
    if a:
        tr = (a.get("trend") or [{}])[0]
        out["analysts"] = {"target_mean": (a.get("targets") or {}).get("mean"),
                           "target_low": (a.get("targets") or {}).get("low"),
                           "target_high": (a.get("targets") or {}).get("high"),
                           "rating": (a.get("targets") or {}).get("rec_key"),
                           "buy": (tr.get("strong_buy") or 0) + (tr.get("buy") or 0),
                           "hold": tr.get("hold"), "sell": (tr.get("sell") or 0) + (tr.get("strong_sell") or 0),
                           "recent_actions": [f"{x['date']} {x['firm']}: {x['action']} {x['to']}"
                                              + (f" PT {x['target']:g}" if x.get("target") else "")
                                              for x in (a.get("actions") or [])[:4]]}
    dv = d.get("dividends") or {}
    if dv and (dv.get("yield_pct") or dv.get("payments")):
        out["dividend"] = {"yield_pct": dv.get("yield_pct"), "annual_rate": dv.get("annual_rate"),
                           "ex_date": dv.get("ex_date"), "growth_5y_pct": dv.get("growth_5y_cagr_pct"),
                           "payout_ratio": dv.get("payout_ratio")}
    n = d.get("news") or {}
    out["headlines"] = [{"t": x["timestamp"], "headline": x["headline"], "source": x["source"], "url": x["url"]}
                        for x in (n.get("articles") or [])[:8]]
    return out


async def market(router, bridge) -> Dict:
    reg, curve, eco, vol = await asyncio.gather(
        _part(bridge.regime()), _part(router.curve()), _part(router.macro()),
        _part(router.market_group("WEI")))
    out = {"generated": time.time()}
    if reg:
        out["regime"] = {"regime": reg.get("regime"), "detail": reg.get("detail"),
                         "spy_5d": reg.get("spy_5d"), "breadth_5d": reg.get("breadth_5d")}
    if curve:
        out["curve"] = {"as_of": curve["as_of"], "spreads_bp": curve["spreads_bp"],
                        "yields": {p["tenor"]: p["now"] for p in curve["points"]}}
    if eco:
        pick = {"CPIAUCSL", "PCEPILFE", "UNRATE", "PAYEMS", "ICSA", "DFF", "BAMLH0A0HYM2",
                "A191RL1Q225SBEA", "UMCSENT", "STLFSI4"}
        out["macro"] = [{"label": r["label"], "value": r["value"], "prior": r["prior"],
                         "unit": r["unit"], "date": r["date"]} for r in eco["rows"] if r["id"] in pick]
    if vol:
        vix = next((r for r in vol["rows"] if r["symbol"] == "^VIX"), None)
        spx = next((r for r in vol["rows"] if r["symbol"] == "^GSPC"), None)
        out["equities"] = {"spx": spx and {"last": spx["price"], "chg_pct": spx["change_pct"],
                                            "chg_1m_pct": spx["chg_1m_pct"]},
                           "vix": vix and vix["price"]}
    return out


# ── text for Claude ──────────────────────────────────────────

def _f(v, fmt="{:,.2f}", none="n/a"):
    if v is None:
        return none
    try:
        return fmt.format(v)
    except Exception:
        return str(v)


def security_text(d: Dict) -> str:
    L = [f"=== {d['symbol']} — {d.get('name', '')} ({d.get('sector') or 'n/a'}) ==="]
    p = d.get("price") or {}
    L.append(f"Price {_f(p.get('last'))} ({_f(p.get('change_pct'), '{:+.2f}%')} today)"
             + (f", extended hours {_f(p.get('ext_price'))} ({_f(p.get('ext_change_pct'), '{:+.2f}%')})"
                if p.get("ext_price") else "")
             + f"; 52w range {_f(p.get('lo_52w'))}-{_f(p.get('hi_52w'))}; 50d MA {_f(p.get('ma_50'))}, "
               f"200d MA {_f(p.get('ma_200'))}.")
    v = d.get("valuation") or {}
    L.append(f"Valuation: mkt cap {_f(v.get('market_cap'), '{:,.0f}')}, P/E ttm {_f(v.get('pe_ttm'))}, "
             f"fwd {_f(v.get('pe_fwd'))}, P/S {_f(v.get('ps_ttm'))}, beta {_f(v.get('beta'))}, "
             f"div yield {_f(v.get('div_yield_pct'), '{:.2f}%')}.")
    o = d.get("oracle")
    if o:
        L.append(f"Stock Oracle: {o['prediction']} (signal {_f(o.get('signal'), '{:+.3f}')}, "
                 f"confidence {_f(o.get('confidence'), '{:.0%}')}, threshold ±{_f(o.get('threshold'), '{:.3f}')}, "
                 f"{o.get('source')}, {str(o.get('timestamp', ''))[:16]}). Top signals: "
                 + ", ".join(f"{s['collector']} {_f(s['signal'], '{:+.2f}')}" for s in o.get("top", [])))
    e = d.get("estimates")
    if e:
        L.append(f"Estimates: FY EPS {_f(e.get('fy_eps'))} ({_f(e.get('analysts'), '{:d}')} analysts, growth "
                 f"{_f(e.get('fy_growth'), '{:+.0%}')}); FY EPS estimate moved {_f(e.get('fy_eps_chg_30d_pct'), '{:+.1f}%')} "
                 f"in 30 days, {_f(e.get('fy_eps_chg_90d_pct'), '{:+.1f}%')} in 90; net revisions (30d) "
                 f"{_f(e.get('fy_net_revisions_30d'), '{:+d}')}.")
    a = d.get("analysts")
    if a:
        L.append(f"Analysts: {a.get('buy')} buy / {a.get('hold')} hold / {a.get('sell')} sell, rating "
                 f"'{a.get('rating')}', target mean {_f(a.get('target_mean'))} (range {_f(a.get('target_low'))}-"
                 f"{_f(a.get('target_high'))}). Recent: " + "; ".join(a.get("recent_actions") or []))
    er = d.get("earnings")
    if er:
        L.append(f"Earnings: next {er.get('next') or 'n/a'} {er.get('hour') or ''}, EPS est {_f(er.get('eps_est'))}; "
                 f"last surprises % {er.get('last_surprises_pct')}.")
    ins = d.get("insiders")
    if ins:
        L.append(f"Insiders (90d, open market): {ins.get('buys')} buys ${_f(ins.get('buy_value'), '{:,.0f}')}, "
                 f"{ins.get('sells')} sales ${_f(ins.get('sell_value'), '{:,.0f}')}.")
    sh = d.get("short")
    if sh:
        L.append(f"Short: {_f(sh.get('pct_float'), '{:.1%}')} of float, {_f(sh.get('days_to_cover'))} days to cover, "
                 f"{_f(sh.get('change_pct'), '{:+.1f}%')} vs prior month (as of {sh.get('as_of') or 'n/a'}); "
                 f"off-exchange short volume ratio 5d {_f(sh.get('offexch_short_ratio_5d'), '{:.1f}%')}, "
                 f"20d {_f(sh.get('offexch_short_ratio_20d'), '{:.1f}%')}.")
    f = d.get("filings")
    if f:
        c = f.get("last_90d") or {}
        L.append(f"SEC filings (90d): {c.get('8k', 0)} 8-Ks, {c.get('insider', 0)} insider forms, "
                 f"{c.get('dilution', 0)} offering/registration filings, {c.get('ownership', 0)} 13D/G. Recent: "
                 + "; ".join(f"{r['filed']} {r['form']} {r['what']}" + (f" ({', '.join(r['items'])})" if r["items"] else "")
                             for r in f.get("recent", [])))
    dv = d.get("dividend")
    if dv:
        L.append(f"Dividend: yield {_f(dv.get('yield_pct'), '{:.2f}%')}, rate {_f(dv.get('annual_rate'))}, "
                 f"ex-date {dv.get('ex_date') or 'n/a'}, 5y growth {_f(dv.get('growth_5y_pct'), '{:.1f}%')}/yr, "
                 f"payout {_f(dv.get('payout_ratio'), '{:.0%}')}.")
    if d.get("headlines"):
        L.append("Headlines: " + " | ".join(
            f"{time.strftime('%m-%d', time.localtime(h['t'])) if h.get('t') else ''} {h['headline']} ({h['source']})"
            for h in d["headlines"][:6]))
    return "\n".join(L)


def market_text(m: Dict) -> str:
    L = ["=== Market backdrop ==="]
    r = m.get("regime")
    if r:
        L.append(f"Oracle regime: {r.get('detail')}")
    eq = m.get("equities") or {}
    if eq.get("spx"):
        L.append(f"S&P 500 {_f(eq['spx']['last'])} ({_f(eq['spx']['chg_pct'], '{:+.2f}%')} today, "
                 f"{_f(eq['spx']['chg_1m_pct'], '{:+.1f}%')} 1m); VIX {_f(eq.get('vix'))}.")
    c = m.get("curve")
    if c:
        y = c["yields"]
        L.append(f"Treasuries ({c['as_of']}): 3m {_f(y.get('3M'))}%, 2y {_f(y.get('2Y'))}%, 10y {_f(y.get('10Y'))}%, "
                 f"30y {_f(y.get('30Y'))}%; 2s10s {c['spreads_bp'].get('2s10s')}bp, 3m10y {c['spreads_bp'].get('3m10y')}bp.")
    for row in m.get("macro") or []:
        unit = row["unit"]
        fmt = ("{:.2f}%" if unit == "%" else "{:,.0f}k" if unit == "k"
               else "{:,.0f}" if unit == "count" else "{:,.2f}")
        L.append(f"{row['label']}: {_f(row['value'], fmt)} (prior {_f(row['prior'], fmt)}, {row['date']})")
    return "\n".join(L)
