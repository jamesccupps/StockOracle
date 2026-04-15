"""
Narrative Generator
====================
Builds plain-English analysis summaries from signal data.
No Ollama required — pure pattern matching on signal values.

Used in:
- Deep Dive Analysis tab (top section)
- Detail panel (single-click on card)
- Export reports
"""
from typing import Dict, List, Tuple


def generate_narrative(result: Dict) -> str:
    """
    Generate a plain-English narrative explaining why the prediction
    is what it is, summarizing the key findings across all collectors.
    """
    ticker = result.get("ticker", "?")
    pred = result.get("prediction", "NEUTRAL")
    sig = result.get("signal", 0)
    conf = result.get("confidence", 0)
    wp = result.get("weighted_prediction", {})
    core = wp.get("core_analysis_score", 0)
    method = result.get("method", "")
    signals = result.get("signals", [])

    # Build signal lookup
    def _s(name):
        return next((s for s in signals if s.get("collector") == name), {})

    def _val(name):
        return _s(name).get("signal", 0)

    def _conf(name):
        return _s(name).get("confidence", 0)

    def _raw(name):
        r = _s(name).get("raw_data")
        return r if isinstance(r, dict) else {}

    # ── Extract key data points ──
    tech = _s("technical_analysis")
    tech_raw = _raw("technical_analysis")
    indicators = tech_raw.get("indicators", {})
    rsi = indicators.get("rsi")
    macd_h = indicators.get("macd_histogram", 0)
    bb_pos = indicators.get("bollinger_position", 0.5)

    fund_raw = _raw("fundamental_analysis")
    metrics = fund_raw.get("metrics", {})
    pe = metrics.get("pe")
    margin = metrics.get("profit_margin")
    rev_growth = metrics.get("revenue_growth")

    analyst_raw = _raw("analyst_consensus")
    target_upside = analyst_raw.get("target_upside_pct")

    momentum_val = _val("momentum_quality")
    momentum_raw = _raw("momentum_quality")
    quality = momentum_raw.get("quality", "")

    fg_raw = _raw("fear_greed_proxy")
    vix = fg_raw.get("vix")
    mood = fg_raw.get("mood", "")

    mp_raw = _raw("market_pulse")
    spy_chg = mp_raw.get("spy_change")
    pulse = mp_raw.get("pulse", "")

    short_raw = _raw("short_interest")
    short_pct = short_raw.get("short_pct_float")

    price_info = result.get("price_data", [])

    # ── Build narrative sections ──
    parts = []

    # 1. Opening verdict
    if pred == "BULLISH":
        strength = "strongly" if sig > 0.12 else "moderately" if sig > 0.08 else "slightly"
        parts.append(
            f"{ticker} is showing {strength} bullish signals with {core:.0%} "
            f"conviction from core analysis."
        )
    elif pred == "BEARISH":
        strength = "strongly" if sig < -0.12 else "moderately" if sig < -0.08 else "slightly"
        parts.append(
            f"{ticker} is showing {strength} bearish signals with {core:.0%} "
            f"conviction from core analysis."
        )
    else:
        if sig > 0.03:
            parts.append(
                f"{ticker} is leaning slightly bullish but not enough to cross "
                f"the conviction threshold. Core analysis score is {core:.0%}."
            )
        elif sig < -0.03:
            parts.append(
                f"{ticker} is leaning slightly bearish but signals are mixed. "
                f"Core analysis score is {core:.0%}."
            )
        else:
            parts.append(
                f"{ticker} shows no clear directional signal. "
                f"Bullish and bearish indicators are roughly balanced."
            )

    # 2. Technical picture
    tech_parts = []
    if rsi is not None:
        if rsi < 30:
            tech_parts.append(f"RSI at {rsi:.0f} indicates oversold conditions, suggesting a potential bounce")
        elif rsi < 40:
            tech_parts.append(f"RSI at {rsi:.0f} is approaching oversold territory")
        elif rsi > 70:
            tech_parts.append(f"RSI at {rsi:.0f} indicates overbought conditions, suggesting caution")
        elif rsi > 60:
            tech_parts.append(f"RSI at {rsi:.0f} is running warm but not yet overbought")
        else:
            tech_parts.append(f"RSI at {rsi:.0f} is in neutral range")

    if macd_h > 0.5:
        tech_parts.append("MACD is bullish with strong momentum")
    elif macd_h > 0:
        tech_parts.append("MACD is mildly bullish")
    elif macd_h < -0.5:
        tech_parts.append("MACD is bearish with negative momentum")
    elif macd_h < 0:
        tech_parts.append("MACD is mildly bearish")

    if bb_pos < 0.1:
        tech_parts.append("price is near the lower Bollinger Band (potentially oversold)")
    elif bb_pos > 0.9:
        tech_parts.append("price is near the upper Bollinger Band (potentially overbought)")

    if tech_parts:
        parts.append("Technical indicators: " + "; ".join(tech_parts) + ".")

    # 3. Momentum quality
    if momentum_val > 0.1:
        parts.append(
            f"Momentum is positive — the trend has been consistent with "
            f"{momentum_raw.get('up_day_pct', 50):.0f}% up days over the last 20 sessions."
        )
    elif momentum_val < -0.1:
        dd = momentum_raw.get("drawdown_pct", 0)
        parts.append(
            f"Momentum is fading — only {momentum_raw.get('up_day_pct', 50):.0f}% up days recently"
            f"{f' with a {dd:.1f}% drawdown from the 20-day high' if dd < -3 else ''}."
        )

    # 4. Fundamentals
    fund_parts = []
    if pe is not None and pe > 0:
        if pe > 50:
            fund_parts.append(f"P/E of {pe:.0f} is elevated")
        elif pe > 25:
            fund_parts.append(f"P/E of {pe:.0f} is above average")
        elif pe > 15:
            fund_parts.append(f"P/E of {pe:.0f} is reasonable")
        else:
            fund_parts.append(f"P/E of {pe:.0f} looks attractive")

    if rev_growth is not None:
        if rev_growth > 20:
            fund_parts.append(f"strong revenue growth of {rev_growth:+.0f}%")
        elif rev_growth > 0:
            fund_parts.append(f"revenue growing at {rev_growth:+.0f}%")
        elif rev_growth < -5:
            fund_parts.append(f"revenue declining at {rev_growth:+.0f}%")

    if margin is not None:
        if margin > 20:
            fund_parts.append(f"healthy {margin:.0f}% profit margins")
        elif margin < 0:
            fund_parts.append(f"company is unprofitable ({margin:.0f}% margins)")

    if fund_parts:
        parts.append("Fundamentals: " + ", ".join(fund_parts) + ".")

    # 5. Analyst consensus
    if target_upside is not None:
        if target_upside > 30:
            parts.append(f"Wall Street analysts see significant upside — average target is {target_upside:+.0f}% above current price.")
        elif target_upside > 10:
            parts.append(f"Analyst targets suggest {target_upside:+.0f}% upside from here.")
        elif target_upside < -5:
            parts.append(f"Analysts have targets below current price ({target_upside:+.0f}%), suggesting overvaluation.")
        elif target_upside < 5:
            parts.append(f"Analyst targets are close to current price ({target_upside:+.0f}%), limited upside expected.")

    # 6. Short interest
    if short_pct is not None and short_pct > 5:
        parts.append(
            f"Short interest at {short_pct:.1f}% of float is elevated — "
            f"{'potential short squeeze setup' if short_pct > 15 else 'bears are active'}."
        )

    # 7. Market environment
    if spy_chg is not None:
        if spy_chg > 0.5:
            parts.append(f"The broad market is rallying today (S&P 500 {spy_chg:+.2f}%). {pulse}.")
        elif spy_chg < -0.5:
            parts.append(f"The broad market is selling off today (S&P 500 {spy_chg:+.2f}%). {pulse}.")
        elif spy_chg < -0.1:
            parts.append(f"The market is drifting lower today (S&P 500 {spy_chg:+.2f}%).")
    elif vix is not None:
        if vix > 30:
            parts.append(f"Market environment is fearful with VIX at {vix:.0f}. {mood}.")
        elif vix > 25:
            parts.append(f"Market is showing elevated uncertainty (VIX {vix:.0f}). {mood}.")
        elif vix < 15:
            parts.append(f"Market is calm with VIX at {vix:.0f}. {mood}.")

    # 8. Notable signals (strongest bullish and bearish)
    active = [s for s in signals if s.get("confidence", 0) > 0.2]
    strong_bull = sorted([s for s in active if s.get("signal", 0) > 0.15],
                         key=lambda x: x["signal"] * x["confidence"], reverse=True)
    strong_bear = sorted([s for s in active if s.get("signal", 0) < -0.15],
                         key=lambda x: x["signal"] * x["confidence"])

    if strong_bull[:3]:
        names = [_format_collector_name(s["collector"]) for s in strong_bull[:3]]
        parts.append(f"Strongest bullish signals from: {', '.join(names)}.")

    if strong_bear[:3]:
        names = [_format_collector_name(s["collector"]) for s in strong_bear[:3]]
        parts.append(f"Bearish headwinds from: {', '.join(names)}.")

    # 9. Key risk/opportunity
    if pred == "BULLISH" and momentum_val < -0.1:
        parts.append(
            "Key risk: momentum is fading despite bullish fundamentals. "
            "Watch for confirmation before acting."
        )
    elif pred == "NEUTRAL" and target_upside and target_upside > 20:
        parts.append(
            "Worth watching: analysts see significant upside but current "
            "momentum doesn't support it yet."
        )
    elif pred == "BEARISH" and rsi and rsi < 35:
        parts.append(
            "Note: despite bearish signals, RSI is approaching oversold — "
            "a bounce could be near."
        )

    # Active signal count
    active_count = len([s for s in signals if s.get("confidence", 0) > 0.1])
    total_count = len(signals)
    parts.append(f"\nBased on {active_count} active signals out of {total_count} collectors.")

    return "\n\n".join(parts)


def generate_market_summary(results: Dict) -> str:
    """
    Generate a market-wide narrative from all ticker results.
    Used for the market overview section.
    """
    if not results:
        return "No analysis data available."

    bullish = sum(1 for r in results.values() if r.get("prediction") == "BULLISH")
    bearish = sum(1 for r in results.values() if r.get("prediction") == "BEARISH")
    neutral = sum(1 for r in results.values() if r.get("prediction") == "NEUTRAL")
    total = len(results)

    avg_sig = sum(r.get("signal", 0) for r in results.values()) / total

    # Get market-level indicators from any result
    any_result = next(iter(results.values()))
    fg_data = None
    for s in any_result.get("signals", []):
        if s.get("collector") == "fear_greed_proxy":
            fg_data = s.get("raw_data") if isinstance(s.get("raw_data"), dict) else {}
            break

    parts = []

    # Overall market read
    if avg_sig > 0.05:
        parts.append(f"Market outlook is cautiously bullish. {bullish}/{total} tickers showing bullish signals.")
    elif avg_sig < -0.05:
        parts.append(f"Market outlook is cautiously bearish. {bearish}/{total} tickers showing bearish signals.")
    else:
        parts.append(f"Market is mixed. {bullish} bullish, {bearish} bearish, {neutral} neutral out of {total} tickers.")

    # VIX/Fear gauge
    if fg_data:
        vix = fg_data.get("vix")
        mood = fg_data.get("mood", "")
        if vix:
            parts.append(f"VIX at {vix:.1f} — {mood}.")

    # Top movers
    sorted_results = sorted(results.items(), key=lambda x: x[1].get("signal", 0), reverse=True)
    top_bull = [(t, r) for t, r in sorted_results[:3] if r.get("signal", 0) > 0.05]
    top_bear = [(t, r) for t, r in sorted_results[-3:] if r.get("signal", 0) < -0.05]

    if top_bull:
        names = [f"{t} ({r['signal']:+.3f})" for t, r in top_bull]
        parts.append(f"Strongest bullish: {', '.join(names)}.")
    if top_bear:
        names = [f"{t} ({r['signal']:+.3f})" for t, r in top_bear]
        parts.append(f"Most bearish: {', '.join(names)}.")

    return " ".join(parts)


def _format_collector_name(name: str) -> str:
    """Convert collector_name to human-readable."""
    replacements = {
        "technical_analysis": "technicals",
        "fundamental_analysis": "fundamentals",
        "analyst_consensus": "analyst targets",
        "momentum_quality": "momentum",
        "fear_greed_proxy": "market fear/greed",
        "dividend_vs_treasury": "yield gap",
        "insider_ratio": "insider activity",
        "options_flow": "options flow",
        "short_interest": "short interest",
        "finnhub_realtime": "real-time price",
        "yahoo_finance": "price data",
        "cross_stock": "sector correlation",
        "earnings_contagion": "earnings contagion",
        "news_sentiment": "news sentiment",
        "reddit_sentiment": "Reddit sentiment",
        "hackernews_sentiment": "HN sentiment",
        "employee_sentiment": "employee sentiment",
        "earnings_nlp": "earnings call NLP",
        "sec_edgar": "SEC filings",
        "supply_chain": "supply chain",
        "viral_catalyst": "viral catalyst",
        "energy_cascade": "energy prices",
        "talent_flow": "talent flow",
        "wikipedia_velocity": "Wikipedia interest",
        "market_pulse": "broad market direction",
    }
    return replacements.get(name, name.replace("_", " "))
