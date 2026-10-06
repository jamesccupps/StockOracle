// Stock Oracle functions: ORC, REG, BRK, ACC
import { h, kv, table, sec, note, divBar, shareBar } from '../ui.js';
import { px, num, int, pct, signed, dir, agoSeconds } from '../format.js';
import { live } from '../livecells.js';

const VCLS = { BULLISH: 'up', BEARISH: 'down', NEUTRAL: 'amber' };

function gauge(signal, threshold, range = 0.5) {
  const pos = (v) => `${50 + Math.max(-1, Math.min(1, v / range)) * 50}%`;
  const g = h('div', { class: 'gauge', title: `signal ${signed(signal, 3)}, conviction threshold ±${num(threshold, 3)}` });
  const fill = h('div', { class: `fill ${dir(signal)}`, style: { background: signal >= 0 ? 'var(--up)' : 'var(--down)' } });
  const a = 50, b = parseFloat(pos(signal || 0));
  fill.style.left = `${Math.min(a, b)}%`;
  fill.style.width = `${Math.abs(b - a)}%`;
  g.append(fill, h('div', { class: 'mid' }));
  if (threshold) {
    g.append(h('div', { class: 'th', style: { left: pos(threshold) } }), h('div', { class: 'th', style: { left: pos(-threshold) } }));
  }
  return h('div', {}, g, h('div', { class: 'gauge-scale' }, h('span', {}, `−${range}`), h('span', {}, '0'), h('span', {}, `+${range}`)));
}

function accLine(acc) {
  const f = acc.five_day, i = acc.intraday;
  const parts = [];
  if (f && f.total) parts.push(`5-day: ${num(f.accuracy, 0)}% correct, ${num(f.directional, 0)}% direction (n=${f.total})`);
  if (i && i.total) parts.push(`intraday: ${num(i.accuracy, 0)}% correct, ${num(i.directional_pct, 0)}% direction (n=${i.total})`);
  return parts.length ? parts.join('  ·  ') : 'No verified predictions for this ticker yet.';
}

// ── ORC ──────────────────────────────────────────────────────
export const ORC = {
  load: (c) => c.get(`/api/oracle/${encodeURIComponent(c.sym)}`),
  events: { oracle: (c, msg) => (msg.s === c.sym ? 'reload' : null) },
  render(c, d, el) {
    const r = d.result;
    const busy = d.state;
    const runBtn = h('button', { class: 'btn', disabled: !!busy, onclick: () => c.open(`RUN ${c.sym}`) },
      busy === 'running' ? 'Analyzing… (10-60s)' : busy === 'queued' ? 'Queued…' : r ? 'Run again' : 'Run Oracle now');
    if (d.error) el.append(h('p', { class: 'state err' }, `Last run failed: ${d.error}`));
    if (!r) {
      el.append(h('p', { class: 'state' }, `No Oracle analysis for ${c.sym} yet.`),
        h('p', {}, runBtn), note('Runs all collectors, the ML ensemble and signal intelligence — the same analysis as the desktop app.'));
      return;
    }
    const sigs = r.signals || [];
    const hasDetail = sigs.some((s) => s.details !== undefined);
    el.append(
      h('div', { class: 'lead' },
        h('span', { class: `verdict ${VCLS[r.prediction] || ''}` }, r.prediction || '—'),
        h('span', {}, 'signal ', h('b', { class: dir(r.signal) }, signed(r.signal, 4))),
        h('span', {}, 'confidence ', h('b', {}, `${num((r.confidence || 0) * 100, 0)}%`)),
        h('span', { class: 'dim' }, `${r.source || 'terminal'}, ${agoSeconds(d.age)} ago`),
        runBtn),
      gauge(r.signal || 0, r.conviction_threshold),
      h('div', { class: 'cols', style: { marginTop: '8px' } },
        sec('Context', kv([
          ['Price at analysis', px(r.price)], ['Now', live(c.store, c.sym, 'p')],
          ['Conviction threshold', `±${num(r.conviction_threshold, 3)}`], ['Volatility', r.volatility != null ? `${num(r.volatility * 100, 2)}%` : '—'],
          ['Dynamic signals', int(r.dynamic_signals)], ['Stale signals', int(r.stale_signals)],
          ['Market session', (r.market_session || '—').replace('_', ' ')], ['Regime', `${r.market_regime || '—'}${r.regime_bias ? ` (${signed(r.regime_bias, 3)})` : ''}`],
        ])),
        sec('Model', kv([
          ['Method', r.method || '—'],
          ['Weighted analysis', r.weighted ? `${r.weighted.prediction} ${signed(r.weighted.signal, 3)}` : '—'],
          ['Core conviction', r.weighted ? num(r.weighted.core_analysis_score, 2) : num(r.core_conviction, 2)],
          ['ML ensemble', r.ml ? `${r.ml.prediction || '—'} ${r.ml.confidence != null ? `${num(r.ml.confidence * 100, 0)}%` : ''}` : '—'],
        ], 1)),
      ),
      sec('Track record', h('p', { class: 'dim' }, accLine(d.accuracy))),
      r.narrative ? sec('Narrative', h('p', { class: 'summary', style: { whiteSpace: 'pre-wrap' } }, r.narrative)) : null,
      sec(`Collector signals (${sigs.length})`, table({ rows: sigs, columns: [
        { key: 'collector', label: 'Collector', fmt: (v, s) => h('span', { class: s.stale ? 'faint' : '' }, v.replace(/_/g, ' ')) },
        { label: '', fmt: (v, s) => divBar(s.signal, 0.5) },
        { key: 'signal', label: 'Signal', align: 'r', fmt: (v) => signed(v, 3), cls: (v) => dir(v) },
        { key: 'confidence', label: 'Conf', align: 'r', fmt: (v) => `${num((v || 0) * 100, 0)}%` },
        ...(hasDetail ? [
          { key: 'weight', label: 'Weight', align: 'r', fmt: (v) => num(v, 2) },
          { key: 'stale', label: 'Stale', fmt: (v) => (v ? 'stale' : ''), cls: () => 'faint' },
          { key: 'details', label: 'Details', cls: () => 'dim' },
        ] : []),
      ] })),
      !hasDetail ? note('This result came from the desktop app\'s session file, which stores signal values only. Run again for full details.') : null,
    );
  },
  text: (c, d) => (d.result ? `Oracle ${d.result.prediction} ${signed(d.result.signal, 3)} conf ${num(d.result.confidence * 100, 0)}%` : 'Oracle: no analysis'),
};

// ── REG ──────────────────────────────────────────────────────
export const REG = {
  load: (c) => c.get('/api/regime'),
  refresh: 600,
  render(c, d, el) {
    const cls = { SELLOFF: 'down', DECLINING: 'down', RALLY: 'up', RISING: 'up' }[d.regime] || 'amber';
    el.append(
      h('div', { class: 'lead' }, h('span', { class: `verdict ${cls}` }, d.regime),
        h('span', {}, 'confidence ', h('b', {}, `${num((d.confidence || 0) * 100, 0)}%`)),
        h('span', {}, 'bias applied to every prediction ', h('b', { class: dir(d.bias) }, signed(d.bias, 4)))),
      sec('S&P 500 (SPY)', kv([
        ['Price', px(d.spy_price)], ['Score', signed(d.score, 0)],
        ['1 day', pct((d.spy_1d || 0) * 100), dir(d.spy_1d)], ['3 days', pct((d.spy_3d || 0) * 100), dir(d.spy_3d)],
        ['5 days', pct((d.spy_5d || 0) * 100), dir(d.spy_5d)], ['10 days', pct((d.spy_10d || 0) * 100), dir(d.spy_10d)],
        ['Above MAs (5/10/20)', `${d.ma_score ?? '—'} of 3`], ['Daily volatility', d.volatility != null ? `${num(d.volatility * 100, 2)}%` : '—'],
        ['Sectors up today', d.breadth_1d != null ? `${num(d.breadth_1d * 100, 0)}%` : '—'], ['Sectors up 5 days', d.breadth_5d != null ? `${num(d.breadth_5d * 100, 0)}%` : '—'],
      ])),
      note('SELLOFF/DECLINING shift every Oracle prediction bearish, RALLY/RISING bullish; RANGING and VOLATILE apply no bias. Recomputed every 10 minutes.'),
    );
  },
  text: (c, d) => `Market regime ${d.detail}`,
};

// ── BRK ──────────────────────────────────────────────────────
export const BRK = {
  load: (c) => c.get('/api/brk'),
  render(c, d, el) {
    const gcls = { STRONG: 'up', BUILDING: 'amber', EARLY: '', NONE: 'faint' };
    el.append(
      table({ rows: d.rows, onRow: (r) => c.load(r.ticker), rowTitle: (r) => r.company?.short_desc || r.ticker, columns: [
        { key: 'ticker', label: 'Ticker', cls: () => 'tk' },
        { key: 'score', label: 'Score', align: 'r' },
        { label: '', fmt: (v, r) => shareBar(r.score / 100) },
        { key: 'grade', label: 'Setup', cls: (v) => gcls[v] || '' },
        { key: 'price', label: 'Price', align: 'r', fmt: (v) => px(v) },
        { label: 'Today', align: 'r', fmt: (v, r) => live(c.store, r.ticker, 'cp') },
        { key: 'timeframe', label: 'Timeframe', cls: () => 'dim' },
        { key: 'details', label: 'Patterns', cls: () => 'dim' },
      ] }),
      note('Oracle breakout scanner: Bollinger squeeze, volume accumulation, 52-week-high proximity, RSI, MACD, MA alignment, range compression, relative strength. Cached 15 minutes.'),
    );
  },
  text: (c, d) => `Breakouts: ${d.rows.slice(0, 8).map((r) => `${r.ticker} ${r.score} ${r.grade}`).join(', ')}`,
};

// ── ACC ──────────────────────────────────────────────────────
export const ACC = {
  load: (c) => c.get('/api/acc'),
  events: { oracle: (c, m) => (m.state === 'done' ? 'reload' : null) },
  render(c, d, el) {
    const f = d.five_day || {}, i = d.intraday || {};
    const tickers = [...new Set([...Object.keys(d.by_ticker_5d || {}), ...Object.keys(d.by_ticker_intraday || {})])].sort();
    const rows = tickers.map((t) => ({ t, f: (d.by_ticker_5d || {})[t] || {}, i: (d.by_ticker_intraday || {})[t] || {} }));
    const types = Object.entries(f.prediction_type_stats || {}).map(([k, v]) => ({ k, ...v,
      ip: (i.by_prediction || {})[k] || {} }));
    el.append(
      h('div', { class: 'cols' },
        sec('5-day predictions', kv([
          ['Verified', int(f.total_verified)], ['Pending', int(f.pending_verification)],
          ['Accuracy', f.total_verified ? `${num(f.accuracy_pct, 1)}%` : '—'], ['Direction right', f.total_verified ? `${num(f.directional_accuracy, 1)}%` : '—'],
        ])),
        sec('Intraday (monitoring)', kv([
          ['Verified', int(i.verified)], ['', ''],
          ['Accuracy', i.verified ? `${num(i.accuracy, 1)}%` : '—'], ['Direction right', i.verified ? `${num(i.directional, 1)}%` : '—'],
        ])),
      ),
      sec('By call', table({ sortable: false, rows: types, columns: [
        { key: 'k', label: 'Call', cls: (v) => VCLS[v] || '' },
        { label: '5-day', align: 'r', fmt: (v, r) => (r.total ? `${num(r.correct / r.total * 100, 0)}% (${r.total})` : '—') },
        { label: 'Intraday', align: 'r', fmt: (v, r) => (r.ip.total ? `${num(r.ip.correct / r.ip.total * 100, 0)}% (${r.ip.total})` : '—') },
      ] })),
      sec('By ticker', table({ rows, onRow: (r) => c.load(r.t), empty: 'Nothing verified yet — 5-day predictions verify after five days; intraday ones during GUI monitoring.', columns: [
        { key: 't', label: 'Ticker', cls: () => 'tk' },
        { label: '5d acc', align: 'r', key: 't', sort: (r) => r.f.accuracy, fmt: (v, r) => (r.f.total ? `${num(r.f.accuracy, 0)}%` : '—') },
        { label: '5d dir', align: 'r', key: 't', sort: (r) => r.f.directional, fmt: (v, r) => (r.f.total ? `${num(r.f.directional, 0)}%` : '—') },
        { label: 'n', align: 'r', fmt: (v, r) => r.f.total || '' , cls: () => 'dim' },
        { label: 'Intraday acc', align: 'r', key: 't', sort: (r) => r.i.accuracy, fmt: (v, r) => (r.i.total ? `${num(r.i.accuracy, 0)}%` : '—') },
        { label: 'Intraday dir', align: 'r', key: 't', sort: (r) => r.i.directional_pct, fmt: (v, r) => (r.i.total ? `${num(r.i.directional_pct, 0)}%` : '—') },
        { label: 'n', align: 'r', fmt: (v, r) => r.i.total || '', cls: () => 'dim' },
      ] })),
      (f.recent_predictions || []).length ? sec('Recent verified (5-day)', table({ rows: [...f.recent_predictions].reverse(), columns: [
        { key: 'date', label: 'Date' }, { key: 'ticker', label: 'Ticker', cls: () => 'tk' },
        { key: 'prediction', label: 'Call', cls: (v) => VCLS[v] || '' },
        { key: 'price_at', label: 'Price then', align: 'r', fmt: (v) => px(v) }, { key: 'actual_price', label: 'After', align: 'r', fmt: (v) => px(v) },
        { key: 'pct_change', label: 'Move', align: 'r', fmt: (v) => pct((v || 0) * 100), cls: (v) => dir(v) },
        { key: 'directional', label: 'Right?', fmt: (v) => (v ? 'yes' : 'no'), cls: (v) => (v ? 'up' : 'down') },
      ] })) : null,
    );
  },
  text: (c, d) => `Oracle accuracy: 5-day ${num(d.five_day?.accuracy_pct, 1)}% (n=${d.five_day?.total_verified}); intraday ${num(d.intraday?.accuracy, 1)}% (n=${d.intraday?.verified})`,
};
