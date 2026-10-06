// Market-level functions: W, WEI/SECT/GOVT/FX/CMDTY/CRYPTO, ECO, GC, MOST, EVTS, TOP
import { h, kv, table, sec, note, link, spark, tabs } from '../ui.js';
import { px, num, pct, big, signed, dir, agoSeconds, shortDate } from '../format.js';
import { live } from '../livecells.js';
import { newsList } from './security.js';

const VERDICT = { BULLISH: ['BULL', 'up'], BEARISH: ['BEAR', 'down'], NEUTRAL: ['NEUT', 'dim'] };

// ── W: watchlist monitor ─────────────────────────────────────
export const W = {
  async load(c) {
    const [wl, orc] = await Promise.all([c.get('/api/watchlist'), c.get('/api/oracle')]);
    return { tickers: wl.tickers, verdicts: orc.verdicts, scan: orc.scan };
  },
  events: { oracle: 'reload', scan: 'reload', watchlist: 'reload' },
  render(c, d, el) {
    const rows = d.tickers.map((t) => ({ t, v: d.verdicts[t] || {} }));
    const scan = d.scan || {};
    el.append(
      h('div', { class: 'lead' },
        h('span', { class: 'dim' }, `${d.tickers.length} securities`),
        h('button', { class: 'btn', disabled: scan.active, onclick: () => c.open('SCAN'),
          title: 'Run Oracle on every ticker (fast mode, one at a time)' },
        scan.active ? `Scanning ${scan.done}/${scan.total}…` : 'Run Oracle on all'),
        h('span', { class: 'dim' }, 'Add: W ADD TICKER  ·  remove: W DEL TICKER')),
      table({ rows, onRow: (r) => c.load(r.t), columns: [
        { key: 't', label: 'Ticker', cls: () => 'tk' },
        { label: 'Last', align: 'r', fmt: (v, r) => live(c.store, r.t, 'p') },
        { label: 'Chg', align: 'r', fmt: (v, r) => live(c.store, r.t, 'c') },
        { label: '%Chg', align: 'r', key: 't', sort: (r) => c.store.get(r.t)?.cp, fmt: (v, r) => live(c.store, r.t, 'cp') },
        { label: 'Ext', align: 'r', fmt: (v, r) => live(c.store, r.t, 'x') },
        { label: 'Volume', align: 'r', fmt: (v, r) => live(c.store, r.t, 'v') },
        { label: 'Oracle', key: 'v', sort: (r) => r.v.signal, fmt: (v, r) => {
          if (r.v.state) return h('span', { class: 'amber' }, r.v.state);
          const [txt, cls] = VERDICT[r.v.prediction] || ['—', 'faint'];
          return h('span', { class: cls }, txt);
        } },
        { label: 'Signal', align: 'r', key: 'v', sort: (r) => r.v.signal, fmt: (v, r) => (r.v.signal != null ? signed(r.v.signal, 3) : '—'), cls: (v, r) => dir(r.v.signal) },
        { label: 'Conf', align: 'r', key: 'v', sort: (r) => r.v.confidence, fmt: (v, r) => (r.v.confidence != null ? `${num(r.v.confidence * 100, 0)}%` : '—') },
        { label: 'Age', align: 'r', fmt: (v, r) => (r.v.age != null ? agoSeconds(r.v.age) : ''), cls: () => 'dim' },
      ] }),
      note('Oracle columns show the newest result from this terminal or the GUI\'s monitoring session. Click a row to load it.'),
    );
  },
  text: (c, d) => d.tickers.map((t) => {
    const q = c.store.get(t), v = d.verdicts[t] || {};
    return `${t} ${q ? `${px(q.p)} ${pct(q.cp)}` : ''} ${v.prediction || ''}`.trim();
  }).join('; '),
};

// ── Cross-asset groups ───────────────────────────────────────
function marketPanel(code) {
  return {
    load: (c) => c.get(`/api/market/${code}`),
    refresh: 120,
    render(c, d, el) {
      el.append(table({ rows: d.rows, onRow: (r) => c.load(r.symbol), rowTitle: (r) => r.symbol, columns: [
        { key: 'label', label: d.title },
        { key: 'symbol', label: 'Symbol', cls: () => 'dim' },
        { key: 'price', label: 'Last', align: 'r', fmt: (v, r) => (c.store.get(r.symbol) ? live(c.store, r.symbol, 'p') : px(v, r.symbol)) },
        { key: 'change', label: 'Chg', align: 'r', cls: (v, r) => dir(liveOr(c, r, 'c', v)),
          fmt: (v, r) => (r.is_yield ? `${signed(liveOr(c, r, 'c', v) * 100, 0)}bp`
            : (c.store.get(r.symbol) ? live(c.store, r.symbol, 'c') : signed(v))) },
        { key: 'change_pct', label: '%Chg', align: 'r', fmt: (v, r) => (c.store.get(r.symbol) ? live(c.store, r.symbol, 'cp') : pct(v)), cls: (v) => dir(v) },
        { key: 'chg_5d_pct', label: '5D', align: 'r', fmt: (v) => pct(v, 1), cls: (v) => dir(v) },
        { key: 'chg_1m_pct', label: '1M', align: 'r', fmt: (v) => pct(v, 1), cls: (v) => dir(v) },
        { label: '30D', fmt: (v, r) => spark(r.spark) },
      ] }), note('Delayed index/futures data from Yahoo; equities update live. Click a row to load it.'));
    },
    text: (c, d) => d.rows.map((r) => `${r.label} ${px(r.price, r.symbol)} ${pct(r.change_pct)}`).join('; '),
  };
}
const liveOr = (c, r, k, v) => { const q = c.store.get(r.symbol); return q && q[k] != null ? q[k] : v; };

export const WEI = marketPanel('WEI');
export const SECT = marketPanel('SECT');
export const GOVT = marketPanel('GOVT');
export const FX = marketPanel('FX');
export const CMDTY = marketPanel('CMDTY');
export const CRYPTO = marketPanel('CRYPTO');

// ── ECO: macro dashboard ─────────────────────────────────────
function ecoFmt(v, unit) {
  if (v == null) return '—';
  if (unit === '%') return `${num(v, 2)}%`;
  if (unit === 'k') return `${num(v, 0)}k`;
  if (unit === 'count') return num(v, 0);
  return num(v, 2);
}
export const ECO = {
  load: (c) => c.get('/api/eco'),
  render(c, d, el) {
    const groups = [...new Set(d.rows.map((r) => r.group))];
    const rows = [];
    for (const g of groups) {
      rows.push({ group: g });
      rows.push(...d.rows.filter((r) => r.group === g));
    }
    const tbl = table({ sortable: false, rows, columns: [
      { key: 'label', label: 'Indicator', fmt: (v, r) => (r.group && !r.id ? h('b', { class: 'amber' }, r.group) : link(r.url, v)) },
      { key: 'value', label: 'Latest', align: 'r', fmt: (v, r) => (r.id ? h('b', {}, ecoFmt(v, r.unit)) : '') },
      { key: 'prior', label: 'Prior', align: 'r', fmt: (v, r) => (r.id ? ecoFmt(v, r.unit) : '') },
      { key: 'year_ago', label: 'Year ago', align: 'r', fmt: (v, r) => (r.id ? ecoFmt(v, r.unit) : '') },
      { key: 'date', label: 'As of', fmt: (v, r) => (r.id ? shortDate(v) : ''), cls: () => 'dim' },
      { label: 'Trend', fmt: (v, r) => (r.spark ? spark(r.spark, { cls: 'flat' }) : '') },
    ] });
    el.append(tbl, note('Federal Reserve Economic Data (St. Louis Fed). Inflation, production, sales, M2 and home prices are year-over-year changes.'));
  },
  text: (c, d) => d.rows.map((r) => `${r.label} ${ecoFmt(r.value, r.unit)} (${r.date})`).join('; '),
};

// ── GC: yield curve ──────────────────────────────────────────
function curveSvg(points) {
  const NS = 'http://www.w3.org/2000/svg';
  const W = 560, H = 210, padL = 38, padR = 12, padT = 12, padB = 26;
  const svg = document.createElementNS(NS, 'svg');
  svg.setAttribute('viewBox', `0 0 ${W} ${H}`);
  svg.setAttribute('class', 'curve');
  svg.style.width = '100%'; svg.style.maxWidth = '640px';
  const vals = points.flatMap((p) => [p.now, p['1m'], p['1y']]).filter((v) => v != null);
  const lo = Math.floor(Math.min(...vals) * 2) / 2, hi = Math.ceil(Math.max(...vals) * 2) / 2;
  const x = (i) => padL + (i / (points.length - 1)) * (W - padL - padR);
  const y = (v) => padT + (1 - (v - lo) / (hi - lo || 1)) * (H - padT - padB);
  const el = (tag, attrs, text) => {
    const e = document.createElementNS(NS, tag);
    for (const [k, v] of Object.entries(attrs)) e.setAttribute(k, v);
    if (text != null) e.textContent = text;
    svg.append(e); return e;
  };
  for (let v = lo; v <= hi + 1e-9; v += 0.5) {
    el('line', { x1: padL, x2: W - padR, y1: y(v), y2: y(v), stroke: '#1d1d1d' });
    el('text', { x: padL - 6, y: y(v) + 4, fill: '#777', 'font-size': 10, 'text-anchor': 'end' }, `${v.toFixed(1)}`);
  }
  points.forEach((p, i) => el('text', { x: x(i), y: H - 8, fill: '#ffa028', 'font-size': 10, 'text-anchor': 'middle' }, p.tenor));
  const series = [['1y', '#555', '4 3'], ['1m', '#9a6118', '3 2'], ['now', '#ececec', '']];
  for (const [k, color, dash] of series) {
    const pts = points.map((p, i) => (p[k] != null ? `${x(i)},${y(p[k])}` : null)).filter(Boolean);
    el('polyline', { points: pts.join(' '), fill: 'none', stroke: color, 'stroke-width': k === 'now' ? 2 : 1.3, 'stroke-dasharray': dash });
  }
  return svg;
}
export const GC = {
  load: (c) => c.get('/api/curve'),
  render(c, d, el) {
    const sp = d.spreads_bp || {};
    el.append(
      h('div', { class: 'lead' },
        h('span', {}, '2s10s ', h('b', { class: dir(sp['2s10s']) }, `${signed(sp['2s10s'], 0)}bp`)),
        h('span', {}, '3m10y ', h('b', { class: dir(sp['3m10y']) }, `${signed(sp['3m10y'], 0)}bp`)),
        h('span', {}, '5s30s ', h('b', { class: dir(sp['5s30s']) }, `${signed(sp['5s30s'], 0)}bp`)),
        h('span', { class: 'dim' }, `as of ${d.as_of}`)),
      curveSvg(d.points),
      h('p', { class: 'dim' }, h('span', { style: { color: '#ececec' } }, '━ now  '), h('span', { style: { color: '#9a6118' } }, '╌ 1 month ago  '), h('span', { style: { color: '#777' } }, '╌ 1 year ago')),
      table({ sortable: false, rows: d.points, columns: [
        { key: 'tenor', label: 'Tenor', cls: () => 'amber' },
        { key: 'now', label: 'Yield', align: 'r', fmt: (v) => (v != null ? `${num(v)}%` : '—') },
        { key: '1m', label: '1m ago', align: 'r', fmt: (v) => (v != null ? `${num(v)}%` : '—') },
        { label: 'Δ 1m', align: 'r', fmt: (v, r) => (r.now != null && r['1m'] != null ? `${signed((r.now - r['1m']) * 100, 0)}bp` : '—'), cls: (v, r) => dir((r.now ?? 0) - (r['1m'] ?? 0)) },
        { key: '1y', label: '1y ago', align: 'r', fmt: (v) => (v != null ? `${num(v)}%` : '—') },
        { label: 'Δ 1y', align: 'r', fmt: (v, r) => (r.now != null && r['1y'] != null ? `${signed((r.now - r['1y']) * 100, 0)}bp` : '—'), cls: (v, r) => dir((r.now ?? 0) - (r['1y'] ?? 0)) },
      ] }),
      note('Constant-maturity Treasury yields from FRED (end of day). GOVT shows live futures-based yields.'),
    );
  },
  text: (c, d) => `Treasury curve ${d.as_of}: ${d.points.map((p) => `${p.tenor} ${num(p.now)}%`).join(', ')}; 2s10s ${d.spreads_bp['2s10s']}bp`,
};

// ── MOST: movers ─────────────────────────────────────────────
export const MOST = {
  load: (c) => c.get(`/api/most?code=${encodeURIComponent((c.args[0] || 'GAINERS').toUpperCase())}`),
  refresh: 120,
  render(c, d, el) {
    el.append(
      tabs(Object.entries(d.screens), d.code, (v) => c.setArgs([v])),
      table({ rows: d.rows, onRow: (r) => c.load(r.symbol), rowTitle: (r) => r.name, columns: [
        { key: 'symbol', label: 'Ticker', cls: () => 'tk' }, { key: 'name', label: 'Name' },
        { key: 'price', label: 'Last', align: 'r', fmt: (v) => px(v) },
        { key: 'change_pct', label: '%Chg', align: 'r', fmt: (v) => pct(v), cls: (v) => dir(v) },
        { key: 'volume', label: 'Volume', align: 'r', fmt: (v) => big(v) },
        { label: 'Rel vol', align: 'r', key: 'volume', sort: (r) => (r.avg_volume ? r.volume / r.avg_volume : null),
          fmt: (v, r) => (r.avg_volume ? `${num(r.volume / r.avg_volume, 1)}×` : '—') },
        { key: 'market_cap', label: 'Mkt cap', align: 'r', fmt: (v) => big(v) },
        { key: 'short_pct_float', label: 'Short % fl', align: 'r', fmt: (v) => (v != null ? `${num(v * 100, 1)}%` : '—') },
      ] }),
    );
  },
  text: (c, d) => `${d.title}: ${d.rows.slice(0, 10).map((r) => `${r.symbol} ${pct(r.change_pct)}`).join(', ')}`,
};

// ── EVTS: earnings calendar for the watchlist ────────────────
export const EVTS = {
  load: (c) => c.get('/api/evts'),
  render(c, d, el) {
    el.append(
      table({ rows: d.rows, onRow: (r) => c.load(r.symbol), columns: [
        { key: 'date', label: 'Date', fmt: (v) => shortDate(v) },
        { key: 'days', label: 'In', align: 'r', fmt: (v) => (v === 0 ? 'today' : v != null ? `${v}d` : '—'), cls: (v) => (v != null && v <= 7 ? 'amber' : '') },
        { key: 'symbol', label: 'Ticker', cls: () => 'tk' }, { key: 'hour', label: 'When', cls: () => 'dim' },
        { key: 'eps_avg', label: 'EPS est', align: 'r', fmt: (v) => num(v, 3) },
        { key: 'rev_avg', label: 'Revenue est', align: 'r', fmt: (v) => big(v) },
      ], empty: 'No upcoming earnings dates for your watchlist.' }),
      d.no_date.length ? note(`No date published (ETFs or not yet scheduled): ${d.no_date.join(', ')}`) : null,
    );
  },
  text: (c, d) => `Upcoming earnings: ${d.rows.slice(0, 10).map((r) => `${r.symbol} ${r.date}`).join(', ')}`,
};

// ── TOP: market news ─────────────────────────────────────────
export const TOP = {
  load: (c) => c.get('/api/news'),
  refresh: 300,
  render(c, d, el) { el.append(newsList(d.articles)); },
  text: (c, d) => `Market headlines: ${d.articles.slice(0, 8).map((a) => a.headline).join(' | ')}`,
};
