// Security-level functions: DES FA EEO ANR EE INS HDS CF SI DVD HOLD OMON HP RV N
import { h, kv, table, sec, note, link, spark, shareBar, tabs, chg } from '../ui.js';
import { px, num, int, pct, ratioPct, signedRatioPct, big, signed, dir, ago, shortDate } from '../format.js';
import { live } from '../livecells.js';

const enc = encodeURIComponent;
const S = (s) => enc(s);

function quoteLead(c, extra = []) {
  return h('div', { class: 'lead' },
    h('span', { class: 'bigpx' }, live(c.store, c.sym, 'p')),
    live(c.store, c.sym, 'c'), live(c.store, c.sym, 'cp'),
    live(c.store, c.sym, 'x', 'dim'),
    ...extra);
}

// ── DES ──────────────────────────────────────────────────────
export const DES = {
  load: (c) => c.get(`/api/des/${S(c.sym)}`),
  render(c, d, el) {
    const s = d.stats || {};
    const summary = h('p', { class: 'summary clamp', title: 'Click to expand' }, d.summary || 'No description available.');
    summary.addEventListener('click', () => summary.classList.toggle('clamp'));
    const upside = s.target_mean && s.price ? (s.target_mean / s.price - 1) * 100 : null;
    el.append(
      h('div', { class: 'lead' }, h('span', { class: 'amber' }, d.name),
        h('span', { class: 'dim' }, [d.exchange, d.type, d.currency].filter(Boolean).join(' · '))),
      quoteLead(c, [h('span', { class: 'dim' }, 'vol ', live(c.store, c.sym, 'v'))]),
      h('p', { class: 'dim' }, [d.sector, d.industry, d.location].filter(Boolean).join(' / ')),
      summary,
      h('div', { class: 'cols' },
        sec('Valuation', kv([
          ['Market cap', big(s.market_cap)], ['Enterprise value', big(s.enterprise_value)],
          ['P/E ttm', num(s.pe_ttm)], ['P/E forward', num(s.pe_fwd)],
          ['PEG', num(s.peg)], ['Price/sales', num(s.ps_ttm)],
          ['Price/book', num(s.pb)], ['Beta', num(s.beta)],
          ['EPS ttm', num(s.eps_ttm)], ['EPS forward', num(s.eps_fwd)],
          ['Dividend yield', s.div_yield_pct != null ? `${num(s.div_yield_pct)}%` : '—'], ['Payout ratio', ratioPct(s.payout_ratio)],
          ...(s.total_assets ? [['Fund assets', big(s.total_assets)], ['Expense ratio', ratioPct(s.expense_ratio, 2)]] : []),
        ])),
        sec('Trading', kv([
          ['52w high', px(s.hi_52w)], ['52w low', px(s.lo_52w)],
          ['50-day avg', px(s.ma_50)], ['200-day avg', px(s.ma_200)],
          ['Avg volume 3m', big(s.avg_vol_3m)], ['Avg volume 10d', big(s.avg_vol_10d)],
          ['Shares out', big(s.shares_out)], ['Float', big(s.float)],
          ['Short % float', ratioPct(s.short_pct_float)], ['Days to cover', num(s.short_ratio)],
          ['Target mean', px(s.target_mean)], ['Upside to target', upside != null ? pct(upside, 1) : '—', dir(upside)],
          ['Rating', (s.rec_key || '—').replace('_', ' ')], ['Analysts', int(s.num_analysts)],
        ])),
      ),
      sec('Company', kv([
        ['Employees', int(d.employees)], ['Website', d.website ? link(d.website, d.website.replace(/^https?:\/\//, '')) : '—'],
        ...(d.ipo ? [['IPO', d.ipo]] : []),
        ...(d.officers || []).slice(0, 4).map((o) => [o.title, o.name]),
      ], 1)),
    );
  },
  text: (c, d) => `${d.name}: P/E ${num(d.stats.pe_ttm)}, fwd ${num(d.stats.pe_fwd)}, mkt cap ${big(d.stats.market_cap)}`,
};

// ── FA ───────────────────────────────────────────────────────
export const FA = {
  load: (c) => c.get(`/api/fa/${S(c.sym)}`),
  render(c, d, el) {
    const mode = (c.args[0] || 'A').toUpperCase().startsWith('Q') ? 'quarterly' : 'annual';
    const r = d.ratios || {};
    const block = d[mode] || { periods: [], rows: [] };
    el.append(
      tabs([['A', 'Annual'], ['Q', 'Quarterly']], mode === 'annual' ? 'A' : 'Q', (v) => c.setArgs([v])),
      sec('Profitability & growth', kv([
        ['Revenue ttm', big(r.revenue_ttm)], ['EBITDA ttm', big(r.ebitda_ttm)],
        ['Gross margin', ratioPct(r.gross_margin)], ['Operating margin', ratioPct(r.operating_margin)],
        ['Net margin', ratioPct(r.profit_margin)], ['ROE', ratioPct(r.roe)],
        ['Revenue growth (yoy)', signedRatioPct(r.revenue_growth), dir(r.revenue_growth)],
        ['Earnings growth (yoy)', signedRatioPct(r.earnings_growth), dir(r.earnings_growth)],
      ])),
      sec('Balance sheet & cash', kv([
        ['Cash', big(r.total_cash)], ['Debt', big(r.total_debt)],
        ['Debt/equity', num(r.debt_to_equity)], ['Current ratio', num(r.current_ratio)],
        ['Operating cash flow', big(r.operating_cashflow)], ['Free cash flow', big(r.free_cashflow)],
        ['EV/EBITDA', num(r.ev_ebitda)], ['EV/revenue', num(r.ev_revenue)],
      ])),
      sec(`${mode === 'annual' ? 'Annual' : 'Quarterly'} statements (${d.currency})`,
        table({
          sortable: false,
          columns: [{ key: 'label', label: '' }, ...block.periods.map((p, i) => ({
            label: shortDate(p), align: 'r',
            fmt: (v, row) => (row.label === 'Diluted EPS' ? num(row.values[i]) : big(row.values[i])),
          }))],
          rows: block.rows, empty: 'No statements published for this security.',
        })),
    );
  },
  text: (c, d) => `Margins: gross ${ratioPct(d.ratios.gross_margin)}, net ${ratioPct(d.ratios.profit_margin)}; revenue growth ${signedRatioPct(d.ratios.revenue_growth)}; FCF ${big(d.ratios.free_cashflow)}`,
};

// ── EEO ──────────────────────────────────────────────────────
export const EEO = {
  load: (c) => c.get(`/api/eeo/${S(c.sym)}`),
  render(c, d, el) {
    const sm = d.summary || {};
    el.append(
      h('div', { class: 'lead' },
        h('span', {}, 'FY EPS estimate, last 30 days ', h('b', { class: dir(sm.fy_eps_chg_30d_pct) }, pct(sm.fy_eps_chg_30d_pct, 1))),
        h('span', {}, '90 days ', h('b', { class: dir(sm.fy_eps_chg_90d_pct) }, pct(sm.fy_eps_chg_90d_pct, 1))),
        h('span', {}, 'net revisions 30d ', h('b', { class: dir(sm.fy_net_revisions_30d) }, signed(sm.fy_net_revisions_30d, 0)))),
      sec('EPS estimate trend', table({ sortable: false, rows: d.trend, columns: [
        { key: 'label', label: 'Period' }, { key: 'current', label: 'Now', align: 'r', fmt: (v) => num(v, 3) },
        { key: 'd7', label: '7d ago', align: 'r', fmt: (v) => num(v, 3) }, { key: 'd30', label: '30d ago', align: 'r', fmt: (v) => num(v, 3) },
        { key: 'd90', label: '90d ago', align: 'r', fmt: (v) => num(v, 3) },
        { key: 'chg_30d_pct', label: 'Chg 30d', align: 'r', fmt: (v) => pct(v, 1), cls: (v) => dir(v) },
        { key: 'chg_90d_pct', label: 'Chg 90d', align: 'r', fmt: (v) => pct(v, 1), cls: (v) => dir(v) },
      ] })),
      sec('Revisions (number of analysts)', table({ sortable: false, rows: d.revisions, columns: [
        { key: 'label', label: 'Period' },
        { key: 'up_7d', label: 'Up 7d', align: 'r', cls: (v) => (v ? 'up' : '') }, { key: 'down_7d', label: 'Down 7d', align: 'r', cls: (v) => (v ? 'down' : '') },
        { key: 'up_30d', label: 'Up 30d', align: 'r', cls: (v) => (v ? 'up' : '') }, { key: 'down_30d', label: 'Down 30d', align: 'r', cls: (v) => (v ? 'down' : '') },
        { key: 'net_30d', label: 'Net 30d', align: 'r', fmt: (v) => signed(v, 0), cls: (v) => dir(v) },
      ] })),
      sec('Consensus EPS', table({ sortable: false, rows: d.earnings, columns: [
        { key: 'label', label: 'Period' }, { key: 'avg', label: 'Avg', align: 'r', fmt: (v) => num(v, 3) },
        { key: 'low', label: 'Low', align: 'r', fmt: (v) => num(v, 3) }, { key: 'high', label: 'High', align: 'r', fmt: (v) => num(v, 3) },
        { key: 'year_ago', label: 'Year ago', align: 'r', fmt: (v) => num(v, 3) },
        { key: 'growth', label: 'Growth', align: 'r', fmt: (v) => signedRatioPct(v), cls: (v) => dir(v) },
        { key: 'analysts', label: 'Analysts', align: 'r' },
      ] })),
      sec('Consensus revenue', table({ sortable: false, rows: d.revenue, columns: [
        { key: 'label', label: 'Period' }, { key: 'avg', label: 'Avg', align: 'r', fmt: (v) => big(v) },
        { key: 'low', label: 'Low', align: 'r', fmt: (v) => big(v) }, { key: 'high', label: 'High', align: 'r', fmt: (v) => big(v) },
        { key: 'year_ago', label: 'Year ago', align: 'r', fmt: (v) => big(v) },
        { key: 'growth', label: 'Growth', align: 'r', fmt: (v) => signedRatioPct(v), cls: (v) => dir(v) },
        { key: 'analysts', label: 'Analysts', align: 'r' },
      ] })),
      d.growth.length ? sec('Growth vs S&P 500', table({ sortable: false, rows: d.growth, columns: [
        { key: 'label', label: 'Period' }, { key: 'stock', label: c.sym, align: 'r', fmt: (v) => signedRatioPct(v), cls: (v) => dir(v) },
        { key: 'index', label: 'Index', align: 'r', fmt: (v) => signedRatioPct(v) },
      ] })) : null,
    );
  },
  text: (c, d) => `EPS revisions: FY estimate ${pct(d.summary.fy_eps_chg_30d_pct, 1)} over 30d, net ${d.summary.fy_net_revisions_30d} revisions`,
};

// ── ANR ──────────────────────────────────────────────────────
export const ANR = {
  load: (c) => c.get(`/api/anr/${S(c.sym)}`),
  render(c, d, el) {
    const t = d.targets || {};
    const up = t.mean && t.price ? (t.mean / t.price - 1) * 100 : null;
    const cur = d.trend[0] || {};
    const total = ['strong_buy', 'buy', 'hold', 'sell', 'strong_sell'].reduce((a, k) => a + (cur[k] || 0), 0);
    el.append(
      quoteLead(c),
      h('div', { class: 'cols' },
        sec('Price targets', kv([
          ['Mean', px(t.mean)], ['Upside', pct(up, 1), dir(up)], ['Median', px(t.median)], ['Analysts', int(t.num_analysts)],
          ['Low', px(t.low)], ['High', px(t.high)], ['Consensus', (t.rec_key || '—').replace('_', ' ')], ['', ''],
        ])),
        sec('Current ratings', total ? h('div', {},
          ...[['strong_buy', 'Strong buy'], ['buy', 'Buy'], ['hold', 'Hold'], ['sell', 'Sell'], ['strong_sell', 'Strong sell']].map(([k, label]) =>
            h('div', { style: { display: 'grid', gridTemplateColumns: '8em 3em 1fr', gap: '8px', alignItems: 'center' } },
              h('span', { class: 'dim' }, label), h('span', { class: 'r' }, cur[k] || 0), shareBar((cur[k] || 0) / total))))
          : note('No rating counts.')),
      ),
      sec('Rating trend', table({ sortable: false, rows: d.trend, columns: [
        { key: 'period', label: 'Period', fmt: (v) => (v === '0m' ? 'Now' : v.replace('-', '').replace('m', ' mo ago')) },
        { key: 'strong_buy', label: 'Str buy', align: 'r' }, { key: 'buy', label: 'Buy', align: 'r' },
        { key: 'hold', label: 'Hold', align: 'r' }, { key: 'sell', label: 'Sell', align: 'r' }, { key: 'strong_sell', label: 'Str sell', align: 'r' },
      ] })),
      sec('Recent rating actions', table({ rows: d.actions, columns: [
        { key: 'date', label: 'Date' }, { key: 'firm', label: 'Firm' },
        { key: 'action', label: 'Action', fmt: (v) => ({ up: 'Upgrade', down: 'Downgrade', init: 'Initiate', main: 'Maintain', reit: 'Reiterate' }[v] || v),
          cls: (v) => (v === 'up' ? 'up' : v === 'down' ? 'down' : '') },
        { key: 'to', label: 'Rating', fmt: (v, r) => (r.from && r.from !== v ? `${r.from} → ${v}` : v) },
        { key: 'target', label: 'Target', align: 'r', fmt: (v, r) => (v ? (r.prior_target && r.prior_target !== v ? `${px(r.prior_target)} → ${px(v)}` : px(v)) : '—') },
      ], empty: 'No recent rating changes.' })),
    );
  },
  text: (c, d) => `Analyst target mean ${px(d.targets.mean)} (${d.targets.num_analysts} analysts, ${d.targets.rec_key})`,
};

// ── EE ───────────────────────────────────────────────────────
export const EE = {
  load: (c) => c.get(`/api/ee/${S(c.sym)}`),
  render(c, d, el) {
    const n = d.next || {};
    el.append(
      sec('Next report', kv([
        ['Date', n.date ? `${n.date} ${n.hour || ''}` : '—'], ['EPS estimate', num(n.eps_avg, 3)],
        ['EPS range', n.eps_low != null ? `${num(n.eps_low, 3)} – ${num(n.eps_high, 3)}` : '—'], ['Revenue estimate', big(n.rev_avg)],
        ['Revenue range', n.rev_low != null ? `${big(n.rev_low)} – ${big(n.rev_high)}` : '—'], ['Ex-dividend', n.ex_dividend || '—'],
      ])),
      sec('History', table({ rows: d.history, columns: [
        { key: 'date', label: 'Date' }, { key: 'eps_est', label: 'EPS est', align: 'r', fmt: (v) => num(v, 3) },
        { key: 'eps_actual', label: 'Reported', align: 'r', fmt: (v) => num(v, 3) },
        { key: 'surprise_pct', label: 'Surprise', align: 'r', fmt: (v) => pct(v, 1), cls: (v) => dir(v) },
      ], empty: 'No earnings history.' })),
    );
  },
  text: (c, d) => `Next earnings ${d.next?.date || 'n/a'}; last surprises ${d.history.slice(0, 4).map((x) => pct(x.surprise_pct, 1)).join(', ')}`,
};

// ── INS ──────────────────────────────────────────────────────
export const INS = {
  load: (c) => c.get(`/api/ins/${S(c.sym)}`),
  render(c, d, el) {
    const q = d.last_90d || {};
    el.append(
      h('div', { class: 'lead' },
        h('span', {}, 'Open-market activity, 90 days: '),
        h('span', { class: 'up' }, `${q.buys} buys ${big(q.buy_value)}`),
        h('span', { class: 'down' }, `${q.sells} sales ${big(q.sell_value)}`),
        h('span', {}, 'net ', h('b', { class: dir(q.net_value) }, big(q.net_value)))),
      d.six_month.length ? sec('Six months (shares)', kv(d.six_month.map((r) => [r.label, `${big(r.shares)}${r.transactions != null ? ` · ${r.transactions} trades` : ''}`]), 1)) : null,
      sec('Transactions', table({ rows: d.rows, columns: [
        { key: 'date', label: 'Date' }, { key: 'insider', label: 'Insider' }, { key: 'position', label: 'Role' },
        { key: 'kind', label: 'Type', cls: (v) => (v === 'Buy' ? 'up' : v === 'Sale' ? 'down' : 'dim') },
        { key: 'shares', label: 'Shares', align: 'r', fmt: (v) => big(v) },
        { key: 'value', label: 'Value', align: 'r', fmt: (v) => (v ? big(v) : '—') },
        { key: 'ownership', label: 'Held' },
        { key: 'text', label: 'Detail', cls: () => 'dim' },
      ], empty: 'No insider transactions reported.' })),
      note('Awards and option exercises are listed but excluded from the buy/sale totals.'),
    );
  },
  text: (c, d) => `Insiders 90d: ${d.last_90d.buys} buys ${big(d.last_90d.buy_value)}, ${d.last_90d.sells} sales ${big(d.last_90d.sell_value)}`,
};

// ── HDS ──────────────────────────────────────────────────────
const holderCols = [
  { key: 'holder', label: 'Holder' }, { key: 'pct', label: '% out', align: 'r', fmt: (v) => ratioPct(v, 2) },
  { key: 'shares', label: 'Shares', align: 'r', fmt: (v) => big(v) }, { key: 'value', label: 'Value', align: 'r', fmt: (v) => big(v) },
  { key: 'pct_change', label: 'Chg', align: 'r', fmt: (v) => signedRatioPct(v), cls: (v) => dir(v) },
  { key: 'date', label: 'As of' },
];
export const HDS = {
  load: (c) => c.get(`/api/hds/${S(c.sym)}`),
  render(c, d, el) {
    el.append(
      sec('Ownership', kv([
        ['Insiders', ratioPct(d.insiders_pct, 2)], ['Institutions', ratioPct(d.institutions_pct)],
        ['Institutions (of float)', ratioPct(d.institutions_float_pct)], ['Institution count', int(d.institutions_count)],
      ])),
      sec('Top institutions', table({ rows: d.institutions, columns: holderCols })),
      sec('Top funds', table({ rows: d.funds, columns: holderCols })),
      note('From 13F filings, which lag the quarter end by up to 45 days.'),
    );
  },
  text: (c, d) => `Institutions own ${ratioPct(d.institutions_pct)}; insiders ${ratioPct(d.insiders_pct, 2)}`,
};

// ── CF ───────────────────────────────────────────────────────
const CF_FILTERS = [['ALL', 'All'], ['EVENT', '8-K events'], ['PERIODIC', '10-Q/10-K'], ['INSIDER', 'Insider'],
  ['OWNERSHIP', '13D/G'], ['DILUTION', 'Offerings'], ['OTHER', 'Other']];
export const CF = {
  load: (c) => c.get(`/api/cf/${S(c.sym)}`),
  render(c, d, el) {
    const f = (c.args[0] || 'ALL').toUpperCase();
    const n = d.last_90d || {};
    const rows = f === 'ALL' ? d.rows : d.rows.filter((r) => r.category.toUpperCase() === f);
    el.append(
      h('div', { class: 'lead' }, h('span', { class: 'amber' }, d.name),
        h('span', { class: 'dim' }, `CIK ${d.cik}${d.sic ? ` · ${d.sic}` : ''}`),
        link(d.edgar_url, 'EDGAR')),
      h('div', { class: 'lead' }, h('span', { class: 'dim' }, 'Last 90 days:'),
        h('span', {}, `${n['8k']} 8-K`), h('span', {}, `${n.insider} insider`),
        h('span', { class: n.dilution ? 'down' : '' }, `${n.dilution} offering/registration`),
        h('span', {}, `${n.ownership} 13D/G`)),
      tabs(CF_FILTERS, f, (v) => c.setArgs([v])),
      table({ rows, columns: [
        { key: 'filed', label: 'Filed' },
        { key: 'form', label: 'Form', cls: (v, r) => (r.category === 'dilution' ? 'down' : r.category === 'event' ? 'amber' : '') },
        { key: 'description', label: 'What', fmt: (v, r) => link(r.url, v || r.form) },
        { key: 'items', label: '8-K items', fmt: (v) => (v || []).filter((i) => i.code !== '9.01').map((i) => i.label || i.code).join(', '), cls: () => 'dim' },
      ], empty: 'No filings in this category.' }),
      d.ua_placeholder ? note('Tip: set SEC_USER_AGENT in Settings to "StockOracle you@yourdomain.com" — SEC asks every client for a real contact email.') : null,
    );
  },
  text: (c, d) => `SEC filings 90d: ${d.last_90d['8k']} 8-K, ${d.last_90d.dilution} offering filings; latest: ${d.rows.slice(0, 4).map((r) => `${r.filed} ${r.form}`).join(', ')}`,
};

// ── SI ───────────────────────────────────────────────────────
export const SI = {
  load: (c) => c.get(`/api/si/${S(c.sym)}`),
  render(c, d, el) {
    const i = d.interest || {};
    const v = d.daily_volume || { rows: [] };
    el.append(
      sec(`Short interest${i.as_of ? ` (as of ${i.as_of})` : ''}`, kv([
        ['Shares short', big(i.shares_short)], ['Prior month', big(i.prior_month)],
        ['Change', pct(i.change_pct, 1), dir(-(i.change_pct || 0))], ['Days to cover', num(i.days_to_cover)],
        ['% of float', ratioPct(i.pct_float)], ['% of shares out', ratioPct(i.pct_shares_out)],
        ['Float', big(i.float)], ['Avg volume', big(i.avg_volume)],
      ])),
      sec('Daily short-sale volume (off-exchange, FINRA)', h('div', {},
        h('div', { class: 'lead' }, h('span', {}, '5-day ', h('b', {}, v.avg_5d != null ? `${num(v.avg_5d, 1)}%` : '—')),
          h('span', {}, '20-day ', h('b', {}, v.avg_20d != null ? `${num(v.avg_20d, 1)}%` : '—')),
          spark(v.rows.map((r) => r.ratio), { w: 160, h: 26, cls: 'flat' })),
        table({ rows: [...v.rows].reverse(), columns: [
          { key: 'date', label: 'Date' }, { key: 'short', label: 'Short vol', align: 'r', fmt: (x) => big(x) },
          { key: 'total', label: 'Total vol', align: 'r', fmt: (x) => big(x) },
          { key: 'ratio', label: 'Short %', align: 'r', fmt: (x) => `${num(x, 1)}%`, cls: (x) => (x >= 55 ? 'down' : x <= 35 ? 'up' : '') },
        ], empty: 'No FINRA data for this symbol.' }))),
      note('Short-sale volume counts trades marked short that day (often market-maker hedging); short interest is the open position. ~40-50% is typical.'),
    );
  },
  text: (c, d) => `Short ${ratioPct(d.interest?.pct_float)} of float, ${num(d.interest?.days_to_cover)} days to cover; off-exchange short volume 5d ${num(d.daily_volume?.avg_5d, 1)}%`,
};

// ── DVD ──────────────────────────────────────────────────────
export const DVD = {
  load: (c) => c.get(`/api/dvd/${S(c.sym)}`),
  render(c, d, el) {
    const maxYear = Math.max(...d.annual.map((a) => a.total), 0) || 1;
    el.append(
      sec('Dividend', kv([
        ['Yield', d.yield_pct != null ? `${num(d.yield_pct)}%` : '—'], ['Annual rate', px(d.annual_rate)],
        ['Paid last 12m', px(d.ttm_paid)], ['Payout ratio', ratioPct(d.payout_ratio)],
        ['Ex-date', d.ex_date || '—'], ['5y growth / yr', d.growth_5y_cagr_pct != null ? pct(d.growth_5y_cagr_pct, 1) : '—', dir(d.growth_5y_cagr_pct)],
        ['5y avg yield', d.five_year_avg_yield != null ? `${num(d.five_year_avg_yield)}%` : '—'], ['', ''],
      ])),
      h('div', { class: 'cols' },
        sec('By year', table({ sortable: false, rows: d.annual, columns: [
          { key: 'year', label: 'Year' }, { key: 'total', label: 'Total', align: 'r', fmt: (v) => num(v, 4) },
          { label: '', fmt: (v, r) => shareBar(r.total / maxYear) },
        ], empty: 'No dividends paid.' })),
        sec('Payments', table({ sortable: false, rows: d.payments.slice(0, 16), columns: [
          { key: 'date', label: 'Ex-date' }, { key: 'amount', label: 'Amount', align: 'r', fmt: (v) => num(v, 4) },
        ], empty: 'No dividends paid.' })),
      ),
      d.splits.length ? sec('Splits', table({ sortable: false, rows: d.splits, columns: [
        { key: 'date', label: 'Date' }, { key: 'label', label: 'Split' }] })) : null,
    );
  },
  text: (c, d) => `Dividend yield ${num(d.yield_pct)}%, 5y growth ${pct(d.growth_5y_cagr_pct, 1)}/yr`,
};

// ── HOLD (ETF holdings) ──────────────────────────────────────
export const HOLD = {
  load: (c) => c.get(`/api/hold/${S(c.sym)}`),
  render(c, d, el) {
    const sectors = Object.entries(d.sectors || {}).filter(([, v]) => v);
    el.append(
      h('div', { class: 'lead' }, h('span', { class: 'amber' }, d.category || d.legal_type), h('span', { class: 'dim' }, d.family)),
      sec('Fund', kv([
        ['Expense ratio', ratioPct(d.expense_ratio, 2)], ['Turnover', ratioPct(d.turnover, 0)],
        ['Top-10 weight', ratioPct(d.top10_pct)], ['Net assets', d.net_assets ? `${big(d.net_assets * 1e6)}` : '—'],
        ...Object.entries(d.equity_stats || {}).slice(0, 4).map(([k, v]) => [k, num(v)]),
      ])),
      h('div', { class: 'cols' },
        sec('Top holdings', table({ rows: d.top_holdings, onRow: (r) => c.load(r.symbol), columns: [
          { key: 'symbol', label: 'Ticker', cls: () => 'tk' }, { key: 'name', label: 'Name' },
          { key: 'pct', label: 'Weight', align: 'r', fmt: (v) => ratioPct(v, 2) },
          { label: 'Today', align: 'r', fmt: (v, r) => live(c.store, r.symbol, 'cp') },
        ] })),
        sec('Sectors', table({ sortable: false, rows: sectors.map(([k, v]) => ({ k, v })), columns: [
          { key: 'k', label: 'Sector', fmt: (v) => v.replace(/_/g, ' ') },
          { key: 'v', label: 'Weight', align: 'r', fmt: (v) => ratioPct(v) }, { label: '', fmt: (v, r) => shareBar(r.v) },
        ] })),
      ),
    );
  },
  text: (c, d) => `ETF ${d.category}; expense ${ratioPct(d.expense_ratio, 2)}; top holdings ${d.top_holdings.slice(0, 5).map((x) => x.symbol).join(', ')}`,
};

// ── OMON ─────────────────────────────────────────────────────
export const OMON = {
  load: (c) => c.get(`/api/omon/${S(c.sym)}${c.args[0] ? `?expiry=${enc(c.args[0])}` : ''}`),
  render(c, d, el) {
    const t = d.totals || {};
    const select = h('select', { class: 'inp', onchange: (e) => c.setArgs([e.target.value]) },
      d.expiries.map((x) => h('option', { value: x, selected: x === d.expiry }, x)));
    const strikes = [...new Set([...d.calls, ...d.puts].map((r) => r.strike))].sort((a, b) => a - b);
    const byStrike = (arr) => Object.fromEntries(arr.map((r) => [r.strike, r]));
    const C = byStrike(d.calls), P = byStrike(d.puts);
    const side = (r, k, f) => (r && r[k] != null ? f(r[k]) : '');
    const rows = strikes.map((k) => ({ k, c: C[k], p: P[k] }));
    const ivf = (v) => `${num(v * 100, 1)}%`;
    el.append(
      h('div', { class: 'lead' }, h('span', {}, 'Expiry '), select, h('span', { class: 'dim' }, 'spot'), h('b', {}, px(d.spot)),
        h('span', { class: 'dim' }, 'P/C volume'), h('b', {}, num(t.pc_volume)), h('span', { class: 'dim' }, 'P/C open interest'), h('b', {}, num(t.pc_oi))),
      table({ sortable: false, rows, columns: [
        { label: 'C bid', align: 'r', fmt: (v, r) => side(r.c, 'bid', (x) => num(x)) },
        { label: 'C ask', align: 'r', fmt: (v, r) => side(r.c, 'ask', (x) => num(x)) },
        { label: 'C last', align: 'r', fmt: (v, r) => side(r.c, 'last', (x) => num(x)) },
        { label: 'C vol', align: 'r', fmt: (v, r) => side(r.c, 'volume', int) },
        { label: 'C OI', align: 'r', fmt: (v, r) => side(r.c, 'oi', int) },
        { label: 'C IV', align: 'r', fmt: (v, r) => side(r.c, 'iv', ivf) },
        { label: 'Strike', align: 'r', fmt: (v, r) => h('b', { class: 'amber' }, num(r.k)) },
        { label: 'P bid', align: 'r', fmt: (v, r) => side(r.p, 'bid', (x) => num(x)) },
        { label: 'P ask', align: 'r', fmt: (v, r) => side(r.p, 'ask', (x) => num(x)) },
        { label: 'P last', align: 'r', fmt: (v, r) => side(r.p, 'last', (x) => num(x)) },
        { label: 'P vol', align: 'r', fmt: (v, r) => side(r.p, 'volume', int) },
        { label: 'P OI', align: 'r', fmt: (v, r) => side(r.p, 'oi', int) },
        { label: 'P IV', align: 'r', fmt: (v, r) => side(r.p, 'iv', ivf) },
      ] }),
      note(`Totals for ${d.expiry}: calls ${int(t.call_volume)} vol / ${int(t.call_oi)} OI, puts ${int(t.put_volume)} vol / ${int(t.put_oi)} OI. Yahoo options data is delayed.`),
    );
  },
  text: (c, d) => `Options ${d.expiry}: put/call volume ${num(d.totals.pc_volume)}, OI ${num(d.totals.pc_oi)}`,
};

// ── HP ───────────────────────────────────────────────────────
export const HP = {
  load: (c) => c.get(`/api/history/${S(c.sym)}?range=${enc((c.args[0] || '3M').toUpperCase())}&daily=1`),
  render(c, d, el) {
    const rng = (c.args[0] || '3M').toUpperCase();
    const rows = d.candles.map((k, i) => ({ ...k, chg: i ? (k.close / d.candles[i - 1].close - 1) * 100 : null })).reverse();
    el.append(
      tabs([['1M', '1M'], ['3M', '3M'], ['6M', '6M'], ['1Y', '1Y'], ['5Y', '5Y (weekly)']], rng, (v) => c.setArgs([v])),
      table({ rows, columns: [
        { key: 'time', label: 'Date', fmt: (v) => (typeof v === 'number' ? new Date(v * 1000).toISOString().slice(0, 16).replace('T', ' ') : v) },
        { key: 'open', label: 'Open', align: 'r', fmt: (v) => px(v, c.sym) }, { key: 'high', label: 'High', align: 'r', fmt: (v) => px(v, c.sym) },
        { key: 'low', label: 'Low', align: 'r', fmt: (v) => px(v, c.sym) }, { key: 'close', label: 'Close', align: 'r', fmt: (v) => px(v, c.sym) },
        { key: 'chg', label: 'Chg', align: 'r', fmt: (v) => pct(v), cls: (v) => dir(v) },
        { key: 'volume', label: 'Volume', align: 'r', fmt: (v) => big(v) },
      ] }),
    );
  },
};

// ── RV (peers) ───────────────────────────────────────────────
export const RV = {
  load: (c) => c.get(`/api/peers/${S(c.sym)}`),
  render(c, d, el) {
    el.append(
      d.src === 'watchlist-sector' ? note('No Finnhub key, so peers are the same-sector names from your watchlist.') : null,
      table({ rows: d.rows, onRow: (r) => c.load(r.symbol), rowTitle: (r) => r.name, columns: [
        { key: 'symbol', label: 'Ticker', cls: (v) => (v === c.sym ? 'tk amber' : 'tk') },
        { key: 'price', label: 'Last', align: 'r', fmt: (v, r) => live(c.store, r.symbol, 'p') },
        { label: 'Chg %', align: 'r', fmt: (v, r) => live(c.store, r.symbol, 'cp') },
        { key: 'market_cap', label: 'Mkt cap', align: 'r', fmt: (v) => big(v) },
        { key: 'pe_ttm', label: 'P/E', align: 'r', fmt: (v) => num(v, 1) }, { key: 'pe_fwd', label: 'Fwd P/E', align: 'r', fmt: (v) => num(v, 1) },
        { key: 'ps_ttm', label: 'P/S', align: 'r', fmt: (v) => num(v, 1) }, { key: 'pb', label: 'P/B', align: 'r', fmt: (v) => num(v, 1) },
        { key: 'revenue_growth', label: 'Rev gr', align: 'r', fmt: (v) => signedRatioPct(v), cls: (v) => dir(v) },
        { key: 'gross_margin', label: 'Gross m', align: 'r', fmt: (v) => ratioPct(v) },
        { key: 'profit_margin', label: 'Net m', align: 'r', fmt: (v) => ratioPct(v) },
        { key: 'div_yield_pct', label: 'Yield', align: 'r', fmt: (v) => (v != null ? `${num(v)}%` : '—') },
        { key: 'beta', label: 'Beta', align: 'r', fmt: (v) => num(v) },
      ] }),
    );
  },
  text: (c, d) => `Peers: ${d.rows.map((r) => `${r.symbol} P/E ${num(r.pe_ttm, 1)}`).join(', ')}`,
};

// ── N / TOP ──────────────────────────────────────────────────
export function newsList(articles) {
  if (!articles.length) return note('No recent articles.');
  return h('ul', { class: 'news' }, articles.map((a) => h('li', {},
    h('span', { class: 'dim', title: a.timestamp ? new Date(a.timestamp * 1000).toLocaleString() : '' }, ago(a.timestamp)),
    h('span', {}, link(a.url, a.headline), h('span', { class: 'src' }, a.source)))));
}

export const N = {
  load: (c) => c.get(`/api/news/${S(c.sym)}`),
  refresh: 300,
  render(c, d, el) { el.append(newsList(d.articles), note(`Source: ${d.src === 'finnhub' ? 'Finnhub company news' : 'Yahoo Finance'}`)); },
  text: (c, d) => `Headlines: ${d.articles.slice(0, 6).map((a) => a.headline).join(' | ')}`,
};
