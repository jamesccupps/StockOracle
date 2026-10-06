// BRIEF — every source on one page for a security, each block drilling into
// its full function. The same dossier is what Claude receives with ASK.
import { h, kv as kv2, sec, note, link } from '../ui.js';

const kv = (pairs) => kv2(pairs.filter(([label]) => label !== ''), 1);
import { px, num, int, pct, ratioPct, signedRatioPct, big, signed, dir, ago, agoSeconds } from '../format.js';
import { live } from '../livecells.js';

const VCLS = { BULLISH: 'up', BEARISH: 'down', NEUTRAL: 'amber' };

function block(c, title, fn, body) {
  const s = h('section', { class: 'sec' });
  const head = h('h3', {}, h('span', {}, title));
  if (fn) head.append(h('span', { class: 'drill', title: `Open ${fn}`, onclick: () => c.open(`${c.sym} ${fn}`) }, `${fn} ›`));
  s.append(head, body);
  return s;
}

export const BRIEF = {
  load: (c) => c.get(`/api/brief/${encodeURIComponent(c.sym)}`),
  refresh: 600,
  events: { oracle: (c, m) => (m.s === c.sym && m.state === 'done' ? 'reload' : null) },
  render(c, d, el) {
    const p = d.price || {}, v = d.valuation || {};
    const range = p.hi_52w && p.lo_52w ? (p.last - p.lo_52w) / (p.hi_52w - p.lo_52w) : null;
    el.append(
      h('div', { class: 'lead' }, h('span', { class: 'amber' }, d.name), h('span', { class: 'dim' }, d.sector)),
      h('div', { class: 'lead' },
        h('span', { class: 'bigpx' }, live(c.store, c.sym, 'p')), live(c.store, c.sym, 'c'), live(c.store, c.sym, 'cp'),
        live(c.store, c.sym, 'x', 'dim'),
        d.oracle ? h('span', { class: `chip ${VCLS[d.oracle.prediction] || ''}`, title: 'Stock Oracle verdict — open ORC for the breakdown',
          style: { cursor: 'pointer' }, onclick: () => c.open(`${c.sym} ORC`) },
        `Oracle ${d.oracle.prediction} ${signed(d.oracle.signal, 3)}`) : null),
    );

    const blocks = [];
    blocks.push(block(c, 'Price & valuation', 'DES', kv([
      ['52w range', `${px(p.lo_52w)} – ${px(p.hi_52w)}`], ['In range', range != null ? `${num(range * 100, 0)}%` : '—'],
      ['50 / 200-day', `${px(p.ma_50)} / ${px(p.ma_200)}`], ['Market cap', big(v.market_cap)],
      ['P/E ttm / fwd', `${num(v.pe_ttm, 1)} / ${num(v.pe_fwd, 1)}`], ['P/S', num(v.ps_ttm, 1)],
      ['Beta', num(v.beta)], ['Yield', v.div_yield_pct != null ? `${num(v.div_yield_pct)}%` : '—'],
    ])));

    if (d.oracle) {
      const o = d.oracle;
      blocks.push(block(c, 'Stock Oracle', 'ORC', h('div', {},
        kv([['Verdict', o.prediction, VCLS[o.prediction]], ['Signal', signed(o.signal, 4), dir(o.signal)],
          ['Confidence', `${num((o.confidence || 0) * 100, 0)}%`], ['Threshold', `±${num(o.threshold, 3)}`],
          ['Regime', o.regime || '—'], ['From', `${o.source || ''}`]]),
        h('p', { class: 'dim' }, 'Strongest: ', o.top.map((s) => `${s.collector.replace(/_/g, ' ')} ${signed(s.signal, 2)}`).join(', ') || '—'))));
    } else if (d.estimates || d.insiders) {
      blocks.push(block(c, 'Stock Oracle', 'ORC', h('p', {}, h('span', { class: 'dim' }, 'Not analyzed yet. '),
        h('button', { class: 'btn', onclick: () => c.open(`RUN ${c.sym}`) }, 'Run Oracle now'))));
    }

    if (d.estimates) {
      const e = d.estimates;
      blocks.push(block(c, 'Estimates & revisions', 'EEO', kv([
        ['FY EPS consensus', num(e.fy_eps, 2)], ['Analysts', int(e.analysts)],
        ['FY growth', signedRatioPct(e.fy_growth), dir(e.fy_growth)], ['Net revisions 30d', signed(e.fy_net_revisions_30d, 0), dir(e.fy_net_revisions_30d)],
        ['Estimate chg 30d', pct(e.fy_eps_chg_30d_pct, 1), dir(e.fy_eps_chg_30d_pct)], ['Estimate chg 90d', pct(e.fy_eps_chg_90d_pct, 1), dir(e.fy_eps_chg_90d_pct)],
      ])));
    }
    if (d.analysts) {
      const a = d.analysts;
      const up = a.target_mean && p.last ? (a.target_mean / p.last - 1) * 100 : null;
      blocks.push(block(c, 'Analysts', 'ANR', h('div', {},
        kv([['Buy / hold / sell', `${a.buy ?? '—'} / ${a.hold ?? '—'} / ${a.sell ?? '—'}`], ['Rating', (a.rating || '—').replace('_', ' ')],
          ['Target mean', px(a.target_mean)], ['Upside', pct(up, 1), dir(up)],
          ['Target range', `${px(a.target_low)} – ${px(a.target_high)}`], ['', '']]),
        a.recent_actions?.length ? h('p', { class: 'dim' }, a.recent_actions.slice(0, 3).join(' · ')) : null)));
    }
    if (d.earnings) {
      const e = d.earnings;
      blocks.push(block(c, 'Earnings', 'EE', kv([
        ['Next report', e.next ? `${e.next} ${e.hour || ''}` : '—'], ['EPS estimate', num(e.eps_est, 3)],
        ['Recent surprises', (e.last_surprises_pct || []).map((x) => pct(x, 1)).join('  ') || '—'], ['', ''],
      ])));
    }
    if (d.insiders) {
      const i = d.insiders;
      blocks.push(block(c, 'Insiders, 90 days (open market)', 'INS', kv([
        ['Buys', `${i.buys} · ${big(i.buy_value)}`, i.buys ? 'up' : ''], ['Sales', `${i.sells} · ${big(i.sell_value)}`, i.sells ? 'down' : ''],
        ['Net', big(i.net_value), dir(i.net_value)], ['', ''],
      ])));
    }
    if (d.short) {
      const s = d.short;
      blocks.push(block(c, 'Short positioning', 'SI', kv([
        ['% of float short', ratioPct(s.pct_float)], ['Days to cover', num(s.days_to_cover)],
        ['vs prior month', pct(s.change_pct, 1), dir(-(s.change_pct || 0))], ['As of', s.as_of || '—'],
        ['Short vol ratio 5d', s.offexch_short_ratio_5d != null ? `${num(s.offexch_short_ratio_5d, 1)}%` : '—'],
        ['Short vol ratio 20d', s.offexch_short_ratio_20d != null ? `${num(s.offexch_short_ratio_20d, 1)}%` : '—'],
      ])));
    }
    if (d.filings) {
      const f = d.filings, n = f.last_90d || {};
      blocks.push(block(c, 'SEC filings', 'CF', h('div', {},
        h('p', {}, h('span', { class: 'dim' }, '90 days: '), `${n['8k']} 8-K · ${n.insider} insider · `,
          h('span', { class: n.dilution ? 'down' : '' }, `${n.dilution} offering`), ` · ${n.ownership} 13D/G`),
        h('ul', { class: 'news' }, (f.recent || []).slice(0, 5).map((r) => h('li', {},
          h('span', { class: 'dim' }, r.filed.slice(5)),
          h('span', {}, h('b', { class: r.form.startsWith('8-K') ? 'amber' : '' }, r.form), ' ', r.what,
            r.items?.length ? h('span', { class: 'dim' }, ` — ${r.items.join(', ')}`) : null)))))));
    }
    if (d.dividend) {
      const dv = d.dividend;
      blocks.push(block(c, 'Dividend', 'DVD', kv([
        ['Yield', dv.yield_pct != null ? `${num(dv.yield_pct)}%` : '—'], ['Annual rate', px(dv.annual_rate)],
        ['Ex-date', dv.ex_date || '—'], ['5y growth / yr', pct(dv.growth_5y_pct, 1), dir(dv.growth_5y_pct)],
      ])));
    }
    el.append(h('div', { class: 'cols' }, blocks));
    el.append(block(c, 'Headlines', 'N', d.headlines?.length
      ? h('ul', { class: 'news' }, d.headlines.map((x) => h('li', {}, h('span', { class: 'dim' }, ago(x.t)),
        h('span', {}, link(x.url, x.headline), h('span', { class: 'src' }, x.source)))))
      : note('No recent articles.')));
    if (d.summary) el.append(block(c, 'Business', 'DES', h('p', { class: 'summary' }, d.summary)));
    el.append(note(`Compiled ${agoSeconds(Date.now() / 1000 - d.generated)} ago from Yahoo, SEC EDGAR, FINRA${d.oracle ? ' and Stock Oracle' : ''}. Each heading opens the full function.`));
  },
  text: (c, d) => `Briefing for ${d.symbol} on screen (${d.name})`,
};
