// GP — price graph (TradingView Lightweight Charts, vendored in /static/vendor)
import { h, tabs } from '../ui.js';
import { px, pct, big, dir } from '../format.js';
import { RANGES } from '../commands.js';

const LWC = () => window.LightweightCharts;
const INTRADAY_BAR = { '1D': 60, '5D': 300, '1M': 1800, '3M': 3600 };

function sma(candles, n) {
  const out = [];
  let sum = 0;
  for (let i = 0; i < candles.length; i++) {
    sum += candles[i].close;
    if (i >= n) sum -= candles[i - n].close;
    if (i >= n - 1) out.push({ time: candles[i].time, value: sum / n });
  }
  return out;
}

// Wall-clock seconds for "now" in New York, matching the server's shifted bar times
function nowWallClockET() {
  const parts = new Intl.DateTimeFormat('en-US', { timeZone: 'America/New_York', hourCycle: 'h23',
    year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit' })
    .formatToParts(new Date()).reduce((o, p) => (o[p.type] = p.value, o), {});
  return Date.UTC(+parts.year, +parts.month - 1, +parts.day, +parts.hour, +parts.minute, +parts.second) / 1000;
}

export const GP = {
  flush: true,
  load: (c) => c.get(`/api/history/${encodeURIComponent(c.sym)}?range=${encodeURIComponent(GP.range(c))}`),
  range: (c) => (RANGES.includes((c.args[0] || '').toUpperCase()) ? c.args[0].toUpperCase() : '6M'),
  render(c, d, el) {
    const rng = GP.range(c);
    const st = c.state;
    st.mas = st.mas || { 20: false, 50: true, 200: rng !== '1D' && rng !== '5D' };
    const legend = h('span', { class: 'chart-legend' });
    const maBtns = h('div', { class: 'tabs' }, [20, 50, 200].map((n) =>
      h('button', { class: st.mas[n] ? 'on' : '', onclick: () => { st.mas[n] = !st.mas[n]; c.rerender(); } }, `MA${n}`)));
    const bar = h('div', { class: 'chart-bar' },
      tabs(RANGES.map((r) => [r, r]), rng, (v) => c.setArgs([v])), maBtns, legend);
    const box = h('div', { class: 'chart-box' });
    el.append(bar, box);
    if (!LWC()) { box.append(h('p', { class: 'state err' }, 'Chart library failed to load.')); return; }

    const chart = LWC().createChart(box, {
      autoSize: true,
      layout: { background: { color: '#000' }, textColor: '#9a9a9a', fontFamily: 'Consolas, Menlo, monospace', fontSize: 11,
        attributionLogo: true },
      grid: { vertLines: { color: '#111' }, horzLines: { color: '#111' } },
      rightPriceScale: { borderColor: '#2a2a2a' },
      timeScale: { borderColor: '#2a2a2a', timeVisible: d.intraday, secondsVisible: false, rightOffset: 4 },
      crosshair: { mode: 0 },
      localization: { locale: 'en-US' },
    });
    const candles = chart.addSeries(LWC().CandlestickSeries, {
      upColor: '#3ccf6e', downColor: '#ff5b4f', wickUpColor: '#3ccf6e', wickDownColor: '#ff5b4f', borderVisible: false,
    });
    candles.setData(d.candles);
    const vol = chart.addSeries(LWC().HistogramSeries, { priceScaleId: 'vol', priceFormat: { type: 'volume' }, lastValueVisible: false, priceLineVisible: false });
    chart.priceScale('vol').applyOptions({ scaleMargins: { top: 0.82, bottom: 0 } });
    vol.setData(d.candles.map((k) => ({ time: k.time, value: k.volume, color: k.close >= k.open ? 'rgba(60,207,110,.35)' : 'rgba(255,91,79,.35)' })));
    const maColors = { 20: '#79c0ff', 50: '#ffa028', 200: '#c792ea' };
    for (const n of [20, 50, 200]) {
      if (st.mas[n] && d.candles.length > n) {
        chart.addSeries(LWC().LineSeries, { color: maColors[n], lineWidth: 1, lastValueVisible: false, priceLineVisible: false, crosshairMarkerVisible: false })
          .setData(sma(d.candles, n));
      }
    }
    if (d.prev_close && rng === '1D') {
      candles.createPriceLine({ price: d.prev_close, color: '#5c5c5c', lineStyle: 2, lineWidth: 1, axisLabelVisible: true, title: 'prev close' });
    }
    chart.timeScale().fitContent();

    const first = d.candles[0];
    let last = d.candles[d.candles.length - 1];
    const showLegend = (k) => {
      if (!k) k = last;
      const base = rng === '1D' && d.prev_close ? d.prev_close : first.open;
      const chg = (k.close / base - 1) * 100;
      legend.textContent = `O ${px(k.open, c.sym)}  H ${px(k.high, c.sym)}  L ${px(k.low, c.sym)}  C ${px(k.close, c.sym)}  V ${big(k.volume)}  ${pct(chg)} ${rng}`;
      legend.className = `chart-legend ${dir(chg)}`;
    };
    showLegend();
    const byTime = new Map(d.candles.map((k) => [typeof k.time === 'object' ? JSON.stringify(k.time) : k.time, k]));
    chart.subscribeCrosshairMove((p) => showLegend(p && p.time != null ? byTime.get(p.time) : null));

    // Live: extend/update the last intraday bar from streamed prices
    const step = INTRADAY_BAR[rng];
    st.live = (q) => {
      if (!step || !q || q.p == null || !last) return;
      const t = Math.floor(nowWallClockET() / step) * step;
      if (t < last.time) return;
      if (t === last.time) {
        last.high = Math.max(last.high, q.p); last.low = Math.min(last.low, q.p); last.close = q.p;
      } else {
        last = { time: t, open: last.close, high: Math.max(last.close, q.p), low: Math.min(last.close, q.p), close: q.p, volume: 0 };
        d.candles.push(last);
        byTime.set(t, last);
      }
      candles.update({ time: last.time, open: last.open, high: last.high, low: last.low, close: last.close });
      showLegend();
    };
    st.dispose = () => chart.remove();
  },
  onQuotes(c, d, el, changed) {
    if (changed.includes(c.sym) && c.state.live) c.state.live(c.store.get(c.sym));
  },
  watch: (c) => [c.sym],
  text: (c, d) => {
    const k = d.candles; if (!k.length) return '';
    return `${c.sym} ${d.range} chart: ${px(k[0].open)} → ${px(k[k.length - 1].close)} (${pct((k[k.length - 1].close / k[0].open - 1) * 100)})`;
  },
};
