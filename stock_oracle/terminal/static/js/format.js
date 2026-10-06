// Number, date and color formatting shared by every panel.

const isNum = (v) => typeof v === 'number' && Number.isFinite(v);

export function priceDigits(sym, v) {
  if (sym && /=X$/.test(sym)) return 4;
  if (!isNum(v)) return 2;
  const a = Math.abs(v);
  if (a >= 1) return 2;
  if (a >= 0.01) return 4;
  return 6;
}

export function px(v, sym) {
  if (!isNum(v)) return '—';
  const d = priceDigits(sym, v);
  return v.toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d });
}

export function num(v, d = 2) {
  if (!isNum(v)) return '—';
  return v.toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d });
}

export function int(v) {
  if (!isNum(v)) return '—';
  return Math.round(v).toLocaleString('en-US');
}

export function signed(v, d = 2) {
  if (!isNum(v)) return '—';
  return (v > 0 ? '+' : v < 0 ? '−' : '') + Math.abs(v).toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d });
}

export function pct(v, d = 2) {          // v already in percent units
  return isNum(v) ? `${signed(v, d)}%` : '—';
}

export function ratioPct(v, d = 1) {     // v is a fraction (0.25 → 25.0%)
  return isNum(v) ? `${num(v * 100, d)}%` : '—';
}

export function signedRatioPct(v, d = 1) {
  return isNum(v) ? `${signed(v * 100, d)}%` : '—';
}

export function bp(v) {                  // v in percentage points → basis points
  return isNum(v) ? `${signed(v * 100, 0)}bp` : '—';
}

export function big(v, d = 2) {
  if (!isNum(v)) return '—';
  const a = Math.abs(v), s = v < 0 ? '−' : '';
  if (a >= 1e12) return `${s}${num(a / 1e12, d)}T`;
  if (a >= 1e9) return `${s}${num(a / 1e9, d)}B`;
  if (a >= 1e6) return `${s}${num(a / 1e6, d)}M`;
  if (a >= 1e3) return `${s}${num(a / 1e3, 1)}K`;
  return `${s}${num(a, a >= 100 ? 0 : 2)}`;
}

export function dir(v) {
  if (!isNum(v) || v === 0) return 'flat';
  return v > 0 ? 'up' : 'down';
}

export function ago(epochSeconds) {
  if (!isNum(epochSeconds) || epochSeconds <= 0) return '';
  const s = Math.max(0, Date.now() / 1000 - epochSeconds);
  if (s < 60) return `${Math.floor(s)}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m`;
  if (s < 86400) return `${Math.floor(s / 3600)}h`;
  if (s < 86400 * 30) return `${Math.floor(s / 86400)}d`;
  return new Date(epochSeconds * 1000).toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
}

export function agoSeconds(seconds) {
  return isNum(seconds) ? ago(Date.now() / 1000 - seconds) : '';
}

export function shortDate(iso) {
  if (!iso) return '—';
  const d = new Date(iso.length <= 10 ? `${iso}T12:00:00` : iso);
  if (isNaN(d)) return iso;
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: '2-digit' });
}

export function clockET(date = new Date()) {
  return date.toLocaleTimeString('en-US', { timeZone: 'America/New_York', hour12: false });
}

export const fmt = { px, num, int, signed, pct, ratioPct, signedRatioPct, bp, big, dir, ago, agoSeconds, shortDate, clockET };
