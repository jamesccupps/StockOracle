// Cells that update themselves from the live quote stream.
// A panel drops live(sym, field) anywhere; the panel host calls
// refreshLive(root, store, changedSymbols) whenever quotes arrive.

import { px, signed, pct, big, dir } from './format.js';
import { h } from './ui.js';

const FIELDS = {
  p: (q, sym) => px(q.p, sym),
  c: (q, sym) => signed(q.c, q.p != null && Math.abs(q.p) < 1 ? 4 : 2),
  cp: (q) => pct(q.cp),
  v: (q) => big(q.v),
  // extended-hours move, e.g. "post +0.6%"
  x: (q) => (q.xp != null && q.xcp != null ? `${q.xs || 'ext'} ${pct(q.xcp)}` : ''),
  h: (q, sym) => px(q.h, sym),
  l: (q, sym) => px(q.l, sym),
  pc: (q, sym) => px(q.pc, sym),
};

const COLORED = { c: 'c', cp: 'c', x: 'xcp' };   // field → quote key that decides the color

export function live(store, sym, field, extraClass = '') {
  const el = h('span', { class: `lv ${extraClass}`, dataset: { q: sym, f: field } });
  const q = store.get(sym);
  if (q) paint(el, q, sym, field, false);
  else el.textContent = '—';
  return el;
}

function paint(el, q, sym, field, flash) {
  const text = FIELDS[field] ? FIELDS[field](q, sym) : '';
  if (el.textContent === text) return;
  el.textContent = text || (field === 'x' ? '' : '—');
  if (COLORED[field]) {
    el.classList.remove('up', 'down', 'flat');
    el.classList.add(dir(q[COLORED[field]]));
  }
  if (flash && field === 'p' && q.prevPrice != null && q.p !== q.prevPrice) {
    const cell = el.closest('td') || el;
    cell.classList.remove('fl-up', 'fl-down');
    void cell.offsetWidth;                       // restart the animation
    cell.classList.add(q.p > q.prevPrice ? 'fl-up' : 'fl-down');
  }
}

export function refreshLive(root, store, changed) {
  if (!root) return;
  const set = changed ? new Set(changed) : null;
  for (const el of root.querySelectorAll('.lv[data-q]')) {
    const sym = el.dataset.q;
    if (set && !set.has(sym)) continue;
    const q = store.get(sym);
    if (q) paint(el, q, sym, el.dataset.f, true);
  }
}

// Symbols referenced by live cells under root (for subscriptions)
export function liveSymbols(root) {
  const out = new Set();
  if (root) for (const el of root.querySelectorAll('.lv[data-q]')) out.add(el.dataset.q);
  return [...out];
}
