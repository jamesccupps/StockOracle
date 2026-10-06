// Tiny DOM toolkit. Everything goes through createElement/textContent —
// data from the network is never inserted as HTML.

import { dir } from './format.js';

export function h(tag, attrs = {}, ...children) {
  const el = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (v === undefined || v === null || v === false) continue;
    if (k === 'class') el.className = v;
    else if (k === 'style' && typeof v === 'object') Object.assign(el.style, v);
    else if (k.startsWith('on') && typeof v === 'function') el.addEventListener(k.slice(2).toLowerCase(), v);
    else if (k === 'dataset') Object.assign(el.dataset, v);
    else el.setAttribute(k, v === true ? '' : v);
  }
  append(el, children);
  return el;
}

function append(el, children) {
  for (const c of children.flat(Infinity)) {
    if (c === undefined || c === null || c === false) continue;
    el.appendChild(c instanceof Node ? c : document.createTextNode(String(c)));
  }
}

export function clear(el) { while (el.firstChild) el.removeChild(el.firstChild); return el; }

export function link(url, text, cls = 'ext') {
  if (!url || !/^https?:\/\//i.test(url)) return h('span', {}, text);
  return h('a', { href: url, target: '_blank', rel: 'noopener noreferrer', class: cls }, text);
}

export function sec(title, ...children) {
  return h('section', { class: 'sec' }, title ? h('h3', {}, title) : null, ...children);
}

export function note(text) { return h('p', { class: 'note' }, text); }

export function chg(v, text) { return h('span', { class: dir(v) }, text); }

// Label/value grid
export function kv(pairs, cols = 2) {
  const grid = h('dl', { class: `kv kv${cols}` });
  for (const [label, value, cls] of pairs) {
    if (label === null) continue;
    grid.append(h('dt', {}, label), h('dd', { class: cls || '' }, value ?? '—'));
  }
  return grid;
}

// Sortable data table.
//   columns: [{ key, label, align: 'r'|'l', fmt(v,row) → string|Node, cls(v,row), sort(row) }]
export function table({ columns, rows, onRow, rowTitle, className = '', sortable = true, empty = 'Nothing to show.' }) {
  const wrap = h('div', { class: 'tbl-wrap' });
  if (!rows || !rows.length) { wrap.append(note(empty)); return wrap; }
  const tbl = h('table', { class: `tbl ${className}` });
  let sortKey = null, sortDir = 1;
  const thead = h('thead', {}, h('tr', {}, columns.map((c) => {
    const th = h('th', { class: c.align === 'r' ? 'r' : '', title: c.title || '' }, c.label);
    if (sortable && c.key) {
      th.classList.add('sortable');
      th.addEventListener('click', () => {
        sortDir = sortKey === c.key ? -sortDir : (c.align === 'r' ? -1 : 1);
        sortKey = c.key;
        const get = c.sort || ((r) => r[c.key]);
        rows = [...rows].sort((a, b) => {
          const x = get(a), y = get(b);
          if (x == null && y == null) return 0;
          if (x == null) return 1;
          if (y == null) return -1;
          return (x > y ? 1 : x < y ? -1 : 0) * sortDir;
        });
        body();
      });
    }
    return th;
  })));
  const tbody = h('tbody');
  const body = () => {
    clear(tbody);
    for (const r of rows) {
      const tr = h('tr', { title: rowTitle ? rowTitle(r) : '' });
      if (onRow) { tr.classList.add('click'); tr.addEventListener('click', (e) => onRow(r, e)); }
      for (const c of columns) {
        const v = c.key ? r[c.key] : undefined;
        const content = c.fmt ? c.fmt(v, r) : v;
        tr.append(h('td', { class: [c.align === 'r' ? 'r' : '', c.cls ? c.cls(v, r) : ''].join(' ').trim() }, content ?? '—'));
      }
      tbody.append(tr);
    }
  };
  body();
  tbl.append(thead, tbody);
  wrap.append(tbl);
  return wrap;
}

// Inline sparkline (SVG). Colored by first→last direction.
export function spark(values, { w = 90, h: ht = 22, cls } = {}) {
  const vals = (values || []).filter((v) => typeof v === 'number' && Number.isFinite(v));
  const NS = 'http://www.w3.org/2000/svg';
  const svg = document.createElementNS(NS, 'svg');
  svg.setAttribute('width', w); svg.setAttribute('height', ht);
  svg.setAttribute('viewBox', `0 0 ${w} ${ht}`);
  svg.setAttribute('class', `spark ${cls || dir(vals.length ? vals[vals.length - 1] - vals[0] : 0)}`);
  svg.setAttribute('aria-hidden', 'true');
  if (vals.length < 2) return svg;
  const min = Math.min(...vals), max = Math.max(...vals), span = max - min || 1;
  const pts = vals.map((v, i) => `${(i / (vals.length - 1) * (w - 2) + 1).toFixed(1)},${(ht - 2 - (v - min) / span * (ht - 4)).toFixed(1)}`);
  const line = document.createElementNS(NS, 'polyline');
  line.setAttribute('points', pts.join(' '));
  svg.append(line);
  return svg;
}

// Diverging bar for values in [-1, 1] (Oracle signals)
export function divBar(v, max = 1) {
  const f = Math.max(-1, Math.min(1, (v || 0) / max));
  const bar = h('span', { class: 'dbar' });
  const fill = h('i', { class: f >= 0 ? 'up' : 'down' });
  fill.style.left = f >= 0 ? '50%' : `${50 + f * 50}%`;
  fill.style.width = `${Math.abs(f) * 50}%`;
  bar.append(fill);
  return bar;
}

// Horizontal share bar for 0..1 values (weights, percentages)
export function shareBar(f, cls = '') {
  const bar = h('span', { class: `sbar ${cls}` });
  const fill = h('i');
  fill.style.width = `${Math.max(0, Math.min(1, f || 0)) * 100}%`;
  bar.append(fill);
  return bar;
}

export function tabs(options, current, onPick) {
  return h('div', { class: 'tabs', role: 'tablist' }, options.map(([value, label]) =>
    h('button', { class: value === current ? 'on' : '', role: 'tab', 'aria-selected': value === current ? 'true' : 'false',
      onclick: () => onPick(value) }, label)));
}
