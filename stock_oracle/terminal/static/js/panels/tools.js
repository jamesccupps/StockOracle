// ASK (Claude with live data), SECF (find a security), HELP
import { h, table, sec, note, clear } from '../ui.js';
import { num } from '../format.js';
import { FUNCTIONS, ALIASES, ACTIONS } from '../commands.js';

// ── ASK ──────────────────────────────────────────────────────
export const ASK = {
  load: async () => ({}),
  render(c, d, el) {
    const st = c.state;
    st.log = st.log || [];
    const logEl = h('div', { class: 'ask-log' });
    const paint = () => {
      clear(logEl);
      if (!st.log.length) {
        logEl.append(note('Ask about anything on screen or in your watchlist. Each question is sent with a live briefing on the focused security (price, Oracle verdict, estimates, insiders, short data, filings, news) plus the macro backdrop, so answers cite current data.'),
          note('Examples:  why is Oracle bearish on LUNR?  ·  what changed for NVDA this week?  ·  which of my ETFs has the best dividend growth?'));
      }
      for (const e of st.log) {
        logEl.append(h('div', {},
          h('div', { class: 'ask-q' }, `› ${e.q}`, e.sym ? h('span', { class: 'dim' }, `  [${e.sym}]`) : null),
          e.pending ? h('div', { class: 'dim' }, 'Gathering data and asking Claude…')
            : e.err ? h('div', { class: 'state err' }, e.err)
              : h('div', { class: 'ask-a' }, e.a),
          e.status && e.status.spent != null ? h('div', { class: 'faint' },
            `Spent $${num(e.status.spent, 4)} of $${num(e.status.cap, 2)} this month · ${e.status.model || ''}`) : null));
      }
      logEl.scrollIntoView({ block: 'end' });
    };
    const ta = h('textarea', { placeholder: `Question about ${c.app.focusSymbol() || 'the market'}… (Enter to send, Shift+Enter for a new line)`, rows: 2 });
    ta.addEventListener('keydown', (e) => {
      if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); send(); }
      e.stopPropagation();
    });
    const send = () => { const q = ta.value.trim(); if (q) { ta.value = ''; ASK.submit(c, q); } };
    st.paint = paint;
    el.append(logEl, h('div', { class: 'ask-form' }, ta, h('button', { class: 'btn', onclick: send }, 'Ask')));
    paint();
    if (st.queued) { const q = st.queued; st.queued = null; ASK.submit(c, q); }
  },
  async submit(c, question) {
    const st = c.state;
    st.log = st.log || [];
    if (!st.paint) { st.queued = question; return; }
    const sym = c.app.focusSymbol();
    const entry = { q: question, sym, pending: true };
    st.log.push(entry);
    st.paint();
    try {
      const res = await c.post('/api/ask', { question, symbol: sym, screen: c.app.screenSummary() });
      entry.a = res.answer;
      entry.status = res.status;
    } catch (e) {
      entry.err = e.message;
    }
    entry.pending = false;
    if (st.paint) st.paint();
  },
  text: () => '',
};

// ── SECF ─────────────────────────────────────────────────────
export const SECF = {
  load: (c) => (c.args.length ? c.get(`/api/search?q=${encodeURIComponent(c.args.join(' '))}`) : Promise.resolve({ results: null })),
  render(c, d, el) {
    const inp = h('input', { class: 'inp', value: c.args.join(' '), placeholder: 'Company or fund name', style: { width: '28em' } });
    inp.addEventListener('keydown', (e) => {
      e.stopPropagation();
      if (e.key === 'Enter' && inp.value.trim()) c.setArgs(inp.value.trim().split(/\s+/));
    });
    el.append(h('div', { class: 'lead' }, inp, h('button', { class: 'btn', onclick: () => inp.value.trim() && c.setArgs(inp.value.trim().split(/\s+/)) }, 'Search')));
    if (d.results) {
      el.append(table({ rows: d.results, onRow: (r) => c.load(r.symbol), columns: [
        { key: 'symbol', label: 'Symbol', cls: () => 'tk' }, { key: 'name', label: 'Name' },
        { key: 'type', label: 'Type', cls: () => 'dim' }, { key: 'exchange', label: 'Exchange', cls: () => 'dim' },
      ], empty: 'No matches.' }));
    }
    setTimeout(() => inp.focus(), 0);
  },
};

// ── HELP ─────────────────────────────────────────────────────
const GROUPS = [
  ['Security — type a ticker first, e.g. NVDA BRIEF', ['BRIEF', 'GP', 'DES', 'ORC', 'N', 'FA', 'EEO', 'ANR', 'EE', 'INS', 'HDS', 'CF', 'SI', 'DVD', 'HOLD', 'OMON', 'HP', 'RV']],
  ['Market', ['W', 'TOP', 'ECO', 'GC', 'MOST', 'EVTS', 'WEI', 'SECT', 'GOVT', 'FX', 'CMDTY', 'CRYPTO']],
  ['Stock Oracle', ['ORC', 'REG', 'BRK', 'ACC']],
  ['Tools', ['ASK', 'SECF', 'HELP']],
];

export const HELP = {
  load: async () => ({}),
  render(c, d, el) {
    const filter = (c.args[0] || '').toUpperCase();
    const aliasOf = {};
    for (const [a, code] of Object.entries(ALIASES)) (aliasOf[code] = aliasOf[code] || []).push(a);
    const rows = [];
    for (const [title, codes] of GROUPS) {
      const list = codes.filter((k) => !filter || k.startsWith(filter) || (aliasOf[k] || []).includes(filter));
      if (!list.length) continue;
      rows.push({ group: title });
      for (const k of list) rows.push({ code: k, title: FUNCTIONS[k].title, args: FUNCTIONS[k].args || '', aliases: (aliasOf[k] || []).join(' ') });
    }
    el.append(
      sec('How to type', h('div', {},
        h('p', {}, h('span', { class: 'amber' }, 'NVDA'), ' loads a security into the focused panel and every linked panel (', h('span', { class: 'amber' }, '⛓'), ' in the header).'),
        h('p', {}, h('span', { class: 'amber' }, 'NVDA GP 1Y'), ' opens a function for a ticker; ', h('span', { class: 'amber' }, 'GP 5D'), ' uses the panel\'s current ticker; ',
          h('span', { class: 'amber' }, 'NVDA 1Y'), ' is shorthand for a chart.'),
        h('p', {}, h('span', { class: 'amber' }, 'ASK your question'), ' asks Claude with live data attached. ',
          h('span', { class: 'amber' }, 'W ADD AMD'), ' / ', h('span', { class: 'amber' }, 'W DEL AMD'), ' edit the watchlist (shared with the desktop app). ',
          h('span', { class: 'amber' }, 'LAY 6'), ' switches to six panels.'),
        h('p', {}, 'Use ', h('span', { class: 'amber' }, '$W'), ' or ', h('span', { class: 'amber' }, 'W US'), ' for a ticker that matches a function code. Anything else searches by name.'),
        h('p', { class: 'dim' }, 'Keys: start typing anywhere to reach the command line · Enter runs it · Tab completes · ↑/↓ history · Esc clears · Alt+1…6 focus a panel · click a row to load that security.'))),
      table({ sortable: false, rows, onRow: (r) => r.code && c.open(r.code), columns: [
        { key: 'code', label: 'Function', fmt: (v, r) => (r.group ? h('b', { class: 'amber' }, r.group) : h('span', { class: 'key' }, v)) },
        { key: 'title', label: '', fmt: (v, r) => (r.group ? '' : v) },
        { key: 'args', label: 'Options', cls: () => 'dim' },
        { key: 'aliases', label: 'Also', cls: () => 'faint' },
      ] }),
      sec('Actions', table({ sortable: false, rows: Object.entries(ACTIONS).map(([k, v]) => ({ k, v })), columns: [
        { key: 'k', label: '', fmt: (v) => h('span', { class: 'key' }, v) }, { key: 'v', label: '' }] })),
    );
  },
};
