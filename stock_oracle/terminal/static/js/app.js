// Stock Oracle Terminal — panels, command line, tape and status line.
import { getJSON, postJSON, Bus, Live } from './api.js';
import { parse, suggest, FUNCTIONS } from './commands.js';
import { PANELS } from './panels/index.js';
import { h, clear } from './ui.js';
import { refreshLive, liveSymbols, live } from './livecells.js';
import { clockET, signed } from './format.js';

const STORE_KEY = 'so-terminal-v1';
const SLOTS = 6;
const DEFAULT_STATE = {
  layout: 4, focus: 1, last: null, history: [],
  panels: [
    { fn: 'W', sym: null, args: [], linked: true },
    { fn: 'BRIEF', sym: null, args: [], linked: true },
    { fn: 'GP', sym: null, args: ['6M'], linked: true },
    { fn: 'ORC', sym: null, args: [], linked: true },
    { fn: 'N', sym: null, args: [], linked: true },
    { fn: 'ECO', sym: null, args: [], linked: true },
  ],
};

const scopeOf = (fn) => (FUNCTIONS[fn] ? FUNCTIONS[fn].scope : 'global');

// ── One panel ────────────────────────────────────────────────
class PanelHost {
  constructor(app, idx, st) {
    this.app = app;
    this.idx = idx;
    this.st = st;                 // persisted: {fn, sym, args, linked}
    this.data = null;
    this.seq = 0;
    this.ui = {};                 // per-render scratch state for the panel definition
    this.timer = null;
    this.visible = false;

    this.numEl = h('span', { class: 'num' }, `${idx + 1}`);
    this.keyEl = h('span', { class: 'key' });
    this.symEl = h('span', { class: 'sym' });
    this.titleEl = h('span', { class: 'title' });
    this.linkBtn = h('button', { title: 'Linked: follows the security you load. Click to unlink.', onclick: (e) => { e.stopPropagation(); this.toggleLink(); } }, '⛓');
    this.reloadBtn = h('button', { title: 'Reload', onclick: (e) => { e.stopPropagation(); this.load(true); } }, '↻');
    this.body = h('div', { class: 'pbody' });
    this.el = h('div', { class: 'panel', onmousedown: () => app.setFocus(idx) },
      h('div', { class: 'phead' }, this.numEl, this.keyEl, this.symEl, this.titleEl,
        h('span', { class: 'busy' }, '…'), this.linkBtn, this.reloadBtn),
      this.body);
    this.paintHeader();
  }

  get def() { return PANELS[this.st.fn]; }

  ctx() {
    return {
      sym: this.st.sym, args: this.st.args || [], state: this.ui, app: this.app, store: this.app.live,
      get: getJSON, post: postJSON,
      open: (cmd) => this.app.execute(cmd, this),
      load: (sym) => this.app.loadSecurity(sym, this),
      setArgs: (args) => { this.st.args = args; this.app.save(); this.load(); },
      rerender: () => this.renderData(),
    };
  }

  paintHeader() {
    this.keyEl.textContent = this.st.fn;
    this.symEl.textContent = scopeOf(this.st.fn) === 'security' ? (this.st.sym || '') : '';
    this.titleEl.textContent = FUNCTIONS[this.st.fn]?.title || '';
    this.linkBtn.classList.toggle('linked', !!this.st.linked);
    this.linkBtn.style.opacity = this.st.linked ? '1' : '.35';
  }

  toggleLink() { this.st.linked = !this.st.linked; this.paintHeader(); this.app.save(); }

  set(fn, sym, args) {
    const nextSym = scopeOf(fn) === 'security' ? sym : null;
    const changed = fn !== this.st.fn || nextSym !== this.st.sym;
    this.st.fn = fn;
    this.st.sym = nextSym;
    this.st.args = args || [];
    if (changed) {
      // New function or security: fresh scratch state for the panel definition
      this.dispose();
      this.data = null;
      this.ui = {};
      this.loadedFor = `${fn}|${nextSym}`;
    }
    this.paintHeader();
    this.app.save();
    if (this.visible) this.load();
  }

  show(on) {
    this.visible = on;
    this.el.style.display = on ? '' : 'none';
    if (on) this.load();
    else { this.dispose(); this.app.live.unwatch(this); clearInterval(this.timer); }
  }

  state(kind, message, hint) {
    this.dispose();
    clear(this.body);
    this.body.className = 'pbody';
    this.body.append(h('div', { class: `state ${kind}` }, message, hint ? h('span', { class: 'hint' }, hint) : null));
  }

  async load(force = false) {
    const def = this.def;
    clearInterval(this.timer);
    if (!def) { this.state('err', `Unknown function ${this.st.fn}`); return; }
    if (scopeOf(this.st.fn) === 'security' && !this.st.sym) {
      this.app.live.unwatch(this);
      this.state('', `${FUNCTIONS[this.st.fn].title} — type a ticker, e.g.`, `NVDA ${this.st.fn}`);
      return;
    }
    const seq = ++this.seq;
    const key = `${this.st.fn}|${this.st.sym}|${(this.st.args || []).join(' ')}`;
    if (this.loadedKey !== key) this.state('', 'Loading…');
    this.el.classList.add('loading');
    try {
      const data = await def.load(this.ctx());
      if (seq !== this.seq) return;
      this.data = data;
      this.loadedKey = key;
      this.renderData();
    } catch (e) {
      if (seq !== this.seq) return;
      this.data = null;
      this.loadedKey = null;
      this.state('err', e.message || String(e),
        e.status === 404 ? 'Try another function (HELP), or check the ticker.' : e.status >= 500 ? 'The data source didn\'t answer — ↻ to retry.' : '');
    } finally {
      if (seq === this.seq) this.el.classList.remove('loading');
    }
    if (def.refresh && this.visible) {
      this.timer = setInterval(() => this.visible && this.load(), def.refresh * 1000);
    }
  }

  dispose() {
    if (this.ui && typeof this.ui.dispose === 'function') {
      try { this.ui.dispose(); } catch { /* ignore */ }
    }
    if (this.ui) { this.ui.dispose = null; this.ui.live = null; }
  }

  renderData() {
    if (!this.data) return;
    const scroll = this.body.scrollTop;
    this.dispose();
    if (this.loadedFor !== `${this.st.fn}|${this.st.sym}`) {
      this.ui = {};
      this.loadedFor = `${this.st.fn}|${this.st.sym}`;
    }
    clear(this.body);
    this.body.className = `pbody${this.def.flush ? ' flush' : ''}`;
    try {
      this.def.render(this.ctx(), this.data, this.body);
    } catch (e) {
      console.error(e);
      this.state('err', `Display error: ${e.message}`);
      return;
    }
    this.body.scrollTop = scroll;
    const syms = new Set(liveSymbols(this.body));
    if (this.def.watch) for (const s of this.def.watch(this.ctx())) syms.add(s);
    this.app.live.watch(this, [...syms]);
  }

  onQuotes(changed) {
    if (!this.visible || !this.data) return;
    refreshLive(this.body, this.app.live, changed);
    if (this.def.onQuotes) this.def.onQuotes(this.ctx(), this.data, this.body, changed);
  }

  onEvent(name, msg) {
    if (!this.visible || !this.def || !this.def.events) return;
    const rule = this.def.events[name];
    const action = typeof rule === 'function' ? rule(this.ctx(), msg) : rule;
    if (action === 'reload') this.load();
  }

  describe() { return { fn: this.st.fn, sym: scopeOf(this.st.fn) === 'security' ? this.st.sym : null }; }

  summary() {
    if (!this.data || !this.def.text) return '';
    try { return this.def.text(this.ctx(), this.data) || ''; } catch { return ''; }
  }
}

// ── The app ──────────────────────────────────────────────────
class App {
  constructor() {
    this.bus = new Bus();
    this.live = new Live(this.bus);
    this.s = this.restore();
    this.grid = document.getElementById('grid');
    this.cmd = document.getElementById('cmd');
    this.sugEl = document.getElementById('suggest');
    this.msgEl = document.getElementById('msg');
    this.hosts = this.s.panels.map((st, i) => new PanelHost(this, i, st));
    for (const host of this.hosts) this.grid.append(host.el);
    this.histPos = -1;
    this.sugs = [];
    this.sugIdx = -1;
  }

  restore() {
    try {
      const saved = JSON.parse(localStorage.getItem(STORE_KEY) || 'null');
      if (saved && Array.isArray(saved.panels) && saved.panels.length === SLOTS) return saved;
    } catch { /* storage unavailable */ }
    return structuredClone(DEFAULT_STATE);
  }

  save() {
    try { localStorage.setItem(STORE_KEY, JSON.stringify(this.s)); } catch { /* ignore */ }
  }

  async start() {
    this.wireCommandLine();
    this.wireKeys();
    this.bus.on('quotes', (changed) => {
      for (const host of this.hosts) host.onQuotes(changed);
      refreshLive(document.getElementById('tape'), this.live, changed);
    });
    for (const evt of ['oracle', 'scan', 'watchlist']) this.bus.on(evt, (m) => this.hosts.forEach((p) => p.onEvent(evt, m)));
    this.bus.on('status', (m) => this.paintStatus(m));
    this.bus.on('ws', (m) => this.paintWs(m));
    this.bus.on('oracle', (m) => {
      if (m.state === 'done' && m.summary) {
        this.message(`Oracle ${m.s}: ${m.summary.prediction} ${signed(m.summary.signal, 3)} · confidence ${Math.round((m.summary.confidence || 0) * 100)}%`);
      } else if (m.state === 'error') this.message(`Oracle ${m.s} failed: ${m.error}`, true);
    });
    this.bus.on('scan', (m) => { if (!m.active && m.total) this.message(`Oracle scan finished (${m.done}/${m.total})`); });
    setInterval(() => { document.getElementById('clock-t').textContent = clockET(); }, 1000);
    this.live.connect();

    // First visit: point the security panels at the first watchlist name
    if (!this.s.last) {
      try {
        const wl = await getJSON('/api/watchlist');
        this.s.last = wl.tickers.find((t) => !/^\^|=|-USD$/.test(t)) || 'SPY';
        for (const host of this.hosts) if (scopeOf(host.st.fn) === 'security' && !host.st.sym) host.st.sym = this.s.last;
        this.save();
      } catch { this.s.last = 'SPY'; }
    }
    for (const host of this.hosts) host.paintHeader();
    this.setLayout(this.s.layout, false);
    this.setFocus(Math.min(this.s.focus, this.s.layout - 1));
    this.buildTape();
  }

  // ── layout & focus ─────────────────────────────────────────
  setLayout(n, announce = true) {
    this.s.layout = n;
    this.grid.className = `lay-${n}`;
    this.hosts.forEach((host, i) => {
      const want = i < n;
      if (want !== host.visible) host.show(want);
      else host.el.style.display = want ? '' : 'none';
    });
    if (this.s.focus >= n) this.setFocus(0);
    this.save();
    if (announce) this.message(`Layout ${n}`);
  }

  setFocus(i) {
    this.s.focus = i;
    this.hosts.forEach((host, k) => host.el.classList.toggle('focus', k === i));
    this.save();
  }

  get focused() { return this.hosts[this.s.focus]; }

  focusSymbol() {
    const f = this.focused;
    return (scopeOf(f.st.fn) === 'security' && f.st.sym) || this.s.last || null;
  }

  visibleHosts() { return this.hosts.filter((host) => host.visible); }

  screenSummary() {
    const vis = this.visibleHosts();
    return {
      panels: vis.map((p) => p.describe()),
      text: vis.filter((p) => p.st.fn !== 'ASK').map((p) => {
        const s = p.summary();
        return s ? `[${p.st.fn}${p.st.sym ? ` ${p.st.sym}` : ''}] ${s}` : '';
      }).filter(Boolean).join('\n'),
    };
  }

  // ── security loading ───────────────────────────────────────
  async loadSecurity(sym, source = null) {
    // Unfamiliar ticker? Check it exists first so a typo or a company name
    // ("nvidia") turns into a name search instead of six empty panels.
    if (!this.live.get(sym)) {
      this.message(`Looking up ${sym}…`);
      try {
        await getJSON(`/api/quote/${encodeURIComponent(sym)}`);
      } catch (e) {
        if (e.status === 404 || e.status === 400) {
          this.message(`No security "${sym}" — searching by name`, true);
          (source && source.visible ? source : this.focused).set('SECF', null, [sym]);
          return;
        }
      }
    }
    this.s.last = sym;
    const targets = new Set();
    const f = this.focused;
    if (f !== source && scopeOf(f.st.fn) === 'security') targets.add(f);
    const linkFrom = source || f;
    if (linkFrom.st.linked) {
      for (const host of this.visibleHosts()) {
        if (host !== source && host.st.linked && scopeOf(host.st.fn) === 'security') targets.add(host);
      }
    }
    if (!targets.size) {
      const spare = this.visibleHosts().find((host) => host !== source) || f;
      spare.set('BRIEF', sym, []);
    } else {
      for (const host of targets) host.set(host.st.fn, sym, host.st.fn === 'GP' || host.st.fn === 'HP' ? host.st.args : []);
    }
    this.save();
    this.message(`${sym} loaded`);
  }

  // ── commands ───────────────────────────────────────────────
  async execute(input, source = null) {
    const a = parse(input);
    const target = source || this.focused;
    switch (a.type) {
      case 'none': return;
      case 'error': this.message(a.message, true); return;
      case 'load': this.loadSecurity(a.sym, null); return;
      case 'layout': this.setLayout(a.n); return;
      case 'search': target.set('SECF', null, a.query.split(/\s+/)); return;
      case 'ask': return this.ask(a.question);
      case 'run': return this.runOracle(a.sym || this.focusSymbol());
      case 'scan': {
        try {
          const r = await postJSON('/api/scan', {});
          this.message(`Oracle scan started: ${r.total} tickers (fast mode — skips the slow local-LLM collectors)`);
        } catch (e) { this.message(e.message, true); }
        return;
      }
      case 'watchlist': {
        try {
          const r = await postJSON('/api/watchlist', { [a.op]: a.symbols });
          this.message(`${a.op === 'add' ? 'Added' : 'Removed'} ${a.symbols.join(', ')} — watchlist has ${r.tickers.length}`);
          this.bus.emit('watchlist', r);
        } catch (e) { this.message(e.message, true); }
        return;
      }
      case 'fn': {
        let fn = a.fn;
        if (scopeOf(fn) === 'security') {
          const sym = a.sym || (scopeOf(target.st.fn) === 'security' && target.st.sym) || this.s.last;
          if (!sym) {
            if (fn === 'N') { target.set('TOP', null, []); return; }
            this.message(`Type a ticker first, e.g. NVDA ${fn}`, true); return;
          }
          target.set(fn, sym, a.args);
          if (a.sym) {
            this.s.last = a.sym;
            if (target.st.linked) {
              for (const host of this.visibleHosts()) {
                if (host !== target && host.st.linked && scopeOf(host.st.fn) === 'security') {
                  host.set(host.st.fn, a.sym, host.st.fn === 'GP' || host.st.fn === 'HP' ? host.st.args : []);
                }
              }
            }
          }
        } else {
          target.set(fn, null, a.args);
        }
        this.save();
      }
    }
  }

  async runOracle(sym) {
    if (!sym) { this.message('Load a ticker first, then RUN', true); return; }
    try {
      await postJSON(`/api/oracle/${encodeURIComponent(sym)}/run`);
      this.message(`Oracle queued for ${sym} — full analysis takes 10-60 seconds`);
      const showing = this.visibleHosts().some((p) => p.st.fn === 'ORC' && p.st.sym === sym);
      if (!showing) {
        const orc = this.visibleHosts().find((p) => p.st.fn === 'ORC');
        (orc || this.focused).set('ORC', sym, []);
      }
    } catch (e) { this.message(e.message, true); }
  }

  ask(question) {
    let host = this.visibleHosts().find((p) => p.st.fn === 'ASK');
    if (!host) { host = this.focused; host.set('ASK', null, []); }
    PANELS.ASK.submit(host.ctx(), question);
  }

  // ── command line ───────────────────────────────────────────
  wireCommandLine() {
    const cmd = this.cmd;
    const run = () => {
      const text = cmd.value.trim();
      this.closeSuggest();
      if (!text) return;
      this.s.history = [text, ...(this.s.history || []).filter((x) => x !== text)].slice(0, 50);
      this.histPos = -1;
      cmd.value = '';
      this.execute(text);
    };
    document.getElementById('go').addEventListener('click', run);
    cmd.addEventListener('input', () => this.openSuggest(cmd.value));
    cmd.addEventListener('blur', () => setTimeout(() => this.closeSuggest(), 150));
    cmd.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') {
        e.preventDefault();
        if (this.sugIdx >= 0 && this.sugs[this.sugIdx]) cmd.value = this.sugs[this.sugIdx].insert;
        run();
      } else if (e.key === 'Escape') {
        cmd.value = ''; this.closeSuggest(); cmd.blur();
      } else if (e.key === 'Tab') {
        if (this.sugs.length) {
          e.preventDefault();
          cmd.value = `${this.sugs[Math.max(0, this.sugIdx)].insert} `;
          this.openSuggest(cmd.value);
        }
      } else if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
        e.preventDefault();
        const down = e.key === 'ArrowDown';
        if (this.sugs.length) {
          this.sugIdx = (this.sugIdx + (down ? 1 : -1) + this.sugs.length) % this.sugs.length;
          this.paintSuggest();
        } else {
          const hist = this.s.history || [];
          this.histPos = Math.max(-1, Math.min(hist.length - 1, this.histPos + (down ? -1 : 1)));
          cmd.value = this.histPos >= 0 ? hist[this.histPos] : '';
        }
      }
    });
  }

  openSuggest(text) {
    this.sugs = /^(ask|\?\?)\s/i.test(text) ? [] : suggest(text);
    this.sugIdx = -1;
    this.paintSuggest();
  }

  paintSuggest() {
    clear(this.sugEl);
    this.sugEl.classList.toggle('open', this.sugs.length > 0);
    this.sugs.forEach((s, i) => {
      this.sugEl.append(h('li', { class: i === this.sugIdx ? 'sel' : '', onmousedown: (e) => {
        e.preventDefault(); this.cmd.value = s.insert; this.closeSuggest(); this.cmd.focus();
        this.execute(s.insert); this.cmd.value = '';
      } }, h('span', { class: 'key' }, s.code), h('span', {}, s.insert), h('span', { class: 't' }, s.title)));
    });
  }

  closeSuggest() { this.sugs = []; this.sugIdx = -1; this.paintSuggest(); }

  wireKeys() {
    document.addEventListener('keydown', (e) => {
      const tag = (e.target.tagName || '').toLowerCase();
      if (e.altKey && /^[1-6]$/.test(e.key)) {
        const i = Number(e.key) - 1;
        if (i < this.s.layout) { e.preventDefault(); this.setFocus(i); }
        return;
      }
      if (tag === 'input' || tag === 'textarea' || tag === 'select') return;
      if (e.ctrlKey || e.metaKey || e.altKey) return;
      if (e.key.length === 1 && e.key !== ' ') this.cmd.focus();   // typing anywhere goes to the command line
    });
  }

  // ── tape & status ──────────────────────────────────────────
  async buildTape() {
    const tape = document.getElementById('tape');
    try {
      const st = await getJSON('/api/status');
      clear(tape);
      for (const t of st.tape) {
        tape.append(h('span', { class: 'item', title: t.s, onclick: () => this.loadSecurity(t.s) },
          h('span', { class: 'lbl' }, t.label), live(this.live, t.s, 'p'), ' ', live(this.live, t.s, 'cp')));
      }
      this.live.watch('tape', st.tape.map((t) => t.s));
      this.paintStatus(st);
    } catch (e) {
      this.message(`Server unreachable: ${e.message}`, true);
    }
  }

  paintWs(m) {
    if (!m.connected) {
      document.getElementById('st-stream').textContent = m.code === 4401 ? 'not authorized — reopen the URL with ?token=' : 'reconnecting…';
      document.getElementById('st-dot').className = 'dot off';
    }
  }

  paintStatus(m) {
    const dot = document.getElementById('st-dot');
    const stream = document.getElementById('st-stream');
    if (m.stream && m.stream !== 'poll') {
      dot.className = `dot ${m.connected ? 'live' : 'poll'}`;
      stream.textContent = m.connected
        ? `LIVE ${m.stream} · ${m.streaming} symbols · ${m.tps} ticks/s`
        : `${m.stream} connecting…`;
    } else {
      dot.className = 'dot poll';
      stream.textContent = `Yahoo refresh every ${Math.round(m.poll_seconds || 15)}s${m.keys && !m.keys.finnhub ? ' (add a Finnhub key for live ticks)' : ''}`;
    }
    document.getElementById('st-session').textContent = m.session_detail || '';
    const k = m.keys || {};
    document.getElementById('st-keys').textContent =
      `Finnhub ${k.finnhub ? '✓' : '–'}  Alpaca ${k.alpaca ? '✓' : '–'}  Claude ${k.anthropic ? '✓' : '–'}`;
  }

  message(text, isErr = false) {
    this.msgEl.textContent = text;
    this.msgEl.className = isErr ? 'err' : '';
    clearTimeout(this._msgT);
    this._msgT = setTimeout(() => { this.msgEl.textContent = ''; }, isErr ? 12000 : 6000);
  }
}

const app = new App();
window.soTerminal = app;     // handy from the browser console
app.start();
