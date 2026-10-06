// REST helpers, an event bus, and the live-quote websocket client.

export async function getJSON(path) {
  const r = await fetch(path, { headers: { Accept: 'application/json' } });
  return handle(r);
}

export async function postJSON(path, body = {}) {
  const r = await fetch(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
    body: JSON.stringify(body),
  });
  return handle(r);
}

async function handle(r) {
  let data = null;
  try { data = await r.json(); } catch { /* non-JSON error page */ }
  if (!r.ok) {
    const err = new Error((data && data.detail) || `HTTP ${r.status}`);
    err.status = r.status;
    throw err;
  }
  return data;
}

export class Bus {
  constructor() { this.handlers = new Map(); }
  on(evt, fn) {
    if (!this.handlers.has(evt)) this.handlers.set(evt, new Set());
    this.handlers.get(evt).add(fn);
    return () => this.handlers.get(evt).delete(fn);
  }
  emit(evt, data) {
    for (const fn of this.handlers.get(evt) || []) {
      try { fn(data); } catch (e) { console.error(`handler for ${evt}`, e); }
    }
  }
}

// Live quotes. Panels call watch(owner, symbols); the union of everything
// watched is sent to the server, which pushes {"t":"quotes"} batches back.
export class Live {
  constructor(bus) {
    this.bus = bus;
    this.quotes = new Map();
    this.owners = new Map();
    this.ws = null;
    this.backoff = 1000;
    this.connected = false;
    this._subTimer = null;
    this._lastSent = '';
  }

  connect() {
    const proto = location.protocol === 'https:' ? 'wss' : 'ws';
    const ws = new WebSocket(`${proto}://${location.host}/ws`);
    this.ws = ws;
    ws.onopen = () => {
      this.connected = true;
      this.backoff = 1000;
      this._lastSent = '';
      this._sendSubs();
      this.bus.emit('ws', { connected: true });
    };
    ws.onmessage = (ev) => {
      let msg;
      try { msg = JSON.parse(ev.data); } catch { return; }
      if (msg.t === 'quotes') {
        const changed = [];
        for (const q of msg.d) {
          const prev = this.quotes.get(q.s);
          q.prevPrice = prev ? prev.p : undefined;
          this.quotes.set(q.s, q);
          changed.push(q.s);
        }
        this.bus.emit('quotes', changed);
      } else if (msg.t) {
        this.bus.emit(msg.t, msg);
      }
    };
    ws.onclose = (ev) => {
      this.connected = false;
      this.bus.emit('ws', { connected: false, code: ev.code });
      if (ev.code === 4401) return;          // bad token: don't hammer the server
      setTimeout(() => this.connect(), this.backoff);
      this.backoff = Math.min(this.backoff * 2, 15000);
    };
    ws.onerror = () => { try { ws.close(); } catch { /* ignore */ } };
    clearInterval(this._ping);
    this._ping = setInterval(() => {
      if (this.connected) ws.send(JSON.stringify({ op: 'ping' }));
    }, 25000);
  }

  watch(owner, symbols) {
    this.owners.set(owner, new Set((symbols || []).filter(Boolean)));
    this._scheduleSubs();
  }

  unwatch(owner) {
    this.owners.delete(owner);
    this._scheduleSubs();
  }

  get(sym) { return this.quotes.get(sym); }

  _scheduleSubs() {
    clearTimeout(this._subTimer);
    this._subTimer = setTimeout(() => this._sendSubs(), 120);
  }

  _sendSubs() {
    if (!this.connected) return;
    const all = new Set();
    for (const set of this.owners.values()) for (const s of set) all.add(s);
    const list = [...all].sort();
    const key = list.join(',');
    if (key === this._lastSent) return;
    this._lastSent = key;
    this.ws.send(JSON.stringify({ op: 'sub', symbols: list }));
  }
}
