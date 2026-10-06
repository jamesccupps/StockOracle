// Command language — pure functions, no DOM (unit-testable in Node).
//
//   NVDA            load NVDA into the focused panel and every linked panel
//   NVDA GP 1Y      price graph, one year
//   GP 5D           price graph for the panel's current security
//   NVDA 1Y         shorthand for NVDA GP 1Y
//   ORC  RUN  SCAN  Oracle analysis / run it now / analyze the whole watchlist
//   WEI SECT GOVT FX CMDTY CRYPTO   market monitors
//   W   W ADD AMD PLTR   W DEL PLUG
//   ASK why is NVDA bearish?
//   LAY 4           layout with 1, 2, 3, 4 or 6 panels
//   $W  or  W US    the ticker W, when it collides with a function code
//   nvidia          anything that isn't a ticker or function searches by name

export const FUNCTIONS = {
  // Security functions
  BRIEF:  { title: 'Briefing — everything on one page', scope: 'security' },
  GP:     { title: 'Price graph',              scope: 'security', args: '1D 5D 1M 3M 6M YTD 1Y 2Y 5Y 10Y MAX' },
  DES:    { title: 'Description & key stats',  scope: 'security' },
  ORC:    { title: 'Oracle analysis',          scope: 'security' },
  N:      { title: 'Company news',             scope: 'security' },
  FA:     { title: 'Financial statements',     scope: 'security', args: 'A Q' },
  EEO:    { title: 'Estimates & revisions',    scope: 'security' },
  ANR:    { title: 'Analyst ratings',          scope: 'security' },
  EE:     { title: 'Earnings history',         scope: 'security' },
  INS:    { title: 'Insider transactions',     scope: 'security' },
  HDS:    { title: 'Institutional holders',    scope: 'security' },
  CF:     { title: 'SEC filings',              scope: 'security', args: 'ALL EVENT PERIODIC INSIDER OWNERSHIP DILUTION OTHER' },
  SI:     { title: 'Short interest & volume',  scope: 'security' },
  DVD:    { title: 'Dividends & splits',       scope: 'security' },
  HOLD:   { title: 'ETF holdings',             scope: 'security' },
  OMON:   { title: 'Option chain',             scope: 'security' },
  HP:     { title: 'Historical prices',        scope: 'security', args: '1M 3M 6M 1Y 5Y' },
  RV:     { title: 'Peers (relative value)',   scope: 'security' },
  // Market functions
  W:      { title: 'Watchlist monitor',        scope: 'global' },
  TOP:    { title: 'Top market news',          scope: 'global' },
  ECO:    { title: 'Macro dashboard (FRED)',   scope: 'global' },
  GC:     { title: 'Treasury yield curve',     scope: 'global' },
  MOST:   { title: 'Market movers',            scope: 'global', args: 'GAINERS LOSERS ACTIVE SHORTED SMALLCAP VALUE GROWTH' },
  EVTS:   { title: 'Watchlist earnings calendar', scope: 'global' },
  WEI:    { title: 'World equity indices',     scope: 'global', market: true },
  SECT:   { title: 'US sectors',               scope: 'global', market: true },
  GOVT:   { title: 'Treasury yields (live)',   scope: 'global', market: true },
  FX:     { title: 'Currencies',               scope: 'global', market: true },
  CMDTY:  { title: 'Commodities',              scope: 'global', market: true },
  CRYPTO: { title: 'Crypto',                   scope: 'global', market: true },
  REG:    { title: 'Market regime (Oracle)',   scope: 'global' },
  BRK:    { title: 'Breakout scanner (Oracle)', scope: 'global' },
  ACC:    { title: 'Oracle accuracy',          scope: 'global' },
  ASK:    { title: 'Ask Claude (with live data)', scope: 'global' },
  SECF:   { title: 'Find a security by name',  scope: 'global' },
  HELP:   { title: 'Functions & keys',         scope: 'global' },
};

// Actions that are typed like functions but don't open a panel
export const ACTIONS = {
  RUN:  'Run Oracle on the current security now',
  SCAN: 'Run Oracle across the watchlist (fast mode)',
  LAY:  'Layout: LAY 1 | 2 | 3 | 4 | 6',
};

export const ALIASES = {
  BR: 'BRIEF', BQ: 'BRIEF', DOSSIER: 'BRIEF', ALL: 'BRIEF',
  G: 'GP', GIP: 'GP', CHART: 'GP', GRAPH: 'GP',
  Q: 'DES', INFO: 'DES', PROFILE: 'DES',
  ORACLE: 'ORC', SIG: 'ORC',
  CN: 'N', NEWS: 'N', NI: 'TOP',
  FIN: 'FA', EST: 'EEO', EM: 'EEO', ESTIMATES: 'EEO', REV: 'EEO',
  ERN: 'EE', EARN: 'EE', OPT: 'OMON', OPTIONS: 'OMON',
  INSIDER: 'INS', INSIDERS: 'INS', FORM4: 'INS',
  OWN: 'HDS', HOLDERS: 'HDS', INST: 'HDS',
  FILINGS: 'CF', SEC: 'CF', EDGAR: 'CF',
  SHORT: 'SI', SVOL: 'SI', DIV: 'DVD', DIVS: 'DVD', SPLITS: 'DVD',
  ETF: 'HOLD', MEMB: 'HOLD', MEMBERS: 'HOLD',
  PEER: 'RV', PEERS: 'RV', COMP: 'RV', HIST: 'HP',
  WL: 'W', MON: 'W', MKT: 'WEI', INDEX: 'WEI', IMAP: 'SECT', SECTORS: 'SECT',
  MACRO: 'ECO', ECST: 'ECO', ECON: 'ECO',
  CURVE: 'GC', YCRV: 'GC', WB: 'GOVT', BTMM: 'GOVT', RATES: 'GOVT',
  MOVERS: 'MOST', ACTV: 'MOST', CAL: 'EVTS', ECAL: 'EVTS', CALENDAR: 'EVTS',
  FXC: 'FX', WCRS: 'FX', GLCO: 'CMDTY', CMD: 'CMDTY', CRYP: 'CRYPTO', XBTC: 'CRYPTO',
  REGIME: 'REG', BREAKOUT: 'BRK', BO: 'BRK', ACCURACY: 'ACC',
  '?': 'HELP', H: 'HELP',
};

export const RANGES = ['1D', '5D', '1M', '3M', '6M', 'YTD', '1Y', '2Y', '5Y', '10Y', 'MAX'];

const TICKER = /^\^?[A-Z0-9][A-Z0-9.\-=]{0,14}$/;
const NOT_TICKER_FIRST = new Set(['HELP', 'ASK', 'SECF', 'LAY', 'SCAN', 'RUN']);

export function resolveFunction(tok) {
  const t = (tok || '').toUpperCase();
  if (FUNCTIONS[t]) return t;
  if (ALIASES[t]) return ALIASES[t];
  return null;
}

// Mirror of symbols.normalize() on the server
export function normalizeSymbol(raw) {
  let s = (raw || '').trim().toUpperCase();
  if (s.startsWith('$')) s = s.slice(1);
  for (const suf of [' US EQUITY', ' EQUITY', ' US']) {
    if (s.endsWith(suf)) { s = s.slice(0, -suf.length); break; }
  }
  s = s.trim();
  const m = s.match(/^([A-Z]{1,5})[./]([A-Z])$/);
  if (m) s = `${m[1]}-${m[2]}`;
  return s;
}

export function looksLikeTicker(tok) {
  return TICKER.test(normalizeSymbol(tok));
}

function isRange(tok) { return RANGES.includes((tok || '').toUpperCase()); }

// "FA Q", "CF DILUTION", "GP 1Y": the word is an option of the function, not a ticker
function isOwnArg(fn, tok) {
  return isRange(tok) || (FUNCTIONS[fn]?.args || '').split(' ').includes((tok || '').toUpperCase());
}

// Parse one line of input into an action object.
export function parse(input) {
  const raw = (input || '').trim();
  if (!raw) return { type: 'none' };

  // ASK keeps the user's casing
  const askMatch = raw.match(/^(ask|\?\?)\s+([\s\S]+)$/i);
  if (askMatch) return { type: 'ask', question: askMatch[2].trim() };
  if (/^ask$/i.test(raw)) return { type: 'fn', fn: 'ASK', sym: null, args: [] };

  let tokens = raw.toUpperCase().split(/\s+/);
  let forcedTicker = tokens[0].startsWith('$');

  // "AAPL US EQUITY GP" -> "AAPL GP" (the suffix also marks it as a ticker: "W US")
  if (tokens.length > 1 && tokens[1] === 'US') {
    tokens = [tokens[0], ...tokens.slice(tokens[2] === 'EQUITY' ? 3 : 2)];
    forcedTicker = true;
  } else if (tokens.length > 1 && tokens[1] === 'EQUITY') {
    tokens = [tokens[0], ...tokens.slice(2)];
    forcedTicker = true;
  }
  const [first, ...rest] = tokens;
  const firstFn = forcedTicker ? null : resolveFunction(first);

  // Actions
  if (!forcedTicker) {
    if (first === 'LAY' || first === 'LAYOUT') {
      const n = parseInt(rest[0], 10);
      if ([1, 2, 3, 4, 6].includes(n)) return { type: 'layout', n };
      return { type: 'error', message: 'Layout takes 1, 2, 3, 4 or 6 — e.g. LAY 4' };
    }
    if (first === 'RUN') return { type: 'run', sym: rest[0] ? normalizeSymbol(rest[0]) : null };
    if (first === 'SCAN') return { type: 'scan' };
    if (first === 'SECF' || first === 'FIND') {
      const q = raw.split(/\s+/).slice(1).join(' ');
      return q ? { type: 'search', query: q } : { type: 'fn', fn: 'SECF', sym: null, args: [] };
    }
    if (firstFn === 'W' && rest.length && ['ADD', 'DEL', 'RM', 'REMOVE', '+', '-'].includes(rest[0])) {
      const op = ['ADD', '+'].includes(rest[0]) ? 'add' : 'remove';
      const syms = rest.slice(1).map(normalizeSymbol).filter(s => TICKER.test(s));
      if (!syms.length) return { type: 'error', message: `W ${rest[0]} needs at least one ticker` };
      return { type: 'watchlist', op, symbols: syms };
    }
  }

  // TICKER FUNC [args] — checked before FUNC [args] so "N GP" means ticker N
  if (rest.length && looksLikeTicker(first) && !NOT_TICKER_FIRST.has(first)) {
    const fn = resolveFunction(rest[0]);
    if (fn && fn !== 'HELP' && !(firstFn && !forcedTicker && isOwnArg(firstFn, rest[0]))) {
      const sym = normalizeSymbol(first);
      if (rest[0] === 'GIP' && !rest[1]) return { type: 'fn', fn: 'GP', sym, args: ['1D'] };
      if (rest[1] === 'RUN' && fn === 'ORC') return { type: 'run', sym };
      return { type: 'fn', fn, sym, args: rest.slice(1) };
    }
  }

  // FUNC [args]
  if (firstFn) {
    if (first === 'GIP' && !rest.length) return { type: 'fn', fn: 'GP', sym: null, args: ['1D'] };
    if (firstFn === 'ORC' && rest[0] === 'RUN') return { type: 'run', sym: null };
    if (firstFn === 'HELP') return { type: 'fn', fn: 'HELP', sym: null, args: rest };
    if (FUNCTIONS[firstFn].scope === 'security' && rest.length && !isOwnArg(firstFn, rest[0]) && looksLikeTicker(rest[0])) {
      // "DES NVDA" works too
      return { type: 'fn', fn: firstFn, sym: normalizeSymbol(rest[0]), args: rest.slice(1) };
    }
    return { type: 'fn', fn: firstFn, sym: null, args: rest };
  }

  // TICKER [range]
  if (looksLikeTicker(first)) {
    const sym = normalizeSymbol(first);
    if (!rest.length) return { type: 'load', sym };
    if (isRange(rest[0])) return { type: 'fn', fn: 'GP', sym, args: [rest[0]] };
    // Typed terminal-style (caps) with a short second word: probably a mistyped function
    if (raw === raw.toUpperCase() && rest[0].length <= 5) {
      return { type: 'error', message: `Unknown function ${rest[0]} — type HELP for the list` };
    }
  }

  // Anything else: find a security by name
  return { type: 'search', query: raw };
}

// Suggestions for the autocomplete list under the command line.
export function suggest(input, { limit = 8 } = {}) {
  const raw = (input || '').trim().toUpperCase();
  if (!raw) return [];
  const tokens = raw.split(/\s+/);
  const out = [];
  const push = (code, title, insert) => {
    if (out.length < limit && !out.some(o => o.insert === insert)) out.push({ code, title, insert });
  };
  const fnMatches = (prefix) => {
    const hits = [];
    for (const [code, f] of Object.entries(FUNCTIONS)) {
      if (code.startsWith(prefix)) hits.push([code, f.title]);
    }
    for (const [code, title] of Object.entries(ACTIONS)) {
      if (code.startsWith(prefix)) hits.push([code, title]);
    }
    for (const [alias, code] of Object.entries(ALIASES)) {
      if (alias.startsWith(prefix) && alias !== prefix && !hits.some(h => h[0] === code)) {
        hits.push([code, FUNCTIONS[code].title]);
      }
    }
    return hits;
  };

  if (tokens.length === 1) {
    const t = tokens[0];
    for (const [code, title] of fnMatches(t)) push(code, title, code);
    // title search ("news", "earn")
    if (t.length >= 3) {
      for (const [code, f] of Object.entries(FUNCTIONS)) {
        if (f.title.toUpperCase().includes(t)) push(code, f.title, code);
      }
    }
    if (looksLikeTicker(t) && !resolveFunction(t)) {
      const sym = normalizeSymbol(t);
      push(sym, 'Load security', sym);
      for (const code of ['BRIEF', 'GP', 'ORC', 'N', 'CF']) push(code, `${sym} ${FUNCTIONS[code].title}`, `${sym} ${code}`);
    }
  } else if (tokens.length === 2 && looksLikeTicker(tokens[0])
             && !(resolveFunction(tokens[0]) && FUNCTIONS[resolveFunction(tokens[0])].args)) {
    const sym = normalizeSymbol(tokens[0]);
    for (const [code, title] of fnMatches(tokens[1])) {
      if (FUNCTIONS[code] && FUNCTIONS[code].scope === 'security') push(code, `${sym} ${title}`, `${sym} ${code}`);
    }
  } else if (tokens.length >= 2) {
    const fn = resolveFunction(tokens[0]);
    if (fn && FUNCTIONS[fn].args) {
      const last = tokens[tokens.length - 1];
      for (const r of FUNCTIONS[fn].args.split(' ')) {
        if (r.startsWith(last)) push(r, `${FUNCTIONS[fn].title} — ${r}`, [...tokens.slice(0, -1), r].join(' '));
      }
    }
  }
  return out;
}
