// Function code → panel definition.
//
// A panel definition is a plain object:
//   load(ctx)                      → Promise of data (fetch)
//   render(ctx, data, bodyEl)      → build the DOM
//   text(ctx, data)                → one-line summary sent with ASK (optional)
//   refresh                        → seconds between automatic reloads (optional)
//   events: { busEvent: 'reload' | (ctx, msg) => 'reload' | null }  (optional)
//   onQuotes(ctx, data, el, syms)  → custom live handling (optional; live cells update on their own)
//   watch(ctx)                     → extra symbols to subscribe (optional)
//   flush                          → body without padding (charts)
import * as security from './security.js';
import * as market from './market.js';
import * as oracle from './oracle.js';
import { BRIEF } from './brief.js';
import { GP } from './chart.js';
import * as tools from './tools.js';

export const PANELS = {
  BRIEF, GP,
  DES: security.DES, FA: security.FA, EEO: security.EEO, ANR: security.ANR, EE: security.EE,
  INS: security.INS, HDS: security.HDS, CF: security.CF, SI: security.SI, DVD: security.DVD,
  HOLD: security.HOLD, OMON: security.OMON, HP: security.HP, RV: security.RV, N: security.N,
  W: market.W, TOP: market.TOP, ECO: market.ECO, GC: market.GC, MOST: market.MOST, EVTS: market.EVTS,
  WEI: market.WEI, SECT: market.SECT, GOVT: market.GOVT, FX: market.FX, CMDTY: market.CMDTY, CRYPTO: market.CRYPTO,
  ORC: oracle.ORC, REG: oracle.REG, BRK: oracle.BRK, ACC: oracle.ACC,
  ASK: tools.ASK, SECF: tools.SECF, HELP: tools.HELP,
};
