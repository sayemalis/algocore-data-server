/**
 * AlgoCore Data Server — standalone, separate from the trade-execution bot.
 *
 * Purpose: keep a warm, 24/7 in-memory cache of live tickers + candle history
 * for your watched coins, so the React app's tab refresh just asks THIS
 * server for a ready snapshot instead of re-running staggered REST calls
 * against Binance every single time.
 *
 * This server does NOT place trades, does NOT touch your bot's positions,
 * and can crash/restart freely without affecting live trading — it only
 * re-warms its cache from Binance REST + WS on boot, same as before.
 *
 * Endpoints:
 *   GET  /health                — uptime check (for Render + UptimeRobot)
 *   GET  /api/coins              — list of currently tracked symbols
 *   POST /api/coins              — body: { symbol: "BTCUSDT" } — start tracking a coin
 *   DELETE /api/coins/:symbol    — stop tracking a coin
 *   GET  /api/snapshot/:symbol   — { ticker, candles: { [tf]: [...] } } full warm cache for one coin
 *   WS   /ws                     — live push: { type: "TICKER_UPDATE", ... } / { type: "CANDLE_UPDATE", ... }
 */

const express = require("express");
const cors = require("cors");
const http = require("http");
const WebSocket = require("ws");

// ── Crash safety net ──────────────────────────────────────────────────────
// Today's actual crash (an uncaught WebSocket.send() race in connectBinance,
// now fixed below) took the ENTIRE process down on every occurrence — no
// memory pressure involved, just a single unhandled throw. On a free-tier
// instance, every crash means a cold restart + however long it takes to
// re-track every symbol from scratch. These two handlers mean any FUTURE
// stray error (a bug we haven't found yet) gets logged instead of killing
// the whole server. This does not paper over the bug above — that's fixed
// at the source — it's defense in depth for whatever's next.
process.on("uncaughtException", (err) => {
  console.error("[uncaughtException] survived:", err);
});
process.on("unhandledRejection", (err) => {
  console.error("[unhandledRejection] survived:", err);
});

const PORT = process.env.PORT || 3001;
const TIMEFRAMES = { "4h": 14400, "1d": 86400 }; // interval -> seconds
// Per-timeframe candle caps — 4H gets the deepest history since trade
// decisions are made there; 1D is only used for HTF bias context, not
// deep structure, so it's capped much shallower.
// 5m, 15m, 1h, and 1w removed entirely — no chart tab, no backend use
// (1H/1W chart tabs and the weekly-bias feature were both removed since
// neither ever actually gated a trade decision).
const CANDLE_LIMITS = {
  "4h": 1200, // ~200 days — primary decision TF
  "1d": 400,  // ~1.1 years
};
const CANDLE_LIMIT = 1000; // legacy fallback for any TF not in CANDLE_LIMITS

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: "/ws" });

// ── In-memory state ──────────────────────────────────────────────────────
const tickers = {};          // symbol -> { price, changePct, high, low, ts }
const candles = {};          // symbol -> { [tf]: [{time,open,high,low,close,volume}] }
const trackedSymbols = new Set();

// ── Broadcast helper — push to every connected frontend client ──────────
function broadcast(msg) {
  const data = JSON.stringify(msg);
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) client.send(data);
  });
}

// ── REST backfill (once per symbol, on boot/add) ─────────────────────────
async function backfillCandles(symbol) {
  candles[symbol] = candles[symbol] || {};
  for (const [tf, _secs] of Object.entries(TIMEFRAMES)) {
    const limit = CANDLE_LIMITS[tf] || CANDLE_LIMIT;
    try {
      const res = await fetch(
        `https://data-api.binance.vision/api/v3/klines?symbol=${symbol}&interval=${tf}&limit=${limit}`
      );
      const raw = await res.json();
      candles[symbol][tf] = raw.map((c) => ({
        time: Math.floor(c[0] / 1000),
        open: parseFloat(c[1]),
        high: parseFloat(c[2]),
        low: parseFloat(c[3]),
        close: parseFloat(c[4]),
        volume: parseFloat(c[5]),
      }));
    } catch (err) {
      console.error(`[backfill] ${symbol} ${tf} failed:`, err.message);
      candles[symbol][tf] = candles[symbol][tf] || [];
    }
  }
}

// ── Binance ticker WebSocket (single persistent connection, dynamic subscribe) ──
// IMPORTANT: this connects ONCE and stays open. Adding a new tracked symbol
// sends a SUBSCRIBE message on the existing socket — it does NOT tear down
// and reopen the connection. Reconnecting per-symbol was the original bug
// here: with hundreds of already-saved coins all calling ensureWSSubscribed
// on every page load, each one triggered a full reconnect, so the socket
// never settled into a stable subscribed state and most symbols silently
// never got ticker pushes — forcing the frontend's REST fallback for nearly
// everything instead of using this server's live feed at all.
let binanceWS = null;
let binanceConnecting = false;
let reconnectTimer = null;
let msgId = 1;
const pendingSubscribes = new Set(); // queued while socket is still connecting

function streamNameFor(symbol) {
  return `${symbol.toLowerCase()}@ticker`;
}
function klineStreamNamesFor(symbol) {
  return Object.keys(TIMEFRAMES).map((tf) => `${symbol.toLowerCase()}@kline_${tf}`);
}
function streamsForSymbol(symbol) {
  return [streamNameFor(symbol), ...klineStreamNamesFor(symbol)];
}

function connectBinance() {
  if (binanceConnecting || (binanceWS && binanceWS.readyState === WebSocket.OPEN)) return;
  binanceConnecting = true;

  const streams = [];
  trackedSymbols.forEach((s) => streams.push(...streamsForSymbol(s)));
  const url =
    "wss://stream.binance.com:9443/stream?streams=" +
    (streams.length ? streams.join("/") : "btcusdt@ticker");

  // Use a local `ws` reference throughout this function instead of always
  // reading the mutable global `binanceWS`. Without this, an overlapping
  // reconnect (e.g. this socket's "close"/"error" fires, a new connect
  // attempt starts and reassigns the global, and THEN this socket's
  // delayed "open" event finally fires) would call .send() on whatever
  // socket the global currently points to — which can still be CONNECTING
  // — throwing an uncaught exception that crashed the entire process.
  const ws = new WebSocket(url);
  binanceWS = ws;

  ws.on("open", () => {
    binanceConnecting = false;
    console.log(`[Binance WS] connected — ${trackedSymbols.size} symbols, ${streams.length} streams`);
    // Flush anything that was added while we were still connecting.
    // Double-guarded: only act on THIS socket instance, and only if it's
    // actually open (readyState can still change between the event firing
    // and this callback running, in rare cases).
    if (pendingSubscribes.size && ws.readyState === WebSocket.OPEN) {
      const params = [...pendingSubscribes].flatMap(streamsForSymbol);
      ws.send(JSON.stringify({ method: "SUBSCRIBE", params, id: msgId++ }));
      pendingSubscribes.clear();
    }
  });

  ws.on("message", (raw) => {
    try {
      const msg = JSON.parse(raw);
      const data = msg.data || msg;
      if (!data) return;

      if (data.e === "24hrTicker") {
        const update = {
          symbol: data.s,
          price: parseFloat(data.c),
          changePct: parseFloat(data.P),
          high: parseFloat(data.h),
          low: parseFloat(data.l),
          ts: Date.now(),
        };
        tickers[data.s] = update;
        broadcast({ type: "TICKER_UPDATE", ...update });
      }

      if (data.e === "kline") {
        const k = data.k;
        const tf = k.i; // e.g. "15m"
        const symbol = k.s;
        if (!candles[symbol]) candles[symbol] = {};
        if (!candles[symbol][tf]) candles[symbol][tf] = [];
        const arr = candles[symbol][tf];
        const candle = {
          time: Math.floor(k.t / 1000),
          open: parseFloat(k.o),
          high: parseFloat(k.h),
          low: parseFloat(k.l),
          close: parseFloat(k.c),
          volume: parseFloat(k.v),
        };
        const last = arr[arr.length - 1];
        if (last && last.time === candle.time) {
          arr[arr.length - 1] = candle; // update open candle in place
        } else if (k.x) {
          arr.push(candle); // closed candle, new bucket
          const limit = CANDLE_LIMITS[tf] || CANDLE_LIMIT;
          if (arr.length > limit) arr.shift();
        } else {
          arr.push(candle);
        }
        broadcast({ type: "CANDLE_UPDATE", symbol, tf, candle });
      }
    } catch (err) {
      // ignore malformed frame, don't crash the ingestion loop
    }
  });

  ws.on("close", () => {
    binanceConnecting = false;
    // Only clear the global if THIS socket is still the active one — an
    // overlapping reconnect may have already replaced it with a newer
    // socket, and this stale close event must not null that out.
    if (binanceWS === ws) binanceWS = null;
    clearTimeout(reconnectTimer);
    reconnectTimer = setTimeout(connectBinance, 3000);
  });

  ws.on("error", () => { try { ws.close(); } catch {} });
}

// ── Rate-limited send queue for Binance control messages ─────────────────
// Binance disconnects any connection that receives more than 5 messages/sec
// (this counts SUBSCRIBE/UNSUBSCRIBE, not just data) — repeated violations
// risk an IP-level penalty. Each subscribeSymbol() call below used to call
// binanceWS.send() immediately and unconditionally: on a fresh page load
// with 70-100+ tracked coins all subscribing in a tight burst, that's easily
// 10-20+ SUBSCRIBE messages landing in under a second, blowing past the
// limit and getting this connection disconnected mid-burst. Whatever
// symbols were already baked into a stream URL (boot, or post-reconnect)
// kept working; everything still mid-subscribe when the disconnect hit
// silently lost its live feed — matching the "first ~70 update, the rest
// never do" symptom exactly. Fix: queue every send and drain it at a safe,
// steady rate instead of firing them all at once.
const SEND_RATE_MS = 250; // 4/sec — comfortable margin under Binance's 5/sec hard limit
const sendQueue = [];
let sendTimer = null;
function queueBinanceSend(payload) {
  sendQueue.push(payload);
  if (!sendTimer) sendTimer = setInterval(drainSendQueue, SEND_RATE_MS);
}
function drainSendQueue() {
  if (!sendQueue.length) { clearInterval(sendTimer); sendTimer = null; return; }
  if (!binanceWS || binanceWS.readyState !== WebSocket.OPEN) return; // wait for reconnect, don't drop the queue
  const payload = sendQueue.shift();
  try { binanceWS.send(JSON.stringify(payload)); } catch { /* connection dropped mid-send, will retry via reconnect */ }
}

// ── Subscribe/unsubscribe a single symbol on the EXISTING socket ────────
// This is the fix: adding/removing one symbol never tears down the shared
// connection. If the socket isn't open yet, the symbol is queued and gets
// flushed in one batched SUBSCRIBE once the connection opens (see above).
function subscribeSymbol(symbol) {
  if (!binanceWS || binanceWS.readyState !== WebSocket.OPEN) {
    pendingSubscribes.add(symbol);
    connectBinance();
    return;
  }
  queueBinanceSend({ method: "SUBSCRIBE", params: streamsForSymbol(symbol), id: msgId++ });
}

function unsubscribeSymbol(symbol) {
  pendingSubscribes.delete(symbol);
  if (binanceWS && binanceWS.readyState === WebSocket.OPEN) {
    queueBinanceSend({ method: "UNSUBSCRIBE", params: streamsForSymbol(symbol), id: msgId++ });
  }
}

// ── REST backfill concurrency limiter ────────────────────────────────────
// Without this, N simultaneous POST /api/coins requests (e.g. 293 already-
// saved coins all hitting this endpoint on one page load) would fire N×6
// concurrent REST calls at Binance from this server's single IP — a real
// risk of a 418 rate-limit ban on the SERVER itself, which would then break
// candle data for every connected client at once. Caps how many symbols'
// backfills run at the same time; the rest simply wait their turn.
const MAX_CONCURRENT_BACKFILLS = 3;
let activeBackfills = 0;
const backfillQueue = [];
function runBackfillQueued(symbol) {
  return new Promise((resolve) => {
    const task = async () => {
      activeBackfills++;
      try { await backfillCandles(symbol); } finally {
        activeBackfills--;
        resolve();
        if (backfillQueue.length) backfillQueue.shift()();
      }
    };
    if (activeBackfills < MAX_CONCURRENT_BACKFILLS) task();
    else backfillQueue.push(task);
  });
}

// ── Public API ────────────────────────────────────────────────────────────

app.get("/health", (_req, res) => {
  res.json({ ok: true, trackedSymbols: trackedSymbols.size, uptimeSec: process.uptime() });
});

app.get("/api/coins", (_req, res) => {
  res.json({ symbols: [...trackedSymbols] });
});

// ── Tracked-symbol cap ───────────────────────────────────────────────────
// Binance hard-disconnects (or silently ignores subscribes beyond) 1024
// streams per connection. Each tracked symbol uses 3 streams (ticker +
// kline_4h + kline_1d). Without a cap, trackedSymbols only ever grows —
// nothing previously removed a symbol unless a client explicitly called
// DELETE for it, and the continuous live auto-scanner just keeps adding
// newly-qualifying coins on top of whatever's already there. Over enough
// uptime this silently crosses the 1024-stream ceiling: symbols that got
// subscribed before crossing it keep working, everything after never gets
// live data — exactly the "only the first N coins update" symptom. Cap at
// 300 symbols (900 streams, comfortable margin under 1024) and evict the
// oldest-tracked symbol (Sets preserve insertion order, so this is a cheap
// FIFO) to make room for new ones instead of growing forever.
const MAX_TRACKED_SYMBOLS = 300;
function evictOldestSymbol() {
  const oldest = trackedSymbols.values().next().value;
  if (!oldest) return;
  trackedSymbols.delete(oldest);
  delete tickers[oldest];
  delete candles[oldest];
  unsubscribeSymbol(oldest);
  console.log(`[evict] ${oldest} dropped to stay under the stream cap`);
}

app.post("/api/coins", async (req, res) => {
  const symbol = (req.body?.symbol || "").toUpperCase();
  if (!symbol) return res.status(400).json({ error: "symbol required" });
  if (trackedSymbols.has(symbol)) return res.json({ ok: true, alreadyTracked: true });

  if (trackedSymbols.size >= MAX_TRACKED_SYMBOLS) evictOldestSymbol();

  trackedSymbols.add(symbol);
  subscribeSymbol(symbol); // live stream starts immediately — doesn't wait on backfill
  res.json({ ok: true, queued: true }); // respond right away; backfill runs in the background below

  runBackfillQueued(symbol).catch((err) => console.error(`[backfill] ${symbol} failed:`, err.message));
});

app.delete("/api/coins/:symbol", (req, res) => {
  const symbol = req.params.symbol.toUpperCase();
  trackedSymbols.delete(symbol);
  delete tickers[symbol];
  delete candles[symbol];
  unsubscribeSymbol(symbol);
  res.json({ ok: true });
});

app.get("/api/snapshot/:symbol", (req, res) => {
  const symbol = req.params.symbol.toUpperCase();
  const tf = req.query.tf; // optional — e.g. ?tf=4h
  const allCandles = candles[symbol] || {};
  // Without this filter, every single-TF cache-refresh on the client pulled
  // the FULL multi-TF payload (every tracked timeframe, full depth) every
  // time — measured at ~28GB over a few hours of real usage. Each TF has
  // its own refresh cadence (5m/15m every 5min, 1H every 15min, etc.), so
  // that meant re-downloading everything else redundantly on every single
  // one of those refreshes. ?tf= lets the client ask for only what it
  // actually needs right now.
  const responseCandles = tf ? { [tf]: allCandles[tf] || [] } : allCandles;
  res.json({
    symbol,
    ticker: tickers[symbol] || null,
    candles: responseCandles,
  });
});

// ── WebSocket: send a ticker snapshot on connect, then live updates ─────
// Candle data used to be included here too, but the client never actually
// read it (only msg.tickers was ever consumed) — every single connect or
// reconnect was silently transmitting the full multi-TF candle set for
// every tracked symbol for nothing. That dead weight is gone; candles are
// only ever fetched on-demand via GET /api/snapshot/:symbol?tf=... now.
wss.on("connection", (ws) => {
  ws.send(JSON.stringify({
    type: "SNAPSHOT",
    tickers,
  }));
});

// ── Boot ──────────────────────────────────────────────────────────────────
server.listen(PORT, () => {
  console.log(`[algocore-data-server] listening on port ${PORT}`);
  // Seed with a default coin so the WS connection has something to do on
  // boot even before the frontend adds anything — adjust/remove as needed.
  (async () => {
    trackedSymbols.add("BTCUSDT");
    await backfillCandles("BTCUSDT");
    connectBinance();
  })();
});