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
// An earlier uncaught WebSocket.send() race in the connection logic took
// the ENTIRE process down on every occurrence — no memory pressure
// involved, just a single unhandled throw. On a free-tier instance, every
// crash means a cold restart + however long it takes to re-track every
// symbol from scratch. These two handlers mean any FUTURE stray error (a
// bug not yet found) gets logged instead of killing the whole server. This
// does not paper over bugs — it's defense in depth for whatever's next.
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
const trackedSymbols = new Set(); // every symbol tracked, across all shards combined

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

function streamNameFor(symbol) {
  return `${symbol.toLowerCase()}@ticker`;
}
function klineStreamNamesFor(symbol) {
  return Object.keys(TIMEFRAMES).map((tf) => `${symbol.toLowerCase()}@kline_${tf}`);
}
function streamsForSymbol(symbol) {
  return [streamNameFor(symbol), ...klineStreamNamesFor(symbol)];
}

// ── Sharded Binance connections ───────────────────────────────────────────
// Binance hard-disconnects any single connection that exceeds 1024 streams,
// and each tracked symbol uses 3 streams (ticker + kline_4h + kline_1d).
// A single connection therefore tops out at ~341 symbols — comfortably
// exceeded by real usage (432+ manually-tracked coins). The previous fix
// for this was a hard cap with FIFO eviction, which "worked" only by
// actively dropping live data for coins the person specifically chose to
// track — actively wrong for anyone tracking more than ~300-340 coins.
// The actual fix: multiple independent Binance connections ("shards"),
// each capped well under the 1024-stream ceiling, all feeding the same
// shared tickers/candles cache and broadcasting to the same connected
// frontend clients. Adding symbol #301 just spins up a second shard
// instead of evicting symbol #1.
const SHARD_CAPACITY = 300; // symbols per shard — 900 streams, safe margin under 1024
const shards = []; // [{ id, ws, connecting, reconnectTimer, pendingSubscribes, symbols, sendQueue, sendTimer }]
const symbolToShard = new Map(); // symbol -> shard, for O(1) routing on unsubscribe

function createShard() {
  const shard = {
    id: shards.length,
    ws: null,
    connecting: false,
    reconnectTimer: null,
    pendingSubscribes: new Set(),
    symbols: new Set(),
    sendQueue: [],
    sendTimer: null,
  };
  shards.push(shard);
  return shard;
}

function shardForNewSymbol() {
  let shard = shards.find((s) => s.symbols.size < SHARD_CAPACITY);
  if (!shard) shard = createShard();
  return shard;
}

function connectShard(shard) {
  if (shard.connecting || (shard.ws && shard.ws.readyState === WebSocket.OPEN)) return;
  shard.connecting = true;

  const streams = [];
  shard.symbols.forEach((s) => streams.push(...streamsForSymbol(s)));
  const url =
    "wss://stream.binance.com:9443/stream?streams=" +
    (streams.length ? streams.join("/") : "btcusdt@ticker");

  // Local `ws` reference used throughout, not the mutable shard.ws field —
  // an overlapping reconnect (this socket's close/error fires, a new
  // connect attempt starts and reassigns shard.ws, and THEN this socket's
  // delayed open event finally fires) would otherwise call .send() on
  // whatever shard.ws currently points to, which can still be CONNECTING —
  // an uncaught exception that crashed the entire process previously.
  const ws = new WebSocket(url);
  shard.ws = ws;

  ws.on("open", () => {
    shard.connecting = false;
    console.log(`[Binance WS shard ${shard.id}] connected — ${shard.symbols.size} symbols, ${streams.length} streams`);
    if (shard.pendingSubscribes.size && ws.readyState === WebSocket.OPEN) {
      const params = [...shard.pendingSubscribes].flatMap(streamsForSymbol);
      ws.send(JSON.stringify({ method: "SUBSCRIBE", params, id: nextMsgId() }));
      shard.pendingSubscribes.clear();
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
        const tf = k.i;
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
    shard.connecting = false;
    if (shard.ws === ws) shard.ws = null;
    clearTimeout(shard.reconnectTimer);
    shard.reconnectTimer = setTimeout(() => connectShard(shard), 3000);
  });

  ws.on("error", () => { try { ws.close(); } catch {} });
}

// ── Rate-limited send queue, per shard ────────────────────────────────────
// Binance disconnects any connection receiving more than 5 messages/sec
// (counts SUBSCRIBE/UNSUBSCRIBE, not just data) — repeated violations risk
// an IP-level penalty. A burst of many coins subscribing at once easily
// exceeds that if sent immediately and unconditionally. Each shard is a
// SEPARATE Binance connection with its own independent 5/sec budget, so
// each gets its own queue/timer rather than sharing one globally.
const SEND_RATE_MS = 250; // 4/sec per shard — comfortable margin under Binance's 5/sec hard limit
let _msgId = 1;
function nextMsgId() { return _msgId++; }

function queueShardSend(shard, payload) {
  shard.sendQueue.push(payload);
  if (!shard.sendTimer) shard.sendTimer = setInterval(() => drainShardQueue(shard), SEND_RATE_MS);
}
function drainShardQueue(shard) {
  if (!shard.sendQueue.length) { clearInterval(shard.sendTimer); shard.sendTimer = null; return; }
  if (!shard.ws || shard.ws.readyState !== WebSocket.OPEN) return; // wait for reconnect, don't drop the queue
  const payload = shard.sendQueue.shift();
  try { shard.ws.send(JSON.stringify(payload)); } catch { /* connection dropped mid-send, will retry via reconnect */ }
}

// ── Subscribe/unsubscribe a single symbol ────────────────────────────────
// Routes to whichever shard owns the symbol (existing) or has room (new).
// Adding/removing one symbol never tears down its shard's connection.
function subscribeSymbol(symbol) {
  let shard = symbolToShard.get(symbol);
  if (!shard) {
    shard = shardForNewSymbol();
    shard.symbols.add(symbol);
    symbolToShard.set(symbol, shard);
  }
  if (!shard.ws || shard.ws.readyState !== WebSocket.OPEN) {
    shard.pendingSubscribes.add(symbol);
    connectShard(shard);
    return;
  }
  queueShardSend(shard, { method: "SUBSCRIBE", params: streamsForSymbol(symbol), id: nextMsgId() });
}

function unsubscribeSymbol(symbol) {
  const shard = symbolToShard.get(symbol);
  if (!shard) return;
  shard.symbols.delete(symbol);
  shard.pendingSubscribes.delete(symbol);
  symbolToShard.delete(symbol);
  if (shard.ws && shard.ws.readyState === WebSocket.OPEN) {
    queueShardSend(shard, { method: "UNSUBSCRIBE", params: streamsForSymbol(symbol), id: nextMsgId() });
  }
}

// ── REST backfill concurrency limiter ────────────────────────────────────
// Without this, N simultaneous POST /api/coins requests (e.g. hundreds of
// already-saved coins all hitting this endpoint on one page load) would
// fire N×2 concurrent REST calls at Binance from this server's single IP —
// a real risk of a 418 rate-limit ban on the SERVER itself, which would
// then break candle data for every connected client at once. Caps how many
// symbols' backfills run at the same time; the rest simply wait their turn.
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
  res.json({
    ok: true,
    trackedSymbols: trackedSymbols.size,
    shards: shards.map((s) => ({ id: s.id, symbols: s.symbols.size, connected: s.ws?.readyState === WebSocket.OPEN })),
    uptimeSec: process.uptime(),
  });
});

app.get("/api/coins", (_req, res) => {
  res.json({ symbols: [...trackedSymbols] });
});

app.post("/api/coins", async (req, res) => {
  const symbol = (req.body?.symbol || "").toUpperCase();
  if (!symbol) return res.status(400).json({ error: "symbol required" });
  if (trackedSymbols.has(symbol)) return res.json({ ok: true, alreadyTracked: true });

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
  // time. ?tf= lets the client ask for only what it actually needs right now.
  const responseCandles = tf ? { [tf]: allCandles[tf] || [] } : allCandles;
  res.json({
    symbol,
    ticker: tickers[symbol] || null,
    candles: responseCandles,
  });
});

// ── WebSocket: send a ticker snapshot on connect, then live updates ─────
wss.on("connection", (ws) => {
  ws.send(JSON.stringify({
    type: "SNAPSHOT",
    tickers,
  }));
});

// ── Boot ──────────────────────────────────────────────────────────────────
server.listen(PORT, () => {
  console.log(`[algocore-data-server] listening on port ${PORT}`);
  // Seed with a default coin so the first shard has something to do on
  // boot even before the frontend adds anything — adjust/remove as needed.
  (async () => {
    trackedSymbols.add("BTCUSDT");
    await backfillCandles("BTCUSDT");
    subscribeSymbol("BTCUSDT");
  })();
});