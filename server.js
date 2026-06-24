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

const PORT = process.env.PORT || 3001;
const TIMEFRAMES = { "1h": 3600, "4h": 14400, "1d": 86400, "1w": 604800 }; // interval -> seconds
// Per-timeframe candle caps — 4H gets the deepest history since trade
// decisions are made there; 1D/1W are only used for HTF/weekly bias
// context, not deep structure, so they're capped much shallower.
const CANDLE_LIMITS = {
  "1h": 1500, // ~62.5 days
  "4h": 2500, // ~417 days (~14 months) — primary decision TF, deepest history
  "1d": 750,  // ~2.05 years
  "1w": 300,  // ~5.76 years
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

  binanceWS = new WebSocket(url);

  binanceWS.on("open", () => {
    binanceConnecting = false;
    console.log(`[Binance WS] connected — ${trackedSymbols.size} symbols, ${streams.length} streams`);
    // Flush anything that was added while we were still connecting
    if (pendingSubscribes.size) {
      const params = [...pendingSubscribes].flatMap(streamsForSymbol);
      binanceWS.send(JSON.stringify({ method: "SUBSCRIBE", params, id: msgId++ }));
      pendingSubscribes.clear();
    }
  });

  binanceWS.on("message", (raw) => {
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

  binanceWS.on("close", () => {
    binanceConnecting = false;
    binanceWS = null;
    clearTimeout(reconnectTimer);
    reconnectTimer = setTimeout(connectBinance, 3000);
  });

  binanceWS.on("error", () => { try { binanceWS.close(); } catch {} });
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
  binanceWS.send(JSON.stringify({ method: "SUBSCRIBE", params: streamsForSymbol(symbol), id: msgId++ }));
}

function unsubscribeSymbol(symbol) {
  pendingSubscribes.delete(symbol);
  if (binanceWS && binanceWS.readyState === WebSocket.OPEN) {
    binanceWS.send(JSON.stringify({ method: "UNSUBSCRIBE", params: streamsForSymbol(symbol), id: msgId++ }));
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
  res.json({
    symbol,
    ticker: tickers[symbol] || null,
    candles: candles[symbol] || {},
  });
});

// ── WebSocket: send a full snapshot on connect, then live updates ───────
wss.on("connection", (ws) => {
  ws.send(JSON.stringify({
    type: "SNAPSHOT",
    tickers,
    candles,
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
