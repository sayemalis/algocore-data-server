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
const TIMEFRAMES = { "5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400, "1w": 604800 }; // interval -> seconds
const CANDLE_LIMIT = 200;

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
    try {
      const res = await fetch(
        `https://data-api.binance.vision/api/v3/klines?symbol=${symbol}&interval=${tf}&limit=${CANDLE_LIMIT}`
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

// ── Binance ticker WebSocket (combined stream, dynamic subscribe) ───────
let binanceWS = null;
let binanceConnecting = false;
let reconnectTimer = null;
let msgId = 1;

function streamNameFor(symbol) {
  return `${symbol.toLowerCase()}@ticker`;
}
function klineStreamNamesFor(symbol) {
  return Object.keys(TIMEFRAMES).map((tf) => `${symbol.toLowerCase()}@kline_${tf}`);
}

function connectBinance() {
  if (binanceConnecting) return;
  binanceConnecting = true;

  const streams = [];
  trackedSymbols.forEach((s) => {
    streams.push(streamNameFor(s));
    streams.push(...klineStreamNamesFor(s));
  });
  const url =
    "wss://stream.binance.com:9443/stream?streams=" +
    (streams.length ? streams.join("/") : "btcusdt@ticker");

  try { binanceWS?.close(); } catch {}
  binanceWS = new WebSocket(url);

  binanceWS.on("open", () => {
    binanceConnecting = false;
    console.log(`[Binance WS] connected — ${trackedSymbols.size} symbols, ${streams.length} streams`);
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
          if (arr.length > CANDLE_LIMIT) arr.shift();
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
    clearTimeout(reconnectTimer);
    reconnectTimer = setTimeout(connectBinance, 3000);
  });

  binanceWS.on("error", () => { try { binanceWS.close(); } catch {} });
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
  await backfillCandles(symbol); // one-time REST cost, same as before — just done once server-side, not per browser tab
  connectBinance(); // reconnect with the updated stream list

  res.json({ ok: true });
});

app.delete("/api/coins/:symbol", (req, res) => {
  const symbol = req.params.symbol.toUpperCase();
  trackedSymbols.delete(symbol);
  delete tickers[symbol];
  delete candles[symbol];
  connectBinance();
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
