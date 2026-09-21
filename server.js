import express from "express";
import path from "node:path";
import crypto from "node:crypto";
import fs from "node:fs";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PORT = 3000;
const HOST = "0.0.0.0";
const STATIC_DIR = path.join(__dirname, "static");
const DATA_URL_RE = /^data:(image\/[a-zA-Z0-9.+-]+);base64,(.+)$/s;
const APP_VERSION = (process.env.RENDER_GIT_COMMIT || "local").slice(0, 7);

const app = express();
app.use(express.json({ limit: "15mb" }));
app.use(express.text({ type: "application/json", limit: "15mb" }));

// ---------- In-memory State & Persistence ----------
function defaultStateJson() {
  return JSON.stringify({ communities: [], games: [] });
}

function md5Hash(data) {
  return crypto.createHash("md5").update(data, "utf8").digest("hex");
}

let sharedState = defaultStateJson();
let sharedStateHash = md5Hash(sharedState);
let stateHistory = []; // { id: number, data: string, savedAt: string }
let historyNextId = 1;
const identities = new Map(); // clientId -> stringified data

// ---------- SSE Subscribers for Instant State Notification ----------
const subscribers = new Set();

function broadcastStateChanged() {
  for (const client of subscribers) {
    try {
      client.write("data: changed\n\n");
    } catch {
      subscribers.delete(client);
    }
  }
}

// ---------- Static File Routes ----------
app.get("/", (_req, res) => {
  res.setHeader("Cache-Control", "no-cache, must-revalidate");
  res.sendFile(path.join(STATIC_DIR, "index.html"));
});

app.get("/sw.js", (_req, res) => {
  res.setHeader("Cache-Control", "no-cache, must-revalidate");
  res.sendFile(path.join(STATIC_DIR, "sw.js"));
});

app.use(
  "/static",
  express.static(STATIC_DIR, {
    setHeaders: (res) => {
      res.setHeader("Cache-Control", "no-cache, must-revalidate");
    },
  })
);

// ---------- API Routes ----------
app.get("/api/version", (_req, res) => {
  res.json({ version: APP_VERSION });
});

app.get("/api/state", (_req, res) => {
  res.setHeader("Content-Type", "application/json");
  res.send(sharedState);
});

app.get("/api/state/hash", (_req, res) => {
  res.json({ hash: sharedStateHash });
});

app.post("/api/state", (req, res) => {
  let rawData = typeof req.body === "string" ? req.body : JSON.stringify(req.body);
  try {
    JSON.parse(rawData);
  } catch {
    return res.status(400).json({ error: "Invalid JSON" });
  }

  const checkpoint = req.query.checkpoint === "1";
  const baseHash = req.query.baseHash;

  if (baseHash && baseHash !== sharedStateHash) {
    return res.status(409).json({ conflict: true });
  }

  if (checkpoint && sharedState) {
    stateHistory.unshift({
      id: historyNextId++,
      data: sharedState,
      savedAt: new Date().toISOString(),
    });
    // Keep only the last 3 history snapshots
    if (stateHistory.length > 3) {
      stateHistory = stateHistory.slice(0, 3);
    }
  }

  sharedState = rawData;
  sharedStateHash = md5Hash(sharedState);

  broadcastStateChanged();

  return res.json({ ok: true, hash: sharedStateHash });
});

app.get("/api/state/history", (_req, res) => {
  res.json(
    stateHistory.map((entry) => ({
      id: entry.id,
      savedAt: entry.savedAt,
    }))
  );
});

app.get("/api/state/history/:id", (req, res) => {
  const targetId = parseInt(req.params.id, 10);
  const entry = stateHistory.find((h) => h.id === targetId);
  if (!entry) {
    return res.status(404).json({ error: "not found" });
  }
  res.setHeader("Content-Type", "application/json");
  res.send(entry.data);
});

app.get("/api/events", (req, res) => {
  res.writeHead(200, {
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    Connection: "keep-alive",
    "X-Accel-Buffering": "no",
  });

  res.write(": connected\n\n");
  subscribers.add(res);

  const keepAlive = setInterval(() => {
    try {
      res.write(": keep-alive\n\n");
    } catch {
      clearInterval(keepAlive);
      subscribers.delete(res);
    }
  }, 25000);

  req.on("close", () => {
    clearInterval(keepAlive);
    subscribers.delete(res);
  });
});

app.get("/api/identity", (req, res) => {
  const clientId = (req.query.clientId || "").toString();
  if (identities.has(clientId)) {
    res.setHeader("Content-Type", "application/json");
    return res.send(identities.get(clientId));
  }
  return res.json(null);
});

app.post("/api/identity", (req, res) => {
  const clientId = (req.query.clientId || "").toString();
  const rawData = typeof req.body === "string" ? req.body : JSON.stringify(req.body);
  try {
    JSON.parse(rawData);
  } catch {
    return res.status(400).json({ error: "Invalid JSON" });
  }
  identities.set(clientId, rawData);
  return res.json({ ok: true });
});

// ---------- AI Vision Endpoint (Gemini / Anthropic) ----------
let genAIClient = null;

function getGenAI() {
  if (!genAIClient) {
    const key = process.env.GEMINI_API_KEY;
    if (!key) {
      return null;
    }
    const { GoogleGenAI } = import("@google/genai");
    // lazy-loaded or initialized
  }
  return genAIClient;
}

app.post("/api/vision", async (req, res) => {
  const body = req.body || {};
  let dataUrls = body.dataUrls;
  if (!dataUrls || !Array.isArray(dataUrls) || dataUrls.length === 0) {
    if (body.dataUrl) {
      dataUrls = [body.dataUrl];
    } else {
      return res.status(400).json({ error: "unsupported image data" });
    }
  }
  const promptText = body.promptText || "";

  // Prepare images
  const imageParts = [];
  for (const dataUrl of dataUrls) {
    const match = DATA_URL_RE.exec(dataUrl);
    if (!match) {
      return res.status(400).json({ error: "unsupported image data" });
    }
    const mimeType = match[1];
    const b64data = match[2];
    imageParts.push({ mimeType, b64data });
  }

  // Check for Gemini API key first
  const geminiKey = process.env.GEMINI_API_KEY;
  if (geminiKey) {
    try {
      const { GoogleGenAI } = await import("@google/genai");
      const ai = new GoogleGenAI({ apiKey: geminiKey });

      const contents = [
        ...imageParts.map((img) => ({
          inlineData: {
            mimeType: img.mimeType,
            data: img.b64data,
          },
        })),
        { text: promptText },
      ];

      const response = await ai.models.generateContent({
        model: "gemini-2.5-flash",
        contents,
      });

      const responseText = response.text || "";
      const cleaned = responseText.replace(/```json/gi, "").replace(/```/g, "").trim();
      const parsed = JSON.parse(cleaned);
      return res.json(parsed);
    } catch (err) {
      console.error("Gemini vision error:", err);
      // If error is JSON parse error vs API error
      if (err instanceof SyntaxError) {
        return res.status(502).json({ error: "could not parse model response" });
      }
      return res.status(502).json({ error: `Gemini API error: ${err.message || err}` });
    }
  }

  // Check for Anthropic API key as fallback
  const anthropicKey = process.env.ANTHROPIC_API_KEY;
  if (anthropicKey) {
    try {
      const anthropicModule = await import("@anthropic-ai/sdk").catch(() => null);
      if (anthropicModule) {
        const Anthropic = anthropicModule.default || anthropicModule.Anthropic;
        const client = new Anthropic({ apiKey: anthropicKey });

        const content = [
          ...imageParts.map((img) => ({
            type: "image",
            source: {
              type: "base64",
              media_type: img.mimeType,
              data: img.b64data,
            },
          })),
          { type: "text", text: promptText },
        ];

        const response = await client.messages.create({
          model: "claude-3-5-sonnet-latest",
          max_tokens: 1000,
          messages: [{ role: "user", content }],
        });

        const text = response.content
          .filter((b) => b.type === "text")
          .map((b) => b.text)
          .join("\n");
        const cleaned = text.replace(/```json/gi, "").replace(/```/g, "").trim();
        const parsed = JSON.parse(cleaned);
        return res.json(parsed);
      }
    } catch (err) {
      console.error("Anthropic error:", err);
      return res.status(502).json({ error: `Claude API error: ${err.message || err}` });
    }
  }

  return res.status(400).json({
    error: "API key required for vision recognition. Please configure GEMINI_API_KEY in settings.",
  });
});

app.listen(PORT, HOST, () => {
  console.log(`Poker Manager running on http://${HOST}:${PORT}`);
});
