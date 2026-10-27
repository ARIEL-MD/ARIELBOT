// ===================== SITE WEB D'APPAIRAGE =====================
// Petit serveur HTTP sans dépendance. Il remplace /pair de Telegram :
//   GET  /               → page d'appairage (public/index.html)
//   POST /api/pair       → { phone }  démarre l'appairage
//   GET  /api/status     → état + code d'appairage
//   POST /api/stop       → arrête l'instance du visiteur
//
// Identification du visiteur : un jeton aléatoire (48 caractères hex) généré
// par le navigateur, gardé en localStorage et envoyé dans l'en-tête X-Sid.
// Pas de cookie → fonctionne aussi dans un iframe sur un autre site.
// ==============================================================================

const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const SID_RE = /^[a-f0-9]{48}$/;
const PAGE = path.join(__dirname, "public", "index.html");

function start({ cfg, requestPair, stopInstance, getState, name, admin }) {
  const port = cfg.WEB_PORT || 3000;
  const origins = cfg.WEB_ALLOWED_ORIGINS || ["*"];
  const maxPerHour = cfg.WEB_MAX_PAIRS_PER_IP_PER_HOUR || 5;

  // ip -> [timestamps des demandes d'appairage]
  const hits = new Map();
  setInterval(() => {
    const limit = Date.now() - 3600_000;
    for (const [ip, arr] of hits) {
      const kept = arr.filter((t) => t > limit);
      kept.length ? hits.set(ip, kept) : hits.delete(ip);
    }
  }, 600_000).unref();

  // ---- Admin : mot de passe + anti force brute ----
  const ADMIN_PAGE = path.join(__dirname, "public", "admin.html");
  const adminEnabled = Boolean(admin && admin.password);
  if (!adminEnabled) console.log("ℹ️  Page admin désactivée (WEB_ADMIN_PASSWORD vide).");
  const sha = (s) => crypto.createHash("sha256").update(String(s)).digest();
  const adminHash = adminEnabled ? sha(admin.password) : null;
  const fails = new Map(); // ip -> [timestamps d'échecs]
  function adminAuth(req, ip) {
    const now = Date.now();
    const recent = (fails.get(ip) || []).filter((t) => t > now - 15 * 60_000);
    if (recent.length >= 10) return "limit";
    const ok = crypto.timingSafeEqual(sha(req.headers["x-admin-pass"] || ""), adminHash);
    if (!ok) { recent.push(now); fails.set(ip, recent); return "bad"; }
    return "ok";
  }

  function clientIp(req) {
    const fwd = String(req.headers["x-forwarded-for"] || "").split(",")[0].trim();
    return fwd || req.socket.remoteAddress || "?";
  }

  function cors(req, res) {
    const origin = req.headers.origin;
    if (origin && (origins.includes("*") || origins.includes(origin))) {
      res.setHeader("Access-Control-Allow-Origin", origins.includes("*") ? "*" : origin);
      res.setHeader("Vary", "Origin");
    }
    res.setHeader("Access-Control-Allow-Headers", "Content-Type, X-Sid");
    res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  }

  function json(res, code, obj) {
    res.writeHead(code, { "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store" });
    res.end(JSON.stringify(obj));
  }

  function readBody(req) {
    return new Promise((resolve) => {
      let data = "";
      req.on("data", (c) => {
        data += c;
        if (data.length > 2000) req.destroy();
      });
      req.on("end", () => {
        try { resolve(JSON.parse(data || "{}")); } catch { resolve({}); }
      });
      req.on("error", () => resolve({}));
    });
  }

  const server = http.createServer(async (req, res) => {
    const url = (req.url || "/").split("?")[0];
    if (!url.startsWith("/api/admin") && url !== "/admin") cors(req, res);

    if (req.method === "OPTIONS") { res.writeHead(204); return res.end(); }

    if (req.method === "GET" && (url === "/" || url === "/index.html")) {
      try {
        const html = fs.readFileSync(PAGE, "utf8").replace(/__BOT_NAME__/g, String(name || "Bot WhatsApp").replace(/[<>&"]/g, ""));
        res.writeHead(200, { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-cache" });
        return res.end(html);
      } catch {
        return json(res, 500, { ok: false, error: "Page introuvable." });
      }
    }

    if (url === "/admin") {
      if (!adminEnabled) return json(res, 404, { ok: false, error: "Introuvable." });
      try {
        res.writeHead(200, { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store", "X-Robots-Tag": "noindex" });
        return res.end(fs.readFileSync(ADMIN_PAGE, "utf8"));
      } catch { return json(res, 500, { ok: false, error: "Page introuvable." }); }
    }

    if (url.startsWith("/api/admin/")) {
      if (!adminEnabled) return json(res, 404, { ok: false, error: "Introuvable." });
      const a = adminAuth(req, clientIp(req));
      if (a === "limit") return json(res, 429, { ok: false, error: "Trop d'essais. Réessaie dans 15 minutes." });
      if (a !== "ok") return json(res, 401, { ok: false, error: "Mot de passe incorrect." });

      if (req.method === "GET" && url === "/api/admin/instances") {
        return json(res, 200, { ok: true, stats: admin.stats(), instances: admin.list() });
      }
      if (req.method === "POST") {
        const b = await readBody(req);
        if (url === "/api/admin/kill") return json(res, 200, admin.kill(b.id, { wipe: !!b.wipe, remove: !!b.remove }));
        if (url === "/api/admin/killall") return json(res, 200, admin.killAll({ wipe: !!b.wipe }));
        if (url === "/api/admin/rename") return json(res, 200, admin.rename(b.id, b.label));
      }
      return json(res, 404, { ok: false, error: "Introuvable." });
    }

    if (!url.startsWith("/api/")) return json(res, 404, { ok: false, error: "Introuvable." });

    const sid = String(req.headers["x-sid"] || "");
    if (!SID_RE.test(sid)) return json(res, 400, { ok: false, error: "Session invalide, recharge la page." });
    const id = "web_" + sid;

    if (req.method === "GET" && url === "/api/status") {
      return json(res, 200, { ok: true, ...getState(id) });
    }

    if (req.method === "POST" && url === "/api/pair") {
      const ip = clientIp(req);
      const now = Date.now();
      const recent = (hits.get(ip) || []).filter((t) => t > now - 3600_000);
      if (recent.length >= maxPerHour) {
        return json(res, 429, { ok: false, error: "Trop de tentatives. Réessaie dans une heure." });
      }
      recent.push(now);
      hits.set(ip, recent);

      const body = await readBody(req);
      const r = requestPair(id, body.phone, "Site web");
      return json(res, r.ok ? 200 : 400, r.ok ? { ok: true } : { ok: false, error: r.error });
    }

    if (req.method === "POST" && url === "/api/stop") {
      const stopped = stopInstance(id, "demande utilisateur (web)");
      return json(res, 200, { ok: true, stopped });
    }

    return json(res, 404, { ok: false, error: "Introuvable." });
  });

  server.on("error", (e) => console.error("⚠️  Serveur web indisponible :", e.message));
  server.listen(port, () => console.log(`🌐 Site d'appairage en ligne sur le port ${port}.`));
}

module.exports = { start };
