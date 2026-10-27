// ===================== ORCHESTRATEUR MULTI-INSTANCES =====================
// Bot Telegram PUBLIC en libre-service. N'importe qui peut envoyer (en
// message privé) :
//
//     /pair <numero_whatsapp>
//
// et obtient sa PROPRE instance du bot WhatsApp (session, contacts, réglages
// totalement isolés des autres utilisateurs), lancée comme un processus Node
// séparé, tournant à côté des autres sur ce serveur.
//
// Le code du bot (index.js, commands/, etc.) reste UNIQUE et PARTAGÉ : toutes
// les données propres à un utilisateur (session/, settings.json,
// known-contacts.json, blocklist-history.json, data/...) vivent dans
// orchestrator/instances/<telegram_chat_id>/ (variable BOT_DATA_DIR).
//
// Commandes disponibles (Telegram) :
//   /pair <numero>          — crée/relance TON instance avec ce numéro
//   /status                 — état de TON instance
//   /stop                   — arrête TON instance (garde tes données)
//   /stats                  — (admin uniquement) vue d'ensemble
//   /instances              — (admin uniquement) liste toutes les instances (avec leur chatId)
//   /name <chatId> <nom>    — (admin uniquement) donne un nom lisible à une instance (ex : "Ami Paul")
//   /kill <chatId> [wipe]   — (admin uniquement) arrête l'instance de N'IMPORTE QUEL
//                             utilisateur (ajouter "wipe" pour effacer aussi ses données)
//   /killall [wipe]         — (admin uniquement) arrête TOUTES les instances actives
//
// Lancement : node orchestrator/manager.js
// ==============================================================================

const fs = require("fs");
const path = require("path");
const { fork } = require("child_process");

const cfg = require("./config");

if (!cfg.WEB_ENABLED) {
  console.error("❌ Mets WEB_ENABLED: true dans orchestrator/config.js avant de lancer.");
  process.exit(1);
}
fs.mkdirSync(cfg.INSTANCES_DIR, { recursive: true });

// Réglages internes (modifiables dans config.js si besoin, sinon valeurs par défaut).
const MAX_INSTANCES = Number(cfg.MAX_INSTANCES) || 0; // 0 = pas de limite
const PAIRING_DEADLINE_MS = 3 * 60 * 1000; // temps laissé pour entrer le code dans WhatsApp
const CODE_TIMEOUT_MS = 45 * 1000; // temps max pour recevoir un code après /pair
const RESTART_WINDOW_MS = 10 * 60 * 1000; // fenêtre de comptage des redémarrages auto
const MAX_RESTARTS_IN_WINDOW = 5;
const MAX_LOG_BYTES = 5 * 1024 * 1024;

// ===================== REGISTRE PERSISTANT =====================
// Contient la liste des instances, pour tout retrouver si l'orchestrateur
// redémarre (relance automatique plus bas).
function loadRegistry() {
  try {
    const data = JSON.parse(fs.readFileSync(cfg.REGISTRY_FILE, "utf8"));
    return {
      instances: data.instances || {},
    };
  } catch {
    return { instances: {} };
  }
}

let registry = loadRegistry();

// Écriture atomique (fichier temporaire puis renommage) : un arrêt brutal
// en pleine écriture ne peut plus corrompre registry.json.
function saveRegistry() {
  try {
    const tmp = cfg.REGISTRY_FILE + ".tmp";
    fs.writeFileSync(tmp, JSON.stringify(registry, null, 2), "utf8");
    fs.renameSync(tmp, cfg.REGISTRY_FILE);
  } catch (e) {
    console.error("⚠️  Erreur sauvegarde registry.json :", e.message);
  }
}

// chatId (string) -> { child, timeoutHandle, deadlineHandle }
const running = new Map();
// chatId (string) -> [timestamps des redémarrages automatiques récents]
const restartHistory = new Map();

// chatId (string) -> true : instance en cours de relance silencieuse suite à
// un redémarrage du SERVEUR/orchestrateur (pas une vraie reconnexion WhatsApp
// initiée par l'utilisateur). Évite de spammer chaque utilisateur (et
// l'admin) d'un message "connecté !" pour chaque instance à chaque fois que
// l'orchestrateur redémarre.
const silentBoot = new Set();

function instanceDir(chatId) {
  const dir = path.join(cfg.INSTANCES_DIR, String(chatId));
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

function logLine(chatId, line) {
  const dir = instanceDir(chatId);
  fs.appendFile(path.join(dir, "bot.log"), `[${new Date().toISOString()}] ${line}\n`, () => {});
}

function trimLogIfHuge(chatId) {
  try {
    const file = path.join(instanceDir(chatId), "bot.log");
    if (fs.existsSync(file) && fs.statSync(file).size > MAX_LOG_BYTES) fs.writeFileSync(file, "");
  } catch {}
}

function hasSession(chatId) {
  return fs.existsSync(path.join(instanceDir(chatId), "session", "creds.json"));
}

// Efface les données d'une instance (session comprise), en gardant le journal.
function wipeInstanceData(chatId) {
  const dir = instanceDir(chatId);
  for (const name of fs.readdirSync(dir)) {
    if (name === "bot.log") continue;
    try {
      fs.rmSync(path.join(dir, name), { recursive: true, force: true });
    } catch (e) {
      console.error(`⚠️  Impossible d'effacer ${name} (instance ${chatId}) :`, e.message);
    }
  }
}

// ===================== BOT TELEGRAM PUBLIC =====================
// Telegram n'est plus utilisé : l'appairage se fait depuis le site (web.js).
// Ce faux "bot" ignore les anciens envois de messages Telegram.
const noop = () => Promise.resolve();
const bot = { on() {}, onText() {}, sendMessage: noop, sendPhoto: noop, answerCallbackQuery: noop };
bot.on("polling_error", (err) => console.error("⚠️  Erreur Telegram (orchestrateur) :", err.message));

// ===================== INSTANCES CRÉÉES DEPUIS LE SITE WEB =====================
// Un visiteur du site reçoit un identifiant "web_<token>" (au lieu d'un chatId
// Telegram). Les messages qui seraient envoyés sur Telegram sont gardés ici
// pour que la page web les affiche (code d'appairage, erreurs...).
const live = new Map(); // id -> { code, notice, noticeAt }
const isWeb = (id) => String(id).startsWith("web_");
function liveOf(id) {
  id = String(id);
  if (!live.has(id)) live.set(id, { code: null, notice: null, noticeAt: 0 });
  return live.get(id);
}
function stripHtml(t) {
  return String(t).replace(/<[^>]+>/g, "").replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&amp;/g, "&").trim();
}

function isAdmin(chatId) {
  return cfg.ADMIN_CHAT_ID && String(chatId) === String(cfg.ADMIN_CHAT_ID);
}

// Nom lisible d'une instance pour l'affichage (/instances, /kill, /killall) :
// priorité au nom donné manuellement par l'admin via /name, sinon au nom de
// profil WhatsApp (le plus fiable : c'est le vrai nom que l'ami a choisi sur
// WhatsApp, capté dès sa connexion), sinon son nom Telegram (capté dès le
// /pair, donc disponible même avant qu'il finisse de se connecter), sinon le
// numéro, sinon juste le chatId brut en tout dernier recours.
function instanceLabel(id) {
  const e = registry.instances[String(id)] || {};
  return e.label || e.whatsappName || e.telegramName || e.phoneNumber || String(id);
}

// Envoie un message à l'admin (ADMIN_CHAT_ID) — utilisé pour le prévenir
// quand un(e) ami(e) commence un appairage ou se connecte, sans qu'il ait à
// vérifier /instances en boucle. Ne fait rien si ADMIN_CHAT_ID n'est pas
// configuré, ou si c'est l'admin lui-même qui pair sa propre instance
// (il le sait déjà, pas besoin de se notifier soi-même).
function notifyAdmin(chatId, text) {
  if (!cfg.ADMIN_CHAT_ID || isAdmin(chatId)) return;
  reply(cfg.ADMIN_CHAT_ID, text);
}

// Toutes les réponses passent par ici, en HTML, pour un rendu uniforme
// (avant : certains messages étaient en texte brut, d'autres en HTML avec
// emojis/gras — d'où un rendu "mélangé" selon la commande).
function reply(chatId, text, opts = {}) {
  if (isWeb(chatId)) {
    const l = liveOf(chatId);
    l.notice = stripHtml(text);
    l.noticeAt = Date.now();
    return Promise.resolve();
  }
  return bot
    .sendMessage(chatId, text, { parse_mode: "HTML", ...opts })
    .catch((e) => console.error("⚠️  Envoi Telegram échoué :", e.message));
}

// Regroupe des lignes dans un bloc visuellement détaché (même style que le
// message d'accueil), pour séparer clairement le corps d'un message de son
// titre. Ignore les lignes vides/absentes.
function box(lines) {
  return "<blockquote>" + lines.filter(Boolean).join("\n") + "</blockquote>";
}

// Titre uniforme pour toutes les réponses : emoji + texte en gras, suivi
// d'une ligne vide avant le corps du message.
function title(emoji, text) {
  return `${emoji} <b>${text}</b>\n\n`;
}

// Badge visuel cohérent pour un statut d'instance, réutilisé partout
// (/status, /instances, /stats) au lieu d'afficher le mot brut ("connected",
// "pairing"...) qui n'a pas de repère visuel immédiat.
const STATUS_BADGES = {
  connected: "🟢 Connecté",
  reconnecting: "🟠 Reconnexion…",
  pairing: "🟡 En attente du code",
  stopped: "⚪ Arrêté",
  error: "🔴 Erreur",
  crashed: "🔴 Plantée (boucle de redémarrage)",
  logged_out: "🔴 Déconnecté de WhatsApp",
};
function statusBadge(status) {
  return STATUS_BADGES[status] || `⚪ ${escapeHtml(status || "inconnu")}`;
}

function clearTimers(r) {
  if (!r) return;
  clearTimeout(r.timeoutHandle);
  clearTimeout(r.deadlineHandle);
}

// Démarre (ou relance) l'instance d'un chatId donné. phoneNumber est optionnel
// pour une relance (session déjà existante) ; obligatoire pour un tout
// premier pairing.
function startInstance(chatId, phoneNumber) {
  chatId = String(chatId);
  if (running.has(chatId)) {
    return { alreadyRunning: true };
  }

  const dir = instanceDir(chatId);
  trimLogIfHuge(chatId);
  const needsPairing = !hasSession(chatId);
  const args = phoneNumber ? [phoneNumber] : [];

  // Prévient tout de suite selon le cas : pas de session locale (premier
  // appairage, ou après un /kill wipe) vs reconnexion d'une session déjà
  // connue (pas besoin de nouveau code dans ce cas).
  if (isWeb(chatId)) {
    // Pas de message "session introuvable / reconnexion" pour le site web :
    // la page affiche déjà l'état.
  } else if (needsPairing) {
    const knownNumber = phoneNumber || (registry.instances[chatId] || {}).phoneNumber || "?";
    reply(chatId, `⚠️ Aucune session trouvée pour <b>${escapeHtml(knownNumber)}</b>.`);
  } else if (!silentBoot.has(chatId)) {
    // Pas de message "Reconnexion en cours" lors d'une relance silencieuse
    // au démarrage du serveur (silentBoot) : l'utilisateur n'a rien demandé,
    // pas besoin de le prévenir. Le message reste envoyé pour une reconnexion
    // demandée explicitement (ex: /pair) ou après une coupure WhatsApp.
    reply(chatId, title("🔄", "Reconnexion") + "Reconnexion de ton instance en cours…");
  }

  const child = fork(cfg.BOT_ENTRY, args, {
    cwd: dir,
    env: {
      ...process.env,
      OWNER_NUMBER_OVERRIDE: phoneNumber || "",
      // Dossier où l'instance range TOUTES ses données (voir lib/paths.js).
      BOT_DATA_DIR: dir,
    },
    stdio: ["ignore", "pipe", "pipe", "ipc"],
    silent: true,
  });

  child.stdout.on("data", (buf) => logLine(chatId, buf.toString().trim()));
  child.stderr.on("data", (buf) => logLine(chatId, "STDERR: " + buf.toString().trim()));

  const entry = registry.instances[chatId] || {};
  entry.phoneNumber = phoneNumber || entry.phoneNumber;
  entry.status = needsPairing ? "pairing" : "reconnecting";
  entry.updatedAt = Date.now();
  entry.createdAt = entry.createdAt || Date.now();
  registry.instances[chatId] = entry;
  saveRegistry();

  const handle = { child, timeoutHandle: null, deadlineHandle: null };

  // Premier pairing uniquement : si aucun code/erreur ne revient dans ce
  // délai, le démarrage a échoué (dépendances manquantes, config invalide...).
  // Une simple reconnexion d'une session existante n'a pas besoin de code.
  if (needsPairing) {
    handle.timeoutHandle = setTimeout(() => {
      reply(chatId, title("❌", "Délai dépassé") + "En attente du code d'appairage. Réessaie avec /pair.");
      stopInstance(chatId, "timeout", { wipe: true });
    }, CODE_TIMEOUT_MS);
  }

  child.on("error", (err) => {
    logLine(chatId, "Erreur process : " + err.message);
  });

  child.on("message", (msg) => {
    if (!msg || typeof msg !== "object") return;

    if (msg.type === "pairing_code") {
      clearTimeout(handle.timeoutHandle);
      // Laisse 3 minutes pour entrer le code (WhatsApp peut en régénérer un
      // nouveau si le premier expire) ; au-delà, on arrête proprement au lieu
      // de renvoyer des codes indéfiniment.
      if (!handle.deadlineHandle) {
        handle.deadlineHandle = setTimeout(() => {
          reply(chatId, title("⌛", "Appairage non terminé") + "Le temps imparti est écoulé. Envoie /pair pour recommencer.");
          stopInstance(chatId, "appairage expiré", { wipe: true });
        }, PAIRING_DEADLINE_MS);
      }
      if (isWeb(chatId)) {
        const l = liveOf(chatId);
        l.code = msg.code;
        l.notice = null;
      } else {
        reply(chatId, pairingCard(entry.phoneNumber, msg.code), { reply_markup: pairingCardKeyboard(msg.code) });
      }
    } else if (msg.type === "pairing_error") {
      clearTimers(handle);
      if (isWeb(chatId)) liveOf(chatId).code = null;
      reply(chatId, title("❌", "Échec de l'appairage") + escapeHtml(msg.message));
      notifyAdmin(chatId, title("❌", "Échec d'appairage") + `${escapeHtml(instanceLabel(chatId))} : ${escapeHtml(msg.message)}`);
      entry.status = "error";
      saveRegistry();
      // Le process se termine tout seul juste après ; on nettoie la session incomplète.
      stopInstance(chatId, "erreur d'appairage", { wipe: true });
    } else if (msg.type === "connected") {
      clearTimers(handle);
      if (isWeb(chatId)) liveOf(chatId).code = null;
      // "connected" est aussi envoyé à chaque reconnexion automatique de
      // WhatsApp : on ne prévient l'utilisateur (et l'admin) qu'à la première fois,
      // mais on met à jour le nom WhatsApp à chaque fois (il peut changer).
      if (msg.waName) {
        entry.whatsappName = msg.waName;
      }
      // Relance silencieuse suite à un redémarrage du serveur : on met à jour
      // le statut sans notifier personne (l'utilisateur n'a rien demandé,
      // pas besoin de le prévenir que son bot, qui marchait déjà, marche
      // toujours).
      const wasSilentBoot = silentBoot.delete(chatId);
      if (entry.status !== "connected" && !wasSilentBoot) {
        reply(chatId, title("✅", "Bot connecté") + "Ton bot WhatsApp est actif !");
        notifyAdmin(
          chatId,
          title("✅", "Nouvelle connexion") +
            `${escapeHtml(instanceLabel(chatId))} (numéro <code>${escapeHtml(entry.phoneNumber || "?")}</code>) vient de connecter son bot WhatsApp.`
        );
      }
      entry.status = "connected";
      entry.updatedAt = Date.now();
      saveRegistry();
    } else if (msg.type === "logged_out") {
      // L'utilisateur a supprimé l'appareil depuis son téléphone : la session
      // est morte. Sans ce nettoyage, le process resterait actif et /pair
      // répondrait "tu as déjà une instance active".
      reply(
        chatId,
        title("⚠️", "Bot déconnecté") +
          "Ton bot a été déconnecté de WhatsApp (appareil supprimé depuis ton téléphone).\n" +
          "Envoie <code>/pair &lt;code&gt; &lt;numéro&gt;</code> pour te reconnecter."
      );
      notifyAdmin(chatId, title("⚠️", "Déconnexion") + `${escapeHtml(instanceLabel(chatId))} a été déconnecté(e) de WhatsApp (appareil supprimé).`);
      stopInstance(chatId, "déconnecté depuis WhatsApp", { wipe: true });
      entry.status = "logged_out";
      saveRegistry();
    }
  });

  child.on("exit", (exitCode) => {
    logLine(chatId, `Process terminé (code ${exitCode}).`);
    const current = running.get(chatId);
    // Si ce process n'est plus celui enregistré, c'est qu'on l'a arrêté nous-mêmes.
    if (!current || current.child !== child) return;

    clearTimers(current);
    running.delete(chatId);

    // Arrêt inattendu (crash, .restart, mémoire...) d'une instance qui
    // fonctionnait : on la relance, avec une limite pour éviter les boucles.
    if (entry.status === "connected" || entry.status === "reconnecting") {
      const now = Date.now();
      const recent = (restartHistory.get(chatId) || []).filter((t) => now - t < RESTART_WINDOW_MS);
      if (recent.length >= MAX_RESTARTS_IN_WINDOW) {
        entry.status = "crashed";
        saveRegistry();
        reply(chatId, title("❌", "Redémarrages en boucle") + "Ton bot a été mis en pause. Envoie <code>/pair &lt;code&gt; &lt;numéro&gt;</code> pour le relancer.");
        return;
      }
      recent.push(now);
      restartHistory.set(chatId, recent);
      logLine(chatId, "Relance automatique dans 3 s...");
      setTimeout(() => {
        if (!running.has(chatId)) startInstance(chatId, entry.phoneNumber);
      }, 3000);
      return;
    }

    if (entry.status === "pairing") {
      entry.status = "stopped";
      saveRegistry();
    }
  });

  running.set(chatId, handle);
  return { started: true };
}

function stopInstance(chatId, reason = "manuel", { wipe = false } = {}) {
  chatId = String(chatId);
  const r = running.get(chatId);
  if (!r) return false;
  clearTimers(r);
  running.delete(chatId); // retiré AVANT le kill : le handler "exit" saura que c'est voulu
  if (isWeb(chatId)) liveOf(chatId).code = null;
  try {
    r.child.kill("SIGTERM");
  } catch {}
  const entry = registry.instances[chatId];
  if (entry) {
    entry.status = "stopped";
    entry.stoppedReason = reason;
    entry.updatedAt = Date.now();
    saveRegistry();
  }
  if (wipe) {
    // Petit délai : laisse le process se terminer avant d'effacer ses fichiers.
    // Si l'utilisateur relance /pair entre-temps, on n'efface rien.
    setTimeout(() => {
      if (!running.has(chatId)) wipeInstanceData(chatId);
    }, 1500);
  }
  return true;
}

// ===================== ACCUEIL (/start) =====================
// Personnalise ce message et cette image comme tu veux : c'est ce que voit
// chaque nouvel utilisateur en ouvrant le bot. Mets un lien direct vers une
// image (ex : hébergée sur https://imgbb.com ou https://catbox.moe) dans
// WELCOME_IMAGE_URL, ou laisse "" pour un message texte seul, sans photo.
const WELCOME_IMAGE_URL = "https://i.ibb.co/TMMHn8d8/Chat-GPT-Image-21-sept-2026-02-36-00.png";

// Échappe les caractères réservés du HTML pour parse_mode: "HTML".
// (Bien plus fiable que l'ancien parse_mode "Markdown" de Telegram, qui
// plante dès qu'un `*`, `_` ou backtick n'est pas parfaitement apparié —
// ex. c'était le cas ici si BOT_DISPLAY_NAME contenait un caractère spécial.)
function escapeHtml(str) {
  return String(str)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

// Convertit un texte en "petites capitales" unicode (ᴀʙᴄᴅᴇ...), pour le style
// des libellés dans les cartes (/pair). Purement décoratif ; les lettres sans
// équivalent (s, x) restent inchangées.
const SMALL_CAPS_MAP = {
  a: "ᴀ", b: "ʙ", c: "ᴄ", d: "ᴅ", e: "ᴇ", f: "ꜰ", g: "ɢ", h: "ʜ", i: "ɪ",
  j: "ᴊ", k: "ᴋ", l: "ʟ", m: "ᴍ", n: "ɴ", o: "ᴏ", p: "ᴘ", q: "ǫ", r: "ʀ",
  s: "s", t: "ᴛ", u: "ᴜ", v: "ᴠ", w: "ᴡ", x: "x", y: "ʏ", z: "ᴢ",
};
function smallCaps(str) {
  return String(str)
    .toLowerCase()
    .split("")
    .map((c) => SMALL_CAPS_MAP[c] || c)
    .join("");
}

// Carte "Pairing Code" façon capture d'écran fournie : encadré en
// caractères de dessin de boîte (rendu aligné grâce à <pre>, en police
// à chasse fixe), libellés en petites capitales. Le bouton "Copier le
// code" utilise le bouton natif Telegram copy_text (Bot API ≥ 8.0) :
// le code est copié dans le presse-papier du téléphone sans quitter la
// conversation.
function pairingCard(phoneNumber, code) {
  const num = escapeHtml(phoneNumber || "?");
  const cod = escapeHtml(code);
  return (
    "<pre>╭─❰ Pairing Code ❱──╮\n" +
    `┃ ↔ ${smallCaps("Number")} : ${num}\n` +
    `┃ ↔ ${smallCaps("Pairing code")} : ${cod}\n` +
    "╰────────────────────╯</pre>\n\n" +
    // Doublon volontaire du code en <code> : copie au tap, en secours si le
    // bouton "Copier le code" ci-dessous ne s'affiche pas (vieille version
    // de l'app Telegram, par ex.).
    `👉 <code>${cod}</code> <i>(touche pour copier)</i>\n\n` +
    "📱 WhatsApp → Paramètres → Appareils liés → Lier un appareil → " +
    '"Lier avec un numéro de téléphone", puis entre ce code ' +
    "⏱ <b>RAPIDEMENT</b> (il expire après environ 60 secondes)."
  );
}
function pairingCardKeyboard(code) {
  return { inline_keyboard: [[{ text: "📋 Copier le code", copy_text: { text: code } }]] };
}

function welcomeText() {
  const name = escapeHtml(cfg.BOT_DISPLAY_NAME || "Bot WhatsApp");
  return (
    `🤖 <b>${name}</b>\n` +
    `<i>Ton propre bot WhatsApp, en quelques secondes.</i>\n\n` +
    "<blockquote>" +
    "📌 <b>Comment démarrer</b>\n" +
    "<code>/pair &lt;ton_numéro&gt;</code>\n" +
    "<i>Exemple :</i> <code>/pair 2250700000000</code>\n\n" +
    "Tu recevras un code à entrer dans WhatsApp :\n" +
    "<i>Paramètres → Appareils liés → Lier avec un numéro de téléphone</i>" +
    "</blockquote>\n" +
    "👇 Besoin d'aide ou envie d'écrire directement ? Utilise les boutons ci-dessous."
  );
}

function welcomeKeyboard() {
  return {
    inline_keyboard: [
      [{ text: "📖 Comment ça marche", callback_data: "help" }],
      [{ text: "📊 Mon statut", callback_data: "status" }],
      [
        { text: "💬 WhatsApp", url: "https://wa.me/2250788523990" },
        { text: "✈️ Telegram", url: "https://t.me/Ariel_475" },
      ],
    ],
  };
}

bot.onText(/^\/start(?:@\w+)?$/i, (msg) => {
  const chatId = msg.chat.id;
  const opts = { parse_mode: "HTML", reply_markup: welcomeKeyboard() };
  if (WELCOME_IMAGE_URL) {
    bot
      .sendPhoto(chatId, WELCOME_IMAGE_URL, { caption: welcomeText(), ...opts })
      .catch((e) => console.error("⚠️  Envoi photo /start échoué :", e.message));
  } else {
    reply(chatId, welcomeText(), opts);
  }
});

// Un bouton du clavier ci-dessus a été appuyé.
bot.on("callback_query", (query) => {
  const chatId = query.message.chat.id;
  bot.answerCallbackQuery(query.id).catch(() => {});
  if (query.data === "help") {
    reply(
      chatId,
      title("📖", "Comment ça marche") +
        box([
          "1️⃣ Envoie <code>/pair &lt;numéro&gt;</code>",
          "2️⃣ Tu reçois un code d'appairage ici",
          "3️⃣ WhatsApp → Appareils liés → Lier avec un numéro de téléphone → entre le code",
          "4️⃣ ✅ Ton bot est actif !",
        ])
    );
  } else if (query.data === "status") {
    sendStatus(chatId);
  } else if (query.data === "admin_stats") {
    sendStats(chatId);
  } else if (query.data === "admin_instances") {
    sendInstancesList(chatId);
  } else if (query.data === "admin_name") {
    sendNameUsage(chatId);
  } else if (query.data === "admin_kill") {
    sendKillUsage(chatId);
  } else if (query.data === "admin_killall") {
    sendKillallUsage(chatId);
  }
});

// ===================== COMMANDES TELEGRAM =====================
// Logique d'appairage commune à Telegram (/pair) et au site web.
// key = chatId Telegram ou "web_<token>". Renvoie { ok:true } ou { ok:false, title, error }.
function requestPair(key, rawNumber, displayName) {
  key = String(key);
  const phoneNumber = String(rawNumber || "").replace(/[^0-9]/g, "");

  if (phoneNumber.length < 8 || phoneNumber.length > 15) {
    return { ok: false, title: "Numéro invalide", error: "Format attendu : numéro avec indicatif pays (ex : 2250700000000)." };
  }
  if (running.has(key)) {
    return { ok: false, title: "Instance déjà active", error: "Arrête d'abord ton instance si tu veux changer de numéro." };
  }

  // Un même numéro ne peut être lié que par UN utilisateur à la fois.
  for (const [otherId, e] of Object.entries(registry.instances)) {
    if (otherId !== key && e.phoneNumber === phoneNumber && ["pairing", "reconnecting", "connected"].includes(e.status)) {
      return { ok: false, title: "Numéro déjà utilisé", error: "Ce numéro est déjà lié à une autre instance." };
    }
  }

  if (MAX_INSTANCES > 0 && running.size >= MAX_INSTANCES) {
    return { ok: false, title: "Serveur plein", error: "Réessaie plus tard." };
  }

  // Changement de numéro : l'ancienne session ne correspond plus au nouveau compte.
  const existing = registry.instances[key];
  if (existing && existing.phoneNumber && existing.phoneNumber !== phoneNumber) {
    wipeInstanceData(key);
  }

  registry.instances[key] = {
    ...(registry.instances[key] || {}),
    telegramName: displayName || "Sans nom",
  };
  saveRegistry();

  if (isWeb(key)) {
    const l = liveOf(key);
    l.code = null;
    l.notice = null;
  }

  notifyAdmin(
    key,
    title("🆕", "Nouvel appairage") +
      `${escapeHtml(instanceLabel(key))} vient de lancer un appairage avec le numéro <code>${escapeHtml(phoneNumber)}</code> — tu seras prévenu ici dès que la connexion WhatsApp aboutit.`
  );

  startInstance(key, phoneNumber);
  return { ok: true };
}

bot.onText(/^\/pair(?:@\w+)?\s+([0-9+\s]+)/i, (msg, match) => {
  const chatId = msg.chat.id;

  // Toujours en message privé : dans un groupe, le numéro serait visible de
  // tous les membres.
  if (msg.chat.type !== "private") {
    reply(chatId, title("🔒", "Message privé requis") + "Écris-moi en message privé pour utiliser /pair.");
    return;
  }

  const fullName = [msg.from?.first_name, msg.from?.last_name].filter(Boolean).join(" ");
  const usernamePart = msg.from?.username ? ` (@${msg.from.username})` : "";
  const res = requestPair(chatId, match[1], (fullName || "Sans nom") + usernamePart);
  if (!res.ok) reply(chatId, title("❌", res.title) + escapeHtml(res.error));
});

bot.onText(/^\/pair(?:@\w+)?\s*$/i, (msg) => {
  reply(
    msg.chat.id,
    title("📌", "Utilisation") +
      box([
        "<code>/pair &lt;ton_numéro&gt;</code>",
        "<i>Exemple :</i> <code>/pair 2250788523990</code>",
      ])
  );
});

bot.onText(/^\/status(?:@\w+)?$/i, (msg) => {
  sendStatus(msg.chat.id);
});
function sendStatus(chatId) {
  const entry = registry.instances[chatId];
  if (!entry) {
    reply(chatId, title("ℹ️", "Aucune instance") + "Envoie <code>/pair &lt;numéro&gt;</code> pour en créer une.");
    return;
  }
  reply(
    chatId,
    title("📊", "Ton instance") +
      box([
        `📱 <b>Numéro :</b> <code>${escapeHtml(entry.phoneNumber || "?")}</code>`,
        `⚙️ <b>Statut :</b> ${statusBadge(entry.status)}${running.has(String(chatId)) ? " • processus actif" : ""}`,
        `🕒 <b>Mise à jour :</b> ${escapeHtml(new Date(entry.updatedAt || entry.createdAt).toLocaleString("fr-FR"))}`,
      ])
  );
}

bot.onText(/^\/stop(?:@\w+)?$/i, (msg) => {
  const chatId = msg.chat.id;
  const stopped = stopInstance(chatId, "demande utilisateur");
  reply(
    chatId,
    stopped
      ? title("🛑", "Instance arrêtée") + "Tes données sont conservées, /pair pour la relancer."
      : title("ℹ️", "Rien à arrêter") + "Tu n'as pas d'instance active."
  );
});

bot.onText(/^\/stats(?:@\w+)?$/i, (msg) => {
  sendStats(msg.chat.id);
});
function sendStats(chatId) {
  if (!isAdmin(chatId)) return;
  const all = Object.values(registry.instances);
  const byStatus = {};
  for (const e of all) byStatus[e.status] = (byStatus[e.status] || 0) + 1;
  reply(
    chatId,
    title("📊", "Vue d'ensemble") +
      box([
        `👥 <b>Instances :</b> ${all.length} au total`,
        `🟢 <b>Processus actifs :</b> ${running.size}${MAX_INSTANCES > 0 ? ` / ${MAX_INSTANCES}` : " (pas de limite)"}`,
        ...Object.entries(byStatus).map(([s, n]) => `${statusBadge(s)} : <b>${n}</b>`),
      ])
  );
}

// Aide-mémoire pour l'admin : rappelle la syntaxe exacte de toutes les
// commandes réservées à l'admin, sans avoir à les mettre dans le menu "/"
// public de BotFather (donc invisible pour les autres utilisateurs — il
// suffit de taper /admin de mémoire, même approximative, pour se rappeler).
// Les boutons ci-dessous appellent directement les mêmes fonctions que les
// commandes tapées (voir callback_query plus haut) : pour /name et /kill
// (qui ont besoin d'un argument), le bouton affiche le mode d'emploi exact,
// exactement comme taper la commande seule le ferait.
function adminKeyboard() {
  return {
    inline_keyboard: [
      [{ text: "📊 Stats", callback_data: "admin_stats" }, { text: "📋 Instances", callback_data: "admin_instances" }],
      [{ text: "✏️ Name", callback_data: "admin_name" }],
      [{ text: "🛑 Kill", callback_data: "admin_kill" }, { text: "🛑 Killall", callback_data: "admin_killall" }],
    ],
  };
}
bot.onText(/^\/admin(?:@\w+)?$/i, (msg) => {
  const chatId = msg.chat.id;
  if (!isAdmin(chatId)) return;
  reply(
    chatId,
    "<pre>┏━━ ⚙ Admin Panel ━━┓\n" +
      "┃ 📊 /stats\n" +
      "┃ 📋 /instances\n" +
      "┃ ✏️ /name &lt;chatId&gt; &lt;nom&gt;\n" +
      "┃ 🛑 /kill &lt;chatId&gt; [wipe]\n" +
      "┃ 🛑 /killall [wipe]\n" +
      "┗━━━━━━━━━━━━━━━━━━━━┛</pre>\n" +
      "💡 <i>Commence toujours par /instances pour retrouver le bon chatId avant /name ou /kill.</i>",
    { reply_markup: adminKeyboard() }
  );
});

// Liste toutes les instances connues (actives ou non) avec leur chatId, pour
// que l'admin sache quel identifiant utiliser avec /kill.
bot.onText(/^\/instances(?:@\w+)?$/i, (msg) => {
  sendInstancesList(msg.chat.id);
});
function sendInstancesList(chatId) {
  if (!isAdmin(chatId)) return;
  const entries = Object.entries(registry.instances);
  if (!entries.length) {
    reply(chatId, title("📋", "Instances") + "Aucune instance enregistrée.");
    return;
  }
  const lines = entries.map(([id, e]) => {
    const active = running.has(String(id)) ? "🟢" : "⚪";
    return (
      `${active} <b>${escapeHtml(instanceLabel(id))}</b> — <code>${escapeHtml(id)}</code>\n` +
      `   📱 ${escapeHtml(e.phoneNumber || "?")} · ${statusBadge(e.status)}`
    );
  });
  reply(chatId, title("📋", `${entries.length} instance(s)`) + lines.join("\n\n"));
}

// Donne (ou change) un nom lisible à une instance, pour l'identifier
// facilement dans /instances et /kill sans se fier au chatId brut — utile
// par exemple pour noter "Ami Paul" au lieu d'un numéro Telegram illisible.
bot.onText(/^\/name(?:@\w+)?\s+(\S+)\s+(.+)/i, (msg, match) => {
  const chatId = msg.chat.id;
  if (!isAdmin(chatId)) return;
  const targetId = match[1];
  const label = match[2].trim();

  if (!registry.instances[targetId]) {
    reply(chatId, title("❌", "Instance introuvable") + `Aucune instance pour l'id <code>${escapeHtml(targetId)}</code>. Utilise /instances pour voir la liste.`);
    return;
  }
  registry.instances[targetId].label = label;
  saveRegistry();
  reply(chatId, title("✅", "Instance renommée") + `<code>${escapeHtml(targetId)}</code> s'appelle maintenant <b>${escapeHtml(label)}</b>.`);
});

bot.onText(/^\/name(?:@\w+)?\s*$/i, (msg) => {
  sendNameUsage(msg.chat.id);
});
function sendNameUsage(chatId) {
  if (!isAdmin(chatId)) return;
  reply(
    chatId,
    title("📌", "Utilisation") +
      box([
        "<code>/name &lt;chatId&gt; &lt;nom&gt;</code>",
        "<i>Exemple :</i> <code>/name 123456789 Ami Paul</code>",
      ]) +
      "\n💡 <i>Utilise /instances pour voir la liste des chatId.</i>"
  );
}

// Arrête l'instance de N'IMPORTE QUEL utilisateur (identifié par son chatId
// Telegram, visible via /instances) — contrairement à /stop qui n'agit que
// sur SA PROPRE instance. Ajouter "wipe" en second argument pour effacer
// aussi les données (session, contacts, réglages) de cette instance.
bot.onText(/^\/kill(?:@\w+)?\s+(\S+)(?:\s+(wipe))?/i, (msg, match) => {
  const chatId = msg.chat.id;
  if (!isAdmin(chatId)) return;
  const targetId = match[1];
  const wipe = Boolean(match[2]);

  if (!registry.instances[targetId] && !running.has(targetId)) {
    reply(chatId, title("❌", "Instance introuvable") + `Aucune instance pour l'id <code>${escapeHtml(targetId)}</code>. Utilise /instances pour voir la liste.`);
    return;
  }

  const wasRunning = stopInstance(targetId, `tué par l'admin (${chatId})`, { wipe });
  if (!wasRunning && wipe) {
    // Instance déjà arrêtée : stopInstance() n'efface rien dans ce cas
    // (son "wipe" différé ne se déclenche que si un process tournait), donc
    // on efface directement ici.
    wipeInstanceData(targetId);
  }
  reply(
    chatId,
    title("🛑", "Instance arrêtée") +
      box([
        `👤 <b>${escapeHtml(instanceLabel(targetId))}</b> (<code>${escapeHtml(targetId)}</code>)`,
        `💾 ${wipe ? "Données effacées" : "Données conservées"}${wasRunning ? "" : " · elle n'était déjà plus active"}`,
      ])
  );
  reply(
    targetId,
    title("⚠️", "Instance arrêtée par l'administrateur") +
      (wipe ? "Tes données ont aussi été effacées." : "Tes données sont conservées, /pair pour la relancer.")
  );
});

bot.onText(/^\/kill(?:@\w+)?\s*$/i, (msg) => {
  sendKillUsage(msg.chat.id);
});
function sendKillUsage(chatId) {
  if (!isAdmin(chatId)) return;
  reply(
    chatId,
    title("📌", "Utilisation") +
      box([
        "<code>/kill &lt;chatId&gt; [wipe]</code>",
        "<i>Exemple :</i> <code>/kill 123456789</code>",
        "<i>Avec effacement :</i> <code>/kill 123456789 wipe</code>",
      ]) +
      "\n💡 <i>Utilise /instances pour voir la liste des chatId.</i>"
  );
}

// Arrête TOUTES les instances actives d'un coup (ex : avant une maintenance
// serveur). Ajouter "wipe" pour effacer aussi les données de chacune.
bot.onText(/^\/killall(?:@\w+)?(?:\s+(wipe))?$/i, (msg, match) => {
  const chatId = msg.chat.id;
  if (!isAdmin(chatId)) return;
  killAllInstances(chatId, Boolean(match[1]));
});
function killAllInstances(chatId, wipe) {
  if (!isAdmin(chatId)) return;
  const ids = [...running.keys()];
  if (!ids.length) {
    reply(chatId, title("ℹ️", "Rien à arrêter") + "Aucune instance active.");
    return;
  }
  for (const id of ids) {
    stopInstance(id, `killall admin (${chatId})`, { wipe });
    reply(
      id,
      title("⚠️", "Instance arrêtée par l'administrateur") +
        (wipe ? "Tes données ont aussi été effacées." : "Tes données sont conservées, /pair pour la relancer.")
    );
  }
  reply(
    chatId,
    title("🛑", `${ids.length} instance(s) arrêtée(s)`) +
      box([
        `👥 ${ids.map((id) => escapeHtml(instanceLabel(id))).join(", ")}`,
        `💾 ${wipe ? "Données effacées" : "Données conservées"}`,
      ])
  );
}
// Le bouton "Killall" du panneau admin ne déclenche PAS l'arrêt directement
// (un tap accidentel arrêterait le bot de TOUT LE MONDE) : il rappelle juste
// la commande, comme /kill et /name.
function sendKillallUsage(chatId) {
  if (!isAdmin(chatId)) return;
  reply(
    chatId,
    title("📌", "Utilisation") +
      box([
        "<code>/killall [wipe]</code>",
        "<i>Arrête les instances actives de TOUT LE MONDE.</i>",
        "<i>Avec effacement :</i> <code>/killall wipe</code>",
      ])
  );
}

// ===================== RELANCE AUTOMATIQUE AU DÉMARRAGE =====================
// Toute instance qui était connectée avant l'arrêt de l'orchestrateur est
// relancée automatiquement — sans redemander de code, puisque la session
// existe déjà sur disque. Les instances restées en "pairing" ne sont PAS
// relancées : l'utilisateur n'attend plus de code, on ne lui en renvoie pas.
{
  const relaunched = [];
  for (const [chatId, entry] of Object.entries(registry.instances)) {
    if ((entry.status === "connected" || entry.status === "reconnecting") && hasSession(chatId)) {
      console.log(`↻ Relance automatique de l'instance ${chatId} (${entry.phoneNumber})...`);
      silentBoot.add(String(chatId));
      relaunched.push(chatId);
      startInstance(chatId, entry.phoneNumber);
    } else if (entry.status === "pairing") {
      entry.status = "stopped";
    }
  }
  saveRegistry();
  // Un seul message récap pour l'admin plutôt qu'un message par instance
  // (notifyAdmin() n'est pas utilisable ici : elle est faite pour prévenir
  // l'admin à propos d'UN AUTRE utilisateur, et s'auto-bloque si on lui passe
  // l'ID de l'admin lui-même).
  if (relaunched.length && cfg.ADMIN_CHAT_ID) {
    reply(
      cfg.ADMIN_CHAT_ID,
      `🔄 Orchestrateur redémarré : ${relaunched.length} instance(s) relancée(s) silencieusement (${relaunched.map((id) => instanceLabel(id)).join(", ")}).`
    );
  }
}

// ===================== ARRÊT PROPRE =====================
// Sans ça, arrêter l'orchestrateur laisserait les instances tourner en
// arrière-plan ; au redémarrage, elles seraient relancées EN DOUBLE avec la
// même session WhatsApp, ce qui déconnecte/corrompt les sessions.
function shutdown(signal) {
  console.log(`\n${signal} reçu : arrêt des ${running.size} instance(s)...`);
  for (const [chatId, r] of running) {
    clearTimers(r);
    running.delete(chatId); // évite la relance automatique
    try {
      r.child.kill("SIGTERM");
    } catch {}
  }
  saveRegistry();
  setTimeout(() => process.exit(0), 1500);
}
process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));

// ===================== SITE WEB =====================
if (cfg.WEB_ENABLED) {
  require("./web").start({
    cfg,
    requestPair,
    stopInstance,
    getState(id) {
      id = String(id);
      const e = registry.instances[id];
      const l = live.get(id) || {};
      if (!e) return { status: "none" };
      return {
        status: e.status || "none",
        phone: e.phoneNumber || null,
        name: e.whatsappName || null,
        code: l.code || null,
        notice: l.notice || null,
        active: running.has(id),
      };
    },
    name: cfg.BOT_DISPLAY_NAME,
    admin: {
      password: process.env.ADMIN_PASSWORD || cfg.WEB_ADMIN_PASSWORD || "",
      list() {
        return Object.entries(registry.instances).map(([id, e]) => ({
          id,
          label: instanceLabel(id),
          phone: e.phoneNumber || null,
          status: e.status || "unknown",
          active: running.has(id),
          source: isWeb(id) ? "site" : "telegram",
          createdAt: e.createdAt || null,
          updatedAt: e.updatedAt || null,
        }));
      },
      stats() {
        const all = Object.values(registry.instances);
        const byStatus = {};
        for (const e of all) byStatus[e.status] = (byStatus[e.status] || 0) + 1;
        return { total: all.length, running: running.size, max: MAX_INSTANCES, byStatus };
      },
      // remove = supprime aussi l'instance de la liste (dossier compris).
      kill(id, { wipe = false, remove = false } = {}) {
        id = String(id);
        if (!registry.instances[id] && !running.has(id)) return { ok: false, error: "Instance introuvable." };
        const wasRunning = stopInstance(id, "arrêtée par l'admin (web)", { wipe: wipe && !remove });
        if (remove) {
          setTimeout(() => {
            if (running.has(id)) return;
            try { fs.rmSync(path.join(cfg.INSTANCES_DIR, id), { recursive: true, force: true }); } catch {}
            delete registry.instances[id];
            live.delete(id);
            saveRegistry();
          }, 1500);
        } else if (!wasRunning && wipe) {
          wipeInstanceData(id);
        }
        return { ok: true, wasRunning };
      },
      killAll({ wipe = false } = {}) {
        const ids = [...running.keys()];
        for (const id of ids) stopInstance(id, "killall admin (web)", { wipe });
        return { ok: true, count: ids.length };
      },
      rename(id, label) {
        id = String(id);
        if (!registry.instances[id]) return { ok: false, error: "Instance introuvable." };
        registry.instances[id].label = String(label || "").slice(0, 60);
        saveRegistry();
        return { ok: true };
      },
    },
  });
}

console.log("✅ Orchestrateur démarré. Prêt.");
