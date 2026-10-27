// ===================== CONFIG DE L'ORCHESTRATEUR MULTI-INSTANCES =====================
// Ce fichier pilote le SITE D'APPAIRAGE : n'importe qui peut, depuis le site,
// obtenir sa PROPRE instance du bot WhatsApp, isolée des autres (session,
// contacts, réglages séparés), tournant à côté des autres sur ce même serveur.
//
// ⚠️ Ne partage jamais ce fichier une fois rempli.
// =====================================================================================

const path = require("path");

module.exports = {
  // Nom du bot.
  BOT_DISPLAY_NAME: "ARIEL-BOT",

  // ---- SITE WEB D'APPAIRAGE ----
  // true = lance l'API d'appairage utilisée par ton site.
  WEB_ENABLED: true,

  // Port d'écoute. Les hébergeurs (Render, Railway, panels...) fournissent
  // la variable PORT ; sinon 3000.
  WEB_PORT: Number(process.env.PORT || process.env.SERVER_PORT) || 3000,

  // Sites autorisés à appeler l'API (sans "/" à la fin). "*" = tous.
  WEB_ALLOWED_ORIGINS: ["https://arielbot-pro.vercel.app"],

  // Anti-abus : nombre max de demandes d'appairage par adresse IP et par heure.
  WEB_MAX_PAIRS_PER_IP_PER_HOUR: 5,

  // Mot de passe de la page admin (https://TON-SERVEUR/admin). Vide = page
  // désactivée. Choisis un mot de passe LONG et unique. Tu peux aussi le
  // mettre dans la variable d'environnement ADMIN_PASSWORD du panel.
  WEB_ADMIN_PASSWORD: "",

  // Nombre maximum d'instances actives en même temps. 0 = pas de limite.
  // (Chaque instance est un processus Node : surveille la RAM de ton serveur
  // et mets une limite ici si tu en as besoin.)
  MAX_INSTANCES: 0,

  // Fichier index.js à lancer pour chaque instance (le bot WhatsApp
  // partagé — un seul exemplaire de code pour tout le monde).
  BOT_ENTRY: path.join(__dirname, "..", "index.js"),

  // Dossier où chaque utilisateur reçoit son propre sous-dossier
  // (instances/<chatId>/session, settings.json, known-contacts.json...).
  INSTANCES_DIR: path.join(__dirname, "instances"),

  // Fichier où l'orchestrateur retient qui a quelle instance, pour tout
  // relancer automatiquement si le serveur redémarre.
  REGISTRY_FILE: path.join(__dirname, "registry.json"),
};
