const fs = require("fs");
const path = require("path");
const axios = require("axios");

const TELEGRAM_BOT_TOKEN = String(process.env.KL_TELEGRAM_BOT_TOKEN || "").trim();
const TELEGRAM_CHAT_IDS = String(
  process.env.KL_TELEGRAM_CHAT_IDS
  || process.env.KL_TELEGRAM_CHAT_ID
  || ""
)
  .split(/[,\s;]+/)
  .map((value) => String(value || "").trim())
  .filter(Boolean);

const TELEGRAM_NOTIFIER_ENABLED = Boolean(TELEGRAM_BOT_TOKEN && TELEGRAM_CHAT_IDS.length);
const TELEGRAM_API_TIMEOUT_MS = Math.max(4000, Number(process.env.KL_TELEGRAM_TIMEOUT_MS || 12000));
const TELEGRAM_STATE_PATH = path.join(__dirname, "..", "data", "telegram-message-state.json");
const TELEGRAM_MAX_TEXT_LENGTH = 3900;

const state = {
  loaded: false,
  dirty: false,
  data: {
    version: 1,
    byConversation: {}
  }
};

let sendQueue = Promise.resolve();

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const normalizeText = (value) => String(value || "").replace(/\s+/g, " ").trim();

const truncateText = (value, maxLength) => {
  const text = normalizeText(value);
  if (!maxLength || text.length <= maxLength) return text;
  return `${text.slice(0, Math.max(0, maxLength - 3))}...`;
};

const ensureStateDir = () => {
  try {
    fs.mkdirSync(path.dirname(TELEGRAM_STATE_PATH), { recursive: true });
  } catch (error) {
    // ignore directory creation issues; saving will fail and be logged later
  }
};

const normalizeStateEntry = (entry) => {
  if (!entry) return { fingerprint: "", updatedAt: 0 };
  if (typeof entry === "string") {
    return { fingerprint: entry, updatedAt: 0 };
  }
  return {
    fingerprint: String(entry.fingerprint || "").trim(),
    updatedAt: Number(entry.updatedAt || 0)
  };
};

const loadState = () => {
  if (state.loaded) return;
  state.loaded = true;
  try {
    const raw = fs.readFileSync(TELEGRAM_STATE_PATH, "utf8");
    const parsed = JSON.parse(raw);
    const byConversation = parsed && typeof parsed === "object" && parsed.byConversation && typeof parsed.byConversation === "object"
      ? parsed.byConversation
      : {};
    const normalized = {};
    Object.entries(byConversation).forEach(([key, value]) => {
      const normalizedKey = String(key || "").trim();
      if (!normalizedKey) return;
      const entry = normalizeStateEntry(value);
      if (!entry.fingerprint) return;
      normalized[normalizedKey] = entry;
    });
    state.data = {
      version: 1,
      byConversation: normalized
    };
  } catch (error) {
    state.data = {
      version: 1,
      byConversation: {}
    };
  }
};

const pruneState = () => {
  const entries = Object.entries(state.data.byConversation || {});
  if (entries.length <= 5000) return;
  entries.sort((left, right) => Number(right[1]?.updatedAt || 0) - Number(left[1]?.updatedAt || 0));
  state.data.byConversation = Object.fromEntries(entries.slice(0, 3500));
  state.dirty = true;
};

const saveState = () => {
  if (!state.dirty) return;
  ensureStateDir();
  const payload = {
    version: 1,
    updatedAt: new Date().toISOString(),
    byConversation: state.data.byConversation
  };
  try {
    fs.writeFileSync(TELEGRAM_STATE_PATH, JSON.stringify(payload, null, 2));
    state.dirty = false;
  } catch (error) {
    console.log(`[telegramNotifier] Failed to save state: ${error.message || error}`);
  }
};

const buildConversationKey = (item, index = 0) => {
  const accountId = String(item?.accountId || "").trim();
  const conversationId = String(item?.conversationId || "").trim();
  const conversationUrl = String(item?.conversationUrl || "").trim();
  const itemId = String(item?.id || "").trim();
  const sender = normalizeText(item?.sender || "");
  const adTitle = normalizeText(item?.adTitle || "");
  const fallback = `${sender}|${adTitle}|${index}`;
  return `acc:${accountId || "unknown"}|cid:${conversationId || conversationUrl || itemId || fallback}`;
};

const buildFingerprint = (item) => {
  const text = truncateText(item?.message || "", 1200);
  const date = String(item?.date || "").trim();
  const time = String(item?.time || "").trim();
  const sender = truncateText(item?.sender || "", 120);
  const adTitle = truncateText(item?.adTitle || "", 180);
  return `${sender}|${adTitle}|${text}|${date}|${time}`;
};

const shouldNotify = (item) => {
  if (!item || typeof item !== "object") return false;
  if (!item.unread) return false;
  const text = normalizeText(item.message || "");
  if (!text) return false;
  const sender = normalizeText(item.sender || "").toLowerCase();
  if (sender === "вы" || sender === "you") return false;
  return true;
};

const formatTelegramMessage = (item) => {
  const accountName = truncateText(item?.accountName || "", 140) || "-";
  const senderName = truncateText(item?.sender || "", 140) || "-";
  const adTitle = truncateText(item?.adTitle || "", 240) || "-";
  const message = truncateText(item?.message || "", 2400) || "-";
  const dateText = String(item?.date || "").trim();
  const timeText = String(item?.time || "").trim();
  const timeLine = [dateText, timeText].filter(Boolean).join(" ").trim();
  const lines = [
    "New message (Kleinanzeigen)",
    `Account: ${accountName}`,
    `Sender: ${senderName}`,
    `Ad: ${adTitle}`,
    `Message: ${message}`
  ];
  if (timeLine) lines.push(`Time: ${timeLine}`);
  return truncateText(lines.join("\n"), TELEGRAM_MAX_TEXT_LENGTH);
};

const sendMessageToTelegram = async (text) => {
  if (!TELEGRAM_NOTIFIER_ENABLED) return false;
  const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;
  let delivered = false;

  for (const chatId of TELEGRAM_CHAT_IDS) {
    try {
      await axios.post(
        url,
        {
          chat_id: chatId,
          text,
          disable_web_page_preview: true
        },
        {
          timeout: TELEGRAM_API_TIMEOUT_MS
        }
      );
      delivered = true;
    } catch (error) {
      const reason = error?.response?.data?.description
        || error?.message
        || "unknown";
      console.log(`[telegramNotifier] Failed to send to chat ${chatId}: ${reason}`);
    }
  }

  return delivered;
};

const notifyConversations = async (conversations = []) => {
  if (!TELEGRAM_NOTIFIER_ENABLED) {
    return { enabled: false, queued: 0, sent: 0, skipped: 0 };
  }
  loadState();

  const list = Array.isArray(conversations) ? conversations : [];
  let queued = 0;
  let sent = 0;
  let skipped = 0;

  for (let index = 0; index < list.length; index += 1) {
    const item = list[index];
    if (!shouldNotify(item)) {
      skipped += 1;
      continue;
    }

    const key = buildConversationKey(item, index);
    const fingerprint = buildFingerprint(item);
    if (!key || !fingerprint) {
      skipped += 1;
      continue;
    }

    const previous = normalizeStateEntry(state.data.byConversation[key]);
    if (previous.fingerprint && previous.fingerprint === fingerprint) {
      skipped += 1;
      continue;
    }

    queued += 1;
    const text = formatTelegramMessage(item);
    const delivered = await sendMessageToTelegram(text);
    if (!delivered) continue;

    state.data.byConversation[key] = {
      fingerprint,
      updatedAt: Date.now()
    };
    state.dirty = true;
    sent += 1;
    await sleep(120);
  }

  pruneState();
  saveState();
  return { enabled: true, queued, sent, skipped };
};

const queueTelegramConversationNotifications = (conversations = []) => {
  sendQueue = sendQueue
    .catch(() => {})
    .then(() => notifyConversations(conversations));
  return sendQueue;
};

module.exports = {
  queueTelegramConversationNotifications,
  isTelegramNotifierEnabled: () => TELEGRAM_NOTIFIER_ENABLED
};
