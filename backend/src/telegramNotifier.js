const fs = require("fs");
const path = require("path");
const axios = require("axios");

const TELEGRAM_BOT_TOKEN = String(
  process.env.KL_TELEGRAM_BOT_TOKEN
  || "8160454540:AAFNw45RFKPJf2_QzMXsoHcBa8h-RVRRwvk"
).trim();
const TELEGRAM_DEFAULT_CHAT_IDS = String(
  process.env.KL_TELEGRAM_CHAT_IDS
  || process.env.KL_TELEGRAM_CHAT_ID
  || "5583690035"
)
  .split(/[,\s;]+/)
  .map((value) => String(value || "").trim())
  .filter(Boolean);
const TELEGRAM_ALLOW_OWNERLESS_FALLBACK = String(
  process.env.KL_TELEGRAM_ALLOW_OWNERLESS_FALLBACK || "0"
).trim() === "1";

const TELEGRAM_NOTIFIER_ENABLED = Boolean(TELEGRAM_BOT_TOKEN);
const TELEGRAM_API_TIMEOUT_MS = Math.max(4000, Number(process.env.KL_TELEGRAM_TIMEOUT_MS || 12000));
const TELEGRAM_STATE_PATH = path.join(__dirname, "..", "data", "telegram-message-state.json");
const TELEGRAM_MAX_TEXT_LENGTH = 3900;
const TELEGRAM_SYNC_INTERVAL_MS = Math.max(5000, Number(process.env.KL_TELEGRAM_SYNC_INTERVAL_MS || 15000));
const TELEGRAM_UPDATES_LIMIT = Math.max(1, Math.min(100, Number(process.env.KL_TELEGRAM_UPDATES_LIMIT || 100)));
const TELEGRAM_MAX_OWNERS = Math.max(100, Number(process.env.KL_TELEGRAM_MAX_OWNERS || 5000));
const TELEGRAM_AWAIT_TOKEN_TTL_MS = Math.max(60000, Number(process.env.KL_TELEGRAM_AWAIT_TOKEN_TTL_MS || 1800000));
const TELEGRAM_TOKEN_BUTTON_TEXT = "Ввести токен";
const TELEGRAM_POLL_INTERVAL_MS = Math.max(
  5000,
  Number(process.env.KL_TELEGRAM_POLL_INTERVAL_MS || TELEGRAM_SYNC_INTERVAL_MS)
);

const state = {
  loaded: false,
  dirty: false,
  data: {
    version: 2,
    byConversation: {},
    byOwner: {},
    updateOffset: 0
  }
};

let sendQueue = Promise.resolve();
let bindingResolver = null;
let syncInFlight = null;
let lastSyncAt = 0;
const awaitingTokenByChat = new Map();
let commandPollTimer = null;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const normalizeText = (value) => String(value || "").replace(/\s+/g, " ").trim();
const normalizeOwnerId = (value) => String(value || "").trim();
const normalizeChatId = (value) => String(value || "").trim();

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

const normalizeOwnerBindingEntry = (entry) => {
  if (!entry || typeof entry !== "object") {
    return { chatIds: [], updatedAt: 0 };
  }
  const chatIds = Array.isArray(entry.chatIds)
    ? entry.chatIds.map((chatId) => normalizeChatId(chatId)).filter(Boolean)
    : [];
  return {
    chatIds: Array.from(new Set(chatIds)),
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
    const rawByOwner = parsed && typeof parsed === "object" && parsed.byOwner && typeof parsed.byOwner === "object"
      ? parsed.byOwner
      : {};
    const byOwner = {};
    Object.entries(rawByOwner).forEach(([ownerId, entry]) => {
      const normalizedOwnerId = normalizeOwnerId(ownerId);
      if (!normalizedOwnerId) return;
      const normalizedEntry = normalizeOwnerBindingEntry(entry);
      if (!normalizedEntry.chatIds.length) return;
      byOwner[normalizedOwnerId] = normalizedEntry;
    });
    const updateOffset = Number(parsed?.updateOffset || 0);
    state.data = {
      version: 2,
      byConversation: normalized,
      byOwner,
      updateOffset: Number.isFinite(updateOffset) && updateOffset > 0 ? Math.floor(updateOffset) : 0
    };
  } catch (error) {
    state.data = {
      version: 2,
      byConversation: {},
      byOwner: {},
      updateOffset: 0
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

const pruneOwnerBindings = () => {
  const entries = Object.entries(state.data.byOwner || {});
  if (entries.length <= TELEGRAM_MAX_OWNERS) return;
  entries.sort((left, right) => Number(right[1]?.updatedAt || 0) - Number(left[1]?.updatedAt || 0));
  state.data.byOwner = Object.fromEntries(entries.slice(0, TELEGRAM_MAX_OWNERS));
  state.dirty = true;
};

const saveState = () => {
  if (!state.dirty) return;
  ensureStateDir();
  const payload = {
    version: 2,
    updatedAt: new Date().toISOString(),
    byConversation: state.data.byConversation,
    byOwner: state.data.byOwner,
    updateOffset: state.data.updateOffset
  };
  try {
    fs.writeFileSync(TELEGRAM_STATE_PATH, JSON.stringify(payload, null, 2));
    state.dirty = false;
  } catch (error) {
    console.log(`[telegramNotifier] Failed to save state: ${error.message || error}`);
  }
};

const buildConversationKey = (item, index = 0, ownerId = "") => {
  const ownerKey = normalizeOwnerId(ownerId) || "ownerless";
  const accountId = String(item?.accountId || "").trim();
  const conversationId = String(item?.conversationId || "").trim();
  const conversationUrl = String(item?.conversationUrl || "").trim();
  const itemId = String(item?.id || "").trim();
  const sender = normalizeText(item?.sender || "");
  const adTitle = normalizeText(item?.adTitle || "");
  const fallback = `${sender}|${adTitle}|${index}`;
  return `owner:${ownerKey}|acc:${accountId || "unknown"}|cid:${conversationId || conversationUrl || itemId || fallback}`;
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

const sendMessageToTelegram = async ({ text, chatIds = [], replyMarkup = null } = {}) => {
  if (!TELEGRAM_NOTIFIER_ENABLED) return 0;
  const targets = Array.isArray(chatIds)
    ? Array.from(new Set(chatIds.map((chatId) => normalizeChatId(chatId)).filter(Boolean)))
    : [];
  if (!targets.length) return 0;
  const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;
  let delivered = 0;

  for (const chatId of targets) {
    try {
      const payload = {
        chat_id: chatId,
        text,
        disable_web_page_preview: true
      };
      if (replyMarkup && typeof replyMarkup === "object") {
        payload.reply_markup = replyMarkup;
      }
      await axios.post(
        url,
        payload,
        {
          timeout: TELEGRAM_API_TIMEOUT_MS
        }
      );
      delivered += 1;
    } catch (error) {
      const reason = error?.response?.data?.description
        || error?.message
        || "unknown";
      console.log(`[telegramNotifier] Failed to send to chat ${chatId}: ${reason}`);
    }
  }

  return delivered;
};

const parseCommand = (rawText) => {
  const text = String(rawText || "").trim();
  if (!text.startsWith("/")) return null;
  const parts = text.split(/\s+/).filter(Boolean);
  if (!parts.length) return null;
  const command = String(parts[0] || "").toLowerCase().replace(/@.+$/, "");
  return {
    command,
    payload: parts.slice(1).join(" ").trim()
  };
};

const createTokenKeyboard = () => ({
  keyboard: [[{ text: TELEGRAM_TOKEN_BUTTON_TEXT }]],
  resize_keyboard: true
});

const markAwaitingToken = (chatId) => {
  const normalizedChatId = normalizeChatId(chatId);
  if (!normalizedChatId) return;
  awaitingTokenByChat.set(normalizedChatId, Date.now() + TELEGRAM_AWAIT_TOKEN_TTL_MS);
};

const clearAwaitingToken = (chatId) => {
  const normalizedChatId = normalizeChatId(chatId);
  if (!normalizedChatId) return;
  awaitingTokenByChat.delete(normalizedChatId);
};

const isAwaitingToken = (chatId) => {
  const normalizedChatId = normalizeChatId(chatId);
  if (!normalizedChatId) return false;
  const expiresAt = Number(awaitingTokenByChat.get(normalizedChatId) || 0);
  if (!expiresAt) return false;
  if (expiresAt <= Date.now()) {
    awaitingTokenByChat.delete(normalizedChatId);
    return false;
  }
  return true;
};

const setTelegramTokenResolver = (resolver) => {
  bindingResolver = typeof resolver === "function" ? resolver : null;
};

const resolveOwnerByToken = (token) => {
  if (!bindingResolver) return { ok: false, reason: "Token resolver is not configured." };
  try {
    const result = bindingResolver(String(token || "").trim()) || {};
    const ownerId = normalizeOwnerId(result.ownerId || "");
    if (!ownerId) {
      return { ok: false, reason: String(result.reason || "Token is invalid.") };
    }
    return {
      ok: true,
      ownerId,
      label: String(result.label || "")
    };
  } catch (error) {
    return { ok: false, reason: String(error?.message || "Token validation failed.") };
  }
};

const bindOwnerChat = ({ ownerId, chatId }) => {
  const normalizedOwnerId = normalizeOwnerId(ownerId);
  const normalizedChatId = normalizeChatId(chatId);
  if (!normalizedOwnerId || !normalizedChatId) return false;
  const current = normalizeOwnerBindingEntry(state.data.byOwner[normalizedOwnerId]);
  if (!current.chatIds.includes(normalizedChatId)) {
    current.chatIds.push(normalizedChatId);
  }
  current.updatedAt = Date.now();
  state.data.byOwner[normalizedOwnerId] = current;
  state.dirty = true;
  return true;
};

const removeChatFromAllOwners = (chatId) => {
  const normalizedChatId = normalizeChatId(chatId);
  if (!normalizedChatId) return 0;
  let changed = 0;
  Object.entries(state.data.byOwner || {}).forEach(([ownerId, entry]) => {
    const normalizedEntry = normalizeOwnerBindingEntry(entry);
    const nextChatIds = normalizedEntry.chatIds.filter((value) => value !== normalizedChatId);
    if (nextChatIds.length === normalizedEntry.chatIds.length) return;
    changed += 1;
    if (!nextChatIds.length) {
      delete state.data.byOwner[ownerId];
      state.dirty = true;
      return;
    }
    state.data.byOwner[ownerId] = {
      chatIds: nextChatIds,
      updatedAt: Date.now()
    };
    state.dirty = true;
  });
  return changed;
};

const findOwnersByChat = (chatId) => {
  const normalizedChatId = normalizeChatId(chatId);
  if (!normalizedChatId) return [];
  return Object.entries(state.data.byOwner || {})
    .filter(([, entry]) => normalizeOwnerBindingEntry(entry).chatIds.includes(normalizedChatId))
    .map(([ownerId]) => ownerId);
};

const bindChatToOwner = ({ ownerId, chatId }) => {
  const normalizedOwnerId = normalizeOwnerId(ownerId);
  const normalizedChatId = normalizeChatId(chatId);
  if (!normalizedOwnerId || !normalizedChatId) return false;
  // One chat should map to one owner only to avoid cross-delivery.
  removeChatFromAllOwners(normalizedChatId);
  return bindOwnerChat({
    ownerId: normalizedOwnerId,
    chatId: normalizedChatId
  });
};

const isTokenButtonText = (text) => {
  return normalizeText(text).toLowerCase() === TELEGRAM_TOKEN_BUTTON_TEXT.toLowerCase();
};

const sendStartWithTokenButton = async (chatId) => {
  await sendMessageToTelegram({
    chatIds: [chatId],
    text: "Нажмите «Ввести токен», затем отправьте токен доступа одним сообщением.",
    replyMarkup: createTokenKeyboard()
  });
};

const sendTokenRequestPrompt = async (chatId) => {
  markAwaitingToken(chatId);
  await sendMessageToTelegram({
    chatIds: [chatId],
    text: "Введите токен доступа с сайта одним сообщением.",
    replyMarkup: createTokenKeyboard()
  });
};

const tryBindTokenForChat = async ({ chatId, token }) => {
  const normalizedChatId = normalizeChatId(chatId);
  const normalizedToken = String(token || "").trim();
  if (!normalizedChatId || !normalizedToken) {
    return { ok: false, reason: "Токен пустой." };
  }

  const resolved = resolveOwnerByToken(normalizedToken);
  if (!resolved.ok) {
    return { ok: false, reason: resolved.reason || "Неверный токен." };
  }

  bindChatToOwner({
    ownerId: resolved.ownerId,
    chatId: normalizedChatId
  });
  clearAwaitingToken(normalizedChatId);
  await sendMessageToTelegram({
    chatIds: [normalizedChatId],
    text: "Мониторинг сообщений работает.",
    replyMarkup: createTokenKeyboard()
  });
  return { ok: true, ownerId: resolved.ownerId };
};

const processIncomingCommand = async ({ chatId, text }) => {
  const normalizedChatId = normalizeChatId(chatId);
  const rawText = String(text || "").trim();
  if (!normalizedChatId || !rawText) return false;

  const parsed = parseCommand(rawText);
  if (parsed) {
    if (parsed.command === "/unbind" || parsed.command === "/stop") {
      clearAwaitingToken(normalizedChatId);
      const removed = removeChatFromAllOwners(normalizedChatId);
      await sendMessageToTelegram({
        chatIds: [normalizedChatId],
        text: removed
          ? "Привязка Telegram для этого чата удалена."
          : "Для этого чата привязка не найдена.",
        replyMarkup: createTokenKeyboard()
      });
      return true;
    }

    if (parsed.command === "/status") {
      const owners = findOwnersByChat(normalizedChatId);
      const textMessage = owners.length
        ? `Чат привязан к ownerId: ${owners.join(", ")}`
        : "Чат пока не привязан. Нажмите «Ввести токен».";
      await sendMessageToTelegram({
        chatIds: [normalizedChatId],
        text: textMessage,
        replyMarkup: createTokenKeyboard()
      });
      return true;
    }

    if (["/start", "/bind", "/link"].includes(parsed.command)) {
      if (!parsed.payload) {
        if (parsed.command === "/start") {
          await sendStartWithTokenButton(normalizedChatId);
          return true;
        }
        await sendTokenRequestPrompt(normalizedChatId);
        return true;
      }
      const bindResult = await tryBindTokenForChat({
        chatId: normalizedChatId,
        token: parsed.payload
      });
      if (!bindResult.ok) {
        markAwaitingToken(normalizedChatId);
        await sendMessageToTelegram({
          chatIds: [normalizedChatId],
          text: `Токен не подошел: ${bindResult.reason}. Отправьте токен снова.`,
          replyMarkup: createTokenKeyboard()
        });
      }
      return true;
    }

    return false;
  }

  if (isTokenButtonText(rawText)) {
    await sendTokenRequestPrompt(normalizedChatId);
    return true;
  }

  if (!isAwaitingToken(normalizedChatId)) {
    return false;
  }

  const bindResult = await tryBindTokenForChat({
    chatId: normalizedChatId,
    token: rawText
  });
  if (!bindResult.ok) {
    await sendMessageToTelegram({
      chatIds: [normalizedChatId],
      text: `Токен не подошел: ${bindResult.reason}. Отправьте токен снова.`,
      replyMarkup: createTokenKeyboard()
    });
  }
  return true;
};

const syncTelegramBindings = async () => {
  if (!TELEGRAM_NOTIFIER_ENABLED) return { enabled: false };
  loadState();

  const now = Date.now();
  if (!syncInFlight && now - lastSyncAt < TELEGRAM_SYNC_INTERVAL_MS) {
    return { enabled: true, cached: true };
  }
  if (syncInFlight) return syncInFlight;

  syncInFlight = (async () => {
    const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/getUpdates`;
    let processed = 0;
    let highestUpdateId = Number(state.data.updateOffset || 0);
    try {
      const response = await axios.get(url, {
        timeout: TELEGRAM_API_TIMEOUT_MS,
        params: {
          offset: Math.max(0, highestUpdateId),
          limit: TELEGRAM_UPDATES_LIMIT,
          timeout: 0
        }
      });
      const updates = Array.isArray(response?.data?.result) ? response.data.result : [];
      for (const update of updates) {
        const updateId = Number(update?.update_id || 0);
        if (Number.isFinite(updateId) && updateId >= highestUpdateId) {
          highestUpdateId = updateId + 1;
        }
        const message = update?.message || update?.edited_message || update?.channel_post || null;
        if (!message) continue;
        const handled = await processIncomingCommand({
          chatId: message?.chat?.id,
          text: message?.text || message?.caption || ""
        });
        if (handled) processed += 1;
      }
    } catch (error) {
      const reason = error?.response?.data?.description
        || error?.message
        || "unknown";
      console.log(`[telegramNotifier] getUpdates failed: ${reason}`);
    } finally {
      const normalizedOffset = Number(highestUpdateId || 0);
      if (Number.isFinite(normalizedOffset) && normalizedOffset > Number(state.data.updateOffset || 0)) {
        state.data.updateOffset = Math.floor(normalizedOffset);
        state.dirty = true;
      }
      pruneOwnerBindings();
      saveState();
      lastSyncAt = Date.now();
      const stats = { enabled: true, processed };
      syncInFlight = null;
      return stats;
    }
  })();

  return syncInFlight;
};

const getTargetChatIds = (ownerId) => {
  const normalizedOwnerId = normalizeOwnerId(ownerId);
  if (!normalizedOwnerId) {
    if (bindingResolver && !TELEGRAM_ALLOW_OWNERLESS_FALLBACK) {
      return [];
    }
    return Array.from(new Set(TELEGRAM_DEFAULT_CHAT_IDS.map((chatId) => normalizeChatId(chatId)).filter(Boolean)));
  }
  const entry = normalizeOwnerBindingEntry(state.data.byOwner[normalizedOwnerId]);
  return entry.chatIds;
};

const notifyConversations = async (conversations = [], options = {}) => {
  if (!TELEGRAM_NOTIFIER_ENABLED) {
    return { enabled: false, queued: 0, sent: 0, skipped: 0 };
  }
  loadState();
  await syncTelegramBindings();

  const ownerId = normalizeOwnerId(options?.ownerId || "");
  const chatIds = getTargetChatIds(ownerId);
  if (!chatIds.length) {
    const hasOwner = Boolean(ownerId);
    return {
      enabled: true,
      queued: 0,
      sent: 0,
      skipped: Array.isArray(conversations) ? conversations.length : 0,
      reason: hasOwner
        ? "OWNER_NOT_LINKED"
        : (bindingResolver && !TELEGRAM_ALLOW_OWNERLESS_FALLBACK ? "OWNER_TOKEN_REQUIRED" : "NO_CHAT_CONFIGURED")
    };
  }

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

    const key = buildConversationKey(item, index, ownerId);
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
    const deliveredCount = await sendMessageToTelegram({ text, chatIds });
    if (!deliveredCount) continue;

    state.data.byConversation[key] = {
      fingerprint,
      updatedAt: Date.now()
    };
    state.dirty = true;
    sent += 1;
    await sleep(120);
  }

  pruneState();
  pruneOwnerBindings();
  saveState();
  return { enabled: true, queued, sent, skipped };
};

const queueTelegramConversationNotifications = (conversations = [], options = {}) => {
  sendQueue = sendQueue
    .catch(() => {})
    .then(() => notifyConversations(conversations, options));
  return sendQueue;
};

const startTelegramCommandPolling = () => {
  if (!TELEGRAM_NOTIFIER_ENABLED) return false;
  if (commandPollTimer) return true;
  const run = () => {
    syncTelegramBindings().catch((error) => {
      console.log(`[telegramNotifier] background sync failed: ${error?.message || error}`);
    });
  };
  run();
  commandPollTimer = setInterval(run, TELEGRAM_POLL_INTERVAL_MS);
  if (typeof commandPollTimer?.unref === "function") {
    commandPollTimer.unref();
  }
  return true;
};

module.exports = {
  queueTelegramConversationNotifications,
  isTelegramNotifierEnabled: () => TELEGRAM_NOTIFIER_ENABLED,
  setTelegramTokenResolver,
  startTelegramCommandPolling
};
