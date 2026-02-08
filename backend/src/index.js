const express = require("express");
const cors = require("cors");
const proxyChecker = require("./proxyChecker");
const axios = require("axios");
const multer = require("multer");
const fs = require("fs");
const path = require("path");
const os = require("os");
const crypto = require("crypto");
const puppeteerExtra = require("puppeteer-extra");
const StealthPlugin = require("puppeteer-extra-plugin-stealth");
const { listAccounts, insertAccount, deleteAccount, countAccountsByStatus, getAccountById, updateAccount } = require("./db");
const { buildProxyUrl } = require("./cookieUtils");
const {
  publishAd,
  parseExtraSelectFields,
  parseExtraSelectFieldsAcrossContexts,
  acceptCookieModal,
  acceptGdprConsent,
  isGdprPage,
  extractCategoryIdFromUrl,
  resolveCategoryPathFromCache,
  getCategorySelectionUrl,
  openCategorySelection,
  openCategorySelectionByPost,
  waitForCategorySelectionReady,
  selectCategoryPathOnSelectionPage,
  applyCategoryPathViaTree,
  hasCategoryTree,
  clickCategoryWeiter
} = require("./adPublisher");
const { validateCookies, pickDeviceProfile } = require("./cookieValidator");
const { fetchActiveAds, performAdAction } = require("./adsService");
const { upsertAd } = require("./adsStore");
const { getCategories, getCategoryChildren } = require("./categoryService");
const {
  fetchMessages,
  fetchThreadMessages,
  summarizeConversations,
  sendConversationMessage,
  declineConversationOffer,
  sendConversationMedia
} = require("./messageService");
const { translateText } = require("./translateService");

const app = express();
const PORT = process.env.PORT || 5000;
const PUPPETEER_PROTOCOL_TIMEOUT = Number(process.env.PUPPETEER_PROTOCOL_TIMEOUT || 120000);
const PUPPETEER_LAUNCH_TIMEOUT = Number(process.env.PUPPETEER_LAUNCH_TIMEOUT || 120000);
const PUPPETEER_NAV_TIMEOUT = Number(process.env.PUPPETEER_NAV_TIMEOUT || 60000);

const proxies = [];
const proxiesPath = path.join(__dirname, "..", "data", "proxies.json");
const subscriptionTokensPath = path.join(__dirname, "..", "data", "subscription-tokens.json");
const categoryChildrenCachePath = path.join(__dirname, "..", "data", "category-children.json");
const categoryFieldsCachePath = path.join(__dirname, "..", "data", "category-fields.json");
const CATEGORY_CHILDREN_CACHE_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const CATEGORY_CHILDREN_EMPTY_TTL_MS = 60 * 60 * 1000;
const CATEGORY_FIELDS_CACHE_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const CATEGORY_FIELDS_EMPTY_TTL_MS = 5 * 60 * 1000;
const categoryChildrenCache = { items: {} };
let categoryChildrenCacheSaveTimer = null;
const categoryFieldsCache = { items: {} };
let categoryFieldsCacheSaveTimer = null;
const DEBUG_FIELDS = process.env.KL_DEBUG_FIELDS === "1";
let puppeteerStealthReady = false;
const subscriptionTokens = { items: [] };
const activePublishByAccount = new Map();
const recentSuccessfulPublishes = new Map();
const RECENT_PUBLISH_TTL_MS = 30 * 60 * 1000;
const ADS_ACTIVE_CACHE_TTL_MS = Number(process.env.KL_ACTIVE_ADS_CACHE_TTL_MS || 90 * 1000);
const activeAdsCache = new Map();

const getPuppeteer = () => {
  if (!puppeteerStealthReady) {
    puppeteerExtra.use(StealthPlugin());
    puppeteerStealthReady = true;
  }
  return puppeteerExtra;
};

const toDeviceProfile = (rawProfile) => {
  if (!rawProfile) {
    return pickDeviceProfile();
  }
  if (typeof rawProfile === "string") {
    try {
      return JSON.parse(rawProfile);
    } catch (error) {
      return pickDeviceProfile();
    }
  }
  return rawProfile;
};

const sanitizeFilename = (value) => (value || "account")
  .toString()
  .replace(/[^a-z0-9._-]+/gi, "_")
  .slice(0, 60);

const safeJsonStringify = (value) => {
  try {
    return JSON.stringify(value);
  } catch (error) {
    return String(value ?? "");
  }
};

const buildPublishRequestFingerprint = ({
  accountId,
  title,
  description,
  price,
  currency,
  postalCode,
  categoryId,
  categoryUrl,
  categoryPath,
  extraFields,
  uploadedFiles
}) => {
  const payload = {
    accountId: String(accountId || "").trim(),
    title: String(title || "").trim(),
    description: String(description || "").trim(),
    price: String(price || "").trim(),
    currency: String(currency || "").trim(),
    postalCode: String(postalCode || "").trim(),
    categoryId: String(categoryId || "").trim(),
    categoryUrl: String(categoryUrl || "").trim(),
    categoryPath: safeJsonStringify(categoryPath || null),
    extraFields: safeJsonStringify(extraFields || null),
    files: (uploadedFiles || []).map((file) => ({
      originalname: String(file?.originalname || ""),
      size: Number(file?.size || 0),
      mimetype: String(file?.mimetype || "")
    }))
  };
  return crypto.createHash("sha256").update(JSON.stringify(payload)).digest("hex");
};

const cleanupRecentSuccessfulPublishes = () => {
  const now = Date.now();
  for (const [fingerprint, item] of recentSuccessfulPublishes.entries()) {
    if (!item || !item.finishedAt || (now - item.finishedAt) > RECENT_PUBLISH_TTL_MS) {
      recentSuccessfulPublishes.delete(fingerprint);
    }
  }
};

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const withTimeout = (promise, timeoutMs, label = "timeout") => {
  let timeoutId;
  const timeoutPromise = new Promise((_, reject) => {
    timeoutId = setTimeout(() => reject(new Error(label)), timeoutMs);
  });
  return Promise.race([promise, timeoutPromise]).finally(() => clearTimeout(timeoutId));
};

const ensureDebugDir = () => {
  const debugDir = path.join(__dirname, "..", "data", "debug");
  if (!fs.existsSync(debugDir)) {
    fs.mkdirSync(debugDir, { recursive: true });
  }
  return debugDir;
};

const getPublishRequestLogPath = () => path.join(ensureDebugDir(), "publish-requests.log");
const getServerErrorLogPath = () => path.join(ensureDebugDir(), "server-errors.log");
const getFieldsRequestLogPath = () => path.join(ensureDebugDir(), "fields-requests.log");
const getMessageActionsLogPath = () => path.join(ensureDebugDir(), "message-actions.log");

const normalizeTokenValue = (value) => String(value || "").trim();
const parseEnvTokenList = (value) => String(value || "")
  .split(/[,\n;]/)
  .map((item) => normalizeTokenValue(item))
  .filter(Boolean);

const getEnvTokens = () => {
  const tokens = [
    ...parseEnvTokenList(process.env.KL_ACCESS_TOKENS),
    ...parseEnvTokenList(process.env.KL_VALID_TOKENS),
    ...parseEnvTokenList(process.env.ACCESS_TOKENS),
    ...parseEnvTokenList(process.env.VALID_TOKENS)
  ];
  return Array.from(new Set(tokens));
};

const findEnvToken = (token) => {
  const normalized = normalizeTokenValue(token);
  if (!normalized) return null;
  const tokens = getEnvTokens();
  if (!tokens.length) return null;
  if (!tokens.includes(normalized)) return null;
  return { token: normalized, role: "user" };
};

const parseTokenExpiryMs = (value) => {
  if (value === undefined || value === null || value === "") return null;
  if (typeof value === "number" && Number.isFinite(value)) return value;
  const asNumber = Number(value);
  if (!Number.isNaN(asNumber) && String(value).trim() !== "") {
    // Guard against out-of-range timestamps that crash Date#toISOString.
    if (!Number.isFinite(asNumber) || Math.abs(asNumber) > 8640000000000000) {
      return null;
    }
    return asNumber;
  }
  const asDate = new Date(value);
  if (!Number.isNaN(asDate.getTime())) return asDate.getTime();
  return null;
};

const toIsoStringOrNull = (value) => {
  const expiryMs = parseTokenExpiryMs(value);
  if (expiryMs === null) return null;
  const parsed = new Date(expiryMs);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString();
};

const loadSubscriptionTokens = () => {
  try {
    if (!fs.existsSync(subscriptionTokensPath)) {
      fs.writeFileSync(subscriptionTokensPath, JSON.stringify({ items: [] }, null, 2), "utf8");
    }
    const raw = fs.readFileSync(subscriptionTokensPath, "utf8");
    const parsed = JSON.parse(raw);
    subscriptionTokens.items = Array.isArray(parsed.items) ? parsed.items : [];
  } catch (error) {
    subscriptionTokens.items = [];
  }
};

const saveSubscriptionTokens = () => {
  try {
    fs.writeFileSync(
      subscriptionTokensPath,
      JSON.stringify({ items: subscriptionTokens.items }, null, 2),
      "utf8"
    );
  } catch (error) {
    // ignore save errors
  }
};

const findSubscriptionToken = (token) => {
  const normalized = normalizeTokenValue(token);
  if (!normalized) return null;
  return subscriptionTokens.items.find((entry) => entry?.token === normalized) || null;
};

const getTokenOwnerId = (entry) => {
  if (!entry) return "";
  if (entry.ownerId) return String(entry.ownerId);
  if (!entry.token) return "";
  return crypto.createHash("sha256").update(String(entry.token)).digest("hex").slice(0, 16);
};

const isAdminToken = (entry) => {
  if (!entry) return false;
  if (entry.role && String(entry.role).toLowerCase() === "admin") return true;
  return Boolean(entry.isAdmin);
};

const getSubscriptionTokenStatus = (token) => {
  loadSubscriptionTokens();
  const normalized = normalizeTokenValue(token);
  if (!normalized) {
    return { valid: false, reason: "Токен не указан" };
  }
  const entry = findSubscriptionToken(normalized) || findEnvToken(normalized);
  if (!entry) {
    return { valid: false, reason: "Токен не найден" };
  }
  const expiryMs = parseTokenExpiryMs(entry.expiresAt);
  if (entry.expiresAt !== undefined && entry.expiresAt !== null && expiryMs === null) {
    return { valid: false, reason: "Некорректный срок действия токена" };
  }
  if (expiryMs && Date.now() > expiryMs) {
    return { valid: false, reason: "Срок действия токена истек", expiresAt: expiryMs };
  }
  const ownerId = getTokenOwnerId(entry);
  return {
    valid: true,
    expiresAt: expiryMs || null,
    label: entry.label || "",
    ownerId,
    role: isAdminToken(entry) ? "admin" : "user",
    isAdmin: isAdminToken(entry)
  };
};

const extractAccessToken = (req) => {
  const header = req.headers?.authorization || "";
  if (header.toLowerCase().startsWith("bearer ")) {
    return normalizeTokenValue(header.slice(7));
  }
  const headerToken = req.headers?.["x-access-token"];
  if (headerToken) return normalizeTokenValue(headerToken);
  const queryToken = req.query?.accessToken || req.query?.token;
  if (queryToken) return normalizeTokenValue(queryToken);
  const bodyToken = req.body?.accessToken || req.body?.token;
  if (bodyToken) return normalizeTokenValue(bodyToken);
  return "";
};

const appendServerLog = (pathTarget, payload) => {
  try {
    const entry = {
      ts: new Date().toISOString(),
      ...payload
    };
    fs.appendFileSync(pathTarget, JSON.stringify(entry) + "\n", "utf8");
  } catch (error) {
    // ignore logging failures
  }
};

const appendFieldsRequestLog = (payload) => {
  try {
    const entry = {
      ts: new Date().toISOString(),
      ...payload
    };
    fs.appendFileSync(getFieldsRequestLogPath(), JSON.stringify(entry) + "\n", "utf8");
  } catch (error) {
    // ignore logging failures
  }
};

process.on("uncaughtException", (error) => {
  appendServerLog(getServerErrorLogPath(), {
    type: "uncaughtException",
    message: error?.message || String(error),
    stack: error?.stack || ""
  });
  process.exit(1);
});

process.on("unhandledRejection", (reason) => {
  appendServerLog(getServerErrorLogPath(), {
    type: "unhandledRejection",
    message: reason?.message || String(reason),
    stack: reason?.stack || ""
  });
});

const dumpFieldsDebug = async (page, { accountLabel = "account", step = "unknown", error = "", extra = {}, force = false } = {}) => {
  if ((!DEBUG_FIELDS && !force) || !page) return;
  try {
    const debugDir = ensureDebugDir();

    const safeLabel = sanitizeFilename(accountLabel);
    const safeStep = sanitizeFilename(step);
    const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
    const base = `fields-${safeLabel}-${safeStep}-${timestamp}`;
    const htmlPath = path.join(debugDir, `${base}.html`);
    const screenshotPath = path.join(debugDir, `${base}.png`);
    const metaPath = path.join(debugDir, `${base}.json`);

    const [html, meta] = await Promise.all([
      page.content().catch(() => ""),
      page.evaluate(() => ({
        url: window.location.href,
        title: document.title,
        bodyTextSample: (document.body?.innerText || "").slice(0, 2000)
      })).catch(() => ({}))
    ]);

    if (html) {
      fs.writeFileSync(htmlPath, html, "utf8");
    }
    await page.screenshot({ path: screenshotPath, fullPage: true }).catch(() => {});
    fs.writeFileSync(
      metaPath,
      JSON.stringify(
        {
          step,
          error,
          ...meta,
          extra
        },
        null,
        2
      ),
      "utf8"
    );
    console.log(`[fields] Debug saved: ${metaPath}`);
  } catch (dumpError) {
    console.log(`[fields] Debug dump failed: ${dumpError.message}`);
  }
};

const dumpFieldsDebugMeta = ({ accountLabel = "account", step = "unknown", error = "", extra = {}, force = false } = {}) => {
  if (!DEBUG_FIELDS && !force) return;
  try {
    const debugDir = ensureDebugDir();
    const safeLabel = sanitizeFilename(accountLabel);
    const safeStep = sanitizeFilename(step);
    const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
    const base = `fields-${safeLabel}-${safeStep}-${timestamp}-meta`;
    const metaPath = path.join(debugDir, `${base}.json`);
    fs.writeFileSync(
      metaPath,
      JSON.stringify(
        {
          step,
          error,
          extra,
          createdAt: new Date().toISOString()
        },
        null,
        2
      ),
      "utf8"
    );
    console.log(`[fields] Debug meta saved: ${metaPath}`);
  } catch (dumpError) {
    console.log(`[fields] Debug meta dump failed: ${dumpError.message}`);
  }
};

const loadProxies = () => {
  try {
    const raw = fs.readFileSync(proxiesPath, "utf8");
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed)) {
      proxies.push(...parsed);
    }
  } catch (error) {
    // ignore missing or invalid proxy cache
  }
};

const saveProxies = () => {
  try {
    fs.writeFileSync(proxiesPath, JSON.stringify(proxies, null, 2));
  } catch (error) {
    // ignore persistence errors
  }
};

loadProxies();
const loadCategoryChildrenCache = () => {
  try {
    const raw = fs.readFileSync(categoryChildrenCachePath, "utf8");
    const parsed = JSON.parse(raw);
    if (parsed && typeof parsed === "object" && parsed.items && typeof parsed.items === "object") {
      categoryChildrenCache.items = parsed.items;
    }
  } catch (error) {
    // ignore missing or invalid cache
  }
};

const loadCategoryFieldsCache = () => {
  try {
    const raw = fs.readFileSync(categoryFieldsCachePath, "utf8");
    const parsed = JSON.parse(raw);
    if (parsed && typeof parsed === "object" && parsed.items && typeof parsed.items === "object") {
      categoryFieldsCache.items = parsed.items;
    }
  } catch (error) {
    // ignore missing or invalid cache
  }
};

const scheduleCategoryChildrenCacheSave = () => {
  if (categoryChildrenCacheSaveTimer) return;
  categoryChildrenCacheSaveTimer = setTimeout(() => {
    categoryChildrenCacheSaveTimer = null;
    try {
      fs.writeFileSync(categoryChildrenCachePath, JSON.stringify({
        updatedAt: new Date().toISOString(),
        items: categoryChildrenCache.items
      }, null, 2));
    } catch (error) {
      // ignore persistence errors
    }
  }, 1000);
};

const scheduleCategoryFieldsCacheSave = () => {
  if (categoryFieldsCacheSaveTimer) return;
  categoryFieldsCacheSaveTimer = setTimeout(() => {
    categoryFieldsCacheSaveTimer = null;
    try {
      fs.writeFileSync(categoryFieldsCachePath, JSON.stringify({
        updatedAt: new Date().toISOString(),
        items: categoryFieldsCache.items
      }, null, 2));
    } catch (error) {
      // ignore persistence errors
    }
  }, 1000);
};

const normalizeCategoryUrlForCache = (value) =>
  String(value || "")
    .replace(/^https?:\/\/www\.kleinanzeigen\.de/i, "")
    .replace(/\/$/, "");

const buildCategoryChildrenCacheKey = ({ id, url }) => {
  if (id) return `id:${id}`;
  if (url) return `url:${normalizeCategoryUrlForCache(url)}`;
  return "";
};

const getCachedCategoryChildrenEntry = (key) => {
  if (!key) return null;
  const entry = categoryChildrenCache.items[key];
  if (!entry) return null;
  const now = Date.now();
  if (entry.expiresAt && entry.expiresAt < now) {
    delete categoryChildrenCache.items[key];
    scheduleCategoryChildrenCacheSave();
    return null;
  }
  return entry;
};

const setCachedCategoryChildren = (key, children, { empty = false } = {}) => {
  if (!key) return;
  const now = Date.now();
  const ttl = empty ? CATEGORY_CHILDREN_EMPTY_TTL_MS : CATEGORY_CHILDREN_CACHE_TTL_MS;
  categoryChildrenCache.items[key] = {
    savedAt: now,
    expiresAt: now + ttl,
    children: Array.isArray(children) ? children : []
  };
  scheduleCategoryChildrenCacheSave();
};

loadCategoryChildrenCache();
loadCategoryFieldsCache();

const getCachedCategoryFields = (categoryId) => {
  const key = String(categoryId || "");
  if (!key) return null;
  const entry = categoryFieldsCache.items[key];
  if (!entry) return null;
  const now = Date.now();
  if (entry.expiresAt && entry.expiresAt < now) {
    delete categoryFieldsCache.items[key];
    scheduleCategoryFieldsCacheSave();
    return null;
  }
  return entry.fields || null;
};

const setCachedCategoryFields = (categoryId, fields) => {
  const key = String(categoryId || "");
  if (!key) return;
  const now = Date.now();
  const list = Array.isArray(fields) ? fields : [];
  const ttl = list.length ? CATEGORY_FIELDS_CACHE_TTL_MS : CATEGORY_FIELDS_EMPTY_TTL_MS;
  categoryFieldsCache.items[key] = {
    savedAt: now,
    expiresAt: now + ttl,
    fields: list
  };
  scheduleCategoryFieldsCacheSave();
};

const getOwnerContext = (req) => ({
  ownerId: req?.subscription?.ownerId || "",
  isAdmin: Boolean(req?.subscription?.isAdmin)
});

const filterByOwner = (items, ownerContext = {}) => {
  const list = Array.isArray(items) ? items : [];
  if (ownerContext.isAdmin) return list;
  if (!ownerContext.ownerId) return [];
  return list.filter((item) => item?.ownerId && String(item.ownerId) === String(ownerContext.ownerId));
};

const isOwnerMatch = (item, ownerContext = {}) => {
  if (ownerContext.isAdmin) return true;
  if (!ownerContext.ownerId) return false;
  return item?.ownerId && String(item.ownerId) === String(ownerContext.ownerId);
};

const normalizeEntityId = (value) => String(value ?? "").trim();

const getActiveAdsCacheScope = (ownerContext = {}) => {
  if (ownerContext?.isAdmin) return "admin";
  const ownerId = String(ownerContext?.ownerId || "").trim();
  return `owner:${ownerId || "none"}`;
};

const buildActiveAdsCacheKey = ({ ownerContext = {}, accounts = [] } = {}) => {
  const scope = getActiveAdsCacheScope(ownerContext);
  const accountSignature = (Array.isArray(accounts) ? accounts : [])
    .map((account) => `${normalizeEntityId(account?.id)}:${normalizeEntityId(account?.proxyId)}`)
    .filter(Boolean)
    .sort()
    .join(",");
  return `${scope}|${accountSignature || "none"}`;
};

const pruneActiveAdsCache = () => {
  const now = Date.now();
  for (const [key, entry] of activeAdsCache.entries()) {
    if (!entry) {
      activeAdsCache.delete(key);
      continue;
    }
    if (entry.inFlight) continue;
    if (entry.expiresAt && entry.expiresAt > now) continue;
    activeAdsCache.delete(key);
  }
};

const getCachedActiveAdsEntry = (cacheKey) => {
  if (!cacheKey) return null;
  pruneActiveAdsCache();
  const entry = activeAdsCache.get(cacheKey);
  if (!entry) return null;
  if (entry.inFlight) return null;
  if (!entry.expiresAt || entry.expiresAt < Date.now()) {
    activeAdsCache.delete(cacheKey);
    return null;
  }
  return entry;
};

const setCachedActiveAdsEntry = (cacheKey, ads) => {
  if (!cacheKey) return;
  const now = Date.now();
  activeAdsCache.set(cacheKey, {
    ads: Array.isArray(ads) ? ads : [],
    cachedAt: now,
    expiresAt: now + Math.max(5000, ADS_ACTIVE_CACHE_TTL_MS),
    inFlight: null
  });
};

const setActiveAdsInFlight = (cacheKey, inFlight) => {
  if (!cacheKey) return;
  const current = activeAdsCache.get(cacheKey) || {};
  activeAdsCache.set(cacheKey, {
    ...current,
    inFlight
  });
};

const clearActiveAdsInFlight = (cacheKey) => {
  if (!cacheKey) return;
  const current = activeAdsCache.get(cacheKey);
  if (!current) return;
  if (!current.ads || !current.expiresAt) {
    activeAdsCache.delete(cacheKey);
    return;
  }
  current.inFlight = null;
  activeAdsCache.set(cacheKey, current);
};

const invalidateActiveAdsCacheForOwner = (ownerContext = {}) => {
  const scopePrefix = `${getActiveAdsCacheScope(ownerContext)}|`;
  for (const key of activeAdsCache.keys()) {
    if (key.startsWith(scopePrefix)) {
      activeAdsCache.delete(key);
    }
  }
};

const isSameEntityId = (left, right) => {
  const leftId = normalizeEntityId(left);
  const rightId = normalizeEntityId(right);
  return Boolean(leftId && rightId && leftId === rightId);
};

const findProxyById = (proxyId) => proxies.find((item) => isSameEntityId(item?.id, proxyId));

const findProxyIndexById = (proxyId) => proxies.findIndex((item) => isSameEntityId(item?.id, proxyId));

const hasProxyWithId = (proxyList, proxyId) => {
  const list = Array.isArray(proxyList) ? proxyList : [];
  return list.some((item) => isSameEntityId(item?.id, proxyId));
};

const getProxyLabel = (proxyId, ownerContext = {}) => {
  const proxy = findProxyById(proxyId);
  if (!proxy) return "Нет";
  if (!isOwnerMatch(proxy, ownerContext)) return "Нет";
  return `${proxy.name} (${proxy.host}:${proxy.port})`;
};

const requireAccountProxy = (account, res, contextLabel = "операция", ownerContext = {}) => {
  if (res?.headersSent) return null;
  if (!account?.proxyId) {
    res.status(400).json({
      success: false,
      error: `Аккаунт должен быть привязан к прокси для ${contextLabel}.`
    });
    return null;
  }
  const proxy = findProxyById(account.proxyId);
  if (!proxy) {
    res.status(400).json({
      success: false,
      error: `Прокси для аккаунта не найден. ${contextLabel} остановлена.`
    });
    return null;
  }
  if (!isOwnerMatch(proxy, ownerContext)) {
    res.status(403).json({
      success: false,
      error: "Недостаточно прав для использования прокси."
    });
    return null;
  }
  // Hard fail early if proxy is misconfigured. We must not fall back to server IP.
  if (!buildProxyUrl(proxy)) {
    res.status(400).json({
      success: false,
      error: `Прокси аккаунта настроен некорректно (host/port). ${contextLabel} остановлена.`
    });
    return null;
  }
  return proxy;
};

const getAccountForRequest = (accountId, req, res) => {
  const account = getAccountById(Number(accountId));
  if (!account) {
    res.status(404).json({ success: false, error: "Аккаунт не найден" });
    return null;
  }
  const ownerContext = getOwnerContext(req);
  if (!isOwnerMatch(account, ownerContext)) {
    res.status(404).json({ success: false, error: "Аккаунт не найден" });
    return null;
  }
  return account;
};

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 2 * 1024 * 1024 }
});

const messageUpload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 10 * 1024 * 1024,
    files: 10
  }
});

const adUploadDir = path.join(__dirname, "..", "data", "ad-uploads");
if (!fs.existsSync(adUploadDir)) {
  fs.mkdirSync(adUploadDir, { recursive: true });
}

const imageMimeExtensions = {
  "image/jpeg": ".jpg",
  "image/jpg": ".jpg",
  "image/png": ".png",
  "image/gif": ".gif",
  "image/webp": ".webp",
  "image/heic": ".heic",
  "image/heif": ".heif"
};

const adUpload = multer({
  storage: multer.diskStorage({
    destination: (req, file, cb) => cb(null, adUploadDir),
    filename: (req, file, cb) => {
      const originalExt = (path.extname(file.originalname || "") || "").toLowerCase();
      const mimeExt = imageMimeExtensions[file.mimetype] || "";
      const safeExt = originalExt || mimeExt || "";
      const base = crypto.randomBytes(16).toString("hex");
      cb(null, safeExt ? `${base}${safeExt}` : base);
    }
  }),
  limits: { fileSize: 10 * 1024 * 1024, files: 20 }
});

const mapAdUploadErrorMessage = (error) => {
  if (!error) return "Не удалось загрузить файлы объявления";
  if (error instanceof multer.MulterError) {
    if (error.code === "LIMIT_FILE_SIZE") {
      return "Файл слишком большой. Максимум 10MB на изображение.";
    }
    if (error.code === "LIMIT_FILE_COUNT") {
      return "Слишком много файлов. Максимум 20 изображений.";
    }
    if (error.code === "LIMIT_UNEXPECTED_FILE") {
      return "Некорректный формат загрузки изображений.";
    }
    return error.message || "Ошибка загрузки изображений";
  }
  return error.message || "Ошибка загрузки изображений";
};

const adUploadMiddleware = (req, res, next) => {
  adUpload.array("images", 20)(req, res, (error) => {
    if (!error) {
      next();
      return;
    }
    const requestId = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const message = mapAdUploadErrorMessage(error);
    appendServerLog(getPublishRequestLogPath(), {
      event: "upload-error",
      requestId,
      ip: req.ip,
      errorCode: error.code || "",
      error: error.message || String(error)
    });
    res.status(error instanceof multer.MulterError ? 400 : 500).json({
      success: false,
      error: message,
      debugId: requestId
    });
  });
};

// Middleware
const parseOriginList = (value) => String(value || "")
  .split(",")
  .map((item) => item.trim())
  .filter(Boolean);

const defaultOrigins = ["http://localhost:3000", "http://127.0.0.1:3000"];
const envOrigins = [
  ...parseOriginList(process.env.KL_CORS_ORIGINS),
  ...parseOriginList(process.env.CORS_ORIGINS),
  process.env.RENDER_EXTERNAL_URL
].filter(Boolean);

const allowedOrigins = Array.from(new Set([...defaultOrigins, ...envOrigins]));
const allowAllOrigins = String(process.env.KL_ALLOW_ALL_ORIGINS || "").trim() === "1";
const allowAnyOriginByDefault = !allowAllOrigins
  && envOrigins.length === 0
  && String(process.env.KL_ALLOW_ALL_ORIGINS || "").trim() !== "0";

const corsOptions = {
  origin: (origin, callback) => {
    if (!origin) return callback(null, true);
    if (allowAllOrigins || allowAnyOriginByDefault) return callback(null, true);
    if (allowedOrigins.includes(origin)) return callback(null, true);
    return callback(new Error("Not allowed by CORS"));
  },
  methods: ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Authorization", "X-Access-Token"],
  credentials: true
};

app.use(cors(corsOptions));
app.options("*", cors(corsOptions));
app.use(express.json());
loadSubscriptionTokens();

const requireAccessToken = (req, res, next) => {
  if (process.env.KL_REQUIRE_TOKEN === "0") return next();
  if (req.method === "OPTIONS") return next();
  if (!req.path.startsWith("/api")) return next();
  if (req.path.startsWith("/api/auth/")) return next();

  const token = extractAccessToken(req);
  const status = getSubscriptionTokenStatus(token);
  if (!status.valid) {
    res.status(401).json({ success: false, error: status.reason });
    return;
  }
  req.subscription = status;
  req.subscription.token = token;
  next();
};

app.use(requireAccessToken);

// Маршруты
app.get("/", (req, res) => {
  res.json({ 
    message: "✅ Kleinanzeigen Backend работает!",
    endpoints: [
      "/api/accounts - список аккаунтов",
      "/api/proxies - список прокси",
      "/health - статус сервиса"
    ]
  });
});

app.post("/api/auth/validate", (req, res) => {
  try {
    const token = extractAccessToken(req);
    const status = getSubscriptionTokenStatus(token);
    res.json({
      valid: status.valid,
      error: status.valid ? "" : status.reason,
      expiresAt: toIsoStringOrNull(status.expiresAt),
      label: status.label || "",
      role: status.role || "user",
      ownerId: status.ownerId || ""
    });
  } catch (error) {
    appendServerLog(getServerErrorLogPath(), {
      type: "auth-validate-error",
      message: error?.message || String(error),
      stack: error?.stack || ""
    });
    res.status(500).json({ valid: false, error: "Ошибка проверки токена" });
  }
});

app.get("/api/accounts", (req, res) => {
  const ownerContext = getOwnerContext(req);
  const accounts = filterByOwner(listAccounts(), ownerContext);
  res.json(accounts.map(({ cookie, deviceProfile, ...account }) => ({
    ...account,
    proxy: getProxyLabel(account.proxyId, ownerContext)
  })));
});

app.get("/api/accounts/:id/plz", async (req, res) => {
  try {
    const account = getAccountForRequest(req.params.id, req, res);
    if (!account) return;
    const selectedProxy = requireAccountProxy(account, res, "загрузки PLZ", getOwnerContext(req));
    if (!selectedProxy) return;

    const puppeteer = getPuppeteer();
    const proxyChain = require("proxy-chain");
    const { parseCookies, normalizeCookie, buildProxyServer, buildProxyUrl } = require("./cookieUtils");
    const { pickDeviceProfile } = require("./cookieValidator");

    const cookies = parseCookies(account.cookie).map(normalizeCookie);
    if (!cookies.length) {
      res.status(400).json({ success: false, error: "Cookie пустые." });
      return;
    }

    let deviceProfile = pickDeviceProfile();
    if (account.deviceProfile) {
      try {
        deviceProfile = typeof account.deviceProfile === "string"
          ? JSON.parse(account.deviceProfile)
          : account.deviceProfile;
      } catch (error) {
        deviceProfile = pickDeviceProfile();
      }
    }

    const proxyServer = buildProxyServer(selectedProxy);
    const proxyUrl = buildProxyUrl(selectedProxy);
    const needsProxyChain = Boolean(
      proxyUrl && ((selectedProxy?.type || "").toLowerCase().startsWith("socks") || selectedProxy?.username || selectedProxy?.password)
    );
    let anonymizedProxyUrl;

    const launchArgs = ["--no-sandbox", "--disable-setuid-sandbox", "--lang=de-DE", "--disable-dev-shm-usage", "--no-zygote", "--disable-gpu"];
    if (proxyServer) {
      if (needsProxyChain) {
        anonymizedProxyUrl = await proxyChain.anonymizeProxy(proxyUrl);
        launchArgs.push(`--proxy-server=${anonymizedProxyUrl}`);
      } else {
        launchArgs.push(`--proxy-server=${proxyServer}`);
      }
    }

    const browser = await puppeteer.launch({
      headless: "new",
      args: launchArgs,
      timeout: PUPPETEER_LAUNCH_TIMEOUT,
      protocolTimeout: PUPPETEER_PROTOCOL_TIMEOUT
    });

    try {
      const page = await browser.newPage();
      page.setDefaultTimeout(PUPPETEER_NAV_TIMEOUT);
      page.setDefaultTimeout(PUPPETEER_NAV_TIMEOUT);
      page.setDefaultNavigationTimeout(PUPPETEER_NAV_TIMEOUT);
      if (!anonymizedProxyUrl && (selectedProxy?.username || selectedProxy?.password)) {
        await page.authenticate({
          username: selectedProxy.username || "",
          password: selectedProxy.password || ""
        });
      }

      if (deviceProfile?.userAgent) {
        await page.setUserAgent(deviceProfile.userAgent);
      }
      if (deviceProfile?.viewport) {
        await page.setViewport(deviceProfile.viewport);
      }
      if (deviceProfile?.locale) {
        await page.setExtraHTTPHeaders({ "Accept-Language": deviceProfile.locale });
      }
      if (deviceProfile?.timezone) {
        await page.emulateTimezone(deviceProfile.timezone);
      }

      await page.goto("https://www.kleinanzeigen.de/", { waitUntil: "domcontentloaded", timeout: 20000 });
      await delay(300 + Math.random() * 500);
      await acceptCookieModal(page, { timeout: 15000 }).catch(() => {});
      await acceptGdprConsent(page, { timeout: 15000 }).catch(() => {});
      await page.setCookie(...cookies);
      await delay(400 + Math.random() * 600);

      await page.goto("https://www.kleinanzeigen.de/p-anzeige-aufgeben-schritt2.html", { waitUntil: "domcontentloaded", timeout: 20000 });
      await acceptCookieModal(page, { timeout: 15000 }).catch(() => {});
      if (isGdprPage(page.url())) {
        await acceptGdprConsent(page, { timeout: 20000 }).catch(() => {});
      }
      await delay(300 + Math.random() * 500);

      const postalCode = await page.evaluate(() => {
        const candidates = [
          'input[name="zipcode"]',
          'input[name="plz"]',
          'input[id*="zip"]',
          'input[id*="plz"]',
          'input[placeholder*="PLZ"]',
          'input[aria-label*="PLZ"]',
          'input[placeholder*="Postleitzahl"]',
          'input[aria-label*="Postleitzahl"]'
        ];
        for (const selector of candidates) {
          const el = document.querySelector(selector);
          if (el) {
            const value = el.value || el.getAttribute("value") || "";
            if (value) return value.trim();
          }
        }
        return "";
      });

      if (postalCode) {
        updateAccount(account.id, { postalCode });
      }
      res.json({ success: true, postalCode: postalCode || "" });
    } finally {
      await browser.close();
      if (anonymizedProxyUrl) {
        await proxyChain.closeAnonymizedProxy(anonymizedProxyUrl, true);
      }
    }
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.post("/api/accounts/:id/refresh", async (req, res) => {
  try {
    const account = getAccountForRequest(req.params.id, req, res);
    if (!account) return;
    const ownerContext = getOwnerContext(req);

    if (!account.proxyId) {
      res.status(400).json({ success: false, error: "Для обновления аккаунта нужен прокси." });
      return;
    }

    const selectedProxy = findProxyById(account.proxyId) || null;
    if (!selectedProxy) {
      res.status(400).json({ success: false, error: "Прокси для аккаунта не найден." });
      return;
    }
    if (!isOwnerMatch(selectedProxy, ownerContext)) {
      res.status(403).json({ success: false, error: "Недостаточно прав для использования прокси." });
      return;
    }

    let deviceProfile = pickDeviceProfile();
    if (account.deviceProfile) {
      try {
        deviceProfile = typeof account.deviceProfile === "string"
          ? JSON.parse(account.deviceProfile)
          : account.deviceProfile;
      } catch (error) {
        deviceProfile = pickDeviceProfile();
      }
    }

    let validation = await validateCookies(account.cookie, {
      deviceProfile,
      proxy: selectedProxy
    });

    const normalizedName = String(validation.profileName || "").replace(/\s+/g, " ").trim();
    const safeName = /mein(e)? anzeigen|mein profil|meine anzeigen|profil und meine anzeigen/i.test(normalizedName)
      ? ""
      : normalizedName;

    const updates = {
      status: validation.valid ? "active" : "failed",
      lastCheck: new Date().toISOString(),
      error: validation.valid ? null : (validation.reason || "Куки невалидны"),
      profileName: safeName || account.profileName || "",
      profileEmail: validation.profileEmail || account.profileEmail || "",
      deviceProfile: account.deviceProfile || JSON.stringify(validation.deviceProfile || deviceProfile || pickDeviceProfile())
    };

    const updated = updateAccount(account.id, updates);

    res.status(validation.valid ? 200 : 400).json({
      success: validation.valid,
      error: validation.valid ? null : updates.error,
      account: {
        ...(updated || account),
        proxy: getProxyLabel(account.proxyId, ownerContext)
      }
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.get("/api/ads/active", async (req, res) => {
  try {
    const ownerContext = getOwnerContext(req);
    const scopedAccounts = filterByOwner(listAccounts(), ownerContext);
    const scopedProxies = filterByOwner(proxies, ownerContext);
    const accounts = scopedAccounts.filter((account) => {
      if (!account.proxyId) {
        return false;
      }
      return hasProxyWithId(scopedProxies, account.proxyId);
    });
    const forceRefresh = ["1", "true", "yes"].includes(String(req.query.force || "").trim().toLowerCase());
    const cacheKey = buildActiveAdsCacheKey({ ownerContext, accounts });

    if (!forceRefresh) {
      const cached = getCachedActiveAdsEntry(cacheKey);
      if (cached) {
        res.json({
          ads: Array.isArray(cached.ads) ? cached.ads : [],
          cached: true,
          cachedAt: new Date(cached.cachedAt || Date.now()).toISOString()
        });
        return;
      }
      const inflightEntry = activeAdsCache.get(cacheKey);
      if (inflightEntry?.inFlight) {
        const ads = await inflightEntry.inFlight;
        res.json({ ads, cached: false });
        return;
      }
    }

    const fetchPromise = (async () => {
      const ads = await fetchActiveAds({ accounts, proxies: scopedProxies, ownerContext });
      return Array.isArray(ads) ? ads : [];
    })();
    setActiveAdsInFlight(cacheKey, fetchPromise);

    const ads = await fetchPromise;
    setCachedActiveAdsEntry(cacheKey, ads);
    res.json({ ads, cached: false });
  } catch (error) {
    dumpFieldsDebugMeta({
      accountLabel: "account",
      step: "fields-exception",
      error: error?.message || "unknown-error",
      extra: { stack: error?.stack || "" },
      force: req?.query?.debug === "true" || req?.query?.debug === "1"
    });
    res.status(500).json({ success: false, error: error.message });
  } finally {
    try {
      const ownerContext = getOwnerContext(req);
      const scopedAccounts = filterByOwner(listAccounts(), ownerContext);
      const scopedProxies = filterByOwner(proxies, ownerContext);
      const accounts = scopedAccounts.filter((account) => account?.proxyId && hasProxyWithId(scopedProxies, account.proxyId));
      const cacheKey = buildActiveAdsCacheKey({ ownerContext, accounts });
      clearActiveAdsInFlight(cacheKey);
    } catch (error) {
      // ignore cleanup failures
    }
  }
});

app.post("/api/ads/:adId/reserve", async (req, res) => {
  try {
    const adId = String(req.params.adId || "").trim();
    const accountId = req.body?.accountId ? Number(req.body.accountId) : null;
    const adHref = req.body?.adHref ? String(req.body.adHref) : "";
    const adTitle = req.body?.adTitle ? String(req.body.adTitle) : "";
    if (!adId || !accountId) {
      res.status(400).json({ success: false, error: "Передайте adId и accountId" });
      return;
    }

    const account = getAccountForRequest(accountId, req, res);
    if (!account) return;

    const ownerContext = getOwnerContext(req);
    const selectedProxy = requireAccountProxy(account, res, "управления объявлениями", ownerContext);
    if (!selectedProxy) return;

    const accountLabel = account.profileEmail
      ? `${account.profileName || account.username || "Аккаунт"} (${account.profileEmail})`
      : (account.profileName || account.username || "Аккаунт");

    const result = await performAdAction({
      account,
      proxy: selectedProxy,
      adId,
      action: "reserve",
      accountLabel,
      adHref,
      adTitle
    });
    if (result?.success) {
      invalidateActiveAdsCacheForOwner(ownerContext);
    }
    res.json(result);
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.post("/api/ads/:adId/activate", async (req, res) => {
  try {
    const adId = String(req.params.adId || "").trim();
    const accountId = req.body?.accountId ? Number(req.body.accountId) : null;
    const adHref = req.body?.adHref ? String(req.body.adHref) : "";
    const adTitle = req.body?.adTitle ? String(req.body.adTitle) : "";
    if (!adId || !accountId) {
      res.status(400).json({ success: false, error: "Передайте adId и accountId" });
      return;
    }

    const account = getAccountForRequest(accountId, req, res);
    if (!account) return;

    const ownerContext = getOwnerContext(req);
    const selectedProxy = requireAccountProxy(account, res, "управления объявлениями", ownerContext);
    if (!selectedProxy) return;

    const accountLabel = account.profileEmail
      ? `${account.profileName || account.username || "Аккаунт"} (${account.profileEmail})`
      : (account.profileName || account.username || "Аккаунт");

    const result = await performAdAction({
      account,
      proxy: selectedProxy,
      adId,
      action: "activate",
      accountLabel,
      adHref,
      adTitle
    });
    if (result?.success) {
      invalidateActiveAdsCacheForOwner(ownerContext);
    }
    res.json(result);
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.post("/api/ads/:adId/delete", async (req, res) => {
  try {
    const adId = String(req.params.adId || "").trim();
    const accountId = req.body?.accountId ? Number(req.body.accountId) : null;
    const adHref = req.body?.adHref ? String(req.body.adHref) : "";
    const adTitle = req.body?.adTitle ? String(req.body.adTitle) : "";
    if (!adId || !accountId) {
      res.status(400).json({ success: false, error: "Передайте adId и accountId" });
      return;
    }

    const account = getAccountForRequest(accountId, req, res);
    if (!account) return;

    const ownerContext = getOwnerContext(req);
    const selectedProxy = requireAccountProxy(account, res, "управления объявлениями", ownerContext);
    if (!selectedProxy) return;

    const accountLabel = account.profileEmail
      ? `${account.profileName || account.username || "Аккаунт"} (${account.profileEmail})`
      : (account.profileName || account.username || "Аккаунт");

    const result = await performAdAction({
      account,
      proxy: selectedProxy,
      adId,
      action: "delete",
      accountLabel,
      adHref,
      adTitle
    });
    if (result?.success) {
      invalidateActiveAdsCacheForOwner(ownerContext);
    }
    res.json(result);
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.get("/api/messages", async (req, res) => {
  try {
    const accountId = req.query.accountId ? Number(req.query.accountId) : null;
    const maxConversations = req.query.limit ? Number(req.query.limit) : undefined;
    let accounts = accountId ? [getAccountForRequest(accountId, req, res)].filter(Boolean) : filterByOwner(listAccounts(), getOwnerContext(req));

    if (!accounts.length) {
      res.json([]);
      return;
    }

    if (accountId) {
      const proxy = requireAccountProxy(accounts[0], res, "загрузки сообщений", getOwnerContext(req));
      if (!proxy) return;
    } else {
      const ownerContext = getOwnerContext(req);
      const scopedProxies = filterByOwner(proxies, ownerContext);
      accounts = accounts.filter((account) => {
        if (!account.proxyId) return false;
        return hasProxyWithId(scopedProxies, account.proxyId);
      });
      if (!accounts.length) {
        res.json([]);
        return;
      }
    }

    const conversations = await fetchMessages({
      accounts,
      proxies: filterByOwner(proxies, getOwnerContext(req)),
      options: { maxConversations }
    });
    const summaries = summarizeConversations(conversations);
    res.json(summaries);
  } catch (error) {
    if (error.code === "AUTH_REQUIRED") {
      res.status(401).json({
        success: false,
        error: "Сессия истекла, пожалуйста, перелогиньтесь в Kleinanzeigen."
      });
      return;
    }
    res.status(500).json({ success: false, error: error.message });
  }
});

app.get("/api/messages/image", async (req, res) => {
  try {
    const rawUrl = String(req.query.url || "").trim();
    const accountId = req.query.accountId ? Number(req.query.accountId) : null;
    if (!rawUrl) {
      res.status(400).send("Image URL is required");
      return;
    }
    if (!accountId) {
      res.status(400).send("Account is required for image loading");
      return;
    }
    if (rawUrl.length > 2048) {
      res.status(400).send("Image URL is too long");
      return;
    }

    let parsed;
    try {
      parsed = new URL(rawUrl);
    } catch (error) {
      res.status(400).send("Invalid image URL");
      return;
    }

    const protocol = String(parsed.protocol || "").toLowerCase();
    if (protocol !== "http:" && protocol !== "https:") {
      res.status(400).send("Unsupported image URL protocol");
      return;
    }

    const hostname = String(parsed.hostname || "").toLowerCase();
    const isKleinanzeigenHost = hostname === "kleinanzeigen.de"
      || hostname.endsWith(".kleinanzeigen.de");
    if (!isKleinanzeigenHost) {
      res.status(403).send("Image host is not allowed");
      return;
    }

    const account = getAccountForRequest(accountId, req, res);
    if (!account) return;
    const proxy = requireAccountProxy(account, res, "загрузки изображения", getOwnerContext(req));
    if (!proxy) return;

    const isProdAdsImage = parsed.hostname.toLowerCase() === "img.kleinanzeigen.de"
      && parsed.pathname.startsWith("/api/v1/prod-ads/images/");

    const defaultRule = "$_57.JPG";
    const ruleVariants = [defaultRule, "$_24.JPG", "$_2.JPG", "$_57.AUTO", "$_24.AUTO"];
    const looksLikeMalformedRule = (rule) => /(imageid|\$\{.*\}|\$_\{.*\})/i.test(String(rule || ""));
    const isValidRule = (rule) => /^\$_[a-z0-9_.-]+$/i.test(String(rule || ""));

    const baseUrl = new URL(parsed.toString());
    if (isProdAdsImage) {
      const currentRule = baseUrl.searchParams.get("rule") || "";
      if (!isValidRule(currentRule) || looksLikeMalformedRule(currentRule)) {
        baseUrl.searchParams.set("rule", defaultRule);
      }
    }

    const candidates = [baseUrl.toString()];
    if (isProdAdsImage) {
      for (const variant of ruleVariants) {
        const candidate = new URL(baseUrl.toString());
        candidate.searchParams.set("rule", variant);
        const href = candidate.toString();
        if (!candidates.includes(href)) candidates.push(href);
      }
    }

    const requestHeaders = {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
      "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
      "Referer": "https://www.kleinanzeigen.de/"
    };

    let lastStatus = 502;
    let lastErrorMessage = "Image fetch failed";
    const axiosConfig = proxyChecker.buildAxiosConfig(proxy, 20000);
    axiosConfig.headers = { ...axiosConfig.headers, ...requestHeaders };
    axiosConfig.responseType = "arraybuffer";
    axiosConfig.maxRedirects = 5;
    axiosConfig.validateStatus = (status) => status >= 200 && status < 500;

    for (const candidateUrl of candidates) {
      let upstream;
      try {
        upstream = await axios.get(candidateUrl, axiosConfig);
      } catch (error) {
        lastStatus = 502;
        lastErrorMessage = `Image fetch failed: ${error.message || "network error"}`;
        continue;
      }

      if (!upstream || upstream.status < 200 || upstream.status >= 300) {
        lastStatus = upstream?.status || 502;
        lastErrorMessage = `Image fetch failed: ${upstream?.status || "network error"}`;
        continue;
      }

      const contentType = String(upstream.headers?.["content-type"] || "").toLowerCase();
      if (!contentType.startsWith("image/")) {
        lastStatus = 415;
        lastErrorMessage = "Upstream resource is not an image";
        continue;
      }

      const cacheControl = String(upstream.headers?.["cache-control"] || "").trim();
      const payload = Buffer.isBuffer(upstream.data) ? upstream.data : Buffer.from(upstream.data || "");

      res.setHeader("Content-Type", contentType);
      res.setHeader("Cache-Control", cacheControl || "public, max-age=900");
      res.send(payload);
      return;
    }

    res.status(lastStatus).send(lastErrorMessage);
  } catch (error) {
    res.status(502).send(`Image proxy failed: ${error.message}`);
  }
});

app.get("/api/messages/thread", async (req, res) => {
  try {
    const accountId = req.query.accountId ? Number(req.query.accountId) : null;
    const conversationId = req.query.conversationId ? String(req.query.conversationId) : "";
    const conversationUrl = req.query.conversationUrl ? String(req.query.conversationUrl) : "";
    const participant = req.query.participant ? String(req.query.participant) : "";
    const adTitle = req.query.adTitle ? String(req.query.adTitle) : "";

    if (!accountId || (!conversationId && !conversationUrl && !participant && !adTitle)) {
      res.status(400).json({ success: false, error: "Недостаточно данных для загрузки диалога." });
      return;
    }

    const account = getAccountForRequest(accountId, req, res);
    if (!account) return;

    const proxy = requireAccountProxy(account, res, "загрузки диалога", getOwnerContext(req));
    if (!proxy) return;

    const result = await fetchThreadMessages({
      account,
      proxy,
      conversationId,
      conversationUrl,
      participant,
      adTitle
    });

    res.json({ success: true, ...result });
  } catch (error) {
    if (error.code === "AUTH_REQUIRED") {
      res.status(401).json({
        success: false,
        error: "Сессия истекла, пожалуйста, перелогиньтесь в Kleinanzeigen."
      });
      return;
    }
    res.status(500).json({ success: false, error: error.message });
  }
});

app.post("/api/messages/send", async (req, res) => {
  try {
    const accountId = req.body?.accountId ? Number(req.body.accountId) : null;
    const conversationId = req.body?.conversationId ? String(req.body.conversationId) : "";
    const conversationUrl = req.body?.conversationUrl ? String(req.body.conversationUrl) : "";
    const participant = req.body?.participant ? String(req.body.participant) : "";
    const adTitle = req.body?.adTitle ? String(req.body.adTitle) : "";
    const text = req.body?.text ? String(req.body.text) : "";

    if (!accountId || (!conversationId && !conversationUrl && !participant && !adTitle) || !text.trim()) {
      res.status(400).json({ success: false, error: "Недостаточно данных для отправки." });
      return;
    }

    const account = getAccountForRequest(accountId, req, res);
    if (!account) return;

    const proxy = requireAccountProxy(account, res, "отправки сообщения", getOwnerContext(req));
    if (!proxy) return;

    const result = await sendConversationMessage({
      account,
      proxy,
      conversationId,
      conversationUrl,
      participant,
      adTitle,
      text: text.trim()
    });

    res.json({
      success: true,
      messages: result.messages || [],
      conversationId: result.conversationId || conversationId,
      conversationUrl: result.conversationUrl || conversationUrl
    });
  } catch (error) {
    if (error.code === "AUTH_REQUIRED") {
      res.status(401).json({
        success: false,
        error: "Сессия истекла, пожалуйста, перелогиньтесь в Kleinanzeigen."
      });
      return;
    }
    res.status(500).json({ success: false, error: error.message });
  }
});

app.post("/api/messages/offer/decline", async (req, res) => {
  req.setTimeout(0);
  res.setTimeout(0);
  if (req.socket) {
    req.socket.setTimeout(0);
  }
  const debugId = `msg-decline-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  const clientRequestId = String(req.get("x-client-request-id") || "");
  const startedAt = Date.now();
  appendServerLog(getMessageActionsLogPath(), {
    event: "start",
    route: "offer-decline",
    debugId,
    clientRequestId,
    ip: req.ip,
    bodyKeys: Object.keys(req.body || {})
  });
  req.on("aborted", () => {
    appendServerLog(getMessageActionsLogPath(), {
      event: "request-aborted",
      route: "offer-decline",
      debugId,
      elapsedMs: Date.now() - startedAt
    });
  });
  res.on("close", () => {
    appendServerLog(getMessageActionsLogPath(), {
      event: "response-close",
      route: "offer-decline",
      debugId,
      clientRequestId,
      elapsedMs: Date.now() - startedAt,
      statusCode: res.statusCode,
      writableEnded: res.writableEnded,
      headersSent: res.headersSent
    });
  });
  try {
    const accountId = req.body?.accountId ? Number(req.body.accountId) : null;
    const conversationId = req.body?.conversationId ? String(req.body.conversationId) : "";
    const conversationUrl = req.body?.conversationUrl ? String(req.body.conversationUrl) : "";
    appendServerLog(getMessageActionsLogPath(), {
      event: "payload",
      route: "offer-decline",
      debugId,
      clientRequestId,
      accountId,
      hasConversationId: Boolean(conversationId),
      hasConversationUrl: Boolean(conversationUrl)
    });

    if (!accountId || (!conversationId && !conversationUrl)) {
      res.status(400).json({
        success: false,
        error: "Недостаточно данных для отклонения заявки.",
        debugId
      });
      return;
    }

    const account = getAccountForRequest(accountId, req, res);
    if (!account) {
      appendServerLog(getMessageActionsLogPath(), {
        event: "account-not-found",
        route: "offer-decline",
        debugId,
        accountId
      });
      return;
    }

    const proxy = requireAccountProxy(account, res, "отклонения заявки", getOwnerContext(req));
    if (!proxy) {
      appendServerLog(getMessageActionsLogPath(), {
        event: "proxy-required",
        route: "offer-decline",
        debugId,
        accountId,
        proxyId: account?.proxyId || null
      });
      return;
    }

    appendServerLog(getMessageActionsLogPath(), {
      event: "service-start",
      route: "offer-decline",
      debugId
    });
    const result = await declineConversationOffer({
      account,
      proxy,
      conversationId,
      conversationUrl
    });
    appendServerLog(getMessageActionsLogPath(), {
      event: "service-success",
      route: "offer-decline",
      debugId,
      clientRequestId,
      elapsedMs: Date.now() - startedAt,
      messagesCount: Array.isArray(result?.messages) ? result.messages.length : 0
    });

    res.json({
      success: true,
      messages: result.messages || [],
      conversationId: result.conversationId || conversationId,
      conversationUrl: result.conversationUrl || conversationUrl,
      debugId,
      requestId: clientRequestId
    });
  } catch (error) {
    appendServerLog(getMessageActionsLogPath(), {
      event: "service-error",
      route: "offer-decline",
      debugId,
      clientRequestId,
      elapsedMs: Date.now() - startedAt,
      code: error?.code || "",
      message: error?.message || String(error),
      stack: error?.stack || ""
    });
    if (error.code === "AUTH_REQUIRED") {
      res.status(401).json({
        success: false,
        error: "Сессия истекла, пожалуйста, перелогиньтесь в Kleinanzeigen.",
        debugId,
        requestId: clientRequestId
      });
      return;
    }
    if (error.code === "PROXY_TUNNEL_CONNECTION_FAILED") {
      res.status(502).json({
        success: false,
        error: "Прокси аккаунта не может подключиться к Kleinanzeigen. Проверьте прокси аккаунта и попробуйте снова.",
        code: error.code,
        details: error?.details || "",
        debugId,
        requestId: clientRequestId
      });
      return;
    }
    if (error.code === "MESSAGE_ACTION_TIMEOUT") {
      res.status(504).json({
        success: false,
        error: "Действие в сообщениях заняло слишком много времени. Проверьте прокси аккаунта и попробуйте снова.",
        code: error.code,
        details: error?.details || "",
        debugId,
        requestId: clientRequestId
      });
      return;
    }
    res.status(500).json({
      success: false,
      error: error?.message || "Не удалось отклонить предложение",
      code: error?.code || "",
      debugId,
      requestId: clientRequestId
    });
  }
});

app.post("/api/messages/send-media", messageUpload.array("images", 10), async (req, res) => {
  req.setTimeout(0);
  res.setTimeout(0);
  if (req.socket) {
    req.socket.setTimeout(0);
  }
  const debugId = `msg-media-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  const clientRequestId = String(req.get("x-client-request-id") || "");
  const startedAt = Date.now();
  appendServerLog(getMessageActionsLogPath(), {
    event: "start",
    route: "send-media",
    debugId,
    clientRequestId,
    ip: req.ip,
    bodyKeys: Object.keys(req.body || {})
  });
  req.on("aborted", () => {
    appendServerLog(getMessageActionsLogPath(), {
      event: "request-aborted",
      route: "send-media",
      debugId,
      elapsedMs: Date.now() - startedAt
    });
  });
  res.on("close", () => {
    appendServerLog(getMessageActionsLogPath(), {
      event: "response-close",
      route: "send-media",
      debugId,
      clientRequestId,
      elapsedMs: Date.now() - startedAt,
      statusCode: res.statusCode,
      writableEnded: res.writableEnded,
      headersSent: res.headersSent
    });
  });
  try {
    const accountId = req.body?.accountId ? Number(req.body.accountId) : null;
    const conversationId = req.body?.conversationId ? String(req.body.conversationId) : "";
    const conversationUrl = req.body?.conversationUrl ? String(req.body.conversationUrl) : "";
    const text = req.body?.text ? String(req.body.text) : "";
    const files = Array.isArray(req.files) ? req.files : [];
    const imageFiles = files.filter((file) => String(file?.mimetype || "").toLowerCase().startsWith("image/"));
    appendServerLog(getMessageActionsLogPath(), {
      event: "payload",
      route: "send-media",
      debugId,
      clientRequestId,
      accountId,
      hasConversationId: Boolean(conversationId),
      hasConversationUrl: Boolean(conversationUrl),
      textLength: text.trim().length,
      filesCount: files.length,
      imageFilesCount: imageFiles.length
    });

    if (!accountId || (!conversationId && !conversationUrl) || (!text.trim() && !imageFiles.length)) {
      res.status(400).json({
        success: false,
        error: "Недостаточно данных для отправки.",
        debugId
      });
      return;
    }

    if (files.length && !imageFiles.length) {
      res.status(400).json({
        success: false,
        error: "Разрешены только изображения.",
        debugId
      });
      return;
    }

    const account = getAccountForRequest(accountId, req, res);
    if (!account) {
      appendServerLog(getMessageActionsLogPath(), {
        event: "account-not-found",
        route: "send-media",
        debugId,
        accountId
      });
      return;
    }

    const proxy = requireAccountProxy(account, res, "отправки фотографий", getOwnerContext(req));
    if (!proxy) {
      appendServerLog(getMessageActionsLogPath(), {
        event: "proxy-required",
        route: "send-media",
        debugId,
        accountId,
        proxyId: account?.proxyId || null
      });
      return;
    }

    appendServerLog(getMessageActionsLogPath(), {
      event: "service-start",
      route: "send-media",
      debugId
    });
    const result = await sendConversationMedia({
      account,
      proxy,
      conversationId,
      conversationUrl,
      text: text.trim(),
      files: imageFiles
    });
    appendServerLog(getMessageActionsLogPath(), {
      event: "service-success",
      route: "send-media",
      debugId,
      clientRequestId,
      elapsedMs: Date.now() - startedAt,
      messagesCount: Array.isArray(result?.messages) ? result.messages.length : 0
    });

    res.json({
      success: true,
      messages: result.messages || [],
      conversationId: result.conversationId || conversationId,
      conversationUrl: result.conversationUrl || conversationUrl,
      debugId,
      requestId: clientRequestId
    });
  } catch (error) {
    appendServerLog(getMessageActionsLogPath(), {
      event: "service-error",
      route: "send-media",
      debugId,
      clientRequestId,
      elapsedMs: Date.now() - startedAt,
      code: error?.code || "",
      message: error?.message || String(error),
      details: error?.details || "",
      stack: error?.stack || ""
    });
    if (error.code === "AUTH_REQUIRED") {
      res.status(401).json({
        success: false,
        error: "Сессия истекла, пожалуйста, перелогиньтесь в Kleinanzeigen.",
        debugId,
        requestId: clientRequestId
      });
      return;
    }
    if (error.code === "PROXY_TUNNEL_CONNECTION_FAILED") {
      res.status(502).json({
        success: false,
        error: "Прокси аккаунта не может подключиться к Kleinanzeigen. Проверьте прокси аккаунта и попробуйте снова.",
        code: error.code,
        details: error?.details || "",
        debugId,
        requestId: clientRequestId
      });
      return;
    }
    if (error.code === "MESSAGE_ACTION_TIMEOUT") {
      res.status(504).json({
        success: false,
        error: "Отправка медиа заняла слишком много времени. Проверьте прокси аккаунта и попробуйте снова.",
        code: error.code,
        details: error?.details || "",
        debugId,
        requestId: clientRequestId
      });
      return;
    }
    res.status(500).json({
      success: false,
      error: error?.message || "Не удалось отправить фото",
      code: error?.code || "",
      details: error?.details || "",
      debugId,
      requestId: clientRequestId
    });
  }
});

app.post("/api/translate", async (req, res) => {
  try {
    const text = req.body?.text ? String(req.body.text) : "";
    const to = req.body?.to ? String(req.body.to) : "ru";
    const from = req.body?.from ? String(req.body.from) : "";
    const accountIdRaw = req.body?.accountId;
    const hasAccountId = accountIdRaw !== undefined && accountIdRaw !== null && String(accountIdRaw).trim();
    const accountId = hasAccountId ? Number(accountIdRaw) : null;

    const trimmed = text.trim();
    if (!trimmed) {
      res.status(400).json({ success: false, error: "Текст обязателен для перевода." });
      return;
    }
    if (trimmed.length > 5000) {
      res.status(400).json({ success: false, error: "Слишком длинный текст для перевода (макс 5000 символов)." });
      return;
    }

    if (!hasAccountId) {
      res.status(400).json({
        success: false,
        error: "Для перевода сообщений нужен accountId (перевод выполняется через прокси аккаунта)."
      });
      return;
    }

    let proxy = null;
    if (hasAccountId) {
      if (!Number.isFinite(accountId)) {
        res.status(400).json({ success: false, error: "Некорректный accountId." });
        return;
      }
      const account = getAccountForRequest(accountId, req, res);
      if (!account) return;
      const ownerContext = getOwnerContext(req);
      proxy = requireAccountProxy(account, res, "перевода сообщений", ownerContext);
      if (!proxy) return;
    }

    const result = await translateText({ text: trimmed, to, from, proxy });
    res.json({ success: true, ...result });
  } catch (error) {
    if (error?.code === "NOT_CONFIGURED") {
      res.status(500).json({ success: false, error: "Переводчик не настроен на сервере." });
      return;
    }
    if (error?.code === "PROXY_REQUIRED" || error?.code === "PROXY_INIT_FAILED") {
      res.status(400).json({
        success: false,
        error: "Для перевода требуется рабочий прокси аккаунта (без прокси перевод не выполняется)."
      });
      return;
    }
    if (error?.code === "BAD_REQUEST") {
      res.status(400).json({ success: false, error: "Некорректный запрос на перевод." });
      return;
    }
    res.status(502).json({ success: false, error: error?.message || "Ошибка перевода" });
  }
});

app.get("/api/proxies", (req, res) => {
  const ownerContext = getOwnerContext(req);
  res.json(filterByOwner(proxies, ownerContext));
});

app.get("/api/categories", async (req, res) => {
  try {
    const forceRefresh = req.query.refresh === "true";
    const accountId = req.query.accountId ? Number(req.query.accountId) : null;
    let selectedProxy = null;
    if (accountId) {
      const account = getAccountForRequest(accountId, req, res);
      if (!account) return;
      const ownerContext = getOwnerContext(req);
      if (account.proxyId) {
        const proxy = findProxyById(account.proxyId);
        if (proxy && isOwnerMatch(proxy, ownerContext)) {
          selectedProxy = proxy;
        }
      }
    }
    const data = await getCategories({ forceRefresh, proxy: selectedProxy });
    res.json(data);
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.get("/api/categories/children", async (req, res) => {
  try {
    const id = req.query.id ? String(req.query.id) : "";
    const url = req.query.url ? String(req.query.url) : "";
    const accountId = req.query.accountId ? Number(req.query.accountId) : null;
    const forceRefresh = req.query.refresh === "true";
    const forceDebug = req.query.debug === "true" || req.query.debug === "1";
    const ownerContext = getOwnerContext(req);
    const requestAccount = accountId ? getAccountForRequest(accountId, req, res) : null;
    if (accountId && !requestAccount) {
      return;
    }
    const scopedProxies = filterByOwner(proxies, ownerContext);
    const selectedProxy = requestAccount?.proxyId
      ? scopedProxies.find((item) => isSameEntityId(item?.id, requestAccount.proxyId))
      : null;
    const cacheKey = buildCategoryChildrenCacheKey({ id, url });
    if (!forceRefresh && cacheKey) {
      const cachedEntry = getCachedCategoryChildrenEntry(cacheKey);
      if (cachedEntry) {
        if (process.env.KL_DEBUG_CATEGORIES === "1") {
          const sample = (cachedEntry.children || []).slice(0, 10).map((item) => `${item.id}:${item.name}`).join(", ");
          console.log(`[categories/children] cache hit key=${cacheKey} count=${cachedEntry.children?.length || 0} sample=${sample}`);
        }
        res.json({ children: cachedEntry.children || [] });
        return;
      }
    }
    let children = await getCategoryChildren({ id, url, proxy: selectedProxy });
    if (process.env.KL_DEBUG_CATEGORIES === "1") {
      console.log(`[categories/children] request id=${id} url=${url} accountId=${accountId} initialChildren=${children.length}`);
    }
    const dedupeChildren = (items) => {
      const seen = new Set();
      return (items || []).filter((item) => {
        const key = `${item?.id || ""}:${item?.name || ""}`;
        if (!key) return false;
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      });
    };

    if (children.length || !accountId) {
      const finalChildren = dedupeChildren(children);
      if (cacheKey) {
        setCachedCategoryChildren(cacheKey, finalChildren, { empty: finalChildren.length === 0 });
      }
      if (process.env.KL_DEBUG_CATEGORIES === "1") {
        const sample = finalChildren.slice(0, 10).map((item) => `${item.id}:${item.name}`).join(", ");
        console.log(`[categories/children] response id=${id} url=${url} count=${finalChildren.length} sample=${sample}`);
      }
      res.json({ children: finalChildren });
      return;
    }

    const account = requestAccount || getAccountById(accountId);
    if (!account) {
      if (process.env.KL_DEBUG_CATEGORIES === "1") {
        console.log("[categories/children] account not found, skip puppeteer fallback");
      }
      res.json({ children });
      return;
    }

    if (!account.proxyId || !hasProxyWithId(scopedProxies, account.proxyId)) {
      if (process.env.KL_DEBUG_CATEGORIES === "1") {
        console.log("[categories/children] proxy missing, skip puppeteer fallback");
      }
      const finalChildren = dedupeChildren(children);
      if (cacheKey) {
        setCachedCategoryChildren(cacheKey, finalChildren, { empty: finalChildren.length === 0 });
      }
      res.json({ children: finalChildren });
      return;
    }

    const resolveCategoryPath = async () => {
      const data = await getCategories({ forceRefresh: false, proxy: selectedProxy }).catch(() => null);
      const categories = data?.categories || [];
      const targetId = id || "";
      const targetUrl = url || "";
      const normalizeUrl = (value) =>
        String(value || "")
          .replace(/^https?:\/\/www\.kleinanzeigen\.de/i, "")
          .replace(/\/$/, "");

      const walk = (nodes, path = []) => {
        for (const node of nodes || []) {
          const matchesId = targetId && String(node.id) === String(targetId);
          const matchesUrl = targetUrl && normalizeUrl(node.url || "") === normalizeUrl(targetUrl);
          const nextPath = [...path, node];
          if (matchesId || matchesUrl) return nextPath;
          if (node.children?.length) {
            const found = walk(node.children, nextPath);
            if (found) return found;
          }
        }
        return null;
      };

      return walk(categories) || [];
    };

    const puppeteer = getPuppeteer();
    const proxyChain = require("proxy-chain");
    const { parseCookies, normalizeCookie, buildProxyServer, buildProxyUrl } = require("./cookieUtils");
    const { pickDeviceProfile } = require("./cookieValidator");

    const cookies = parseCookies(account.cookie).map(normalizeCookie);
    const deviceProfile = toDeviceProfile(account.deviceProfile);
    const proxyServer = buildProxyServer(selectedProxy);
    const proxyUrl = buildProxyUrl(selectedProxy);
    const needsProxyChain = Boolean(
      proxyUrl && ((selectedProxy?.type || "").toLowerCase().startsWith("socks") || selectedProxy?.username || selectedProxy?.password)
    );
    let anonymizedProxyUrl;

    const launchArgs = ["--no-sandbox", "--disable-setuid-sandbox", "--lang=de-DE", "--disable-dev-shm-usage", "--no-zygote", "--disable-gpu"];
    if (proxyServer) {
      if (needsProxyChain) {
        if (forceDebug) {
          dumpFieldsDebugMeta({
            accountLabel: account.email || `account-${accountId}`,
            step: "fields-proxy-chain-start",
            error: "",
            extra: { proxyType: selectedProxy?.type || "", host: selectedProxy?.host || "" },
            force: true
          });
        }
        try {
          anonymizedProxyUrl = await withTimeout(
            proxyChain.anonymizeProxy(proxyUrl),
            15000,
            "proxy-chain-timeout"
          );
        } catch (error) {
          dumpFieldsDebugMeta({
            accountLabel: account.email || `account-${accountId}`,
            step: "fields-proxy-chain-failed",
            error: error?.message || "proxy-chain-failed",
            extra: { proxyType: selectedProxy?.type || "", host: selectedProxy?.host || "" },
            force: true
          });
          res.status(502).json({ success: false, error: "Не удалось подключиться к прокси (proxy-chain)." });
          return;
        }
        if (forceDebug) {
          dumpFieldsDebugMeta({
            accountLabel: account.email || `account-${accountId}`,
            step: "fields-proxy-chain-ready",
            error: "",
            extra: { anonymizedProxyUrl },
            force: true
          });
        }
        launchArgs.push(`--proxy-server=${anonymizedProxyUrl}`);
      } else {
        launchArgs.push(`--proxy-server=${proxyServer}`);
      }
    }

    const extractCategoryTreeFromHtml = (html) => {
      if (!html) return null;
      const initIndex = html.indexOf("CategorySelectView.init");
      const searchFrom = initIndex >= 0 ? initIndex : 0;
      const markerIndex = html.indexOf("categoryTree", searchFrom);
      if (markerIndex === -1) return null;
      const colonIndex = html.indexOf(":", markerIndex);
      if (colonIndex === -1) return null;
      const startIndex = html.indexOf("{", colonIndex);
      if (startIndex === -1) return null;
      let depth = 0;
      let inString = false;
      let escaped = false;
      let endIndex = -1;
      for (let i = startIndex; i < html.length; i += 1) {
        const ch = html[i];
        if (inString) {
          if (escaped) {
            escaped = false;
          } else if (ch === "\\") {
            escaped = true;
          } else if (ch === "\"") {
            inString = false;
          }
          continue;
        }
        if (ch === "\"") {
          inString = true;
          continue;
        }
        if (ch === "{") {
          depth += 1;
        } else if (ch === "}") {
          depth -= 1;
          if (depth === 0) {
            endIndex = i;
            break;
          }
        }
      }
      if (endIndex === -1) return null;
      const jsonText = html.slice(startIndex, endIndex + 1);
      try {
        return JSON.parse(jsonText);
      } catch (error) {
        return null;
      }
    };

    const normalizeCategoryTreeLocal = (nodes) => {
      if (!Array.isArray(nodes)) return [];
      const extractIdFromUrl = (value) => {
        if (!value) return "";
        const match = String(value).match(/\/c(\d+)(?:\/|$)/);
        return match ? match[1] : "";
      };
      const normalizeName = (value) => String(value || "").replace(/\s+/g, " ").trim();
      const getChildCollections = (node) => {
        if (!node || typeof node !== "object") return [];
        const buckets = [];
        if (Array.isArray(node.children)) {
          buckets.push(node.children);
        }
        for (const value of Object.values(node)) {
          if (!Array.isArray(value)) continue;
          if (value.some((item) =>
            item && typeof item === "object" && (
              item.id ||
              item.identifier ||
              item.fieldValue ||
              item.value ||
              item.categoryId ||
              item.categoryName ||
              item.name ||
              item.label
            )
          )) {
            buckets.push(value);
          }
        }
        return buckets;
      };
      const pickChildren = (node) => {
        const collections = getChildCollections(node);
        if (!collections.length) return [];
        return collections.flat();
      };

      return nodes.map((node) => {
        const id = node?.id ||
          node?.identifier ||
          node?.fieldValue ||
          node?.value ||
          node?.categoryId ||
          extractIdFromUrl(node?.url || node?.categoryUrl || node?.seoUrl || "");
        const name = node?.name || node?.label || node?.categoryName || node?.title || "";
        const url = node?.url || node?.categoryUrl || node?.seoUrl || "";
        const children = pickChildren(node);
        return {
          id: id ? String(id) : "",
          name: normalizeName(name),
          url,
          children: normalizeCategoryTreeLocal(children)
        };
      }).filter((node) => node.id && node.name);
    };

    const findNode = (nodes, targetId, targetUrl) => {
      const normalizedUrl = (value) =>
        String(value || "")
          .replace(/^https?:\/\/www\.kleinanzeigen\.de/i, "")
          .replace(/\/$/, "");
      const matchNode = (node) => {
        if (!node) return false;
        if (targetId && String(node.id) === String(targetId)) return true;
        if (targetUrl) {
          const nodeUrl = normalizedUrl(node.url || "");
          return nodeUrl && nodeUrl === normalizedUrl(targetUrl);
        }
        return false;
      };
      let best = null;
      const visit = (node) => {
        if (!matchNode(node)) return;
        if (!best) {
          best = node;
          return;
        }
        const bestChildren = best?.children?.length || 0;
        const nodeChildren = node?.children?.length || 0;
        if (nodeChildren > bestChildren) {
          best = node;
          return;
        }
        if (nodeChildren === bestChildren && node?.url && !best?.url) {
          best = node;
        }
      };
      const walk = (items) => {
        for (const node of items || []) {
          visit(node);
          if (node.children?.length) {
            walk(node.children);
          }
        }
      };
      walk(nodes);
      return best;
    };

    let browser = null;
    const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
    const DEBUG_CATEGORIES = process.env.KL_DEBUG_CATEGORIES === "1";
    const dumpCategoryDebug = async (page, label) => {
      if (!DEBUG_CATEGORIES || !page) return;
      try {
        const debugDir = path.join(__dirname, "..", "data", "debug");
        if (!fs.existsSync(debugDir)) {
          fs.mkdirSync(debugDir, { recursive: true });
        }
        const safeLabel = String(label || "category").replace(/[^a-z0-9._-]+/gi, "_").slice(0, 60);
        const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
        const base = `category-${safeLabel}-${timestamp}`;
        const htmlPath = path.join(debugDir, `${base}.html`);
        const screenshotPath = path.join(debugDir, `${base}.png`);
        const html = await page.content().catch(() => "");
        if (html) {
          fs.writeFileSync(htmlPath, html, "utf8");
        }
        await page.screenshot({ path: screenshotPath, fullPage: true }).catch(() => {});
        console.log(`[categories/children] debug saved: ${htmlPath}`);
      } catch (error) {
        console.log(`[categories/children] debug dump failed: ${error.message}`);
      }
    };
    try {
      browser = await puppeteer.launch({
        headless: "new",
        args: launchArgs,
        timeout: PUPPETEER_LAUNCH_TIMEOUT,
        protocolTimeout: PUPPETEER_PROTOCOL_TIMEOUT
      });
      const page = await browser.newPage();
      page.setDefaultTimeout(PUPPETEER_NAV_TIMEOUT);
      page.setDefaultNavigationTimeout(PUPPETEER_NAV_TIMEOUT);
      if (!anonymizedProxyUrl && (selectedProxy?.username || selectedProxy?.password)) {
        await page.authenticate({
          username: selectedProxy.username || "",
          password: selectedProxy.password || ""
        });
      }

      await page.setUserAgent(deviceProfile.userAgent);
      await page.setViewport(deviceProfile.viewport);
      await page.setExtraHTTPHeaders({ "Accept-Language": deviceProfile.locale });
      await page.emulateTimezone(deviceProfile.timezone);
      try {
        await page.goto("https://www.kleinanzeigen.de/", { waitUntil: "domcontentloaded" });
      } catch (error) {
        await page.waitForSelector("body", { timeout: 60000 });
      }
      await acceptCookieModal(page, { timeout: 15000 }).catch(() => {});
      await acceptGdprConsent(page, { timeout: 15000 }).catch(() => {});
      await page.setCookie(...cookies);

      try {
        await page.goto("https://www.kleinanzeigen.de/p-anzeige-aufgeben-schritt2.html", { waitUntil: "domcontentloaded" });
      } catch (error) {
        await page.waitForSelector("body", { timeout: 60000 });
      }
      await acceptCookieModal(page, { timeout: 15000 }).catch(() => {});
      if (isGdprPage(page.url())) {
        await acceptGdprConsent(page, { timeout: 20000 });
        const redirectTarget = new URL(page.url()).searchParams.get("redirectTo");
        const target = redirectTarget ? decodeURIComponent(redirectTarget) : "https://www.kleinanzeigen.de/p-anzeige-aufgeben-schritt2.html";
        try {
          await page.goto(target, { waitUntil: "domcontentloaded" });
        } catch (error) {
          await page.waitForSelector("body", { timeout: 60000 });
        }
        await acceptCookieModal(page, { timeout: 15000 }).catch(() => {});
      }

      const sessionCookies = await page.cookies().catch(() => []);

      if (DEBUG_CATEGORIES) {
        await dumpCategoryDebug(page, `${id || "unknown"}-before-overlay`);
      }

      const tree = await page.evaluate(() => {
        const isCategoryArray = (arr) => Array.isArray(arr) && arr.length > 0 && arr.every((item) =>
          item && typeof item === "object" && (
            item.id ||
            item.identifier ||
            item.fieldValue ||
            item.value ||
            item.categoryId ||
            item.categoryName ||
            item.name ||
            item.label
          )
        );

        const findTree = (obj) => {
          if (!obj || typeof obj !== "object") return null;
          if (Array.isArray(obj)) {
            if (isCategoryArray(obj)) return obj;
            for (const item of obj) {
              const found = findTree(item);
              if (found) return found;
            }
            return null;
          }
          if (obj.categories && Array.isArray(obj.categories)) return obj.categories;
          if (obj.categoryTree) {
            if (Array.isArray(obj.categoryTree)) return obj.categoryTree;
            if (obj.categoryTree && typeof obj.categoryTree === "object") return [obj.categoryTree];
          }
          if (obj.categoryHierarchy && Array.isArray(obj.categoryHierarchy)) return obj.categoryHierarchy;
          if (obj.rootCategories && Array.isArray(obj.rootCategories)) return obj.rootCategories;
          if (obj.items && Array.isArray(obj.items) && isCategoryArray(obj.items)) return obj.items;
          if (obj.nodes && Array.isArray(obj.nodes) && isCategoryArray(obj.nodes)) return obj.nodes;
          for (const value of Object.values(obj)) {
            const found = findTree(value);
            if (found) return found;
          }
          return null;
        };
        const stateCandidates = [
          window.__INITIAL_STATE__,
          window.__PRELOADED_STATE__,
          window.__NEXT_DATA__,
          window.__NUXT__
        ];
        for (const candidate of stateCandidates) {
          const found = findTree(candidate);
          if (found) return found;
        }
        return null;
      });

      if (tree) {
        const normalized = normalizeCategoryTreeLocal(tree);
        const node = findNode(normalized, id, url);
        if (node?.children?.length) {
          children = node.children;
        }
      }

      if (!children.length && url) {
        try {
          const listingUrl = String(url || "");
          if (listingUrl) {
            let listingPage = null;
            try {
              listingPage = await browser.newPage();
              if (!anonymizedProxyUrl && (selectedProxy?.username || selectedProxy?.password)) {
                await listingPage.authenticate({
                  username: selectedProxy.username || "",
                  password: selectedProxy.password || ""
                });
              }
              await listingPage.setUserAgent(deviceProfile.userAgent);
              await listingPage.setViewport(deviceProfile.viewport);
              await listingPage.setExtraHTTPHeaders({
                "Accept-Language": deviceProfile.locale,
                "Referer": "https://www.kleinanzeigen.de/p-anzeige-aufgeben-schritt2.html"
              });
              await listingPage.emulateTimezone(deviceProfile.timezone);
              if (sessionCookies.length) {
                await listingPage.setCookie(...sessionCookies);
              }
              await listingPage.setCookie(...cookies);
              try {
                await listingPage.goto(listingUrl, { waitUntil: "domcontentloaded" });
              } catch (error) {
                await listingPage.waitForSelector("body", { timeout: 60000 });
              }
              await acceptCookieModal(listingPage, { timeout: 8000 }).catch(() => {});
              await listingPage.waitForFunction(() => {
                return Boolean(
                  document.querySelector("a[href*='audio_hifi']") ||
                  document.querySelector(".browsebox-itemlist") ||
                  document.querySelector("a[href*='path=']") ||
                  document.querySelector("a[href*='/c']") ||
                  document.querySelectorAll("a[href]").length > 10
                );
              }, { timeout: 15000 }).catch(() => {});
              const listingResult = await listingPage.evaluate((targetUrl) => {
              const normalize = (val) => (val || "").replace(/\s+/g, " ").trim();
              const extractIdFromHref = (value) => {
                if (!value) return "";
                const raw = String(value);
                const attrMatch = raw.match(/\+[^/]+:([^+/?&#]+)/);
                if (attrMatch) return attrMatch[1];
                const pathMatch = raw.match(/[?#]path=([^&]+)/i);
                if (pathMatch) {
                  let path = pathMatch[1] || "";
                  try {
                    path = decodeURIComponent(path);
                  } catch (error) {
                    // ignore decode errors
                  }
                  const parts = path.split("/").filter(Boolean);
                  return parts.length ? parts[parts.length - 1] : "";
                }
                const slugMatch = raw.match(/\/s-[^\/]+\/([^\/]+)\/c\d+(?:[/?+]|$)/);
                if (slugMatch) return slugMatch[1];
                const match = raw.match(/\/c(\d+)(?:\/|$)/);
                if (match) return match[1];
                return "";
              };

              const resolveHref = (href) => {
                if (!href) return "";
                if (/^https?:\/\//i.test(href)) return href;
                return `https://www.kleinanzeigen.de${href.startsWith("/") ? "" : "/"}${href}`;
              };

              const targetIdMatch = String(targetUrl || "").match(/\/c(\d+)(?:\/|$)/);
              const targetId = targetIdMatch ? targetIdMatch[1] : "";
              const targetToken = targetId ? `/c${targetId}` : "";

              const section = Array.from(document.querySelectorAll("section")).find((node) =>
                /kategorien/i.test(node.textContent || "")
              );
              const root = section || document;
              const links = Array.from(root.querySelectorAll("a[href]"));
              const results = new Map();
              const hrefSamples = [];
              let pathHrefCount = 0;
              let audioHrefCount = 0;
              links.forEach((link) => {
                const href = link.getAttribute("href") || "";
                const fullHref = resolveHref(href);
                if (hrefSamples.length < 5 && fullHref && /c\d+/.test(fullHref)) {
                  hrefSamples.push(fullHref);
                }
                if (fullHref && /[?#]path=/.test(fullHref)) pathHrefCount += 1;
                if (fullHref && /audio_hifi/i.test(fullHref)) audioHrefCount += 1;
                if (!fullHref) return;
                const isPathHref = /[?#]path=/.test(fullHref);
                if (isPathHref) {
                  const pathMatch = fullHref.match(/[?#]path=([^&]+)/i);
                  if (!pathMatch) return;
                  let path = pathMatch[1] || "";
                  try {
                    path = decodeURIComponent(path);
                  } catch (error) {
                    // ignore decode errors
                  }
                  if (targetId && !path.split("/").includes(targetId)) return;
                } else {
                  if (!/\/c\d+/.test(fullHref)) return;
                  if (targetId && !fullHref.includes(targetToken)) return;
                  if (!/\+/.test(fullHref) && !/\/s-[^\/]+\/[^\/]+\/c\d+/.test(fullHref)) return;
                }
                const name = normalize(link.textContent || link.getAttribute("aria-label") || "");
                if (!name || /alle kategorien/i.test(name)) return;
                const id = extractIdFromHref(fullHref);
                if (!id) return;
                if (!results.has(id)) {
                  results.set(id, { id, name, url: fullHref });
                }
              });
              return {
                children: Array.from(results.values()),
                hrefSamples,
                meta: {
                  url: window.location.href,
                  title: document.title,
                  anchorCount: links.length,
                  pathHrefCount,
                  audioHrefCount,
                  textLength: (document.body?.innerText || "").length
                }
              };
            }, listingUrl);

            const listingChildren = Array.isArray(listingResult?.children) ? listingResult.children : [];
            if (listingChildren.length) {
              children = listingChildren.map((item) => ({
                id: String(item.id),
                name: item.name,
                url: item.url,
                children: []
              }));
              if (DEBUG_CATEGORIES) {
                const sample = children.slice(0, 10).map((item) => `${item.id}:${item.name}`).join(", ");
                console.log(`[categories/children] listing dom parsed url=${listingUrl} count=${children.length} sample=${sample}`);
                if (listingResult?.hrefSamples?.length) {
                  console.log(`[categories/children] listing dom href sample: ${listingResult.hrefSamples.join(" | ")}`);
                }
                if (listingResult?.meta) {
                  console.log(`[categories/children] listing dom meta url=${listingResult.meta.url} title=${listingResult.meta.title} anchors=${listingResult.meta.anchorCount} pathHrefs=${listingResult.meta.pathHrefCount} audioHrefs=${listingResult.meta.audioHrefCount} textLen=${listingResult.meta.textLength}`);
                }
              }
            } else if (DEBUG_CATEGORIES) {
              console.log(`[categories/children] listing dom parsed url=${listingUrl} count=0`);
              if (listingResult?.hrefSamples?.length) {
                console.log(`[categories/children] listing dom href sample: ${listingResult.hrefSamples.join(" | ")}`);
              }
              if (listingResult?.meta) {
                console.log(`[categories/children] listing dom meta url=${listingResult.meta.url} title=${listingResult.meta.title} anchors=${listingResult.meta.anchorCount} pathHrefs=${listingResult.meta.pathHrefCount} audioHrefs=${listingResult.meta.audioHrefCount} textLen=${listingResult.meta.textLength}`);
              }
            }
            } finally {
              if (listingPage) {
                await listingPage.close();
              }
            }
          }
        } catch (error) {
          if (DEBUG_CATEGORIES) {
            console.log(`[categories/children] listing dom parse failed: ${error.message}`);
          }
        }
      }

      if (!children.length) {
        let selectionTree = null;
        let selectionPage = null;
        let selectionDomChildren = null;
        try {
          const targetId = id || (() => {
            const match = String(url || "").match(/\/c(\d+)(?:\/|$)/);
            return match ? match[1] : "";
          })();

          const decodeHtml = (value) =>
            String(value || "")
              .replace(/&amp;/g, "&")
              .replace(/&quot;/g, "\"")
              .replace(/&#39;/g, "'")
              .replace(/&nbsp;/g, " ")
              .replace(/&lt;/g, "<")
              .replace(/&gt;/g, ">");

          const stripTags = (value) => String(value || "").replace(/<[^>]*>/g, " ");
          const normalizeText = (value) => stripTags(value).replace(/\s+/g, " ").trim();

          const extractFromSelectionHtml = (html, sourceLabel, sourceUrl = "") => {
            if (!html) return [];
            const anchorRegex = /<a[^>]+href=(["'])(.*?)\1[^>]*>([\s\S]*?)<\/a>/gi;
            const children = new Map();
            const hrefSamples = [];
            let anchorCount = 0;
            let pathHrefCount = 0;
            let match = null;
            while ((match = anchorRegex.exec(html)) !== null) {
              anchorCount += 1;
              const rawHref = decodeHtml(match[2] || "");
              if (!/path=/.test(rawHref)) continue;
              pathHrefCount += 1;
              const fullHref = /^https?:\/\//i.test(rawHref)
                ? rawHref
                : `https://www.kleinanzeigen.de${rawHref.startsWith("/") ? "" : "/"}${rawHref}`;
              if (hrefSamples.length < 5) {
                hrefSamples.push(fullHref);
              }
              const pathMatch = fullHref.match(/[?#]path=([^&]+)/i);
              if (!pathMatch) continue;
              let path = pathMatch[1] || "";
              try {
                path = decodeURIComponent(path);
              } catch (error) {
                // ignore decode errors
              }
              const parts = path.split("/").filter(Boolean);
              if (!parts.length) continue;
              if (targetId && !parts.includes(String(targetId))) continue;
              if (parts.length < 2) continue;
              const leaf = parts[parts.length - 1];
              if (!leaf) continue;
              const name = normalizeText(match[3] || "");
              if (!name) continue;
              if (!children.has(leaf)) {
                children.set(leaf, {
                  id: leaf,
                  name,
                  url: fullHref
                });
              }
            }

            const parsedChildren = Array.from(children.values());
            if (DEBUG_CATEGORIES) {
              const sample = parsedChildren.slice(0, 10).map((item) => `${item.id}:${item.name}`).join(", ");
              console.log(`[categories/children] selection dom parsed source=${sourceLabel} count=${parsedChildren.length} sample=${sample}`);
              if (hrefSamples.length) {
                console.log(`[categories/children] selection dom href sample: ${hrefSamples.join(" | ")}`);
              }
              console.log(`[categories/children] selection dom meta url=${sourceUrl || ""} anchors=${anchorCount} pathHrefs=${pathHrefCount}`);
            }
            return parsedChildren;
          };

          try {
            await acceptCookieModal(page, { timeout: 8000 }).catch(() => {});
            await acceptGdprConsent(page, { timeout: 8000 }).catch(() => {});
            await page.waitForSelector("#adForm", { timeout: 8000 });
            const navPromise = page.waitForNavigation({ waitUntil: "domcontentloaded", timeout: 20000 }).catch(() => null);
            const didSubmit = await page.evaluate(() => {
              const form = document.querySelector("#adForm");
              if (!form) return false;
              form.setAttribute("action", "/p-kategorie-aendern.html");
              form.setAttribute("method", "post");
              form.submit();
              return true;
            });
            if (didSubmit) {
              await Promise.race([
                navPromise,
                page.waitForSelector("#postad-category-select-box, #postad-step1-headline, .category-selection-list", { timeout: 20000 }).catch(() => null)
              ]);
            }
            const inlineHtml = await page.content().catch(() => "");
            if (DEBUG_CATEGORIES) {
              await dumpCategoryDebug(page, `${id || "unknown"}-selection-inline`);
              console.log(`[categories/children] selection inline url=${page.url()}`);
            }
            const inlineTree = extractCategoryTreeFromHtml(inlineHtml);
            if (inlineTree) {
              const normalized = normalizeCategoryTreeLocal([inlineTree]);
              const node = findNode(normalized, targetId, url);
              if (node?.children?.length) {
                children = node.children;
                if (DEBUG_CATEGORIES) {
                  const sample = children.slice(0, 10).map((item) => `${item.id}:${item.name}`).join(", ");
                  console.log(`[categories/children] selection inline tree parsed count=${children.length} sample=${sample}`);
                }
              }
            }
            if (!children.length) {
              selectionDomChildren = extractFromSelectionHtml(inlineHtml, "inline", page.url());
            }
          } catch (error) {
            if (DEBUG_CATEGORIES) {
              console.log(`[categories/children] selection inline failed: ${error.message}`);
            }
          }

          if (!children.length && !selectionDomChildren?.length) {
            selectionPage = await browser.newPage();
            if (!anonymizedProxyUrl && (selectedProxy?.username || selectedProxy?.password)) {
              await selectionPage.authenticate({
                username: selectedProxy.username || "",
                password: selectedProxy.password || ""
              });
            }
            await selectionPage.setUserAgent(deviceProfile.userAgent);
            await selectionPage.setViewport(deviceProfile.viewport);
            await selectionPage.setExtraHTTPHeaders({
              "Accept-Language": deviceProfile.locale,
              "Referer": "https://www.kleinanzeigen.de/p-anzeige-aufgeben-schritt2.html"
            });
            await selectionPage.emulateTimezone(deviceProfile.timezone);
            if (sessionCookies.length) {
              await selectionPage.setCookie(...sessionCookies);
            }
            await selectionPage.setCookie(...cookies);

            let selectionResponse = null;
            try {
              await selectionPage.goto("https://www.kleinanzeigen.de/p-anzeige-aufgeben-schritt2.html", { waitUntil: "domcontentloaded" });
            } catch (error) {
              await selectionPage.waitForSelector("body", { timeout: 60000 });
            }
            await acceptCookieModal(selectionPage, { timeout: 15000 }).catch(() => {});
            if (isGdprPage(selectionPage.url())) {
              await acceptGdprConsent(selectionPage, { timeout: 20000 }).catch(() => {});
            }
            const selectionNavPromise = selectionPage.waitForNavigation({ waitUntil: "domcontentloaded", timeout: 20000 }).catch(() => null);
            const didSubmit = await selectionPage.evaluate(() => {
              const form = document.querySelector("#adForm");
              if (!form) return false;
              form.setAttribute("action", "/p-kategorie-aendern.html");
              form.setAttribute("method", "post");
              form.submit();
              return true;
            });
            if (didSubmit) {
              selectionResponse = await selectionNavPromise;
            }
            const isError = await selectionPage.evaluate(() => {
              const title = (document.title || "").toLowerCase();
              const header = document.querySelector(".outcomebox-error h1");
              const headerText = (header?.textContent || "").toLowerCase();
              return (title.includes("fehler") && title.includes("400")) || (headerText.includes("fehler") && headerText.includes("400"));
            });

            if (DEBUG_CATEGORIES) {
              await dumpCategoryDebug(selectionPage, `${id || "unknown"}-selection-page`);
              if (selectionResponse?.status) {
                console.log(`[categories/children] selection page status=${selectionResponse.status()} url=${selectionPage.url()}`);
              }
            }

            if (!isError) {
              const selectionHtml = await selectionPage.content().catch(() => "");
              selectionTree = extractCategoryTreeFromHtml(selectionHtml);
              if (!selectionDomChildren?.length) {
                selectionDomChildren = extractFromSelectionHtml(selectionHtml, "page", selectionPage.url());
              }
            } else if (DEBUG_CATEGORIES) {
              console.log("[categories/children] selection page returned 400 error");
            }

            if (!selectionDomChildren?.length && !selectionTree) {
              try {
                const step1Response = await selectionPage.goto("https://www.kleinanzeigen.de/p-anzeige-aufgeben-schritt1.html", { waitUntil: "domcontentloaded" });
                await acceptCookieModal(selectionPage, { timeout: 15000 }).catch(() => {});
                if (DEBUG_CATEGORIES) {
                  await dumpCategoryDebug(selectionPage, `${id || "unknown"}-selection-step1`);
                  if (step1Response?.status) {
                    console.log(`[categories/children] selection step1 status=${step1Response.status()} url=${selectionPage.url()}`);
                  }
                }
                const step1Html = await selectionPage.content().catch(() => "");
                selectionTree = selectionTree || extractCategoryTreeFromHtml(step1Html);
                if (!selectionDomChildren?.length) {
                  selectionDomChildren = extractFromSelectionHtml(step1Html, "step1", selectionPage.url());
                }
              } catch (error) {
                if (DEBUG_CATEGORIES) {
                  console.log(`[categories/children] selection step1 failed: ${error.message}`);
                }
              }
            }

            if (!isError && !selectionTree) {
              selectionTree = await selectionPage.evaluate(() => {
              const isCategoryArray = (arr) => Array.isArray(arr) && arr.length > 0 && arr.every((item) =>
                item && typeof item === "object" && (
                  item.id ||
                  item.identifier ||
                  item.fieldValue ||
                  item.value ||
                  item.categoryId ||
                  item.categoryName ||
                  item.name ||
                  item.label
                )
              );

              const findTree = (obj) => {
                if (!obj || typeof obj !== "object") return null;
                if (Array.isArray(obj)) {
                  if (isCategoryArray(obj)) return obj;
                  for (const item of obj) {
                    const found = findTree(item);
                    if (found) return found;
                  }
                  return null;
                }
                if (obj.categories && Array.isArray(obj.categories)) return obj.categories;
                if (obj.categoryTree) {
                  if (Array.isArray(obj.categoryTree)) return obj.categoryTree;
                  if (obj.categoryTree && typeof obj.categoryTree === "object") return [obj.categoryTree];
                }
                if (obj.categoryHierarchy && Array.isArray(obj.categoryHierarchy)) return obj.categoryHierarchy;
                if (obj.rootCategories && Array.isArray(obj.rootCategories)) return obj.rootCategories;
                if (obj.items && Array.isArray(obj.items) && isCategoryArray(obj.items)) return obj.items;
                if (obj.nodes && Array.isArray(obj.nodes) && isCategoryArray(obj.nodes)) return obj.nodes;
                for (const value of Object.values(obj)) {
                  const found = findTree(value);
                  if (found) return found;
                }
                return null;
              };

              const stateCandidates = [
                window.__INITIAL_STATE__,
                window.__PRELOADED_STATE__,
                window.__NEXT_DATA__,
                window.__NUXT__
              ];
              for (const candidate of stateCandidates) {
                const found = findTree(candidate);
                if (found) return found;
              }
              return null;
            });
          }
          }
        } catch (error) {
          if (DEBUG_CATEGORIES) {
            console.log(`[categories/children] selection page failed: ${error.message}`);
          }
        } finally {
          if (selectionPage) {
            await selectionPage.close();
          }
        }

        if (!children.length && Array.isArray(selectionDomChildren) && selectionDomChildren.length) {
          children = selectionDomChildren.map((item) => ({
            id: String(item.id),
            name: item.name,
            url: item.url,
            children: []
          }));
        }

        if (!children.length && selectionTree) {
          const normalized = normalizeCategoryTreeLocal(selectionTree);
          const node = findNode(normalized, targetId, url);
          if (node?.children?.length) {
            children = node.children;
            const needsUrl = children.some((child) => !child?.url);
            if (needsUrl) {
              const resolvedPath = await resolveCategoryPath();
              const pathIds = (resolvedPath || [])
                .map((item) => (item?.id ? String(item.id) : ""))
                .filter(Boolean);
              if (pathIds.length) {
                const baseUrl = "https://www.kleinanzeigen.de/p-kategorie-aendern.html?path=";
                children = children.map((child) => {
                  if (child?.url) return child;
                  const childId = child?.id ? String(child.id) : "";
                  if (!childId) return child;
                  const pathValue = encodeURIComponent([...pathIds, childId].join("/"));
                  return {
                    ...child,
                    url: `${baseUrl}${pathValue}`
                  };
                });
              }
            }
          }
        }
      }

      if (!children.length) {
        let graph = null;
        try {
          graph = await page.evaluate(() => {
            const nodes = {};
            const edges = {};
            const seen = new WeakSet();

            const normalizeName = (value) => (value || "").toString().replace(/\s+/g, " ").trim();
            const normalizeId = (value) => {
              if (value === null || value === undefined) return "";
              const raw = String(value).trim();
              const match = raw.match(/\/c(\d+)(?:\/|$)/);
              if (match) return match[1];
              return /^\d+$/.test(raw) ? raw : "";
            };
            const safeGet = (fn) => {
              try {
                return fn();
              } catch (error) {
                return undefined;
              }
            };
            const safeEntries = (obj) => {
              try {
                return Object.entries(obj);
              } catch (error) {
                return [];
              }
            };
            const safeValues = (obj) => {
              try {
                return Object.values(obj);
              } catch (error) {
                return [];
              }
            };
            const isWindowLike = (obj) => {
              if (!obj || typeof obj !== "object") return false;
              const tag = safeGet(() => Object.prototype.toString.call(obj));
              if (tag === "[object Window]") return true;
              if (safeGet(() => obj.window === obj)) return true;
              if (safeGet(() => obj.self === obj)) return true;
              return false;
            };

            const ensureNode = (id, name, url = "") => {
              if (!id) return;
              if (!nodes[id]) {
                nodes[id] = { id, name: normalizeName(name), url };
              } else if (!nodes[id].name && name) {
                nodes[id].name = normalizeName(name);
              }
              if (url && !nodes[id].url) nodes[id].url = url;
            };

            const addEdge = (fromId, toId) => {
              if (!fromId || !toId) return;
              if (!edges[fromId]) edges[fromId] = new Set();
              edges[fromId].add(toId);
            };

            const collectFromArray = (arr, parentId) => {
              if (!Array.isArray(arr)) return;
              arr.forEach((item) => {
                if (item && typeof item === "object") {
                  if (isWindowLike(item)) return;
                  const id = normalizeId(
                    safeGet(() => item.id) ||
                    safeGet(() => item.identifier) ||
                    safeGet(() => item.fieldValue) ||
                    safeGet(() => item.categoryId) ||
                    safeGet(() => item.categoryID) ||
                    safeGet(() => item.catId) ||
                    safeGet(() => item.value) ||
                    ""
                  );
                  const name = safeGet(() => item.name) || safeGet(() => item.label) || safeGet(() => item.categoryName) || safeGet(() => item.title) || "";
                  const url = safeGet(() => item.url) || safeGet(() => item.categoryUrl) || safeGet(() => item.seoUrl) || "";
                  if (id) {
                    ensureNode(id, name, url);
                    if (parentId) addEdge(parentId, id);
                  }
                  traverse(item, parentId);
                } else {
                  const id = normalizeId(item);
                  if (id && parentId) addEdge(parentId, id);
                }
              });
            };

            const traverse = (obj, parentId = "") => {
              if (!obj || typeof obj !== "object") return;
              if (isWindowLike(obj)) return;
              if (seen.has(obj)) return;
              seen.add(obj);

              const currentId = normalizeId(
                safeGet(() => obj.id) ||
                safeGet(() => obj.identifier) ||
                safeGet(() => obj.fieldValue) ||
                safeGet(() => obj.categoryId) ||
                safeGet(() => obj.categoryID) ||
                safeGet(() => obj.catId) ||
                safeGet(() => obj.value) ||
                ""
              );
              const currentName = safeGet(() => obj.name) || safeGet(() => obj.label) || safeGet(() => obj.categoryName) || safeGet(() => obj.title) || "";
              const currentUrl = safeGet(() => obj.url) || safeGet(() => obj.categoryUrl) || safeGet(() => obj.seoUrl) || "";
              if (currentId) {
                ensureNode(currentId, currentName, currentUrl);
                if (parentId) addEdge(parentId, currentId);
              }

              const collections = [];
              for (const [, value] of safeEntries(obj)) {
                if (Array.isArray(value)) {
                  collections.push({ value });
                }
              }
              collections.forEach(({ value }) => collectFromArray(value, currentId || parentId));

              for (const value of safeValues(obj)) {
                if (value && typeof value === "object") {
                  traverse(value, currentId || parentId);
                }
              }
            };

            const roots = [
              window.__INITIAL_STATE__,
              window.__PRELOADED_STATE__,
              window.__NEXT_DATA__,
              window.__NUXT__
            ];
            roots.forEach((root) => traverse(root, ""));

            const edgesPlain = {};
            Object.keys(edges).forEach((key) => {
              edgesPlain[key] = Array.from(edges[key]);
            });

            return { nodes, edges: edgesPlain };
          });
        } catch (error) {
          graph = null;
        }

        const targetId = id ? String(id) : "";
        if (graph?.edges && targetId && graph.edges[targetId]) {
          const childIds = graph.edges[targetId];
          const nextChildren = childIds
            .map((childId) => graph.nodes?.[childId])
            .filter((node) => node && node.id && node.name)
            .map((node) => ({
              id: String(node.id),
              name: node.name,
              url: node.url || "",
              children: []
            }));
          if (nextChildren.length) {
            children = nextChildren;
          }
        }
      }

      if (!children.length) {
        const path = await resolveCategoryPath();
        const level1 = path.length > 0 ? path[0]?.name : "";
        const level2 = path.length > 1 ? path[1]?.name : "";
        const level1Id = path.length > 0 && path[0]?.id ? String(path[0].id) : "";
        const level2Id = path.length > 1 && path[1]?.id ? String(path[1].id) : "";
        if (!level1 || !level2) {
          if (DEBUG_CATEGORIES) {
            console.log("[categories/children] resolveCategoryPath empty");
            await dumpCategoryDebug(page, `${id || "unknown"}-no-path`);
          }
        }
        if (level1 && level2) {
          const safeClick = async (element) => {
            if (!element) return false;
            try {
              await element.evaluate((node) => {
                node.scrollIntoView({ block: "center", inline: "center", behavior: "instant" });
              });
              await element.click({ delay: 40 });
              return true;
            } catch (error) {
              try {
                await element.evaluate((node) => node.click());
                return true;
              } catch (innerError) {
                return false;
              }
            }
          };

          const clickByText = async (texts) => {
            const candidates = Array.isArray(texts) ? texts : [texts];
            for (const text of candidates) {
              const xpath = `//button[contains(normalize-space(.), "${text}")] | //a[contains(normalize-space(.), "${text}")] | //span[contains(normalize-space(.), "${text}")]/ancestor::*[self::button or self::a][1]`;
              try {
                const handles = await page.$x(xpath);
                if (handles.length && await safeClick(handles[0])) return true;
              } catch (error) {
                // ignore lookup errors
              }
            }
            return false;
          };

          const clickById = async (targetId) => {
            if (!targetId) return false;
            const selectors = [
              `[data-val="${targetId}"]`,
              `[data-id="${targetId}"]`,
              `[data-value="${targetId}"]`,
              `#cat_${targetId}`,
              `a[href*="/c${targetId}"]`
            ];
            for (const selector of selectors) {
              let element = null;
              try {
                element = await page.$(selector);
              } catch (error) {
                element = null;
              }
              if (element && await safeClick(element)) return true;
            }
            return false;
          };

          const waitForCategoryOverlay = async () => {
            try {
              await page.waitForFunction(() => {
                return Boolean(
                  document.querySelector("#postad-category-select-box") ||
                  document.querySelector(".category-selection-col") ||
                  document.querySelector(".category-selection-list-item-link")
                );
              }, { timeout: 8000 });
              return true;
            } catch (error) {
              return false;
            }
          };

          const isErrorPage = async () => {
            try {
              return await page.evaluate(() => {
                const title = (document.title || "").toLowerCase();
                if (title.includes("fehler") && title.includes("400")) return true;
                const header = document.querySelector(".outcomebox-error h1");
                const headerText = (header?.textContent || "").toLowerCase();
                return headerText.includes("fehler") && headerText.includes("400");
              });
            } catch (error) {
              return false;
            }
          };

          const recoverToPostAdForm = async () => {
            const errored = await isErrorPage();
            if (!errored) return false;
            try {
              await page.goto("https://www.kleinanzeigen.de/p-anzeige-aufgeben-schritt2.html", { waitUntil: "domcontentloaded" });
            } catch (error) {
              await page.waitForSelector("body", { timeout: 60000 });
            }
            await acceptCookieModal(page, { timeout: 8000 }).catch(() => {});
            if (isGdprPage(page.url())) {
              await acceptGdprConsent(page, { timeout: 20000 }).catch(() => {});
              const redirectTarget = new URL(page.url()).searchParams.get("redirectTo");
              const target = redirectTarget ? decodeURIComponent(redirectTarget) : "https://www.kleinanzeigen.de/p-anzeige-aufgeben-schritt2.html";
              try {
                await page.goto(target, { waitUntil: "domcontentloaded" });
              } catch (error) {
                await page.waitForSelector("body", { timeout: 60000 });
              }
              await acceptCookieModal(page, { timeout: 8000 }).catch(() => {});
            }
            return true;
          };

          const openCategorySelection = async () => {
            await page.waitForFunction(
              () => Boolean(window?.Belen?.PostAd?.PostAdView || document.querySelector("#pstad-lnk-chngeCtgry")),
              { timeout: 5000 }
            ).catch(() => {});
            const selectors = [
              "#pstad-lnk-chngeCtgry",
              "#categorySection a"
            ];
            for (const selector of selectors) {
              const element = await page.$(selector);
              if (!element) continue;
              if (await safeClick(element)) {
                return true;
              }
            }
            const byText = await clickByText(["Wähle deine Kategorie", "Kategorie wählen"]);
            if (byText) return true;
            try {
              const invoked = await page.evaluate(() => {
                const view = window?.Belen?.PostAd?.PostAdView;
                const candidates = [
                  "openCategorySelection",
                  "openCategorySelector",
                  "selectCategory",
                  "openCategory",
                  "showCategorySelection"
                ];
                if (view) {
                  for (const key of candidates) {
                    if (typeof view[key] === "function") {
                      view[key]();
                      return true;
                    }
                  }
                }
                const link = document.querySelector("#pstad-lnk-chngeCtgry");
                if (link) {
                  link.dispatchEvent(new MouseEvent("click", { bubbles: true, cancelable: true }));
                  return true;
                }
                return false;
              });
              if (invoked) return true;
            } catch (error) {
              // ignore evaluate errors
            }
            return false;
          };

          await delay(800);
          await acceptCookieModal(page, { timeout: 8000 });
          await page.waitForSelector("#categorySection", { timeout: 10000 }).catch(() => {});
          await openCategorySelection();
          let navigationHappened = false;
          try {
            const navResult = await Promise.race([
              page.waitForNavigation({ waitUntil: "domcontentloaded", timeout: 8000 }).then(() => "nav").catch(() => null),
              page.waitForFunction(
                () => Boolean(document.querySelector("#postad-category-select-box")),
                { timeout: 8000 }
              ).then(() => "overlay").catch(() => null)
            ]);
            navigationHappened = navResult === "nav";
          } catch (error) {
            navigationHappened = false;
          }
          let overlayReady = await waitForCategoryOverlay();
          if (!overlayReady) {
            if (await recoverToPostAdForm()) {
              await openCategorySelection();
              overlayReady = await waitForCategoryOverlay();
            }
          } else if (navigationHappened) {
            await acceptCookieModal(page, { timeout: 8000 }).catch(() => {});
            if (isGdprPage(page.url())) {
              await acceptGdprConsent(page, { timeout: 20000 }).catch(() => {});
            }
          }
          await delay(1200);
          await acceptCookieModal(page, { timeout: 8000 });
          let resolvedByTree = false;
          try {
            const html = await page.content().catch(() => "");
            const treeFromHtml = extractCategoryTreeFromHtml(html);
            if (treeFromHtml) {
              const normalized = normalizeCategoryTreeLocal(Array.isArray(treeFromHtml) ? treeFromHtml : [treeFromHtml]);
              const node = findNode(normalized, id, url);
              if (node?.children?.length) {
                children = node.children;
                resolvedByTree = true;
              }
            }
          } catch (error) {
            resolvedByTree = false;
          }

          if (!resolvedByTree) {
            if (!(await clickById(level1Id))) {
              await clickByText(level1);
            }
            await delay(800);
            await acceptCookieModal(page, { timeout: 8000 });
            if (!(await clickById(level2Id))) {
              await clickByText(level2);
            }
            await delay(1500);
            await acceptCookieModal(page, { timeout: 8000 });

            // Ожидаем появления L3 категорий
            try {
              await page.waitForFunction(() => {
                const container = document.querySelector("#postad-category-select-box");
                if (!container) return false;
                const cols = Array.from(container.querySelectorAll(".category-selection-col")).filter((col) => {
                  const style = window.getComputedStyle(col);
                  return style.display !== "none" && style.visibility !== "hidden" && !col.classList.contains("is-hidden");
                });
                return cols.length >= 3;
              }, { timeout: 5000 });
            } catch (error) {
              // ignore timeout
            }

            await delay(500);
            if (DEBUG_CATEGORIES) {
              await dumpCategoryDebug(page, `${id || "unknown"}-after-clicks`);
            }

            let domChildren = [];
            try {
              domChildren = await page.evaluate(() => {
              const normalize = (val) => (val || "").replace(/\s+/g, " ").trim();
              const extractIdFromHref = (value) => {
                if (!value) return "";
                const raw = String(value);
                const match = raw.match(/\/c(\d+)(?:\/|$)/);
                if (match) return match[1];
                const pathMatch = raw.match(/[?#]path=([^&]+)/i);
                if (pathMatch) {
                  let path = pathMatch[1] || "";
                  try {
                    path = decodeURIComponent(path);
                  } catch (error) {
                    // ignore decode errors
                  }
                  const parts = path.split("/").filter(Boolean);
                  return parts.length ? parts[parts.length - 1] : "";
                }
                return "";
              };
              const extractIdFromAttr = (value) => {
                if (!value) return "";
                const raw = String(value).trim();
                return raw.startsWith("cat_") ? raw.slice(4) : "";
              };
              const extractId = (node) => {
                if (!node) return "";
                const dataVal = node.getAttribute("data-val") || node.getAttribute("data-id") || node.getAttribute("data-value") || "";
                if (dataVal) return String(dataVal).trim();
                const idAttr = extractIdFromAttr(node.getAttribute("id") || "");
                if (idAttr) return idAttr;
                const href = node.getAttribute("href") || "";
                return extractIdFromHref(href);
              };

              const extractFromContainer = () => {
                const container = document.querySelector("#postad-category-select-box");
                if (!container) return [];
                const cols = Array.from(container.querySelectorAll(".category-selection-col")).filter((col) => {
                  const style = window.getComputedStyle(col);
                  if (style.display === "none" || style.visibility === "hidden") return false;
                  if (col.classList.contains("is-hidden")) return false;
                  return Boolean(col.querySelector(".category-selection-list-item-link"));
                });
                if (!cols.length) return [];
                let rightmost = cols[0];
                let maxX = -Infinity;
                cols.forEach((col) => {
                  const rect = col.getBoundingClientRect();
                  if (rect.x > maxX) {
                    maxX = rect.x;
                    rightmost = col;
                  }
                });
                const links = Array.from(rightmost.querySelectorAll(".category-selection-list-item-link"));
                const unique = new Map();
                links.forEach((link) => {
                  const id = extractId(link);
                  const text = normalize(link.textContent || link.getAttribute("aria-label") || "");
                  if (!id || !text) return;
                  if (!unique.has(id)) {
                    unique.set(id, {
                      id,
                      name: text,
                      url: link.getAttribute("href") || ""
                    });
                  }
                });
                return Array.from(unique.values());
              };

              const containerChildren = extractFromContainer();
              if (containerChildren.length) return containerChildren;

              const collectRoots = (root, bucket) => {
                if (!root || bucket.includes(root)) return;
                bucket.push(root);
                const elements = root.querySelectorAll ? root.querySelectorAll("*") : [];
                elements.forEach((el) => {
                  if (el.shadowRoot) collectRoots(el.shadowRoot, bucket);
                });
              };
              const roots = [];
              collectRoots(document, roots);

              const candidates = [];
              roots.forEach((root) => {
                const nodes = Array.from(
                  root.querySelectorAll("a, button, [role='button'], li, div, span")
                );
                nodes.forEach((node) => {
                  const rect = node.getBoundingClientRect();
                  const text = normalize(node.textContent || node.getAttribute("aria-label") || "");
                  const href = node.getAttribute("href") || "";
                  const id = extractId(node);
                  if (!text || !id) return;
                  if (rect.width <= 0 || rect.height <= 0) return;
                  candidates.push({
                    text,
                    id,
                    href,
                    x: rect.x,
                    width: rect.width
                  });
                });
              });

              if (!candidates.length) return [];

              const sorted = candidates.slice().sort((a, b) => a.x - b.x);
              const clusters = [];
              const threshold = 60;
              sorted.forEach((item) => {
                const last = clusters[clusters.length - 1];
                if (!last || Math.abs(last.x - item.x) > threshold) {
                  clusters.push({ x: item.x, items: [item] });
                } else {
                  last.items.push(item);
                }
              });

              const rightmost = clusters.sort((a, b) => b.x - a.x)[0];
              const items = rightmost ? rightmost.items : [];
              const unique = new Map();
              items.forEach((item) => {
                if (!item.id || !item.text) return;
                if (!unique.has(item.id)) {
                  unique.set(item.id, {
                    id: item.id,
                    name: item.text,
                    url: item.href || ""
                  });
                }
              });
                return Array.from(unique.values());
              });
            } catch (error) {
              if (DEBUG_CATEGORIES) {
                console.log(`[categories/children] dom extraction failed: ${error.message}`);
              }
              domChildren = [];
            }

            if (domChildren.length) {
              children = domChildren.map((item) => ({
                id: String(item.id),
                name: item.name,
                url: item.url,
                children: []
              }));
            } else {
              await dumpCategoryDebug(page, `${id || "unknown"}-empty`);
            }
          }
        }
      }
    } catch (error) {
      console.error("[categories/children] puppeteer fallback failed:", error.message);
    } finally {
      if (browser) {
        await browser.close();
      }
      if (anonymizedProxyUrl) {
        await proxyChain.closeAnonymizedProxy(anonymizedProxyUrl, true);
      }
    }

      const finalChildren = dedupeChildren(children);
      if (cacheKey) {
        setCachedCategoryChildren(cacheKey, finalChildren, { empty: finalChildren.length === 0 });
      }
      if (DEBUG_CATEGORIES) {
        const sample = finalChildren.slice(0, 10).map((item) => `${item.id}:${item.name}`).join(", ");
        console.log(`[categories/children] response id=${id} url=${url} count=${finalChildren.length} sample=${sample}`);
      }
      res.json({ children: finalChildren });
  } catch (error) {
    console.error("[categories/children] failed:", error.message);
    res.status(500).json({ success: false, error: error.message });
  }
});

app.get("/api/ads/fields", async (req, res) => {
  let requestId = "";
  let debugEnabled = false;
  let logFields = null;
  let requestStartedAt = 0;
  try {
    requestId = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const accountId = req.query.accountId ? Number(req.query.accountId) : null;
    const categoryIdParam = req.query.categoryId ? String(req.query.categoryId) : "";
    const categoryUrl = req.query.categoryUrl ? String(req.query.categoryUrl) : "";
    const categoryPathRaw = req.query.categoryPath ? String(req.query.categoryPath) : "";
    const parseCategoryPath = (value) => {
      if (!value) return [];
      try {
        const parsed = JSON.parse(value);
        if (Array.isArray(parsed)) return parsed;
      } catch (error) {
        // ignore json errors
      }
      return String(value)
        .split(/[>,]/)
        .map((item) => item.trim())
        .filter(Boolean);
    };
    const extractNumericId = (item) => {
      const raw = String(item || "").trim();
      if (!raw) return "";
      if (/^\d+$/.test(raw)) return raw;
      const pathMatch = raw.match(/path=([^&]+)/i);
      if (pathMatch) {
        try {
          const decoded = decodeURIComponent(pathMatch[1]);
          const parts = decoded.split("/").filter(Boolean);
          const last = parts[parts.length - 1];
          if (last && /^\d+$/.test(last)) return last;
        } catch (error) {
          // ignore decode errors
        }
      }
      const idMatch = raw.match(/\/c(\d+)(?:\/|$)/i) || raw.match(/(\d{2,})/);
      return idMatch ? idMatch[1] : "";
    };
    const categoryPathItems = parseCategoryPath(categoryPathRaw);
    const categoryPathIdsFromRequest = categoryPathItems
      .map(extractNumericId)
      .filter(Boolean);
    const resolvedCategoryId = categoryIdParam
      || (categoryPathIdsFromRequest.length ? categoryPathIdsFromRequest[categoryPathIdsFromRequest.length - 1] : "")
      || extractCategoryIdFromUrl(categoryUrl);
    const forceRefresh = req.query.refresh === "true";
    const forceDebug = true;
    debugEnabled = true;
    requestStartedAt = Date.now();
    logFields = (payload) => {
      if (!debugEnabled) return;
      appendFieldsRequestLog({
        requestId,
        accountId,
        categoryIdParam,
        categoryUrl,
        categoryPathRaw: categoryPathRaw ? String(categoryPathRaw).slice(0, 500) : "",
        ...payload
      });
    };
    logFields({ event: "start" });
    if (!accountId || !resolvedCategoryId) {
      logFields({ event: "error", error: "missing-params" });
      res.status(400).json({
        success: false,
        error: "accountId и categoryId/categoryUrl обязательны",
        debugId: debugEnabled ? requestId : undefined
      });
      return;
    }
    const categoryId = resolvedCategoryId;

    if (forceDebug) {
      dumpFieldsDebugMeta({
        accountLabel: `account-${accountId}`,
        step: "fields-start",
        error: "",
        extra: { accountId, categoryId, categoryUrl },
        force: true
      });
    }

    res.setTimeout(240000, () => {
      if (res.headersSent) return;
      dumpFieldsDebugMeta({
        accountLabel: `account-${accountId}`,
        step: "fields-timeout",
        error: "request-timeout",
        extra: { accountId, categoryId },
        force: forceDebug
      });
      logFields({ event: "timeout", durationMs: Date.now() - requestStartedAt });
      res.status(504).json({
        success: false,
        error: "Таймаут при загрузке параметров категории.",
        debugId: debugEnabled ? requestId : undefined
      });
    });

    const allowCachedEmpty = req.query.allowCachedEmpty === "1";
    if (!forceRefresh) {
      const cached = getCachedCategoryFields(categoryId);
      if (cached !== null) {
        const cachedFields = Array.isArray(cached) ? cached : [];
        if (!cachedFields.length && !allowCachedEmpty) {
          logFields({ event: "cache-empty-bypass" });
        } else {
          logFields({ event: "cache-hit", count: cachedFields.length });
          res.json({
            fields: cachedFields,
            cached: true,
            debugId: debugEnabled ? requestId : undefined
          });
          return;
        }
      }
    }

    const account = getAccountForRequest(accountId, req, res);
    if (!account) return;
    const selectedProxy = requireAccountProxy(account, res, "загрузки параметров категории", getOwnerContext(req));
    if (!selectedProxy) {
      logFields({ event: "error", error: "proxy-required" });
      return;
    }

    const puppeteer = getPuppeteer();
    const proxyChain = require("proxy-chain");
    const { parseCookies, normalizeCookie, buildProxyServer, buildProxyUrl } = require("./cookieUtils");
    const { pickDeviceProfile } = require("./cookieValidator");

    const cookies = parseCookies(account.cookie).map(normalizeCookie);
    const deviceProfile = toDeviceProfile(account.deviceProfile);
    const proxyServer = buildProxyServer(selectedProxy);
    const proxyUrl = buildProxyUrl(selectedProxy);
    const needsProxyChain = Boolean(
      proxyUrl && ((selectedProxy?.type || "").toLowerCase().startsWith("socks") || selectedProxy?.username || selectedProxy?.password)
    );
    let anonymizedProxyUrl;

    const launchArgs = ["--no-sandbox", "--disable-setuid-sandbox", "--lang=de-DE", "--disable-dev-shm-usage", "--no-zygote", "--disable-gpu"];
    if (proxyServer) {
      if (needsProxyChain) {
        anonymizedProxyUrl = await proxyChain.anonymizeProxy(proxyUrl);
        launchArgs.push(`--proxy-server=${anonymizedProxyUrl}`);
      } else {
        launchArgs.push(`--proxy-server=${proxyServer}`);
      }
    }

    if (forceDebug) {
      dumpFieldsDebugMeta({
        accountLabel: account.email || `account-${accountId}`,
        step: "fields-launching-browser",
        error: "",
        extra: { hasProxy: Boolean(selectedProxy), proxyType: selectedProxy?.type || "" },
        force: true
      });
    }

    const profileDir = fs.mkdtempSync(path.join(os.tmpdir(), "kl-fields-"));

    const browser = await withTimeout(
      puppeteer.launch({
        headless: "new",
        args: launchArgs,
        userDataDir: profileDir,
        timeout: PUPPETEER_LAUNCH_TIMEOUT,
        protocolTimeout: PUPPETEER_PROTOCOL_TIMEOUT
      }),
      25000,
      "launch-timeout"
    );

    if (forceDebug) {
      dumpFieldsDebugMeta({
        accountLabel: account.email || `account-${accountId}`,
        step: "fields-browser-launched",
        error: "",
        extra: { profileDir },
        force: true
      });
    }

    try {
      const page = await browser.newPage();
      if (forceDebug) {
        dumpFieldsDebugMeta({
          accountLabel: account.email || `account-${accountId}`,
          step: "fields-page-created",
          error: "",
          extra: { url: page.url() },
          force: true
        });
      }
      if (forceDebug) {
        dumpFieldsDebugMeta({
          accountLabel: account.email || `account-${accountId}`,
          step: "fields-browser-ready",
          error: "",
          extra: { url: page.url() }
        });
      }
      page.setDefaultNavigationTimeout(PUPPETEER_NAV_TIMEOUT);
      page.setDefaultTimeout(20000);
      if (!anonymizedProxyUrl && (selectedProxy?.username || selectedProxy?.password)) {
        await page.authenticate({
          username: selectedProxy.username || "",
          password: selectedProxy.password || ""
        });
      }

      await page.setUserAgent(deviceProfile.userAgent);
      await page.setViewport(deviceProfile.viewport);
      await page.setExtraHTTPHeaders({ "Accept-Language": deviceProfile.locale });
      await page.emulateTimezone(deviceProfile.timezone);
      if (cookies.length) {
        await page.setCookie(...cookies);
      }
      logFields({ event: "browser-ready", durationMs: Date.now() - requestStartedAt });

      let step2Error = null;
      try {
        await page.goto("https://www.kleinanzeigen.de/p-anzeige-aufgeben-schritt2.html", { waitUntil: "domcontentloaded", timeout: 20000 });
      } catch (error) {
        step2Error = error;
      }
      if (step2Error) {
        if (forceDebug) {
          dumpFieldsDebugMeta({
            accountLabel: account.email || `account-${accountId}`,
            step: "fields-step2-failed",
            error: step2Error?.message || "step2-failed",
            extra: { url: page.url() },
            force: true
          });
        }
        try {
          await page.goto("https://www.kleinanzeigen.de/p-anzeige-aufgeben-schritt2.html", { waitUntil: "networkidle2", timeout: 22000 });
          step2Error = null;
        } catch (error) {
          step2Error = error;
        }
      }
      if (forceDebug) {
        dumpFieldsDebugMeta({
          accountLabel: account.email || `account-${accountId}`,
          step: step2Error ? "fields-step2-failed-final" : "fields-step2-loaded",
          error: step2Error ? (step2Error?.message || "step2-failed") : "",
          extra: { url: page.url() },
          force: true
        });
      }
      logFields({ event: "step2-opened", durationMs: Date.now() - requestStartedAt });
      if (step2Error) {
        throw step2Error;
      }
      await acceptCookieModal(page, { timeout: 15000 }).catch(() => {});
      if (isGdprPage(page.url())) {
        await acceptGdprConsent(page, { timeout: 20000 });
        const redirectTarget = new URL(page.url()).searchParams.get("redirectTo");
        const target = redirectTarget ? decodeURIComponent(redirectTarget) : "https://www.kleinanzeigen.de/p-anzeige-aufgeben-schritt2.html";
        await page.goto(target, { waitUntil: "domcontentloaded", timeout: 20000 });
      }

    const categoryPathIds = categoryPathIdsFromRequest.length
      ? categoryPathIdsFromRequest
      : (resolveCategoryPathFromCache(categoryId) || (categoryId ? [categoryId] : []));
    const selectionUrl = categoryPathIds.length > 1
      ? `https://www.kleinanzeigen.de/p-kategorie-aendern.html?path=${encodeURIComponent(categoryPathIds.join("/"))}`
      : getCategorySelectionUrl(categoryId, categoryUrl);

      const injectCategoryId = async () => {
        try {
          await page.evaluate(({ categoryId }) => {
            const selectors = [
              "#categoryIdField",
              "input[name='categoryId']",
              "select[name='categoryId']",
              "input[id*='categoryId']",
              "select[id*='categoryId']",
              "input[id*='category']",
              "select[id*='category']"
            ];
            selectors.forEach((selector) => {
              const el = document.querySelector(selector);
              if (!el) return;
              el.value = categoryId;
              el.dispatchEvent(new Event("input", { bubbles: true }));
              el.dispatchEvent(new Event("change", { bubbles: true }));
            });
          }, { categoryId });
          return true;
        } catch (error) {
          return false;
        }
      };

      const waitForExtraSelect = async (timeoutMs) => {
        try {
          await page.waitForFunction(() => {
            const normalize = (value) => (value || "").replace(/\s+/g, " ").trim().toLowerCase();
            const collectRoots = (root, bucket) => {
              if (!root || bucket.includes(root)) return;
              bucket.push(root);
              const elements = root.querySelectorAll ? root.querySelectorAll("*") : [];
              elements.forEach((el) => {
                if (el && el.shadowRoot) collectRoots(el.shadowRoot, bucket);
              });
            };
            const roots = [];
            collectRoots(document, roots);
            const queryAllDeep = (selector) => {
              const result = [];
              roots.forEach((root) => {
                try {
                  result.push(...Array.from(root.querySelectorAll(selector)));
                } catch (error) {
                  // ignore selector error
                }
              });
              return result;
            };

            const selects = queryAllDeep("select");
            const hasAttributeSelect = selects.some((select) => {
              const name = normalize(select.getAttribute("name") || "");
              const id = normalize(select.getAttribute("id") || "");
              if (name.includes("attributemap") || id.includes("attributemap")) return true;
              const wrapper = select.closest(".formgroup-input, .form-group, .l-row, div, section");
              const label = wrapper ? wrapper.querySelector("label") : null;
              const labelText = normalize(label?.textContent || select.getAttribute("aria-label") || "");
              return labelText.includes("art") || labelText.includes("zustand");
            });
            if (hasAttributeSelect) return true;

            const maybeCombobox = queryAllDeep("[role='combobox'], [aria-haspopup='listbox'], button, div")
              .some((node) => {
                const text = normalize(node.textContent || node.getAttribute("aria-label") || "");
                return text.includes("bitte wählen") || text.includes("bitte waehlen");
              });
            return maybeCombobox;
          }, { timeout: timeoutMs });
        } catch (error) {
          // ignore wait timeout
        }
      };

      const collectFields = async (maxWaitMs = 20000) => {
        await waitForExtraSelect(Math.min(8000, maxWaitMs));
        const formContext = await page.waitForSelector("form", { timeout: 15000 }).then(() => page).catch(() => page);
        let fields = [];
        const startedAt = Date.now();
        while (Date.now() - startedAt < maxWaitMs) {
          fields = await parseExtraSelectFieldsAcrossContexts(page).catch(() => []);
          if (fields.length) break;
          await delay(700);
        }
        if (!fields.length) {
          fields = await parseExtraSelectFields(formContext).catch(() => []);
        }
        return fields;
      };

      const submitCategoryViaStep2Form = async () => {
        try {
          const submitted = await page.evaluate(({ categoryId, categoryPathIds }) => {
            const sanitize = (value) => {
              const raw = String(value || "").trim();
              const match = raw.match(/\d+/);
              return match ? match[0] : "";
            };
            const ids = (Array.isArray(categoryPathIds) ? categoryPathIds : [])
              .map(sanitize)
              .filter(Boolean);
            const fallbackId = sanitize(categoryId);
            if (!ids.length && fallbackId) ids.push(fallbackId);
            if (!ids.length) return false;

            const form = document.querySelector("#postad-step1-frm") || document.querySelector("form");
            if (!form) return false;

            const applyField = (name, value) => {
              let field = form.querySelector(`input[name="${name}"], select[name="${name}"]`);
              if (!field) {
                field = document.createElement("input");
                field.type = "hidden";
                field.name = name;
                form.appendChild(field);
              }
              field.value = String(value ?? "");
            };

            if (ids.length > 1) {
              applyField("parentCategoryId", ids[0]);
            }
            applyField("categoryId", ids[ids.length - 1]);
            applyField("submitted", "true");
            form.submit();
            return true;
          }, { categoryId, categoryPathIds });

          if (!submitted) return false;
          await Promise.race([
            page.waitForNavigation({ waitUntil: "domcontentloaded", timeout: 12000 }).catch(() => false),
            page.waitForFunction(
              () => window.location.href.includes("anzeige-aufgeben-schritt2"),
              { timeout: 12000 }
            ).catch(() => false)
          ]);
          return true;
        } catch (error) {
          return false;
        }
      };

      await injectCategoryId();
      let fields = await collectFields(8000);
      if (fields.length) {
        await dumpFieldsDebug(page, {
          accountLabel: account.email || `account-${accountId}`,
          step: "fields-fast-path",
          error: "",
          extra: { url: page.url(), categoryId, categoryPathIds },
          force: forceDebug
        });
        setCachedCategoryFields(categoryId, fields);
        if (!res.headersSent) {
          logFields({ event: "success-fast-path", count: fields.length, durationMs: Date.now() - requestStartedAt });
          res.json({
            fields,
            debugId: debugEnabled ? requestId : undefined
          });
        }
        return;
      }
      logFields({ event: "fast-path-empty", durationMs: Date.now() - requestStartedAt });

      const quickSubmitted = await submitCategoryViaStep2Form();
      if (quickSubmitted) {
        await dumpFieldsDebug(page, {
          accountLabel: account.email || `account-${accountId}`,
          step: "fields-fast-submit",
          error: "",
          extra: { url: page.url(), categoryId, categoryPathIds },
          force: forceDebug
        });
        fields = await collectFields(7000);
        if (fields.length) {
          setCachedCategoryFields(categoryId, fields);
          logFields({ event: "success-fast-submit", count: fields.length, durationMs: Date.now() - requestStartedAt });
          if (!res.headersSent) {
            res.json({
              fields,
              debugId: debugEnabled ? requestId : undefined
            });
          }
          return;
        }
      }
      logFields({ event: "fast-submit-empty", durationMs: Date.now() - requestStartedAt });

      const applyCategorySelection = async () => {
        if (!categoryPathIds.length) return false;
        const baseSelectionUrl = "https://www.kleinanzeigen.de/p-kategorie-aendern.html";
        const requestedSelectionUrl = selectionUrl || baseSelectionUrl;
        let usedSelectionUrl = "";
        let selectionStatus = 0;
        let selectionTitle = "";
        let openedVia = "";

        const isSelectionErrorPage = async () => {
          const title = await page.title().catch(() => "");
          if (/fehler|error/i.test(title)) return true;
          try {
            return await page.evaluate(() => {
              const text = (document.body?.innerText || "").toLowerCase();
              return text.includes("fehler [400]") || text.includes("fehler 400");
            });
          } catch (error) {
            return false;
          }
        };

        const waitForCategorySelectionContent = async (timeoutMs = 20000) => {
          try {
            await page.waitForFunction(() => {
              const listItems = document.querySelectorAll(
                ".category-selection-list-item-link, [id^='cat_'], [data-category-id], a[href*='path=']"
              );
              const tree = window.Belen?.PostAd?.CategorySelectView?.categoryTree
                || window.Belen?.PostAd?.CategorySelectView?.model?.categoryTree
                || window.Belen?.PostAd?.CategorySelectView?.options?.categoryTree
                || window.categoryTree;
              const form = document.querySelector("#postad-step1-frm") || document.querySelector("form");
              return (listItems && listItems.length > 0) || Boolean(tree) || Boolean(form && listItems.length);
            }, { timeout: timeoutMs });
            return true;
          } catch (error) {
            return false;
          }
        };

        const openSelectionPage = async (url) => {
          const response = await page.goto(url, { waitUntil: "domcontentloaded", timeout: 20000 });
          const status = response?.status?.() || 0;
          const title = await page.title().catch(() => "");
          const errorPage = status >= 400 || /fehler|error/i.test(title);
          return { status, title, errorPage };
        };

        let navResult = null;
        try {
          if (await openCategorySelection(page)) {
            openedVia = "click";
            usedSelectionUrl = page.url();
            selectionTitle = await page.title().catch(() => "");
            navResult = { status: 0, title: selectionTitle, errorPage: await isSelectionErrorPage() };
          } else if (await openCategorySelectionByPost(page)) {
            openedVia = "post";
            usedSelectionUrl = page.url();
            selectionTitle = await page.title().catch(() => "");
            navResult = { status: 0, title: selectionTitle, errorPage: await isSelectionErrorPage() };
          } else {
            openedVia = "direct";
            usedSelectionUrl = requestedSelectionUrl;
            navResult = await openSelectionPage(requestedSelectionUrl);
          }
        } catch (error) {
          navResult = { status: 0, title: "", errorPage: true };
        }

        if (navResult?.errorPage && openedVia === "direct" && requestedSelectionUrl !== baseSelectionUrl) {
          usedSelectionUrl = baseSelectionUrl;
          try {
            navResult = await openSelectionPage(baseSelectionUrl);
          } catch (error) {
            navResult = { status: 0, title: "", errorPage: true };
          }
        }

        await acceptCookieModal(page, { timeout: 15000 }).catch(() => {});
        if (isGdprPage(page.url())) {
          await acceptGdprConsent(page, { timeout: 20000 }).catch(() => {});
        }

        selectionStatus = navResult?.status || 0;
        selectionTitle = navResult?.title || selectionTitle || "";

        let selectionContentReady = false;
        if (!navResult?.errorPage) {
          selectionContentReady = await waitForCategorySelectionContent(20000);
          if (!selectionContentReady) {
            try {
              await page.reload({ waitUntil: "domcontentloaded", timeout: 20000 });
            } catch (error) {
              // ignore reload errors
            }
            await acceptCookieModal(page, { timeout: 15000 }).catch(() => {});
            if (isGdprPage(page.url())) {
              await acceptGdprConsent(page, { timeout: 20000 }).catch(() => {});
            }
            selectionContentReady = await waitForCategorySelectionContent(15000);
          }
        }

        await dumpFieldsDebug(page, {
          accountLabel: account.email || `account-${accountId}`,
          step: "fields-selection-page",
          error: navResult?.errorPage ? "selection-page-error" : (selectionContentReady ? "" : "selection-content-missing"),
          extra: {
            url: page.url(),
            categoryPathIds,
            selectionUrl,
            selectionUrlUsed: usedSelectionUrl,
            selectionUrlStatus: selectionStatus,
            selectionUrlTitle: selectionTitle,
            selectionOpenMethod: openedVia,
            selectionContentReady
          },
          force: forceDebug
        });

        if (navResult?.errorPage || !selectionContentReady) {
          return false;
        }

        const ready = await waitForCategorySelectionReady(page, 20000);
        let navigated = false;
        let clickedPath = false;

        if (ready) {
          const treeResult = await applyCategoryPathViaTree(page, categoryPathIds);
          await dumpFieldsDebug(page, {
            accountLabel: account.email || `account-${accountId}`,
            step: "fields-selection-tree",
            error: treeResult?.success ? "" : (treeResult?.reason || "tree-failed"),
            extra: { url: page.url(), categoryPathIds, treeResult },
            force: forceDebug
          });
          if (treeResult?.success) {
            try {
              await Promise.race([
                page.waitForNavigation({ waitUntil: "domcontentloaded", timeout: 20000 }).then(() => true).catch(() => false),
                page.waitForFunction(
                  () => window.location.href.includes("anzeige-aufgeben-schritt2"),
                  { timeout: 20000 }
                ).then(() => true).catch(() => false)
              ]).then((result) => {
                navigated = Boolean(result);
              });
            } catch (error) {
              navigated = false;
            }
          }
        }

        if (!navigated && ready && page.url().includes("p-kategorie-aendern")) {
          try {
            clickedPath = await withTimeout(
              selectCategoryPathOnSelectionPage(page, categoryPathIds),
              25000,
              "select-path-timeout"
            );
          } catch (error) {
            clickedPath = false;
          }
          await dumpFieldsDebug(page, {
            accountLabel: account.email || `account-${accountId}`,
            step: "fields-selection-clicked",
            error: clickedPath ? "" : "click-path-failed",
            extra: { url: page.url(), categoryPathIds, clickedPath },
            force: forceDebug
          });

          if (clickedPath) {
            await clickCategoryWeiter(page);
            try {
              await page.waitForFunction(
                () => window.location.href.includes("anzeige-aufgeben-schritt2"),
                { timeout: 20000 }
              );
              navigated = true;
            } catch (error) {
              navigated = false;
            }
          }
        }

        if (!navigated && ready && page.url().includes("p-kategorie-aendern")) {
          let fallbackResult = null;
          try {
            fallbackResult = await page.evaluate((ids) => {
              const sanitize = (value) => {
                const raw = String(value || "").trim();
                const match = raw.match(/\d+/);
                return match ? match[0] : "";
              };
              const targetIds = ids.map(sanitize).filter(Boolean);
              if (!targetIds.length) return { success: false, reason: "empty-path" };
              const form = document.querySelector("#postad-step1-frm") || document.querySelector("form");
              if (!form) return { success: false, reason: "form-missing" };
              const escapeName = (name) => {
                if (window.CSS?.escape) return CSS.escape(name);
                return name.replace(/([\\\"'\\[\\]#.:])/g, "\\\\$1");
              };
              const applyField = (name, value) => {
                if (!name) return;
                let field = form.querySelector(`[name="${escapeName(name)}"]`);
                if (!field) {
                  field = document.createElement("input");
                  field.type = "hidden";
                  field.name = name;
                  form.appendChild(field);
                }
                field.value = String(value ?? "");
              };
              if (targetIds.length > 1) {
                applyField("parentCategoryId", targetIds[0]);
              }
              applyField("categoryId", targetIds[targetIds.length - 1]);
              applyField("submitted", "true");
              form.submit();
              return { success: true, submitted: true, targetIds };
            }, categoryPathIds);
          } catch (error) {
            fallbackResult = { success: false, reason: error?.message || "fallback-error" };
          }
          await dumpFieldsDebug(page, {
            accountLabel: account.email || `account-${accountId}`,
            step: "fields-selection-fallback",
            error: fallbackResult?.success ? "" : (fallbackResult?.reason || "fallback-failed"),
            extra: { url: page.url(), categoryPathIds, fallbackResult },
            force: forceDebug
          });
          if (fallbackResult?.submitted) {
            try {
              await Promise.race([
                page.waitForNavigation({ waitUntil: "domcontentloaded", timeout: 20000 }).then(() => true).catch(() => false),
                page.waitForFunction(
                  () => window.location.href.includes("anzeige-aufgeben-schritt2"),
                  { timeout: 20000 }
                ).then(() => true).catch(() => false)
              ]).then((result) => {
                navigated = Boolean(result);
              });
            } catch (error) {
              navigated = false;
            }
          }
        }

        if (!page.url().includes("anzeige-aufgeben-schritt2")) {
          try {
            await page.goto("https://www.kleinanzeigen.de/p-anzeige-aufgeben-schritt2.html", { waitUntil: "domcontentloaded", timeout: 20000 });
          } catch (error) {
            // ignore
          }
        }
        return navigated;
      };

      let categoryApplied = false;
      try {
        logFields({ event: "selection-start", durationMs: Date.now() - requestStartedAt });
        categoryApplied = await withTimeout(applyCategorySelection(), 28000, "category-selection-timeout");
      } catch (error) {
        categoryApplied = false;
      }
      logFields({ event: "selection-end", success: Boolean(categoryApplied), durationMs: Date.now() - requestStartedAt });
      await dumpFieldsDebug(page, {
        accountLabel: account.email || `account-${accountId}`,
        step: "fields-category-applied",
        error: categoryApplied ? "" : "category-not-applied",
        extra: { url: page.url(), categoryId, categoryPathIds, selectionUrl },
        force: forceDebug
      });

      if (!categoryApplied) {
        await injectCategoryId();
      }

      if (!page.url().includes("anzeige-aufgeben-schritt2")) {
        try {
          await page.goto("https://www.kleinanzeigen.de/p-anzeige-aufgeben-schritt2.html", { waitUntil: "domcontentloaded", timeout: 20000 });
        } catch (error) {
          // ignore
        }
      }

      fields = await collectFields(10000);
      await dumpFieldsDebug(page, {
        accountLabel: account.email || `account-${accountId}`,
        step: "fields-parsed",
        error: fields.length ? "" : "no-fields",
        extra: { url: page.url(), categoryId, fieldCount: fields.length },
        force: forceDebug
      });
      setCachedCategoryFields(categoryId, fields);
      if (!res.headersSent) {
        logFields({ event: "success", count: fields.length, durationMs: Date.now() - requestStartedAt });
        res.json({
          fields,
          debugId: debugEnabled ? requestId : undefined
        });
      }
    } finally {
      await browser.close();
      if (anonymizedProxyUrl) {
        await proxyChain.closeAnonymizedProxy(anonymizedProxyUrl, true);
      }
      if (profileDir) {
        fs.rmSync(profileDir, { recursive: true, force: true });
      }
    }
  } catch (error) {
    if (!res.headersSent) {
      if (typeof logFields === "function") {
        logFields({ event: "error", error: error?.message || String(error) });
      }
      res.status(500).json({
        success: false,
        error: error.message,
        debugId: debugEnabled ? requestId : undefined
      });
    }
  }
});

app.get("/health", (req, res) => {
  res.json({
    status: "healthy",
    service: "kleinanzeigen-backend",
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    version: "1.0.0"
  });
});

app.get("/api/stats", (req, res) => {
  const ownerContext = getOwnerContext(req);
  const accounts = filterByOwner(listAccounts(), ownerContext);
  const activeAccounts = accounts.filter((account) => account.status === "active").length;
  const checkingAccounts = accounts.filter((account) => account.status === "checking").length;
  const failedAccounts = accounts.filter((account) => account.status === "invalid").length;
  const scopedProxies = filterByOwner(proxies, ownerContext);
  const activeProxies = scopedProxies.filter((proxy) => proxy.status === "active").length;
  const failedProxies = scopedProxies.filter((proxy) => proxy.status === "failed").length;

  res.json({
    accounts: {
      total: activeAccounts + checkingAccounts + failedAccounts,
      active: activeAccounts,
      checking: checkingAccounts,
      failed: failedAccounts
    },
    proxies: {
      total: scopedProxies.length,
      active: activeProxies,
      failed: failedProxies
    },
    messages: {
      total: 128,
      today: 14,
      unread: 6
    }
  });
});

// Маршрут для загрузки куки (симуляция)
app.post("/api/accounts/upload", upload.single("cookieFile"), async (req, res) => {
  try {
    const ownerContext = getOwnerContext(req);
    if (!req.file) {
      res.status(400).json({ success: false, error: "Ожидается файл с куками" });
      return;
    }

    const rawCookieText = req.file.buffer.toString("utf8");
    const proxyId = req.body?.proxyId !== undefined && req.body?.proxyId !== null
      ? String(req.body.proxyId).trim()
      : "";
    const deviceProfile = pickDeviceProfile();
    const selectedProxy = proxyId ? findProxyById(proxyId) : null;

    if (!proxyId) {
      res.status(400).json({ success: false, error: "Для добавления аккаунта нужен прокси." });
      return;
    }

    if (proxyId && !selectedProxy) {
      res.status(400).json({ success: false, error: "Выбранный прокси не найден" });
      return;
    }
    if (proxyId && selectedProxy && !isOwnerMatch(selectedProxy, ownerContext)) {
      res.status(403).json({ success: false, error: "Нет доступа к выбранному прокси" });
      return;
    }

    let validation = await validateCookies(rawCookieText, {
      deviceProfile,
      proxy: selectedProxy
    });

    if (!validation.valid) {
      res.status(400).json({
        success: false,
        error: validation.reason || "Куки невалидны"
      });
      return;
    }

    const newAccount = {
      username: `klein_${Date.now().toString().slice(-6)}`,
      profileName: validation.profileName || "",
      profileEmail: validation.profileEmail || "",
      status: "active",
      added: new Date().toISOString().slice(0, 10),
      proxyId: selectedProxy.id,
      cookie: rawCookieText,
      deviceProfile: JSON.stringify(validation.deviceProfile),
      lastCheck: new Date().toISOString(),
      error: null,
      ownerId: ownerContext.ownerId || ""
    };

    const accountId = insertAccount(newAccount);

    res.json({
      success: true,
      message: "Куки валидны, аккаунт сохранен",
      account: {
        id: accountId,
        username: newAccount.username,
        profileName: newAccount.profileName,
        profileEmail: newAccount.profileEmail,
        status: newAccount.status,
        added: newAccount.added,
        proxyId: selectedProxy.id,
        proxy: getProxyLabel(selectedProxy.id, ownerContext),
        lastCheck: newAccount.lastCheck
      }
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Маршрут для создания объявления (синхронизация с Kleinanzeigen)
app.post("/api/ads/create", adUploadMiddleware, async (req, res) => {
  req.setTimeout(0);
  res.setTimeout(0);
  if (req.socket) {
    req.socket.setTimeout(0);
  }
  const uploadedFiles = req.files || [];
  const cleanupFiles = () => {
    uploadedFiles.forEach((file) => {
      fs.unlink(file.path, () => {});
    });
  };

  const requestId = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  const forceDebug = true;
  let activePublishKey = null;
  const releasePublishLock = () => {
    if (!activePublishKey) return;
    const lock = activePublishByAccount.get(activePublishKey);
    if (lock?.requestId === requestId) {
      activePublishByAccount.delete(activePublishKey);
    }
    activePublishKey = null;
  };
  appendServerLog(getPublishRequestLogPath(), {
    event: "start",
    requestId,
    ip: req.ip,
    files: uploadedFiles.length,
    debug: Boolean(forceDebug)
  });
  req.on("aborted", () => {
    appendServerLog(getPublishRequestLogPath(), {
      event: "aborted",
      requestId
    });
  });
  res.on("close", () => {
    appendServerLog(getPublishRequestLogPath(), {
      event: "close",
      requestId,
      headersSent: res.headersSent,
      statusCode: res.statusCode,
      writableEnded: res.writableEnded
    });
  });

  try {
    const {
      accountId,
      title,
      description,
      price,
      currency,
      postalCode,
      categoryId,
      categoryUrl,
      categoryPath,
      extraFields
    } = req.body || {};
    if (process.env.KL_DEBUG_PUBLISH === "1") {
      console.log("[ads/create] payload", {
        accountId,
        titleLength: title ? String(title).length : 0,
        descriptionLength: description ? String(description).length : 0,
        price,
        postalCode,
        categoryId,
        categoryUrl,
        categoryPath: categoryPath ? String(categoryPath).slice(0, 120) : "",
        images: uploadedFiles.length
      });
    }
    appendServerLog(getPublishRequestLogPath(), {
      event: "payload",
      requestId,
      accountId,
      titleLength: title ? String(title).length : 0,
      descriptionLength: description ? String(description).length : 0,
      price,
      postalCode,
      categoryId,
      categoryUrl,
      categoryPath: categoryPath ? String(categoryPath).slice(0, 120) : "",
      images: uploadedFiles.length,
      debug: Boolean(forceDebug)
    });

    if (!accountId) {
      res.status(400).json({
        success: false,
        error: "Выберите аккаунт",
        debugId: forceDebug ? requestId : undefined
      });
      appendServerLog(getPublishRequestLogPath(), { event: "error", requestId, error: "missing-account" });
      cleanupFiles();
      return;
    }

    if (!title || !description || !price) {
      res.status(400).json({
        success: false,
        error: "Заполните обязательные поля объявления",
        debugId: forceDebug ? requestId : undefined
      });
      appendServerLog(getPublishRequestLogPath(), { event: "error", requestId, error: "missing-required" });
      cleanupFiles();
      return;
    }

    const account = getAccountForRequest(Number(accountId), req, res);
    if (!account) {
      appendServerLog(getPublishRequestLogPath(), { event: "error", requestId, error: "account-not-found" });
      cleanupFiles();
      return;
    }

    const selectedProxy = requireAccountProxy(account, res, "публикации объявления", getOwnerContext(req));
    if (!selectedProxy) {
      appendServerLog(getPublishRequestLogPath(), { event: "error", requestId, error: "proxy-required" });
      cleanupFiles();
      return;
    }

    const parsedCategoryPath = categoryPath ? (() => {
      try {
        return JSON.parse(categoryPath);
      } catch (error) {
        return categoryPath;
      }
    })() : undefined;

    const parsedExtraFields = extraFields ? (() => {
      try {
        return JSON.parse(extraFields);
      } catch (error) {
        return extraFields;
      }
    })() : undefined;

    cleanupRecentSuccessfulPublishes();
    const fingerprint = buildPublishRequestFingerprint({
      accountId: account.id,
      title,
      description,
      price,
      currency,
      postalCode,
      categoryId,
      categoryUrl,
      categoryPath: parsedCategoryPath,
      extraFields: parsedExtraFields,
      uploadedFiles
    });

    const recentSuccess = recentSuccessfulPublishes.get(fingerprint);
    if (recentSuccess) {
      appendServerLog(getPublishRequestLogPath(), {
        event: "deduplicated-success",
        requestId,
        previousRequestId: recentSuccess.requestId,
        accountId: account.id
      });
      cleanupFiles();
      res.json({
        success: true,
        deduplicated: true,
        message: "Идентичный запрос уже был успешно выполнен. Повторная публикация пропущена.",
        debugId: forceDebug ? requestId : undefined
      });
      return;
    }

    activePublishKey = String(account.id);
    const activePublish = activePublishByAccount.get(activePublishKey);
    if (activePublish) {
      appendServerLog(getPublishRequestLogPath(), {
        event: "rejected-active-publish",
        requestId,
        activeRequestId: activePublish.requestId,
        accountId: account.id
      });
      cleanupFiles();
      res.status(409).json({
        success: false,
        inProgress: true,
        error: "Для этого аккаунта уже выполняется публикация. Дождитесь завершения текущей попытки.",
        activeRequestId: activePublish.requestId,
        debugId: forceDebug ? requestId : undefined
      });
      return;
    }

    activePublishByAccount.set(activePublishKey, {
      requestId,
      startedAt: Date.now(),
      fingerprint
    });

    const result = await publishAd({
      account,
      proxy: selectedProxy,
      ad: {
        title,
        description,
        price,
        currency,
        postalCode,
        categoryId,
        categoryUrl,
        categoryPath: parsedCategoryPath,
        extraFields: parsedExtraFields
      },
      imagePaths: uploadedFiles.map((file) => file.path),
      debug: Boolean(forceDebug)
    }).finally(() => {
      releasePublishLock();
    });
    appendServerLog(getPublishRequestLogPath(), { event: "publish-result", requestId, success: result?.success, error: result?.error || "" });

    if (result.success) {
      recentSuccessfulPublishes.set(fingerprint, {
        requestId,
        accountId: account.id,
        finishedAt: Date.now()
      });
      upsertAd({
        accountId: account.id,
        title,
        price,
        image: "",
        status: "Aktiv",
        createdAt: new Date().toISOString(),
        ownerId: getOwnerContext(req).ownerId || ""
      });
    }

    cleanupFiles();
    res.json({
      ...result,
      debugId: forceDebug ? requestId : undefined
    });
    appendServerLog(getPublishRequestLogPath(), { event: "response-sent", requestId, success: result?.success });
  } catch (error) {
    releasePublishLock();
    cleanupFiles();
    res.status(500).json({
      success: false,
      error: error.message,
      debugId: forceDebug ? requestId : undefined
    });
    appendServerLog(getPublishRequestLogPath(), { event: "exception", requestId, error: error?.message || String(error) });
  }
});

app.post("/api/proxies", async (req, res) => {
  try {
    const ownerContext = getOwnerContext(req);
    const { name, type, host, port, username, password } = req.body || {};

    if (!name || !host || !port || !type) {
      res.status(400).json({ success: false, error: "Заполните обязательные поля" });
      return;
    }

    const proxyConfig = {
      type,
      host,
      port: Number(port),
      username: username || undefined,
      password: password || undefined
    };

    const checkResult = await proxyChecker.checkProxy(proxyConfig);
    if (!checkResult.success) {
      res.json({ success: false, error: checkResult.error, checkResult });
      return;
    }

    const newProxy = {
      id: Date.now(),
      name,
      ...proxyConfig,
      ownerId: ownerContext.ownerId || "",
      status: "active",
      lastChecked: new Date().toISOString(),
      checkResult
    };

    proxies.unshift(newProxy);
    saveProxies();

    res.json({
      success: true,
      proxy: newProxy,
      checkResult
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

app.post("/api/proxies/:id/check", async (req, res) => {
  const proxyId = String(req.params.id || "").trim();
  const proxy = findProxyById(proxyId);

  if (!proxy) {
    res.status(404).json({ success: false, error: "Прокси не найден" });
    return;
  }
  const ownerContext = getOwnerContext(req);
  if (!isOwnerMatch(proxy, ownerContext)) {
    res.status(404).json({ success: false, error: "Прокси не найден" });
    return;
  }

  try {
    const checkResult = await proxyChecker.checkProxy(proxy);
    const status = checkResult.success ? "active" : "failed";

    proxy.status = status;
    proxy.lastChecked = new Date().toISOString();
    proxy.checkResult = checkResult;
    saveProxies();

    res.json({
      ...checkResult,
      proxyId: proxy.id
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

app.post("/api/proxies/check-all", async (req, res) => {
  try {
    const ownerContext = getOwnerContext(req);
    const scopedProxies = filterByOwner(proxies, ownerContext);
    const results = await proxyChecker.checkMultipleProxies(scopedProxies);

    results.forEach((result) => {
      const proxy = findProxyById(result.proxyId);
      if (proxy) {
        proxy.status = result.success ? "active" : "failed";
        proxy.lastChecked = new Date().toISOString();
        proxy.checkResult = result;
      }
    });
    saveProxies();

    res.json({ success: true, results });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.delete("/api/accounts/:id", (req, res) => {
  try {
    const accountId = Number(req.params.id);
    const ownerContext = getOwnerContext(req);
    const account = getAccountById(accountId);
    if (!account || !isOwnerMatch(account, ownerContext)) {
      res.status(404).json({ success: false, error: "Аккаунт не найден" });
      return;
    }
    const deleted = deleteAccount(accountId);
    if (!deleted) {
      res.status(404).json({ success: false, error: "Аккаунт не найден" });
      return;
    }
    res.json({ success: true });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message || "Ошибка при удалении аккаунта" });
  }
});

app.delete("/api/proxies/:id", (req, res) => {
  const proxyId = String(req.params.id || "").trim();
  const index = findProxyIndexById(proxyId);

  if (index === -1) {
    res.status(404).json({ success: false, error: "Прокси не найден" });
    return;
  }
  const ownerContext = getOwnerContext(req);
  if (!isOwnerMatch(proxies[index], ownerContext)) {
    res.status(404).json({ success: false, error: "Прокси не найден" });
    return;
  }

  proxies.splice(index, 1);
  saveProxies();
  res.json({ success: true });
});

// Запуск сервера
const server = app.listen(PORT, () => {
  console.log(`🚀 Backend запущен на порту ${PORT}`);
  console.log(`🌍 Откройте http://localhost:${PORT}`);
  console.log(`📡 Health check: http://localhost:${PORT}/health`);
  console.log(`👥 Аккаунты: http://localhost:${PORT}/api/accounts`);
  console.log(`🔗 Прокси: http://localhost:${PORT}/api/proxies`);
});
server.requestTimeout = 0;
server.headersTimeout = 0;
server.keepAliveTimeout = 0;
