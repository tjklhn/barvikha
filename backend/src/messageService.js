const fs = require("fs");
const os = require("os");
const path = require("path");
const axios = require("axios");
const ProxyAgent = require("proxy-agent");
const puppeteer = require("puppeteer-extra");
const StealthPlugin = require("puppeteer-extra-plugin-stealth");
const proxyChain = require("proxy-chain");
const { parseCookies, normalizeCookie, buildProxyServer, buildProxyUrl } = require("./cookieUtils");
const { pickDeviceProfile } = require("./cookieValidator");
const { acceptCookieModal, acceptGdprConsent, isGdprPage } = require("./adPublisher");
const PUPPETEER_PROTOCOL_TIMEOUT = Number(process.env.PUPPETEER_PROTOCOL_TIMEOUT || 120000);
const PUPPETEER_LAUNCH_TIMEOUT = Number(process.env.PUPPETEER_LAUNCH_TIMEOUT || 120000);
const PUPPETEER_NAV_TIMEOUT = Number(process.env.PUPPETEER_NAV_TIMEOUT || 60000);

puppeteer.use(StealthPlugin());

const MESSAGE_LIST_URL = "https://www.kleinanzeigen.de/m-nachrichten.html";
const MESSAGEBOX_API_HOST = "https://gateway.kleinanzeigen.de";
const MESSAGEBOX_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36";
const DEBUG_MESSAGES = process.env.KL_DEBUG_MESSAGES === "1";
const FORCE_WEB_MESSAGES = process.env.KL_FORCE_WEB_MESSAGES === "1";
const FAST_SNAPSHOT_TIMEOUT_MS = Number(process.env.KL_FAST_SNAPSHOT_TIMEOUT_MS || 4500);
const FAST_SNAPSHOT_ATTEMPTS = Math.max(1, Number(process.env.KL_FAST_SNAPSHOT_ATTEMPTS || 1));
const FAST_SNAPSHOT_DELAY_MS = Math.max(100, Number(process.env.KL_FAST_SNAPSHOT_DELAY_MS || 250));
const MESSAGE_ACTION_DEADLINE_MS = Math.max(25000, Number(process.env.KL_MESSAGE_ACTION_DEADLINE_MS || 45000));
const MESSAGE_CONSENT_TIMEOUT_MS = Math.max(1200, Number(process.env.KL_MESSAGE_CONSENT_TIMEOUT_MS || 2800));
const MESSAGE_GDPR_TIMEOUT_MS = Math.max(1800, Number(process.env.KL_MESSAGE_GDPR_TIMEOUT_MS || 5000));
const PROXY_TUNNEL_ERROR_CODE = "PROXY_TUNNEL_CONNECTION_FAILED";
const DETACHED_FRAME_ERROR_PATTERNS = [
  /attempted to use detached frame/i,
  /execution context was destroyed/i,
  /cannot find context with specified id/i,
  /target closed/i,
  /session closed/i,
  /most likely because of a navigation/i
];
const PROXY_TUNNEL_ERROR_PATTERNS = [
  /err_tunnel_connection_failed/i,
  /err_proxy_connection_failed/i,
  /err_no_supported_proxies/i,
  /proxy connection failed/i,
  /socks connection failed/i,
  /socks proxy/i,
  /tunneling socket could not be established/i,
  /proxyconnect/i,
  /proxy authentication required/i,
  /econnrefused/i,
  /ehostunreach/i,
  /enetunreach/i,
  /etimedout/i
];
const buildConversationUrl = (conversationId, conversationUrl) => {
  if (conversationUrl) return conversationUrl;
  if (!conversationId) return MESSAGE_LIST_URL;
  return `${MESSAGE_LIST_URL}?conversationId=${encodeURIComponent(conversationId)}`;
};

const normalizeMatch = (value) => (value || "").replace(/\s+/g, " ").trim().toLowerCase();
const normalizeErrorMessage = (error) => String(error?.message || error || "").trim();
const normalizeErrorCode = (error) => String(error?.code || "").trim().toUpperCase();

const matchesAnyPattern = (value, patterns = []) => {
  const source = String(value || "");
  if (!source) return false;
  return patterns.some((pattern) => pattern && pattern.test(source));
};

const isDetachedFrameError = (error) => {
  const message = normalizeErrorMessage(error);
  const causeMessage = normalizeErrorMessage(error?.cause);
  return matchesAnyPattern(message, DETACHED_FRAME_ERROR_PATTERNS)
    || matchesAnyPattern(causeMessage, DETACHED_FRAME_ERROR_PATTERNS);
};

const isProxyTunnelError = (error) => {
  const message = normalizeErrorMessage(error);
  const causeMessage = normalizeErrorMessage(error?.cause);
  const code = normalizeErrorCode(error);
  if (matchesAnyPattern(message, PROXY_TUNNEL_ERROR_PATTERNS)) return true;
  if (matchesAnyPattern(causeMessage, PROXY_TUNNEL_ERROR_PATTERNS)) return true;
  return [
    "ERR_TUNNEL_CONNECTION_FAILED",
    "ERR_PROXY_CONNECTION_FAILED",
    "ERR_NO_SUPPORTED_PROXIES",
    "ECONNREFUSED",
    "ECONNRESET",
    "EHOSTUNREACH",
    "ENETUNREACH",
    "ETIMEDOUT",
    "ESOCKETTIMEDOUT"
  ].includes(code);
};

const toProxyTunnelError = (error, context = "") => {
  const wrapped = new Error(PROXY_TUNNEL_ERROR_CODE);
  wrapped.code = PROXY_TUNNEL_ERROR_CODE;
  const detailsParts = [];
  if (context) detailsParts.push(String(context));
  const message = normalizeErrorMessage(error);
  if (message) detailsParts.push(message);
  const causeMessage = normalizeErrorMessage(error?.cause);
  if (causeMessage && causeMessage !== message) detailsParts.push(causeMessage);
  if (detailsParts.length) {
    wrapped.details = detailsParts.join(" | ").slice(0, 800);
  }
  wrapped.originalMessage = message;
  wrapped.cause = error;
  return wrapped;
};

const buildActionTimeoutError = (context = "") => {
  const error = new Error("MESSAGE_ACTION_TIMEOUT");
  error.code = "MESSAGE_ACTION_TIMEOUT";
  if (context) {
    error.details = String(context).slice(0, 300);
  }
  return error;
};

const normalizeImageUrl = (value, baseUrl = MESSAGE_LIST_URL) => {
  const src = String(value || "").trim();
  if (!src) return "";
  const lowered = src.toLowerCase();
  if (["null", "undefined", "none", "false"].includes(lowered)) return "";
  if (src.includes("data:image/") || src.includes("placeholder")) return "";
  const normalizedSrc = src.replace(/^\/+/, "/");
  if (normalizedSrc.startsWith("/api/v1/prod-ads/images/")) {
    try {
      return new URL(normalizedSrc, "https://img.kleinanzeigen.de").href;
    } catch (error) {
      return `https://img.kleinanzeigen.de${normalizedSrc}`;
    }
  }
  if (/^img\.kleinanzeigen\.de/i.test(normalizedSrc)) {
    return `https://${normalizedSrc}`;
  }
  if (/^\/\//.test(src)) {
    return `https:${src}`;
  }
  try {
    const url = new URL(src, baseUrl);
    const host = String(url.hostname || "").toLowerCase();
    const isProdAdsImage = host === "img.kleinanzeigen.de"
      && url.pathname.startsWith("/api/v1/prod-ads/images/");
    if (isProdAdsImage) {
      const ruleParam = String(url.searchParams.get("rule") || "");
      const malformedRule = /(imageid|\$\{.*\}|\$_\{.*\})/i.test(ruleParam);
      const validRule = /^\$_[a-z0-9_.-]+$/i.test(ruleParam);
      if (!validRule || malformedRule) {
        url.searchParams.set("rule", "$_57.JPG");
      }
    }
    return url.href;
  } catch (error) {
    return src;
  }
};

const isValidImageUrl = (value) => {
  const normalized = normalizeImageUrl(value);
  if (!normalized) return false;
  return /^https?:\/\//i.test(normalized);
};

const pickAdImageFromApi = (conversation) => {
  if (!conversation) return "";
  const candidates = [
    conversation.adImage,
    conversation.adImageUrl,
    conversation.adImageURL,
    conversation.adImageUrlLarge,
    conversation.adImageUrlMedium,
    conversation.adImageUrlSmall,
    conversation.adImageThumbnail,
    conversation.adImageThumbnailUrl,
    conversation.adImageSmall,
    conversation.adImageMedium,
    conversation.adImageLarge,
    conversation.imageUrlSmall,
    conversation.imageUrlMedium,
    conversation.imageUrlLarge,
    conversation.imageUrl,
    conversation.image,
    conversation.thumbnailUrl,
    conversation.thumbnail
  ];

  const adPayload = conversation.ad || conversation.adInfo || conversation.item || conversation.advertisement || {};
  if (adPayload) {
    candidates.push(
      adPayload.image,
      adPayload.image?.url,
      adPayload.image?.src,
      adPayload.imageUrl,
      adPayload.imageUrlSmall,
      adPayload.imageUrlMedium,
      adPayload.imageUrlLarge,
      adPayload.imageURL,
      adPayload.thumbnailUrl,
      adPayload.thumbnailUrlSmall,
      adPayload.thumbnailUrlMedium,
      adPayload.thumbnailUrlLarge,
      adPayload.thumbnail
    );
    if (Array.isArray(adPayload.images) && adPayload.images.length) {
      candidates.push(adPayload.images[0]);
      candidates.push(adPayload.images[0]?.url);
      candidates.push(adPayload.images[0]?.src);
    }
  }

  if (Array.isArray(conversation.adImages) && conversation.adImages.length) {
    candidates.push(conversation.adImages[0]);
  }

  for (const candidate of candidates) {
    if (!candidate) continue;
    if (typeof candidate === "string") {
      const normalized = normalizeImageUrl(candidate);
      if (normalized) return normalized;
    } else if (typeof candidate === "object") {
      const nested = candidate.url || candidate.src || candidate.imageUrl || candidate.image;
      const normalized = normalizeImageUrl(nested);
      if (normalized) return normalized;
    }
  }
  return "";
};

const matchConversation = (conversation, criteria) => {
  const wantedParticipant = normalizeMatch(criteria.participant);
  const wantedAdTitle = normalizeMatch(criteria.adTitle);
  const participant = normalizeMatch(conversation.participant);
  const adTitle = normalizeMatch(conversation.adTitle);

  if (wantedParticipant && wantedAdTitle) {
    return participant.includes(wantedParticipant) && adTitle.includes(wantedAdTitle);
  }
  if (wantedParticipant) return participant.includes(wantedParticipant);
  if (wantedAdTitle) return adTitle.includes(wantedAdTitle);
  return false;
};

const pickParticipantFromApi = (conversation, userId) => {
  if (!conversation) return "";
  const buyerId = conversation.userIdBuyer != null ? String(conversation.userIdBuyer) : "";
  const sellerId = conversation.userIdSeller != null ? String(conversation.userIdSeller) : "";
  const currentId = userId ? String(userId) : "";

  if (currentId && buyerId && currentId === buyerId) {
    return conversation.sellerName || "";
  }
  if (currentId && sellerId && currentId === sellerId) {
    return conversation.buyerName || "";
  }
  return conversation.sellerName || conversation.buyerName || "";
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const humanPause = (min = 120, max = 240) => sleep(Math.floor(min + Math.random() * (max - min)));
const createTempProfileDir = () => fs.mkdtempSync(path.join(os.tmpdir(), "kl-profile-"));
const safeCloseBrowser = async (browser, { timeoutMs = 4500 } = {}) => {
  if (!browser) return;
  let finished = false;
  const closePromise = Promise.resolve()
    .then(() => browser.close())
    .catch(() => {})
    .finally(() => {
      finished = true;
    });

  await Promise.race([
    closePromise,
    sleep(Math.max(600, Number(timeoutMs) || 4500))
  ]);

  if (finished) return;

  try {
    const proc = typeof browser.process === "function" ? browser.process() : null;
    if (proc && !proc.killed) {
      proc.kill("SIGKILL");
    }
  } catch (error) {
    // ignore kill errors
  }

  await Promise.race([closePromise, sleep(800)]);
};
const randomInt = (min, max) => {
  const low = Math.floor(Math.min(min, max));
  const high = Math.floor(Math.max(min, max));
  return low + Math.floor(Math.random() * (high - low + 1));
};
const randomChance = (value = 0.5) => Math.random() < Math.max(0, Math.min(1, Number(value) || 0));

const typeTextHumanLike = async (
  inputHandle,
  text,
  { minDelay = 22, maxDelay = 68, pauseChance = 0.08 } = {}
) => {
  if (!inputHandle) return;
  const content = String(text || "");
  if (!content) return;
  for (const char of content) {
    await inputHandle.type(char, { delay: randomInt(minDelay, maxDelay) });
    if (randomChance(pauseChance)) {
      await sleep(randomInt(40, 180));
    }
  }
};

const performHumanLikePageActivity = async (
  page,
  { intensity = "light", force = false } = {}
) => {
  if (!page) return;
  const profile = {
    light: {
      runChance: 0.45,
      minMoves: 1,
      maxMoves: 2,
      scrollChance: 0.35,
      reverseScrollChance: 0.2,
      pauseMin: 70,
      pauseMax: 150
    },
    medium: {
      runChance: 0.7,
      minMoves: 2,
      maxMoves: 4,
      scrollChance: 0.5,
      reverseScrollChance: 0.35,
      pauseMin: 90,
      pauseMax: 220
    },
    strong: {
      runChance: 0.9,
      minMoves: 3,
      maxMoves: 5,
      scrollChance: 0.65,
      reverseScrollChance: 0.45,
      pauseMin: 110,
      pauseMax: 280
    }
  };
  const cfg = profile[intensity] || profile.light;
  if (!force && !randomChance(cfg.runChance)) return;

  try {
    const viewport = page.viewport() || { width: 1365, height: 860 };
    const width = Math.max(220, Number(viewport.width) || 1365);
    const height = Math.max(220, Number(viewport.height) || 860);

    let x = randomInt(20, Math.max(40, width - 20));
    let y = randomInt(20, Math.max(40, height - 20));
    await page.mouse.move(x, y, { steps: randomInt(8, 18) }).catch(() => {});

    const moves = randomInt(cfg.minMoves, cfg.maxMoves);
    for (let i = 0; i < moves; i += 1) {
      const nextX = randomInt(16, Math.max(32, width - 16));
      const nextY = randomInt(16, Math.max(32, height - 16));
      await page.mouse.move(nextX, nextY, { steps: randomInt(10, 26) }).catch(() => {});
      x = nextX;
      y = nextY;
      await sleep(randomInt(30, 120));
    }

    if (randomChance(cfg.scrollChance)) {
      const delta = randomInt(60, 420);
      await page.mouse.wheel({ deltaY: delta }).catch(async () => {
        await page.evaluate((value) => window.scrollBy({ top: value, behavior: "auto" }), delta).catch(() => {});
      });
      await sleep(randomInt(45, 160));
      if (randomChance(cfg.reverseScrollChance)) {
        const reverse = -randomInt(20, Math.min(260, Math.max(40, delta)));
        await page.mouse.wheel({ deltaY: reverse }).catch(async () => {
          await page.evaluate((value) => window.scrollBy({ top: value, behavior: "auto" }), reverse).catch(() => {});
        });
      }
    }

    await humanPause(cfg.pauseMin, cfg.pauseMax);
  } catch (error) {
    // do not fail workflow if human-like jitter failed
  }
};

const clickHandleHumanLike = async (page, handle) => {
  if (!page || !handle) return false;

  await handle.evaluate((node) => {
    node.scrollIntoView({ block: "center", inline: "center" });
  }).catch(() => {});
  await humanPause(60, 140);

  const box = await handle.boundingBox().catch(() => null);
  if (box && box.width > 0 && box.height > 0) {
    const x = box.x + box.width * (0.2 + Math.random() * 0.6);
    const y = box.y + box.height * (0.2 + Math.random() * 0.6);
    try {
      await page.mouse.move(x, y, { steps: randomInt(10, 24) });
      await sleep(randomInt(24, 90));
      await page.mouse.down();
      await sleep(randomInt(22, 86));
      await page.mouse.up();
      return true;
    } catch (error) {
      // fallback to element click
    }
  }

  try {
    await handle.click({ delay: randomInt(32, 110) });
    return true;
  } catch (error) {
    return false;
  }
};

const decodeJwtPayload = (token) => {
  if (!token) return null;
  const parts = token.split(".");
  if (parts.length < 2) return null;
  try {
    const base64 = parts[1].replace(/-/g, "+").replace(/_/g, "/");
    const padded = base64 + "=".repeat((4 - (base64.length % 4)) % 4);
    const decoded = Buffer.from(padded, "base64").toString("utf8");
    return JSON.parse(decoded);
  } catch (error) {
    return null;
  }
};

const extractJwtToken = (token) => String(token || "").replace(/^Bearer\s+/i, "").trim();

const getUserIdFromCookies = (cookies) => {
  const accessToken = (cookies || []).find((cookie) => cookie.name === "access_token");
  if (!accessToken?.value) return "";
  const payload = decodeJwtPayload(accessToken.value);
  return payload?.preferred_username || payload?.uid || payload?.sub || "";
};

const getUserIdFromAccessToken = (token) => {
  const payload = decodeJwtPayload(extractJwtToken(token));
  return payload?.preferred_username || payload?.uid || payload?.sub || "";
};

const buildCookieHeader = (cookies) =>
  (cookies || [])
    .filter((cookie) => cookie?.name && cookie?.value)
    .map((cookie) => `${cookie.name}=${cookie.value}`)
    .join("; ");

const buildAxiosConfig = ({ proxy, headers = {}, timeout = 20000 } = {}) => {
  const config = {
    headers,
    timeout,
    validateStatus: (status) => status >= 200 && status < 500
  };
  const proxyUrl = buildProxyUrl(proxy);
  if (proxyUrl) {
    let agent = null;
    if (typeof ProxyAgent === "function") {
      try {
        agent = new ProxyAgent(proxyUrl);
      } catch (error) {
        agent = ProxyAgent(proxyUrl);
      }
    } else if (ProxyAgent && typeof ProxyAgent.ProxyAgent === "function") {
      agent = new ProxyAgent.ProxyAgent(proxyUrl);
    }
    config.httpAgent = agent;
    config.httpsAgent = agent;
    config.proxy = false;
  }
  return config;
};

const ensureProxyCanReachKleinanzeigen = async (proxy, timeoutMs = 12000) => {
  const effectiveTimeout = Math.max(4000, Number(timeoutMs) || 12000);
  try {
    const response = await axios.get(
      "https://www.kleinanzeigen.de/",
      buildAxiosConfig({
        proxy,
        timeout: effectiveTimeout,
        headers: {
          Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
          "User-Agent": MESSAGEBOX_USER_AGENT
        }
      })
    );
    if (Number(response?.status) === 407) {
      throw toProxyTunnelError(new Error("Proxy authentication required"), "PROXY_PRECHECK_FAILED");
    }
  } catch (error) {
    if (error?.code === PROXY_TUNNEL_ERROR_CODE) {
      throw error;
    }
    if (isProxyTunnelError(error)) {
      throw toProxyTunnelError(error, "PROXY_PRECHECK_FAILED");
    }
    throw error;
  }
};

const fetchMessageboxAccessToken = async ({ cookies, proxy, deviceProfile, timeoutMs = 20000 }) => {
  const cookieHeader = buildCookieHeader(cookies);
  if (!cookieHeader) {
    const error = new Error("AUTH_REQUIRED");
    error.code = "AUTH_REQUIRED";
    throw error;
  }

  const userAgent = deviceProfile?.userAgent || MESSAGEBOX_USER_AGENT;
  const response = await axios.get(
    "https://www.kleinanzeigen.de/m-access-token.json",
    buildAxiosConfig({
      proxy,
      timeout: timeoutMs,
      headers: {
        Cookie: cookieHeader,
        "User-Agent": userAgent,
        Accept: "application/json"
      }
    })
  );

  if (response.status === 401 || response.status === 403) {
    const error = new Error("AUTH_REQUIRED");
    error.code = "AUTH_REQUIRED";
    throw error;
  }

  const token = response.headers?.authorization || response.headers?.Authorization || "";
  if (!token) {
    const error = new Error("AUTH_REQUIRED");
    error.code = "AUTH_REQUIRED";
    throw error;
  }

  const messageboxHeader = response.headers?.messagebox || response.headers?.Messagebox || "";
  const messageboxKey = messageboxHeader ? messageboxHeader.split(" ")[1] : "";
  const expiration = response.data?.expiration || 0;

  return { token, messageboxKey, expiration };
};

const fetchConversationListViaApi = async ({
  userId,
  accessToken,
  cookies,
  proxy,
  deviceProfile,
  page = 0,
  size = 20
}) => {
  const cookieHeader = buildCookieHeader(cookies);
  const userAgent = deviceProfile?.userAgent || MESSAGEBOX_USER_AGENT;
  const url = `${MESSAGEBOX_API_HOST}/messagebox/api/users/${encodeURIComponent(userId)}/conversations`
    + `?page=${page}&size=${size}`;

  const response = await axios.get(
    url,
    buildAxiosConfig({
      proxy,
      headers: {
        Accept: "application/json",
        Authorization: accessToken,
        "X-ECG-USER-AGENT": "messagebox-1",
        Cookie: cookieHeader,
        "User-Agent": userAgent
      }
    })
  );

  if (response.status === 401 || response.status === 403) {
    const error = new Error("AUTH_REQUIRED");
    error.code = "AUTH_REQUIRED";
    throw error;
  }

  if (response.status >= 400) {
    throw new Error(`MESSAGEBOX_API_ERROR_${response.status}`);
  }

  return response.data || {};
};

const fetchConversationDetailViaApi = async ({
  userId,
  conversationId,
  accessToken,
  cookies,
  proxy,
  deviceProfile,
  timeoutMs = 20000
}) => {
  const cookieHeader = buildCookieHeader(cookies);
  const userAgent = deviceProfile?.userAgent || MESSAGEBOX_USER_AGENT;
  const url = `${MESSAGEBOX_API_HOST}/messagebox/api/users/${encodeURIComponent(userId)}/conversations/${encodeURIComponent(conversationId)}`
    + "?contentWarnings=true";

  const response = await axios.get(
    url,
    buildAxiosConfig({
      proxy,
      timeout: timeoutMs,
      headers: {
        Accept: "application/json",
        Authorization: accessToken,
        "X-ECG-USER-AGENT": "messagebox-1",
        Cookie: cookieHeader,
        "User-Agent": userAgent
      }
    })
  );

  if (response.status === 401 || response.status === 403) {
    const error = new Error("AUTH_REQUIRED");
    error.code = "AUTH_REQUIRED";
    throw error;
  }

  if (response.status >= 400) {
    throw new Error(`MESSAGEBOX_API_ERROR_${response.status}`);
  }

  return response.data || {};
};

const sendConversationMessageViaApi = async ({
  userId,
  conversationId,
  accessToken,
  cookies,
  proxy,
  deviceProfile,
  text
}) => {
  const cookieHeader = buildCookieHeader(cookies);
  const userAgent = deviceProfile?.userAgent || MESSAGEBOX_USER_AGENT;
  const url = `${MESSAGEBOX_API_HOST}/messagebox/api/users/${encodeURIComponent(userId)}/conversations/${encodeURIComponent(conversationId)}`;

  const response = await axios.post(
    url,
    { message: text },
    buildAxiosConfig({
      proxy,
      headers: {
        Accept: "application/json",
        Authorization: accessToken,
        "X-ECG-USER-AGENT": "messagebox-1",
        "Content-Type": "application/json",
        Cookie: cookieHeader,
        "User-Agent": userAgent
      }
    })
  );

  if (response.status === 401 || response.status === 403) {
    const error = new Error("AUTH_REQUIRED");
    error.code = "AUTH_REQUIRED";
    throw error;
  }

  if (response.status >= 400) {
    throw new Error(`MESSAGEBOX_API_ERROR_${response.status}`);
  }

  return response.data || {};
};

const sanitizeFilename = (value) => (value || "account")
  .toString()
  .replace(/[^a-z0-9._-]+/gi, "_")
  .slice(0, 60);

const dumpMessageListDebug = async (page, accountLabel) => {
  try {
    const debugDir = path.join(__dirname, "..", "data", "debug");
    if (!fs.existsSync(debugDir)) {
      fs.mkdirSync(debugDir, { recursive: true });
    }

    const safeLabel = sanitizeFilename(accountLabel);
    const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
    const base = `messages-${safeLabel}-${timestamp}`;
    const htmlPath = path.join(debugDir, `${base}.html`);
    const screenshotPath = path.join(debugDir, `${base}.png`);
    const metaPath = path.join(debugDir, `${base}.json`);

    const [html, meta] = await Promise.all([
      page.content().catch(() => ""),
      page.evaluate(() => {
        const pickAttrValues = (attr) => {
          const values = new Set();
          document.querySelectorAll(`[${attr}]`).forEach((node) => {
            const val = node.getAttribute(attr);
            if (val && /message|conversation|chat|inbox|thread/i.test(val)) {
              values.add(val);
            }
          });
          return Array.from(values).slice(0, 80);
        };

        const count = (selector) => {
          try { return document.querySelectorAll(selector).length; } catch (e) { return 0; }
        };

        return {
          url: location.href,
          title: document.title,
          counts: {
            anchors: count("a[href]"),
            nachrichtenLinks: count("a[href*='nachrichten']"),
            conversationLinks: count("a[href*='conversationId']"),
            messageListItems: count("[data-testid*='message-list-item'], [data-qa*='message-list-item']"),
            conversationItems: count("[data-testid*='conversation'], [data-qa*='conversation']"),
            listItems: count("li"),
            roleListItems: count("[role='listitem']"),
            buttons: count("button, [role='button']")
          },
          testIds: pickAttrValues("data-testid"),
          qaIds: pickAttrValues("data-qa")
        };
      })
    ]);

    if (html) {
      fs.writeFileSync(htmlPath, html, "utf8");
    }
    await page.screenshot({ path: screenshotPath, fullPage: true }).catch(() => {});
    fs.writeFileSync(metaPath, JSON.stringify(meta, null, 2), "utf8");

    console.log(`[messageService] Debug saved: ${htmlPath}`);
    console.log(`[messageService] Debug meta: ${metaPath}`);
    console.log(`[messageService] Debug screenshot: ${screenshotPath}`);
  } catch (error) {
    console.log(`[messageService] Debug dump failed: ${error.message}`);
  }
};

const isHandleVisible = async (handle) => {
  if (!handle) return false;
  const box = await handle.boundingBox().catch(() => null);
  return Boolean(box && box.width > 0 && box.height > 0);
};

const findFirstVisibleHandle = async (context, selectors) => {
  for (const selector of selectors) {
    let handles = [];
    try {
      handles = await context.$$(selector);
    } catch (error) {
      continue;
    }
    for (const handle of handles) {
      if (await isHandleVisible(handle)) return handle;
    }
  }
  return null;
};

const getPageContexts = (page) => {
  if (!page) return [];
  try {
    if (typeof page.isClosed === "function" && page.isClosed()) return [];
  } catch (error) {
    return [];
  }
  let frames = [];
  try {
    frames = typeof page.frames === "function" ? page.frames() : [];
  } catch (error) {
    frames = [];
  }
  frames = frames.filter((frame) => {
    try {
      return typeof frame?.isDetached !== "function" || !frame.isDetached();
    } catch (error) {
      return false;
    }
  });
  return [page, ...frames];
};

const evaluateInContext = async (context, fn, ...args) => {
  if (!context || typeof context.evaluate !== "function") return null;
  try {
    return await context.evaluate(fn, ...args);
  } catch (error) {
    return null;
  }
};

const evaluateHandleInContext = async (context, fn, ...args) => {
  if (!context || typeof context.evaluateHandle !== "function") return null;
  try {
    return await context.evaluateHandle(fn, ...args);
  } catch (error) {
    return null;
  }
};

const findFirstDeepHandleInContext = async (context, selectors, { requireVisible = false } = {}) => {
  if (!context || !Array.isArray(selectors) || !selectors.length) return null;
  const resultHandle = await evaluateHandleInContext(context, (selectorList, mustBeVisible) => {
    const normalize = (value) => String(value || "").trim();
    const preparedSelectors = (Array.isArray(selectorList) ? selectorList : [])
      .map((selector) => normalize(selector))
      .filter(Boolean);
    if (!preparedSelectors.length) return null;

    const isVisible = (node) => {
      if (!node || !node.ownerDocument) return false;
      const view = node.ownerDocument.defaultView || window;
      const style = view.getComputedStyle(node);
      if (!style) return false;
      if (style.display === "none" || style.visibility === "hidden" || style.opacity === "0" || style.pointerEvents === "none") {
        return false;
      }
      const rect = node.getBoundingClientRect();
      return rect.width > 0 && rect.height > 0;
    };

    const collectRoots = (startRoot) => {
      const roots = [startRoot];
      const queue = [startRoot];
      const seen = new Set([startRoot]);
      while (queue.length) {
        const root = queue.shift();
        let nodes = [];
        try {
          nodes = Array.from(root.querySelectorAll("*"));
        } catch (error) {
          nodes = [];
        }
        for (const node of nodes) {
          const shadowRoot = node?.shadowRoot;
          if (!shadowRoot || seen.has(shadowRoot)) continue;
          seen.add(shadowRoot);
          roots.push(shadowRoot);
          queue.push(shadowRoot);
        }
      }
      return roots;
    };

    const roots = collectRoots(document);
    for (const selector of preparedSelectors) {
      for (const root of roots) {
        let nodes = [];
        try {
          nodes = Array.from(root.querySelectorAll(selector));
        } catch (error) {
          nodes = [];
        }
        for (const node of nodes) {
          if (mustBeVisible && !isVisible(node)) continue;
          return node;
        }
      }
    }
    return null;
  }, selectors, requireVisible);
  if (!resultHandle) return null;
  const elementHandle = typeof resultHandle.asElement === "function" ? resultHandle.asElement() : null;
  if (!elementHandle) {
    await resultHandle.dispose().catch(() => {});
    return null;
  }
  return elementHandle;
};

const findFirstHandleInAnyContext = async (
  page,
  selectors,
  { timeout = 0, requireVisible = false } = {}
) => {
  const startedAt = Date.now();
  while (true) {
    const contexts = getPageContexts(page);
    for (const context of contexts) {
      if (requireVisible) {
        const handle = await findFirstVisibleHandle(context, selectors);
        if (handle) return { handle, context };
      } else {
        for (const selector of selectors) {
          let handle = null;
          try {
            handle = await context.$(selector);
          } catch (error) {
            handle = null;
          }
          if (handle) return { handle, context };
        }
      }

      const deepHandle = await findFirstDeepHandleInContext(context, selectors, { requireVisible });
      if (deepHandle) {
        return { handle: deepHandle, context };
      }
    }

    if (!timeout || Date.now() - startedAt >= timeout) break;
    await sleep(220);
  }

  return null;
};

const isInteractiveButtonHandle = async (handle) => {
  if (!handle) return false;
  return handle.evaluate((node) => {
    if (!node) return false;
    const style = window.getComputedStyle(node);
    if (!style) return false;
    if (style.display === "none" || style.visibility === "hidden" || style.opacity === "0" || style.pointerEvents === "none") {
      return false;
    }
    const rect = node.getBoundingClientRect();
    if (rect.width <= 0 || rect.height <= 0) return false;
    if (node.hasAttribute("disabled")) return false;
    if (node.getAttribute("aria-disabled") === "true") return false;
    if (node.getAttribute("aria-busy") === "true") return false;
    return true;
  }).catch(() => false);
};

const clickFirstInteractiveHandleInAnyContext = async (page, selectors, { timeout = 15000 } = {}) => {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeout) {
    const contexts = getPageContexts(page);
    for (const context of contexts) {
      for (const selector of selectors) {
        let handles = [];
        try {
          handles = await context.$$(selector);
        } catch (error) {
          handles = [];
        }
        for (const handle of handles) {
          if (!(await isInteractiveButtonHandle(handle))) continue;
          if (randomChance(0.35)) {
            await performHumanLikePageActivity(page, { intensity: "light" });
          }
          const clicked = await clickHandleHumanLike(page, handle);
          if (clicked) return true;
        }
      }
    }
    await sleep(180);
  }
  return false;
};

const findMessageInputHandle = async (page, selectors) => {
  return findFirstHandleInAnyContext(page, selectors, { requireVisible: true });
};

const getDeviceProfile = (account) => {
  if (!account?.deviceProfile) return pickDeviceProfile();
  try {
    return typeof account.deviceProfile === "string"
      ? JSON.parse(account.deviceProfile)
      : account.deviceProfile;
  } catch (error) {
    return pickDeviceProfile();
  }
};

const parseTimestamp = (value) => {
  if (!value) return { date: "", time: "", iso: "" };
  const raw = String(value).trim();
  const normalized = raw.replace(/\s+/g, " ");
  const timeMatch = normalized.match(/(\d{1,2}:\d{2})/);
  let date = "";
  let time = timeMatch ? timeMatch[1] : "";
  let dateObj = null;

  const isoCandidate = /\d{4}-\d{2}-\d{2}T/.test(normalized) ? normalized : "";
  if (isoCandidate) {
    const fixedIso = isoCandidate.replace(/([+-]\d{2})(\d{2})$/, "$1:$2");
    const parsed = new Date(fixedIso);
    if (!Number.isNaN(parsed.getTime())) {
      dateObj = parsed;
    }
  }

  const dateMatch = normalized.match(/(\d{1,2})[./](\d{1,2})[./](\d{2,4})/);
  if (!dateObj && dateMatch) {
    const day = Number(dateMatch[1]);
    const month = Number(dateMatch[2]) - 1;
    const year = Number(dateMatch[3].length === 2 ? `20${dateMatch[3]}` : dateMatch[3]);
    const parsed = new Date(year, month, day);
    if (!Number.isNaN(parsed.getTime())) {
      dateObj = parsed;
    }
  }

  if (!dateObj && /heute/i.test(normalized)) {
    dateObj = new Date();
  }

  if (!dateObj && /gestern/i.test(normalized)) {
    dateObj = new Date(Date.now() - 24 * 60 * 60 * 1000);
  }

  if (dateObj) {
    date = dateObj.toISOString().slice(0, 10);
    if (!time && /\d{1,2}:\d{2}/.test(normalized)) {
      time = normalized.match(/\d{1,2}:\d{2}/)?.[0] || "";
    }
    return { date, time, iso: dateObj.toISOString() };
  }

  return { date: "", time, iso: "" };
};

const pickApiMessageText = (message) => {
  if (!message || typeof message !== "object") return "";
  return message.text || message.textShort || message.textShortTrimmed || message.title || "";
};

const pickApiMessageAttachments = (message) =>
  (message && typeof message === "object" && Array.isArray(message.attachments))
    ? message.attachments
    : [];

const isPaymentAndShippingMessage = (message) => (
  String(message?.type || "").toUpperCase() === "PAYMENT_AND_SHIPPING_MESSAGE"
  || Boolean(message?.paymentAndShippingMessageType)
);

const mapApiThreadMessage = (message, index, participantName) => {
  const text = pickApiMessageText(message);
  const attachments = pickApiMessageAttachments(message);
  const hasContent = Boolean(String(text || "").trim()) || attachments.length > 0 || isPaymentAndShippingMessage(message);
  if (!hasContent) return null;

  const direction = message?.boundness === "OUTBOUND" ? "outgoing" : "incoming";
  const mapped = {
    id: message?.messageId || `message-${index}`,
    text,
    timeLabel: message?.receivedDate || "",
    dateTime: message?.receivedDate || "",
    direction,
    sender: direction === "outgoing" ? "Вы" : (participantName || "")
  };

  if (attachments.length) {
    mapped.attachments = attachments;
  }

  const passthroughKeys = [
    "type",
    "title",
    "active",
    "actions",
    "paymentAndShippingMessageType",
    "itemPriceInEuroCent",
    "shippingCostInEuroCent",
    "sellerTotalInEuroCent",
    "offerId",
    "negotiationId",
    "offeredPriceInEuroCent",
    "shippingType",
    "carrierId",
    "carrierName",
    "shippingOptionName",
    "shippingOptionDescription",
    "liabilityLimitInEuroCent",
    "oppTermsAndConditionsVersion",
    "termsAndConditionsChangeInfo"
  ];

  for (const key of passthroughKeys) {
    if (message?.[key] !== undefined) {
      mapped[key] = message[key];
    }
  }

  return mapped;
};

const clickVisibleButtonByText = async (
  page,
  needle,
  {
    timeout = 15000,
    preferDialog = false,
    preferTopLayer = false,
    requireInSelectors = [],
    excludeInSelectors = []
  } = {}
) => {
  if (!page) return false;
  const wanted = (Array.isArray(needle) ? needle : [needle])
    .map((value) => normalizeMatch(value))
    .filter(Boolean);
  if (!wanted.length) return false;
  const startedAt = Date.now();

  while (Date.now() - startedAt < timeout) {
    const contexts = getPageContexts(page);
    for (const context of contexts) {
      const clicked = await evaluateInContext(
        context,
      (labels, useDialog, useTopLayer, includeSelectors, skipSelectors) => {
        const normalize = (value) => (value || "").replace(/\s+/g, " ").trim().toLowerCase();
        const parseZ = (value) => {
          const parsed = Number.parseInt(String(value || "").replace(/[^\d-]+/g, ""), 10);
          return Number.isFinite(parsed) ? parsed : 0;
        };
        const getComposedParent = (node) => {
          if (!node) return null;
          if (node.parentElement) return node.parentElement;
          const root = typeof node.getRootNode === "function" ? node.getRootNode() : null;
          return root && root.host ? root.host : null;
        };
        const isInsideNode = (node, container) => {
          let current = node;
          while (current) {
            if (current === container) return true;
            current = getComposedParent(current);
          }
          return false;
        };
        const isInsideSelectors = (node, selectors) => {
          if (!node || !Array.isArray(selectors) || !selectors.length) return false;
          return selectors.some((selector) => {
            if (!selector) return false;
            let current = node;
            while (current) {
              try {
                if (typeof current.matches === "function" && current.matches(selector)) return true;
              } catch (error) {
                return false;
              }
              current = getComposedParent(current);
            }
            return false;
          });
        };
        const isVisible = (node) => {
          if (!node) return false;
          const style = window.getComputedStyle(node);
          if (!style) return false;
          if (style.display === "none" || style.visibility === "hidden" || style.opacity === "0" || style.pointerEvents === "none") {
            return false;
          }
          const rect = node.getBoundingClientRect();
          return rect.width > 0 && rect.height > 0;
        };
        const getLayerZIndex = (node) => {
          let max = 0;
          let current = node;
          while (current) {
            const style = window.getComputedStyle(current);
            if (style) {
              const z = parseZ(style.zIndex);
              if (z > max) max = z;
            }
            current = getComposedParent(current);
          }
          return max;
        };
        const isTopHit = (node) => {
          const rect = node.getBoundingClientRect();
          if (rect.width <= 0 || rect.height <= 0) return false;
          const centerX = Math.min(window.innerWidth - 1, Math.max(0, rect.left + rect.width / 2));
          const centerY = Math.min(window.innerHeight - 1, Math.max(0, rect.top + rect.height / 2));
          const hit = document.elementFromPoint(centerX, centerY);
          if (!hit) return false;
          return node === hit || node.contains(hit) || hit.contains(node);
        };
        const dialogSelector = "[role='dialog'], [aria-modal='true'], [data-testid*='modal'], [data-testid*='dialog'], [class*='Modal'], [class*='modal'], [class*='Dialog'], [class*='dialog']";

        const collectAllRoots = () => {
          const roots = [document];
          const queue = [document];
          const seen = new Set([document]);
          while (queue.length) {
            const root = queue.shift();
            const nodes = Array.from(root.querySelectorAll("*"));
            for (const node of nodes) {
              const shadowRoot = node.shadowRoot;
              if (!shadowRoot || seen.has(shadowRoot)) continue;
              seen.add(shadowRoot);
              roots.push(shadowRoot);
              queue.push(shadowRoot);
            }
          }
          return roots;
        };

        const allRoots = collectAllRoots();
        const dialogs = allRoots
          .flatMap((root) => {
            try {
              return Array.from(root.querySelectorAll(dialogSelector));
            } catch (error) {
              return [];
            }
          })
          .filter(isVisible);
        let order = 0;
        const scored = [];
        for (const root of allRoots) {
          let candidates = [];
          try {
            candidates = Array.from(root.querySelectorAll("button, a, [role='button'], input[type='button'], input[type='submit']"));
          } catch (error) {
            candidates = [];
          }
          for (const candidate of candidates) {
            if (!isVisible(candidate)) continue;
            if (candidate.hasAttribute("disabled") || candidate.getAttribute("aria-disabled") === "true") continue;
            if (includeSelectors.length && !isInsideSelectors(candidate, includeSelectors)) continue;
            if (skipSelectors.length && isInsideSelectors(candidate, skipSelectors)) continue;
            const text = normalize(
              candidate.textContent
              || candidate.getAttribute("aria-label")
              || candidate.getAttribute("title")
              || candidate.getAttribute("value")
            );
            if (!text) continue;
            const matchesLabel = labels.some((label) => text === label || text.includes(label));
            if (!matchesLabel) continue;
            const rect = candidate.getBoundingClientRect();
            const inDialog = dialogs.some((dialog) => isInsideNode(candidate, dialog));
            scored.push({
              candidate,
              inDialog,
              topHit: isTopHit(candidate),
              zIndex: getLayerZIndex(candidate),
              order: order += 1,
              area: rect.width * rect.height
            });
          }
        }

        if (!scored.length) return false;
        scored.sort((a, b) => {
          if (useDialog && dialogs.length && a.inDialog !== b.inDialog) {
            return Number(b.inDialog) - Number(a.inDialog);
          }
          if (useTopLayer && a.topHit !== b.topHit) {
            return Number(b.topHit) - Number(a.topHit);
          }
          if (a.zIndex !== b.zIndex) {
            return b.zIndex - a.zIndex;
          }
          if (useTopLayer && a.order !== b.order) {
            return b.order - a.order;
          }
          if (a.area !== b.area) {
            return a.area - b.area;
          }
          return 0;
        });

        const target = scored[0]?.candidate;
        if (!target) return false;
        try {
          if (typeof target.scrollIntoView === "function") {
            target.scrollIntoView({ block: "center", inline: "center" });
          }
          if (typeof target.focus === "function") target.focus({ preventScroll: true });
          const rect = target.getBoundingClientRect();
          const clientX = Math.max(1, Math.min(window.innerWidth - 2, rect.left + rect.width / 2));
          const clientY = Math.max(1, Math.min(window.innerHeight - 2, rect.top + rect.height / 2));
          const dispatchMouse = (type) => {
            target.dispatchEvent(new MouseEvent(type, {
              bubbles: true,
              cancelable: true,
              composed: true,
              view: window,
              clientX,
              clientY
            }));
          };
          dispatchMouse("pointerdown");
          dispatchMouse("mousedown");
          dispatchMouse("pointerup");
          dispatchMouse("mouseup");
          target.click();
          return true;
        } catch (error) {
          return false;
        }
      },
        wanted,
        preferDialog,
        preferTopLayer,
        requireInSelectors,
        excludeInSelectors
      );

      if (clicked === true) return true;
    }
    await sleep(220);
  }

  return false;
};

const hasVisibleDialogInAnyContext = async (page) => {
  const contexts = getPageContexts(page);
  for (const context of contexts) {
    const visible = await evaluateInContext(context, () => {
      const collectRoots = (startRoot) => {
        const roots = [startRoot];
        const queue = [startRoot];
        const seen = new Set([startRoot]);
        while (queue.length) {
          const root = queue.shift();
          let nodes = [];
          try {
            nodes = Array.from(root.querySelectorAll("*"));
          } catch (error) {
            nodes = [];
          }
          for (const node of nodes) {
            const shadowRoot = node?.shadowRoot;
            if (!shadowRoot || seen.has(shadowRoot)) continue;
            seen.add(shadowRoot);
            roots.push(shadowRoot);
            queue.push(shadowRoot);
          }
        }
        return roots;
      };
      const isVisible = (node) => {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        if (!style) return false;
        if (style.display === "none" || style.visibility === "hidden" || style.opacity === "0" || style.pointerEvents === "none") {
          return false;
        }
        const rect = node.getBoundingClientRect();
        return rect.width > 0 && rect.height > 0;
      };

      const dialogSelectors = [
        "[role='dialog']",
        "[aria-modal='true']",
        "[data-testid*='modal']",
        "[data-testid*='dialog']",
        "[class*='Modal']",
        "[class*='modal']",
        "[class*='Dialog']",
        "[class*='dialog']"
      ];
      const roots = collectRoots(document);
      for (const root of roots) {
        for (const selector of dialogSelectors) {
          let nodes = [];
          try {
            nodes = Array.from(root.querySelectorAll(selector));
          } catch (error) {
            nodes = [];
          }
          if (nodes.some(isVisible)) return true;
        }
      }
      return false;
    });
    if (visible) return true;
  }
  return false;
};

const dismissConversationBlockingModals = async (page, { maxPasses = 6 } = {}) => {
  if (!page) return false;
  const continueLabels = [
    "Weiter",
    "Fortfahren",
    "Alles klar",
    "Verstanden",
    "Okay",
    "Ok",
    "Schließen",
    "Schliessen",
    "Später",
    "Nicht jetzt",
    "Überspringen"
  ];
  const closeSelectors = [
    "[role='dialog'] button[aria-label*='schlie']",
    "[role='dialog'] button[aria-label*='close']",
    "[aria-modal='true'] button[aria-label*='schlie']",
    "[aria-modal='true'] button[aria-label*='close']",
    "[role='dialog'] button[data-testid*='close']",
    "[aria-modal='true'] button[data-testid*='close']",
    "[role='dialog'] button[class*='close']",
    "[aria-modal='true'] button[class*='close']"
  ];
  let actedAny = false;

  for (let pass = 0; pass < maxPasses; pass += 1) {
    let acted = false;
    const clickedContinue = await clickVisibleButtonByText(page, continueLabels, {
      timeout: 1400,
      preferDialog: true,
      preferTopLayer: true
    });
    if (clickedContinue) {
      acted = true;
      actedAny = true;
      await humanPause(110, 190);
      continue;
    }

    const clickedClose = await clickFirstInteractiveHandleInAnyContext(page, closeSelectors, { timeout: 1200 });
    if (clickedClose) {
      acted = true;
      actedAny = true;
      await humanPause(100, 170);
      continue;
    }

    await page.keyboard.press("Escape").catch(() => {});
    await sleep(120);

    const hasDialog = await hasVisibleDialogInAnyContext(page);
    if (!hasDialog) break;
    if (!acted) {
      await sleep(120);
    }
  }

  return actedAny;
};

const waitForMessageAttachmentReady = async (page, expectedCount = 1, timeout = 12000) => {
  if (!page) return false;
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeout) {
    const contexts = getPageContexts(page);
    for (const context of contexts) {
      const ready = await evaluateInContext(context, (expected) => {
        const normalizeExpected = Math.max(1, Number(expected) || 1);
        const inputSelectors = [
          "input[data-testid='reply-box-file-input']",
          "input[type='file'][accept*='image']",
          "input[type='file']"
        ];
        const previewSelectors = [
          "[data-testid*='attachment'] img",
          "[class*='Attachment'] img",
          "[class*='attachment'] img",
          "[class*='Reply'] img[src^='blob:']",
          "[class*='Reply'] img[src*='img.kleinanzeigen.de']",
          "[class*='Reply'] img",
          "[class*='reply'] img",
          ".ReplyBox img"
        ];
        const sendSelectors = [
          ".ReplyBox button[data-testid='submit-button']",
          "button[data-testid='submit-button'][aria-label*='Senden']",
          "button[data-testid='submit-button']",
          "button[aria-label*='Senden']",
          "button[type='submit']"
        ];

        const fileCount = inputSelectors.reduce((max, selector) => {
          let current = max;
          try {
            const inputs = Array.from(document.querySelectorAll(selector));
            for (const input of inputs) {
              const count = Number(input?.files?.length || 0);
              if (count > current) current = count;
            }
          } catch (error) {
            // ignore invalid selectors
          }
          return current;
        }, 0);

        const previewCount = previewSelectors.reduce((max, selector) => {
          try {
            return Math.max(max, document.querySelectorAll(selector).length);
          } catch (error) {
            return max;
          }
        }, 0);

        const sendReady = sendSelectors.some((selector) => {
          let buttons = [];
          try {
            buttons = Array.from(document.querySelectorAll(selector));
          } catch (error) {
            buttons = [];
          }
          return buttons.some((button) => {
            if (!button) return false;
            const style = window.getComputedStyle(button);
            if (!style) return false;
            if (style.display === "none" || style.visibility === "hidden" || style.opacity === "0" || style.pointerEvents === "none") {
              return false;
            }
            const rect = button.getBoundingClientRect();
            if (rect.width <= 0 || rect.height <= 0) return false;
            if (button.hasAttribute("disabled")) return false;
            if (button.getAttribute("aria-disabled") === "true") return false;
            if (button.getAttribute("aria-busy") === "true") return false;
            return true;
          });
        });

        const hasAttachment = fileCount >= normalizeExpected || previewCount > 0;
        return Boolean(hasAttachment && sendReady);
      }, expectedCount);

      if (ready) return true;
    }
    await sleep(220);
  }
  return false;
};

const waitForMessageSendButtonReady = async (page, timeout = 12000) => {
  if (!page) return false;
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeout) {
    const ready = await page.evaluate(() => {
      const selectors = [
        ".ReplyBox button[data-testid='submit-button']",
        "button[data-testid='submit-button'][aria-label*='Senden']",
        "button[data-testid='submit-button']",
        "button[aria-label*='Senden']"
      ];
      const isVisible = (node) => {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        if (!style) return false;
        if (style.display === "none" || style.visibility === "hidden" || style.opacity === "0" || style.pointerEvents === "none") {
          return false;
        }
        const rect = node.getBoundingClientRect();
        return rect.width > 0 && rect.height > 0;
      };
      for (const selector of selectors) {
        let buttons = [];
        try {
          buttons = Array.from(document.querySelectorAll(selector));
        } catch (error) {
          buttons = [];
        }
        for (const button of buttons) {
          if (!isVisible(button)) continue;
          if (button.hasAttribute("disabled")) continue;
          if (button.getAttribute("aria-disabled") === "true") continue;
          if (button.getAttribute("aria-busy") === "true") continue;
          return true;
        }
      }
      return false;
    }).catch(() => false);
    if (ready) return true;
    await sleep(180);
  }
  return false;
};

const isLikelyMessagingMutationRequest = (requestUrl, method = "", postData = "") => {
  const normalizedMethod = String(method || "").toUpperCase();
  if (!["POST", "PUT", "PATCH", "DELETE"].includes(normalizedMethod)) return false;
  const href = String(requestUrl || "").trim().toLowerCase();
  if (!href || !href.includes("kleinanzeigen")) return false;
  const payload = String(postData || "").toLowerCase();

  const blockedFragments = [
    "doubleclick",
    "googlesyndication",
    "google-analytics",
    "/analytics",
    "gtm.js",
    "hotjar",
    "clarity",
    "facebook",
    "fbevents",
    "pubmatic",
    "criteo",
    "teads",
    "xplosion",
    "pixel",
    "adserver",
    "adsm.",
    "measurement",
    "logger",
    "tracking",
    "bat.bing"
  ];
  if (blockedFragments.some((fragment) => href.includes(fragment))) {
    return false;
  }

  const payloadHintsMutation = /(mutation|send|reply|upload|attachment|image|media|picture|photo)/i.test(payload);
  if (payload) {
    return payloadHintsMutation;
  }

  return /(\/send|\/reply|\/upload|\/attachment|\/media|bilder|photos?)/i.test(href);
};

const waitForReplyComposerSettledAfterSend = async (
  page,
  { expectTextClear = false, expectAttachmentClear = false, timeout = 10000 } = {}
) => {
  if (!page) return false;
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeout) {
    const contexts = getPageContexts(page);
    let hasComposer = false;
    for (const context of contexts) {
      const state = await evaluateInContext(context, (requireText, requireAttachments) => {
        const composerRoot = document.querySelector(
          ".ReplyBox, [class*='ReplyBox'], textarea#nachricht, textarea[placeholder*='Nachricht'], button[data-testid='submit-button'], input[data-testid='reply-box-file-input']"
        );
        if (!composerRoot) {
          return { hasComposer: false, settled: true };
        }

        let textEmpty = true;
        if (requireText) {
          const textNode = document.querySelector(
            "textarea#nachricht, .ReplyBox textarea, [class*='ReplyBox'] textarea, textarea[placeholder*='Nachricht'], [role='textbox'][contenteditable='true'], [contenteditable='true']"
          );
          if (textNode) {
            const raw = textNode.isContentEditable
              ? (textNode.textContent || "")
              : (textNode.value || textNode.textContent || "");
            textEmpty = String(raw || "").trim().length === 0;
          }
        }

        let attachmentCount = 0;
        const attachmentSelectors = [
          "[data-testid*='attachment'] img",
          "[class*='Attachment'] img",
          "[class*='attachment'] img",
          ".ReplyBox img[src^='blob:']",
          ".ReplyBox img[src*='img.kleinanzeigen.de']"
        ];
        for (const selector of attachmentSelectors) {
          try {
            attachmentCount += document.querySelectorAll(selector).length;
          } catch (error) {
            // ignore invalid selector in this context
          }
        }

        let fileCount = 0;
        const fileInputSelectors = [
          "input[data-testid='reply-box-file-input']",
          ".ReplyBox input[type='file']",
          "input[type='file'][accept*='image']"
        ];
        for (const selector of fileInputSelectors) {
          let nodes = [];
          try {
            nodes = Array.from(document.querySelectorAll(selector));
          } catch (error) {
            nodes = [];
          }
          for (const node of nodes) {
            const count = Number(node?.files?.length || 0);
            if (count > fileCount) fileCount = count;
          }
        }

        const attachmentsCleared = attachmentCount === 0 && fileCount === 0;
        const settled = (!requireText || textEmpty) && (!requireAttachments || attachmentsCleared);
        return { hasComposer: true, settled };
      }, expectTextClear, expectAttachmentClear);

      if (!state) continue;
      if (!state.hasComposer) continue;
      hasComposer = true;
      if (state.settled) return true;
    }

    if (!hasComposer) {
      // Composer is no longer present (e.g. rerender completed) and we have no
      // evidence of pending inputs/attachments in visible contexts.
      return true;
    }
    await sleep(180);
  }
  return false;
};

const extractConversationId = (href) => {
  if (!href) return "";
  try {
    const url = new URL(href, MESSAGE_LIST_URL);
    return url.searchParams.get("conversationId")
      || url.searchParams.get("conversation")
      || url.searchParams.get("id")
      || "";
  } catch (error) {
    return "";
  }
};

const isAuthFailure = async (page) => {
  let currentUrl = "";
  try {
    currentUrl = page.url();
  } catch (error) {
    if (isDetachedFrameError(error)) return false;
    throw error;
  }
  if (/m-einloggen/.test(currentUrl)) return true;
  let content = "";
  try {
    content = await page.content();
  } catch (error) {
    if (isDetachedFrameError(error)) return false;
    throw error;
  }
  return /einloggen|anmelden|login/i.test(content) && /passwort|konto/i.test(content);
};

const gotoWithProxyHandling = async (page, url, options = {}, context = "") => {
  try {
    return await page.goto(url, options);
  } catch (error) {
    if (isProxyTunnelError(error)) {
      throw toProxyTunnelError(error, context || `GOTO_FAILED:${url}`);
    }
    throw error;
  }
};

const waitForDynamicContent = async (page, selectors, timeout = 15000) => {
  const selectorString = selectors.join(", ");
  await page.waitForFunction(
    (sel) => document.querySelectorAll(sel).length > 0,
    { timeout },
    selectorString
  ).catch(() => {});
};

const parseConversationList = async (page) => {
  await waitForDynamicContent(page, [
    "#conversation-list article",
    "[data-testid='conversation-list'] article",
    "a[href*='conversationId']",
    "a[href*='m-nachrichten']",
    "a[href*='nachrichten']",
    "a.AdImage img",
    "#conversation-list img",
    "[data-testid*='conversation']",
    "[data-qa*='conversation']",
    "[data-qa*='message-list']",
    "[data-testid*='message-list-item']",
    "[data-qa*='message-list-item']",
    "[class*='Conversation']",
    "[class*='conversation']",
    "[class*='MessageList']",
    "[class*='messagelist']"
  ]);

  return page.evaluate(() => {
    const normalize = (text) => (text || "").replace(/\s+/g, " ").trim();
    const cleanTitle = (text) => normalize(text).replace(/^(gelösch[^\s]*|gelöscht|reserviert|inaktiv)\s*[•-]?\s*/i, "").trim();
    const isTimeLike = (text) => {
      if (!text) return false;
      if (/\\b(heute|gestern|today|yesterday)\\b/i.test(text)) return true;
      if (/\\d{1,2}[:.]\\d{2}/.test(text)) return true;
      if (/\\d{1,2}\\.\\d{1,2}\\.\\d{2,4}/.test(text)) return true;
      return false;
    };
    const pickText = (root, selectors) => {
      for (const selector of selectors) {
        const nodes = Array.from(root.querySelectorAll(selector));
        for (const node of nodes) {
          const text = normalize(node.textContent);
          if (text) return text;
        }
      }
      return "";
    };

    const extractConversationId = (href, fallback) => {
      if (!href && !fallback) return "";
      try {
        const url = new URL(href || fallback, window.location.origin);
        return url.searchParams.get("conversationId")
          || url.searchParams.get("id")
          || url.searchParams.get("conversation")
          || "";
      } catch (e) {
        return "";
      }
    };

    const extractConversationIdFromTestIdValue = (value) => {
      const trimmed = (value || "").trim();
      if (!trimmed) return "";
      if (/\\s/.test(trimmed)) return "";
      if (!trimmed.includes(":")) return "";
      return trimmed;
    };

    const pickImage = (root, selectors) => {
      const extractFromNode = (node) => {
        if (!node) return "";
        const tag = node.tagName ? node.tagName.toLowerCase() : "";
        const candidates = [];
        if (tag === "img" || tag === "source") {
          const srcset = node.getAttribute("data-srcset")
            || node.getAttribute("srcset")
            || "";
          if (srcset) {
            candidates.push(srcset.split(",")[0].trim().split(" ")[0]);
          }
          candidates.push(
            node.getAttribute("data-src"),
            node.getAttribute("data-lazy-src"),
            node.getAttribute("data-original"),
            node.currentSrc,
            node.src
          );
        } else {
          candidates.push(
            node.getAttribute("data-src"),
            node.getAttribute("data-lazy-src"),
            node.getAttribute("data-original"),
            node.getAttribute("data-bg"),
            node.getAttribute("data-background")
          );
          const style = node.getAttribute("style") || "";
          const match = style.match(/url\\([\"']?([^\"')]+)[\"']?\\)/i);
          if (match) candidates.push(match[1]);
        }

        for (const candidate of candidates) {
          let src = (candidate || "").trim();
          if (!src) continue;
          if (src.includes(",")) {
            src = src.split(",")[0].trim().split(" ")[0];
          }
          if (src.includes("data:image/") || src.includes("placeholder")) continue;
          try {
            return new URL(src, window.location.origin).href;
          } catch (e) {
            return src;
          }
        }

        return "";
      };

      for (const selector of selectors) {
        const node = root.querySelector(selector);
        const src = extractFromNode(node);
        if (src) return src;
      }
      return "";
    };

    // Find conversation list container
    const listContainers = [
      "[data-testid*='message-list']",
      "[data-qa*='message-list']",
      "[data-testid*='conversation-list']",
      "[data-qa*='conversation-list']",
      "[class*='MessageList']",
      "[class*='message-list']",
      "[class*='ConversationList']",
      "[class*='conversation-list']",
      "ul[role='list']",
      "ul"
    ]
      .map((selector) => document.querySelector(selector))
      .filter(Boolean);
    const searchRoots = listContainers.length ? listContainers : [document];

    const baseUrl = "https://www.kleinanzeigen.de/m-nachrichten.html";

    const buildHrefFromId = (id) => {
      if (!id) return "";
      try {
        return `${baseUrl}?conversationId=${encodeURIComponent(id)}`;
      } catch (e) {
        return "";
      }
    };

    const pickHrefFromNode = (node) => {
      if (!node) return "";
      return node.getAttribute("href")
        || node.getAttribute("data-href")
        || node.getAttribute("data-url")
        || node.getAttribute("data-link")
        || node.getAttribute("data-target")
        || node.getAttribute("data-routerlink")
        || "";
    };

    const findHrefInCard = (card) => {
      if (!card) return "";
      const direct = pickHrefFromNode(card);
      if (direct) return direct;
      const candidate = card.querySelector("[href], [data-href], [data-url], [data-link], [data-target], [data-routerlink]");
      return candidate ? pickHrefFromNode(candidate) : "";
    };

    const extractConversationIdFromAttrs = (node) => {
      if (!node) return "";
      return node.getAttribute("data-conversation-id")
        || node.getAttribute("data-qa-conversation-id")
        || node.getAttribute("data-conversationid")
        || node.getAttribute("data-conversation")
        || node.getAttribute("data-id")
        || "";
    };

    const extractConversationIdFromTestId = (node) => {
      if (!node) return "";
      const direct = extractConversationIdFromTestIdValue(node.getAttribute("data-testid"));
      if (direct) return direct;
      const candidates = Array.from(node.querySelectorAll("[data-testid]"));
      for (const candidate of candidates) {
        const value = extractConversationIdFromTestIdValue(candidate.getAttribute("data-testid"));
        if (value) return value;
      }
      return "";
    };

    const pickParticipantFromHeader = (card) => {
      const header = card.querySelector("header");
      if (!header) return "";
      const spans = Array.from(header.querySelectorAll("span"));
      for (const span of spans) {
        const text = normalize(span.textContent);
        if (!text) continue;
        if (isTimeLike(text)) continue;
        return text;
      }
      return "";
    };

    // Find conversation links using multiple strategies
    const linkSelectors = [
      "a[href*='conversationId']",
      "a[href*='m-nachrichten.html?']",
      "a[href*='m-nachrichten']",
      "a[href*='nachrichten']",
      "[data-qa*='conversation-link']",
      "[data-testid*='conversation-link']",
      "[data-qa*='message-link']",
      "[data-testid*='message-link']"
    ];
    const allAnchors = [];
    for (const root of searchRoots) {
      for (const sel of linkSelectors) {
        const nodes = Array.from(root.querySelectorAll(sel));
        for (const node of nodes) {
          if (node.tagName === "A") {
            allAnchors.push(node);
          } else {
            const anchor = node.closest("a[href]");
            if (anchor) allAnchors.push(anchor);
          }
        }
      }
    }
    const uniqueAnchors = Array.from(new Set(allAnchors));

    // Find conversation cards (closest meaningful container)
    const strongCardSelectors =
      "li, article, [role='listitem'], [data-testid*='conversation'], [data-qa*='conversation'], "
      + "[class*='conversation'], [class*='Conversation'], [class*='message-item'], [class*='MessageItem']";
    const linkCountSelector = "a[href*='conversationId'], a[href*='m-nachrichten'], a[href*='nachrichten']";

    const pickCardForAnchor = (anchor) => {
      if (!anchor) return null;
      let lastCandidate = anchor.closest(strongCardSelectors) || anchor;
      let current = anchor;
      while (current && current !== document.body) {
        if (current.matches && current.matches(strongCardSelectors)) return current;
        if (current.matches && (current.matches("div") || current.matches("section"))) {
          const linkCount = current.querySelectorAll(linkCountSelector).length;
          if (linkCount === 1) {
            lastCandidate = current;
          } else if (linkCount > 1) {
            break;
          }
        }
        current = current.parentElement;
      }
      return lastCandidate || anchor;
    };

    const directCards = Array.from(document.querySelectorAll(
      "#conversation-list article, [data-testid='conversation-list'] article, article.ConversationListItem"
    ));

    let cards = directCards.length
      ? directCards
      : uniqueAnchors
        .map((anchor) => pickCardForAnchor(anchor))
        .filter(Boolean);

    if (!cards.length) {
      const fallbackCardSelectors = [
        "[data-testid*='conversation']",
        "[data-qa*='conversation']",
        "[data-testid*='conversation-item']",
        "[data-qa*='conversation-item']",
        "[data-testid*='message-item']",
        "[data-qa*='message-item']",
        "[data-testid*='message-list-item']",
        "[data-qa*='message-list-item']",
        "[class*='conversation']",
        "[class*='Conversation']",
        "[class*='message-item']",
        "[class*='MessageItem']",
        "[class*='messageListItem']",
        "[class*='MessageListItem']",
        "[role='button']",
        "[role='listitem']",
        "li",
        "article"
      ];
      const fallbackCandidates = [];
      for (const root of searchRoots) {
        for (const selector of fallbackCardSelectors) {
          try {
            fallbackCandidates.push(...Array.from(root.querySelectorAll(selector)));
          } catch (e) {}
        }
      }
      const uniqueCandidates = Array.from(new Set(fallbackCandidates));
      const refinedCandidates = uniqueCandidates.filter((node) => {
        if (!node) return false;
        const hasId = Boolean(extractConversationIdFromAttrs(node));
        if (hasId) return true;
        const descendantWithId = node.querySelector(
          "[data-conversation-id], [data-qa-conversation-id], [data-conversationid], [data-conversation], [data-id]"
        );
        if (descendantWithId) return true;
        const linkCount = node.querySelectorAll(linkCountSelector).length;
        return linkCount <= 1;
      });
      const leafCandidates = refinedCandidates.filter(
        (node) => !refinedCandidates.some((other) => other !== node && node.contains(other))
      );
      cards = leafCandidates.length ? leafCandidates : refinedCandidates;
    }
    const uniqueCards = Array.from(new Set(cards));

    return uniqueCards
      .map((card) => {
        // Find conversation link
        const link = card.querySelector("a[href*='conversationId']")
          || card.querySelector("a[href*='m-nachrichten.html?']")
          || card.querySelector("a[href*='m-nachrichten']")
          || card.querySelector("a[href*='nachrichten']");
        let href = link ? (link.href || link.getAttribute("href") || "") : "";
        if (!href) href = findHrefInCard(card);
        if (href && /\/(s-anzeige|zur-anzeige)\//.test(href)) href = "";

        let conversationId = extractConversationId(href)
          || extractConversationIdFromAttrs(card)
          || extractConversationIdFromTestId(card);
        if (!conversationId) {
          const dataNode = card.querySelector("[data-conversation-id], [data-qa-conversation-id], [data-conversationid], [data-conversation], [data-id], [data-testid]");
          conversationId = extractConversationIdFromAttrs(dataNode)
            || extractConversationIdFromTestId(dataNode);
        }
        if (!href && conversationId) href = buildHrefFromId(conversationId);

        // Extract participant name
        const participantFromHeader = pickParticipantFromHeader(card);
        const participant = participantFromHeader || pickText(card, [
          "[data-testid*='username']",
          "[data-testid*='user-name']",
          "[data-testid*='conversation-title']",
          "[data-qa*='username']",
          "[data-qa*='user-name']",
          "[data-qa*='conversation-title']",
          "[data-qa*='partner']",
          "[data-qa*='participant']",
          "[data-testid*='sender']",
          "[data-testid*='participant']",
          "[class*='username']",
          "[class*='userName']",
          "[class*='UserName']",
          ".conversation-title",
          ".conversation__title",
          ".conversation__name",
          "[class*='sender']",
          "[class*='Sender']",
          "strong",
          "b",
          "h4"
        ]);

        // Extract ad title
        let adTitle = pickText(card, [
          "a[href*='/zur-anzeige/']",
          "a[href*='/s-anzeige/']",
          "[data-testid*='ad-title']",
          "[data-testid*='item-title']",
          "[data-testid*='subject']",
          "[data-qa*='ad-title']",
          "[data-qa*='item-title']",
          "[data-qa*='subject']",
          "[data-qa*='title']",
          "[class*='adTitle']",
          "[class*='AdTitle']",
          "[class*='ad-title']",
          "[class*='itemTitle']",
          ".conversation__ad-title",
          ".ad-title",
          ".conversation__subject",
          ".message-list__title",
          "[class*='subject']",
          "[class*='Subject']",
          "h3",
          "h2"
        ]);
        adTitle = cleanTitle(adTitle);

        // Extract last message preview
        const lastMessage = pickText(card, [
          "[data-testid*='snippet']",
          "[data-testid*='preview']",
          "[data-testid*='last-message']",
          "[data-qa*='snippet']",
          "[data-qa*='preview']",
          "[data-qa*='last-message']",
          "[data-qa*='message-preview']",
          "[class*='snippet']",
          "[class*='Snippet']",
          "[class*='preview']",
          "[class*='Preview']",
          "[class*='lastMessage']",
          ".conversation__snippet",
          ".conversation__preview",
          ".message-preview",
          ".conversation__message",
          ".conversation__last-message",
          ".message__preview",
          ".text",
          "p"
        ]);

        // Extract ad image
        const adImage = pickImage(card, [
          "a.AdImage img",
          "a.AdImage picture img",
          "a.AdImage picture source",
          "a.AdImage source",
          ".AdImage img",
          "[class*='AdImage'] img",
          "img[src*='img.kleinanzeigen.de']",
          "img[src*='prod-ads/images']",
          "[data-testid*='ad'] img",
          "[data-testid*='item'] img",
          "[data-testid*='image'] img",
          "[data-qa*='ad'] img",
          "[data-qa*='item'] img",
          "[data-qa*='image'] img",
          "[class*='ad-image'] img",
          "[class*='itemImage'] img",
          ".conversation__image img",
          ".ad-image img",
          "img[alt*='Anzeige']",
          "img[alt*='anzeige']",
          "img[alt*='ad']",
          "picture img",
          "picture source",
          "source",
          "[style*='background-image']",
          "img"
        ]);

        // Extract timestamp
        const timeText = pickText(card, [
          "time",
          "[datetime]",
          "[data-testid*='time']",
          "[data-testid*='date']",
          "[data-testid*='timestamp']",
          "[data-qa*='time']",
          "[data-qa*='date']",
          "[data-qa*='timestamp']",
          "[class*='time']",
          "[class*='Time']",
          "[class*='date']",
          "[class*='Date']",
          "[class*='timestamp']",
          ".conversation__time",
          ".timestamp"
        ]);

        // Detect unread status
        const cardClass = (card.className || "") + " " + (card.getAttribute("data-testid") || "");
        const unread = /unread|new|highlight|unseen|badge/i.test(cardClass)
          || Boolean(card.querySelector(
          ".badge, .unread, [data-testid*='unread'], [data-testid*='badge'], "
            + "[class*='unread'], [class*='Unread'], [class*='badge'], [class*='Badge'], "
            + "[class*='unseen'], [class*='Unseen'], [class*='new-message']"
          ));

        // Extract avatar
        const avatar = pickImage(card, [
          "[data-testid*='avatar'] img",
          "[data-qa*='avatar'] img",
          "[class*='avatar'] img",
          "[class*='Avatar'] img",
          ".user-avatar img",
          "img[class*='avatar']",
          "img[class*='Avatar']"
        ]);

        return {
          href,
          conversationId,
          participant,
          adTitle,
          adImage,
          lastMessage,
          timeText,
          unread,
          avatar
        };
      })
      .filter((item) => item.href || item.conversationId || item.participant || item.adTitle);
  });
};

const parseMessagesFromThread = async (page, fallbackSender) => {
  await waitForDynamicContent(page, [
    "[data-message-id]",
    "[data-testid*='message']",
    "[data-qa*='message']",
    "[class*='message']",
    "[class*='Message']",
    "[class*='chat']",
    "[class*='Chat']"
  ], 12000);

  return page.evaluate((senderFallback) => {
    const normalize = (text) => (text || "").replace(/\s+/g, " ").trim();
    const threadRoots = [
      "[data-testid*='message-thread']",
      "[data-qa*='message-thread']",
      "[data-testid*='chat-thread']",
      "[data-qa*='chat-thread']",
      "[class*='MessageThread']",
      "[class*='message-thread']",
      "[class*='ChatThread']",
      "[class*='chat-thread']",
      "[class*='conversation-thread']",
      "[class*='ConversationThread']"
    ]
      .map((selector) => document.querySelector(selector))
      .filter(Boolean);
    const searchRoots = threadRoots.length ? threadRoots : [document];

    const selectorList = [
      "[data-message-id]",
      "[data-qa*='message']",
      "[data-testid*='message']",
      "[data-testid*='chat-message']",
      "[data-testid*='bubble']",
      "[data-qa*='chat-message']",
      "[data-qa*='bubble']",
      "[data-qa*='message-item']",
      "[data-testid*='message-item']",
      "[class*='MessageBubble']",
      "[class*='messageBubble']",
      "[class*='message-bubble']",
      "[class*='ChatMessage']",
      "[class*='chatMessage']",
      ".message",
      ".chat-message",
      ".msg",
      ".message-thread__message",
      ".chat__message",
      "[class*='Message'][class*='item']",
      "[class*='message'][class*='item']"
    ];

    const allNodes = [];
    for (const root of searchRoots) {
      for (const selector of selectorList) {
        try { allNodes.push(...Array.from(root.querySelectorAll(selector))); } catch (e) {}
      }
    }
    const uniqueNodes = Array.from(new Set(allNodes));
    const nodes = uniqueNodes.filter(
      (node) => !uniqueNodes.some((other) => other !== node && other.contains(node))
    );

    return nodes
      .map((node, index) => {
        const messageId = node.getAttribute("data-message-id")
          || node.getAttribute("data-qa-message-id")
          || node.getAttribute("data-testid")
          || node.getAttribute("id")
          || `message-${index}`;

        const textSelectors = [
          "[data-qa*='text']",
          "[data-testid*='text']",
          "[data-testid*='body']",
          "[data-testid*='content']",
          "[data-qa*='message-text']",
          "[data-qa*='message-body']",
          "[data-qa*='message-content']",
          "[class*='messageText']",
          "[class*='MessageText']",
          "[class*='message-text']",
          "[class*='messageBody']",
          "[class*='MessageBody']",
          "[class*='message-body']",
          "[class*='messageContent']",
          "[class*='MessageContent']",
          ".message-text",
          ".message__text",
          ".chat-message__text",
          ".text",
          "span[dir='auto']",
          "p"
        ];
        let textNode = null;
        for (const sel of textSelectors) {
          textNode = node.querySelector(sel);
          if (textNode) break;
        }
        const rawText = normalize(textNode ? textNode.textContent : "");
        const text = rawText || normalize(node.textContent);
        if (!text) return null;

        const timeSelectors = [
          "time",
          "[datetime]",
          "[data-testid*='time']",
          "[data-testid*='date']",
          "[data-testid*='timestamp']",
          "[data-qa*='time']",
          "[data-qa*='date']",
          "[data-qa*='timestamp']",
          "[class*='time']",
          "[class*='Time']",
          "[class*='date']",
          "[class*='Date']",
          "[class*='timestamp']",
          ".message__time",
          ".chat-message__time"
        ];
        let timeNode = null;
        for (const sel of timeSelectors) {
          timeNode = node.querySelector(sel);
          if (timeNode) break;
        }
        const timeLabel = timeNode ? normalize(timeNode.textContent) : "";
        const dateTime = timeNode
          ? (timeNode.getAttribute("datetime") || timeNode.getAttribute("data-time") || "")
          : "";

        const fullClass = (node.className || "") + " " + (node.getAttribute("data-testid") || "")
          + " " + (node.getAttribute("data-qa") || "") + " " + (node.getAttribute("data-direction") || "");
        const parentClass = node.parentElement ? (node.parentElement.className || "") : "";
        const combinedClass = fullClass + " " + parentClass;

        const qaData = `${node.getAttribute("data-qa") || ""} ${node.getAttribute("data-testid") || ""}`;
        const ariaLabel = `${node.getAttribute("aria-label") || ""}`;
        const isOutgoing = /outgoing|sent|from-me|own|self|right|align-right|myMessage|my-message/i.test(combinedClass)
          || /outgoing|sent|own|self|from-me/i.test(qaData)
          || /du:|von dir|you:/i.test(ariaLabel)
          || node.getAttribute("data-direction") === "outgoing"
          || node.getAttribute("data-type") === "sent";

        // Try to extract sender name from the message
        const senderSelectors = [
          "[data-testid*='sender']",
          "[data-testid*='author']",
          "[data-testid*='username']",
          "[class*='sender']",
          "[class*='Sender']",
          "[class*='author']",
          "[class*='Author']",
          "[class*='username']",
          "[class*='UserName']",
          ".message__sender",
          ".message__author"
        ];
        let senderNode = null;
        for (const sel of senderSelectors) {
          senderNode = node.querySelector(sel);
          if (senderNode) break;
        }
        const extractedSender = senderNode ? normalize(senderNode.textContent) : "";
        const sender = isOutgoing ? "Вы" : (extractedSender || senderFallback || "");

        return {
          id: messageId,
          text,
          timeLabel,
          dateTime,
          direction: isOutgoing ? "outgoing" : "incoming",
          sender
        };
      })
      .filter(Boolean);
  }, fallbackSender);
};

const parseConversationMetaFromThread = async (page) => {
  return page.evaluate(() => {
    const normalize = (text) => (text || "").replace(/\s+/g, " ").trim();
    const headerRoots = [
      "[data-testid*='message-header']",
      "[data-qa*='message-header']",
      "[data-testid*='conversation-header']",
      "[data-qa*='conversation-header']",
      "[class*='message-header']",
      "[class*='MessageHeader']",
      "[class*='conversation-header']",
      "[class*='ConversationHeader']",
      "header"
    ]
      .map((selector) => document.querySelector(selector))
      .filter(Boolean);
    const searchRoots = headerRoots.length ? headerRoots : [document];

    const pickText = (selectors) => {
      for (const root of searchRoots) {
        for (const selector of selectors) {
          try {
            const nodes = Array.from(root.querySelectorAll(selector));
            for (const node of nodes) {
              const text = normalize(node.textContent);
              if (text) return text;
            }
          } catch (e) {}
        }
      }
      return "";
    };
    const pickImage = (selectors) => {
      const extractFromNode = (node) => {
        if (!node) return "";
        const tag = node.tagName ? node.tagName.toLowerCase() : "";
        const candidates = [];
        if (tag === "img" || tag === "source") {
          const srcset = node.getAttribute("data-srcset")
            || node.getAttribute("srcset")
            || "";
          if (srcset) {
            candidates.push(srcset.split(",")[0].trim().split(" ")[0]);
          }
          candidates.push(
            node.getAttribute("data-src"),
            node.getAttribute("data-lazy-src"),
            node.getAttribute("data-original"),
            node.currentSrc,
            node.src
          );
        } else {
          candidates.push(
            node.getAttribute("data-src"),
            node.getAttribute("data-lazy-src"),
            node.getAttribute("data-original"),
            node.getAttribute("data-bg"),
            node.getAttribute("data-background")
          );
          const style = node.getAttribute("style") || "";
          const match = style.match(/url\\([\"']?([^\"')]+)[\"']?\\)/i);
          if (match) candidates.push(match[1]);
        }

        for (const candidate of candidates) {
          let src = (candidate || "").trim();
          if (!src) continue;
          if (src.includes(",")) {
            src = src.split(",")[0].trim().split(" ")[0];
          }
          if (src.includes("data:image/") || src.includes("placeholder")) continue;
          try {
            return new URL(src, window.location.origin).href;
          } catch (e) {
            return src;
          }
        }
        return "";
      };

      for (const root of searchRoots) {
        for (const selector of selectors) {
          try {
            const node = root.querySelector(selector);
            const src = extractFromNode(node);
            if (src) return src;
          } catch (e) {}
        }
      }
      return "";
    };

    let adTitle = pickText([
      "[data-testid*='ad-title']",
      "[data-testid*='item-title']",
      "[data-testid*='subject']",
      "[data-qa*='ad-title']",
      "[data-qa*='item-title']",
      "[data-qa*='subject']",
      "[data-qa*='title']",
      "[class*='adTitle']",
      "[class*='AdTitle']",
      "[class*='ad-title']",
      "[class*='itemTitle']",
      "[class*='ItemTitle']",
      ".message-header__title",
      ".conversation__ad-title",
      ".chat-header__subject",
      ".message-header__subject",
      ".chat-header__title",
      "[class*='header'] a[href*='/zur-anzeige/']",
      "[class*='header'] a[href*='/s-anzeige/']",
      "a[href*='/zur-anzeige/']",
      "a[href*='/s-anzeige/']",
      "h1",
      "h2"
    ]);
    adTitle = normalize(adTitle).replace(/^(gelösch[^\s]*|gelöscht|reserviert|inaktiv)\s*[•-]?\s*/i, "").trim();
    const adImageFromLink = pickImage([
      "a.AdImage img",
      "a.AdImage picture img",
      "a.AdImage picture source",
      "a.AdImage source",
      "a[href*='/zur-anzeige/'] img",
      "a[href*='/zur-anzeige/'] picture img",
      "a[href*='/zur-anzeige/'] picture source",
      "a[href*='/zur-anzeige/'] source",
      "a[href*='/zur-anzeige/'] [style*='background-image']",
      "a[href*='/s-anzeige/'] img",
      "a[href*='/s-anzeige/'] picture img",
      "a[href*='/s-anzeige/'] picture source",
      "a[href*='/s-anzeige/'] source",
      "a[href*='/s-anzeige/'] [style*='background-image']"
    ]);
    const adImage = adImageFromLink || pickImage([
      "[data-testid*='ad-image'] img",
      "[data-testid*='ad'] img",
      "[data-testid*='item-image'] img",
      "[data-testid*='item'] img",
      "[data-qa*='ad-image'] img",
      "[data-qa*='ad'] img",
      "[data-qa*='item-image'] img",
      "[data-qa*='item'] img",
      "[class*='adImage'] img",
      "[class*='AdImage'] img",
      "[class*='ad-image'] img",
      "[class*='itemImage'] img",
      "[class*='ItemImage'] img",
      ".message-header__image img",
      ".conversation__image img",
      ".chat-header__image img",
      "[class*='header'] img",
      "img[alt*='Anzeige']",
      "img[alt*='anzeige']",
      "picture img",
      "picture source",
      "source",
      "[style*='background-image']",
      "img"
    ]);

    // Also try to extract participant name from thread header
    const participant = pickText([
      "[data-testid*='username']",
      "[data-testid*='user-name']",
      "[data-testid*='partner']",
      "[data-testid*='participant']",
      "[data-qa*='username']",
      "[data-qa*='user-name']",
      "[data-qa*='partner']",
      "[data-qa*='participant']",
      "[class*='partnerName']",
      "[class*='PartnerName']",
      "[class*='partner-name']",
      "[class*='username']",
      "[class*='UserName']"
    ]);

    return { adTitle, adImage, participant };
  });
};

const buildConversationKey = (conversation, accountId) => {
  const href = conversation?.href || conversation?.conversationUrl || "";
  const resolvedId = conversation?.conversationId || extractConversationId(href);
  const participant = (conversation?.participant || conversation?.sender || "").trim().toLowerCase();
  const adTitle = (conversation?.adTitle || "").trim().toLowerCase();
  const lastMessage = (conversation?.lastMessage || conversation?.message || "").trim().toLowerCase();
  const timeText = (conversation?.timeText || conversation?.time || "").trim().toLowerCase();
  const accountKey = accountId ?? conversation?.accountId ?? "unknown";
  if (resolvedId) return `id:${accountKey}:${resolvedId}`;
  return `fallback:${accountKey}:${participant}|${adTitle}|${lastMessage}|${timeText}`;
};

const dedupeConversationList = (conversations, accountId) => {
  const seen = new Set();
  const deduped = [];
  for (const conversation of conversations) {
    const key = buildConversationKey(conversation, accountId);
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(conversation);
  }
  return deduped;
};

const fetchConversationListFromWeb = async ({
  account,
  proxy,
  accountLabel,
  deviceProfile,
  cookies,
  maxConversations
}) => {
  const proxyServer = buildProxyServer(proxy);
  const proxyUrl = buildProxyUrl(proxy);
  const needsProxyChain = Boolean(
    proxyUrl && ((proxy?.type || "").toLowerCase().startsWith("socks") || proxy?.username || proxy?.password)
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

  const profileDir = createTempProfileDir();
  const browser = await puppeteer.launch({
    headless: "new",
    args: launchArgs,
    userDataDir: profileDir,
    timeout: PUPPETEER_LAUNCH_TIMEOUT,
    protocolTimeout: PUPPETEER_PROTOCOL_TIMEOUT
  });

  try {
    const page = await browser.newPage();
    page.setDefaultTimeout(PUPPETEER_NAV_TIMEOUT);
    page.setDefaultNavigationTimeout(PUPPETEER_NAV_TIMEOUT);
    if (!anonymizedProxyUrl && (proxy?.username || proxy?.password)) {
      await page.authenticate({
        username: proxy.username || "",
        password: proxy.password || ""
      });
    }

    await page.setUserAgent(deviceProfile.userAgent);
    await page.setViewport(deviceProfile.viewport);
    await page.setExtraHTTPHeaders({ "Accept-Language": deviceProfile.locale });
    await page.emulateTimezone(deviceProfile.timezone);
    await page.evaluateOnNewDocument((platform) => {
      Object.defineProperty(navigator, "platform", {
        get: () => platform
      });
    }, deviceProfile.platform);

    await page.goto("https://www.kleinanzeigen.de/", { waitUntil: "domcontentloaded" });
    await humanPause();
    await performHumanLikePageActivity(page, { intensity: "light" });
    await page.setCookie(...cookies);
    await humanPause();
    await page.goto(MESSAGE_LIST_URL, { waitUntil: "domcontentloaded" });
    await humanPause(180, 320);
    await performHumanLikePageActivity(page, { intensity: "medium" });

    if (await isAuthFailure(page)) {
      const error = new Error("AUTH_REQUIRED");
      error.code = "AUTH_REQUIRED";
      throw error;
    }

    const conversations = await parseConversationList(page);
    const limited = maxConversations ? conversations.slice(0, maxConversations) : conversations;
    console.log(`[messageService] Web list conversations: ${limited.length} for ${accountLabel}`);
    return limited.map((conversation) => ({
      ...conversation,
      accountId: account.id,
      accountLabel
    }));
  } finally {
    await browser.close();
    if (anonymizedProxyUrl) {
      await proxyChain.closeAnonymizedProxy(anonymizedProxyUrl, true);
    }
    fs.rmSync(profileDir, { recursive: true, force: true });
  }
};

const fetchConversationHeaderPreviews = async ({
  account,
  proxy,
  accountLabel,
  deviceProfile,
  cookies,
  conversations
}) => {
  if (!Array.isArray(conversations) || conversations.length === 0) return [];

  const proxyServer = buildProxyServer(proxy);
  const proxyUrl = buildProxyUrl(proxy);
  const needsProxyChain = Boolean(
    proxyUrl && ((proxy?.type || "").toLowerCase().startsWith("socks") || proxy?.username || proxy?.password)
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

  const profileDir = createTempProfileDir();
  const browser = await puppeteer.launch({
    headless: "new",
    args: launchArgs,
    userDataDir: profileDir,
    timeout: PUPPETEER_LAUNCH_TIMEOUT,
    protocolTimeout: PUPPETEER_PROTOCOL_TIMEOUT
  });

  try {
    const page = await browser.newPage();
    page.setDefaultTimeout(PUPPETEER_NAV_TIMEOUT);
    page.setDefaultNavigationTimeout(PUPPETEER_NAV_TIMEOUT);
    if (!anonymizedProxyUrl && (proxy?.username || proxy?.password)) {
      await page.authenticate({
        username: proxy.username || "",
        password: proxy.password || ""
      });
    }

    await page.setUserAgent(deviceProfile.userAgent);
    await page.setViewport(deviceProfile.viewport);
    await page.setExtraHTTPHeaders({ "Accept-Language": deviceProfile.locale });
    await page.emulateTimezone(deviceProfile.timezone);
    await page.evaluateOnNewDocument((platform) => {
      Object.defineProperty(navigator, "platform", {
        get: () => platform
      });
    }, deviceProfile.platform);

    await page.goto("https://www.kleinanzeigen.de/", { waitUntil: "domcontentloaded" });
    await humanPause();
    await performHumanLikePageActivity(page, { intensity: "light" });
    await page.setCookie(...cookies);
    await humanPause();
    await page.goto(MESSAGE_LIST_URL, { waitUntil: "domcontentloaded" });
    await humanPause(180, 320);
    await performHumanLikePageActivity(page, { intensity: "medium" });

    if (await isAuthFailure(page)) {
      const error = new Error("AUTH_REQUIRED");
      error.code = "AUTH_REQUIRED";
      throw error;
    }

    const results = [];
    for (const conversation of conversations) {
      const href = conversation.conversationUrl
        || conversation.href
        || (conversation.conversationId ? buildConversationUrl(conversation.conversationId) : "");
      if (!href) continue;

      await page.goto(href, { waitUntil: "domcontentloaded" });
      await humanPause(160, 280);
      if (randomChance(0.55)) {
        await performHumanLikePageActivity(page, { intensity: "light" });
      }

      const meta = await parseConversationMetaFromThread(page);
      const resolvedId = conversation.conversationId || extractConversationId(href);
      const adImage = normalizeImageUrl(meta.adImage);
      results.push({
        conversationId: resolvedId,
        adImage,
        adTitle: meta.adTitle || conversation.adTitle || "",
        participant: meta.participant || conversation.participant || "",
        href
      });
    }
    console.log(`[messageService] Header previews fetched: ${results.length} for ${accountLabel}`);
    return results;
  } finally {
    await browser.close();
    if (anonymizedProxyUrl) {
      await proxyChain.closeAnonymizedProxy(anonymizedProxyUrl, true);
    }
    fs.rmSync(profileDir, { recursive: true, force: true });
  }
};

const sendConversationMessage = async ({
  account,
  proxy,
  conversationId,
  conversationUrl,
  participant,
  adTitle,
  text
}) => {
  if (!conversationId && !conversationUrl && !participant && !adTitle) {
    const error = new Error("CONVERSATION_ID_REQUIRED");
    error.code = "CONVERSATION_ID_REQUIRED";
    throw error;
  }

  const deviceProfile = getDeviceProfile(account);
  const cookies = parseCookies(account.cookie).map(normalizeCookie);
  if (!cookies.length) {
    const error = new Error("AUTH_REQUIRED");
    error.code = "AUTH_REQUIRED";
    throw error;
  }

  let userId = getUserIdFromCookies(cookies);
  let accessTokenInfo = null;
  if (!FORCE_WEB_MESSAGES) {
    if (!userId) {
      try {
        accessTokenInfo = await fetchMessageboxAccessToken({ cookies, proxy, deviceProfile });
        userId = getUserIdFromAccessToken(accessTokenInfo.token);
      } catch (error) {
        console.log(`[messageService] Access token lookup failed: ${error.message}`);
      }
    }
    if (userId) {
      try {
        if (!accessTokenInfo) {
          accessTokenInfo = await fetchMessageboxAccessToken({ cookies, proxy, deviceProfile });
        }
        let resolvedConversationId = conversationId || extractConversationId(conversationUrl);

        if (!resolvedConversationId) {
          const apiList = await fetchConversationListViaApi({
            userId,
            accessToken: accessTokenInfo.token,
            cookies,
            proxy,
            deviceProfile,
            page: 0,
            size: 50
          });
          const candidates = Array.isArray(apiList?.conversations) ? apiList.conversations : [];
          const matched = candidates.find((item) => {
            const participantName = pickParticipantFromApi(item, userId);
            return matchConversation(
              { participant: participantName, adTitle: item.adTitle || "" },
              { participant, adTitle }
            );
          });
          if (matched) {
            resolvedConversationId = matched.id;
          }
        }

        if (!resolvedConversationId) {
          const error = new Error("CONVERSATION_ID_REQUIRED");
          error.code = "CONVERSATION_ID_REQUIRED";
          throw error;
        }

        await sendConversationMessageViaApi({
          userId,
          conversationId: resolvedConversationId,
          accessToken: accessTokenInfo.token,
          cookies,
          proxy,
          deviceProfile,
          text
        });

        const detail = await fetchConversationDetailViaApi({
          userId,
          conversationId: resolvedConversationId,
          accessToken: accessTokenInfo.token,
          cookies,
          proxy,
          deviceProfile
        });

        const participantName = pickParticipantFromApi(detail, userId);
        const apiMessages = (detail.messages || [])
          .map((message, index) => mapApiThreadMessage(message, index, participantName))
          .filter(Boolean);

        const normalizedMessages = apiMessages.map((message) => {
          const parsed = parseTimestamp(message.dateTime || message.timeLabel || "");
          return {
            ...message,
            date: parsed.date,
            time: parsed.time
          };
        });

        return {
          messages: normalizedMessages,
          adTitle: detail.adTitle || "",
          adImage: detail.adImage || "",
          participant: participantName || "",
          conversationId: resolvedConversationId,
          conversationUrl: buildConversationUrl(resolvedConversationId, conversationUrl)
        };
      } catch (error) {
        console.log(`[messageService] API send failed: ${error.message}`);
      }
    }
  }

  const proxyServer = buildProxyServer(proxy);
  const proxyUrl = buildProxyUrl(proxy);
  const needsProxyChain = Boolean(
    proxyUrl && ((proxy?.type || "").toLowerCase().startsWith("socks") || proxy?.username || proxy?.password)
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

  const profileDir = createTempProfileDir();
  const browser = await puppeteer.launch({
    headless: "new",
    args: launchArgs,
    userDataDir: profileDir,
    timeout: PUPPETEER_LAUNCH_TIMEOUT,
    protocolTimeout: PUPPETEER_PROTOCOL_TIMEOUT
  });

  try {
    const page = await browser.newPage();
    page.setDefaultTimeout(PUPPETEER_NAV_TIMEOUT);
    page.setDefaultNavigationTimeout(PUPPETEER_NAV_TIMEOUT);
    if (!anonymizedProxyUrl && (proxy?.username || proxy?.password)) {
      await page.authenticate({
        username: proxy.username || "",
        password: proxy.password || ""
      });
    }

    await page.setUserAgent(deviceProfile.userAgent);
    await page.setViewport(deviceProfile.viewport);
    await page.setExtraHTTPHeaders({ "Accept-Language": deviceProfile.locale });
    await page.emulateTimezone(deviceProfile.timezone);
    await page.evaluateOnNewDocument((platform) => {
      Object.defineProperty(navigator, "platform", {
        get: () => platform
      });
    }, deviceProfile.platform);

    await page.goto("https://www.kleinanzeigen.de/", { waitUntil: "domcontentloaded" });
    await humanPause();
    await performHumanLikePageActivity(page, { intensity: "light" });
    await page.setCookie(...cookies);
    await humanPause();
    let resolvedConversationId = conversationId;
    let resolvedConversationUrl = conversationUrl;
    if (!resolvedConversationId && !resolvedConversationUrl) {
      await page.goto(MESSAGE_LIST_URL, { waitUntil: "domcontentloaded" });
      await humanPause(120, 240);
      await performHumanLikePageActivity(page, { intensity: "medium" });
      const conversations = await parseConversationList(page);
      const matched = conversations.find((item) => matchConversation(item, { participant, adTitle }));
      if (matched) {
        resolvedConversationUrl = matched.href
          || (matched.conversationId ? buildConversationUrl(matched.conversationId) : "");
        resolvedConversationId = matched.conversationId || extractConversationId(matched.href);
      }
    }

    resolvedConversationUrl = buildConversationUrl(resolvedConversationId, resolvedConversationUrl);
    await page.goto(resolvedConversationUrl, { waitUntil: "domcontentloaded" });
    await humanPause(120, 240);
    await performHumanLikePageActivity(page, { intensity: "medium", force: true });

    if (await isAuthFailure(page)) {
      const error = new Error("AUTH_REQUIRED");
      error.code = "AUTH_REQUIRED";
      throw error;
    }

    const inputSelectors = [
      "textarea[name='message']",
      "textarea[name='text']",
      "textarea[name='body']",
      "textarea[placeholder*='Nachricht']",
      "textarea[placeholder*='message']",
      "textarea[aria-label*='Nachricht']",
      "textarea[aria-label*='message']",
      "[data-testid*='message-input'] textarea",
      "[data-testid*='message-input']",
      "[data-qa*='message-input'] textarea",
      "[data-qa*='message-input']",
      "[role='textbox'][contenteditable='true']",
      "[contenteditable='true'][aria-label*='Nachricht']",
      "[contenteditable='true'][aria-label*='message']",
      "[contenteditable='true']",
      "textarea"
    ];

    await waitForDynamicContent(page, inputSelectors, 20000);
    const inputResult = await findMessageInputHandle(page, inputSelectors);

    if (!inputResult?.handle) {
      const error = new Error("MESSAGE_INPUT_NOT_FOUND");
      error.code = "MESSAGE_INPUT_NOT_FOUND";
      throw error;
    }

    const inputHandle = inputResult.handle;
    await inputHandle.evaluate((input) => {
      input.scrollIntoView({ block: "center", inline: "center" });
      if (input.tagName === "TEXTAREA" || input.tagName === "INPUT") {
        input.value = "";
        input.dispatchEvent(new Event("input", { bubbles: true }));
      } else if (input.isContentEditable) {
        input.textContent = "";
      }
    });
    await inputHandle.focus();
    await typeTextHumanLike(inputHandle, text);

    const clicked = await inputHandle.evaluate((input) => {
      const root = input.closest("form")
        || input.closest("[data-testid*='composer']")
        || input.closest("[data-qa*='composer']")
        || input.closest("[class*='composer']")
        || input.closest("[class*='Composer']")
        || input.parentElement;
      if (!root) return false;

      const candidates = [
        "button[type='submit']",
        "button[aria-label*='Senden']",
        "button[aria-label*='send']",
        "button[aria-label*='Nachricht']",
        "button[title*='Senden']",
        "button[title*='send']",
        "[data-testid*='send']",
        "[data-qa*='send']",
        "[class*='send']",
        "button"
      ];
      for (const selector of candidates) {
        const buttons = Array.from(root.querySelectorAll(selector));
        for (const button of buttons) {
          const svg = button.querySelector("svg[data-title='sendOutline']")
            || button.querySelector("svg[aria-label*='send']")
            || button.querySelector("svg[data-icon*='send']");
          if (svg || /senden|send/i.test(button.textContent || "")) {
            button.click();
            return true;
          }
        }
      }
      return false;
    });

    if (!clicked) {
      await page.keyboard.press("Enter");
    }

    await humanPause(120, 180);
    const messages = await parseMessagesFromThread(page, "");
    const normalizedMessages = (messages || []).map((message) => {
      const parsed = parseTimestamp(message.dateTime || message.timeLabel || "");
      return {
        ...message,
        date: parsed.date,
        time: parsed.time
      };
    });
    return {
      messages: normalizedMessages,
      conversationId: resolvedConversationId,
      conversationUrl: resolvedConversationUrl
    };
  } finally {
    await browser.close();
    if (anonymizedProxyUrl) {
      await proxyChain.closeAnonymizedProxy(anonymizedProxyUrl, true);
    }
    fs.rmSync(profileDir, { recursive: true, force: true });
  }
};

const fetchAccountConversations = async ({ account, proxy, accountLabel, options = {} }) => {
  const deviceProfile = getDeviceProfile(account);
  const cookies = parseCookies(account.cookie).map(normalizeCookie);
  if (!cookies.length) return { conversations: [] };
  const maxConversations = Number.isFinite(options.maxConversations)
    ? Math.max(1, Number(options.maxConversations))
    : null;

  let userId = getUserIdFromCookies(cookies);
  let accessTokenInfo = null;
  if (!FORCE_WEB_MESSAGES) {
    if (!userId) {
      try {
        accessTokenInfo = await fetchMessageboxAccessToken({ cookies, proxy, deviceProfile });
        userId = getUserIdFromAccessToken(accessTokenInfo.token);
      } catch (error) {
        console.log(`[messageService] Access token lookup failed for ${accountLabel}: ${error.message}`);
      }
    }

    if (userId) {
      try {
        if (!accessTokenInfo) {
          accessTokenInfo = await fetchMessageboxAccessToken({ cookies, proxy, deviceProfile });
        }
      const size = maxConversations || 50;
      const apiData = await fetchConversationListViaApi({
        userId,
        accessToken: accessTokenInfo.token,
        cookies,
        proxy,
        deviceProfile,
        page: 0,
        size
      });

      let apiConversations = Array.isArray(apiData?.conversations)
        ? apiData.conversations
        : Array.isArray(apiData?.items)
          ? apiData.items
          : Array.isArray(apiData?.data)
            ? apiData.data
            : Array.isArray(apiData?.results)
              ? apiData.results
              : [];
      const totalFound = Number(apiData?._meta?.numFound || apiData?.total || apiData?.totalElements || apiConversations.length);
      if (!maxConversations && apiConversations.length < totalFound) {
        let page = 1;
        while (apiConversations.length < totalFound && page < 10) {
          const nextPage = await fetchConversationListViaApi({
            userId,
            accessToken: accessTokenInfo.token,
            cookies,
            proxy,
            deviceProfile,
            page,
            size
          });
          const nextConversations = Array.isArray(nextPage?.conversations)
            ? nextPage.conversations
            : Array.isArray(nextPage?.items)
              ? nextPage.items
              : Array.isArray(nextPage?.data)
                ? nextPage.data
                : Array.isArray(nextPage?.results)
                  ? nextPage.results
                  : [];
          if (!nextConversations.length) break;
          apiConversations = apiConversations.concat(nextConversations);
          page += 1;
        }
      }
      const uniqueApi = [];
      const seenApi = new Set();
      for (const conversation of apiConversations) {
        const id = conversation?.id || conversation?.conversationId || "";
        if (id && seenApi.has(id)) continue;
        if (id) seenApi.add(id);
        uniqueApi.push(conversation);
      }

      const limited = maxConversations ? uniqueApi.slice(0, maxConversations) : uniqueApi;
      let mapped = limited.map((conversation) => ({
        href: buildConversationUrl(conversation.id),
        conversationId: conversation.id,
        participant: pickParticipantFromApi(conversation, userId),
        adTitle: conversation.adTitle || "",
        adImage: pickAdImageFromApi(conversation),
        lastMessage: conversation.textShortTrimmed || "",
        timeText: conversation.receivedDate || "",
        unread: Boolean(conversation.unread),
        accountId: account.id,
        accountLabel
      }));

      mapped = mapped.map((item) => ({
        ...item,
        adImage: normalizeImageUrl(item.adImage)
      }));

      mapped = dedupeConversationList(mapped, account.id);

      if (mapped.some((item) => !isValidImageUrl(item.adImage))) {
        try {
          const webList = await fetchConversationListFromWeb({
            account,
            proxy,
            accountLabel,
            deviceProfile,
            cookies,
            maxConversations
          });
          const dedupedWeb = dedupeConversationList(webList, account.id);
          const webById = new Map();
          const webByFallback = new Map();
          for (const item of dedupedWeb) {
            const id = item.conversationId || extractConversationId(item.href || "");
            if (id && !webById.has(id)) webById.set(id, item);
            const fallbackKey = `${(item.participant || "").trim().toLowerCase()}|${(item.adTitle || "").trim().toLowerCase()}`;
            if (fallbackKey !== "|") webByFallback.set(fallbackKey, item);
          }
          mapped = mapped.map((item) => {
            const id = item.conversationId || extractConversationId(item.conversationUrl || item.href || "");
            const fallbackKey = `${(item.participant || "").trim().toLowerCase()}|${(item.adTitle || "").trim().toLowerCase()}`;
            const webItem = (id && webById.get(id)) || webByFallback.get(fallbackKey);
            if (!webItem) return item;
            const nextImage = isValidImageUrl(item.adImage)
              ? item.adImage
              : (isValidImageUrl(webItem.adImage) ? normalizeImageUrl(webItem.adImage) : "");
            return {
              ...item,
              adTitle: item.adTitle || webItem.adTitle || "",
              adImage: nextImage || item.adImage || "",
              conversationUrl: item.conversationUrl || webItem.href || item.conversationUrl
            };
          });
        } catch (error) {
          console.log(`[messageService] Web preview fetch failed for ${accountLabel}: ${error.message}`);
        }
      }

      const stillMissing = mapped.filter((item) => !isValidImageUrl(item.adImage));
      if (stillMissing.length) {
        try {
          const missingLimit = maxConversations || 25;
          const targets = stillMissing.slice(0, missingLimit);
          const headerPreviews = await fetchConversationHeaderPreviews({
            account,
            proxy,
            accountLabel,
            deviceProfile,
            cookies,
            conversations: targets
          });
          const byId = new Map();
          const byFallback = new Map();
          for (const preview of headerPreviews) {
            if (!isValidImageUrl(preview.adImage)) continue;
            if (preview.conversationId) {
              byId.set(String(preview.conversationId), preview.adImage);
            }
            const fallbackKey = `${normalizeMatch(preview.participant)}|${normalizeMatch(preview.adTitle)}`;
            if (fallbackKey !== "|") byFallback.set(fallbackKey, preview.adImage);
          }
          mapped = mapped.map((item) => {
            if (isValidImageUrl(item.adImage)) return item;
            const id = item.conversationId || extractConversationId(item.conversationUrl || item.href || "");
            const fallbackKey = `${normalizeMatch(item.participant)}|${normalizeMatch(item.adTitle)}`;
            const image = (id && byId.get(String(id))) || byFallback.get(fallbackKey);
            if (!image) return item;
            return { ...item, adImage: image };
          });
        } catch (error) {
          console.log(`[messageService] Header preview fallback failed for ${accountLabel}: ${error.message}`);
        }
      }

      console.log(`[messageService] API conversations: ${mapped.length} for ${accountLabel}`);
      if (!mapped.length) {
        throw new Error("MESSAGEBOX_API_EMPTY");
      }
      return { conversations: mapped };
      } catch (error) {
        console.log(`[messageService] API fetch failed for ${accountLabel}: ${error.message}`);
      }
    }
  }

  const proxyServer = buildProxyServer(proxy);
  const proxyUrl = buildProxyUrl(proxy);
  const needsProxyChain = Boolean(
    proxyUrl && ((proxy?.type || "").toLowerCase().startsWith("socks") || proxy?.username || proxy?.password)
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

  const profileDir = createTempProfileDir();
  const browser = await puppeteer.launch({
    headless: "new",
    args: launchArgs,
    userDataDir: profileDir,
    timeout: PUPPETEER_LAUNCH_TIMEOUT,
    protocolTimeout: PUPPETEER_PROTOCOL_TIMEOUT
  });

  try {
    const page = await browser.newPage();
    page.setDefaultTimeout(PUPPETEER_NAV_TIMEOUT);
    page.setDefaultNavigationTimeout(PUPPETEER_NAV_TIMEOUT);
    if (!anonymizedProxyUrl && (proxy?.username || proxy?.password)) {
      await page.authenticate({
        username: proxy.username || "",
        password: proxy.password || ""
      });
    }

    await page.setUserAgent(deviceProfile.userAgent);
    await page.setViewport(deviceProfile.viewport);
    await page.setExtraHTTPHeaders({ "Accept-Language": deviceProfile.locale });
    await page.emulateTimezone(deviceProfile.timezone);
    await page.evaluateOnNewDocument((platform) => {
      Object.defineProperty(navigator, "platform", {
        get: () => platform
      });
    }, deviceProfile.platform);

    await page.goto("https://www.kleinanzeigen.de/", { waitUntil: "domcontentloaded" });
    await humanPause();
    await performHumanLikePageActivity(page, { intensity: "light" });
    await page.setCookie(...cookies);
    await humanPause();
    await page.goto(MESSAGE_LIST_URL, { waitUntil: "domcontentloaded" });
    await humanPause(180, 360);
    await performHumanLikePageActivity(page, { intensity: "medium" });

    if (await isAuthFailure(page)) {
      const error = new Error("AUTH_REQUIRED");
      error.code = "AUTH_REQUIRED";
      throw error;
    }

    const conversations = (await parseConversationList(page)).map((conversation) => {
      const href = conversation.href
        || (conversation.conversationId ? buildConversationUrl(conversation.conversationId) : "");
      return { ...conversation, href };
    });
    const dedupedConversations = dedupeConversationList(conversations, account.id);
    console.log(`[messageService] Found ${dedupedConversations.length} conversations for ${accountLabel}`);
    if (DEBUG_MESSAGES && dedupedConversations.length === 0) {
      await dumpMessageListDebug(page, accountLabel);
    }
    const conversationLimit = maxConversations || dedupedConversations.length;

    const parsed = [];
    for (const conversation of dedupedConversations.slice(0, conversationLimit)) {
      const href = conversation.href || "";
      const conversationId = conversation.conversationId || extractConversationId(href);
      if (!href) continue;

      await page.goto(href, { waitUntil: "domcontentloaded" });
      await humanPause(120, 240);
      if (randomChance(0.5)) {
        await performHumanLikePageActivity(page, { intensity: "light" });
      }

      const messages = await parseMessagesFromThread(page, conversation.participant);
      const meta = await parseConversationMetaFromThread(page);
      console.log(`[messageService] Thread ${conversationId}: ${messages.length} messages, adTitle="${meta.adTitle || conversation.adTitle}"`);
      parsed.push({
        ...conversation,
        participant: conversation.participant || meta.participant || "",
        adTitle: conversation.adTitle || meta.adTitle,
        adImage: conversation.adImage || meta.adImage,
        conversationId,
        messages,
        accountId: account.id,
        accountLabel
      });
    }

    return { conversations: parsed };
  } finally {
    await browser.close();
    if (anonymizedProxyUrl) {
      await proxyChain.closeAnonymizedProxy(anonymizedProxyUrl, true);
    }
    fs.rmSync(profileDir, { recursive: true, force: true });
  }
};

const fetchMessages = async ({ accounts, proxies, options = {} }) => {
  const results = [];
  for (const account of accounts) {
    if (!account.cookie) continue;
    const proxy = account.proxyId
      ? proxies.find((item) => item.id === account.proxyId)
      : null;
    const profileName = account.profileName || account.username || "Аккаунт";
    const profileEmail = account.profileEmail || "";
    const accountLabel = profileEmail ? `${profileName} (${profileEmail})` : profileName;
    const data = await fetchAccountConversations({ account, proxy, accountLabel, options });
    results.push(...data.conversations);
  }
  return results;
};

const fetchThreadMessages = async ({
  account,
  proxy,
  conversationId,
  conversationUrl,
  participant,
  adTitle
}) => {
  if (!conversationId && !conversationUrl && !participant && !adTitle) {
    const error = new Error("CONVERSATION_ID_REQUIRED");
    error.code = "CONVERSATION_ID_REQUIRED";
    throw error;
  }

  const deviceProfile = getDeviceProfile(account);
  const cookies = parseCookies(account.cookie).map(normalizeCookie);
  if (!cookies.length) {
    const error = new Error("AUTH_REQUIRED");
    error.code = "AUTH_REQUIRED";
    throw error;
  }

  let userId = getUserIdFromCookies(cookies);
  let accessTokenInfo = null;
  if (!FORCE_WEB_MESSAGES) {
    if (!userId) {
      try {
        accessTokenInfo = await fetchMessageboxAccessToken({ cookies, proxy, deviceProfile });
        userId = getUserIdFromAccessToken(accessTokenInfo.token);
      } catch (error) {
        console.log(`[messageService] Access token lookup failed: ${error.message}`);
      }
    }
    if (userId) {
      try {
        if (!accessTokenInfo) {
          accessTokenInfo = await fetchMessageboxAccessToken({ cookies, proxy, deviceProfile });
        }
        let resolvedConversationId = conversationId || extractConversationId(conversationUrl);

      if (!resolvedConversationId) {
        const apiList = await fetchConversationListViaApi({
          userId,
          accessToken: accessTokenInfo.token,
          cookies,
          proxy,
          deviceProfile,
          page: 0,
          size: 50
        });
        const candidates = Array.isArray(apiList?.conversations) ? apiList.conversations : [];
        const matched = candidates.find((item) => {
          const participantName = pickParticipantFromApi(item, userId);
          return matchConversation(
            { participant: participantName, adTitle: item.adTitle || "" },
            { participant, adTitle }
          );
        });
        if (matched) {
          resolvedConversationId = matched.id;
        }
      }

      if (!resolvedConversationId) {
        const error = new Error("CONVERSATION_ID_REQUIRED");
        error.code = "CONVERSATION_ID_REQUIRED";
        throw error;
      }

      const detail = await fetchConversationDetailViaApi({
        userId,
        conversationId: resolvedConversationId,
        accessToken: accessTokenInfo.token,
        cookies,
        proxy,
        deviceProfile
      });

      const participantName = pickParticipantFromApi(detail, userId);
      const apiMessages = (detail.messages || [])
        .map((message, index) => mapApiThreadMessage(message, index, participantName))
        .filter(Boolean);

      const normalizedMessages = apiMessages.map((message) => {
        const parsed = parseTimestamp(message.dateTime || message.timeLabel || "");
        return {
          ...message,
          date: parsed.date,
          time: parsed.time
        };
      });

      return {
        messages: normalizedMessages,
        adTitle: detail.adTitle || "",
        adImage: detail.adImage || "",
        participant: participantName || "",
        conversationId: resolvedConversationId,
        conversationUrl: buildConversationUrl(resolvedConversationId, conversationUrl)
      };
      } catch (error) {
        console.log(`[messageService] API thread fetch failed: ${error.message}`);
      }
    }
  }

  const proxyServer = buildProxyServer(proxy);
  const proxyUrl = buildProxyUrl(proxy);
  const needsProxyChain = Boolean(
    proxyUrl && ((proxy?.type || "").toLowerCase().startsWith("socks") || proxy?.username || proxy?.password)
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

  const profileDir = createTempProfileDir();
  const browser = await puppeteer.launch({
    headless: "new",
    args: launchArgs,
    userDataDir: profileDir,
    timeout: PUPPETEER_LAUNCH_TIMEOUT,
    protocolTimeout: PUPPETEER_PROTOCOL_TIMEOUT
  });

  try {
    const page = await browser.newPage();
    page.setDefaultTimeout(PUPPETEER_NAV_TIMEOUT);
    page.setDefaultNavigationTimeout(PUPPETEER_NAV_TIMEOUT);
    if (!anonymizedProxyUrl && (proxy?.username || proxy?.password)) {
      await page.authenticate({
        username: proxy.username || "",
        password: proxy.password || ""
      });
    }

    await page.setUserAgent(deviceProfile.userAgent);
    await page.setViewport(deviceProfile.viewport);
    await page.setExtraHTTPHeaders({ "Accept-Language": deviceProfile.locale });
    await page.emulateTimezone(deviceProfile.timezone);
    await page.evaluateOnNewDocument((platform) => {
      Object.defineProperty(navigator, "platform", {
        get: () => platform
      });
    }, deviceProfile.platform);

    await page.goto("https://www.kleinanzeigen.de/", { waitUntil: "domcontentloaded" });
    await humanPause();
    await performHumanLikePageActivity(page, { intensity: "light" });
    await page.setCookie(...cookies);
    await humanPause();
    let resolvedConversationId = conversationId;
    let resolvedConversationUrl = conversationUrl;
    if (!resolvedConversationId && !resolvedConversationUrl) {
      await page.goto(MESSAGE_LIST_URL, { waitUntil: "domcontentloaded" });
      await humanPause(120, 240);
      await performHumanLikePageActivity(page, { intensity: "medium" });
      const conversations = await parseConversationList(page);
      const matched = conversations.find((item) => matchConversation(item, { participant, adTitle }));
      if (matched) {
        resolvedConversationUrl = matched.href
          || (matched.conversationId ? buildConversationUrl(matched.conversationId) : "");
        resolvedConversationId = matched.conversationId || extractConversationId(matched.href);
      }
    }

    resolvedConversationUrl = buildConversationUrl(resolvedConversationId, resolvedConversationUrl);
    await page.goto(resolvedConversationUrl, { waitUntil: "domcontentloaded" });
    await humanPause(120, 240);
    await performHumanLikePageActivity(page, { intensity: "medium", force: true });

    if (await isAuthFailure(page)) {
      const error = new Error("AUTH_REQUIRED");
      error.code = "AUTH_REQUIRED";
      throw error;
    }

    const meta = await parseConversationMetaFromThread(page);
    const messages = await parseMessagesFromThread(page, meta.participant || "");
    console.log(`[messageService] fetchThread: ${messages.length} messages, adTitle="${meta.adTitle}", participant="${meta.participant}"`);
    const normalizedMessages = (messages || []).map((message) => {
      const parsed = parseTimestamp(message.dateTime || message.timeLabel || "");
      return {
        ...message,
        date: parsed.date,
        time: parsed.time
      };
    });

    return {
      messages: normalizedMessages,
      adTitle: meta.adTitle,
      adImage: meta.adImage,
      participant: meta.participant,
      conversationId: resolvedConversationId,
      conversationUrl: resolvedConversationUrl
    };
  } finally {
    await browser.close();
    if (anonymizedProxyUrl) {
      await proxyChain.closeAnonymizedProxy(anonymizedProxyUrl, true);
    }
    fs.rmSync(profileDir, { recursive: true, force: true });
  }
};

const fetchConversationSnapshotViaApi = async ({
  account,
  proxy,
  conversationId,
  conversationUrl,
  requestTimeoutMs = 20000
}) => {
  const deviceProfile = getDeviceProfile(account);
  const cookies = parseCookies(account.cookie).map(normalizeCookie);
  if (!cookies.length) {
    const error = new Error("AUTH_REQUIRED");
    error.code = "AUTH_REQUIRED";
    throw error;
  }

  const resolvedConversationId = conversationId || extractConversationId(conversationUrl);
  if (!resolvedConversationId) {
    const error = new Error("CONVERSATION_ID_REQUIRED");
    error.code = "CONVERSATION_ID_REQUIRED";
    throw error;
  }

  const normalizedTimeout = Number.isFinite(Number(requestTimeoutMs)) && Number(requestTimeoutMs) > 0
    ? Math.max(2500, Number(requestTimeoutMs))
    : 20000;

  let userId = getUserIdFromCookies(cookies);
  let accessTokenInfo = null;
  if (!userId) {
    accessTokenInfo = await fetchMessageboxAccessToken({
      cookies,
      proxy,
      deviceProfile,
      timeoutMs: normalizedTimeout
    });
    userId = getUserIdFromAccessToken(accessTokenInfo.token);
  }

  if (!userId) {
    const error = new Error("AUTH_REQUIRED");
    error.code = "AUTH_REQUIRED";
    throw error;
  }

  if (!accessTokenInfo) {
    accessTokenInfo = await fetchMessageboxAccessToken({
      cookies,
      proxy,
      deviceProfile,
      timeoutMs: normalizedTimeout
    });
  }

  const detail = await fetchConversationDetailViaApi({
    userId,
    conversationId: resolvedConversationId,
    accessToken: accessTokenInfo.token,
    cookies,
    proxy,
    deviceProfile,
    timeoutMs: normalizedTimeout
  });

  const participantName = pickParticipantFromApi(detail, userId);
  const apiMessages = (detail.messages || [])
    .map((message, index) => mapApiThreadMessage(message, index, participantName))
    .filter(Boolean);
  const normalizedMessages = apiMessages.map((message) => {
    const parsed = parseTimestamp(message.dateTime || message.timeLabel || "");
    return {
      ...message,
      date: parsed.date,
      time: parsed.time
    };
  });

  return {
    messages: normalizedMessages,
    adTitle: detail.adTitle || "",
    adImage: detail.adImage || "",
    participant: participantName || "",
    conversationId: resolvedConversationId,
    conversationUrl: buildConversationUrl(resolvedConversationId, conversationUrl)
  };
};

const countOutgoingAttachmentUnits = (snapshot) => {
  const messages = Array.isArray(snapshot?.messages) ? snapshot.messages : [];
  return messages.reduce((sum, message) => {
    const direction = String(message?.direction || "").toLowerCase();
    if (direction !== "outgoing") return sum;
    const attachmentBuckets = [
      message?.attachments,
      message?.images,
      message?.imageUrls,
      message?.media,
      message?.pictures
    ];
    const attachmentCount = attachmentBuckets.reduce((count, bucket) => {
      if (!Array.isArray(bucket)) return count;
      return count + bucket.length;
    }, 0);
    return sum + attachmentCount;
  }, 0);
};

const hasOutgoingTextInSnapshot = (snapshot, text) => {
  const expected = normalizeMatch(text || "");
  if (!expected) return false;
  const messages = Array.isArray(snapshot?.messages) ? snapshot.messages : [];
  return messages.some((message) => {
    const direction = String(message?.direction || "").toLowerCase();
    if (direction !== "outgoing") return false;
    const current = normalizeMatch(message?.text || "");
    return Boolean(current && (current === expected || current.includes(expected)));
  });
};

const countOutgoingTextMatches = (snapshot, text) => {
  const expected = normalizeMatch(text || "");
  if (!expected) return 0;
  const messages = Array.isArray(snapshot?.messages) ? snapshot.messages : [];
  return messages.reduce((count, message) => {
    const direction = String(message?.direction || "").toLowerCase();
    if (direction !== "outgoing") return count;
    const current = normalizeMatch(message?.text || "");
    if (!current) return count;
    if (current === expected || current.includes(expected)) {
      return count + 1;
    }
    return count;
  }, 0);
};

const hasOfferActionsInSnapshot = (snapshot) => {
  const messages = Array.isArray(snapshot?.messages) ? snapshot.messages : [];
  return messages.some((message) => {
    if (!isPaymentAndShippingMessage(message)) return false;
    const actions = Array.isArray(message?.actions) ? message.actions : [];
    if (!actions.length) return false;
    return actions.some((action) => {
      const text = normalizeMatch(
        action?.ctaText
        || action?.label
        || action?.title
        || action?.text
        || action?.actionType
      );
      return text.includes("ablehnen") || text.includes("gegenangebot") || text.includes("akzeptieren");
    });
  });
};

const fetchConversationSnapshotViaApiWithRetry = async ({
  account,
  proxy,
  conversationId,
  conversationUrl,
  attempts = 3,
  delayMs = 900,
  requestTimeoutMs = 20000
}) => {
  let lastError = null;
  for (let attempt = 0; attempt < Math.max(1, attempts); attempt += 1) {
    try {
      return await fetchConversationSnapshotViaApi({
        account,
        proxy,
        conversationId,
        conversationUrl,
        requestTimeoutMs
      });
    } catch (error) {
      lastError = error;
      if (attempt < attempts - 1) {
        await sleep(delayMs);
      }
    }
  }
  throw lastError || new Error("SNAPSHOT_FETCH_FAILED");
};

const declineConversationOffer = async ({
  account,
  proxy,
  conversationId,
  conversationUrl,
  abortSignal = null,
  hardDeadlineMs = null
}) => {
  const actionStartedAt = Date.now();
  const normalizedHardDeadlineMs = Number(hardDeadlineMs);
  const actionDeadlineMs = Math.max(
    25000,
    Math.min(
      MESSAGE_ACTION_DEADLINE_MS,
      Number.isFinite(normalizedHardDeadlineMs) && normalizedHardDeadlineMs > 5000
        ? normalizedHardDeadlineMs - 2500
        : MESSAGE_ACTION_DEADLINE_MS
    )
  );
  const snapshotTimeoutMs = Math.max(1500, Math.min(3000, FAST_SNAPSHOT_TIMEOUT_MS));
  const remainingMs = () => actionDeadlineMs - (Date.now() - actionStartedAt);
  const ensureDeadline = (context = "") => {
    if (Date.now() - actionStartedAt >= actionDeadlineMs) {
      throw buildActionTimeoutError(context || "offer-decline");
    }
  };
  const hasTimeLeft = (reserveMs = 0) => Date.now() - actionStartedAt < (actionDeadlineMs - Math.max(0, reserveMs));
  const throwIfAborted = (context = "") => {
    if (abortSignal?.aborted) {
      throw buildActionTimeoutError(context || "offer-decline-aborted");
    }
  };
  const getStepTimeout = (desiredMs, minMs = 1200, reserveMs = 2500, context = "") => {
    const normalizedMin = Math.max(500, Number(minMs) || 500);
    const desired = Math.max(normalizedMin, Number(desiredMs) || normalizedMin);
    const remainingBudget = remainingMs() - Math.max(0, reserveMs);
    if (remainingBudget <= normalizedMin) {
      throw buildActionTimeoutError(context || "offer-decline-time-budget");
    }
    return Math.max(normalizedMin, Math.min(desired, remainingBudget));
  };
  if (!conversationId && !conversationUrl) {
    const error = new Error("CONVERSATION_ID_REQUIRED");
    error.code = "CONVERSATION_ID_REQUIRED";
    throw error;
  }

  const deviceProfile = getDeviceProfile(account);
  const cookies = parseCookies(account.cookie).map(normalizeCookie);
  if (!cookies.length) {
    const error = new Error("AUTH_REQUIRED");
    error.code = "AUTH_REQUIRED";
    throw error;
  }

  const resolvedConversationId = conversationId || extractConversationId(conversationUrl);
  if (!resolvedConversationId) {
    const error = new Error("CONVERSATION_ID_REQUIRED");
    error.code = "CONVERSATION_ID_REQUIRED";
    throw error;
  }
  const resolvedConversationUrl = buildConversationUrl(resolvedConversationId, conversationUrl);
  const beforeSnapshot = null;
  const fetchAfterSnapshotIfAvailable = async () => {
    if (!hasTimeLeft(2200)) return null;
    const timeoutForSnapshot = Math.max(1200, Math.min(snapshotTimeoutMs, Math.max(1400, remainingMs() - 1200)));
    if (timeoutForSnapshot < 1200) return null;
    return fetchConversationSnapshotViaApiWithRetry({
      account,
      proxy,
      conversationId: resolvedConversationId,
      conversationUrl: resolvedConversationUrl,
      attempts: 1,
      delayMs: FAST_SNAPSHOT_DELAY_MS,
      requestTimeoutMs: timeoutForSnapshot
    }).catch(() => null);
  };

  const proxyServer = buildProxyServer(proxy);
  const proxyUrl = buildProxyUrl(proxy);
  const needsProxyChain = Boolean(
    proxyUrl && ((proxy?.type || "").toLowerCase().startsWith("socks") || proxy?.username || proxy?.password)
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

  const profileDir = createTempProfileDir();
  const browser = await puppeteer.launch({
    headless: "new",
    args: launchArgs,
    userDataDir: profileDir,
    timeout: PUPPETEER_LAUNCH_TIMEOUT,
    protocolTimeout: PUPPETEER_PROTOCOL_TIMEOUT
  });

  let abortListener = null;
  if (abortSignal) {
    abortListener = () => {
      safeCloseBrowser(browser).catch(() => {});
    };
    if (abortSignal.aborted) {
      abortListener();
    } else {
      abortSignal.addEventListener("abort", abortListener, { once: true });
    }
  }

  try {
    throwIfAborted("offer-decline-before-new-page");
    const page = await browser.newPage();
    const localTimeout = getStepTimeout(
      Math.min(PUPPETEER_NAV_TIMEOUT, 18000),
      4500,
      9000,
      "offer-decline-navigation-budget"
    );
    page.setDefaultTimeout(localTimeout);
    page.setDefaultNavigationTimeout(localTimeout);
    if (!anonymizedProxyUrl && (proxy?.username || proxy?.password)) {
      await page.authenticate({
        username: proxy.username || "",
        password: proxy.password || ""
      });
    }

    await page.setUserAgent(deviceProfile.userAgent);
    await page.setViewport(deviceProfile.viewport);
    await page.setExtraHTTPHeaders({ "Accept-Language": deviceProfile.locale });
    await page.emulateTimezone(deviceProfile.timezone);
    await page.evaluateOnNewDocument((platform) => {
      Object.defineProperty(navigator, "platform", {
        get: () => platform
      });
    }, deviceProfile.platform);

    ensureDeadline("offer-decline-before-set-cookie");
    throwIfAborted("offer-decline-before-set-cookie");
    await page.setCookie(...cookies);
    await humanPause(35, 90);
    const conversationGotoTimeout = getStepTimeout(
      localTimeout,
      4500,
      6000,
      "offer-decline-conversation-goto"
    );
    await gotoWithProxyHandling(
      page,
      resolvedConversationUrl,
      { waitUntil: "domcontentloaded", timeout: conversationGotoTimeout },
      "DECLINE_CONVERSATION_GOTO"
    );
    throwIfAborted("offer-decline-after-conversation-goto");
    if (await isAuthFailure(page)) {
      const error = new Error("AUTH_REQUIRED");
      error.code = "AUTH_REQUIRED";
      throw error;
    }
    await acceptCookieModal(page, { timeout: MESSAGE_CONSENT_TIMEOUT_MS }).catch(() => {});
    if (isGdprPage(page.url())) {
      await acceptGdprConsent(page, { timeout: MESSAGE_GDPR_TIMEOUT_MS }).catch(() => {});
    }
    await humanPause(30, 70);
    await dismissConversationBlockingModals(page, { maxPasses: 1 }).catch(() => {});

    if (await isAuthFailure(page)) {
      const error = new Error("AUTH_REQUIRED");
      error.code = "AUTH_REQUIRED";
      throw error;
    }

    const paymentActionSelectors = [
      "section.PaymentMessageBox",
      ".PaymentMessageBox",
      "[data-testid='payment-message-header-extended']"
    ];
    const declineLabels = [
      "Anfrage ablehnen",
      "Angebot ablehnen",
      "Anfrage jetzt ablehnen",
      "Anfrage wirklich ablehnen",
      "Ja, ablehnen",
      "Ja ablehnen",
      "Jetzt ablehnen",
      "Ablehnen bestätigen",
      "Ablehnen"
    ];
    const modalContinueLabels = [
      "Weiter",
      "Fortfahren",
      "Alles klar",
      "Verstanden",
      "Ok",
      "Okay",
      "Schliessen",
      "Schließen",
      "Weiterlesen"
    ];

    const hasVisibleButtonsOutsidePayment = async (labels) => {
      const contexts = getPageContexts(page);
      for (const context of contexts) {
        const hasVisible = await evaluateInContext(context, (needles, excludeSelectors) => {
          const normalize = (value) => (value || "").replace(/\s+/g, " ").trim().toLowerCase();
          const preparedNeedles = (Array.isArray(needles) ? needles : [])
            .map((value) => normalize(value))
            .filter(Boolean);
          if (!preparedNeedles.length) return false;
          const isVisible = (node) => {
            if (!node) return false;
            const style = window.getComputedStyle(node);
            if (!style) return false;
            if (style.display === "none" || style.visibility === "hidden" || style.opacity === "0" || style.pointerEvents === "none") {
              return false;
            }
            const rect = node.getBoundingClientRect();
            return rect.width > 0 && rect.height > 0;
          };
          const getComposedParent = (node) => {
            if (!node) return null;
            if (node.parentElement) return node.parentElement;
            const root = typeof node.getRootNode === "function" ? node.getRootNode() : null;
            return root && root.host ? root.host : null;
          };
          const isInsideSelectors = (node, selectors) => {
            if (!node || !Array.isArray(selectors) || !selectors.length) return false;
            return selectors.some((selector) => {
              if (!selector) return false;
              let current = node;
              while (current) {
                try {
                  if (typeof current.matches === "function" && current.matches(selector)) return true;
                } catch (error) {
                  return false;
                }
                current = getComposedParent(current);
              }
              return false;
            });
          };

          const collectRoots = () => {
            const roots = [document];
            const queue = [document];
            const seen = new Set([document]);
            while (queue.length) {
              const root = queue.shift();
              let nodes = [];
              try {
                nodes = Array.from(root.querySelectorAll("*"));
              } catch (error) {
                nodes = [];
              }
              for (const node of nodes) {
                const shadowRoot = node?.shadowRoot;
                if (!shadowRoot || seen.has(shadowRoot)) continue;
                seen.add(shadowRoot);
                roots.push(shadowRoot);
                queue.push(shadowRoot);
              }
            }
            return roots;
          };

          const nodes = collectRoots().flatMap((root) => {
            try {
              return Array.from(root.querySelectorAll("button, [role='button'], a"));
            } catch (error) {
              return [];
            }
          });
          return nodes.some((node) => {
            if (!isVisible(node)) return false;
            if (isInsideSelectors(node, excludeSelectors)) return false;
            const text = normalize(
              node.textContent
              || node.getAttribute("aria-label")
              || node.getAttribute("title")
              || node.getAttribute("value")
            );
            if (!text) return false;
            return preparedNeedles.some((label) => text === label || text.includes(label));
          });
        }, labels, paymentActionSelectors);
        if (hasVisible) return true;
      }
      return false;
    };

    const hasInlineDeclineButtons = async () => {
      const contexts = getPageContexts(page);
      for (const context of contexts) {
        const hasInline = await evaluateInContext(context, (selectors) => {
          const normalize = (value) => (value || "").replace(/\s+/g, " ").trim().toLowerCase();
          const isVisible = (node) => {
            if (!node) return false;
            const style = window.getComputedStyle(node);
            if (!style) return false;
            if (style.display === "none" || style.visibility === "hidden" || style.opacity === "0" || style.pointerEvents === "none") {
              return false;
            }
            const rect = node.getBoundingClientRect();
            return rect.width > 0 && rect.height > 0;
          };
          return selectors.some((selector) => {
            let roots = [];
            try {
              roots = Array.from(document.querySelectorAll(selector));
            } catch (error) {
              roots = [];
            }
            return roots.some((root) => {
              const buttons = Array.from(root.querySelectorAll("button, [role='button'], a"));
              return buttons.some((button) => {
                if (!isVisible(button)) return false;
                const text = normalize(button.textContent || button.getAttribute("aria-label") || button.getAttribute("title"));
                return text.includes("anfrage ablehnen") || text.includes("angebot ablehnen");
              });
            });
          });
        }, paymentActionSelectors);
        if (hasInline) return true;
      }
      return false;
    };

    await page.waitForFunction((selectors) => {
      const normalize = (value) => (value || "").replace(/\s+/g, " ").trim().toLowerCase();
      const isVisible = (node) => {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        if (!style) return false;
        if (style.display === "none" || style.visibility === "hidden" || style.opacity === "0" || style.pointerEvents === "none") {
          return false;
        }
        const rect = node.getBoundingClientRect();
        return rect.width > 0 && rect.height > 0;
      };
      return selectors.some((selector) => {
        let roots = [];
        try {
          roots = Array.from(document.querySelectorAll(selector));
        } catch (error) {
          roots = [];
        }
        return roots.some((root) => {
          const buttons = Array.from(root.querySelectorAll("button, [role='button'], a"));
          return buttons.some((button) => {
            if (!isVisible(button)) return false;
            const text = normalize(button.textContent || button.getAttribute("aria-label") || button.getAttribute("title"));
            return text.includes("anfrage ablehnen") || text.includes("angebot ablehnen");
          });
        });
      });
    }, { timeout: 1000 }, paymentActionSelectors).catch(() => {});

    // There can be several onboarding/payment dialogs before the actual decline action appears.
    for (let pass = 0; pass < 3 && hasTimeLeft(5000); pass += 1) {
      const progressed = await clickVisibleButtonByText(page, modalContinueLabels, {
        timeout: 850,
        preferDialog: true,
        preferTopLayer: true
      });
      if (!progressed) break;
      await humanPause(90, 150);
      await dismissConversationBlockingModals(page, { maxPasses: 2 }).catch(() => {});
    }

    const clicked = await clickVisibleButtonByText(page, declineLabels, {
      timeout: 4200,
      preferDialog: true,
      preferTopLayer: true
    });
    let declineClicksPerformed = Boolean(clicked);
    if (!clicked) {
      const fallbackClicked = await clickVisibleButtonByText(page, declineLabels, {
        timeout: 2200,
        requireInSelectors: paymentActionSelectors
      });
      if (!fallbackClicked) {
        const clickedAfterContinue = await clickVisibleButtonByText(page, modalContinueLabels, {
          timeout: 1200,
          preferDialog: true,
          preferTopLayer: true
        });
        if (clickedAfterContinue) {
          await humanPause(100, 170);
        }
        const finalTry = await clickVisibleButtonByText(page, declineLabels, {
          timeout: 1800,
          preferDialog: true,
          preferTopLayer: true
        });
        if (!finalTry) {
          const maybeAlreadyDeclined = await fetchAfterSnapshotIfAvailable();
          if (maybeAlreadyDeclined && !hasOfferActionsInSnapshot(maybeAlreadyDeclined)) {
            return maybeAlreadyDeclined;
          }
          throw new Error("DECLINE_BUTTON_NOT_FOUND");
        }
        declineClicksPerformed = true;
      } else {
        declineClicksPerformed = true;
      }
    }
    await humanPause(100, 170);
    await performHumanLikePageActivity(page, { intensity: "light" });

    const declineFlowState = async () => {
      const hasInline = await hasInlineDeclineButtons();
      const hasModalDecline = await hasVisibleButtonsOutsidePayment(declineLabels);
      const hasModalContinue = await hasVisibleButtonsOutsidePayment(modalContinueLabels);
      return {
        hasInline,
        hasModalDecline,
        hasModalContinue,
        visible: hasInline || hasModalDecline || hasModalContinue
      };
    };

    let confirmed = false;
    for (let attempt = 0; attempt < 5 && hasTimeLeft(5000); attempt += 1) {
      const state = await declineFlowState();
      if (!state.visible) {
        confirmed = true;
        break;
      }

      let acted = false;
      const clickedContinue = await clickVisibleButtonByText(page, modalContinueLabels, {
        timeout: state.hasModalContinue ? 1300 : 650,
        preferDialog: true,
        preferTopLayer: true,
        excludeInSelectors: paymentActionSelectors
      });
      if (clickedContinue) {
        acted = true;
      }

      const clickedDecline = await clickVisibleButtonByText(page, declineLabels, {
        timeout: state.hasModalDecline ? 1700 : 750,
        preferDialog: true,
        preferTopLayer: true,
        excludeInSelectors: paymentActionSelectors
      });
      if (clickedDecline) {
        acted = true;
        declineClicksPerformed = true;
      }

      if (state.hasInline) {
        const clickedInline = await clickVisibleButtonByText(page, ["Anfrage ablehnen", "Angebot ablehnen"], {
          timeout: 900,
          requireInSelectors: paymentActionSelectors
        });
        if (clickedInline) {
          acted = true;
          declineClicksPerformed = true;
        }
      }

      if (!acted) {
        break;
      }
      await humanPause(100, 170);
      await dismissConversationBlockingModals(page, { maxPasses: 2 }).catch(() => {});
    }

    if (!confirmed) {
      const state = await declineFlowState();
      confirmed = !state.visible;
    }

    if (!confirmed && declineClicksPerformed && hasTimeLeft(4000)) {
      for (let retry = 0; retry < 2 && hasTimeLeft(2500); retry += 1) {
        let acted = false;
        const clickedContinue = await clickVisibleButtonByText(page, modalContinueLabels, {
          timeout: 900,
          preferDialog: true,
          preferTopLayer: true,
          excludeInSelectors: paymentActionSelectors
        });
        if (clickedContinue) acted = true;
        const clickedDecline = await clickVisibleButtonByText(page, declineLabels, {
          timeout: 1000,
          preferDialog: true,
          preferTopLayer: true,
          excludeInSelectors: paymentActionSelectors
        });
        if (clickedDecline) {
          acted = true;
          declineClicksPerformed = true;
        }
        if (!acted) break;
        await humanPause(90, 150);
      }
      const state = await declineFlowState();
      confirmed = !state.visible;
    }

    if (!confirmed && !declineClicksPerformed) {
      const maybeAlreadyDeclined = await fetchAfterSnapshotIfAvailable();
      if (maybeAlreadyDeclined && !hasOfferActionsInSnapshot(maybeAlreadyDeclined)) {
        return maybeAlreadyDeclined;
      }
      throw new Error("DECLINE_BUTTON_NOT_FOUND");
    }
    if (!confirmed && hasTimeLeft(1000)) {
      const state = await declineFlowState();
      if (state.visible) {
        throw new Error("DECLINE_NOT_APPLIED");
      }
    }

    ensureDeadline("offer-decline-after-clicks");
    throwIfAborted("offer-decline-after-clicks");
  } finally {
    if (abortSignal && abortListener) {
      abortSignal.removeEventListener("abort", abortListener);
    }
    await safeCloseBrowser(browser);
    if (anonymizedProxyUrl) {
      await proxyChain.closeAnonymizedProxy(anonymizedProxyUrl, true);
    }
    fs.rmSync(profileDir, { recursive: true, force: true });
  }

  // Prefer the messagebox API for a fresh thread snapshot, but keep this fast:
  // callers should not block on slow messagebox endpoints.
  if (Date.now() - actionStartedAt > actionDeadlineMs) {
    return {
      messages: [],
      conversationId: resolvedConversationId,
      conversationUrl: resolvedConversationUrl
    };
  }
  try {
    let afterSnapshot = await fetchConversationSnapshotViaApiWithRetry({
      account,
      proxy,
      conversationId: resolvedConversationId,
      conversationUrl: resolvedConversationUrl,
      attempts: FAST_SNAPSHOT_ATTEMPTS,
      delayMs: FAST_SNAPSHOT_DELAY_MS,
      requestTimeoutMs: snapshotTimeoutMs
    });
    const hadOfferActionsBefore = Boolean(beforeSnapshot && hasOfferActionsInSnapshot(beforeSnapshot));
    if (hadOfferActionsBefore && hasOfferActionsInSnapshot(afterSnapshot)) {
      await sleep(350);
      const retrySnapshot = await fetchConversationSnapshotViaApiWithRetry({
        account,
        proxy,
        conversationId: resolvedConversationId,
        conversationUrl: resolvedConversationUrl,
        attempts: 1,
        delayMs: FAST_SNAPSHOT_DELAY_MS,
        requestTimeoutMs: snapshotTimeoutMs
      }).catch(() => null);
      if (retrySnapshot) {
        afterSnapshot = retrySnapshot;
      }
    }
    return afterSnapshot;
  } catch (error) {
    return {
      messages: [],
      conversationId: resolvedConversationId,
      conversationUrl: resolvedConversationUrl
    };
  }
};

const sendConversationMedia = async ({
  account,
  proxy,
  conversationId,
  conversationUrl,
  text,
  files,
  abortSignal = null,
  hardDeadlineMs = null
}) => {
  const actionStartedAt = Date.now();
  const normalizedHardDeadlineMs = Number(hardDeadlineMs);
  const actionDeadlineMs = Math.max(
    25000,
    Math.min(
      MESSAGE_ACTION_DEADLINE_MS,
      Number.isFinite(normalizedHardDeadlineMs) && normalizedHardDeadlineMs > 5000
        ? normalizedHardDeadlineMs - 2500
        : MESSAGE_ACTION_DEADLINE_MS
    )
  );
  const snapshotTimeoutMs = Math.max(1500, Math.min(3000, FAST_SNAPSHOT_TIMEOUT_MS));
  const remainingMs = () => actionDeadlineMs - (Date.now() - actionStartedAt);
  const ensureDeadline = (context = "") => {
    if (Date.now() - actionStartedAt >= actionDeadlineMs) {
      throw buildActionTimeoutError(context || "send-media");
    }
  };
  const hasTimeLeft = (reserveMs = 0) => Date.now() - actionStartedAt < (actionDeadlineMs - Math.max(0, reserveMs));
  const throwIfAborted = (context = "") => {
    if (abortSignal?.aborted) {
      throw buildActionTimeoutError(context || "send-media-aborted");
    }
  };
  const getStepTimeout = (desiredMs, minMs = 1200, reserveMs = 2500, context = "") => {
    const normalizedMin = Math.max(500, Number(minMs) || 500);
    const desired = Math.max(normalizedMin, Number(desiredMs) || normalizedMin);
    const remainingBudget = remainingMs() - Math.max(0, reserveMs);
    if (remainingBudget <= normalizedMin) {
      throw buildActionTimeoutError(context || "send-media-time-budget");
    }
    return Math.max(normalizedMin, Math.min(desired, remainingBudget));
  };
  if (!conversationId && !conversationUrl) {
    const error = new Error("CONVERSATION_ID_REQUIRED");
    error.code = "CONVERSATION_ID_REQUIRED";
    throw error;
  }

  const trimmedText = String(text || "").trim();
  const fileList = Array.isArray(files) ? files : [];
  if (!trimmedText && !fileList.length) {
    const error = new Error("MESSAGE_EMPTY");
    error.code = "MESSAGE_EMPTY";
    throw error;
  }

  const deviceProfile = getDeviceProfile(account);
  const cookies = parseCookies(account.cookie).map(normalizeCookie);
  if (!cookies.length) {
    const error = new Error("AUTH_REQUIRED");
    error.code = "AUTH_REQUIRED";
    throw error;
  }

  const resolvedConversationId = conversationId || extractConversationId(conversationUrl);
  if (!resolvedConversationId) {
    const error = new Error("CONVERSATION_ID_REQUIRED");
    error.code = "CONVERSATION_ID_REQUIRED";
    throw error;
  }
  const resolvedConversationUrl = buildConversationUrl(resolvedConversationId, conversationUrl);
  const beforeSnapshot = null;

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "kl-media-"));
  const tempPaths = [];
  let observedSendRequestAfterClick = false;
  let observedSendRequestUrl = "";
  let composerSettledAfterSend = false;
  let sendInteractionTriggered = false;

  const guessExtension = (file) => {
    const name = String(file?.originalname || "").trim().toLowerCase();
    const match = name.match(/\\.(png|jpe?g|webp|gif|bmp|heic|heif)$/i);
    if (match) return match[0];
    const mime = String(file?.mimetype || "").toLowerCase();
    if (mime.includes("png")) return ".png";
    if (mime.includes("webp")) return ".webp";
    if (mime.includes("gif")) return ".gif";
    if (mime.includes("bmp")) return ".bmp";
    if (mime.includes("heic")) return ".heic";
    if (mime.includes("heif")) return ".heif";
    return ".jpg";
  };

  try {
    for (let index = 0; index < fileList.length; index += 1) {
      const file = fileList[index];
      const buffer = Buffer.isBuffer(file?.buffer) ? file.buffer : Buffer.from(file?.buffer || "");
      if (!buffer.length) continue;
      const base = sanitizeFilename(file?.originalname || `image-${index + 1}`);
      const ext = guessExtension(file);
      const safeName = base.toLowerCase().endsWith(ext) ? base : `${base}${ext}`;
      const targetPath = path.join(tmpDir, safeName);
      fs.writeFileSync(targetPath, buffer);
      tempPaths.push(targetPath);
    }

    const proxyServer = buildProxyServer(proxy);
    const proxyUrl = buildProxyUrl(proxy);
    const needsProxyChain = Boolean(
      proxyUrl && ((proxy?.type || "").toLowerCase().startsWith("socks") || proxy?.username || proxy?.password)
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

    const profileDir = createTempProfileDir();
    const browser = await puppeteer.launch({
      headless: "new",
      args: launchArgs,
      userDataDir: profileDir,
      timeout: PUPPETEER_LAUNCH_TIMEOUT,
      protocolTimeout: PUPPETEER_PROTOCOL_TIMEOUT
    });

    let abortListener = null;
    if (abortSignal) {
      abortListener = () => {
        safeCloseBrowser(browser).catch(() => {});
      };
      if (abortSignal.aborted) {
        abortListener();
      } else {
        abortSignal.addEventListener("abort", abortListener, { once: true });
      }
    }

    let page = null;
    let requestListener = null;
    let waitForLikelyMessageRequestAfter = async () => false;
    try {
      throwIfAborted("send-media-before-new-page");
      page = await browser.newPage();
      const localTimeout = getStepTimeout(
        Math.min(PUPPETEER_NAV_TIMEOUT, 18000),
        4500,
        9000,
        "send-media-navigation-budget"
      );
      page.setDefaultTimeout(localTimeout);
      page.setDefaultNavigationTimeout(localTimeout);
      if (!anonymizedProxyUrl && (proxy?.username || proxy?.password)) {
        await page.authenticate({
          username: proxy.username || "",
          password: proxy.password || ""
        });
      }

      await page.setUserAgent(deviceProfile.userAgent);
      await page.setViewport(deviceProfile.viewport);
      await page.setExtraHTTPHeaders({ "Accept-Language": deviceProfile.locale });
      await page.emulateTimezone(deviceProfile.timezone);
      await page.evaluateOnNewDocument((platform) => {
        Object.defineProperty(navigator, "platform", {
          get: () => platform
        });
      }, deviceProfile.platform);

      ensureDeadline("send-media-before-set-cookie");
      throwIfAborted("send-media-before-set-cookie");
      await page.setCookie(...cookies);
      await humanPause(35, 90);
      const conversationGotoTimeout = getStepTimeout(
        localTimeout,
        4500,
        6000,
        "send-media-conversation-goto"
      );
      await gotoWithProxyHandling(
        page,
        resolvedConversationUrl,
        { waitUntil: "domcontentloaded", timeout: conversationGotoTimeout },
        "MEDIA_CONVERSATION_GOTO"
      );
      throwIfAborted("send-media-after-conversation-goto");
      if (await isAuthFailure(page)) {
        const error = new Error("AUTH_REQUIRED");
        error.code = "AUTH_REQUIRED";
        throw error;
      }
      await acceptCookieModal(page, { timeout: MESSAGE_CONSENT_TIMEOUT_MS }).catch(() => {});
      if (isGdprPage(page.url())) {
        await acceptGdprConsent(page, { timeout: MESSAGE_GDPR_TIMEOUT_MS }).catch(() => {});
      }
      await humanPause(30, 70);
      await dismissConversationBlockingModals(page, { maxPasses: 1 }).catch(() => {});
      ensureDeadline("send-media-after-conversation-open");
      throwIfAborted("send-media-after-conversation-open");

      if (await isAuthFailure(page)) {
        const error = new Error("AUTH_REQUIRED");
        error.code = "AUTH_REQUIRED";
        throw error;
      }

      let lastLikelyMessageRequestAt = 0;
      let lastLikelyMessageRequestUrl = "";
      requestListener = (request) => {
        const requestUrl = typeof request?.url === "function" ? request.url() : "";
        const requestMethod = typeof request?.method === "function" ? request.method() : "";
        const requestBody = typeof request?.postData === "function" ? request.postData() : "";
        if (!isLikelyMessagingMutationRequest(requestUrl, requestMethod, requestBody)) return;
        lastLikelyMessageRequestAt = Date.now();
        lastLikelyMessageRequestUrl = requestUrl;
      };
      page.on("request", requestListener);
      waitForLikelyMessageRequestAfter = async (minTimestamp, timeoutMs = 9000) => {
        const startedAt = Date.now();
        while (Date.now() - startedAt < timeoutMs) {
          ensureDeadline("send-media-wait-network-request");
          if (!hasTimeLeft(2500)) return false;
          if (lastLikelyMessageRequestAt >= minTimestamp) {
            observedSendRequestAfterClick = true;
            observedSendRequestUrl = lastLikelyMessageRequestUrl;
            return true;
          }
          await sleep(180);
        }
        return false;
      };

      if (tempPaths.length) {
        await page.evaluate(() => {
          const target = document.querySelector(".ReplyBox, [class*='ReplyBox'], textarea#nachricht, textarea[placeholder*='Nachricht']");
          if (target && typeof target.scrollIntoView === "function") {
            target.scrollIntoView({ block: "center", inline: "center" });
          }
        }).catch(() => {});
        await waitForDynamicContent(page, [
          ".ReplyBox",
          "[class*='ReplyBox']",
          "textarea#nachricht",
          "button[aria-label*='Bilder hochladen']",
          "input[data-testid='reply-box-file-input']",
          "input[type='file']"
        ], 6500);
        await dismissConversationBlockingModals(page, { maxPasses: 4 }).catch(() => {});
        const fileInputSelectors = [
          ".ReplyBox input[data-testid='reply-box-file-input']",
          "[class*='ReplyBox'] input[data-testid='reply-box-file-input']",
          ".ReplyBox input[type='file'][accept*='image']",
          "[class*='ReplyBox'] input[type='file'][accept*='image']",
          ".ReplyBox input[type='file']",
          "[class*='ReplyBox'] input[type='file']",
          "input[data-testid='reply-box-file-input']",
          "input[data-qa*='reply-box-file-input']",
          "input[type='file'][accept*='image']",
          "input[type='file'][accept*='image/*']",
          ".Reply--Actions input[type='file']",
          "[class*='Reply'] input[type='file'][multiple]",
          "[class*='Reply'] input[type='file']",
          "input[type='file'][multiple]",
          "input[type='file']"
        ];
        const cameraButtonSelectors = [
          ".ReplyBox button[data-testid='generic-button-ghost'][aria-label*='Bilder hochladen']",
          "[class*='ReplyBox'] button[aria-label*='Bilder hochladen']",
          "button[data-testid='generic-button-ghost'][aria-label='Bilder hochladen']",
          "button[aria-label*='Bilder hochladen']",
          "button[aria-label*='Foto']",
          "button[aria-label*='Bild']",
          "button[data-testid='generic-button-ghost'][aria-label*='Bilder']",
          ".ReplyBox button[data-testid='generic-button-ghost']",
          "[class*='ReplyBox'] button[data-testid='generic-button-ghost']"
        ];
        let uploaded = false;
        let fileInputHandle = null;

        const tryCameraChooserUpload = async () => {
          let cameraButton = await findFirstHandleInAnyContext(page, cameraButtonSelectors, {
            timeout: 1200,
            requireVisible: true
          });
          if (!cameraButton?.handle) {
            const cameraSvgHost = await findFirstHandleInAnyContext(page, ["svg[data-title*='camera']"], {
              timeout: 1000,
              requireVisible: true
            });
            if (cameraSvgHost?.handle) {
              const maybeButton = await cameraSvgHost.handle.evaluateHandle((node) => {
                if (!node) return null;
                let current = node;
                while (current) {
                  if (current.tagName === "BUTTON") return current;
                  const root = typeof current.getRootNode === "function" ? current.getRootNode() : null;
                  if (current.parentElement) {
                    current = current.parentElement;
                  } else if (root && root.host) {
                    current = root.host;
                  } else {
                    current = null;
                  }
                }
                return null;
              }).catch(() => null);
              const buttonElement = typeof maybeButton?.asElement === "function" ? maybeButton.asElement() : null;
              if (buttonElement) {
                cameraButton = { handle: buttonElement };
              } else if (maybeButton) {
                await maybeButton.dispose().catch(() => {});
              }
            }
          }
          if (!cameraButton?.handle) return false;
          const chooserPromise = page.waitForFileChooser({ timeout: 2600 }).catch(() => null);
          const clickedCamera = await clickHandleHumanLike(page, cameraButton.handle);
          if (!clickedCamera) {
            await cameraButton.handle.click({ delay: 30 }).catch(() => {});
          }
          const chooser = await chooserPromise;
          if (!chooser) return false;
          await chooser.accept(tempPaths);
          return true;
        };

        for (let attempt = 0; attempt < 3 && !uploaded && hasTimeLeft(11000); attempt += 1) {
          ensureDeadline("send-media-find-file-input");
          if (attempt === 0 || randomChance(0.35)) {
            uploaded = await tryCameraChooserUpload();
            if (uploaded) break;
          }
          if (!fileInputHandle) {
            const fileInputResult = await findFirstHandleInAnyContext(page, fileInputSelectors, {
              timeout: attempt === 0 ? 1800 : 3600,
              requireVisible: false
            });
            fileInputHandle = fileInputResult?.handle || null;
          }
          if (fileInputHandle) break;

          await dismissConversationBlockingModals(page, { maxPasses: 2 }).catch(() => {});
          await humanPause(100, 180);
        }

        if (!uploaded && !fileInputHandle) {
          uploaded = await tryCameraChooserUpload();
        }

        if (!uploaded && !fileInputHandle) {
          const fallbackInput = await findFirstHandleInAnyContext(page, fileInputSelectors, {
            timeout: 5200,
            requireVisible: false
          });
          fileInputHandle = fallbackInput?.handle || null;
        }

        if (!uploaded) {
          if (!fileInputHandle) {
            throw new Error("MESSAGE_FILE_INPUT_NOT_FOUND");
          }
          await fileInputHandle.evaluate((node) => {
            node.hidden = false;
            node.style.display = "block";
            node.style.visibility = "visible";
            node.style.opacity = "1";
          }).catch(() => {});

          await fileInputHandle.uploadFile(...tempPaths);
          await fileInputHandle.evaluate((node) => {
            node.dispatchEvent(new Event("input", { bubbles: true }));
            node.dispatchEvent(new Event("change", { bubbles: true }));
          }).catch(() => {});
        }

        await humanPause(140, 220);
        let attachmentsReady = await waitForMessageAttachmentReady(page, tempPaths.length, 3500).catch(() => false);
        let sendReadyAfterAttachment = await waitForMessageSendButtonReady(page, 3000).catch(() => false);
        if (!attachmentsReady && !sendReadyAfterAttachment) {
          await humanPause(140, 240);
          attachmentsReady = await waitForMessageAttachmentReady(page, tempPaths.length, 2500).catch(() => false);
          sendReadyAfterAttachment = await waitForMessageSendButtonReady(page, 2500).catch(() => false);
        }
        if (!attachmentsReady && !sendReadyAfterAttachment) {
          // Kleinanzeigen can keep image previews in loading state for a while.
          // Do not fail hard here; proceed to send click path with retries below.
          await humanPause(100, 170);
        }
        await performHumanLikePageActivity(page, { intensity: "light" });
      }

      if (trimmedText) {
        ensureDeadline("send-media-before-text-fill");
        const inputSelectors = [
          "textarea#nachricht",
          "textarea[name='message']",
          "textarea[name='text']",
          "textarea[placeholder*='Nachricht']",
          "textarea[aria-label*='Nachricht']",
          "[role='textbox'][contenteditable='true']",
          "[contenteditable='true']",
          "textarea"
        ];
        await waitForDynamicContent(page, inputSelectors, 4500);
        const inputResult = await findMessageInputHandle(page, inputSelectors);
        if (!inputResult?.handle) {
          throw new Error("MESSAGE_INPUT_NOT_FOUND");
        }
        const inputHandle = inputResult.handle;
        await inputHandle.evaluate((input) => {
          input.scrollIntoView({ block: "center", inline: "center" });
          if (input.tagName === "TEXTAREA" || input.tagName === "INPUT") {
            input.value = "";
            input.dispatchEvent(new Event("input", { bubbles: true }));
          } else if (input.isContentEditable) {
            input.textContent = "";
          }
        }).catch(() => {});
        await inputHandle.focus();
        await typeTextHumanLike(inputHandle, trimmedText);
        if (randomChance(0.3)) {
          await performHumanLikePageActivity(page, { intensity: "light" });
        }
      }
      await dismissConversationBlockingModals(page, { maxPasses: 3 }).catch(() => {});

      const sendSelectors = [
        ".ReplyBox button[data-testid='submit-button'][aria-label*='Senden']",
        "button[data-testid='submit-button'][aria-label*='Senden']",
        ".ReplyBox button[data-testid='submit-button']",
        "button[data-testid='submit-button']",
        "button[aria-label*='Senden']",
        "button[type='submit']"
      ];
      ensureDeadline("send-media-before-send-click");
      const sendAttemptStartedAt = Date.now();
      await waitForDynamicContent(page, sendSelectors, 4500);
      await waitForMessageSendButtonReady(page, 2500).catch(() => false);
      let sendClicked = await clickFirstInteractiveHandleInAnyContext(page, sendSelectors, { timeout: 4500 });
      if (!sendClicked) {
        sendClicked = await clickVisibleButtonByText(page, ["Senden"], {
          timeout: 1200,
          preferTopLayer: true
        });
      }
      if (!sendClicked) {
        sendClicked = await page.evaluate(() => {
          const selectors = [
            "button[data-testid='submit-button']",
            "button[aria-label*='Senden']",
            ".ReplyBox button[type='submit']",
            "button[type='submit']"
          ];
          const isVisible = (node) => {
            if (!node) return false;
            const style = window.getComputedStyle(node);
            if (!style) return false;
            if (style.display === "none" || style.visibility === "hidden" || style.opacity === "0" || style.pointerEvents === "none") {
              return false;
            }
            const rect = node.getBoundingClientRect();
            return rect.width > 0 && rect.height > 0;
          };
          for (const selector of selectors) {
            let nodes = [];
            try {
              nodes = Array.from(document.querySelectorAll(selector));
            } catch (error) {
              nodes = [];
            }
            for (const node of nodes) {
              if (!isVisible(node)) continue;
              if (node.hasAttribute("disabled")) continue;
              if (node.getAttribute("aria-disabled") === "true") continue;
              node.click();
              return true;
            }
          }
          return false;
        }).catch(() => false);
      }
      if (!sendClicked) {
        await page.keyboard.press("Enter").catch(() => {});
        await humanPause(90, 150);
        sendClicked = await clickFirstInteractiveHandleInAnyContext(page, sendSelectors, { timeout: 1400 });
      }
      if (!sendClicked && !trimmedText) {
        await page.keyboard.down("Control").catch(() => {});
        await page.keyboard.press("Enter").catch(() => {});
        await page.keyboard.up("Control").catch(() => {});
        await humanPause(90, 150);
        sendClicked = await page.evaluate(() => {
          const previewSelectors = [
            "[data-testid*='attachment'] img",
            "[class*='Attachment'] img",
            "[class*='attachment'] img",
            ".ReplyBox img[src^='blob:']"
          ];
          const previewCount = previewSelectors.reduce((count, selector) => {
            try {
              return count + document.querySelectorAll(selector).length;
            } catch (error) {
              return count;
            }
          }, 0);
          if (previewCount === 0) return true;

          const sendButton = document.querySelector("button[data-testid='submit-button'], button[aria-label*='Senden'], button[type='submit']");
          if (!sendButton) return false;
          if (sendButton.hasAttribute("disabled")) return true;
          if (sendButton.getAttribute("aria-disabled") === "true") return true;
          if (sendButton.getAttribute("aria-busy") === "true") return true;
          return false;
        }).catch(() => false);
      }
      if (!sendClicked) {
        throw new Error("MESSAGE_SEND_BUTTON_NOT_READY");
      }
      sendInteractionTriggered = true;
      await humanPause(120, 190);
      throwIfAborted("send-media-after-send-click");

      await waitForLikelyMessageRequestAfter(
        sendAttemptStartedAt,
        tempPaths.length ? 3200 : 2200
      ).catch(() => false);

      composerSettledAfterSend = await waitForReplyComposerSettledAfterSend(page, {
        expectTextClear: Boolean(trimmedText),
        expectAttachmentClear: tempPaths.length > 0,
        timeout: tempPaths.length ? 3500 : 2500
      }).catch(() => false);

      if (tempPaths.length > 0 && !observedSendRequestAfterClick && !composerSettledAfterSend) {
        ensureDeadline("send-media-retry-send-click");
        const retrySendAttemptStartedAt = Date.now();
        let retryClicked = await clickFirstInteractiveHandleInAnyContext(page, sendSelectors, { timeout: 1400 });
        if (!retryClicked) {
          retryClicked = await clickVisibleButtonByText(page, ["Senden"], {
            timeout: 900,
            preferTopLayer: true
          });
        }
        if (retryClicked) {
          await humanPause(100, 170);
          await waitForLikelyMessageRequestAfter(retrySendAttemptStartedAt, 3200).catch(() => false);
          composerSettledAfterSend = await waitForReplyComposerSettledAfterSend(page, {
            expectTextClear: Boolean(trimmedText),
            expectAttachmentClear: true,
            timeout: 2800
          }).catch(() => false);
        }
      }
    } finally {
      if (page && requestListener) {
        if (typeof page.off === "function") {
          page.off("request", requestListener);
        } else if (typeof page.removeListener === "function") {
          page.removeListener("request", requestListener);
        }
      }
      if (abortSignal && abortListener) {
        abortSignal.removeEventListener("abort", abortListener);
      }
      await safeCloseBrowser(browser);
      if (anonymizedProxyUrl) {
        await proxyChain.closeAnonymizedProxy(anonymizedProxyUrl, true);
      }
      fs.rmSync(profileDir, { recursive: true, force: true });
    }
  } finally {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }

  if (Date.now() - actionStartedAt > actionDeadlineMs) {
    return {
      messages: [],
      conversationId: resolvedConversationId,
      conversationUrl: resolvedConversationUrl
    };
  }

  try {
    let afterSnapshot = await fetchConversationSnapshotViaApiWithRetry({
      account,
      proxy,
      conversationId: resolvedConversationId,
      conversationUrl: resolvedConversationUrl,
      attempts: FAST_SNAPSHOT_ATTEMPTS,
      delayMs: FAST_SNAPSHOT_DELAY_MS,
      requestTimeoutMs: snapshotTimeoutMs
    });

    const beforeTextCount = countOutgoingTextMatches(beforeSnapshot, trimmedText);
    const beforeAttachmentUnits = countOutgoingAttachmentUnits(beforeSnapshot);
    const requireTextConfirmation = Boolean(trimmedText);
    const requireMediaConfirmation = tempPaths.length > 0;

    let textConfirmed = !trimmedText
      || countOutgoingTextMatches(afterSnapshot, trimmedText) > beforeTextCount
      || (beforeTextCount === 0 && hasOutgoingTextInSnapshot(afterSnapshot, trimmedText));
    let mediaConfirmed = !requireMediaConfirmation
      || countOutgoingAttachmentUnits(afterSnapshot) > beforeAttachmentUnits
      || observedSendRequestAfterClick
      || composerSettledAfterSend;

    if ((!textConfirmed || !mediaConfirmed) && (trimmedText || tempPaths.length)) {
      await sleep(350);
      afterSnapshot = await fetchConversationSnapshotViaApiWithRetry({
        account,
        proxy,
        conversationId: resolvedConversationId,
        conversationUrl: resolvedConversationUrl,
        attempts: 1,
        delayMs: FAST_SNAPSHOT_DELAY_MS,
        requestTimeoutMs: snapshotTimeoutMs
      });
      textConfirmed = !trimmedText
        || countOutgoingTextMatches(afterSnapshot, trimmedText) > beforeTextCount
        || (beforeTextCount === 0 && hasOutgoingTextInSnapshot(afterSnapshot, trimmedText));
      mediaConfirmed = !requireMediaConfirmation
        || countOutgoingAttachmentUnits(afterSnapshot) > beforeAttachmentUnits
        || observedSendRequestAfterClick
        || composerSettledAfterSend;
    }
    const likelyUiSendSuccess = sendInteractionTriggered || observedSendRequestAfterClick || composerSettledAfterSend;

    if (!textConfirmed && requireTextConfirmation && !likelyUiSendSuccess) {
      const error = new Error("MESSAGE_SEND_NOT_CONFIRMED");
      error.code = "MESSAGE_SEND_NOT_CONFIRMED";
      throw error;
    }

    if (!mediaConfirmed && requireMediaConfirmation && !likelyUiSendSuccess) {
      const error = new Error("MESSAGE_MEDIA_SEND_NOT_CONFIRMED");
      error.code = "MESSAGE_MEDIA_SEND_NOT_CONFIRMED";
      if (observedSendRequestUrl) {
        error.details = observedSendRequestUrl;
      }
      throw error;
    }

    return afterSnapshot;
  } catch (error) {
    const likelyUiSendSuccess = sendInteractionTriggered || observedSendRequestAfterClick || composerSettledAfterSend;
    if (!likelyUiSendSuccess && (error.code === "MESSAGE_SEND_NOT_CONFIRMED" || error.code === "MESSAGE_MEDIA_SEND_NOT_CONFIRMED")) {
      throw error;
    }
    return {
      messages: [],
      conversationId: resolvedConversationId,
      conversationUrl: resolvedConversationUrl
    };
  }
};

const summarizeConversations = (conversations) => {
  const summaries = [];
  const deduped = dedupeConversationList(conversations || []);

  deduped.forEach((conversation, index) => {
    const href = conversation.href || conversation.conversationUrl || "";
    const resolvedConversationId = conversation.conversationId || extractConversationId(href);

    const lastMessage = conversation.messages?.[conversation.messages.length - 1];
    const timeSource = lastMessage?.dateTime || lastMessage?.timeLabel || conversation.timeText;
    const parsedTime = parseTimestamp(timeSource);
    const fallbackText = conversation.lastMessage || lastMessage?.text || "";

    summaries.push({
      id: resolvedConversationId || conversation.conversationId || `${conversation.accountId}-${index}`,
      conversationId: resolvedConversationId || conversation.conversationId,
      sender: conversation.participant || lastMessage?.sender || "",
      message: fallbackText,
      date: parsedTime.date,
      time: parsedTime.time,
      unread: Boolean(conversation.unread),
      accountId: conversation.accountId,
      accountName: conversation.accountLabel,
      conversationUrl: href || (resolvedConversationId ? buildConversationUrl(resolvedConversationId) : ""),
      adTitle: conversation.adTitle,
      adImage: normalizeImageUrl(conversation.adImage),
      messages: (conversation.messages || []).map((message) => {
        const parsed = parseTimestamp(message.dateTime || message.timeLabel || "");
        return {
          ...message,
          date: parsed.date,
          time: parsed.time
        };
      })
    });
  });

  return summaries;
};

module.exports = {
  fetchMessages,
  fetchThreadMessages,
  summarizeConversations,
  sendConversationMessage,
  declineConversationOffer,
  sendConversationMedia
};
