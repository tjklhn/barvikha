const fs = require("fs");
const os = require("os");
const path = require("path");
const puppeteer = require("puppeteer-extra");
const StealthPlugin = require("puppeteer-extra-plugin-stealth");

puppeteer.use(StealthPlugin());

const DEVICE_PROFILES = [
  {
    id: "de-win-chrome",
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    viewport: { width: 1366, height: 768 },
    locale: "de-DE,de;q=0.9,en;q=0.8",
    timezone: "Europe/Berlin",
    platform: "Win32",
    geolocation: { latitude: 52.520008, longitude: 13.404954, accuracy: 50 }
  },
  {
    id: "de-mac-chrome",
    userAgent:
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    viewport: { width: 1440, height: 900 },
    locale: "de-DE,de;q=0.9,en;q=0.8",
    timezone: "Europe/Berlin",
    platform: "MacIntel",
    geolocation: { latitude: 52.520008, longitude: 13.404954, accuracy: 50 }
  },
  {
    id: "de-win-firefox",
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    viewport: { width: 1536, height: 864 },
    locale: "de-DE,de;q=0.9,en;q=0.8",
    timezone: "Europe/Berlin",
    platform: "Win32",
    geolocation: { latitude: 52.520008, longitude: 13.404954, accuracy: 50 }
  }
];

const pickDeviceProfile = () => {
  const index = Math.floor(Math.random() * DEVICE_PROFILES.length);
  return DEVICE_PROFILES[index];
};

const proxyChain = require("proxy-chain");
const axios = require("axios");
const proxyChecker = require("./proxyChecker");
const {
  parseCookies,
  normalizeCookies,
  normalizeCookie,
  filterKleinanzeigenCookies,
  buildCookieHeaderForUrl,
  buildProxyServer,
  buildProxyUrl,
  buildPuppeteerProxyUrl
} = require("./cookieUtils");
const createTempProfileDir = () => fs.mkdtempSync(path.join(os.tmpdir(), "kl-profile-"));
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const humanPause = (min = 120, max = 260) => sleep(Math.floor(min + Math.random() * (max - min)));
const PUPPETEER_PROTOCOL_TIMEOUT = Number(process.env.PUPPETEER_PROTOCOL_TIMEOUT || 120000);
const PUPPETEER_LAUNCH_TIMEOUT = Number(process.env.PUPPETEER_LAUNCH_TIMEOUT || 120000);
const PUPPETEER_NAV_TIMEOUT = Number(process.env.PUPPETEER_NAV_TIMEOUT || 60000);
const RETRY_PROTOCOL_ERRORS = process.env.PUPPETEER_RETRY_PROTOCOL_ERRORS !== "0";
const MAX_PROTOCOL_RETRIES = Math.max(0, Number(process.env.PUPPETEER_PROTOCOL_RETRIES || 1));
const PROXY_PREFLIGHT_ENABLED = process.env.KL_PROXY_PREFLIGHT !== "0";
const PROXY_PREFLIGHT_TIMEOUT = Number(process.env.KL_PROXY_PREFLIGHT_TIMEOUT || 12000);

const isProtocolTimeoutError = (error) => {
  const message = String(error?.message || "");
  return /Network\.enable timed out/i.test(message) || /Protocol error/i.test(message);
};

const buildLaunchArgs = (baseArgs, attempt) => {
  if (!attempt) return baseArgs;
  const fallbackArgs = [
    "--single-process",
    "--disable-features=site-per-process",
    "--disable-features=IsolateOrigins",
    "--no-first-run",
    "--no-default-browser-check"
  ];
  const next = [...baseArgs];
  for (const flag of fallbackArgs) {
    if (!next.includes(flag)) {
      next.push(flag);
    }
  }
  return next;
};

const isTimeoutLikeError = (error) => {
  const message = String(error?.message || "");
  return /ERR_TIMED_OUT|Navigation timeout|net::ERR/i.test(message);
};

const isProxyConnectionError = (error) => {
  const message = String(error?.message || "");
  return /ERR_TUNNEL_CONNECTION_FAILED|ERR_PROXY_CONNECTION_FAILED|ERR_SOCKS_CONNECTION_FAILED|ERR_NO_SUPPORTED_PROXIES/i.test(message);
};

const gotoWithRetries = async (page, url, options = {}, retries = 2) => {
  let lastError;
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    try {
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: PUPPETEER_NAV_TIMEOUT, ...options });
      return true;
    } catch (error) {
      lastError = error;
      if (!isTimeoutLikeError(error) || attempt === retries) {
        throw error;
      }
      await sleep(800 + attempt * 900);
    }
  }
  throw lastError;
};

const sanitizeProfileName = (value) => {
  const normalized = (value || "").replace(/\s+/g, " ").trim();
  if (!normalized) return "";
  if (/mein(e)? anzeigen|mein profil|meine anzeigen|profil und meine anzeigen/i.test(normalized)) return "";
  return normalized;
};

const decodeJwtPayload = (token) => {
  if (!token) return null;
  const parts = String(token).split(".");
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

const buildCookieHeader = (cookies) =>
  buildCookieHeaderForUrl(cookies, "https://www.kleinanzeigen.de/m-access-token.json");

const extractProfileFromCookies = (cookies) => {
  const accessToken = (cookies || []).find((cookie) => cookie?.name === "access_token" && cookie?.value);
  if (!accessToken?.value) return { profileName: "", profileEmail: "", profileUserId: "", profileUrl: "" };
  const payload = decodeJwtPayload(accessToken.value);
  if (!payload) return { profileName: "", profileEmail: "", profileUserId: "", profileUrl: "" };
  const name = payload?.name || payload?.preferred_username || "";
  const email = payload?.email || "";
  const profileUserId = pickUserIdCandidate(
    payload?.userId,
    payload?.user_id,
    payload?.uid
  );
  return {
    profileName: sanitizeProfileName(name),
    profileEmail: String(email || "").trim(),
    profileUserId,
    profileUrl: buildProfileUrlByUserId(profileUserId)
  };
};

const EMAIL_PATTERN = /[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}/i;

const pickEmailCandidate = (...values) => {
  for (const value of values) {
    const text = String(value || "").trim();
    if (!text) continue;
    const matched = text.match(EMAIL_PATTERN);
    if (matched?.[0]) return matched[0].toLowerCase();
  }
  return "";
};

const pickUserIdCandidate = (...values) => {
  for (const value of values) {
    if (typeof value === "number" && Number.isFinite(value) && value > 0) {
      const numeric = String(Math.trunc(value));
      if (/^\d{4,}$/.test(numeric)) return numeric;
      continue;
    }
    if (typeof value === "bigint" && value > 0n) {
      const bigintValue = String(value);
      if (/^\d{4,}$/.test(bigintValue)) return bigintValue;
      continue;
    }
    const normalized = String(value ?? "").trim();
    if (!normalized) continue;
    if (/^\d{4,}$/.test(normalized)) return normalized;
    const queryMatch = normalized.match(/(?:^|[?&#])userId=(\d{4,})(?:[&#]|$)/i);
    if (queryMatch?.[1]) return queryMatch[1];
  }
  return "";
};

const buildProfileUrlByUserId = (userId) => {
  const normalizedId = pickUserIdCandidate(userId);
  if (!normalizedId) return "";
  return `https://www.kleinanzeigen.de/s-bestandsliste.html?userId=${encodeURIComponent(normalizedId)}`;
};

const toAbsoluteKleinanzeigenUrl = (value, base = "https://www.kleinanzeigen.de/") => {
  const input = String(value || "").trim();
  if (!input) return "";
  try {
    const resolved = new URL(input, base);
    const host = String(resolved.hostname || "").toLowerCase();
    if (host === "kleinanzeigen.de" || host.endsWith(".kleinanzeigen.de")) {
      return resolved.href;
    }
    return "";
  } catch (error) {
    return "";
  }
};

const extractProfileFromHomepageHtml = (html) => {
  const source = String(html || "");
  if (!source) return { profileName: "", profileEmail: "", profileUserId: "", profileUrl: "" };

  let profileEmail = "";
  let profileName = "";
  let profileUserId = "";
  let profileUrl = "";

  try {
    const scriptMatch = source.match(/<script[^>]*id=["']astroSharedData["'][^>]*>([\s\S]*?)<\/script>/i);
    const jsonText = scriptMatch?.[1] ? String(scriptMatch[1]).trim() : "";
    if (jsonText) {
      const data = JSON.parse(jsonText);
      profileEmail = pickEmailCandidate(
        data?.user?.email,
        data?.userProfile?.email
      );
      profileUserId = pickUserIdCandidate(
        data?.user?.userId,
        data?.userProfile?.userId
      );
      profileName = sanitizeProfileName(
        data?.user?.contactName
        || data?.userProfile?.contactName
        || ""
      );
    }
  } catch (error) {
    // ignore JSON parse issues and continue with text fallbacks
  }

  if (!profileEmail) {
    const labelMatch = source.match(/angemeldet\s+als\s*:\s*([^<\n]+)/i);
    profileEmail = pickEmailCandidate(labelMatch?.[1] || "");
  }

  if (!profileEmail) {
    const jsonEmailMatch = source.match(/"(?:email|userEmail)"\s*:\s*"([^"]+)"/i);
    profileEmail = pickEmailCandidate(jsonEmailMatch?.[1] || "");
  }

  if (!profileUserId) {
    const userScopedMatch = source.match(/"(?:user|userProfile)"\s*:\s*\{[\s\S]{0,500}?"userId"\s*:\s*"?(\d{4,})"?/i);
    const userIdMatch = userScopedMatch || source.match(/(?:\?|&|\\u0026)userId=(\d{4,})/i);
    profileUserId = pickUserIdCandidate(userIdMatch?.[1] || "");
  }

  const profileLinkMatch = source.match(/href=["']([^"']*s-bestandsliste\.html\?userId=\d+[^"']*)["']/i);
  const profileLink = toAbsoluteKleinanzeigenUrl(profileLinkMatch?.[1] || "");
  const profileLinkUserId = pickUserIdCandidate(profileLink);
  if (!profileUserId && profileLinkUserId) {
    profileUserId = profileLinkUserId;
  }
  if (profileLink && (!profileUserId || profileLinkUserId === profileUserId)) {
    profileUrl = profileLink;
  }
  if (!profileUrl) {
    profileUrl = buildProfileUrlByUserId(profileUserId);
  }

  return {
    profileName: sanitizeProfileName(profileName),
    profileEmail,
    profileUserId,
    profileUrl
  };
};

const mergeProfileData = (primary = {}, fallback = {}) => {
  const candidateUrl = toAbsoluteKleinanzeigenUrl(primary?.profileUrl || fallback?.profileUrl || "");
  const profileUserId = pickUserIdCandidate(
    primary?.profileUserId,
    fallback?.profileUserId,
    candidateUrl
  );
  return {
    profileName: sanitizeProfileName(primary?.profileName || fallback?.profileName || ""),
    profileEmail: pickEmailCandidate(primary?.profileEmail, fallback?.profileEmail),
    profileUserId,
    profileUrl: buildProfileUrlByUserId(profileUserId) || candidateUrl
  };
};

const fetchProfileFromHomepageViaHttp = async ({ cookies, proxy, deviceProfile, timeoutMs = 20000 } = {}) => {
  const cookieHeader = buildCookieHeaderForUrl(cookies, "https://www.kleinanzeigen.de/");
  if (!cookieHeader) {
    return { profileName: "", profileEmail: "", profileUserId: "", profileUrl: "" };
  }
  const axiosConfig = proxyChecker.buildAxiosConfig(proxy, Math.max(4000, Number(timeoutMs) || 20000));
  axiosConfig.headers = {
    ...axiosConfig.headers,
    Cookie: cookieHeader,
    "User-Agent": deviceProfile?.userAgent || axiosConfig.headers?.["User-Agent"] || "Mozilla/5.0",
    "Accept-Language": deviceProfile?.locale || "de-DE,de;q=0.9,en;q=0.8",
    Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
  };
  axiosConfig.validateStatus = (status) => status >= 200 && status < 500;

  const response = await axios.get("https://www.kleinanzeigen.de/", axiosConfig);
  if (response.status >= 400) {
    return { profileName: "", profileEmail: "", profileUserId: "", profileUrl: "" };
  }
  return extractProfileFromHomepageHtml(response.data);
};

const validateCookiesViaAccessToken = async ({ cookieHeader, proxy, deviceProfile, timeoutMs = 20000 } = {}) => {
  if (!cookieHeader) {
    return { ok: false, status: 0, reason: "EMPTY_COOKIE_HEADER" };
  }
  const axiosConfig = proxyChecker.buildAxiosConfig(proxy, Math.max(4000, Number(timeoutMs) || 20000));
  axiosConfig.headers = {
    ...axiosConfig.headers,
    Cookie: cookieHeader,
    "User-Agent": deviceProfile?.userAgent || axiosConfig.headers?.["User-Agent"] || "Mozilla/5.0",
    "Accept-Language": deviceProfile?.locale || "de-DE,de;q=0.9,en;q=0.8",
    Accept: "application/json"
  };
  axiosConfig.validateStatus = (status) => status >= 200 && status < 500;

  const response = await axios.get("https://www.kleinanzeigen.de/m-access-token.json", axiosConfig);
  if (response.status === 401 || response.status === 403) {
    return { ok: false, status: response.status, reason: "AUTH_REQUIRED" };
  }
  const token = response.headers?.authorization || response.headers?.Authorization || "";
  if (!token) {
    return { ok: false, status: response.status, reason: "AUTH_REQUIRED" };
  }
  return { ok: true, status: response.status, token };
};

const validateCookies = async (rawCookieText, options = {}) => {
  const deviceProfile = options.deviceProfile || pickDeviceProfile();
  const cookies = normalizeCookies(filterKleinanzeigenCookies(parseCookies(rawCookieText)));
  const proxyServer = buildProxyServer(options.proxy);
  // Puppeteer/Chromium must receive a Chromium-compatible proxy URL (no `socks5h://`).
  const proxyUrl = buildPuppeteerProxyUrl(options.proxy);
  const proxyUrlForAgents = buildProxyUrl(options.proxy);
  const needsProxyChain = Boolean(
    proxyUrlForAgents && ((options.proxy?.type || "").toLowerCase().startsWith("socks") || options.proxy?.username || options.proxy?.password)
  );

  if (!cookies.length) {
    return {
      valid: false,
      reason: "Cookie файл пустой или не распознан",
      deviceProfile
    };
  }

  if (!options.proxy) {
    return {
      valid: false,
      reason: "Прокси не задан для проверки cookies.",
      deviceProfile
    };
  }

  if (PROXY_PREFLIGHT_ENABLED) {
    try {
      const connectionCheck = await proxyChecker.checkProxyConnection(options.proxy, "www.kleinanzeigen.de", 443);
      if (!connectionCheck.ok) {
        return {
          valid: false,
          reason: connectionCheck.error || "Прокси не отвечает",
          deviceProfile
        };
      }
      const axiosConfig = proxyChecker.buildAxiosConfig(options.proxy, PROXY_PREFLIGHT_TIMEOUT);
      const client = axios.create(axiosConfig);
      await client.get("https://www.kleinanzeigen.de/robots.txt", { timeout: PROXY_PREFLIGHT_TIMEOUT });
    } catch (error) {
      return {
        valid: false,
        reason: `Прокси не открывает kleinanzeigen.de: ${error.message || error}`,
        deviceProfile
      };
    }
  }

  // Primary validation: use the same token endpoint as the web app.
  // This avoids flaky Puppeteer-based checks and works reliably with token cookies.
  try {
    const cookieHeader = buildCookieHeader(cookies);
    const tokenCheck = await validateCookiesViaAccessToken({
      cookieHeader,
      proxy: options.proxy,
      deviceProfile,
      timeoutMs: 20000
    });
    if (tokenCheck.ok) {
      const profileFromCookies = extractProfileFromCookies(cookies);
      let profile = profileFromCookies;
      try {
        const profileFromHomepage = await fetchProfileFromHomepageViaHttp({
          cookies,
          proxy: options.proxy,
          deviceProfile,
          timeoutMs: 20000
        });
        // Homepage profile data is more trustworthy for own profile URL/userId than JWT payload fields.
        profile = mergeProfileData(profileFromHomepage, profileFromCookies);
      } catch (error) {
        // ignore homepage fallback errors
      }
      return {
        valid: true,
        reason: null,
        deviceProfile,
        profileName: profile.profileName || "",
        profileEmail: profile.profileEmail || "",
        profileUserId: profile.profileUserId || "",
        profileUrl: buildProfileUrlByUserId(profile.profileUserId) || profile.profileUrl || ""
      };
    }
  } catch (error) {
    // Fall back to Puppeteer validation below when the token endpoint is unreachable.
  }

  const runValidation = async (attempt = 0, useProxyChain = needsProxyChain) => {
    let localAnonymizedProxyUrl = "";
    let browser;
    const baseLaunchArgs = [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--lang=de-DE",
      "--disable-dev-shm-usage",
      "--no-zygote",
      "--disable-gpu"
    ];

    if (proxyServer) {
      if (useProxyChain) {
        localAnonymizedProxyUrl = await proxyChain.anonymizeProxy(proxyUrl);
        baseLaunchArgs.push(`--proxy-server=${localAnonymizedProxyUrl}`);
      } else {
        baseLaunchArgs.push(`--proxy-server=${proxyUrl}`);
      }
    }

    const profileDir = createTempProfileDir();
    try {
      browser = await puppeteer.launch({
        headless: "new",
        args: buildLaunchArgs(baseLaunchArgs, attempt),
        userDataDir: profileDir,
        timeout: PUPPETEER_LAUNCH_TIMEOUT,
        protocolTimeout: PUPPETEER_PROTOCOL_TIMEOUT
      });

      const page = await browser.newPage();
      page.setDefaultTimeout(PUPPETEER_NAV_TIMEOUT);
      page.setDefaultNavigationTimeout(PUPPETEER_NAV_TIMEOUT);
      const shouldAuthenticate = !localAnonymizedProxyUrl && useProxyChain === false ? false : !localAnonymizedProxyUrl && (options.proxy?.username || options.proxy?.password);
      if (shouldAuthenticate) {
        await page.authenticate({
          username: options.proxy.username || "",
          password: options.proxy.password || ""
        });
      }

      await page.setUserAgent(deviceProfile.userAgent);
      await page.setViewport(deviceProfile.viewport);
      await page.setExtraHTTPHeaders({
        "Accept-Language": deviceProfile.locale
      });
      await page.emulateTimezone(deviceProfile.timezone);
      await page.evaluateOnNewDocument((platform) => {
        Object.defineProperty(navigator, "platform", {
          get: () => platform
        });
      }, deviceProfile.platform);
      const context = browser.defaultBrowserContext();
      await context.overridePermissions("https://www.kleinanzeigen.de", ["geolocation"]);
      if (deviceProfile.geolocation) {
        await page.setGeolocation(deviceProfile.geolocation);
      }

      await gotoWithRetries(page, "https://www.kleinanzeigen.de/");
      await humanPause();
      await page.setCookie(...cookies);
      await humanPause();
      await gotoWithRetries(page, "https://www.kleinanzeigen.de/m-nachrichten.html");
      await humanPause(180, 320);

      const currentUrl = page.url();
      const content = await page.content();
      const safePath = (() => {
        try {
          return new URL(currentUrl).pathname || "";
        } catch (error) {
          return "";
        }
      })();
      const loggedIn =
        !currentUrl.includes("m-einloggen") &&
        (/Abmelden/i.test(content) || /Mein Konto/i.test(content) || /Nachrichten/i.test(content));

      const homeProfile = extractProfileFromHomepageHtml(content);
      let profileName = homeProfile.profileName || "";
      let profileEmail = homeProfile.profileEmail || "";
      let profileUserId = homeProfile.profileUserId || "";
      let profileUrl = homeProfile.profileUrl || "";
      if (loggedIn) {
        try {
          if (!profileEmail || !profileUrl) {
            await gotoWithRetries(page, "https://www.kleinanzeigen.de/");
            await humanPause(140, 240);
            const homepageHtml = await page.content();
            const homepageProfile = extractProfileFromHomepageHtml(homepageHtml);
            profileName = profileName || homepageProfile.profileName || "";
            profileEmail = profileEmail || homepageProfile.profileEmail || "";
            profileUserId = profileUserId || homepageProfile.profileUserId || "";
            profileUrl = profileUrl || homepageProfile.profileUrl || "";
          }
          if (!profileEmail || !profileUrl) {
            await gotoWithRetries(page, "https://www.kleinanzeigen.de/m-meine-anzeigen.html");
            await page.waitForSelector("[data-testid='ownprofile-header'] h2, h2.text-title2", { timeout: 15000 }).catch(() => {});
            await humanPause(160, 280);
            const profileData = await page.evaluate(() => {
              const normalize = (text) => (text || "").replace(/\s+/g, " ").trim();
              const header = document.querySelector("[data-testid='ownprofile-header']") || document.querySelector(".ownprofile-header");
              const heading = header ? header.querySelector("h2") : Array.from(document.querySelectorAll("h2")).find((node) => {
                const srOnly = node.querySelector("span.sr-only");
                return srOnly && /profil von/i.test(srOnly.textContent || "");
              });
              const emailSpan = document.querySelector("#user-email");
              const rawName = heading ? normalize(heading.textContent) : "";
              const name = rawName.replace(/profil von/i, "").trim();
              const emailRaw = emailSpan ? normalize(emailSpan.textContent) : "";
              const email = emailRaw.replace(/angemeldet als:\s*/i, "").trim();
              const ownProfileLink = Array.from(document.querySelectorAll("a[href*='s-bestandsliste'], a[href*='userId=']"))
                .map((node) => String(node.getAttribute("href") || "").trim())
                .find((href) => /userId=\d+/i.test(href) || /s-bestandsliste/i.test(href));
              const userIdMatch = ownProfileLink ? String(ownProfileLink).match(/userId=(\d+)/i) : null;
              const profileUserId = userIdMatch?.[1] || "";
              return { name, email, ownProfileLink: ownProfileLink || "", profileUserId };
            });
            profileName = sanitizeProfileName(profileData.name || profileName);
            profileEmail = pickEmailCandidate(profileData.email, profileEmail);
            profileUserId = pickUserIdCandidate(profileData.profileUserId, profileUserId);
            profileUrl = toAbsoluteKleinanzeigenUrl(profileData.ownProfileLink, page.url()) || profileUrl;
          }
        } catch (error) {
          // ignore profile parse errors
        }
      }

      return {
        valid: loggedIn,
        reason: loggedIn
          ? null
          : (
            currentUrl.includes("m-einloggen")
              ? `Kleinanzeigen перенаправил на логин (${safePath || "m-einloggen"}). Частые причины: cookies устарели или cookies получены не через тот же прокси/IP.`
              : currentUrl.includes("/gdpr")
                ? `Kleinanzeigen открыл страницу GDPR (${safePath || "/gdpr"}). Примите cookies/GDPR в браузере через этот же прокси и попробуйте снова.`
                : `Не удалось подтвердить вход в аккаунт (страница: ${safePath || "unknown"}).`
          ),
        deviceProfile,
        profileName,
        profileEmail,
        profileUserId,
        profileUrl: buildProfileUrlByUserId(profileUserId) || profileUrl || ""
      };
    } finally {
      if (browser) {
        await browser.close().catch(() => {});
      }
      if (localAnonymizedProxyUrl) {
        await proxyChain.closeAnonymizedProxy(localAnonymizedProxyUrl, true).catch(() => {});
      }
      fs.rmSync(profileDir, { recursive: true, force: true });
    }
  };

  const proxyType = String(options.proxy?.type || "").toLowerCase();
  const prefersDirect = proxyType.startsWith("socks");
  const strategies = [];
  if (prefersDirect) {
    strategies.push({ useProxyChain: false });
  }
  if (needsProxyChain) {
    strategies.push({ useProxyChain: true });
  }
  if (!prefersDirect) {
    strategies.push({ useProxyChain: false });
  }

  let lastError;
  for (let strategyIndex = 0; strategyIndex < strategies.length; strategyIndex += 1) {
    const { useProxyChain } = strategies[strategyIndex];
    const retries = RETRY_PROTOCOL_ERRORS ? MAX_PROTOCOL_RETRIES : 0;
    for (let attempt = 0; attempt <= retries; attempt += 1) {
      try {
        return await runValidation(attempt, useProxyChain);
      } catch (error) {
        lastError = error;
        if (!RETRY_PROTOCOL_ERRORS || !isProtocolTimeoutError(error) || attempt === retries) {
          break;
        }
        await sleep(800 + attempt * 800);
      }
    }
    if (!isProxyConnectionError(lastError)) {
      break;
    }
  }

  return {
    valid: false,
    reason: `Ошибка проверки cookies: ${lastError?.message || lastError || "unknown"}`,
    deviceProfile
  };
};

module.exports = {
  validateCookies,
  pickDeviceProfile
};
