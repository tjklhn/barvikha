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
const { parseCookies, normalizeCookie, buildProxyServer, buildProxyUrl } = require("./cookieUtils");
const createTempProfileDir = () => fs.mkdtempSync(path.join(os.tmpdir(), "kl-profile-"));
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const humanPause = (min = 120, max = 260) => sleep(Math.floor(min + Math.random() * (max - min)));

const sanitizeProfileName = (value) => {
  const normalized = (value || "").replace(/\s+/g, " ").trim();
  if (!normalized) return "";
  if (/mein(e)? anzeigen|mein profil|meine anzeigen|profil und meine anzeigen/i.test(normalized)) return "";
  return normalized;
};

const validateCookies = async (rawCookieText, options = {}) => {
  const deviceProfile = options.deviceProfile || pickDeviceProfile();
  const cookies = parseCookies(rawCookieText).map(normalizeCookie);
  const proxyServer = buildProxyServer(options.proxy);
  const proxyUrl = buildProxyUrl(options.proxy);
  const needsProxyChain = Boolean(
    proxyUrl && ((options.proxy?.type || "").toLowerCase().startsWith("socks") || options.proxy?.username || options.proxy?.password)
  );
  let anonymizedProxyUrl;

  if (!cookies.length) {
    return {
      valid: false,
      reason: "Cookie файл пустой или не распознан",
      deviceProfile
    };
  }

  const launchArgs = [
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--lang=de-DE"
  ];

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
    userDataDir: profileDir
  });

  try {
    const page = await browser.newPage();
    if (!anonymizedProxyUrl && (options.proxy?.username || options.proxy?.password)) {
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

    await page.goto("https://www.kleinanzeigen.de/", { waitUntil: "domcontentloaded" });
    await humanPause();
    await page.setCookie(...cookies);
    await humanPause();
    await page.goto("https://www.kleinanzeigen.de/m-nachrichten.html", {
      waitUntil: "domcontentloaded"
    });
    await humanPause(180, 320);

    const currentUrl = page.url();
    const content = await page.content();
    const loggedIn =
      !currentUrl.includes("m-einloggen") &&
      (/Abmelden/i.test(content) || /Mein Konto/i.test(content) || /Nachrichten/i.test(content));

    let profileName = "";
    let profileEmail = "";
    if (loggedIn) {
      try {
        await page.goto("https://www.kleinanzeigen.de/m-meine-anzeigen.html", { waitUntil: "domcontentloaded" });
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
          return { name, email };
        });
        profileName = sanitizeProfileName(profileData.name);
        profileEmail = profileData.email;
      } catch (error) {
        // ignore profile parse errors
      }
    }

    return {
      valid: loggedIn,
      reason: loggedIn ? null : "Kleinanzeigen перенаправил на логин",
      deviceProfile,
      profileName,
      profileEmail
    };
  } catch (error) {
    return {
      valid: false,
      reason: error.message,
      deviceProfile
    };
  } finally {
    await browser.close();
    if (anonymizedProxyUrl) {
      await proxyChain.closeAnonymizedProxy(anonymizedProxyUrl, true);
    }
    fs.rmSync(profileDir, { recursive: true, force: true });
  }
};

module.exports = {
  validateCookies,
  pickDeviceProfile
};
