const fs = require("fs");
const os = require("os");
const path = require("path");
const puppeteer = require("puppeteer-extra");
const StealthPlugin = require("puppeteer-extra-plugin-stealth");
const proxyChain = require("proxy-chain");
const { parseCookies, normalizeCookie, buildProxyServer, buildProxyUrl } = require("./cookieUtils");
const { pickDeviceProfile } = require("./cookieValidator");
const { updateAccount } = require("./db");
const PUPPETEER_PROTOCOL_TIMEOUT = Number(process.env.PUPPETEER_PROTOCOL_TIMEOUT || 120000);
const PUPPETEER_LAUNCH_TIMEOUT = Number(process.env.PUPPETEER_LAUNCH_TIMEOUT || 120000);
const PUPPETEER_NAV_TIMEOUT = Number(process.env.PUPPETEER_NAV_TIMEOUT || 60000);

puppeteer.use(StealthPlugin());

const sanitizeProfileName = (value) => {
  const normalized = (value || "").replace(/\s+/g, " ").trim();
  if (!normalized) return "";
  if (/mein(e)? anzeigen|mein profil|meine anzeigen|profil und meine anzeigen/i.test(normalized)) return "";
  return normalized;
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const humanPause = (min = 120, max = 240) => sleep(Math.floor(min + Math.random() * (max - min)));
const createTempProfileDir = () => fs.mkdtempSync(path.join(os.tmpdir(), "kl-profile-"));

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

const fetchAccountAds = async ({ account, proxy, accountLabel }) => {
  const deviceProfile = getDeviceProfile(account);
  const cookies = parseCookies(account.cookie).map(normalizeCookie);
  if (!cookies.length) return [];
  if (!proxy) return [];

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
    await page.setCookie(...cookies);
    await humanPause();
    await page.goto("https://www.kleinanzeigen.de/m-meine-anzeigen.html", { waitUntil: "domcontentloaded" });
    await humanPause(180, 360);
    await page.waitForSelector("[data-testid='ownprofile-header'] h2, h2.text-title2", { timeout: 15000 }).catch(() => {});

    const profileData = await page.evaluate(() => {
      const normalize = (text) => (text || "").replace(/\s+/g, " ").trim();
      const header = document.querySelector("[data-testid='ownprofile-header']") || document.querySelector(".ownprofile-header");
      const heading = header ? header.querySelector("h2") : Array.from(document.querySelectorAll("h2")).find((node) => {
        const srOnly = node.querySelector("span.sr-only");
        return srOnly && /profil von/i.test(srOnly.textContent || "");
      });
      const rawName = heading ? normalize(heading.textContent) : "";
      const name = rawName.replace(/profil von/i, "").trim();
      const postedAds = document.querySelector("[data-testid='posted-ads']");
      const postedText = postedAds ? normalize(postedAds.textContent) : "";
      const postedCountMatch = postedText.match(/(\d+)/);
      const postedCount = postedCountMatch ? Number(postedCountMatch[1]) : null;
      return { name, postedCount };
    });

    const safeProfileName = sanitizeProfileName(profileData?.name || "");
    if (safeProfileName) {
      updateAccount(account.id, { profileName: safeProfileName });
    }

    const ads = await page.evaluate(() => {
      const findMeineAnzeigenSection = () => {
        const heading = Array.from(document.querySelectorAll("h1, h2, h3"))
          .find((node) => /Meine Anzeigen/i.test(node.textContent || ""));
        if (!heading) return null;
        return heading.closest("section") || heading.parentElement;
      };

      const normalize = (text) => (text || "").replace(/\s+/g, " ").trim();
      const extractAdIdFromHref = (href) => {
        if (!href) return "";
        const match = href.match(/\/(\\d+)(?:-[^/]+)?$/);
        if (match) return match[1];
        const fallback = href.match(/(\\d{6,})/);
        return fallback ? fallback[1] : "";
      };
      const parsePrice = (card) => {
        if (!card) return "";
        const primaryPrice = card.querySelector("ul.list li.text-title3")
          || card.querySelector("[data-testid*='ad-price']")
          || card.querySelector("[data-qa*='ad-price']")
          || card.querySelector(".ad-price")
          || card.querySelector(".price");
        if (primaryPrice) return normalize(primaryPrice.textContent);
        const candidates = Array.from(card.querySelectorAll("span, div, li"))
          .map((node) => normalize(node.textContent))
          .filter((text) => text && /€|VB/i.test(text));
        return candidates[0] || "";
      };
      const parseMetrics = (card) => {
        if (!card) return { views: null, favorites: null };
        const texts = Array.from(card.querySelectorAll("li, span, div"))
          .map((node) => normalize(node.textContent))
          .filter(Boolean);
        let views = null;
        let favorites = null;
        texts.forEach((text) => {
          if (/besucher/i.test(text)) {
            const match = text.match(/(\d+)/);
            if (match) views = Number(match[1]);
          }
          if (/gemerkt/i.test(text)) {
            const match = text.match(/(\d+)/);
            if (match) favorites = Number(match[1]);
          }
        });
        return { views, favorites };
      };

      const extractImage = (node) => {
        if (!node) return "";
        const tag = node.tagName ? node.tagName.toLowerCase() : "";
        let src = "";
        if (tag === "img") {
          const srcset = node.getAttribute("srcset")
            || node.getAttribute("data-srcset")
            || node.getAttribute("data-lazy-srcset")
            || "";
          if (srcset) {
            src = srcset.split(",")[0].trim().split(" ")[0];
          }
          if (!src) {
            src = node.currentSrc || node.src
              || node.getAttribute("data-src")
              || node.getAttribute("data-lazy-src")
              || node.getAttribute("data-original")
              || "";
          }
        } else if (tag === "source") {
          const srcset = node.getAttribute("srcset")
            || node.getAttribute("data-srcset")
            || node.getAttribute("data-lazy-srcset")
            || "";
          if (srcset) src = srcset.split(",")[0].trim().split(" ")[0];
        } else {
          src = node.getAttribute("data-src")
            || node.getAttribute("data-lazy-src")
            || node.getAttribute("data-original")
            || node.getAttribute("data-bg")
            || node.getAttribute("data-background")
            || "";
          if (!src) {
            const style = node.getAttribute("style") || "";
            const match = style.match(/url\\([\"']?([^\"')]+)[\"']?\\)/i);
            if (match) src = match[1];
          }
        }
        if (src && src.includes(",")) {
          src = src.split(",")[0].trim().split(" ")[0];
        }
        if (src && !src.includes("data:image/") && !src.includes("placeholder")) {
          try {
            return new URL(src, window.location.origin).href;
          } catch (e) {
            return src;
          }
        }
        return "";
      };

      const pickImage = (card) => {
        const preferred = card.querySelector("img[data-testid*='ad-image']")
          || card.querySelector("img[alt]");
        if (preferred) {
          const src = extractImage(preferred);
          if (src) return src;
        }
        const imgNodes = Array.from(card.querySelectorAll("img"));
        for (const img of imgNodes) {
          const src = extractImage(img);
          if (src) return src;
        }
        const sourceNodes = Array.from(card.querySelectorAll("source"));
        for (const source of sourceNodes) {
          const src = extractImage(source);
          if (src) return src;
        }
        const bgNode = card.querySelector("[style*='background-image']")
          || card.querySelector("[data-bg]")
          || card.querySelector("[data-background]");
        if (bgNode) {
          const src = extractImage(bgNode);
          if (src) return src;
        }
        return "";
      };

      const section = findMeineAnzeigenSection();
      const scope = section || document;
      let cards = Array.from(scope.querySelectorAll("[data-testid='ad-card'], li[data-testid='ad-card']"));
      if (!cards.length) {
        const links = Array.from(scope.querySelectorAll("a[href*='/s-anzeige/']"));
        const containers = links.map((link) => link.closest("article, li, div")).filter(Boolean);
        const uniqueContainers = Array.from(new Set(containers));
        const leafContainers = uniqueContainers.filter(
          (node) => !uniqueContainers.some((other) => other !== node && node.contains(other))
        );
        cards = leafContainers.length ? leafContainers : uniqueContainers;
      }

      return cards.map((card) => {
        const adId = card.getAttribute("data-adid")
          || card.getAttribute("data-ad-id")
          || "";
        const linkEl = card.querySelector("a[href*='/s-anzeige/']");
        const hrefRaw = linkEl ? (linkEl.href || linkEl.getAttribute("href") || "") : "";
        let href = hrefRaw;
        if (hrefRaw) {
          try {
            href = new URL(hrefRaw, window.location.origin).href;
          } catch (e) {
            href = hrefRaw;
          }
        }
        if (!href || !/\/s-anzeige\//.test(href)) return null;
        if (!/\/s-anzeige\/.+\d/.test(href)) return null;

        const titleEl =
          card.querySelector("[data-testid*='ad-title']") ||
          card.querySelector("[data-qa*='ad-title']") ||
          card.querySelector(".ad-title") ||
          card.querySelector("h3 a") ||
          card.querySelector("h3") ||
          card.querySelector("h2");
        const priceText = parsePrice(card);
        const metrics = parseMetrics(card);
        const statusCandidates = Array.from(card.querySelectorAll("span, div, li"))
          .map((node) => normalize(node.textContent))
          .filter(Boolean);
        const statusRaw = statusCandidates.find((text) => /reserviert|gelösch|deleted|entfernt|inaktiv/i.test(text)) || "";
        let status = "Aktiv";
        if (/reserviert/i.test(statusRaw)) {
          status = "Reserviert";
        }
        if (/gelösch|deleted|entfernt/i.test(statusRaw)) {
          status = "Gelöscht";
        }
        const image = pickImage(card);
        let rawTitle = titleEl ? normalize(titleEl.textContent) : "";
        if (!rawTitle && linkEl) {
          rawTitle = normalize(linkEl.getAttribute("title") || linkEl.textContent || "");
        }
        if (!rawTitle && image) {
          const img = card.querySelector("img[alt]");
          rawTitle = img ? normalize(img.getAttribute("alt") || "") : "";
        }
        const title = rawTitle.replace(/^(Reserviert|Gelösch[^\s]*|Gelöscht|Inaktiv)\s*[•-]?\s*/i, "").trim();
        if (!title || title.length < 3 || /^\\d+$/.test(title)) return null;
        if (!/[A-Za-zÄÖÜäöüА-Яа-я]/.test(title)) return null;
        const resolvedAdId = adId || extractAdIdFromHref(href);
        return {
          adId: resolvedAdId || "",
          title,
          price: priceText,
          image,
          href,
          status,
          views: metrics.views,
          favorites: metrics.favorites
        };
      }).filter((item) => item && item.title && !/Meine Anzeigen|Profil von/i.test(item.title));
    });

    const deduped = [];
    const seen = new Set();
    for (const item of ads) {
      const keyBase = item.adId || item.href || `${item.title || ""}|${item.price || ""}`;
      const key = keyBase.trim().toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);
      deduped.push(item);
    }

    return deduped.map((item) => ({
      ...item,
      accountId: account.id,
      accountLabel
    }));
  } catch (error) {
    return [];
  } finally {
    await browser.close();
    if (anonymizedProxyUrl) {
      await proxyChain.closeAnonymizedProxy(anonymizedProxyUrl, true);
    }
    fs.rmSync(profileDir, { recursive: true, force: true });
  }
};

const fetchActiveAds = async ({ accounts, proxies, ownerContext = {} }) => {
  const results = [];
  for (const account of accounts) {
    if (!account.cookie) continue;
    const proxy = account.proxyId
      ? proxies.find((item) => item.id === account.proxyId)
      : null;
    const profileName = account.profileName || account.username || "Аккаунт";
    const profileEmail = account.profileEmail || "";
    const accountLabel = profileEmail ? `${profileName} (${profileEmail})` : profileName;
    const ads = await fetchAccountAds({ account, proxy, accountLabel });
    results.push(...ads);
  }
  const seen = new Set();
  const deduped = [];
  for (const item of results) {
    const keyBase = item.href || `${item.accountId || ""}|${item.title || ""}|${item.price || ""}|${item.image || ""}`;
    const key = keyBase.trim().toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(item);
  }
  return deduped;
};

const performAdAction = async ({
  account,
  proxy,
  adId,
  action,
  accountLabel,
  adHref,
  adTitle
}) => {
  if (!account?.cookie) {
    return { success: false, error: "AUTH_REQUIRED" };
  }
  if (!proxy) {
    return { success: false, error: "PROXY_REQUIRED" };
  }
  if (!adId) {
    return { success: false, error: "AD_ID_REQUIRED" };
  }
  const deviceProfile = getDeviceProfile(account);
  const cookies = parseCookies(account.cookie).map(normalizeCookie);
  if (!cookies.length) {
    return { success: false, error: "AUTH_REQUIRED" };
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
    await page.setCookie(...cookies);
    await humanPause();
    await page.goto("https://www.kleinanzeigen.de/m-meine-anzeigen.html", { waitUntil: "domcontentloaded" });
    await humanPause(200, 360);
    await page.evaluate(() => {
      const normalize = (text) => (text || "").replace(/\s+/g, " ").trim().toLowerCase();
      const isVisible = (node) => {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        if (style.display === "none" || style.visibility === "hidden" || style.opacity === "0") return false;
        const rect = node.getBoundingClientRect();
        return rect.width > 0 && rect.height > 0;
      };
      const buttons = Array.from(document.querySelectorAll("button, a, [role='tab']"));
      const tab = buttons.find((btn) => {
        const text = normalize(btn.textContent || btn.getAttribute("aria-label") || "");
        return text === "alle";
      });
      if (tab && isVisible(tab)) {
        tab.click();
      }
    });
    await humanPause(200, 360);
    await page.waitForSelector("[data-testid='ad-card'], #my-manageitems-adlist li", { timeout: 10000 }).catch(() => {});

    const actionResult = await page.evaluate(({ adId, action, adHref, adTitle }) => {
      const normalize = (text) => (text || "").replace(/\s+/g, " ").trim().toLowerCase();
      const isVisible = (node) => {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        if (style.display === "none" || style.visibility === "hidden" || style.opacity === "0") return false;
        const rect = node.getBoundingClientRect();
        return rect.width > 0 && rect.height > 0;
      };
      const normalizeHref = (value) => {
        if (!value) return "";
        try {
          const url = new URL(value, window.location.origin);
          return `${url.pathname}`.toLowerCase();
        } catch (error) {
          return String(value).toLowerCase();
        }
      };
      const targetHref = normalizeHref(adHref);
      const targetTitle = normalize(adTitle);
      const findCard = () => {
        const byId = document.querySelector(`[data-adid="${adId}"], [data-ad-id="${adId}"]`);
        if (byId) return byId;
        let link = document.querySelector(`a[href*='/${adId}-'], a[href*='/${adId}/'], a[href*='=${adId}']`);
        if (!link && targetHref) {
          const anchors = Array.from(document.querySelectorAll("a[href*='/s-anzeige/']"));
          link = anchors.find((anchor) => normalizeHref(anchor.getAttribute("href")) === targetHref)
            || anchors.find((anchor) => normalizeHref(anchor.getAttribute("href")).includes(targetHref));
        }
        if (link) {
          return link.closest("[data-testid='ad-card'], li, article, div") || link.parentElement;
        }
        if (targetTitle) {
          const cards = Array.from(document.querySelectorAll("[data-testid='ad-card'], li, article, div"));
          const match = cards.find((card) => normalize(card.textContent || "").includes(targetTitle));
          if (match) return match;
        }
        return null;
      };
      let card = findCard();
      if (!card && adId) {
        card = document.querySelector(`[data-adid="${adId}"], [data-ad-id="${adId}"]`);
      }
      if (!card) {
        return { ok: false, reason: "AD_NOT_FOUND" };
      }

      const findActionButton = (labels) => {
        const buttons = Array.from(card.querySelectorAll("button, [role='button'], a"));
        for (const button of buttons) {
          if (!isVisible(button)) continue;
          const text = normalize(button.textContent || button.getAttribute("aria-label") || "");
          if (!text) continue;
          if (labels.some((label) => text.includes(label))) {
            return button;
          }
        }
        return null;
      };

      if (action === "reserve") {
        const reserveButton = findActionButton(["reservieren"]);
        if (!reserveButton) {
          return { ok: false, reason: "RESERVE_BUTTON_NOT_FOUND" };
        }
        try { reserveButton.scrollIntoView({ block: "center", inline: "center" }); } catch (e) {}
        reserveButton.click();
        return { ok: true };
      }

      if (action === "activate") {
        const activateButton = findActionButton(["aktivieren"]);
        if (!activateButton) {
          return { ok: false, reason: "ACTIVATE_BUTTON_NOT_FOUND" };
        }
        try { activateButton.scrollIntoView({ block: "center", inline: "center" }); } catch (e) {}
        activateButton.click();
        return { ok: true };
      }

      if (action === "delete") {
        const deleteButton = findActionButton(["löschen", "loeschen", "anzeigen löschen", "anzeige löschen"]);
        if (!deleteButton) {
          return { ok: false, reason: "DELETE_BUTTON_NOT_FOUND" };
        }
        try { deleteButton.scrollIntoView({ block: "center", inline: "center" }); } catch (e) {}
        deleteButton.click();
        return { ok: true };
      }

      return { ok: false, reason: "UNKNOWN_ACTION" };
    }, { adId: String(adId), action });

    if (!actionResult?.ok) {
      return { success: false, error: actionResult?.reason || "ACTION_FAILED" };
    }

    await humanPause(200, 400);

    if (action === "delete") {
      // Wait for either a modal dialog or navigation to delete page
      await Promise.race([
        page.waitForNavigation({ waitUntil: "domcontentloaded", timeout: 6000 }).catch(() => null),
        page.waitForFunction(() => {
          return Boolean(
            document.querySelector("[role='dialog'], [data-testid*='modal'], [data-qa*='modal'], .modal, .dialog, .overlay")
          );
        }, { timeout: 6000 }).catch(() => null)
      ]);

      const confirmClicked = await page.evaluate(() => {
        const normalize = (text) => (text || "").replace(/\s+/g, " ").trim().toLowerCase();
        const isVisible = (node) => {
          if (!node) return false;
          const style = window.getComputedStyle(node);
          if (style.display === "none" || style.visibility === "hidden" || style.opacity === "0") return false;
          const rect = node.getBoundingClientRect();
          return rect.width > 0 && rect.height > 0;
        };

        const dialogSelectors = [
          "[role='dialog']",
          "[data-testid*='modal']",
          "[data-qa*='modal']",
          ".modal",
          ".dialog",
          ".overlay"
        ];
        const dialogs = dialogSelectors
          .map((selector) => Array.from(document.querySelectorAll(selector)))
          .flat()
          .filter(Boolean);

        const roots = dialogs.length ? dialogs : [document];

        // If there is a reason selection, pick the first available option
        roots.forEach((root) => {
          const radio = root.querySelector("input[type='radio'], input[type='checkbox']");
          if (radio && isVisible(radio)) {
            radio.click();
            radio.dispatchEvent(new Event("change", { bubbles: true }));
          }
        });

        const clickByLabels = (labels) => {
          for (const root of roots) {
            const buttons = Array.from(root.querySelectorAll("button, [role='button'], input[type='submit']"));
            for (const button of buttons) {
              if (!isVisible(button)) continue;
              const text = normalize(button.textContent || button.value || button.getAttribute("aria-label") || "");
              if (labels.some((label) => text.includes(label))) {
                button.click();
                return true;
              }
            }
          }
          return false;
        };

        // First try "Weiter"/"Bestätigen", then final "Löschen"
        const stepOne = clickByLabels(["weiter", "bestätigen", "bestaetigen", "ok", "ja"]);
        if (stepOne) return true;
        return clickByLabels(["löschen", "loeschen", "entfernen"]);
      });

      if (confirmClicked) {
        await humanPause(250, 520);
        // Sometimes there is a second confirmation layer
        await page.evaluate(() => {
          const normalize = (text) => (text || "").replace(/\s+/g, " ").trim().toLowerCase();
          const buttons = Array.from(document.querySelectorAll("button, [role='button'], input[type='submit']"));
          const target = buttons.find((btn) => {
            const text = normalize(btn.textContent || btn.value || btn.getAttribute("aria-label") || "");
            return text.includes("löschen") || text.includes("loeschen") || text.includes("entfernen");
          });
          if (target) target.click();
        });
        await humanPause(250, 520);
      }
    }

    const verify = await page.evaluate(({ adId, action, adHref, adTitle }) => {
      const normalize = (text) => (text || "").replace(/\s+/g, " ").trim().toLowerCase();
      const normalizeHref = (value) => {
        if (!value) return "";
        try {
          const url = new URL(value, window.location.origin);
          return `${url.pathname}`.toLowerCase();
        } catch (error) {
          return String(value).toLowerCase();
        }
      };
      const targetHref = normalizeHref(adHref);
      const targetTitle = normalize(adTitle);
      let card = document.querySelector(`[data-adid="${adId}"], [data-ad-id="${adId}"]`)
        || document.querySelector(`a[href*='/${adId}-'], a[href*='/${adId}/']`)?.closest("[data-testid='ad-card'], li, article, div");
      if (!card && targetHref) {
        const anchors = Array.from(document.querySelectorAll("a[href*='/s-anzeige/']"));
        const link = anchors.find((anchor) => normalizeHref(anchor.getAttribute("href")) === targetHref)
          || anchors.find((anchor) => normalizeHref(anchor.getAttribute("href")).includes(targetHref));
        if (link) {
          card = link.closest("[data-testid='ad-card'], li, article, div");
        }
      }
      if (!card && targetTitle) {
        const cards = Array.from(document.querySelectorAll("[data-testid='ad-card'], li, article, div"));
        card = cards.find((candidate) => normalize(candidate.textContent || "").includes(targetTitle));
      }
      if (!card) {
        return { ok: action === "delete", removed: true };
      }
      const text = normalize(card.textContent || "");
      const buttonTexts = Array.from(card.querySelectorAll("button, a"))
        .map((btn) => normalize(btn.textContent || ""));
      if (action === "reserve") {
        const hasReservedText = text.includes("reserviert");
        const hasActivateButton = buttonTexts.some((btnText) => btnText.includes("aktivieren"));
        return { ok: hasReservedText || hasActivateButton, removed: false };
      }
      if (action === "activate") {
        const hasActiveText = text.includes("aktiv");
        const hasReserveButton = buttonTexts.some((btnText) => btnText.includes("reservieren"));
        return { ok: hasActiveText || hasReserveButton, removed: false };
      }
      if (action === "delete") {
        return { ok: text.includes("gelösch") || text.includes("deleted") || text.includes("entfernt"), removed: false };
      }
      return { ok: true };
    }, { adId: String(adId), action });

    const confirmed = Boolean(verify?.ok);
    if (!confirmed && (action === "reserve" || action === "activate")) {
      return {
        success: true,
        confirmed: false,
        removed: Boolean(verify?.removed),
        message: "ACTION_PENDING"
      };
    }

    return {
      success: confirmed,
      confirmed,
      removed: Boolean(verify?.removed),
      message: confirmed ? "OK" : "ACTION_NOT_CONFIRMED"
    };
  } catch (error) {
    return { success: false, error: error?.message || "ACTION_FAILED" };
  } finally {
    await browser.close();
    if (anonymizedProxyUrl) {
      await proxyChain.closeAnonymizedProxy(anonymizedProxyUrl, true);
    }
    fs.rmSync(profileDir, { recursive: true, force: true });
  }
};

module.exports = {
  fetchActiveAds,
  performAdAction
};
