const fs = require("fs");
const os = require("os");
const path = require("path");
const puppeteer = require("puppeteer-extra");
const StealthPlugin = require("puppeteer-extra-plugin-stealth");
const proxyChain = require("proxy-chain");
const { parseCookies, normalizeCookies, normalizeCookie, buildProxyServer, buildProxyUrl, buildPuppeteerProxyUrl } = require("./cookieUtils");
const { pickDeviceProfile } = require("./cookieValidator");

puppeteer.use(StealthPlugin());

const CREATE_AD_URL = "https://www.kleinanzeigen.de/p-anzeige-aufgeben-schritt2.html";
const DEBUG_PUBLISH = process.env.KL_DEBUG_PUBLISH === "1";
let publishDebugOverride = false;
const isPublishDebugEnabled = () => DEBUG_PUBLISH || publishDebugOverride;
const CATEGORY_SELECTION_NEW_PAGE = process.env.KL_CATEGORY_SELECTION_NEW_PAGE === "1";
const PUBLISH_FLOW_VERSION = "2026-02-07-v5";
const PUPPETEER_PROTOCOL_TIMEOUT = Number(process.env.PUPPETEER_PROTOCOL_TIMEOUT || 120000);
const PUPPETEER_LAUNCH_TIMEOUT = Number(process.env.PUPPETEER_LAUNCH_TIMEOUT || 120000);
const PUPPETEER_NAV_TIMEOUT = Number(process.env.PUPPETEER_NAV_TIMEOUT || 60000);

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const humanPause = (min = 120, max = 280) => sleep(Math.floor(min + Math.random() * (max - min)));
const createTempProfileDir = () => fs.mkdtempSync(path.join(os.tmpdir(), "kl-profile-"));
const withTimeout = async (promise, timeoutMs, label = "timeout") => {
  let timeoutId;
  const timeoutPromise = new Promise((_, reject) => {
    timeoutId = setTimeout(() => reject(new Error(label)), timeoutMs);
  });
  try {
    return await Promise.race([promise, timeoutPromise]);
  } finally {
    clearTimeout(timeoutId);
  }
};
const categoriesPath = path.join(__dirname, "..", "data", "categories.json");
const categoryChildrenCachePath = path.join(__dirname, "..", "data", "category-children.json");

const extractCategoryIdFromUrl = (url) => {
  if (!url) return "";
  const match = url.match(/\/c(\d+)(?:\/|$)/);
  if (match) return match[1];
  const trailing = url.match(/(\d+)(?:\/|$)/);
  return trailing ? trailing[1] : "";
};

const normalizeCategoryPathInput = (value) => {
  if (!value) return [];
  if (Array.isArray(value)) {
    return value.map((item) => String(item || "").trim()).filter(Boolean);
  }
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) return [];
    try {
      const parsed = JSON.parse(trimmed);
      if (Array.isArray(parsed)) {
        return parsed.map((item) => String(item || "").trim()).filter(Boolean);
      }
    } catch (error) {
      // ignore JSON parse errors
    }
    return trimmed.split(/[>\\/|,]/).map((item) => item.trim()).filter(Boolean);
  }
  return [];
};

const extractCategoryIdFromPathItem = (item) => {
  const raw = String(item || "").trim();
  if (!raw) return "";
  if (/^\d+$/.test(raw)) return raw;
  const fromUrl = extractCategoryIdFromUrl(raw);
  if (fromUrl) return fromUrl;
  const fallback = raw.match(/(\d{2,})/);
  return fallback ? fallback[1] : "";
};

const normalizeCategoryPathIds = (pathInput) => {
  const rawItems = normalizeCategoryPathInput(pathInput);
  const result = [];
  for (const item of rawItems) {
    const id = extractCategoryIdFromPathItem(item);
    if (!id) continue;
    if (result.length && result[result.length - 1] === id) continue;
    result.push(id);
  }
  return result;
};

const normalizeCategoryId = (value) => {
  const raw = String(value || "").trim();
  return /^\d+$/.test(raw) ? raw : "";
};

const normalizeSelectionUrl = (value) => {
  if (!value) return "";
  try {
    return new URL(String(value), "https://www.kleinanzeigen.de").toString();
  } catch (error) {
    return String(value);
  }
};

const readJsonSafe = (filePath) => {
  try {
    if (!fs.existsSync(filePath)) return null;
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch (error) {
    return null;
  }
};

const findPathById = (nodes, targetId, current = []) => {
  if (!Array.isArray(nodes)) return null;
  for (const node of nodes) {
    const id = String(node?.id || "");
    if (!id) continue;
    const next = [...current, id];
    if (id === targetId) return next;
    const childPath = findPathById(node?.children || [], targetId, next);
    if (childPath) return childPath;
  }
  return null;
};

const findNodeById = (nodes, targetId) => {
  if (!Array.isArray(nodes)) return null;
  for (const node of nodes) {
    const id = String(node?.id || "");
    if (id && id === targetId) return node;
    const child = findNodeById(node?.children || [], targetId);
    if (child) return child;
  }
  return null;
};

const resolveCategoryPathFromCache = (targetId) => {
  const normalizedId = normalizeCategoryId(targetId);
  if (!normalizedId) return null;
  const categories = readJsonSafe(categoriesPath);
  const nodes = categories?.categories || categories || [];
  const directPath = findPathById(nodes, normalizedId);
  if (directPath) return directPath;

  const childrenCache = readJsonSafe(categoryChildrenCachePath);
  const items = childrenCache?.items || {};
  let parentId = "";
  for (const [key, value] of Object.entries(items)) {
    const children = value?.children || [];
    if (children.some((child) => String(child?.id || "") === normalizedId)) {
      const match = key.match(/id:(\d+)/);
      if (match) {
        parentId = match[1];
      } else if (key.startsWith("id:")) {
        parentId = key.slice(3);
      }
      break;
    }
  }
  if (parentId) {
    const parentPath = findPathById(nodes, parentId);
    if (parentPath) return [...parentPath, normalizedId];
  }
  return null;
};

const resolveCategoryNameFromCache = (targetId) => {
  const normalizedId = normalizeCategoryId(targetId);
  if (!normalizedId) return "";
  const categories = readJsonSafe(categoriesPath);
  const nodes = categories?.categories || categories || [];
  const node = findNodeById(nodes, normalizedId);
  return node?.name || "";
};

const getCategorySelectionUrl = (categoryId, categoryUrl) => {
  if (categoryUrl && /p-kategorie-aendern/i.test(categoryUrl)) {
    return categoryUrl;
  }
  const resolvedId = normalizeCategoryId(categoryId) || extractCategoryIdFromUrl(categoryUrl);
  const path = resolveCategoryPathFromCache(resolvedId);
  if (path && path.length) {
    return `https://www.kleinanzeigen.de/p-kategorie-aendern.html?path=${encodeURIComponent(path.join("/"))}`;
  }
  if (resolvedId) {
    return `https://www.kleinanzeigen.de/p-kategorie-aendern.html?path=${encodeURIComponent(resolvedId)}`;
  }
  return "";
};

const collectPreferredFieldValues = ({ ad, categoryPathIds = [] } = {}) => {
  const values = [];
  const seen = new Set();
  const push = (value) => {
    const token = String(value || "").trim().toLowerCase();
    if (!token || seen.has(token)) return;
    seen.add(token);
    values.push(token);
  };

  (Array.isArray(categoryPathIds) ? categoryPathIds : []).forEach(push);
  normalizeCategoryPathInput(ad?.categoryPath).forEach(push);

  const categoryUrl = String(ad?.categoryUrl || "");
  categoryUrl
    .split(/[/?&#=._:-]+/)
    .map((part) => part.trim().toLowerCase())
    .filter(Boolean)
    .forEach(push);

  const extraFields = ad?.extraFields;
  if (Array.isArray(extraFields)) {
    extraFields.forEach((entry) => {
      if (!entry) return;
      push(entry.name);
      push(entry.value);
      push(entry.label);
    });
  } else if (extraFields && typeof extraFields === "object") {
    Object.entries(extraFields).forEach(([key, value]) => {
      push(key);
      push(value);
    });
  }

  return values;
};

const selectCategoryPathOnSelectionPage = async (page, pathIds = []) => {
  if (!pathIds.length) return false;
  try {
    await page.waitForFunction(() => {
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
      return roots.some((root) => {
        try {
          return Boolean(
            root.querySelector("#postad-step1-frm")
            || root.querySelector(".category-selection-list")
            || root.querySelector("#postad-category-select-box")
          );
        } catch (error) {
          return false;
        }
      });
    }, { timeout: 12000 });
  } catch (error) {
    // continue even if list is not yet visible
  }

  const buildSelectors = (id) => ([
    `#cat_${id}`,
    `a#cat_${id}`,
    `a[href^="#?path=${id}"]`,
    `a[href*="path=${id}"]`,
    `a[data-id="${id}"]`,
    `[data-category-id="${id}"]`,
    `a[href*="/c${id}"]`,
    `a[href*="c${id}"]`
  ]);

  for (let index = 0; index < pathIds.length; index += 1) {
    const id = normalizeCategoryId(pathIds[index]);
    if (!id) return false;
    const selectors = buildSelectors(id);
    let clicked = false;
    try {
      await page.waitForFunction((selectorList) => {
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
        const selectors = selectorList.split(",");
        return roots.some((root) => selectors.some((selector) => {
          try {
            return Boolean(root.querySelector(selector));
          } catch (error) {
            return false;
          }
        }));
      }, { timeout: 10000 }, selectors.join(","));
    } catch (error) {
      // ignore selector timeout, fallback to text match
    }
    clicked = await page.evaluate((selectorList) => {
      const isVisible = (node) => {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        if (style.display === "none" || style.visibility === "hidden") return false;
        const rect = node.getBoundingClientRect();
        return rect.width > 0 && rect.height > 0;
      };
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
      const selectors = selectorList.split(",");
      for (const root of roots) {
        for (const selector of selectors) {
          let candidate = null;
          try {
            candidate = root.querySelector(selector);
          } catch (error) {
            candidate = null;
          }
          if (candidate && isVisible(candidate)) {
            candidate.click();
            return true;
          }
        }
      }
      return false;
    }, selectors.join(","));

    if (!clicked) {
      const name = resolveCategoryNameFromCache(id);
      if (name) {
        clicked = await clickByText(page, [name]);
        if (!clicked) {
          clicked = await page.evaluate((label) => {
            const normalize = (value) => (value || "").replace(/\s+/g, " ").trim().toLowerCase();
            const target = normalize(label);
            const isVisible = (node) => {
              if (!node) return false;
              const style = window.getComputedStyle(node);
              if (style.display === "none" || style.visibility === "hidden") return false;
              const rect = node.getBoundingClientRect();
              return rect.width > 0 && rect.height > 0;
            };
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
            const tags = ["a", "button", "[role='button']", "[role='option']", "li", "div", "span"];
            const candidates = [];
            const fallback = [];
            roots.forEach((root) => {
              const nodes = Array.from(root.querySelectorAll(tags.join(","))).filter((node) => isVisible(node));
              nodes.forEach((node) => {
                const text = normalize(node.textContent);
                if (!text) return;
                if (text === target) {
                  candidates.push({ node, text });
                } else if (text.includes(target)) {
                  fallback.push({ node, text });
                }
              });
            });
            const pick = candidates[0] || fallback.sort((a, b) => a.text.length - b.text.length)[0];
            if (!pick?.node) return false;
            pick.node.click();
            return true;
          }, name);
        }
      }
    }
    if (!clicked) return false;

    await humanPause(200, 400);
    try {
      await Promise.race([
        page.waitForNavigation({ waitUntil: "domcontentloaded", timeout: 8000 }),
        page.waitForTimeout(600)
      ]);
    } catch (error) {
      // ignore navigation timeout between steps
    }

    const nextId = normalizeCategoryId(pathIds[index + 1]);
    if (nextId) {
      const nextSelectorList = buildSelectors(nextId).join(",");
      try {
        await page.waitForSelector(nextSelectorList, { timeout: 10000 });
      } catch (error) {
        // continue even if next level takes longer
      }
    }
  }
  return true;
};

const waitForCategorySelectionReady = async (page, timeout = 15000) => {
  try {
    await page.waitForFunction(() => {
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
      const selectList = (root) =>
        root.querySelectorAll(".category-selection-list-item-link, [id^='cat_'], [data-category-id], a[href*='path=']");
      const listReady = roots.some((root) => {
        try {
          const items = selectList(root);
          return items && items.length > 0;
        } catch (error) {
          return false;
        }
      });
      const form = roots.find((root) => {
        try {
          return root.querySelector("#postad-step1-frm")
            || root.querySelector("form[action*='p-kategorie-aendern']")
            || root.querySelector("form");
        } catch (error) {
          return false;
        }
      });
      const tree = window.Belen?.PostAd?.CategorySelectView?.categoryTree
        || window.Belen?.PostAd?.CategorySelectView?.model?.categoryTree
        || window.Belen?.PostAd?.CategorySelectView?.options?.categoryTree
        || window.categoryTree;
      return Boolean(form && (listReady || tree));
    }, { timeout });
    return true;
  } catch (error) {
    return false;
  }
};

const isCategorySelectionPageReady = async (page) => {
  try {
    return await page.evaluate(() => {
      if (window.location.href.includes("p-kategorie-aendern")) return true;
      const title = document.title || "";
      if (/kategorie\s+ausw/i.test(title)) return true;
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
      for (const root of roots) {
        try {
          const form = root.querySelector("#postad-step1-frm");
          if (form) return true;
          const listItems = root.querySelectorAll(
            ".category-selection-list-item-link, [id^='cat_'], [data-category-id], a[href*='path=']"
          );
          if (listItems && listItems.length) return true;
        } catch (error) {
          // ignore root errors
        }
      }
      return false;
    });
  } catch (error) {
    return false;
  }
};

const hasCategoryTree = async (page) => {
  try {
    return await page.evaluate(() => Boolean(
      window.Belen?.PostAd?.CategorySelectView?.categoryTree
        || window.Belen?.PostAd?.CategorySelectView?.model?.categoryTree
        || window.Belen?.PostAd?.CategorySelectView?.options?.categoryTree
        || window.categoryTree
    ));
  } catch (error) {
    return false;
  }
};

const applyCategoryPathViaTree = async (page, pathIds = []) => {
  if (!pathIds.length) return { success: false, reason: "empty-path" };
  try {
    return await page.evaluate((ids) => {
      const normalize = (value) => String(value || "").trim().toLowerCase();
      const targetIds = ids.map(normalize).filter(Boolean);
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

      const applyFallback = () => {
        const numericIds = targetIds.filter((id) => /^\d+$/.test(id));
        if (!numericIds.length) {
          return { success: false, reason: "non-numeric-path", targetIds, numericOnly: false };
        }
        if (numericIds.length > 1) {
          applyField("parentCategoryId", numericIds[0]);
        }
        applyField("categoryId", numericIds[numericIds.length - 1]);
        applyField("submitted", "true");
        form.submit();
        return {
          success: true,
          applied: [
            numericIds.length > 1 ? { fieldName: "parentCategoryId", fieldValue: numericIds[0] } : null,
            { fieldName: "categoryId", fieldValue: numericIds[numericIds.length - 1] },
            { fieldName: "submitted", fieldValue: "true" }
          ].filter(Boolean),
          fallback: true,
          submitted: true,
          targetIds,
          numericOnly: numericIds.length === targetIds.length
        };
      };

      const getTree = () => {
        const view = window.Belen?.PostAd?.CategorySelectView;
        return view?.categoryTree || view?.model?.categoryTree || view?.options?.categoryTree || window.categoryTree || null;
      };

      const tree = getTree();
      if (!tree) return applyFallback();

      const findPath = (node, remaining, acc = []) => {
        if (!node || !remaining.length) return null;
        const [currentId, ...rest] = remaining;
        if (normalize(node.identifier) === currentId) {
          const nextAcc = [...acc, node];
          if (!rest.length) return nextAcc;
          const children = Array.isArray(node.children) ? node.children : [];
          for (const child of children) {
            const childPath = findPath(child, rest, nextAcc);
            if (childPath) return childPath;
          }
          return null;
        }
        const children = Array.isArray(node.children) ? node.children : [];
        for (const child of children) {
          const childPath = findPath(child, remaining, acc);
          if (childPath) return childPath;
        }
        return null;
      };

      const pathNodes = findPath(tree, targetIds);
      if (!pathNodes) return applyFallback();

      const applied = [];
      pathNodes.forEach((node) => {
        const fieldName = node?.fieldName;
        const fieldValue = node?.fieldValue;
        if (!fieldName) return;
        applyField(fieldName, fieldValue ?? node?.identifier ?? "");
        applied.push({ fieldName, fieldValue: fieldValue ?? node?.identifier ?? "" });
      });
      applyField("submitted", "true");
      applied.push({ fieldName: "submitted", fieldValue: "true" });

      form.submit();
      return { success: true, applied, submitted: true };
    }, pathIds);
  } catch (error) {
    const message = error?.message || "";
    if (/Execution context was destroyed|Cannot find context with specified id|navigation/i.test(message)) {
      return { success: true, submitted: true, navigationLikely: true };
    }
    return { success: false, reason: message };
  }
};

const sanitizeFilename = (value) => (value || "account")
  .toString()
  .replace(/[^a-z0-9._-]+/gi, "_")
  .slice(0, 60);

const appendPublishTrace = (payload) => {
  if (!isPublishDebugEnabled()) return;
  try {
    const debugDir = path.join(__dirname, "..", "data", "debug");
    if (!fs.existsSync(debugDir)) {
      fs.mkdirSync(debugDir, { recursive: true });
    }
    const entry = {
      ts: new Date().toISOString(),
      ...payload
    };
    fs.appendFileSync(path.join(debugDir, "publish-steps.log"), JSON.stringify(entry) + "\n", "utf8");
  } catch (error) {
    // ignore trace failures
  }
};

const dumpPublishDebug = async (page, { accountLabel = "account", step = "unknown", error = "", extra = {} } = {}) => {
  if (!isPublishDebugEnabled() || !page) return;
  try {
    const debugDir = path.join(__dirname, "..", "data", "debug");
    if (!fs.existsSync(debugDir)) {
      fs.mkdirSync(debugDir, { recursive: true });
    }

    const safeLabel = sanitizeFilename(accountLabel);
    const safeStep = sanitizeFilename(step);
    const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
    const base = `publish-${safeLabel}-${safeStep}-${timestamp}`;
    const htmlPath = path.join(debugDir, `${base}.html`);
    const screenshotPath = path.join(debugDir, `${base}.png`);
    const metaPath = path.join(debugDir, `${base}.json`);

    const [html, meta] = await Promise.all([
      withTimeout(page.content().catch(() => ""), 8000, "debug-content-timeout").catch(() => ""),
      withTimeout(page.evaluate(() => ({
        url: window.location.href,
        title: document.title,
        bodyTextSample: (document.body?.innerText || "").slice(0, 2000)
      })).catch(() => ({})), 8000, "debug-meta-timeout").catch(() => ({}))
    ]);

    if (html) {
      fs.writeFileSync(htmlPath, html, "utf8");
    }
    await withTimeout(
      page.screenshot({ path: screenshotPath, fullPage: true }).catch(() => {}),
      8000,
      "debug-screenshot-timeout"
    ).catch(() => {});
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

    console.log(`[publishAd] Debug saved: ${metaPath}`);
    appendPublishTrace({ step: "debug-saved", label: step, metaPath });
  } catch (dumpError) {
    console.log(`[publishAd] Debug dump failed: ${dumpError.message}`);
  }
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

const setValue = async (context, selector, value) => {
  await context.evaluate(
    ({ selector, value }) => {
      const element = document.querySelector(selector);
      if (!element) return;
      element.focus();
      element.value = value;
      element.dispatchEvent(new Event("input", { bubbles: true }));
      element.dispatchEvent(new Event("change", { bubbles: true }));
    },
    { selector, value }
  );
};

const setValueOnHandle = async (handle, value) => {
  if (!handle) return false;
  await handle.evaluate((node, nextValue) => {
    try {
      node.focus();
    } catch (error) {
      // ignore focus errors
    }
    node.value = nextValue;
    node.dispatchEvent(new Event("input", { bubbles: true }));
    node.dispatchEvent(new Event("change", { bubbles: true }));
  }, value);
  return true;
};

const isHandleVisible = async (handle) => {
  if (!handle) return false;
  try {
    const box = await handle.boundingBox();
    if (!box) return false;
    return box.width > 0 && box.height > 0;
  } catch (error) {
    return false;
  }
};

const isBlankValue = (value) => value === undefined || value === null || value === "";

const setValueIfExists = async (context, selector, value) => {
  if (isBlankValue(value)) return false;
  const element = await context.$(selector);
  if (!element) return false;
  await setValue(context, selector, value);
  return true;
};

const scrollIntoView = async (element) => {
  if (!element) return;
  await element.evaluate((node) => {
    node.scrollIntoView({ block: "center", inline: "center", behavior: "instant" });
  });
};

const safeClick = async (element) => {
  if (!element) return false;
  try {
    await scrollIntoView(element);
    await element.click({ delay: 60 });
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

const fillField = async (context, selectors, value) => {
  if (isBlankValue(value)) return false;
  for (const selector of selectors) {
    let element = null;
    try {
      element = await context.waitForSelector(selector, { visible: true, timeout: 2000 });
    } catch (error) {
      element = await context.$(selector);
    }
    if (element) {
      await scrollIntoView(element);
      try {
        await element.click({ clickCount: 3, delay: 40 });
      } catch (error) {
        await element.evaluate((node) => node.click());
      }
      try {
        await element.type(String(value), { delay: 40 + Math.floor(Math.random() * 40) });
      } catch (error) {
        await setValue(context, selector, value);
      }
      await humanPause();
      return true;
    }
  }
  return false;
};

const fillByLabel = async (context, labelTexts, value) => {
  if (isBlankValue(value)) return false;
  for (const labelText of labelTexts) {
    const labels = await context.$$(`xpath//label[contains(normalize-space(.), "${labelText}")]`);
    if (!labels.length) continue;
    for (const label of labels) {
      const forId = await label.evaluate((node) => node.getAttribute("for"));
      if (forId) {
        const selector = `#${forId}`;
        const exists = await context.$(selector);
        if (exists) {
          await scrollIntoView(exists);
          await exists.click({ clickCount: 3, delay: 40 });
          await setValue(context, selector, value);
          return true;
        }
      }
      const input = await label.$("input, textarea");
      if (input) {
        const selector = await input.evaluate((node) => node.tagName.toLowerCase() + (node.id ? `#${node.id}` : ""));
        await scrollIntoView(input);
        try {
          await input.click({ clickCount: 3, delay: 40 });
        } catch (error) {
          await input.evaluate((node) => node.click());
        }
        if (selector) {
          try {
            await input.type(String(value), { delay: 40 + Math.floor(Math.random() * 40) });
          } catch (error) {
            await setValue(context, selector, value);
          }
        } else {
          await input.type(value);
        }
        await humanPause();
        return true;
      }
    }
  }
  return false;
};

const fillFieldAcrossContexts = async (page, selectors, value) => {
  for (const context of [page, ...page.frames()]) {
    try {
      if (await fillField(context, selectors, value)) return true;
    } catch (error) {
      // ignore cross-origin frames
    }
  }
  return false;
};

const fillContentEditable = async (context, selectors, value) => {
  if (isBlankValue(value)) return false;
  try {
    return await context.evaluate(({ selectors, value }) => {
      const normalize = (text) => (text || "").replace(/\s+/g, " ").trim();
      const setNode = (node) => {
        if (!node) return false;
        try {
          node.focus();
        } catch (error) {
          // ignore
        }
        if ("value" in node) {
          node.value = value;
        } else {
          node.textContent = value;
        }
        node.dispatchEvent(new Event("input", { bubbles: true }));
        node.dispatchEvent(new Event("change", { bubbles: true }));
        return true;
      };

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

      for (const root of roots) {
        for (const selector of selectors) {
          let candidate = null;
          try {
            candidate = root.querySelector(selector);
          } catch (error) {
            candidate = null;
          }
          if (candidate) {
            const text = normalize(candidate.value ?? candidate.textContent ?? "");
            if (text) return true;
            if (setNode(candidate)) return true;
          }
        }
      }
      return false;
    }, { selectors, value });
  } catch (error) {
    return false;
  }
};

const fillContentEditableAcrossContexts = async (page, selectors, value) => {
  for (const context of [page, ...page.frames()]) {
    try {
      if (await fillContentEditable(context, selectors, value)) return true;
    } catch (error) {
      // ignore
    }
  }
  return false;
};

const fillFieldDeep = async (context, { labelKeywords = [], value }) => {
  if (isBlankValue(value)) return false;
  return context.evaluate(({ labelKeywords, value }) => {
    const normalize = (text) => (text || "").replace(/\s+/g, " ").trim().toLowerCase();
    const matches = (text) => labelKeywords.some((keyword) => normalize(text).includes(keyword));
    const isVisible = (node) => {
      if (!node) return false;
      const style = window.getComputedStyle(node);
      if (style.visibility === "hidden" || style.display === "none") return false;
      const rect = node.getBoundingClientRect();
      return rect.width > 0 && rect.height > 0;
    };

    const collectRoots = (root, bucket) => {
      if (!root || bucket.includes(root)) return;
      bucket.push(root);
      const elements = root.querySelectorAll ? root.querySelectorAll("*") : [];
      elements.forEach((el) => {
        if (el.shadowRoot) collectRoots(el.shadowRoot, bucket);
      });
    };

    const getLabelText = (node) => {
      if (!node) return "";
      const aria = node.getAttribute("aria-label");
      if (aria) return aria;
      const placeholder = node.getAttribute("placeholder");
      if (placeholder) return placeholder;
      const name = node.getAttribute("name") || node.getAttribute("id");
      if (name) return name;
      const labelledBy = node.getAttribute("aria-labelledby");
      if (labelledBy) {
        const ids = labelledBy.split(/\s+/).filter(Boolean);
        const texts = ids
          .map((id) => document.getElementById(id))
          .filter(Boolean)
          .map((el) => el.textContent || "")
          .join(" ");
        if (texts) return texts;
      }
      const label = node.closest("label");
      if (label) return label.textContent || "";
      if (node.id) {
        const labelEl = document.querySelector(`label[for="${node.id}"]`);
        if (labelEl) return labelEl.textContent || "";
      }
      return "";
    };

    const roots = [];
    collectRoots(document, roots);

    const candidates = [];
    for (const root of roots) {
      let nodes = [];
      try {
        nodes = root.querySelectorAll("textarea, input, [contenteditable='true'], [role='textbox']");
      } catch (error) {
        nodes = [];
      }
      nodes.forEach((node) => {
        if (!isVisible(node)) return;
        const tag = node.tagName?.toLowerCase() || "";
        if (tag === "input") {
          const type = (node.getAttribute("type") || "text").toLowerCase();
          if (!["text", "search", "email", "tel", "url", ""].includes(type)) return;
        }
        const labelText = getLabelText(node);
        if (!labelText) return;
        if (matches(labelText)) {
          candidates.push(node);
        }
      });
    }

    const setNodeValue = (node) => {
      if (!node) return false;
      try {
        node.focus();
      } catch (error) {
        // ignore
      }
      if ("value" in node) {
        node.value = value;
      } else {
        node.textContent = value;
      }
      node.dispatchEvent(new Event("input", { bubbles: true }));
      node.dispatchEvent(new Event("change", { bubbles: true }));
      return true;
    };

    for (const candidate of candidates) {
      const current = (candidate.value ?? candidate.textContent ?? "").trim();
      if (current) return true;
      if (setNodeValue(candidate)) return true;
    }

    return false;
  }, { labelKeywords: labelKeywords.map((k) => k.toLowerCase()), value });
};

const fillFieldDeepAcrossContexts = async (page, options) => {
  for (const context of [page, ...page.frames()]) {
    try {
      if (await fillFieldDeep(context, options)) return true;
    } catch (error) {
      // ignore
    }
  }
  return false;
};

const clickByText = async (page, texts) => {
  for (const text of texts) {
    const directMatches = await page.$$(`xpath//button[contains(normalize-space(.), "${text}")] | //a[contains(normalize-space(.), "${text}")]`);
    if (directMatches.length > 0) {
      if (await safeClick(directMatches[0])) {
        return true;
      }
    }

    const spanMatches = await page.$$(`xpath//span[contains(normalize-space(.), "${text}")]/ancestor::*[self::button or self::a][1]`);
    if (spanMatches.length > 0) {
      if (await safeClick(spanMatches[0])) {
        return true;
      }
    }
  }
  return false;
};

const clickByTextInContext = async (context, texts) => {
  for (const text of texts) {
    let matches = [];
    try {
      matches = await context.$$(`xpath//button[contains(normalize-space(.), "${text}")] | //a[contains(normalize-space(.), "${text}")]`);
    } catch (error) {
      matches = [];
    }
    if (matches.length > 0) {
      if (await safeClick(matches[0])) {
        return true;
      }
    }

    let spanMatches = [];
    try {
      spanMatches = await context.$$(`xpath//span[contains(normalize-space(.), "${text}")]/ancestor::*[self::button or self::a][1]`);
    } catch (error) {
      spanMatches = [];
    }
    if (spanMatches.length > 0) {
      if (await safeClick(spanMatches[0])) {
        return true;
      }
    }
  }
  return false;
};

const isGdprPage = (url) => /\/gdpr/i.test(url || "");

const getGdprRedirectTarget = (url) => {
  if (!url) return "";
  try {
    const parsed = new URL(url);
    const redirectTo = parsed.searchParams.get("redirectTo");
    if (!redirectTo) return "";
    return decodeURIComponent(redirectTo);
  } catch (error) {
    return "";
  }
};

const acceptCookieModal = async (page, { timeout = 15000 } = {}) => {
  if (!page) return false;
  const consentTexts = [
    "Alle akzeptieren",
    "Akzeptieren",
    "Zustimmen",
    "Einverstanden",
    "Alle annehmen",
    "Alle erlauben",
    "Alles akzeptieren",
    "Auswahl speichern",
    "Accept all",
    "Accept",
    "Agree",
    "I agree"
  ];

  const clickByXpathInContext = async (context, texts) => {
    for (const text of texts) {
      // Be conservative: cookie banners typically use buttons. Avoid clicking anchors
      // to prevent accidental navigations to policy/help pages.
      const xpath = `//button[contains(normalize-space(.), "${text}")] | //span[contains(normalize-space(.), "${text}")]/ancestor::button[1] | //input[( @type="button" or @type="submit") and contains(normalize-space(@value), "${text}")]`;
      let handles = [];
      try {
        handles = await context.$x(xpath);
      } catch (error) {
        handles = [];
      }
      if (!handles.length) continue;
      const handle = handles[0];
      try {
        await handle.click({ delay: 40 });
        return true;
      } catch (error) {
        try {
          const box = await handle.boundingBox();
          if (box) {
            await page.mouse.click(box.x + box.width / 2, box.y + box.height / 2, { delay: 40 });
            return true;
          }
        } catch (innerError) {
          // ignore
        }
        try {
          await handle.evaluate((node) => node.click());
          return true;
        } catch (innerError) {
          // ignore
        }
      }
    }
    return false;
  };

  const clickBySelectorInContext = async (context) => {
    const selectors = [
      "button[data-testid*='accept']",
      "button[class*='accept']",
      "button[id*='accept']",
      "button[aria-label*='accept']",
      "button[aria-label*='akzept']",
      "button[class*='agree']",
      "button[id*='agree']",
      "button[class*='consent']",
      "button[id*='consent']"
    ];
    for (const selector of selectors) {
      let handle = null;
      try {
        handle = await context.$(selector);
      } catch (error) {
        handle = null;
      }
      if (!handle) continue;
      try {
        await handle.click({ delay: 40 });
        return true;
      } catch (error) {
        try {
          const box = await handle.boundingBox();
          if (box) {
            await page.mouse.click(box.x + box.width / 2, box.y + box.height / 2, { delay: 40 });
            return true;
          }
        } catch (innerError) {
          // ignore
        }
      }
    }
    return false;
  };

  const isConsentVisible = async (context) => {
    try {
      return await context.evaluate(() => {
        const isVisible = (node) => {
          if (!node) return false;
          const style = window.getComputedStyle(node);
          if (style.display === "none" || style.visibility === "hidden" || style.opacity === "0") return false;
          const rect = node.getBoundingClientRect();
          return rect.width > 0 && rect.height > 0;
        };

        const consentBanner = document.querySelector("#consentBanner");
        if (isVisible(consentBanner)) return true;

        const dialog = document.querySelector("#gdpr-banner[open]");
        if (isVisible(dialog)) return true;

        const acceptButton = document.querySelector("#gdpr-banner-accept, [data-testid='gdpr-banner-accept']");
        if (isVisible(acceptButton)) return true;

        const management = document.querySelector("#consentManagementPage");
        if (isVisible(management)) return true;

        const container = document.querySelector("[data-testid='gdpr-banner-container']");
        if (isVisible(container)) return true;

        return false;
      });
    } catch (error) {
      return false;
    }
  };

  const clickKnownConsentButtons = async (context) => {
    const selectors = [
      "#gdpr-banner-accept",
      "[data-testid='gdpr-banner-accept']",
      "button[aria-label*='akzept']",
      "button[aria-label*='accept']",
      "button[id*='accept']",
      "button[data-testid*='accept']"
    ];
    for (const selector of selectors) {
      let handle = null;
      try {
        handle = await context.$(selector);
      } catch (error) {
        handle = null;
      }
      if (!handle) continue;
      try {
        await handle.click({ delay: 40 });
        return true;
      } catch (error) {
        try {
          const box = await handle.boundingBox();
          if (box) {
            await page.mouse.click(box.x + box.width / 2, box.y + box.height / 2, { delay: 40 });
            return true;
          }
        } catch (innerError) {
          // ignore
        }
        try {
          await handle.evaluate((node) => node.click());
          return true;
        } catch (innerError) {
          // ignore
        }
      }
    }
    return false;
  };

  const tryClickInContext = async (context) => {
    try {
      const clicked = await context.evaluate((texts) => {
        const normalize = (value) => (value || "").replace(/\s+/g, " ").trim().toLowerCase();
        const matches = (text) => texts.some((needle) => normalize(text).includes(needle));
        const isBlocked = (text) => {
          const normalized = normalize(text);
          return normalized.includes("entwurf") || normalized.includes("anzeige aufgeben");
        };
        const isVisible = (node) => {
          if (!node) return false;
          const style = window.getComputedStyle(node);
          if (style.display === "none" || style.visibility === "hidden") return false;
          const rect = node.getBoundingClientRect();
          return rect.width > 0 && rect.height > 0;
        };
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
        for (const root of roots) {
          const candidates = Array.from(
            root.querySelectorAll("button, [role='button'], input[type='button'], input[type='submit']")
          );
          for (const node of candidates) {
            if (!isVisible(node)) continue;
            const text = normalize(
              node.textContent ||
              node.value ||
              node.getAttribute("aria-label") ||
              node.getAttribute("title")
            );
            if (text && matches(text) && !isBlocked(text)) {
              node.click();
              return true;
            }
          }
        }
        return false;
      }, consentTexts.map((text) => text.toLowerCase()));
      return clicked;
    } catch (error) {
      return false;
    }
  };

  const containsConsentText = async (context) => {
    try {
      return await context.evaluate(() => {
        const banner = document.querySelector("#consentBanner");
        if (banner) {
          const style = window.getComputedStyle(banner);
          if (style.display !== "none" && style.visibility !== "hidden") {
            return true;
          }
        }
        const text = document.body?.innerText || "";
        return /Willkommen bei Kleinanzeigen/i.test(text) || /Alle akzeptieren/i.test(text);
      });
    } catch (error) {
      return false;
    }
  };

  const startedAt = Date.now();
  while (Date.now() - startedAt < timeout) {
    const contexts = [page, ...page.frames()];
    for (const context of contexts) {
      if (!(await isConsentVisible(context))) continue;
      const clickedKnown = await clickKnownConsentButtons(context);
      if (clickedKnown) {
        await sleep(600);
        if (!(await isConsentVisible(context))) {
          return true;
        }
      }
    }
    for (const context of contexts) {
      try {
        if (!(await isConsentVisible(context)) && !(await containsConsentText(context))) continue;
        if (await clickByXpathInContext(context, consentTexts)) {
          try {
            await Promise.race([
              page.waitForFunction(() => !/Willkommen bei Kleinanzeigen/i.test(document.body?.innerText || ""), { timeout: 8000 }),
              sleep(800)
            ]);
          } catch (error) {
            // ignore
          }
          if (!(await isConsentVisible(context))) {
            return true;
          }
        }
      } catch (error) {
        // ignore
      }
    }
    for (const context of contexts) {
      if (!(await isConsentVisible(context)) && !(await containsConsentText(context))) continue;
      const clicked = await tryClickInContext(context);
      if (clicked) {
        try {
          await Promise.race([
            page.waitForFunction(() => !/Willkommen bei Kleinanzeigen/i.test(document.body?.innerText || ""), { timeout: 8000 }),
            sleep(800)
          ]);
        } catch (error) {
          // ignore
        }
        if (!(await isConsentVisible(context))) {
          return true;
        }
      }
    }
    for (const context of contexts) {
      const hasConsent = await containsConsentText(context);
      if (!hasConsent) continue;
      const clickedKnown = await clickKnownConsentButtons(context);
      if (clickedKnown) {
        await sleep(600);
        if (!(await isConsentVisible(context))) {
          return true;
        }
      }
      const xpathClicked = await clickByXpathInContext(context, consentTexts);
      if (xpathClicked) {
        await sleep(600);
        if (!(await isConsentVisible(context))) {
          return true;
        }
      }
      const selectorClicked = await clickBySelectorInContext(context);
      if (selectorClicked) {
        await sleep(600);
        if (!(await isConsentVisible(context))) {
          return true;
        }
      }
    }
    try {
      await sleep(400);
    } catch (error) {
      break;
    }
  }
  return false;
};

const acceptGdprConsent = async (page, { timeout = 15000 } = {}) => {
  if (!page) return false;
  if (!isGdprPage(page.url())) return false;

  const buttonTexts = [
    "alle akzeptieren",
    "akzeptieren",
    "zustimmen",
    "einverstanden",
    "alle annehmen",
    "alle erlauben",
    "alles akzeptieren",
    "auswahl speichern",
    "speichern",
    "accept all",
    "accept",
    "agree",
    "i agree"
  ];

  const startedAt = Date.now();
  while (Date.now() - startedAt < timeout) {
    // Prefer the same logic as regular cookie banners; Kleinanzeigen reuses it on /gdpr.
    const acceptedViaCookieModal = await acceptCookieModal(page, { timeout: Math.min(5000, Math.max(1200, timeout - (Date.now() - startedAt))) })
      .catch(() => false);

    if (acceptedViaCookieModal) {
      try {
		        await Promise.race([
		          page.waitForNavigation({ waitUntil: "domcontentloaded", timeout: 20000 }),
		          page.waitForFunction(() => !String(window.location.href || "").toLowerCase().includes("/gdpr"), { timeout: 20000 })
		        ]);
		      } catch (error) {
		        // ignore navigation; URL can also stay the same while consent is applied
		      }
      return true;
    }

    // Fallback: click a visible button (avoid anchors to prevent going to policy pages).
    const contexts = [page, ...page.frames()];
    for (const context of contexts) {
      const clicked = await context.evaluate((texts) => {
        const normalize = (value) => (value || "").replace(/\\s+/g, " ").trim().toLowerCase();
        const needles = Array.isArray(texts) ? texts.map(normalize).filter(Boolean) : [];
        if (!needles.length) return false;
        const isVisible = (node) => {
          if (!node) return false;
          const style = window.getComputedStyle(node);
          if (!style) return false;
          if (style.display === "none" || style.visibility === "hidden" || style.opacity === "0" || style.pointerEvents === "none") return false;
          const rect = node.getBoundingClientRect();
          return rect.width > 0 && rect.height > 0;
        };
        const candidates = Array.from(document.querySelectorAll("button, [role='button'], input[type='button'], input[type='submit']"));
        for (const node of candidates) {
          if (!isVisible(node)) continue;
          const text = normalize(
            node.textContent
            || node.value
            || node.getAttribute("aria-label")
            || node.getAttribute("title")
            || node.getAttribute("data-testid")
            || node.getAttribute("data-test")
          );
          if (!text) continue;
          if (!needles.some((needle) => text.includes(needle))) continue;
          try {
            node.click();
            return true;
          } catch (error) {
            // ignore and continue
          }
        }
        return false;
      }, buttonTexts);

      if (clicked) {
	        try {
	          await Promise.race([
	            page.waitForNavigation({ waitUntil: "domcontentloaded", timeout: 20000 }),
	            page.waitForFunction(() => !String(window.location.href || "").toLowerCase().includes("/gdpr"), { timeout: 20000 })
	          ]);
	        } catch (error) {
	          // ignore
	        }
        return true;
      }
    }

    await sleep(400).catch(() => {});
  }
  return false;
};

const clickSubmitButtonInContext = async (context, options = {}) => {
  const { allowFallback = true } = options;
  const primaryTexts = ["Anzeige aufgeben", "Anzeige veröffentlichen", "Veröffentlichen", "Jetzt veröffentlichen"];
  const fallbackTexts = [
    "Weiter",
    "Fortfahren",
    "Weiter zur Vorschau",
    "Weiter zur Veröffentlichung",
    "Anzeige einstellen",
    "Jetzt einstellen",
    "Verbindlich einstellen"
  ];

  const clickBestSubmitCandidate = async (texts, { requireSubmitLike = false } = {}) => {
    const handle = await context.evaluateHandle((variants, strictSubmitLike) => {
      const normalize = (value) => (value || "").replace(/\s+/g, " ").trim().toLowerCase();
      const needles = Array.isArray(variants) ? variants.map(normalize).filter(Boolean) : [];
      if (!needles.length) return null;

      const isVisible = (node) => {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        if (style.display === "none" || style.visibility === "hidden") return false;
        const rect = node.getBoundingClientRect();
        return rect.width > 0 && rect.height > 0;
      };

      const nodes = Array.from(
        document.querySelectorAll('button, input[type="submit"], input[type="button"], [role="button"], a')
      );
      let best = null;
      let bestScore = Number.NEGATIVE_INFINITY;

      for (const node of nodes) {
        const text = normalize(
          node.innerText ||
          node.textContent ||
          node.value ||
          node.getAttribute("aria-label") ||
          node.getAttribute("title")
        );
        if (!text) continue;
        if (!needles.some((needle) => text.includes(needle))) continue;
        if (text.includes("entwurf") || text.includes("draft")) continue;
        if (node.disabled) continue;
        if (node.getAttribute("aria-disabled") === "true") continue;
        if (node.classList.contains("disabled")) continue;
        if (!isVisible(node)) continue;

        const tag = String(node.tagName || "").toLowerCase();
        const type = String(node.getAttribute("type") || "").toLowerCase();
        const form = node.form || node.closest("form");
        const inForm = Boolean(form);
        const formId = String((form && form.id) || "").toLowerCase();
        const formAction = String((form && form.getAttribute("action")) || "").toLowerCase();
        const isSubmitLike = type === "submit" || tag === "button";
        if (strictSubmitLike && !isSubmitLike && !inForm) continue;

        const rect = node.getBoundingClientRect();
        let score = 0;
        if (isSubmitLike) score += 120;
        if (inForm) score += 70;
        if (formId.includes("postad") || formId.includes("anzeige")) score += 40;
        if (formAction.includes("postad") || formAction.includes("anzeige")) score += 40;
        if (text.includes("anzeige aufgeben")) score += 60;
        if (text.includes("veröffentlichen") || text.includes("veroeffentlichen")) score += 40;
        score += Math.min(45, Math.max(0, Math.round(rect.y / 40)));
        score += Math.min(20, Math.max(0, Math.round(rect.height / 6)));

        if (score > bestScore) {
          bestScore = score;
          best = node;
        }
      }

      return best || null;
    }, texts, requireSubmitLike);

    const element = handle.asElement();
    if (!element) {
      await handle.dispose();
      return false;
    }
    try {
      await scrollIntoView(element);
      return await safeClick(element);
    } finally {
      try {
        await element.dispose();
      } catch (error) {
        // ignore dispose errors
      }
    }
  };

  const findVisibleClickable = async (text) => {
    const handle = await context.evaluateHandle((targetText) => {
      const normalize = (value) => (value || "").replace(/\s+/g, " ").trim();
      const target = normalize(targetText);
      const candidates = Array.from(
        document.querySelectorAll(
          'button, input[type="submit"], input[type="button"], [role="button"], a'
        )
      );
      const match = candidates.find((node) => {
        const label = normalize(
          node.innerText ||
            node.value ||
            node.getAttribute("aria-label") ||
            node.getAttribute("data-testid") ||
            node.getAttribute("data-test") ||
            node.getAttribute("title")
        );
        if (!label.includes(target)) return false;
        if (node.disabled) return false;
        if (node.getAttribute("aria-disabled") === "true") return false;
        if (node.classList.contains("disabled")) return false;
        const style = window.getComputedStyle(node);
        if (style.display === "none" || style.visibility === "hidden") return false;
        const rect = node.getBoundingClientRect();
        return rect.width > 0 && rect.height > 0;
      });
      return match || null;
    }, text);
    const element = handle.asElement();
    if (!element) {
      await handle.dispose();
      return null;
    }
    return element;
  };

  const clickWithTexts = async (texts) => {
    for (const text of texts) {
      const buttons = await context.$$(
        `xpath//button[contains(normalize-space(.), "${text}")] | //span[contains(normalize-space(.), "${text}")]/ancestor::button[1]`
      );
      for (const button of buttons) {
        await scrollIntoView(button);
        const isVisible = await button.evaluate((node) => {
          const style = window.getComputedStyle(node);
          if (style.display === "none" || style.visibility === "hidden") return false;
          const rect = node.getBoundingClientRect();
          return rect.width > 0 && rect.height > 0;
        });
        if (!isVisible) {
          continue;
        }
        const isDisabled = await button.evaluate(
          (node) =>
            node.hasAttribute("disabled") ||
            node.getAttribute("aria-disabled") === "true" ||
            node.classList.contains("disabled")
        );
        if (isDisabled) {
          continue;
        }
        // Проверяем, что это не кнопка черновика (Entwurf)
        const isDraftButton = await button.evaluate((node) => {
          const buttonText = (node.innerText || node.textContent || node.value || "").toLowerCase();
          return buttonText.includes("entwurf") || buttonText.includes("als entwurf");
        });
        if (isDraftButton) {
          continue;
        }
        if (await safeClick(button)) {
          return true;
        }
      }
    }
    return false;
  };

  if (await clickBestSubmitCandidate(primaryTexts, { requireSubmitLike: true })) {
    return true;
  }

  if (await clickWithTexts(primaryTexts)) {
    return true;
  }

  for (const text of primaryTexts) {
    const element = await findVisibleClickable(text);
    if (element) {
      await scrollIntoView(element);
      if (await safeClick(element)) {
        return true;
      }
    }
  }

  if (allowFallback && await clickBestSubmitCandidate(fallbackTexts, { requireSubmitLike: false })) {
    return true;
  }

  if (allowFallback && await clickWithTexts(fallbackTexts)) {
    return true;
  }

  // Deep shadow-root search fallback
  try {
    const clicked = await context.evaluate((texts) => {
      const normalize = (value) => (value || "").replace(/\s+/g, " ").trim().toLowerCase();
      const needles = texts.map((text) => normalize(text));
      const isBlocked = (text) => {
        const normalized = normalize(text);
        return normalized.includes("entwurf") || normalized.includes("draft");
      };
      const isVisible = (node) => {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        if (style.display === "none" || style.visibility === "hidden") return false;
        const rect = node.getBoundingClientRect();
        return rect.width > 0 && rect.height > 0;
      };
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
      for (const root of roots) {
        const candidates = Array.from(
          root.querySelectorAll("button, a, [role='button'], input[type='button'], input[type='submit']")
        );
        for (const node of candidates) {
          if (!isVisible(node)) continue;
          const text = normalize(
            node.textContent ||
            node.value ||
            node.getAttribute("aria-label") ||
            node.getAttribute("title")
          );
          if (!text || isBlocked(text)) continue;
          if (needles.some((needle) => text.includes(needle))) {
            node.click();
            return true;
          }
        }
      }
      return false;
    }, allowFallback ? [...primaryTexts, ...fallbackTexts] : [...primaryTexts]);
    if (clicked) return true;
  } catch (error) {
    // ignore deep click errors
  }

  return false;
};

const clickSubmitButton = async (page, contexts = [], options = {}) => {
  const queue = [];
  const pushUnique = (ctx) => {
    if (!ctx || queue.includes(ctx)) return;
    queue.push(ctx);
  };
  contexts.forEach(pushUnique);
  pushUnique(page);
  page.frames().forEach(pushUnique);

  for (const context of queue) {
    if (await clickSubmitButtonInContext(context, options)) {
      return true;
    }
  }
  return false;
};

const forceSubmitFormInContext = async (context) => {
  try {
    return await context.evaluate(() => {
      const normalize = (value) => (value || "").replace(/\s+/g, " ").trim();
      const forms = Array.from(document.querySelectorAll("form"));
      if (!forms.length) {
        return { submitted: false, reason: "form-not-found" };
      }
      const candidates = forms.filter((form) => {
        const action = String(form.getAttribute("action") || "").toLowerCase();
        const id = String(form.id || "").toLowerCase();
        return action.includes("anzeige") || id.includes("postad") || id.includes("anzeige");
      });
      const target = candidates.find((form) =>
        Boolean(form.querySelector('button[type="submit"], input[type="submit"]'))
      ) || candidates[0] || forms[0];
      if (!target) {
        return { submitted: false, reason: "target-form-not-found" };
      }

      const submitter = target.querySelector(
        'button[type="submit"]:not([disabled]), input[type="submit"]:not([disabled])'
      );
      if (submitter) {
        submitter.click();
        return {
          submitted: true,
          via: "submitter-click",
          submitterText: normalize(submitter.innerText || submitter.value || submitter.getAttribute("aria-label"))
        };
      }

      if (typeof target.requestSubmit === "function") {
        target.requestSubmit();
        return { submitted: true, via: "requestSubmit" };
      }

      target.submit();
      return { submitted: true, via: "submit" };
    });
  } catch (error) {
    return {
      submitted: false,
      reason: error?.message || "force-submit-error"
    };
  }
};

const hardSubmitFormInContext = async (context) => {
  try {
    return await context.evaluate(() => {
      const forms = Array.from(document.querySelectorAll("form"));
      if (!forms.length) {
        return { submitted: false, reason: "form-not-found" };
      }

      const candidates = forms.filter((form) => {
        const action = String(form.getAttribute("action") || "").toLowerCase();
        const id = String(form.id || "").toLowerCase();
        return action.includes("anzeige") || id.includes("postad") || id.includes("anzeige");
      });
      const target = candidates.find((form) =>
        Boolean(form.querySelector('button[type="submit"], input[type="submit"]'))
      ) || candidates[0] || forms[0];
      if (!target) {
        return { submitted: false, reason: "target-form-not-found" };
      }

      target.submit();
      return {
        submitted: true,
        via: "native-submit",
        formId: String(target.id || ""),
        action: String(target.getAttribute("action") || "")
      };
    });
  } catch (error) {
    return {
      submitted: false,
      reason: error?.message || "hard-submit-error"
    };
  }
};

const getSubmitCandidatesInContext = async (context) => {
  try {
    return await context.evaluate(() => {
      const normalize = (value) => (value || "").replace(/\s+/g, " ").trim();
      const textMatches = (value) => {
        const text = normalize(value).toLowerCase();
        if (!text) return false;
        return (
          text.includes("anzeige aufgeben")
          || text.includes("anzeige veroffentlichen")
          || text.includes("anzeige veröffentlichen")
          || text.includes("veroffentlichen")
          || text.includes("veröffentlichen")
          || text.includes("weiter zur veroffentlichung")
          || text.includes("weiter zur veröffentlichung")
        );
      };
      const isVisible = (node) => {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        if (style.display === "none" || style.visibility === "hidden") return false;
        const rect = node.getBoundingClientRect();
        return rect.width > 0 && rect.height > 0;
      };

      const nodes = Array.from(
        document.querySelectorAll('button, input[type="submit"], input[type="button"], [role="button"], a')
      );
      const items = [];
      for (const node of nodes) {
        const label = normalize(
          node.innerText ||
          node.textContent ||
          node.value ||
          node.getAttribute("aria-label") ||
          node.getAttribute("title")
        );
        if (!textMatches(label)) continue;
        const rect = node.getBoundingClientRect();
        items.push({
          label: label.slice(0, 180),
          tag: (node.tagName || "").toLowerCase(),
          type: String(node.getAttribute("type") || "").toLowerCase(),
          id: String(node.id || "").slice(0, 120),
          className: String(node.className || "").slice(0, 180),
          disabled: Boolean(node.disabled),
          ariaDisabled: node.getAttribute("aria-disabled") === "true",
          visible: isVisible(node),
          x: Math.round(rect.x),
          y: Math.round(rect.y),
          w: Math.round(rect.width),
          h: Math.round(rect.height),
          name: String(node.getAttribute("name") || "").slice(0, 120),
          formAction: String(node.getAttribute("formaction") || "").slice(0, 240)
        });
        if (items.length >= 40) break;
      }
      return {
        url: window.location.href,
        title: document.title || "",
        count: items.length,
        items
      };
    });
  } catch (error) {
    return {
      url: "",
      title: "",
      count: 0,
      items: [],
      error: error?.message || String(error)
    };
  }
};

const collectSubmitCandidatesDebug = async (page, contexts = []) => {
  const queue = [];
  const pushUnique = (ctx) => {
    if (!ctx || queue.includes(ctx)) return;
    queue.push(ctx);
  };
  contexts.forEach(pushUnique);
  pushUnique(page);
  page.frames().forEach(pushUnique);

  const snapshots = [];
  for (const context of queue) {
    const snapshot = await getSubmitCandidatesInContext(context);
    let contextUrl = "";
    try {
      contextUrl = typeof context.url === "function" ? context.url() : "";
    } catch (error) {
      contextUrl = "";
    }
    snapshots.push({
      contextType: context === page ? "page" : "frame",
      contextUrl,
      ...snapshot
    });
  }
  return snapshots;
};

const waitForPublishState = async (page, timeout) => {
  try {
    const handle = await page.waitForFunction(
      () => {
        const bodyText = document.body?.innerText || "";
        const url = window.location.href;
        if (
          url.includes("p-anzeige-aufgeben-bestaetigung") ||
          url.includes("anzeige-aufgeben-bestaetigung") ||
          url.includes("anzeige-aufgeben-schritt3") ||
          url.includes("anzeige-aufgeben-schritt4") ||
          url.includes("anzeige-aufgeben-danke") ||
          url.includes("anzeige-aufgeben-abschliessen") ||
          url.includes("meine-anzeigen") ||
          bodyText.includes("Anzeige wird aufgegeben") ||
          bodyText.includes("Anzeige wurde erstellt") ||
          bodyText.includes("Anzeige ist online") ||
          bodyText.includes("Anzeige wurde erfolgreich") ||
          bodyText.includes("Anzeige wurde veröffentlicht") ||
          bodyText.includes("Anzeige wird geprüft") ||
          bodyText.includes("Deine Anzeige wird geprüft") ||
          bodyText.includes("Wir prüfen deine Anzeige") ||
          bodyText.includes("Anzeige wurde eingereicht") ||
          bodyText.includes("Vielen Dank") ||
          bodyText.includes("Danke")
        ) {
          return "success";
        }
        const hasPreviewUrl = /vorschau|preview/i.test(url);
        const headingText = Array.from(document.querySelectorAll("h1, h2, h3"))
          .map((node) => node.textContent || "")
          .join(" ");
        const titleText = document.title || "";
        const previewHints = `${titleText} ${headingText}`.toLowerCase();
        const hasPreviewText = /vorschau/.test(previewHints);
        const previewButtons = Array.from(
          document.querySelectorAll('button[type="submit"], input[type="submit"], button')
        ).map((button) => (button.innerText || button.value || "").toLowerCase());
        const hasPublishButton = previewButtons.some((text) =>
          text.includes("veröffentlichen")
          || text.includes("veroeffentlichen")
          || text.includes("anzeige aufgeben")
        );
        if (hasPreviewUrl || (hasPreviewText && hasPublishButton)) {
          return "preview";
        }
        const confirmationElement = document.querySelector(
          '[data-testid*="success"], [data-test*="success"], [data-testid*="confirmation"], [data-test*="confirmation"], [class*="success"], [class*="confirmation"]'
        );
        if (confirmationElement) {
          const text = (confirmationElement.textContent || "").trim();
          if (text) {
            return "success";
          }
        }
        if (url.includes("anzeige-aufgeben") || url.includes("anzeige-abschicken")) {
          return "form";
        }
        return false;
      },
      { timeout }
    );
    return await handle.jsonValue();
  } catch (error) {
    return null;
  }
};

// After clicking submit on step2 Kleinanzeigen may navigate to the final step (p-anzeige-abschicken)
// asynchronously. waitForPublishState() returns "form" immediately on form URLs, so we need a
// dedicated waiter to detect step transitions or visible validation errors.
const waitForPostSubmitTransition = async (page, { initialUrl = "", timeoutMs = 45000 } = {}) => {
  if (!page) return null;
  try {
    const handle = await page.waitForFunction(
      (initial) => {
        const bodyText = document.body?.innerText || "";
        const url = window.location.href;

        const isVisible = (node) => {
          if (!node) return false;
          const style = window.getComputedStyle(node);
          if (!style) return false;
          if (style.display === "none" || style.visibility === "hidden" || style.opacity === "0") return false;
          const rect = node.getBoundingClientRect();
          return rect.width > 0 && rect.height > 0;
        };

        if (
          url.includes("p-anzeige-aufgeben-bestaetigung") ||
          url.includes("anzeige-aufgeben-bestaetigung") ||
          url.includes("anzeige-aufgeben-schritt3") ||
          url.includes("anzeige-aufgeben-schritt4") ||
          url.includes("anzeige-aufgeben-danke") ||
          url.includes("anzeige-aufgeben-abschliessen") ||
          url.includes("meine-anzeigen") ||
          bodyText.includes("Anzeige wird aufgegeben") ||
          bodyText.includes("Anzeige wurde erstellt") ||
          bodyText.includes("Anzeige ist online") ||
          bodyText.includes("Anzeige wurde erfolgreich") ||
          bodyText.includes("Anzeige wurde veröffentlicht") ||
          bodyText.includes("Anzeige wird geprüft") ||
          bodyText.includes("Deine Anzeige wird geprüft") ||
          bodyText.includes("Wir prüfen deine Anzeige") ||
          bodyText.includes("Anzeige wurde eingereicht") ||
          bodyText.includes("Vielen Dank") ||
          bodyText.includes("Danke")
        ) {
          return { outcome: "success", url };
        }

        const hasPreviewUrl = /vorschau|preview/i.test(url);
        const headingText = Array.from(document.querySelectorAll("h1, h2, h3"))
          .map((node) => node.textContent || "")
          .join(" ");
        const titleText = document.title || "";
        const previewHints = `${titleText} ${headingText}`.toLowerCase();
        const hasPreviewText = /vorschau/.test(previewHints);
        const previewButtons = Array.from(
          document.querySelectorAll('button[type="submit"], input[type="submit"], button')
        ).map((button) => (button.innerText || button.value || "").toLowerCase());
        const hasPublishButton = previewButtons.some((text) =>
          text.includes("veröffentlichen")
          || text.includes("veroeffentlichen")
          || text.includes("anzeige aufgeben")
        );
        if (hasPreviewUrl || (hasPreviewText && hasPublishButton)) {
          return { outcome: "preview", url };
        }

        const initialIsAbschicken = Boolean(initial && /p-anzeige-abschicken|anzeige-abschicken/i.test(initial));
        if (/p-anzeige-abschicken|anzeige-abschicken/i.test(url) && !initialIsAbschicken) {
          return { outcome: "abschicken", url };
        }

        // Visible validation errors on the current page (e.g. step2 staying on the form).
        const errorSelectors = [
          ".formerror",
          "[role='alert']",
          "[data-testid*='error']",
          ".error-message",
          ".form-error",
          ".validation-error",
          "#buyNow\\.errors"
        ];
        for (const selector of errorSelectors) {
          const nodes = Array.from(document.querySelectorAll(selector));
          if (nodes.some(isVisible)) {
            return { outcome: "errors", url };
          }
        }

        if (initial && url !== initial && /anzeige-aufgeben|anzeige-abschicken/i.test(url)) {
          return { outcome: "url-changed", url };
        }

        return false;
      },
      { timeout: timeoutMs },
      initialUrl || ""
    );
    const result = await handle.jsonValue();
    try {
      await handle.dispose();
    } catch (error) {
      // ignore dispose errors
    }
    return result;
  } catch (error) {
    return null;
  }
};

const waitForPublishProgress = async (page, startUrl, timeout = 30000) => {
  try {
    await page.waitForFunction(
      (initialUrl) => {
        const url = window.location.href;
        const isFormUrl = /anzeige-aufgeben|anzeige-abschicken/i.test(url);
        if (url !== initialUrl && !isFormUrl) {
          return true;
        }
        const submitButtons = Array.from(
          document.querySelectorAll('button[type="submit"], input[type="submit"], button')
        ).filter((button) => {
          const text = button.innerText || button.value || "";
          const normalized = String(text || "").toLowerCase();
          if (!normalized.includes("anzeige aufgeben")) return false;
          const style = window.getComputedStyle(button);
          if (style.display === "none" || style.visibility === "hidden") return false;
          const rect = button.getBoundingClientRect();
          return rect.width > 0 && rect.height > 0;
        });
        if (!submitButtons.length) return false;
        return submitButtons.some((button) => {
          if (button.disabled) return true;
          if (button.getAttribute("aria-disabled") === "true") return true;
          if (button.classList.contains("disabled")) return true;
          const busy = button.getAttribute("aria-busy");
          return busy === "true";
        });
      },
      { timeout },
      startUrl
    );
    return true;
  } catch (error) {
    return false;
  }
};

const acceptTermsIfPresent = async (context) => {
  try {
    return await context.evaluate(() => {
      const tokens = [
        "nutzungsbedingungen",
        "bedingungen",
        "datenschutz",
        "agb",
        "einverstanden",
        "zustimmen",
        "akzeptiere"
      ];
      const normalize = (value) => (value || "").replace(/\s+/g, " ").trim().toLowerCase();
      const matches = (text) => tokens.some((token) => normalize(text).includes(token));
      const isVisible = (node) => {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        if (style.display === "none" || style.visibility === "hidden") return false;
        const rect = node.getBoundingClientRect();
        return rect.width > 0 && rect.height > 0;
      };
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

      let clicked = false;
      for (const root of roots) {
        const inputs = Array.from(root.querySelectorAll("input[type='checkbox'], [role='checkbox']"));
        for (const input of inputs) {
          const checked = input.checked || input.getAttribute("aria-checked") === "true";
          if (checked) continue;
          const label = input.closest("label") || (input.id ? root.querySelector(`label[for="${input.id}"]`) : null);
          const labelText = label ? label.textContent : "";
          const ariaLabel = input.getAttribute("aria-label") || "";
          const text = `${labelText} ${ariaLabel}`;
          if (!matches(text)) continue;
          if (!isVisible(input) && (!label || !isVisible(label))) continue;
          if (label && isVisible(label)) {
            label.click();
          } else {
            input.click();
          }
          input.dispatchEvent(new Event("input", { bubbles: true }));
          input.dispatchEvent(new Event("change", { bubbles: true }));
          clicked = true;
        }
      }
      return clicked;
    });
  } catch (error) {
    return false;
  }
};

const submitFormFallback = async (context) =>
  context.evaluate(() => {
    const form = document.querySelector("#adForm");
    if (!form) return false;
    form.submit();
    return true;
  });

const getPublishStateSnapshot = async (context) =>
  context.evaluate(() => {
    const bodyText = document.body?.innerText || "";
    const url = window.location.href;
    if (
      url.includes("p-anzeige-aufgeben-bestaetigung") ||
      url.includes("anzeige-aufgeben-bestaetigung") ||
      url.includes("anzeige-aufgeben-schritt3") ||
      url.includes("anzeige-aufgeben-schritt4") ||
      url.includes("anzeige-aufgeben-danke") ||
      url.includes("anzeige-aufgeben-abschliessen") ||
      url.includes("meine-anzeigen") ||
      bodyText.includes("Anzeige wird aufgegeben") ||
      bodyText.includes("Anzeige wurde erstellt") ||
      bodyText.includes("Anzeige ist online") ||
      bodyText.includes("Anzeige wurde erfolgreich") ||
      bodyText.includes("Anzeige wurde veröffentlicht") ||
      bodyText.includes("Anzeige wird geprüft") ||
      bodyText.includes("Deine Anzeige wird geprüft") ||
      bodyText.includes("Wir prüfen deine Anzeige") ||
      bodyText.includes("Anzeige wurde eingereicht") ||
      bodyText.includes("Vielen Dank") ||
      bodyText.includes("Danke")
    ) {
      return "success";
    }
    const hasPreviewUrl = /vorschau|preview/i.test(url);
    const headingText = Array.from(document.querySelectorAll("h1, h2, h3"))
      .map((node) => node.textContent || "")
      .join(" ");
    const titleText = document.title || "";
    const previewHints = `${titleText} ${headingText}`.toLowerCase();
    const hasPreviewText = /vorschau/.test(previewHints);
    const previewButtons = Array.from(
      document.querySelectorAll('button[type="submit"], input[type="submit"], button')
    ).map((button) => (button.innerText || button.value || "").toLowerCase());
    const hasPublishButton = previewButtons.some((text) =>
      text.includes("veröffentlichen")
      || text.includes("veroeffentlichen")
      || text.includes("anzeige aufgeben")
    );
    if (hasPreviewUrl || (hasPreviewText && hasPublishButton)) {
      return "preview";
    }
    const confirmationElement = document.querySelector(
      '[data-testid*="success"], [data-test*="success"], [data-testid*="confirmation"], [data-test*="confirmation"], [class*="success"], [class*="confirmation"]'
    );
    if (confirmationElement) {
      const text = (confirmationElement.textContent || "").trim();
      if (text) {
        return "success";
      }
    }
    if (url.includes("anzeige-aufgeben") || url.includes("anzeige-abschicken")) {
      return "form";
    }
    return null;
  });

const getPublishStateFromFrames = async (page) => {
  for (const frame of page.frames()) {
    try {
      const state = await getPublishStateSnapshot(frame);
      if (state) return state;
    } catch (error) {
      // ignore frame errors
    }
  }
  return null;
};

const inferPublishSuccess = async (page) => {
  try {
    return await page.evaluate(() => {
      const url = window.location.href;
      const title = String(document.title || "");
      const titleLower = title.toLowerCase();
      const pageType = String(window.pageType || window.page_type || "");
      const pageTypeLower = pageType.toLowerCase();
      const bodyText = (document.body?.innerText || "").toLowerCase();
      const successHints = [
        "anzeige wurde",
        "anzeige ist online",
        "anzeige wird geprüft",
        "deine anzeige wird geprüft",
        "wir prüfen deine anzeige",
        "anzeige wurde eingereicht",
        "vielen dank",
        "danke"
      ];

      const walkNodes = (root) => {
        const nodes = [];
        if (!root) return nodes;
        const treeWalker = document.createTreeWalker(root, NodeFilter.SHOW_ELEMENT);
        let current = treeWalker.currentNode;
        while (current) {
          nodes.push(current);
          const shadowRoot = current.shadowRoot;
          if (shadowRoot) {
            nodes.push(...walkNodes(shadowRoot));
          }
          current = treeWalker.nextNode();
        }
        return nodes;
      };

      const allNodes = walkNodes(document);
      const shadowText = allNodes
        .map((node) => node.textContent || "")
        .join(" ");
      const hasShadowSuccessText = successHints.some((hint) => shadowText.toLowerCase().includes(hint));

      const hasAdLink = allNodes.some((node) => {
        if (!(node instanceof HTMLAnchorElement)) return false;
        const href = node.getAttribute("href") || "";
        return href.includes("/s-anzeige/") || href.includes("meine-anzeigen");
      });

      const hasSubmit = Array.from(
        document.querySelectorAll('button[type="submit"], input[type="submit"], button')
      ).some((button) => {
        const text = button.innerText || button.value || "";
        return text.includes("Anzeige aufgeben");
      });

      const hasSuccessText = successHints.some((hint) => bodyText.includes(hint));

      const isSuccessUrl = url.includes("p-anzeige-aufgeben-bestaetigung") ||
        url.includes("anzeige-aufgeben-bestaetigung") ||
        url.includes("anzeige-aufgeben-schritt3") ||
        url.includes("anzeige-aufgeben-schritt4") ||
        url.includes("anzeige-aufgeben-danke") ||
        url.includes("anzeige-aufgeben-abschliessen") ||
        url.includes("meine-anzeigen");
      const hasPageTypeSuccess = pageTypeLower.includes("postadsuccess");
      const hasSuccessTitle = titleLower.includes("geht bald online") ||
        titleLower.includes("anzeige geht") ||
        titleLower.includes("vielen dank") ||
        titleLower.includes("danke");

      const isKnownForm = url.includes("anzeige-abschicken") || (url.includes("anzeige-aufgeben") && !isSuccessUrl);
      const isPreview = url.includes("vorschau");

      return {
        url,
        title,
        pageType,
        isKnownForm,
        isPreview,
        isSuccessUrl,
        hasPageTypeSuccess,
        hasSuccessTitle,
        hasSubmit,
        hasSuccessText,
        hasShadowSuccessText,
        hasAdLink
      };
    });
  } catch (error) {
    return {
      url: "",
      title: "",
      pageType: "",
      isKnownForm: false,
      isPreview: false,
      isSuccessUrl: false,
      hasPageTypeSuccess: false,
      hasSuccessTitle: false,
      hasSubmit: false,
      hasSuccessText: false,
      hasShadowSuccessText: false,
      hasAdLink: false,
      error: error?.message || String(error)
    };
  }
};

const verifyPublishedByCheckingMyAds = async (browser, { title, deviceProfile, timeoutMs = 45000 } = {}) => {
  const rawTitle = String(title || "").replace(/\s+/g, " ").trim();
  if (!browser || !rawTitle) {
    return { verified: false, reason: "missing-browser-or-title" };
  }

  const normalizedTitle = rawTitle.toLowerCase();
  const needles = Array.from(
    new Set(
      [
        normalizedTitle,
        normalizedTitle.slice(0, 48).trim(),
        normalizedTitle.slice(0, 32).trim(),
        normalizedTitle.slice(0, 24).trim()
      ].filter((value) => value && value.length >= 10)
    )
  );
  if (!needles.length) {
    return { verified: false, reason: "title-too-short" };
  }

  const MY_ADS_URL = "https://www.kleinanzeigen.de/m-meine-anzeigen.html";
  let verifyPage = null;
  try {
    verifyPage = await browser.newPage();
    if (deviceProfile?.userAgent) {
      await verifyPage.setUserAgent(deviceProfile.userAgent);
    }
    if (deviceProfile?.viewport) {
      await verifyPage.setViewport(deviceProfile.viewport);
    }
    if (deviceProfile?.locale) {
      await verifyPage.setExtraHTTPHeaders({ "Accept-Language": deviceProfile.locale });
    }
    if (deviceProfile?.timezone) {
      await verifyPage.emulateTimezone(deviceProfile.timezone).catch(() => {});
    }

    const startedAt = Date.now();
    const attempts = 3;
    for (let attempt = 0; attempt < attempts; attempt += 1) {
      const remaining = timeoutMs - (Date.now() - startedAt);
      if (remaining <= 0) break;
      const navTimeout = Math.max(8000, Math.min(20000, remaining));

      await verifyPage.goto(MY_ADS_URL, { waitUntil: "domcontentloaded", timeout: navTimeout }).catch(() => {});
      await acceptCookieModal(verifyPage, { timeout: Math.min(7000, navTimeout) }).catch(() => {});
      if (isGdprPage(verifyPage.url())) {
        await acceptGdprConsent(verifyPage, { timeout: Math.min(15000, remaining) }).catch(() => {});
      }

      const result = await verifyPage.evaluate((needleList) => {
        const normalize = (value) => String(value || "").toLowerCase().replace(/\s+/g, " ").trim();
        const body = normalize(document.body?.innerText || "");
        const matchesBody = needleList.some((needle) => needle && body.includes(needle));

        let adUrl = "";
        if (needleList.length) {
          const anchors = Array.from(document.querySelectorAll("a[href]"));
          for (const anchor of anchors) {
            const href = anchor.getAttribute("href") || "";
            if (!href.includes("/s-anzeige/")) continue;
            const text = normalize(anchor.textContent || "");
            if (!text) continue;
            const matched = needleList.some((needle) => needle && text.includes(needle));
            if (!matched) continue;
            try {
              adUrl = new URL(href, window.location.origin).toString();
            } catch (error) {
              adUrl = href;
            }
            break;
          }
        }

        return { matchesBody, adUrl, url: window.location.href };
      }, needles).catch(() => null);

      if (result?.adUrl) {
        return { verified: true, method: "my-ads-ad-link", url: result.adUrl };
      }
      if (result?.matchesBody) {
        return { verified: true, method: "my-ads-body-text", url: result?.url || verifyPage.url() };
      }

      if (attempt < attempts - 1) {
        await sleep(2500);
        await verifyPage.reload({ waitUntil: "domcontentloaded", timeout: navTimeout }).catch(() => {});
        await sleep(1200);
      }
    }

    return { verified: false, reason: "not-found" };
  } catch (error) {
    return { verified: false, reason: error?.message || String(error) };
  } finally {
    if (verifyPage) {
      await verifyPage.close().catch(() => {});
    }
  }
};

const collectFormErrors = async (page) => {
  try {
    return await page.evaluate(() => {
      const selectors = [
        '[role="alert"]',
        '[data-testid*="error"]',
        '.error',
        '.error-message',
        '.form-error',
        '.formerror',
        '.validation-error',
        '#buyNow\\.errors'
      ];
      const nodes = selectors.flatMap((selector) =>
        Array.from(document.querySelectorAll(selector))
      );
      const normalize = (value) => (value || "").replace(/\s+/g, " ").trim();
      const isVisible = (node) => {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        if (style.display === "none" || style.visibility === "hidden") return false;
        const rect = node.getBoundingClientRect();
        return rect.width > 0 && rect.height > 0;
      };
      const texts = nodes
        .filter((node) => isVisible(node))
        .map((node) => normalize(node.textContent))
        .filter((text) => text.length > 0);
      const unique = Array.from(new Set(texts));
      return unique.slice(0, 6);
    });
  } catch (error) {
    return [];
  }
};

const repairInvalidRequiredFieldsInContext = async (context) => {
  try {
    return await context.evaluate(() => {
      const normalize = (value) => (value || "").replace(/\s+/g, " ").trim();
      const isVisible = (node) => {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        if (style.display === "none" || style.visibility === "hidden") return false;
        const rect = node.getBoundingClientRect();
        return rect.width > 0 && rect.height > 0;
      };
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

      const controls = [];
      roots.forEach((root) => {
        try {
          controls.push(...Array.from(root.querySelectorAll("input, select, textarea")));
        } catch (error) {
          // ignore root query errors
        }
      });

      const repaired = [];
      const repairedRadioNames = new Set();

      const markResult = (node, fixed) => {
        repaired.push({
          fixed: Boolean(fixed),
          tag: String(node.tagName || "").toLowerCase(),
          type: String(node.getAttribute("type") || "").toLowerCase(),
          name: String(node.getAttribute("name") || ""),
          id: String(node.id || ""),
          required: Boolean(node.required || node.getAttribute("aria-required") === "true"),
          validationMessage: normalize(node.validationMessage || "")
        });
      };

      for (const node of controls) {
        if (!node || node.disabled || !isVisible(node)) continue;
        const tag = String(node.tagName || "").toLowerCase();
        const type = String(node.getAttribute("type") || "").toLowerCase();
        if (type === "hidden" || type === "button") continue;

        const isInvalid = typeof node.checkValidity === "function" ? !node.checkValidity() : false;
        if (!isInvalid) continue;

        let fixed = false;

        if (tag === "select") {
          const option = Array.from(node.options || []).find((opt) => opt && !opt.disabled && String(opt.value || "").trim() !== "");
          if (option) {
            node.value = option.value;
            node.dispatchEvent(new Event("input", { bubbles: true }));
            node.dispatchEvent(new Event("change", { bubbles: true }));
            fixed = true;
          }
        } else if (type === "checkbox" && node.required && !node.checked) {
          node.click();
          fixed = true;
        } else if (type === "radio" && node.required) {
          const radioName = String(node.getAttribute("name") || "");
          if (radioName && !repairedRadioNames.has(radioName)) {
            const radios = controls.filter((item) =>
              item &&
              String(item.tagName || "").toLowerCase() === "input" &&
              String(item.getAttribute("type") || "").toLowerCase() === "radio" &&
              String(item.getAttribute("name") || "") === radioName &&
              !item.disabled &&
              isVisible(item)
            );
            const target = radios.find((item) => !item.checked) || radios[0];
            if (target) {
              target.click();
              fixed = true;
            }
            repairedRadioNames.add(radioName);
          }
        }

        markResult(node, fixed);
      }

      return {
        totalInvalid: repaired.length,
        fixedCount: repaired.filter((item) => item.fixed).length,
        items: repaired.slice(0, 30)
      };
    });
  } catch (error) {
    return {
      totalInvalid: 0,
      fixedCount: 0,
      items: [],
      error: error?.message || String(error)
    };
  }
};

const repairInvalidRequiredFieldsAcrossContexts = async (page, primaryContext) => {
  const contexts = [];
  const pushUnique = (ctx) => {
    if (!ctx || contexts.includes(ctx)) return;
    contexts.push(ctx);
  };
  pushUnique(primaryContext);
  pushUnique(page);
  try {
    page.frames().forEach(pushUnique);
  } catch (error) {
    // ignore frame access errors
  }

  const results = [];
  for (const context of contexts) {
    try {
      const result = await repairInvalidRequiredFieldsInContext(context);
      let contextUrl = "";
      try {
        contextUrl = typeof context.url === "function" ? context.url() : "";
      } catch (error) {
        contextUrl = "";
      }
      results.push({
        contextType: context === page ? "page" : "frame",
        contextUrl,
        ...result
      });
    } catch (error) {
      // ignore per-context repair errors
    }
  }

  return {
    totalInvalid: results.reduce((sum, item) => sum + Number(item?.totalInvalid || 0), 0),
    fixedCount: results.reduce((sum, item) => sum + Number(item?.fixedCount || 0), 0),
    contexts: results
  };
};

const repairServerSideRequiredErrorsInContext = async (context, preferredValues = []) => {
  try {
    return await context.evaluate((preferred) => {
      const normalize = (value) => (value || "").toString().replace(/\s+/g, " ").trim().toLowerCase();
      const preferredTokens = Array.isArray(preferred) ? preferred.map(normalize).filter(Boolean) : [];
      const isVisible = (node) => {
        if (!node) return false;
        const style = window.getComputedStyle(node);
        if (style.display === "none" || style.visibility === "hidden") return false;
        const rect = node.getBoundingClientRect();
        return rect.width > 0 && rect.height > 0;
      };
      const dispatchChanges = (node) => {
        node.dispatchEvent(new Event("input", { bubbles: true }));
        node.dispatchEvent(new Event("change", { bubbles: true }));
      };
      const isAttributeCandidate = (node) => {
        if (!node) return false;
        const name = normalize(node.getAttribute?.("name") || "");
        const id = normalize(node.getAttribute?.("id") || node.id || "");
        const className = normalize(node.getAttribute?.("class") || "");
        return name.includes("attributemap") ||
          id.includes("attributemap") ||
          className.includes("attribute");
      };

      const parseFieldKey = (errorNode) => {
        const ids = [
          String(errorNode?.id || ""),
          String(errorNode?.parentElement?.id || ""),
          String(errorNode?.closest?.("[id]")?.id || ""),
          String(errorNode?.closest?.("[id*='attributeMap']")?.id || "")
        ].filter(Boolean);
        for (const rawId of ids) {
          let match = rawId.match(/attributeMap\[([^\]]+)\]/);
          if (match && match[1]) return match[1];
          match = rawId.match(/^attributeMap(.+)\.errors$/i);
          if (match && match[1]) return match[1];
          match = rawId.match(/attributeMap([A-Za-z0-9_.-]+)(?:\.errors|-error)?$/i);
          if (match && match[1]) return match[1];
        }

        const formGroup = errorNode?.closest?.(".formgroup");
        if (formGroup) {
          const label = formGroup.querySelector("label[for]");
          const labelFor = String(label?.getAttribute("for") || "");
          if (labelFor) return labelFor;
        }
        return "";
      };

      const findControlByKey = (key, errorNode) => {
        if (key) {
          const byId = document.getElementById(key);
          if (byId) return byId;

          const targetName = `attributeMap[${key}]`;
          const byName = Array.from(document.querySelectorAll("select,input,textarea"))
            .find((node) => String(node.getAttribute("name") || "") === targetName);
          if (byName) return byName;
        }

        const formGroup = errorNode?.closest?.(".formgroup");
        if (formGroup) {
          const inGroup = formGroup.querySelector("select,input,textarea");
          if (inGroup) return inGroup;
        }
        return null;
      };

      const pickSelectValue = (selectNode) => {
        const options = Array.from(selectNode.options || [])
          .map((option) => ({
            value: String(option?.value || ""),
            text: String(option?.textContent || ""),
            disabled: Boolean(option?.disabled)
          }));

        let chosen = options.find((option) => {
          const valueNorm = normalize(option.value);
          const textNorm = normalize(option.text);
          if (!valueNorm || option.disabled) return false;
          return preferredTokens.some((token) =>
            token === valueNorm ||
            textNorm.includes(token) ||
            token.includes(valueNorm)
          );
        });

        if (!chosen) {
          chosen = options.find((option) => !option.disabled && normalize(option.value));
        }
        if (!chosen || !chosen.value) return null;
        selectNode.value = chosen.value;
        dispatchChanges(selectNode);
        return chosen;
      };
      const applyFixToControl = (control, options = {}) => {
        const { allowHidden = false } = options;
        if (!control || control.disabled) {
          return { fixed: false, tag: "", type: "", value: "", reason: "control-unavailable" };
        }
        if (!allowHidden && !isVisible(control)) {
          return { fixed: false, tag: "", type: "", value: "", reason: "control-hidden" };
        }

        const tag = String(control.tagName || "").toLowerCase();
        const type = String(control.getAttribute("type") || "").toLowerCase();
        let fixed = false;
        let value = "";

        if (tag === "select") {
          const chosen = pickSelectValue(control);
          fixed = Boolean(chosen?.value);
          value = chosen?.value || "";
        } else if (type === "radio") {
          const radioName = String(control.getAttribute("name") || "");
          if (radioName) {
            const radios = Array.from(document.querySelectorAll('input[type="radio"]'))
              .filter((node) =>
                !node.disabled &&
                String(node.getAttribute("name") || "") === radioName &&
                (allowHidden || isVisible(node))
              );
            const target = radios.find((node) => !node.checked) || radios[0];
            if (target) {
              target.checked = true;
              dispatchChanges(target);
              fixed = true;
              value = String(target.value || "");
            }
          }
        } else if (type === "checkbox") {
          if (!control.checked) {
            control.checked = true;
          }
          dispatchChanges(control);
          fixed = Boolean(control.checked);
          value = control.checked ? "true" : "false";
        } else if (tag === "input" || tag === "textarea") {
          const current = String(control.value || "").trim();
          if (!current) {
            const fallback = preferredTokens.find((token) => token.length > 1 && !/^\d+$/.test(token)) || "1";
            control.value = fallback;
            dispatchChanges(control);
            value = fallback;
            fixed = true;
          } else {
            fixed = true;
            value = current;
          }
        }

        return { fixed, tag, type, value };
      };

      const allErrorNodes = Array.from(
        document.querySelectorAll(".formerror, [id*='errors'], [id*='error']")
      );
      const uniqueErrors = [];
      const seenNodes = new Set();
      for (const node of allErrorNodes) {
        if (!node || seenNodes.has(node)) continue;
        seenNodes.add(node);
        uniqueErrors.push(node);
      }

      const requiredErrorNodes = uniqueErrors.filter((node) => {
        if (!isVisible(node)) return false;
        const text = normalize(node.textContent || "");
        return text.includes("bitte gib einen wert ein")
          || text.includes("bitte waehlen")
          || text.includes("bitte wählen")
          || text.includes("pflichtfeld")
          || text.includes("required");
      });

      const repairs = [];
      for (const errorNode of requiredErrorNodes) {
        const key = parseFieldKey(errorNode);
        const control = findControlByKey(key, errorNode);
        if (!control) {
          repairs.push({
            key,
            fixed: false,
            reason: "control-not-found",
            errorText: String(errorNode.textContent || "").trim().slice(0, 200)
          });
          continue;
        }

        const fixedResult = applyFixToControl(control);
        const { fixed, tag, type, value } = fixedResult;

        repairs.push({
          key,
          fixed,
          tag,
          type,
          value: String(value || "").slice(0, 120),
          id: String(control.id || ""),
          name: String(control.getAttribute("name") || ""),
          errorText: String(errorNode.textContent || "").trim().slice(0, 200)
        });
      }

      const bodyText = normalize(document.body?.innerText || "");
      const hasGenericRequiredError = bodyText.includes("bitte gib einen wert ein")
        || bodyText.includes("bitte waehlen")
        || bodyText.includes("bitte wählen")
        || bodyText.includes("pflichtfeld")
        || bodyText.includes("required");
      const needsGenericFallback = hasGenericRequiredError &&
        (requiredErrorNodes.length === 0 || repairs.every((item) => !item.fixed));
      if (needsGenericFallback) {
        const fallbackControls = Array.from(document.querySelectorAll("select,input,textarea"))
          .filter((node) => isAttributeCandidate(node))
          .filter((node) => {
            if (!node || node.disabled) return false;
            const tag = String(node.tagName || "").toLowerCase();
            const type = String(node.getAttribute("type") || "").toLowerCase();
            if (tag === "select") {
              return String(node.value || "").trim() === "";
            }
            if (type === "radio") {
              const radioName = String(node.getAttribute("name") || "");
              if (!radioName) return false;
              const radios = Array.from(document.querySelectorAll('input[type="radio"]'))
                .filter((radio) => !radio.disabled && String(radio.getAttribute("name") || "") === radioName);
              return radios.length > 0 && !radios.some((radio) => radio.checked);
            }
            if (type === "checkbox") {
              return !node.checked;
            }
            return String(node.value || "").trim() === "";
          });
        const handledRadioNames = new Set();
        for (const control of fallbackControls) {
          const type = String(control.getAttribute("type") || "").toLowerCase();
          const radioName = String(control.getAttribute("name") || "");
          if (type === "radio" && radioName) {
            if (handledRadioNames.has(radioName)) continue;
            handledRadioNames.add(radioName);
          }
          const fixedResult = applyFixToControl(control, { allowHidden: true });
          if (!fixedResult.fixed) continue;
          repairs.push({
            key: "",
            fixed: true,
            tag: fixedResult.tag,
            type: fixedResult.type,
            value: String(fixedResult.value || "").slice(0, 120),
            id: String(control.id || ""),
            name: String(control.getAttribute("name") || ""),
            errorText: "generic-required-fallback"
          });
        }
      }

      return {
        totalErrors: Math.max(requiredErrorNodes.length, hasGenericRequiredError ? 1 : 0),
        fixedCount: repairs.filter((item) => item.fixed).length,
        items: repairs.slice(0, 40)
      };
    }, preferredValues);
  } catch (error) {
    return {
      totalErrors: 0,
      fixedCount: 0,
      items: [],
      error: error?.message || String(error)
    };
  }
};

const repairServerSideRequiredErrorsAcrossContexts = async (page, primaryContext, preferredValues = []) => {
  const contexts = [];
  const pushUnique = (ctx) => {
    if (!ctx || contexts.includes(ctx)) return;
    contexts.push(ctx);
  };
  pushUnique(primaryContext);
  pushUnique(page);
  try {
    page.frames().forEach(pushUnique);
  } catch (error) {
    // ignore frame access errors
  }

  const results = [];
  for (const context of contexts) {
    try {
      const result = await repairServerSideRequiredErrorsInContext(context, preferredValues);
      let contextUrl = "";
      try {
        contextUrl = typeof context.url === "function" ? context.url() : "";
      } catch (error) {
        contextUrl = "";
      }
      results.push({
        contextType: context === page ? "page" : "frame",
        contextUrl,
        ...result
      });
    } catch (error) {
      // ignore per-context errors
    }
  }

  return {
    totalErrors: results.reduce((sum, item) => sum + Number(item?.totalErrors || 0), 0),
    fixedCount: results.reduce((sum, item) => sum + Number(item?.fixedCount || 0), 0),
    contexts: results
  };
};

const getAdFormContext = async (page, timeout = 20000) => {
  const candidateSelectors = [
    'input[name="title"]',
    'input[name="adTitle"]',
    'textarea[name="description"]',
    'textarea[name="adDescription"]',
    'input[name="price"]',
    'input[name="priceAmount"]',
    'input[id="micro-frontend-price"]'
  ];
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeout) {
    for (const frame of page.frames()) {
      try {
        for (const selector of candidateSelectors) {
          const handle = await frame.$(selector);
          if (handle) {
            return frame;
          }
        }
      } catch (error) {
        // ignore frame lookup errors
      }
    }
    for (const selector of candidateSelectors) {
      const handle = await page.$(selector);
      if (handle) {
        return page;
      }
    }
    await sleep(400);
  }
  return null;
};

const fillMissingRequiredFields = async (context) =>
  context.evaluate(() => {
    const isVisible = (node) => {
      if (!node) return false;
      const style = window.getComputedStyle(node);
      if (style.display === "none" || style.visibility === "hidden") return false;
      const rect = node.getBoundingClientRect();
      return rect.width > 0 && rect.height > 0;
    };

    const getLabel = (node) => {
      if (!node) return "";
      const aria = node.getAttribute("aria-label");
      if (aria) return aria.trim();
      if (node.id) {
        const label = document.querySelector(`label[for="${node.id}"]`);
        if (label) return (label.textContent || "").trim();
      }
      const parentLabel = node.closest("label");
      if (parentLabel) return (parentLabel.textContent || "").trim();
      return node.name || node.id || node.getAttribute("placeholder") || "";
    };

    const filled = [];

    const requiredSelects = Array.from(document.querySelectorAll("select[required], select[aria-required='true']"));
    requiredSelects.forEach((select) => {
      if (!isVisible(select)) return;
      if (select.value) return;
      const option = Array.from(select.options).find(
        (opt) => opt.value && !opt.disabled
      );
      if (!option) return;
      select.value = option.value;
      select.dispatchEvent(new Event("input", { bubbles: true }));
      select.dispatchEvent(new Event("change", { bubbles: true }));
      filled.push(getLabel(select));
    });

    const requiredCheckboxes = Array.from(
      document.querySelectorAll("input[type='checkbox'][required], input[type='checkbox'][aria-required='true']")
    );
    requiredCheckboxes.forEach((checkbox) => {
      if (!isVisible(checkbox)) return;
      if (checkbox.checked) return;
      checkbox.click();
      filled.push(getLabel(checkbox));
    });

    const radioGroups = new Map();
    const radios = Array.from(document.querySelectorAll("input[type='radio']"));
    radios.forEach((radio) => {
      if (!radio.name) return;
      const required = radio.required || radio.getAttribute("aria-required") === "true";
      if (!required) return;
      if (!radioGroups.has(radio.name)) {
        radioGroups.set(radio.name, []);
      }
      radioGroups.get(radio.name).push(radio);
    });
    radioGroups.forEach((group) => {
      const visibleGroup = group.filter(isVisible);
      if (!visibleGroup.length) return;
      const checked = visibleGroup.some((radio) => radio.checked);
      if (checked) return;
      const candidate = visibleGroup.find((radio) => !radio.disabled);
      if (!candidate) return;
      candidate.click();
      filled.push(getLabel(candidate));
    });

    return filled.filter((label) => label.length > 0);
  });

const autoSelectAttributeMapFields = async (context) =>
  context.evaluate(() => {
    const normalize = (value) => (value || "").replace(/\s+/g, " ").trim();
    const isVisible = (node) => {
      if (!node) return false;
      const style = window.getComputedStyle(node);
      if (style.display === "none" || style.visibility === "hidden") return false;
      const rect = node.getBoundingClientRect();
      return rect.width > 0 && rect.height > 0;
    };
    const getLabel = (node) => {
      if (!node) return "";
      const aria = node.getAttribute("aria-label");
      if (aria) return normalize(aria);
      if (node.id) {
        const label = document.querySelector(`label[for="${node.id}"]`);
        if (label) return normalize(label.textContent);
      }
      const parentLabel = node.closest("label");
      if (parentLabel) return normalize(parentLabel.textContent);
      return normalize(node.getAttribute("name") || node.getAttribute("id") || "");
    };
    const ignoreLabels = [
      "Preis",
      "Preisart",
      "Preistyp",
      "PLZ",
      "Ort",
      "Versand",
      "Versandmethoden",
      "Angebotstyp",
      "Gebot",
      "Gesuch",
      "Direkt kaufen"
    ];
    const isAttributeSelect = (select) => {
      const name = (select.getAttribute("name") || "").toLowerCase();
      const id = (select.getAttribute("id") || "").toLowerCase();
      const className = (select.getAttribute("class") || "").toLowerCase();
      return name.includes("attributemap") ||
        id.includes("attributemap") ||
        className.includes("pstad-select") ||
        className.includes("attribute");
    };

    const filled = [];
    const selects = Array.from(document.querySelectorAll("select"));
    selects.forEach((select) => {
      if (!isAttributeSelect(select)) return;
      if (select.disabled) return;
      if (select.value) return;
      const label = getLabel(select);
      if (label && ignoreLabels.some((item) => label.toLowerCase().includes(item.toLowerCase()))) return;
      const option = Array.from(select.options || [])
        .find((opt) => opt.value && !opt.disabled);
      if (!option) return;
      select.value = option.value;
      select.dispatchEvent(new Event("input", { bubbles: true }));
      select.dispatchEvent(new Event("change", { bubbles: true }));
      filled.push(label || select.getAttribute("name") || select.getAttribute("id") || "attribute");
    });

    const radios = Array.from(document.querySelectorAll("input[type='radio']"));
    const groups = new Map();
    radios.forEach((radio) => {
      const name = radio.getAttribute("name") || "";
      if (!name || !/attributeMap/i.test(name)) return;
      if (!groups.has(name)) groups.set(name, []);
      groups.get(name).push(radio);
    });
    groups.forEach((group) => {
      if (group.some((radio) => radio.checked)) return;
      const visible = group.filter(isVisible);
      if (!visible.length) return;
      const candidate = visible.find((radio) => !radio.disabled) || visible[0];
      if (!candidate) return;
      candidate.click();
      filled.push(getLabel(candidate) || candidate.getAttribute("name") || "attribute");
    });

    return filled.filter((label) => label.length > 0);
  });

const autoSelectAttributeMapFieldsAcrossContexts = async (page, primaryContext) => {
  const filled = [];
  const seen = new Set();
  const contexts = [];
  if (primaryContext) contexts.push(primaryContext);
  if (primaryContext !== page) contexts.push(page);
  try {
    page.frames().forEach((frame) => contexts.push(frame));
  } catch (error) {
    // ignore frame access errors
  }
  for (const context of contexts) {
    try {
      const result = await autoSelectAttributeMapFields(context);
      if (!Array.isArray(result)) continue;
      result.forEach((label) => {
        if (!label || seen.has(label)) return;
        seen.add(label);
        filled.push(label);
      });
    } catch (error) {
      // ignore context errors
    }
  }
  return filled;
};

const clickCategoryWeiter = async (page) => {
  await page.waitForSelector("body", { timeout: 15000 });
  await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
  await sleep(500);
  let clicked = false;
  try {
    const buttonHandle = await page.$(
      "#postad-step1-frm button[type='submit'], form[action*='p-kategorie-aendern'] button[type='submit'], button.button[type='submit']"
    );
    if (buttonHandle) {
      clicked = await safeClick(buttonHandle);
    }
  } catch (error) {
    // ignore button lookup errors
  }
  if (!clicked) {
    try {
      clicked = await page.evaluate(() => {
        const buttons = Array.from(document.querySelectorAll("button[type='submit']"));
        const match = buttons.find((button) => {
          const text = (button.textContent || "").replace(/\s+/g, " ").trim();
          return text === "Weiter";
        });
        if (!match) return false;
        match.click();
        return true;
      });
    } catch (error) {
      clicked = false;
    }
  }
  if (!clicked) {
    clicked = await clickByText(page, ["Weiter"]);
  }
  if (!clicked) {
    try {
      clicked = await page.evaluate(() => {
        const normalize = (value) => (value || "").replace(/\s+/g, " ").trim().toLowerCase();
        const target = "weiter";
        const isVisible = (node) => {
          if (!node) return false;
          const style = window.getComputedStyle(node);
          if (style.display === "none" || style.visibility === "hidden") return false;
          const rect = node.getBoundingClientRect();
          return rect.width > 0 && rect.height > 0;
        };
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
        for (const root of roots) {
          const candidates = Array.from(root.querySelectorAll("button, a, [role='button'], input[type='submit']"));
          for (const node of candidates) {
            if (!isVisible(node)) continue;
            const text = normalize(node.textContent || node.value || node.getAttribute("aria-label") || node.getAttribute("title"));
            if (text === target || text.includes(target)) {
              node.click();
              return true;
            }
          }
        }
        return false;
      });
    } catch (error) {
      clicked = false;
    }
  }
  if (!clicked) return false;
  try {
    await Promise.race([
      page.waitForNavigation({ waitUntil: "domcontentloaded", timeout: 20000 }),
      page.waitForFunction(
        () => window.location.href.includes("anzeige-aufgeben-schritt2"),
        { timeout: 20000 }
      )
    ]);
  } catch (error) {
    // continue even if we cannot confirm navigation
  }
  return true;
};

const openCategorySelection = async (page, { selectionUrl } = {}) => {
  const resolveSelectionUrl = async () => {
    try {
      return await page.evaluate(() => {
        const base = window.location.origin || "https://www.kleinanzeigen.de";
        const direct = window.Belen?.PostAd?.PostAdView?.urlSelectCategory
          || window.Belen?.PostAd?.PostAdView?.options?.urlSelectCategory
          || window.Belen?.PostAd?.PostAdView?.config?.urlSelectCategory
          || window.Belen?.PostAd?.PostAdView?.settings?.urlSelectCategory;
        if (!direct) return "";
        try {
          return new URL(direct, base).toString();
        } catch (error) {
          return direct;
        }
      });
    } catch (error) {
      return "";
    }
  };
  const resolvedUrl = normalizeSelectionUrl(await resolveSelectionUrl());
  const providedUrl = normalizeSelectionUrl(selectionUrl);
  const directUrl = resolvedUrl || providedUrl;
  const tryGoto = async (url) => {
    if (!url) return false;
    try {
      const needsReload = !page.url().includes("p-kategorie-aendern") || await isError400Page(page);
      if (needsReload) {
        await page.goto(url, { waitUntil: "domcontentloaded", timeout: 20000 });
        await humanPause(200, 400);
      }
    } catch (error) {
      return false;
    }
    if (await isError400Page(page)) {
      return false;
    }
    return isCategorySelectionPageReady(page);
  };

  if (directUrl) {
    const opened = await tryGoto(directUrl);
    if (opened) return true;
  }

  if (resolvedUrl && providedUrl && resolvedUrl !== providedUrl) {
    const opened = await tryGoto(providedUrl);
    if (opened) return true;
  }

  const selectors = [
    "#pstad-lnk-chngeCtgry",
    "#categorySection a",
    "a[href*='p-kategorie-aendern']"
  ];
  for (const selector of selectors) {
    const element = await page.$(selector);
    if (!element) continue;
    if (!(await safeClick(element))) continue;
    try {
      await Promise.race([
        page.waitForNavigation({ waitUntil: "domcontentloaded", timeout: 15000 }),
        page.waitForFunction(
          () => window.location.href.includes("p-kategorie-aendern"),
          { timeout: 15000 }
        )
      ]);
    } catch (error) {
      // ignore navigation timeout
    }
    if (await isCategorySelectionPageReady(page)) return true;
  }
  const clicked = await clickByText(page, ["Wähle deine Kategorie", "Kategorie wählen"]);
  if (clicked) {
    try {
      await Promise.race([
        page.waitForNavigation({ waitUntil: "domcontentloaded", timeout: 15000 }),
        page.waitForFunction(
          () => window.location.href.includes("p-kategorie-aendern"),
          { timeout: 15000 }
        )
      ]);
    } catch (error) {
      // ignore navigation timeout
    }
    if (await isCategorySelectionPageReady(page)) return true;
  }
  const fallbackUrl = "https://www.kleinanzeigen.de/p-kategorie-aendern.html";
  return tryGoto(fallbackUrl);
};

const openCategorySelectionByPost = async (page) => {
  try {
    await page.waitForSelector("form", { timeout: 10000 });
  } catch (error) {
    return false;
  }
  let originalForm = null;
  try {
    originalForm = await page.evaluate(() => {
      const form = document.querySelector("#adForm") || document.querySelector("form");
      if (!form) return null;
      return {
        action: form.getAttribute("action"),
        method: form.getAttribute("method")
      };
    });
  } catch (error) {
    originalForm = null;
  }
  let didSubmit = false;
  try {
    didSubmit = await withTimeout(page.evaluate(() => {
      const form = document.querySelector("#adForm") || document.querySelector("form");
      if (!form) return false;
      form.setAttribute("action", "/p-kategorie-aendern.html");
      form.setAttribute("method", "post");
      form.submit();
      return true;
    }), 5000, "category-post-timeout");
  } catch (error) {
    didSubmit = false;
  }
  if (!didSubmit) return false;
  try {
    await withTimeout(page.waitForNavigation({ waitUntil: "domcontentloaded", timeout: 20000 }), 8000, "category-post-nav-timeout");
  } catch (error) {
    // ignore navigation timeout
  }
  const ready = await isCategorySelectionPageReady(page);
  if (!ready && originalForm) {
    try {
      await page.evaluate((restore) => {
        const form = document.querySelector("#adForm") || document.querySelector("form");
        if (!form) return;
        if (restore.action !== null && restore.action !== undefined) {
          form.setAttribute("action", restore.action);
        }
        if (restore.method !== null && restore.method !== undefined) {
          form.setAttribute("method", restore.method);
        }
      }, originalForm);
    } catch (error) {
      // ignore restore errors
    }
  }
  return ready;
};

const isError400Page = async (page) => {
  try {
    const title = await page.title();
    if (/Fehler\s*\[?400/i.test(title)) return true;
  } catch (error) {
    // ignore title errors
  }
  try {
    return await page.evaluate(() => {
      const text = (document.body?.innerText || "").toLowerCase();
      return text.includes("fehler [400]") || text.includes("fehler 400");
    });
  } catch (error) {
    return false;
  }
};

const selectCategoryViaNewPage = async (browser, {
  cookies = [],
  deviceProfile,
  categoryPathIds = [],
  accountLabel = "account",
  selectionUrl = ""
} = {}) => {
  if (!browser || !categoryPathIds.length) return false;
  let selectionPage = null;
  try {
    selectionPage = await browser.newPage();
    if (deviceProfile?.userAgent) {
      await selectionPage.setUserAgent(deviceProfile.userAgent);
    }
    if (deviceProfile?.viewport) {
      await selectionPage.setViewport(deviceProfile.viewport);
    }
    if (deviceProfile?.locale) {
      await selectionPage.setExtraHTTPHeaders({ "Accept-Language": deviceProfile.locale });
    }
    if (deviceProfile?.timezone) {
      await selectionPage.emulateTimezone(deviceProfile.timezone);
    }
    if (cookies.length) {
      await selectionPage.setCookie(...cookies);
    }
    const normalizedSelectionUrl = normalizeSelectionUrl(selectionUrl);
    let opened = false;
    if (normalizedSelectionUrl) {
      appendPublishTrace({ step: "category-selection-new-page-goto", url: normalizedSelectionUrl });
      try {
        await selectionPage.goto(normalizedSelectionUrl, { waitUntil: "domcontentloaded", timeout: 20000 });
      } catch (error) {
        appendPublishTrace({ step: "category-selection-new-page-goto-failed", error: error?.message || "goto-failed" });
        // ignore direct navigation errors
      }
      await acceptCookieModal(selectionPage, { timeout: 15000 }).catch(() => {});
      if (isGdprPage(selectionPage.url())) {
        await acceptGdprConsent(selectionPage, { timeout: 20000 }).catch(() => {});
      }
      opened = await isCategorySelectionPageReady(selectionPage);
      appendPublishTrace({ step: "category-selection-new-page-ready", opened, url: selectionPage.url() });
    }

    if (!opened) {
      appendPublishTrace({ step: "category-selection-new-page-open-via-form" });
      await selectionPage.goto(CREATE_AD_URL, { waitUntil: "domcontentloaded", timeout: 20000 });
      await acceptCookieModal(selectionPage, { timeout: 15000 }).catch(() => {});
      if (isGdprPage(selectionPage.url())) {
        await acceptGdprConsent(selectionPage, { timeout: 20000 }).catch(() => {});
      }
      opened = await withTimeout((async () => {
        let opened = await openCategorySelectionByPost(selectionPage);
        if (!opened) {
          opened = await openCategorySelection(selectionPage, { selectionUrl });
        }
        return opened;
      })(), 15000, "category-selection-open-timeout");
      appendPublishTrace({ step: "category-selection-new-page-opened", opened, url: selectionPage.url() });
    }

    if (!opened || await isError400Page(selectionPage)) {
      await dumpPublishDebug(selectionPage, {
        accountLabel,
        step: "category-selection-open-failed",
        error: "category-selection-open-failed",
        extra: { url: selectionPage.url(), categoryPathIds }
      });
      return false;
    }

    await dumpPublishDebug(selectionPage, {
      accountLabel,
      step: "category-selection-opened",
      error: "",
      extra: { url: selectionPage.url(), categoryPathIds }
    });

    const ready = await waitForCategorySelectionReady(selectionPage, 15000);
    if (!ready) {
      await dumpPublishDebug(selectionPage, {
        accountLabel,
        step: "category-selection-wait-timeout",
        error: "category-selection-not-ready",
        extra: { url: selectionPage.url(), categoryPathIds }
      });
    }

    const treeAvailable = await hasCategoryTree(selectionPage);
    let treeResult = null;
    if (treeAvailable) {
      treeResult = await applyCategoryPathViaTree(selectionPage, categoryPathIds);
    }
    if (treeResult?.success) {
      let navigated = false;
      try {
        await Promise.race([
          selectionPage.waitForNavigation({ waitUntil: "domcontentloaded", timeout: 15000 }).then(() => true).catch(() => false),
          selectionPage.waitForFunction(
            () => window.location.href.includes("anzeige-aufgeben-schritt2"),
            { timeout: 15000 }
          ).then(() => true).catch(() => false)
        ]).then((result) => {
          navigated = Boolean(result);
        });
      } catch (error) {
        navigated = false;
      }
      if (!navigated) {
        await clickCategoryWeiter(selectionPage);
        try {
          await Promise.race([
            selectionPage.waitForNavigation({ waitUntil: "domcontentloaded", timeout: 15000 }).then(() => true).catch(() => false),
            selectionPage.waitForFunction(
              () => window.location.href.includes("anzeige-aufgeben-schritt2"),
              { timeout: 15000 }
            ).then(() => true).catch(() => false)
          ]).then((result) => {
            navigated = Boolean(result) || navigated;
          });
        } catch (error) {
          // ignore
        }
      }
      await dumpPublishDebug(selectionPage, {
        accountLabel,
        step: "category-selection-tree-submit",
        error: "",
        extra: { url: selectionPage.url(), categoryPathIds, treeResult, navigated, treeAvailable }
      });
      if (navigated) {
        return true;
      }
      treeResult = { ...treeResult, navigated: false };
    }

    let pathApplied = await selectCategoryPathOnSelectionPage(selectionPage, categoryPathIds.filter((item) => /^\d+$/.test(String(item))));
    if (!pathApplied) {
      await dumpPublishDebug(selectionPage, {
        accountLabel,
        step: "category-selection-path-failed",
        error: "category-selection-path-failed",
        extra: { url: selectionPage.url(), categoryPathIds, treeResult }
      });
      return false;
    }

    await clickCategoryWeiter(selectionPage);
    try {
      await selectionPage.waitForNavigation({ waitUntil: "domcontentloaded", timeout: 15000 });
    } catch (error) {
      // ignore navigation timeout
    }

    await dumpPublishDebug(selectionPage, {
      accountLabel,
      step: "category-selection-done",
      error: "",
      extra: { url: selectionPage.url(), categoryPathIds }
    });
    return true;
  } catch (error) {
    if (selectionPage) {
      await dumpPublishDebug(selectionPage, {
        accountLabel,
        step: "category-selection-exception",
        error: error.message,
        extra: { url: selectionPage.url(), categoryPathIds }
      });
    }
    return false;
  } finally {
    if (selectionPage) {
      await selectionPage.close().catch(() => {});
    }
  }
};

const ensureRequiredFields = async (context) =>
  context.evaluate(() => {
    const getValue = (selectors) => {
      for (const selector of selectors) {
        const node = document.querySelector(selector);
        if (!node) continue;
        const rawValue = node.value ?? node.getAttribute("value") ?? "";
        const value = String(rawValue).trim();
        if (value) return value;
      }
      return "";
    };

    const getTextContent = (selectors) => {
      for (const selector of selectors) {
        const node = document.querySelector(selector);
        if (!node) continue;
        const rawValue = node.value ?? node.textContent ?? node.getAttribute("value") ?? "";
        const value = String(rawValue).trim();
        if (value) return value;
      }
      return "";
    };

    const getValueByLabel = (labels) => {
      const labelNodes = Array.from(document.querySelectorAll("label"));
      const normalize = (value) => (value || "").replace(/\s+/g, " ").trim();
      for (const labelText of labels) {
        const label = labelNodes.find((node) =>
          normalize(node.textContent).includes(labelText)
        );
        if (!label) continue;
        const forId = label.getAttribute("for");
        if (forId) {
          const target = document.getElementById(forId);
          if (target && "value" in target) {
            const value = (target.value || "").trim();
            if (value) return value;
          }
        }
        const input = label.querySelector("input, textarea, select");
        if (input && "value" in input) {
          const value = (input.value || "").trim();
          if (value) return value;
        }
      }
      return "";
    };

    const title = getValue([
      'input[name="title"]',
      'input[name="adTitle"]',
      'input[id*="title"]',
      'input[id*="adTitle"]',
      'input[placeholder*="Titel"]',
      'input[aria-label*="Titel"]',
      'input[data-testid*="title"]'
    ]) || getValueByLabel(["Titel", "Title"]);
    const description = getValue([
      'textarea[name="description"]',
      'textarea[name="adDescription"]',
      'textarea[id*="description"]',
      'textarea[id*="adDescription"]',
      'textarea[placeholder*="Beschreibung"]',
      'textarea[aria-label*="Beschreibung"]',
      'textarea[data-testid*="description"]'
    ])
      || getTextContent([
        '[contenteditable="true"][aria-label*="Beschreibung"]',
        '[role="textbox"][aria-label*="Beschreibung"]',
        '[contenteditable="true"][data-testid*="description"]',
        '[role="textbox"][data-testid*="description"]',
        '[contenteditable="true"][id*="description"]',
        '[role="textbox"][id*="description"]'
      ])
      || getValueByLabel(["Beschreibung", "Description"]);
    const price = getValue([
      'input[name="price"]',
      'input[name="priceAmount"]',
      'input[name="priceInCents"]',
      'input[id*="priceAmount"]',
      'input[id="micro-frontend-price"]',
      'input[id*="price"]',
      'input[placeholder*="Preis"]',
      'input[aria-label*="Preis"]',
      'input[data-testid*="price"]'
    ]) || getValueByLabel(["Preis", "Price"]);
    const category = getValue([
      'input[name="categoryId"]',
      'select[name="categoryId"]',
      'input[id*="category"]',
      'select[id*="category"]',
      'input[data-testid*="category"]',
      'select[data-testid*="category"]'
    ]) || getValueByLabel(["Kategorie", "Category"]);

    const categorySummary = Array.from(
      document.querySelectorAll(
        '[data-testid*="category"], [data-test*="category"], [class*="category"], a[href*="kategorie-aendern"]'
      )
    )
      .map((node) => (node.textContent || "").trim())
      .find((text) => text.length > 0);

    return {
      titleFilled: Boolean(title),
      descriptionFilled: Boolean(description),
      priceFilled: Boolean(price),
      categorySelected: Boolean(category) || Boolean(categorySummary)
    };
  });

const mergeRequiredStates = (primary, secondary) => ({
  titleFilled: Boolean(primary?.titleFilled || secondary?.titleFilled),
  descriptionFilled: Boolean(primary?.descriptionFilled || secondary?.descriptionFilled),
  priceFilled: Boolean(primary?.priceFilled || secondary?.priceFilled),
  categorySelected: Boolean(primary?.categorySelected || secondary?.categorySelected)
});

const ensureRequiredFieldsAcrossContexts = async (page, primaryContext) => {
  let merged = {
    titleFilled: false,
    descriptionFilled: false,
    priceFilled: false,
    categorySelected: false
  };
  const contexts = [primaryContext];
  if (primaryContext !== page) contexts.push(page);
  for (const frame of page.frames()) {
    contexts.push(frame);
  }
  for (const context of contexts) {
    try {
      const state = await ensureRequiredFields(context);
      merged = mergeRequiredStates(merged, state);
    } catch (error) {
      // ignore cross-origin frames
    }
  }
  return merged;
};

const getPriceValue = async (context) =>
  context.evaluate(() => {
    const selectors = [
      'input[name="price"]',
      'input[name="priceAmount"]',
      'input[name="priceInCents"]',
      'input[id*="priceAmount"]',
      'input[id="micro-frontend-price"]',
      'input[id*="price"]',
      'input[placeholder*="Preis"]',
      'input[aria-label*="Preis"]',
      'input[data-testid*="price"]'
    ];
    for (const selector of selectors) {
      const node = document.querySelector(selector);
      if (!node) continue;
      const rawValue = node.value ?? node.getAttribute("value") ?? "";
      const value = String(rawValue).trim();
      if (value) return value;
    }
    return "";
  });

const fillPriceField = async (context, value) => {
  if (isBlankValue(value)) return false;
  const selectors = [
    'input#micro-frontend-price',
    'input[name="price"]',
    'input[name="priceAmount"]',
    'input[name="priceInCents"]',
    'input[id*="priceAmount"]',
    'input[id*="price"]',
    'input[placeholder*="Preis"]',
    'input[aria-label*="Preis"]',
    'input[data-testid*="price"]'
  ];
  let hiddenCandidate = null;
  for (const selector of selectors) {
    let element = null;
    try {
      element = await context.waitForSelector(selector, { visible: true, timeout: 2000 });
    } catch (error) {
      element = null;
    }
    if (!element) {
      const candidate = await context.$(selector);
      if (candidate) {
        const visible = await isHandleVisible(candidate);
        if (visible) {
          element = candidate;
        } else if (!hiddenCandidate) {
          hiddenCandidate = candidate;
        }
      }
    }
    if (!element) continue;
    await scrollIntoView(element);
    try {
      await element.click({ clickCount: 3, delay: 40 });
    } catch (error) {
      await element.evaluate((node) => node.click());
    }
    try {
      await element.type(String(value), { delay: 40 + Math.floor(Math.random() * 40) });
    } catch (error) {
      await setValueOnHandle(element, value);
    }
    await element.evaluate((node) => node.blur());
    await humanPause(80, 160);
    const currentValue = await getPriceValue(context);
    if (currentValue) {
      return true;
    }
  }
  if (hiddenCandidate) {
    try {
      await setValueOnHandle(hiddenCandidate, value);
      await humanPause(80, 160);
      const currentValue = await getPriceValue(context);
      if (currentValue) return true;
    } catch (error) {
      // ignore hidden input errors
    }
  }
  return false;
};

const escapeSelectorAttrValue = (value) => String(value ?? "")
  .replace(/\\/g, "\\\\")
  .replace(/"/g, '\\"');

const buildSelectFieldSelectors = (fieldName) => {
  const normalized = String(fieldName || "").trim();
  if (!normalized) return [];
  const safe = escapeSelectorAttrValue(normalized);
  return [
    `select[name="${safe}"]`,
    `select[id="${safe}"]`
  ];
};

const selectOption = async (context, selectors, value) => {
  if (isBlankValue(value)) return false;
  for (const selector of selectors) {
    if (!selector || typeof selector !== "string") continue;
    let element = null;
    try {
      element = await context.$(selector);
    } catch (error) {
      // ignore invalid selector syntax and keep trying fallbacks
      continue;
    }
    if (!element) continue;
    try {
      await context.select(selector, String(value));
      return true;
    } catch (error) {
      // Fallback to direct value set when option exists but select() fails.
      try {
        const applied = await element.evaluate((node, targetValue) => {
          if (!node || node.tagName !== "SELECT") return false;
          const desired = String(targetValue ?? "");
          const hasOption = Array.from(node.options || []).some((opt) => String(opt.value) === desired);
          if (!hasOption) return false;
          node.value = desired;
          node.dispatchEvent(new Event("input", { bubbles: true }));
          node.dispatchEvent(new Event("change", { bubbles: true }));
          return true;
        }, value);
        if (applied) return true;
      } catch (innerError) {
        // ignore and continue selector loop
      }
    }
  }
  return false;
};

const selectOptionByLabel = async (context, labelText, value) => {
  if (isBlankValue(labelText) || isBlankValue(value)) return false;
  return context.evaluate(({ labelText, value }) => {
    const normalize = (val) => (val || "").replace(/\s+/g, " ").trim().toLowerCase();
    const labelNodes = Array.from(document.querySelectorAll("label"));
    const target = labelNodes.find((node) => normalize(node.textContent).includes(normalize(labelText)));
    if (!target) return false;
    const forId = target.getAttribute("for");
    let select = null;
    if (forId) {
      const candidate = document.getElementById(forId);
      if (candidate && candidate.tagName === "SELECT") {
        select = candidate;
      }
    }
    if (!select) {
      const candidate = target.querySelector("select");
      if (candidate) select = candidate;
    }
    if (!select) {
      const wrapper = target.closest("div") || target.parentElement;
      if (wrapper) {
        const candidate = wrapper.querySelector("select");
        if (candidate) select = candidate;
      }
    }
    if (!select) return false;
    select.value = value;
    select.dispatchEvent(new Event("input", { bubbles: true }));
    select.dispatchEvent(new Event("change", { bubbles: true }));
    return true;
  }, { labelText, value });
};

const normalizeExtraFieldEntries = (extraFields) => {
  if (!extraFields) return [];
  if (Array.isArray(extraFields)) return extraFields.filter(Boolean);
  if (typeof extraFields === "object") {
    return Object.entries(extraFields).map(([name, value]) => ({ name, value }));
  }
  return [];
};

const applyExtraFieldsToSelects = async (page, primaryContext, extraFields) => {
  const entries = normalizeExtraFieldEntries(extraFields);
  if (!entries.length) {
    return { total: 0, appliedCount: 0 };
  }

  const contexts = [];
  const pushUnique = (ctx) => {
    if (!ctx || contexts.includes(ctx)) return;
    contexts.push(ctx);
  };
  pushUnique(primaryContext);
  pushUnique(page);

  let appliedCount = 0;
  for (const entry of entries) {
    if (!entry) continue;
    const fieldName = entry.name || "";
    const fieldValue = entry.value;
    const fieldLabel = entry.label || entry.name || "";
    if (isBlankValue(fieldValue)) continue;

    const selectors = fieldName ? buildSelectFieldSelectors(fieldName) : [];
    let applied = false;

    if (selectors.length) {
      for (const ctx of contexts) {
        applied = await selectOption(ctx, selectors, fieldValue);
        if (applied) break;
      }
    }

    if (!applied && fieldLabel) {
      for (const ctx of contexts) {
        applied = await selectOptionByLabel(ctx, fieldLabel, fieldValue);
        if (applied) break;
      }
    }

    if (applied) {
      appliedCount += 1;
      await humanPause(80, 140);
    }
  }

  return { total: entries.length, appliedCount };
};

const applyPreferredAttributeSelectsInContext = async (context, preferredValues = []) => {
  try {
    return await context.evaluate((preferred) => {
      const normalize = (value) => (value || "").toString().replace(/\s+/g, " ").trim().toLowerCase();
      const preferredTokens = Array.isArray(preferred)
        ? preferred
          .map(normalize)
          .filter((token) => token && token.length >= 3 && /[a-z]/.test(token))
        : [];
      if (!preferredTokens.length) return { changed: 0, applied: [] };

      const dispatchChanges = (node) => {
        node.dispatchEvent(new Event("input", { bubbles: true }));
        node.dispatchEvent(new Event("change", { bubbles: true }));
      };

      const selects = Array.from(document.querySelectorAll('select[name^="attributeMap["]'));
      let changed = 0;
      const applied = [];

      for (const select of selects) {
        if (!select || select.disabled) continue;
        if (String(select.value || "").trim()) continue;

        const options = Array.from(select.options || [])
          .map((option) => ({
            value: String(option?.value || ""),
            text: String(option?.textContent || ""),
            disabled: Boolean(option?.disabled)
          }))
          .filter((opt) => opt.value && !opt.disabled);

        if (!options.length) continue;

        const chosen = options.find((option) => {
          const valueNorm = normalize(option.value);
          const textNorm = normalize(option.text);
          return preferredTokens.some((token) =>
            token === valueNorm ||
            valueNorm.includes(token) ||
            textNorm.includes(token)
          );
        });
        if (!chosen) continue;

        select.value = chosen.value;
        dispatchChanges(select);
        changed += 1;
        applied.push({
          id: String(select.id || ""),
          name: String(select.getAttribute("name") || ""),
          value: chosen.value
        });
      }

      return { changed, applied: applied.slice(0, 20) };
    }, preferredValues);
  } catch (error) {
    return { changed: 0, applied: [], error: error?.message || String(error) };
  }
};

const applyPreferredAttributeSelectsAcrossContexts = async (page, primaryContext, preferredValues = []) => {
  const contexts = [];
  const pushUnique = (ctx) => {
    if (!ctx || contexts.includes(ctx)) return;
    contexts.push(ctx);
  };
  pushUnique(primaryContext);
  pushUnique(page);

  const results = [];
  for (const context of contexts) {
    try {
      const result = await applyPreferredAttributeSelectsInContext(context, preferredValues);
      let contextUrl = "";
      try {
        contextUrl = typeof context.url === "function" ? context.url() : "";
      } catch (error) {
        contextUrl = "";
      }
      results.push({
        contextType: context === page ? "page" : "frame",
        contextUrl,
        ...result
      });
    } catch (error) {
      // ignore context errors
    }
  }

  return {
    changed: results.reduce((sum, item) => sum + Number(item?.changed || 0), 0),
    contexts: results
  };
};

const ensureVersandSelectionInContext = async (context, desiredValue) => {
  try {
    return await context.evaluate((desired) => {
      const normalize = (value) => String(value || "").trim();
      const dispatchChanges = (node) => {
        node.dispatchEvent(new Event("input", { bubbles: true }));
        node.dispatchEvent(new Event("change", { bubbles: true }));
      };

      const candidates = [];
      const selectors = [
        'select[id$=".versand_s"]',
        'select[name*=".versand_s]"]',
        'select[name*="versand_s"]'
      ];
      selectors.forEach((selector) => {
        try {
          candidates.push(...Array.from(document.querySelectorAll(selector)));
        } catch (error) {
          // ignore selector errors
        }
      });
      const selects = Array.from(new Set(candidates));

      const items = [];
      let changed = 0;
      const target = normalize(desired).toLowerCase();

      for (const select of selects) {
        if (!select || select.disabled) continue;
        if (normalize(select.value)) continue;

        const options = Array.from(select.options || [])
          .filter((opt) => opt && !opt.disabled && normalize(opt.value));
        if (!options.length) continue;

        let next = "";
        if (target) {
          const match = options.find((opt) => normalize(opt.value).toLowerCase() === target);
          if (match) next = match.value;
        }
        if (!next) {
          next = options[0].value;
        }
        if (!next) continue;

        select.value = next;
        dispatchChanges(select);
        changed += 1;
        items.push({
          id: normalize(select.id),
          name: normalize(select.getAttribute("name") || ""),
          value: next
        });
      }

      return { changed, items: items.slice(0, 10) };
    }, desiredValue);
  } catch (error) {
    return { changed: 0, items: [], error: error?.message || String(error) };
  }
};

const ensureVersandSelectionAcrossContexts = async (page, primaryContext, desiredValue) => {
  const contexts = [];
  const pushUnique = (ctx) => {
    if (!ctx || contexts.includes(ctx)) return;
    contexts.push(ctx);
  };
  pushUnique(primaryContext);
  pushUnique(page);

  const results = [];
  for (const context of contexts) {
    try {
      const result = await ensureVersandSelectionInContext(context, desiredValue);
      let contextUrl = "";
      try {
        contextUrl = typeof context.url === "function" ? context.url() : "";
      } catch (error) {
        contextUrl = "";
      }
      results.push({
        contextType: context === page ? "page" : "frame",
        contextUrl,
        ...result
      });
    } catch (error) {
      // ignore context errors
    }
  }

  return {
    changed: results.reduce((sum, item) => sum + Number(item?.changed || 0), 0),
    contexts: results
  };
};

const parseExtraSelectFields = async (context) =>
  context.evaluate(() => {
    const normalize = (text) => (text || "").replace(/\s+/g, " ").trim();
    const escapeSelector = (value) => {
      const raw = String(value || "");
      if (!raw) return "";
      if (window.CSS && typeof window.CSS.escape === "function") {
        return window.CSS.escape(raw);
      }
      return raw.replace(/([\\\"'\\[\\]#.:>+~()])/g, "\\$1");
    };
    const isVisible = (node) => {
      if (!node) return false;
      const style = window.getComputedStyle(node);
      if (style.display === "none" || style.visibility === "hidden") return false;
      const rect = node.getBoundingClientRect();
      return rect.width > 0 && rect.height > 0;
    };
    const collectRoots = (root, bucket) => {
      if (!root || bucket.includes(root)) return;
      bucket.push(root);
      const elements = root.querySelectorAll ? root.querySelectorAll("*") : [];
      elements.forEach((el) => {
        if (el && el.shadowRoot) {
          collectRoots(el.shadowRoot, bucket);
        }
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
          // ignore bad selector for this root
        }
      });
      return result;
    };
    const pickLabel = (select) => {
      const aria = select.getAttribute("aria-label");
      if (aria) return normalize(aria);

      const labelledBy = select.getAttribute("aria-labelledby");
      if (labelledBy) {
        const text = labelledBy
          .split(/\s+/)
          .map((id) => {
            const safeId = escapeSelector(id);
            if (!safeId) return "";
            const nodes = queryAllDeep(`#${safeId}`);
            return nodes.length ? normalize(nodes[0].textContent) : "";
          })
          .filter(Boolean)
          .join(" ");
        if (text) return text;
      }

      if (select.id) {
        const safeId = escapeSelector(select.id);
        if (safeId) {
          const labels = queryAllDeep(`label[for="${safeId}"]`);
          if (labels.length) return normalize(labels[0].textContent);
        }
      }
      const parentLabel = select.closest("label");
      if (parentLabel) return normalize(parentLabel.textContent);
      const wrapper = select.closest(
        ".formgroup-input, .form-group, .l-row, [data-testid*='attribute'], div, section, li"
      );
      if (wrapper) {
        const labelNode = wrapper.querySelector("label");
        if (labelNode) return normalize(labelNode.textContent);
      }
      return normalize(
        select.getAttribute("name")
        || select.getAttribute("id")
        || select.getAttribute("data-testid")
        || ""
      );
    };
    const ignoreLabels = [
      "Preis",
      "Preisart",
      "Preistyp",
      "PLZ",
      "Ort",
      "Versand",
      "Angebotstyp",
      "Gebot",
      "Gesuch",
      "Direkt kaufen"
    ];
    const fields = [];
    const seen = new Set();
    const selects = queryAllDeep("select");
    selects.forEach((select) => {
      const nameAttr = normalize(
        select.getAttribute("name")
        || select.getAttribute("id")
        || select.getAttribute("data-testid")
        || ""
      );
      const isAttributeSelect = /attributeMap/i.test(nameAttr);
      if (!isVisible(select) && !isAttributeSelect) return;
      const label = pickLabel(select);
      if (!label) return;
      const labelLower = label.toLowerCase();
      if (
        !isAttributeSelect &&
        (ignoreLabels.some((item) => labelLower.includes(item.toLowerCase())) || /preis|price/i.test(nameAttr))
      ) {
        return;
      }
      const options = Array.from(select.options || [])
        .map((opt) => ({
          value: opt.value,
          label: normalize(opt.textContent)
        }))
        .filter((opt) => opt.label.length > 0);
      if (options.length <= 1) return;
      const fieldName = nameAttr || label;
      const dedupeKey = `${fieldName}::${label}`;
      if (seen.has(dedupeKey)) return;
      seen.add(dedupeKey);
      fields.push({
        name: fieldName,
        label,
        required: select.required || select.getAttribute("aria-required") === "true",
        options
      });
    });
    return fields;
  });

const parseExtraSelectFieldsAcrossContexts = async (page) => {
  const collected = [];
  const seen = new Set();
  const contexts = [page, ...page.frames()];
  for (const context of contexts) {
    try {
      const fields = await parseExtraSelectFields(context);
      if (!Array.isArray(fields)) continue;
      fields.forEach((field) => {
        const key = field?.name || field?.label;
        if (!key || seen.has(key)) return;
        seen.add(key);
        collected.push(field);
      });
    } catch (error) {
      // ignore frame parsing errors
    }
  }
  return collected;
};

const setCategoryIdInForm = async (context, categoryId, categoryPathIds = []) => {
  if (isBlankValue(categoryId)) return false;
  let updated = false;
  const normalizedCategoryId = String(categoryId || "").trim();
  const numericPath = (Array.isArray(categoryPathIds) ? categoryPathIds : [])
    .map((item) => String(item || "").trim())
    .filter((item) => /^\d+$/.test(item));
  const selectors = [
    "#categoryIdField",
    'input[name="categoryId"]',
    'select[name="categoryId"]',
    'input[id*="categoryId"]',
    'input[id*="category"]',
    'select[id*="category"]',
    'input[data-testid*="category"]',
    'select[data-testid*="category"]'
  ];

  for (const selector of selectors) {
    if (selector.startsWith("select")) {
      const selected = await selectOption(context, [selector], categoryId);
      if (selected) updated = true;
    } else {
      const set = await setValueIfExists(context, selector, categoryId);
      if (set) updated = true;
    }
  }

  try {
    const hiddenFallbackApplied = await context.evaluate(({ categoryIdValue, numericPathIds }) => {
      const form = document.querySelector("#adForm") || document.querySelector("form");
      if (!form || !categoryIdValue) return false;
      const applyField = (name, value) => {
        if (!name) return false;
        let field = form.querySelector(`input[name="${name}"]`);
        if (!field) {
          field = document.createElement("input");
          field.type = "hidden";
          field.name = name;
          form.appendChild(field);
        }
        field.value = String(value ?? "");
        field.dispatchEvent(new Event("input", { bubbles: true }));
        field.dispatchEvent(new Event("change", { bubbles: true }));
        return true;
      };

      const categoryFieldNames = [
        "categoryId",
        "selectedCategoryId",
        "adCategoryId"
      ];
      let changed = false;
      for (const name of categoryFieldNames) {
        changed = applyField(name, categoryIdValue) || changed;
      }

      if (numericPathIds.length > 1) {
        changed = applyField("parentCategoryId", numericPathIds[0]) || changed;
      }
      if (numericPathIds.length) {
        const pathValue = numericPathIds.join("/");
        changed = applyField("categoryPath", pathValue) || changed;
        changed = applyField("path", pathValue) || changed;
      }

      const selectCandidates = Array.from(
        form.querySelectorAll('select[name*="category"], select[id*="category"]')
      );
      for (const select of selectCandidates) {
        const hasOption = Array.from(select.options || []).some((opt) => String(opt.value) === categoryIdValue);
        if (!hasOption) continue;
        select.value = categoryIdValue;
        select.dispatchEvent(new Event("input", { bubbles: true }));
        select.dispatchEvent(new Event("change", { bubbles: true }));
        changed = true;
      }

      return changed;
    }, { categoryIdValue: normalizedCategoryId, numericPathIds: numericPath });
    if (hiddenFallbackApplied) {
      updated = true;
    }
  } catch (error) {
    // ignore hidden fallback errors
  }

  return updated;
};

const directBuyContainerSelectors = [
  "[data-testid*='direct-buy']",
  "[data-testid*='direkt']",
  "[data-testid*='direct']",
  "[data-testid*='buy-now']",
  "[data-testid*='buynow']",
  "[class*='direct-buy']",
  "[class*='direkt']",
  "[class*='buy-now']",
  "[class*='buynow']",
  "fieldset[id*='buy-now']",
  "fieldset[id*='buynow']",
  "fieldset[id*='direkt']",
  "fieldset[id*='direct']"
];

const waitForDirectBuyText = async (context, timeout = 8000) => {
  try {
    await context.waitForFunction(
      () => {
        const bodyText = (document.body?.innerText || "").toLowerCase();
        return bodyText.includes("direkt kaufen") ||
          bodyText.includes("direktkaufen") ||
          bodyText.includes("direct buy") ||
          bodyText.includes("buy now") ||
          bodyText.includes("sicher bezahlen") ||
          bodyText.includes("sofort kaufen") ||
          bodyText.includes("sofortkauf");
      },
      { timeout }
    );
    return true;
  } catch (error) {
    return false;
  }
};

const trySelectDirectBuyNoIfText = async (context, timeout = 4000) => {
  try {
    const textReady = await waitForDirectBuyText(context, timeout);
    if (!textReady) return false;
    return await trySelectDirectBuyNoByText(context);
  } catch (error) {
    return false;
  }
};

const trySelectDirectBuyNoByText = async (context) =>
  context.evaluate(() => {
    const normalize = (value) => (value || "").replace(/\s+/g, " ").trim().toLowerCase();
    const includesAny = (value, needles) => needles.some((needle) => value.includes(needle));
    const directBuyTokens = [
      "direkt kaufen",
      "direct buy",
      "buy now",
      "direktkaufen",
      "sicher bezahlen",
      "sofort kaufen",
      "sofortkauf"
    ];
    const negativeTokens = ["nein", "nicht", "ohne", "kein", "nicht nutzen", "nicht verwenden", "nicht aktiv"];

    const isVisible = (node) => {
      if (!node) return false;
      const style = window.getComputedStyle(node);
      if (style.display === "none" || style.visibility === "hidden") return false;
      const rect = node.getBoundingClientRect();
      return rect.width > 0 && rect.height > 0;
    };

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

    const queryAllDeep = (selector) => {
      const results = [];
      for (const root of roots) {
        try {
          results.push(...Array.from(root.querySelectorAll(selector)));
        } catch (error) {
          // ignore selector errors
        }
      }
      return results;
    };

    const candidates = [];
    for (const root of roots) {
      const nodes = Array.from(root.querySelectorAll("section, fieldset, div, form"));
      for (const node of nodes) {
        if (!isVisible(node)) continue;
        const text = normalize(node.innerText || node.textContent || "");
        if (!text) continue;
        if (includesAny(text, directBuyTokens)) {
          candidates.push(node);
        }
      }
    }

    const applyInput = (input) => {
      if (!input) return false;
      try {
        input.click();
      } catch (error) {
        // ignore click errors
      }
      input.checked = true;
      input.dispatchEvent(new Event("input", { bubbles: true }));
      input.dispatchEvent(new Event("change", { bubbles: true }));
      return true;
    };

    const clickLabelForInput = (label) => {
      if (!label) return false;
      try {
        label.click();
        return true;
      } catch (error) {
        // ignore label click errors
      }
      const forId = label.getAttribute("for");
      if (forId) {
        const input = document.getElementById(forId);
        if (applyInput(input)) {
          return true;
        }
        if (input && isVisible(input)) {
          input.click();
          return true;
        }
      }
      const input = label.querySelector("input");
      if (applyInput(input)) {
        return true;
      }
      if (input && isVisible(input)) {
        input.click();
        return true;
      }
      return false;
    };

    const pickNegativeOption = (container) => {
      if (!container) return false;
      const labels = Array.from(container.querySelectorAll("label"));
      for (const label of labels) {
        const text = normalize(label.textContent || "");
        if (text && includesAny(text, negativeTokens)) {
          if (clickLabelForInput(label)) return true;
        }
      }

      const buttons = Array.from(container.querySelectorAll("button, [role='button']"));
      for (const button of buttons) {
        const text = normalize(button.textContent || button.getAttribute("aria-label") || "");
        if (text && includesAny(text, negativeTokens) && isVisible(button)) {
          button.click();
          return true;
        }
      }

      const radios = Array.from(container.querySelectorAll("input[type='radio']"));
      if (radios.length >= 2) {
        const negative = radios.find((radio) => {
          const label = container.querySelector(`label[for="${radio.id}"]`);
          const text = normalize(label?.textContent || "");
          return text && includesAny(text, negativeTokens);
        });
        if (negative && isVisible(negative)) {
          if (applyInput(negative)) return true;
          negative.click();
          return true;
        }
      }

      const selects = Array.from(container.querySelectorAll("select"));
      for (const select of selects) {
        if (!isVisible(select)) continue;
        const options = Array.from(select.options || []);
        const option = options.find((opt) => includesAny(normalize(opt.textContent || ""), negativeTokens));
        if (option) {
          select.value = option.value;
          select.dispatchEvent(new Event("input", { bubbles: true }));
          select.dispatchEvent(new Event("change", { bubbles: true }));
          return true;
        }
      }

      const switches = Array.from(container.querySelectorAll("[role='switch']"));
      for (const toggle of switches) {
        if (!isVisible(toggle)) continue;
        const labelText = normalize(toggle.getAttribute("aria-label") || toggle.textContent || "");
        if (!includesAny(labelText, directBuyTokens)) continue;
        const checked = toggle.getAttribute("aria-checked");
        if (checked === "true") {
          toggle.click();
          return true;
        }
      }

      return false;
    };

    for (const container of candidates) {
      if (pickNegativeOption(container)) return true;
    }

    const explicitLabels = queryAllDeep('label[for="radio-buy-now-no"]');
    for (const label of explicitLabels) {
      if (clickLabelForInput(label)) return true;
    }

    const explicitInputs = queryAllDeep('input[name="buy-now"][value="no"], input#radio-buy-now-no, input[name*="buy-now"][value="no"], input[name*="buynow"][value="no"]');
    for (const input of explicitInputs) {
      if (!input) continue;
      if (applyInput(input)) return true;
      input.click();
      return true;
    }

    return false;
  });

const getDirectBuyState = async (context) =>
  context.evaluate(() => {
    const normalize = (value) => (value || "").replace(/\s+/g, " ").trim().toLowerCase();
    const negativeTokens = ["nein", "nicht", "no", "false", "0"];
    const positiveTokens = ["ja", "yes", "true", "1"];
    const isVisible = (node) => {
      if (!node) return false;
      const style = window.getComputedStyle(node);
      if (style.display === "none" || style.visibility === "hidden") return false;
      const rect = node.getBoundingClientRect();
      return rect.width > 0 && rect.height > 0;
    };

    const roots = [];
    const collectRoots = (root, bucket) => {
      if (!root || bucket.includes(root)) return;
      bucket.push(root);
      const elements = root.querySelectorAll ? root.querySelectorAll("*") : [];
      elements.forEach((el) => {
        if (el.shadowRoot) collectRoots(el.shadowRoot, bucket);
      });
    };
    collectRoots(document, roots);

    let hasBlock = false;
    let hasSelection = false;
    let selectionValue = "";
    let selectionLabel = "";
    let selectionIsNo = false;
    let selectionIsYes = false;

    const getLabelForInput = (input, root) => {
      if (!input) return "";
      const id = input.getAttribute("id");
      if (id) {
        const label = root.querySelector(`label[for="${id}"]`);
        if (label) return label.textContent || "";
      }
      const parent = input.closest("label");
      if (parent) return parent.textContent || "";
      const wrapper = input.parentElement;
      if (wrapper) return wrapper.textContent || "";
      return "";
    };

    const setSelection = (value, label) => {
      if (!value && !label) return;
      if (!selectionValue) selectionValue = value || "";
      if (!selectionLabel) selectionLabel = label || "";
      const normalized = normalize(`${value || ""} ${label || ""}`);
      if (negativeTokens.some((token) => normalized.includes(token))) selectionIsNo = true;
      if (positiveTokens.some((token) => normalized.includes(token))) selectionIsYes = true;
      hasSelection = true;
    };

    const findBuyNowInputs = (root) => {
      try {
        return Array.from(
          root.querySelectorAll("input[name*='buy-now'], input[id*='buy-now'], input[name*='buynow'], input[id*='buynow']")
        );
      } catch (error) {
        return [];
      }
    };

    const findBuyNowFieldsets = (root) => {
      try {
        return Array.from(
          root.querySelectorAll("#buy-now-selector, fieldset[id*='buy-now'], fieldset[id*='buynow'], fieldset[id*='direkt'], fieldset[id*='direct']")
        );
      } catch (error) {
        return [];
      }
    };

    for (const root of roots) {
      const buyNowInputs = findBuyNowInputs(root);
      if (buyNowInputs.length) {
        hasBlock = true;
        const selectedInput = buyNowInputs.find((input) => input.checked);
        if (selectedInput) {
          setSelection(selectedInput.value || selectedInput.getAttribute("value") || selectedInput.id || "", getLabelForInput(selectedInput, root));
          break;
        }
      }

      const fieldsets = findBuyNowFieldsets(root);
      for (const fieldset of fieldsets) {
        if (!isVisible(fieldset)) continue;
        hasBlock = true;
        const scopedInputs = Array.from(fieldset.querySelectorAll("input[type='radio'], input[type='checkbox'], [role='radio']"));
        const selectedInput = scopedInputs.find((input) => input.checked || input.getAttribute("aria-checked") === "true");
        if (selectedInput) {
          setSelection(selectedInput.value || selectedInput.getAttribute("value") || selectedInput.id || "", getLabelForInput(selectedInput, root));
          break;
        }
      }
      if (hasSelection) break;
    }

    if (!hasSelection) {
      for (const root of roots) {
        const hidden = root.querySelector("input#buyNowSelected, input[name='buyNow']");
        if (!hidden) continue;
        const value = (hidden.value || "").trim();
        if (!value) continue;
        hasBlock = true;
        setSelection(value, "buyNowSelected");
        break;
      }
    }

    return {
      hasBlock,
      hasSelection,
      selectionValue,
      selectionLabel,
      selectionIsNo,
      selectionIsYes
    };
  });

const getDirectBuyRoot = async (context) => {
  const selectors = [
    ...directBuyContainerSelectors,
    "#buy-now-selector",
    "fieldset[id*='buy-now']",
    "fieldset[id*='buynow']",
    "fieldset[id*='direkt']",
    "fieldset[id*='direct']"
  ];
  for (const selector of selectors) {
    try {
      const handle = await context.$(selector);
      if (handle) return handle;
    } catch (error) {
      // ignore
    }
  }
  return null;
};

const trySelectDirectBuyNoFast = async (context) =>
  context.evaluate(() => {
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

    const queryAllDeep = (selector) => {
      const results = [];
      for (const root of roots) {
        try {
          results.push(...Array.from(root.querySelectorAll(selector)));
        } catch (error) {
          // ignore selector errors
        }
      }
      return results;
    };

    const setHiddenBuyNowNo = () => {
      const hidden = document.querySelector("input#buyNowSelected, input[name='buyNow']");
      if (!hidden) return false;
      hidden.value = "false";
      hidden.dispatchEvent(new Event("input", { bubbles: true }));
      hidden.dispatchEvent(new Event("change", { bubbles: true }));
      return true;
    };

    const applyInput = (input) => {
      if (!input) return false;
      try {
        input.click();
      } catch (error) {
        // ignore click errors
      }
      input.checked = true;
      input.dispatchEvent(new Event("input", { bubbles: true }));
      input.dispatchEvent(new Event("change", { bubbles: true }));
      setHiddenBuyNowNo();
      return true;
    };

    const clickLabelForInput = (label) => {
      if (!label) return false;
      try {
        label.click();
        setHiddenBuyNowNo();
        return true;
      } catch (error) {
        // ignore label click errors
      }
      const forId = label.getAttribute("for");
      if (forId) {
        const input = document.getElementById(forId);
        if (applyInput(input)) {
          return true;
        }
        if (input) {
          input.click();
          return true;
        }
      }
      const input = label.querySelector("input");
      if (applyInput(input)) {
        return true;
      }
      if (input) {
        input.click();
        return true;
      }
      return false;
    };

    const labels = queryAllDeep('label[for="radio-buy-now-no"]');
    for (const label of labels) {
      if (clickLabelForInput(label)) return true;
    }

    const inputs = queryAllDeep('input[name="buy-now"][value="no"], input#radio-buy-now-no, input[name*="buy-now"][value="no"], input[name*="buynow"][value="no"]');
    for (const input of inputs) {
      if (!input) continue;
      if (applyInput(input)) return true;
      input.click();
      return true;
    }

    return false;
  });

const collectDirectBuyContexts = (page, primaryContext) => {
  const contexts = [];
  const add = (ctx) => {
    if (!ctx || contexts.includes(ctx)) return;
    contexts.push(ctx);
  };
  add(primaryContext);
  add(page);
  try {
    page.frames().forEach(add);
  } catch (error) {
    // ignore frame errors
  }
  return contexts;
};

const getDirectBuyStateAcrossContexts = async (page, primaryContext) => {
  const contexts = collectDirectBuyContexts(page, primaryContext);
  let best = { hasBlock: false, hasSelection: false, context: null };
  for (const ctx of contexts) {
    try {
      const state = await getDirectBuyState(ctx);
      if (!state) continue;
      if (state.hasSelection) {
        return { ...state, context: ctx };
      }
      if (state.hasBlock && !best.hasBlock) {
        best = { ...state, context: ctx };
      }
    } catch (error) {
      // ignore context errors
    }
  }
  return best;
};

const summarizeDirectBuyState = (state) => {
  if (!state) return null;
  let contextUrl = null;
  if (state.context && typeof state.context.url === "function") {
    try {
      contextUrl = state.context.url();
    } catch (error) {
      contextUrl = null;
    }
  }
  return {
    hasBlock: Boolean(state.hasBlock),
    hasSelection: Boolean(state.hasSelection),
    selectionValue: state.selectionValue || "",
    selectionLabel: state.selectionLabel || "",
    selectionIsNo: Boolean(state.selectionIsNo),
    selectionIsYes: Boolean(state.selectionIsYes),
    contextUrl
  };
};

const waitForDirectBuyState = async (page, primaryContext, timeout = 5000) => {
  const start = Date.now();
  while (Date.now() - start < timeout) {
    const state = await getDirectBuyStateAcrossContexts(page, primaryContext);
    if (state?.hasBlock) return state;
    await humanPause(250, 420);
  }
  return { hasBlock: false, hasSelection: false, context: null };
};

const forceDirectBuyNoInContext = async (context) => {
  try {
    return await context.evaluate(() => {
      const inputNo = document.querySelector("input#radio-buy-now-no") ||
        document.querySelector("input[name='buy-now'][value='no']") ||
        document.querySelector("input[name*='buy-now'][value='no']") ||
        document.querySelector("input[name*='buynow'][value='no']");
      const label = inputNo
        ? (inputNo.id ? document.querySelector(`label[for="${inputNo.id}"]`) : null) || inputNo.closest("label")
        : document.querySelector("label[for='radio-buy-now-no']");

      if (label) {
        label.click();
      }
      if (inputNo) {
        inputNo.checked = true;
        inputNo.setAttribute("checked", "checked");
        inputNo.setAttribute("aria-checked", "true");
        inputNo.dispatchEvent(new Event("input", { bubbles: true }));
        inputNo.dispatchEvent(new Event("change", { bubbles: true }));
      }

      const hidden = document.querySelector("input#buyNowSelected, input[name='buyNow']");
      if (hidden) {
        hidden.value = "false";
        hidden.dispatchEvent(new Event("input", { bubbles: true }));
        hidden.dispatchEvent(new Event("change", { bubbles: true }));
      }

      const error = document.querySelector("#buyNow\\.errors");
      if (error) {
        error.style.display = "none";
      }

      return {
        checked: Boolean(inputNo?.checked),
        hiddenValue: hidden ? hidden.value : "",
        hasInput: Boolean(inputNo)
      };
    });
  } catch (error) {
    return { checked: false, hiddenValue: "", hasInput: false, error: error?.message || String(error) };
  }
};

const forceDirectBuyNoAcrossContexts = async (page, primaryContext) => {
  const contexts = collectDirectBuyContexts(page, primaryContext);
  let last = null;
  for (const ctx of contexts) {
    try {
      last = await forceDirectBuyNoInContext(ctx);
    } catch (error) {
      // ignore
    }
  }
  return last;
};

const ensureBuyNowSelectedFalse = async (context) => {
  try {
    return await context.evaluate(() => {
      const hidden = document.querySelector("input#buyNowSelected, input[name='buyNow']");
      if (!hidden) return false;
      hidden.value = "false";
      hidden.dispatchEvent(new Event("input", { bubbles: true }));
      hidden.dispatchEvent(new Event("change", { bubbles: true }));
      return true;
    });
  } catch (error) {
    return false;
  }
};

const selectDirectBuyNo = async (context, { retries = 3 } = {}) => {
  const clickIfPossible = async (handle) => {
    if (!handle) return false;
    try {
      await scrollIntoView(handle);
      await humanPause(100, 200);
      await handle.click({ delay: 30 });
      return true;
    } catch (error) {
      try {
        await handle.evaluate((node) => node.click());
        return true;
      } catch (innerError) {
        return false;
      }
    }
  };

  const setHiddenBuyNowNo = async (ctx) => {
    try {
      return await ctx.evaluate(() => {
        const hidden = document.querySelector("input#buyNowSelected, input[name='buyNow']");
        if (!hidden) return false;
        hidden.value = "false";
        hidden.dispatchEvent(new Event("input", { bubbles: true }));
        hidden.dispatchEvent(new Event("change", { bubbles: true }));
        return true;
      });
    } catch (error) {
      return false;
    }
  };

  const closeDirectBuyDialog = async (ctx) => {
    try {
      await ctx.evaluate(() => {
        const normalize = (value) => (value || "").replace(/\s+/g, " ").trim().toLowerCase();
        const isVisible = (node) => {
          if (!node) return false;
          const style = window.getComputedStyle(node);
          if (style.display === "none" || style.visibility === "hidden") return false;
          const rect = node.getBoundingClientRect();
          return rect.width > 0 && rect.height > 0;
        };
        const dialogs = Array.from(document.querySelectorAll("dialog[open], [role='dialog']"));
        const target = dialogs.find((dialog) => normalize(dialog.textContent || "").includes("direkt kaufen"));
        if (!target || !isVisible(target)) return false;
        const buttons = Array.from(target.querySelectorAll("button, [role='button']"));
        const closeButton = buttons.find((btn) => {
          const text = normalize(btn.textContent || btn.getAttribute("aria-label") || "");
          return text.includes("schließen") || text.includes("close");
        }) || target.querySelector("[data-testid='close-button']");
        if (!closeButton) return false;
        closeButton.click();
        return true;
      });
    } catch (error) {
      // ignore dialog close errors
    }
  };

  const deepSelectNo = async (ctx) => {
    try {
      return await ctx.evaluate(() => {
        const normalize = (value) => (value || "").replace(/\s+/g, " ").trim().toLowerCase();
        const isVisible = (node) => {
          if (!node) return false;
          const style = window.getComputedStyle(node);
          if (style.display === "none" || style.visibility === "hidden") return false;
          const rect = node.getBoundingClientRect();
          return rect.width > 0 && rect.height > 0;
        };
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

        for (const root of roots) {
          const noInputs = Array.from(
            root.querySelectorAll("input[name*='buy-now'][value='no'], input[id*='buy-now'][value='no'], input[name*='buynow'][value='no']")
          );
          const noInput = noInputs.find((input) => input && (isVisible(input) || input.type === "radio"));
          if (noInput) {
            const label = root.querySelector(`label[for="${noInput.id}"]`) || noInput.closest("label");
            if (label) {
              label.click();
              return true;
            }
            noInput.click();
            return true;
          }

          const fieldsets = Array.from(
            root.querySelectorAll("#buy-now-selector, fieldset[id*='buy-now'], fieldset[id*='buynow'], fieldset[id*='direkt'], fieldset[id*='direct']")
          );
          for (const fieldset of fieldsets) {
            if (!isVisible(fieldset)) continue;
            const scopedNoInput = fieldset.querySelector("input[name*='buy-now'][value='no'], input[id*='buy-now'][value='no'], input[name*='buynow'][value='no']");
            if (scopedNoInput) {
              const label = fieldset.querySelector(`label[for="${scopedNoInput.id}"]`) || scopedNoInput.closest("label");
              if (label) {
                label.click();
                return true;
              }
              scopedNoInput.click();
              return true;
            }
            const labelCandidates = Array.from(fieldset.querySelectorAll("label"));
            const targetLabel = labelCandidates.find((label) => normalize(label.textContent).includes("nein"));
            if (targetLabel && isVisible(targetLabel)) {
              targetLabel.click();
              return true;
            }
          }
        }
        return false;
      });
    } catch (error) {
      return false;
    }
  };

  const selectorCandidates = [
    ...directBuyContainerSelectors.map((selector) => `${selector} input[type='radio']`),
    ...directBuyContainerSelectors.map((selector) => `${selector} input[type='checkbox']`),
    ...directBuyContainerSelectors.map((selector) => `${selector} [role='radio']`),
    "label[for='radio-buy-now-no']",
    "input#radio-buy-now-no",
    "input[name='buy-now'][value='no']",
    "input[name*='buy-now'][value='no']",
    "input[type='radio'][aria-label*='Direkt kaufen']",
    "input[type='checkbox'][aria-label*='Direkt kaufen']",
    "[role='radio'][aria-label*='Direkt kaufen']",
    "input[type='radio'][name*='direct']",
    "input[type='radio'][name*='direkt']",
    "input[type='checkbox'][name*='direct']",
    "input[type='checkbox'][name*='direkt']"
  ];

  for (let attempt = 1; attempt <= retries; attempt += 1) {
    console.log(`[publishAd] Direkt kaufen opt-out attempt ${attempt}/${retries}`);
    try {
      await closeDirectBuyDialog(context);
      await setHiddenBuyNowNo(context);
      const forcedState = await forceDirectBuyNoInContext(context);
      if (forcedState?.checked || forcedState?.hiddenValue === "false") {
        console.log("[publishAd] Direkt kaufen opt-out successful via forceDirectBuyNoInContext");
        return true;
      }
      try {
        const forced = await context.evaluate(() => {
          const input = document.querySelector("input#radio-buy-now-no") ||
            document.querySelector("input[name='buy-now'][value='no']") ||
            document.querySelector("input[name*='buy-now'][value='no']") ||
            document.querySelector("input[name*='buynow'][value='no']");
          if (!input) return false;
          const label = input.id ? document.querySelector(`label[for="${input.id}"]`) : null;
          if (label) {
            label.click();
          } else {
            input.click();
          }
          input.checked = true;
          input.dispatchEvent(new Event("input", { bubbles: true }));
          input.dispatchEvent(new Event("change", { bubbles: true }));
          return input.checked || input.getAttribute("aria-checked") === "true";
        });
        if (forced) {
          await setHiddenBuyNowNo(context);
          console.log("[publishAd] Direkt kaufen opt-out successful via forced evaluate");
          return true;
        }
      } catch (error) {
        // ignore forced selection errors
      }
      const scopedRoot = await getDirectBuyRoot(context);
      if (scopedRoot) {
        console.log("[publishAd] Direkt kaufen search scoped to form container");
      }
      const searchContext = scopedRoot || context;

      const explicitHandle = await searchContext.$("label[for='radio-buy-now-no'], input#radio-buy-now-no, input[name='buy-now'][value='no']");
      if (explicitHandle) {
        const clicked = await clickIfPossible(explicitHandle);
        if (clicked) {
          await setHiddenBuyNowNo(context);
          console.log("[publishAd] Direkt kaufen opt-out successful via explicit selector");
          return true;
        }
      }

      const textReady = await waitForDirectBuyText(searchContext, 6000);
      console.log(`[publishAd] Direkt kaufen text detected: ${textReady}`);
      if (textReady) {
        const textSelected = await trySelectDirectBuyNoByText(searchContext);
        if (textSelected) {
          console.log("[publishAd] Direkt kaufen opt-out successful via text scan");
          return true;
        }
      }

      const selectorStart = Date.now();
      try {
        await searchContext.waitForSelector(selectorCandidates.join(", "), { timeout: 5000 });
        console.log(`[publishAd] Direkt kaufen selector wait ${Date.now() - selectorStart}ms`);
      } catch (waitError) {
        console.log(`[publishAd] Direkt kaufen block not detected yet (${Date.now() - selectorStart}ms)`);
      }

      // Попытка 1: Найти через селекторы
      const selectorMatches = await searchContext.$$(selectorCandidates.join(", "));
      for (const handle of selectorMatches) {
        const isVisible = await handle.evaluate((node) => {
          const style = window.getComputedStyle(node);
          if (style.display === "none" || style.visibility === "hidden") return false;
          const rect = node.getBoundingClientRect();
          return rect.width > 0 && rect.height > 0;
        });
        if (!isVisible) continue;

        const clicked = await clickIfPossible(handle);
        if (clicked) {
          await setHiddenBuyNowNo(context);
          console.log("[publishAd] Direkt kaufen opt-out successful via selector");
          return true;
        }
      }

      // Попытка 2: Найти через текст "Nein"
      try {
        const clicked = await searchContext.evaluate(() => {
          const findClickableWithText = (text) => {
            const normalize = (val) => (val || "").toLowerCase().trim();
            const isVisible = (node) => {
              if (!node) return false;
              const style = window.getComputedStyle(node);
              if (style.display === "none" || style.visibility === "hidden") return false;
              const rect = node.getBoundingClientRect();
              return rect.width > 0 && rect.height > 0;
            };

            // Поиск радиокнопок с текстом "Nein" рядом
            const radioButtons = Array.from(document.querySelectorAll("input[type='radio'], [role='radio']"));
            for (const radio of radioButtons) {
              if (!isVisible(radio)) continue;

              // Проверяем aria-label
              const ariaLabel = normalize(radio.getAttribute("aria-label") || "");
              if (ariaLabel.includes("nein") || ariaLabel.includes("nicht")) {
                radio.click();
                return true;
              }

              // Проверяем родительский label
              const parent = radio.closest("label");
              if (parent && normalize(parent.textContent).includes(text)) {
                radio.click();
                return true;
              }

              // Проверяем следующий элемент (label)
              let sibling = radio.nextElementSibling;
              while (sibling) {
                if (sibling.tagName === "LABEL" && normalize(sibling.textContent).includes(text)) {
                  radio.click();
                  return true;
                }
                sibling = sibling.nextElementSibling;
              }
            }
            return false;
          };

          return findClickableWithText("nein") || findClickableWithText("nicht nutzen");
        });

        if (clicked) {
          await setHiddenBuyNowNo(context);
          console.log("[publishAd] Direkt kaufen opt-out successful via text search");
          return true;
        }
      } catch (error) {
        console.log(`[publishAd] Text-based search failed: ${error.message}`);
      }

      const deepClicked = await deepSelectNo(context);
      if (deepClicked) {
        await setHiddenBuyNowNo(context);
        console.log("[publishAd] Direkt kaufen opt-out successful via deep search");
        return true;
      }

    } catch (error) {
      console.log(`[publishAd] Direkt kaufen opt-out attempt failed: ${error.message}`);
    }
    await humanPause(300, 600);
  }

  console.log("[publishAd] Direkt kaufen opt-out not applied");
  return false;
};

const findUploadInput = async (context) => {
  let inputs = [];
  try {
    inputs = await context.$$('input[type="file"]');
  } catch (error) {
    inputs = [];
  }
  if (!inputs.length) return null;

  const scored = [];
  for (const handle of inputs) {
    try {
      const info = await handle.evaluate((node) => {
        const rect = node.getBoundingClientRect();
        const id = node.id || "";
        const name = node.name || "";
        const accept = node.getAttribute("accept") || "";
        const multiple = node.hasAttribute("multiple");
        const hidden = node.type === "hidden" || node.getAttribute("aria-hidden") === "true";
        const inUploadSection = Boolean(
          node.closest("[id*='upload'], [class*='upload'], [class*='image'], [class*='photo'], [class*='picture'], [data-testid*='image']")
        );
        const scoreBase = (rect.width > 0 && rect.height > 0) ? 2 : 0;
        return {
          id,
          name,
          accept,
          multiple,
          hidden,
          inUploadSection,
          rect: { width: rect.width, height: rect.height },
          scoreBase
        };
      });
      let score = info.scoreBase;
      if (/^html5_/i.test(info.id)) score += 3;
      if (info.inUploadSection) score += 3;
      if (info.multiple) score += 2;
      if (/image|jpg|jpeg|png|gif|webp/i.test(info.accept)) score += 2;
      if (/upload|image|photo|picture/i.test(info.id + info.name)) score += 1;
      scored.push({ handle, info, score });
    } catch (error) {
      // ignore individual input errors
    }
  }
  if (!scored.length) return null;
  scored.sort((a, b) => b.score - a.score);
  return scored[0];
};

const waitForUploadedImages = async (page, timeout = 15000) => {
  try {
    await page.waitForFunction(() => {
      const previewSelectors = [
        "img[src^='blob:']",
        "img[src*='image/']",
        "[data-testid*='image'] img",
        "#j-pictureupload-thumbnails li:not(.is-placeholder)",
        ".pictureupload-thumbnails li:not(.is-placeholder)",
        "#j-pictureupload-thumbnails img",
        ".pictureupload-thumbnails img",
        ".image-preview img",
        ".image-list img",
        ".adimage img"
      ];
      return previewSelectors.some((selector) => {
        const nodes = document.querySelectorAll(selector);
        return nodes && nodes.length > 0;
      });
    }, { timeout });
    return true;
  } catch (error) {
    return false;
  }
};

const waitForImageUploadCompletion = async (page, timeout = 90000) => {
  try {
    await page.waitForFunction(() => {
      const text = (document.body?.innerText || "").toLowerCase();
      if (text.includes("wird hochgeladen") || text.includes("uploading")) return false;

      const statusNodes = Array.from(
        document.querySelectorAll(".pictureupload-file-status, #status-message-image")
      );
      if (statusNodes.some((node) => {
        const value = (node.textContent || "").toLowerCase();
        return value.includes("wird hochgeladen") || value.includes("uploading");
      })) {
        return false;
      }

      const fileList = Array.from(document.querySelectorAll(".pictureupload-filelist li"));
      if (fileList.length) {
        const hasSpinner = fileList.some((item) => item.querySelector(".spinner, .pictureupload-file-prgrss"));
        if (hasSpinner) return false;
        const hasUploadingText = fileList.some((item) => {
          const value = (item.textContent || "").toLowerCase();
          return value.includes("wird hochgeladen") || value.includes("uploading");
        });
        if (hasUploadingText) return false;
      }

      return true;
    }, { timeout });
    return true;
  } catch (error) {
    return false;
  }
};

const openUploadPicker = async (context) => {
  const selectors = [
    "#pictureupload-pickfiles-icon",
    "#pictureupload-pickfiles",
    "button[aria-label*='Bilder']",
    "button[aria-label*='Fotos']",
    "button[title*='Bilder']",
    "button[title*='Fotos']",
    "[id*='pictureupload'] button",
    "[class*='upload'] button"
  ];
  for (const selector of selectors) {
    try {
      const handle = await context.$(selector);
      if (!handle) continue;
      await safeClick(handle);
      await humanPause(100, 200);
      return true;
    } catch (error) {
      // ignore
    }
  }
  return false;
};

const uploadImages = async (page, imagePaths, { accountLabel = "account" } = {}) => {
  if (!imagePaths || imagePaths.length === 0) {
    return { success: false, reason: "no-images" };
  }

  try {
    await page.waitForSelector("input[type='file']", { timeout: 10000 });
  } catch (error) {
    // ignore wait errors
  }

  const contexts = [page, ...page.frames()];
  let selected = null;
  let selectedContextIndex = -1;
  for (let index = 0; index < contexts.length; index += 1) {
    const context = contexts[index];
    const candidate = await findUploadInput(context);
    if (!candidate) continue;
    selected = candidate;
    selectedContextIndex = index;
    break;
  }
  if (!selected) {
    for (const context of contexts) {
      try {
        await openUploadPicker(context);
      } catch (error) {
        // ignore
      }
    }
    await humanPause(200, 400);
    for (let index = 0; index < contexts.length; index += 1) {
      const context = contexts[index];
      const candidate = await findUploadInput(context);
      if (!candidate) continue;
      selected = candidate;
      selectedContextIndex = index;
      break;
    }
  }
  if (!selected) {
    return { success: false, reason: "file-input-not-found" };
  }

  try {
    await selected.handle.uploadFile(...imagePaths);
    try {
      await selected.handle.evaluate((node) => {
        node.dispatchEvent(new Event("input", { bubbles: true }));
        node.dispatchEvent(new Event("change", { bubbles: true }));
      });
    } catch (error) {
      // ignore dispatch errors
    }
  } catch (error) {
    return { success: false, reason: error?.message || "upload-failed", inputInfo: selected.info };
  }

  let fileCount = null;
  try {
    fileCount = await selected.handle.evaluate((node) => (node.files ? node.files.length : 0));
  } catch (error) {
    fileCount = null;
  }
  const uploaded = await waitForUploadedImages(page, 20000);
  return {
    success: true,
    uploaded,
    inputInfo: selected.info,
    contextIndex: selectedContextIndex,
    count: imagePaths.length,
    fileCount
  };
};

const publishAd = async ({ account, proxy, ad, imagePaths, debug }) => {
  const prevDebugOverride = publishDebugOverride;
  publishDebugOverride = Boolean(debug);
  const accountLabel = account?.profileEmail
    ? `${account.profileName || account.username || "Аккаунт"} (${account.profileEmail})`
    : (account?.profileName || account?.username || "Аккаунт");
  const deviceProfile = toDeviceProfile(account.deviceProfile);
  const cookies = normalizeCookies(parseCookies(account.cookie));

  if (!cookies.length) {
    return { success: false, error: "Cookie файл пустой" };
  }
  if (!proxy) {
    return { success: false, error: "Прокси обязателен для публикации объявления." };
  }

  const attemptPublish = async ({ useProxy }) => {
    const proxyServer = useProxy ? buildProxyServer(proxy) : null;
    const proxyUrl = useProxy ? buildPuppeteerProxyUrl(proxy) : null;
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
    const defaultTimeout = 600000;
    const protocolTimeout = 600000;
    const browser = await puppeteer.launch({
      headless: "new",
      args: launchArgs,
      userDataDir: profileDir,
      protocolTimeout
    });

    let page;
    try {
      page = await browser.newPage();
      page.setDefaultTimeout(defaultTimeout);
      page.setDefaultNavigationTimeout(defaultTimeout);
      if (useProxy && !anonymizedProxyUrl && (proxy?.username || proxy?.password)) {
        await page.authenticate({
          username: proxy.username || "",
          password: proxy.password || ""
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

      const gotoHomeStart = Date.now();
      await page.goto("https://www.kleinanzeigen.de/", { waitUntil: "domcontentloaded" });
      await acceptGdprConsent(page);
      console.log(`[publishAd] goto homepage in ${Date.now() - gotoHomeStart}ms`);
      await humanPause(200, 480);
      await page.setCookie(...cookies);
      await humanPause(120, 260);
      try {
        await page.goto("https://www.kleinanzeigen.de/gdpr", { waitUntil: "domcontentloaded" });
        await acceptGdprConsent(page);
      } catch (error) {
        // ignore consent preload errors
      }

      const gotoCreateStart = Date.now();
      await page.goto(CREATE_AD_URL, { waitUntil: "domcontentloaded" });
      await acceptGdprConsent(page);
      console.log(`[publishAd] goto create page in ${Date.now() - gotoCreateStart}ms`);
      await humanPause(200, 500);

      const currentUrl = page.url();
      if (currentUrl.includes("m-einloggen")) {
        await dumpPublishDebug(page, {
          accountLabel,
          step: "login-redirect",
          error: "redirected-to-login",
          extra: { url: currentUrl }
        });
        return {
          success: false,
          error: "Kleinanzeigen перенаправил на страницу логина. Проверьте актуальность cookies.",
          url: currentUrl
        };
      }
      if (isGdprPage(currentUrl)) {
        await acceptGdprConsent(page, { timeout: 20000 });
        const redirectTarget = getGdprRedirectTarget(page.url()) || CREATE_AD_URL;
        await page.goto(redirectTarget, { waitUntil: "domcontentloaded" });
        await humanPause(200, 500);
      }

      let formContext = await getAdFormContext(page);
      if (!formContext) {
        await dumpPublishDebug(page, {
          accountLabel,
          step: "form-not-found",
          error: "form-context-missing",
          extra: { url: page.url() }
        });
        return {
          success: false,
          error: "Не удалось найти форму создания объявления на странице.",
          url: page.url()
        };
      }

      const categoryPathFromAd = normalizeCategoryPathIds(ad?.categoryPath);
      const categoryPathNumeric = categoryPathFromAd.filter((item) => /^\d+$/.test(String(item)));
      const resolvedCategoryId = normalizeCategoryId(ad.categoryId) ||
        (categoryPathNumeric.length ? String(categoryPathNumeric[categoryPathNumeric.length - 1]) : "") ||
        extractCategoryIdFromUrl(ad.categoryUrl);
      const categoryPathIds = categoryPathFromAd.length
        ? categoryPathFromAd
        : (resolveCategoryPathFromCache(resolvedCategoryId) || []);
      const categoryPathIdsNumeric = categoryPathNumeric.length
        ? categoryPathNumeric
        : categoryPathIds.filter((item) => /^\d+$/.test(String(item)));
      const preferredFieldValues = collectPreferredFieldValues({ ad, categoryPathIds });
      let selectionUrl = getCategorySelectionUrl(resolvedCategoryId, ad.categoryUrl);
      if (!selectionUrl && categoryPathIdsNumeric.length) {
        selectionUrl = `https://www.kleinanzeigen.de/p-kategorie-aendern.html?path=${encodeURIComponent(categoryPathIdsNumeric.join("/"))}`;
      }
      let categorySet = false;
      let categoryUrlApplied = false;
      let categorySelectionDone = false;

      if (DEBUG_PUBLISH) {
        await dumpPublishDebug(page, {
          accountLabel,
          step: "publish-input",
          error: "",
          extra: {
            categoryId: ad?.categoryId || "",
            categoryUrl: ad?.categoryUrl || "",
            categoryPath: Array.isArray(ad?.categoryPath) ? ad.categoryPath : (ad?.categoryPath ? String(ad.categoryPath).slice(0, 120) : ""),
            resolvedCategoryId,
            categoryPathIds,
            categoryPathIdsNumeric,
            selectionUrl
          }
        });
      }
      appendPublishTrace({ step: "after-publish-input", resolvedCategoryId, categoryPathIdsLength: categoryPathIds.length });
      appendPublishTrace({ step: "publish-flow-version", version: PUBLISH_FLOW_VERSION });
      if (resolvedCategoryId) {
        categorySet = await setCategoryIdInForm(formContext, resolvedCategoryId, categoryPathIdsNumeric);
        if (!categorySet && formContext !== page) {
          categorySet = await setCategoryIdInForm(page, resolvedCategoryId, categoryPathIdsNumeric);
        }
        if (categorySet) {
          await humanPause(120, 220);
        }
      } else if (ad.categoryUrl && /p-kategorie-aendern|anzeige-aufgeben/i.test(ad.categoryUrl)) {
        await openCategorySelection(page, { selectionUrl });
        await humanPause(200, 400);
        await page.goto(ad.categoryUrl, { waitUntil: "domcontentloaded" });
        await humanPause(200, 400);
        await clickCategoryWeiter(page);
        await humanPause(200, 400);
        categoryUrlApplied = true;
        const refreshedContext = await getAdFormContext(page);
        if (refreshedContext) {
          formContext = refreshedContext;
        }
      }

      const applyCategoryFallbackOnCurrentForm = async () => {
        if (!resolvedCategoryId) return false;
        let applied = false;
        try {
          applied = await setCategoryIdInForm(formContext, resolvedCategoryId, categoryPathIdsNumeric);
        } catch (error) {
          applied = false;
        }
        if (!applied && formContext !== page) {
          try {
            applied = await setCategoryIdInForm(page, resolvedCategoryId, categoryPathIdsNumeric);
          } catch (error) {
            applied = false;
          }
        }
        if (applied) {
          categorySet = true;
          await humanPause(120, 220);
          const refreshedContext = await getAdFormContext(page);
          if (refreshedContext) {
            formContext = refreshedContext;
          }
        }
        return applied;
      };

      if (CATEGORY_SELECTION_NEW_PAGE && categoryPathIds.length) {
        appendPublishTrace({ step: "category-selection-new-page-start" });
        categorySelectionDone = await withTimeout(
          selectCategoryViaNewPage(browser, {
            cookies,
            deviceProfile,
            categoryPathIds,
            accountLabel,
            selectionUrl
          }),
          60000,
          "category-selection-new-page-timeout"
        ).catch(() => false);
        appendPublishTrace({ step: "category-selection-new-page-done", success: categorySelectionDone });
        if (categorySelectionDone) {
          await page.goto(CREATE_AD_URL, { waitUntil: "domcontentloaded" });
          await acceptCookieModal(page, { timeout: 15000 }).catch(() => {});
          if (isGdprPage(page.url())) {
            await acceptGdprConsent(page, { timeout: 20000 }).catch(() => {});
            const redirectTarget = getGdprRedirectTarget(page.url()) || CREATE_AD_URL;
            await page.goto(redirectTarget, { waitUntil: "domcontentloaded" });
          }
          const refreshedContext = await getAdFormContext(page);
          if (refreshedContext) {
            formContext = refreshedContext;
          }
        }
      }

      const titleSelectors = [
        'input[name="title"]',
        'input[name="adTitle"]',
        'input[id*="title"]',
        'input[placeholder*="Titel"]',
        'input[placeholder*="Title"]',
        'input[aria-label*="Titel"]',
        'input[aria-label*="Title"]',
        'input[data-testid*="title"]'
      ];
      const descriptionSelectors = [
        'textarea[name="description"]',
        'textarea[name="adDescription"]',
        'textarea[id*="description"]',
        'textarea[placeholder*="Beschreibung"]',
        'textarea[placeholder*="Description"]',
        'textarea[aria-label*="Beschreibung"]',
        'textarea[aria-label*="Description"]',
        'textarea[data-testid*="description"]'
      ];
      const descriptionEditableSelectors = [
        '[contenteditable="true"][aria-label*="Beschreibung"]',
        '[role="textbox"][aria-label*="Beschreibung"]',
        '[contenteditable="true"][data-testid*="description"]',
        '[role="textbox"][data-testid*="description"]',
        '[contenteditable="true"][id*="description"]',
        '[role="textbox"][id*="description"]'
      ];

      appendPublishTrace({ step: "fill-title-start" });
      const titleFilled = await fillField(formContext, titleSelectors, ad.title);
      appendPublishTrace({ step: "fill-title-done", success: titleFilled });

      appendPublishTrace({ step: "fill-description-start" });
      let descriptionFilled = await fillField(formContext, descriptionSelectors, ad.description);
      if (!descriptionFilled && formContext !== page) {
        descriptionFilled = await fillField(page, descriptionSelectors, ad.description);
      }
      if (!descriptionFilled) {
        descriptionFilled = await fillFieldAcrossContexts(page, descriptionSelectors, ad.description);
      }
      if (!descriptionFilled) {
        descriptionFilled = await fillContentEditableAcrossContexts(page, descriptionEditableSelectors, ad.description);
      }
      if (!descriptionFilled) {
        descriptionFilled = await fillFieldDeepAcrossContexts(page, {
          labelKeywords: ["beschreibung", "description"],
          value: ad.description
        });
      }
      appendPublishTrace({ step: "fill-description-done", success: descriptionFilled });

      let titleFallback = titleFilled ? true : await fillByLabel(formContext, ["Titel", "Title"], ad.title);
      let descriptionFallback = descriptionFilled
        ? true
        : await fillByLabel(formContext, ["Beschreibung", "Description"], ad.description);
      if (!descriptionFallback && formContext !== page) {
        descriptionFallback = await fillByLabel(page, ["Beschreibung", "Description"], ad.description);
      }

      await humanPause(120, 240);
      appendPublishTrace({ step: "fill-price-start" });
      const priceFilled = await fillPriceField(formContext, ad.price);
      appendPublishTrace({ step: "fill-price-done", success: priceFilled });
      let priceFallback = priceFilled ? true : await fillByLabel(formContext, ["Preis", "Price"], ad.price);
      if (!priceFallback && ad.price) {
        const microFrontendFilled = await setValueIfExists(formContext, "#micro-frontend-price", ad.price);
        if (microFrontendFilled) {
          await humanPause(80, 160);
          priceFallback = true;
        }
      }
      if (priceFallback && ad.price) {
        const priceValue = await getPriceValue(formContext);
        if (!priceValue) {
          const microFrontendFilled = await setValueIfExists(formContext, "#micro-frontend-price", ad.price);
          if (microFrontendFilled) {
            await humanPause(80, 160);
          }
        }
      }

      if (DEBUG_PUBLISH) {
        let descriptionValue = "";
        try {
          descriptionValue = await formContext.evaluate((selectors, editableSelectors) => {
            const normalize = (val) => (val ?? "").toString();
            const pickValue = (list) => {
              for (const selector of list) {
                const node = document.querySelector(selector);
                if (!node) continue;
                if ("value" in node) {
                  const val = normalize(node.value);
                  if (val) return val;
                }
                const text = normalize(node.textContent || node.innerText || "");
                if (text) return text;
              }
              return "";
            };
            return pickValue(selectors) || pickValue(editableSelectors);
          }, descriptionSelectors, descriptionEditableSelectors);
        } catch (error) {
          if (formContext !== page) {
            try {
              descriptionValue = await page.evaluate((selectors, editableSelectors) => {
                const normalize = (val) => (val ?? "").toString();
                const pickValue = (list) => {
                  for (const selector of list) {
                    const node = document.querySelector(selector);
                    if (!node) continue;
                    if ("value" in node) {
                      const val = normalize(node.value);
                      if (val) return val;
                    }
                    const text = normalize(node.textContent || node.innerText || "");
                    if (text) return text;
                  }
                  return "";
                };
                return pickValue(selectors) || pickValue(editableSelectors);
              }, descriptionSelectors, descriptionEditableSelectors);
            } catch (innerError) {
              descriptionValue = "";
            }
          }
        }
        let priceValue = "";
        try {
          priceValue = await getPriceValue(formContext);
        } catch (error) {
          if (formContext !== page) {
            try {
              priceValue = await getPriceValue(page);
            } catch (innerError) {
              priceValue = "";
            }
          }
        }
        const trimmedDescription = (descriptionValue || "").trim();
        const trimmedPrice = (priceValue || "").trim();
        await dumpPublishDebug(page, {
          accountLabel,
          step: "form-values",
          error: "",
          extra: {
            titleLength: ad?.title ? String(ad.title).length : 0,
            descriptionLength: descriptionValue ? descriptionValue.length : 0,
            descriptionTail: descriptionValue ? descriptionValue.slice(-60) : "",
            priceValue,
            descriptionEndsWithPrice: Boolean(trimmedPrice && trimmedDescription.endsWith(trimmedPrice))
          }
        });
      }

      await humanPause(120, 240);
      const postalFilled = await fillField(formContext, [
        'input[name="zipcode"]',
        'input[name="plz"]',
        'input[name*="zip"]',
        'input[id*="zip"]',
        'input[id*="plz"]',
        'input[placeholder*="PLZ"]',
        'input[placeholder*="Postleitzahl"]',
        'input[aria-label*="PLZ"]',
        'input[aria-label*="Postleitzahl"]',
        'input[data-testid*="zip"]'
      ], ad.postalCode);
      const postalFallback = postalFilled ? true : await fillByLabel(formContext, ["PLZ", "Postleitzahl"], ad.postalCode);

      await selectOption(formContext, [
        'select[name="categoryId"]',
        'select[id*="category"]'
      ], resolvedCategoryId);

      const shouldRunCategorySelection = Boolean(
        (selectionUrl || categoryPathIds.length) &&
        !categoryUrlApplied &&
        !categorySelectionDone &&
        !categorySet
      );
      if (!shouldRunCategorySelection && (selectionUrl || categoryPathIds.length)) {
        appendPublishTrace({
          step: "category-selection-skipped",
          reason: categorySet ? "category-already-set-in-form" : "conditions-not-met"
        });
      }
      if (shouldRunCategorySelection) {
        try {
          console.log("[publishAd] Applying category selection");
          appendPublishTrace({ step: "category-selection-start", selectionUrl, categoryPathIdsLength: categoryPathIds.length });
          await dumpPublishDebug(page, {
            accountLabel,
            step: "category-selection-start",
            error: "",
            extra: { url: page.url(), selectionUrl, categoryPathIds }
          });
          await withTimeout((async () => {
            let opened = await openCategorySelectionByPost(page);
            if (!opened) {
              await dumpPublishDebug(page, {
                accountLabel,
                step: "category-selection-post-failed",
                error: "category-selection-post-failed",
                extra: { url: page.url(), selectionUrl, categoryPathIds }
              });
            }
            if (!opened) {
              opened = await openCategorySelection(page, { selectionUrl });
            }
            if (opened && await isError400Page(page)) {
              await dumpPublishDebug(page, {
                accountLabel,
                step: "category-selection-400",
                error: "category-selection-error-400",
                extra: { url: page.url(), selectionUrl, categoryPathIds }
              });
              opened = false;
            }
            if (opened) {
              await dumpPublishDebug(page, {
                accountLabel,
                step: "category-selection-opened",
                error: "",
                extra: { url: page.url(), selectionUrl, categoryPathIds }
              });
              const ready = await waitForCategorySelectionReady(page, 20000);
              if (!ready) {
                await dumpPublishDebug(page, {
                  accountLabel,
                  step: "category-selection-wait-timeout",
                  error: "category-selection-not-ready",
                  extra: { url: page.url(), selectionUrl, categoryPathIds }
                });
              }
              const treeAvailable = await hasCategoryTree(page);
              let treeResult = null;
              if (treeAvailable) {
                treeResult = await applyCategoryPathViaTree(page, categoryPathIds);
              }
              if (treeResult?.success) {
                let navigated = false;
                try {
                  await Promise.race([
                    page.waitForNavigation({ waitUntil: "domcontentloaded", timeout: 15000 }).then(() => true).catch(() => false),
                    page.waitForFunction(
                      () => window.location.href.includes("anzeige-aufgeben-schritt2"),
                      { timeout: 15000 }
                    ).then(() => true).catch(() => false)
                  ]).then((result) => {
                    navigated = Boolean(result);
                  });
                } catch (error) {
                  navigated = false;
                }
                if (!navigated) {
                  await clickCategoryWeiter(page);
                  try {
                    await Promise.race([
                      page.waitForNavigation({ waitUntil: "domcontentloaded", timeout: 15000 }).then(() => true).catch(() => false),
                      page.waitForFunction(
                        () => window.location.href.includes("anzeige-aufgeben-schritt2"),
                        { timeout: 15000 }
                      ).then(() => true).catch(() => false)
                    ]).then((result) => {
                      navigated = Boolean(result) || navigated;
                    });
                  } catch (error) {
                    // ignore
                  }
                }
                await dumpPublishDebug(page, {
                  accountLabel,
                  step: "category-selection-tree-submit",
                  error: "",
                  extra: { url: page.url(), selectionUrl, categoryPathIds, treeResult, navigated, treeAvailable }
                });
                if (navigated) {
                  const refreshedContext = await getAdFormContext(page);
                  if (refreshedContext) {
                    formContext = refreshedContext;
                  }
                  return;
                }
              }
              const pathApplied = categoryPathIdsNumeric.length
                ? await selectCategoryPathOnSelectionPage(page, categoryPathIdsNumeric)
                : false;
              if (pathApplied) {
                await humanPause(200, 400);
              }
              await clickCategoryWeiter(page);
              await humanPause(200, 400);
              try {
                await page.waitForFunction(
                  () => !window.location.href.includes("p-kategorie-aendern"),
                  { timeout: 15000 }
                );
              } catch (error) {
                // ignore
              }
            } else if (selectionUrl) {
              await humanPause(200, 400);
              const targetUrl = normalizeSelectionUrl(selectionUrl) || "https://www.kleinanzeigen.de/p-kategorie-aendern.html";
              try {
                await page.goto(targetUrl, { waitUntil: "domcontentloaded", timeout: 20000 });
              } catch (error) {
                // ignore navigation errors
              }
              await humanPause(200, 400);
              const pathApplied = categoryPathIdsNumeric.length
                ? await selectCategoryPathOnSelectionPage(page, categoryPathIdsNumeric)
                : false;
              if (pathApplied) {
                await humanPause(200, 400);
              }
              await clickCategoryWeiter(page);
              await humanPause(200, 400);
            }
            if (page.url().includes("p-kategorie-aendern") || await isError400Page(page)) {
              await dumpPublishDebug(page, {
                accountLabel,
                step: "category-selection-stuck",
                error: "category-selection-did-not-return",
                extra: { url: page.url(), selectionUrl, categoryPathIds }
              });
              try {
                await page.evaluate(() => {
                  const form = document.querySelector("#postad-step1-frm") || document.querySelector("form");
                  if (form) form.submit();
                });
                await Promise.race([
                  page.waitForNavigation({ waitUntil: "domcontentloaded", timeout: 15000 }),
                  page.waitForFunction(
                    () => window.location.href.includes("anzeige-aufgeben-schritt2"),
                    { timeout: 15000 }
                  )
                ]);
              } catch (error) {
                // ignore submit errors
              }
              if (page.url().includes("p-kategorie-aendern")) {
                try {
                  await page.goBack({ waitUntil: "domcontentloaded", timeout: 15000 });
                } catch (error) {
                  // ignore goBack errors
                }
              }
              await humanPause(200, 400);
            }
            const refreshedContext = await getAdFormContext(page);
            if (refreshedContext) {
              formContext = refreshedContext;
            }
            await dumpPublishDebug(page, {
              accountLabel,
              step: "category-selection-done",
              error: "",
              extra: { url: page.url(), selectionUrl, categoryPathIds }
            });
          })(), 60000, "category-selection-timeout");
          appendPublishTrace({ step: "category-selection-finished" });
        } catch (error) {
          await dumpPublishDebug(page, {
            accountLabel,
            step: "category-selection-timeout",
            error: error.message,
            extra: { url: page.url(), selectionUrl, categoryPathIds }
          });
          appendPublishTrace({ step: "category-selection-timeout", error: error.message });
          console.log(`[publishAd] Category selection via selectionUrl failed: ${error.message}`);
          const timeoutFormFallbackDone = await applyCategoryFallbackOnCurrentForm();
          appendPublishTrace({
            step: "category-selection-timeout-form-fallback",
            success: timeoutFormFallbackDone
          });
          await dumpPublishDebug(page, {
            accountLabel,
            step: "category-selection-timeout-form-fallback",
            error: timeoutFormFallbackDone ? "" : "form-fallback-failed",
            extra: { url: page.url(), selectionUrl, categoryPathIds, resolvedCategoryId }
          });
          if (timeoutFormFallbackDone) {
            categorySelectionDone = true;
            categorySet = true;
          }
          if (!categorySelectionDone && categoryPathIds.length && CATEGORY_SELECTION_NEW_PAGE) {
            appendPublishTrace({ step: "category-selection-new-page-fallback-start" });
            const fallbackDone = await withTimeout(
              selectCategoryViaNewPage(browser, {
                cookies,
                deviceProfile,
                categoryPathIds,
                accountLabel,
                selectionUrl
              }),
              60000,
              "category-selection-new-page-timeout"
            ).catch(() => false);
            appendPublishTrace({ step: "category-selection-new-page-fallback-done", success: fallbackDone });
            if (fallbackDone) {
              categorySelectionDone = true;
              categorySet = true;
              await page.goto(CREATE_AD_URL, { waitUntil: "domcontentloaded", timeout: 20000 });
              await acceptCookieModal(page, { timeout: 15000 }).catch(() => {});
              if (isGdprPage(page.url())) {
                await acceptGdprConsent(page, { timeout: 20000 }).catch(() => {});
                const redirectTarget = getGdprRedirectTarget(page.url()) || CREATE_AD_URL;
                await page.goto(redirectTarget, { waitUntil: "domcontentloaded", timeout: 20000 });
              }
              const refreshedContext = await getAdFormContext(page);
              if (refreshedContext) {
                formContext = refreshedContext;
              }
            }
          }
        }
      }

      const requiredAfterCategory = await ensureRequiredFieldsAcrossContexts(page, formContext);
      if (!requiredAfterCategory.titleFilled || !requiredAfterCategory.descriptionFilled || !requiredAfterCategory.priceFilled) {
        appendPublishTrace({ step: "refill-after-category", requiredAfterCategory });
        if (!requiredAfterCategory.titleFilled) {
          const refilled = await fillField(formContext, titleSelectors, ad.title);
          if (!refilled && formContext !== page) {
            await fillField(page, titleSelectors, ad.title);
          }
          if (!titleFallback) {
            titleFallback = await fillByLabel(formContext, ["Titel", "Title"], ad.title);
            if (!titleFallback && formContext !== page) {
              titleFallback = await fillByLabel(page, ["Titel", "Title"], ad.title);
            }
          }
        }
        if (!requiredAfterCategory.descriptionFilled) {
          let refilled = await fillField(formContext, descriptionSelectors, ad.description);
          if (!refilled && formContext !== page) {
            refilled = await fillField(page, descriptionSelectors, ad.description);
          }
          if (!refilled) {
            refilled = await fillFieldAcrossContexts(page, descriptionSelectors, ad.description);
          }
          if (!refilled) {
            refilled = await fillContentEditableAcrossContexts(page, descriptionEditableSelectors, ad.description);
          }
          if (!refilled) {
            await fillFieldDeepAcrossContexts(page, {
              labelKeywords: ["beschreibung", "description"],
              value: ad.description
            });
          }
          if (!descriptionFallback) {
            descriptionFallback = await fillByLabel(formContext, ["Beschreibung", "Description"], ad.description);
            if (!descriptionFallback && formContext !== page) {
              descriptionFallback = await fillByLabel(page, ["Beschreibung", "Description"], ad.description);
            }
          }
        }
        if (!requiredAfterCategory.priceFilled) {
          const refilled = await fillPriceField(formContext, ad.price);
          if (!priceFallback) {
            priceFallback = refilled ? true : await fillByLabel(formContext, ["Preis", "Price"], ad.price);
          }
        }
      }

      const autoFilledRequired = await fillMissingRequiredFields(formContext);
      if (autoFilledRequired.length) {
        await humanPause(120, 220);
      }
      const autoFilledAttributes = await autoSelectAttributeMapFieldsAcrossContexts(page, formContext);
      if (autoFilledAttributes.length) {
        await humanPause(120, 220);
      }

      let requiredState = await ensureRequiredFieldsAcrossContexts(page, formContext);
      if (!requiredState.categorySelected && resolvedCategoryId) {
        const lateCategoryFallback = await applyCategoryFallbackOnCurrentForm();
        appendPublishTrace({
          step: "category-late-form-fallback",
          success: lateCategoryFallback
        });
        if (lateCategoryFallback) {
          requiredState = await ensureRequiredFieldsAcrossContexts(page, formContext);
        }
      }

      const missingFields = [];
      if (!titleFallback && !requiredState.titleFilled) missingFields.push("Titel");
      if (!descriptionFallback && !requiredState.descriptionFilled) missingFields.push("Beschreibung");
      if (!priceFallback && !requiredState.priceFilled) missingFields.push("Preis");
      if (!categorySet && !requiredState.categorySelected) missingFields.push("Kategorie");
      if (!postalFallback && ad.postalCode) missingFields.push("PLZ");
      if (missingFields.length > 0) {
        const autoDetails = autoFilledRequired.length
          ? ` (автозаполнение: ${autoFilledRequired.join(", ")})`
          : "";
        await dumpPublishDebug(page, {
          accountLabel,
          step: "missing-fields",
          error: "required-fields-missing",
          extra: {
            missingFields,
            requiredState,
            autoFilledRequired,
            resolvedCategoryId,
            categorySet
          }
        });
        return {
          success: false,
          error: `Не удалось заполнить обязательные поля объявления: ${missingFields.join(", ")}${autoDetails}`
        };
      }

      await humanPause(300, 500);
      try {
        await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
      } catch (error) {
        // ignore scroll errors
      }
      let directBuyState = null;
      let directBuyBeforeSummary = null;
      try {
        directBuyState = await getDirectBuyStateAcrossContexts(page, formContext);
        directBuyBeforeSummary = summarizeDirectBuyState(directBuyState);
        if (!directBuyState?.hasBlock) {
          directBuyState = await waitForDirectBuyState(page, formContext, 8000);
          directBuyBeforeSummary = summarizeDirectBuyState(directBuyState);
        }
      } catch (error) {
        console.log(`[publishAd] Direkt kaufen state precheck failed: ${error.message}`);
      }

      if (!directBuyState?.hasBlock) {
        let fastSelected = false;
        try {
          const contexts = collectDirectBuyContexts(page, formContext);
          for (const ctx of contexts) {
            try {
              fastSelected = await trySelectDirectBuyNoFast(ctx);
            } catch (error) {
              // ignore fast scan errors
            }
            if (fastSelected) break;
          }
        } catch (error) {
          // ignore fast scan errors
        }
        if (fastSelected) {
          console.log("[publishAd] Direkt kaufen opt-out successful via fast scan");
          await humanPause(120, 220);
        } else {
          let textSelected = false;
          try {
            textSelected = await trySelectDirectBuyNoIfText(formContext, 4000);
            if (!textSelected && formContext !== page) {
              textSelected = await trySelectDirectBuyNoIfText(page, 4000);
            }
          } catch (error) {
            // ignore text scan errors
          }
          if (textSelected) {
            console.log("[publishAd] Direkt kaufen opt-out successful via text scan (no block detected)");
            await humanPause(120, 220);
          } else {
            await dumpPublishDebug(page, {
              accountLabel,
              step: "direct-buy-not-present",
              error: "direct-buy-block-missing",
              extra: {
                url: page.url(),
                categoryUrl: ad?.categoryUrl || "",
                selectionUrl: selectionUrl || ""
              }
            });
            console.log("[publishAd] Direkt kaufen block not present, skipping opt-out");
          }
        }
      } else if (directBuyState?.hasSelection && directBuyBeforeSummary?.selectionIsNo) {
        console.log("[publishAd] Direkt kaufen already set to Nein, skipping opt-out");
      } else {
        console.log("[publishAd] Rebinding form context before Direkt kaufen selection");
        let contextForDirectBuy = directBuyState?.context || formContext;
        try {
          const directBuyContext = await getAdFormContext(page);
          if (directBuyContext) {
            contextForDirectBuy = directBuyContext;
          }
        } catch (error) {
          console.log(`[publishAd] Failed to rebind form context: ${error.message}`);
        }

        let directBuySelected = false;
        try {
          directBuySelected = await selectDirectBuyNo(contextForDirectBuy);
        } catch (error) {
          console.log(`[publishAd] Direkt kaufen selection failed in context: ${error.message}`);
        }
        if (!directBuySelected) {
          console.log("[publishAd] Retrying Direkt kaufen selection on page context");
          directBuySelected = await selectDirectBuyNo(page);
        }
        if (!directBuySelected) {
          const contexts = collectDirectBuyContexts(page, contextForDirectBuy);
          for (const ctx of contexts) {
            if (ctx === contextForDirectBuy || ctx === page) continue;
            try {
              directBuySelected = await selectDirectBuyNo(ctx, { retries: 2 });
            } catch (error) {
              // ignore
            }
            if (directBuySelected) break;
          }
        }
      if (directBuySelected) {
        await humanPause(120, 220);
      }
    }

      // Финальная принудительная установка buyNowSelected/радио "Nein" в каждом контексте
      try {
        await forceDirectBuyNoAcrossContexts(page, formContext);
      } catch (error) {
        // ignore force errors
      }

      if (DEBUG_PUBLISH) {
        let directBuyAfterSummary = null;
        try {
          const directBuyAfter = await getDirectBuyStateAcrossContexts(page, formContext);
          directBuyAfterSummary = summarizeDirectBuyState(directBuyAfter);
        } catch (error) {
          directBuyAfterSummary = null;
        }
        await dumpPublishDebug(page, {
          accountLabel,
          step: "direct-buy-after",
          error: "",
          extra: {
            before: directBuyBeforeSummary,
            after: directBuyAfterSummary
          }
        });
        if (directBuyAfterSummary?.hasBlock && directBuyAfterSummary.selectionIsNo === false) {
          appendPublishTrace({ step: "direct-buy-selected-not-no", selection: directBuyAfterSummary });
        }
      }

      if (directBuyBeforeSummary?.hasBlock) {
        try {
          let directBuyCheck = await getDirectBuyStateAcrossContexts(page, formContext);
          if (directBuyCheck?.hasBlock && directBuyCheck.selectionIsNo === false) {
            await selectDirectBuyNo(formContext, { retries: 2 });
            await selectDirectBuyNo(page, { retries: 2 });
            directBuyCheck = await getDirectBuyStateAcrossContexts(page, formContext);
          }
          if (directBuyCheck?.hasBlock && directBuyCheck.selectionIsNo === false) {
            await dumpPublishDebug(page, {
              accountLabel,
              step: "direct-buy-not-no",
              error: "direct-buy-not-no",
              extra: summarizeDirectBuyState(directBuyCheck)
            });
            return {
              success: false,
              error: "Не удалось выбрать пункт «Nein, Direkt kaufen nicht nutzen». Проверьте блок Direkt kaufen."
            };
          }
        } catch (error) {
          console.log(`[publishAd] Direkt kaufen validation failed: ${error.message}`);
        }
      }

      const extraFieldsApplied = await applyExtraFieldsToSelects(page, formContext, ad?.extraFields);
      appendPublishTrace({
        step: "extra-fields-applied",
        total: extraFieldsApplied.total,
        appliedCount: extraFieldsApplied.appliedCount
      });

      // Повторно пытаемся выбрать "Nein" после заполнения полей/селектов
      try {
        const directBuyState = await getDirectBuyStateAcrossContexts(page, formContext);
        if (directBuyState?.hasBlock && !directBuyState?.hasSelection) {
          console.log("[publishAd] Direkt kaufen block detected before submit, retrying opt-out");
          await selectDirectBuyNo(formContext, { retries: 2 });
          await selectDirectBuyNo(page, { retries: 2 });
          if (directBuyState?.context && directBuyState.context !== formContext && directBuyState.context !== page) {
            await selectDirectBuyNo(directBuyState.context, { retries: 2 });
          }
          await humanPause(120, 220);
        } else if (!directBuyState?.hasBlock) {
          let fastSelected = false;
          const contexts = collectDirectBuyContexts(page, formContext);
          for (const ctx of contexts) {
            try {
              fastSelected = await trySelectDirectBuyNoFast(ctx);
            } catch (error) {
              // ignore fast scan errors
            }
            if (fastSelected) break;
          }
          if (fastSelected) {
            console.log("[publishAd] Direkt kaufen opt-out successful via fast scan (pre-submit)");
            await humanPause(120, 220);
          }
        }
      } catch (error) {
        console.log(`[publishAd] Direkt kaufen state check failed: ${error.message}`);
      }

      appendPublishTrace({ step: "images-upload-start", count: Array.isArray(imagePaths) ? imagePaths.length : 0 });
      let uploadResult = await uploadImages(page, imagePaths, { accountLabel });
      if (uploadResult?.success && !uploadResult.uploaded) {
        await humanPause(800, 1200);
        const uploadedLater = await waitForUploadedImages(page, 15000);
        uploadResult = {
          ...uploadResult,
          uploaded: uploadResult.uploaded || uploadedLater,
          uploadedLater
        };
      }
      appendPublishTrace({ step: "images-upload-done", success: uploadResult?.success, uploaded: uploadResult?.uploaded });
      await dumpPublishDebug(page, {
        accountLabel,
        step: "images-upload",
        error: uploadResult?.success ? "" : (uploadResult?.reason || "upload-failed"),
        extra: {
          imageCount: Array.isArray(imagePaths) ? imagePaths.length : 0,
          uploadResult
        }
      });
      if (Array.isArray(imagePaths) && imagePaths.length > 0) {
        if (!uploadResult?.success) {
          return {
            success: false,
            error: `Не удалось загрузить фотографии: ${uploadResult?.reason || "неизвестная ошибка"}`
          };
        }
        if (!uploadResult?.uploaded) {
          return {
            success: false,
            error: "Фотографии не загрузились (превью не появилось на странице). Проверьте изображения и повторите попытку."
          };
        }

        const uploadCompleted = await waitForImageUploadCompletion(page, 90000);
        if (!uploadCompleted) {
          await dumpPublishDebug(page, {
            accountLabel,
            step: "images-upload-still-running",
            error: "images-upload-still-running",
            extra: { url: page.url() }
          });
          return {
            success: false,
            error: "Фотографии еще загружаются. Пожалуйста, подождите завершения загрузки и повторите попытку."
          };
        }
      }

      // После загрузки фото повторно фиксируем Direkt kaufen = Nein
      try {
        await forceDirectBuyNoAcrossContexts(page, formContext);
        await ensureBuyNowSelectedFalse(page);
      } catch (error) {
        // ignore
      }

      try {
        await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
      } catch (error) {
        // ignore scroll errors (navigation may be in progress)
      }
      await sleep(500);
      if (isGdprPage(page.url())) {
        await acceptGdprConsent(page);
        await page.goto(CREATE_AD_URL, { waitUntil: "domcontentloaded" });
        await humanPause(200, 400);
      }
      appendPublishTrace({ step: "before-submit" });
      const waitForSubmitEnabled = async (context) => {
        try {
          await context.waitForFunction(
            () => {
              const candidates = Array.from(
                document.querySelectorAll('button[type="submit"], input[type="submit"], button')
              );
              return candidates.some((button) => {
                const text = button.innerText || button.value || "";
                if (!text.includes("Anzeige aufgeben")) return false;
                if (button.disabled) return false;
                if (button.getAttribute("aria-disabled") === "true") return false;
                return true;
              });
            },
            { timeout: 15000 }
          );
          return true;
        } catch (error) {
          return false;
        }
      };
      const submitReady = await waitForSubmitEnabled(formContext);
      if (!submitReady && formContext !== page) {
        await waitForSubmitEnabled(page);
      }
      await sleep(1000);
      const initialUrl = page.url();
      await dumpPublishDebug(page, {
        accountLabel,
        step: "before-publish-page",
        error: "",
        extra: {
          url: page.url(),
          submitReady
        }
      });
      const submitCandidatesBeforeClick = await collectSubmitCandidatesDebug(page, [formContext]);
      await dumpPublishDebug(page, {
        accountLabel,
        step: "before-submit-click",
        error: "",
        extra: {
          url: page.url(),
          submitReady,
          submitCandidates: submitCandidatesBeforeClick
        }
      });
      appendPublishTrace({
        step: "before-submit-click-dumped",
        submitCandidateContexts: submitCandidatesBeforeClick.length,
        submitCandidateTotal: submitCandidatesBeforeClick.reduce((sum, item) => sum + Number(item?.count || 0), 0)
      });

      // Если "Direkt kaufen" всё ещё не выбран - возвращаем ошибку, чтобы не зависать
      try {
        const directBuyState = await getDirectBuyStateAcrossContexts(page, formContext);
        if (directBuyState?.hasBlock && !directBuyState?.hasSelection) {
          let selected = false;
          try {
            selected = await selectDirectBuyNo(formContext, { retries: 1 });
          } catch (error) {
            // ignore
          }
          if (!selected) {
            try {
              selected = await selectDirectBuyNo(page, { retries: 1 });
            } catch (error) {
              // ignore
            }
          }
          if (!selected) {
            const contexts = collectDirectBuyContexts(page, formContext);
            for (const ctx of contexts) {
              try {
                selected = await trySelectDirectBuyNoFast(ctx);
              } catch (error) {
                // ignore
              }
              if (selected) break;
            }
          }
          try {
            await forceDirectBuyNoAcrossContexts(page, formContext);
          } catch (error) {
            // ignore
          }
          const finalState = selected
            ? await getDirectBuyStateAcrossContexts(page, formContext)
            : directBuyState;
          if (finalState?.hasBlock && !finalState?.hasSelection) {
            await dumpPublishDebug(page, {
              accountLabel,
              step: "direct-buy-missing",
              error: "direct-buy-not-selected",
              extra: { url: page.url() }
            });
            return {
              success: false,
              error: "Не удалось выбрать пункт «Nein, Direkt kaufen не использовать». Пожалуйста, проверьте форму на сайте."
            };
          }
        }
      } catch (error) {
        console.log(`[publishAd] Direkt kaufen final check failed: ${error.message}`);
      }

      const submitClicked = await clickSubmitButton(page, [formContext], { allowFallback: false });

      if (!submitClicked) {
        const submitCandidatesAfterMiss = await collectSubmitCandidatesDebug(page, [formContext]);
        await dumpPublishDebug(page, {
          accountLabel,
          step: "submit-not-found",
          error: "submit-button-missing",
          extra: {
            url: page.url(),
            submitReady,
            submitCandidates: submitCandidatesAfterMiss
          }
        });
        appendPublishTrace({ step: "submit-not-found" });
        return {
          success: false,
          error: "Не удалось найти кнопку публикации на Kleinanzeigen"
        };
      }
      appendPublishTrace({ step: "submit-clicked" });

      let publishState = await waitForPublishState(page, defaultTimeout);
      appendPublishTrace({ step: "publish-state-initial", publishState });
      if (publishState === "form") {
        await dumpPublishDebug(page, {
          accountLabel,
          step: "after-submit-form",
          error: "still-on-form-after-submit",
          extra: {
            url: page.url()
          }
        });
      }
      if (publishState !== "success" && isGdprPage(page.url())) {
        const accepted = await acceptGdprConsent(page, { timeout: 20000 });
        const redirectTarget = getGdprRedirectTarget(page.url()) || CREATE_AD_URL;
        if (accepted && redirectTarget) {
          try {
            await page.goto(redirectTarget, { waitUntil: "domcontentloaded" });
          } catch (error) {
            // ignore navigation errors
          }
        }
        await humanPause(200, 400);
        const retryContext = (await getAdFormContext(page)) || formContext;
        await clickSubmitButton(page, [retryContext], { allowFallback: false });
        publishState = await waitForPublishState(page, defaultTimeout);
        appendPublishTrace({ step: "publish-state-after-gdpr", publishState });
      }

      // Kleinanzeigen can keep the user on step2 for a short time after submit and then navigate to
      // the final step (p-anzeige-abschicken) where additional required selects appear (e.g. Art).
      // waitForPublishState() returns "form" immediately on both steps, so we explicitly wait for
      // the step transition and fill required selects again on the abschicken page.
      const tryHandleAbschickenStep = async (reason) => {
        const url = page.url();
        if (!/anzeige-abschicken/i.test(url)) return false;
        appendPublishTrace({ step: "publish-abschicken-detected", reason, url });

        await page.waitForSelector("body", { timeout: 15000 }).catch(() => {});

        try {
          const refreshed = await getAdFormContext(page);
          if (refreshed) {
            formContext = refreshed;
          }
        } catch (error) {
          // ignore context refresh errors
        }

        const extraApplied = await applyExtraFieldsToSelects(page, formContext, ad?.extraFields);
        const preferredApplied = await applyPreferredAttributeSelectsAcrossContexts(
          page,
          formContext,
          preferredFieldValues
        );

        let desiredVersand = "nein";
        try {
          const entries = normalizeExtraFieldEntries(ad?.extraFields);
          for (const entry of entries) {
            const name = String(entry?.name || "").toLowerCase();
            const label = String(entry?.label || "").toLowerCase();
            const rawValue = entry?.value;
            if (rawValue === undefined || rawValue === null) continue;
            const value = String(rawValue).trim().toLowerCase();
            if (!value) continue;
            const mentionsVersand = name.includes("versand_s") || label.includes("versand");
            if (!mentionsVersand) continue;
            if (value === "ja" || value === "nein") {
              desiredVersand = value;
              break;
            }
            if (value === "shipping" || value.includes("versand möglich") || value.includes("versand moeglich")) {
              desiredVersand = "ja";
              break;
            }
            if (value === "pickup" || value.includes("abholung")) {
              desiredVersand = "nein";
              break;
            }
          }
        } catch (error) {
          // ignore inference errors
        }

        const versandApplied = await ensureVersandSelectionAcrossContexts(page, formContext, desiredVersand);

        // Try to sync the micro-frontend shipping radio group when it exists.
        try {
          const radioLabelSelector = desiredVersand === "ja"
            ? 'label[for="radio-shipping"]'
            : 'label[for="radio-pickup"]';
          const labelHandle = await page.$(radioLabelSelector);
          if (labelHandle) {
            await scrollIntoView(labelHandle);
            await safeClick(labelHandle);
            await humanPause(120, 220);
          }
        } catch (error) {
          // ignore shipping radio sync errors
        }

        try {
          await forceDirectBuyNoAcrossContexts(page, formContext);
          await ensureBuyNowSelectedFalse(page);
        } catch (error) {
          // ignore buy-now repairs
        }

        try {
          await acceptTermsIfPresent(formContext || page);
          if (formContext && formContext !== page) {
            await acceptTermsIfPresent(page);
          }
        } catch (error) {
          // ignore terms
        }

        await dumpPublishDebug(page, {
          accountLabel,
          step: "abschicken-pre-submit",
          error: "",
          extra: {
            url: page.url(),
            reason,
            desiredVersand,
            extraApplied,
            preferredApplied,
            versandApplied
          }
        });

        const urlBeforeSubmit = page.url();
        const clicked = await clickSubmitButton(page, [formContext, page], { allowFallback: false });
        if (!clicked) {
          await clickSubmitButton(page, [page], { allowFallback: true });
        }
        const submitTransition = await waitForPostSubmitTransition(page, {
          initialUrl: urlBeforeSubmit,
          timeoutMs: 60000
        });
        appendPublishTrace({ step: "post-submit-transition-after-abschicken-submit", submitTransition });
        if (submitTransition?.outcome === "success") {
          publishState = "success";
          appendPublishTrace({ step: "publish-state-after-abschicken-submit", publishState });
          return true;
        }
        if (submitTransition?.outcome === "preview") {
          publishState = "preview";
        } else {
          publishState = await waitForPublishState(page, defaultTimeout);
        }
        appendPublishTrace({ step: "publish-state-after-abschicken-submit", publishState });
        return true;
      };

      if (publishState === "form") {
        const urlAfterSubmit = page.url();
        const isStep2 = /anzeige-aufgeben/i.test(urlAfterSubmit) && !/anzeige-abschicken/i.test(urlAfterSubmit);
        if (isStep2) {
          const transition = await waitForPostSubmitTransition(page, {
            initialUrl,
            timeoutMs: 90000
          });
          appendPublishTrace({ step: "post-submit-transition", transition });
          if (transition?.outcome === "success") {
            publishState = "success";
          } else if (transition?.outcome === "preview") {
            publishState = "preview";
          } else if (transition?.outcome === "abschicken") {
            await tryHandleAbschickenStep("transition-from-step2");
          }
        } else if (/anzeige-abschicken/i.test(urlAfterSubmit)) {
          await tryHandleAbschickenStep("already-on-abschicken");
        }
      }
      if (publishState === "preview") {
        try {
          await dumpPublishDebug(page, {
            accountLabel,
            step: "preview-before-submit",
            error: "",
            extra: { url: page.url() }
          });
        } catch (error) {
          // ignore preview debug errors
        }
        await sleep(800);
        let previewContext = formContext;
        try {
          const refreshed = await getAdFormContext(page);
          if (refreshed) previewContext = refreshed;
        } catch (error) {
          // ignore context refresh errors
        }
        try {
          await acceptTermsIfPresent(previewContext || page);
          if (previewContext && previewContext !== page) {
            await acceptTermsIfPresent(page);
          }
        } catch (error) {
          // ignore terms
        }
        const previewSubmitCandidates = await collectSubmitCandidatesDebug(page, [previewContext || page]);
        await dumpPublishDebug(page, {
          accountLabel,
          step: "preview-before-submit-click",
          error: "",
          extra: {
            url: page.url(),
            previewSubmitCandidates
          }
        });
        const clickedPreview = await clickSubmitButton(page, [previewContext || page], { allowFallback: false });
        if (!clickedPreview) {
          await clickSubmitButton(page, [page], { allowFallback: true });
        }
        publishState = await waitForPublishState(page, 120000);
        appendPublishTrace({ step: "publish-state-after-preview", publishState });
        if (publishState === "preview") {
          await dumpPublishDebug(page, {
            accountLabel,
            step: "preview-still",
            error: "preview-still",
            extra: { url: page.url() }
          });
          const errors = await collectFormErrors(page);
          const errorDetails = errors.length ? `: ${errors.join("; ")}` : "";
          return {
            success: false,
            error: `Не удалось подтвердить публикацию на странице предпросмотра${errorDetails}`,
            url: page.url()
          };
        }
      }
      if (publishState !== "success") {
        appendPublishTrace({ step: "publish-state-no-fallback", publishState });
      }
      if (publishState !== "success") {
        const frameState = await getPublishStateFromFrames(page);
        if (frameState) {
          publishState = frameState;
          appendPublishTrace({ step: "publish-state-from-frames", publishState });
        }
      }
      let resubmitAttempts = 0;
      const maxResubmitAttempts = 1;
      const isPrimaryPublishSubmitInProgress = async () => {
        try {
          return await page.evaluate(() => {
            const normalize = (value) => String(value || "").replace(/\s+/g, " ").trim().toLowerCase();
            const tokens = [
              "anzeige aufgeben",
              "anzeige veröffentlichen",
              "anzeige veroeffentlichen",
              "veröffentlichen",
              "veroeffentlichen",
              "jetzt veröffentlichen",
              "jetzt veroeffentlichen"
            ];
            const nodes = Array.from(document.querySelectorAll("button, input[type='submit'], input[type='button']"));
            const candidates = nodes.filter((node) => {
              const text = normalize(node.innerText || node.value || node.getAttribute("aria-label") || "");
              if (!text) return false;
              return tokens.some((token) => text.includes(token));
            });
            if (!candidates.length) return false;
            return candidates.some((node) => {
              if (node.disabled) return true;
              if (node.getAttribute("aria-disabled") === "true") return true;
              if (node.classList.contains("disabled")) return true;
              if (node.getAttribute("aria-busy") === "true") return true;
              const dataBusy = String(node.getAttribute("data-busy") || node.getAttribute("data-loading") || "").toLowerCase();
              return dataBusy === "true" || dataBusy === "1";
            });
          });
        } catch (error) {
          return false;
        }
      };
      const tryResubmitAfterRepair = async (reason) => {
        // Avoid double-posting: do not resubmit if the publish attempt is already in-flight or if
        // the page shows explicit success signals.
        try {
          const inferred = await inferPublishSuccess(page);
          const hasSuccessSignal = Boolean(
            inferred?.isSuccessUrl ||
              inferred?.hasPageTypeSuccess ||
              inferred?.hasSuccessTitle ||
              inferred?.hasSuccessText ||
              inferred?.hasShadowSuccessText
          );
          if (hasSuccessSignal) {
            publishState = "success";
            appendPublishTrace({
              step: "publish-resubmit-skip-success-signal",
              reason,
              url: inferred?.url || page.url()
            });
            return false;
          }
        } catch (error) {
          // ignore inference errors
        }

        if (await isPrimaryPublishSubmitInProgress()) {
          appendPublishTrace({ step: "publish-resubmit-skip-in-progress", reason, url: page.url() });
          return false;
        }

        if (resubmitAttempts >= maxResubmitAttempts) {
          appendPublishTrace({
            step: "publish-resubmit-skipped",
            reason,
            resubmitAttempts
          });
          return false;
        }
        resubmitAttempts += 1;
        appendPublishTrace({
          step: "publish-resubmit",
          reason,
          resubmitAttempts
        });
        await humanPause(180, 320);
        const urlBeforeResubmit = page.url();
        await clickSubmitButton(page, [formContext, page], { allowFallback: false });
        const resubmitTransition = await waitForPostSubmitTransition(page, {
          initialUrl: urlBeforeResubmit,
          timeoutMs: 60000
        });
        appendPublishTrace({ step: "post-submit-transition-after-resubmit", reason, resubmitTransition });
        if (resubmitTransition?.outcome === "success") {
          publishState = "success";
        } else if (resubmitTransition?.outcome === "preview") {
          publishState = "preview";
        } else {
          publishState = await waitForPublishState(page, defaultTimeout);
        }
        appendPublishTrace({
          step: "publish-state-after-resubmit",
          reason,
          publishState,
          resubmitAttempts
        });
        return true;
      };

      if (publishState === "form") {
        const errorsBeforeRepair = await collectFormErrors(page);
        appendPublishTrace({
          step: "publish-form-errors-before-repair",
          errors: errorsBeforeRepair.length
        });
        if (errorsBeforeRepair.length > 0) {
          const invalidRepair = await repairInvalidRequiredFieldsAcrossContexts(page, formContext);
          appendPublishTrace({
            step: "publish-invalid-repair",
            totalInvalid: invalidRepair.totalInvalid,
            fixedCount: invalidRepair.fixedCount
          });
          if (invalidRepair.fixedCount > 0) {
            await tryResubmitAfterRepair("invalid-repair");
          }
        } else {
          appendPublishTrace({ step: "publish-invalid-repair-skipped", reason: "no-visible-errors" });
        }
      }

      // Если остаемся на форме (p-anzeige-abschicken), чиним только явные ошибки и делаем максимум один retry
      if (publishState === "form" && page.url().includes("p-anzeige-abschicken")) {
        const errors = await collectFormErrors(page);
        const buyNowError = errors.find((text) => /direkt kaufen|beiden optionen/i.test(text));
        let needsResubmit = false;

        // Some categories show additional required selects only on p-anzeige-abschicken (e.g. Art).
        // Fill them using explicit extraFields and safe category-based hints before trying other repairs.
        let desiredVersand = "nein";
        try {
          const entries = normalizeExtraFieldEntries(ad?.extraFields);
          for (const entry of entries) {
            const name = String(entry?.name || "").toLowerCase();
            const label = String(entry?.label || "").toLowerCase();
            const rawValue = entry?.value;
            if (rawValue === undefined || rawValue === null) continue;
            const value = String(rawValue).trim().toLowerCase();
            if (!value) continue;
            const mentionsVersand = name.includes("versand_s") || label.includes("versand");
            if (!mentionsVersand) continue;
            if (value === "ja" || value === "nein") {
              desiredVersand = value;
              break;
            }
            if (value === "shipping" || value.includes("versand möglich") || value.includes("versand moeglich")) {
              desiredVersand = "ja";
              break;
            }
            if (value === "pickup" || value.includes("abholung")) {
              desiredVersand = "nein";
              break;
            }
          }
        } catch (error) {
          // ignore inference errors
        }

        const abschickenExtraApplied = await applyExtraFieldsToSelects(page, formContext, ad?.extraFields);
        const abschickenPreferredApplied = await applyPreferredAttributeSelectsAcrossContexts(
          page,
          formContext,
          preferredFieldValues
        );
        const abschickenVersandApplied = await ensureVersandSelectionAcrossContexts(page, formContext, desiredVersand);

        if (
          errors.length > 0 && (
            abschickenExtraApplied.appliedCount > 0 ||
            abschickenPreferredApplied.changed > 0 ||
            abschickenVersandApplied.changed > 0
          )
        ) {
          needsResubmit = true;
          await dumpPublishDebug(page, {
            accountLabel,
            step: "abschicken-required-selects-applied",
            error: "",
            extra: {
              url: page.url(),
              desiredVersand,
              abschickenExtraApplied,
              abschickenPreferredApplied,
              abschickenVersandApplied
            }
          });
        }

        // Try to sync the micro-frontend shipping radio group when it exists.
        if (abschickenVersandApplied.changed > 0) {
          try {
            const radioLabelSelector = desiredVersand === "ja"
              ? 'label[for="radio-shipping"]'
              : 'label[for="radio-pickup"]';
            const labelHandle = await page.$(radioLabelSelector);
            if (labelHandle) {
              await scrollIntoView(labelHandle);
              await safeClick(labelHandle);
              await humanPause(120, 220);
            }
          } catch (error) {
            // ignore shipping radio sync errors
          }
        }

        if (buyNowError) {
          appendPublishTrace({ step: "publish-form-abschicken-repair-buy-now" });
          try {
            await forceDirectBuyNoAcrossContexts(page, formContext);
            await ensureBuyNowSelectedFalse(page);
            await acceptTermsIfPresent(page);
            needsResubmit = true;
          } catch (error) {
            // ignore retry errors
          }
        } else {
          appendPublishTrace({ step: "publish-form-abschicken-buy-now-not-needed" });
        }

        const serverErrorRepair = await repairServerSideRequiredErrorsAcrossContexts(
          page,
          formContext,
          preferredFieldValues
        );
        appendPublishTrace({
          step: "publish-server-error-repair",
          totalErrors: serverErrorRepair.totalErrors,
          fixedCount: serverErrorRepair.fixedCount
        });
        if (errors.length > 0 && serverErrorRepair.fixedCount > 0) {
          needsResubmit = true;
          await dumpPublishDebug(page, {
            accountLabel,
            step: "publish-server-error-repair",
            error: "",
            extra: {
              url: page.url(),
              errorsBefore: errors,
              serverErrorRepair
            }
          });
        }

        if (needsResubmit) {
          await tryResubmitAfterRepair("abschicken-repair");
        } else {
          appendPublishTrace({ step: "publish-form-abschicken-no-retry" });
        }
      }

	      if (publishState !== "success") {
	        const progressDetected = await waitForPublishProgress(page, initialUrl, 30000);
	        const errors = await collectFormErrors(page);
	        const submitCandidatesFinal = await collectSubmitCandidatesDebug(page, [formContext]);
	        const errorDetails = errors.length ? `: ${errors.join("; ")}` : "";
	        const stateDetails = publishState === "form" ? " (страница осталась на форме)" : "";
	        const inferred = await inferPublishSuccess(page);
	        const progressedAwayFromForm = Boolean(
	          progressDetected &&
	          inferred.url &&
	          inferred.url !== initialUrl &&
	          !inferred.isKnownForm
	        );
	        const explicitSuccessSignals = {
	          publishStateSuccess: publishState === "success",
	          isSuccessUrl: Boolean(inferred.isSuccessUrl),
	          hasPageTypeSuccess: Boolean(inferred.hasPageTypeSuccess),
	          hasSuccessTitle: Boolean(inferred.hasSuccessTitle),
	          hasSuccessText: Boolean(inferred.hasSuccessText),
	          hasShadowSuccessText: Boolean(inferred.hasShadowSuccessText),
	          hasAdLink: Boolean(inferred.hasAdLink),
	          progressDetected: Boolean(progressDetected),
	          progressedAwayFromForm
	        };
        appendPublishTrace({
          step: "publish-success-signals",
          publishState,
          errors: errors.length,
          isPreview: Boolean(inferred.isPreview),
          ...explicitSuccessSignals
        });
	        const hardSuccessSignal = explicitSuccessSignals.publishStateSuccess ||
	          explicitSuccessSignals.isSuccessUrl ||
	          explicitSuccessSignals.hasPageTypeSuccess ||
	          explicitSuccessSignals.hasSuccessTitle;
        const softSuccessSignal = explicitSuccessSignals.hasSuccessText ||
          explicitSuccessSignals.hasShadowSuccessText ||
          (explicitSuccessSignals.hasAdLink && !inferred.isKnownForm) ||
          explicitSuccessSignals.progressDetected ||
          explicitSuccessSignals.progressedAwayFromForm;
	        const canTreatAsSuccess = !inferred.isPreview &&
	          (hardSuccessSignal || (!errors.length && softSuccessSignal));
	        if (canTreatAsSuccess) {
	          return {
	            success: true,
	            message: "Объявление отправлено на публикацию",
	            url: inferred.url
	          };
	        }

	        // Some Kleinanzeigen flows keep the user on the form even after the submit succeeds.
	        // When we have no visible errors, do a secondary check by opening "Meine Anzeigen".
	        let fallbackVerification = null;
	        if (!errors.length && !inferred.isPreview) {
	          fallbackVerification = await verifyPublishedByCheckingMyAds(browser, {
	            title: ad?.title,
	            deviceProfile,
	            timeoutMs: 45000
	          });
	          appendPublishTrace({
	            step: "publish-fallback-my-ads-check",
	            verified: Boolean(fallbackVerification?.verified),
	            method: fallbackVerification?.method || "",
	            reason: fallbackVerification?.reason || ""
	          });
	          if (fallbackVerification?.verified) {
	            return {
	              success: true,
	              message: "Объявление отправлено на публикацию",
	              url: fallbackVerification.url || inferred.url || page.url()
	            };
	          }
	        }
	        await dumpPublishDebug(page, {
	          accountLabel,
	          step: "publish-not-confirmed",
	          error: "publish-not-confirmed",
	          extra: {
	            publishState,
	            errors,
	            inferred,
	            fallbackVerification,
	            progressDetected,
	            submitCandidatesFinal,
	            url: page.url()
	          }
	        });
        return {
          success: false,
          error: `Публикация не подтверждена${stateDetails}${errorDetails}`,
          url: page.url()
        };
      }

      await dumpPublishDebug(page, {
        accountLabel,
        step: "publish-success",
        error: "",
        extra: { url: page.url() }
      });
      appendPublishTrace({ step: "publish-success" });
      return {
        success: true,
        message: "Объявление отправлено на публикацию",
        url: page.url()
      };
    } catch (error) {
      const message = error?.message || "Unknown error";
      if (page && /Execution context was destroyed/i.test(message)) {
        try {
          await sleep(1500);
          const publishState = await waitForPublishState(page, 30000);
          if (publishState === "success") {
            return {
              success: true,
              message: "Объявление отправлено на публикацию",
              url: page.url()
            };
          }
        } catch (innerError) {
          // fall through to error handler
        }
      }
      await dumpPublishDebug(page, {
        accountLabel,
        step: "exception",
        error: message
      });
      return {
        success: false,
        error: message
      };
    } finally {
      await browser.close();
      if (anonymizedProxyUrl) {
        await proxyChain.closeAnonymizedProxy(anonymizedProxyUrl, true);
      }
      fs.rmSync(profileDir, { recursive: true, force: true });
    }
  };

  try {
    return await attemptPublish({ useProxy: true });
  } finally {
    publishDebugOverride = prevDebugOverride;
  }
};

module.exports = {
  extractCategoryIdFromUrl,
  publishAd,
  parseExtraSelectFields,
  parseExtraSelectFieldsAcrossContexts,
  acceptCookieModal,
  acceptGdprConsent,
  isGdprPage,
  resolveCategoryPathFromCache,
  getCategorySelectionUrl,
  openCategorySelection,
  openCategorySelectionByPost,
  waitForCategorySelectionReady,
  selectCategoryPathOnSelectionPage,
  applyCategoryPathViaTree,
  hasCategoryTree,
  clickCategoryWeiter
};
