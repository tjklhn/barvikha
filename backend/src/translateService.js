const crypto = require("crypto");
const axios = require("axios");
const ProxyAgent = require("proxy-agent");
const { buildProxyUrl } = require("./cookieUtils");

const DEFAULT_AZURE_ENDPOINT = "https://api.cognitive.microsofttranslator.com";
const DEFAULT_DEEPL_FREE_ENDPOINT = "https://api-free.deepl.com/v2/translate";
const DEFAULT_DEEPL_PRO_ENDPOINT = "https://api.deepl.com/v2/translate";
const DEFAULT_CACHE_TTL_MS = 24 * 60 * 60 * 1000;

const normalizeLang = (value) => String(value || "").trim().toLowerCase();
const normalizeProvider = (value) => String(value || "").trim().toLowerCase();
const toDeepLLang = (value) => String(value || "")
  .trim()
  .replace(/_/g, "-")
  .toUpperCase();

const getAzureTranslatorConfig = () => {
  const key = String(process.env.AZURE_TRANSLATOR_KEY || process.env.AZURE_AI_TRANSLATOR_KEY || "").trim();
  const region = String(process.env.AZURE_TRANSLATOR_REGION || process.env.AZURE_TRANSLATOR_LOCATION || "").trim();
  const endpointRaw = String(process.env.AZURE_TRANSLATOR_ENDPOINT || process.env.AZURE_AI_TRANSLATOR_ENDPOINT || "").trim();
  const endpoint = endpointRaw || DEFAULT_AZURE_ENDPOINT;

  return { key, region, endpoint };
};

const getDeepLConfig = () => {
  const key = String(process.env.DEEPL_AUTH_KEY || process.env.DEEPL_API_KEY || "").trim();
  const endpointRaw = String(process.env.DEEPL_ENDPOINT || process.env.DEEPL_API_BASE || process.env.DEEPL_API_URL || "").trim();
  const plan = normalizeProvider(process.env.DEEPL_PLAN || process.env.DEEPL_TIER || "");

  let endpoint = endpointRaw;
  if (!endpoint) {
    // DeepL free keys usually end with ":fx" but we don't rely on it strictly.
    const looksFree = key.endsWith(":fx");
    if (plan === "pro") endpoint = DEFAULT_DEEPL_PRO_ENDPOINT;
    else if (plan === "free") endpoint = DEFAULT_DEEPL_FREE_ENDPOINT;
    else endpoint = looksFree ? DEFAULT_DEEPL_FREE_ENDPOINT : DEFAULT_DEEPL_PRO_ENDPOINT;
  }

  return { key, endpoint };
};

const buildAzureTranslateUrl = ({ endpoint, to, from }) => {
  const url = new URL(endpoint || DEFAULT_AZURE_ENDPOINT);

  // If user only provided a host, decide correct path automatically.
  const normalizedPath = String(url.pathname || "").replace(/\/+$/, "");
  if (!normalizedPath || normalizedPath === "/") {
    if (String(url.hostname || "").toLowerCase().endsWith(".cognitiveservices.azure.com")) {
      url.pathname = "/translator/text/v3.0/translate";
    } else {
      url.pathname = "/translate";
    }
  }

  url.searchParams.set("api-version", "3.0");
  url.searchParams.set("to", to);
  if (from) url.searchParams.set("from", from);

  return url.toString();
};

const createCacheKey = ({ text, to, from }) =>
  crypto
    .createHash("sha256")
    .update(JSON.stringify({ text, to, from }))
    .digest("hex");

const translationsCache = new Map();

const buildAxiosConfig = ({ proxy, headers = {}, timeout = 12000, validateStatus, requireProxy = false } = {}) => {
  const config = {
    headers,
    timeout,
    validateStatus: validateStatus || ((status) => status >= 200 && status < 500)
  };
  const proxyUrl = buildProxyUrl(proxy);

  if (requireProxy && !proxyUrl) {
    const error = new Error("Proxy is required for this request");
    error.code = "PROXY_REQUIRED";
    throw error;
  }

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

    // Do not silently fall back to direct connection when proxy was provided.
    if (!agent) {
      const error = new Error("Failed to initialize proxy agent");
      error.code = "PROXY_INIT_FAILED";
      throw error;
    }

    config.httpAgent = agent;
    config.httpsAgent = agent;
    config.proxy = false;
  }
  return config;
};

const cleanupCache = (ttlMs) => {
  const now = Date.now();
  for (const [key, entry] of translationsCache.entries()) {
    if (!entry || !entry.createdAt || now - entry.createdAt > ttlMs) {
      translationsCache.delete(key);
    }
  }
};

const getCacheTtlMs = () => {
  const ttlMs = Number(
    process.env.TRANSLATOR_CACHE_TTL_MS
      || process.env.KL_TRANSLATOR_CACHE_TTL_MS
      || process.env.AZURE_TRANSLATOR_CACHE_TTL_MS
      || process.env.DEEPL_CACHE_TTL_MS
      || DEFAULT_CACHE_TTL_MS
  );
  return Number.isFinite(ttlMs) && ttlMs > 0 ? ttlMs : DEFAULT_CACHE_TTL_MS;
};

const translateViaAzure = async ({ text, to, from, proxy }) => {
  const target = normalizeLang(to) || "ru";
  const source = normalizeLang(from);
  const { key, region, endpoint } = getAzureTranslatorConfig();

  if (!key || !region) {
    const error = new Error("Azure Translator is not configured");
    error.code = "NOT_CONFIGURED";
    throw error;
  }

  const url = buildAzureTranslateUrl({ endpoint, to: target, from: source });
  const response = await axios.post(
    url,
    [{ text }],
    buildAxiosConfig({
      proxy,
      requireProxy: Boolean(proxy),
      timeout: 12000,
      headers: {
        "Content-Type": "application/json",
        "Ocp-Apim-Subscription-Key": key,
        "Ocp-Apim-Subscription-Region": region
      },
      validateStatus: (status) => status >= 200 && status < 500
    })
  );

  if (!response || response.status < 200 || response.status >= 300) {
    const detail = typeof response?.data === "string"
      ? response.data.slice(0, 200)
      : JSON.stringify(response?.data || {}).slice(0, 200);
    const error = new Error(`Azure translate failed: HTTP ${response?.status || 0} ${detail}`);
    error.code = "UPSTREAM_ERROR";
    error.status = response?.status;
    throw error;
  }

  const payload = Array.isArray(response.data) ? response.data[0] : null;
  const translated = payload?.translations?.[0]?.text || "";
  if (!translated) {
    const error = new Error("Azure translate returned empty result");
    error.code = "UPSTREAM_EMPTY";
    throw error;
  }

  const detectedLanguage = payload?.detectedLanguage?.language || "";
  return {
    translatedText: translated,
    to: target,
    from: source || detectedLanguage || "",
    detectedLanguage: detectedLanguage || ""
  };
};

const translateViaDeepL = async ({ text, to, from, proxy }) => {
  const target = toDeepLLang(to || "ru") || "RU";
  const source = from ? toDeepLLang(from) : "";
  const { key, endpoint } = getDeepLConfig();

  if (!key || !endpoint) {
    const error = new Error("DeepL is not configured");
    error.code = "NOT_CONFIGURED";
    throw error;
  }

  const params = new URLSearchParams();
  params.append("text", String(text));
  params.set("target_lang", target);
  if (source) params.set("source_lang", source);
  params.set("preserve_formatting", "1");

  const response = await axios.post(
    endpoint,
    params.toString(),
    buildAxiosConfig({
      proxy,
      requireProxy: Boolean(proxy),
      timeout: 12000,
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        Authorization: `DeepL-Auth-Key ${key}`
      },
      validateStatus: (status) => status >= 200 && status < 500
    })
  );

  if (!response || response.status < 200 || response.status >= 300) {
    const status = response?.status || 0;
    const detail = typeof response?.data === "string"
      ? response.data.slice(0, 200)
      : JSON.stringify(response?.data || {}).slice(0, 200);
    const error = new Error(`DeepL translate failed: HTTP ${status} ${detail}`);
    error.code = status === 403 ? "AUTH_FAILED" : "UPSTREAM_ERROR";
    error.status = status;
    throw error;
  }

  const payload = response.data || {};
  const translated = payload?.translations?.[0]?.text || "";
  if (!translated) {
    const error = new Error("DeepL translate returned empty result");
    error.code = "UPSTREAM_EMPTY";
    throw error;
  }

  const detectedLanguage = payload?.translations?.[0]?.detected_source_language || "";
  return {
    translatedText: translated,
    to: target,
    from: source || String(detectedLanguage || "").toLowerCase() || "",
    detectedLanguage: detectedLanguage || ""
  };
};

const translateText = async ({ text, to = "ru", from = "", proxy } = {}) => {
  const sourceText = String(text || "").trim();
  if (!sourceText) {
    const error = new Error("Text is required");
    error.code = "BAD_REQUEST";
    throw error;
  }

  const target = normalizeLang(to) || "ru";
  const source = normalizeLang(from);
  const ttlMs = getCacheTtlMs();
  cleanupCache(ttlMs);

  const cacheKey = createCacheKey({ text: sourceText, to: target, from: source });
  const cached = translationsCache.get(cacheKey);
  if (cached && cached.value && cached.createdAt && Date.now() - cached.createdAt <= ttlMs) {
    return { ...cached.value, cached: true };
  }

  const requestedProvider = normalizeProvider(
    process.env.TRANSLATOR_PROVIDER
      || process.env.KL_TRANSLATOR_PROVIDER
      || process.env.KL_TRANSLATION_PROVIDER
      || ""
  );

  const deeplConfigured = Boolean(getDeepLConfig().key);
  const azureConfigured = Boolean(getAzureTranslatorConfig().key && getAzureTranslatorConfig().region);

  const provider = requestedProvider
    || (deeplConfigured ? "deepl" : "")
    || (azureConfigured ? "azure" : "");

  let translatedResult;
  if (provider === "deepl") {
    translatedResult = await translateViaDeepL({ text: sourceText, to: target, from: source, proxy });
  } else if (provider === "azure") {
    translatedResult = await translateViaAzure({ text: sourceText, to: target, from: source, proxy });
  } else {
    const error = new Error("Translator is not configured");
    error.code = "NOT_CONFIGURED";
    throw error;
  }

  const result = {
    ...translatedResult,
    cached: false
  };

  translationsCache.set(cacheKey, { createdAt: Date.now(), value: result });
  return result;
};

module.exports = {
  translateText
};
