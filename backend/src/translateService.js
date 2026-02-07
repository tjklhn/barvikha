const crypto = require("crypto");
const axios = require("axios");

const DEFAULT_ENDPOINT = "https://api.cognitive.microsofttranslator.com";
const DEFAULT_CACHE_TTL_MS = 24 * 60 * 60 * 1000;

const normalizeLang = (value) => String(value || "").trim().toLowerCase();

const getAzureTranslatorConfig = () => {
  const key = String(process.env.AZURE_TRANSLATOR_KEY || process.env.AZURE_AI_TRANSLATOR_KEY || "").trim();
  const region = String(process.env.AZURE_TRANSLATOR_REGION || process.env.AZURE_TRANSLATOR_LOCATION || "").trim();
  const endpointRaw = String(process.env.AZURE_TRANSLATOR_ENDPOINT || process.env.AZURE_AI_TRANSLATOR_ENDPOINT || "").trim();
  const endpoint = endpointRaw || DEFAULT_ENDPOINT;

  return { key, region, endpoint };
};

const buildTranslateUrl = ({ endpoint, to, from }) => {
  const url = new URL(endpoint || DEFAULT_ENDPOINT);

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

const cleanupCache = (ttlMs) => {
  const now = Date.now();
  for (const [key, entry] of translationsCache.entries()) {
    if (!entry || !entry.createdAt || now - entry.createdAt > ttlMs) {
      translationsCache.delete(key);
    }
  }
};

const translateText = async ({ text, to = "ru", from = "" } = {}) => {
  const sourceText = String(text || "").trim();
  if (!sourceText) {
    const error = new Error("Text is required");
    error.code = "BAD_REQUEST";
    throw error;
  }

  const target = normalizeLang(to) || "ru";
  const source = normalizeLang(from);
  const { key, region, endpoint } = getAzureTranslatorConfig();

  if (!key || !region) {
    const error = new Error("Azure Translator is not configured");
    error.code = "NOT_CONFIGURED";
    throw error;
  }

  const ttlMs = Number(process.env.AZURE_TRANSLATOR_CACHE_TTL_MS || DEFAULT_CACHE_TTL_MS) || DEFAULT_CACHE_TTL_MS;
  cleanupCache(ttlMs);

  const cacheKey = createCacheKey({ text: sourceText, to: target, from: source });
  const cached = translationsCache.get(cacheKey);
  if (cached && cached.value && cached.createdAt && Date.now() - cached.createdAt <= ttlMs) {
    return { ...cached.value, cached: true };
  }

  const url = buildTranslateUrl({ endpoint, to: target, from: source });
  const response = await axios.post(
    url,
    [{ text: sourceText }],
    {
      timeout: 12000,
      headers: {
        "Content-Type": "application/json",
        "Ocp-Apim-Subscription-Key": key,
        "Ocp-Apim-Subscription-Region": region
      },
      validateStatus: (status) => status >= 200 && status < 500
    }
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
  const result = {
    translatedText: translated,
    to: target,
    from: source || detectedLanguage || "",
    detectedLanguage: detectedLanguage || "",
    cached: false
  };

  translationsCache.set(cacheKey, { createdAt: Date.now(), value: result });
  return result;
};

module.exports = {
  translateText
};

