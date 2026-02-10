const normalizeBase = (value) => String(value || "").trim().replace(/\/+$/, "");

const getRuntimeOrigin = () => {
  if (typeof window === "undefined" || !window.location) return "";
  return normalizeBase(window.location.origin || "");
};

// Keep defaults safe for production: prefer same-origin (relative "/api/...") to avoid CORS/mixed-content.
// Allow the hardcoded IP fallback on plain HTTP deployments as a fallback when reverse proxy is misconfigured.
const resolveDefaultApiBases = () => {
  const origin = getRuntimeOrigin();
  const isLocal = origin.includes("localhost") || origin.includes("127.0.0.1");
  const protocol = typeof window !== "undefined" && window.location ? window.location.protocol : "";
  const canUseHttpFallback = protocol !== "https:";

  const bases = [""];
  if (isLocal || canUseHttpFallback) {
    bases.push("http://95.81.100.250");
  }
  return bases;
};

const resolveApiBases = () => {
  const envBase = String(process.env.REACT_APP_API_BASE || "").trim();
  if (envBase) {
    return envBase
      .split(",")
      .map((item) => normalizeBase(item))
      .filter(Boolean);
  }
  return resolveDefaultApiBases()
    .map((item) => normalizeBase(item))
    .filter((item, index, arr) => arr.indexOf(item) === index);
};

const API_BASES = resolveApiBases();

const getCandidateKey = (base) => {
  const normalized = normalizeBase(base);
  if (normalized) return normalized;
  return getRuntimeOrigin() || "__relative__";
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const makeRequestId = () => `req-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;

const isRetryableNetworkError = (error) => {
  if (!error) return false;
  if (error.name === "AbortError") return true;
  const msg = String(error.message || "").toLowerCase();
  return (
    msg.includes("failed to fetch") ||
    msg.includes("networkerror") ||
    msg.includes("network error") ||
    msg.includes("load failed") ||
    msg.includes("connection reset") ||
    msg.includes("timeout") ||
    msg.includes("timed out")
  );
};

export const getAccessToken = () => localStorage.getItem("accessToken") || "";

export const setAccessToken = (token) => {
  const value = String(token || "").trim();
  if (value) {
    localStorage.setItem("accessToken", value);
  } else {
    localStorage.removeItem("accessToken");
  }
};

export const apiFetch = async (path, options = {}) => {
  const {
    timeoutMs = 45000,
    retry,
    allowBaseFallback,
    skipAuth,
    skipClientRequestId,
    _retried = false,
    ...fetchOptions
  } = options || {};

  const method = String(fetchOptions.method || "GET").toUpperCase();
  const allowRetry = typeof retry === "boolean" ? retry : (method === "GET" || method === "HEAD");
  const isSafeMethodForBaseFallback = method === "GET" || method === "HEAD";
  const shouldFallbackBetweenBases = typeof allowBaseFallback === "boolean"
    ? allowBaseFallback
    : isSafeMethodForBaseFallback;
  const timeoutValue = Number(timeoutMs);
  const shouldTimeout = Number.isFinite(timeoutValue) && timeoutValue > 0;
  const requestId = String(fetchOptions.requestId || makeRequestId());

  const headers = new Headers(fetchOptions.headers || {});
  const token = getAccessToken();
  if (!skipAuth && token && !headers.has("Authorization")) {
    headers.set("Authorization", `Bearer ${token}`);
  }
  if (!skipClientRequestId && !headers.has("X-Client-Request-Id")) {
    headers.set("X-Client-Request-Id", requestId);
  }

  const buildApiUrl = (rawPath, base) => {
    if (rawPath.startsWith("http")) return rawPath;

    const normalizedBase = normalizeBase(base);
    let normalizedPath = rawPath.startsWith("/") ? rawPath : `/${rawPath}`;

    // Prevent "/api/api/..." when both base and path already include "/api".
    const baseEndsWithApi = normalizedBase.endsWith("/api");
    if (baseEndsWithApi && normalizedPath.startsWith("/api/")) {
      normalizedPath = normalizedPath.slice(4);
    }

    return `${normalizedBase}${normalizedPath}`;
  };

  const toError = (error) => {
    if (error instanceof Error) return error;
    return new Error(String(error || "Failed to fetch"));
  };

  const candidates = path.startsWith("http")
    ? [""]
    : API_BASES;
  const seenCandidateKeys = new Set();
  const uniqueCandidates = candidates.filter((item) => {
    const key = getCandidateKey(item);
    if (seenCandidateKeys.has(key)) return false;
    seenCandidateKeys.add(key);
    return true;
  });
  const activeCandidates = shouldFallbackBetweenBases
    ? uniqueCandidates
    : uniqueCandidates.slice(0, 1);

  let lastError = null;
  for (let index = 0; index < activeCandidates.length; index += 1) {
    const base = activeCandidates[index];
    const url = buildApiUrl(path, base);
    let timedOut = false;
    const controller = !fetchOptions.signal && shouldTimeout ? new AbortController() : null;
    const timerId = controller
      ? setTimeout(() => {
        timedOut = true;
        controller.abort();
      }, Math.max(5000, timeoutValue))
      : null;

    try {
      const response = await fetch(url, {
        ...fetchOptions,
        headers,
        cache: fetchOptions.cache || "no-store",
        signal: fetchOptions.signal || controller?.signal
      });
      return response;
    } catch (error) {
      if (timedOut) {
        const timeoutError = new Error(`Request timeout after ${Math.max(5000, timeoutValue)}ms (${method} ${path})`);
        timeoutError.name = "AbortError";
        timeoutError.code = "REQUEST_TIMEOUT";
        timeoutError.method = method;
        timeoutError.path = path;
        timeoutError.url = url;
        timeoutError.timeoutMs = Math.max(5000, timeoutValue);
        timeoutError.originalError = error;
        timeoutError.requestId = requestId;
        lastError = timeoutError;
      } else {
        const networkError = toError(error);
        // Keep the error message concise but actionable in the UI.
        if (!networkError.message || networkError.message.toLowerCase() === "failed to fetch") {
          networkError.message = `${networkError.message || "Failed to fetch"} (${method} ${path})`;
        }
        networkError.code = networkError.code || "FETCH_FAILED";
        networkError.method = method;
        networkError.path = path;
        networkError.url = url;
        networkError.requestId = requestId;
        networkError.originalError = error;
        lastError = networkError;
      }
      const hasAnotherBase = shouldFallbackBetweenBases && index < activeCandidates.length - 1;
      if (!hasAnotherBase) break;
      if (!isRetryableNetworkError(lastError)) break;
    } finally {
      if (timerId) clearTimeout(timerId);
    }
  }

  if (!_retried && allowRetry && isRetryableNetworkError(lastError)) {
    await sleep(350);
    return apiFetch(path, { ...options, _retried: true });
  }
  if (lastError) throw lastError;
  const genericError = new Error("Failed to fetch");
  genericError.requestId = requestId;
  throw genericError;
};

export const apiFetchJson = async (path, options = {}) => {
  const response = await apiFetch(path, options);
  const contentType = response.headers.get("content-type") || "";
  let data = {};
  let text = "";

  if (contentType.includes("application/json")) {
    data = await response.json().catch(() => ({}));
  } else {
    text = await response.text().catch(() => "");
  }

  if (!response.ok) {
    const textMessage = text
      ? text.replace(/<[^>]*>/g, " ").replace(/\s+/g, " ").trim().slice(0, 240)
      : "";
    const error = new Error(data?.error || data?.message || textMessage || `HTTP ${response.status}`);
    error.status = response.status;
    error.data = data;
    if (text) {
      error.raw = text;
    }
    throw error;
  }
  return contentType.includes("application/json") ? data : {};
};
