const DEFAULT_API_BASES = [
  "http://95.81.100.250",
  ""
];

const normalizeBase = (value) => String(value || "").trim().replace(/\/+$/, "");

const resolveApiBases = () => {
  const envBase = String(process.env.REACT_APP_API_BASE || "").trim();
  if (envBase) {
    return envBase
      .split(",")
      .map((item) => normalizeBase(item))
      .filter(Boolean);
  }
  return DEFAULT_API_BASES
    .map((item) => normalizeBase(item))
    .filter((item, index, arr) => arr.indexOf(item) === index);
};

const API_BASES = resolveApiBases();
let preferredBaseIndex = 0;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

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
    _retried = false,
    ...fetchOptions
  } = options || {};

  const method = String(fetchOptions.method || "GET").toUpperCase();
  const allowRetry = typeof retry === "boolean" ? retry : (method === "GET" || method === "HEAD");
  const timeoutValue = Number(timeoutMs);
  const shouldTimeout = Number.isFinite(timeoutValue) && timeoutValue > 0;

  const headers = new Headers(fetchOptions.headers || {});
  const token = getAccessToken();
  if (token && !headers.has("Authorization")) {
    headers.set("Authorization", `Bearer ${token}`);
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

  const candidates = path.startsWith("http")
    ? [""]
    : [
      ...API_BASES.slice(preferredBaseIndex),
      ...API_BASES.slice(0, preferredBaseIndex)
    ];
  const uniqueCandidates = candidates.filter((item, index, arr) => arr.indexOf(item) === index);

  let lastError = null;
  for (let index = 0; index < uniqueCandidates.length; index += 1) {
    const base = uniqueCandidates[index];
    const url = buildApiUrl(path, base);
    const controller = !fetchOptions.signal && shouldTimeout ? new AbortController() : null;
    const timerId = controller
      ? setTimeout(() => controller.abort(), Math.max(5000, timeoutValue))
      : null;

    try {
      const response = await fetch(url, {
        ...fetchOptions,
        headers,
        cache: fetchOptions.cache || "no-store",
        signal: fetchOptions.signal || controller?.signal
      });
      const matchedIndex = API_BASES.indexOf(base);
      if (matchedIndex >= 0) {
        preferredBaseIndex = matchedIndex;
      }
      return response;
    } catch (error) {
      lastError = error;
      const hasAnotherBase = index < uniqueCandidates.length - 1;
      if (!hasAnotherBase) break;
      if (!isRetryableNetworkError(error)) break;
    } finally {
      if (timerId) clearTimeout(timerId);
    }
  }

  if (!_retried && allowRetry && isRetryableNetworkError(lastError)) {
    await sleep(350);
    return apiFetch(path, { ...options, _retried: true });
  }
  throw lastError || new Error("Failed to fetch");
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
