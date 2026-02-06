const API_BASE = process.env.REACT_APP_API_BASE || "";

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
    _retried = false,
    ...fetchOptions
  } = options || {};

  const headers = new Headers(fetchOptions.headers || {});
  const token = getAccessToken();
  if (token && !headers.has("Authorization")) {
    headers.set("Authorization", `Bearer ${token}`);
  }

  const buildApiUrl = (rawPath) => {
    if (rawPath.startsWith("http")) return rawPath;

    const normalizedBase = String(API_BASE || "").trim().replace(/\/+$/, "");
    let normalizedPath = rawPath.startsWith("/") ? rawPath : `/${rawPath}`;

    // Prevent "/api/api/..." when both base and path already include "/api".
    const baseEndsWithApi = normalizedBase.endsWith("/api");
    if (baseEndsWithApi && normalizedPath.startsWith("/api/")) {
      normalizedPath = normalizedPath.slice(4);
    }

    return `${normalizedBase}${normalizedPath}`;
  };

  const url = buildApiUrl(path);
  const controller = !fetchOptions.signal ? new AbortController() : null;
  const timerId = controller
    ? setTimeout(() => controller.abort(), Math.max(5000, Number(timeoutMs) || 45000))
    : null;

  try {
    return await fetch(url, {
      ...fetchOptions,
      headers,
      cache: fetchOptions.cache || "no-store",
      signal: fetchOptions.signal || controller?.signal
    });
  } catch (error) {
    if (!_retried && isRetryableNetworkError(error)) {
      await sleep(350);
      return apiFetch(path, { ...options, _retried: true });
    }
    throw error;
  } finally {
    if (timerId) clearTimeout(timerId);
  }
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
