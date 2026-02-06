const API_BASE = process.env.REACT_APP_API_BASE || "";

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
  const headers = new Headers(options.headers || {});
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
  return fetch(url, { ...options, headers });
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
