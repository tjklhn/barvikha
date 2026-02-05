const API_BASE = process.env.REACT_APP_API_BASE || "http://localhost:5000";

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
  const url = path.startsWith("http") ? path : `${API_BASE}${path}`;
  return fetch(url, { ...options, headers });
};

export const apiFetchJson = async (path, options = {}) => {
  const response = await apiFetch(path, options);
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    const error = new Error(data?.error || `HTTP ${response.status}`);
    error.status = response.status;
    error.data = data;
    throw error;
  }
  return data;
};
