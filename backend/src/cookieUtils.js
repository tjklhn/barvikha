const COOKIE_ATTR_KEYS = new Set([
  "path",
  "domain",
  "expires",
  "max-age",
  "secure",
  "httponly",
  "samesite",
  "priority",
  "version",
  "comment",
  "commenturl",
  "discard",
  "port",
  "partitioned"
]);

const isTruthyToken = (value) => /^(true|1|yes|y)$/i.test(String(value || "").trim());

const parseJsonCookies = (rawText) => {
  try {
    const parsed = JSON.parse(rawText);
    if (Array.isArray(parsed)) return parsed;
    if (Array.isArray(parsed?.cookies)) return parsed.cookies;
    if (Array.isArray(parsed?.items)) return parsed.items;
  } catch (error) {
    // ignore JSON parse errors
  }
  return null;
};

const parseCookiePairsFromHeader = (raw) => {
  const text = String(raw || "").trim().replace(/^cookie:\s*/i, "");
  if (!text || !text.includes("=")) return [];
  const cookies = [];
  text.split(";").forEach((segment) => {
    const part = String(segment || "").trim();
    if (!part) return;
    const eq = part.indexOf("=");
    if (eq <= 0) return;
    const name = part.slice(0, eq).trim();
    const value = part.slice(eq + 1).trim();
    if (!name) return;
    if (COOKIE_ATTR_KEYS.has(name.toLowerCase())) return;
    cookies.push({ name, value, domain: ".kleinanzeigen.de", path: "/" });
  });
  return cookies;
};

const parseSetCookieLine = (rawLine) => {
  const line = String(rawLine || "").trim().replace(/^set-cookie:\s*/i, "");
  if (!line || !line.includes("=")) return null;
  const [first, ...rest] = line.split(";");
  const eq = first.indexOf("=");
  if (eq <= 0) return null;
  const name = first.slice(0, eq).trim();
  const value = first.slice(eq + 1).trim();
  if (!name) return null;

  const cookie = {
    name,
    value,
    domain: ".kleinanzeigen.de",
    path: "/"
  };

  for (const attrRaw of rest) {
    const attr = String(attrRaw || "").trim();
    if (!attr) continue;
    const attrEq = attr.indexOf("=");
    const key = (attrEq >= 0 ? attr.slice(0, attrEq) : attr).trim().toLowerCase();
    const val = attrEq >= 0 ? attr.slice(attrEq + 1).trim() : "";
    if (key === "domain" && val) cookie.domain = val;
    if (key === "path" && val) cookie.path = val;
    if (key === "secure") cookie.secure = true;
    if (key === "httponly") cookie.httpOnly = true;
    if (key === "samesite" && val) cookie.sameSite = val;
    if (key === "max-age" && val && /^\d+$/.test(val)) cookie.expires = Math.floor(Date.now() / 1000) + Number(val);
    if (key === "expires" && val) {
      const parsed = Date.parse(val);
      if (!Number.isNaN(parsed)) cookie.expires = Math.floor(parsed / 1000);
    }
  }

  return cookie;
};

const parseNetscapeCookieLines = (lines) => {
  const cookies = [];
  for (const rawLine of lines) {
    const line = String(rawLine || "").trim();
    if (!line || line.startsWith("#")) continue;
    const parts = line.includes("\t")
      ? line.split("\t")
      : line.split(/\s+/);
    if (parts.length < 7) continue;
    const [domainRaw, includeSubdomainsRaw, pathRaw, secureRaw, expiresRaw, name, ...valueParts] = parts;
    const value = valueParts.join("\t");
    if (!name) continue;
    const includeSubdomains = isTruthyToken(includeSubdomainsRaw) || /^true$/i.test(includeSubdomainsRaw || "");
    let domain = String(domainRaw || "").trim();
    if (includeSubdomains && domain && !domain.startsWith(".")) {
      domain = `.${domain}`;
    }
    const secure = isTruthyToken(secureRaw) || /^true$/i.test(secureRaw || "");
    const expiresNum = Number(expiresRaw);
    const expires = Number.isFinite(expiresNum) && expiresNum > 0 ? expiresNum : undefined;
    cookies.push({
      name: String(name).trim(),
      value: String(value || "").trim(),
      domain: domain || ".kleinanzeigen.de",
      path: String(pathRaw || "/").trim() || "/",
      secure,
      expires
    });
  }
  return cookies;
};

const parseCookies = (rawText) => {
  if (!rawText) return [];
  const text = String(rawText || "").trim();
  if (!text) return [];

  const jsonCookies = parseJsonCookies(text);
  if (Array.isArray(jsonCookies)) {
    return jsonCookies.filter((item) => item && item.name);
  }

  const lines = text.split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
  const hasNetscapeRows = lines.some((line) => !line.startsWith("#") && (line.split("\t").length >= 7));
  if (hasNetscapeRows) {
    const parsed = parseNetscapeCookieLines(lines);
    return parsed.filter((item) => item && item.name);
  }

  // Multi-cookie header pasted as a single line: "a=b; c=d; ..."
  if (lines.length === 1 && lines[0].includes(";") && lines[0].includes("=") && !/^set-cookie:/i.test(lines[0])) {
    const parsed = parseCookiePairsFromHeader(lines[0]);
    if (parsed.length) return parsed;
  }

  const cookies = [];
  for (const line of lines) {
    if (!line) continue;
    if (/^set-cookie:/i.test(line)) {
      const parsed = parseSetCookieLine(line);
      if (parsed) cookies.push(parsed);
      continue;
    }

    // If this line looks like a Cookie header (multiple key=value pairs), parse all pairs.
    const maybeHeader = line.includes(";") && line.includes("=") && /;\s*[^=;\s]+=/.test(line);
    if (maybeHeader) {
      cookies.push(...parseCookiePairsFromHeader(line));
      continue;
    }

    const [pair] = line.split(";");
    const eq = pair.indexOf("=");
    if (eq <= 0) continue;
    const name = pair.slice(0, eq).trim();
    const value = pair.slice(eq + 1).trim();
    if (!name) continue;
    if (COOKIE_ATTR_KEYS.has(name.toLowerCase())) continue;
    cookies.push({
      name,
      value,
      domain: ".kleinanzeigen.de",
      path: "/"
    });
  }

  // De-dupe by (name + domain + path). Keep the last seen occurrence.
  const deduped = new Map();
  for (const cookie of cookies) {
    if (!cookie?.name) continue;
    const key = `${cookie.name}|${cookie.domain || ""}|${cookie.path || ""}`;
    deduped.set(key, cookie);
  }
  return Array.from(deduped.values());
};

const normalizeDomain = (domain) => {
  const raw = String(domain || "").trim();
  if (!raw) return ".kleinanzeigen.de";
  if (raw === "kleinanzeigen.de") return ".kleinanzeigen.de";
  return raw;
};

const normalizeExpires = (value) => {
  if (value === undefined || value === null || value === "") return undefined;
  const numberValue = Number(value);
  if (!Number.isFinite(numberValue) || numberValue <= 0) return undefined;
  // Heuristic: millisecond timestamps are ~1e12+, seconds timestamps are ~1e9+.
  if (numberValue > 100000000000) {
    return Math.floor(numberValue / 1000);
  }
  return Math.floor(numberValue);
};

const inferCookieUrl = (domain) => {
  const raw = String(domain || "").trim().replace(/^\./, "");
  if (!raw) return "https://www.kleinanzeigen.de";
  return `https://${raw}`;
};

const normalizeCookie = (cookie) => {
  const name = String(cookie?.name || "").trim();
  const value = cookie?.value === undefined || cookie?.value === null ? "" : String(cookie.value);
  const hostOnly = Boolean(cookie?.hostOnly) || name.startsWith("__Host-");
  const url = cookie?.url ? String(cookie.url).trim() : "";
  const domain = normalizeDomain(cookie?.domain);
  const normalized = {
    name,
    value,
    path: String(cookie?.path || "/") || "/",
    httpOnly: Boolean(cookie?.httpOnly),
    secure: cookie?.secure !== false
  };

  const expires = normalizeExpires(cookie?.expires ?? cookie?.expirationDate ?? cookie?.expiry ?? cookie?.expiration);
  if (expires !== undefined) {
    normalized.expires = expires;
  }

  if (cookie?.sameSite) {
    normalized.sameSite = cookie.sameSite;
  }

  // Chromium enforces __Host- rules. Use host-only cookie injection via `url` for these.
  if (url) {
    normalized.url = url;
  } else if (hostOnly) {
    normalized.url = inferCookieUrl(domain);
    normalized.path = "/";
    normalized.secure = true;
  } else {
    normalized.domain = domain;
  }

  return normalized;
};

const normalizeProxyHost = (value) =>
  String(value || "")
    .trim()
    .replace(/^https?:\/\//i, "")
    .replace(/\/+$/g, "");

const buildProxyUrlInternal = (proxy, { forPuppeteer = false } = {}) => {
  if (!proxy || !proxy.host || !proxy.port) {
    return null;
  }

  let protocol = (proxy.type || "http").toLowerCase();
  if (forPuppeteer) {
    // Chromium does not support the `socks5h://` URL scheme in `--proxy-server`.
    // Use `socks5://` for Puppeteer/Chromium, DNS will still be resolved via SOCKS5 hostname type.
    if (protocol === "socks5h") protocol = "socks5";
  } else {
    // Prefer remote DNS resolution for SOCKS5 in Node clients (axios/agents) to avoid leaking DNS.
    if (protocol === "socks5") protocol = "socks5h";
  }
  const auth = proxy.username && proxy.password
    ? `${encodeURIComponent(proxy.username)}:${encodeURIComponent(proxy.password)}@`
    : "";

  const host = normalizeProxyHost(proxy.host);
  return `${protocol}://${auth}${host}:${proxy.port}`;
};

const buildProxyUrl = (proxy) => buildProxyUrlInternal(proxy, { forPuppeteer: false });

const buildPuppeteerProxyUrl = (proxy) => buildProxyUrlInternal(proxy, { forPuppeteer: true });

const buildProxyServer = (proxy) => {
  // `--proxy-server` is used by Chromium; keep it Chromium-compatible.
  const proxyUrl = buildPuppeteerProxyUrl(proxy);
  if (!proxyUrl) {
    return null;
  }
  return proxyUrl;
};

module.exports = {
  parseCookies,
  normalizeCookie,
  buildProxyUrl,
  buildPuppeteerProxyUrl,
  buildProxyServer
};
