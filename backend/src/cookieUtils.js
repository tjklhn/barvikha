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
  const hostname = raw.replace(/^\./, "");
  if (hostname === "kleinanzeigen.de" || hostname === "www.kleinanzeigen.de") {
    return ".kleinanzeigen.de";
  }
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

const KLEINANZEIGEN_HOSTS = ["kleinanzeigen.de", "www.kleinanzeigen.de"];
const isKleinanzeigenHostname = (hostname) =>
  KLEINANZEIGEN_HOSTS.includes(String(hostname || "").trim().toLowerCase());

const KLEINANZEIGEN_DOMAIN_SUFFIX = "kleinanzeigen.de";

const extractHostname = (value) => {
  const raw = String(value || "").trim();
  if (!raw) return "";
  try {
    if (/^https?:\/\//i.test(raw)) {
      return new URL(raw).hostname.toLowerCase();
    }
  } catch (error) {
    // ignore URL parse errors
  }
  return raw
    .replace(/^https?:\/\//i, "")
    .replace(/^\./, "")
    .split("/")[0]
    .toLowerCase();
};

const isKleinanzeigenCookie = (cookie) => {
  const urlHost = cookie?.url ? extractHostname(cookie.url) : "";
  if (urlHost && urlHost.endsWith(KLEINANZEIGEN_DOMAIN_SUFFIX)) return true;
  const domainHost = cookie?.domain ? extractHostname(cookie.domain) : "";
  if (domainHost && domainHost.endsWith(KLEINANZEIGEN_DOMAIN_SUFFIX)) return true;
  return false;
};

// Cookie exports often include many unrelated third-party cookies. We only need Kleinanzeigen cookies for auth/actions.
// Keep cookies without explicit scope (rare) to avoid dropping pasted Cookie header pairs.
const filterKleinanzeigenCookies = (cookies, { includeUnknown = true } = {}) => {
  const input = Array.isArray(cookies) ? cookies : [];
  return input.filter((cookie) => {
    if (!cookie?.name) return false;
    const hasScope = Boolean(cookie?.url || cookie?.domain);
    if (!hasScope) return includeUnknown;
    return isKleinanzeigenCookie(cookie);
  });
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

const normalizeCookies = (rawCookies, { onlyKleinanzeigen = true } = {}) => {
  const input = Array.isArray(rawCookies) ? rawCookies : [];
  const scoped = onlyKleinanzeigen ? filterKleinanzeigenCookies(input) : input;
  const normalized = scoped.map(normalizeCookie).filter((cookie) => cookie?.name);

  // Many cookie exports contain host-only cookies for either `kleinanzeigen.de` or `www.kleinanzeigen.de`.
  // The app navigates across both; to avoid false "redirect to login" we duplicate host-only cookies across
  // both hostnames. (Domain cookies already cover subdomains.)
  const expanded = [];
  for (const cookie of normalized) {
    if (!cookie?.name) continue;
    if (!cookie.url) {
      expanded.push(cookie);
      continue;
    }

    let hostname = "";
    try {
      hostname = new URL(cookie.url).hostname.toLowerCase();
    } catch (error) {
      hostname = "";
    }

    if (!isKleinanzeigenHostname(hostname)) {
      expanded.push(cookie);
      continue;
    }

    for (const targetHost of KLEINANZEIGEN_HOSTS) {
      expanded.push({
        ...cookie,
        url: `https://${targetHost}`
      });
    }
  }

  // De-dupe by (name + url/domain + path). Keep the last occurrence.
  const deduped = new Map();
  for (const cookie of expanded) {
    if (!cookie?.name) continue;
    const scope = cookie.url ? `url:${cookie.url}` : `domain:${cookie.domain || ""}`;
    const key = `${cookie.name}|${scope}|${cookie.path || ""}`;
    deduped.set(key, cookie);
  }
  return Array.from(deduped.values());
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

const normalizeHostname = (value) => String(value || "").trim().toLowerCase();

const getCookieHost = (cookie) => {
  if (!cookie) return "";
  if (cookie.url) {
    try {
      return new URL(String(cookie.url)).hostname.toLowerCase();
    } catch (error) {
      return normalizeHostname(cookie.url);
    }
  }
  if (cookie.domain) {
    return normalizeHostname(cookie.domain).replace(/^\./, "");
  }
  return "";
};

const domainMatchesHost = (cookieDomain, host) => {
  const normalizedHost = normalizeHostname(host);
  const normalizedDomain = normalizeHostname(cookieDomain).replace(/^\./, "");
  if (!normalizedHost || !normalizedDomain) return false;
  if (normalizedHost === normalizedDomain) return true;
  return normalizedHost.endsWith(`.${normalizedDomain}`);
};

const pathMatchesRequest = (cookiePath, requestPath) => {
  const normalizedCookiePath = String(cookiePath || "/") || "/";
  const normalizedRequestPath = String(requestPath || "/") || "/";
  if (normalizedCookiePath === "/") return true;
  if (!normalizedRequestPath.startsWith(normalizedCookiePath)) return false;
  if (normalizedCookiePath.endsWith("/")) return true;
  const nextChar = normalizedRequestPath.charAt(normalizedCookiePath.length);
  return nextChar === "" || nextChar === "/";
};

// Build a Cookie header for a specific request URL.
// Important: avoid picking the wrong value when the cookie jar contains multiple cookies with the same name
// (e.g. host-only cookies for both kleinanzeigen.de and www.kleinanzeigen.de).
const buildCookieHeaderForUrl = (cookies, targetUrl) => {
  const list = Array.isArray(cookies) ? cookies : [];
  if (!list.length) return "";

  let parsedUrl;
  try {
    parsedUrl = new URL(String(targetUrl || ""));
  } catch (error) {
    parsedUrl = null;
  }
  const hostname = normalizeHostname(parsedUrl?.hostname);
  const protocol = normalizeHostname(parsedUrl?.protocol);
  const requestPath = parsedUrl?.pathname || "/";
  const isHttps = protocol === "https:";
  const nowSeconds = Math.floor(Date.now() / 1000);

  // Pick the most specific cookie per name (host-only > domain, longest path, most specific domain).
  const bestByName = new Map();
  const entries = list
    .map((cookie, index) => ({ cookie, index }))
    .filter(({ cookie }) => cookie && cookie.name);

  for (const { cookie, index } of entries) {
    const name = String(cookie.name || "").trim();
    if (!name) continue;
    if (cookie.value === undefined || cookie.value === null) continue;
    const value = String(cookie.value);
    if (!value) continue;

    const expires = cookie.expires !== undefined && cookie.expires !== null ? Number(cookie.expires) : null;
    if (Number.isFinite(expires) && expires > 0 && expires < nowSeconds) continue;
    if (cookie.secure === true && !isHttps) continue;

    const cookiePath = String(cookie.path || "/") || "/";
    if (!pathMatchesRequest(cookiePath, requestPath)) continue;

    let matchType = 0;
    let domainSpecificity = 0;
    if (cookie.url) {
      if (!hostname) continue;
      const cookieHost = getCookieHost(cookie);
      if (!cookieHost) continue;
      if (cookieHost !== hostname) continue;
      matchType = 2;
      domainSpecificity = cookieHost.length;
    } else if (cookie.domain) {
      if (!hostname) continue;
      if (!domainMatchesHost(cookie.domain, hostname)) continue;
      matchType = 1;
      domainSpecificity = String(cookie.domain || "").trim().replace(/^\./, "").length;
    } else {
      // Cookie without explicit scope (rare); include it for compatibility.
      matchType = 0;
      domainSpecificity = 0;
    }

    const score = {
      matchType,
      pathLength: cookiePath.length,
      domainSpecificity,
      index
    };

    const existing = bestByName.get(name);
    if (!existing) {
      bestByName.set(name, { name, value, score });
      continue;
    }

    const better = (
      score.matchType > existing.score.matchType
      || (score.matchType === existing.score.matchType && score.pathLength > existing.score.pathLength)
      || (
        score.matchType === existing.score.matchType
        && score.pathLength === existing.score.pathLength
        && score.domainSpecificity > existing.score.domainSpecificity
      )
      || (
        score.matchType === existing.score.matchType
        && score.pathLength === existing.score.pathLength
        && score.domainSpecificity === existing.score.domainSpecificity
        && score.index > existing.score.index
      )
    );

    if (better) {
      bestByName.set(name, { name, value, score });
    }
  }

  return Array.from(bestByName.values())
    .map((entry) => `${entry.name}=${entry.value}`)
    .join("; ");
};

module.exports = {
  parseCookies,
  normalizeCookies,
  normalizeCookie,
  isKleinanzeigenCookie,
  filterKleinanzeigenCookies,
  buildCookieHeaderForUrl,
  buildProxyUrl,
  buildPuppeteerProxyUrl,
  buildProxyServer
};
