const parseCookies = (rawText) => {
  if (!rawText) {
    return [];
  }

  try {
    const parsed = JSON.parse(rawText);
    if (Array.isArray(parsed)) {
      return parsed;
    }
  } catch (error) {
    // fall through to text parser
  }

  return rawText
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      const [pair] = line.split(";");
      const [name, ...valueParts] = pair.split("=");
      if (!name) return null;
      return {
        name: name.trim(),
        value: valueParts.join("=").trim(),
        domain: ".kleinanzeigen.de",
        path: "/"
      };
    })
    .filter(Boolean);
};

const normalizeDomain = (domain) => {
  const raw = domain || ".kleinanzeigen.de";
  if (raw === "www.kleinanzeigen.de" || raw === "kleinanzeigen.de") {
    return ".kleinanzeigen.de";
  }
  return raw;
};

const normalizeCookie = (cookie) => ({
  name: cookie.name,
  value: cookie.value,
  domain: normalizeDomain(cookie.domain),
  path: cookie.path || "/",
  httpOnly: Boolean(cookie.httpOnly),
  secure: cookie.secure !== false,
  expires: cookie.expires
});

const buildProxyUrl = (proxy) => {
  if (!proxy || !proxy.host || !proxy.port) {
    return null;
  }

  let protocol = (proxy.type || "http").toLowerCase();
  // Prefer remote DNS resolution for SOCKS5 to avoid leaking DNS via server.
  // This also helps ensure outbound traffic is routed fully through the proxy.
  if (protocol === "socks5") protocol = "socks5h";
  const auth = proxy.username && proxy.password
    ? `${encodeURIComponent(proxy.username)}:${encodeURIComponent(proxy.password)}@`
    : "";

  return `${protocol}://${auth}${proxy.host}:${proxy.port}`;
};

const buildProxyServer = (proxy) => {
  const proxyUrl = buildProxyUrl(proxy);
  if (!proxyUrl) {
    return null;
  }
  return proxyUrl;
};

module.exports = {
  parseCookies,
  normalizeCookie,
  buildProxyUrl,
  buildProxyServer
};
