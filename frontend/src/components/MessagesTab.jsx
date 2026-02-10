import React, { useState, useEffect, useRef } from "react";
import { apiFetchJson, getAccessToken } from "../api";
import { MessageIcon } from "./Icons";

const SEEN_STORAGE_KEY = "kl_messages_seen_v1";
const THREAD_CACHE_TTL_MS = 2 * 60 * 1000;

const detectMobileView = () => {
  if (typeof window === "undefined") return false;
  const viewportWidth = window.innerWidth || document.documentElement.clientWidth || 0;
  const screenWidth = typeof window.screen?.width === "number" ? window.screen.width : viewportWidth;
  const effectiveWidth = Math.min(viewportWidth || screenWidth, screenWidth || viewportWidth);
  const mobileUa = /Android|webOS|iPhone|iPad|iPod|Opera Mini|IEMobile/i.test(
    (typeof navigator !== "undefined" && navigator.userAgent) || ""
  );
  const hasCoarsePointer = typeof window.matchMedia === "function"
    ? window.matchMedia("(pointer: coarse)").matches
    : false;
  const noHover = typeof window.matchMedia === "function"
    ? window.matchMedia("(hover: none)").matches
    : false;
  const touchPoints = typeof navigator !== "undefined" ? Number(navigator.maxTouchPoints || 0) : 0;
  const touchLikeDevice = hasCoarsePointer || noHover || touchPoints > 1;
  return effectiveWidth <= 768 || (touchLikeDevice && effectiveWidth <= 1024) || (mobileUa && effectiveWidth <= 1180);
};

const normalizePreviewImageUrl = (value) => {
  const raw = String(value || "").trim();
  if (!raw) return "";
  if (/^data:/i.test(raw)) return "";
  const normalizeRuleForKleinanzeigen = (parsed) => {
    const host = String(parsed.hostname || "").toLowerCase();
    const isProdAdsImage = host === "img.kleinanzeigen.de"
      && parsed.pathname.startsWith("/api/v1/prod-ads/images/");
    if (!isProdAdsImage) return parsed;
    const ruleParam = String(parsed.searchParams.get("rule") || "");
    const malformedRule = /(imageid|\$\{.*\}|\$_\{.*\})/i.test(ruleParam);
    const validRule = /^\$_[a-z0-9_.-]+$/i.test(ruleParam);
    if (!validRule || malformedRule) {
      parsed.searchParams.set("rule", "$_57.JPG");
    }
    return parsed;
  };
  if (/^(https?:)?\/\//i.test(raw)) {
    try {
      const parsed = normalizeRuleForKleinanzeigen(new URL(raw.startsWith("//") ? `https:${raw}` : raw));
      return parsed.href;
    } catch {
      return raw.startsWith("//") ? `https:${raw}` : raw;
    }
  }
  if (/^img\.kleinanzeigen\.de/i.test(raw)) return normalizePreviewImageUrl(`https://${raw}`);
  if (/^\/api\/v1\/prod-ads\/images\//i.test(raw)) return normalizePreviewImageUrl(`https://img.kleinanzeigen.de${raw}`);
  try {
    const parsed = normalizeRuleForKleinanzeigen(new URL(raw, "https://www.kleinanzeigen.de"));
    return parsed.href;
  } catch {
    return "";
  }
};

const toMessageImageSrc = (value, accountId) => {
  const normalized = normalizePreviewImageUrl(value);
  if (!normalized) return "";
  const accountToken = accountId !== undefined && accountId !== null && String(accountId).trim()
    ? String(accountId).trim()
    : "";
  const accessToken = getAccessToken();
  if (normalized.startsWith("/api/messages/image?")) {
    let src = normalized;
    if (accountToken && !/[?&]accountId=/i.test(src)) {
      const separator = src.includes("?") ? "&" : "?";
      src = `${src}${separator}accountId=${encodeURIComponent(accountToken)}`;
    }
    if (accessToken && !/[?&](accessToken|token)=/i.test(src)) {
      const separator = src.includes("?") ? "&" : "?";
      src = `${src}${separator}token=${encodeURIComponent(accessToken)}`;
    }
    return src;
  }
  if (/^https?:\/\//i.test(normalized)) {
    if (!accountToken) return normalized;
    const params = new URLSearchParams();
    params.set("url", normalized);
    params.set("accountId", accountToken);
    if (accessToken) {
      params.set("token", accessToken);
    }
    return `/api/messages/image?${params.toString()}`;
  }
  return normalized;
};

const getRawMessagePreviewImage = (message) => {
  if (!message || typeof message !== "object") return "";
  const candidates = [
    message.adImage,
    message.adImageUrl,
    message.adImageURL,
    message.imageUrl,
    message.image,
    message.thumbnailUrl,
    message.thumbnail,
    message.previewImage,
    message.ad?.image,
    message.ad?.imageUrl,
    message.ad?.thumbnailUrl
  ];
  for (const candidate of candidates) {
    const normalized = normalizePreviewImageUrl(candidate);
    if (normalized) return normalized;
  }
  return "";
};

const getMessagePreviewImage = (message) =>
  toMessageImageSrc(getRawMessagePreviewImage(message), message?.accountId);

const hasMessagePreviewImage = (message) => Boolean(getRawMessagePreviewImage(message));

const isPaymentAndShippingMessage = (message) => {
  if (!message || typeof message !== "object") return false;
  const type = String(message.type || "").toUpperCase();
  return type === "PAYMENT_AND_SHIPPING_MESSAGE" || Boolean(message.paymentAndShippingMessageType);
};

const formatEuroFromCents = (value) => {
  const cents = Number(value);
  if (!Number.isFinite(cents)) return "";
  return (cents / 100).toLocaleString("de-DE", { style: "currency", currency: "EUR" });
};

const pickOfferField = (message, key) => {
  if (!message || typeof message !== "object") return null;
  const direct = message[key];
  if (direct !== undefined && direct !== null) return direct;
  const actions = Array.isArray(message.actions) ? message.actions : [];
  const withKey = actions.find((action) => action && action[key] !== undefined && action[key] !== null);
  return withKey ? withKey[key] : null;
};

const getOfferActionText = (message, actionType, fallback = "") => {
  const actions = Array.isArray(message?.actions) ? message.actions : [];
  const action = actions.find((item) => item?.actionType === actionType);
  return action?.ctaText || fallback;
};

const extractAttachmentUrls = (message) => {
  const buckets = [
    message?.attachments,
    message?.images,
    message?.imageUrls,
    message?.media,
    message?.pictures
  ].filter((value) => Array.isArray(value));
  const urls = [];
  for (const bucket of buckets) {
    for (const item of bucket) {
      if (!item) continue;
      if (typeof item === "string") {
        urls.push(item);
        continue;
      }
      if (typeof item === "object") {
        // Kleinanzeigen message attachments often need the gateway URL (auth required),
        // while the api.kleinanzeigen.de URL can return 401.
        const candidate = item.gatewayUrl
          || item.url
          || item.href
          || item.src
          || item.imageUrl
          || item.thumbnailUrl;
        if (candidate) urls.push(candidate);
      }
    }
  }
  return urls.filter(Boolean);
};

const getConversationKey = (message) => {
  if (!message || typeof message !== "object") return "";
  const accountId = message.accountId != null ? String(message.accountId) : "";
  const convo = message.conversationId || message.conversationUrl || message.id || "";
  const convoId = convo != null ? String(convo) : "";
  if (!accountId || !convoId) return "";
  return `${accountId}|${convoId}`;
};

const getConversationFingerprint = (message) => {
  if (!message || typeof message !== "object") return "";
  const date = String(message.date || "").trim();
  const time = String(message.time || "").trim();
  const text = String(message.message || "").trim();
  return `${date}|${time}|${text}`;
};

const loadSeenMap = () => {
  if (typeof window === "undefined" || !window.localStorage) return {};
  try {
    const raw = window.localStorage.getItem(SEEN_STORAGE_KEY);
    if (!raw) return {};
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch {
    return {};
  }
};

const saveSeenMap = (next) => {
  if (typeof window === "undefined" || !window.localStorage) return;
  try {
    window.localStorage.setItem(SEEN_STORAGE_KEY, JSON.stringify(next || {}));
  } catch {
    // ignore quota / serialization issues
  }
};

const MessagesTab = () => {
  const [messages, setMessages] = useState([]);
  const [selectedMessage, setSelectedMessage] = useState(null);
  const [replyText, setReplyText] = useState("");
  const [replyImages, setReplyImages] = useState([]);
  const [sending, setSending] = useState(false);
  const [loading, setLoading] = useState(false);
  const [loadingThread, setLoadingThread] = useState(false);
  const [error, setError] = useState(null);
  const [isMobileView, setIsMobileView] = useState(() => detectMobileView());
  const [translationByMessageId, setTranslationByMessageId] = useState({});
  const [declineConfirmOpen, setDeclineConfirmOpen] = useState(false);
  const [decliningOffer, setDecliningOffer] = useState(false);
  const chatScrollRef = useRef(null);
  const fileInputRef = useRef(null);
  const previewHydrationInFlight = useRef(new Set());
  const messagesRefreshInFlight = useRef(false);
  const translateInFlight = useRef(new Set());
  const threadCacheRef = useRef(new Map());

  const clearComposerImages = () => {
    setReplyImages((prev) => {
      const existing = Array.isArray(prev) ? prev : [];
      existing.forEach((item) => {
        if (item?.previewUrl) {
          URL.revokeObjectURL(item.previewUrl);
        }
      });
      return [];
    });
  };

  const getThreadCacheKeyForMessage = (message) => {
    if (!message || typeof message !== "object") return "";
    const accountId = message.accountId != null ? String(message.accountId) : "";
    const conversationId = message.conversationId != null ? String(message.conversationId) : "";
    const conversationUrl = message.conversationUrl != null ? String(message.conversationUrl) : "";
    if (!accountId) return "";
    if (conversationId) return `${accountId}|cid:${conversationId}`;
    if (conversationUrl) return `${accountId}|url:${conversationUrl}`;
    return "";
  };

  const readThreadCacheEntry = (message) => {
    const key = getThreadCacheKeyForMessage(message);
    if (!key) return null;
    const entry = threadCacheRef.current.get(key);
    if (!entry || typeof entry !== "object") return null;
    if (!entry.expiresAt || entry.expiresAt < Date.now()) {
      threadCacheRef.current.delete(key);
      return null;
    }
    return entry;
  };

  const writeThreadCacheEntry = (message, payload) => {
    const key = getThreadCacheKeyForMessage(message);
    if (!key || !payload || typeof payload !== "object") return;
    threadCacheRef.current.set(key, {
      expiresAt: Date.now() + THREAD_CACHE_TTL_MS,
      payload: {
        adTitle: payload.adTitle || "",
        adImage: payload.adImage || "",
        sender: payload.sender || "",
        messages: Array.isArray(payload.messages) ? payload.messages : [],
        conversationId: payload.conversationId || message?.conversationId || "",
        conversationUrl: payload.conversationUrl || message?.conversationUrl || ""
      }
    });
  };

  useEffect(() => {
    loadMessages();
  }, []);

  useEffect(() => {
    const intervalId = window.setInterval(() => {
      // Avoid background polling when the tab is hidden (mobile data / lower load).
      if (typeof document !== "undefined" && document.visibilityState !== "visible") return;
      loadMessages({ silent: true });
    }, 3 * 60 * 1000);

    return () => window.clearInterval(intervalId);
  }, []);

  useEffect(() => {
    if (!selectedMessage) return;
    const container = chatScrollRef.current;
    if (!container) return;
    container.scrollTo({ top: container.scrollHeight, behavior: "smooth" });
  }, [selectedMessage?.id, selectedMessage?.messages]);

  useEffect(() => {
    // Reset composer state when switching conversations.
    setReplyText("");
    clearComposerImages();
    setDeclineConfirmOpen(false);
    setDecliningOffer(false);
  }, [selectedMessage?.id]);

  useEffect(() => {
    const handleResize = () => {
      setIsMobileView(detectMobileView());
    };

    window.addEventListener("resize", handleResize);
    return () => window.removeEventListener("resize", handleResize);
  }, []);

  const hydrateMissingPreviewImages = async (list) => {
    if (!Array.isArray(list) || !list.length) return;

    const candidates = list
      .filter((item) => item && !hasMessagePreviewImage(item))
      .slice(0, 10);

    for (const item of candidates) {
      const key = String(item.conversationId || item.conversationUrl || item.id || "");
      if (!key || previewHydrationInFlight.current.has(key)) continue;
      previewHydrationInFlight.current.add(key);

      try {
        const params = new URLSearchParams();
        if (item.accountId != null) params.set("accountId", String(item.accountId));
        if (item.conversationId) params.set("conversationId", String(item.conversationId));
        if (item.conversationUrl) params.set("conversationUrl", String(item.conversationUrl));
        if (!item.conversationId && !item.conversationUrl) {
          if (item.sender) params.set("participant", String(item.sender));
          if (item.adTitle) params.set("adTitle", String(item.adTitle));
        }
        if (!params.get("accountId")) continue;

        const data = await apiFetchJson(`/api/messages/thread?${params.toString()}`);
        const adImage = normalizePreviewImageUrl(data?.adImage);
        if (!adImage) continue;

        setMessages((prev) => prev.map((msg) => {
          const sameConversation = (
            (item.conversationId && msg.conversationId && String(item.conversationId) === String(msg.conversationId))
            || (item.conversationUrl && msg.conversationUrl && String(item.conversationUrl) === String(msg.conversationUrl))
            || String(msg.id) === String(item.id)
          );
          if (!sameConversation) return msg;
          return {
            ...msg,
            adImage,
            adTitle: data?.adTitle || msg.adTitle,
            conversationId: data?.conversationId || msg.conversationId,
            conversationUrl: data?.conversationUrl || msg.conversationUrl
          };
        }));
      } catch (error) {
        console.error("Не удалось догрузить превью диалога:", error);
      } finally {
        previewHydrationInFlight.current.delete(key);
      }
    }
  };

  const rememberConversationSeen = (message) => {
    const key = getConversationKey(message);
    if (!key) return;
    const fingerprint = getConversationFingerprint(message);
    if (!fingerprint) return;

    const seen = loadSeenMap();
    seen[key] = { fingerprint, seenAt: Date.now() };
    saveSeenMap(seen);

    // Immediately update UI so the badge doesn't come back on refresh.
    setMessages((prev) => prev.map((msg) => {
      const msgKey = getConversationKey(msg);
      if (!msgKey || msgKey !== key) return msg;
      return { ...msg, unread: false };
    }));
  };

  const loadMessages = async (opts = {}) => {
    const silent = Boolean(opts && typeof opts === "object" && opts.silent);
    if (messagesRefreshInFlight.current) return;
    messagesRefreshInFlight.current = true;

    if (!silent) {
      setLoading(true);
      setError(null);
    }
    try {
      // Request only a limited number of conversations per account to keep the endpoint fast/stable.
      const params = new URLSearchParams();
      params.set("limit", "30");
      const data = await apiFetchJson(`/api/messages?${params.toString()}`, { timeoutMs: 120000 });
      const list = Array.isArray(data) ? data : [];
      const seen = new Set();
      const deduped = [];
      for (const item of list) {
        const key = item.conversationId
          || item.conversationUrl
          || item.id
          || `${item.accountId || ""}|${item.sender || ""}|${item.adTitle || ""}|${item.message || ""}|${item.time || ""}`;
        if (seen.has(key)) continue;
        seen.add(key);
        deduped.push(item);
      }
      const seenMap = loadSeenMap();
      const merged = deduped.map((item) => {
        const key = getConversationKey(item);
        if (!key) return item;
        const entry = seenMap[key];
        if (!entry || typeof entry !== "object") return item;
        const currentFingerprint = getConversationFingerprint(item);
        if (entry.fingerprint && entry.fingerprint === currentFingerprint) {
          return { ...item, unread: false };
        }
        return item;
      });

      setMessages(merged);
      hydrateMissingPreviewImages(merged);
    } catch (err) {
      console.error("Ошибка загрузки сообщений:", err);
      if (!silent) {
        const details = [];
        if (err?.requestId) details.push(`id=${err.requestId}`);
        if (err?.status) details.push(`HTTP ${err.status}`);
        const suffix = details.length ? ` (${details.join(", ")})` : "";
        setError((err?.message || "Не удалось загрузить сообщения") + suffix);
      }
    } finally {
      if (!silent) setLoading(false);
      messagesRefreshInFlight.current = false;
    }
  };

  // NOTE: Backend does not guarantee a "mark read" API (and Kleinanzeigen state may lag).
  // We persist "seen" locally so a refresh won't resurrect the "new" badge.
  const markAsRead = (message) => rememberConversationSeen(message);

  const handleReplyImageSelect = (event) => {
    const files = Array.from(event?.target?.files || []);
    if (event?.target) event.target.value = "";
    if (!files.length) return;

    const nextItems = files
      .filter((file) => String(file?.type || "").toLowerCase().startsWith("image/"))
      .slice(0, 10)
      .map((file) => ({
        id: `img-${Date.now()}-${Math.random().toString(16).slice(2)}`,
        file,
        previewUrl: URL.createObjectURL(file)
      }));

    if (!nextItems.length) return;
    setReplyImages((prev) => {
      const existing = Array.isArray(prev) ? prev : [];
      return [...existing, ...nextItems].slice(0, 10);
    });
  };

  const removeReplyImage = (id) => {
    setReplyImages((prev) => {
      const existing = Array.isArray(prev) ? prev : [];
      const target = existing.find((item) => item?.id === id);
      if (target?.previewUrl) {
        URL.revokeObjectURL(target.previewUrl);
      }
      return existing.filter((item) => item?.id !== id);
    });
  };

  const formatRequestError = (error, fallbackText) => {
    const baseMessage = String(error?.message || fallbackText || "Ошибка запроса").trim();
    const data = error?.data && typeof error.data === "object" ? error.data : {};
    const debugId = data?.debugId || error?.debugId || "";
    const requestId = data?.requestId || error?.requestId || "";
    const parts = [];

    if (debugId) {
      parts.push(`debugId: ${debugId}`);
    } else if (requestId) {
      parts.push(`requestId: ${requestId}`);
    }

    if (data?.code === "MESSAGE_ACTION_IN_PROGRESS") {
      if (data?.activeRoute) parts.push(`activeRoute=${data.activeRoute}`);
      if (data?.activeDebugId) parts.push(`activeDebugId=${data.activeDebugId}`);
      if (data?.activeSinceMs != null) {
        const seconds = Math.max(0, Math.round(Number(data.activeSinceMs || 0) / 1000));
        parts.push(`activeFor=${seconds}s`);
      }
    }

    const url = String(error?.url || "").trim();
    const code = String(error?.code || "").trim();
    const shouldShowUrl = Boolean(url) && (
      /failed to fetch/i.test(baseMessage)
      || code === "FETCH_FAILED"
      || code === "REQUEST_TIMEOUT"
    );
    if (shouldShowUrl) {
      parts.push(`url=${url}`);
    }

    const suffix = parts.length ? ` (${parts.join(", ")})` : "";
    return baseMessage + suffix;
  };

  const confirmDeclineOffer = async () => {
    if (!selectedMessage || decliningOffer) return;
    if (!selectedMessage.conversationId && !selectedMessage.conversationUrl) {
      alert("Не найден идентификатор диалога.");
      return;
    }
    setDecliningOffer(true);
    try {
      const result = await apiFetchJson("/api/messages/offer/decline", {
        method: "POST",
        timeoutMs: 240000,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          accountId: selectedMessage.accountId,
          conversationId: selectedMessage.conversationId,
          conversationUrl: selectedMessage.conversationUrl
        })
      });

      const serverMessages = Array.isArray(result.messages) ? result.messages : [];
      if (serverMessages.length) {
        const lastMessage = serverMessages[serverMessages.length - 1];
        const nextConversationId = result.conversationId || selectedMessage.conversationId;
        const nextConversationUrl = result.conversationUrl || selectedMessage.conversationUrl;
        setMessages((prev) => prev.map((msg) => (
          msg.id === selectedMessage.id
            ? {
              ...msg,
              message: lastMessage?.text || msg.message,
              date: lastMessage?.date || msg.date,
              time: lastMessage?.time || msg.time,
              conversationId: nextConversationId || msg.conversationId,
              conversationUrl: nextConversationUrl || msg.conversationUrl,
              unread: false
            }
            : msg
        )));
        setSelectedMessage((prev) => ({
          ...prev,
          messages: serverMessages,
          conversationId: nextConversationId || prev.conversationId,
          conversationUrl: nextConversationUrl || prev.conversationUrl
        }));
        writeThreadCacheEntry(selectedMessage, {
          adTitle: selectedMessage.adTitle,
          adImage: selectedMessage.adImage,
          sender: selectedMessage.sender,
          messages: serverMessages,
          conversationId: nextConversationId,
          conversationUrl: nextConversationUrl
        });
      } else {
        // Fallback: refresh thread when backend doesn't return messages.
        fetchThread(selectedMessage, { force: true });
      }
      setDeclineConfirmOpen(false);
    } catch (error) {
      console.error("Ошибка отклонения заявки:", error, {
        status: error?.status,
        code: error?.code,
        data: error?.data || null
      });
      alert(formatRequestError(error, "Не удалось отклонить заявку"));
    } finally {
      setDecliningOffer(false);
    }
  };

  const sendReply = async () => {
    if (!selectedMessage) return;
    if (!selectedMessage.conversationId && !selectedMessage.conversationUrl
      && !selectedMessage.sender && !selectedMessage.adTitle) {
      alert("Не найден идентификатор диалога.");
      return;
    }

    const textToSend = replyText.trim();
    const imagesToSend = Array.isArray(replyImages) ? replyImages : [];
    if (imagesToSend.length && !selectedMessage.conversationId && !selectedMessage.conversationUrl) {
      alert("Для отправки фото нужен conversationId диалога. Откройте диалог и попробуйте снова.");
      return;
    }
    if (!textToSend && imagesToSend.length === 0) return;
    const now = new Date();
    const optimisticId = `local-${now.getTime()}-${Math.random().toString(16).slice(2)}`;
    const optimisticAttachments = imagesToSend.map((item) => ({
      url: item?.previewUrl || "",
      name: item?.file?.name || "",
      type: item?.file?.type || "",
      local: true
    })).filter((item) => item?.url);
    const previewText = textToSend || (optimisticAttachments.length ? "Фото" : "");
    const optimisticMessage = {
      id: optimisticId,
      text: textToSend,
      date: now.toISOString().slice(0, 10),
      time: now.toTimeString().slice(0, 5),
      direction: "outgoing",
      sender: "Вы",
      pending: true,
      ...(optimisticAttachments.length ? { attachments: optimisticAttachments } : {})
    };

    // Optimistic UI: show message immediately in the thread and conversation list.
    setReplyText("");
    setReplyImages([]);
    setSelectedMessage((prev) => {
      if (!prev) return prev;
      const existing = Array.isArray(prev.messages) && prev.messages.length
        ? prev.messages
        : prev.message
          ? [{
            id: prev.id,
            text: prev.message,
            sender: prev.sender,
            date: prev.date,
            time: prev.time,
            direction: "incoming"
          }]
          : [];
      return {
        ...prev,
        messages: [...existing, optimisticMessage]
      };
    });
    setMessages((prev) => prev.map((msg) => (
      msg.id === selectedMessage.id
        ? {
          ...msg,
          message: previewText,
          date: optimisticMessage.date,
          time: optimisticMessage.time,
          unread: false
        }
        : msg
    )));

    setSending(true);
    try {
      const result = optimisticAttachments.length
        ? await (() => {
          const form = new FormData();
          form.append("accountId", String(selectedMessage.accountId));
          if (selectedMessage.conversationId) form.append("conversationId", String(selectedMessage.conversationId));
          if (selectedMessage.conversationUrl) form.append("conversationUrl", String(selectedMessage.conversationUrl));
          if (textToSend) form.append("text", textToSend);
          imagesToSend.forEach((item) => {
            if (!item?.file) return;
            form.append("images", item.file, item.file.name || "image.jpg");
          });
          return apiFetchJson("/api/messages/send-media", {
            method: "POST",
            timeoutMs: 240000,
            body: form
          });
        })()
        : await apiFetchJson("/api/messages/send", {
          method: "POST",
          timeoutMs: 180000,
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            accountId: selectedMessage.accountId,
            conversationId: selectedMessage.conversationId,
            conversationUrl: selectedMessage.conversationUrl,
            participant: selectedMessage.sender,
            adTitle: selectedMessage.adTitle,
            text: textToSend
          })
        });
      if (result.success) {
        const resolvedConversationId = result.conversationId || selectedMessage.conversationId;
        const resolvedConversationUrl = result.conversationUrl || selectedMessage.conversationUrl;
        const serverMessages = Array.isArray(result.messages) ? result.messages : [];
        const hasSameOutgoing = optimisticAttachments.length
          ? serverMessages.some((m) => (
            String(m?.direction || "").toLowerCase() === "outgoing"
            && (
              (Array.isArray(m?.attachments) && m.attachments.length > 0)
              || (textToSend && String(m?.text || "") === textToSend)
            )
          ))
          : serverMessages.some((m) => (
            String(m?.direction || "").toLowerCase() === "outgoing"
            && String(m?.text || "") === textToSend
          ));
        const nextMessages = serverMessages.length
          ? (hasSameOutgoing ? serverMessages : [...serverMessages, { ...optimisticMessage, pending: false }])
          : null;

        if (nextMessages) {
          const lastMessage = nextMessages[nextMessages.length - 1];
          setMessages((prev) => prev.map((msg) => (
            msg.id === selectedMessage.id
              ? {
                ...msg,
                message: lastMessage?.text || msg.message,
                date: lastMessage?.date || msg.date,
                time: lastMessage?.time || msg.time,
                conversationId: resolvedConversationId || msg.conversationId,
                conversationUrl: resolvedConversationUrl || msg.conversationUrl
              }
              : msg
          )));
          setSelectedMessage((prev) => ({
            ...prev,
            messages: nextMessages,
            conversationId: resolvedConversationId,
            conversationUrl: resolvedConversationUrl
          }));
          writeThreadCacheEntry(selectedMessage, {
            adTitle: selectedMessage.adTitle,
            adImage: selectedMessage.adImage,
            sender: selectedMessage.sender,
            messages: nextMessages,
            conversationId: resolvedConversationId,
            conversationUrl: resolvedConversationUrl
          });
        } else {
          // Backend may respond without thread messages. Keep optimistic entry and mark it as sent.
          setSelectedMessage((prev) => {
            if (!prev) return prev;
            const updated = Array.isArray(prev.messages) ? prev.messages : [];
            return {
              ...prev,
              messages: updated.map((m) => (m?.id === optimisticId ? { ...m, pending: false } : m)),
              conversationId: resolvedConversationId,
              conversationUrl: resolvedConversationUrl
            };
          });
          setMessages((prev) => prev.map((msg) => (
            msg.id === selectedMessage.id
              ? {
                ...msg,
                conversationId: resolvedConversationId || msg.conversationId,
                conversationUrl: resolvedConversationUrl || msg.conversationUrl
              }
              : msg
          )));
        }
      } else {
        // Revert optimistic update on logical failure.
        setSelectedMessage((prev) => {
          if (!prev) return prev;
          const updated = Array.isArray(prev.messages) ? prev.messages : [];
          return { ...prev, messages: updated.filter((m) => m?.id !== optimisticId) };
        });
        setReplyText(textToSend);
        setReplyImages(imagesToSend);
        const debugId = result?.debugId ? ` (debugId: ${result.debugId})` : "";
        alert("Ошибка: " + (result.error || "Не удалось отправить") + debugId);
      }
    } catch (err) {
      console.error("Ошибка отправки:", err, {
        status: err?.status,
        code: err?.code,
        data: err?.data || null
      });
      setSelectedMessage((prev) => {
        if (!prev) return prev;
        const updated = Array.isArray(prev.messages) ? prev.messages : [];
        return { ...prev, messages: updated.filter((m) => m?.id !== optimisticId) };
      });
      setReplyText(textToSend);
      setReplyImages(imagesToSend);
      alert(formatRequestError(err, "Ошибка при отправке сообщения"));
    } finally {
      setSending(false);
    }
  };

  const getTimeAgo = (date, time) => {
    if (!date && !time) return "";
    const now = new Date();
    try {
      const messageDate = new Date(date + "T" + (time || "00:00"));
      if (isNaN(messageDate.getTime())) return time || date || "";
      const diffHours = Math.floor((now - messageDate) / (1000 * 60 * 60));
      if (diffHours < 1) return time || "Только что";
      if (diffHours < 24) return time;
      if (diffHours < 48) return "Вчера";
      return date;
    } catch {
      return time || date || "";
    }
  };

  const formatMessageDate = (date, time) => {
    if (!date && !time) return "";
    const parts = [];
    if (date) {
      try {
        const d = new Date(date + "T00:00");
        if (!isNaN(d.getTime())) {
          const now = new Date();
          const diffDays = Math.floor((now - d) / (1000 * 60 * 60 * 24));
          if (diffDays === 0) parts.push("Сегодня");
          else if (diffDays === 1) parts.push("Вчера");
          else parts.push(d.toLocaleDateString("ru-RU", { day: "numeric", month: "short" }));
        } else {
          parts.push(date);
        }
      } catch {
        parts.push(date);
      }
    }
    if (time) parts.push(time);
    return parts.join(", ");
  };

  const threadMessages = selectedMessage?.messages?.length
    ? selectedMessage.messages
    : selectedMessage
      ? [{
        id: selectedMessage.id,
        text: selectedMessage.message,
        sender: selectedMessage.sender,
        date: selectedMessage.date,
        time: selectedMessage.time,
        direction: "incoming"
      }]
      : [];

  const fetchThread = async (message, { force = false } = {}) => {
    const cached = !force ? readThreadCacheEntry(message) : null;
    if (cached?.payload) {
      const cachedPayload = cached.payload;
      const updatedFromCache = {
        ...message,
        adTitle: cachedPayload.adTitle || message.adTitle,
        adImage: cachedPayload.adImage || message.adImage,
        sender: message.sender || cachedPayload.sender || "",
        messages: Array.isArray(cachedPayload.messages) ? cachedPayload.messages : [],
        conversationId: cachedPayload.conversationId || message.conversationId,
        conversationUrl: cachedPayload.conversationUrl || message.conversationUrl
      };
      setMessages((prev) => prev.map((msg) => (
        String(msg.id) === String(message.id)
          ? {
            ...msg,
            adTitle: updatedFromCache.adTitle,
            adImage: updatedFromCache.adImage,
            conversationId: updatedFromCache.conversationId,
            conversationUrl: updatedFromCache.conversationUrl
          }
          : msg
      )));
      setSelectedMessage(updatedFromCache);
      return;
    }

    setLoadingThread(true);
    try {
      const params = new URLSearchParams();
      params.set("accountId", message.accountId);
      if (message.conversationId) params.set("conversationId", message.conversationId);
      if (message.conversationUrl) params.set("conversationUrl", message.conversationUrl);
      if (!message.conversationId && !message.conversationUrl) {
        if (message.sender) params.set("participant", message.sender);
        if (message.adTitle) params.set("adTitle", message.adTitle);
      }
      const data = await apiFetchJson(`/api/messages/thread?${params.toString()}`);
      if (data.success) {
        const adImage = normalizePreviewImageUrl(data.adImage || message.adImage);
        const updated = {
          ...message,
          adTitle: data.adTitle || message.adTitle,
          adImage,
          sender: message.sender || data.participant || "",
          messages: data.messages || [],
          conversationId: data.conversationId || message.conversationId,
          conversationUrl: data.conversationUrl || message.conversationUrl
        };
        setMessages((prev) => prev.map((msg) => (
          String(msg.id) === String(message.id)
            ? {
              ...msg,
              adTitle: updated.adTitle,
              adImage: updated.adImage,
              conversationId: updated.conversationId,
              conversationUrl: updated.conversationUrl
            }
            : msg
        )));
        setSelectedMessage(updated);
        writeThreadCacheEntry(updated, updated);
      }
    } catch (err) {
      console.error("Ошибка загрузки диалога:", err);
    } finally {
      setLoadingThread(false);
    }
  };

  const handleKeyDown = (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      sendReply();
    }
  };

  const unreadCount = messages.filter(m => m.unread).length;
  const showConversationList = !isMobileView || !selectedMessage;
  const showChatPanel = !isMobileView || Boolean(selectedMessage);
  const selectedRawPreviewImage = selectedMessage ? getRawMessagePreviewImage(selectedMessage) : "";
  const selectedPreviewImage = selectedMessage
    ? toMessageImageSrc(selectedRawPreviewImage, selectedMessage?.accountId)
    : "";
  const selectedDirectPreviewImage = normalizePreviewImageUrl(selectedRawPreviewImage);
  const hasReplyPayload = Boolean(replyText.trim()) || replyImages.length > 0;
  const canSendReply = !sending && hasReplyPayload;

  // Spinner SVG for loading states
  const Spinner = ({ size = 20, color = "#7dd3fc" }) => (
    <svg width={size} height={size} viewBox="0 0 24 24" style={{ animation: "spin 1s linear infinite" }}>
      <circle cx="12" cy="12" r="10" stroke={color} strokeWidth="3" fill="none" strokeDasharray="31.4" strokeDashoffset="10" strokeLinecap="round" />
    </svg>
  );

  return (
    <>
      <style>{`
        @keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }
        @keyframes pulse { 0%, 100% { opacity: 0.4; } 50% { opacity: 0.8; } }
        @keyframes fadeIn { from { opacity: 0; transform: translateY(4px); } to { opacity: 1; transform: translateY(0); } }
        .msg-conv-item:hover { background: rgba(59,130,246,0.12) !important; }
        .msg-send-btn:hover:not(:disabled) { background: #16a34a !important; }
        .msg-refresh-btn:hover:not(:disabled) { background: #0284c7 !important; transform: scale(1.02); }
        .msg-refresh-btn:active:not(:disabled) { transform: scale(0.98); }
        .msg-translate-btn:hover { opacity: 1 !important; transform: translateY(-1px) scale(1.02) !important; }
        .msg-translate-btn:active { transform: translateY(0px) scale(0.98) !important; }
      `}</style>

      <div className="section-header" style={{
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        gap: "12px",
        marginBottom: "20px",
        flexWrap: "wrap"
      }}>
        <h2 style={{
          margin: 0,
          color: "#f8fafc",
          display: "flex",
          alignItems: "center",
          gap: "14px",
          fontWeight: 700
        }}>
          <span style={{
            width: "42px",
            height: "42px",
            borderRadius: "14px",
            background: "linear-gradient(135deg, rgba(249, 115, 22, 0.25), rgba(194, 65, 12, 0.2))",
            border: "1px solid rgba(251, 146, 60, 0.4)",
            boxShadow: "0 4px 15px rgba(251, 146, 60, 0.28)",
            display: "flex",
            alignItems: "center",
            justifyContent: "center"
          }}>
            <MessageIcon size={22} color="#fdba74" />
          </span>
          Сообщения
        </h2>
      </div>

      <div className="messages-layout" style={{ display: "flex", gap: "20px", height: "calc(100vh - 220px)", minHeight: "500px" }}>
        {/* Conversation List */}
        <div className="messages-list" style={{
          width: "400px",
          minWidth: "340px",
          background: "linear-gradient(145deg, rgba(30, 41, 59, 0.6) 0%, rgba(15, 23, 42, 0.95) 100%)",
          borderRadius: "20px",
          overflow: "hidden",
          boxShadow: "0 20px 50px rgba(0,0,0,0.4), 0 0 30px rgba(0,0,0,0.2)",
          border: "1px solid rgba(148,163,184,0.12)",
          color: "#e2e8f0",
          display: "flex",
          flexDirection: "column",
          backdropFilter: "blur(10px)",
          ...(showConversationList ? {} : { display: "none" })
        }}>
          {/* Header */}
          <div style={{
            padding: "16px 20px",
            borderBottom: "1px solid rgba(148,163,184,0.15)",
            background: "rgba(15,23,42,0.5)"
          }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "12px" }}>
              <h3 style={{ margin: 0, fontSize: "18px", fontWeight: 700 }}>Диалоги</h3>
              {unreadCount > 0 && (
                <span style={{
                  padding: "5px 14px",
                  background: "linear-gradient(135deg, #10b981, #059669)",
                  color: "white",
                  borderRadius: "9999px",
                  fontSize: "12px",
                  fontWeight: 600,
                  boxShadow: "0 0 12px rgba(16, 185, 129, 0.4)",
                  border: "1px solid rgba(16, 185, 129, 0.3)"
                }}>
                  {unreadCount} {unreadCount === 1 ? "новое" : "новых"}
                </span>
              )}
            </div>
            <button
              className="msg-refresh-btn"
              onClick={loadMessages}
              disabled={loading}
              style={{
                width: "100%",
                padding: "10px 16px",
                background: loading ? "rgba(14,165,233,0.3)" : "linear-gradient(135deg, #0ea5e9, #0284c7)",
                color: "white",
                border: "none",
                borderRadius: "10px",
                fontSize: "13px",
                fontWeight: 600,
                cursor: loading ? "not-allowed" : "pointer",
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                gap: "8px",
                transition: "all 0.15s ease"
              }}
            >
              {loading ? (
                <>
                  <Spinner size={16} color="#fff" />
                  Загрузка сообщений...
                </>
              ) : (
                <>
                  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                    <polyline points="23 4 23 10 17 10" />
                    <path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10" />
                  </svg>
                  Обновить сообщения
                </>
              )}
            </button>
          </div>

          {/* Error state */}
          {error && (
            <div style={{
              margin: "12px",
              padding: "12px 16px",
              background: "rgba(239,68,68,0.15)",
              border: "1px solid rgba(239,68,68,0.3)",
              borderRadius: "10px",
              fontSize: "13px",
              color: "#fca5a5"
            }}>
              <div style={{ fontWeight: 600, marginBottom: "4px" }}>Ошибка загрузки</div>
              <div>{error}</div>
              <button
                onClick={loadMessages}
                style={{
                  marginTop: "8px",
                  padding: "4px 12px",
                  background: "rgba(239,68,68,0.3)",
                  color: "#fca5a5",
                  border: "1px solid rgba(239,68,68,0.4)",
                  borderRadius: "6px",
                  fontSize: "12px",
                  cursor: "pointer"
                }}
              >
                Попробовать снова
              </button>
            </div>
          )}

          {/* Conversation list */}
          <div style={{ flex: 1, overflow: "auto" }}>
            {loading && messages.length === 0 ? (
              // Loading skeleton
              <div style={{ padding: "8px 0" }}>
                {[1, 2, 3, 4, 5].map(i => (
                  <div key={i} style={{
                    padding: "14px 20px",
                    display: "flex",
                    gap: "12px",
                    alignItems: "center",
                    borderBottom: "1px solid rgba(148,163,184,0.08)"
                  }}>
                    <div style={{
                      width: "52px",
                      height: "52px",
                      borderRadius: "12px",
                      background: "rgba(148,163,184,0.1)",
                      flexShrink: 0,
                      animation: "pulse 1.5s ease-in-out infinite"
                    }} />
                    <div style={{ flex: 1 }}>
                      <div style={{
                        height: "14px",
                        width: "60%",
                        background: "rgba(148,163,184,0.1)",
                        borderRadius: "4px",
                        marginBottom: "8px",
                        animation: "pulse 1.5s ease-in-out infinite"
                      }} />
                      <div style={{
                        height: "12px",
                        width: "80%",
                        background: "rgba(148,163,184,0.08)",
                        borderRadius: "4px",
                        marginBottom: "6px",
                        animation: "pulse 1.5s ease-in-out infinite",
                        animationDelay: "0.1s"
                      }} />
                      <div style={{
                        height: "10px",
                        width: "40%",
                        background: "rgba(148,163,184,0.06)",
                        borderRadius: "4px",
                        animation: "pulse 1.5s ease-in-out infinite",
                        animationDelay: "0.2s"
                      }} />
                    </div>
                  </div>
                ))}
              </div>
            ) : messages.length === 0 && !loading ? (
              <div style={{
                padding: "60px 20px",
                textAlign: "center",
                color: "#64748b"
              }}>
                <div style={{ fontSize: "40px", marginBottom: "12px", opacity: 0.5 }}>
                  <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" style={{ margin: "0 auto", display: "block", color: "#475569" }}>
                    <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z" />
                  </svg>
                </div>
                <div style={{ fontSize: "15px", fontWeight: 600, marginBottom: "4px" }}>Нет сообщений</div>
                <div style={{ fontSize: "13px" }}>Нажмите "Обновить" для загрузки</div>
              </div>
            ) : (
              messages.map((message) => {
                const rawPreviewImage = getRawMessagePreviewImage(message);
                const previewImage = toMessageImageSrc(rawPreviewImage, message?.accountId);
                const directPreviewImage = normalizePreviewImageUrl(rawPreviewImage);
                return (
                <div
                  key={message.id}
                  className="msg-conv-item"
                  onClick={() => {
                    markAsRead(message);
                    setSelectedMessage(message);
                    fetchThread(message);
                  }}
                  style={{
                    padding: "14px 20px",
                    borderBottom: "1px solid rgba(148,163,184,0.08)",
                    cursor: "pointer",
                    background: selectedMessage?.id === message.id
                      ? "rgba(59,130,246,0.15)"
                      : message.unread
                        ? "rgba(34,197,94,0.08)"
                        : "transparent",
                    borderLeft: selectedMessage?.id === message.id
                      ? "3px solid #3b82f6"
                      : message.unread
                        ? "3px solid #22c55e"
                        : "3px solid transparent",
                    display: "flex",
                    alignItems: "flex-start",
                    gap: "12px",
                    transition: "background 0.15s ease",
                    animation: "fadeIn 0.2s ease"
                  }}
                >
                  {/* Product image / avatar */}
                  <div style={{
                    width: "52px",
                    height: "52px",
                    borderRadius: "50%",
                    overflow: "hidden",
                    background: "rgba(15,23,42,0.6)",
                    border: "1px solid rgba(148,163,184,0.15)",
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center",
                    flexShrink: 0
                  }}>
                    {previewImage ? (
                      <img
                        src={previewImage}
                        data-direct-src={directPreviewImage || ""}
                        data-fallback-tried="0"
                        alt=""
                        style={{ width: "100%", height: "100%", objectFit: "cover" }}
                        loading="lazy"
                        referrerPolicy="no-referrer"
                        onError={(e) => {
                          const imageNode = e.currentTarget;
                          const directSrc = String(imageNode.dataset?.directSrc || "").trim();
                          const fallbackTried = imageNode.dataset?.fallbackTried === "1";
                          const currentSrc = String(imageNode.getAttribute("src") || "").trim();
                          if (!fallbackTried && directSrc && directSrc !== currentSrc) {
                            imageNode.dataset.fallbackTried = "1";
                            imageNode.setAttribute("src", directSrc);
                            return;
                          }
                          imageNode.style.display = "none";
                          const fallbackNode = imageNode.nextElementSibling;
                          if (fallbackNode) fallbackNode.style.display = "flex";
                        }}
                      />
                    ) : null}
                    <div style={{
                      width: "100%",
                      height: "100%",
                      background: message.unread
                        ? "linear-gradient(135deg, #22c55e, #16a34a)"
                        : "linear-gradient(135deg, #3b82f6, #2563eb)",
                      color: "white",
                      display: previewImage ? "none" : "flex",
                      alignItems: "center",
                      justifyContent: "center",
                      fontSize: "20px",
                      fontWeight: 700
                    }}>
                      {(message.sender || "?").charAt(0).toUpperCase()}
                    </div>
                  </div>

                  {/* Content */}
                  <div style={{ flex: 1, minWidth: 0 }}>
                    {/* Top row: sender + time */}
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "3px" }}>
                      <span style={{
                        fontWeight: 700,
                        fontSize: "14px",
                        color: "#f1f5f9",
                        overflow: "hidden",
                        textOverflow: "ellipsis",
                        whiteSpace: "nowrap",
                        maxWidth: "65%"
                      }}>
                        {message.sender || "Неизвестный"}
                      </span>
                      <span style={{
                        fontSize: "11px",
                        color: "#64748b",
                        whiteSpace: "nowrap",
                        flexShrink: 0
                      }}>
                        {getTimeAgo(message.date, message.time)}
                      </span>
                    </div>

                    {/* Ad title */}
                    <div style={{
                      fontSize: "13px",
                      color: "#94a3b8",
                      fontWeight: 600,
                      overflow: "hidden",
                      textOverflow: "ellipsis",
                      whiteSpace: "nowrap",
                      marginBottom: "3px"
                    }}>
                      {message.adTitle || "Без названия"}
                    </div>

                    {/* Last message preview */}
                    <div style={{
                      fontSize: "12px",
                      color: "#64748b",
                      overflow: "hidden",
                      textOverflow: "ellipsis",
                      whiteSpace: "nowrap",
                      marginBottom: "4px"
                    }}>
                      {message.message || ""}
                    </div>

                    {/* Account label + unread badge */}
                    <div style={{
                      display: "flex",
                      alignItems: "center",
                      gap: "6px"
                    }}>
                      <span style={{
                        fontSize: "11px",
                        color: "#475569",
                        background: "rgba(148,163,184,0.1)",
                        padding: "1px 6px",
                        borderRadius: "4px"
                      }}>
                        {message.accountName}
                      </span>
                      {message.unread && (
                        <span style={{
                          padding: "3px 10px",
                          background: "linear-gradient(135deg, #8b5cf6, #7c3aed)",
                          color: "white",
                          borderRadius: "9999px",
                          fontSize: "10px",
                          fontWeight: 700,
                          textTransform: "uppercase",
                          letterSpacing: "0.5px",
                          boxShadow: "0 0 10px rgba(139, 92, 246, 0.4)",
                          border: "1px solid rgba(139, 92, 246, 0.3)"
                        }}>
                          new
                        </span>
                      )}
                    </div>
                  </div>
                </div>
                );
              })
            )}
          </div>

          {/* Message count footer */}
          {messages.length > 0 && (
            <div style={{
              padding: "8px 20px",
              borderTop: "1px solid rgba(148,163,184,0.1)",
              fontSize: "11px",
              color: "#475569",
              textAlign: "center",
              background: "rgba(15,23,42,0.3)"
            }}>
              {messages.length} {messages.length === 1 ? "диалог" : messages.length < 5 ? "диалога" : "диалогов"}
            </div>
          )}
        </div>

        {/* Chat Panel */}
        <div className="messages-chat" style={{
          flex: 1,
          position: "relative",
          background: "linear-gradient(145deg, rgba(30, 41, 59, 0.6) 0%, rgba(15, 23, 42, 0.95) 100%)",
          borderRadius: "20px",
          overflow: "hidden",
          boxShadow: "0 20px 50px rgba(0,0,0,0.4), 0 0 30px rgba(0,0,0,0.2)",
          border: "1px solid rgba(148,163,184,0.12)",
          display: "flex",
          flexDirection: "column",
          color: "#e2e8f0",
          backdropFilter: "blur(10px)",
          ...(showChatPanel ? {} : { display: "none" })
        }}>
          {selectedMessage ? (
            <>
              {/* Chat header */}
              <div style={{
                padding: "14px 20px",
                borderBottom: "1px solid rgba(148,163,184,0.15)",
                background: "rgba(15,23,42,0.6)"
              }}>
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                  <div style={{ display: "flex", gap: "14px", alignItems: "center", flex: 1, minWidth: 0 }}>
                    {/* Product image in header */}
                    <div style={{
                      width: "50px",
                      height: "50px",
                      borderRadius: "10px",
                      overflow: "hidden",
                      border: "1px solid rgba(148,163,184,0.15)",
                      background: "rgba(15,23,42,0.8)",
                      flexShrink: 0,
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "center"
                    }}>
                      {selectedPreviewImage ? (
                        <img
                          src={selectedPreviewImage}
                          data-direct-src={selectedDirectPreviewImage || ""}
                          data-fallback-tried="0"
                          alt=""
                          style={{ width: "100%", height: "100%", objectFit: "cover" }}
                          onError={(e) => {
                            const imageNode = e.currentTarget;
                            const directSrc = String(imageNode.dataset?.directSrc || "").trim();
                            const fallbackTried = imageNode.dataset?.fallbackTried === "1";
                            const currentSrc = String(imageNode.getAttribute("src") || "").trim();
                            if (!fallbackTried && directSrc && directSrc !== currentSrc) {
                              imageNode.dataset.fallbackTried = "1";
                              imageNode.setAttribute("src", directSrc);
                              return;
                            }
                            imageNode.style.display = "none";
                          }}
                        />
                      ) : (
                        <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="#475569" strokeWidth="1.5">
                          <rect x="3" y="3" width="18" height="18" rx="2" ry="2" />
                          <circle cx="8.5" cy="8.5" r="1.5" />
                          <polyline points="21 15 16 10 5 21" />
                        </svg>
                      )}
                    </div>

                    <div style={{ minWidth: 0 }}>
                      <div style={{
                        fontWeight: 700,
                        fontSize: "15px",
                        color: "#f1f5f9",
                        overflow: "hidden",
                        textOverflow: "ellipsis",
                        whiteSpace: "nowrap"
                      }}>
                        {selectedMessage.sender || "Неизвестный"}
                      </div>
                      <div style={{
                        fontSize: "13px",
                        color: "#94a3b8",
                        fontWeight: 500,
                        overflow: "hidden",
                        textOverflow: "ellipsis",
                        whiteSpace: "nowrap",
                        marginTop: "2px"
                      }}>
                        {selectedMessage.adTitle || "Без названия"}
                      </div>
                      <div style={{
                        fontSize: "11px",
                        color: "#475569",
                        marginTop: "2px"
                      }}>
                        {selectedMessage.accountName}
                      </div>
                    </div>
                  </div>

                  <button
                    onClick={() => setSelectedMessage(null)}
                    style={{
                      background: "rgba(148,163,184,0.1)",
                      border: "none",
                      width: isMobileView ? "auto" : "32px",
                      height: "32px",
                      padding: isMobileView ? "0 10px" : 0,
                      borderRadius: "8px",
                      cursor: "pointer",
                      color: "#94a3b8",
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "center",
                      fontSize: "16px",
                      fontWeight: 600,
                      flexShrink: 0
                    }}
                  >
                    {isMobileView ? (
                      "← Назад"
                    ) : (
                      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round">
                        <line x1="18" y1="6" x2="6" y2="18" />
                        <line x1="6" y1="6" x2="18" y2="18" />
                      </svg>
                    )}
                  </button>
                </div>
              </div>

              {/* Messages area */}
              <div ref={chatScrollRef} style={{
                flex: 1,
                padding: "20px",
                overflow: "auto",
                background: "rgba(8,12,24,0.4)"
              }}>
                {loadingThread ? (
                  <div style={{
                    display: "flex",
                    flexDirection: "column",
                    alignItems: "center",
                    justifyContent: "center",
                    height: "100%",
                    gap: "12px",
                    color: "#64748b"
                  }}>
                    <Spinner size={32} color="#3b82f6" />
                    <span style={{ fontSize: "14px" }}>Загрузка диалога...</span>
                  </div>
                ) : threadMessages.length === 0 ? (
                  <div style={{ color: "#64748b", textAlign: "center", paddingTop: "40px" }}>
                    <div style={{ fontSize: "14px" }}>Нет сообщений в этом диалоге</div>
                  </div>
                ) : (
                  <>
	                    {threadMessages.map((messageItem, idx) => {
	                      const isOutgoing = messageItem.direction === "outgoing";
	                      const isPending = Boolean(messageItem.pending);
	                      const isOffer = isPaymentAndShippingMessage(messageItem);
	                      const messageKey = String(messageItem.id || `idx-${idx}`);
	                      const translation = translationByMessageId[messageKey] || null;
	                      const hasText = Boolean(String(messageItem.text || "").trim());
	                      const attachmentSrcs = extractAttachmentUrls(messageItem)
	                        .map((url) => toMessageImageSrc(url, selectedMessage?.accountId))
	                        .filter(Boolean);
	                      const canTranslate = !isOffer && !isOutgoing && hasText;

	                      const toggleTranslate = async () => {
	                        if (!canTranslate) return;

                        // Toggle off when already shown.
                        if (translation?.shown && translation?.translatedText) {
                          setTranslationByMessageId((prev) => ({
                            ...prev,
                            [messageKey]: { ...prev[messageKey], shown: false }
                          }));
                          return;
                        }

                        // Toggle on when already fetched.
                        if (translation?.translatedText && !translation?.loading) {
                          setTranslationByMessageId((prev) => ({
                            ...prev,
                            [messageKey]: { ...prev[messageKey], shown: true, error: "" }
                          }));
                          return;
                        }

                        if (translateInFlight.current.has(messageKey)) return;
                        translateInFlight.current.add(messageKey);
                        setTranslationByMessageId((prev) => ({
                          ...prev,
                          [messageKey]: {
                            ...prev[messageKey],
                            loading: true,
                            shown: true,
                            error: ""
                          }
                        }));

                        try {
                          const result = await apiFetchJson("/api/translate", {
                            method: "POST",
                            headers: { "Content-Type": "application/json" },
                            body: JSON.stringify({
                              text: String(messageItem.text || ""),
                              to: "ru",
                              accountId: selectedMessage?.accountId
                            })
                          });
                          setTranslationByMessageId((prev) => ({
                            ...prev,
                            [messageKey]: {
                              loading: false,
                              shown: true,
                              error: "",
                              translatedText: result?.translatedText || ""
                            }
                          }));
                        } catch (error) {
                          console.error("Ошибка перевода:", error);
                          setTranslationByMessageId((prev) => ({
                            ...prev,
                            [messageKey]: {
                              loading: false,
                              shown: true,
                              translatedText: "",
                              error: error?.message || "Не удалось перевести"
                            }
                          }));
                        } finally {
                          translateInFlight.current.delete(messageKey);
                        }
	                      };
	                      const offerTitle = String(messageItem?.title || "Anfrage erhalten").trim() || "Anfrage erhalten";
	                      const offerItemCents = pickOfferField(messageItem, "itemPriceInEuroCent");
	                      const offerShippingCents = pickOfferField(messageItem, "shippingCostInEuroCent");
	                      const offerTotalCents = pickOfferField(messageItem, "sellerTotalInEuroCent");
	                      const offerCarrierName = pickOfferField(messageItem, "carrierName");
	                      const offerOptionName = pickOfferField(messageItem, "shippingOptionName");
	                      const offerOptionDesc = pickOfferField(messageItem, "shippingOptionDescription");
	                      const offerLiabilityCents = pickOfferField(messageItem, "liabilityLimitInEuroCent");

	                      const acceptCta = getOfferActionText(
	                        messageItem,
	                        "ACCEPT_BUYER_OFFER_LEARN_MORE_ACTION",
	                        "Anfrage akzeptieren"
	                      );
	                      const declineCta = getOfferActionText(
	                        messageItem,
	                        "CANCEL_OFFER_ACTION",
	                        "Anfrage ablehnen"
	                      );
	                      const counterCta = getOfferActionText(
	                        messageItem,
	                        "MAKE_COUNTER_OFFER_ACTION",
	                        "Gegenangebot machen"
	                      );
	                      return (
	                        <div
	                          key={messageItem.id || idx}
	                          style={{
                            marginBottom: "12px",
                            display: "flex",
                            justifyContent: isOutgoing ? "flex-end" : "flex-start",
                            animation: "fadeIn 0.2s ease"
                          }}
	                        >
	                          <div style={{
	                            maxWidth: isOffer ? "520px" : "70%",
	                            minWidth: isOffer ? "240px" : "120px"
	                          }}>
	                            {/* Sender name */}
	                            <div style={{
	                              fontSize: "11px",
	                              color: isOutgoing ? "#86efac" : "#7dd3fc",
                              fontWeight: 600,
                              marginBottom: "4px",
                              paddingLeft: isOutgoing ? "0" : "12px",
                              paddingRight: isOutgoing ? "12px" : "0",
                              textAlign: isOutgoing ? "right" : "left"
                            }}>
	                              {messageItem.sender || (isOutgoing ? "Вы" : selectedMessage.sender || "")}
	                            </div>

	                            {isOffer ? (
	                              <div style={{
	                                borderRadius: "18px",
	                                padding: "14px 14px 12px 14px",
	                                background: "linear-gradient(145deg, rgba(2,6,23,0.55) 0%, rgba(15,23,42,0.95) 58%, rgba(6,78,59,0.22) 100%)",
	                                border: "1px solid rgba(34,197,94,0.22)",
	                                boxShadow: "0 18px 40px rgba(0,0,0,0.45)"
	                              }}>
	                                <div style={{
	                                  display: "flex",
	                                  justifyContent: "space-between",
	                                  alignItems: "flex-start",
	                                  gap: "12px",
	                                  marginBottom: "12px"
	                                }}>
	                                  <div style={{ display: "flex", gap: "10px", alignItems: "center", minWidth: 0 }}>
	                                    <div style={{
	                                      width: "36px",
	                                      height: "36px",
	                                      borderRadius: "12px",
	                                      background: "rgba(34,197,94,0.18)",
	                                      border: "1px solid rgba(34,197,94,0.25)",
	                                      display: "flex",
	                                      alignItems: "center",
	                                      justifyContent: "center",
	                                      flexShrink: 0
	                                    }}>
	                                      <span style={{ color: "#4ade80", fontWeight: 900, fontSize: "16px" }}>€</span>
	                                    </div>
	                                    <div style={{ minWidth: 0 }}>
	                                      <div style={{
	                                        fontSize: "14px",
	                                        fontWeight: 800,
	                                        color: "#bbf7d0",
	                                        overflow: "hidden",
	                                        textOverflow: "ellipsis",
	                                        whiteSpace: "nowrap"
	                                      }}>
	                                        {offerTitle}
	                                      </div>
	                                      <div style={{
	                                        fontSize: "12px",
	                                        color: "rgba(148,163,184,0.9)",
	                                        marginTop: "1px"
	                                      }}>
	                                        Sicher bezahlen
	                                      </div>
	                                    </div>
	                                  </div>
	                                  <div style={{
	                                    fontSize: "20px",
	                                    fontWeight: 900,
	                                    color: "#4ade80",
	                                    whiteSpace: "nowrap"
	                                  }}>
	                                    {formatEuroFromCents(offerTotalCents)}
	                                  </div>
	                                </div>

	                                <div style={{ display: "flex", flexDirection: "column", gap: "8px" }}>
	                                  <div style={{ display: "flex", justifyContent: "space-between", color: "rgba(226,232,240,0.92)", fontSize: "13px" }}>
	                                    <span style={{ color: "rgba(148,163,184,0.95)" }}>Betrag</span>
	                                    <span style={{ fontWeight: 700 }}>{formatEuroFromCents(offerItemCents)}</span>
	                                  </div>
	                                  <div style={{ display: "flex", justifyContent: "space-between", color: "rgba(226,232,240,0.92)", fontSize: "13px" }}>
	                                    <span style={{ color: "rgba(148,163,184,0.95)" }}>Versand</span>
	                                    <span style={{ fontWeight: 700 }}>{formatEuroFromCents(offerShippingCents)}</span>
	                                  </div>
	                                  <div style={{
	                                    display: "flex",
	                                    justifyContent: "space-between",
	                                    fontSize: "13px",
	                                    paddingTop: "6px",
	                                    borderTop: "1px solid rgba(148,163,184,0.12)"
	                                  }}>
	                                    <span style={{ color: "rgba(148,163,184,0.95)", fontWeight: 800 }}>Gesamt</span>
	                                    <span style={{ fontWeight: 900, color: "#e2e8f0" }}>{formatEuroFromCents(offerTotalCents)}</span>
	                                  </div>
	                                </div>

	                                {(offerCarrierName || offerOptionName || offerOptionDesc) ? (
	                                  <div style={{
	                                    marginTop: "12px",
	                                    padding: "12px",
	                                    borderRadius: "14px",
	                                    background: "rgba(2,6,23,0.25)",
	                                    border: "1px solid rgba(148,163,184,0.14)"
	                                  }}>
	                                    <div style={{ fontSize: "13px", fontWeight: 800, color: "#e2e8f0" }}>
	                                      Inkl. {offerCarrierName || "Versand"} {offerOptionName || ""}
	                                    </div>
	                                    {offerOptionDesc ? (
	                                      <div style={{ marginTop: "4px", fontSize: "12px", color: "rgba(148,163,184,0.95)" }}>
	                                        {offerOptionDesc}
	                                      </div>
	                                    ) : null}
	                                    {offerLiabilityCents ? (
	                                      <div style={{ marginTop: "6px", fontSize: "12px", color: "rgba(148,163,184,0.95)" }}>
	                                        Mit Sendungsverfolgung und Haftung bis {formatEuroFromCents(offerLiabilityCents)}
	                                      </div>
	                                    ) : null}
	                                  </div>
	                                ) : null}

	                                {hasText ? (
	                                  <div style={{
	                                    marginTop: "12px",
	                                    fontSize: "13px",
	                                    lineHeight: "1.5",
	                                    color: "rgba(226,232,240,0.92)"
	                                  }}>
	                                    {messageItem.text}
	                                  </div>
	                                ) : null}

	                                <div style={{ marginTop: "12px", display: "flex", flexDirection: "column", gap: "10px" }}>
	                                  <button
	                                    type="button"
	                                    disabled
	                                    style={{
	                                      width: "100%",
	                                      padding: "10px 14px",
	                                      borderRadius: "9999px",
	                                      border: "1px solid rgba(34,197,94,0.32)",
	                                      background: "linear-gradient(135deg, rgba(34,197,94,0.55), rgba(22,163,74,0.45))",
	                                      color: "rgba(255,255,255,0.95)",
	                                      fontWeight: 900,
	                                      cursor: "not-allowed",
	                                      opacity: 0.75
	                                    }}
	                                    title="Скоро"
	                                  >
	                                    {acceptCta}
	                                  </button>
	                                  <div style={{ display: "flex", gap: "10px" }}>
	                                    <button
	                                      type="button"
	                                      onClick={() => setDeclineConfirmOpen(true)}
	                                      disabled={decliningOffer}
	                                      style={{
	                                        flex: 1,
	                                        padding: "10px 14px",
	                                        borderRadius: "9999px",
	                                        border: "1px solid rgba(148,163,184,0.28)",
	                                        background: "rgba(2,6,23,0.35)",
	                                        color: "rgba(226,232,240,0.95)",
	                                        fontWeight: 800,
	                                        cursor: decliningOffer ? "not-allowed" : "pointer",
	                                        opacity: decliningOffer ? 0.7 : 1
	                                      }}
	                                    >
	                                      {declineCta}
	                                    </button>
	                                    <button
	                                      type="button"
	                                      disabled
	                                      style={{
	                                        flex: 1,
	                                        padding: "10px 14px",
	                                        borderRadius: "9999px",
	                                        border: "1px solid rgba(148,163,184,0.22)",
	                                        background: "rgba(2,6,23,0.25)",
	                                        color: "rgba(148,163,184,0.95)",
	                                        fontWeight: 800,
	                                        cursor: "not-allowed",
	                                        opacity: 0.75
	                                      }}
	                                      title="Скоро"
	                                    >
	                                      {counterCta}
	                                    </button>
	                                  </div>
	                                </div>

	                                <div style={{
	                                  fontSize: "10px",
	                                  color: "rgba(148,163,184,0.8)",
	                                  marginTop: "10px",
	                                  textAlign: "right"
	                                }}>
	                                  {isPending ? "Отправка..." : formatMessageDate(
	                                    messageItem.date || selectedMessage.date,
	                                    messageItem.time || selectedMessage.time
	                                  )}
	                                </div>
	                              </div>
	                            ) : (
	                              <div style={{
	                                position: "relative",
	                                padding: canTranslate ? "10px 14px 18px 14px" : "10px 14px",
	                                borderRadius: isOutgoing ? "14px 14px 4px 14px" : "14px 14px 14px 4px",
	                                background: isOutgoing
	                                  ? "linear-gradient(135deg, #1a4731, #14532d)"
	                                  : "rgba(15,23,42,0.8)",
	                                border: isOutgoing
	                                  ? "1px solid rgba(34,197,94,0.25)"
	                                  : "1px solid rgba(148,163,184,0.15)"
	                              }}>
	                                {attachmentSrcs.length > 0 && (
	                                  <div style={{
	                                    display: "grid",
	                                    gridTemplateColumns: "repeat(auto-fit, minmax(120px, 1fr))",
	                                    gap: "8px",
	                                    marginBottom: hasText ? "8px" : "0"
	                                  }}>
	                                    {attachmentSrcs.map((src, imageIdx) => (
	                                      <img
	                                        key={`${messageKey}-img-${imageIdx}`}
	                                        src={src}
	                                        alt=""
	                                        loading="lazy"
	                                        referrerPolicy="no-referrer"
	                                        style={{
	                                          width: "100%",
	                                          maxHeight: "220px",
	                                          objectFit: "cover",
	                                          borderRadius: "12px",
	                                          border: "1px solid rgba(148,163,184,0.14)"
	                                        }}
	                                      />
	                                    ))}
	                                  </div>
	                                )}

	                                {hasText ? (
	                                  <div style={{
	                                    fontSize: "14px",
	                                    lineHeight: "1.5",
	                                    whiteSpace: "pre-wrap",
	                                    wordBreak: "break-word",
	                                    color: "#e2e8f0"
	                                  }}>
	                                    {messageItem.text}
	                                  </div>
	                                ) : null}

	                                {/* Translate button */}
	                                {canTranslate && (
	                                  <button
	                                    type="button"
	                                    className="msg-translate-btn"
	                                    onClick={toggleTranslate}
	                                    title="Перевести на русский"
	                                    style={{
	                                      position: "absolute",
	                                      left: "8px",
	                                      bottom: "6px",
	                                      width: "22px",
	                                      height: "22px",
	                                      borderRadius: "8px",
	                                      border: "1px solid rgba(148,163,184,0.18)",
	                                      background: "rgba(2,6,23,0.25)",
	                                      color: "#94a3b8",
	                                      display: "inline-flex",
	                                      alignItems: "center",
	                                      justifyContent: "center",
	                                      cursor: "pointer",
	                                      opacity: 0.75,
	                                      transition: "transform 0.12s ease, opacity 0.12s ease, background 0.12s ease"
	                                    }}
	                                  >
	                                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round">
	                                      <path d="M5 5h6" />
	                                      <path d="M8 5c0 6-3 10-6 12" />
	                                      <path d="M7 13c2 2 4 4 7 6" />
	                                      <path d="M14 19h7" />
	                                      <path d="M17 4l4 15" />
	                                      <path d="M20 19l-3-9-3 9" />
	                                    </svg>
	                                  </button>
	                                )}

	                                {/* Translation result */}
	                                {translation?.shown && (
	                                  <div style={{
	                                    marginTop: "10px",
	                                    paddingTop: "8px",
	                                    borderTop: "1px dashed rgba(148,163,184,0.18)",
	                                    fontSize: "13px",
	                                    lineHeight: "1.45",
	                                    color: translation?.error ? "#fb7185" : "#cbd5e1",
	                                    opacity: 0.95
	                                  }}>
	                                    {translation?.loading ? (
	                                      <span style={{ color: "#94a3b8" }}>Перевод...</span>
	                                    ) : translation?.error ? (
	                                      <span>{translation.error}</span>
	                                    ) : (
	                                      <span>{translation.translatedText}</span>
	                                    )}
	                                  </div>
	                                )}

	                                {/* Timestamp */}
	                                <div style={{
	                                  fontSize: "10px",
	                                  color: "#64748b",
	                                  marginTop: "6px",
	                                  textAlign: "right"
	                                }}>
	                                  {isPending ? "Отправка..." : formatMessageDate(
	                                    messageItem.date || selectedMessage.date,
	                                    messageItem.time || selectedMessage.time
	                                  )}
	                                </div>
	                              </div>
	                            )}
	                          </div>
	                        </div>
	                      );
	                    })}
                  </>
                )}
              </div>

              {/* Reply input */}
              <div style={{
                padding: "14px 20px",
                borderTop: "1px solid rgba(148,163,184,0.15)",
                background: "rgba(15,23,42,0.6)"
              }}>
                <input
                  ref={fileInputRef}
                  type="file"
                  accept="image/*"
                  multiple
                  onChange={handleReplyImageSelect}
                  style={{ display: "none" }}
                />

                {replyImages.length > 0 && (
                  <div style={{
                    marginBottom: "10px",
                    display: "flex",
                    gap: "8px",
                    overflowX: "auto",
                    paddingBottom: "2px"
                  }}>
                    {replyImages.map((item) => (
                      <div
                        key={item.id}
                        style={{
                          position: "relative",
                          width: "72px",
                          height: "72px",
                          borderRadius: "10px",
                          border: "1px solid rgba(148,163,184,0.2)",
                          overflow: "hidden",
                          flexShrink: 0
                        }}
                      >
                        <img
                          src={item.previewUrl}
                          alt=""
                          style={{ width: "100%", height: "100%", objectFit: "cover" }}
                        />
                        <button
                          type="button"
                          onClick={() => removeReplyImage(item.id)}
                          style={{
                            position: "absolute",
                            top: "4px",
                            right: "4px",
                            width: "20px",
                            height: "20px",
                            borderRadius: "9999px",
                            border: "none",
                            background: "rgba(2,6,23,0.72)",
                            color: "#f8fafc",
                            fontSize: "12px",
                            cursor: "pointer",
                            display: "flex",
                            alignItems: "center",
                            justifyContent: "center"
                          }}
                          title="Удалить"
                        >
                          ×
                        </button>
                      </div>
                    ))}
                  </div>
                )}

                <div style={{ display: "flex", gap: "10px", alignItems: "flex-end" }}>
                  <button
                    type="button"
                    onClick={() => fileInputRef.current?.click()}
                    disabled={sending}
                    aria-label="Bilder hochladen"
                    style={{
                      width: "42px",
                      height: "42px",
                      borderRadius: "12px",
                      border: "1px solid rgba(148,163,184,0.25)",
                      background: "rgba(15,23,42,0.7)",
                      color: "#cbd5e1",
                      cursor: sending ? "not-allowed" : "pointer",
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "center",
                      flexShrink: 0
                    }}
                  >
                    <svg viewBox="0 0 24 24" width="18" height="18" fill="currentColor" aria-hidden="true">
                      <path d="M12.0004 8.54376C9.72753 8.54376 7.88501 10.4042 7.88501 12.6991C7.88501 14.994 9.72753 16.8544 12.0004 16.8544C14.2733 16.8544 16.1158 14.994 16.1158 12.6991C16.1158 10.4042 14.2733 8.54376 12.0004 8.54376ZM9.88501 12.6991C9.88501 11.5195 10.8321 10.5632 12.0004 10.5632C13.1687 10.5632 14.1158 11.5195 14.1158 12.6991C14.1158 13.8787 13.1687 14.835 12.0004 14.835C10.8321 14.835 9.88501 13.8787 9.88501 12.6991Z" />
                      <path d="M9.23077 4C8.91601 4 8.61962 4.14963 8.43077 4.40388L6.65385 6.79612H4.38462C3.75218 6.79612 3.14564 7.04979 2.69844 7.50134C2.25124 7.95288 2 8.5653 2 9.20388V17.5922C2 18.2308 2.25124 18.8432 2.69844 19.2948C3.14564 19.7463 3.75218 20 4.38462 20H19.6154C20.2478 20 20.8544 19.7463 21.3016 19.2948C21.7488 18.8432 22 18.2308 22 17.5922V9.20388C22 8.5653 21.7488 7.95288 21.3016 7.50134C20.8544 7.04979 20.2478 6.79612 19.6154 6.79612H17.3462L15.5692 4.40388C15.3804 4.14963 15.084 4 14.7692 4H9.23077ZM7.95385 8.41165L9.73077 6.01942H14.2692L16.0462 8.41165C16.235 8.6659 16.5314 8.81553 16.8462 8.81553H19.6154C19.7179 8.81553 19.8163 8.85665 19.8888 8.9292C19.9614 9.00175 20.0025 9.10014 20.0025 9.20263V17.5909C20.0025 17.6934 19.9614 17.7918 19.8888 17.8644C19.8163 17.9369 19.7179 17.978 19.6154 17.978H4.38462C4.28208 17.978 4.1837 17.9369 4.11115 17.8644C4.0386 17.7918 3.99748 17.6934 3.99748 17.5909V9.20263C3.99748 9.10014 4.0386 9.00175 4.11115 8.9292C4.1837 8.85665 4.28208 8.81553 4.38462 8.81553H7.15385C7.4686 8.81553 7.765 8.6659 7.95385 8.41165Z" />
                    </svg>
                  </button>

                  <textarea
                    value={replyText}
                    onChange={(e) => setReplyText(e.target.value)}
                    onKeyDown={handleKeyDown}
                    placeholder="Nachricht schreiben..."
                    rows={1}
                    style={{
                      flex: 1,
                      padding: "10px 14px",
                      border: "1px solid rgba(148,163,184,0.2)",
                      borderRadius: "12px",
                      resize: "none",
                      minHeight: "42px",
                      maxHeight: "120px",
                      background: "rgba(15,23,42,0.6)",
                      color: "#e2e8f0",
                      fontSize: "14px",
                      lineHeight: "1.4",
                      outline: "none",
                      fontFamily: "inherit"
                    }}
                    onInput={(e) => {
                      e.target.style.height = "42px";
                      e.target.style.height = Math.min(e.target.scrollHeight, 120) + "px";
                    }}
                  />

                  <button
                    className="msg-send-btn"
                    onClick={sendReply}
                    disabled={!canSendReply}
                    style={{
                      padding: "10px 20px",
                      background: sending
                        ? "rgba(34,197,94,0.4)"
                        : !hasReplyPayload
                          ? "rgba(148,163,184,0.15)"
                          : "linear-gradient(135deg, #22c55e, #16a34a)",
                      color: !hasReplyPayload ? "#64748b" : "white",
                      border: "none",
                      borderRadius: "12px",
                      cursor: !canSendReply ? "not-allowed" : "pointer",
                      whiteSpace: "nowrap",
                      fontSize: "14px",
                      fontWeight: 600,
                      display: "flex",
                      alignItems: "center",
                      gap: "6px",
                      height: "42px",
                      transition: "all 0.15s ease"
                    }}
                  >
                    {sending ? (
                      <>
                        <Spinner size={14} color="#fff" />
                        <span>Отправка</span>
                      </>
                    ) : (
                      <>
                        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                          <line x1="22" y1="2" x2="11" y2="13" />
                          <polygon points="22 2 15 22 11 13 2 9 22 2" />
                        </svg>
                        <span>Отправить</span>
                      </>
                    )}
                  </button>
                </div>
                <div style={{ fontSize: "11px", color: "#475569", marginTop: "6px" }}>
                  Enter для отправки, Shift+Enter для новой строки
                </div>
              </div>

              {declineConfirmOpen && (
                <div style={{
                  position: "absolute",
                  inset: 0,
                  background: "rgba(2,6,23,0.65)",
                  backdropFilter: "blur(2px)",
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  zIndex: 40,
                  padding: "16px"
                }}>
                  <div style={{
                    width: "100%",
                    maxWidth: "420px",
                    borderRadius: "16px",
                    border: "1px solid rgba(148,163,184,0.2)",
                    background: "linear-gradient(145deg, rgba(15,23,42,0.96), rgba(2,6,23,0.98))",
                    boxShadow: "0 18px 60px rgba(0,0,0,0.5)",
                    padding: "18px"
                  }}>
                    <div style={{ fontSize: "16px", fontWeight: 700, color: "#f8fafc", marginBottom: "8px" }}>
                      Anfrage ablehnen
                    </div>
                    <div style={{ fontSize: "13px", lineHeight: "1.5", color: "#94a3b8", marginBottom: "16px" }}>
                      Möchtest du diese Anfrage wirklich ablehnen?
                    </div>
                    <div style={{ display: "flex", gap: "10px", justifyContent: "flex-end" }}>
                      <button
                        type="button"
                        onClick={() => setDeclineConfirmOpen(false)}
                        disabled={decliningOffer}
                        style={{
                          padding: "10px 14px",
                          borderRadius: "10px",
                          border: "1px solid rgba(148,163,184,0.22)",
                          background: "rgba(15,23,42,0.65)",
                          color: "#cbd5e1",
                          cursor: decliningOffer ? "not-allowed" : "pointer"
                        }}
                      >
                        Abbrechen
                      </button>
                      <button
                        type="button"
                        onClick={confirmDeclineOffer}
                        disabled={decliningOffer}
                        style={{
                          padding: "10px 14px",
                          borderRadius: "10px",
                          border: "1px solid rgba(239,68,68,0.35)",
                          background: "linear-gradient(135deg, rgba(239,68,68,0.78), rgba(185,28,28,0.82))",
                          color: "#fff",
                          fontWeight: 700,
                          cursor: decliningOffer ? "not-allowed" : "pointer",
                          display: "inline-flex",
                          alignItems: "center",
                          gap: "6px",
                          minWidth: "158px",
                          justifyContent: "center"
                        }}
                      >
                        {decliningOffer ? (
                          <>
                            <Spinner size={14} color="#fff" />
                            <span>Bitte warten</span>
                          </>
                        ) : (
                          "Anfrage ablehnen"
                        )}
                      </button>
                    </div>
                  </div>
                </div>
              )}
            </>
          ) : (
            // Empty state
            <div style={{
              flex: 1,
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              color: "#475569"
            }}>
              <div style={{ textAlign: "center", maxWidth: "280px" }}>
                <svg width="64" height="64" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1" style={{ margin: "0 auto 16px", display: "block", color: "#334155" }}>
                  <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z" />
                </svg>
                <h3 style={{ margin: "0 0 8px", fontSize: "16px", color: "#64748b" }}>
                  Выберите диалог
                </h3>
                <p style={{ margin: 0, fontSize: "13px", lineHeight: "1.5" }}>
                  Выберите диалог из списка слева для просмотра сообщений и ответа
                </p>
              </div>
            </div>
          )}
        </div>
      </div>
    </>
  );
};

export default MessagesTab;
