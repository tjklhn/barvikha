import React, { useState, useEffect, useRef } from "react";
import { apiFetchJson, getAccessToken, setAccessToken } from "./api";
import AddAccountModal from "./components/AddAccountModal";
import AddProxyModal from "./components/AddProxyModal";
import ProxyChecker from "./components/ProxyChecker";
import MessagesTab from "./components/MessagesTab";
import AdModal from "./components/AdModal";
import ActiveAdsTab from "./components/ActiveAdsTab";
import {
  ArmorLogoIcon,
  DashboardIcon,
  UserIcon,
  LinkIcon,
  MessageIcon,
  PlusIcon,
  RefreshIcon,
  EditIcon,
  FileIcon,
  CheckIcon,
  SuccessIcon,
  AlertIcon,
  XIcon,
  TrashIcon,
  TargetIcon,
  PackageIcon
} from "./components/Icons";

const PHONE_VIEW_MAX_WIDTH = 900;
const APP_VERSION_RAW = String(process.env.REACT_APP_VERSION || "").trim();
const APP_VERSION = APP_VERSION_RAW ? `v${APP_VERSION_RAW}` : "vdev";

const detectPhoneView = () => {
  if (typeof window === "undefined") return false;
  const viewportWidth = window.innerWidth || document.documentElement.clientWidth || 0;
  return viewportWidth > 0 && viewportWidth <= PHONE_VIEW_MAX_WIDTH;
};

const getDebugFlag = (key) => {
  if (typeof window === "undefined") return false;
  try {
    return window.localStorage.getItem(key) === "1";
  } catch (error) {
    return false;
  }
};

function App() {
  const [accounts, setAccounts] = useState([]);
  const [proxies, setProxies] = useState([]);
  const [stats, setStats] = useState(null);
  const [accountMetrics, setAccountMetrics] = useState({});
  const [activeTab, setActiveTab] = useState("dashboard");
  const [showAddAccountModal, setShowAddAccountModal] = useState(false);
  const [showAddProxyModal, setShowAddProxyModal] = useState(false);
  const [showAdModal, setShowAdModal] = useState(false);
  const [accessToken, setAccessTokenState] = useState(() => getAccessToken());
  const [accessTokenDraft, setAccessTokenDraft] = useState(() => getAccessToken());
  const [tokenStatus, setTokenStatus] = useState({ state: "unknown", message: "", expiresAt: null });
  const [authError, setAuthError] = useState("");
  const [showProfilePanel, setShowProfilePanel] = useState(false);
  const [openAccountMenuId, setOpenAccountMenuId] = useState(null);
  const [isMobileNavOpen, setIsMobileNavOpen] = useState(false);
  const [isPhoneView, setIsPhoneView] = useState(() => detectPhoneView());
  const profilePanelRef = useRef(null);
  const [checkingAllProxies, setCheckingAllProxies] = useState(false);
  const [adImages, setAdImages] = useState([]);
  const [publishingAd, setPublishingAd] = useState(false);
  const [extraFields, setExtraFields] = useState([]);
  const [extraFieldValues, setExtraFieldValues] = useState({});
  const [loadingExtraFields, setLoadingExtraFields] = useState(false);
  const [extraFieldsError, setExtraFieldsError] = useState("");
  const [categories, setCategories] = useState([]);
  const [categoriesUpdatedAt, setCategoriesUpdatedAt] = useState(null);
  const [loadingCategories, setLoadingCategories] = useState(false);
  const [newAd, setNewAd] = useState({
    accountId: "",
    title: "",
    description: "",
    price: "",
    categoryId: "",
    categoryUrl: "",
    postalCode: "",
    categoryKey: "",
    categoryPath: []
  });
  const debugFieldsEnabled = getDebugFlag("klDebugFields");
  const debugPublishEnabled = getDebugFlag("klDebugPublish");

  const formatAccountLabel = (account) => {
    const rawName = String(account.profileName || "").trim();
    const name = rawName && !/mein(e)? anzeigen|mein profil|meine anzeigen|profil und meine anzeigen/i.test(rawName)
      ? rawName
      : (account.username || "Аккаунт");
    const email = account.profileEmail || "";
    return email ? `${name} (${email})` : name;
  };

  const cardStyle = {
    background: "linear-gradient(145deg, rgba(30, 41, 59, 0.8) 0%, rgba(15, 23, 42, 0.95) 100%)",
    borderRadius: "20px",
    border: "1px solid rgba(148,163,184,0.15)",
    boxShadow: "0 20px 50px rgba(0,0,0,0.4), 0 0 30px rgba(0,0,0,0.2)",
    backdropFilter: "blur(10px)"
  };
  const textMuted = "#94a3b8";
  const textPrimary = "#e2e8f0";
  const textTitle = "#f8fafc";

  // Унифицированные стили для плашек (badges)
  const getBadgeStyle = (status) => {
    const baseStyle = {
      padding: "5px 14px",
      borderRadius: "9999px",
      fontSize: "12px",
      fontWeight: "600",
      textTransform: "uppercase",
      letterSpacing: "0.5px",
      display: "inline-flex",
      alignItems: "center",
      gap: "6px",
      transition: "all 0.2s ease",
      cursor: "default"
    };

    switch (status) {
      case "active":
      case "aktiv":
      case "success":
        return {
          ...baseStyle,
          background: "linear-gradient(135deg, #10b981, #059669)",
          color: "white",
          border: "1px solid rgba(16, 185, 129, 0.3)",
          boxShadow: "0 0 12px rgba(16, 185, 129, 0.35)"
        };
      case "checking":
      case "pending":
      case "warning":
      case "reserviert":
        return {
          ...baseStyle,
          background: "linear-gradient(135deg, #f59e0b, #d97706)",
          color: "white",
          border: "1px solid rgba(245, 158, 11, 0.3)",
          boxShadow: "0 0 12px rgba(245, 158, 11, 0.35)"
        };
      case "failed":
      case "error":
      case "gelöscht":
        return {
          ...baseStyle,
          background: "linear-gradient(135deg, #ef4444, #dc2626)",
          color: "white",
          border: "1px solid rgba(239, 68, 68, 0.3)",
          boxShadow: "0 0 12px rgba(239, 68, 68, 0.35)"
        };
      case "info":
        return {
          ...baseStyle,
          background: "linear-gradient(135deg, #3b82f6, #2563eb)",
          color: "white",
          border: "1px solid rgba(59, 130, 246, 0.3)",
          boxShadow: "0 0 12px rgba(59, 130, 246, 0.35)"
        };
      case "new":
        return {
          ...baseStyle,
          background: "linear-gradient(135deg, #8b5cf6, #7c3aed)",
          color: "white",
          border: "1px solid rgba(139, 92, 246, 0.3)",
          boxShadow: "0 0 12px rgba(139, 92, 246, 0.4)",
          animation: "pulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite"
        };
      default:
        return {
          ...baseStyle,
          background: "linear-gradient(135deg, #475569, #334155)",
          color: "#e2e8f0",
          border: "1px solid rgba(148, 163, 184, 0.2)"
        };
    }
  };

  useEffect(() => {
    loadData();
  }, []);

  useEffect(() => {
    if (activeTab !== "accounts") return;
    loadAccountMetrics();
  }, [activeTab]);

  useEffect(() => {
    const handleResize = () => {
      setIsPhoneView(detectPhoneView());
    };
    handleResize();
    window.addEventListener("resize", handleResize);
    return () => window.removeEventListener("resize", handleResize);
  }, []);

  useEffect(() => {
    if (!isPhoneView && isMobileNavOpen) {
      setIsMobileNavOpen(false);
    }
  }, [isPhoneView, isMobileNavOpen]);

  useEffect(() => {
    if (!accessToken) {
      setTokenStatus({ state: "missing", message: "Токен не задан", expiresAt: null });
      return;
    }
    validateToken(accessToken);
  }, [accessToken]);

  useEffect(() => {
    const handleClickOutside = (event) => {
      if (profilePanelRef.current && !profilePanelRef.current.contains(event.target)) {
        setShowProfilePanel(false);
      }
      if (!event.target.closest("[data-account-menu]") && !event.target.closest("[data-account-menu-button]")) {
        setOpenAccountMenuId(null);
      }
    };
    const handleEscape = (event) => {
      if (event.key === "Escape") {
        setShowProfilePanel(false);
        setOpenAccountMenuId(null);
      }
    };
    document.addEventListener("mousedown", handleClickOutside);
    document.addEventListener("keydown", handleEscape);
    return () => {
      document.removeEventListener("mousedown", handleClickOutside);
      document.removeEventListener("keydown", handleEscape);
    };
  }, []);

  const handleAuthError = (error) => {
    if (error?.status === 401) {
      setAuthError(error.message || "Требуется токен доступа");
    }
  };

  const parseMessageDateTime = (dateValue, timeValue) => {
    const dateText = String(dateValue || "").trim();
    const timeText = String(timeValue || "").trim();
    if (!dateText && !timeText) return null;

    if (/^\d{4}-\d{2}-\d{2}$/.test(dateText)) {
      const parsed = new Date(`${dateText}T${/^\d{1,2}:\d{2}$/.test(timeText) ? timeText : "00:00"}:00`);
      return Number.isNaN(parsed.getTime()) ? null : parsed;
    }

    const dotDateMatch = dateText.match(/^(\d{1,2})[./](\d{1,2})[./](\d{2,4})$/);
    if (dotDateMatch) {
      const day = Number(dotDateMatch[1]);
      const month = Number(dotDateMatch[2]) - 1;
      const year = Number(dotDateMatch[3].length === 2 ? `20${dotDateMatch[3]}` : dotDateMatch[3]);
      const parsed = new Date(year, month, day);
      if (!Number.isNaN(parsed.getTime())) return parsed;
    }

    const fallback = new Date(`${dateText}${timeText ? ` ${timeText}` : ""}`);
    return Number.isNaN(fallback.getTime()) ? null : fallback;
  };

  const dedupeMessageSummaries = (messages = []) => {
    const seen = new Set();
    const unique = [];
    messages.forEach((item) => {
      if (!item || typeof item !== "object") return;
      const key = item.conversationId
        || item.conversationUrl
        || item.id
        || `${item.accountId || ""}|${item.sender || ""}|${item.adTitle || ""}|${item.message || ""}|${item.time || ""}`;
      if (seen.has(key)) return;
      seen.add(key);
      unique.push(item);
    });
    return unique;
  };

  const buildMessagesStats = (messages = []) => {
    const uniqueMessages = dedupeMessageSummaries(messages);
    const now = new Date();
    let unread = 0;
    let today = 0;

    uniqueMessages.forEach((message) => {
      if (message?.unread) unread += 1;
      const parsedDate = parseMessageDateTime(message?.date, message?.time);
      if (!parsedDate) return;
      if (
        parsedDate.getFullYear() === now.getFullYear()
        && parsedDate.getMonth() === now.getMonth()
        && parsedDate.getDate() === now.getDate()
      ) {
        today += 1;
      }
    });

    return {
      total: uniqueMessages.length,
      today,
      unread
    };
  };

  const validateToken = async (tokenValue) => {
    try {
      const response = await apiFetchJson("/api/auth/validate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ token: tokenValue }),
        timeoutMs: 90000
      });
      if (response.valid) {
        setTokenStatus({
          state: "valid",
          message: response.expiresAt ? `Активен до ${new Date(response.expiresAt).toLocaleDateString()}` : "Активен",
          expiresAt: response.expiresAt || null
        });
        setAuthError("");
        return true;
      }
      setTokenStatus({
        state: "invalid",
        message: response.error || "Неверный токен",
        expiresAt: response.expiresAt || null
      });
      return false;
    } catch (error) {
      setTokenStatus({ state: "invalid", message: error.message || "Ошибка проверки токена", expiresAt: null });
      return false;
    }
  };

  const applyAccessToken = async () => {
    const nextToken = String(accessTokenDraft || "").trim();
    setAccessToken(nextToken);
    setAccessTokenState(nextToken);
    const isValid = await validateToken(nextToken);
    if (isValid) {
      loadData();
    }
  };

  const loadData = async () => {
    try {
      const [accountsRes, proxiesRes, statsRes, messagesRes] = await Promise.all([
        apiFetchJson("/api/accounts"),
        apiFetchJson("/api/proxies"),
        apiFetchJson("/api/stats"),
        apiFetchJson("/api/messages").catch(() => null)
      ]);

      setAccounts(accountsRes);
      setProxies(proxiesRes);

      const realMessageStats = Array.isArray(messagesRes) ? buildMessagesStats(messagesRes) : null;
      setStats({
        ...statsRes,
        messages: realMessageStats || statsRes?.messages || { total: 0, today: 0, unread: 0 }
      });
    } catch (error) {
      handleAuthError(error);
      console.error("Ошибка загрузки данных:", error);
    }
  };

  const buildAccountMetrics = (ads = [], messages = []) => {
    const metrics = {};
    const ensure = (accountId) => {
      if (!metrics[accountId]) {
        metrics[accountId] = { active: 0, total: 0, chats: 0, unread: 0 };
      }
      return metrics[accountId];
    };

    ads.forEach((ad) => {
      if (!ad || ad.accountId == null) return;
      const entry = ensure(ad.accountId);
      entry.total += 1;
      const status = String(ad.status || "").toLowerCase();
      if (status === "aktiv" || status === "active") {
        entry.active += 1;
      }
    });

    messages.forEach((msg) => {
      if (!msg || msg.accountId == null) return;
      const entry = ensure(msg.accountId);
      entry.chats += 1;
      if (msg.unread) entry.unread += 1;
    });

    return metrics;
  };

  const loadAccountMetrics = async () => {
    try {
      const [adsRes, messagesRes] = await Promise.all([
        apiFetchJson("/api/ads/active"),
        apiFetchJson("/api/messages")
      ]);
      const ads = Array.isArray(adsRes?.ads) ? adsRes.ads : [];
      const messages = Array.isArray(messagesRes) ? messagesRes : [];
      setAccountMetrics(buildAccountMetrics(ads, messages));
      const accountsRes = await apiFetchJson("/api/accounts");
      setAccounts(accountsRes);
    } catch (error) {
      handleAuthError(error);
      console.error("Ошибка загрузки метрик аккаунтов:", error);
    }
  };

  const handleAddAccount = (newAccount) => {
    setAccounts([newAccount, ...accounts]);
    loadData(); // Перезагружаем статистику
    if (activeTab === "accounts") {
      loadAccountMetrics();
    }
  };

  const handleAddProxy = (newProxy) => {
    setProxies([newProxy, ...proxies]);
    loadData(); // Перезагружаем статистику
  };

  const handleDeleteAccount = async (id) => {
    if (!window.confirm("Удалить аккаунт?")) return;

    try {
      const result = await apiFetchJson(`/api/accounts/${id}`, {
        method: "DELETE"
      });
      if (result.success) {
        setAccounts(accounts.filter(acc => acc.id !== id));
        alert("Аккаунт удален");
      }
    } catch (error) {
      handleAuthError(error);
      alert("Ошибка при удалении");
    }
  };

  const handleRefreshAccount = async (id) => {
    setOpenAccountMenuId(null);
    try {
      const result = await apiFetchJson(`/api/accounts/${id}/refresh`, {
        method: "POST",
        timeoutMs: 180000
      });
      if (!result?.success) {
        alert(result?.error || "Не удалось обновить аккаунт");
      }
      await loadAccountMetrics();
    } catch (error) {
      handleAuthError(error);
      alert(error?.message || "Не удалось обновить аккаунт");
    }
  };

  const handleDeleteProxy = async (id) => {
    if (!window.confirm("Удалить прокси?")) return;

    try {
      const result = await apiFetchJson(`/api/proxies/${id}`, {
        method: "DELETE"
      });
      if (result.success) {
        setProxies(proxies.filter(proxy => proxy.id !== id));
        alert("Прокси удален");
        loadData();
      }
    } catch (error) {
      handleAuthError(error);
      alert("Ошибка при удалении прокси");
    }
  };

  const handleProxyCheckComplete = (proxyId, result) => {
    // Обновляем статус прокси в списке
    setProxies(prev => prev.map(proxy => {
      if (proxy.id === proxyId) {
        return {
          ...proxy,
          status: result.success ? "active" : "failed",
          lastChecked: new Date().toISOString(),
          checkResult: result
        };
      }
      return proxy;
    }));

    // Обновляем статистику
    loadData();
  };

  const checkAllProxies = async () => {
    setCheckingAllProxies(true);
    try {
      const result = await apiFetchJson("/api/proxies/check-all", {
        method: "POST"
      });
      if (result.success) {
        // Обновляем список прокси с новыми данными
        loadData();
        alert("Все прокси проверены!");
      }
    } catch (error) {
      handleAuthError(error);
      alert(error?.data?.error || error?.message || "Ошибка при проверке прокси");
    } finally {
      setCheckingAllProxies(false);
    }
  };

  const loadCategories = async (forceRefresh = false) => {
    setLoadingCategories(true);
    try {
      const data = await apiFetchJson(`/api/categories${forceRefresh ? "?refresh=true" : ""}`);
      const items = Array.isArray(data?.categories) ? data.categories : [];
      setCategories(items);
      setCategoriesUpdatedAt(data?.updatedAt || null);
    } catch (error) {
      handleAuthError(error);
      console.error("Ошибка загрузки категорий:", error);
    } finally {
      setLoadingCategories(false);
    }
  };

  useEffect(() => {
    if (showAdModal) {
      setNewAd((prev) => ({
        ...prev,
        categoryId: "",
        categoryUrl: "",
        categoryKey: ""
      }));
      setExtraFields([]);
      setExtraFieldValues({});
      loadCategories(false);
    }
  }, [showAdModal]);

  useEffect(() => {
    let cancelled = false;
    const controller = new AbortController();

    const fetchExtraFields = async () => {
      if (!showAdModal) return;
      if (!newAd.accountId || (!newAd.categoryId && !newAd.categoryUrl)) {
        setLoadingExtraFields(false);
        setExtraFields([]);
        setExtraFieldValues({});
        setExtraFieldsError("");
        return;
      }

      setLoadingExtraFields(true);
      setExtraFieldsError("");
      try {
        const buildFieldsRequestUrl = (forceRefresh = false) => {
          const params = new URLSearchParams();
          params.set("accountId", newAd.accountId);
          if (newAd.categoryId) params.set("categoryId", newAd.categoryId);
          if (!newAd.categoryId && newAd.categoryUrl) params.set("categoryUrl", newAd.categoryUrl);
          if (Array.isArray(newAd.categoryPath) && newAd.categoryPath.length > 0) {
            params.set("categoryPath", JSON.stringify(newAd.categoryPath));
          }
          if (forceRefresh) {
            params.set("refresh", "true");
          }
          if (debugFieldsEnabled) {
            params.set("debug", "1");
          }
          return `/api/ads/fields?${params.toString()}`;
        };

        const requestOptions = {
          signal: controller.signal,
          timeoutMs: 0,
          retry: false
        };

        let data = await apiFetchJson(buildFieldsRequestUrl(false), requestOptions);
        if (cancelled) return;
        if (data?.success === false) {
          const errorMessage = data?.error || "Ошибка загрузки параметров категории";
          setExtraFields([]);
          setExtraFieldValues({});
          setExtraFieldsError(errorMessage);
          return;
        }
        let fields = Array.isArray(data?.fields) ? data.fields : [];

        if (fields.length === 0) {
          try {
            const refreshed = await apiFetchJson(buildFieldsRequestUrl(true), requestOptions);
            if (!cancelled && refreshed?.success !== false) {
              data = refreshed;
              fields = Array.isArray(refreshed?.fields) ? refreshed.fields : [];
            }
          } catch (refreshError) {
            // Keep the initial result if refresh fallback fails.
          }
        }

        if (data?.debugId) {
          console.log("[fields] response", {
            debugId: data.debugId,
            accountId: newAd.accountId,
            categoryId: newAd.categoryId || "",
            categoryUrl: newAd.categoryUrl || "",
            cached: Boolean(data.cached),
            fieldCount: fields.length
          });
        }

        setExtraFields(fields);
        setExtraFieldsError("");
        const nextValues = {};
        fields.forEach((field) => {
          const key = field.name || field.label;
          if (!key) return;
          nextValues[key] = extraFieldValues[key] || "";
        });
        setExtraFieldValues(nextValues);
      } catch (error) {
        if (cancelled) return;
        if (error?.name === "AbortError") return;
        handleAuthError(error);
        setExtraFields([]);
        setExtraFieldValues({});
        const debugId = error?.data?.debugId ? ` [debugId: ${error.data.debugId}]` : "";
        const errorMessage = (error?.message || "Ошибка загрузки параметров категории") + debugId;
        setExtraFieldsError(errorMessage);
      } finally {
        if (cancelled) return;
        setLoadingExtraFields(false);
      }
    };
    fetchExtraFields();
    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [showAdModal, newAd.accountId, newAd.categoryId, newAd.categoryKey, newAd.categoryUrl, newAd.categoryPath]);

  const handleTabSelect = (tabId) => {
    setActiveTab(tabId);
    if (isPhoneView) {
      setIsMobileNavOpen(false);
    }
  };

  const handleCreateAd = async () => {
    if (publishingAd) return;
    if (!newAd.accountId) {
      alert("Выберите аккаунт");
      return;
    }
    if (!newAd.title.trim() || !newAd.description.trim() || !newAd.price.trim()) {
      alert("Заполните все обязательные поля");
      return;
    }
    if (!newAd.categoryId && !newAd.categoryUrl) {
      alert("Выберите категорию");
      return;
    }

    console.log("[handleCreateAd] Данные для публикации:", {
      accountId: newAd.accountId,
      title: newAd.title,
      categoryId: newAd.categoryId,
      categoryUrl: newAd.categoryUrl,
      price: newAd.price,
      postalCode: newAd.postalCode,
      extraFields: Object.keys(extraFieldValues).length,
      images: adImages.length
    });

    try {
      setPublishingAd(true);
      const formData = new FormData();
      formData.append("accountId", newAd.accountId);
      formData.append("title", newAd.title);
      formData.append("description", newAd.description);
      formData.append("price", newAd.price);
      if (newAd.postalCode) formData.append("postalCode", newAd.postalCode);
      if (newAd.categoryId) formData.append("categoryId", newAd.categoryId);
      if (newAd.categoryUrl) formData.append("categoryUrl", newAd.categoryUrl);
      if (Array.isArray(newAd.categoryPath) && newAd.categoryPath.length > 0) {
        formData.append("categoryPath", JSON.stringify(newAd.categoryPath));
      }
      if (Object.keys(extraFieldValues).length > 0) {
        const payload = extraFields.map((field) => {
          const key = field.name || field.label;
          return {
            name: field.name || "",
            label: field.label || "",
            value: key ? (extraFieldValues[key] || "") : ""
          };
        });
        formData.append("extraFields", JSON.stringify(payload));
      }
      if (debugPublishEnabled) {
        formData.append("debug", "1");
      }
      adImages.forEach((file) => {
        formData.append("images", file);
      });

      const result = await apiFetchJson("/api/ads/create", {
        method: "POST",
        body: formData,
        timeoutMs: 0,
        retry: false
      });
      console.log("[handleCreateAd] Ответ сервера:", result);

      if (result.success) {
        alert(result.message || "Объявление отправлено на публикацию!");
        setShowAdModal(false);
        setNewAd({
          accountId: "",
          title: "",
          description: "",
          price: "",
          categoryId: "",
          categoryUrl: "",
          postalCode: "",
          categoryKey: "",
          categoryPath: []
        });
        setAdImages([]);
        setExtraFields([]);
        setExtraFieldValues({});
      } else {
        console.error("[handleCreateAd] Ошибка:", result?.error);
        alert("Ошибка: " + (result?.error || "Не удалось опубликовать объявление"));
      }
    } catch (error) {
      handleAuthError(error);
      console.error("[handleCreateAd] Исключение:", error);
      const debugId = error?.data?.debugId ? ` [debugId: ${error.data.debugId}]` : "";
      alert("Ошибка при создании объявления: " + error.message + debugId);
    } finally {
      setPublishingAd(false);
    }
  };

  const renderConnectionStatus = (proxy) => {
    const connection = proxy?.checkResult?.connectionCheck;

    if (!connection) {
      return (
        <span style={{ fontSize: "12px", color: textMuted }}>
          Соединение не проверялось
        </span>
      );
    }

    if (connection.ok) {
      return (
        <span style={{ fontSize: "12px", color: "#52c41a" }}>
          CONNECT OK ({connection.connectTime}мс{connection.statusCode ? `, ${connection.statusCode}` : ""})
        </span>
      );
    }

    return (
      <span style={{ fontSize: "12px", color: "#f87171" }}>
        CONNECT fail{connection.statusCode ? ` (${connection.statusCode})` : ""}: {connection.error || "Ошибка"}
      </span>
    );
  };

  const renderSectionTitle = (IconComponent, title, iconTheme = {}) => {
    const {
      iconColor = "#e2e8f0",
      background = "linear-gradient(135deg, rgba(15, 23, 42, 0.85), rgba(30, 41, 59, 0.35))",
      border = "1px solid rgba(148, 163, 184, 0.35)",
      boxShadow = "0 4px 15px rgba(148, 163, 184, 0.25)"
    } = iconTheme;

    return (
      <h2 style={{
        margin: 0,
        color: textTitle,
        display: "flex",
        alignItems: "center",
        gap: "14px",
        fontWeight: "700"
      }}>
        <span style={{
          width: "42px",
          height: "42px",
          borderRadius: "14px",
          background,
          border,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          boxShadow
        }}>
          <IconComponent size={22} color={iconColor} />
        </span>
        {title}
      </h2>
    );
  };

  const DashboardTab = () => (
    <div>
      {renderSectionTitle(DashboardIcon, "Дашборд", {
        iconColor: "#a78bfa",
        background: "linear-gradient(135deg, rgba(99, 102, 241, 0.22), rgba(76, 29, 149, 0.25))",
        border: "1px solid rgba(167, 139, 250, 0.45)",
        boxShadow: "0 4px 15px rgba(139, 92, 246, 0.3)"
      })}
      {stats && (
        <div className="dashboard-stats" style={{
          display: "flex",
          gap: "20px",
          marginTop: "20px",
          flexWrap: "wrap",
          flexDirection: isPhoneView ? "column" : "row"
        }}>
          <div className="stats-card accent" style={{
            padding: "24px",
            background: "linear-gradient(145deg, rgba(59,130,246,0.2) 0%, rgba(15,23,42,0.95) 100%)",
            borderRadius: "20px",
            border: "1px solid rgba(59,130,246,0.3)",
            flex: 1,
            minWidth: isPhoneView ? "0" : "200px",
            color: textPrimary,
            boxShadow: "0 20px 40px rgba(0,0,0,0.3), 0 0 20px rgba(59,130,246,0.1)",
            position: "relative",
            overflow: "hidden",
            transition: "all 0.3s ease"
          }}>
            <div style={{
              position: "absolute",
              top: 0,
              left: 0,
              right: 0,
              height: "3px",
              background: "linear-gradient(90deg, #3b82f6, #60a5fa)",
              opacity: 1
            }} />
            <div style={{ display: "flex", alignItems: "center", gap: "12px" }}>
              <div style={{
                width: "42px",
                height: "42px",
                borderRadius: "14px",
                background: "linear-gradient(135deg, rgba(59,130,246,0.25), rgba(59,130,246,0.1))",
                border: "1px solid rgba(59,130,246,0.35)",
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                boxShadow: "0 4px 15px rgba(59,130,246,0.3)"
              }}>
                <UserIcon size={20} color="#60a5fa" />
              </div>
              <h3 style={{ color: textTitle, margin: 0, fontSize: "18px" }}>Аккаунты</h3>
            </div>
            <p style={{ fontSize: "40px", fontWeight: "700", margin: "16px 0 12px", color: "#ffffff" }}>{stats.accounts.total}</p>
            <div style={{ display: "flex", justifyContent: "space-between", fontSize: "13px", color: textMuted }}>
              <span style={{ display: "flex", alignItems: "center", gap: "6px" }}>
                <span style={getBadgeStyle("active")}><SuccessIcon size={12} color="white" /> {stats.accounts.active}</span>
              </span>
              <span style={{ display: "flex", alignItems: "center", gap: "6px" }}>
                <span style={getBadgeStyle("warning")}><AlertIcon size={12} color="white" /> {stats.accounts.checking}</span>
              </span>
              <span style={{ display: "flex", alignItems: "center", gap: "6px" }}>
                <span style={getBadgeStyle("failed")}><XIcon size={12} color="white" /> {stats.accounts.failed}</span>
              </span>
            </div>
          </div>
          <div className="stats-card primary" style={{
            padding: "24px",
            background: "linear-gradient(145deg, rgba(16,185,129,0.2) 0%, rgba(15,23,42,0.95) 100%)",
            borderRadius: "20px",
            border: "1px solid rgba(16,185,129,0.3)",
            flex: 1,
            minWidth: isPhoneView ? "0" : "200px",
            color: textPrimary,
            boxShadow: "0 20px 40px rgba(0,0,0,0.3), 0 0 20px rgba(16,185,129,0.1)",
            position: "relative",
            overflow: "hidden",
            transition: "all 0.3s ease"
          }}>
            <div style={{
              position: "absolute",
              top: 0,
              left: 0,
              right: 0,
              height: "3px",
              background: "linear-gradient(90deg, #10b981, #34d399)",
              opacity: 1
            }} />
            <div style={{ display: "flex", alignItems: "center", gap: "12px" }}>
              <div style={{
                width: "42px",
                height: "42px",
                borderRadius: "14px",
                background: "linear-gradient(135deg, rgba(16,185,129,0.25), rgba(16,185,129,0.1))",
                border: "1px solid rgba(16,185,129,0.35)",
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                boxShadow: "0 4px 15px rgba(16,185,129,0.3)"
              }}>
                <LinkIcon size={20} color="#34d399" />
              </div>
              <h3 style={{ color: textTitle, margin: 0, fontSize: "18px" }}>Прокси</h3>
            </div>
            <p style={{ fontSize: "40px", fontWeight: "700", margin: "16px 0 12px", color: "#ffffff" }}>{stats.proxies.total}</p>
            <div style={{ display: "flex", justifyContent: "space-between", fontSize: "13px", color: textMuted }}>
              <span style={{ display: "flex", alignItems: "center", gap: "6px" }}>
                <span style={getBadgeStyle("active")}><SuccessIcon size={12} color="white" /> {stats.proxies.active}</span>
              </span>
              <span style={{ display: "flex", alignItems: "center", gap: "6px" }}>
                <span style={getBadgeStyle("failed")}><XIcon size={12} color="white" /> {stats.proxies.failed}</span>
              </span>
            </div>
          </div>
          <div className="stats-card warning" style={{
            padding: "24px",
            background: "linear-gradient(145deg, rgba(249,115,22,0.2) 0%, rgba(15,23,42,0.95) 100%)",
            borderRadius: "20px",
            border: "1px solid rgba(249,115,22,0.3)",
            flex: 1,
            minWidth: isPhoneView ? "0" : "200px",
            color: textPrimary,
            boxShadow: "0 20px 40px rgba(0,0,0,0.3), 0 0 20px rgba(249,115,22,0.1)",
            position: "relative",
            overflow: "hidden",
            transition: "all 0.3s ease"
          }}>
            <div style={{
              position: "absolute",
              top: 0,
              left: 0,
              right: 0,
              height: "3px",
              background: "linear-gradient(90deg, #f97316, #fb923c)",
              opacity: 1
            }} />
            <div style={{ display: "flex", alignItems: "center", gap: "12px" }}>
              <div style={{
                width: "42px",
                height: "42px",
                borderRadius: "14px",
                background: "linear-gradient(135deg, rgba(249,115,22,0.25), rgba(249,115,22,0.1))",
                border: "1px solid rgba(249,115,22,0.35)",
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                boxShadow: "0 4px 15px rgba(249,115,22,0.3)"
              }}>
                <MessageIcon size={20} color="#fb923c" />
              </div>
              <h3 style={{ color: textTitle, margin: 0, fontSize: "18px" }}>Сообщения</h3>
            </div>
            <p style={{ fontSize: "40px", fontWeight: "700", margin: "16px 0 12px", color: "#ffffff" }}>{stats.messages.total}</p>
            <div style={{ display: "flex", justifyContent: "space-between", fontSize: "13px", color: textMuted }}>
              <span style={{ display: "flex", alignItems: "center", gap: "6px" }}>
                <span style={getBadgeStyle("info")}><MessageIcon size={12} color="white" /> {stats.messages.today}</span>
              </span>
              <span style={{ display: "flex", alignItems: "center", gap: "6px" }}>
                <span style={getBadgeStyle("new")}><SuccessIcon size={12} color="white" /> {stats.messages.unread}</span>
              </span>
            </div>
          </div>
        </div>
      )}

      <div style={{
        marginTop: "40px",
        padding: "30px",
        ...cardStyle
      }}>
        <h3 style={{ color: textTitle, fontWeight: "700" }}>Быстрые действия</h3>
        <div className="quick-actions" style={{
          display: "grid",
          gridTemplateColumns: isPhoneView ? "1fr" : "repeat(auto-fit, minmax(220px, 1fr))",
          gap: "18px",
          marginTop: "20px"
        }}>
          {[
            {
              title: "Добавить аккаунт",
              subtitle: "Новые пользователи",
              icon: <PlusIcon size={22} />,
              color: "#8b5cf6",
              gradient: "linear-gradient(135deg, #8b5cf6, #7c3aed)",
              onClick: () => setShowAddAccountModal(true)
            },
            {
              title: "Добавить прокси",
              subtitle: "Подключить IP",
              icon: <LinkIcon size={22} />,
              color: "#06b6d4",
              gradient: "linear-gradient(135deg, #06b6d4, #0891b2)",
              onClick: () => setShowAddProxyModal(true)
            },
            {
              title: checkingAllProxies ? "Проверка..." : "Проверить все",
              subtitle: "Прокси-сервера",
              icon: <RefreshIcon size={22} />,
              color: "#10b981",
              gradient: "linear-gradient(135deg, #10b981, #059669)",
              onClick: checkAllProxies,
              disabled: checkingAllProxies
            },
            {
              title: "Проверить сообщения",
              subtitle: "Входящие",
              icon: <MessageIcon size={22} />,
              color: "#a855f7",
              gradient: "linear-gradient(135deg, #a855f7, #9333ea)",
              onClick: () => setActiveTab("messages")
            },
            {
              title: "Создать объявление",
              subtitle: "Публикация",
              icon: <EditIcon size={22} />,
              color: "#f59e0b",
              gradient: "linear-gradient(135deg, #f59e0b, #d97706)",
              onClick: () => setShowAdModal(true)
            },
            {
              title: "Активные объявления",
              subtitle: "Текущие",
              icon: <PackageIcon size={22} />,
              color: "#14b8a6",
              gradient: "linear-gradient(135deg, #14b8a6, #0d9488)",
              onClick: () => setActiveTab("active-ads")
            }
          ].map((action) => (
            <button
              key={action.title}
              onClick={action.onClick}
              disabled={action.disabled}
              className="quick-action-btn"
              style={{
                padding: "20px",
                borderRadius: "18px",
                border: `1px solid ${action.color}30`,
                background: "linear-gradient(145deg, rgba(30, 41, 59, 0.6) 0%, rgba(15, 23, 42, 0.9) 100%)",
                color: "white",
                display: "flex",
                flexDirection: "column",
                alignItems: "flex-start",
                gap: "14px",
                cursor: action.disabled ? "not-allowed" : "pointer",
                minHeight: "120px",
                boxShadow: `0 15px 30px rgba(0,0,0,0.3), 0 0 20px ${action.color}10`,
                transition: "all 0.3s cubic-bezier(0.4, 0, 0.2, 1)",
                position: "relative",
                overflow: "hidden"
              }}
              onMouseEnter={(e) => {
                if (!action.disabled) {
                  e.currentTarget.style.transform = "translateY(-6px)";
                  e.currentTarget.style.boxShadow = `0 20px 40px rgba(0,0,0,0.4), 0 0 30px ${action.color}30`;
                  e.currentTarget.style.borderColor = `${action.color}60`;
                }
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.transform = "translateY(0)";
                e.currentTarget.style.boxShadow = `0 15px 30px rgba(0,0,0,0.3), 0 0 20px ${action.color}10`;
                e.currentTarget.style.borderColor = `${action.color}30`;
              }}
            >
              <span style={{
                width: "52px",
                height: "52px",
                borderRadius: "16px",
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                background: action.gradient,
                fontSize: "22px",
                color: "white",
                boxShadow: `0 8px 20px ${action.color}50, 0 0 25px ${action.color}25`,
                transition: "all 0.3s ease"
              }}>
                {action.icon}
              </span>
              <div style={{ textAlign: "left" }}>
                <div style={{ fontWeight: "600", fontSize: "16px", marginBottom: "4px" }}>{action.title}</div>
                <div style={{ fontSize: "13px", color: "#94a3b8" }}>{action.subtitle}</div>
              </div>
            </button>
          ))}
        </div>
      </div>

      {/* Статус прокси на дашборде */}
      <div style={{
        marginTop: "40px",
        padding: "20px",
        ...cardStyle
      }}>
        <h3 style={{ color: textTitle, fontWeight: "700" }}>Статус прокси</h3>
        <div style={{ marginTop: "15px" }}>
          {proxies.slice(0, 3).map(proxy => (
            <div key={proxy.id} style={{
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
              padding: "16px 20px",
              background: "linear-gradient(145deg, rgba(30, 41, 59, 0.5) 0%, rgba(15, 23, 42, 0.8) 100%)",
              borderRadius: "16px",
              marginBottom: "10px",
              gap: "16px",
              border: "1px solid rgba(148,163,184,0.15)",
              color: textPrimary,
              transition: "all 0.3s ease"
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.transform = "translateX(4px)";
              e.currentTarget.style.borderColor = "rgba(148,163,184,0.3)";
              e.currentTarget.style.boxShadow = "0 10px 25px rgba(0,0,0,0.2)";
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.transform = "translateX(0)";
              e.currentTarget.style.borderColor = "rgba(148,163,184,0.15)";
              e.currentTarget.style.boxShadow = "none";
            }}
            >
              <div>
                <strong style={{ fontSize: "15px" }}>{proxy.name}</strong>
                <span style={{ marginLeft: "12px", color: textMuted, fontSize: "13px" }}>
                  {proxy.host}:{proxy.port}
                </span>
              </div>
              <div style={{ display: "flex", alignItems: "center", gap: "12px" }}>
                <span style={getBadgeStyle(proxy.status)}>
                  {proxy.status === "active" ? "✓ Работает" :
                     proxy.status === "failed" ? "✗ Не работает" : "⏳ Проверка"}
                </span>
                {proxy.checkResult?.location?.country && (
                  <span style={{ fontSize: "12px", color: textMuted }}>
                    {proxy.checkResult.location.country}
                  </span>
                )}
              </div>
            </div>
          ))}
          {proxies.length > 3 && (
            <button 
              onClick={() => setActiveTab("proxies")}
              style={{
                width: "100%",
                padding: "10px",
                background: "none",
                border: "1px dashed rgba(148,163,184,0.35)",
                borderRadius: "12px",
                cursor: "pointer",
                color: "#7dd3fc",
                marginTop: "10px"
              }}
            >
              Показать все прокси ({proxies.length})
            </button>
          )}
        </div>
      </div>
    </div>
  );

  const AccountsTab = () => (
    <div>
      <div className="section-header" style={{
        display: "flex",
        justifyContent: "space-between",
        alignItems: isPhoneView ? "stretch" : "center",
        flexDirection: isPhoneView ? "column" : "row",
        gap: isPhoneView ? "10px" : "0",
        marginBottom: "20px"
      }}>
        {renderSectionTitle(UserIcon, "Аккаунты", {
          iconColor: "#c4b5fd",
          background: "linear-gradient(135deg, rgba(139, 92, 246, 0.3), rgba(76, 29, 149, 0.28))",
          border: "1px solid rgba(167, 139, 250, 0.45)",
          boxShadow: "0 4px 15px rgba(139, 92, 246, 0.32)"
        })}
        <button
          className="primary-button"
          onClick={() => setShowAddAccountModal(true)}
          style={{
            padding: "10px 20px",
            color: "white",
            border: "none",
            cursor: "pointer",
            display: "flex",
            alignItems: "center",
            gap: "8px",
            width: isPhoneView ? "100%" : "auto",
            minWidth: 0,
            justifyContent: "center"
          }}
        >
          <PlusIcon size={18} />
          Добавить аккаунт
        </button>
      </div>

      <div style={{
        display: "grid",
        gridTemplateColumns: isPhoneView ? "1fr" : "repeat(auto-fill, minmax(340px, 1fr))",
        gap: "20px"
      }}>
        {accounts.map(account => {
          const metrics = accountMetrics[account.id] || { active: 0, total: 0, chats: 0, unread: 0 };
          const rawName = String(account.profileName || "").trim();
          const displayName = rawName && !/mein(e)? anzeigen|mein profil|meine anzeigen|profil und meine anzeigen/i.test(rawName)
            ? rawName
            : (account.username || "Аккаунт");
          return (
            <div
              key={account.id}
              style={{
                ...cardStyle,
                padding: "20px",
                transition: "all 0.3s ease",
                position: "relative"
              }}
              onMouseEnter={(e) => {
                e.currentTarget.style.transform = "translateY(-4px)";
                e.currentTarget.style.boxShadow = "0 25px 50px rgba(0,0,0,0.4), 0 0 40px rgba(139,92,246,0.15)";
                e.currentTarget.style.borderColor = "rgba(139,92,246,0.3)";
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.transform = "translateY(0)";
                e.currentTarget.style.boxShadow = "0 20px 50px rgba(0,0,0,0.4), 0 0 30px rgba(0,0,0,0.2)";
                e.currentTarget.style.borderColor = "rgba(148,163,184,0.15)";
              }}
            >
            {/* Header: Avatar + Name + Badge */}
            <div style={{ display: "flex", alignItems: "center", gap: "14px", marginBottom: "16px" }}>
              <div style={{
                width: "48px",
                height: "48px",
                borderRadius: "14px",
                background: "linear-gradient(135deg, #8b5cf6, #7c3aed)",
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                color: "white",
                fontSize: "20px",
                fontWeight: "700",
                flexShrink: 0
              }}>
                {displayName.charAt(0).toUpperCase()}
              </div>
              <div style={{ flex: 1, minWidth: 0 }}>
                <div style={{ display: "flex", alignItems: "center", gap: "10px", flexWrap: "wrap" }}>
                  <h3 style={{ margin: 0, fontSize: "16px", fontWeight: "700", color: textTitle }}>
                    {displayName}
                  </h3>
                  <span style={{
                    padding: "3px 10px",
                    borderRadius: "6px",
                    fontSize: "10px",
                    fontWeight: "600",
                    textTransform: "uppercase",
                    background: account.status === "active" ? "rgba(16, 185, 129, 0.15)" : account.status === "checking" ? "rgba(245, 158, 11, 0.15)" : "rgba(239, 68, 68, 0.15)",
                    color: account.status === "active" ? "#34d399" : account.status === "checking" ? "#fbbf24" : "#f87171",
                    border: `1px solid ${account.status === "active" ? "rgba(16, 185, 129, 0.3)" : account.status === "checking" ? "rgba(245, 158, 11, 0.3)" : "rgba(239, 68, 68, 0.3)"}`
                  }}>
                    {account.status === "active" ? "Mix" : account.status === "checking" ? "new" : "Top"}
                  </span>
                </div>
                <div style={{ fontSize: "13px", color: textMuted, marginTop: "2px", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                  {account.profileEmail || account.username || "—"}
                </div>
              </div>
              <div style={{ position: "relative" }}>
                <button
                  data-account-menu-button
                  onClick={(e) => {
                    e.stopPropagation();
                    setOpenAccountMenuId(openAccountMenuId === account.id ? null : account.id);
                  }}
                  style={{
                    background: "transparent",
                    border: "none",
                    color: textMuted,
                    cursor: "pointer",
                    padding: "4px",
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center"
                  }}
                >
                  <svg width="20" height="20" viewBox="0 0 24 24" fill="currentColor">
                    <circle cx="12" cy="5" r="2"/>
                    <circle cx="12" cy="12" r="2"/>
                    <circle cx="12" cy="19" r="2"/>
                  </svg>
                </button>
                {openAccountMenuId === account.id && (
                  <div
                    data-account-menu
                    onClick={(e) => e.stopPropagation()}
                    style={{
                      position: "absolute",
                      top: "28px",
                      right: 0,
                      background: "rgba(15, 23, 42, 0.98)",
                      border: "1px solid rgba(148,163,184,0.2)",
                      borderRadius: "10px",
                      padding: "6px",
                      display: "flex",
                      flexDirection: "column",
                      gap: "6px",
                      minWidth: "140px",
                      zIndex: 50,
                      boxShadow: "0 10px 30px rgba(0,0,0,0.45)"
                    }}
                  >
                    <button
                      onClick={() => handleRefreshAccount(account.id)}
                      style={{
                        background: "transparent",
                        border: "1px solid rgba(59,130,246,0.25)",
                        color: "#60a5fa",
                        padding: "8px 10px",
                        borderRadius: "8px",
                        cursor: "pointer",
                        fontSize: "13px",
                        fontWeight: "600"
                      }}
                    >
                      Обновить
                    </button>
                    <button
                      onClick={() => {
                        setOpenAccountMenuId(null);
                        handleDeleteAccount(account.id);
                      }}
                      style={{
                        background: "transparent",
                        border: "1px solid rgba(239,68,68,0.3)",
                        color: "#f87171",
                        padding: "8px 10px",
                        borderRadius: "8px",
                        cursor: "pointer",
                        fontSize: "13px",
                        fontWeight: "600"
                      }}
                    >
                      Удалить
                    </button>
                  </div>
                )}
              </div>
            </div>

            {/* Status row */}
            <div style={{ display: "flex", alignItems: "center", gap: "16px", marginBottom: "12px", flexWrap: "wrap" }}>
              <div style={{ display: "flex", alignItems: "center", gap: "6px" }}>
                <span style={{
                  width: "8px",
                  height: "8px",
                  borderRadius: "50%",
                  background: account.status === "active" ? "#22c55e" : "#ef4444"
                }} />
                <span style={{ fontSize: "12px", color: account.status === "active" ? "#22c55e" : "#ef4444", fontWeight: "600" }}>
                  {account.status === "active" ? "В СЕТИ" : "НЕ В СЕТИ"}
                </span>
              </div>
              <div style={{ display: "flex", alignItems: "center", gap: "6px", color: textMuted, fontSize: "12px" }}>
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                  <circle cx="12" cy="12" r="10"/>
                  <line x1="2" y1="12" x2="22" y2="12"/>
                  <path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/>
                </svg>
                {account.proxy || "proxyless"}
              </div>
            </div>

            {/* Date */}
            <div style={{ display: "flex", alignItems: "center", gap: "6px", color: textMuted, fontSize: "12px", marginBottom: "16px" }}>
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                <rect x="3" y="4" width="18" height="18" rx="2" ry="2"/>
                <line x1="16" y1="2" x2="16" y2="6"/>
                <line x1="8" y1="2" x2="8" y2="6"/>
                <line x1="3" y1="10" x2="21" y2="10"/>
              </svg>
              {account.added || "—"}
            </div>

            {/* Stats row */}
            <div style={{
              display: "grid",
              gridTemplateColumns: "repeat(4, 1fr)",
              gap: "8px",
              marginBottom: "16px",
              padding: "12px",
              background: "rgba(15, 23, 42, 0.6)",
              borderRadius: "12px",
              border: "1px solid rgba(148,163,184,0.1)"
            }}>
              {[
                { icon: "📦", value: metrics.active, label: "Актив" },
                { icon: "📈", value: metrics.total, label: "Всего" },
                { icon: "💬", value: metrics.chats, label: "Чаты" },
                { icon: "🔔", value: metrics.unread, label: "Непроч." }
              ].map((stat, idx) => (
                <div key={idx} style={{ textAlign: "center" }}>
                  <div style={{ fontSize: "14px", marginBottom: "4px", opacity: 0.7 }}>{stat.icon}</div>
                  <div style={{ fontSize: "20px", fontWeight: "700", color: textTitle }}>{stat.value}</div>
                  <div style={{ fontSize: "10px", color: textMuted }}>{stat.label}</div>
                </div>
              ))}
            </div>

            {/* Open button */}
            <button
              onClick={() => {
                const url = account.profileUrl || "https://www.kleinanzeigen.de/m-meine-anzeigen.html";
                window.open(url, "_blank", "noopener,noreferrer");
              }}
              style={{
                width: "100%",
                padding: "12px",
                background: "transparent",
                border: "1px solid rgba(139, 92, 246, 0.3)",
                borderRadius: "10px",
                color: "#a78bfa",
                fontSize: "14px",
                fontWeight: "600",
                cursor: "pointer",
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                gap: "8px",
                transition: "all 0.2s ease"
              }}
              onMouseEnter={(e) => {
                e.currentTarget.style.background = "rgba(139, 92, 246, 0.1)";
                e.currentTarget.style.borderColor = "rgba(139, 92, 246, 0.5)";
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.background = "transparent";
                e.currentTarget.style.borderColor = "rgba(139, 92, 246, 0.3)";
              }}
            >
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/>
                <polyline points="15 3 21 3 21 9"/>
                <line x1="10" y1="14" x2="21" y2="3"/>
              </svg>
              Открыть
            </button>
          </div>
          );
        })}

        {accounts.length === 0 && (
          <div style={{
            ...cardStyle,
            padding: "48px",
            textAlign: "center",
            gridColumn: "1 / -1"
          }}>
            <div style={{
              width: "64px",
              height: "64px",
              borderRadius: "16px",
              background: "rgba(139, 92, 246, 0.1)",
              border: "1px solid rgba(139, 92, 246, 0.2)",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              margin: "0 auto 16px"
            }}>
              <UserIcon size={32} color="#a78bfa" />
            </div>
            <h3 style={{ color: textTitle, marginBottom: "8px", fontWeight: "700" }}>Нет аккаунтов</h3>
            <p style={{ color: textMuted, marginBottom: "20px" }}>Добавьте свой первый аккаунт для начала работы</p>
            <button
              className="primary-button"
              onClick={() => setShowAddAccountModal(true)}
              style={{
                padding: "12px 24px",
                display: "inline-flex",
                alignItems: "center",
                gap: "8px"
              }}
            >
              <PlusIcon size={18} />
              Добавить аккаунт
            </button>
          </div>
        )}
      </div>
    </div>
  );

  const ProxiesTab = () => (
    <div>
      <div className="section-header" style={{
        display: "flex",
        justifyContent: "space-between",
        alignItems: isPhoneView ? "stretch" : "center",
        flexDirection: isPhoneView ? "column" : "row",
        gap: isPhoneView ? "10px" : "0",
        marginBottom: "20px"
      }}>
        {renderSectionTitle(LinkIcon, "Прокси серверы", {
          iconColor: "#67e8f9",
          background: "linear-gradient(135deg, rgba(6, 182, 212, 0.3), rgba(8, 145, 178, 0.28))",
          border: "1px solid rgba(34, 211, 238, 0.45)",
          boxShadow: "0 4px 15px rgba(6, 182, 212, 0.32)"
        })}
        <div className="section-header-actions" style={{
          display: "flex",
          gap: "12px",
          flexDirection: isPhoneView ? "column" : "row",
          width: isPhoneView ? "100%" : "auto"
        }}>
          <button 
            className="primary-button"
            onClick={checkAllProxies}
            disabled={checkingAllProxies}
            style={{
              padding: "10px 20px",
              color: "white",
              border: "none",
              cursor: checkingAllProxies ? "not-allowed" : "pointer",
              width: isPhoneView ? "100%" : "auto",
              minWidth: isPhoneView ? "0" : "180px",
              display: "flex",
              alignItems: "center",
              gap: "8px",
              justifyContent: "center"
            }}
          >
            <RefreshIcon size={18} />
            {checkingAllProxies ? "Проверка..." : "Проверить все"}
          </button>
          <button 
            className="primary-button"
            onClick={() => setShowAddProxyModal(true)}
            style={{
              padding: "10px 20px",
              color: "white",
              border: "none",
              cursor: "pointer",
              display: "flex",
              alignItems: "center",
              gap: "8px",
              minWidth: isPhoneView ? "0" : "180px",
              width: isPhoneView ? "100%" : "auto",
              justifyContent: "center"
            }}
          >
            <PlusIcon size={18} />
            Добавить прокси
          </button>
        </div>
      </div>

      <div style={{
        ...cardStyle,
        padding: "20px",
        marginBottom: "20px"
      }}>
        <div style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: isPhoneView ? "flex-start" : "center",
          flexDirection: isPhoneView ? "column" : "row",
          gap: isPhoneView ? "12px" : "0"
        }}>
          <h3 style={{ margin: 0, color: textTitle, fontWeight: "700" }}>Статистика прокси</h3>
          <div style={{ display: "flex", gap: "15px", flexWrap: isPhoneView ? "wrap" : "nowrap", justifyContent: isPhoneView ? "space-between" : "flex-start" }}>
            <div style={{ textAlign: "center", minWidth: isPhoneView ? "86px" : "0" }}>
              <div style={{ fontSize: "24px", fontWeight: "bold", color: "#52c41a" }}>
                {proxies.filter(p => p.status === "active").length}
              </div>
              <div style={{ fontSize: "12px", color: textMuted }}>Работают</div>
            </div>
            <div style={{ textAlign: "center", minWidth: isPhoneView ? "86px" : "0" }}>
              <div style={{ fontSize: "24px", fontWeight: "bold", color: "#ff4d4f" }}>
                {proxies.filter(p => p.status === "failed").length}
              </div>
              <div style={{ fontSize: "12px", color: textMuted }}>Не работают</div>
            </div>
            <div style={{ textAlign: "center", minWidth: isPhoneView ? "86px" : "0" }}>
              <div style={{ fontSize: "24px", fontWeight: "bold", color: "#faad14" }}>
                {proxies.filter(p => p.status === "unknown" || p.status === "checking").length}
              </div>
              <div style={{ fontSize: "12px", color: textMuted }}>Не проверены</div>
            </div>
          </div>
        </div>
      </div>

      <div style={{ display: "flex", flexDirection: "column", gap: "20px" }}>
        {proxies.map(proxy => (
          <div key={proxy.id} style={{ position: "relative" }}>
            <ProxyChecker
              proxy={proxy}
              onCheckComplete={(result) => handleProxyCheckComplete(proxy.id, result)}
              onDelete={() => handleDeleteProxy(proxy.id)}
              isPhoneView={isPhoneView}
            />
          </div>
        ))}
      </div>
    </div>
  );

  const tabs = [
    { id: "dashboard", label: "Дашборд", icon: <DashboardIcon size={18} />, color: "#60a5fa" },
    { id: "accounts", label: "Аккаунты", icon: <UserIcon size={18} />, color: "#a78bfa" },
    { id: "proxies", label: "Прокси", icon: <LinkIcon size={18} />, color: "#34d399" },
    { id: "messages", label: "Сообщения", icon: <MessageIcon size={18} />, color: "#fb923c" },
    { id: "active-ads", label: "Объявления", icon: <PackageIcon size={18} />, color: "#facc15" }
  ];

  return (
    <div className={`app-root${isPhoneView ? " phone-view" : ""}`} style={{
      minHeight: "100vh",
      background: "radial-gradient(circle at top, #111827 0%, #0b1220 45%, #070b14 100%)",
      fontFamily: "Inter, 'Segoe UI', sans-serif",
      color: textPrimary
    }}>
      {/* Header */}
      <div className="app-header" style={{
        background: "linear-gradient(180deg, rgba(15,23,42,0.95) 0%, rgba(15,23,42,0.85) 100%)",
        padding: isPhoneView ? "0 12px" : "0 24px",
        boxShadow: "0 10px 40px rgba(0,0,0,0.4), 0 0 40px rgba(0,0,0,0.2)",
        backdropFilter: "blur(12px)",
        borderBottom: "1px solid rgba(148,163,184,0.15)",
        position: "relative",
        zIndex: 3000
      }}>
        <div className="app-header-inner" style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          maxWidth: "1200px",
          margin: "0 auto",
          height: isPhoneView ? "64px" : "70px",
          gap: isPhoneView ? "10px" : "16px"
        }}>
          <div style={{
            display: "flex",
            alignItems: "center",
            gap: isPhoneView ? "10px" : "14px",
            minWidth: 0
          }}>
            <div style={{
              width: isPhoneView ? "34px" : "38px",
              height: isPhoneView ? "34px" : "38px",
              borderRadius: "14px",
              background: "rgba(167, 139, 250, 0.12)",
              border: "1px solid rgba(167, 139, 250, 0.28)",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              boxShadow: "0 0 18px rgba(167, 139, 250, 0.18)",
              flex: "0 0 auto"
            }}>
              <ArmorLogoIcon size={isPhoneView ? 20 : 24} />
            </div>
            <div style={{ display: "flex", flexDirection: "column", minWidth: 0, lineHeight: 1.05 }}>
              <h1 style={{
                margin: 0,
                color: "#f8fafc",
                fontSize: isPhoneView ? "18px" : "24px",
                fontWeight: "800",
                letterSpacing: "-0.02em",
                background: "linear-gradient(135deg, #f8fafc 0%, #a78bfa 60%, #7dd3fc 100%)",
                WebkitBackgroundClip: "text",
                WebkitTextFillColor: "transparent",
                whiteSpace: "nowrap",
                overflow: "hidden",
                textOverflow: "ellipsis"
              }}>armor</h1>
              <div style={{
                marginTop: "4px",
                fontSize: "12px",
                fontWeight: "600",
                color: "#94a3b8",
                letterSpacing: "0.02em",
                whiteSpace: "nowrap"
              }} title={APP_VERSION}>{APP_VERSION}</div>
            </div>
          </div>
          {tokenStatus.state === "valid" && (
          <div style={{ position: "relative" }} ref={profilePanelRef}>
            <button
              onClick={() => setShowProfilePanel((prev) => !prev)}
              style={{
                width: "44px",
                height: "44px",
                borderRadius: "14px",
                border: "1px solid rgba(148,163,184,0.25)",
                background: "linear-gradient(145deg, rgba(30, 41, 59, 0.6) 0%, rgba(15, 23, 42, 0.8) 100%)",
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                cursor: "pointer",
                color: "#e2e8f0",
                transition: "all 0.3s cubic-bezier(0.4, 0, 0.2, 1)",
                boxShadow: "0 4px 12px rgba(0,0,0,0.2)"
              }}
              onMouseEnter={(e) => {
                e.currentTarget.style.borderColor = "rgba(59,130,246,0.5)";
                e.currentTarget.style.boxShadow = "0 6px 20px rgba(59,130,246,0.25)";
                e.currentTarget.style.transform = "translateY(-2px)";
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.borderColor = "rgba(148,163,184,0.25)";
                e.currentTarget.style.boxShadow = "0 4px 12px rgba(0,0,0,0.2)";
                e.currentTarget.style.transform = "translateY(0)";
              }}
            >
              <UserIcon size={20} />
            </button>

            {showProfilePanel && (
              <div className="profile-panel" style={{
                position: "fixed",
                top: "80px",
                right: "24px",
                width: "320px",
                background: "#0f172a",
                border: "1px solid rgba(148,163,184,0.3)",
                borderRadius: "16px",
                boxShadow: "0 25px 50px rgba(0,0,0,0.8), 0 0 0 1px rgba(148,163,184,0.1)",
                padding: "18px",
                zIndex: 99999
              }}>
                <div style={{ display: "flex", alignItems: "center", gap: "12px", marginBottom: "14px" }}>
                  <div style={{
                    width: "44px",
                    height: "44px",
                    borderRadius: "14px",
                    background: "rgba(59,130,246,0.18)",
                    border: "1px solid rgba(59,130,246,0.35)",
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center",
                    color: "#7dd3fc",
                    fontWeight: "600"
                  }}>
                    <UserIcon size={18} />
                  </div>
                  <div>
                    <div style={{ fontWeight: "600", color: "#f8fafc" }}>Ваш профиль</div>
                    <div style={{ fontSize: "12px", color: "#94a3b8" }}>
                      {tokenStatus.state === "valid"
                        ? (tokenStatus.expiresAt
                          ? `Подписка до ${new Date(tokenStatus.expiresAt).toLocaleDateString()}`
                          : "Подписка: бессрочно")
                        : "Подписка не активна"}
                    </div>
                  </div>
                </div>

                <div style={{
                  background: "rgba(15,23,42,0.7)",
                  borderRadius: "12px",
                  border: "1px solid rgba(148,163,184,0.2)",
                  padding: "12px",
                  display: "flex",
                  flexDirection: "column",
                  gap: "10px"
                }}>
                  <div>
                    <div style={{ fontSize: "12px", color: "#94a3b8", marginBottom: "6px" }}>
                      Токен доступа
                    </div>
                    <input
                      type="text"
                      readOnly
                      value={accessToken || ""}
                      placeholder="Токен не задан"
                      style={{
                        width: "100%",
                        padding: "8px 10px",
                        borderRadius: "10px",
                        border: "1px solid rgba(148,163,184,0.3)",
                        background: "rgba(2,6,23,0.6)",
                        color: "#e2e8f0",
                        fontSize: "12px"
                      }}
                    />
                  </div>

                  <button
                    onClick={() => window.open("https://t.me/anklang", "_blank", "noopener,noreferrer")}
                    style={{
                      width: "100%",
                      padding: "10px 12px",
                      borderRadius: "10px",
                      border: "none",
                      cursor: "pointer",
                      background: "linear-gradient(135deg, #2563eb 0%, #7c3aed 100%)",
                      color: "white",
                      fontWeight: "600",
                      fontSize: "13px"
                    }}
                  >
                    Продлить подписку
                  </button>
                </div>
              </div>
            )}
          </div>
          )}
        </div>
      </div>

      {authError && (
        <div style={{
          background: "rgba(239,68,68,0.15)",
          color: "#fca5a5",
          borderBottom: "1px solid rgba(239,68,68,0.3)",
          padding: "8px 24px",
          fontSize: "13px"
        }}>
          {authError}
        </div>
      )}

      {tokenStatus.state !== "valid" && (
        <div className="token-gate-overlay" style={{
          position: "fixed",
          inset: 0,
          background: "radial-gradient(circle at top, rgba(15,23,42,0.85), rgba(2,6,23,0.92))",
          backdropFilter: "blur(10px)",
          zIndex: 2000,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          padding: "20px"
        }}>
          <div className="token-gate-card" style={{
            width: "100%",
            maxWidth: "520px",
            background: "linear-gradient(135deg, rgba(17,24,39,0.98) 0%, rgba(8,12,20,0.98) 100%)",
            borderRadius: "20px",
            border: "1px solid rgba(148,163,184,0.2)",
            boxShadow: "0 30px 60px rgba(2,6,23,0.6)",
            padding: "28px 30px",
            color: "#e2e8f0",
            position: "relative"
          }}>
            <div className="token-gate-icon" style={{
              width: "56px",
              height: "56px",
              borderRadius: "16px",
              background: "rgba(59,130,246,0.15)",
              border: "1px solid rgba(59,130,246,0.35)",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              fontSize: "24px",
              marginBottom: "16px"
            }}>
              🔒
            </div>
            <h2 className="token-gate-title" style={{ margin: 0, fontSize: "22px" }}>Требуется токен доступа</h2>
            <p className="token-gate-desc" style={{ margin: "8px 0 18px", color: "#94a3b8", fontSize: "14px" }}>
              Введите действующий токен, чтобы разблокировать функции менеджера.
            </p>

            <div className="token-gate-form" style={{
              display: "flex",
              gap: "10px",
              flexDirection: isPhoneView ? "column" : "row"
            }}>
              <input
                className="token-gate-input"
                type="password"
                placeholder="Введите токен"
                value={accessTokenDraft}
                onChange={(e) => setAccessTokenDraft(e.target.value)}
                style={{
                  flex: 1,
                  padding: "12px 14px",
                  borderRadius: "12px",
                  border: "1px solid rgba(148,163,184,0.3)",
                  background: "rgba(15,23,42,0.7)",
                  color: "#e2e8f0",
                  fontSize: "14px",
                  width: isPhoneView ? "100%" : "auto"
                }}
              />
              <button
                className="token-gate-submit"
                onClick={applyAccessToken}
                style={{
                  padding: "12px 16px",
                  borderRadius: "12px",
                  border: "none",
                  cursor: "pointer",
                  background: "linear-gradient(135deg, #2563eb 0%, #7c3aed 100%)",
                  color: "white",
                  fontWeight: "600",
                  fontSize: "14px",
                  whiteSpace: isPhoneView ? "normal" : "nowrap",
                  width: isPhoneView ? "100%" : "auto"
                }}
              >
                Применить токен
              </button>
            </div>

            <div className="token-gate-status" style={{
              marginTop: "12px",
              fontSize: "12px",
              color: tokenStatus.state === "missing" ? "#94a3b8" : "#fca5a5"
            }}>
              {tokenStatus.message || " "}
            </div>
          </div>
        </div>
      )}

      {/* Navigation */}
      <div className="app-nav" style={{
        background: "linear-gradient(180deg, rgba(15,23,42,0.9) 0%, rgba(15,23,42,0.8) 100%)",
        marginTop: "1px",
        borderBottom: "1px solid rgba(148,163,184,0.12)"
      }}>
        <div className="app-nav-inner" style={{
          maxWidth: "1200px",
          margin: "0 auto",
          padding: isPhoneView ? "8px 12px" : "8px 24px",
          gap: "8px"
        }}>
          {isPhoneView && (
            <button
              type="button"
              className={`mobile-nav-toggle ${isMobileNavOpen ? "open" : ""}`}
              onClick={() => setIsMobileNavOpen((prev) => !prev)}
              aria-expanded={isMobileNavOpen}
              aria-label={isMobileNavOpen ? "Скрыть разделы" : "Показать разделы"}
              style={{
                width: "100%",
                maxWidth: "200px",
                display: "inline-flex",
                alignItems: "center",
                justifyContent: "center",
                gap: "10px",
                padding: "10px 14px",
                borderRadius: "12px",
                border: isMobileNavOpen
                  ? "1px solid rgba(125, 211, 252, 0.45)"
                  : "1px solid rgba(148, 163, 184, 0.3)",
                background: "linear-gradient(145deg, rgba(15,23,42,0.85), rgba(2,6,23,0.85))",
                color: "#e2e8f0",
                fontWeight: "700",
                fontSize: "13px",
                boxShadow: isMobileNavOpen ? "0 0 20px rgba(59, 130, 246, 0.2)" : "none"
              }}
            >
              <span className="mobile-nav-burger" aria-hidden="true">
                <span />
                <span />
                <span />
              </span>
              <span>{isMobileNavOpen ? "Скрыть разделы" : "Разделы"}</span>
            </button>
          )}

          {!isPhoneView && (
            <div
              className="app-nav-list"
              style={{
                display: "flex",
                flexDirection: "row",
                alignItems: "center",
                flexWrap: "nowrap",
                gap: "8px",
                width: "max-content"
              }}
            >
              {tabs.map(tab => (
              <button
                key={tab.id}
                onClick={() => handleTabSelect(tab.id)}
                className={`nav-button ${activeTab === tab.id ? 'active' : ''}`}
                style={{
                  "--tab-color": tab.color,
                  padding: "12px 18px",
                  borderRadius: "8px",
                  cursor: "pointer",
                  fontSize: "14px",
                  fontWeight: "700",
                  display: "flex",
                  alignItems: "center",
                  gap: "10px",
                  whiteSpace: "nowrap",
                  background: "transparent",
                  border: "none",
                  boxShadow: "none",
                  color: "#ffffff",
                  WebkitTextFillColor: "#ffffff"
                }}
              >
                <span className="tab-icon" style={{ color: tab.color }}>{tab.icon}</span>
                {tab.label}
              </button>
            ))}
            </div>
          )}
        </div>

        {isPhoneView && (
          <div
            className={`app-nav-mobile-panel ${isMobileNavOpen ? "open" : ""}`}
            style={{
              display: isMobileNavOpen ? "block" : "none",
              borderTop: "1px solid rgba(148, 163, 184, 0.12)",
              background: "linear-gradient(180deg, rgba(15,23,42,0.94), rgba(2,6,23,0.9))"
            }}
          >
            <div
              className="app-nav-mobile-inner"
              style={{
                maxWidth: "1200px",
                margin: "0 auto",
                padding: "10px 12px 12px",
                display: "grid",
                gridTemplateColumns: "1fr",
                gap: "8px"
              }}
            >
              {tabs.map((tab) => (
                <button
                  key={`mobile-${tab.id}`}
                  onClick={() => handleTabSelect(tab.id)}
                  className={`mobile-nav-item ${activeTab === tab.id ? "active" : ""}`}
                  style={{
                    "--tab-color": tab.color,
                    width: "100%",
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "flex-start",
                    gap: "10px",
                    padding: "11px 12px",
                    borderRadius: "12px",
                    border: activeTab === tab.id
                      ? `1px solid ${tab.color}`
                      : "1px solid rgba(148, 163, 184, 0.2)",
                    background: activeTab === tab.id
                      ? "rgba(30, 41, 59, 0.85)"
                      : "rgba(15, 23, 42, 0.55)",
                    color: "#e2e8f0",
                    fontSize: "14px",
                    fontWeight: "700",
                    boxShadow: "none"
                  }}
                >
                  <span className="tab-icon" style={{ color: tab.color }}>{tab.icon}</span>
                  {tab.label}
                </button>
              ))}
            </div>
          </div>
        )}
      </div>

      {/* Content */}
      <div className="app-content" style={{
        maxWidth: "1200px",
        margin: isPhoneView ? "12px auto 0" : "24px auto 0",
        padding: isPhoneView ? "0 12px" : "0 24px",
        paddingBottom: isPhoneView ? "28px" : "48px"
      }}>
        {activeTab === "dashboard" && <DashboardTab />}
        {activeTab === "accounts" && <AccountsTab />}
        {activeTab === "proxies" && <ProxiesTab />}
        {activeTab === "messages" && <MessagesTab />}
        {activeTab === "active-ads" && <ActiveAdsTab />}

      </div>

      {/* Модальные окна */}
      {showAddAccountModal && (
        <AddAccountModal
          isOpen={showAddAccountModal}
          onClose={() => setShowAddAccountModal(false)}
          onSuccess={handleAddAccount}
          proxies={proxies}
        />
      )}

      {showAddProxyModal && (
        <AddProxyModal
          isOpen={showAddProxyModal}
          onClose={() => setShowAddProxyModal(false)}
          onSuccess={handleAddProxy}
        />
      )}

      {showAdModal && (
        <AdModal
          isOpen={showAdModal}
          onClose={() => setShowAdModal(false)}
          onSubmit={handleCreateAd}
          publishing={publishingAd}
          accounts={accounts}
          categories={categories}
          categoriesUpdatedAt={categoriesUpdatedAt}
          onRefreshCategories={loadCategories}
          loadingCategories={loadingCategories}
          extraFields={extraFields}
          extraFieldValues={extraFieldValues}
          setExtraFieldValues={setExtraFieldValues}
          loadingExtraFields={loadingExtraFields}
          extraFieldsError={extraFieldsError}
          newAd={newAd}
          setNewAd={setNewAd}
          adImages={adImages}
          setAdImages={setAdImages}
        />
      )}
    </div>
  );
}

export default App;
