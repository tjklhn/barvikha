import React, { useEffect, useState } from "react";
import { apiFetchJson } from "../api";
import { PackageIcon, RefreshIcon } from "./Icons";

const ACTIVE_ADS_CACHE_KEY = "kl_active_ads_cache_v1";
const ACTIVE_ADS_CACHE_TTL_MS = 90 * 1000;

const ActiveAdsTab = () => {
  const [ads, setAds] = useState([]);
  const [loading, setLoading] = useState(false);
  const [actionBusy, setActionBusy] = useState({});

  // Унифицированные стили для плашек
  const getBadgeStyle = (status) => {
    const baseStyle = {
      padding: "5px 14px",
      borderRadius: "9999px",
      fontSize: "11px",
      fontWeight: "600",
      textTransform: "uppercase",
      letterSpacing: "0.5px",
      display: "inline-flex",
      alignItems: "center",
      gap: "6px",
      transition: "all 0.2s ease"
    };

    const statusLower = (status || "aktiv").toLowerCase();

    if (statusLower === "aktiv" || statusLower === "active") {
      return {
        ...baseStyle,
        background: "linear-gradient(135deg, #10b981, #059669)",
        color: "white",
        border: "1px solid rgba(16, 185, 129, 0.3)",
        boxShadow: "0 0 12px rgba(16, 185, 129, 0.35)"
      };
    } else if (statusLower === "reserviert" || statusLower === "reserved") {
      return {
        ...baseStyle,
        background: "linear-gradient(135deg, #f59e0b, #d97706)",
        color: "white",
        border: "1px solid rgba(245, 158, 11, 0.3)",
        boxShadow: "0 0 12px rgba(245, 158, 11, 0.35)"
      };
    } else if (statusLower === "gelöscht" || statusLower === "deleted") {
      return {
        ...baseStyle,
        background: "linear-gradient(135deg, #ef4444, #dc2626)",
        color: "white",
        border: "1px solid rgba(239, 68, 68, 0.3)",
        boxShadow: "0 0 12px rgba(239, 68, 68, 0.35)"
      };
    }
    return {
      ...baseStyle,
      background: "linear-gradient(135deg, #475569, #334155)",
      color: "#e2e8f0",
      border: "1px solid rgba(148, 163, 184, 0.2)"
    };
  };

  const readCachedAds = () => {
    if (typeof window === "undefined" || !window.localStorage) return null;
    try {
      const raw = window.localStorage.getItem(ACTIVE_ADS_CACHE_KEY);
      if (!raw) return null;
      const parsed = JSON.parse(raw);
      if (!parsed || typeof parsed !== "object") return null;
      const savedAt = Number(parsed.savedAt || 0);
      const list = Array.isArray(parsed.ads) ? parsed.ads : [];
      if (!savedAt || Date.now() - savedAt > ACTIVE_ADS_CACHE_TTL_MS) return null;
      return list;
    } catch {
      return null;
    }
  };

  const writeCachedAds = (list) => {
    if (typeof window === "undefined" || !window.localStorage) return;
    try {
      window.localStorage.setItem(ACTIVE_ADS_CACHE_KEY, JSON.stringify({
        savedAt: Date.now(),
        ads: Array.isArray(list) ? list : []
      }));
    } catch {
      // ignore cache write issues
    }
  };

  const loadAds = async ({ force = false, useLocalCache = true } = {}) => {
    if (useLocalCache && !force) {
      const cached = readCachedAds();
      if (cached && cached.length) {
        setAds(cached);
      }
    }
      setLoading(true);
    try {
      const suffix = force ? "?force=1" : "";
      const data = await apiFetchJson(`/api/ads/active${suffix}`, { timeoutMs: 180000 });
      const list = Array.isArray(data?.ads) ? data.ads : [];
      const seen = new Set();
      const deduped = [];
      for (const ad of list) {
        const key = ad.href
          || `${ad.accountId || ""}|${ad.title || ""}|${ad.price || ""}|${ad.image || ""}`;
        if (seen.has(key)) continue;
        seen.add(key);
        deduped.push(ad);
      }
      setAds(deduped);
      writeCachedAds(deduped);
    } catch (error) {
      console.error("Ошибка загрузки активных объявлений:", error);
      setAds([]);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadAds({ force: false, useLocalCache: true });
  }, []);

  const updateActionBusy = (key, value) => {
    setActionBusy((prev) => ({ ...prev, [key]: value }));
  };

  const handleReserve = async (ad) => {
    if (!ad?.adId || !ad?.accountId) {
      alert("Не удалось определить объявление для резерва.");
      return;
    }
    const key = `reserve-${ad.adId}`;
    updateActionBusy(key, true);
    try {
      const result = await apiFetchJson(`/api/ads/${encodeURIComponent(ad.adId)}/reserve`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          accountId: ad.accountId,
          adHref: ad.href || "",
          adTitle: ad.title || ""
        })
      });
      if (!result?.success) {
        alert("Ошибка резерва: " + (result?.error || "Не удалось зарезервировать"));
      } else {
        await loadAds({ force: true, useLocalCache: false });
      }
    } catch (error) {
      console.error("Ошибка резерва:", error);
      alert("Ошибка резерва: " + error.message);
    } finally {
      updateActionBusy(key, false);
    }
  };

  const handleActivate = async (ad) => {
    if (!ad?.adId || !ad?.accountId) {
      alert("Не удалось определить объявление для активации.");
      return;
    }
    const key = `activate-${ad.adId}`;
    updateActionBusy(key, true);
    try {
      const result = await apiFetchJson(`/api/ads/${encodeURIComponent(ad.adId)}/activate`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          accountId: ad.accountId,
          adHref: ad.href || "",
          adTitle: ad.title || ""
        })
      });
      if (!result?.success) {
        alert("Ошибка активации: " + (result?.error || "Не удалось активировать"));
      } else {
        await loadAds({ force: true, useLocalCache: false });
      }
    } catch (error) {
      console.error("Ошибка активации:", error);
      alert("Ошибка активации: " + error.message);
    } finally {
      updateActionBusy(key, false);
    }
  };

  const handleDelete = async (ad) => {
    if (!ad?.adId || !ad?.accountId) {
      alert("Не удалось определить объявление для удаления.");
      return;
    }
    if (!window.confirm(`Удалить объявление "${ad.title}"?`)) return;
    const key = `delete-${ad.adId}`;
    updateActionBusy(key, true);
    try {
      const result = await apiFetchJson(`/api/ads/${encodeURIComponent(ad.adId)}/delete`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          accountId: ad.accountId,
          adHref: ad.href || "",
          adTitle: ad.title || ""
        })
      });
      if (!result?.success) {
        alert("Ошибка удаления: " + (result?.error || "Не удалось удалить"));
      } else {
        await loadAds({ force: true, useLocalCache: false });
      }
    } catch (error) {
      console.error("Ошибка удаления:", error);
      alert("Ошибка удаления: " + error.message);
    } finally {
      updateActionBusy(key, false);
    }
  };

  return (
    <div>
      <div className="section-header" style={{
        display: "flex",
        justifyContent: "space-between",
        alignItems: "center",
        marginBottom: "24px"
      }}>
        <h2 style={{
          margin: 0,
          color: "#f8fafc",
          fontSize: "24px",
          fontWeight: "700",
          display: "flex",
          alignItems: "center",
          gap: "12px"
        }}>
          <div style={{
            width: "42px",
            height: "42px",
            borderRadius: "14px",
            background: "linear-gradient(135deg, rgba(250, 204, 21, 0.2), rgba(250, 204, 21, 0.1))",
            border: "1px solid rgba(250, 204, 21, 0.3)",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            boxShadow: "0 4px 15px rgba(250, 204, 21, 0.2)"
          }}>
            <PackageIcon size={22} color="#facc15" />
          </div>
          Активные объявления
        </h2>
        <button
          className="primary-button"
          onClick={() => loadAds({ force: true, useLocalCache: false })}
          disabled={loading}
          style={{
            padding: "12px 24px",
            color: "white",
            border: "none",
            cursor: loading ? "not-allowed" : "pointer",
            fontSize: "14px",
            display: "flex",
            alignItems: "center",
            gap: "8px",
            fontWeight: "600"
          }}
        >
          <RefreshIcon size={16} style={{ animation: loading ? "spin 1s linear infinite" : "none" }} />
          {loading ? "Загрузка..." : "Обновить"}
        </button>
      </div>

      {ads.length === 0 && !loading && (
        <div style={{
          padding: "40px",
          borderRadius: "20px",
          border: "1px solid rgba(148,163,184,0.15)",
          background: "linear-gradient(145deg, rgba(30, 41, 59, 0.5) 0%, rgba(15, 23, 42, 0.8) 100%)",
          color: "#94a3b8",
          textAlign: "center",
          fontSize: "15px"
        }}>
          <PackageIcon size={48} color="#475569" />
          <p style={{ marginTop: "16px", marginBottom: 0 }}>Активных объявлений не найдено.</p>
        </div>
      )}

      <div style={{
        display: "grid",
        gridTemplateColumns: "repeat(auto-fill, minmax(300px, 1fr))",
        gap: "20px"
      }}>
        {ads.map((ad, index) => (
          <div
            key={`${ad.accountId || "acc"}-${index}-${ad.title}`}
            style={{
              border: "1px solid rgba(148,163,184,0.15)",
              borderRadius: "20px",
              overflow: "hidden",
              background: "linear-gradient(145deg, rgba(30, 41, 59, 0.6) 0%, rgba(15, 23, 42, 0.9) 100%)",
              boxShadow: "0 20px 50px rgba(0,0,0,0.3)",
              color: "#e2e8f0",
              transition: "all 0.3s cubic-bezier(0.4, 0, 0.2, 1)"
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.transform = "translateY(-6px)";
              e.currentTarget.style.boxShadow = "0 25px 60px rgba(0,0,0,0.4), 0 0 30px rgba(0,0,0,0.2)";
              e.currentTarget.style.borderColor = "rgba(148,163,184,0.25)";
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.transform = "translateY(0)";
              e.currentTarget.style.boxShadow = "0 20px 50px rgba(0,0,0,0.3)";
              e.currentTarget.style.borderColor = "rgba(148,163,184,0.15)";
            }}
          >
            <div style={{
              height: "180px",
              background: "linear-gradient(145deg, rgba(15,23,42,0.9) 0%, rgba(15,23,42,0.7) 100%)",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              position: "relative",
              overflow: "hidden"
            }}>
              {ad.image ? (
                <img
                  src={ad.image}
                  alt={ad.title}
                  style={{
                    width: "100%",
                    height: "100%",
                    objectFit: "cover",
                    transition: "transform 0.3s ease"
                  }}
                />
              ) : (
                <div style={{
                  display: "flex",
                  flexDirection: "column",
                  alignItems: "center",
                  gap: "8px"
                }}>
                  <PackageIcon size={40} color="#475569" />
                  <span style={{ color: "#64748b", fontSize: "12px" }}>Нет изображения</span>
                </div>
              )}
              <div style={{
                position: "absolute",
                top: "12px",
                right: "12px"
              }}>
                <span style={getBadgeStyle(ad.status)}>
                  {ad.status || "Aktiv"}
                </span>
              </div>
            </div>
            <div style={{ padding: "18px 20px" }}>
              <div style={{
                fontWeight: "600",
                color: "#f8fafc",
                marginBottom: "8px",
                fontSize: "16px",
                lineHeight: "1.4",
                overflow: "hidden",
                textOverflow: "ellipsis",
                whiteSpace: "nowrap"
              }}>
                {ad.title}
              </div>
              <div style={{
                color: "#60a5fa",
                fontWeight: "700",
                marginBottom: "12px",
                fontSize: "18px"
              }}>
                {ad.price || "Цена не указана"}
              </div>
              <div style={{
                display: "flex",
                gap: "16px",
                color: "#94a3b8",
                fontSize: "13px",
                marginBottom: "8px"
              }}>
                <span style={{ display: "inline-flex", alignItems: "center", gap: "6px" }}>
                  👁 {Number.isFinite(ad.views) ? ad.views : "—"}
                </span>
                <span style={{ display: "inline-flex", alignItems: "center", gap: "6px" }}>
                  ❤ {Number.isFinite(ad.favorites) ? ad.favorites : "—"}
                </span>
              </div>
              <div style={{
                display: "flex",
                alignItems: "center",
                justifyContent: "space-between",
                paddingTop: "12px",
                borderTop: "1px solid rgba(148,163,184,0.1)"
              }}>
                <div style={{ fontSize: "13px", color: "#94a3b8" }}>
                  Аккаунт: <strong style={{ color: "#e2e8f0" }}>{ad.accountLabel || "—"}</strong>
                </div>
              </div>
              <div style={{
                display: "flex",
                gap: "10px",
                marginTop: "14px"
              }}>
                {(() => {
                  const reserveDisabled = actionBusy[`reserve-${ad.adId}`]
                    || !ad?.adId
                    || !ad?.accountId
                    || /reserviert/i.test(ad.status || "");
                  const activateDisabled = actionBusy[`activate-${ad.adId}`]
                    || !ad?.adId
                    || !ad?.accountId
                    || !/reserviert/i.test(ad.status || "");
                  const deleteDisabled = actionBusy[`delete-${ad.adId}`]
                    || !ad?.adId
                    || !ad?.accountId;
                  const isReserved = /reserviert/i.test(ad.status || "");
                  return (
                    <>
                <button
                  onClick={() => (isReserved ? handleActivate(ad) : handleReserve(ad))}
                  disabled={isReserved ? activateDisabled : reserveDisabled}
                  style={{
                    flex: 1,
                    padding: "10px 12px",
                    borderRadius: "12px",
                    border: "1px solid rgba(59,130,246,0.35)",
                    background: isReserved
                      ? "rgba(59,130,246,0.1)"
                      : "linear-gradient(135deg, rgba(59,130,246,0.25), rgba(37,99,235,0.2))",
                    color: "#bfdbfe",
                    fontWeight: 600,
                    fontSize: "13px",
                    cursor: (isReserved ? activateDisabled : reserveDisabled)
                      ? "not-allowed"
                      : "pointer"
                  }}
                >
                  {isReserved
                    ? (actionBusy[`activate-${ad.adId}`] ? "Активация..." : "Активировать")
                    : (actionBusy[`reserve-${ad.adId}`] ? "Резерв..." : "Резервировать")}
                </button>
                <button
                  onClick={() => handleDelete(ad)}
                  disabled={deleteDisabled}
                  style={{
                    flex: 1,
                    padding: "10px 12px",
                    borderRadius: "12px",
                    border: "1px solid rgba(239,68,68,0.35)",
                    background: "linear-gradient(135deg, rgba(239,68,68,0.2), rgba(185,28,28,0.2))",
                    color: "#fecaca",
                    fontWeight: 600,
                    fontSize: "13px",
                    cursor: deleteDisabled ? "not-allowed" : "pointer"
                  }}
                >
                  {actionBusy[`delete-${ad.adId}`] ? "Удаление..." : "Удалить"}
                </button>
                    </>
                  );
                })()}
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
};

export default ActiveAdsTab;
