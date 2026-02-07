import React, { useState, useEffect } from "react";
import { apiFetchJson } from "../api";
import { RefreshIcon, TrashIcon, CheckIcon, XIcon, LinkIcon } from "./Icons";

const ProxyChecker = ({ proxy, onCheckComplete, onDelete, isPhoneView = false }) => {
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState(null);
  const [history, setHistory] = useState([]);

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

    switch (status) {
      case "active":
        return {
          ...baseStyle,
          background: "linear-gradient(135deg, #10b981, #059669)",
          color: "white",
          border: "1px solid rgba(16, 185, 129, 0.3)",
          boxShadow: "0 0 12px rgba(16, 185, 129, 0.35)"
        };
      case "failed":
        return {
          ...baseStyle,
          background: "linear-gradient(135deg, #ef4444, #dc2626)",
          color: "white",
          border: "1px solid rgba(239, 68, 68, 0.3)",
          boxShadow: "0 0 12px rgba(239, 68, 68, 0.35)"
        };
      case "checking":
        return {
          ...baseStyle,
          background: "linear-gradient(135deg, #f59e0b, #d97706)",
          color: "white",
          border: "1px solid rgba(245, 158, 11, 0.3)",
          boxShadow: "0 0 12px rgba(245, 158, 11, 0.35)"
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

  const checkProxy = async () => {
    setLoading(true);
    try {
      const data = await apiFetchJson(`/api/proxies/${proxy.id}/check`, {
        method: "POST",
        headers: { "Content-Type": "application/json" }
      });
      setResult(data);

      const newHistoryItem = {
        timestamp: new Date().toLocaleTimeString(),
        success: data.success,
        ip: data.ip,
        location: data.location,
        responseTime: data.responseTime,
        ping: data.ping
      };

      setHistory(prev => [newHistoryItem, ...prev.slice(0, 4)]);

      if (onCheckComplete) {
        onCheckComplete(data);
      }
    } catch (error) {
      const backendResult = error?.data && typeof error.data === "object" ? error.data : {};
      const backendError = backendResult.error || error?.message || "Ошибка при проверке прокси";
      setResult({
        success: false,
        ...backendResult,
        error: backendError
      });
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (proxy.checkResult) {
      setResult(proxy.checkResult);
    }
  }, [proxy]);

  const getStatusText = (status) => {
    switch (status) {
      case "active": return "Работает";
      case "failed": return "Не работает";
      case "checking": return "Проверяется";
      default: return "Не проверен";
    }
  };

  const formatMs = (value) => {
    const parsed = Number(value);
    if (!Number.isFinite(parsed) || parsed < 0) return "—";
    return `${Math.round(parsed)}мс`;
  };

  return (
    <div className="proxy-checker-card" style={{
      width: "100%",
      background: "linear-gradient(145deg, rgba(30, 41, 59, 0.6) 0%, rgba(15, 23, 42, 0.95) 100%)",
      borderRadius: isPhoneView ? "16px" : "20px",
      padding: isPhoneView ? "14px" : "24px",
      marginBottom: "20px",
      border: "1px solid rgba(148,163,184,0.15)",
      boxShadow: "0 20px 50px rgba(0,0,0,0.3), 0 0 30px rgba(0,0,0,0.15)",
      color: "#e2e8f0",
      backdropFilter: "blur(10px)",
      transition: "all 0.3s ease"
    }}
    onMouseEnter={(e) => {
      e.currentTarget.style.borderColor = "rgba(148,163,184,0.25)";
      e.currentTarget.style.boxShadow = "0 25px 60px rgba(0,0,0,0.4), 0 0 35px rgba(0,0,0,0.2)";
    }}
    onMouseLeave={(e) => {
      e.currentTarget.style.borderColor = "rgba(148,163,184,0.15)";
      e.currentTarget.style.boxShadow = "0 20px 50px rgba(0,0,0,0.3), 0 0 30px rgba(0,0,0,0.15)";
    }}
    >
      <div style={{
        display: "flex",
        justifyContent: "space-between",
        alignItems: isPhoneView ? "stretch" : "flex-start",
        flexDirection: isPhoneView ? "column" : "row",
        marginBottom: "20px",
        gap: "20px",
        flexWrap: "wrap"
      }}>
        <div style={{ flex: 1, minWidth: 0 }}>
          <h3 style={{
            margin: 0,
            display: "flex",
            alignItems: "center",
            gap: "12px",
            fontSize: isPhoneView ? "16px" : "18px",
            fontWeight: "700",
            flexWrap: "wrap"
          }}>
            <div style={{
              width: "40px",
              height: "40px",
              borderRadius: "12px",
              background: "linear-gradient(135deg, rgba(16, 185, 129, 0.2), rgba(16, 185, 129, 0.1))",
              border: "1px solid rgba(16, 185, 129, 0.3)",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              boxShadow: "0 4px 12px rgba(16, 185, 129, 0.2)"
            }}>
              <LinkIcon size={20} color="#34d399" />
            </div>
            {proxy.name}
            <span style={getBadgeStyle(proxy.status)}>
              {getStatusText(proxy.status)}
            </span>
          </h3>
          <p style={{
            margin: isPhoneView ? "10px 0 0 0" : "12px 0 0 52px",
            color: "#94a3b8",
            fontSize: "14px",
            display: "flex",
            alignItems: "center",
            gap: "8px",
            flexWrap: "wrap",
            overflowWrap: "anywhere"
          }}>
            <span style={{
              padding: "4px 10px",
              background: "rgba(59, 130, 246, 0.15)",
              border: "1px solid rgba(59, 130, 246, 0.25)",
              borderRadius: "8px",
              color: "#60a5fa",
              fontWeight: "600",
              fontSize: "12px"
            }}>
              {proxy.type.toUpperCase()}
            </span>
            <span>{proxy.host}:{proxy.port}</span>
            {proxy.lastChecked && (
              <span style={{ fontSize: "12px", color: "#64748b" }}>
                • Проверено: {new Date(proxy.lastChecked).toLocaleTimeString()}
              </span>
            )}
          </p>
        </div>

        <div style={{ display: "flex", flexDirection: "column", gap: "10px", alignItems: "stretch", width: isPhoneView ? "100%" : "auto" }}>
          <button
            className="primary-button"
            onClick={checkProxy}
            disabled={loading}
            style={{
              padding: "12px 24px",
              color: "white",
              border: "none",
              cursor: loading ? "not-allowed" : "pointer",
              display: "flex",
              alignItems: "center",
              gap: "10px",
              minWidth: isPhoneView ? "0" : "180px",
              width: isPhoneView ? "100%" : "auto",
              justifyContent: "center",
              fontSize: "14px",
              fontWeight: "600"
            }}
          >
            {loading ? (
              <>
                <div style={{
                  width: "16px",
                  height: "16px",
                  border: "2px solid rgba(255,255,255,0.3)",
                  borderTop: "2px solid white",
                  borderRadius: "50%",
                  animation: "spin 1s linear infinite"
                }} />
                Проверка...
              </>
            ) : (
              <>
                <RefreshIcon size={16} />
                Проверить прокси
              </>
            )}
          </button>
          {onDelete && (
            <button
              className="danger-button"
              onClick={onDelete}
              style={{
                padding: "12px 24px",
                color: "white",
                border: "none",
              borderRadius: "12px",
              cursor: "pointer",
              minWidth: isPhoneView ? "0" : "180px",
              width: isPhoneView ? "100%" : "auto",
              display: "flex",
              alignItems: "center",
              gap: "10px",
                justifyContent: "center",
                fontSize: "14px",
                fontWeight: "600"
              }}
            >
              <TrashIcon size={16} />
              Удалить
            </button>
          )}
        </div>
      </div>

      {result && (
        <div style={{
          marginTop: "20px",
          padding: "20px",
          background: result.success
            ? "linear-gradient(145deg, rgba(16, 185, 129, 0.1) 0%, rgba(16, 185, 129, 0.05) 100%)"
            : "linear-gradient(145deg, rgba(239, 68, 68, 0.1) 0%, rgba(239, 68, 68, 0.05) 100%)",
          borderRadius: "16px",
          border: `1px solid ${result.success ? "rgba(16, 185, 129, 0.25)" : "rgba(239, 68, 68, 0.25)"}`
        }}>
          <div style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: isPhoneView ? "flex-start" : "center",
            flexWrap: "wrap",
            flexDirection: isPhoneView ? "column" : "row",
            gap: "12px"
          }}>
            <h4 style={{
              margin: 0,
              color: result.success ? "#34d399" : "#f87171",
              display: "flex",
              alignItems: "center",
              gap: "10px",
              fontSize: "16px",
              fontWeight: "600"
            }}>
              {result.success ? (
                <>
                  <div style={{
                    width: "28px",
                    height: "28px",
                    borderRadius: "50%",
                    background: "linear-gradient(135deg, #10b981, #059669)",
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center",
                    boxShadow: "0 0 15px rgba(16, 185, 129, 0.4)"
                  }}>
                    <CheckIcon size={16} color="white" />
                  </div>
                  Прокси работает
                </>
              ) : (
                <>
                  <div style={{
                    width: "28px",
                    height: "28px",
                    borderRadius: "50%",
                    background: "linear-gradient(135deg, #ef4444, #dc2626)",
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center",
                    boxShadow: "0 0 15px rgba(239, 68, 68, 0.4)"
                  }}>
                    <XIcon size={16} color="white" />
                  </div>
                  Прокси не работает
                </>
              )}
            </h4>
            <div style={{
              display: "flex",
              gap: "12px",
              flexWrap: "wrap"
            }}>
              <span style={{
                padding: "6px 12px",
                background: "rgba(15, 23, 42, 0.6)",
                borderRadius: "10px",
                fontSize: "13px",
                color: "#94a3b8",
                border: "1px solid rgba(148, 163, 184, 0.15)"
              }}>
                Ответ: <strong style={{ color: "#e2e8f0" }}>{formatMs(result.responseTime)}</strong>
              </span>
              {(result.ping !== undefined && result.ping !== null) && (
                <span style={{
                  padding: "6px 12px",
                  background: "rgba(15, 23, 42, 0.6)",
                  borderRadius: "10px",
                  fontSize: "13px",
                  color: "#94a3b8",
                  border: "1px solid rgba(148, 163, 184, 0.15)"
                }}>
                  Пинг: <strong style={{ color: "#e2e8f0" }}>{formatMs(result.ping)}</strong>
                </span>
              )}
            </div>
          </div>

          {result.success ? (
            <div style={{ marginTop: "16px" }}>
              <div style={{
                display: "grid",
                gridTemplateColumns: isPhoneView ? "1fr" : "repeat(auto-fit, minmax(180px, 1fr))",
                gap: "12px"
              }}>
                {[
                  { label: "IP Адрес", value: result.ip, icon: "🌐" },
                  { label: "Локация", value: `${result.location?.country}${result.location?.city ? `, ${result.location.city}` : ""}`, icon: "📍" },
                  { label: "Провайдер", value: result.isp, icon: "🏢" },
                  { label: "Тип прокси", value: proxy.type.toUpperCase(), icon: "🔗" }
                ].map((item, idx) => (
                  <div key={idx} style={{
                    padding: "14px 16px",
                    background: "linear-gradient(145deg, rgba(30, 41, 59, 0.5) 0%, rgba(15, 23, 42, 0.7) 100%)",
                    borderRadius: "14px",
                    border: "1px solid rgba(148,163,184,0.12)",
                    transition: "all 0.2s ease"
                  }}>
                    <div style={{ fontSize: "12px", color: "#64748b", marginBottom: "6px", display: "flex", alignItems: "center", gap: "6px" }}>
                      <span>{item.icon}</span>
                      {item.label}
                    </div>
                    <div style={{ fontWeight: "700", fontSize: "15px", color: "#f8fafc" }}>{item.value || "—"}</div>
                  </div>
                ))}
              </div>

              {result.location?.timezone && (
                <div style={{
                  marginTop: "12px",
                  padding: "14px 16px",
                  background: "linear-gradient(145deg, rgba(59, 130, 246, 0.1) 0%, rgba(59, 130, 246, 0.05) 100%)",
                  borderRadius: "14px",
                  fontSize: "13px",
                  color: "#94a3b8",
                  border: "1px solid rgba(59, 130, 246, 0.2)"
                }}>
                  <span style={{ color: "#60a5fa", fontWeight: "600" }}>Дополнительно:</span>{" "}
                  Часовой пояс: {result.location.timezone}
                  {result.location.coordinates && ` • Координаты: ${result.location.coordinates[0]}, ${result.location.coordinates[1]}`}
                </div>
              )}
            </div>
          ) : (
            <div style={{
              marginTop: "12px",
              padding: "14px 16px",
              background: "rgba(239, 68, 68, 0.1)",
              borderRadius: "12px",
              color: "#fca5a5",
              border: "1px solid rgba(239, 68, 68, 0.2)"
            }}>
              <strong>Ошибка:</strong> {result.error || "Неизвестная ошибка"}
            </div>
          )}
        </div>
      )}

      {history.length > 0 && (
        <div style={{ marginTop: "24px" }}>
          <h4 style={{ margin: "0 0 12px 0", fontSize: "14px", color: "#94a3b8", fontWeight: "600" }}>
            История проверок:
          </h4>
          <div style={{
            display: "flex",
            flexDirection: "column",
            gap: "8px",
            maxHeight: "160px",
            overflowY: "auto"
          }}>
            {history.map((item, index) => (
              <div key={index} style={{
                display: "flex",
                justifyContent: "space-between",
                alignItems: isPhoneView ? "flex-start" : "center",
                flexDirection: isPhoneView ? "column" : "row",
                padding: "12px 16px",
                background: index === 0
                  ? "linear-gradient(145deg, rgba(30, 41, 59, 0.6) 0%, rgba(30, 41, 59, 0.4) 100%)"
                  : "rgba(15, 23, 42, 0.5)",
                borderRadius: "12px",
                borderLeft: `4px solid ${item.success ? "#10b981" : "#ef4444"}`,
                transition: "all 0.2s ease",
                gap: isPhoneView ? "8px" : "0"
              }}>
                <div style={{ display: "flex", alignItems: "center", gap: "10px" }}>
                  <span style={{
                    width: "24px",
                    height: "24px",
                    borderRadius: "50%",
                    background: item.success
                      ? "linear-gradient(135deg, rgba(16, 185, 129, 0.2), rgba(16, 185, 129, 0.1))"
                      : "linear-gradient(135deg, rgba(239, 68, 68, 0.2), rgba(239, 68, 68, 0.1))",
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center",
                    color: item.success ? "#34d399" : "#f87171",
                    fontSize: "12px"
                  }}>
                    {item.success ? "✓" : "✗"}
                  </span>
                  <span style={{ fontWeight: "600", color: "#f8fafc" }}>{item.ip || "N/A"}</span>
                  {item.location && (
                    <span style={{ color: "#64748b", fontSize: "13px" }}>
                      {item.location.country}
                    </span>
                  )}
                </div>
                <div style={{ fontSize: "12px", color: "#64748b" }}>
                  {item.timestamp} • {formatMs(item.responseTime)}
                  {(item.ping !== undefined && item.ping !== null) ? ` • ${formatMs(item.ping)}` : ""}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      <style jsx="true">{`
        @keyframes spin {
          0% { transform: rotate(0deg); }
          100% { transform: rotate(360deg); }
        }
      `}</style>
    </div>
  );
};

export default ProxyChecker;
