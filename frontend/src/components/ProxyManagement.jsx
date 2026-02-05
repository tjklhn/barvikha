import React, { useState } from "react";
import { apiFetchJson } from "../api";

const ProxyManagement = ({ proxies, onDelete, onTest, onRefresh }) => {
  const [testingId, setTestingId] = useState(null);

  const handleTest = async (proxyId) => {
    setTestingId(proxyId);
    try {
      const result = await apiFetchJson("/api/proxies/test", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ proxyId })
      });
      alert(result.success ? "✅ Прокси работает!" : "❌ Прокси не работает: " + result.error);
      onRefresh(); // Обновляем список
    } catch (error) {
      alert("Ошибка тестирования: " + error.message);
    } finally {
      setTestingId(null);
    }
  };

  const handleDelete = async (proxyId, proxyName) => {
    if (!window.confirm(`Удалить прокси "${proxyName}"?`)) return;
    
    try {
      const result = await apiFetchJson(`/api/proxies/${proxyId}`, {
        method: "DELETE"
      });
      if (result.success) {
        alert("✅ Прокси удален!");
        onRefresh();
      } else {
        alert("Ошибка: " + result.error);
      }
    } catch (error) {
      alert("Ошибка удаления: " + error.message);
    }
  };

  const getProxyTypeIcon = (type) => {
    switch(type) {
      case "http": return "🌐";
      case "socks4": return "🔒";
      case "socks5": return "🔐";
      default: return "❓";
    }
  };

  const getStatusBadge = (status) => {
    switch(status) {
      case "active": return { text: "Активен", color: "#22c55e", bg: "rgba(34,197,94,0.15)" };
      case "failed": return { text: "Не работает", color: "#f87171", bg: "rgba(239,68,68,0.15)" };
      case "testing": return { text: "Тестируется", color: "#f59e0b", bg: "rgba(245,158,11,0.15)" };
      default: return { text: status, color: "#94a3b8", bg: "rgba(15,23,42,0.6)" };
    }
  };

  return (
    <div>
      <div style={{ 
        display: "flex", 
        justifyContent: "space-between", 
        alignItems: "center", 
        marginBottom: "20px" 
      }}>
        <h2 style={{ margin: 0, color: "#f8fafc" }}>🔗 Управление прокси</h2>
        <div style={{ display: "flex", gap: "10px" }}>
          <button 
            onClick={onRefresh}
            style={{
              padding: "8px 16px",
              background: "#1e40af",
              color: "white",
              border: "none",
              borderRadius: "10px",
              cursor: "pointer",
              display: "flex",
              alignItems: "center",
              gap: "6px"
            }}
          >
            🔄 Обновить
          </button>
        </div>
      </div>

      <div style={{ 
        background: "linear-gradient(135deg, #0b1220 0%, #111827 100%)",
        borderRadius: "16px",
        overflow: "hidden",
        boxShadow: "0 18px 40px rgba(15,23,42,0.35)",
        border: "1px solid rgba(148,163,184,0.2)",
        color: "#e2e8f0"
      }}>
        {proxies.length === 0 ? (
          <div style={{ padding: "40px 20px", textAlign: "center", color: "#94a3b8" }}>
            <div style={{ fontSize: "48px", marginBottom: "20px" }}>🔗</div>
            <h3>Нет добавленных прокси</h3>
            <p>Добавьте свой первый прокси для использования с Kleinanzeigen</p>
          </div>
        ) : (
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead style={{ background: "rgba(15,23,42,0.7)" }}>
              <tr>
                <th style={{ padding: "16px", textAlign: "left" }}>Прокси</th>
                <th style={{ padding: "16px", textAlign: "left" }}>Тип</th>
                <th style={{ padding: "16px", textAlign: "left" }}>Адрес</th>
                <th style={{ padding: "16px", textAlign: "left" }}>Статус</th>
                <th style={{ padding: "16px", textAlign: "left" }}>Провайдер</th>
                <th style={{ padding: "16px", textAlign: "left" }}>Действия</th>
              </tr>
            </thead>
            <tbody>
              {proxies.map(proxy => {
                const statusBadge = getStatusBadge(proxy.id === testingId ? "testing" : proxy.status);
                
                return (
                  <tr key={proxy.id} style={{ borderTop: "1px solid rgba(148,163,184,0.2)" }}>
                    <td style={{ padding: "16px" }}>
                      <div style={{ fontWeight: "500" }}>{proxy.name}</div>
                      {proxy.username && (
                        <div style={{ fontSize: "12px", color: "#94a3b8", marginTop: "4px" }}>
                          User: {proxy.username}
                        </div>
                      )}
                    </td>
                    <td style={{ padding: "16px" }}>
                      <span style={{ fontSize: "20px", marginRight: "8px" }}>
                        {getProxyTypeIcon(proxy.type)}
                      </span>
                      {proxy.type.toUpperCase()}
                    </td>
                    <td style={{ padding: "16px" }}>
                      <div style={{ fontFamily: "monospace", fontSize: "14px" }}>
                        {proxy.host}:{proxy.port}
                      </div>
                    </td>
                    <td style={{ padding: "16px" }}>
                      <span style={{
                        padding: "4px 12px",
                        borderRadius: "10px",
                        background: statusBadge.bg,
                        color: statusBadge.color,
                        fontSize: "12px",
                        fontWeight: "500",
                        display: "inline-flex",
                        alignItems: "center",
                        gap: "6px"
                      }}>
                        {proxy.id === testingId && (
                          <span className="spinner" style={{ 
                            width: "12px", 
                            height: "12px", 
                            border: "2px solid currentColor",
                            borderTopColor: "transparent",
                            borderRadius: "50%",
                            animation: "spin 1s linear infinite"
                          }}></span>
                        )}
                        {statusBadge.text}
                      </span>
                    </td>
                    <td style={{ padding: "16px" }}>
                      <span style={{
                        padding: "4px 10px",
                        borderRadius: "6px",
                        background: proxy.provider === "credixis" ? "rgba(59,130,246,0.2)" : "rgba(34,197,94,0.2)",
                        color: proxy.provider === "credixis" ? "#7dd3fc" : "#22c55e",
                        fontSize: "11px",
                        fontWeight: "500",
                        textTransform: "uppercase"
                      }}>
                        {proxy.provider === "credixis" ? "Credixis" : "Пользовательский"}
                      </span>
                    </td>
                    <td style={{ padding: "16px" }}>
                      <div style={{ display: "flex", gap: "8px" }}>
                        <button
                          onClick={() => handleTest(proxy.id)}
                          disabled={testingId === proxy.id}
                          style={{
                            padding: "6px 12px",
                            background: testingId === proxy.id ? "#16a34a" : "#2563eb",
                            color: "white",
                            border: "none",
                            borderRadius: "10px",
                            cursor: testingId === proxy.id ? "not-allowed" : "pointer",
                            fontSize: "12px"
                          }}
                        >
                          {testingId === proxy.id ? "Тестирование..." : "Тестировать"}
                        </button>
                        
                        {proxy.provider === "custom" && (
                          <button
                            onClick={() => handleDelete(proxy.id, proxy.name)}
                            style={{
                              padding: "6px 12px",
                              background: "#ef4444",
                              color: "white",
                              border: "none",
                              borderRadius: "10px",
                              cursor: "pointer",
                              fontSize: "12px"
                            }}
                          >
                            Удалить
                          </button>
                        )}
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </div>

      <style>
        {`
          @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
          }
        `}
      </style>
    </div>
  );
};

export default ProxyManagement;
