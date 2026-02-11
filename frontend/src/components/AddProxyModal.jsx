import React, { useState } from "react";
import { apiFetchJson } from "../api";
import { LinkIcon, XIcon, RefreshIcon, PlusIcon, CheckIcon } from "./Icons";

const AddProxyModal = ({ isOpen, onClose, onSuccess }) => {
  const [loading, setLoading] = useState(false);
  const [formData, setFormData] = useState({
    name: "",
    type: "http",
    host: "",
    port: "",
    username: "",
    password: ""
  });
  const [testResult, setTestResult] = useState(null);

  const formatMs = (value) => {
    const parsed = Number(value);
    if (!Number.isFinite(parsed) || parsed < 0) return "—";
    return `${Math.round(parsed)}мс`;
  };

  const handleInputChange = (e) => {
    const { name, value } = e.target;
    setFormData(prev => ({
      ...prev,
      [name]: value
    }));
  };

  const testProxy = async () => {
    if (!formData.host || !formData.port) {
      alert("Заполните адрес и порт прокси");
      return;
    }

    setLoading(true);
    setTestResult(null);

    try {
      const data = await apiFetchJson("/api/proxies", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(formData)
      });
      setTestResult(data);
    } catch (error) {
      const backendResult = error?.data && typeof error.data === "object" ? error.data : {};
      const backendError = backendResult.error || error?.message || "Ошибка при тестировании прокси";
      setTestResult({
        success: false,
        ...backendResult,
        error: backendError,
        status: error?.status || null
      });
    } finally {
      setLoading(false);
    }
  };

  const addProxy = async () => {
    if (!testResult || !testResult.success) {
      alert("Сначала протестируйте прокси");
      return;
    }

    onSuccess(testResult.proxy);
    onClose();
  };

  if (!isOpen) return null;

  const labelStyle = {
    display: "block",
    marginBottom: "10px",
    fontWeight: "600",
    color: "#e2e8f0",
    fontSize: "13px"
  };

  const inputStyle = {
    width: "100%",
    padding: "12px 16px",
    border: "1px solid rgba(148,163,184,0.2)",
    borderRadius: "14px",
    backgroundColor: "rgba(15,23,42,0.8)",
    color: "#e2e8f0",
    fontSize: "14px",
    transition: "all 0.2s ease",
    outline: "none"
  };

  return (
    <div className="modal-overlay" style={{
      position: "fixed",
      top: 0,
      left: 0,
      right: 0,
      bottom: 0,
      backgroundColor: "rgba(2, 6, 23, 0.85)",
      backdropFilter: "blur(8px)",
      display: "flex",
      alignItems: "center",
      justifyContent: "center",
      zIndex: 5000,
      padding: "20px",
      animation: "fadeIn 0.2s ease-out"
    }}>
      <div className="modal-card modal-card-wide" style={{
        background: "linear-gradient(145deg, rgba(30, 41, 59, 0.95) 0%, rgba(15, 23, 42, 0.98) 100%)",
        borderRadius: "24px",
        padding: "32px",
        width: "640px",
        maxWidth: "100%",
        maxHeight: "90vh",
        overflow: "auto",
        border: "1px solid rgba(148,163,184,0.15)",
        boxShadow: "0 30px 60px rgba(0,0,0,0.5), 0 0 60px rgba(0,0,0,0.3)",
        color: "#e2e8f0",
        animation: "scaleIn 0.3s ease-out"
      }}>
        <div style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          marginBottom: "28px"
        }}>
          <div style={{ display: "flex", alignItems: "center", gap: "14px" }}>
            <div style={{
              width: "48px",
              height: "48px",
              borderRadius: "14px",
              background: "linear-gradient(135deg, rgba(16, 185, 129, 0.2), rgba(16, 185, 129, 0.1))",
              border: "1px solid rgba(16, 185, 129, 0.3)",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              boxShadow: "0 4px 15px rgba(16, 185, 129, 0.2)"
            }}>
              <LinkIcon size={24} color="#34d399" />
            </div>
            <h2 style={{ margin: 0, fontSize: "22px", fontWeight: "700" }}>Добавить прокси</h2>
          </div>
          <button
            onClick={onClose}
            style={{
              width: "40px",
              height: "40px",
              borderRadius: "12px",
              background: "rgba(148,163,184,0.1)",
              border: "1px solid rgba(148,163,184,0.2)",
              cursor: "pointer",
              color: "#94a3b8",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              transition: "all 0.2s ease"
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.background = "rgba(239, 68, 68, 0.15)";
              e.currentTarget.style.borderColor = "rgba(239, 68, 68, 0.3)";
              e.currentTarget.style.color = "#f87171";
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.background = "rgba(148,163,184,0.1)";
              e.currentTarget.style.borderColor = "rgba(148,163,184,0.2)";
              e.currentTarget.style.color = "#94a3b8";
            }}
          >
            <XIcon size={18} />
          </button>
        </div>

        <div style={{
          display: "grid",
          gridTemplateColumns: "1fr 1fr",
          gap: "20px",
          marginBottom: "28px"
        }}>
          <div>
            <label style={labelStyle}>Название прокси *</label>
            <input
              type="text"
              name="name"
              value={formData.name}
              onChange={handleInputChange}
              placeholder="Например: Germany Proxy"
              style={inputStyle}
              onFocus={(e) => {
                e.currentTarget.style.borderColor = "rgba(16, 185, 129, 0.5)";
                e.currentTarget.style.boxShadow = "0 0 0 3px rgba(16, 185, 129, 0.15)";
              }}
              onBlur={(e) => {
                e.currentTarget.style.borderColor = "rgba(148,163,184,0.2)";
                e.currentTarget.style.boxShadow = "none";
              }}
            />
          </div>

          <div>
            <label style={labelStyle}>Тип прокси *</label>
            <select
              name="type"
              value={formData.type}
              onChange={handleInputChange}
              style={{ ...inputStyle, cursor: "pointer" }}
              onFocus={(e) => {
                e.currentTarget.style.borderColor = "rgba(16, 185, 129, 0.5)";
                e.currentTarget.style.boxShadow = "0 0 0 3px rgba(16, 185, 129, 0.15)";
              }}
              onBlur={(e) => {
                e.currentTarget.style.borderColor = "rgba(148,163,184,0.2)";
                e.currentTarget.style.boxShadow = "none";
              }}
            >
              <option value="http">HTTP</option>
              <option value="https">HTTPS</option>
              <option value="socks4">SOCKS4</option>
              <option value="socks5">SOCKS5</option>
            </select>
          </div>

          <div>
            <label style={labelStyle}>Адрес (IP или домен) *</label>
            <input
              type="text"
              name="host"
              value={formData.host}
              onChange={handleInputChange}
              placeholder="192.168.1.100"
              style={inputStyle}
              onFocus={(e) => {
                e.currentTarget.style.borderColor = "rgba(16, 185, 129, 0.5)";
                e.currentTarget.style.boxShadow = "0 0 0 3px rgba(16, 185, 129, 0.15)";
              }}
              onBlur={(e) => {
                e.currentTarget.style.borderColor = "rgba(148,163,184,0.2)";
                e.currentTarget.style.boxShadow = "none";
              }}
            />
          </div>

          <div>
            <label style={labelStyle}>Порт *</label>
            <input
              type="number"
              name="port"
              value={formData.port}
              onChange={handleInputChange}
              placeholder="8080"
              style={inputStyle}
              onFocus={(e) => {
                e.currentTarget.style.borderColor = "rgba(16, 185, 129, 0.5)";
                e.currentTarget.style.boxShadow = "0 0 0 3px rgba(16, 185, 129, 0.15)";
              }}
              onBlur={(e) => {
                e.currentTarget.style.borderColor = "rgba(148,163,184,0.2)";
                e.currentTarget.style.boxShadow = "none";
              }}
            />
          </div>

          <div>
            <label style={labelStyle}>Имя пользователя (опционально)</label>
            <input
              type="text"
              name="username"
              value={formData.username}
              onChange={handleInputChange}
              placeholder="Логин для прокси"
              style={inputStyle}
              onFocus={(e) => {
                e.currentTarget.style.borderColor = "rgba(16, 185, 129, 0.5)";
                e.currentTarget.style.boxShadow = "0 0 0 3px rgba(16, 185, 129, 0.15)";
              }}
              onBlur={(e) => {
                e.currentTarget.style.borderColor = "rgba(148,163,184,0.2)";
                e.currentTarget.style.boxShadow = "none";
              }}
            />
          </div>

          <div>
            <label style={labelStyle}>Пароль (опционально)</label>
            <input
              type="password"
              name="password"
              value={formData.password}
              onChange={handleInputChange}
              placeholder="Пароль для прокси"
              style={inputStyle}
              onFocus={(e) => {
                e.currentTarget.style.borderColor = "rgba(16, 185, 129, 0.5)";
                e.currentTarget.style.boxShadow = "0 0 0 3px rgba(16, 185, 129, 0.15)";
              }}
              onBlur={(e) => {
                e.currentTarget.style.borderColor = "rgba(148,163,184,0.2)";
                e.currentTarget.style.boxShadow = "none";
              }}
            />
          </div>
        </div>

        <div style={{
          display: "flex",
          justifyContent: "center",
          marginBottom: "24px"
        }}>
          <button
            className="primary-button"
            onClick={testProxy}
            disabled={loading || !formData.host || !formData.port}
            style={{
              padding: "14px 36px",
              color: "white",
              border: "none",
              cursor: loading ? "not-allowed" : "pointer",
              display: "flex",
              alignItems: "center",
              gap: "10px",
              fontSize: "14px",
              fontWeight: "600",
              borderRadius: "14px"
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
                Тестирование...
              </>
            ) : (
              <>
                <RefreshIcon size={18} />
                Протестировать прокси
              </>
            )}
          </button>
        </div>

        {testResult && (
          <div style={{
            marginBottom: "24px",
            padding: "20px",
            background: testResult.success
              ? "linear-gradient(145deg, rgba(16, 185, 129, 0.1) 0%, rgba(16, 185, 129, 0.05) 100%)"
              : "linear-gradient(145deg, rgba(239, 68, 68, 0.1) 0%, rgba(239, 68, 68, 0.05) 100%)",
            borderRadius: "16px",
            border: `1px solid ${testResult.success ? "rgba(16, 185, 129, 0.25)" : "rgba(239, 68, 68, 0.25)"}`
          }}>
            <h4 style={{
              margin: "0 0 16px 0",
              color: testResult.success ? "#34d399" : "#f87171",
              display: "flex",
              alignItems: "center",
              gap: "10px",
              fontSize: "16px"
            }}>
              {testResult.success ? (
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

            {testResult.success ? (
              <div style={{
                display: "grid",
                gridTemplateColumns: "repeat(auto-fit, minmax(140px, 1fr))",
                gap: "12px"
              }}>
                {[
                  { label: "Время ответа", value: formatMs(testResult.checkResult?.responseTime), icon: "⚡" },
                  { label: "IP адрес", value: testResult.checkResult?.ip, icon: "🌐" },
                  { label: "Локация", value: `${testResult.checkResult?.location?.country}${testResult.checkResult?.location?.city ? `, ${testResult.checkResult.location.city}` : ""}`, icon: "📍" },
                  { label: "Провайдер", value: testResult.checkResult?.isp, icon: "🏢" }
                ].map((item, idx) => (
                  <div key={idx} style={{
                    padding: "12px 14px",
                    background: "rgba(15, 23, 42, 0.6)",
                    borderRadius: "12px",
                    border: "1px solid rgba(148,163,184,0.1)"
                  }}>
                    <div style={{ fontSize: "11px", color: "#64748b", marginBottom: "4px", display: "flex", alignItems: "center", gap: "6px" }}>
                      <span>{item.icon}</span>
                      {item.label}
                    </div>
                    <div style={{ fontWeight: "600", fontSize: "14px", color: "#f8fafc" }}>{item.value || "—"}</div>
                  </div>
                ))}
              </div>
            ) : (
              <div style={{
                padding: "14px 16px",
                background: "rgba(239, 68, 68, 0.1)",
                borderRadius: "12px",
                color: "#fca5a5",
                border: "1px solid rgba(239, 68, 68, 0.2)"
              }}>
                {testResult.status ? <><strong>HTTP:</strong> {testResult.status}<br /></> : null}
                <strong>Ошибка:</strong> {testResult.error || testResult.checkResult?.error}
              </div>
            )}
          </div>
        )}

        <div style={{
          display: "flex",
          gap: "12px",
          justifyContent: "flex-end"
        }}>
          <button
            type="button"
            onClick={onClose}
            className="secondary-button"
            style={{
              padding: "12px 24px",
              border: "none",
              cursor: "pointer",
              borderRadius: "14px",
              fontSize: "14px",
              fontWeight: "600"
            }}
          >
            Отмена
          </button>
          <button
            className="primary-button"
            onClick={addProxy}
            disabled={!testResult?.success}
            style={{
              padding: "12px 28px",
              color: "white",
              border: "none",
              cursor: testResult?.success ? "pointer" : "not-allowed",
              borderRadius: "14px",
              fontSize: "14px",
              fontWeight: "600",
              display: "flex",
              alignItems: "center",
              gap: "8px"
            }}
          >
            <PlusIcon size={16} />
            Добавить прокси
          </button>
        </div>

        <style jsx="true">{`
          @keyframes fadeIn {
            from { opacity: 0; }
            to { opacity: 1; }
          }
          @keyframes scaleIn {
            from { opacity: 0; transform: scale(0.95); }
            to { opacity: 1; transform: scale(1); }
          }
          @keyframes spin {
            from { transform: rotate(0deg); }
            to { transform: rotate(360deg); }
          }
        `}</style>
      </div>
    </div>
  );
};

export default AddProxyModal;
