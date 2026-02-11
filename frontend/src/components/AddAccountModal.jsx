import React, { useEffect, useState } from "react";
import { apiFetchJson, getAccessToken } from "../api";
import { UserIcon, XIcon, FileIcon, PlusIcon } from "./Icons";

const AddAccountModal = ({ isOpen, onClose, onSuccess, proxies }) => {
  const [loading, setLoading] = useState(false);
  const [selectedProxy, setSelectedProxy] = useState("");
  const [file, setFile] = useState(null);
  const [cookieText, setCookieText] = useState("");
  const [uploadMode, setUploadMode] = useState("file");
  const [availableProxies, setAvailableProxies] = useState([]);

  useEffect(() => {
    setAvailableProxies(proxies || []);
  }, [proxies]);

  useEffect(() => {
    if (!isOpen) return;
    let cancelled = false;

    const loadProxies = async () => {
      try {
        const data = await apiFetchJson("/api/proxies");
        if (!cancelled) {
          setAvailableProxies(Array.isArray(data) ? data : []);
        }
      } catch (error) {
        if (!cancelled) {
          setAvailableProxies(proxies || []);
        }
      }
    };

    loadProxies();

    return () => {
      cancelled = true;
    };
  }, [isOpen, proxies]);

  useEffect(() => {
    if (!selectedProxy) return;
    const stillExists = availableProxies?.some((proxy) => String(proxy.id) === String(selectedProxy));
    if (!stillExists) {
      setSelectedProxy("");
    }
  }, [availableProxies, selectedProxy]);

  const handleFileChange = (e) => {
    const selectedFile = e.target.files[0];
    if (selectedFile) {
      setFile(selectedFile);
    }
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (uploadMode === "file" && !file) {
      alert("Пожалуйста, выберите файл");
      return;
    }
    if (uploadMode === "text" && !cookieText.trim()) {
      alert("Пожалуйста, введите куки");
      return;
    }

    setLoading(true);

    try {
      const buildFormData = () => {
        const formData = new FormData();
        if (uploadMode === "file") {
          formData.append("cookieFile", file);
        } else {
          const blob = new Blob([cookieText], { type: "text/plain" });
          formData.append("cookieFile", blob, "cookies.txt");
        }
        if (selectedProxy) {
          formData.append("proxyId", selectedProxy);
        }
        return formData;
      };

      const isNetworkLikeError = (error) => {
        if (!error) return false;
        if (error?.name === "AbortError") return true;
        const msg = String(error?.message || "").toLowerCase();
        return (
          msg.includes("failed to fetch") ||
          msg.includes("networkerror") ||
          msg.includes("network error") ||
          msg.includes("load failed") ||
          msg.includes("connection reset") ||
          msg.includes("timeout") ||
          msg.includes("timed out")
        );
      };

      const uploadWithAuthHeader = async () => apiFetchJson("/api/accounts/upload", {
        method: "POST",
        body: buildFormData(),
        timeoutMs: 240000,
        allowBaseFallback: true
      });

      const uploadWithTokenQuery = async () => {
        const token = getAccessToken();
        // Some reverse-proxies/WAF rules can drop requests that include `accessToken` in the query string.
        // Backend accepts both `token` and `accessToken`, so prefer the more generic `token`.
        const tokenQuery = token ? `?token=${encodeURIComponent(token)}` : "";
        return apiFetchJson(`/api/accounts/upload${tokenQuery}`, {
          method: "POST",
          body: buildFormData(),
          timeoutMs: 240000,
          allowBaseFallback: true,
          // Avoid CORS preflight issues on some deployments by not sending custom headers.
          skipAuth: true,
          skipClientRequestId: true
        });
      };

      let result;
      try {
        result = await uploadWithAuthHeader();
      } catch (firstError) {
        // Some setups drop requests that contain tokens in the query string; others have strict CORS.
        // Try the alternate auth delivery for network-layer failures.
        if (!isNetworkLikeError(firstError)) {
          throw firstError;
        }
        result = await uploadWithTokenQuery();
      }

      if (result.success) {
        alert(result.message);
        onSuccess(result.account);
        onClose();
      } else {
        alert("Ошибка: " + (result.error || "Неизвестная ошибка"));
      }
    } catch (error) {
      const details = [];
      if (error?.url) details.push(error.url);
      if (error?.requestId) details.push(`id=${error.requestId}`);
      if (typeof window !== "undefined" && window.location?.origin) details.push(window.location.origin);
      const suffix = details.length ? ` (${details.join(", ")})` : "";
      alert("Ошибка при загрузке: " + (error?.message || "Failed to fetch") + suffix);
    } finally {
      setLoading(false);
    }
  };

  if (!isOpen) return null;

  const labelStyle = {
    display: "block",
    marginBottom: "10px",
    fontWeight: "600",
    color: "#e2e8f0",
    fontSize: "14px"
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
      <div className="modal-card" style={{
        background: "linear-gradient(145deg, rgba(30, 41, 59, 0.95) 0%, rgba(15, 23, 42, 0.98) 100%)",
        borderRadius: "24px",
        padding: "32px",
        width: "520px",
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
              background: "linear-gradient(135deg, rgba(139, 92, 246, 0.2), rgba(139, 92, 246, 0.1))",
              border: "1px solid rgba(139, 92, 246, 0.3)",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              boxShadow: "0 4px 15px rgba(139, 92, 246, 0.2)"
            }}>
              <UserIcon size={24} color="#a78bfa" />
            </div>
            <h2 style={{ margin: 0, fontSize: "22px", fontWeight: "700" }}>Добавить аккаунт</h2>
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
          marginBottom: "24px",
          display: "flex",
          gap: "12px"
        }}>
          {[
            { value: "file", label: "Загрузить файл", icon: <FileIcon size={16} /> },
            { value: "text", label: "Вставить текст", icon: "📝" }
          ].map((option) => (
            <label
              key={option.value}
              style={{
                flex: 1,
                padding: "14px 18px",
                borderRadius: "14px",
                background: uploadMode === option.value
                  ? "linear-gradient(135deg, rgba(139, 92, 246, 0.2), rgba(139, 92, 246, 0.1))"
                  : "rgba(15, 23, 42, 0.6)",
                border: uploadMode === option.value
                  ? "1px solid rgba(139, 92, 246, 0.4)"
                  : "1px solid rgba(148,163,184,0.15)",
                cursor: "pointer",
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                gap: "10px",
                transition: "all 0.2s ease",
                color: uploadMode === option.value ? "#a78bfa" : "#94a3b8",
                fontWeight: "500",
                fontSize: "14px"
              }}
            >
              <input
                type="radio"
                value={option.value}
                checked={uploadMode === option.value}
                onChange={(e) => setUploadMode(e.target.value)}
                style={{ display: "none" }}
              />
              {option.icon}
              {option.label}
            </label>
          ))}
        </div>

        <form onSubmit={handleSubmit}>
          {uploadMode === "file" ? (
            <div style={{ marginBottom: "24px" }}>
              <label style={labelStyle}>Файл с куки:</label>
              <div
                style={{
                  padding: "32px 24px",
                  border: "2px dashed rgba(148,163,184,0.3)",
                  borderRadius: "16px",
                  backgroundColor: "rgba(15,23,42,0.6)",
                  textAlign: "center",
                  cursor: "pointer",
                  transition: "all 0.2s ease",
                  position: "relative"
                }}
                onDragOver={(e) => {
                  e.preventDefault();
                  e.currentTarget.style.borderColor = "rgba(139, 92, 246, 0.5)";
                  e.currentTarget.style.background = "rgba(139, 92, 246, 0.05)";
                }}
                onDragLeave={(e) => {
                  e.currentTarget.style.borderColor = "rgba(148,163,184,0.3)";
                  e.currentTarget.style.background = "rgba(15,23,42,0.6)";
                }}
                onDrop={(e) => {
                  e.preventDefault();
                  const droppedFile = e.dataTransfer.files[0];
                  if (droppedFile) setFile(droppedFile);
                  e.currentTarget.style.borderColor = "rgba(148,163,184,0.3)";
                  e.currentTarget.style.background = "rgba(15,23,42,0.6)";
                }}
                onClick={() => document.getElementById("cookie-file-input").click()}
              >
                <input
                  id="cookie-file-input"
                  type="file"
                  accept=".txt,.json,.cookies"
                  onChange={handleFileChange}
                  style={{
                    display: "none"
                  }}
                />
                <div style={{
                  width: "56px",
                  height: "56px",
                  borderRadius: "16px",
                  background: "linear-gradient(135deg, rgba(139, 92, 246, 0.2), rgba(139, 92, 246, 0.1))",
                  border: "1px solid rgba(139, 92, 246, 0.3)",
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  margin: "0 auto 16px"
                }}>
                  <FileIcon size={28} color="#a78bfa" />
                </div>
                <p style={{ margin: 0, color: "#94a3b8", fontSize: "14px" }}>
                  Перетащите файл сюда или <span style={{ color: "#a78bfa", fontWeight: "600" }}>выберите</span>
                </p>
                <p style={{ margin: "8px 0 0", color: "#64748b", fontSize: "12px" }}>
                  Поддерживаются: .txt, .json, .cookies
                </p>
              </div>
              {file && (
                <div style={{
                  marginTop: "12px",
                  padding: "12px 16px",
                  background: "linear-gradient(135deg, rgba(16, 185, 129, 0.1), rgba(16, 185, 129, 0.05))",
                  borderRadius: "12px",
                  border: "1px solid rgba(16, 185, 129, 0.25)",
                  display: "flex",
                  alignItems: "center",
                  gap: "10px"
                }}>
                  <FileIcon size={18} color="#34d399" />
                  <span style={{ color: "#34d399", fontWeight: "500", fontSize: "14px" }}>
                    {file.name}
                  </span>
                  <span style={{ color: "#64748b", fontSize: "12px" }}>
                    ({(file.size / 1024).toFixed(2)} KB)
                  </span>
                </div>
              )}
            </div>
          ) : (
            <div style={{ marginBottom: "24px" }}>
              <label style={labelStyle}>Текст куки:</label>
              <textarea
                value={cookieText}
                onChange={(e) => setCookieText(e.target.value)}
                placeholder="Вставьте содержимое файла куки здесь..."
                style={{
                  width: "100%",
                  height: "200px",
                  padding: "16px",
                  border: "1px solid rgba(148,163,184,0.2)",
                  borderRadius: "14px",
                  backgroundColor: "rgba(15,23,42,0.8)",
                  color: "#e2e8f0",
                  fontFamily: "'JetBrains Mono', monospace",
                  fontSize: "13px",
                  resize: "vertical",
                  outline: "none",
                  transition: "all 0.2s ease"
                }}
                onFocus={(e) => {
                  e.currentTarget.style.borderColor = "rgba(139, 92, 246, 0.5)";
                  e.currentTarget.style.boxShadow = "0 0 0 3px rgba(139, 92, 246, 0.15)";
                }}
                onBlur={(e) => {
                  e.currentTarget.style.borderColor = "rgba(148,163,184,0.2)";
                  e.currentTarget.style.boxShadow = "none";
                }}
              />
            </div>
          )}

          <div style={{ marginBottom: "28px" }}>
            <label style={labelStyle}>Выберите прокси:</label>
            <select
              value={selectedProxy}
              onChange={(e) => setSelectedProxy(e.target.value)}
              style={{
                ...inputStyle,
                cursor: "pointer"
              }}
              onFocus={(e) => {
                e.currentTarget.style.borderColor = "rgba(139, 92, 246, 0.5)";
                e.currentTarget.style.boxShadow = "0 0 0 3px rgba(139, 92, 246, 0.15)";
              }}
              onBlur={(e) => {
                e.currentTarget.style.borderColor = "rgba(148,163,184,0.2)";
                e.currentTarget.style.boxShadow = "none";
              }}
            >
              <option value="">Без прокси</option>
              {availableProxies?.map((proxy) => (
                <option key={proxy.id} value={proxy.id}>
                  {proxy.name} ({proxy.host}:{proxy.port})
                </option>
              ))}
            </select>
          </div>

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
              type="submit"
              disabled={loading}
              className="primary-button"
              style={{
                padding: "12px 28px",
                color: "white",
                border: "none",
                cursor: loading ? "not-allowed" : "pointer",
                borderRadius: "14px",
                fontSize: "14px",
                fontWeight: "600",
                display: "flex",
                alignItems: "center",
                gap: "8px"
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
                  Добавление...
                </>
              ) : (
                <>
                  <PlusIcon size={16} />
                  Добавить аккаунт
                </>
              )}
            </button>
          </div>
        </form>
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
  );
};

export default AddAccountModal;
