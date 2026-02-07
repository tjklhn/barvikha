import React, { useState, useEffect, useRef } from "react";
import { apiFetchJson } from "../api";
import { MessageIcon } from "./Icons";

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

const MessagesTab = () => {
  const [messages, setMessages] = useState([]);
  const [selectedMessage, setSelectedMessage] = useState(null);
  const [replyText, setReplyText] = useState("");
  const [sending, setSending] = useState(false);
  const [loading, setLoading] = useState(false);
  const [loadingThread, setLoadingThread] = useState(false);
  const [error, setError] = useState(null);
  const [isMobileView, setIsMobileView] = useState(() => detectMobileView());
  const chatScrollRef = useRef(null);

  useEffect(() => {
    loadMessages();
  }, []);

  useEffect(() => {
    if (!selectedMessage) return;
    const container = chatScrollRef.current;
    if (!container) return;
    container.scrollTo({ top: container.scrollHeight, behavior: "smooth" });
  }, [selectedMessage?.id, selectedMessage?.messages]);

  useEffect(() => {
    const handleResize = () => {
      setIsMobileView(detectMobileView());
    };

    window.addEventListener("resize", handleResize);
    return () => window.removeEventListener("resize", handleResize);
  }, []);

  const loadMessages = async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await apiFetchJson("/api/messages");
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
      setMessages(deduped);
    } catch (err) {
      console.error("Ошибка загрузки сообщений:", err);
      setError(err.message || "Не удалось загрузить сообщения");
    } finally {
      setLoading(false);
    }
  };

  const markAsRead = async (messageId) => {
    // Immediately update UI for better responsiveness
    setMessages(prev => prev.map(msg =>
      msg.id === messageId ? { ...msg, unread: false } : msg
    ));

    try {
      await apiFetchJson(`/api/messages/${messageId}/read`, {
        method: "PUT"
      });
    } catch (err) {
      console.error("Ошибка при отметке сообщения как прочитанного:", err);
      // Optionally revert on error
    }
  };

  const sendReply = async () => {
    if (!replyText.trim() || !selectedMessage) return;
    if (!selectedMessage.conversationId && !selectedMessage.conversationUrl
      && !selectedMessage.sender && !selectedMessage.adTitle) {
      alert("Не найден идентификатор диалога.");
      return;
    }

    setSending(true);
    try {
      const result = await apiFetchJson("/api/messages/send", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          accountId: selectedMessage.accountId,
          conversationId: selectedMessage.conversationId,
          conversationUrl: selectedMessage.conversationUrl,
          participant: selectedMessage.sender,
          adTitle: selectedMessage.adTitle,
          text: replyText
        })
      });
      if (result.success) {
        const resolvedConversationId = result.conversationId || selectedMessage.conversationId;
        const resolvedConversationUrl = result.conversationUrl || selectedMessage.conversationUrl;
        const latestMessages = result.messages || [];
        const lastMessage = latestMessages[latestMessages.length - 1];
        if (lastMessage) {
          setMessages(prev => prev.map(msg => (
            msg.id === selectedMessage.id
              ? {
                ...msg,
                message: lastMessage.text,
                date: lastMessage.date,
                time: lastMessage.time,
                conversationId: resolvedConversationId || msg.conversationId,
                conversationUrl: resolvedConversationUrl || msg.conversationUrl
              }
              : msg
          )));
        }
        setSelectedMessage(prev => ({
          ...prev,
          messages: latestMessages,
          conversationId: resolvedConversationId,
          conversationUrl: resolvedConversationUrl
        }));
        setReplyText("");
      } else {
        alert("Ошибка: " + (result.error || "Не удалось отправить"));
      }
    } catch (err) {
      console.error("Ошибка отправки:", err);
      alert("Ошибка при отправке сообщения");
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

  const fetchThread = async (message) => {
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
        const updated = {
          ...message,
          adTitle: data.adTitle || message.adTitle,
          adImage: data.adImage || message.adImage,
          sender: message.sender || data.participant || "",
          messages: data.messages || [],
          conversationId: data.conversationId || message.conversationId,
          conversationUrl: data.conversationUrl || message.conversationUrl
        };
        setSelectedMessage(updated);
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
        {showConversationList && <div className="messages-list" style={{
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
          backdropFilter: "blur(10px)"
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
              messages.map(message => (
                <div
                  key={message.id}
                  className="msg-conv-item"
                  onClick={() => {
                    setSelectedMessage(message);
                    fetchThread(message);
                    if (message.unread) markAsRead(message.id);
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
                    borderRadius: "12px",
                    overflow: "hidden",
                    background: "rgba(15,23,42,0.6)",
                    border: "1px solid rgba(148,163,184,0.15)",
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center",
                    flexShrink: 0
                  }}>
                    {message.adImage ? (
                      <img
                        src={message.adImage}
                        alt=""
                        style={{ width: "100%", height: "100%", objectFit: "cover" }}
                        onError={(e) => { e.target.style.display = "none"; }}
                      />
                    ) : (
                      <div style={{
                        width: "100%",
                        height: "100%",
                        background: message.unread
                          ? "linear-gradient(135deg, #22c55e, #16a34a)"
                          : "linear-gradient(135deg, #3b82f6, #2563eb)",
                        color: "white",
                        display: "flex",
                        alignItems: "center",
                        justifyContent: "center",
                        fontSize: "20px",
                        fontWeight: 700
                      }}>
                        {(message.sender || "?").charAt(0).toUpperCase()}
                      </div>
                    )}
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
              ))
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
        </div>}

        {/* Chat Panel */}
        {showChatPanel && <div className="messages-chat" style={{
          flex: 1,
          background: "linear-gradient(145deg, rgba(30, 41, 59, 0.6) 0%, rgba(15, 23, 42, 0.95) 100%)",
          borderRadius: "20px",
          overflow: "hidden",
          boxShadow: "0 20px 50px rgba(0,0,0,0.4), 0 0 30px rgba(0,0,0,0.2)",
          border: "1px solid rgba(148,163,184,0.12)",
          display: "flex",
          flexDirection: "column",
          color: "#e2e8f0",
          backdropFilter: "blur(10px)"
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
                      {selectedMessage.adImage ? (
                        <img
                          src={selectedMessage.adImage}
                          alt=""
                          style={{ width: "100%", height: "100%", objectFit: "cover" }}
                          onError={(e) => { e.target.style.display = "none"; }}
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
                            maxWidth: "70%",
                            minWidth: "120px"
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

                            {/* Message bubble */}
                            <div style={{
                              padding: "10px 14px",
                              borderRadius: isOutgoing ? "14px 14px 4px 14px" : "14px 14px 14px 4px",
                              background: isOutgoing
                                ? "linear-gradient(135deg, #1a4731, #14532d)"
                                : "rgba(15,23,42,0.8)",
                              border: isOutgoing
                                ? "1px solid rgba(34,197,94,0.25)"
                                : "1px solid rgba(148,163,184,0.15)"
                            }}>
                              <div style={{
                                fontSize: "14px",
                                lineHeight: "1.5",
                                whiteSpace: "pre-wrap",
                                wordBreak: "break-word",
                                color: "#e2e8f0"
                              }}>
                                {messageItem.text}
                              </div>

                              {/* Timestamp */}
                              <div style={{
                                fontSize: "10px",
                                color: "#64748b",
                                marginTop: "6px",
                                textAlign: "right"
                              }}>
                                {formatMessageDate(
                                  messageItem.date || selectedMessage.date,
                                  messageItem.time || selectedMessage.time
                                )}
                              </div>
                            </div>
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
                <div style={{ display: "flex", gap: "10px", alignItems: "flex-end" }}>
                  <textarea
                    value={replyText}
                    onChange={(e) => setReplyText(e.target.value)}
                    onKeyDown={handleKeyDown}
                    placeholder="Введите сообщение..."
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
                    disabled={sending || !replyText.trim()}
                    style={{
                      padding: "10px 20px",
                      background: sending
                        ? "rgba(34,197,94,0.4)"
                        : !replyText.trim()
                          ? "rgba(148,163,184,0.15)"
                          : "linear-gradient(135deg, #22c55e, #16a34a)",
                      color: !replyText.trim() ? "#64748b" : "white",
                      border: "none",
                      borderRadius: "12px",
                      cursor: sending || !replyText.trim() ? "not-allowed" : "pointer",
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
        </div>}
      </div>
    </>
  );
};

export default MessagesTab;
