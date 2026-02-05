const express = require("express");
const cors = require("cors");
const multer = require("multer");
const path = require("path");
const fs = require("fs");
const proxyChecker = require("./src/proxyChecker");
const app = express();
const PORT = 5000;

// Настройка multer для загрузки файлов
const storage = multer.diskStorage({
  destination: function (req, file, cb) {
    const uploadDir = "uploads/cookies/";
    if (!fs.existsSync(uploadDir)) {
      fs.mkdirSync(uploadDir, { recursive: true });
    }
    cb(null, uploadDir);
  },
  filename: function (req, file, cb) {
    const uniqueName = Date.now() + "-" + Math.round(Math.random() * 1E9) + path.extname(file.originalname);
    cb(null, uniqueName);
  }
});

const upload = multer({ 
  storage: storage,
  limits: { fileSize: 5 * 1024 * 1024 },
  fileFilter: function (req, file, cb) {
    const filetypes = /txt|json|cookies/;
    const extname = filetypes.test(path.extname(file.originalname).toLowerCase());
    if (extname) {
      return cb(null, true);
    }
    cb(new Error("Только файлы .txt, .json, .cookies"));
  }
});

// Middleware
app.use(cors());
app.use(express.json());
app.use("/uploads", express.static("uploads"));

// Маршруты
app.get("/", (req, res) => {
  res.json({ 
    message: "✅ Kleinanzeigen Backend работает!",
    endpoints: [
      "/api/accounts - список аккаунтов",
      "/api/proxies - список прокси",
      "/api/proxies/check - проверка прокси",
      "/api/messages - список сообщений",
      "/api/stats - статистика",
      "/health - статус сервиса"
    ]
  });
});

// База данных в памяти (для демо)
let accounts = [
  { id: 1, username: "user_klein_1", status: "active", added: "2024-01-29", proxyId: 1 },
  { id: 2, username: "user_klein_2", status: "checking", added: "2024-01-30", proxyId: 2 },
  { id: 3, username: "user_klein_3", status: "active", added: "2024-01-28", proxyId: 1 },
  { id: 4, username: "user_klein_4", status: "inactive", added: "2024-01-27", proxyId: null }
];

let proxies = [
  { 
    id: 1, 
    name: "Credixis Proxy", 
    type: "http", 
    host: "proxy.credixis.com", 
    port: 8080, 
    status: "unknown",
    lastChecked: null,
    checkResult: null
  },
  { 
    id: 2, 
    name: "Germany Proxy", 
    type: "http", 
    host: "85.214.139.189", 
    port: 8080, 
    status: "unknown",
    lastChecked: null,
    checkResult: null
  },
  { 
    id: 3, 
    name: "USA Proxy", 
    type: "socks5", 
    host: "45.77.56.113", 
    port: 1080, 
    status: "unknown",
    lastChecked: null,
    checkResult: null
  },
  { 
    id: 4, 
    name: "Netherlands Proxy", 
    type: "http", 
    host: "146.185.204.187", 
    port: 8080, 
    status: "unknown",
    lastChecked: null,
    checkResult: null
  }
];

let proxyCheckResults = {};

// Аккаунты
app.get("/api/accounts", (req, res) => {
  const accountsWithProxy = accounts.map(account => {
    const proxy = proxies.find(p => p.id === account.proxyId);
    return {
      ...account,
      proxy: proxy ? proxy.name : "Нет"
    };
  });
  res.json(accountsWithProxy);
});

// Добавление аккаунта
app.post("/api/accounts/upload", upload.single("cookieFile"), (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: "Файл не загружен" });
    }

    const { proxyId } = req.body;
    
    const newAccount = {
      id: Date.now(),
      username: `user_${Date.now().toString().slice(-6)}`,
      status: "checking",
      added: new Date().toISOString().split("T")[0],
      proxyId: proxyId ? parseInt(proxyId) : null,
      cookieFile: req.file.filename,
      fileSize: (req.file.size / 1024).toFixed(2) + " KB"
    };

    accounts.unshift(newAccount);
    
    setTimeout(() => {
      newAccount.status = Math.random() > 0.3 ? "active" : "failed";
      res.json({
        success: true,
        message: "Аккаунт успешно добавлен",
        account: newAccount
      });
    }, 1500);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Удаление аккаунта
app.delete("/api/accounts/:id", (req, res) => {
  const id = parseInt(req.params.id);
  const index = accounts.findIndex(acc => acc.id === id);
  
  if (index !== -1) {
    accounts.splice(index, 1);
    res.json({ success: true, message: "Аккаунт удален" });
  } else {
    res.status(404).json({ error: "Аккаунт не найден" });
  }
});

// Получение списка прокси
app.get("/api/proxies", (req, res) => {
  // Обновляем статусы прокси на основе последних проверок
  const proxiesWithStatus = proxies.map(proxy => {
    const checkResult = proxyCheckResults[proxy.id];
    return {
      ...proxy,
      status: checkResult?.success ? "active" : 
              checkResult?.success === false ? "failed" : "unknown",
      lastChecked: checkResult?.timestamp,
      checkResult: checkResult
    };
  });
  
  res.json(proxiesWithStatus);
});

// Проверка конкретного прокси
app.post("/api/proxies/:id/check", async (req, res) => {
  try {
    const proxyId = parseInt(req.params.id);
    const proxy = proxies.find(p => p.id === proxyId);
    
    if (!proxy) {
      return res.status(404).json({ error: "Прокси не найден" });
    }

    const checkResult = await proxyChecker.checkProxy(proxy);
    proxyCheckResults[proxyId] = checkResult;

    // Обновляем статус прокси в списке
    const proxyIndex = proxies.findIndex(p => p.id === proxyId);
    if (proxyIndex !== -1) {
      proxies[proxyIndex].lastChecked = new Date().toISOString();
      proxies[proxyIndex].status = checkResult.success ? "active" : "failed";
    }

    res.json({
      success: true,
      proxyId: proxyId,
      ...checkResult
    });
  } catch (error) {
    res.status(500).json({ 
      success: false, 
      error: error.message 
    });
  }
});

// Быстрая проверка всех прокси
app.post("/api/proxies/check-all", async (req, res) => {
  try {
    const results = [];
    
    for (const proxy of proxies) {
      const checkResult = await proxyChecker.checkProxy(proxy);
      proxyCheckResults[proxy.id] = checkResult;
      
      results.push({
        proxyId: proxy.id,
        proxyName: proxy.name,
        ...checkResult
      });
      
      // Задержка между проверками
      await new Promise(resolve => setTimeout(resolve, 2000));
    }

    res.json({
      success: true,
      results: results,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    res.status(500).json({ 
      success: false, 
      error: error.message 
    });
  }
});

// Добавление нового прокси
app.post("/api/proxies", async (req, res) => {
  try {
    const { name, type, host, port, username, password } = req.body;
    
    // Проверяем валидность данных
    if (!name || !type || !host || !port) {
      return res.status(400).json({ error: "Заполните все обязательные поля" });
    }

    const newProxy = {
      id: Date.now(),
      name,
      type: type.toLowerCase(),
      host,
      port: parseInt(port),
      status: "checking",
      lastChecked: null,
      checkResult: null
    };

    if (username) newProxy.username = username;
    if (password) newProxy.password = password;

    // Проверяем прокси перед добавлением
    const checkResult = await proxyChecker.checkProxy(newProxy);
    
    newProxy.status = checkResult.success ? "active" : "failed";
    newProxy.lastChecked = new Date().toISOString();
    newProxy.checkResult = checkResult;

    proxies.push(newProxy);
    proxyCheckResults[newProxy.id] = checkResult;

    res.json({
      success: true,
      message: checkResult.success ? "Прокси добавлен и работает" : "Прокси добавлен, но не работает",
      proxy: newProxy,
      checkResult: checkResult
    });
  } catch (error) {
    res.status(500).json({ 
      success: false, 
      error: error.message 
    });
  }
});

// Удаление прокси
app.delete("/api/proxies/:id", (req, res) => {
  const id = parseInt(req.params.id);
  const index = proxies.findIndex(p => p.id === id);
  
  if (index !== -1) {
    proxies.splice(index, 1);
    delete proxyCheckResults[id];
    res.json({ success: true, message: "Прокси удален" });
  } else {
    res.status(404).json({ error: "Прокси не найден" });
  }
});

// Сообщения
let messages = [
  { 
    id: 1, 
    accountId: 1, 
    accountName: "user_klein_1",
    sender: "Покупатель 1", 
    message: "Здравствуйте! Подскажите, товар в наличии?", 
    time: "10:30", 
    date: "2024-01-30",
    unread: true,
    type: "incoming"
  }
];

app.get("/api/messages", (req, res) => {
  res.json(messages);
});

// Отметить сообщение как прочитанное
app.put("/api/messages/:id/read", (req, res) => {
  const id = parseInt(req.params.id);
  const message = messages.find(msg => msg.id === id);
  
  if (message) {
    message.unread = false;
    res.json({ success: true, message: "Сообщение прочитано" });
  } else {
    res.status(404).json({ error: "Сообщение не найдено" });
  }
});

// Отправить сообщение
app.post("/api/messages/send", (req, res) => {
  const { accountId, text } = req.body;
  
  const newMessage = {
    id: Date.now(),
    accountId,
    accountName: `user_${accountId}`,
    sender: "Вы",
    message: text,
    time: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
    date: new Date().toISOString().split("T")[0],
    unread: false,
    type: "outgoing"
  };
  
  messages.unshift(newMessage);
  
  setTimeout(() => {
    const replyMessage = {
      id: Date.now() + 1,
      accountId,
      accountName: `user_${accountId}`,
      sender: "Покупатель",
      message: "Спасибо за информацию!",
      time: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
      date: new Date().toISOString().split("T")[0],
      unread: true,
      type: "incoming"
    };
    
    messages.unshift(replyMessage);
    
    res.json({
      success: true,
      message: newMessage,
      reply: replyMessage
    });
  }, 1000);
});

// Статистика
app.get("/api/stats", (req, res) => {
  const workingProxies = proxies.filter(p => p.status === "active").length;
  
  const stats = {
    accounts: {
      total: accounts.length,
      active: accounts.filter(a => a.status === "active").length,
      checking: accounts.filter(a => a.status === "checking").length,
      failed: accounts.filter(a => a.status === "failed" || a.status === "inactive").length
    },
    messages: {
      total: messages.length,
      unread: messages.filter(m => m.unread).length,
      today: messages.filter(m => m.date === new Date().toISOString().split("T")[0]).length
    },
    proxies: {
      total: proxies.length,
      active: workingProxies,
      failed: proxies.length - workingProxies
    }
  };
  
  res.json(stats);
});

// Создание объявления
app.post("/api/ads/create", (req, res) => {
  setTimeout(() => {
    res.json({
      success: true,
      message: "Объявление успешно создано",
      ad: {
        id: Date.now(),
        title: req.body.title || "Тестовое объявление",
        status: "published",
        url: "https://www.kleinanzeigen.de/s-anzeige/test",
        price: req.body.price || "0",
        currency: req.body.currency || "EUR"
      }
    });
  }, 1500);
});

// Health check
app.get("/health", (req, res) => {
  res.json({
    status: "healthy",
    service: "kleinanzeigen-backend",
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    version: "1.0.0"
  });
});

// Запуск сервера
app.listen(PORT, () => {
  console.log(`🚀 Backend запущен на порту ${PORT}`);
  console.log(`🌍 Откройте http://localhost:${PORT}`);
  console.log(`📡 Health check: http://localhost:${PORT}/health`);
  console.log(`👥 Аккаунты: http://localhost:${PORT}/api/accounts`);
  console.log(`🔗 Прокси: http://localhost:${PORT}/api/proxies`);
});
