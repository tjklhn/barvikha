const fs = require("fs");
const path = require("path");

const dataDir = path.join(__dirname, "..", "data");
const dataPath = path.join(dataDir, "accounts.json");

const ensureStore = () => {
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
  }
  if (!fs.existsSync(dataPath)) {
    fs.writeFileSync(
      dataPath,
      JSON.stringify({ lastId: 0, accounts: [] }, null, 2)
    );
  }
};

const readStore = () => {
  ensureStore();
  try {
    const raw = fs.readFileSync(dataPath, "utf8");
    return JSON.parse(raw);
  } catch (error) {
    const fallback = { lastId: 0, accounts: [] };
    try {
      fs.writeFileSync(dataPath, JSON.stringify(fallback, null, 2));
    } catch (writeError) {
      // ignore write errors, fallback will be used in memory
    }
    return fallback;
  }
};

const writeStore = (store) => {
  fs.writeFileSync(dataPath, JSON.stringify(store, null, 2));
};

const listAccounts = () => {
  const store = readStore();
  return [...store.accounts].reverse();
};

const insertAccount = (account) => {
  const store = readStore();
  const nextId = store.lastId + 1;
  store.lastId = nextId;
  store.accounts.push({ id: nextId, ...account });
  writeStore(store);
  return nextId;
};

const deleteAccount = (id) => {
  const store = readStore();
  const initialLength = store.accounts.length;
  store.accounts = store.accounts.filter((acc) => acc.id !== id);
  if (store.accounts.length === initialLength) {
    return false;
  }
  writeStore(store);
  return true;
};

const getAccountById = (id) => {
  const store = readStore();
  return store.accounts.find((acc) => acc.id === id) || null;
};

const updateAccount = (id, updates = {}) => {
  const store = readStore();
  const idx = store.accounts.findIndex((acc) => acc.id === id);
  if (idx === -1) return null;
  store.accounts[idx] = { ...store.accounts[idx], ...updates };
  writeStore(store);
  return store.accounts[idx];
};

const countAccountsByStatus = (status) => {
  const store = readStore();
  return store.accounts.filter((acc) => acc.status === status).length;
};

module.exports = {
  listAccounts,
  insertAccount,
  deleteAccount,
  getAccountById,
  updateAccount,
  countAccountsByStatus
};
