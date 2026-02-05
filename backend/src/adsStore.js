const fs = require("fs");
const path = require("path");

const dataDir = path.join(__dirname, "..", "data");
const adsPath = path.join(dataDir, "ads.json");

const ensureStore = () => {
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
  }
  if (!fs.existsSync(adsPath)) {
    fs.writeFileSync(adsPath, JSON.stringify({ ads: [] }, null, 2));
  }
};

const readStore = () => {
  ensureStore();
  const raw = fs.readFileSync(adsPath, "utf8");
  return JSON.parse(raw);
};

const writeStore = (store) => {
  fs.writeFileSync(adsPath, JSON.stringify(store, null, 2));
};

const listAds = () => {
  const store = readStore();
  return store.ads || [];
};

const upsertAd = (ad) => {
  const store = readStore();
  const ads = store.ads || [];
  const key = `${ad.accountId}-${ad.title}-${ad.price || ""}`;
  const existingIndex = ads.findIndex((item) => `${item.accountId}-${item.title}-${item.price || ""}` === key);
  if (existingIndex >= 0) {
    ads[existingIndex] = { ...ads[existingIndex], ...ad };
  } else {
    ads.push(ad);
  }
  store.ads = ads;
  writeStore(store);
  return ad;
};

module.exports = {
  listAds,
  upsertAd
};
