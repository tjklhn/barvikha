#!/usr/bin/env node
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const dataPath = path.join(__dirname, "..", "data", "subscription-tokens.json");

const args = process.argv.slice(2);
const getArgValue = (name) => {
  const index = args.findIndex((arg) => arg === name);
  if (index === -1) return null;
  return args[index + 1] || null;
};
const hasFlag = (name) => args.includes(name);

const toNumber = (value) => {
  if (value === null || value === undefined) return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
};

const now = Date.now();
const days = toNumber(getArgValue("--days")) || 0;
const hours = toNumber(getArgValue("--hours")) || 0;
const expiresAtArg = getArgValue("--expires");
const label = getArgValue("--label") || "";
const roleArg = getArgValue("--role");
const isAdmin = hasFlag("--admin") || String(roleArg || "").toLowerCase() === "admin";
const ownerIdArg = getArgValue("--owner");

let expiresAt = null;
if (expiresAtArg) {
  const parsed = new Date(expiresAtArg);
  if (!Number.isNaN(parsed.getTime())) {
    expiresAt = parsed.toISOString();
  }
} else if (days || hours) {
  const ms = now + (days * 24 * 60 * 60 * 1000) + (hours * 60 * 60 * 1000);
  expiresAt = new Date(ms).toISOString();
}

const token = crypto.randomBytes(24).toString("hex");

let store = { items: [] };
try {
  if (fs.existsSync(dataPath)) {
    store = JSON.parse(fs.readFileSync(dataPath, "utf8"));
  }
} catch (error) {
  store = { items: [] };
}

if (!Array.isArray(store.items)) store.items = [];

store.items.push({
  token,
  label,
  role: isAdmin ? "admin" : "user",
  ownerId: ownerIdArg || undefined,
  createdAt: new Date(now).toISOString(),
  expiresAt
});

fs.writeFileSync(dataPath, JSON.stringify(store, null, 2), "utf8");

console.log("Token:", token);
console.log("Expires:", expiresAt || "never");
if (label) {
  console.log("Label:", label);
}
if (isAdmin) {
  console.log("Role: admin");
}
