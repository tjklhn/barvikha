#!/usr/bin/env node
/* eslint-disable no-console */
"use strict";

const fs = require("fs");
const path = require("path");
const { spawn } = require("child_process");

const FRONTEND_DIR = path.resolve(__dirname, "..");
const REPO_ROOT = path.resolve(FRONTEND_DIR, "..");

const readText = (filePath) => {
  try {
    return fs.readFileSync(filePath, "utf8");
  } catch (error) {
    return "";
  }
};

const resolveGitDir = (repoRoot) => {
  const gitPath = path.join(repoRoot, ".git");
  try {
    const stat = fs.statSync(gitPath);
    if (stat.isDirectory()) return gitPath;
  } catch (error) {
    // ignore
  }

  // Worktree/submodule can store .git as a file: "gitdir: /path/to/dir"
  const maybeFile = readText(gitPath).trim();
  if (!maybeFile) return "";
  const match = maybeFile.match(/^gitdir:\s*(.+)\s*$/i);
  if (!match) return "";
  const dirRaw = match[1].trim();
  if (!dirRaw) return "";
  return path.resolve(repoRoot, dirRaw);
};

const resolveHeadHash = (gitDir) => {
  if (!gitDir) return "";
  const head = readText(path.join(gitDir, "HEAD")).trim();
  if (!head) return "";

  const refMatch = head.match(/^ref:\s*(.+)\s*$/i);
  if (!refMatch) {
    // Detached HEAD, HEAD contains the hash.
    return head;
  }

  const ref = refMatch[1].trim();
  if (!ref) return "";

  const looseRef = readText(path.join(gitDir, ref)).trim();
  if (looseRef) return looseRef;

  const packedRefsPath = path.join(gitDir, "packed-refs");
  const packed = readText(packedRefsPath);
  if (!packed) return "";

  // Lines are: "<hash> <ref>" (ignore comments and annotated tags).
  const lines = packed.split(/\r?\n/);
  for (const line of lines) {
    if (!line || line.startsWith("#") || line.startsWith("^")) continue;
    const spaceIndex = line.indexOf(" ");
    if (spaceIndex <= 0) continue;
    const hash = line.slice(0, spaceIndex).trim();
    const refName = line.slice(spaceIndex + 1).trim();
    if (refName === ref) return hash;
  }
  return "";
};

const toShortSha = (hash) => {
  const normalized = String(hash || "").trim();
  if (!/^[0-9a-f]{7,}$/i.test(normalized)) return "";
  return normalized.slice(0, 8).toLowerCase();
};

const resolveVersion = () => {
  const fromEnv = String(process.env.REACT_APP_VERSION || "").trim();
  if (fromEnv && fromEnv.toLowerCase() !== "dev") {
    return fromEnv;
  }

  const gitDir = resolveGitDir(REPO_ROOT);
  const headHash = resolveHeadHash(gitDir);
  const shortSha = toShortSha(headHash);
  if (shortSha) return shortSha;

  // Fallback: timestamp, so deployments are still visibly different.
  return new Date().toISOString().replace(/[-:TZ.]/g, "").slice(0, 12);
};

const version = resolveVersion();
process.env.REACT_APP_VERSION = version;
console.log(`[build] REACT_APP_VERSION=${version}`);

const buildScriptPath = require.resolve("react-scripts/scripts/build");
const nodeBinary = process.execPath;
const child = spawn(nodeBinary, [buildScriptPath], {
  cwd: FRONTEND_DIR,
  stdio: "inherit",
  env: process.env
});

child.on("exit", (code) => {
  process.exit(code == null ? 1 : code);
});

