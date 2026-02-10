#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BACKEND_DIR="$ROOT_DIR/backend"
FRONTEND_DIR="$ROOT_DIR/frontend"

log() {
  printf '[dev] %s\n' "$*"
}

load_env_file() {
  local file="$1"
  [ -f "$file" ] || return 0
  # shellcheck disable=SC1090
  set -a
  . "$file"
  set +a
}

# Optional local env files (not committed).
load_env_file "$ROOT_DIR/.env.local"
load_env_file "$BACKEND_DIR/.env.local"
load_env_file "$BACKEND_DIR/.env"
load_env_file "$FRONTEND_DIR/.env.local"
load_env_file "$FRONTEND_DIR/.env"

export PORT="${PORT:-5000}"
export BROWSER="${BROWSER:-none}"

log "root: $ROOT_DIR"
log "backend: http://127.0.0.1:$PORT"
log "frontend: http://127.0.0.1:3000"
log "artifacts: $BACKEND_DIR/data/debug/message-action-artifacts"
log "Ctrl+C to stop"

if [ ! -d "$BACKEND_DIR/node_modules" ]; then
  log "install backend deps"
  (cd "$BACKEND_DIR" && npm install)
fi

if [ ! -d "$FRONTEND_DIR/node_modules" ]; then
  log "install frontend deps"
  (cd "$FRONTEND_DIR" && npm install)
fi

log "start backend (nodemon)"
(cd "$BACKEND_DIR" && npm run dev) &
BACK_PID="$!"

for _ in {1..40}; do
  if curl -fsS "http://127.0.0.1:$PORT/health" >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done

log "start frontend (react-scripts)"
(cd "$FRONTEND_DIR" && npm start) &
FRONT_PID="$!"

cleanup() {
  log "stopping..."
  kill "$FRONT_PID" "$BACK_PID" >/dev/null 2>&1 || true
}

trap cleanup INT TERM EXIT
wait
