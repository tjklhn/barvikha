#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FRONTEND_DIR="$ROOT_DIR/frontend"
BACKEND_DIR="$ROOT_DIR/backend"

log() {
  printf '[deploy] %s\n' "$*"
}

resolve_app_version() {
  local version="dev"
  if command -v git >/dev/null 2>&1 && [ -d "$ROOT_DIR/.git" ]; then
    version="$(git -C "$ROOT_DIR" rev-parse --short HEAD 2>/dev/null || echo dev)"
  else
    version="$(date +%Y%m%d-%H%M%S)"
  fi
  printf '%s' "$version"
}

ensure_frontend_api_base() {
  local api_file="$FRONTEND_DIR/src/api.js"

  if [ ! -f "$api_file" ]; then
    log "skip api.js patch: not found"
    return
  fi

  # Keep API base empty by default and rely on Nginx reverse proxy (/api -> backend).
  sed -i 's|const API_BASE = process.env.REACT_APP_API_BASE || "/api";|const API_BASE = process.env.REACT_APP_API_BASE || "";|g' "$api_file"
  sed -i 's|const API_BASE = process.env.REACT_APP_API_BASE || "http://localhost:5000";|const API_BASE = process.env.REACT_APP_API_BASE || "";|g' "$api_file"
}

sanitize_frontend_env() {
  local env_file
  for env_file in "$FRONTEND_DIR/.env" "$FRONTEND_DIR/.env.local" "$FRONTEND_DIR/.env.production" "$FRONTEND_DIR/.env.production.local"; do
    [ -f "$env_file" ] || continue
    # REACT_APP_API_BASE=/api + paths already containing /api cause /api/api.
    sed -i 's|^REACT_APP_API_BASE=/api/*$|REACT_APP_API_BASE=|g' "$env_file"
    sed -i 's|^REACT_APP_API_BASE=http://localhost:5000/*$|REACT_APP_API_BASE=|g' "$env_file"
  done
}

build_frontend() {
  log "build frontend"
  cd "$FRONTEND_DIR"
  npm install
  rm -rf build
  npm run build

  if grep -R -n "/api/api/" build >/dev/null 2>&1; then
    log "error: frontend build still contains /api/api/"
    grep -R -n "/api/api/" build | head -n 5
    exit 1
  fi
}

install_backend() {
  log "install backend deps"
  cd "$BACKEND_DIR"
  if [ -f package-lock.json ]; then
    npm ci --omit=dev || npm install --omit=dev
  else
    npm install --omit=dev
  fi
}

restart_services() {
  log "restart services"
  if systemctl list-unit-files | grep -q '^kl-backend.service'; then
    systemctl restart kl-backend
  fi
  systemctl restart nginx
}

smoke_checks() {
  log "smoke checks"
  curl -fsS http://127.0.0.1/ >/dev/null

  local code
  code="$(curl -sS -o /tmp/kl-auth-validate.json -w '%{http_code}' \
    -X POST http://127.0.0.1/api/auth/validate \
    -H 'Content-Type: application/json' \
    -d '{"token":"__probe__"}')"

  if [ "$code" != "200" ]; then
    log "error: /api/auth/validate returned HTTP $code"
    cat /tmp/kl-auth-validate.json || true
    exit 1
  fi

  log "frontend main bundle: $(curl -s http://127.0.0.1/ | grep -o 'main\\.[^\"]*\\.js' | head -n 1)"
  log "auth validate response: $(cat /tmp/kl-auth-validate.json)"
}

main() {
  log "root: $ROOT_DIR"

  if [ -d "$ROOT_DIR/.git" ] && [ "${1:-}" != "--no-pull" ]; then
    log "git pull"
    cd "$ROOT_DIR"
    git pull --ff-only
  fi

  export REACT_APP_VERSION
  REACT_APP_VERSION="$(resolve_app_version)"
  log "app version: v$REACT_APP_VERSION"

  ensure_frontend_api_base
  sanitize_frontend_env
  install_backend
  build_frontend
  restart_services
  smoke_checks

  log "done"
}

main "$@"
