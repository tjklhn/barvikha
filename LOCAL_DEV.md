# Локальный запуск (UI + Backend) и дебаг (png/html)

## Вариант A (проще): запуск через Node.js

Требуется: Node.js 18+ и npm.

1) Backend env (по желанию):
```bash
cd backend
cp .env.example .env.local
```

2) Запуск (одной командой):
```bash
cd ..
./scripts/dev-local.sh
```

Откроется:
- UI: `http://127.0.0.1:3000`
- Backend: `http://127.0.0.1:5000`

Артефакты дебага сохраняются сюда:
- `backend/data/debug/message-action-artifacts/<debugId>/`

Примечание:
- Для быстрого локального теста можно оставить `KL_REQUIRE_TOKEN=0` (в `backend/.env.local`).
- Чтобы было как на проде: `KL_REQUIRE_TOKEN=1` и задай `KL_ACCESS_TOKENS=...`, затем в UI введи тот же token.

## Вариант B: запуск через Docker (без установки Node)

Требуется: Docker Desktop.

```bash
docker compose up --build
```

Откроется:
- UI: `http://127.0.0.1:3000`
- Backend: `http://127.0.0.1:5000`

## Как смотреть “пошаговый” дебаг (png/html)

1) Сначала узнай `debugId` по последним действиям:
```bash
TOKEN="YOUR_TOKEN_IF_REQUIRED"
curl -s "http://127.0.0.1:5000/api/debug/message-actions/recent?accessToken=$TOKEN&limit=60"
```

2) Получи список артефактов по `debugId`:
```bash
TOKEN="YOUR_TOKEN_IF_REQUIRED"
DID="msg-media-..."   # или msg-decline-...
curl -s "http://127.0.0.1:5000/api/debug/message-actions/artifacts?accessToken=$TOKEN&debugId=$DID&limit=120"
```

3) Скачай нужный `.png`/`.html`:
```bash
TOKEN="YOUR_TOKEN_IF_REQUIRED"
DID="msg-media-..."
FILE="001-send-media-home-opened-....png" # или .html
curl -s "http://127.0.0.1:5000/api/debug/message-actions/artifacts/file?accessToken=$TOKEN&debugId=$DID&file=$FILE" -o "$FILE"
```

Если в UI ошибка “Failed to fetch” и в `/recent` нет твоего `requestId`, значит запрос не доходит до backend (тогда артефактов не будет) и нужно смотреть прокси/NGINX/домен/HTTPS (network path).
