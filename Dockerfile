FROM node:18-alpine

ENV PUPPETEER_SKIP_DOWNLOAD=1     PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium-browser

WORKDIR /app

RUN apk add --no-cache   chromium   nss   freetype   harfbuzz   ca-certificates   ttf-freefont   font-noto-emoji

COPY backend/package*.json ./

RUN npm install --omit=dev

COPY backend/ ./

RUN mkdir -p uploads/cookies uploads/images

USER node

EXPOSE 5000

CMD ["node", "src/index.js"]
