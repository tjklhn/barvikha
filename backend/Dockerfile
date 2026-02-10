FROM node:18-alpine

ENV PUPPETEER_SKIP_DOWNLOAD=1 \
    PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium

WORKDIR /app

RUN apk add --no-cache \
  chromium \
  nss \
  freetype \
  harfbuzz \
  ca-certificates \
  ttf-freefont \
  font-noto-emoji

COPY package*.json ./

RUN npm install --omit=dev

COPY . .

RUN mkdir -p uploads/cookies uploads/images data \
  && chown -R node:node /app

USER node

EXPOSE 5000

CMD ["node", "src/index.js"]
