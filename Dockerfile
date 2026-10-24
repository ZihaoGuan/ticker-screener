FROM node:22-bookworm-slim AS tradingview-runtime

WORKDIR /opt/tradingview
COPY deploy/tradingview-runtime/package.json deploy/tradingview-runtime/package-lock.json ./
RUN npm ci --omit=dev

FROM tradingview-runtime AS frontend-runtime
WORKDIR /frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1 npm ci --omit=dev

FROM python:3.12-slim

ARG CODE_VERSION=unknown

ENV PYTHONUNBUFFERED=1 \
    PLAYWRIGHT_EXECUTABLE_PATH=/usr/bin/chromium \
    TICKER_SCREENER_CODE_VERSION=${CODE_VERSION}

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates chromium curl libstdc++6 libatomic1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=tradingview-runtime /usr/local/bin/node /usr/local/bin/node
COPY --from=tradingview-runtime /opt/tradingview /opt/tradingview
ENV TRADINGVIEW_CLIENT_DIR=/opt/tradingview/node_modules/@mathieuc/tradingview

COPY requirements*.txt ./
RUN pip install --no-cache-dir -r requirements.txt -r requirements-web.txt -r requirements-finviz.txt

COPY src ./src
COPY scripts ./scripts
COPY web ./web
COPY config ./config
COPY sql ./sql
COPY vendor ./vendor
COPY frontend/package.json frontend/package-lock.json ./frontend/
COPY --from=frontend-runtime /frontend/node_modules ./frontend/node_modules
COPY frontend/scripts ./frontend/scripts

CMD ["uvicorn", "web.app:app", "--host", "0.0.0.0", "--port", "8000"]
