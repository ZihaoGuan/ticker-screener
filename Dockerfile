FROM python:3.12-slim

ARG CODE_VERSION=unknown

ENV PYTHONUNBUFFERED=1 \
    PLAYWRIGHT_EXECUTABLE_PATH=/usr/bin/chromium \
    TICKER_SCREENER_CODE_VERSION=${CODE_VERSION}

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates chromium curl nodejs npm \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements*.txt ./
RUN pip install --no-cache-dir -r requirements.txt -r requirements-web.txt -r requirements-finviz.txt

COPY src ./src
COPY scripts ./scripts
COPY web ./web
COPY config ./config
COPY sql ./sql
COPY vendor ./vendor
COPY frontend/scripts ./frontend/scripts

CMD ["uvicorn", "web.app:app", "--host", "0.0.0.0", "--port", "8000"]
