#!/bin/sh
set -eu

if ! command -v node >/dev/null 2>&1 || ! command -v chromium >/dev/null 2>&1; then
  apt-get update
  apt-get install -y --no-install-recommends nodejs npm chromium curl
  rm -rf /var/lib/apt/lists/*
fi

cd /app
pip install --no-cache-dir -r requirements.txt -r requirements-web.txt -r requirements-finviz.txt

if [ "$#" -eq 0 ]; then
  exec uvicorn web.app:app --host 0.0.0.0 --port 8000
fi

exec "$@"
