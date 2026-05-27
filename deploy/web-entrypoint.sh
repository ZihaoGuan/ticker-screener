#!/bin/sh
set -eu

cd /app
pip install --no-cache-dir -r requirements.txt -r requirements-web.txt

if [ "$#" -eq 0 ]; then
  exec uvicorn web.app:app --host 0.0.0.0 --port 8000
fi

exec "$@"
