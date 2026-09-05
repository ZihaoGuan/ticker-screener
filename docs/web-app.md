# Web app

The application has one React frontend and one FastAPI backend.

## Architecture

- React, TypeScript, and Vite provide the browser UI in `frontend/`.
- Caddy serves the production build from `frontend/dist`.
- FastAPI exposes `/api/*` and `/healthz` from `web/app.py`.
- Postgres stores application state and market data.
- Existing `artifacts/` outputs remain available to backend services and the reports host.

The database schema lives in `sql/postgres_app_schema.sql`.

## Local development

Install backend dependencies and run FastAPI:

```bash
python3 -m pip install -r requirements.txt -r requirements-web.txt
uvicorn web.app:app --reload --host 0.0.0.0 --port 8000
```

Run the frontend from a second terminal:

```bash
cd frontend
npm install
npm run dev
```

Use `npm run build` for production frontend validation. Backend changes should use focused unit/API tests for the affected services and routes.
