# React frontend architecture

The React migration is complete.

## Production

- Caddy serves `frontend/dist` for application routes.
- FastAPI serves `/api/*` and `/healthz` only.
- The deploy workflow builds the frontend before restarting the `web` and `caddy` services.
- There is no server-rendered Jinja fallback.

## Local development

Run FastAPI on port 8000:

```bash
uvicorn web.app:app --reload --host 0.0.0.0 --port 8000
```

Run Vite separately from `frontend/`:

```bash
npm install
npm run dev
```

Vite owns browser routes such as `/`, `/charts`, `/scanner`, `/screeners`, and `/watchlists`. FastAPI owns the JSON API consumed by those pages.

## Validation

```bash
cd frontend
npm run build
```

Backend changes should additionally import `web.app`, inspect the FastAPI route table, and run focused service/API tests.
