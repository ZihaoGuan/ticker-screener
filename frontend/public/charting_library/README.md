Place the official TradingView Advanced Charts distribution in this directory.

Expected entrypoint:

- `frontend/public/charting_library/charting_library.js`

Why this folder exists:

- The React watchlist detail page will automatically use Advanced Charts when
  `charting_library.js` is present.
- If the library files are missing, the UI falls back to the existing
  lightweight chart implementation so the app remains usable.

Notes:

- Do not rely on Pine Script. Advanced Charts libraries do not support it.
- The app uses `/api/charting/*` as its custom datafeed backend.
