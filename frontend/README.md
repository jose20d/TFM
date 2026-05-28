# Frontend (Next.js)

This folder contains the product UI for the TFM project.

## Stack

- Next.js (App Router)
- React
- Leaflet / react-leaflet (maps)
- Recharts (charts)

## Run locally

```bash
npm install
export BACKEND_API_URL=http://127.0.0.1:8000
npm run dev
```

Open `http://127.0.0.1:3000/`.

## Environment

- `BACKEND_API_URL`: FastAPI base URL used by the frontend proxy route.
- `INTERNAL_ADMIN_TOKEN`: shared secret used by the private metrics API bridge.
- `ADMIN_PANEL_USER`: Basic Auth username for internal admin routes.
- `ADMIN_PANEL_PASSWORD`: Basic Auth password for internal admin routes.

## Language behavior

- UI language is controlled through `lang` query param (`es`, `en`).
- Header language switch propagates `lang` to backend requests.
- Domain values (countries, minerals, statuses, etc.) are localized by backend i18n materialization.

## Main routes

- `/` - home KPIs and highlights
- `/explorar` - geoterritorial exploration (country-aware limits + paginated retrieval)
- `/comparar` - country comparison
- `/analisis` - global analysis charts
- `/consultas` - guided analytical queries
- `/terreno` - terrain-oriented spatial tools

## Notes

- The frontend targets backend endpoints through `/api/v1/*` (for example behind Nginx reverse proxy).
- For stable dev behavior after large UI refactors, clear `.next/` and restart dev server.
