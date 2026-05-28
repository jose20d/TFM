# TFM — Main Project Codebase

This repository contains the production code for the thesis project: ETL, analytical API, and bilingual frontend.

## Current scope

- End-to-end ETL from official sources to PostgreSQL/PostGIS.
- FastAPI backend (`web/app.py`) for analytics, terrain, and guided queries.
- Next.js frontend (`frontend/`) with bilingual UX (`es` / `en`).
- Hybrid i18n for domain data (`country`, `mineral`, and related domains).
- Local Docker workflow for backend, frontend, postgres/postgis, and on-demand ETL.

## Runtime modes

### 1) Local mode (without Docker)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=your_db
export DB_USER=your_user
export DB_PASSWORD=your_password

# ETL (both commands are supported)
python3 main.py
python3 -m etl.run_etl

# Backend
uvicorn web.app:app --reload --port 8000
```

In another terminal:

```bash
cd frontend
npm install
export BACKEND_API_URL=http://127.0.0.1:8000
npm run dev
```

### 2) Local Docker mode

```bash
cp .env.example .env
docker compose up -d --build
```

Run ETL on demand (not automatic on `up`):

```bash
docker compose --profile jobs run --rm etl
```

## Local URLs

- Frontend: `http://127.0.0.1:3000/`
- Backend/API: `http://127.0.0.1:8000/`
- OpenAPI docs: `http://127.0.0.1:8000/docs`

## Production deployment

- GitHub Actions workflow: `.github/workflows/deploy-production.yml`.
- Trigger: push to `main`.
- The deploy playbook (`ansible/deploy.yml`) now writes `/opt/tfm-geocontext/.env` from GitHub Secrets before `docker compose up -d`.
- Recommended repository secrets:
  - `EC2_HOST`, `EC2_USER`, `EC2_SSH_KEY`
  - `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`
  - `NEXT_PUBLIC_API_URL`
  - `INTERNAL_ADMIN_TOKEN`, `ADMIN_PANEL_USER`, `ADMIN_PANEL_PASSWORD`
  - `ETL_SCHEDULE_CRON`, `ETL_TIMEZONE` (optional)

## ETL notes

- Main ETL module: `etl/run_etl.py`
- Backward-compatible wrapper: `main.py`
- Raw files are preserved in `data/raw/` for traceability.
- Idempotency and ETL observability:
  - `etl_dataset_state`
  - `etl_dataset_run_log`
  - `etl_load_log`
- ISO normalization reference:
  - raw file: `data/raw/iso/country-codes.csv`
  - table: `iso_country_codes`

## i18n notes

- Dictionary + translation + materialized serving table:
  - `i18n_term_catalog`
  - `i18n_term_translation`
  - `i18n_term_materialized`
- Frontend language is controlled by `lang` query param (`es`, `en`).
- Backend localizes domain payload values before response.

## Prerequisites

- **OS**: Linux (Ubuntu 22.04+ recommended).
- **Python**: 3.10+.
- **Node.js**: 20+.
- **PostgreSQL**: 14+.
- **PostGIS**: enabled in the target DB.

Enable PostGIS once (as admin):

```sql
CREATE EXTENSION IF NOT EXISTS postgis;
```

## Key architectural constraints

- No intermediate JSON layer in the main ETL path.
- `dataset_config` is the metadata registry.
- ETL remains a finite process (run-and-exit), no internal scheduler.
- Docker ETL runs under profile `jobs` to avoid implicit execution on web startup.

## Repository conventions

- Generated data and local outputs are ignored by `.gitignore`.
- Sensitive local env files are ignored (`.env*`, except `.env.example`).
- `README_WINDOWS.md` is local-only documentation (not tracked in Git).






