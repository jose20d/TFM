# TFM — Main Project Codebase

This repository contains the production code for the Master Thesis project: ETL, analytical API, and bilingual web frontend.

## Current scope

- End-to-end ETL (`python3 main.py`) from official raw sources to PostgreSQL/PostGIS.
- Analytical API with FastAPI (`web/app.py`) for dashboard, terrain, and guided queries.
- Next.js frontend (`frontend/`) with bilingual UI (`es`/`en`) and data localization.
- Hybrid i18n for domain terms (`country`, `mineral`, and other domains):
  - canonical dictionary (`i18n_term_catalog`, `i18n_term_translation`),
  - materialized serving table (`i18n_term_materialized`),
  - ETL seed file (`database/i18n_terms_seed.csv`).

## Quick start (Linux)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=your_db
export DB_USER=your_user
export DB_PASSWORD=your_password

python3 main.py
uvicorn web.app:app --reload --port 8000
```

In another terminal:

```bash
cd frontend
npm install
export BACKEND_API_URL=http://127.0.0.1:8000
npm run dev
```

## Local URLs

- Frontend: `http://127.0.0.1:3000/`
- Backend/API: `http://127.0.0.1:8000/`
- OpenAPI docs: `http://127.0.0.1:8000/docs`

## Core runtime behaviors

- Raw files are downloaded and preserved in `data/raw/` for traceability.
- ETL hash/idempotency tracking:
  - `etl_dataset_state`
  - `etl_dataset_run_log`
- ISO whitelist normalization before loading country dimension:
  - raw: `data/raw/iso/country-codes.csv`
  - DB table: `iso_country_codes`
- Domain translation and serving:
  - `i18n_term_catalog`
  - `i18n_term_translation`
  - `i18n_term_materialized`

## Frontend notes

- Language is controlled by `lang` query param (`es` or `en`).
- The app propagates language to backend endpoints.
- Explore view uses country-aware limits and paginated retrieval for high-volume cases.

## Archived Week 1 demo

The original Week 1 source-validation demo remains archived for traceability:

- `archive/week1_data_consumption_demo/`

Run it with:

```bash
cd archive/week1_data_consumption_demo && bash ./run_demo.sh
```

## Prerequisites

- **OS**: Linux (Ubuntu 22.04+ recommended). This project is tested for Linux environments.
- **Python**: 3.10+ (recommended 3.12).
- **PostgreSQL**: 14+ (server and client tools).
- **PostGIS**: enabled in the target database.

### PostgreSQL installation

Follow the official PostgreSQL installation guide for your Linux distribution:
- PostgreSQL Global Development Group. (2024). *PostgreSQL: Linux downloads (Debian/Ubuntu)*. https://www.postgresql.org/download/linux/ubuntu/

### PostGIS installation

Install PostGIS using the official documentation:
- PostGIS Project. (2024). *PostGIS: Installation*. https://postgis.net/documentation/

After installing PostGIS, enable it in your database (as a superuser):

```sql
CREATE EXTENSION IF NOT EXISTS postgis;
```

## Architecture decisions

- Raw datasets are preserved unmodified for auditability and reproducibility.
- Normalization writes directly into PostgreSQL (no JSONL staging in main path).
- ETL metadata and run history are persisted for lineage and repeatability.
- PostGIS is enabled to support geospatial queries and terrain analysis.
- Schema scripts are idempotent to support safe reruns.

## Database architecture
- `dataset_config` defines sources and formats.
- ETL run tracking lives in `etl_load_log`, `etl_dataset_state`, and `etl_dataset_run_log`.
- Domain i18n serving uses catalog + translation + materialized labels.
- Geometry fields and spatial indexes support map-based exploration and proximity queries.

## Design constraints / tribunal guardrails

- The pipeline does not require superuser; PostGIS must be enabled beforehand by an admin.
- Raw downloads are kept intact for traceability.
- No JSONL staging in the main ETL path.
- `dataset_config` is the metadata registry (`dim_dataset` is not used).
- Single-command ETL execution: `python3 main.py`.

##  Repository conventions

- **Language**: code and primary documentation are in **English**.
- **Database layer**: the main pipeline loads into PostgreSQL/PostGIS.
- **No generated data in Git**: `data/`, `output/`, and `otros/` are generated and ignored by `.gitignore`.






