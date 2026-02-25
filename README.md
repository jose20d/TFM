# TFM — Main project codebase

This is the **main codebase** for the Master Thesis project. New development starts here.

## Current scope (Phase 2)

- Download raw datasets for traceability.
- Clean and normalize data directly into PostgreSQL/PostGIS.
- Explore relationships locally via Streamlit (optional).

## Traceability: Week 1 data-source validation demo (archived)

The Week 1 technical demo used to validate the approved data sources (download → local JSON/JSONL → local HTML map) has been archived here:

- `archive/week1_data_consumption_demo/`

That archived folder is self-contained (it includes its own `README`, `requirements.txt`, and runnable scripts) and is kept for traceability. It is **not** part of the main project runtime.

To run the archived Week 1 demo:

```bash
cd archive/week1_data_consumption_demo && bash ./run_demo.sh
```

## How to run (main codebase)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Database connection (example: adjust as needed)
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=your_db
export DB_USER=your_user
export DB_PASSWORD=your_password

# Run the full pipeline (download → clean → load → Streamlit)
python3 main.py
```

Raw files are always re-downloaded to keep CI runs deterministic.

## ISO country reference (whitelist)

The pipeline downloads ISO 3166-1 country codes and loads them into PostgreSQL.
Only countries present in that ISO dataset are inserted into `dim_country`.

- Raw file: `data/raw/iso/country-codes.csv`
- DB table: `iso_country_codes`
- Usage: whitelist filter before inserting `dim_country`

## ETL state and audit logs

The ETL tracks dataset hashes and logs each run for auditability.

- Current state table: `etl_dataset_state`
- Historical run log: `etl_dataset_run_log`
- Behavior: if the file hash is unchanged, the load step is skipped.

## Optional UI (local Streamlit)

```bash
streamlit run streamlit_app.py
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

## Architecture Decisions

- Raw datasets are preserved unmodified to keep a verifiable source of truth for audits and reprocessing.
- Normalization writes directly to PostgreSQL to avoid intermediate JSON staging and reduce duplication of storage.
- Dataset metadata is stored in the database to support lineage, traceability, and repeatable ETL execution.
- PostGIS is enabled from the beginning to support geospatial queries on mineral deposit data (installed by DB admin).
- Schema creation is idempotent to allow safe reruns in CI, local setups, and recovery workflows.

## Database Architecture
- **Dataset configuration vs. ETL logs**: `dataset_config` defines sources and formats, while `etl_load_log` captures execution results and data lineage.
- **Raw preservation**: raw downloads remain intact so every load can be reproduced or audited without ambiguity.
- **PostgreSQL + PostGIS**: a relational core is needed for joins and analytics, and PostGIS prepares the model for spatial queries on deposits.
- **No intermediate JSON layer**: data is normalized directly into PostgreSQL to avoid duplicate storage and reduce operational complexity.
- **Future geospatial analytics**: the schema includes geometry fields and spatial indexes to support map-based exploration and distance queries.

## Design constraints / tribunal guardrails

- The pipeline never requires a PostgreSQL superuser; PostGIS must be enabled by an administrator beforehand.
- Raw downloads are kept intact for traceability and auditability.
- No JSONL staging is used in the main path; data is cleaned in-memory and loaded directly into PostgreSQL.
- `dataset_config` is the single metadata registry table; `dim_dataset` is intentionally not used.
- A single command (`python3 main.py`) runs the end-to-end flow with no interactive prompts.

## Repository conventions

- **Language**: code and primary documentation are in **English**.
- **Database layer**: the main pipeline loads into PostgreSQL/PostGIS.
- **No generated data in Git**: `data/`, `output/`, and `otros/` are generated and ignored by `.gitignore`.






