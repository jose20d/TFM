# TFM — Main project codebase

This is the **main codebase** for the Master Thesis project. New development starts here.

## Current scope (Phase 2)

- Normalize reference datasets (CSV/XLSX) into JSONL, no database.
- Filter by country using ISO3 or country name (with aliases).
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

# Normalize XLSX → JSONL (GDP, Population, CPI, FSI)
python scripts/normalize_xlsx.py

# Build MRDS dep_id → country map
python scripts/build_mrds_country_map.py

# Run example queries (CPI/FSI/GDP/Population)
python scripts/run_queries.py

# Filter MRDS tables by country
python scripts/filter_mrds_by_country.py --input references/Rocks.csv --country "Chile" --out output/queries/rocks_chile.json
```

## Optional UI (local Streamlit)

```bash
streamlit run streamlit_app.py
```

## Architecture Decisions

- Raw datasets are preserved unmodified to keep a verifiable source of truth for audits and reprocessing.
- Normalization writes directly to PostgreSQL to avoid intermediate JSON staging and reduce duplication of storage.
- Dataset metadata is stored in the database to support lineage, traceability, and repeatable ETL execution.
- PostGIS is enabled from the beginning to support geospatial queries on mineral deposit data.
- Schema creation is idempotent to allow safe reruns in CI, local setups, and recovery workflows.

## Repository conventions

- **Language**: code and primary documentation are in **English**.
- **No database**: Week 1 artifacts produce local files only.
- **No generated data in Git**: `data/`, `output/`, and `otros/` are generated and ignored by `.gitignore`.






