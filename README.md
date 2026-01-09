# Week 1 — Reproducible data consumption demo (no DB, no server)

This repository is a **Week 1** demo for a Big Data & BI master project. The goal is **not** analysis or visualization per se, but to prove that the **approved public data sources** are consumable in a **real and reproducible** way with **professional error handling**, producing **local files** (no database, no server).

## Approved data sources (Week 1)

All sources are declared in `configs/sources.json`:

- **World Bank — Worldwide Governance Indicators (WGI / PV.EST)** (`worldbank_wgi`) — REST API (JSON)
- **USGS — Mineral Resources Data System (MRDS)** (`usgs_mrds`) — file download (CSV)
- **OneGeology — Geological Map Data** (`onegeology_wms`) — WMS GetCapabilities (XML)

## What this demo generates

### Download outputs (always local)

After running the downloader, you will get:

- `data/demo/<source_name>/records_100.json` (list of dicts, up to `--limit`)
- `data/demo/<source_name>/records_100.jsonl` (JSON Lines)
- `data/demo/<source_name>/metadata.json` (always generated, even on failure)
- `data/demo/<source_name>/debug_payload_snippet.txt` (only when applicable)
- `data/demo/demo_report.json` (run-level status by source)

### Visible result for non-technical reviewers

After generating the map:

- `output/demo_map.html` (interactive map, open locally with a double-click)
- `output/demo_summary.json` (execution stats and chosen WGI country/value)

> Note: `data/` and `output/` are **generated** and are intentionally ignored by Git.

## Quick start (one command)

```bash
bash ./run_demo.sh
```

The script will:

1. Create/activate `.venv`
2. Install dependencies from `requirements.txt`
3. Remove previously generated outputs
4. Download the approved sources (best-effort: it continues even if one source fails)
5. Generate `output/demo_map.html` and open it if `xdg-open` is available

## Manual run (step-by-step)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

python scripts/clean_data.py --yes
python scripts/demo_fetch.py --limit 100 --sources all
python scripts/generate_demo_map.py \
  --mrds ./data/demo/usgs_mrds/records_100.json \
  --wgi  ./data/demo/worldbank_wgi/records_100.json \
  --out  ./output/demo_map.html
```

## Project structure

- `run_demo.sh`: one-command runner (clean → download → map)
- `scripts/demo_fetch.py`: downloads sources and writes JSON/JSONL + metadata + `demo_report.json`
- `scripts/generate_demo_map.py`: reads **local** MRDS+WGI JSON and generates `output/demo_map.html`
- `scripts/clean_data.py`: deletes generated folders (`data/demo`, `output`, etc.)

## Notes on limitations (OneGeology)

OneGeology is consumed via a **WMS GetCapabilities** endpoint. In some environments the host `portal.onegeology.org` may fail to resolve (DNS/NXDOMAIN) or be blocked. In that case:

- `demo_fetch.py` will mark `onegeology_wms` as `failed` in `metadata.json` and `demo_report.json`
- the rest of the demo (MRDS + World Bank + HTML map) still works




