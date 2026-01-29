# Week 1 — Archived demo (data consumption + local HTML map)

This folder contains the **Week 1** demo that was delivered to validate that the **approved public data sources** are consumable in a real and reproducible way (no DB, no server), producing local JSON/JSONL outputs and a local interactive HTML map.

> This demo is archived for **traceability** and is not part of the main project codebase.

## Approved data sources (Week 1)

All sources are declared in `configs/sources.json`:

- **World Bank — Worldwide Governance Indicators (WGI / PV.EST)** (`worldbank_wgi`) — REST API (JSON)
- **USGS — Mineral Resources Data System (MRDS)** (`usgs_mrds`) — file download (CSV)
- **OneGeology — Geological Map Data** (`onegeology_wms`) — WMS GetCapabilities (XML)

## What this demo generates

### Download outputs (always local)

- `data/demo/<source_name>/records_100.json` (list of dicts, up to `--limit`)
- `data/demo/<source_name>/records_100.jsonl` (JSON Lines)
- `data/demo/<source_name>/metadata.json` (always generated, even on failure)
- `data/demo/<source_name>/debug_payload_snippet.txt` (only when applicable)
- `data/demo/demo_report.json` (run-level status by source)

### Visible result for non-technical reviewers

- `output/demo_map.html` (interactive map, open locally with a double-click)
- `output/demo_summary.json` (execution stats and chosen WGI country/value)

> Note: `data/` and `output/` are generated and are ignored by Git at repo level.

## Quick start (one command)

```bash
bash ./run_demo.sh
```

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

## Notes on limitations (OneGeology)

In some environments the host `portal.onegeology.org` may fail to resolve (DNS/NXDOMAIN) or be blocked. In that case the source will be marked as failed, but the rest of the demo (MRDS + World Bank + HTML map) still works.

