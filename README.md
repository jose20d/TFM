# Week 1 — Technical data consumption demo (no DB)

Python 3.x demo that consumes approved sources and writes local outputs (JSON + JSONL).

## Run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python scripts/demo_fetch.py --limit 100 --sources all
```

Outputs: `data/demo/<source_name>/{records_100.json,records_100.jsonl,metadata.json}` and `data/demo/demo_report.json`.



