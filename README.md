# TFM — Main project codebase

This is the **main codebase** for the Master Thesis project. New development starts here.

## Traceability: Week 1 data-source validation demo (archived)

The Week 1 technical demo used to validate the approved data sources (download → local JSON/JSONL → local HTML map) has been archived here:

- `archive/week1_data_consumption_demo/`

That archived folder is self-contained (it includes its own `README`, `requirements.txt`, and runnable scripts) and is kept for traceability. It is **not** part of the main project runtime.

To run the archived Week 1 demo:

```bash
cd archive/week1_data_consumption_demo && bash ./run_demo.sh
```

## Repository conventions

- **Language**: code and primary documentation are in **English**.
- **No database**: Week 1 artifacts produce local files only.
- **No generated data in Git**: `data/` and `output/` are generated and ignored by `.gitignore`.






