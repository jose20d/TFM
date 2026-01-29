#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

try:
    import pandas as pd  # type: ignore
except ModuleNotFoundError as exc:  # pragma: no cover
    pd = None  # type: ignore
    _IMPORT_ERROR = str(exc)
else:
    _IMPORT_ERROR = None

# Ensure repo root is on sys.path for local imports
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.country_filter import load_aliases, normalize_country_name, normalize_iso3


def read_registry(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="normalize_xlsx", add_help=True)
    p.add_argument("--registry", default="references/data_registry.json", help="Path to data registry JSON")
    p.add_argument("--out-dir", default="data/normalized", help="Output directory for JSONL files")
    p.add_argument("--aliases", default="references/country_aliases.json", help="Country aliases JSON")
    p.add_argument("--ids", help="Comma-separated dataset ids to process (optional)")
    args = p.parse_args(argv)

    if pd is None:
        print(
            "ERROR: pandas is required to read .xlsx files. Install dependencies from requirements.txt.",
            file=sys.stderr,
        )
        return 2

    registry_path = Path(args.registry).expanduser()
    out_dir = Path(args.out_dir).expanduser()
    aliases_path = Path(args.aliases).expanduser() if args.aliases else None

    registry = read_registry(registry_path)
    datasets = registry.get("datasets") or []
    if not isinstance(datasets, list):
        print("ERROR: registry.datasets must be a list", file=sys.stderr)
        return 2

    wanted = None
    if args.ids:
        wanted = {x.strip() for x in args.ids.split(",") if x.strip()}

    aliases = load_aliases(aliases_path)
    processed = 0

    for ds in datasets:
        if not isinstance(ds, dict):
            continue
        if ds.get("type") != "xlsx":
            continue
        ds_id = ds.get("id")
        if wanted and ds_id not in wanted:
            continue

        path = Path(ds.get("path", "")).expanduser()
        if not path.is_absolute():
            path = (Path.cwd() / path).resolve()
        if not path.exists():
            print(f"[skip] {ds_id}: file not found at {path}", file=sys.stderr)
            continue

        header_row = ds.get("header_row_index")
        header_index = int(header_row) - 1 if header_row else 0

        country_field = ds.get("country_field")
        iso_field = ds.get("iso_field")
        year_field = ds.get("year_field")
        fixed_year = ds.get("fixed_year")
        value_field = ds.get("value_field")

        rank_field = "Rank" if ds_id == "fsi_2023" else None
        usecols = [c for c in [country_field, iso_field, year_field, value_field, rank_field] if c]

        try:
            df = pd.read_excel(path, header=header_index, usecols=usecols)
        except Exception as exc:
            print(f"[error] {ds_id}: failed to read Excel: {exc}", file=sys.stderr)
            continue

        rows: list[dict[str, Any]] = []
        for _, row in df.iterrows():
            raw_country = row.get(country_field) if country_field else None
            raw_iso = row.get(iso_field) if iso_field else None
            raw_year = row.get(year_field) if year_field else None
            if rank_field and rank_field in row:
                raw_value = row.get(rank_field)
                if raw_value is None or (isinstance(raw_value, float) and pd.isna(raw_value)):
                    raw_value = row.get(value_field) if value_field else None
            else:
                raw_value = row.get(value_field) if value_field else None

            if raw_country is None and raw_iso is None:
                continue

            country = str(raw_country).strip() if raw_country is not None else None
            iso3 = str(raw_iso).strip() if raw_iso is not None else None

            if country:
                norm = normalize_country_name(country)
                country_norm = aliases.get(norm, norm)
            else:
                country_norm = None

            iso3_norm = normalize_iso3(iso3) if iso3 else None

            year = int(fixed_year) if fixed_year is not None else raw_year
            value = raw_value

            if value is None or (isinstance(value, float) and pd.isna(value)):
                continue

            rows.append(
                {
                    "dataset_id": ds_id,
                    "country": country,
                    "country_norm": country_norm,
                    "iso3": iso3,
                    "iso3_norm": iso3_norm,
                    "year": year,
                    "value": value,
                }
            )

        out_path = out_dir / f"{ds_id}.jsonl"
        write_jsonl(out_path, rows)
        print(f"[ok] {ds_id}: {len(rows)} rows -> {out_path}")
        processed += 1

    if processed == 0:
        print("No XLSX datasets processed.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

