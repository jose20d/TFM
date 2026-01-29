#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any

# Ensure repo root is on sys.path for local imports
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.country_filter import load_aliases, normalize_country_name, normalize_iso3


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="build_mrds_country_map", add_help=True)
    p.add_argument("--location", default="references/Location.csv", help="Path to MRDS Location.csv")
    p.add_argument("--out", default="data/normalized/mrds_dep_country.jsonl", help="Output JSONL path")
    p.add_argument("--summary", default="data/normalized/mrds_dep_country_summary.json", help="Summary JSON path")
    p.add_argument("--aliases", default="references/country_aliases.json", help="Country aliases JSON")
    p.add_argument("--country-field", default="country", help="Country field name in Location.csv")
    p.add_argument("--dep-id-field", default="dep_id", help="dep_id field name in Location.csv")
    p.add_argument("--iso-field", default="", help="Optional ISO field in Location.csv (if present)")
    args = p.parse_args(argv)

    location_path = Path(args.location).expanduser()
    out_path = Path(args.out).expanduser()
    summary_path = Path(args.summary).expanduser()
    aliases_path = Path(args.aliases).expanduser() if args.aliases else None

    aliases = load_aliases(aliases_path)

    dep_to_country: dict[str, dict[str, Any]] = {}
    conflicts = 0
    rows_total = 0

    with location_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows_total += 1
            dep_id = row.get(args.dep_id_field)
            country_raw = row.get(args.country_field)
            if not dep_id or not country_raw:
                continue

            country = str(country_raw).strip()
            if not country:
                continue
            country_norm = normalize_country_name(country)
            country_norm = aliases.get(country_norm, country_norm)

            iso_raw = row.get(args.iso_field) if args.iso_field else None
            iso3_norm = normalize_iso3(str(iso_raw).strip()) if iso_raw else None

            entry = dep_to_country.get(dep_id)
            if entry is None:
                dep_to_country[dep_id] = {
                    "dep_id": dep_id,
                    "country": country,
                    "country_norm": country_norm,
                    "iso3_norm": iso3_norm,
                }
            else:
                # Detect conflicting country for same dep_id
                if entry.get("country_norm") != country_norm:
                    conflicts += 1

    rows = list(dep_to_country.values())
    write_jsonl(out_path, rows)

    summary = {
        "location_path": str(location_path),
        "rows_total": rows_total,
        "dep_ids_unique": len(rows),
        "conflicts": conflicts,
        "output_jsonl": str(out_path),
    }
    write_json(summary_path, summary)
    print(f"[ok] {len(rows)} dep_id rows -> {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

