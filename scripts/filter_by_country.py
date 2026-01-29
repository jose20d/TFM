#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any

from src.country_filter import filter_by_country, load_aliases


def read_json(path: Path) -> list[dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("JSON input must be a list of objects.")
    return [r for r in data if isinstance(r, dict)]


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if isinstance(obj, dict):
                out.append(obj)
    return out


def read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return [row for row in csv.DictReader(f)]


def write_json(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="filter_by_country", add_help=True)
    p.add_argument("--input", required=True, help="Path to input file (.json, .jsonl, .csv)")
    p.add_argument("--out", required=True, help="Path to output JSON file")
    p.add_argument("--country", help="Country name to match (case/accents insensitive)")
    p.add_argument("--iso3", help="ISO-3 code to match (e.g., MEX)")
    p.add_argument("--country-fields", default="country,Country,Country Name", help="Comma-separated country field names")
    p.add_argument("--iso-fields", default="iso3,ISO3,Country Code,country_code", help="Comma-separated ISO field names")
    p.add_argument("--aliases", help="Optional JSON file with country aliases")
    args = p.parse_args(argv)

    if not args.country and not args.iso3:
        print("ERROR: provide --country or --iso3", file=sys.stderr)
        return 2

    input_path = Path(args.input).expanduser()
    out_path = Path(args.out).expanduser()
    aliases_path = Path(args.aliases).expanduser() if args.aliases else None

    country_fields = [c.strip() for c in args.country_fields.split(",") if c.strip()]
    iso_fields = [c.strip() for c in args.iso_fields.split(",") if c.strip()]
    aliases = load_aliases(aliases_path)

    ext = input_path.suffix.lower()
    if ext == ".json":
        records = read_json(input_path)
    elif ext == ".jsonl":
        records = read_jsonl(input_path)
    elif ext == ".csv":
        records = read_csv(input_path)
    else:
        print("ERROR: input must be .json, .jsonl, or .csv", file=sys.stderr)
        return 2

    filtered = filter_by_country(
        records,
        country=args.country,
        iso3=args.iso3,
        country_fields=country_fields,
        iso_fields=iso_fields,
        aliases=aliases,
    )

    write_json(out_path, filtered)
    print(f"OK: {len(filtered)} records written to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

