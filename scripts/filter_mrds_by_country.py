#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any

# Ensure repo root is on sys.path for local imports.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.country_filter import normalize_country_name, normalize_iso3


def _detect_delimiter(path: Path) -> str:
    return "\t" if path.suffix.lower() == ".txt" else ","


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if isinstance(obj, dict):
                rows.append(obj)
    return rows


def _write_json(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(rows, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="filter_mrds_by_country", add_help=True)
    p.add_argument("--input", required=True, help="Path to MRDS table (.txt or .csv).")
    p.add_argument(
        "--map",
        default="data/normalized/mrds_dep_country.jsonl",
        help="Path to dep_id → country map JSONL.",
    )
    p.add_argument("--out", required=True, help="Output JSON path.")
    p.add_argument("--country", help="Country name to match.")
    p.add_argument("--iso3", help="ISO-3 code to match.")
    p.add_argument("--dep-id-field", default="dep_id", help="dep_id field name.")
    args = p.parse_args(argv)

    if not args.country and not args.iso3:
        print("ERROR: provide --country or --iso3", file=sys.stderr)
        return 2

    input_path = Path(args.input).expanduser()
    map_path = Path(args.map).expanduser()
    out_path = Path(args.out).expanduser()

    if not input_path.exists():
        print(f"ERROR: input not found: {input_path}", file=sys.stderr)
        return 2
    if not map_path.exists():
        print(f"ERROR: map not found: {map_path}", file=sys.stderr)
        return 2

    dep_map: dict[str, dict[str, Any]] = {}
    for row in _read_jsonl(map_path):
        dep_id = row.get("dep_id")
        if dep_id:
            dep_map[str(dep_id)] = row

    if args.iso3:
        iso_target = normalize_iso3(args.iso3)

        def match(dep_row: dict[str, Any]) -> bool:
            return dep_row.get("iso3_norm") == iso_target

    else:
        country_target = normalize_country_name(args.country)

        def match(dep_row: dict[str, Any]) -> bool:
            return dep_row.get("country_norm") == country_target

    matched: list[dict[str, Any]] = []
    delimiter = _detect_delimiter(input_path)
    with input_path.open("r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        for row in reader:
            dep_id = row.get(args.dep_id_field)
            if not dep_id:
                continue
            dep_row = dep_map.get(str(dep_id).strip())
            if not dep_row:
                continue
            if match(dep_row):
                matched.append(row)

    _write_json(out_path, matched)
    print(f"[ok] {len(matched)} rows -> {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

