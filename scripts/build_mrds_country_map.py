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

from src.country_filter import load_aliases, normalize_country_name, normalize_iso3


def _detect_delimiter(path: Path) -> str:
    return "\t" if path.suffix.lower() == ".txt" else ","


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _load_iso3_map(path: Path, aliases: dict[str, str]) -> dict[str, str]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        return {}
    out: dict[str, str] = {}
    for k, v in data.items():
        if isinstance(k, str) and isinstance(v, str):
            key = normalize_country_name(k)
            key = aliases.get(key, key)
            out[key] = normalize_iso3(v)
    return out


def _build_iso3_map_from_worldbank(
    paths: list[Path], aliases: dict[str, str]
) -> dict[str, str]:
    iso_map: dict[str, str] = {}
    for path in paths:
        if not path.exists():
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        data = payload[1] if isinstance(payload, list) and len(payload) > 1 else payload
        if not isinstance(data, list):
            continue
        for item in data:
            if not isinstance(item, dict):
                continue
            country = item.get("country", {}).get("value")
            iso3 = item.get("countryiso3code")
            if not isinstance(country, str) or not isinstance(iso3, str):
                continue
            iso3 = iso3.strip()
            if len(iso3) != 3:
                continue
            country_norm = normalize_country_name(country)
            country_norm = aliases.get(country_norm, country_norm)
            if country_norm not in iso_map:
                iso_map[country_norm] = normalize_iso3(iso3)
    return iso_map


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="build_mrds_country_map", add_help=True)
    p.add_argument(
        "--location",
        default="data/raw/mrds_csv/extracted/Location.txt",
        help="Path to MRDS Location file (.txt or .csv).",
    )
    p.add_argument(
        "--out",
        default="data/normalized/mrds_dep_country.jsonl",
        help="Output JSONL path.",
    )
    p.add_argument(
        "--summary",
        default="data/normalized/mrds_dep_country_summary.json",
        help="Summary JSON path.",
    )
    p.add_argument(
        "--aliases",
        default="references/country_aliases.json",
        help="Country aliases JSON.",
    )
    p.add_argument("--country-field", default="country", help="Country field name.")
    p.add_argument("--dep-id-field", default="dep_id", help="dep_id field name.")
    p.add_argument(
        "--iso-field",
        default="",
        help="Optional ISO field name (if present in the Location table).",
    )
    p.add_argument(
        "--iso-map",
        default="",
        help="Optional JSON mapping country → ISO3 (used when Location has no ISO field).",
    )
    args = p.parse_args(argv)

    location_path = Path(args.location).expanduser()
    if not location_path.exists():
        print(f"ERROR: Location file not found: {location_path}", file=sys.stderr)
        return 2

    out_path = Path(args.out).expanduser()
    summary_path = Path(args.summary).expanduser()
    aliases_path = Path(args.aliases).expanduser() if args.aliases else None

    aliases = load_aliases(aliases_path)
    delimiter = _detect_delimiter(location_path)
    iso_map: dict[str, str] = {}
    if args.iso_map:
        iso_map_path = Path(args.iso_map).expanduser()
        if iso_map_path.exists():
            iso_map = _load_iso3_map(iso_map_path, aliases)
    if not iso_map and not args.iso_field:
        wb_paths = [
            REPO_ROOT / "data" / "raw" / "worldbank_gdp" / "NY.GDP.MKTP.CD.json",
            REPO_ROOT / "data" / "raw" / "worldbank_population" / "SP.POP.TOTL.json",
        ]
        iso_map = _build_iso3_map_from_worldbank(wb_paths, aliases)

    dep_to_country: dict[str, dict[str, Any]] = {}
    conflicts = 0
    rows_total = 0
    rows_missing_dep = 0
    rows_missing_country = 0

    with location_path.open("r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        for row in reader:
            rows_total += 1
            dep_id = row.get(args.dep_id_field)
            country_raw = row.get(args.country_field)
            if not dep_id:
                rows_missing_dep += 1
                continue
            if not country_raw:
                rows_missing_country += 1
                continue

            country = str(country_raw).strip()
            if not country:
                rows_missing_country += 1
                continue

            country_norm = normalize_country_name(country)
            country_norm = aliases.get(country_norm, country_norm)

            iso_raw = row.get(args.iso_field) if args.iso_field else None
            iso3_norm = normalize_iso3(str(iso_raw).strip()) if iso_raw else None
            if not iso3_norm and iso_map:
                iso3_norm = iso_map.get(country_norm)

            dep_key = str(dep_id).strip()
            entry = dep_to_country.get(dep_key)
            if entry is None:
                dep_to_country[dep_key] = {
                    "dep_id": dep_key,
                    "country": country,
                    "country_norm": country_norm,
                    "iso3_norm": iso3_norm,
                }
            else:
                if entry.get("country_norm") != country_norm:
                    conflicts += 1

    rows = list(dep_to_country.values())
    _write_jsonl(out_path, rows)

    iso3_mapped = sum(1 for row in rows if row.get("iso3_norm"))
    summary = {
        "location_path": str(location_path),
        "rows_total": rows_total,
        "rows_missing_dep_id": rows_missing_dep,
        "rows_missing_country": rows_missing_country,
        "dep_ids_unique": len(rows),
        "conflicts": conflicts,
        "iso3_mapped": iso3_mapped,
        "output_jsonl": str(out_path),
    }
    _write_json(summary_path, summary)
    print(f"[ok] {len(rows)} dep_id rows -> {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

