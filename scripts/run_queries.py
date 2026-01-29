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


def read_examples(path: Path) -> list[dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("Query examples must be a list.")
    return [x for x in data if isinstance(x, dict)]


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="run_queries", add_help=True)
    p.add_argument("--examples", default="references/query_examples.json", help="Path to query examples JSON")
    p.add_argument("--out-dir", default="output/queries", help="Default output directory")
    args = p.parse_args(argv)

    examples_path = Path(args.examples).expanduser()
    out_dir = Path(args.out_dir).expanduser()

    try:
        examples = read_examples(examples_path)
    except Exception as exc:
        print(f"ERROR: failed to read examples: {exc}", file=sys.stderr)
        return 2

    for ex in examples:
        ex_id = ex.get("id", "unknown")
        input_path = Path(ex.get("input_path", "")).expanduser()
        input_type = (ex.get("input_type") or "").lower()
        output_path = ex.get("output_path") or f"{ex_id}.json"
        output_path = Path(output_path)
        if not output_path.is_absolute():
            output_path = (out_dir / output_path).resolve()

        aliases_path = ex.get("aliases_path")
        aliases = load_aliases(Path(aliases_path)) if aliases_path else {}

        country = (ex.get("filter") or {}).get("country")
        iso3 = (ex.get("filter") or {}).get("iso3")
        fields = ex.get("fields") or {}
        country_fields = fields.get("country_fields") or []
        iso_fields = fields.get("iso_fields") or []

        if input_type == "json":
            records = read_json(input_path)
        elif input_type == "jsonl":
            records = read_jsonl(input_path)
        elif input_type == "csv":
            records = read_csv(input_path)
        else:
            print(f"[skip] {ex_id}: unsupported input_type '{input_type}'", file=sys.stderr)
            continue

        filtered = filter_by_country(
            records,
            country=country,
            iso3=iso3,
            country_fields=country_fields,
            iso_fields=iso_fields,
            aliases=aliases,
        )

        write_json(output_path, filtered)
        print(f"[ok] {ex_id}: {len(filtered)} rows -> {output_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

