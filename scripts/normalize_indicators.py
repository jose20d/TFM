#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd

# Ensure repo root is on sys.path for local imports
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.country_filter import load_aliases, normalize_country_name, normalize_iso3


WORLD_BANK_AGGREGATES = {
    "Africa Eastern and Southern",
    "Africa Western and Central",
    "Arab World",
    "East Asia & Pacific",
    "Europe & Central Asia",
    "Latin America & Caribbean",
    "Middle East & North Africa",
    "North America",
    "South Asia",
    "Sub-Saharan Africa",
    "World",
    "High income",
    "Upper middle income",
    "Lower middle income",
    "Low income",
    "OECD members",
    "Euro area",
}


def _read_config(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _dataset_path(cfg: dict[str, Any], dataset_id: str, raw_dir: Path) -> Path | None:
    datasets = cfg.get("datasets") or []
    for ds in datasets:
        if ds.get("id") == dataset_id:
            filename = ds.get("output_filename")
            if filename:
                return raw_dir / dataset_id / str(filename)
    return None


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def _normalize_country(value: str, aliases: dict[str, str]) -> str:
    norm = normalize_country_name(value)
    return aliases.get(norm, norm)


def _normalize_iso3(value: str | None) -> str | None:
    if not value:
        return None
    text = str(value).strip()
    return normalize_iso3(text) if text else None


def _build_worldbank_jsonl(
    raw_path: Path,
    dataset_id: str,
    out_path: Path,
    aliases: dict[str, str],
) -> None:
    payload = json.loads(raw_path.read_text(encoding="utf-8"))
    data = payload[1] if isinstance(payload, list) and len(payload) > 1 else payload
    if not isinstance(data, list):
        print(f"[skip] {dataset_id}: unexpected JSON shape", file=sys.stderr)
        return

    rows: list[dict[str, Any]] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        country = item.get("country", {}).get("value")
        iso3 = item.get("countryiso3code")
        year = item.get("date")
        value = item.get("value")
        if not isinstance(country, str) or not country.strip():
            continue
        country = country.strip()
        if country in WORLD_BANK_AGGREGATES:
            continue
        rows.append(
            {
                "dataset_id": dataset_id,
                "country": country,
                "country_norm": _normalize_country(country, aliases),
                "iso3": iso3,
                "iso3_norm": _normalize_iso3(iso3),
                "year": year,
                "value": value,
            }
        )

    _write_jsonl(out_path, rows)
    print(f"[ok] {dataset_id}: {len(rows)} rows -> {out_path}")


def _build_fsi_jsonl(
    raw_path: Path,
    dataset_id: str,
    out_path: Path,
    aliases: dict[str, str],
) -> None:
    df = pd.read_excel(raw_path, usecols=["Country", "Rank"])
    df = df.rename(columns={"Country": "country", "Rank": "value"})
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    rows: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        country = row.get("country")
        if not isinstance(country, str) or not country.strip():
            continue
        country = country.strip()
        if country in WORLD_BANK_AGGREGATES:
            continue
        rows.append(
            {
                "dataset_id": dataset_id,
                "country": country,
                "country_norm": _normalize_country(country, aliases),
                "iso3": None,
                "iso3_norm": None,
                "year": 2023,
                "value": row.get("value"),
            }
        )
    _write_jsonl(out_path, rows)
    print(f"[ok] {dataset_id}: {len(rows)} rows -> {out_path}")


def main() -> int:
    config_path = REPO_ROOT / "configs" / "datasets.json"
    raw_dir = REPO_ROOT / "data" / "raw"
    out_dir = REPO_ROOT / "data" / "normalized"
    aliases_path = REPO_ROOT / "references" / "country_aliases.json"

    if not config_path.exists():
        print(f"ERROR: config not found at {config_path}", file=sys.stderr)
        return 2

    cfg = _read_config(config_path)
    aliases = load_aliases(aliases_path) if aliases_path.exists() else {}

    gdp_path = _dataset_path(cfg, "worldbank_gdp", raw_dir)
    if gdp_path and gdp_path.exists():
        _build_worldbank_jsonl(gdp_path, "worldbank_gdp", out_dir / "worldbank_gdp.jsonl", aliases)
    else:
        print("[skip] worldbank_gdp: raw file not found", file=sys.stderr)

    pop_path = _dataset_path(cfg, "worldbank_population", raw_dir)
    if pop_path and pop_path.exists():
        _build_worldbank_jsonl(
            pop_path, "worldbank_population", out_dir / "worldbank_population.jsonl", aliases
        )
    else:
        print("[skip] worldbank_population: raw file not found", file=sys.stderr)

    fsi_path = _dataset_path(cfg, "fsi_2023", raw_dir)
    if fsi_path and fsi_path.exists():
        _build_fsi_jsonl(fsi_path, "fsi_2023", out_dir / "fsi_2023.jsonl", aliases)
    else:
        print("[skip] fsi_2023: raw file not found", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
