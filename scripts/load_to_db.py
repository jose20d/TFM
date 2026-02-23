#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
from psycopg2.extras import execute_values

# Ensure repo root is on sys.path for local imports.
# This keeps the ETL self-contained without additional packaging.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.country_filter import load_aliases, normalize_country_name, normalize_iso3
from src.db import get_connection
from src.init_db import initialize_schema


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

# Indicator codes used for database normalization.
# This keeps a stable, queryable identifier in the relational layer.
INDICATOR_CODES = {
    "worldbank_gdp": "NY.GDP.MKTP.CD",
    "worldbank_population": "SP.POP.TOTL",
    "fsi_2023": "RANK",
    "cpi_2023": "CPI_2023",
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


def _norm_country(value: str, aliases: dict[str, str]) -> str:
    norm = normalize_country_name(value)
    return aliases.get(norm, norm)


def _norm_iso3(value: str | None) -> str | None:
    if not value:
        return None
    text = str(value).strip()
    return normalize_iso3(text) if text else None


def _load_worldbank_rows(
    raw_path: Path, dataset_id: str, aliases: dict[str, str]
) -> list[dict[str, Any]]:
    payload = json.loads(raw_path.read_text(encoding="utf-8"))
    data = payload[1] if isinstance(payload, list) and len(payload) > 1 else payload
    if not isinstance(data, list):
        return []

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
                "indicator_code": INDICATOR_CODES.get(dataset_id),
                "country": country,
                "country_norm": _norm_country(country, aliases),
                "iso3": _norm_iso3(iso3),
                "year": int(year) if str(year).isdigit() else None,
                "value": value,
            }
        )
    return rows


def _load_fsi_rows(raw_path: Path, aliases: dict[str, str]) -> list[dict[str, Any]]:
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
                "dataset_id": "fsi_2023",
                "indicator_code": INDICATOR_CODES.get("fsi_2023"),
                "country": country,
                "country_norm": _norm_country(country, aliases),
                "iso3": None,
                "year": 2023,
                "value": row.get("value"),
            }
        )
    return rows


def _load_cpi_rows(raw_path: Path, aliases: dict[str, str]) -> list[dict[str, Any]]:
    df = pd.read_excel(
        raw_path,
        header=3,
        usecols=["Country / Territory", "ISO3", "CPI score 2023"],
    )
    df = df.rename(
        columns={
            "Country / Territory": "country",
            "ISO3": "iso3",
            "CPI score 2023": "value",
        }
    )
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    rows: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        country = row.get("country")
        if not isinstance(country, str) or not country.strip():
            continue
        country = country.strip()
        if country in WORLD_BANK_AGGREGATES:
            continue
        iso3 = row.get("iso3")
        rows.append(
            {
                "dataset_id": "cpi_2023",
                "indicator_code": INDICATOR_CODES.get("cpi_2023"),
                "country": country,
                "country_norm": _norm_country(country, aliases),
                "iso3": _norm_iso3(iso3),
                "year": 2023,
                "value": row.get("value"),
            }
        )
    return rows


def _file_hash(path: Path) -> str | None:
    if not path.exists():
        return None
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _file_size(path: Path) -> int | None:
    return path.stat().st_size if path.exists() else None


def _log_etl(
    cur,
    dataset_id: str,
    raw_path: Path | None,
    rows_inserted: int | None,
    rows_failed: int | None,
    status: str,
    error_message: str | None = None,
) -> None:
    raw_filename = raw_path.name if raw_path else "unknown"
    sql = """
        INSERT INTO etl_load_log (
            dataset_id, raw_filename, file_hash, file_size_bytes,
            rows_inserted, rows_failed, load_status, error_message
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    # Insert an audit row per dataset load to preserve ETL traceability.
    cur.execute(
        sql,
        (
            dataset_id,
            raw_filename,
            _file_hash(raw_path) if raw_path else None,
            _file_size(raw_path) if raw_path else None,
            rows_inserted,
            rows_failed,
            status,
            error_message,
        ),
    )


def _load_mrds_location(path: Path, aliases: dict[str, str]) -> pd.DataFrame:
    df = pd.read_csv(path, usecols=["dep_id", "country", "state_prov", "region", "county"])
    df = df[df["country"].notna()]
    df["country"] = df["country"].astype(str).str.strip()
    invalid_countries = {"AF", "EU", "AS", "OC", "SA", "CR"}
    df = df[~df["country"].isin(invalid_countries)]
    df = df[df["country"] != ""]
    df["country_norm"] = df["country"].apply(normalize_country_name)
    df["country_norm"] = df["country_norm"].map(lambda x: aliases.get(x, x))
    return df


def _dedupe_countries(
    rows: Iterable[tuple[str, str, str | None]]
) -> list[tuple[str, str, str | None]]:
    unique: dict[str, tuple[str, str, str | None]] = {}
    for name, norm, iso3 in rows:
        if not norm:
            continue
        if norm not in unique:
            unique[norm] = (name, norm, iso3)
            continue
        # Prefer an ISO3 if we see one later.
        if unique[norm][2] is None and iso3:
            unique[norm] = (name, norm, iso3)
    return list(unique.values())


def _insert_countries(cur, rows: Iterable[tuple[str, str, str | None]]) -> None:
    rows = _dedupe_countries(rows)
    if not rows:
        return
    sql = """
        INSERT INTO dim_country (country_name, country_norm, iso3)
        VALUES %s
        ON CONFLICT (country_norm) DO UPDATE
        SET iso3 = COALESCE(dim_country.iso3, EXCLUDED.iso3)
    """
    execute_values(cur, sql, rows)


def _country_id_map(cur) -> dict[str, int]:
    cur.execute("SELECT country_id, country_norm FROM dim_country")
    return {row[1]: row[0] for row in cur.fetchall()}


def _insert_dataset_config(cur, cfg: dict[str, Any]) -> None:
    datasets = cfg.get("datasets") or []
    rows = []
    for ds in datasets:
        rows.append(
            (
                ds.get("id"),
                ds.get("name"),
                ds.get("url"),
                Path(str(ds.get("output_filename", ""))).suffix.lstrip(".") or "unknown",
                None,
            )
        )
    sql = """
        INSERT INTO dataset_config (dataset_id, source_name, source_url, format, update_frequency)
        VALUES %s
        ON CONFLICT (dataset_id) DO UPDATE
        SET source_name = EXCLUDED.source_name,
            source_url = EXCLUDED.source_url,
            format = EXCLUDED.format
    """
    execute_values(cur, sql, rows)


def _ensure_dataset_config_seed(cur) -> None:
    """
    Seed dataset_config if the table is empty to avoid first-run failures.
    """
    cur.execute("SELECT COUNT(*) FROM dataset_config")
    count = cur.fetchone()[0]
    if count and count > 0:
        return

    seed_path = REPO_ROOT / "database" / "seed_dataset_config.sql"
    if not seed_path.exists():
        return
    cur.execute(seed_path.read_text(encoding="utf-8"))


def main() -> int:
    config_path = REPO_ROOT / "configs" / "datasets.json"
    raw_dir = REPO_ROOT / "data" / "raw"
    aliases_path = REPO_ROOT / "references" / "country_aliases.json"
    mrds_extract = raw_dir / "mrds_csv" / "extracted"

    if not config_path.exists():
        print(f"ERROR: config not found at {config_path}", file=sys.stderr)
        return 2

    cfg = _read_config(config_path)
    aliases = load_aliases(aliases_path) if aliases_path.exists() else {}

    # Initialize schema before loading data to keep the pipeline idempotent.
    # PostGIS must be enabled by the database administrator.
    initialize_schema()

    with get_connection() as conn:
        with conn.cursor() as cur:
            _ensure_dataset_config_seed(cur)
            _insert_dataset_config(cur, cfg)

            # Countries from MRDS Location
            loc_path = mrds_extract / "Location.csv"
            location_df = _load_mrds_location(loc_path, aliases) if loc_path.exists() else pd.DataFrame()

            # Countries from indicators
            countries = []
            if not location_df.empty:
                countries.extend(
                    zip(location_df["country"], location_df["country_norm"], [None] * len(location_df))
                )

            gdp_path = _dataset_path(cfg, "worldbank_gdp", raw_dir)
            pop_path = _dataset_path(cfg, "worldbank_population", raw_dir)
            fsi_path = _dataset_path(cfg, "fsi_2023", raw_dir)
            cpi_path = _dataset_path(cfg, "cpi_2023", raw_dir)

            gdp_rows = _load_worldbank_rows(gdp_path, "worldbank_gdp", aliases) if gdp_path and gdp_path.exists() else []
            pop_rows = _load_worldbank_rows(pop_path, "worldbank_population", aliases) if pop_path and pop_path.exists() else []
            fsi_rows = _load_fsi_rows(fsi_path, aliases) if fsi_path and fsi_path.exists() else []
            cpi_rows = _load_cpi_rows(cpi_path, aliases) if cpi_path and cpi_path.exists() else []

            for rows in (gdp_rows, pop_rows, fsi_rows, cpi_rows):
                if rows:
                    countries.extend(
                        [(r["country"], r["country_norm"], r["iso3"]) for r in rows]
                    )

            _insert_countries(cur, countries)
            country_map = _country_id_map(cur)

            # MRDS deposit
            mrds_path = mrds_extract / "MRDS.csv"
            mrds_inserted = 0
            if mrds_path.exists():
                df = pd.read_csv(
                    mrds_path,
                    usecols=["dep_id", "name", "dev_stat", "code_list", "latitude", "longitude"],
                )
                df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
                df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
                df = df[df["latitude"].between(-90, 90) & df["longitude"].between(-180, 180)]
                mrds_inserted = len(df)
                rows = [
                    (
                        int(r.dep_id),
                        r.name,
                        r.dev_stat,
                        r.code_list,
                        r.latitude,
                        r.longitude,
                    )
                    for r in df.itertuples(index=False)
                ]
                sql = """
                    INSERT INTO mrds_deposit (dep_id, name, dev_stat, code_list, latitude, longitude, geom)
                    VALUES %s
                    ON CONFLICT (dep_id) DO UPDATE
                    SET name = EXCLUDED.name,
                        dev_stat = EXCLUDED.dev_stat,
                        code_list = EXCLUDED.code_list,
                        latitude = EXCLUDED.latitude,
                        longitude = EXCLUDED.longitude,
                        geom = EXCLUDED.geom
                """
                execute_values(
                    cur,
                    sql,
                    [
                        (
                            dep_id,
                            name,
                            dev_stat,
                            code_list,
                            lat,
                            lon,
                            f"SRID=4326;POINT({lon} {lat})",
                        )
                        for dep_id, name, dev_stat, code_list, lat, lon in rows
                    ],
                    template="(%s,%s,%s,%s,%s,%s,ST_GeomFromText(%s))",
                )

            # MRDS location
            if not location_df.empty:
                location_df["country_id"] = location_df["country_norm"].map(country_map)
                rows = [
                    (
                        int(r.dep_id),
                        int(r.country_id) if r.country_id else None,
                        r.state_prov,
                        r.region,
                        r.county,
                    )
                    for r in location_df.itertuples(index=False)
                ]
                sql = """
                    INSERT INTO mrds_location (dep_id, country_id, state_prov, region, county)
                    VALUES %s
                    ON CONFLICT (dep_id) DO UPDATE
                    SET country_id = EXCLUDED.country_id,
                        state_prov = EXCLUDED.state_prov,
                        region = EXCLUDED.region,
                        county = EXCLUDED.county
                """
                execute_values(cur, sql, rows)

            # MRDS related tables
            related = {
                "Commodity.csv": (
                    "mrds_commodity",
                    ["dep_id", "commod", "code", "commod_tp", "commod_group", "import"],
                ),
                "Materials.csv": (
                    "mrds_material",
                    ["dep_id", "rec", "ore_gangue", "material"],
                ),
                "Ownership.csv": (
                    "mrds_ownership",
                    ["dep_id", "owner_name", "owner_tp"],
                ),
                "Physiography.csv": (
                    "mrds_physiography",
                    ["dep_id", "phys_div", "phys_prov", "phys_sect", "phys_det"],
                ),
                "Ages.csv": (
                    "mrds_ages",
                    ["dep_id", "age_tp", "age_young"],
                ),
                "Rocks.csv": (
                    "mrds_rocks",
                    ["dep_id", "rock_cls", "first_ord_nm", "second_ord_nm", "third_ord_nm", "low_name"],
                ),
            }
            for filename, (table, cols) in related.items():
                path = mrds_extract / filename
                if not path.exists():
                    continue
                df = pd.read_csv(path, usecols=cols)
                rows = [tuple(r) for r in df.itertuples(index=False)]
                sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s"
                execute_values(cur, sql, rows)

            # Country indicators (no JSON staging; direct DB normalization)
            # This avoids duplicate storage and keeps a single source of truth in PostgreSQL.
            indicator_rows: list[dict[str, Any]] = []
            indicator_rows.extend(gdp_rows)
            indicator_rows.extend(pop_rows)
            indicator_rows.extend(fsi_rows)
            indicator_rows.extend(cpi_rows)

            rows = []
            for r in indicator_rows:
                country_id = country_map.get(r["country_norm"])
                if not country_id or not r.get("year"):
                    continue
                rows.append(
                    (
                        int(country_id),
                        r["dataset_id"],
                        r.get("indicator_code"),
                        int(r["year"]),
                        r.get("value"),
                    )
                )
            sql = """
                INSERT INTO country_indicator (country_id, dataset_id, indicator_code, year, value)
                VALUES %s
                ON CONFLICT (country_id, dataset_id, indicator_code, year) DO UPDATE
                SET value = EXCLUDED.value
            """
            execute_values(cur, sql, rows)

            # ETL audit logs per dataset.
            for ds_id, path, count in [
                ("worldbank_gdp", gdp_path, len(gdp_rows)),
                ("worldbank_population", pop_path, len(pop_rows)),
                ("fsi_2023", fsi_path, len(fsi_rows)),
                ("cpi_2023", cpi_path, len(cpi_rows)),
                ("mrds_csv", mrds_path, mrds_inserted),
            ]:
                status = "SUCCESS" if path and path.exists() else "FAILED"
                _log_etl(
                    cur,
                    ds_id,
                    path,
                    count if status == "SUCCESS" else None,
                    0 if status == "SUCCESS" else None,
                    status,
                    None if status == "SUCCESS" else "Raw file not found",
                )

        conn.commit()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
