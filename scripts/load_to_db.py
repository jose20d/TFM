#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import tempfile
import zipfile
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

# Non-country labels sometimes appear in raw sources (regions or aggregates).
# These should be excluded to keep the country dimension clean and consistent.
NON_COUNTRY_LABELS = WORLD_BANK_AGGREGATES | {
    "Middle East",
    "North Africa",
    "West Africa",
    "East Africa",
    "Central Africa",
    "Southern Africa",
    "North America",
    "South America",
    "Central America",
    "Caribbean",
    "Europe",
    "Eastern Europe",
    "Western Europe",
    "Northern Europe",
    "Southern Europe",
    "Asia",
    "East Asia",
    "South Asia",
    "Southeast Asia",
    "Central Asia",
    "Oceania",
}

# Indicator codes used for database normalization.
# This keeps a stable, queryable identifier in the relational layer.
INDICATOR_CODES = {
    "worldbank_gdp": "NY.GDP.MKTP.CD",
    "worldbank_population": "SP.POP.TOTL",
    "fsi": "RANK",
    "cpi": "CPI",
}


def _read_config(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _dataset_path(cfg: dict[str, Any], dataset_id: str, raw_dir: Path) -> Path | None:
    datasets = cfg.get("datasets") or []
    for ds in datasets:
        if ds.get("id") == dataset_id:
            filename = ds.get("output_filename")
            if filename:
                output_dir = ds.get("output_dir") or dataset_id
                return raw_dir / str(output_dir) / str(filename)
    return None


def _dataset_entry(cfg: dict[str, Any], dataset_id: str) -> dict[str, Any] | None:
    for ds in cfg.get("datasets") or []:
        if ds.get("id") == dataset_id:
            return ds
    return None


def _infer_year_from_text(text: str | None) -> int | None:
    if not text:
        return None
    match = re.search(r"(19|20)\d{2}", text)
    if not match:
        return None
    try:
        return int(match.group(0))
    except ValueError:
        return None


def _infer_year_from_dataset(ds: dict[str, Any] | None, raw_path: Path | None) -> int | None:
    candidates: list[str] = []
    if raw_path:
        candidates.extend([raw_path.name, raw_path.parent.name])
    if ds:
        candidates.extend(
            [
                str(ds.get("output_filename") or ""),
                str(ds.get("url") or ""),
                str(ds.get("name") or ""),
                str(ds.get("id") or ""),
            ]
        )
    for item in candidates:
        year = _infer_year_from_text(item)
        if year:
            return year
    return None


def _legacy_fsi_path(raw_dir: Path) -> Path | None:
    legacy_dir = raw_dir / "fsi_2023"
    if not legacy_dir.exists():
        return None
    for candidate in legacy_dir.iterdir():
        if candidate.is_file() and candidate.suffix.lower() in {".xlsx", ".xls"}:
            return candidate
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
        if country in NON_COUNTRY_LABELS:
            continue
        if not iso3 or not isinstance(iso3, str) or len(iso3.strip()) != 3:
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
    df = pd.DataFrame(rows)
    if df.empty:
        return []
    df["value"] = pd.to_numeric(df["value"], errors="coerce").round(0)
    df["value"] = df["value"].astype("Int64")
    df = df[df["value"].notna() & (df["value"] > 0)]
    df = df[df["year"].notna()]
    df["year"] = df["year"].astype(int)
    # Keep the latest available year per ISO3 code.
    df = df.sort_values(["iso3", "year"]).drop_duplicates(subset=["iso3"], keep="last")
    return df.to_dict(orient="records")


def _load_fsi_rows(
    raw_path: Path,
    aliases: dict[str, str],
    *,
    dataset_id: str,
    year_hint: int | None,
) -> list[dict[str, Any]]:
    df = pd.read_excel(raw_path)
    if "Country" not in df.columns or "Rank" not in df.columns:
        return []
    df = df.rename(columns={"Country": "country", "Rank": "value"})
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df[df["value"].notna()]

    year_col = "Year" if "Year" in df.columns else None
    if year_col:
        df["year"] = pd.to_numeric(df[year_col], errors="coerce")
        df = df[df["year"].notna()]
        df["year"] = df["year"].astype(int)
        df = df.sort_values(["country", "year"]).drop_duplicates(subset=["country"], keep="last")
    else:
        if year_hint is None:
            print("[warn] fsi: Year column missing and no year hint; skipping", file=sys.stderr)
            return []
        # Treat as latest snapshot when no year column is present.
        df["year"] = year_hint

    rows: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        country = row.get("country")
        if not isinstance(country, str) or not country.strip():
            continue
        country = country.strip()
        if country in NON_COUNTRY_LABELS:
            continue
        rows.append(
            {
                "dataset_id": dataset_id,
                "indicator_code": INDICATOR_CODES.get(dataset_id),
                "country": country,
                "country_norm": _norm_country(country, aliases),
                "iso3": None,
                "year": int(row.get("year")) if row.get("year") else None,
                "value": row.get("value"),
            }
        )
    return rows


STRICT_XML_MAP = {
    b"http://purl.oclc.org/ooxml/spreadsheetml/main": b"http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    b"http://purl.oclc.org/ooxml/officeDocument/relationships": b"http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    b"http://purl.oclc.org/ooxml/drawingml/main": b"http://schemas.openxmlformats.org/drawingml/2006/main",
    b"http://purl.oclc.org/ooxml/drawingml/chart": b"http://schemas.openxmlformats.org/drawingml/2006/chart",
    b"http://purl.oclc.org/ooxml/drawingml/diagram": b"http://schemas.openxmlformats.org/drawingml/2006/diagram",
    b"http://purl.oclc.org/ooxml/drawingml/chartDrawing": b"http://schemas.openxmlformats.org/drawingml/2006/chartDrawing",
}


def _is_strict_ooxml_xlsx(path: Path) -> bool:
    if path.suffix.lower() != ".xlsx":
        return False
    try:
        with zipfile.ZipFile(path) as zf:
            data = zf.read("xl/workbook.xml")
    except Exception:
        return False
    return b"purl.oclc.org/ooxml/spreadsheetml/main" in data


def _rewrite_strict_xlsx(src: Path, dest: Path) -> None:
    with zipfile.ZipFile(src, "r") as zin, zipfile.ZipFile(dest, "w") as zout:
        for info in zin.infolist():
            payload = zin.read(info.filename)
            if info.filename.endswith(".xml"):
                for old, new in STRICT_XML_MAP.items():
                    payload = payload.replace(old, new)
            zout.writestr(info, payload)


def _prepare_cpi_workbook(path: Path) -> tuple[Path, callable]:
    """
    CPI 2025 is a strict OOXML file that openpyxl can't read directly.
    If detected, rewrite namespaces into a temp workbook for parsing.
    """
    if not _is_strict_ooxml_xlsx(path):
        return path, lambda: None
    fd, tmp_name = tempfile.mkstemp(prefix="cpi_ooxml_", suffix=".xlsx")
    os.close(fd)
    tmp_path = Path(tmp_name)
    _rewrite_strict_xlsx(path, tmp_path)
    return tmp_path, lambda: tmp_path.unlink(missing_ok=True)


def _load_cpi_rows(
    raw_path: Path,
    aliases: dict[str, str],
    *,
    dataset_id: str,
    year_hint: int | None,
) -> list[dict[str, Any]]:
    workbook_path, cleanup = _prepare_cpi_workbook(raw_path)
    try:
        # CPI files can shift the header row; detect it dynamically.
        dataset_label = dataset_id
        try:
            preview = pd.read_excel(workbook_path, header=None, nrows=10)
        except Exception as exc:
            print(f"[warn] {dataset_label}: failed to read workbook ({exc}); skipping", file=sys.stderr)
            return []
        header_idx = None
        for idx, row in preview.iterrows():
            values = {str(v).strip().lower() for v in row.values if isinstance(v, str)}
            has_country = bool({"country / territory", "country/territory"} & values)
            has_score = any(("cpi" in v and "score" in v) for v in values)
            if has_country and has_score:
                header_idx = idx
                break
        if header_idx is None:
            print(f"[warn] {dataset_label}: header row not found; skipping", file=sys.stderr)
            return []

        try:
            df = pd.read_excel(workbook_path, header=header_idx)
        except Exception as exc:
            print(f"[warn] {dataset_label}: failed to read sheet ({exc}); skipping", file=sys.stderr)
            return []
        df.columns = [str(c).strip() for c in df.columns]
        col_map: dict[str, str] = {}
        score_year = None
        for col in df.columns:
            key = col.strip().lower()
            if key in {"country / territory", "country/territory"}:
                col_map[col] = "country"
            elif key == "iso3":
                col_map[col] = "iso3"
            elif "cpi" in key and "score" in key:
                col_map[col] = "value"
                if score_year is None:
                    score_year = _infer_year_from_text(key)

        df = df.rename(columns=col_map)
        if "country" not in df.columns or "value" not in df.columns:
            print(f"[warn] {dataset_label}: required columns missing; skipping", file=sys.stderr)
            return []

        year = score_year or year_hint
        if year is None:
            print(f"[warn] {dataset_label}: year not detected; skipping", file=sys.stderr)
            return []

        df["value"] = pd.to_numeric(df["value"], errors="coerce").round(0)
        df["value"] = df["value"].astype("Int64")
        df = df[df["value"].between(0, 100)]
        rows: list[dict[str, Any]] = []
        for _, row in df.iterrows():
            country = row.get("country")
            if not isinstance(country, str) or not country.strip():
                continue
            country = country.strip()
            if country in NON_COUNTRY_LABELS:
                continue
            iso3 = row.get("iso3")
            rows.append(
                {
                    "dataset_id": dataset_id,
                    "indicator_code": INDICATOR_CODES.get(dataset_id),
                    "country": country,
                    "country_norm": _norm_country(country, aliases),
                    "iso3": _norm_iso3(iso3),
                    "year": year,
                    "value": row.get("value"),
                }
            )
        return rows
    finally:
        cleanup()


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


def _print_sanity_checks(conn) -> None:
    """
    Print basic row counts to validate the load quickly.
    """
    with conn.cursor() as cur:
        checks = [
            ("dataset_config", "SELECT COUNT(*) FROM dataset_config"),
            ("etl_load_log", "SELECT COUNT(*) FROM etl_load_log"),
            ("dim_country", "SELECT COUNT(*) FROM dim_country"),
            ("mrds_deposit", "SELECT COUNT(*) FROM mrds_deposit"),
            ("country_indicator", "SELECT COUNT(*) FROM country_indicator"),
        ]
        for label, sql in checks:
            cur.execute(sql)
            count = cur.fetchone()[0]
            print(f"[sanity] {label}: {count}")

        cur.execute(
            "SELECT dataset_id, COUNT(*) FROM country_indicator GROUP BY dataset_id ORDER BY dataset_id"
        )
        for dataset_id, count in cur.fetchall():
            print(f"[sanity] country_indicator[{dataset_id}]: {count}")


def _load_mrds_location(path: Path, aliases: dict[str, str]) -> pd.DataFrame:
    df = _read_mrds_table(path, usecols=["dep_id", "country", "state_prov", "region", "county"])
    df = df[df["country"].notna()]
    df["country"] = df["country"].astype(str).str.strip()
    invalid_countries = {"AF", "EU", "AS", "OC", "SA", "CR"}
    df = df[~df["country"].isin(invalid_countries)]
    df = df[~df["country"].isin(NON_COUNTRY_LABELS)]
    df = df[df["country"] != ""]
    df["country_norm"] = df["country"].apply(normalize_country_name)
    df["country_norm"] = df["country_norm"].map(lambda x: aliases.get(x, x))
    # Replace blanks in location columns to keep consistent reporting.
    for col in ["state_prov", "region", "county"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df.loc[df[col].isin(["", "nan", "None"]), col] = "N/A"
    return df


def _resolve_mrds_file(base_dir: Path, name: str) -> Path | None:
    """
    Resolve MRDS tables that may be delivered as .csv or .txt.
    The rdbms-tab-all bundle uses tab-delimited .txt files.
    """
    csv_path = base_dir / f"{name}.csv"
    txt_path = base_dir / f"{name}.txt"
    if csv_path.exists():
        return csv_path
    if txt_path.exists():
        return txt_path
    return None


def _read_mrds_table(path: Path, usecols: list[str]) -> pd.DataFrame:
    """
    Read MRDS tables from either .csv or .txt files.
    Tab-delimited .txt files are used in the rdbms-tab-all archive.
    """
    delimiter = "\t" if path.suffix.lower() == ".txt" else ","
    header = pd.read_csv(path, sep=delimiter, nrows=0, low_memory=False)
    available = set(header.columns)
    cols = [c for c in usecols if c in available]
    df = pd.read_csv(path, usecols=cols, sep=delimiter, low_memory=False)
    for missing in (set(usecols) - set(cols)):
        df[missing] = None
    return df[usecols]


def _strip_text_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for col in columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df.loc[df[col].isin(["", "nan", "None"]), col] = None
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


def _ensure_dataset_config_seed(cur, cfg: dict[str, Any]) -> None:
    """
    Ensure dataset_config contains all dataset_ids referenced by the ETL.
    Seed a baseline list if the table is empty, then upsert from config.
    """
    cur.execute("SELECT COUNT(*) FROM dataset_config")
    count = cur.fetchone()[0]
    seed_path = REPO_ROOT / "database" / "seed_dataset_config.sql"
    if count == 0 and seed_path.exists():
        cur.execute(seed_path.read_text(encoding="utf-8"))

    existing = set()
    cur.execute("SELECT dataset_id FROM dataset_config")
    for row in cur.fetchall():
        existing.add(row[0])

    missing = []
    for ds in cfg.get("datasets") or []:
        if ds.get("id") not in existing:
            missing.append(ds.get("id"))
    if missing:
        _insert_dataset_config(cur, cfg)


def _normalize_dataset_ids(cur) -> None:
    cur.execute("SELECT dataset_id FROM dataset_config")
    existing = {row[0] for row in cur.fetchall()}
    if "fsi_2023" not in existing:
        return
    if "fsi" not in existing:
        # Ensure target id exists (will be updated by config upsert afterward).
        cur.execute(
            """
            INSERT INTO dataset_config (dataset_id, source_name, source_url, format, update_frequency)
            VALUES ('fsi', 'Fragile States Index', 'https://fragilestatesindex.org/', 'xlsx', 'annual')
            ON CONFLICT (dataset_id) DO NOTHING
            """
        )
        existing.add("fsi")

    # Update references first to satisfy FK constraints.
    cur.execute("UPDATE country_indicator SET dataset_id = 'fsi' WHERE dataset_id = 'fsi_2023'")
    cur.execute("UPDATE etl_load_log SET dataset_id = 'fsi' WHERE dataset_id = 'fsi_2023'")

    if "fsi" in existing:
        cur.execute("DELETE FROM dataset_config WHERE dataset_id = 'fsi_2023'")
    else:
        cur.execute("UPDATE dataset_config SET dataset_id = 'fsi' WHERE dataset_id = 'fsi_2023'")


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
            _ensure_dataset_config_seed(cur, cfg)
            _insert_dataset_config(cur, cfg)
            _normalize_dataset_ids(cur)

            # Countries from MRDS Location
            loc_path = _resolve_mrds_file(mrds_extract, "Location")
            location_df = _load_mrds_location(loc_path, aliases) if loc_path else pd.DataFrame()

            # Countries from indicators
            countries = []
            if not location_df.empty:
                countries.extend(
                    zip(location_df["country"], location_df["country_norm"], [None] * len(location_df))
                )

            gdp_path = _dataset_path(cfg, "worldbank_gdp", raw_dir)
            pop_path = _dataset_path(cfg, "worldbank_population", raw_dir)
            fsi_entry = _dataset_entry(cfg, "fsi")
            fsi_path = _dataset_path(cfg, "fsi", raw_dir)
            if not fsi_path or not fsi_path.exists():
                legacy_path = _legacy_fsi_path(raw_dir)
                if legacy_path:
                    fsi_path = legacy_path
            cpi_entry = _dataset_entry(cfg, "cpi")
            cpi_path = _dataset_path(cfg, "cpi", raw_dir)

            gdp_rows = _load_worldbank_rows(gdp_path, "worldbank_gdp", aliases) if gdp_path and gdp_path.exists() else []
            pop_rows = _load_worldbank_rows(pop_path, "worldbank_population", aliases) if pop_path and pop_path.exists() else []
            fsi_year_hint = _infer_year_from_dataset(fsi_entry, fsi_path)
            fsi_rows = (
                _load_fsi_rows(fsi_path, aliases, dataset_id="fsi", year_hint=fsi_year_hint)
                if fsi_path and fsi_path.exists()
                else []
            )
            cpi_year_hint = _infer_year_from_dataset(cpi_entry, cpi_path)
            cpi_rows = (
                _load_cpi_rows(cpi_path, aliases, dataset_id="cpi", year_hint=cpi_year_hint)
                if cpi_path and cpi_path.exists()
                else []
            )

            for rows in (gdp_rows, pop_rows, fsi_rows, cpi_rows):
                if rows:
                    countries.extend(
                        [(r["country"], r["country_norm"], r["iso3"]) for r in rows]
                    )

            _insert_countries(cur, countries)
            country_map = _country_id_map(cur)

            # MRDS deposit
            mrds_path = _resolve_mrds_file(mrds_extract, "MRDS")
            mrds_inserted = 0
            valid_dep_ids: set[int] = set()
            if mrds_path and mrds_path.exists():
                df = _read_mrds_table(
                    mrds_path,
                    usecols=["dep_id", "name", "dev_stat", "code_list", "latitude", "longitude"],
                )
                df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
                df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
                df = df[df["dep_id"].notna()]
                df["dep_id"] = df["dep_id"].astype(int)
                mrds_inserted = len(df)
                valid_dep_ids = set(df["dep_id"].tolist())
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
                            f"SRID=4326;POINT({lon} {lat})"
                            if pd.notna(lat) and pd.notna(lon)
                            else None,
                        )
                        for dep_id, name, dev_stat, code_list, lat, lon in rows
                    ],
                    template="(%s,%s,%s,%s,%s,%s,ST_GeomFromText(%s))",
                )

            # MRDS location
            if not location_df.empty:
                # Ensure locations only reference deposits we loaded.
                if valid_dep_ids:
                    location_df = location_df[location_df["dep_id"].astype(int).isin(valid_dep_ids)]
                location_df["country_id"] = location_df["country_norm"].map(country_map)
                # MRDS Location can contain repeated dep_id rows; keep one per deposit.
                location_df = location_df.drop_duplicates(subset=["dep_id"])
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
                "Commodity": (
                    "mrds_commodity",
                    ["dep_id", "commod", "code", "commod_tp", "commod_group", "import"],
                ),
                "Materials": (
                    "mrds_material",
                    ["dep_id", "rec", "ore_gangue", "material"],
                ),
                "Ownership": (
                    "mrds_ownership",
                    ["dep_id", "owner_name", "owner_tp"],
                ),
                "Physiography": (
                    "mrds_physiography",
                    ["dep_id", "phys_div", "phys_prov", "phys_sect", "phys_det"],
                ),
                "Ages": (
                    "mrds_ages",
                    ["dep_id", "age_tp", "age_young"],
                ),
                "Rocks": (
                    "mrds_rocks",
                    ["dep_id", "rock_cls", "first_ord_nm", "second_ord_nm", "third_ord_nm", "low_name"],
                ),
            }
            for name, (table, cols) in related.items():
                path = _resolve_mrds_file(mrds_extract, name)
                if not path or not path.exists():
                    continue
                df = _read_mrds_table(path, usecols=cols)
                if name == "Materials" and "ore_gangue" not in df.columns:
                    # Some MRDS exports use "ore_gauge"; normalize to ore_gangue.
                    alt = _read_mrds_table(path, usecols=["dep_id", "rec", "ore_gauge", "material"])
                    if "ore_gauge" in alt.columns:
                        alt = alt.rename(columns={"ore_gauge": "ore_gangue"})
                        df = alt[cols]
                if name == "Rocks":
                    for col in ["first_ord_nm", "second_ord_nm", "third_ord_nm"]:
                        if col in df.columns:
                            df[col] = df[col].astype(str).str.strip()
                            df.loc[df[col].isin(["", "nan", "None"]), col] = "N/A"
                text_cols = [c for c in cols if c != "dep_id"]
                df = _strip_text_columns(df, text_cols)
                if "dep_id" in df.columns and valid_dep_ids:
                    df = df[df["dep_id"].astype(int).isin(valid_dep_ids)]
                if df.empty:
                    continue
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
            # Deduplicate within the batch to avoid ON CONFLICT double-hit errors.
            unique_rows: dict[tuple[int, str, str | None, int], tuple] = {}
            for row in rows:
                key = (row[0], row[1], row[2], row[3])
                if key not in unique_rows:
                    unique_rows[key] = row
            rows = list(unique_rows.values())
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
                ("fsi", fsi_path, len(fsi_rows)),
                ("cpi", cpi_path, len(cpi_rows)),
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
        _print_sanity_checks(conn)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
