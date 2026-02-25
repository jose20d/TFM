#!/usr/bin/env python3
from __future__ import annotations

"""ETL pipeline: load raw datasets directly into PostgreSQL."""

import hashlib
import json
import os
import re
import sys
import tempfile
import time
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


# Indicator codes used for database normalization.
# This keeps a stable, queryable identifier in the relational layer.
INDICATOR_CODES = {
    "worldbank_gdp": "NY.GDP.MKTP.CD",
    "worldbank_population": "SP.POP.TOTL",
    "fsi": "RANK",
    "cpi": "CPI",
}


def _read_config(path: Path) -> dict[str, Any]:
    """Load the datasets configuration JSON."""
    return json.loads(path.read_text(encoding="utf-8"))


def _dataset_path(cfg: dict[str, Any], dataset_id: str, raw_dir: Path) -> Path | None:
    """Resolve a dataset's raw file path from config."""
    datasets = cfg.get("datasets") or []
    for ds in datasets:
        if ds.get("id") == dataset_id:
            filename = ds.get("output_filename")
            if filename:
                output_dir = ds.get("output_dir") or dataset_id
                return raw_dir / str(output_dir) / str(filename)
    return None


def _dataset_entry(cfg: dict[str, Any], dataset_id: str) -> dict[str, Any] | None:
    """Return the dataset config entry by id."""
    for ds in cfg.get("datasets") or []:
        if ds.get("id") == dataset_id:
            return ds
    return None


def _infer_year_from_text(text: str | None) -> int | None:
    """Extract a 4-digit year from a string, if present."""
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
    """Infer a year from dataset metadata or filenames."""
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
    """Locate legacy FSI files when dataset ids change."""
    legacy_dir = raw_dir / "fsi_2023"
    if not legacy_dir.exists():
        return None
    for candidate in legacy_dir.iterdir():
        if candidate.is_file() and candidate.suffix.lower() in {".xlsx", ".xls"}:
            return candidate
    return None


def _norm_country(value: str, aliases: dict[str, str]) -> str:
    """Normalize country names with alias support."""
    norm = normalize_country_name(value)
    return aliases.get(norm, norm)


def _norm_iso3(value: str | None) -> str | None:
    """Normalize ISO3 codes to uppercase."""
    if not value:
        return None
    text = str(value).strip()
    return normalize_iso3(text) if text else None


def _load_worldbank_rows(
    raw_path: Path, dataset_id: str, aliases: dict[str, str]
) -> list[dict[str, Any]]:
    """Parse World Bank JSON and return the latest value per ISO3."""
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


def _read_iso_country_codes(
    path: Path, aliases: dict[str, str]
) -> tuple[pd.DataFrame, set[str], set[str]]:
    """
    Load ISO 3166-1 country codes (Alpha-3) and normalized country names.
    Returns a normalized dataframe plus lookup sets for filtering.
    """
    df = pd.read_csv(path)
    cols = {c.lower(): c for c in df.columns}
    name_col = cols.get("cldr display name") or cols.get("name") or cols.get("official name en")
    iso3_col = cols.get("iso3166-1-alpha-3") or cols.get("iso3166-1-alpha-3 code") or cols.get("alpha-3")
    iso2_col = cols.get("iso3166-1-alpha-2") or cols.get("iso3166-1-alpha-2 code") or cols.get("alpha-2")
    iso_num_col = cols.get("iso3166-1-numeric") or cols.get("iso3166-1-numeric code") or cols.get("numeric")
    if not name_col or not iso3_col:
        return pd.DataFrame(), set(), set()

    out_rows = []
    iso3_set: set[str] = set()
    name_set: set[str] = set()
    for _, row in df.iterrows():
        name = row.get(name_col)
        iso3 = row.get(iso3_col)
        if not isinstance(name, str) or not isinstance(iso3, str):
            continue
        name_norm = normalize_country_name(name)
        name_norm = aliases.get(name_norm, name_norm)
        iso3_norm = normalize_iso3(iso3)
        iso2 = row.get(iso2_col) if iso2_col else None
        iso_num = row.get(iso_num_col) if iso_num_col else None
        out_rows.append(
            {
                "country_name": name.strip(),
                "country_norm": name_norm,
                "iso2": str(iso2).strip().upper() if isinstance(iso2, str) else None,
                "iso3": iso3_norm,
                "iso_numeric": str(iso_num).strip() if iso_num is not None else None,
            }
        )
        iso3_set.add(iso3_norm)
        name_set.add(name_norm)

    df_out = pd.DataFrame(out_rows)
    if not df_out.empty:
        df_out = df_out.drop_duplicates(subset=["iso3"])
    return df_out, iso3_set, name_set


def _filter_countries_by_iso(
    rows: Iterable[tuple[str, str, str | None]],
    iso3_set: set[str],
    name_set: set[str],
) -> list[tuple[str, str, str | None]]:
    """Keep only countries present in the ISO 3166-1 whitelist."""
    if not iso3_set and not name_set:
        return list(rows)
    filtered = []
    for name, norm, iso3 in rows:
        if iso3 and normalize_iso3(iso3) in iso3_set:
            filtered.append((name, norm, iso3))
            continue
        if norm and norm in name_set:
            filtered.append((name, norm, iso3))
    return filtered


def _insert_iso_country_codes(cur, df: pd.DataFrame) -> int:
    """Insert ISO country reference rows into the database."""
    if df.empty:
        return 0
    rows = [
        (
            r.country_name,
            r.country_norm,
            r.iso2,
            r.iso3,
            r.iso_numeric,
        )
        for r in df.itertuples(index=False)
    ]
    sql = """
        INSERT INTO iso_country_codes (country_name, country_norm, iso2, iso3, iso_numeric)
        VALUES %s
        ON CONFLICT (iso3) DO UPDATE
        SET country_name = EXCLUDED.country_name,
            country_norm = EXCLUDED.country_norm,
            iso2 = EXCLUDED.iso2,
            iso_numeric = EXCLUDED.iso_numeric
    """
    execute_values(cur, sql, rows)
    return len(rows)


def _load_fsi_rows(
    raw_path: Path,
    aliases: dict[str, str],
    *,
    dataset_id: str,
    year_hint: int | None,
) -> list[dict[str, Any]]:
    """Parse the Fragile States Index Excel into normalized rows."""
    df = pd.read_excel(raw_path)
    if "Country" not in df.columns or "Rank" not in df.columns:
        return []
    df = df.rename(columns={"Country": "country", "Rank": "value"})
    # Normalize rank values like "144th" to numeric.
    df["value"] = (
        df["value"]
        .astype(str)
        .str.extract(r"(\d+)", expand=False)
        .pipe(pd.to_numeric, errors="coerce")
    )
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
    """Detect strict OOXML workbooks that need namespace rewriting."""
    if path.suffix.lower() != ".xlsx":
        return False
    try:
        with zipfile.ZipFile(path) as zf:
            data = zf.read("xl/workbook.xml")
    except Exception:
        return False
    return b"purl.oclc.org/ooxml/spreadsheetml/main" in data


def _rewrite_strict_xlsx(src: Path, dest: Path) -> None:
    """Rewrite strict OOXML namespaces into standard OOXML."""
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
    """Parse CPI Excel into normalized rows with inferred year."""
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
    """Compute a SHA-256 hash for a file."""
    if not path.exists():
        return None
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _file_size(path: Path) -> int | None:
    """Return file size in bytes if the path exists."""
    return path.stat().st_size if path.exists() else None


def _get_dataset_state(cur, dataset_id: str) -> tuple[str | None, bool | None]:
    """Return the last hash and success flag for a dataset."""
    cur.execute(
        "SELECT last_hash, last_success FROM etl_dataset_state WHERE dataset_id = %s",
        (dataset_id,),
    )
    row = cur.fetchone()
    if not row:
        return None, None
    return row[0], row[1]


def _upsert_dataset_state(cur, dataset_id: str, hash_value: str, success: bool) -> None:
    """Insert or update the dataset state after a successful load."""
    cur.execute(
        """
        INSERT INTO etl_dataset_state (dataset_id, last_hash, last_loaded_at, last_success)
        VALUES (%s, %s, NOW(), %s)
        ON CONFLICT (dataset_id) DO UPDATE
        SET last_hash = EXCLUDED.last_hash,
            last_loaded_at = EXCLUDED.last_loaded_at,
            last_success = EXCLUDED.last_success
        """,
        (dataset_id, hash_value, success),
    )


def _insert_run_log(
    cur,
    *,
    dataset_id: str,
    download_success: bool,
    hash_value: str | None,
    has_changes: bool,
    load_success: bool,
    rows_inserted: int,
    rows_updated: int,
    duration_ms: int,
    error_message: str | None,
) -> None:
    """Insert a historical ETL run log row."""
    cur.execute(
        """
        INSERT INTO etl_dataset_run_log (
            dataset_id, download_success, hash_value, has_changes, load_success,
            rows_inserted, rows_updated, duration_ms, error_message
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            dataset_id,
            download_success,
            hash_value,
            has_changes,
            load_success,
            rows_inserted,
            rows_updated,
            duration_ms,
            error_message,
        ),
    )


def _log_etl(
    cur,
    dataset_id: str,
    raw_path: Path | None,
    rows_inserted: int | None,
    rows_failed: int | None,
    status: str,
    error_message: str | None = None,
) -> None:
    """Insert a single ETL audit row."""
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
    """Load MRDS Location data and normalize country names."""
    df = _read_mrds_table(path, usecols=["dep_id", "country", "state_prov", "region", "county"])
    df = df[df["country"].notna()]
    df["country"] = df["country"].astype(str).str.strip()
    invalid_countries = {"AF", "EU", "AS", "OC", "SA", "CR"}
    df = df[~df["country"].isin(invalid_countries)]
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
    """Normalize text columns by trimming and nulling empty strings."""
    for col in columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df.loc[df[col].isin(["", "nan", "None"]), col] = None
    return df


def _dedupe_countries(
    rows: Iterable[tuple[str, str, str | None]]
) -> list[tuple[str, str, str | None]]:
    """Deduplicate country rows, preferring ISO3 when available."""
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
    """Insert countries into dim_country with upsert semantics."""
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
    """Build a country_norm → country_id map."""
    cur.execute("SELECT country_id, country_norm FROM dim_country")
    return {row[1]: row[0] for row in cur.fetchall()}


def _insert_dataset_config(cur, cfg: dict[str, Any]) -> None:
    """Upsert dataset configuration metadata."""
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
    """Normalize legacy dataset ids to current ids (e.g., fsi_2023 → fsi)."""
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
    """Run the ETL to load all datasets into PostgreSQL."""
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

            iso_path = _dataset_path(cfg, "iso_country_codes", raw_dir)
            iso_df = pd.DataFrame()
            iso3_set: set[str] = set()
            iso_name_set: set[str] = set()
            if iso_path and iso_path.exists():
                iso_df, iso3_set, iso_name_set = _read_iso_country_codes(iso_path, aliases)

            def log_no_change(dataset_id: str, hash_value: str | None, duration_ms: int) -> None:
                _insert_run_log(
                    cur,
                    dataset_id=dataset_id,
                    download_success=True,
                    hash_value=hash_value,
                    has_changes=False,
                    load_success=True,
                    rows_inserted=0,
                    rows_updated=0,
                    duration_ms=duration_ms,
                    error_message=None,
                )

            def log_missing(dataset_id: str, duration_ms: int) -> None:
                _insert_run_log(
                    cur,
                    dataset_id=dataset_id,
                    download_success=False,
                    hash_value=None,
                    has_changes=False,
                    load_success=False,
                    rows_inserted=0,
                    rows_updated=0,
                    duration_ms=duration_ms,
                    error_message="Raw file not found",
                )

            def process_dataset(
                dataset_id: str,
                raw_path: Path | None,
                loader,
            ) -> None:
                start = time.time()
                if not raw_path or not raw_path.exists():
                    log_missing(dataset_id, int((time.time() - start) * 1000))
                    conn.commit()
                    return

                hash_value = _file_hash(raw_path)
                last_hash, _ = _get_dataset_state(cur, dataset_id)
                if hash_value and last_hash == hash_value:
                    log_no_change(dataset_id, hash_value, int((time.time() - start) * 1000))
                    conn.commit()
                    return

                try:
                    rows_inserted, rows_updated = loader()
                    _upsert_dataset_state(cur, dataset_id, hash_value or "", True)
                    _insert_run_log(
                        cur,
                        dataset_id=dataset_id,
                        download_success=True,
                        hash_value=hash_value,
                        has_changes=True,
                        load_success=True,
                        rows_inserted=rows_inserted,
                        rows_updated=rows_updated,
                        duration_ms=int((time.time() - start) * 1000),
                        error_message=None,
                    )
                    conn.commit()
                except Exception as exc:
                    conn.rollback()
                    _insert_run_log(
                        cur,
                        dataset_id=dataset_id,
                        download_success=True,
                        hash_value=hash_value,
                        has_changes=True,
                        load_success=False,
                        rows_inserted=0,
                        rows_updated=0,
                        duration_ms=int((time.time() - start) * 1000),
                        error_message=str(exc),
                    )
                    conn.commit()

            def load_iso_codes() -> tuple[int, int]:
                if iso_df.empty:
                    return 0, 0
                inserted = _insert_iso_country_codes(cur, iso_df)
                return inserted, 0

            def load_worldbank(dataset_id: str, raw_path: Path) -> tuple[int, int]:
                rows = _load_worldbank_rows(raw_path, dataset_id, aliases)
                countries = [(r["country"], r["country_norm"], r["iso3"]) for r in rows]
                countries = _filter_countries_by_iso(countries, iso3_set, iso_name_set)
                _insert_countries(cur, countries)
                country_map = _country_id_map(cur)

                payload = []
                for r in rows:
                    country_id = country_map.get(r["country_norm"])
                    if not country_id or not r.get("year"):
                        continue
                    payload.append(
                        (
                            int(country_id),
                            r["dataset_id"],
                            r.get("indicator_code"),
                            int(r["year"]),
                            r.get("value"),
                        )
                    )
                unique_rows: dict[tuple[int, str, str | None, int], tuple] = {}
                for row in payload:
                    key = (row[0], row[1], row[2], row[3])
                    if key not in unique_rows:
                        unique_rows[key] = row
                payload = list(unique_rows.values())
                sql = """
                    INSERT INTO country_indicator (country_id, dataset_id, indicator_code, year, value)
                    VALUES %s
                    ON CONFLICT (country_id, dataset_id, indicator_code, year) DO UPDATE
                    SET value = EXCLUDED.value
                """
                execute_values(cur, sql, payload)
                return len(payload), 0

            def load_fsi(raw_path: Path) -> tuple[int, int]:
                fsi_entry = _dataset_entry(cfg, "fsi")
                year_hint = _infer_year_from_dataset(fsi_entry, raw_path)
                rows = _load_fsi_rows(raw_path, aliases, dataset_id="fsi", year_hint=year_hint)
                countries = [(r["country"], r["country_norm"], r["iso3"]) for r in rows]
                countries = _filter_countries_by_iso(countries, iso3_set, iso_name_set)
                _insert_countries(cur, countries)
                country_map = _country_id_map(cur)

                payload = []
                for r in rows:
                    country_id = country_map.get(r["country_norm"])
                    if not country_id or not r.get("year"):
                        continue
                    payload.append(
                        (
                            int(country_id),
                            r["dataset_id"],
                            r.get("indicator_code"),
                            int(r["year"]),
                            r.get("value"),
                        )
                    )
                unique_rows: dict[tuple[int, str, str | None, int], tuple] = {}
                for row in payload:
                    key = (row[0], row[1], row[2], row[3])
                    if key not in unique_rows:
                        unique_rows[key] = row
                payload = list(unique_rows.values())
                sql = """
                    INSERT INTO country_indicator (country_id, dataset_id, indicator_code, year, value)
                    VALUES %s
                    ON CONFLICT (country_id, dataset_id, indicator_code, year) DO UPDATE
                    SET value = EXCLUDED.value
                """
                execute_values(cur, sql, payload)
                return len(payload), 0

            def load_cpi(raw_path: Path) -> tuple[int, int]:
                cpi_entry = _dataset_entry(cfg, "cpi")
                year_hint = _infer_year_from_dataset(cpi_entry, raw_path)
                rows = _load_cpi_rows(raw_path, aliases, dataset_id="cpi", year_hint=year_hint)
                countries = [(r["country"], r["country_norm"], r["iso3"]) for r in rows]
                countries = _filter_countries_by_iso(countries, iso3_set, iso_name_set)
                _insert_countries(cur, countries)
                country_map = _country_id_map(cur)

                payload = []
                for r in rows:
                    country_id = country_map.get(r["country_norm"])
                    if not country_id or not r.get("year"):
                        continue
                    payload.append(
                        (
                            int(country_id),
                            r["dataset_id"],
                            r.get("indicator_code"),
                            int(r["year"]),
                            r.get("value"),
                        )
                    )
                unique_rows: dict[tuple[int, str, str | None, int], tuple] = {}
                for row in payload:
                    key = (row[0], row[1], row[2], row[3])
                    if key not in unique_rows:
                        unique_rows[key] = row
                payload = list(unique_rows.values())
                sql = """
                    INSERT INTO country_indicator (country_id, dataset_id, indicator_code, year, value)
                    VALUES %s
                    ON CONFLICT (country_id, dataset_id, indicator_code, year) DO UPDATE
                    SET value = EXCLUDED.value
                """
                execute_values(cur, sql, payload)
                return len(payload), 0

            def load_mrds(raw_path: Path) -> tuple[int, int]:
                loc_path = _resolve_mrds_file(mrds_extract, "Location")
                location_df = _load_mrds_location(loc_path, aliases) if loc_path else pd.DataFrame()
                if iso_name_set:
                    location_df = location_df[location_df["country_norm"].isin(iso_name_set)]

                countries = []
                if not location_df.empty:
                    countries.extend(
                        zip(location_df["country"], location_df["country_norm"], [None] * len(location_df))
                    )
                countries = _filter_countries_by_iso(countries, iso3_set, iso_name_set)
                _insert_countries(cur, countries)
                country_map = _country_id_map(cur)

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

                if not location_df.empty:
                    if valid_dep_ids:
                        location_df = location_df[location_df["dep_id"].astype(int).isin(valid_dep_ids)]
                    location_df["country_id"] = location_df["country_norm"].map(country_map)
                    location_df = location_df[location_df["country_id"].notna()]
                    location_df = location_df.drop_duplicates(subset=["dep_id"])
                    rows = [
                        (
                            int(r.dep_id),
                            int(r.country_id),
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
                dep_id_list = list(valid_dep_ids)
                for name, (table, cols) in related.items():
                    path = _resolve_mrds_file(mrds_extract, name)
                    if not path or not path.exists():
                        continue
                    df = _read_mrds_table(path, usecols=cols)
                    if name == "Materials" and "ore_gangue" not in df.columns:
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
                    if dep_id_list:
                        cur.execute(
                            f"DELETE FROM {table} WHERE dep_id = ANY(%s)",
                            (dep_id_list,),
                        )
                    rows = [tuple(r) for r in df.itertuples(index=False)]
                    sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s"
                    execute_values(cur, sql, rows)

                return mrds_inserted, 0

            datasets = cfg.get("datasets") or []
            dataset_ids = {ds.get("id") for ds in datasets if isinstance(ds, dict)}

            if "iso_country_codes" in dataset_ids:
                process_dataset("iso_country_codes", iso_path, load_iso_codes)
            if "worldbank_gdp" in dataset_ids:
                gdp_path = _dataset_path(cfg, "worldbank_gdp", raw_dir)
                process_dataset("worldbank_gdp", gdp_path, lambda: load_worldbank("worldbank_gdp", gdp_path))
            if "worldbank_population" in dataset_ids:
                pop_path = _dataset_path(cfg, "worldbank_population", raw_dir)
                process_dataset(
                    "worldbank_population",
                    pop_path,
                    lambda: load_worldbank("worldbank_population", pop_path),
                )
            if "fsi" in dataset_ids:
                fsi_path = _dataset_path(cfg, "fsi", raw_dir)
                if not fsi_path or not fsi_path.exists():
                    legacy_path = _legacy_fsi_path(raw_dir)
                    if legacy_path:
                        fsi_path = legacy_path
                process_dataset("fsi", fsi_path, lambda: load_fsi(fsi_path))
            if "cpi" in dataset_ids:
                cpi_path = _dataset_path(cfg, "cpi", raw_dir)
                process_dataset("cpi", cpi_path, lambda: load_cpi(cpi_path))
            if "mrds_csv" in dataset_ids:
                mrds_zip = _dataset_path(cfg, "mrds_csv", raw_dir)
                process_dataset("mrds_csv", mrds_zip, lambda: load_mrds(mrds_zip))

        _print_sanity_checks(conn)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
