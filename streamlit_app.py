#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd
import streamlit as st


ROOT = Path(__file__).resolve().parent
DATA_NORMALIZED = ROOT / "data" / "normalized"
REFERENCES = ROOT / "references"


def _file_exists(path: Path) -> bool:
    return path.exists() and path.is_file()


@st.cache_data(show_spinner=False)
def load_jsonl(path: Path) -> pd.DataFrame:
    return pd.read_json(path, lines=True)


@st.cache_data(show_spinner=False)
def load_dep_country_map() -> pd.DataFrame:
    path = DATA_NORMALIZED / "mrds_dep_country.jsonl"
    return load_jsonl(path)


@st.cache_data(show_spinner=False)
def load_country_indicator(dataset_id: str) -> pd.DataFrame:
    path = DATA_NORMALIZED / f"{dataset_id}.jsonl"
    return load_jsonl(path)


def filter_mrds_by_country(dep_ids: set[str], csv_path: Path) -> pd.DataFrame:
    chunks = []
    for chunk in pd.read_csv(csv_path, chunksize=100_000):
        if "dep_id" not in chunk.columns:
            return pd.DataFrame()
        mask = chunk["dep_id"].astype(str).isin(dep_ids)
        if mask.any():
            chunks.append(chunk.loc[mask])
    return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()


def _normalize_na(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for col in columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df.loc[df[col].isin(["", "nan", "None"]), col] = "N/A"
    return df


def _load_csv_columns(path: Path) -> list[str]:
    try:
        header = pd.read_csv(path, nrows=0)
        return list(header.columns)
    except Exception:
        return []


def load_clean_mrds_table(
    csv_path: Path,
    dep_ids: set[str],
    keep_columns: list[str],
    *,
    invalid_countries: set[str] | None = None,
) -> pd.DataFrame:
    columns = _load_csv_columns(csv_path)
    if "dep_id" not in columns:
        return pd.DataFrame()

    usecols = [c for c in ["dep_id", *keep_columns] if c in columns]
    chunks = []
    for chunk in pd.read_csv(csv_path, usecols=usecols, chunksize=100_000):
        mask = chunk["dep_id"].astype(str).isin(dep_ids)
        if not mask.any():
            continue
        filtered = chunk.loc[mask].copy()

        if csv_path.name == "Location.csv":
            if "country" in filtered.columns:
                filtered["country"] = filtered["country"].astype(str).str.strip()
                if invalid_countries:
                    filtered = filtered[~filtered["country"].isin(invalid_countries)]
                filtered = filtered[filtered["country"].notna() & (filtered["country"] != "")]
            if "state_prov" in filtered.columns:
                filtered["state_prov"] = filtered["state_prov"].astype(str).str.strip()
                filtered.loc[filtered["state_prov"] == "", "state_prov"] = "N/A"

        if csv_path.name == "MRDS.csv":
            if "latitude" in filtered.columns:
                filtered["latitude"] = pd.to_numeric(filtered["latitude"], errors="coerce")
            if "longitude" in filtered.columns:
                filtered["longitude"] = pd.to_numeric(filtered["longitude"], errors="coerce")
            if "latitude" in filtered.columns and "longitude" in filtered.columns:
                filtered = filtered[
                    filtered["latitude"].between(-90, 90)
                    & filtered["longitude"].between(-180, 180)
                ]

        text_cols = [c for c in keep_columns if c in filtered.columns]
        filtered = _normalize_na(filtered, text_cols)
        chunks.append(filtered)

    return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()


def build_clean_mrds_join(dep_ids: set[str], references_root: Path) -> pd.DataFrame:
    invalid_countries = {"AF", "EU", "AS", "OC", "SA", "CR"}
    tables = {
        "Location": (
            references_root / "Location.csv",
            ["country", "state_prov"],
        ),
        "Ages": (
            references_root / "Ages.csv",
            ["age_tp", "age_young"],
        ),
        "Commodity": (
            references_root / "Commodity.csv",
            ["commod", "code", "commod_tp", "commod_group", "import"],
        ),
        "Materials": (
            references_root / "Materials.csv",
            ["rec", "ore_gangue", "material"],
        ),
        "MRDS": (
            references_root / "MRDS.csv",
            ["code_list", "longitude", "latitude", "name", "dev_stat"],
        ),
        "Ownership": (
            references_root / "Ownership.csv",
            ["owner_name", "owner_tp"],
        ),
        "Physiography": (
            references_root / "Physiography.csv",
            ["phys_div", "phys_prov", "phys_sect", "phys_det"],
        ),
        "Rocks": (
            references_root / "Rocks.csv",
            ["rock_cls", "first_ord_nm", "second_ord_nm", "third_ord_nm", "low_name"],
        ),
    }

    joined: pd.DataFrame | None = None
    for name, (path, cols) in tables.items():
        if not _file_exists(path):
            continue
        table_df = load_clean_mrds_table(
            path, dep_ids, cols, invalid_countries=invalid_countries
        )
        if table_df.empty:
            return pd.DataFrame()
        table_df = table_df.rename(columns={c: f"{name.lower()}_{c}" for c in cols})
        if joined is None:
            joined = table_df
        else:
            joined = joined.merge(table_df, on="dep_id", how="inner")

    return joined if joined is not None else pd.DataFrame()


def get_country_options(df: pd.DataFrame) -> list[str]:
    if "country_norm" in df.columns:
        return sorted({str(x) for x in df["country_norm"].dropna().unique()})
    if "country" in df.columns:
        return sorted({str(x) for x in df["country"].dropna().unique()})
    return []


def filter_country(df: pd.DataFrame, selected: str) -> pd.DataFrame:
    if "country_norm" in df.columns and selected in df["country_norm"].unique():
        return df[df["country_norm"] == selected]
    if "country" in df.columns:
        return df[df["country"] == selected]
    return pd.DataFrame()


def latest_value_for_country(df: pd.DataFrame, selected: str) -> tuple[object | None, int | None]:
    filtered = filter_country(df, selected)
    if filtered.empty or "value" not in filtered.columns:
        return None, None

    if "year" in filtered.columns:
        years = pd.to_numeric(filtered["year"], errors="coerce")
        if years.notna().any():
            max_year = int(years.max())
            year_mask = years == max_year
            filtered = filtered.loc[year_mask]
            value = filtered["value"].iloc[0]
            return value, max_year

    value = filtered["value"].iloc[0]
    return value, None


def main() -> None:
    st.set_page_config(page_title="TFM Data Explorer", layout="wide")
    st.title("TFM — Data Relationship Explorer (Local)")
    st.caption("Local, no database. Built to validate relationships across datasets.")

    st.sidebar.header("Filters")
    mode = st.sidebar.selectbox(
        "Choose a data group",
        ["Country indicators (GDP/Population/CPI/FSI)", "MRDS tables (Rocks/Commodity/etc.)"],
    )

    st.sidebar.markdown("---")
    st.sidebar.subheader("Required pre-processing")
    st.sidebar.markdown(
        "- `python scripts/normalize_xlsx.py`\n"
        "- `python scripts/build_mrds_country_map.py`"
    )

    indicator_map = {
        "GDP (World Bank)": "worldbank_gdp",
        "Population (World Bank)": "worldbank_population",
        "CPI 2023": "cpi_2023",
        "FSI 2023": "fsi_2023",
    }
    indicator_data: dict[str, pd.DataFrame] = {}
    missing_files: list[Path] = []
    for ds_id in indicator_map.values():
        path = DATA_NORMALIZED / f"{ds_id}.jsonl"
        if _file_exists(path):
            indicator_data[ds_id] = load_country_indicator(ds_id)
        else:
            missing_files.append(path)

    if missing_files:
        missing_list = "\n".join(f"- {p}" for p in missing_files)
        st.warning(f"Missing indicator files:\n{missing_list}")

    all_countries: set[str] = set()
    for df in indicator_data.values():
        all_countries.update(get_country_options(df))

    if not all_countries:
        st.error("No country indicator data available.")
        st.stop()

    countries = sorted(all_countries)
    selected = st.sidebar.selectbox("Country", countries)

    gdp_value, gdp_year = latest_value_for_country(
        indicator_data.get("worldbank_gdp", pd.DataFrame()), selected
    )
    pop_value, pop_year = latest_value_for_country(
        indicator_data.get("worldbank_population", pd.DataFrame()), selected
    )
    cpi_value, cpi_year = latest_value_for_country(
        indicator_data.get("cpi_2023", pd.DataFrame()), selected
    )
    fsi_value, fsi_year = latest_value_for_country(
        indicator_data.get("fsi_2023", pd.DataFrame()), selected
    )

    col_gdp, col_pop, col_cpi, col_fsi = st.columns(4)
    gdp_label = f"PIB ({gdp_year})" if gdp_year else "PIB"
    pop_label = f"Poblacion ({pop_year})" if pop_year else "Poblacion"
    cpi_label = f"CPI score {cpi_year}" if cpi_year else "CPI score 2023"
    fsi_label = (
        f"Indice de fragilidad (Rank {fsi_year})"
        if fsi_year
        else "Indice de fragilidad (Rank)"
    )

    col_gdp.metric(gdp_label, gdp_value if gdp_value is not None else "N/A")
    col_pop.metric(pop_label, pop_value if pop_value is not None else "N/A")
    col_cpi.metric(cpi_label, cpi_value if cpi_value is not None else "N/A")
    col_fsi.metric(fsi_label, fsi_value if fsi_value is not None else "N/A")

    if mode.startswith("Country indicators"):
        choice = st.sidebar.selectbox("Dataset", list(indicator_map.keys()))
        dataset_id = indicator_map[choice]
        df = indicator_data.get(dataset_id, pd.DataFrame())
        filtered = filter_country(df, selected)

        st.subheader(choice)
        st.write(f"Rows: {len(filtered)}")
        st.dataframe(filtered, use_container_width=True)

    else:
        mrds_map_path = DATA_NORMALIZED / "mrds_dep_country.jsonl"
        if not _file_exists(mrds_map_path):
            st.error(f"Missing file: {mrds_map_path}")
            st.stop()

        dep_map = load_dep_country_map()
        mrds_tables = {
            "Rocks": REFERENCES / "Rocks.csv",
            "Commodity": REFERENCES / "Commodity.csv",
            "Materials": REFERENCES / "Materials.csv",
            "Ownership": REFERENCES / "Ownership.csv",
            "Physiography": REFERENCES / "Physiography.csv",
            "Ages": REFERENCES / "Ages.csv",
        }
        table_choice = st.sidebar.selectbox("MRDS table", list(mrds_tables.keys()))
        csv_path = mrds_tables[table_choice]

        if not _file_exists(csv_path):
            st.error(f"Missing file: {csv_path}")
            st.stop()

        if "country_norm" in dep_map.columns and selected in dep_map["country_norm"].unique():
            dep_ids = set(dep_map.loc[dep_map["country_norm"] == selected, "dep_id"].astype(str))
        else:
            dep_ids = set(dep_map.loc[dep_map["country"] == selected, "dep_id"].astype(str))

        st.subheader(f"{table_choice} — {selected}")
        st.write(f"dep_id matched: {len(dep_ids)}")

        with st.spinner("Filtering MRDS table..."):
            filtered = filter_mrds_by_country(dep_ids, csv_path)

        st.write(f"Rows: {len(filtered)}")
        st.dataframe(filtered.head(500), use_container_width=True)
        st.caption("Showing first 500 rows for performance.")

        st.markdown("---")
        st.subheader("Clean Data (Unified by dep_id)")
        st.caption("Inner join across MRDS tables with cleaned columns.")

        with st.spinner("Building clean unified dataset..."):
            clean_join = build_clean_mrds_join(dep_ids, REFERENCES)

        st.write(f"Rows after inner join: {len(clean_join)}")
        st.dataframe(clean_join.head(500), use_container_width=True)
        st.caption("Showing first 500 rows for performance.")


if __name__ == "__main__":
    main()

