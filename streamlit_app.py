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


def get_country_options(df: pd.DataFrame) -> list[str]:
    if "country" in df.columns:
        return sorted({str(x) for x in df["country"].dropna().unique()})
    if "country_norm" in df.columns:
        return sorted({str(x) for x in df["country_norm"].dropna().unique()})
    return []


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

    if mode.startswith("Country indicators"):
        indicator_map = {
            "GDP (World Bank)": "worldbank_gdp",
            "Population (World Bank)": "worldbank_population",
            "CPI 2023": "cpi_2023",
            "FSI 2023": "fsi_2023",
        }
        choice = st.sidebar.selectbox("Dataset", list(indicator_map.keys()))
        dataset_id = indicator_map[choice]
        path = DATA_NORMALIZED / f"{dataset_id}.jsonl"

        if not _file_exists(path):
            st.error(f"Missing file: {path}")
            st.stop()

        df = load_country_indicator(dataset_id)
        countries = get_country_options(df)
        selected = st.sidebar.selectbox("Country", countries)

        if "country_norm" in df.columns and selected in df["country_norm"].unique():
            filtered = df[df["country_norm"] == selected]
        else:
            filtered = df[df["country"] == selected]

        st.subheader(choice)
        st.write(f"Rows: {len(filtered)}")
        st.dataframe(filtered, use_container_width=True)

    else:
        mrds_map_path = DATA_NORMALIZED / "mrds_dep_country.jsonl"
        if not _file_exists(mrds_map_path):
            st.error(f"Missing file: {mrds_map_path}")
            st.stop()

        dep_map = load_dep_country_map()
        countries = get_country_options(dep_map)
        selected = st.sidebar.selectbox("Country", countries)

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


if __name__ == "__main__":
    main()

