#!/usr/bin/env python3
from __future__ import annotations

import pandas as pd
import streamlit as st

from src.db import get_connection
# The UI reads from PostgreSQL to keep a single source of truth
# and avoid intermediate JSON files in the presentation layer.


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


def _fetch_countries() -> pd.DataFrame:
    # The UI reads from PostgreSQL to avoid intermediate JSON files.
    try:
        with get_connection() as conn:
            df = pd.read_sql_query(
                "SELECT country_norm, country_name FROM dim_country ORDER BY country_name",
                conn,
            )
        return df
    except Exception as exc:
        st.error(
            "Database is not initialized. Run `python3 main.py` after enabling PostGIS.",
        )
        st.caption(f"Details: {exc}")
        return pd.DataFrame()


def _fetch_indicator(country_norm: str, dataset_id: str) -> pd.DataFrame:
    with get_connection() as conn:
        query = """
            SELECT d.dataset_id,
                   c.country_name AS country,
                   c.country_norm,
                   c.iso3 AS iso3,
                   NULL::text AS iso3_norm,
                   ci.year,
                   ci.value
            FROM country_indicator ci
            JOIN dim_country c ON c.country_id = ci.country_id
            JOIN dataset_config d ON d.dataset_id = ci.dataset_id
            WHERE c.country_norm = %s AND d.dataset_id = %s
            ORDER BY ci.year DESC
        """
        return pd.read_sql_query(query, conn, params=(country_norm, dataset_id))


def _fetch_dep_ids(country_norm: str) -> list[int]:
    with get_connection() as conn:
        df = pd.read_sql_query(
            """
            SELECT d.dep_id
            FROM mrds_location d
            JOIN dim_country c ON c.country_id = d.country_id
            WHERE c.country_norm = %s
            """,
            conn,
            params=(country_norm,),
        )
    return df["dep_id"].astype(int).tolist()


def _fetch_mrds_table(table_name: str, dep_ids: list[int]) -> pd.DataFrame:
    if not dep_ids:
        return pd.DataFrame()
    with get_connection() as conn:
        query = f"SELECT * FROM {table_name} WHERE dep_id = ANY(%s)"
        return pd.read_sql_query(query, conn, params=(dep_ids,))


def _fetch_clean_join(dep_ids: list[int]) -> pd.DataFrame:
    if not dep_ids:
        return pd.DataFrame()
    with get_connection() as conn:
        query = """
            SELECT d.dep_id,
                   l.country_id, l.state_prov,
                   r.rock_cls, r.first_ord_nm, r.second_ord_nm, r.third_ord_nm, r.low_name,
                   c.commod, c.code, c.commod_tp, c.commod_group, c.import,
                   m.rec, m.ore_gangue, m.material,
                   o.owner_name, o.owner_tp,
                   p.phys_div, p.phys_prov, p.phys_sect, p.phys_det,
                   a.age_tp, a.age_young
            FROM mrds_deposit d
            JOIN mrds_location l ON l.dep_id = d.dep_id
            JOIN mrds_rocks r ON r.dep_id = d.dep_id
            JOIN mrds_commodity c ON c.dep_id = d.dep_id
            JOIN mrds_material m ON m.dep_id = d.dep_id
            JOIN mrds_ownership o ON o.dep_id = d.dep_id
            JOIN mrds_physiography p ON p.dep_id = d.dep_id
            JOIN mrds_ages a ON a.dep_id = d.dep_id
            WHERE d.dep_id = ANY(%s)
            LIMIT 500
        """
        return pd.read_sql_query(query, conn, params=(dep_ids,))


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
    st.sidebar.markdown("- `python3 main.py` (download → clean → load → Streamlit)")

    countries_df = _fetch_countries()
    if countries_df.empty:
        st.error("No country data available in the database.")
        st.stop()

    selected = st.sidebar.selectbox(
        "Country",
        countries_df["country_norm"].tolist(),
        format_func=lambda x: countries_df.set_index("country_norm").loc[x, "country_name"],
    )

    indicator_map = {
        "GDP (World Bank)": "worldbank_gdp",
        "Population (World Bank)": "worldbank_population",
        "CPI (latest)": "cpi",
        "FSI (latest)": "fsi",
    }
    indicator_data: dict[str, pd.DataFrame] = {}
    for ds_id in indicator_map.values():
        indicator_data[ds_id] = _fetch_indicator(selected, ds_id)

    gdp_value, gdp_year = latest_value_for_country(
        indicator_data.get("worldbank_gdp", pd.DataFrame()), selected
    )
    pop_value, pop_year = latest_value_for_country(
        indicator_data.get("worldbank_population", pd.DataFrame()), selected
    )
    cpi_value, cpi_year = latest_value_for_country(
        indicator_data.get("cpi", pd.DataFrame()), selected
    )
    fsi_value, fsi_year = latest_value_for_country(
        indicator_data.get("fsi", pd.DataFrame()), selected
    )

    col_gdp, col_pop, col_cpi, col_fsi = st.columns(4)
    gdp_label = f"PIB ({gdp_year})" if gdp_year else "PIB"
    pop_label = f"Poblacion ({pop_year})" if pop_year else "Poblacion"
    cpi_label = f"CPI score {cpi_year}" if cpi_year else "CPI score"
    fsi_label = (
        f"Indice de fragilidad (Rank {fsi_year})"
        if fsi_year
        else "Indice de fragilidad (Rank)"
    )

    def _metric_value(value: object) -> str | object:
        if value is None:
            return "N/A"
        if isinstance(value, float) and pd.isna(value):
            return "N/A"
        return value

    col_gdp.metric(gdp_label, _metric_value(gdp_value))
    col_pop.metric(pop_label, _metric_value(pop_value))
    col_cpi.metric(cpi_label, _metric_value(cpi_value))
    col_fsi.metric(fsi_label, _metric_value(fsi_value))

    if mode.startswith("Country indicators"):
        choice = st.sidebar.selectbox("Dataset", list(indicator_map.keys()))
        dataset_id = indicator_map[choice]
        df = indicator_data.get(dataset_id, pd.DataFrame())
        filtered = filter_country(df, selected)

        st.subheader(choice)
        st.write(f"Rows: {len(filtered)}")
        st.dataframe(filtered.fillna("N/A"), use_container_width=True)

    else:
        dep_ids = _fetch_dep_ids(selected)
        st.subheader(f"MRDS tables — {selected}")
        st.write(f"dep_id matched: {len(dep_ids)}")

        table_choice = st.sidebar.selectbox(
            "MRDS table",
            ["Rocks", "Commodity", "Materials", "Ownership", "Physiography", "Ages"],
        )
        table_map = {
            "Rocks": "mrds_rocks",
            "Commodity": "mrds_commodity",
            "Materials": "mrds_material",
            "Ownership": "mrds_ownership",
            "Physiography": "mrds_physiography",
            "Ages": "mrds_ages",
        }

        with st.spinner("Filtering MRDS table..."):
            filtered = _fetch_mrds_table(table_map[table_choice], dep_ids)

        st.write(f"Rows: {len(filtered)}")
        st.dataframe(filtered.head(500).fillna("N/A"), use_container_width=True)
        st.caption("Showing first 500 rows for performance.")

        st.markdown("---")
        st.subheader("Clean Data (Unified by dep_id)")
        st.caption("Inner join across MRDS tables with cleaned columns.")

        with st.spinner("Building clean unified dataset..."):
            clean_join = _fetch_clean_join(dep_ids)

        st.write(f"Rows after inner join: {len(clean_join)}")
        st.dataframe(clean_join.fillna("N/A"), use_container_width=True)


if __name__ == "__main__":
    main()
