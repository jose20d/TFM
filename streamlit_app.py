#!/usr/bin/env python3
from __future__ import annotations

"""Streamlit UI for browsing country indicators and MRDS relations."""

import pandas as pd
import streamlit as st

from src.db import get_connection
# The UI reads from PostgreSQL to keep a single source of truth
# and avoid intermediate JSON files in the presentation layer.

INDICATOR_CODES = {
    "worldbank_gdp": "NY.GDP.MKTP.CD",
    "cpi": "CPI",
    "fsi": "RANK",
}


def filter_country(df: pd.DataFrame, selected: str) -> pd.DataFrame:
    """Filter a dataframe to the selected country (normalized or raw)."""
    if "country_norm" in df.columns and selected in df["country_norm"].unique():
        return df[df["country_norm"] == selected]
    if "country" in df.columns:
        return df[df["country"] == selected]
    return pd.DataFrame()


def latest_value_for_country(df: pd.DataFrame, selected: str) -> tuple[object | None, int | None]:
    """Return the latest value and year for a country in a dataset."""
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
    """Fetch available countries from the database."""
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
    """Fetch indicator rows for a country and dataset."""
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
    """Fetch MRDS dep_id values associated with a country."""
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
    """Fetch a MRDS table subset for the given dep_id list."""
    if not dep_ids:
        return pd.DataFrame()
    with get_connection() as conn:
        query = f"SELECT * FROM {table_name} WHERE dep_id = ANY(%s)"
        return pd.read_sql_query(query, conn, params=(dep_ids,))


def _fetch_clean_join(dep_ids: list[int]) -> pd.DataFrame:
    """Build a unified join across MRDS tables for a small sample."""
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


def _fetch_example_dep_ids(limit: int = 3) -> list[int]:
    """Return a small sample of dep_id values for UI examples."""
    with get_connection() as conn:
        df = pd.read_sql_query(
            "SELECT dep_id FROM mrds_deposit ORDER BY dep_id LIMIT %s",
            conn,
            params=(limit,),
        )
    return df["dep_id"].astype(int).tolist()


def _fetch_example_minerals(limit: int = 3) -> list[str]:
    """Return top minerals by frequency as UI examples."""
    with get_connection() as conn:
        df = pd.read_sql_query(
            """
            SELECT mc.commod
            FROM mrds_commodity mc
            GROUP BY mc.commod
            ORDER BY COUNT(*) DESC
            LIMIT %s
            """,
            conn,
            params=(limit,),
        )
    return [str(x) for x in df["commod"].tolist()]


SQL_WORLD_VIEW = """
    SELECT d.dep_id,
           d.name,
           d.latitude,
           d.longitude,
           d.dev_stat,
           c.country_name
    FROM mrds_deposit d
    JOIN mrds_location l ON d.dep_id = l.dep_id
    JOIN dim_country c ON l.country_id = c.country_id
"""

SQL_FILTER_MINERAL = """
    SELECT d.dep_id,
           d.name,
           d.latitude,
           d.longitude,
           d.dev_stat,
           c.country_name
    FROM mrds_commodity mc
    JOIN mrds_deposit d ON mc.dep_id = d.dep_id
    JOIN mrds_location l ON d.dep_id = l.dep_id
    JOIN dim_country c ON l.country_id = c.country_id
    WHERE LOWER(mc.commod) = LOWER(%s)
"""

SQL_TOP_COUNTRIES = """
    SELECT c.country_name,
           COUNT(d.dep_id) AS total_deposits
    FROM dim_country c
    JOIN mrds_location l ON c.country_id = l.country_id
    JOIN mrds_deposit d ON l.dep_id = d.dep_id
    GROUP BY c.country_name
    ORDER BY total_deposits DESC
    LIMIT %s
"""

SQL_COUNTRY_SUMMARY = """
    SELECT c.country_name,
           COUNT(DISTINCT d.dep_id) AS total_deposits,
           MAX(CASE WHEN ci.indicator_code = %s THEN ci.value END) AS gdp,
           MAX(CASE WHEN ci.indicator_code = %s THEN ci.value END) AS cpi,
           MAX(CASE WHEN ci.indicator_code = %s THEN ci.value END) AS fsi
    FROM dim_country c
    LEFT JOIN mrds_location l ON c.country_id = l.country_id
    LEFT JOIN mrds_deposit d ON l.dep_id = d.dep_id
    LEFT JOIN country_indicator ci ON c.country_id = ci.country_id
    WHERE c.iso3 = %s
    GROUP BY c.country_name
"""

SQL_TOP_MINERALS = """
    SELECT mc.commod,
           COUNT(*) AS occurrences
    FROM mrds_commodity mc
    GROUP BY mc.commod
    ORDER BY occurrences DESC
    LIMIT %s
"""

SQL_DEPOSIT_DETAIL = """
    SELECT d.name,
           d.dev_stat,
           STRING_AGG(DISTINCT mc.commod, ', ') AS commodities,
           STRING_AGG(DISTINCT r.first_ord_nm, ', ') AS rock_types,
           a.age_young,
           o.owner_name
    FROM mrds_deposit d
    LEFT JOIN mrds_commodity mc ON d.dep_id = mc.dep_id
    LEFT JOIN mrds_rocks r ON d.dep_id = r.dep_id
    LEFT JOIN mrds_ages a ON d.dep_id = a.dep_id
    LEFT JOIN mrds_ownership o ON d.dep_id = o.dep_id
    WHERE d.dep_id = %s
    GROUP BY d.name, d.dev_stat, a.age_young, o.owner_name
"""

SQL_MINING_VS_CPI = """
    SELECT c.country_name,
           COUNT(d.dep_id) AS deposits,
           MAX(CASE WHEN ci.indicator_code = %s THEN ci.value END) AS cpi_score
    FROM dim_country c
    LEFT JOIN mrds_location l ON c.country_id = l.country_id
    LEFT JOIN mrds_deposit d ON l.dep_id = d.dep_id
    LEFT JOIN country_indicator ci ON c.country_id = ci.country_id
    GROUP BY c.country_name
    ORDER BY deposits DESC
    LIMIT %s
"""


def main() -> None:
    """Render the Streamlit UI."""
    st.set_page_config(page_title="TFM Data Explorer", layout="wide")
    st.title("TFM — Data Relationship Explorer (Local)")
    st.caption("Local, PostgreSQL-backed. Built to validate relationships across datasets.")

    st.sidebar.header("Navigation")
    page = st.sidebar.radio(
        "Page",
        ["Data Explorer", "SQL Queries (SGBD)"],
    )

    if page == "SQL Queries (SGBD)":
        st.subheader("Representative SQL Queries")
        st.caption("Queries aligned with the relational schema in PostgreSQL.")

        example_iso3 = ["ESP", "CRI", "MEX"]
        tabs = st.tabs(
            [
                "1) World view",
                "2) Mineral filter",
                "3) Top countries",
                "4) Country summary",
                "5) Top minerals",
                "6) Deposit detail",
                "7) Mining vs CPI",
            ]
        )

        with tabs[0]:
            map_limit = st.number_input(
                "World view limit (optional)",
                min_value=0,
                value=5000,
                step=1000,
                key="world_limit",
            )
            with st.expander("Show SQL", expanded=False):
                display_sql = SQL_WORLD_VIEW.strip()
                if map_limit:
                    display_sql += "\nLIMIT %s"
                st.code(display_sql, language="sql")
            with get_connection() as conn:
                sql = SQL_WORLD_VIEW
                params = None
                if map_limit:
                    sql += " LIMIT %s"
                    params = (int(map_limit),)
                world_df = pd.read_sql_query(sql, conn, params=params)
            st.dataframe(world_df.fillna("N/A"), use_container_width=True)

        with tabs[1]:
            mineral_examples = _fetch_example_minerals() or ["Gold", "Copper", "Silver"]
            # Keep the example selector and text input in sync.
            if "mineral_input" not in st.session_state:
                st.session_state["mineral_input"] = mineral_examples[0]

            def _sync_mineral() -> None:
                st.session_state["mineral_input"] = st.session_state["ex_mineral"]

            st.selectbox(
                "Example minerals",
                mineral_examples,
                index=0,
                key="ex_mineral",
                on_change=_sync_mineral,
            )
            mineral = st.text_input("Mineral (commodity) filter", key="mineral_input")
            with st.expander("Show SQL", expanded=False):
                st.code(SQL_FILTER_MINERAL.strip(), language="sql")
            with get_connection() as conn:
                mineral_df = pd.read_sql_query(SQL_FILTER_MINERAL, conn, params=(mineral,))
            st.dataframe(mineral_df.fillna("N/A"), use_container_width=True)

        with tabs[2]:
            top_n = st.number_input(
                "Top N countries",
                min_value=1,
                value=10,
                key="top_countries_n",
            )
            with st.expander("Show SQL", expanded=False):
                st.code(SQL_TOP_COUNTRIES.strip(), language="sql")
            with get_connection() as conn:
                top_countries = pd.read_sql_query(SQL_TOP_COUNTRIES, conn, params=(int(top_n),))
            st.dataframe(top_countries.fillna("N/A"), use_container_width=True)

        with tabs[3]:
            # Keep the example selector and text input in sync.
            if "iso3_input" not in st.session_state:
                st.session_state["iso3_input"] = example_iso3[0]

            def _sync_iso3() -> None:
                st.session_state["iso3_input"] = st.session_state["ex_iso3"]

            st.selectbox(
                "Example ISO3",
                example_iso3,
                index=0,
                key="ex_iso3",
                on_change=_sync_iso3,
            )
            iso3 = st.text_input("Country ISO3 (summary)", key="iso3_input")
            with st.expander("Show SQL", expanded=False):
                st.code(SQL_COUNTRY_SUMMARY.strip(), language="sql")
            with get_connection() as conn:
                summary_df = pd.read_sql_query(
                    SQL_COUNTRY_SUMMARY,
                    conn,
                    params=(
                        INDICATOR_CODES["worldbank_gdp"],
                        INDICATOR_CODES["cpi"],
                        INDICATOR_CODES["fsi"],
                        iso3.upper(),
                    ),
                )
            st.dataframe(summary_df.fillna("N/A"), use_container_width=True)

        with tabs[4]:
            top_minerals_n = st.number_input(
                "Top N minerals",
                min_value=1,
                value=10,
                key="top_minerals_n",
            )
            with st.expander("Show SQL", expanded=False):
                st.code(SQL_TOP_MINERALS.strip(), language="sql")
            with get_connection() as conn:
                top_minerals = pd.read_sql_query(
                    SQL_TOP_MINERALS, conn, params=(int(top_minerals_n),)
                )
            st.dataframe(top_minerals.fillna("N/A"), use_container_width=True)

        with tabs[5]:
            dep_examples = _fetch_example_dep_ids() or [1, 2, 3]
            # Keep the example selector and number input in sync.
            if "dep_id_input" not in st.session_state:
                st.session_state["dep_id_input"] = dep_examples[0]

            def _sync_dep_id() -> None:
                st.session_state["dep_id_input"] = st.session_state["ex_dep_id"]

            st.selectbox(
                "Example dep_id",
                dep_examples,
                index=0,
                key="ex_dep_id",
                on_change=_sync_dep_id,
            )
            dep_id = st.number_input(
                "Deposit ID (detail)",
                min_value=1,
                step=1,
                key="dep_id_input",
            )
            with st.expander("Show SQL", expanded=False):
                st.code(SQL_DEPOSIT_DETAIL.strip(), language="sql")
            with get_connection() as conn:
                detail_df = pd.read_sql_query(SQL_DEPOSIT_DETAIL, conn, params=(int(dep_id),))
            st.dataframe(detail_df.fillna("N/A"), use_container_width=True)

        with tabs[6]:
            top_cpi_n = st.number_input(
                "Top N countries (mining vs CPI)",
                min_value=1,
                value=10,
                key="top_cpi_n",
            )
            with st.expander("Show SQL", expanded=False):
                st.code(SQL_MINING_VS_CPI.strip(), language="sql")
            with get_connection() as conn:
                mining_vs_cpi = pd.read_sql_query(
                    SQL_MINING_VS_CPI, conn, params=(INDICATOR_CODES["cpi"], int(top_cpi_n))
                )
            st.dataframe(mining_vs_cpi.fillna("N/A"), use_container_width=True)
        return

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
