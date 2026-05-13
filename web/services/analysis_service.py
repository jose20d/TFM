"""Analysis service functions."""

from web.services.common.i18n_service import localize_payload
from web.services.common.query_service import fetch_all


def country_overview(lang: str) -> list[dict]:
    sql = """
        WITH country_bucket AS (
            SELECT DISTINCT ON (c.iso3)
                   c.iso3,
                   c.country_name
            FROM dim_country c
            WHERE c.iso3 IS NOT NULL
              AND TRIM(c.iso3) <> ''
            ORDER BY c.iso3, LENGTH(c.country_name) DESC, c.country_name
        ),
        deposits AS (
            SELECT c.iso3,
                   COUNT(*)::int AS total_deposits
            FROM mrds_location ml
            JOIN dim_country c ON c.country_id = ml.country_id
            WHERE c.iso3 IS NOT NULL
              AND TRIM(c.iso3) <> ''
            GROUP BY c.iso3
        ),
        latest AS (
            SELECT DISTINCT ON (c.iso3, ci.indicator_code)
                   c.iso3,
                   ci.indicator_code,
                   ci.value
            FROM country_indicator ci
            JOIN dim_country c ON c.country_id = ci.country_id
            WHERE ci.indicator_code IN ('NY.GDP.MKTP.CD', 'CPI', 'RANK')
            ORDER BY c.iso3, ci.indicator_code, ci.year DESC
        )
        SELECT cb.country_name,
               cb.iso3,
               COALESCE(d.total_deposits, 0) AS total_deposits,
               (SELECT value FROM latest WHERE iso3 = cb.iso3 AND indicator_code = 'NY.GDP.MKTP.CD') AS gdp,
               (SELECT value FROM latest WHERE iso3 = cb.iso3 AND indicator_code = 'CPI') AS cpi,
               (SELECT value FROM latest WHERE iso3 = cb.iso3 AND indicator_code = 'RANK') AS fsi
        FROM country_bucket cb
        LEFT JOIN deposits d ON d.iso3 = cb.iso3
        ORDER BY cb.country_name
    """
    return localize_payload(fetch_all(sql), lang)

