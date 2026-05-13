"""Compare service functions."""

from fastapi import HTTPException

from web.services.common.i18n_service import localize_payload
from web.services.common.query_service import fetch_all


def countries_compare(iso3: list[str], lang: str) -> list[dict]:
    normalized = []
    seen: set[str] = set()
    for item in iso3:
        value = str(item).upper().strip()
        if len(value) != 3 or value in seen:
            continue
        seen.add(value)
        normalized.append(value)

    if len(normalized) < 2:
        raise HTTPException(status_code=400, detail="Provide at least two valid ISO3 values.")
    if len(normalized) > 5:
        normalized = normalized[:5]

    sql = """
        WITH selected AS (
            SELECT s.iso3, s.ord
            FROM unnest(%s::text[]) WITH ORDINALITY AS s(iso3, ord)
        ),
        country_bucket AS (
            SELECT
                s.iso3,
                s.ord,
                (
                    SELECT c2.country_name
                    FROM dim_country c2
                    WHERE c2.iso3 = s.iso3
                    ORDER BY LENGTH(c2.country_name) DESC, c2.country_name
                    LIMIT 1
                ) AS country_name,
                (
                    SELECT i2.iso2
                    FROM iso_country_codes i2
                    WHERE i2.iso3 = s.iso3
                    LIMIT 1
                ) AS iso2
            FROM selected s
            WHERE EXISTS (SELECT 1 FROM dim_country c WHERE c.iso3 = s.iso3)
        ),
        deposits AS (
            SELECT c.iso3, COUNT(*) AS deposits
            FROM mrds_location ml
            JOIN dim_country c ON c.country_id = ml.country_id
            JOIN selected s ON s.iso3 = c.iso3
            GROUP BY c.iso3
        ),
        latest AS (
            SELECT DISTINCT ON (c.iso3, ci.indicator_code)
                   c.iso3,
                   ci.indicator_code,
                   ci.value
            FROM country_indicator ci
            JOIN dim_country c ON c.country_id = ci.country_id
            JOIN selected s ON s.iso3 = c.iso3
            WHERE ci.indicator_code IN ('NY.GDP.MKTP.CD', 'CPI', 'RANK')
            ORDER BY c.iso3, ci.indicator_code, ci.year DESC
        )
        SELECT cb.country_name,
               cb.iso3,
               cb.iso2,
               COALESCE(d.deposits, 0) AS deposits,
               (SELECT value FROM latest WHERE iso3 = cb.iso3 AND indicator_code = 'NY.GDP.MKTP.CD') AS gdp,
               (SELECT value FROM latest WHERE iso3 = cb.iso3 AND indicator_code = 'CPI') AS cpi,
               (SELECT value FROM latest WHERE iso3 = cb.iso3 AND indicator_code = 'RANK') AS fsi
        FROM country_bucket cb
        LEFT JOIN deposits d ON d.iso3 = cb.iso3
        ORDER BY cb.ord
    """
    return localize_payload(fetch_all(sql, (normalized,)), lang)

