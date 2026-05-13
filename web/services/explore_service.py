"""Explore service functions."""

from fastapi import HTTPException

from web.services.common.explore_limits_service import get_explore_max_limit
from web.services.common.i18n_service import localize_payload
from web.services.common.i18n_service import resolve_source_term
from web.services.common.query_service import fetch_all
from web.services.common.query_service import fetch_one


def deposits_map(limit: int, lang: str) -> list[dict]:
    sql = """
        SELECT d.dep_id,
               d.name,
               d.latitude,
               d.longitude,
               d.dev_stat,
               c.country_name,
               c.iso3
        FROM mrds_deposit d
        JOIN mrds_location l ON l.dep_id = d.dep_id
        JOIN dim_country c ON c.country_id = l.country_id
        WHERE d.latitude IS NOT NULL AND d.longitude IS NOT NULL
        ORDER BY d.dep_id
        LIMIT %s
    """
    return localize_payload(fetch_all(sql, (limit,)), lang)


def explore_deposits(
    country_iso3: str | None,
    mineral: str | None,
    limit: int,
    offset: int,
    lang: str,
) -> list[dict]:
    iso3 = (country_iso3 or "").strip().upper()
    if not iso3:
        return []
    mineral_q = (resolve_source_term("mineral", mineral) or "").strip()
    effective_limit = min(limit, get_explore_max_limit(iso3))
    sql = """
        SELECT d.dep_id,
               d.name,
               d.latitude,
               d.longitude,
               d.dev_stat,
               COALESCE(c.country_name, 'N/A') AS country_name,
               COALESCE(c.iso3, 'N/A') AS iso3,
               STRING_AGG(DISTINCT mc.commod, ', ') AS minerals
        FROM mrds_deposit d
        LEFT JOIN mrds_location l ON l.dep_id = d.dep_id
        LEFT JOIN dim_country c ON c.country_id = l.country_id
        LEFT JOIN mrds_commodity mc ON mc.dep_id = d.dep_id
        WHERE d.latitude IS NOT NULL
          AND d.longitude IS NOT NULL
          AND (%s = '' OR c.iso3 = %s)
          AND (%s = '' OR EXISTS (
                SELECT 1
                FROM mrds_commodity x
                WHERE x.dep_id = d.dep_id
                  AND LOWER(x.commod) LIKE LOWER(%s)
          ))
        GROUP BY d.dep_id, d.name, d.latitude, d.longitude, d.dev_stat, c.country_name, c.iso3
        ORDER BY LOWER(COALESCE(c.country_name, 'N/A')),
                 LOWER(COALESCE(d.name, '')),
                 d.dep_id
        LIMIT %s
        OFFSET %s
    """
    like_mineral = f"%{mineral_q}%"
    return localize_payload(fetch_all(sql, (iso3, iso3, mineral_q, like_mineral, effective_limit, offset)), lang)


def explore_limits(country_iso3: str | None) -> dict:
    iso3 = (country_iso3 or "").strip().upper()
    max_limit = get_explore_max_limit(iso3 if len(iso3) == 3 else None)
    return {"default_limit": 500, "max_limit": max_limit}


def explore_deposits_count(country_iso3: str | None, mineral: str | None) -> dict:
    iso3 = (country_iso3 or "").strip().upper()
    if not iso3:
        return {"total": 0}
    mineral_q = (resolve_source_term("mineral", mineral) or "").strip()
    like_mineral = f"%{mineral_q}%"
    sql = """
        SELECT COUNT(DISTINCT d.dep_id) AS total
        FROM mrds_deposit d
        LEFT JOIN mrds_location l ON l.dep_id = d.dep_id
        LEFT JOIN dim_country c ON c.country_id = l.country_id
        WHERE d.latitude IS NOT NULL
          AND d.longitude IS NOT NULL
          AND (%s = '' OR c.iso3 = %s)
          AND (%s = '' OR EXISTS (
                SELECT 1
                FROM mrds_commodity x
                WHERE x.dep_id = d.dep_id
                  AND LOWER(x.commod) LIKE LOWER(%s)
          ))
    """
    row = fetch_one(sql, (iso3, iso3, mineral_q, like_mineral))
    return {"total": int(row.get("total") or 0)}


def country_summary(iso3: str, lang: str) -> dict:
    normalized_iso3 = iso3.upper().strip()
    sql = """
        WITH target_country AS (
            SELECT country_id, country_name, iso3
            FROM dim_country
            WHERE iso3 = %s
            LIMIT 1
        ),
        latest AS (
            SELECT DISTINCT ON (ci.country_id, ci.indicator_code)
                   ci.country_id,
                   ci.indicator_code,
                   ci.value
            FROM country_indicator ci
            JOIN target_country tc ON tc.country_id = ci.country_id
            WHERE ci.indicator_code IN ('NY.GDP.MKTP.CD', 'CPI', 'RANK')
            ORDER BY ci.country_id, ci.indicator_code, ci.year DESC
        ),
        top_minerals AS (
            SELECT mc.commod
            FROM mrds_commodity mc
            JOIN mrds_location ml ON ml.dep_id = mc.dep_id
            JOIN target_country tc ON tc.country_id = ml.country_id
            WHERE mc.commod IS NOT NULL AND TRIM(mc.commod) <> ''
            GROUP BY mc.commod
            ORDER BY COUNT(*) DESC
            LIMIT 3
        )
        SELECT
            tc.country_name,
            tc.iso3,
            (SELECT COUNT(*) FROM mrds_location ml WHERE ml.country_id = tc.country_id) AS deposits_count,
            (SELECT value FROM latest WHERE indicator_code = 'NY.GDP.MKTP.CD') AS gdp,
            (SELECT value FROM latest WHERE indicator_code = 'CPI') AS cpi,
            (SELECT value FROM latest WHERE indicator_code = 'RANK') AS fsi,
            (SELECT ARRAY(SELECT commod FROM top_minerals)) AS top_minerals
        FROM target_country tc;
    """
    row = fetch_one(sql, (normalized_iso3,))
    if not row:
        raise HTTPException(status_code=404, detail=f"Country with ISO3 '{normalized_iso3}' not found")
    row["top_minerals"] = row.get("top_minerals") or []
    return localize_payload(row, lang)

