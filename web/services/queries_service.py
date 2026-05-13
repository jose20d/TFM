"""Query service functions."""

from fastapi import HTTPException

from web.db import get_connection
from web.services.common.i18n_service import localize_payload
from web.services.common.i18n_service import resolve_source_term
from web.services.common.query_service import fetch_all
from web.services.common.query_service import fetch_one


def deposits_by_mineral(
    country_iso3: str | None,
    mineral: str,
    deposit_status: str | None,
    min_minerals: int,
    limit: int,
    lang: str,
) -> dict:
    iso3 = (country_iso3 or "").strip().upper()
    mineral_q = (resolve_source_term("mineral", mineral) or "").strip()
    status_q = (deposit_status or "").strip()

    sql = """
        SELECT
            COALESCE(d.name, CONCAT('Deposito ', d.dep_id::text)) AS deposit_name,
            COALESCE(c.country_name, 'N/A') AS country_name,
            COALESCE(c.iso3, 'N/A') AS iso3,
            COALESCE(NULLIF(TRIM(d.dev_stat), ''), 'N/A') AS deposit_status,
            COALESCE(
                ARRAY_AGG(DISTINCT TRIM(mc.commod))
                FILTER (WHERE mc.commod IS NOT NULL AND TRIM(mc.commod) <> ''),
                ARRAY[]::text[]
            ) AS minerals,
            COUNT(DISTINCT TRIM(mc.commod))
                FILTER (WHERE mc.commod IS NOT NULL AND TRIM(mc.commod) <> '')::int AS minerals_count,
            d.latitude,
            d.longitude
        FROM mrds_deposit d
        LEFT JOIN mrds_location l ON l.dep_id = d.dep_id
        LEFT JOIN dim_country c ON c.country_id = l.country_id
        LEFT JOIN mrds_commodity mc ON mc.dep_id = d.dep_id
        WHERE (%s = '' OR c.iso3 = %s)
          AND (%s = '' OR LOWER(COALESCE(d.dev_stat, '')) LIKE LOWER('%%' || %s || '%%'))
          AND (
            %s = '' OR EXISTS (
                SELECT 1
                FROM mrds_commodity m2
                WHERE m2.dep_id = d.dep_id
                  AND m2.commod IS NOT NULL
                  AND TRIM(m2.commod) <> ''
                  AND LOWER(TRIM(m2.commod)) LIKE LOWER('%%' || %s || '%%')
            )
          )
        GROUP BY d.dep_id, d.name, c.country_name, c.iso3, d.dev_stat, d.latitude, d.longitude
        HAVING COUNT(DISTINCT TRIM(mc.commod))
                FILTER (WHERE mc.commod IS NOT NULL AND TRIM(mc.commod) <> '') >= %s
        ORDER BY COALESCE(c.country_name, 'N/A'), COALESCE(d.name, '')
        LIMIT %s
    """
    rows = fetch_all(
        sql,
        (iso3, iso3, status_q, status_q, mineral_q, mineral_q, min_minerals, limit),
    )

    results = []
    for row in rows:
        minerals = sorted(list(row.get("minerals") or []))
        results.append(
            {
                "deposit": row.get("deposit_name") or "N/A",
                "country": row.get("country_name") or "N/A",
                "iso3": row.get("iso3") or "N/A",
                "status": row.get("deposit_status") or "N/A",
                "minerals": minerals,
                "minerals_count": int(row.get("minerals_count") or 0),
                "lat": float(row["latitude"]) if row.get("latitude") is not None else None,
                "lng": float(row["longitude"]) if row.get("longitude") is not None else None,
            }
        )

    return localize_payload({
        "mode": "deposits_by_mineral",
        "result_count": len(results),
        "summary": (
            f"Se encontraron {len(results)} depositos que cumplen los criterios seleccionados."
            if results
            else "No se encontraron registros para los criterios seleccionados."
        ),
        "rows": results,
    }, lang)


def combined_minerals(
    country_iso3: str | None,
    mineral_a: str,
    mineral_b: str,
    exclude_mineral: str | None,
    limit: int,
    lang: str,
) -> dict:
    iso3 = (country_iso3 or "").strip().upper()
    mineral_a_q = (resolve_source_term("mineral", mineral_a) or "").strip()
    mineral_b_q = (resolve_source_term("mineral", mineral_b) or "").strip()
    exclude_q = (resolve_source_term("mineral", exclude_mineral) or "").strip()
    if not mineral_a_q or not mineral_b_q:
        raise HTTPException(status_code=400, detail="mineral_a and mineral_b are required.")

    sql = """
        SELECT
            COALESCE(d.name, CONCAT('Deposito ', d.dep_id::text)) AS deposit_name,
            COALESCE(c.country_name, 'N/A') AS country_name,
            COALESCE(c.iso3, 'N/A') AS iso3,
            COALESCE(
                ARRAY_AGG(DISTINCT TRIM(mc.commod))
                FILTER (WHERE mc.commod IS NOT NULL AND TRIM(mc.commod) <> ''),
                ARRAY[]::text[]
            ) AS minerals
        FROM mrds_deposit d
        JOIN mrds_location l ON l.dep_id = d.dep_id
        JOIN dim_country c ON c.country_id = l.country_id
        LEFT JOIN mrds_commodity mc ON mc.dep_id = d.dep_id
        WHERE (%s = '' OR c.iso3 = %s)
          AND EXISTS (
            SELECT 1
            FROM mrds_commodity a
            WHERE a.dep_id = d.dep_id
              AND a.commod IS NOT NULL
              AND TRIM(a.commod) <> ''
              AND LOWER(TRIM(a.commod)) LIKE LOWER('%%' || %s || '%%')
          )
          AND EXISTS (
            SELECT 1
            FROM mrds_commodity b
            WHERE b.dep_id = d.dep_id
              AND b.commod IS NOT NULL
              AND TRIM(b.commod) <> ''
              AND LOWER(TRIM(b.commod)) LIKE LOWER('%%' || %s || '%%')
          )
          AND (
            %s = '' OR NOT EXISTS (
                SELECT 1
                FROM mrds_commodity ex
                WHERE ex.dep_id = d.dep_id
                  AND ex.commod IS NOT NULL
                  AND TRIM(ex.commod) <> ''
                  AND LOWER(TRIM(ex.commod)) LIKE LOWER('%%' || %s || '%%')
            )
          )
        GROUP BY d.dep_id, d.name, c.country_name, c.iso3
        ORDER BY COALESCE(c.country_name, 'N/A'), COALESCE(d.name, '')
        LIMIT %s
    """
    rows = fetch_all(
        sql,
        (iso3, iso3, mineral_a_q, mineral_b_q, exclude_q, exclude_q, limit),
    )

    total = len(rows)
    results = [
        {
            "deposit": row.get("deposit_name") or "N/A",
            "country": row.get("country_name") or "N/A",
            "iso3": row.get("iso3") or "N/A",
            "minerals": sorted(list(row.get("minerals") or [])),
        }
        for row in rows
    ]

    return localize_payload({
        "mode": "combined_minerals",
        "result_count": total,
        "summary": (
            f"Se encontraron {total} depositos donde {mineral_a_q} y {mineral_b_q} aparecen juntos."
            if total
            else "No se encontraron registros para los criterios seleccionados."
        ),
        "coexistence": {
            "mineral_a": mineral_a_q,
            "mineral_b": mineral_b_q,
            "excluded": exclude_q or None,
        },
        "rows": results,
    }, lang)


def spatial_nearby(
    country_iso3: str,
    base_dep_id: int,
    radius_km: float,
    mineral: str | None,
    limit: int,
    lang: str,
) -> dict:
    iso3 = (country_iso3 or "").strip().upper()
    mineral_q = (resolve_source_term("mineral", mineral) or "").strip()
    radius_m = float(radius_km) * 1000.0

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    d.dep_id,
                    COALESCE(d.name, CONCAT('Deposito ', d.dep_id::text)) AS name,
                    d.latitude,
                    d.longitude,
                    c.country_name
                FROM mrds_deposit d
                JOIN mrds_location l ON l.dep_id = d.dep_id
                JOIN dim_country c ON c.country_id = l.country_id
                WHERE c.iso3 = %s
                  AND d.dep_id = %s
                  AND d.latitude IS NOT NULL
                  AND d.longitude IS NOT NULL
                LIMIT 1
                """,
                (iso3, base_dep_id),
            )
            base_row = cur.fetchone()
            if base_row is None:
                raise HTTPException(
                    status_code=404,
                    detail="El deposito base no existe en el pais seleccionado o no tiene coordenadas validas.",
                )
            base_dep = {
                "name": base_row[1],
                "lat": float(base_row[2]),
                "lng": float(base_row[3]),
                "country": base_row[4],
            }

            cur.execute(
                """
                WITH base AS (
                    SELECT ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography AS base_geog
                )
                SELECT
                    COALESCE(d.name, CONCAT('Deposito ', d.dep_id::text)) AS deposit_name,
                    COALESCE(c.country_name, 'N/A') AS country_name,
                    ROUND(
                        (ST_Distance(
                            ST_SetSRID(ST_MakePoint(d.longitude, d.latitude), 4326)::geography,
                            b.base_geog
                        ) / 1000.0)::numeric,
                        3
                    ) AS distance_km,
                    COALESCE(
                        ARRAY_AGG(DISTINCT TRIM(mc.commod))
                        FILTER (WHERE mc.commod IS NOT NULL AND TRIM(mc.commod) <> ''),
                        ARRAY[]::text[]
                    ) AS minerals,
                    d.latitude,
                    d.longitude
                FROM mrds_deposit d
                JOIN mrds_location l ON l.dep_id = d.dep_id
                JOIN dim_country c ON c.country_id = l.country_id
                CROSS JOIN base b
                LEFT JOIN mrds_commodity mc ON mc.dep_id = d.dep_id
                WHERE c.iso3 = %s
                  AND d.dep_id <> %s
                  AND d.latitude IS NOT NULL
                  AND d.longitude IS NOT NULL
                  AND ST_DWithin(
                        ST_SetSRID(ST_MakePoint(d.longitude, d.latitude), 4326)::geography,
                        b.base_geog,
                        %s
                  )
                  AND (
                    %s = '' OR EXISTS (
                        SELECT 1
                        FROM mrds_commodity m2
                        WHERE m2.dep_id = d.dep_id
                          AND m2.commod IS NOT NULL
                          AND TRIM(m2.commod) <> ''
                          AND LOWER(TRIM(m2.commod)) LIKE LOWER('%%' || %s || '%%')
                    )
                  )
                GROUP BY d.dep_id, d.name, c.country_name, d.latitude, d.longitude, b.base_geog
                ORDER BY distance_km ASC, COALESCE(d.name, '')
                LIMIT %s
                """,
                (
                    base_dep["lng"],
                    base_dep["lat"],
                    iso3,
                    base_dep_id,
                    radius_m,
                    mineral_q,
                    mineral_q,
                    limit,
                ),
            )
            nearby_rows = cur.fetchall()

    rows = [
        {
            "deposit": row[0],
            "country": row[1],
            "distance_km": float(row[2] or 0.0),
            "minerals": sorted(list(row[3] or [])),
            "lat": float(row[4]) if row[4] is not None else None,
            "lng": float(row[5]) if row[5] is not None else None,
        }
        for row in nearby_rows
    ]

    return localize_payload({
        "mode": "spatial_nearby",
        "base_deposit": base_dep,
        "radius_km": round(float(radius_km), 2),
        "result_count": len(rows),
        "summary": (
            f"Se encontraron {len(rows)} depositos cercanos al punto base."
            if rows
            else "No se encontraron registros para los criterios seleccionados."
        ),
        "rows": rows,
    }, lang)


def country_profile(
    min_deposits: int,
    gdp_min: float | None,
    gdp_max: float | None,
    cpi_min: float | None,
    cpi_max: float | None,
    fsi_min: float | None,
    fsi_max: float | None,
    limit: int,
    lang: str,
) -> dict:
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
            SELECT c.iso3, COUNT(*)::int AS total_deposits
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
        ),
        overview AS (
            SELECT
                cb.country_name,
                cb.iso3,
                COALESCE(d.total_deposits, 0) AS total_deposits,
                (SELECT value FROM latest WHERE iso3 = cb.iso3 AND indicator_code = 'NY.GDP.MKTP.CD') AS gdp,
                (SELECT value FROM latest WHERE iso3 = cb.iso3 AND indicator_code = 'CPI') AS cpi,
                (SELECT value FROM latest WHERE iso3 = cb.iso3 AND indicator_code = 'RANK') AS fsi
            FROM country_bucket cb
            LEFT JOIN deposits d ON d.iso3 = cb.iso3
        )
        SELECT *
        FROM overview
        WHERE total_deposits >= %s
          AND (%s::numeric IS NULL OR gdp >= %s::numeric)
          AND (%s::numeric IS NULL OR gdp <= %s::numeric)
          AND (%s::numeric IS NULL OR cpi >= %s::numeric)
          AND (%s::numeric IS NULL OR cpi <= %s::numeric)
          AND (%s::numeric IS NULL OR fsi >= %s::numeric)
          AND (%s::numeric IS NULL OR fsi <= %s::numeric)
        ORDER BY total_deposits DESC, country_name
        LIMIT %s
    """
    rows = fetch_all(
        sql,
        (
            min_deposits,
            gdp_min,
            gdp_min,
            gdp_max,
            gdp_max,
            cpi_min,
            cpi_min,
            cpi_max,
            cpi_max,
            fsi_min,
            fsi_min,
            fsi_max,
            fsi_max,
            limit,
        ),
    )

    results = []
    for row in rows:
        deposits = int(row.get("total_deposits") or 0)
        gdp = float(row["gdp"]) if row.get("gdp") is not None else None
        relative_intensity = None
        if gdp and gdp > 0:
            relative_intensity = round(deposits / (gdp / 1_000_000_000), 3)
        results.append(
            {
                "country": row.get("country_name") or "N/A",
                "iso3": row.get("iso3") or "N/A",
                "deposits": deposits,
                "gdp": gdp,
                "cpi": float(row["cpi"]) if row.get("cpi") is not None else None,
                "fsi": float(row["fsi"]) if row.get("fsi") is not None else None,
                "relative_intensity": relative_intensity,
            }
        )

    return localize_payload({
        "mode": "country_profile",
        "result_count": len(results),
        "summary": (
            f"{len(results)} paises cumplen los criterios seleccionados."
            if results
            else "No se encontraron registros para los criterios seleccionados."
        ),
        "rows": results,
    }, lang)


def country_profile_bounds() -> dict:
    sql = """
        WITH country_bucket AS (
            SELECT DISTINCT ON (c.iso3)
                   c.iso3
            FROM dim_country c
            WHERE c.iso3 IS NOT NULL
              AND TRIM(c.iso3) <> ''
            ORDER BY c.iso3
        ),
        deposits AS (
            SELECT c.iso3, COUNT(*)::int AS total_deposits
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
        ),
        overview AS (
            SELECT
                cb.iso3,
                COALESCE(d.total_deposits, 0) AS total_deposits,
                (SELECT value FROM latest WHERE iso3 = cb.iso3 AND indicator_code = 'NY.GDP.MKTP.CD') AS gdp,
                (SELECT value FROM latest WHERE iso3 = cb.iso3 AND indicator_code = 'CPI') AS cpi,
                (SELECT value FROM latest WHERE iso3 = cb.iso3 AND indicator_code = 'RANK') AS fsi
            FROM country_bucket cb
            LEFT JOIN deposits d ON d.iso3 = cb.iso3
        )
        SELECT
            MIN(total_deposits)::int AS deposits_min,
            MAX(total_deposits)::int AS deposits_max,
            MIN(gdp) AS gdp_min,
            MAX(gdp) AS gdp_max,
            MIN(cpi) AS cpi_min,
            MAX(cpi) AS cpi_max,
            MIN(fsi) AS fsi_min,
            MAX(fsi) AS fsi_max
        FROM overview
    """
    row = fetch_one(sql)
    return {
        "deposits_min": int(row.get("deposits_min") or 0),
        "deposits_max": int(row.get("deposits_max") or 0),
        "gdp_min": float(row["gdp_min"]) if row.get("gdp_min") is not None else None,
        "gdp_max": float(row["gdp_max"]) if row.get("gdp_max") is not None else None,
        "cpi_min": float(row["cpi_min"]) if row.get("cpi_min") is not None else None,
        "cpi_max": float(row["cpi_max"]) if row.get("cpi_max") is not None else None,
        "fsi_min": float(row["fsi_min"]) if row.get("fsi_min") is not None else None,
        "fsi_max": float(row["fsi_max"]) if row.get("fsi_max") is not None else None,
    }

