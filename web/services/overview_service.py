"""Overview and shared list service functions."""

from pathlib import Path

from fastapi import Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from web.services.common.i18n_service import localize_payload
from web.services.common.i18n_service import sort_key_localized
from web.services.common.query_service import fetch_all
from web.services.common.query_service import fetch_one

APP_DIR = Path(__file__).resolve().parents[1]
templates = Jinja2Templates(directory=str(APP_DIR / "templates"))
EXCLUDED_COUNTRY_KEYS = {"antartica", "antarctica", "antartida", "antarctida"}


def web_index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(request, "index.html")


def health() -> dict:
    row = fetch_one("SELECT 1 AS ok")
    return {"status": "ok", "db": bool(row.get("ok") == 1)}


def countries(q: str | None, limit: int, lang: str) -> list[dict]:
    like_q = f"%{(q or '').strip()}%"
    sql = """
        SELECT c.country_name,
               c.iso3,
               i.iso2
        FROM dim_country c
        LEFT JOIN iso_country_codes i ON i.iso3 = c.iso3
        WHERE (
            %s = '' OR
            LOWER(c.country_name) LIKE LOWER(%s) OR
            LOWER(COALESCE(c.iso3, '')) LIKE LOWER(%s) OR
            LOWER(COALESCE(i.iso2, '')) LIKE LOWER(%s)
        )
        ORDER BY c.country_name
        LIMIT %s
    """
    localized = localize_payload(fetch_all(sql, (q or "", like_q, like_q, like_q, limit)), lang)
    localized = [
        item
        for item in localized
        if sort_key_localized(item.get("country_name")) not in EXCLUDED_COUNTRY_KEYS
    ]
    localized.sort(key=lambda item: sort_key_localized(item.get("country_name")))
    return localized


def home_defaults() -> dict:
    sql = """
        WITH default_country AS (
            SELECT c.iso3
            FROM dim_country c
            WHERE c.iso3 IS NOT NULL
            ORDER BY CASE WHEN LOWER(c.country_name) = 'costa rica' THEN 0 ELSE 1 END,
                     c.country_name
            LIMIT 1
        ),
        top_compare AS (
            SELECT c.iso3
            FROM dim_country c
            JOIN mrds_location l ON l.country_id = c.country_id
            JOIN mrds_deposit d ON d.dep_id = l.dep_id
            WHERE c.iso3 IS NOT NULL
            GROUP BY c.iso3, c.country_name
            ORDER BY CASE WHEN LOWER(c.country_name) = 'costa rica' THEN 0 ELSE 1 END,
                     COUNT(d.dep_id) DESC,
                     c.country_name
            LIMIT 5
        )
        SELECT
            (SELECT iso3 FROM default_country) AS default_iso3,
            (SELECT ARRAY(SELECT iso3 FROM top_compare)) AS compare_iso3
    """
    row = fetch_one(sql)
    return {
        "default_iso3": row.get("default_iso3"),
        "compare_iso3": row.get("compare_iso3") or [],
    }


def overview(lang: str) -> dict:
    sql = """
        WITH latest AS (
            SELECT DISTINCT ON (country_id, indicator_code)
                   country_id,
                   indicator_code,
                   value
            FROM country_indicator
            WHERE indicator_code IN ('CPI', 'RANK')
            ORDER BY country_id, indicator_code, year DESC
        )
        SELECT
            (SELECT COUNT(*) FROM dim_country) AS countries_count,
            (SELECT COUNT(*) FROM mrds_deposit) AS deposits_count,
            (SELECT commod
             FROM mrds_commodity
             WHERE commod IS NOT NULL AND TRIM(commod) <> ''
             GROUP BY commod
             ORDER BY COUNT(*) DESC
             LIMIT 1) AS top_mineral,
            (SELECT ROUND(AVG(value)::numeric, 2) FROM latest WHERE indicator_code = 'CPI') AS avg_cpi,
            (SELECT ROUND(AVG(value)::numeric, 2) FROM latest WHERE indicator_code = 'RANK') AS avg_fsi;
    """
    return localize_payload(fetch_one(sql), lang)


def top_countries(limit: int, lang: str) -> list[dict]:
    sql = """
        SELECT c.country_name,
               c.iso3,
               COUNT(d.dep_id) AS total_deposits
        FROM dim_country c
        JOIN mrds_location l ON l.country_id = c.country_id
        JOIN mrds_deposit d ON d.dep_id = l.dep_id
        GROUP BY c.country_name, c.iso3
        ORDER BY total_deposits DESC
        LIMIT %s
    """
    return localize_payload(fetch_all(sql, (limit,)), lang)


def top_minerals(limit: int, lang: str) -> list[dict]:
    sql = """
        SELECT mc.commod,
               COUNT(*) AS occurrences
        FROM mrds_commodity mc
        WHERE mc.commod IS NOT NULL AND TRIM(mc.commod) <> ''
        GROUP BY mc.commod
        ORDER BY occurrences DESC
        LIMIT %s
    """
    return localize_payload(fetch_all(sql, (limit,)), lang)


def minerals(q: str | None, limit: int, lang: str) -> list[dict]:
    like_q = f"%{(q or '').strip()}%"
    sql = """
        WITH src AS (
            SELECT LOWER(TRIM(mc.commod)) AS commod_norm,
                   MIN(TRIM(mc.commod)) AS commod_source
            FROM mrds_commodity mc
            WHERE mc.commod IS NOT NULL
              AND TRIM(mc.commod) <> ''
            GROUP BY LOWER(TRIM(mc.commod))
        )
        SELECT src.commod_source,
               src.commod_source AS commod
        FROM src
        WHERE (%s = '' OR LOWER(src.commod_source) LIKE LOWER(%s))
        ORDER BY src.commod_source
        LIMIT %s
    """
    localized = localize_payload(fetch_all(sql, (q or "", like_q, limit)), lang)
    localized.sort(key=lambda item: sort_key_localized(item.get("commod")))
    return localized

