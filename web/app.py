from __future__ import annotations

"""FastAPI application for the production-oriented web experience."""

from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from src.db import get_connection


APP_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(APP_DIR / "templates"))

app = FastAPI(
    title="TFM GeoContext API",
    version="1.0.0",
    description="API and web layer for mineral and socioeconomic insights.",
)
app.mount("/static", StaticFiles(directory=str(APP_DIR / "static")), name="static")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:3000",
        "http://localhost:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _fetch_one(sql: str, params: tuple | None = None) -> dict:
    """Execute a query and return one row as dictionary."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            row = cur.fetchone()
            if row is None:
                return {}
            columns = [desc[0] for desc in cur.description]
            return dict(zip(columns, row))


def _fetch_all(sql: str, params: tuple | None = None) -> list[dict]:
    """Execute a query and return rows as dictionaries."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in rows]


@app.get("/", response_class=HTMLResponse)
def web_index(request: Request) -> HTMLResponse:
    """Render the first dashboard page."""
    return templates.TemplateResponse(request, "index.html")


@app.get("/api/v1/health")
def api_health() -> dict:
    """Simple health endpoint."""
    row = _fetch_one("SELECT 1 AS ok")
    return {"status": "ok", "db": bool(row.get("ok") == 1)}


@app.get("/api/v1/countries")
def api_countries(
    q: str | None = Query(default=None, min_length=1),
    limit: int = Query(default=300, ge=1, le=500),
) -> list[dict]:
    """List countries for selectors (name + ISO3 + ISO2)."""
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
    return _fetch_all(sql, (q or "", like_q, like_q, like_q, limit))


@app.get("/api/v1/home/defaults")
def api_home_defaults() -> dict:
    """Return DB-driven defaults for initial country selections."""
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
    row = _fetch_one(sql)
    return {
        "default_iso3": row.get("default_iso3"),
        "compare_iso3": row.get("compare_iso3") or [],
    }


@app.get("/api/v1/overview")
def api_overview() -> dict:
    """Return global KPIs for the dashboard header."""
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
    return _fetch_one(sql)


@app.get("/api/v1/top-countries")
def api_top_countries(limit: int = Query(default=5, ge=1, le=25)) -> list[dict]:
    """Return countries sorted by number of mineral deposits."""
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
    return _fetch_all(sql, (limit,))


@app.get("/api/v1/top-minerals")
def api_top_minerals(limit: int = Query(default=5, ge=1, le=25)) -> list[dict]:
    """Return most common minerals across deposits."""
    sql = """
        SELECT mc.commod,
               COUNT(*) AS occurrences
        FROM mrds_commodity mc
        WHERE mc.commod IS NOT NULL AND TRIM(mc.commod) <> ''
        GROUP BY mc.commod
        ORDER BY occurrences DESC
        LIMIT %s
    """
    return _fetch_all(sql, (limit,))


@app.get("/api/v1/deposits/map")
def api_deposits_map(limit: int = Query(default=2000, ge=50, le=10000)) -> list[dict]:
    """Return map-friendly deposit points."""
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
    return _fetch_all(sql, (limit,))


@app.get("/api/v1/explore/deposits")
def api_explore_deposits(
    country_iso3: str | None = Query(default=None),
    mineral: str | None = Query(default=None),
    limit: int = Query(default=2000, ge=50, le=10000),
) -> list[dict]:
    """Return map points using dynamic country/mineral filters."""
    iso3 = (country_iso3 or "").strip().upper()
    mineral_q = (mineral or "").strip()
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
    """
    like_mineral = f"%{mineral_q}%"
    return _fetch_all(sql, (iso3, iso3, mineral_q, like_mineral, limit))


@app.get("/api/v1/countries/{iso3}/summary")
def api_country_summary(iso3: str) -> dict:
    """Return a country profile for dashboard detail cards."""
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
    row = _fetch_one(sql, (normalized_iso3,))
    if not row:
        raise HTTPException(status_code=404, detail=f"Country with ISO3 '{normalized_iso3}' not found")
    row["top_minerals"] = row.get("top_minerals") or []
    return row


@app.get("/api/v1/analysis/country-overview")
def api_analysis_country_overview() -> list[dict]:
    """Return country-level metrics for global analysis charts."""
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
    return _fetch_all(sql)


@app.get("/api/v1/countries/compare")
def api_countries_compare(iso3: list[str] = Query(default=["CRI", "CHL", "PER"])) -> list[dict]:
    """Return comparable metrics for a small set of countries."""
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
    return _fetch_all(sql, (normalized,))
