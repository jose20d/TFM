"""Internal visit metrics service."""

from web.db import get_connection
from web.services.common.query_service import fetch_all
from web.services.common.query_service import fetch_one


def _execute(sql: str, params: tuple | None = None) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
        conn.commit()


def _ensure_table() -> None:
    _execute(
        """
        CREATE TABLE IF NOT EXISTS internal_visit_events (
            event_id BIGSERIAL PRIMARY KEY,
            visited_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            route_path TEXT NOT NULL,
            referer TEXT,
            user_agent TEXT,
            source_ip TEXT
        );
        """
    )
    _execute(
        """
        CREATE INDEX IF NOT EXISTS idx_internal_visit_events_visited_at
            ON internal_visit_events (visited_at DESC);
        """
    )
    _execute(
        """
        CREATE INDEX IF NOT EXISTS idx_internal_visit_events_route_path
            ON internal_visit_events (route_path);
        """
    )


def register_visit(route_path: str, referer: str | None, user_agent: str | None, source_ip: str | None) -> None:
    _ensure_table()
    _execute(
        """
        INSERT INTO internal_visit_events (route_path, referer, user_agent, source_ip)
        VALUES (%s, %s, %s, %s)
        """,
        (
            (route_path or "/")[:500],
            (referer or "")[:1000] or None,
            (user_agent or "")[:1000] or None,
            (source_ip or "")[:100] or None,
        ),
    )


def get_visit_summary(days: int = 30) -> dict:
    _ensure_table()
    safe_days = max(1, min(int(days or 30), 365))

    totals = fetch_one(
        """
        SELECT
            COUNT(*)::bigint AS total_visits,
            COUNT(*) FILTER (WHERE visited_at >= NOW() - INTERVAL '24 hours')::bigint AS last_24h,
            COUNT(*) FILTER (WHERE visited_at >= NOW() - INTERVAL '7 days')::bigint AS last_7d,
            COUNT(*) FILTER (WHERE visited_at >= NOW() - INTERVAL '30 days')::bigint AS last_30d
        FROM internal_visit_events
        """
    )

    daily = fetch_all(
        """
        SELECT
            TO_CHAR(DATE_TRUNC('day', visited_at), 'YYYY-MM-DD') AS day,
            COUNT(*)::bigint AS visits
        FROM internal_visit_events
        WHERE visited_at >= NOW() - (%s * INTERVAL '1 day')
        GROUP BY 1
        ORDER BY 1 DESC
        """,
        (safe_days,),
    )

    top_paths = fetch_all(
        """
        SELECT
            route_path AS path,
            COUNT(*)::bigint AS visits
        FROM internal_visit_events
        WHERE visited_at >= NOW() - (%s * INTERVAL '1 day')
        GROUP BY route_path
        ORDER BY visits DESC, route_path ASC
        LIMIT 20
        """,
        (safe_days,),
    )

    return {
        "window_days": safe_days,
        "totals": {
            "total_visits": int(totals.get("total_visits") or 0),
            "last_24h": int(totals.get("last_24h") or 0),
            "last_7d": int(totals.get("last_7d") or 0),
            "last_30d": int(totals.get("last_30d") or 0),
        },
        "daily": [
            {
                "day": row.get("day"),
                "visits": int(row.get("visits") or 0),
            }
            for row in daily
        ],
        "top_paths": [
            {
                "path": row.get("path") or "/",
                "visits": int(row.get("visits") or 0),
            }
            for row in top_paths
        ],
    }
