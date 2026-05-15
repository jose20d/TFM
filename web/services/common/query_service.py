"""Shared DB query helpers."""

from web.db import get_connection


def fetch_one(sql: str, params: tuple | None = None) -> dict:
    """Execute a query and return one row as dictionary."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            row = cur.fetchone()
            if row is None:
                return {}
            columns = [desc[0] for desc in cur.description]
            return dict(zip(columns, row))


def fetch_all(sql: str, params: tuple | None = None) -> list[dict]:
    """Execute a query and return rows as dictionaries."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in rows]

