from __future__ import annotations

from pathlib import Path

from src.db import get_connection


def _read_sql(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def initialize_schema() -> None:
    """
    Initialize database schema and indexes in an idempotent way.

    PostGIS is enabled by the schema script to support spatial features
    even if the first iterations only use basic geometry fields.
    """
    repo_root = Path(__file__).resolve().parents[1]
    schema_path = repo_root / "database" / "create_schema.sql"
    indexes_path = repo_root / "database" / "indexes.sql"

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(_read_sql(schema_path))
            cur.execute(_read_sql(indexes_path))
        conn.commit()


if __name__ == "__main__":
    initialize_schema()
