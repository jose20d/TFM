from __future__ import annotations

import os
from typing import Any

import psycopg2
from psycopg2.extensions import connection as PgConnection


def _get_env(name: str, default: str | None = None) -> str | None:
    # Environment variables keep credentials out of source control.
    value = os.getenv(name, default)
    if value is None or value == "":
        return default
    return value


def get_connection() -> PgConnection:
    """
    Build a PostgreSQL connection using environment variables.

    Required:
    - DB_HOST
    - DB_PORT
    - DB_NAME
    - DB_USER
    - DB_PASSWORD

    Optional:
    - DB_SSLMODE
    """
    host = _get_env("DB_HOST")
    port = _get_env("DB_PORT")
    name = _get_env("DB_NAME")
    user = _get_env("DB_USER")
    password = _get_env("DB_PASSWORD")
    sslmode = _get_env("DB_SSLMODE")

    if not all([host, port, name, user, password]):
        missing = [k for k, v in {
            "DB_HOST": host,
            "DB_PORT": port,
            "DB_NAME": name,
            "DB_USER": user,
            "DB_PASSWORD": password,
        }.items() if not v]
        raise RuntimeError(f"Missing database environment variables: {', '.join(missing)}")

    params: dict[str, Any] = {
        "host": host,
        "port": int(port),
        "dbname": name,
        "user": user,
        "password": password,
    }
    if sslmode:
        params["sslmode"] = sslmode

    return psycopg2.connect(**params)
