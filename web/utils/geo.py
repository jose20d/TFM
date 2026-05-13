"""Geo helpers."""

import json


def safe_geojson(value: str | None) -> dict:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    except json.JSONDecodeError:
        return {}

__all__ = ["safe_geojson"]

