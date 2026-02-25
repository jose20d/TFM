from __future__ import annotations

"""Country normalization and filtering helpers."""

import json
import unicodedata
from pathlib import Path
from typing import Any, Iterable


def normalize_country_name(value: str) -> str:
    """Normalize a country name for stable comparisons."""
    text = value.strip().lower()
    text = "".join(
        ch for ch in unicodedata.normalize("NFKD", text) if not unicodedata.combining(ch)
    )
    text = " ".join(text.split())
    return text


def normalize_iso3(value: str) -> str:
    """Normalize an ISO3 code to uppercase."""
    return value.strip().upper()


def load_aliases(path: Path | None) -> dict[str, str]:
    """Load country aliases as a normalized mapping."""
    if path is None:
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        return {}
    out: dict[str, str] = {}
    for k, v in data.items():
        if isinstance(k, str) and isinstance(v, str):
            out[normalize_country_name(k)] = normalize_country_name(v)
    return out


def iter_records(records: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]]:
    """Yield only dictionary records from an iterable."""
    for r in records:
        if isinstance(r, dict):
            yield r


def match_country(
    record: dict[str, Any],
    *,
    country_query: str | None,
    iso_query: str | None,
    country_fields: list[str],
    iso_fields: list[str],
    aliases: dict[str, str],
) -> bool:
    """Return True if a record matches a country or ISO filter."""
    if iso_query:
        iso_target = normalize_iso3(iso_query)
        for f in iso_fields:
            v = record.get(f)
            if isinstance(v, str) and normalize_iso3(v) == iso_target:
                return True
        return False

    if country_query:
        name_target = normalize_country_name(country_query)
        name_target = aliases.get(name_target, name_target)
        for f in country_fields:
            v = record.get(f)
            if isinstance(v, str):
                name = normalize_country_name(v)
                name = aliases.get(name, name)
                if name == name_target:
                    return True
        return False

    return False


def filter_by_country(
    records: Iterable[dict[str, Any]],
    *,
    country: str | None,
    iso3: str | None,
    country_fields: list[str],
    iso_fields: list[str],
    aliases: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    """Filter records by country or ISO3 fields."""
    alias_map = aliases or {}
    out: list[dict[str, Any]] = []
    for r in iter_records(records):
        if match_country(
            r,
            country_query=country,
            iso_query=iso3,
            country_fields=country_fields,
            iso_fields=iso_fields,
            aliases=alias_map,
        ):
            out.append(r)
    return out

