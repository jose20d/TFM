"""i18n helpers for data and payload localization."""

import re
import time
import unicodedata
from typing import Any

from web.services.common.query_service import fetch_all

I18N_CACHE_TTL_SECONDS = 300
_I18N_CACHE: dict[str, object] = {"loaded_at": 0.0, "rows": []}


def normalize_lang(lang: str | None) -> str:
    value = (lang or "es").strip().lower()
    return value if value in {"es", "en"} else "es"


def normalize_term(value: str | None) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def _load_i18n_rows() -> list[dict]:
    now = time.time()
    loaded_at = float(_I18N_CACHE.get("loaded_at") or 0.0)
    if now - loaded_at <= I18N_CACHE_TTL_SECONDS and _I18N_CACHE.get("rows"):
        return list(_I18N_CACHE.get("rows") or [])
    try:
        rows = fetch_all(
            """
            SELECT domain,
                   source_value_norm,
                   source_value_original,
                   label_es,
                   label_en
            FROM i18n_term_materialized
            """
        )
    except Exception:
        rows = []
    _I18N_CACHE["loaded_at"] = now
    _I18N_CACHE["rows"] = rows
    return rows


def translate_term(domain: str, value: str | None, lang: str) -> str | None:
    if value is None:
        return None
    norm = normalize_term(value)
    if not norm:
        return value
    target_lang = normalize_lang(lang)
    for row in _load_i18n_rows():
        if row.get("domain") == domain and row.get("source_value_norm") == norm:
            return row.get("label_es") if target_lang == "es" else row.get("label_en")
    return value


def resolve_source_term(domain: str, value: str | None) -> str | None:
    if value is None:
        return None
    norm = normalize_term(value)
    if not norm:
        return value
    for row in _load_i18n_rows():
        if row.get("domain") != domain:
            continue
        if norm in {
            normalize_term(row.get("source_value_original")),
            normalize_term(row.get("label_es")),
            normalize_term(row.get("label_en")),
        }:
            return row.get("source_value_original")
    return value


def localize_payload(payload: Any, lang: str):
    target_lang = normalize_lang(lang)
    if isinstance(payload, list):
        return [localize_payload(item, target_lang) for item in payload]
    if isinstance(payload, dict):
        localized = {}
        for key, value in payload.items():
            if (
                key == "name"
                and isinstance(value, str)
                and "iso3" in payload
                and "dep_id" not in payload
            ):
                localized[key] = translate_term("country", value, target_lang)
                continue
            if key in {"country_name", "country"} and isinstance(value, str):
                localized[key] = translate_term("country", value, target_lang)
                continue
            if key in {"commod", "mineral", "coexistence_focus_mineral", "dominant_mineral", "top_mineral"} and isinstance(value, str):
                localized[key] = translate_term("mineral", value, target_lang)
                continue
            if key in {"status", "deposit_status", "dev_stat"} and isinstance(value, str):
                localized[key] = translate_term("deposit_status", value, target_lang)
                continue
            if key in {"owner_tp"} and isinstance(value, str):
                localized[key] = translate_term("ownership_type", value, target_lang)
                continue
            if key in {"material"} and isinstance(value, str):
                localized[key] = translate_term("material", value, target_lang)
                continue
            if key == "minerals" and isinstance(value, str):
                parts = [part.strip() for part in value.split(",")]
                localized[key] = ", ".join(
                    [translate_term("mineral", part, target_lang) or part for part in parts if part]
                )
                continue
            if key in {"minerals", "common_endpoint_minerals"} and isinstance(value, list):
                localized[key] = [
                    translate_term("mineral", item, target_lang) if isinstance(item, str) else item
                    for item in value
                ]
                continue
            localized[key] = localize_payload(value, target_lang)
        return localized
    return payload


def sort_key_localized(value: str | None) -> str:
    text = str(value or "").strip().lower()
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))

