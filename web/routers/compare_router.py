"""Compare router."""

from fastapi import APIRouter, Query

from web.services import compare_service

router = APIRouter()


@router.get("/api/v1/countries/compare")
def api_countries_compare(
    iso3: list[str] = Query(default=["CRI", "CHL", "PER"]),
    lang: str = Query(default="es"),
) -> list[dict]:
    return compare_service.countries_compare(iso3=iso3, lang=lang)

