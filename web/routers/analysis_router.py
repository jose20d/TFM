"""Analysis router."""

from fastapi import APIRouter, Query

from web.services import analysis_service

router = APIRouter()


@router.get("/api/v1/analysis/country-overview")
def api_analysis_country_overview(lang: str = Query(default="es")) -> list[dict]:
    return analysis_service.country_overview(lang=lang)

