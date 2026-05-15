"""Overview router."""

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse

from web.services import overview_service

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
def web_index(request: Request) -> HTMLResponse:
    return overview_service.web_index(request)


@router.get("/api/v1/health")
def api_health() -> dict:
    return overview_service.health()


@router.get("/api/v1/countries")
def api_countries(
    q: str | None = Query(default=None, min_length=1),
    limit: int = Query(default=300, ge=1, le=500),
    lang: str = Query(default="es"),
) -> list[dict]:
    return overview_service.countries(q=q, limit=limit, lang=lang)


@router.get("/api/v1/home/defaults")
def api_home_defaults() -> dict:
    return overview_service.home_defaults()


@router.get("/api/v1/overview")
def api_overview(lang: str = Query(default="es")) -> dict:
    return overview_service.overview(lang=lang)


@router.get("/api/v1/top-countries")
def api_top_countries(
    limit: int = Query(default=5, ge=1, le=25),
    lang: str = Query(default="es"),
) -> list[dict]:
    return overview_service.top_countries(limit=limit, lang=lang)


@router.get("/api/v1/top-minerals")
def api_top_minerals(
    limit: int = Query(default=5, ge=1, le=25),
    lang: str = Query(default="es"),
) -> list[dict]:
    return overview_service.top_minerals(limit=limit, lang=lang)


@router.get("/api/v1/minerals")
def api_minerals(
    q: str | None = Query(default=None, min_length=1),
    limit: int = Query(default=1000, ge=1, le=5000),
    lang: str = Query(default="es"),
) -> list[dict]:
    return overview_service.minerals(q=q, limit=limit, lang=lang)

