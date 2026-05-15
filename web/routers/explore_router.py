"""Explore router."""

from fastapi import APIRouter, Query

from web.services import explore_service

router = APIRouter()


@router.get("/api/v1/deposits/map")
def api_deposits_map(
    limit: int = Query(default=2000, ge=50, le=10000),
    lang: str = Query(default="es"),
) -> list[dict]:
    return explore_service.deposits_map(limit=limit, lang=lang)


@router.get("/api/v1/explore/deposits")
def api_explore_deposits(
    country_iso3: str | None = Query(default=None),
    mineral: str | None = Query(default=None),
    limit: int = Query(default=500, ge=1, le=500000),
    offset: int = Query(default=0, ge=0, le=1000000),
    lang: str = Query(default="es"),
) -> list[dict]:
    return explore_service.explore_deposits(
        country_iso3=country_iso3,
        mineral=mineral,
        limit=limit,
        offset=offset,
        lang=lang,
    )


@router.get("/api/v1/explore/limits")
def api_explore_limits(country_iso3: str | None = Query(default=None)) -> dict:
    return explore_service.explore_limits(country_iso3=country_iso3)


@router.get("/api/v1/explore/deposits-count")
def api_explore_deposits_count(
    country_iso3: str | None = Query(default=None),
    mineral: str | None = Query(default=None),
) -> dict:
    return explore_service.explore_deposits_count(country_iso3=country_iso3, mineral=mineral)


@router.get("/api/v1/countries/{iso3}/summary")
def api_country_summary(iso3: str, lang: str = Query(default="es")) -> dict:
    return explore_service.country_summary(iso3=iso3, lang=lang)

