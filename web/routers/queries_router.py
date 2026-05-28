"""Guided queries router."""

from fastapi import APIRouter, Query

from web.services import queries_service

router = APIRouter()


@router.get("/api/v1/queries/deposits-by-mineral")
def api_queries_deposits_by_mineral(
    country_iso3: str | None = Query(default=None),
    mineral: str = Query(default=""),
    deposit_status: str | None = Query(default=None),
    min_minerals: int = Query(default=1, ge=1, le=20),
    limit: int = Query(default=1000, ge=1, le=100000),
    offset: int = Query(default=0, ge=0, le=1000000),
    lang: str = Query(default="es"),
) -> dict:
    return queries_service.deposits_by_mineral(
        country_iso3=country_iso3,
        mineral=mineral,
        deposit_status=deposit_status,
        min_minerals=min_minerals,
        limit=limit,
        offset=offset,
        lang=lang,
    )


@router.get("/api/v1/queries/combined-minerals")
def api_queries_combined_minerals(
    country_iso3: str | None = Query(default=None),
    mineral_a: str = Query(..., min_length=1),
    mineral_b: str = Query(..., min_length=1),
    exclude_mineral: str | None = Query(default=None),
    limit: int = Query(default=1000, ge=1, le=100000),
    offset: int = Query(default=0, ge=0, le=1000000),
    lang: str = Query(default="es"),
) -> dict:
    return queries_service.combined_minerals(
        country_iso3=country_iso3,
        mineral_a=mineral_a,
        mineral_b=mineral_b,
        exclude_mineral=exclude_mineral,
        limit=limit,
        offset=offset,
        lang=lang,
    )


@router.get("/api/v1/queries/spatial-nearby")
def api_queries_spatial_nearby(
    country_iso3: str = Query(..., min_length=3, max_length=3),
    base_dep_id: int = Query(..., ge=1),
    radius_km: float = Query(default=20, ge=1, le=200),
    mineral: str | None = Query(default=None),
    limit: int = Query(default=1000, ge=1, le=100000),
    offset: int = Query(default=0, ge=0, le=1000000),
    lang: str = Query(default="es"),
) -> dict:
    return queries_service.spatial_nearby(
        country_iso3=country_iso3,
        base_dep_id=base_dep_id,
        radius_km=radius_km,
        mineral=mineral,
        limit=limit,
        offset=offset,
        lang=lang,
    )


@router.get("/api/v1/queries/country-profile")
def api_queries_country_profile(
    min_deposits: int = Query(default=0, ge=0),
    gdp_min: float | None = Query(default=None),
    gdp_max: float | None = Query(default=None),
    cpi_min: float | None = Query(default=None),
    cpi_max: float | None = Query(default=None),
    fsi_min: float | None = Query(default=None),
    fsi_max: float | None = Query(default=None),
    limit: int = Query(default=1000, ge=1, le=100000),
    offset: int = Query(default=0, ge=0, le=1000000),
    lang: str = Query(default="es"),
) -> dict:
    return queries_service.country_profile(
        min_deposits=min_deposits,
        gdp_min=gdp_min,
        gdp_max=gdp_max,
        cpi_min=cpi_min,
        cpi_max=cpi_max,
        fsi_min=fsi_min,
        fsi_max=fsi_max,
        limit=limit,
        offset=offset,
        lang=lang,
    )


@router.get("/api/v1/queries/country-profile/bounds")
def api_queries_country_profile_bounds() -> dict:
    return queries_service.country_profile_bounds()

