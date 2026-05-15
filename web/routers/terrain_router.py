"""Terrain router."""

from fastapi import APIRouter, Query

from web.services import terrain_service

router = APIRouter()


@router.get("/api/v1/terrain/corridor")
def api_terrain_corridor(
    country_iso3: str = Query(...),
    from_dep_id: int = Query(..., ge=1),
    to_dep_id: int = Query(..., ge=1),
    width_km: float = Query(default=2, ge=1, le=50),
    lang: str = Query(default="es"),
) -> dict:
    return terrain_service.corridor(
        country_iso3=country_iso3,
        from_dep_id=from_dep_id,
        to_dep_id=to_dep_id,
        width_km=width_km,
        lang=lang,
    )


@router.get("/api/v1/terrain/zone-interest")
def api_terrain_zone_interest(
    country_iso3: str = Query(...),
    lat: float = Query(..., ge=-90, le=90),
    lng: float = Query(..., ge=-180, le=180),
    radius_km: float = Query(default=10, ge=1, le=50),
    lang: str = Query(default="es"),
) -> dict:
    return terrain_service.zone_interest(
        country_iso3=country_iso3,
        lat=lat,
        lng=lng,
        radius_km=radius_km,
        lang=lang,
    )


@router.get("/api/v1/terrain/frequent-minerals")
def api_terrain_frequent_minerals(
    country_iso3: str = Query(...),
    mineral: str | None = Query(default=None),
    limit: int = Query(default=20),
    show_all: bool = Query(default=False),
    lang: str = Query(default="es"),
) -> dict:
    return terrain_service.frequent_minerals(
        country_iso3=country_iso3,
        mineral=mineral,
        limit=limit,
        show_all=show_all,
        lang=lang,
    )


@router.get("/api/v1/terrain/exploratory-potential")
def api_terrain_exploratory_potential(
    country_iso3: str = Query(...),
    mineral: str = Query(...),
    intensity_level: str = Query(default="medium"),
    lang: str = Query(default="es"),
) -> dict:
    return terrain_service.exploratory_potential(
        country_iso3=country_iso3,
        mineral=mineral,
        intensity_level=intensity_level,
        lang=lang,
    )

