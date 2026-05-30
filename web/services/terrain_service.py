"""Terrain service functions."""

import json

from fastapi import HTTPException

from web.db import get_connection
from web.services.common.i18n_service import localize_payload
from web.services.common.i18n_service import resolve_source_term


def _tr(lang: str, es: str, en: str) -> str:
    return en if (lang or "").strip().lower() == "en" else es


def _localize_region(value: str | None, lang: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return _tr(lang, "Sin region", "Unknown region")
    lowered = raw.lower()
    if lowered in {"sin region", "sin región", "unknown region"}:
        return _tr(lang, "Sin region", "Unknown region")
    return raw


def _safe_geojson(value: str | None) -> dict:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    except json.JSONDecodeError:
        return {}


def corridor(country_iso3: str, from_dep_id: int, to_dep_id: int, width_km: float, lang: str) -> dict:
    iso3 = (country_iso3 or "").strip().upper()
    if len(iso3) != 3:
        raise HTTPException(status_code=400, detail="country_iso3 must be a valid ISO3 code.")
    if from_dep_id == to_dep_id:
        raise HTTPException(status_code=400, detail="from_dep_id and to_dep_id must be different.")

    width_m = float(width_km) * 1000.0

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT c.country_name
                FROM dim_country c
                WHERE c.iso3 = %s
                ORDER BY LENGTH(c.country_name) DESC, c.country_name
                LIMIT 1
                """,
                (iso3,),
            )
            row = cur.fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail=f"Country with ISO3 '{iso3}' was not found.")
            country_name = row[0]

            cur.execute(
                """
                SELECT COUNT(*)
                FROM mrds_deposit d
                JOIN mrds_location l ON l.dep_id = d.dep_id
                JOIN dim_country c ON c.country_id = l.country_id
                WHERE c.iso3 = %s
                  AND d.latitude IS NOT NULL
                  AND d.longitude IS NOT NULL
                """,
                (iso3,),
            )
            georef_count = int(cur.fetchone()[0] or 0)
            if georef_count < 2:
                raise HTTPException(
                    status_code=400,
                    detail="Selected country has fewer than 2 georeferenced deposits.",
                )

            cur.execute(
                """
                SELECT d.dep_id,
                       COALESCE(d.name, CONCAT('Dep. ', d.dep_id::text)) AS name,
                       d.latitude AS lat,
                       d.longitude AS lng,
                       COALESCE((
                           SELECT ARRAY_AGG(DISTINCT TRIM(mc.commod) ORDER BY TRIM(mc.commod))
                           FROM mrds_commodity mc
                           WHERE mc.dep_id = d.dep_id
                             AND mc.commod IS NOT NULL
                             AND TRIM(mc.commod) <> ''
                       ), ARRAY[]::text[]) AS minerals
                FROM mrds_deposit d
                JOIN mrds_location l ON l.dep_id = d.dep_id
                JOIN dim_country c ON c.country_id = l.country_id
                WHERE c.iso3 = %s
                  AND d.dep_id = ANY(%s::bigint[])
                  AND d.latitude IS NOT NULL
                  AND d.longitude IS NOT NULL
                ORDER BY d.dep_id
                """,
                (iso3, [from_dep_id, to_dep_id]),
            )
            endpoint_rows = cur.fetchall()
            if len(endpoint_rows) != 2:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Both endpoints must exist in the selected country and "
                        "must have valid georeferenced coordinates."
                    ),
                )

            endpoints_by_id = {
                int(dep_id): {
                    "dep_id": int(dep_id),
                    "name": name,
                    "lat": float(lat),
                    "lng": float(lng),
                    "minerals": list(minerals or []),
                }
                for dep_id, name, lat, lng, minerals in endpoint_rows
            }
            from_deposit = endpoints_by_id[from_dep_id]
            to_deposit = endpoints_by_id[to_dep_id]

            cur.execute(
                """
                WITH endpoints AS (
                    SELECT
                        ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography AS from_geog,
                        ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography AS to_geog
                ),
                corridor AS (
                    SELECT
                        ST_MakeLine(from_geog::geometry, to_geog::geometry)::geography AS axis_geog
                    FROM endpoints
                )
                SELECT
                    ROUND((ST_Distance(e.from_geog, e.to_geog) / 1000.0)::numeric, 3) AS distance_km,
                    ST_AsGeoJSON(c.axis_geog::geometry) AS line_geojson,
                    ST_AsGeoJSON(ST_Buffer(c.axis_geog, %s)::geometry) AS corridor_geojson
                FROM endpoints e
                CROSS JOIN corridor c
                """,
                (
                    from_deposit["lng"],
                    from_deposit["lat"],
                    to_deposit["lng"],
                    to_deposit["lat"],
                    width_m,
                ),
            )
            line_row = cur.fetchone()
            distance_km = float(line_row[0] or 0.0)
            line_geojson = _safe_geojson(line_row[1] if line_row else None)
            corridor_geojson = _safe_geojson(line_row[2] if line_row else None)

            cur.execute(
                """
                WITH endpoints AS (
                    SELECT
                        ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography AS from_geog,
                        ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography AS to_geog
                ),
                corridor AS (
                    SELECT ST_MakeLine(from_geog::geometry, to_geog::geometry)::geography AS axis_geog
                    FROM endpoints
                ),
                deposits_in_corridor AS (
                    SELECT
                        d.dep_id,
                        COALESCE(d.name, CONCAT('Dep. ', d.dep_id::text)) AS name,
                        d.latitude AS lat,
                        d.longitude AS lng,
                        ROUND(
                            (
                                ST_Distance(
                                ST_SetSRID(ST_MakePoint(d.longitude, d.latitude), 4326)::geography,
                                c.axis_geog
                                ) / 1000.0
                            )::numeric,
                            3
                        ) AS distance_to_axis_km
                    FROM mrds_deposit d
                    JOIN mrds_location l ON l.dep_id = d.dep_id
                    JOIN dim_country dc ON dc.country_id = l.country_id
                    CROSS JOIN corridor c
                    WHERE dc.iso3 = %s
                      AND d.latitude IS NOT NULL
                      AND d.longitude IS NOT NULL
                      AND (
                        ST_DWithin(
                            ST_SetSRID(ST_MakePoint(d.longitude, d.latitude), 4326)::geography,
                            c.axis_geog,
                            %s
                        )
                        OR d.dep_id = %s
                        OR d.dep_id = %s
                      )
                )
                SELECT
                    d.dep_id,
                    d.name,
                    d.lat,
                    d.lng,
                    d.distance_to_axis_km,
                    COALESCE((
                        SELECT ARRAY_AGG(DISTINCT TRIM(mc.commod) ORDER BY TRIM(mc.commod))
                        FROM mrds_commodity mc
                        WHERE mc.dep_id = d.dep_id
                          AND mc.commod IS NOT NULL
                          AND TRIM(mc.commod) <> ''
                    ), ARRAY[]::text[]) AS minerals
                FROM deposits_in_corridor d
                ORDER BY d.distance_to_axis_km ASC, d.dep_id ASC
                """,
                (
                    from_deposit["lng"],
                    from_deposit["lat"],
                    to_deposit["lng"],
                    to_deposit["lat"],
                    iso3,
                    width_m,
                    from_dep_id,
                    to_dep_id,
                ),
            )
            corridor_rows = cur.fetchall()

    deposits_in_corridor: list[dict] = []
    mineral_counts: dict[str, int] = {}
    deposit_mineral_sets: dict[int, set[str]] = {}

    for dep_id, name, lat, lng, distance_to_axis_km, minerals in corridor_rows:
        dep_minerals = [m for m in list(minerals or []) if isinstance(m, str) and m.strip()]
        unique_minerals = set(dep_minerals)
        deposit_mineral_sets[int(dep_id)] = unique_minerals
        for mineral in unique_minerals:
            mineral_counts[mineral] = mineral_counts.get(mineral, 0) + 1
        deposits_in_corridor.append(
            {
                "dep_id": int(dep_id),
                "name": name,
                "lat": float(lat),
                "lng": float(lng),
                "distance_to_axis_km": float(distance_to_axis_km or 0.0),
                "minerals": sorted(unique_minerals),
                "intensity_score": 0.0,
            }
        )

    deposit_count = len(deposits_in_corridor)
    corridor_minerals: list[dict] = []
    mineral_percentages: dict[str, float] = {}
    if deposit_count > 0:
        for mineral, count in mineral_counts.items():
            percentage = round((count * 100.0) / deposit_count, 2)
            mineral_percentages[mineral] = percentage
            if percentage >= 50:
                intensity = "high"
            elif percentage >= 20:
                intensity = "medium"
            else:
                intensity = "low"
            corridor_minerals.append(
                {
                    "mineral": mineral,
                    "count": int(count),
                    "percentage": percentage,
                    "intensity": intensity,
                }
            )
        corridor_minerals.sort(key=lambda item: (-item["count"], item["mineral"]))

    for deposit in deposits_in_corridor:
        dep_id = deposit["dep_id"]
        minerals = deposit_mineral_sets.get(dep_id, set())
        if not minerals:
            deposit["intensity_score"] = 0.0
            continue
        score = sum(mineral_percentages.get(mineral, 0.0) for mineral in minerals) / len(minerals)
        deposit["intensity_score"] = round(score, 2)

    from_minerals = set(from_deposit.get("minerals") or [])
    to_minerals = set(to_deposit.get("minerals") or [])
    common_endpoint_minerals = sorted(from_minerals.intersection(to_minerals))

    payload = {
        "country": {"iso3": iso3, "name": country_name},
        "from": from_deposit,
        "to": to_deposit,
        "width_km": round(float(width_km), 2),
        "distance_km": round(distance_km, 3),
        "deposit_count": deposit_count,
        "common_endpoint_minerals": common_endpoint_minerals,
        "corridor_minerals": corridor_minerals,
        "deposits_in_corridor": deposits_in_corridor,
        "line_geojson": line_geojson,
        "corridor_geojson": corridor_geojson,
    }
    return localize_payload(payload, lang)


def zone_interest(country_iso3: str, lat: float, lng: float, radius_km: float, lang: str) -> dict:
    iso3 = (country_iso3 or "").strip().upper()
    if len(iso3) != 3:
        raise HTTPException(status_code=400, detail="country_iso3 must be a valid ISO3 code.")

    radius_m = float(radius_km) * 1000.0

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT c.country_name
                FROM dim_country c
                WHERE c.iso3 = %s
                ORDER BY LENGTH(c.country_name) DESC, c.country_name
                LIMIT 1
                """,
                (iso3,),
            )
            row = cur.fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail=f"Country with ISO3 '{iso3}' was not found.")
            country_name = row[0]

            cur.execute(
                """
                SELECT COUNT(*)
                FROM mrds_deposit d
                JOIN mrds_location l ON l.dep_id = d.dep_id
                JOIN dim_country c ON c.country_id = l.country_id
                WHERE c.iso3 = %s
                  AND d.latitude IS NOT NULL
                  AND d.longitude IS NOT NULL
                """,
                (iso3,),
            )
            georef_count = int(cur.fetchone()[0] or 0)
            if georef_count == 0:
                raise HTTPException(
                    status_code=400,
                    detail="Selected country has no georeferenced deposits.",
                )

            cur.execute(
                """
                WITH center_point AS (
                    SELECT ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography AS center_geog
                )
                SELECT ST_AsGeoJSON(ST_Buffer(center_geog, %s)::geometry) AS zone_geojson
                FROM center_point
                """,
                (lng, lat, radius_m),
            )
            zone_row = cur.fetchone()
            zone_geojson = _safe_geojson(zone_row[0] if zone_row else None)

            cur.execute(
                """
                WITH center_point AS (
                    SELECT ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography AS center_geog
                ),
                deposits_in_zone AS (
                    SELECT
                        d.dep_id,
                        COALESCE(d.name, CONCAT('Dep. ', d.dep_id::text)) AS name,
                        d.latitude AS lat,
                        d.longitude AS lng,
                        ROUND(
                            (
                                ST_Distance(
                                    ST_SetSRID(ST_MakePoint(d.longitude, d.latitude), 4326)::geography,
                                    cp.center_geog
                                ) / 1000.0
                            )::numeric,
                            3
                        ) AS distance_km
                    FROM mrds_deposit d
                    JOIN mrds_location l ON l.dep_id = d.dep_id
                    JOIN dim_country c ON c.country_id = l.country_id
                    CROSS JOIN center_point cp
                    WHERE c.iso3 = %s
                      AND d.latitude IS NOT NULL
                      AND d.longitude IS NOT NULL
                      AND ST_DWithin(
                          ST_SetSRID(ST_MakePoint(d.longitude, d.latitude), 4326)::geography,
                          cp.center_geog,
                          %s
                      )
                )
                SELECT
                    d.dep_id,
                    d.name,
                    d.lat,
                    d.lng,
                    d.distance_km,
                    COALESCE((
                        SELECT ARRAY_AGG(DISTINCT TRIM(mc.commod) ORDER BY TRIM(mc.commod))
                        FROM mrds_commodity mc
                        WHERE mc.dep_id = d.dep_id
                          AND mc.commod IS NOT NULL
                          AND TRIM(mc.commod) <> ''
                    ), ARRAY[]::text[]) AS minerals
                FROM deposits_in_zone d
                ORDER BY d.distance_km ASC, d.dep_id ASC
                """,
                (lng, lat, iso3, radius_m),
            )
            rows = cur.fetchall()

    deposits: list[dict] = []
    mineral_counts: dict[str, int] = {}
    deposit_mineral_sets: dict[int, set[str]] = {}

    for dep_id, name, dep_lat, dep_lng, distance_km, minerals in rows:
        clean_minerals = [m for m in list(minerals or []) if isinstance(m, str) and m.strip()]
        unique_minerals = set(clean_minerals)
        deposit_mineral_sets[int(dep_id)] = unique_minerals
        for mineral in unique_minerals:
            mineral_counts[mineral] = mineral_counts.get(mineral, 0) + 1
        deposits.append(
            {
                "dep_id": int(dep_id),
                "name": name,
                "lat": float(dep_lat),
                "lng": float(dep_lng),
                "distance_km": float(distance_km or 0.0),
                "minerals": sorted(unique_minerals),
            }
        )

    deposit_count = len(deposits)
    minerals: list[dict] = []
    if deposit_count > 0:
        for mineral, count in mineral_counts.items():
            percentage = round((count * 100.0) / deposit_count, 2)
            if percentage >= 50:
                intensity = "high"
            elif percentage >= 20:
                intensity = "medium"
            else:
                intensity = "low"
            minerals.append(
                {
                    "mineral": mineral,
                    "count": int(count),
                    "percentage": percentage,
                    "intensity": intensity,
                }
            )
        minerals.sort(key=lambda item: (-item["count"], item["mineral"]))

    response = {
        "country": {"iso3": iso3, "name": country_name},
        "center": {"lat": round(float(lat), 6), "lng": round(float(lng), 6)},
        "radius_km": round(float(radius_km), 2),
        "deposit_count": deposit_count,
        "minerals": minerals,
        "deposits": deposits,
        "zone_geojson": zone_geojson,
    }
    if deposit_count == 0:
        response["message"] = _tr(
            lang,
            "No se encontraron depositos registrados dentro del radio seleccionado.",
            "No registered deposits were found within the selected radius.",
        )
    return localize_payload(response, lang)


def frequent_minerals(
    country_iso3: str,
    mineral: str | None,
    limit: int,
    show_all: bool,
    lang: str,
) -> dict:
    iso3 = (country_iso3 or "").strip().upper()
    if len(iso3) != 3:
        raise HTTPException(status_code=400, detail="country_iso3 must be a valid ISO3 code.")
    if limit not in {10, 20, 50}:
        raise HTTPException(status_code=400, detail="limit must be one of: 10, 20, 50.")
    selected_mineral = (resolve_source_term("mineral", mineral) or "").strip()

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT c.country_name
                FROM dim_country c
                WHERE c.iso3 = %s
                ORDER BY LENGTH(c.country_name) DESC, c.country_name
                LIMIT 1
                """,
                (iso3,),
            )
            row = cur.fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail=f"Country with ISO3 '{iso3}' was not found.")
            country_name = row[0]

            cur.execute(
                """
                SELECT d.dep_id,
                       COALESCE(d.name, CONCAT('Dep. ', d.dep_id::text)) AS name,
                       d.latitude AS lat,
                       d.longitude AS lng,
                       COALESCE(NULLIF(TRIM(l.region), ''), NULLIF(TRIM(l.state_prov), ''), 'Sin region') AS region
                FROM mrds_deposit d
                JOIN mrds_location l ON l.dep_id = d.dep_id
                JOIN dim_country c ON c.country_id = l.country_id
                WHERE c.iso3 = %s
                  AND d.latitude IS NOT NULL
                  AND d.longitude IS NOT NULL
                  AND (
                    %s = '' OR EXISTS (
                      SELECT 1
                      FROM mrds_commodity x
                      WHERE x.dep_id = d.dep_id
                        AND x.commod IS NOT NULL
                        AND TRIM(x.commod) <> ''
                        AND LOWER(TRIM(x.commod)) = LOWER(%s)
                    )
                  )
                ORDER BY d.dep_id
                """,
                (iso3, selected_mineral, selected_mineral),
            )
            deposit_rows = cur.fetchall()

            if not deposit_rows:
                return localize_payload({
                    "country": {"iso3": iso3, "name": country_name},
                    "selected_mineral": selected_mineral or None,
                    "total_deposits": 0,
                    "minerals": [],
                    "top_regions": [],
                    "heat_points": [],
                    "coexistence_focus_mineral": selected_mineral or None,
                    "coexistence": [],
                    "available_minerals": [],
                    "points_geojson": {},
                    "message": _tr(
                        lang,
                        "No se encontraron minerales asociados para esta seleccion.",
                        "No associated minerals were found for this selection.",
                    ),
                }, lang)

            dep_ids = [int(row[0]) for row in deposit_rows]
            dep_ids_set = set(dep_ids)

            cur.execute(
                """
                SELECT DISTINCT mc.dep_id, TRIM(mc.commod) AS mineral
                FROM mrds_commodity mc
                WHERE mc.dep_id = ANY(%s::bigint[])
                  AND mc.commod IS NOT NULL
                  AND TRIM(mc.commod) <> ''
                """,
                (dep_ids,),
            )
            dep_mineral_rows = cur.fetchall()

            cur.execute(
                """
                SELECT ST_AsGeoJSON(ST_Collect(ST_SetSRID(ST_MakePoint(d.longitude, d.latitude), 4326))) AS points_geojson
                FROM mrds_deposit d
                WHERE d.dep_id = ANY(%s::bigint[])
                  AND d.latitude IS NOT NULL
                  AND d.longitude IS NOT NULL
                """,
                (dep_ids,),
            )
            geo_row = cur.fetchone()
            points_geojson = _safe_geojson(geo_row[0] if geo_row else None)

    total_deposits = len(dep_ids)
    minerals_by_deposit: dict[int, set[str]] = {dep_id: set() for dep_id in dep_ids}
    mineral_to_deposits: dict[str, set[int]] = {}

    for dep_id, mineral_name in dep_mineral_rows:
        dep_id_int = int(dep_id)
        if dep_id_int not in dep_ids_set:
            continue
        mineral_clean = str(mineral_name or "").strip()
        if not mineral_clean:
            continue
        minerals_by_deposit[dep_id_int].add(mineral_clean)
        mineral_to_deposits.setdefault(mineral_clean, set()).add(dep_id_int)

    available_sorted = sorted(
        (
            {
                "mineral": mineral_name,
                "deposit_count": len(deposit_set),
            }
            for mineral_name, deposit_set in mineral_to_deposits.items()
        ),
        key=lambda item: (-item["deposit_count"], item["mineral"]),
    )

    minerals_rank = available_sorted if show_all else available_sorted[:limit]

    mineral_frequency: dict[str, float] = {}
    minerals_payload: list[dict] = []
    for item in minerals_rank:
        mineral_name = item["mineral"]
        count = int(item["deposit_count"])
        percentage = round((count * 100.0) / total_deposits, 2) if total_deposits else 0.0
        mineral_frequency[mineral_name] = percentage
        if percentage >= 50:
            intensity = "high"
        elif percentage >= 20:
            intensity = "medium"
        else:
            intensity = "low"

        co_counts: dict[str, int] = {}
        for dep_id in mineral_to_deposits.get(mineral_name, set()):
            for other in minerals_by_deposit.get(dep_id, set()):
                if other == mineral_name:
                    continue
                co_counts[other] = co_counts.get(other, 0) + 1
        common_with = [
            name
            for name, _ in sorted(co_counts.items(), key=lambda value: (-value[1], value[0]))[:3]
        ]

        minerals_payload.append(
            {
                "mineral": mineral_name,
                "deposit_count": count,
                "percentage": percentage,
                "intensity": intensity,
                "common_with": common_with,
            }
        )

    focus_mineral = selected_mineral or (minerals_payload[0]["mineral"] if minerals_payload else None)
    coexistence_counts: dict[str, int] = {}
    if focus_mineral and focus_mineral in mineral_to_deposits:
        for dep_id in mineral_to_deposits.get(focus_mineral, set()):
            for other in minerals_by_deposit.get(dep_id, set()):
                if other == focus_mineral:
                    continue
                coexistence_counts[other] = coexistence_counts.get(other, 0) + 1
    coexistence = [
        {"mineral": name, "count": count}
        for name, count in sorted(coexistence_counts.items(), key=lambda value: (-value[1], value[0]))[:10]
    ]

    region_groups: dict[str, list[int]] = {}
    for dep_id, _name, _lat, _lng, region in deposit_rows:
        region_groups.setdefault(region, []).append(int(dep_id))

    top_regions: list[dict] = []
    for region, region_dep_ids in region_groups.items():
        regional_count: dict[str, int] = {}
        for dep_id in region_dep_ids:
            for mineral_name in minerals_by_deposit.get(dep_id, set()):
                regional_count[mineral_name] = regional_count.get(mineral_name, 0) + 1
        dominant = (
            sorted(regional_count.items(), key=lambda value: (-value[1], value[0]))[0][0]
            if regional_count
            else "N/A"
        )
        top_regions.append(
            {
                "region": _localize_region(region, lang),
                "dominant_mineral": dominant,
                "deposit_count": len(region_dep_ids),
            }
        )
    top_regions.sort(key=lambda item: (-item["deposit_count"], item["region"]))
    top_regions = top_regions[:10]

    max_minerals_per_deposit = max((len(values) for values in minerals_by_deposit.values()), default=1)
    heat_points: list[dict] = []
    for dep_id, name, dep_lat, dep_lng, region in deposit_rows:
        dep_id_int = int(dep_id)
        deposit_minerals = minerals_by_deposit.get(dep_id_int, set())
        if focus_mineral and focus_mineral in deposit_minerals:
            weight = 1.0
        else:
            weight = len(deposit_minerals) / max_minerals_per_deposit if max_minerals_per_deposit else 0.0
        if focus_mineral and focus_mineral in deposit_minerals:
            marker_mineral = focus_mineral
        else:
            marker_mineral = (
                sorted(
                    deposit_minerals,
                    key=lambda value: (-len(mineral_to_deposits.get(value, set())), value),
                )[0]
                if deposit_minerals
                else "N/A"
            )
        heat_points.append(
            {
                "dep_name": name,
                "lat": float(dep_lat),
                "lng": float(dep_lng),
                "weight": round(float(weight), 3),
                "mineral": marker_mineral,
                "region": _localize_region(region, lang),
            }
        )

    return localize_payload({
        "country": {"iso3": iso3, "name": country_name},
        "selected_mineral": selected_mineral or None,
        "total_deposits": total_deposits,
        "minerals": minerals_payload,
        "top_regions": top_regions,
        "heat_points": heat_points,
        "coexistence_focus_mineral": focus_mineral,
        "coexistence": coexistence,
        "available_minerals": available_sorted,
        "points_geojson": points_geojson,
    }, lang)


def exploratory_potential(country_iso3: str, mineral: str, intensity_level: str, lang: str) -> dict:
    iso3 = (country_iso3 or "").strip().upper()
    if len(iso3) != 3:
        raise HTTPException(status_code=400, detail="country_iso3 must be a valid ISO3 code.")
    mineral_target = (resolve_source_term("mineral", mineral) or "").strip()
    if not mineral_target:
        raise HTTPException(status_code=400, detail="mineral is required.")
    level = (intensity_level or "medium").strip().lower()
    if level not in {"low", "medium", "high"}:
        raise HTTPException(status_code=400, detail="intensity_level must be one of: low, medium, high.")

    settings_by_level = {
        "low": {"radius_km": 22.0, "min_points": 2},
        "medium": {"radius_km": 12.0, "min_points": 3},
        "high": {"radius_km": 6.0, "min_points": 4},
    }
    level_settings = settings_by_level[level]
    radius_km = level_settings["radius_km"]
    min_points = level_settings["min_points"]
    radius_m = radius_km * 1000.0

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT c.country_name
                FROM dim_country c
                WHERE c.iso3 = %s
                ORDER BY LENGTH(c.country_name) DESC, c.country_name
                LIMIT 1
                """,
                (iso3,),
            )
            row = cur.fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail=f"Country with ISO3 '{iso3}' was not found.")
            country_name = row[0]

            cur.execute(
                """
                WITH target_points AS (
                    SELECT
                        d.dep_id,
                        COALESCE(d.name, CONCAT('Dep. ', d.dep_id::text)) AS name,
                        d.latitude AS lat,
                        d.longitude AS lng,
                        COALESCE(NULLIF(TRIM(l.region), ''), NULLIF(TRIM(l.state_prov), ''), 'Sin region') AS region,
                        ST_Transform(ST_SetSRID(ST_MakePoint(d.longitude, d.latitude), 4326), 3857) AS geom_3857,
                        ST_SetSRID(ST_MakePoint(d.longitude, d.latitude), 4326) AS geom_4326
                    FROM mrds_deposit d
                    JOIN mrds_location l ON l.dep_id = d.dep_id
                    JOIN dim_country c ON c.country_id = l.country_id
                    WHERE c.iso3 = %s
                      AND d.latitude IS NOT NULL
                      AND d.longitude IS NOT NULL
                      AND EXISTS (
                        SELECT 1
                        FROM mrds_commodity mc
                        WHERE mc.dep_id = d.dep_id
                          AND mc.commod IS NOT NULL
                          AND TRIM(mc.commod) <> ''
                          AND TRIM(mc.commod) ILIKE ('%%' || %s || '%%')
                      )
                )
                SELECT dep_id, name, lat, lng, region
                FROM target_points
                ORDER BY dep_id
                """,
                (iso3, mineral_target),
            )
            deposit_rows = cur.fetchall()

            if len(deposit_rows) < 2:
                return localize_payload({
                    "country": {"iso3": iso3, "name": country_name},
                    "mineral": mineral_target,
                    "intensity_level": level,
                    "radius_km": radius_km,
                    "total_deposits": len(deposit_rows),
                    "spatial_classification": _tr(lang, "concentracion dispersa", "dispersed concentration"),
                    "spatial_pattern": _tr(lang, "insuficiente", "insufficient"),
                    "clusters": [],
                    "heat_points": [],
                    "top_regions": [],
                    "points_geojson": {},
                    "message": _tr(
                        lang,
                        "No se encontraron suficientes registros para identificar patrones espaciales.",
                        "Not enough records were found to identify spatial patterns.",
                    ),
                }, lang)

            cur.execute(
                """
                WITH target_points AS (
                    SELECT
                        d.dep_id,
                        COALESCE(d.name, CONCAT('Dep. ', d.dep_id::text)) AS name,
                        d.latitude AS lat,
                        d.longitude AS lng,
                        COALESCE(NULLIF(TRIM(l.region), ''), NULLIF(TRIM(l.state_prov), ''), 'Sin region') AS region,
                        ST_Transform(ST_SetSRID(ST_MakePoint(d.longitude, d.latitude), 4326), 3857) AS geom_3857,
                        ST_SetSRID(ST_MakePoint(d.longitude, d.latitude), 4326) AS geom_4326
                    FROM mrds_deposit d
                    JOIN mrds_location l ON l.dep_id = d.dep_id
                    JOIN dim_country c ON c.country_id = l.country_id
                    WHERE c.iso3 = %s
                      AND d.latitude IS NOT NULL
                      AND d.longitude IS NOT NULL
                      AND EXISTS (
                        SELECT 1
                        FROM mrds_commodity mc
                        WHERE mc.dep_id = d.dep_id
                          AND mc.commod IS NOT NULL
                          AND TRIM(mc.commod) <> ''
                          AND TRIM(mc.commod) ILIKE ('%%' || %s || '%%')
                      )
                ),
                clustered AS (
                    SELECT
                        dep_id,
                        name,
                        lat,
                        lng,
                        region,
                        geom_4326,
                        ST_ClusterDBSCAN(geom_3857, eps := %s, minpoints := %s) OVER () AS cluster_id
                    FROM target_points
                )
                SELECT
                    COALESCE(cluster_id, -1) AS cluster_id,
                    COUNT(*) AS deposit_count,
                    ST_AsGeoJSON(ST_Collect(geom_4326)) AS cluster_points_geojson,
                    ST_AsGeoJSON(ST_ConvexHull(ST_Collect(geom_4326))) AS cluster_hull_geojson,
                    ST_Y(ST_Centroid(ST_Collect(geom_4326))) AS centroid_lat,
                    ST_X(ST_Centroid(ST_Collect(geom_4326))) AS centroid_lng
                FROM clustered
                GROUP BY COALESCE(cluster_id, -1)
                ORDER BY deposit_count DESC
                """,
                (iso3, mineral_target, radius_m, min_points),
            )
            cluster_rows = cur.fetchall()

            cur.execute(
                """
                WITH target_points AS (
                    SELECT
                        d.dep_id,
                        COALESCE(d.name, CONCAT('Dep. ', d.dep_id::text)) AS name,
                        d.latitude AS lat,
                        d.longitude AS lng,
                        COALESCE(NULLIF(TRIM(l.region), ''), NULLIF(TRIM(l.state_prov), ''), 'Sin region') AS region,
                        ST_Transform(ST_SetSRID(ST_MakePoint(d.longitude, d.latitude), 4326), 3857) AS geom_3857
                    FROM mrds_deposit d
                    JOIN mrds_location l ON l.dep_id = d.dep_id
                    JOIN dim_country c ON c.country_id = l.country_id
                    WHERE c.iso3 = %s
                      AND d.latitude IS NOT NULL
                      AND d.longitude IS NOT NULL
                      AND EXISTS (
                        SELECT 1
                        FROM mrds_commodity mc
                        WHERE mc.dep_id = d.dep_id
                          AND mc.commod IS NOT NULL
                          AND TRIM(mc.commod) <> ''
                          AND TRIM(mc.commod) ILIKE ('%%' || %s || '%%')
                      )
                )
                SELECT dep_id, COALESCE(ST_ClusterDBSCAN(geom_3857, eps := %s, minpoints := %s) OVER (), -1) AS cluster_id
                FROM target_points
                """,
                (iso3, mineral_target, radius_m, min_points),
            )
            cluster_detail_rows = cur.fetchall()

            cur.execute(
                """
                WITH target_points AS (
                    SELECT
                        d.dep_id,
                        COALESCE(d.name, CONCAT('Dep. ', d.dep_id::text)) AS name,
                        d.latitude AS lat,
                        d.longitude AS lng,
                        COALESCE(NULLIF(TRIM(l.region), ''), NULLIF(TRIM(l.state_prov), ''), 'Sin region') AS region
                    FROM mrds_deposit d
                    JOIN mrds_location l ON l.dep_id = d.dep_id
                    JOIN dim_country c ON c.country_id = l.country_id
                    WHERE c.iso3 = %s
                      AND d.latitude IS NOT NULL
                      AND d.longitude IS NOT NULL
                      AND EXISTS (
                        SELECT 1
                        FROM mrds_commodity mc
                        WHERE mc.dep_id = d.dep_id
                          AND mc.commod IS NOT NULL
                          AND TRIM(mc.commod) <> ''
                          AND TRIM(mc.commod) ILIKE ('%%' || %s || '%%')
                      )
                )
                SELECT region, COUNT(*) AS deposit_count
                FROM target_points
                GROUP BY region
                ORDER BY deposit_count DESC, region
                LIMIT 10
                """,
                (iso3, mineral_target),
            )
            top_region_rows = cur.fetchall()

            cur.execute(
                """
                WITH target_points AS (
                    SELECT
                        d.dep_id,
                        ST_SetSRID(ST_MakePoint(d.longitude, d.latitude), 4326)::geography AS geog
                    FROM mrds_deposit d
                    JOIN mrds_location l ON l.dep_id = d.dep_id
                    JOIN dim_country c ON c.country_id = l.country_id
                    WHERE c.iso3 = %s
                      AND d.latitude IS NOT NULL
                      AND d.longitude IS NOT NULL
                      AND EXISTS (
                        SELECT 1
                        FROM mrds_commodity mc
                        WHERE mc.dep_id = d.dep_id
                          AND mc.commod IS NOT NULL
                          AND TRIM(mc.commod) <> ''
                          AND TRIM(mc.commod) ILIKE ('%%' || %s || '%%')
                      )
                ),
                nearest AS (
                    SELECT
                        a.dep_id,
                        MIN(ST_Distance(a.geog, b.geog)) AS nearest_dist_m
                    FROM target_points a
                    JOIN target_points b ON a.dep_id <> b.dep_id
                    GROUP BY a.dep_id
                )
                SELECT ROUND(AVG(nearest_dist_m)::numeric, 2) AS avg_nearest_m
                FROM nearest
                """,
                (iso3, mineral_target),
            )
            nearest_row = cur.fetchone()
            avg_nearest_m = float(nearest_row[0] or 0.0)

            cur.execute(
                """
                WITH target_points AS (
                    SELECT ST_SetSRID(ST_MakePoint(d.longitude, d.latitude), 4326) AS geom
                    FROM mrds_deposit d
                    JOIN mrds_location l ON l.dep_id = d.dep_id
                    JOIN dim_country c ON c.country_id = l.country_id
                    WHERE c.iso3 = %s
                      AND d.latitude IS NOT NULL
                      AND d.longitude IS NOT NULL
                      AND EXISTS (
                        SELECT 1
                        FROM mrds_commodity mc
                        WHERE mc.dep_id = d.dep_id
                          AND mc.commod IS NOT NULL
                          AND TRIM(mc.commod) <> ''
                          AND TRIM(mc.commod) ILIKE ('%%' || %s || '%%')
                      )
                )
                SELECT ST_AsGeoJSON(ST_Collect(geom)) AS points_geojson
                FROM target_points
                """,
                (iso3, mineral_target),
            )
            points_row = cur.fetchone()
            points_geojson = _safe_geojson(points_row[0] if points_row else None)

    total_deposits = len(deposit_rows)
    clustered_deposits = sum(int(row[1]) for row in cluster_rows if int(row[0]) != -1)
    clustered_ratio = (clustered_deposits / total_deposits) if total_deposits else 0.0
    if clustered_ratio >= 0.6:
        spatial_classification = _tr(lang, "concentracion alta", "high concentration")
        spatial_pattern = _tr(lang, "agrupado", "clustered")
    elif clustered_ratio >= 0.3:
        spatial_classification = _tr(lang, "concentracion media", "medium concentration")
        spatial_pattern = _tr(lang, "mixto", "mixed")
    else:
        spatial_classification = _tr(lang, "concentracion dispersa", "dispersed concentration")
        spatial_pattern = _tr(lang, "disperso", "dispersed")

    max_cluster_size = max((int(row[1]) for row in cluster_rows), default=1)
    cluster_size_by_id = {int(cluster_id): int(dep_count) for cluster_id, dep_count, *_ in cluster_rows}
    cluster_by_deposit = {int(dep_id): int(cluster_id) for dep_id, cluster_id in cluster_detail_rows}
    heat_points = []
    for dep_id, name, dep_lat, dep_lng, region in deposit_rows:
        cluster_id = cluster_by_deposit.get(int(dep_id), -1)
        local_count = cluster_size_by_id.get(cluster_id, 1)
        weight = min(1.0, local_count / max_cluster_size) if max_cluster_size else 0.0
        heat_points.append(
            {
                "dep_name": name,
                "lat": float(dep_lat),
                "lng": float(dep_lng),
                "weight": round(weight, 3),
                "mineral": mineral_target,
                "region": _localize_region(region, lang),
                "cluster_id": int(cluster_id),
                "cluster_size": int(local_count),
            }
        )

    clusters = []
    for cluster_id, deposit_count, cluster_points_geojson, cluster_hull_geojson, centroid_lat, centroid_lng in cluster_rows:
        clusters.append(
            {
                "cluster_id": int(cluster_id),
                "deposit_count": int(deposit_count),
                "centroid": {"lat": float(centroid_lat), "lng": float(centroid_lng)},
                "points_geojson": _safe_geojson(cluster_points_geojson),
                "hull_geojson": _safe_geojson(cluster_hull_geojson),
            }
        )

    top_regions = [
        {"region": _localize_region(region, lang), "deposit_count": int(dep_count)}
        for region, dep_count in top_region_rows
    ]

    return localize_payload({
        "country": {"iso3": iso3, "name": country_name},
        "mineral": mineral_target,
        "intensity_level": level,
        "sensitivity_level": level,
        "radius_km": radius_km,
        "cluster_min_points": min_points,
        "total_deposits": total_deposits,
        "spatial_classification": spatial_classification,
        "spatial_pattern": spatial_pattern,
        "avg_nearest_distance_km": round(avg_nearest_m / 1000.0, 3),
        "clusters": clusters,
        "top_regions": top_regions,
        "heat_points": heat_points,
        "points_geojson": points_geojson,
        "explanation": _tr(
            lang,
            "Las zonas resaltadas representan concentraciones espaciales de registros mineralogicos "
            "asociados al mineral seleccionado.",
            "Highlighted areas represent spatial concentrations of mineralogical records "
            "associated with the selected mineral.",
        ),
    }, lang)

