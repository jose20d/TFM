"""Shared explore limit calculations."""

from web.services.common.query_service import fetch_one


def get_explore_max_limit(country_iso3: str | None = None) -> int:
    iso3 = (country_iso3 or "").strip().upper()
    if iso3:
        row = fetch_one(
            """
            SELECT COUNT(DISTINCT d.dep_id) AS max_limit
            FROM mrds_deposit d
            JOIN mrds_location l ON l.dep_id = d.dep_id
            JOIN dim_country c ON c.country_id = l.country_id
            WHERE d.latitude IS NOT NULL
              AND d.longitude IS NOT NULL
              AND c.iso3 = %s
            """,
            (iso3,),
        )
        return max(1, int(row.get("max_limit") or 0))

    row = fetch_one(
        """
        SELECT COALESCE(MAX(total_deposits), 500) AS max_limit
        FROM (
            SELECT c.iso3, COUNT(DISTINCT d.dep_id) AS total_deposits
            FROM mrds_deposit d
            JOIN mrds_location l ON l.dep_id = d.dep_id
            JOIN dim_country c ON c.country_id = l.country_id
            WHERE d.latitude IS NOT NULL
              AND d.longitude IS NOT NULL
              AND c.iso3 IS NOT NULL
            GROUP BY c.iso3
        ) t
        """
    )
    return max(500, int(row.get("max_limit") or 500))

