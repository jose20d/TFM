#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any


try:
    import folium  # type: ignore
except ModuleNotFoundError as _e:  # pragma: no cover
    folium = None  # type: ignore
    _FOLIUM_IMPORT_ERROR = str(_e)
else:
    _FOLIUM_IMPORT_ERROR = None


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _to_float(x: Any) -> float | None:
    if x is None:
        return None
    if isinstance(x, (int, float)) and not (isinstance(x, float) and (math.isnan(x) or math.isinf(x))):
        return float(x)
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return None
        try:
            return float(s)
        except Exception:
            return None
    return None


def _norm_country_key(s: str) -> str:
    return " ".join(s.strip().lower().split())


def _get_str(d: dict[str, Any], *keys: str) -> str | None:
    for k in keys:
        v = d.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _get_country_name_from_wgi_record(r: dict[str, Any]) -> str | None:
    c = r.get("country")
    if isinstance(c, dict):
        v = c.get("value")
        if isinstance(v, str) and v.strip():
            return v.strip()
    if isinstance(c, str) and c.strip():
        return c.strip()
    return None


def _get_year_from_wgi_record(r: dict[str, Any]) -> int | None:
    d = r.get("date")
    if isinstance(d, int):
        return d
    if isinstance(d, str):
        s = d.strip()
        if s.isdigit():
            try:
                return int(s)
            except Exception:
                return None
    return None


@dataclass(frozen=True)
class WgiLatest:
    country_name: str
    year: int
    value: float | None


def _build_wgi_latest_lookup(wgi_records: list[dict[str, Any]]) -> tuple[dict[str, WgiLatest], dict[str, WgiLatest]]:
    """
    Returns two lookups:
    - by normalized country name (country.value)
    - by ISO3 code (countryiso3code)
    """
    by_name: dict[str, WgiLatest] = {}
    by_iso3: dict[str, WgiLatest] = {}

    for r in wgi_records:
        if not isinstance(r, dict):
            continue
        country_name = _get_country_name_from_wgi_record(r)
        year = _get_year_from_wgi_record(r)
        if not country_name or year is None:
            continue
        value = _to_float(r.get("value"))
        latest = WgiLatest(country_name=country_name, year=year, value=value)

        key_name = _norm_country_key(country_name)
        prev = by_name.get(key_name)
        if prev is None or year > prev.year:
            by_name[key_name] = latest

        iso3 = r.get("countryiso3code")
        if isinstance(iso3, str) and iso3.strip():
            iso3_key = iso3.strip().upper()
            prev2 = by_iso3.get(iso3_key)
            if prev2 is None or year > prev2.year:
                by_iso3[iso3_key] = latest

    return by_name, by_iso3


def _pick_wgi_for_country(country: str, by_name: dict[str, WgiLatest], by_iso3: dict[str, WgiLatest]) -> WgiLatest | None:
    if not country:
        return None
    c = country.strip()
    if len(c) == 3 and c.isalpha() and c.upper() == c:
        return by_iso3.get(c)
    return by_name.get(_norm_country_key(c))


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="generate_demo_map", add_help=True)
    p.add_argument("--mrds", required=True, help="Path to ./data/demo/usgs_mrds/records_100.json")
    p.add_argument("--wgi", required=True, help="Path to ./data/demo/worldbank_wgi/records_100.json")
    p.add_argument("--out", required=True, help="Path to output HTML (e.g., ./output/demo_map.html)")
    args = p.parse_args(argv)

    out_html = Path(args.out).expanduser().resolve()
    out_dir = out_html.parent
    out_summary = out_dir / "demo_summary.json"

    if folium is None:
        msg = (
            "Missing dependency: folium. Activate your venv and install requirements.\n"
            "Example:\n"
            "  source .venv/bin/activate\n"
            "  pip install -r requirements.txt\n"
        )
        print(msg.strip(), file=sys.stderr)
        _write_json(
            out_summary,
            {"status": "failed", "error": {"code": "MISSING_DEPENDENCY", "message": msg.strip()}, "output_html_path": str(out_html)},
        )
        return 0

    print("[map] Reading local JSON inputs...")
    mrds_path = Path(args.mrds).expanduser().resolve()
    wgi_path = Path(args.wgi).expanduser().resolve()

    try:
        mrds_raw = _read_json(mrds_path)
        wgi_raw = _read_json(wgi_path)
    except Exception as exc:
        msg = f"Failed to read input JSON files: {exc}"
        print(msg, file=sys.stderr)
        _write_json(out_summary, {"status": "failed", "error": {"code": "INPUT_READ_ERROR", "message": msg}})
        return 0

    if not isinstance(mrds_raw, list):
        msg = f"MRDS JSON must be a list of objects. Got: {type(mrds_raw).__name__}"
        print(msg, file=sys.stderr)
        _write_json(out_summary, {"status": "failed", "error": {"code": "INVALID_INPUT_SCHEMA", "message": msg}})
        return 0
    if not isinstance(wgi_raw, list):
        msg = f"WGI JSON must be a list of objects. Got: {type(wgi_raw).__name__}"
        print(msg, file=sys.stderr)
        _write_json(out_summary, {"status": "failed", "error": {"code": "INVALID_INPUT_SCHEMA", "message": msg}})
        return 0

    print("[map] Building map and summary...")
    mrds_records: list[dict[str, Any]] = [r for r in mrds_raw if isinstance(r, dict)]
    wgi_records: list[dict[str, Any]] = [r for r in wgi_raw if isinstance(r, dict)]

    total_records_mrds_in = len(mrds_records)
    skipped_missing_coords = 0
    points: list[tuple[float, float, dict[str, Any]]] = []

    country_counts: Counter[str] = Counter()

    for r in mrds_records:
        lat = _to_float(r.get("latitude"))
        lon = _to_float(r.get("longitude"))
        if lat is None or lon is None:
            skipped_missing_coords += 1
            continue
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            skipped_missing_coords += 1
            continue

        country = _get_str(r, "country") or "Unknown"
        country_counts[country] += 1
        points.append((lat, lon, r))

    total_points_plotted = len(points)

    mrds_countries_top5 = [{"country": k, "count": v} for k, v in country_counts.most_common(5)]
    chosen_country_for_wgi = country_counts.most_common(1)[0][0] if country_counts else None

    by_name, by_iso3 = _build_wgi_latest_lookup(wgi_records)
    wgi_latest = _pick_wgi_for_country(chosen_country_for_wgi or "", by_name, by_iso3) if chosen_country_for_wgi else None

    # Create map centered on MRDS points (fit bounds), or a world view if none.
    m = folium.Map(location=[20.0, 0.0], zoom_start=2, tiles="OpenStreetMap")

    if points:
        min_lat = min(p[0] for p in points)
        max_lat = max(p[0] for p in points)
        min_lon = min(p[1] for p in points)
        max_lon = max(p[1] for p in points)
        m.fit_bounds([[min_lat, min_lon], [max_lat, max_lon]])

    # Add markers (top 100 already, but keep it safe)
    for lat, lon, r in points[:100]:
        name = _get_str(r, "dep_name", "site_name") or "Unknown site"
        country = _get_str(r, "country") or "Unknown"
        province = _get_str(r, "province")
        commod1 = _get_str(r, "commod1")
        commod2 = _get_str(r, "commod2")

        lines = [
            f"<b>{name}</b>",
            f"Country: {country}",
        ]
        if province:
            lines.append(f"Province: {province}")
        if commod1:
            lines.append(f"Primary commodity: {commod1}")
        if commod2:
            lines.append(f"Secondary commodity: {commod2}")
        popup_html = "<br/>".join(lines)

        folium.Marker(
            location=[lat, lon],
            popup=folium.Popup(popup_html, max_width=350),
            tooltip=name,
        ).add_to(m)

    wgi_text = "Not available"
    if chosen_country_for_wgi and wgi_latest is not None:
        wgi_text = f"{wgi_latest.country_name} — {wgi_latest.year}: {wgi_latest.value if wgi_latest.value is not None else 'null'}"
    elif chosen_country_for_wgi:
        wgi_text = f"{chosen_country_for_wgi}: Not available"

    # Simple, readable overlay box
    overlay_html = f"""
    <div style="
        position: fixed;
        top: 12px; right: 12px;
        z-index: 9999;
        background: rgba(255,255,255,0.92);
        padding: 12px 14px;
        border: 1px solid #ddd;
        border-radius: 8px;
        box-shadow: 0 2px 10px rgba(0,0,0,0.15);
        font-family: Arial, sans-serif;
        max-width: 360px;">
      <div style="font-size: 16px; font-weight: 700; margin-bottom: 6px;">Week 1 Demo — Mineral points + Governance indicator</div>
      <div style="font-size: 13px; margin-bottom: 8px;">
        <b>Legend:</b> Points = mineral occurrences (USGS MRDS)
      </div>
      <div style="font-size: 13px; margin-bottom: 8px;">
        <b>WGI PV.EST (World Bank):</b><br/>
        Most frequent MRDS country → <span style="font-family: monospace;">{wgi_text}</span>
      </div>
      <div style="font-size: 12px; color: #444;">
        Ethical note: Public sources; this does not indicate extraction areas.
      </div>
    </div>
    """
    m.get_root().html.add_child(folium.Element(overlay_html))

    out_dir.mkdir(parents=True, exist_ok=True)
    m.save(str(out_html))
    print(f"[map] HTML saved: {out_html}")

    summary = {
        "status": "success",
        "total_records_mrds_in": total_records_mrds_in,
        "total_points_plotted": total_points_plotted,
        "skipped_missing_coords": skipped_missing_coords,
        "mrds_countries_top5": mrds_countries_top5,
        "chosen_country_for_wgi": chosen_country_for_wgi,
        "wgi_latest_year": wgi_latest.year if wgi_latest else None,
        "wgi_latest_value": wgi_latest.value if wgi_latest else None,
        "output_html_path": str(out_html),
    }
    _write_json(out_summary, summary)
    print(f"[map] Summary saved: {out_summary}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # pragma: no cover
        # Never leak tracebacks in demo mode.
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(0)


