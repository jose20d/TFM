"use client";

import { useEffect } from "react";
import { CircleMarker, MapContainer, TileLayer, Tooltip, useMap } from "react-leaflet";
import L from "leaflet";

const DEFAULT_CENTER = [15, -20];
const DEFAULT_ZOOM = 2;

function formatNumber(value, decimals = 0, locale = "es-ES") {
  if (value === null || value === undefined) return "N/A";
  const num = Number(value);
  if (!Number.isFinite(num)) return "N/A";
  return new Intl.NumberFormat(locale, {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  }).format(num);
}

function SpatialAutoZoom({ rows }) {
  const map = useMap();

  useEffect(() => {
    const points = (rows || [])
      .map((row) => [Number(row.lat), Number(row.lng)])
      .filter(([lat, lng]) => Number.isFinite(lat) && Number.isFinite(lng));

    if (!points.length) {
      map.setView(DEFAULT_CENTER, DEFAULT_ZOOM, { animate: true });
      return;
    }
    if (points.length === 1) {
      map.setView(points[0], 7, { animate: true });
      return;
    }
    map.fitBounds(L.latLngBounds(points), { padding: [24, 24], maxZoom: 8, animate: true });
  }, [map, rows]);

  return null;
}

export default function SpatialResultsMap({ rows, lang = "es" }) {
  const locale = lang === "en" ? "en-US" : "es-ES";
  const tr = (es, en) => (lang === "en" ? en : es);
  return (
    <MapContainer center={DEFAULT_CENTER} zoom={DEFAULT_ZOOM} style={{ height: "100%", width: "100%" }}>
      <TileLayer
        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
        url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
      />
      <SpatialAutoZoom rows={rows} />
      {(rows || []).map((row, idx) => {
        const lat = Number(row.lat);
        const lng = Number(row.lng);
        if (!Number.isFinite(lat) || !Number.isFinite(lng)) return null;
        return (
          <CircleMarker
            key={`spatial-map-${idx}-${row.deposit}`}
            center={[lat, lng]}
            radius={5}
            pathOptions={{
              color: "#2563eb",
              fillColor: "#38bdf8",
              fillOpacity: 0.78,
              weight: 1.1,
            }}
          >
            <Tooltip direction="top" offset={[0, -2]}>
              <strong>{row.deposit}</strong>
              <br />
              {tr("Distancia", "Distance")}: {formatNumber(row.distance_km, 2, locale)} km
              <br />
              {tr("Minerales", "Minerals")}: {Array.isArray(row.minerals) ? row.minerals.join(", ") || "N/A" : "N/A"}
            </Tooltip>
          </CircleMarker>
        );
      })}
    </MapContainer>
  );
}
