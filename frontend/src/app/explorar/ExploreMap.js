"use client";

import { useEffect, useRef } from "react";
import { CircleMarker, MapContainer, TileLayer, Tooltip, useMap } from "react-leaflet";
import L from "leaflet";
import "leaflet/dist/leaflet.css";

const DEFAULT_VIEW = [15, -20];
const DEFAULT_ZOOM = 2;

L.Icon.Default.mergeOptions({
  iconRetinaUrl: "https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon-2x.png",
  iconUrl: "https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon.png",
  shadowUrl: "https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png",
});

function formatNumber(value, locale = "es-ES") {
  if (value === null || value === undefined) return "N/A";
  const num = Number(value);
  if (!Number.isFinite(num)) return String(value);
  return new Intl.NumberFormat(locale).format(num);
}

function MapAutoZoom({ rows, countryIso, loading }) {
  const map = useMap();
  const lastCountryRef = useRef("__init__");
  const pendingZoomRef = useRef(true);

  useEffect(() => {
    const normalizedCountry = String(countryIso || "").toUpperCase();
    if (normalizedCountry !== lastCountryRef.current) {
      lastCountryRef.current = normalizedCountry;
      pendingZoomRef.current = true;
    }
    if (!pendingZoomRef.current || loading) return;
    if (!normalizedCountry) {
      map.setView(DEFAULT_VIEW, DEFAULT_ZOOM, { animate: true });
      pendingZoomRef.current = false;
      return;
    }
    if (!rows.length) return;

    const targetRows = rows.filter((item) => String(item.iso3 || "").toUpperCase() === normalizedCountry);
    if (!targetRows.length) return;

    const points = targetRows
      .map((item) => [Number(item.latitude), Number(item.longitude)])
      .filter(([lat, lon]) => Number.isFinite(lat) && Number.isFinite(lon));
    if (!points.length) return;

    if (points.length === 1) {
      map.setView(points[0], 6, { animate: true });
      pendingZoomRef.current = false;
      return;
    }

    const bounds = L.latLngBounds(points);
    map.fitBounds(bounds, { padding: [30, 30], maxZoom: 7, animate: true });
    pendingZoomRef.current = false;
  }, [map, rows, countryIso, loading]);

  return null;
}

export default function ExploreMap({ mapRows, countryIso, loading, lang }) {
  const locale = lang === "en" ? "en-US" : "es-ES";
  return (
    <MapContainer
      center={DEFAULT_VIEW}
      zoom={DEFAULT_ZOOM}
      scrollWheelZoom
      preferCanvas
      style={{ height: "100%", width: "100%" }}
    >
      <TileLayer
        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
        url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
      />
      <MapAutoZoom rows={mapRows} countryIso={countryIso} loading={loading} />
      {mapRows.map((item) => (
        <CircleMarker
          key={item.dep_id}
          center={[Number(item.latitude), Number(item.longitude)]}
          radius={4}
          pathOptions={{ color: "#2e86ff", fillColor: "#42c6b8", fillOpacity: 0.75, weight: 1 }}
        >
          <Tooltip direction="top" offset={[0, -2]}>
            <strong>{item.name || (lang === "en" ? "Deposit" : "Deposito")}</strong>
            <br />
            {lang === "en" ? "Country" : "Pais"}: {item.country_name} ({item.iso3 || "N/A"})
            <br />
            {lang === "en" ? "Minerals" : "Minerales"}: {item.minerals || "N/A"}
            <br />
            {lang === "en" ? "Coordinates" : "Coordenadas"}: {formatNumber(item.latitude, locale)}, {formatNumber(item.longitude, locale)}
            <br />
            {lang === "en" ? "Source" : "Fuente"}: MRDS
          </Tooltip>
        </CircleMarker>
      ))}
    </MapContainer>
  );
}
