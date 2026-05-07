"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import { Circle, CircleMarker, GeoJSON, MapContainer, TileLayer, Tooltip, useMap, useMapEvents } from "react-leaflet";
import L from "leaflet";
import "leaflet/dist/leaflet.css";
import styles from "./terreno.module.css";

L.Icon.Default.mergeOptions({
  iconRetinaUrl: "https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon-2x.png",
  iconUrl: "https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon.png",
  shadowUrl: "https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png",
});

const DEFAULT_VIEW = [15, -20];
const DEFAULT_ZOOM = 2;

function PlaceholderSelect({ options }) {
  return (
    <select>
      {options.map((option) => (
        <option key={option.value} value={option.value}>
          {option.label}
        </option>
      ))}
    </select>
  );
}

function formatNumber(value, decimals = 0) {
  if (value === null || value === undefined) return "N/A";
  const num = Number(value);
  if (!Number.isFinite(num)) return "N/A";
  return new Intl.NumberFormat("es-ES", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  }).format(num);
}

function toFeature(geometry) {
  if (!geometry || typeof geometry !== "object") return null;
  return { type: "Feature", geometry, properties: {} };
}

function markerStyleForScore(score) {
  if (score >= 50) {
    return { color: "#ef4444", fillColor: "#f97316", fillOpacity: 0.8, weight: 1.2, radius: 7 };
  }
  if (score >= 20) {
    return { color: "#f59e0b", fillColor: "#facc15", fillOpacity: 0.75, weight: 1.2, radius: 6 };
  }
  return { color: "#2563eb", fillColor: "#38bdf8", fillOpacity: 0.7, weight: 1, radius: 5 };
}

function CorridorAutoZoom({ points, focusMode, trigger }) {
  const map = useMap();
  const lastTriggerRef = useRef(0);

  useEffect(() => {
    if (trigger === lastTriggerRef.current) return;
    lastTriggerRef.current = trigger;

    if (!points.length) {
      map.setView(DEFAULT_VIEW, DEFAULT_ZOOM, { animate: true });
      return;
    }
    if (points.length === 1) {
      map.setView(points[0], 6, { animate: true });
      return;
    }
    const bounds = L.latLngBounds(points);
    const isCorridorFocus = focusMode === "corridor";
    map.fitBounds(bounds, {
      padding: isCorridorFocus ? [45, 45] : [30, 30],
      maxZoom: isCorridorFocus ? 9 : 8,
      animate: true,
    });
  }, [map, points, focusMode, trigger]);

  return null;
}

function ZoneMapPicker({ enabled, onSelect }) {
  useMapEvents({
    click(event) {
      if (!enabled) return;
      onSelect({
        lat: Number(event.latlng.lat),
        lng: Number(event.latlng.lng),
      });
    },
  });
  return null;
}

function ZoneAutoZoom({ points, center, radiusKm, countryIso, trigger }) {
  const map = useMap();
  const lastTriggerRef = useRef(-1);

  useEffect(() => {
    if (trigger === lastTriggerRef.current) return;
    lastTriggerRef.current = trigger;

    if (!countryIso) {
      map.setView(DEFAULT_VIEW, DEFAULT_ZOOM, { animate: true });
      return;
    }

    if (center && Number.isFinite(center.lat) && Number.isFinite(center.lng) && radiusKm > 0) {
      const centerLatLng = L.latLng(center.lat, center.lng);
      const bounds = centerLatLng.toBounds(Math.max(1000, radiusKm * 2000));
      map.fitBounds(bounds, { padding: [35, 35], maxZoom: 10, animate: true });
      return;
    }

    if (!points.length) {
      map.setView(DEFAULT_VIEW, DEFAULT_ZOOM, { animate: true });
      return;
    }
    if (points.length === 1) {
      map.setView(points[0], 6, { animate: true });
      return;
    }

    // Use robust bounds to avoid outlier points keeping the map at world scale.
    const lats = points.map((point) => Number(point[0])).filter((value) => Number.isFinite(value));
    const lngs = points.map((point) => Number(point[1])).filter((value) => Number.isFinite(value));
    const sortedLats = [...lats].sort((a, b) => a - b);
    const sortedLngs = [...lngs].sort((a, b) => a - b);
    const q05Idx = Math.floor((sortedLats.length - 1) * 0.05);
    const q95Idx = Math.ceil((sortedLats.length - 1) * 0.95);
    const minLat = sortedLats[q05Idx];
    const maxLat = sortedLats[q95Idx];
    const minLng = sortedLngs[q05Idx];
    const maxLng = sortedLngs[q95Idx];
    const robustPoints = points.filter(([lat, lng]) => (
      Number(lat) >= minLat
      && Number(lat) <= maxLat
      && Number(lng) >= minLng
      && Number(lng) <= maxLng
    ));

    const bounds = L.latLngBounds(robustPoints.length >= 2 ? robustPoints : points);
    map.fitBounds(bounds, { padding: [30, 30], maxZoom: 8, animate: true });
  }, [map, points, center, radiusKm, countryIso, trigger]);

  return null;
}

export default function TerrenoClient() {
  const [activeTool, setActiveTool] = useState("corridor");
  const [countries, setCountries] = useState([]);
  const [countryIso, setCountryIso] = useState("");
  const [countryDeposits, setCountryDeposits] = useState([]);
  const [countryLoading, setCountryLoading] = useState(false);
  const [countryError, setCountryError] = useState("");
  const [selectedFromId, setSelectedFromId] = useState(null);
  const [selectedToId, setSelectedToId] = useState(null);
  const [widthKm, setWidthKm] = useState(2);
  const [analysisLoading, setAnalysisLoading] = useState(false);
  const [analysisError, setAnalysisError] = useState("");
  const [corridorResult, setCorridorResult] = useState(null);
  const analysisSeqRef = useRef(0);
  const [autoZoomTrigger, setAutoZoomTrigger] = useState(0);
  const [zoneCountryIso, setZoneCountryIso] = useState("");
  const [zoneCountryDeposits, setZoneCountryDeposits] = useState([]);
  const [zoneCountryLoading, setZoneCountryLoading] = useState(false);
  const [zoneCountryError, setZoneCountryError] = useState("");
  const [zoneCenter, setZoneCenter] = useState(null);
  const [zoneRadiusKm, setZoneRadiusKm] = useState(10);
  const [zoneAnalysisLoading, setZoneAnalysisLoading] = useState(false);
  const [zoneAnalysisError, setZoneAnalysisError] = useState("");
  const [zoneResult, setZoneResult] = useState(null);
  const [zoneAutoZoomTrigger, setZoneAutoZoomTrigger] = useState(0);
  const zoneSeqRef = useRef(0);

  const toolTabs = useMemo(
    () => [
      { id: "corridor", label: "Corredor entre depositos" },
      { id: "zone", label: "Zona de interes" },
      { id: "minerals", label: "Minerales frecuentes" },
      { id: "potential", label: "Potencial exploratorio" },
    ],
    [],
  );

  useEffect(() => {
    fetch("/api/backend/api/v1/countries?limit=300", { cache: "no-store" })
      .then((response) => {
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        return response.json();
      })
      .then((data) => setCountries(Array.isArray(data) ? data : []))
      .catch(() => setCountries([]));
  }, []);

  const countryLabel = useMemo(() => {
    const match = countries.find((country) => country.iso3 === countryIso);
    return match?.country_name || "";
  }, [countries, countryIso]);
  const zoneCountryLabel = useMemo(() => {
    const match = countries.find((country) => country.iso3 === zoneCountryIso);
    return match?.country_name || "";
  }, [countries, zoneCountryIso]);

  useEffect(() => {
    setCountryDeposits([]);
    setCountryError("");
    setSelectedFromId(null);
    setSelectedToId(null);
    setCorridorResult(null);
    setAnalysisError("");

    if (!countryIso) return;

    const controller = new AbortController();
    setCountryLoading(true);
    fetch(`/api/backend/api/v1/explore/deposits?country_iso3=${countryIso}&limit=5000`, {
      cache: "no-store",
      signal: controller.signal,
    })
      .then((response) => {
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        return response.json();
      })
      .then((rows) => {
        const parsed = Array.isArray(rows)
          ? rows
              .map((item) => ({
                dep_id: Number(item.dep_id),
                name: item.name || `Dep. ${item.dep_id}`,
                latitude: Number(item.latitude),
                longitude: Number(item.longitude),
                minerals: String(item.minerals || "")
                  .split(",")
                  .map((value) => value.trim())
                  .filter(Boolean),
              }))
              .filter(
                (item) =>
                  Number.isFinite(item.dep_id) &&
                  Number.isFinite(item.latitude) &&
                  Number.isFinite(item.longitude),
              )
          : [];
        setCountryDeposits(parsed);
      })
      .catch((err) => {
        if (err?.name === "AbortError") return;
        setCountryError(err?.message || "No fue posible cargar depositos del pais seleccionado.");
      })
      .finally(() => setCountryLoading(false));

    return () => controller.abort();
  }, [countryIso]);

  useEffect(() => {
    setZoneCountryDeposits([]);
    setZoneCountryError("");
    setZoneCenter(null);
    setZoneRadiusKm(10);
    setZoneResult(null);
    setZoneAnalysisError("");
    zoneSeqRef.current += 1;

    if (!zoneCountryIso) return;

    const controller = new AbortController();
    setZoneCountryLoading(true);
    fetch(`/api/backend/api/v1/explore/deposits?country_iso3=${zoneCountryIso}&limit=5000`, {
      cache: "no-store",
      signal: controller.signal,
    })
      .then((response) => {
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        return response.json();
      })
      .then((rows) => {
        const parsed = Array.isArray(rows)
          ? rows
              .map((item) => ({
                dep_id: Number(item.dep_id),
                name: item.name || `Dep. ${item.dep_id}`,
                latitude: Number(item.latitude),
                longitude: Number(item.longitude),
                minerals: String(item.minerals || "")
                  .split(",")
                  .map((value) => value.trim())
                  .filter(Boolean),
              }))
              .filter(
                (item) =>
                  Number.isFinite(item.dep_id) &&
                  Number.isFinite(item.latitude) &&
                  Number.isFinite(item.longitude),
              )
          : [];
        setZoneCountryDeposits(parsed);
        setZoneAutoZoomTrigger((value) => value + 1);
      })
      .catch((err) => {
        if (err?.name === "AbortError") return;
        setZoneCountryError(err?.message || "No fue posible cargar depositos del pais seleccionado.");
      })
      .finally(() => setZoneCountryLoading(false));

    return () => controller.abort();
  }, [zoneCountryIso]);

  const depositsById = useMemo(() => {
    const map = new Map();
    countryDeposits.forEach((deposit) => map.set(deposit.dep_id, deposit));
    return map;
  }, [countryDeposits]);

  const selectedFrom = selectedFromId ? depositsById.get(selectedFromId) || null : null;
  const selectedTo = selectedToId ? depositsById.get(selectedToId) || null : null;

  const mapPoints = useMemo(() => {
    if (corridorResult?.deposits_in_corridor?.length) {
      return corridorResult.deposits_in_corridor
        .map((item) => [Number(item.lat), Number(item.lng)])
        .filter(([lat, lng]) => Number.isFinite(lat) && Number.isFinite(lng));
    }
    if (selectedFrom && selectedTo) {
      return [
        [selectedFrom.latitude, selectedFrom.longitude],
        [selectedTo.latitude, selectedTo.longitude],
      ];
    }
    return countryDeposits.map((item) => [item.latitude, item.longitude]);
  }, [countryDeposits, corridorResult, selectedFrom, selectedTo]);

  const zoomFocusMode = corridorResult?.deposits_in_corridor?.length ? "corridor" : "country";

  const corridorFeature = useMemo(() => toFeature(corridorResult?.corridor_geojson), [corridorResult]);
  const lineFeature = useMemo(() => toFeature(corridorResult?.line_geojson), [corridorResult]);
  const corridorGeoKey = useMemo(
    () => JSON.stringify(corridorResult?.corridor_geojson || {}),
    [corridorResult],
  );
  const lineGeoKey = useMemo(() => JSON.stringify(corridorResult?.line_geojson || {}), [corridorResult]);
  const corridorDepositsById = useMemo(() => {
    const map = new Map();
    (corridorResult?.deposits_in_corridor || []).forEach((deposit) => {
      map.set(Number(deposit.dep_id), deposit);
    });
    return map;
  }, [corridorResult]);

  function clearSelection() {
    analysisSeqRef.current += 1;
    setSelectedFromId(null);
    setSelectedToId(null);
    setWidthKm(2);
    setCorridorResult(null);
    setAnalysisError("");
    setAutoZoomTrigger((value) => value + 1);
  }

  useEffect(() => {
    if (activeTool !== "corridor") return;
    if (!selectedFromId || !selectedToId) return;
    if (selectedFromId === selectedToId) return;
    setAutoZoomTrigger((value) => value + 1);
  }, [activeTool, selectedFromId, selectedToId]);

  useEffect(() => {
    if (activeTool !== "corridor") return;
    if (!countryIso) return;
    if (countryLoading) return;
    if (!countryDeposits.length) return;
    if (selectedFromId || selectedToId) return;
    // Keep initial country auto-zoom behavior when no endpoints are selected yet.
    setAutoZoomTrigger((value) => value + 1);
  }, [activeTool, countryIso, countryLoading, countryDeposits.length, selectedFromId, selectedToId]);

  function handleDepositClick(deposit) {
    if (!deposit) return;
    setAnalysisError("");
    setCorridorResult(null);

    if (selectedFromId === deposit.dep_id) {
      setSelectedFromId(null);
      return;
    }
    if (selectedToId === deposit.dep_id) {
      setSelectedToId(null);
      return;
    }
    if (!selectedFromId) {
      setSelectedFromId(deposit.dep_id);
      return;
    }
    if (!selectedToId) {
      setSelectedToId(deposit.dep_id);
      return;
    }
    // Reuse the last selected endpoint as the new starting point.
    setSelectedFromId(selectedToId);
    setSelectedToId(deposit.dep_id);
  }

  const analyzeCorridor = useCallback(async () => {
    if (!countryIso) {
      setAnalysisError("Selecciona un pais para iniciar el analisis de corredor.");
      return;
    }
    if (!selectedFromId || !selectedToId) {
      setAnalysisError("Selecciona dos depositos distintos sobre el mapa.");
      return;
    }
    if (selectedFromId === selectedToId) {
      setAnalysisError("Deposito A y Deposito B no pueden ser el mismo.");
      return;
    }

    const qs = new URLSearchParams({
      country_iso3: countryIso,
      from_dep_id: String(selectedFromId),
      to_dep_id: String(selectedToId),
      width_km: String(widthKm),
    });

    const requestId = analysisSeqRef.current + 1;
    analysisSeqRef.current = requestId;

    setAnalysisLoading(true);
    setAnalysisError("");
    try {
      const response = await fetch(`/api/backend/api/v1/terrain/corridor?${qs.toString()}`, {
        cache: "no-store",
      });
      const payload = await response.json();
      if (requestId !== analysisSeqRef.current) return;
      if (!response.ok) {
        throw new Error(payload?.detail || `HTTP ${response.status}`);
      }
      setCorridorResult(payload);
    } catch (error) {
      if (requestId !== analysisSeqRef.current) return;
      setCorridorResult(null);
      setAnalysisError(error?.message || "No fue posible ejecutar el analisis del corredor.");
    } finally {
      if (requestId !== analysisSeqRef.current) return;
      setAnalysisLoading(false);
    }
  }, [countryIso, selectedFromId, selectedToId, widthKm]);

  useEffect(() => {
    if (activeTool !== "corridor") return;
    if (!countryIso || !selectedFromId || !selectedToId) return;
    if (selectedFromId === selectedToId) return;

    const timeoutId = setTimeout(() => {
      void analyzeCorridor();
    }, 400);

    return () => clearTimeout(timeoutId);
  }, [activeTool, countryIso, selectedFromId, selectedToId, widthKm, analyzeCorridor]);

  const zoneFeature = useMemo(() => toFeature(zoneResult?.zone_geojson), [zoneResult]);
  const zoneGeoKey = useMemo(() => JSON.stringify(zoneResult?.zone_geojson || {}), [zoneResult]);
  const zoneDepositsById = useMemo(() => {
    const map = new Map();
    (zoneResult?.deposits || []).forEach((deposit) => {
      map.set(Number(deposit.dep_id), deposit);
    });
    return map;
  }, [zoneResult]);
  const zoneMapPoints = useMemo(
    () => zoneCountryDeposits.map((item) => [item.latitude, item.longitude]),
    [zoneCountryDeposits],
  );

  function clearZone() {
    zoneSeqRef.current += 1;
    setZoneCenter(null);
    setZoneRadiusKm(10);
    setZoneResult(null);
    setZoneAnalysisError("");
    setZoneAutoZoomTrigger((value) => value + 1);
  }

  const analyzeZone = useCallback(async () => {
    if (!zoneCountryIso) {
      setZoneAnalysisError("Selecciona un pais para comenzar la exploracion.");
      return;
    }
    if (!zoneCenter) {
      setZoneAnalysisError("Selecciona una zona haciendo clic en el mapa.");
      return;
    }

    const qs = new URLSearchParams({
      country_iso3: zoneCountryIso,
      lat: String(zoneCenter.lat),
      lng: String(zoneCenter.lng),
      radius_km: String(zoneRadiusKm),
    });

    const requestId = zoneSeqRef.current + 1;
    zoneSeqRef.current = requestId;
    setZoneAnalysisLoading(true);
    setZoneAnalysisError("");
    try {
      const response = await fetch(`/api/backend/api/v1/terrain/zone-interest?${qs.toString()}`, {
        cache: "no-store",
      });
      const payload = await response.json();
      if (requestId !== zoneSeqRef.current) return;
      if (!response.ok) {
        throw new Error(payload?.detail || `HTTP ${response.status}`);
      }
      setZoneResult(payload);
    } catch (error) {
      if (requestId !== zoneSeqRef.current) return;
      setZoneResult(null);
      setZoneAnalysisError(error?.message || "No fue posible ejecutar el analisis de zona.");
    } finally {
      if (requestId !== zoneSeqRef.current) return;
      setZoneAnalysisLoading(false);
    }
  }, [zoneCountryIso, zoneCenter, zoneRadiusKm]);

  useEffect(() => {
    if (activeTool !== "zone") return;
    if (!zoneCountryIso || !zoneCenter) return;
    const timeoutId = setTimeout(() => {
      void analyzeZone();
    }, 350);
    return () => clearTimeout(timeoutId);
  }, [activeTool, zoneCountryIso, zoneCenter, zoneRadiusKm, analyzeZone]);

  function renderActiveTool() {
    if (activeTool === "corridor") {
      return (
        <>
          <div className={styles.corridorIntroRow}>
            <p className="muted">
              Selecciona pais y dos depositos (A/B). El sistema analiza automaticamente los depositos y
              minerales dentro del corredor.
            </p>
            <button type="button" className={styles.secondaryBtn} onClick={clearSelection}>
              Limpiar seleccion
            </button>
          </div>
          <div className={styles.controls}>
            <label>
              Pais
              <select value={countryIso} onChange={(event) => setCountryIso(event.target.value)}>
                <option value="">Seleccionar pais</option>
                {countries.map((country) => (
                  <option key={`${country.country_name}-${country.iso3}`} value={country.iso3 || ""}>
                    {country.country_name} ({country.iso3 || "N/A"})
                  </option>
                ))}
              </select>
            </label>
            <label>
              Ancho del corredor (km)
              <input
                type="range"
                min={1}
                max={50}
                step={1}
                value={widthKm}
                disabled={!selectedFromId || !selectedToId}
                onChange={(event) => setWidthKm(Number(event.target.value) || 2)}
              />
              <span className={styles.rangeValue}>{formatNumber(widthKm)} km</span>
            </label>
          </div>

          <div className={styles.selectionInfo}>
            <p>
              <strong>Pais:</strong> {countryLabel || "Sin seleccionar"}
            </p>
            <p>
              <strong>Deposito A:</strong> {selectedFrom?.name || "Haz clic en un marcador"}
            </p>
            <p>
              <strong>Deposito B:</strong> {selectedTo?.name || "Haz clic en otro marcador"}
            </p>
            <p className="muted">Los depositos A y B solo definen los extremos del corredor.</p>
          </div>

          {(countryError || analysisError) && (
            <div className={styles.messageBox}>{countryError || analysisError}</div>
          )}
          {countryLoading && <p className="muted">Cargando depositos georreferenciados...</p>}
          {!countryLoading && analysisLoading && (
            <p className="muted">Analizando corredor automaticamente...</p>
          )}
          {!countryLoading && countryIso && !countryDeposits.length && !countryError && (
            <p className="muted">No hay depositos georreferenciados para este pais.</p>
          )}

          <div className={styles.corridorLayout}>
            <article className={styles.mapCard}>
              <h4>Mapa del corredor</h4>
              <div className={styles.mapWrap}>
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
                  <CorridorAutoZoom
                    points={mapPoints}
                    focusMode={zoomFocusMode}
                    trigger={autoZoomTrigger}
                  />
                  {corridorFeature && (
                    <GeoJSON
                      key={`corridor-${corridorGeoKey}`}
                      data={corridorFeature}
                      style={() => ({
                        color: "#2563eb",
                        weight: 1.5,
                        fillColor: "#60a5fa",
                        fillOpacity: 0.15,
                      })}
                    />
                  )}
                  {lineFeature && (
                    <GeoJSON
                      key={`line-${lineGeoKey}`}
                      data={lineFeature}
                      style={() => ({
                        color: "#f97316",
                        weight: 3,
                        opacity: 0.95,
                      })}
                    />
                  )}
                  {countryDeposits.map((deposit) => {
                    const inCorridor = corridorDepositsById.get(deposit.dep_id);
                    let markerStyle = markerStyleForScore(Number(inCorridor?.intensity_score || 0));

                    if (deposit.dep_id === selectedFromId) {
                      markerStyle = {
                        color: "#991b1b",
                        fillColor: "#ef4444",
                        fillOpacity: 0.95,
                        weight: 2,
                        radius: 8,
                      };
                    } else if (deposit.dep_id === selectedToId) {
                      markerStyle = {
                        color: "#854d0e",
                        fillColor: "#facc15",
                        fillOpacity: 0.95,
                        weight: 2,
                        radius: 8,
                      };
                    } else if (!inCorridor) {
                      markerStyle = {
                        color: "#1d4ed8",
                        fillColor: "#60a5fa",
                        fillOpacity: 0.45,
                        weight: 1,
                        radius: 4,
                      };
                    }

                    return (
                      <CircleMarker
                        key={`dep-${deposit.dep_id}`}
                        center={[deposit.latitude, deposit.longitude]}
                        radius={markerStyle.radius}
                        pathOptions={markerStyle}
                        eventHandlers={{ click: () => handleDepositClick(deposit) }}
                      >
                        <Tooltip direction="top" offset={[0, -2]}>
                          <strong>{deposit.name}</strong>
                          <br />
                          ID: {deposit.dep_id}
                          <br />
                          {inCorridor ? `Intensidad: ${formatNumber(inCorridor.intensity_score, 2)}` : "Fuera del corredor"}
                        </Tooltip>
                      </CircleMarker>
                    );
                  })}
                </MapContainer>
              </div>
              <p className="muted">
                Este analisis se basa en ocurrencias registradas y proximidad espacial. No garantiza la
                presencia de minerales en campo.
              </p>
            </article>

            <article className={styles.resultsCard}>
              <h4>Resumen mineralogico del corredor</h4>
              {!corridorResult && (
                <p className="muted">Selecciona dos depositos para ejecutar el analisis automaticamente.</p>
              )}
              {corridorResult && (
                <>
                  <div className={styles.kpisGrid}>
                    <div>
                      <strong>Distancia A-B</strong>
                      <p>{formatNumber(corridorResult.distance_km, 2)} km</p>
                    </div>
                    <div>
                      <strong>Ancho del corredor</strong>
                      <p>{formatNumber(corridorResult.width_km, 0)} km</p>
                    </div>
                    <div>
                      <strong>Depositos en corredor</strong>
                      <p>{formatNumber(corridorResult.deposit_count)}</p>
                    </div>
                  </div>

                  <section className={styles.resultSection}>
                    <h5>Minerales comunes entre A y B (dato adicional)</h5>
                    {corridorResult.common_endpoint_minerals?.length ? (
                      <div className={styles.badgesRow}>
                        {corridorResult.common_endpoint_minerals.map((mineral) => (
                          <span key={`common-${mineral}`} className={styles.commonBadge}>
                            {mineral}
                          </span>
                        ))}
                      </div>
                    ) : (
                      <p className="muted">No se detectaron minerales comunes entre los extremos.</p>
                    )}
                  </section>

                  <section className={styles.resultSection}>
                    <h5>Ranking de minerales del corredor</h5>
                    <p className="muted">
                      La intensidad se calcula segun la frecuencia del mineral dentro de los depositos
                      encontrados en el corredor.
                    </p>
                    {corridorResult.corridor_minerals?.length ? (
                      <ul className={styles.rankingList}>
                        {corridorResult.corridor_minerals.map((item) => (
                          <li key={`ranking-${item.mineral}`}>
                            <span>{item.mineral}</span>
                            <span>{formatNumber(item.count)} deps</span>
                            <span>{formatNumber(item.percentage, 2)}%</span>
                            <span className={styles[`intensity-${item.intensity}`]}>{item.intensity}</span>
                          </li>
                        ))}
                      </ul>
                    ) : (
                      <p className="muted">No se detectaron minerales en el corredor.</p>
                    )}
                  </section>

                  <section className={styles.resultSection}>
                    <h5>Depositos dentro del corredor</h5>
                    <div className={styles.depositsScroll}>
                      <ul className={styles.depositsList}>
                        {corridorResult.deposits_in_corridor?.map((deposit) => (
                          <li key={`corridor-dep-${deposit.dep_id}`}>
                            <strong>{deposit.name}</strong> ({deposit.dep_id}) -{" "}
                            {formatNumber(deposit.distance_to_axis_km, 2)} km al eje - Intensidad{" "}
                            {formatNumber(deposit.intensity_score, 2)} - Minerales:{" "}
                            {deposit.minerals?.length ? deposit.minerals.join(", ") : "N/A"}
                          </li>
                        ))}
                      </ul>
                    </div>
                  </section>
                </>
              )}
            </article>
          </div>
        </>
      );
    }

    if (activeTool === "zone") {
      const nearestDeposit = zoneResult?.deposits?.[0] || null;
      return (
        <>
          <div className={styles.zoneIntroRow}>
            <p className="muted">
              Selecciona pais y luego haz clic en el mapa para definir el centro de la zona de interes.
            </p>
            <button type="button" className={styles.secondaryBtn} onClick={clearZone}>
              Limpiar zona
            </button>
          </div>
          <div className={styles.controls}>
            <label>
              Pais
              <select value={zoneCountryIso} onChange={(event) => setZoneCountryIso(event.target.value)}>
                <option value="">Seleccionar pais</option>
                {countries.map((country) => (
                  <option key={`zone-${country.country_name}-${country.iso3}`} value={country.iso3 || ""}>
                    {country.country_name} ({country.iso3 || "N/A"})
                  </option>
                ))}
              </select>
            </label>
            <label>
              Radio de busqueda (km)
              <input
                type="range"
                min={1}
                max={50}
                step={1}
                value={zoneRadiusKm}
                disabled={!zoneCountryIso}
                onChange={(event) => setZoneRadiusKm(Number(event.target.value) || 10)}
              />
              <span className={styles.rangeValue}>Radio actual: {formatNumber(zoneRadiusKm)} km</span>
            </label>
          </div>

          <div className={styles.selectionInfo}>
            <p>
              <strong>Pais:</strong> {zoneCountryLabel || "Sin seleccionar"}
            </p>
            <p>
              <strong>Centro de zona:</strong>{" "}
              {zoneCenter
                ? `${formatNumber(zoneCenter.lat, 4)}, ${formatNumber(zoneCenter.lng, 4)}`
                : "Haz clic en el mapa para seleccionar un punto"}
            </p>
          </div>

          {(zoneCountryError || zoneAnalysisError) && (
            <div className={styles.messageBox}>{zoneCountryError || zoneAnalysisError}</div>
          )}
          {!zoneCountryIso && (
            <p className="muted">Selecciona un pais para comenzar la exploracion.</p>
          )}
          {zoneCountryLoading && <p className="muted">Cargando depositos georreferenciados...</p>}
          {!zoneCountryLoading && zoneCountryIso && !zoneCountryDeposits.length && !zoneCountryError && (
            <p className="muted">El pais seleccionado no tiene depositos georreferenciados.</p>
          )}
          {!zoneCountryLoading && zoneAnalysisLoading && (
            <p className="muted">Analizando zona automaticamente...</p>
          )}

          <div className={styles.corridorLayout}>
            <article className={styles.mapCard}>
              <h4>Mapa de zona de interes</h4>
              <div className={styles.mapWrap}>
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
                  <ZoneAutoZoom
                    points={zoneMapPoints}
                    center={zoneCenter}
                    radiusKm={zoneRadiusKm}
                    countryIso={zoneCountryIso}
                    trigger={zoneAutoZoomTrigger}
                  />
                  <ZoneMapPicker
                    enabled={Boolean(zoneCountryIso)}
                    onSelect={(center) => {
                      setZoneCenter(center);
                      setZoneResult(null);
                      setZoneAnalysisError("");
                      setZoneAutoZoomTrigger((value) => value + 1);
                    }}
                  />

                  {zoneCenter && (
                    <>
                      <Circle
                        center={[zoneCenter.lat, zoneCenter.lng]}
                        radius={zoneRadiusKm * 1000}
                        pathOptions={{
                          color: "#2563eb",
                          weight: 2,
                          fillColor: "#60a5fa",
                          fillOpacity: 0.16,
                        }}
                      />
                      <CircleMarker
                        center={[zoneCenter.lat, zoneCenter.lng]}
                        radius={7}
                        pathOptions={{
                          color: "#991b1b",
                          fillColor: "#ef4444",
                          fillOpacity: 0.95,
                          weight: 2,
                        }}
                      >
                        <Tooltip direction="top" offset={[0, -2]}>
                          <strong>Centro de zona</strong>
                          <br />
                          {formatNumber(zoneCenter.lat, 4)}, {formatNumber(zoneCenter.lng, 4)}
                        </Tooltip>
                      </CircleMarker>
                    </>
                  )}

                  {zoneFeature && (
                    <GeoJSON
                      key={`zone-${zoneGeoKey}`}
                      data={zoneFeature}
                      style={() => ({
                        color: "#2563eb",
                        weight: 1.5,
                        fillColor: "#60a5fa",
                        fillOpacity: 0.1,
                      })}
                    />
                  )}

                  {zoneCountryDeposits.map((deposit) => {
                    const inZone = zoneDepositsById.get(deposit.dep_id);
                    const mineralsCount = inZone?.minerals?.length || 0;
                    let markerStyle = {
                      color: "#1d4ed8",
                      fillColor: "#60a5fa",
                      fillOpacity: 0.45,
                      weight: 1,
                      radius: 4,
                    };
                    if (inZone) {
                      if (mineralsCount >= 5) {
                        markerStyle = {
                          color: "#b45309",
                          fillColor: "#f59e0b",
                          fillOpacity: 0.85,
                          weight: 1.5,
                          radius: 7,
                        };
                      } else if (mineralsCount >= 2) {
                        markerStyle = {
                          color: "#1d4ed8",
                          fillColor: "#38bdf8",
                          fillOpacity: 0.8,
                          weight: 1.2,
                          radius: 6,
                        };
                      } else {
                        markerStyle = {
                          color: "#0f766e",
                          fillColor: "#14b8a6",
                          fillOpacity: 0.78,
                          weight: 1.2,
                          radius: 5,
                        };
                      }
                    }

                    return (
                      <CircleMarker
                        key={`zone-dep-${deposit.dep_id}`}
                        center={[deposit.latitude, deposit.longitude]}
                        radius={markerStyle.radius}
                        pathOptions={markerStyle}
                      >
                        <Tooltip direction="top" offset={[0, -2]}>
                          <strong>{deposit.name}</strong>
                          <br />
                          {inZone
                            ? `En zona: ${formatNumber(inZone.distance_km, 2)} km al centro`
                            : "Fuera de la zona"}
                        </Tooltip>
                      </CircleMarker>
                    );
                  })}
                </MapContainer>
              </div>
              <p className="muted">
                Este analisis se basa en registros mineralogicos y proximidad espacial. No garantiza la
                presencia de minerales en campo.
              </p>
            </article>

            <article className={styles.resultsCard}>
              <h4>Resumen de zona</h4>
              {!zoneResult && (
                <p className="muted">
                  Selecciona una zona en el mapa para ejecutar el analisis automaticamente.
                </p>
              )}
              {zoneResult && (
                <>
                  <div className={styles.kpisGrid}>
                    <div>
                      <strong>Depositos encontrados</strong>
                      <p>{formatNumber(zoneResult.deposit_count)}</p>
                    </div>
                    <div>
                      <strong>Radio</strong>
                      <p>{formatNumber(zoneResult.radius_km, 0)} km</p>
                    </div>
                    <div>
                      <strong>Deposito mas cercano</strong>
                      <p>{nearestDeposit ? nearestDeposit.name : "N/A"}</p>
                    </div>
                  </div>

                  {zoneResult.message && <p className="muted">{zoneResult.message}</p>}

                  <section className={styles.resultSection}>
                    <h5>Ranking de minerales</h5>
                    <p className="muted">
                      La intensidad representa la frecuencia del mineral dentro de los depositos encontrados
                      en la zona seleccionada.
                    </p>
                    {zoneResult.minerals?.length ? (
                      <ul className={styles.rankingList}>
                        {zoneResult.minerals.map((item) => (
                          <li key={`zone-ranking-${item.mineral}`}>
                            <span>{item.mineral}</span>
                            <span>{formatNumber(item.count)} deps</span>
                            <span>{formatNumber(item.percentage, 2)}%</span>
                            <span className={styles[`intensity-${item.intensity}`]}>{item.intensity}</span>
                          </li>
                        ))}
                      </ul>
                    ) : (
                      <p className="muted">No hay minerales registrados para la zona seleccionada.</p>
                    )}
                  </section>

                  <section className={styles.resultSection}>
                    <h5>Depositos dentro de la zona</h5>
                    <div className={styles.depositsScroll}>
                      <ul className={styles.depositsList}>
                        {(zoneResult.deposits || []).map((deposit) => (
                          <li key={`zone-result-${deposit.dep_id}`}>
                            <strong>{deposit.name}</strong> - {formatNumber(deposit.distance_km, 2)} km al
                            centro - Minerales:{" "}
                            {deposit.minerals?.length ? deposit.minerals.join(", ") : "N/A"}
                          </li>
                        ))}
                      </ul>
                    </div>
                  </section>
                </>
              )}
            </article>
          </div>
        </>
      );
    }

    if (activeTool === "minerals") {
      return (
        <>
          <h3>Minerales frecuentes</h3>
          <p className="muted">
            Esta subseccion mostrara los minerales mas frecuentes en la zona seleccionada, usando los
            registros existentes en la base de datos.
          </p>
          <div className={styles.placeholderList}>
            <p>1. Placeholder mineral A - n ocurrencias</p>
            <p>2. Placeholder mineral B - n ocurrencias</p>
            <p>3. Placeholder mineral C - n ocurrencias</p>
          </div>
          <div className={styles.placeholderArea}>Espacio reservado para grafico o tabla de ranking.</div>
        </>
      );
    }

    return (
      <>
        <h3>Potencial exploratorio</h3>
        <p className="muted">
          Visualizacion experimental para identificar zonas con mayor concentracion de registros
          mineralogicos.
        </p>
        <div className={styles.controls}>
          <label>
            Mineral objetivo
            <PlaceholderSelect
              options={[
                { value: "", label: "Seleccionar mineral" },
                { value: "gold", label: "Gold (placeholder)" },
                { value: "nickel", label: "Nickel (placeholder)" },
              ]}
            />
          </label>
          <label>
            Intensidad / radio
            <PlaceholderSelect
              options={[
                { value: "low", label: "Baja (placeholder)" },
                { value: "mid", label: "Media (placeholder)" },
                { value: "high", label: "Alta (placeholder)" },
              ]}
            />
          </label>
        </div>
        <div className={styles.placeholderArea}>Espacio reservado para heatmap o capa geoespacial.</div>
      </>
    );
  }

  return (
    <div className="page-shell">
      <header className="nav">
        <div className="brand">
          <span className="brand-dot" />
          <div>
            <strong>GeoContext</strong>
            <br />
            <span>Plataforma Analitica</span>
          </div>
        </div>
        <nav className="menu">
          <Link href="/">Inicio</Link>
          <Link href="/explorar">Explorar</Link>
          <Link href="/comparar">Comparar</Link>
          <Link href="/analisis">Analisis</Link>
          <Link href="/terreno">Terreno</Link>
          <a href="#">Consultas</a>
        </nav>
      </header>

      <main className="container">
        <section className={`panel ${styles.heroPanel}`}>
          <p className="muted">
            Herramientas para explorar zonas de interes mineralogico a partir de depositos registrados, proximidad
            espacial y minerales asociados.
          </p>
        </section>

        <section className="panel">
          <div className={styles.toolTabs}>
            {toolTabs.map((tool) => (
              <button
                key={tool.id}
                type="button"
                className={tool.id === activeTool ? styles.toolTabActive : styles.toolTab}
                onClick={() => setActiveTool(tool.id)}
              >
                {tool.label}
              </button>
            ))}
          </div>
        </section>

        <section className="panel">
          {renderActiveTool()}
        </section>
      </main>
    </div>
  );
}
