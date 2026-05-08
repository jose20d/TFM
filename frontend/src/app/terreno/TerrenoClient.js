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

function markerStyleForWeight(weight) {
  if (weight >= 0.8) {
    return { color: "#b45309", fillColor: "#f97316", fillOpacity: 0.9, weight: 1.4, radius: 8 };
  }
  if (weight >= 0.5) {
    return { color: "#0369a1", fillColor: "#38bdf8", fillOpacity: 0.82, weight: 1.2, radius: 6 };
  }
  return { color: "#1d4ed8", fillColor: "#60a5fa", fillOpacity: 0.7, weight: 1, radius: 5 };
}

function InfoHint({ text, label }) {
  return (
    <span className="acronym-hint" data-tooltip={text} aria-label={text} tabIndex={0}>
      {label ? (
        <>
          {label} <span className="hint-icon">ⓘ</span>
        </>
      ) : (
        <span className="hint-icon">ⓘ</span>
      )}
    </span>
  );
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
  const [freqCountryIso, setFreqCountryIso] = useState("");
  const [freqMineral, setFreqMineral] = useState("");
  const [freqLimit, setFreqLimit] = useState(20);
  const [freqLoading, setFreqLoading] = useState(false);
  const [freqError, setFreqError] = useState("");
  const [freqResult, setFreqResult] = useState(null);
  const [freqAutoZoomTrigger, setFreqAutoZoomTrigger] = useState(0);
  const [potentialCountryIso, setPotentialCountryIso] = useState("");
  const [potentialMineral, setPotentialMineral] = useState("");
  const [potentialMineralOptions, setPotentialMineralOptions] = useState([]);
  const [potentialIntensity, setPotentialIntensity] = useState("medium");
  const [potentialLoading, setPotentialLoading] = useState(false);
  const [potentialError, setPotentialError] = useState("");
  const [potentialResult, setPotentialResult] = useState(null);
  const [potentialAutoZoomTrigger, setPotentialAutoZoomTrigger] = useState(0);

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
  const freqCountryLabel = useMemo(() => {
    const match = countries.find((country) => country.iso3 === freqCountryIso);
    return match?.country_name || "";
  }, [countries, freqCountryIso]);
  const potentialCountryLabel = useMemo(() => {
    const match = countries.find((country) => country.iso3 === potentialCountryIso);
    return match?.country_name || "";
  }, [countries, potentialCountryIso]);

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

  useEffect(() => {
    setFreqMineral("");
    setFreqResult(null);
    setFreqError("");
    setFreqAutoZoomTrigger((value) => value + 1);
  }, [freqCountryIso]);

  useEffect(() => {
    setPotentialResult(null);
    setPotentialError("");
    setPotentialAutoZoomTrigger((value) => value + 1);
  }, [potentialCountryIso, potentialMineral, potentialIntensity]);

  useEffect(() => {
    setPotentialMineralOptions([]);
    setPotentialMineral("");
    if (!potentialCountryIso) return;

    const controller = new AbortController();
    fetch(
      `/api/backend/api/v1/terrain/frequent-minerals?country_iso3=${potentialCountryIso}&show_all=true&limit=50`,
      { cache: "no-store", signal: controller.signal },
    )
      .then((response) => {
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        return response.json();
      })
      .then((data) => {
        const options = Array.isArray(data?.available_minerals)
          ? data.available_minerals
              .map((item) => {
                if (typeof item === "string") return item;
                if (item && typeof item === "object") return item.mineral || "";
                return "";
              })
              .map((value) => String(value).trim())
              .filter(Boolean)
          : [];
        setPotentialMineralOptions(options);
        if (options.length) setPotentialMineral(options[0]);
      })
      .catch((error) => {
        if (error?.name === "AbortError") return;
        setPotentialMineralOptions([]);
      });

    return () => controller.abort();
  }, [potentialCountryIso]);

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
  const freqMapPoints = useMemo(() => {
    if (freqResult?.heat_points?.length) {
      return freqResult.heat_points
        .map((point) => [Number(point.lat), Number(point.lng)])
        .filter(([lat, lng]) => Number.isFinite(lat) && Number.isFinite(lng));
    }
    return [];
  }, [freqResult]);
  const potentialVisibleHeatPoints = useMemo(() => {
    const points = Array.isArray(potentialResult?.heat_points) ? potentialResult.heat_points : [];
    if (!points.length) return [];

    const minClusterSizeBySensitivity = {
      low: 2,
      medium: 4,
      high: 6,
    };
    const threshold = minClusterSizeBySensitivity[potentialIntensity] || 4;
    if (potentialIntensity === "low") {
      return points;
    }
    const filtered = points.filter(
      (point) => Number(point.cluster_id) !== -1 && Number(point.cluster_size || 0) >= threshold,
    );
    return filtered;
  }, [potentialResult, potentialIntensity]);
  const potentialMapPoints = useMemo(() => {
    const points = potentialVisibleHeatPoints;
    if (points?.length) {
      return points
        .map((point) => [Number(point.lat), Number(point.lng)])
        .filter(([lat, lng]) => Number.isFinite(lat) && Number.isFinite(lng));
    }
    return [];
  }, [potentialVisibleHeatPoints]);
  const potentialHullFeatures = useMemo(() => {
    const clusters = Array.isArray(potentialResult?.clusters) ? potentialResult.clusters : [];
    const minHullClusterBySensitivity = {
      low: 2,
      medium: 4,
      high: 6,
    };
    const threshold = minHullClusterBySensitivity[potentialIntensity] || 4;
    const ranked = clusters
      .filter((cluster) => cluster?.cluster_id !== -1 && cluster?.hull_geojson)
      .sort((a, b) => Number(b.deposit_count || 0) - Number(a.deposit_count || 0));
    const filtered = ranked.filter((cluster) => Number(cluster.deposit_count || 0) >= threshold);
    const selected = filtered.length ? filtered : ranked.slice(0, 1);
    return selected
      .map((cluster) => ({
        feature: toFeature(cluster.hull_geojson),
        clusterId: cluster.cluster_id,
      }))
      .filter((item) => item.feature);
  }, [potentialResult, potentialIntensity]);

  useEffect(() => {
    if (activeTool !== "minerals") return;
    if (!freqCountryIso) return;
    const controller = new AbortController();
    const timeoutId = setTimeout(async () => {
      setFreqLoading(true);
      setFreqError("");
      try {
        const qs = new URLSearchParams({
          country_iso3: freqCountryIso,
          limit: String(freqLimit),
        });
        if (freqMineral.trim()) qs.set("mineral", freqMineral.trim());
        const response = await fetch(`/api/backend/api/v1/terrain/frequent-minerals?${qs.toString()}`, {
          cache: "no-store",
          signal: controller.signal,
        });
        const payload = await response.json();
        if (!response.ok) throw new Error(payload?.detail || `HTTP ${response.status}`);
        setFreqResult(payload);
        setFreqAutoZoomTrigger((value) => value + 1);
      } catch (error) {
        if (error?.name === "AbortError") return;
        setFreqResult(null);
        setFreqError(error?.message || "No fue posible cargar minerales frecuentes.");
      } finally {
        setFreqLoading(false);
      }
    }, 280);

    return () => {
      controller.abort();
      clearTimeout(timeoutId);
    };
  }, [activeTool, freqCountryIso, freqMineral, freqLimit]);

  useEffect(() => {
    if (activeTool !== "potential") return;
    if (!potentialCountryIso || !potentialMineral.trim()) return;

    const controller = new AbortController();
    const timeoutId = setTimeout(async () => {
      setPotentialLoading(true);
      setPotentialError("");
      try {
        const qs = new URLSearchParams({
          country_iso3: potentialCountryIso,
          mineral: potentialMineral.trim(),
          intensity_level: potentialIntensity,
        });
        const response = await fetch(`/api/backend/api/v1/terrain/exploratory-potential?${qs.toString()}`, {
          cache: "no-store",
          signal: controller.signal,
        });
        const payload = await response.json();
        if (!response.ok) throw new Error(payload?.detail || `HTTP ${response.status}`);
        setPotentialResult(payload);
        setPotentialAutoZoomTrigger((value) => value + 1);
      } catch (error) {
        if (error?.name === "AbortError") return;
        setPotentialResult(null);
        setPotentialError(error?.message || "No fue posible analizar el potencial exploratorio.");
      } finally {
        setPotentialLoading(false);
      }
    }, 280);

    return () => {
      controller.abort();
      clearTimeout(timeoutId);
    };
  }, [activeTool, potentialCountryIso, potentialMineral, potentialIntensity]);

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
          <p className="muted">
            <InfoHint
              label="Intensidad mineralogica"
              text="La intensidad refleja frecuencia relativa del mineral dentro del corredor analizado."
            />
          </p>

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
              <h4>
                <InfoHint
                  label="Mapa del corredor"
                  text="Area de influencia espacial calculada entre los depositos seleccionados."
                />
              </h4>
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
                          {inCorridor
                            ? `Distancia al eje: ${formatNumber(inCorridor.distance_to_axis_km, 2)} km`
                            : "Fuera del corredor"}
                          <br />
                          Minerales:{" "}
                          {inCorridor?.minerals?.length ? inCorridor.minerals.join(", ") : "N/A"}
                          <br />
                          Intensidad: {inCorridor ? formatNumber(inCorridor.intensity_score, 2) : "N/A"}
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
                    <h5>
                      <InfoHint
                        label="Ranking de minerales del corredor"
                        text="La intensidad refleja frecuencia relativa del mineral dentro del corredor analizado."
                      />
                    </h5>
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
                            <strong
                              className="acronym-hint"
                              data-tooltip={`Distancia: ${formatNumber(deposit.distance_to_axis_km, 2)} km | Intensidad: ${formatNumber(deposit.intensity_score, 2)} | Minerales: ${deposit.minerals?.length ? deposit.minerals.join(", ") : "N/A"}`}
                              tabIndex={0}
                            >
                              {deposit.name}
                            </strong>{" "}
                            -{" "}
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
          <p className="muted">
            <InfoHint
              label="Radio de busqueda"
              text="Radio espacial utilizado para buscar depositos cercanos."
            />
          </p>

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
                            ? `Deposito registrado dentro de la zona seleccionada (${formatNumber(inZone.distance_km, 2)} km al centro)`
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
                    <h5>
                      <InfoHint
                        label="Ranking de minerales"
                        text="Frecuencia relativa del mineral dentro de la zona analizada."
                      />
                    </h5>
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
          <p className="muted">
            Explora la distribucion mineralogica por pais para identificar minerales dominantes,
            coexistencias frecuentes y concentracion espacial observada.
          </p>
          <div className={styles.controls}>
            <label>
              Pais
              <select value={freqCountryIso} onChange={(event) => setFreqCountryIso(event.target.value)}>
                <option value="">Seleccionar pais</option>
                {countries.map((country) => (
                  <option key={`freq-${country.country_name}-${country.iso3}`} value={country.iso3 || ""}>
                    {country.country_name} ({country.iso3 || "N/A"})
                  </option>
                ))}
              </select>
            </label>
            <label>
              Mineral (opcional)
              <select value={freqMineral} onChange={(event) => setFreqMineral(event.target.value)}>
                <option value="">Todos los minerales</option>
                {(freqResult?.available_minerals || []).map((item) => (
                  <option key={`min-opt-${item.mineral}`} value={item.mineral}>
                    {item.mineral}
                  </option>
                ))}
              </select>
            </label>
            <label>
              Top N
              <select value={freqLimit} onChange={(event) => setFreqLimit(Number(event.target.value) || 20)}>
                <option value={10}>10</option>
                <option value={20}>20</option>
                <option value={50}>50</option>
              </select>
            </label>
          </div>

          {!freqCountryIso && (
            <p className="muted">Selecciona un pais para explorar minerales frecuentes.</p>
          )}
          {freqError && <div className={styles.messageBox}>{freqError}</div>}
          {freqCountryIso && freqLoading && (
            <p className="muted">Analizando distribucion mineralogica...</p>
          )}
          {freqCountryIso && !freqLoading && !freqResult?.minerals?.length && !freqError && (
            <p className="muted">No se encontraron minerales asociados para esta seleccion.</p>
          )}

          <div className={styles.corridorLayout}>
            <article className={styles.mapCard}>
              <h4>
                <InfoHint
                  label="Mapa de intensidad mineralogica"
                  text="La concentracion visual representa frecuencia espacial observada del mineral seleccionado."
                />
              </h4>
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
                    points={freqMapPoints}
                    center={null}
                    radiusKm={0}
                    countryIso={freqCountryIso}
                    trigger={freqAutoZoomTrigger}
                  />
                  {(freqResult?.heat_points || []).map((point, index) => {
                    const markerStyle = markerStyleForWeight(Number(point.weight || 0));
                    return (
                      <CircleMarker
                        key={`freq-point-${index}-${point.dep_name}`}
                        center={[Number(point.lat), Number(point.lng)]}
                        radius={markerStyle.radius}
                        pathOptions={markerStyle}
                      >
                        <Tooltip direction="top" offset={[0, -2]}>
                          <strong>{point.dep_name}</strong>
                          <br />
                          Mineral dominante: {point.mineral || "N/A"}
                          <br />
                          Intensidad: {formatNumber((Number(point.weight) || 0) * 100, 0)}%
                        </Tooltip>
                      </CircleMarker>
                    );
                  })}
                </MapContainer>
              </div>
              <p className="muted">
                Los resultados se basan en registros mineralogicos existentes y distribucion espacial
                observada.
              </p>
            </article>

            <article className={styles.resultsCard}>
              <h4>Resumen mineralogico</h4>
              <div className={styles.kpisGrid}>
                <div>
                  <strong>Pais</strong>
                  <p>{freqCountryLabel || "N/A"}</p>
                </div>
                <div>
                  <strong>Depositos analizados</strong>
                  <p>{formatNumber(freqResult?.total_deposits || 0)}</p>
                </div>
                <div>
                  <strong>Mineral foco</strong>
                  <p>{freqResult?.coexistence_focus_mineral || "N/A"}</p>
                </div>
              </div>

              <section className={styles.resultSection}>
                <h5>
                  <InfoHint
                    label="Ranking de minerales"
                    text="Porcentaje de depositos del pais donde aparece este mineral."
                  />
                </h5>
                <p className="muted">
                  La intensidad representa la frecuencia relativa del mineral dentro de los depositos
                  registrados del pais seleccionado.
                </p>
                {(freqResult?.minerals || []).length ? (
                  <ul className={styles.rankingBars}>
                    {(freqResult?.minerals || []).map((item) => (
                      <li key={`freq-rank-${item.mineral}`}>
                        <div className={styles.rankingBarsHead}>
                          <span>{item.mineral}</span>
                          <span>{formatNumber(item.percentage, 1)}%</span>
                        </div>
                        <div className={styles.mineralBarTrack}>
                          <div className={styles.mineralBarFill} style={{ width: `${Math.min(100, item.percentage)}%` }} />
                        </div>
                        <span
                          className={`${styles[`intensity-${item.intensity}`]} acronym-hint`}
                          data-tooltip="Clasificacion relativa basada en frecuencia observada dentro del pais seleccionado."
                          aria-label="Clasificacion relativa basada en frecuencia observada dentro del pais seleccionado."
                          tabIndex={0}
                        >
                          {item.intensity}
                        </span>
                      </li>
                    ))}
                  </ul>
                ) : (
                  <p className="muted">No hay minerales para mostrar en el ranking.</p>
                )}
              </section>

              <section className={styles.resultSection}>
                <h5>Coexistencia mineralogica</h5>
                <p className="muted">
                  {freqResult?.coexistence_focus_mineral
                    ? `${freqResult.coexistence_focus_mineral} suele aparecer junto con:`
                    : "Selecciona un mineral para profundizar coexistencia."}
                </p>
                {(freqResult?.coexistence || []).length ? (
                  <ul className={styles.simpleList}>
                    {freqResult.coexistence.map((item) => (
                      <li key={`coexist-${item.mineral}`}>
                        {item.mineral} ({formatNumber(item.count)})
                      </li>
                    ))}
                  </ul>
                ) : (
                  <p className="muted">No se detecto coexistencia relevante para esta seleccion.</p>
                )}
              </section>

              <section className={styles.resultSection}>
                <h5>Zonas dominantes</h5>
                {(freqResult?.top_regions || []).length ? (
                  <ul className={styles.simpleList}>
                    {(freqResult?.top_regions || []).map((region) => (
                      <li key={`region-${region.region}`}>
                        {region.region} - {region.dominant_mineral} ({formatNumber(region.deposit_count)} dep)
                      </li>
                    ))}
                  </ul>
                ) : (
                  <p className="muted">No se encontraron regiones dominantes para esta seleccion.</p>
                )}
              </section>
            </article>
          </div>
        </>
      );
    }

    return (
      <>
        <p className="muted">
          Analiza patrones espaciales historicos para identificar concentraciones relativas de registros
          asociados al mineral seleccionado.
        </p>
        <div className={styles.controls}>
          <label>
            Pais
            <select value={potentialCountryIso} onChange={(event) => setPotentialCountryIso(event.target.value)}>
              <option value="">Seleccionar pais</option>
              {countries.map((country) => (
                <option key={`pot-${country.country_name}-${country.iso3}`} value={country.iso3 || ""}>
                  {country.country_name} ({country.iso3 || "N/A"})
                </option>
              ))}
            </select>
          </label>
          <label>
            Mineral objetivo
            <select value={potentialMineral} onChange={(event) => setPotentialMineral(event.target.value)}>
              <option value="">Seleccionar mineral</option>
              {potentialMineralOptions.map((option) => (
                <option key={`pot-min-${option}`} value={option}>
                  {option}
                </option>
              ))}
            </select>
          </label>
          <label>
            Sensibilidad espacial
            <select value={potentialIntensity} onChange={(event) => setPotentialIntensity(event.target.value)}>
              <option value="low">Baja (mas agrupaciones)</option>
              <option value="medium">Media</option>
              <option value="high">Alta (menos agrupaciones)</option>
            </select>
          </label>
        </div>
        {potentialCountryIso && !potentialMineralOptions.length && (
          <p className="muted">No hay minerales disponibles para este pais en los registros actuales.</p>
        )}
        {!potentialCountryIso || !potentialMineral.trim() ? (
          <p className="muted">Selecciona un pais y un mineral para analizar patrones espaciales.</p>
        ) : null}
        {potentialError && <div className={styles.messageBox}>{potentialError}</div>}
        {potentialLoading && <p className="muted">Analizando patron espacial...</p>}
        {!potentialLoading && potentialResult?.message && (
          <p className="muted">{potentialResult.message}</p>
        )}
        {!potentialLoading &&
          !potentialResult?.message &&
          potentialResult?.total_deposits > 0 &&
          potentialVisibleHeatPoints.length === 0 && (
            <p className="muted">
              Con esta sensibilidad no se detectaron agrupaciones robustas. Prueba con sensibilidad media o
              baja.
            </p>
          )}

        <div className={styles.corridorLayout}>
          <article className={styles.mapCard}>
            <h4>
              <InfoHint
                label="Mapa de potencial exploratorio"
                text="Visualizacion exploratoria basada en registros historicos y proximidad espacial."
              />
            </h4>
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
                  points={potentialMapPoints}
                  center={null}
                  radiusKm={0}
                  countryIso={potentialCountryIso}
                  trigger={potentialAutoZoomTrigger}
                />
                {potentialHullFeatures.map((item, index) => (
                  <GeoJSON
                    key={`pot-hull-${item.clusterId}-${index}`}
                    data={item.feature}
                    style={() => ({
                      color: "#7c3aed",
                      weight: 2.2,
                      fillColor: "#7c3aed",
                      fillOpacity: 0.08,
                    })}
                  />
                ))}
                {potentialVisibleHeatPoints.map((point, index) => {
                  const markerStyle = markerStyleForWeight(Number(point.weight || 0));
                  return (
                    <CircleMarker
                      key={`pot-point-${index}-${point.dep_name}`}
                      center={[Number(point.lat), Number(point.lng)]}
                      radius={markerStyle.radius}
                      pathOptions={markerStyle}
                    >
                      <Tooltip direction="top" offset={[0, -2]}>
                        <strong>{point.dep_name}</strong>
                        <br />
                        Mineral: {point.mineral}
                        <br />
                        Zona: {point.region || "N/A"}
                        <br />
                        Presencia relativa: {formatNumber((Number(point.weight) || 0) * 100, 0)}%
                      </Tooltip>
                    </CircleMarker>
                  );
                })}
              </MapContainer>
            </div>
            <p className="muted">
              El potencial exploratorio mostrado se basa unicamente en registros historicos y distribucion
              espacial observada.{" "}
              <InfoHint
                text="Este analisis no representa una prediccion geologica profesional ni garantiza presencia mineral en campo."
              />
            </p>
          </article>

          <article className={styles.resultsCard}>
            <h4>Resumen de patron espacial</h4>
            <div className={styles.kpisGrid}>
              <div>
                <strong>Pais</strong>
                <p>{potentialCountryLabel || "N/A"}</p>
              </div>
              <div>
                <strong>Mineral objetivo</strong>
                <p>{potentialResult?.mineral || potentialMineral || "N/A"}</p>
              </div>
              <div>
                <strong>Depositos analizados</strong>
                <p>{formatNumber(potentialResult?.total_deposits || 0)}</p>
              </div>
            </div>

            <section className={styles.resultSection}>
              <h5>
                <InfoHint
                  label="Clasificacion espacial"
                  text="Patron estimado a partir de agrupamiento de depositos y frecuencia mineralogica."
                />
              </h5>
              <div className={styles.badgesRow}>
                <span className={styles.commonBadge}>{potentialResult?.spatial_classification || "N/A"}</span>
                <span className={styles.commonBadge}>{potentialResult?.spatial_pattern || "N/A"}</span>
              </div>
            </section>

            <section className={styles.resultSection}>
              <h5>Zonas principales detectadas</h5>
              {(potentialResult?.top_regions || []).length ? (
                <ul className={styles.simpleList}>
                  {(potentialResult?.top_regions || []).map((item) => (
                    <li key={`pot-region-${item.region}`}>
                      {item.region} ({formatNumber(item.deposit_count)} dep)
                    </li>
                  ))}
                </ul>
              ) : (
                <p className="muted">No hay zonas dominantes para esta seleccion.</p>
              )}
            </section>

            <section className={styles.resultSection}>
              <h5>Resumen de concentracion</h5>
              <p className="muted">
                {potentialResult?.explanation ||
                  "Las zonas resaltadas representan concentraciones espaciales de registros mineralogicos asociados al mineral seleccionado."}
              </p>
            </section>
          </article>
        </div>
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
          <Link href="/consultas">Consultas</Link>
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
