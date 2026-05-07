"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import { CircleMarker, GeoJSON, MapContainer, TileLayer, Tooltip, useMap } from "react-leaflet";
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

function CorridorAutoZoom({ points }) {
  const map = useMap();

  useEffect(() => {
    if (!points.length) {
      map.setView(DEFAULT_VIEW, DEFAULT_ZOOM, { animate: true });
      return;
    }
    if (points.length === 1) {
      map.setView(points[0], 6, { animate: true });
      return;
    }
    const bounds = L.latLngBounds(points);
    map.fitBounds(bounds, { padding: [30, 30], maxZoom: 8, animate: true });
  }, [map, points]);

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

  const depositsById = useMemo(() => {
    const map = new Map();
    countryDeposits.forEach((deposit) => map.set(deposit.dep_id, deposit));
    return map;
  }, [countryDeposits]);

  const selectedFrom = selectedFromId ? depositsById.get(selectedFromId) || null : null;
  const selectedTo = selectedToId ? depositsById.get(selectedToId) || null : null;

  const mapPoints = useMemo(
    () => countryDeposits.map((item) => [item.latitude, item.longitude]),
    [countryDeposits],
  );

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
  }

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
                  <CorridorAutoZoom points={mapPoints} />
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
      return (
        <>
          <h3>Zona de interes</h3>
          <p className="muted">
            Selecciona una zona o punto de interes para identificar depositos cercanos y minerales
            registrados en el area.
          </p>
          <div className={styles.controls}>
            <label>
              Pais
              <PlaceholderSelect
                options={[
                  { value: "", label: "Seleccionar pais" },
                  { value: "cri", label: "Costa Rica (placeholder)" },
                  { value: "aus", label: "Australia (placeholder)" },
                ]}
              />
            </label>
            <label>
              Mineral (opcional)
              <PlaceholderSelect
                options={[
                  { value: "", label: "Todos" },
                  { value: "gold", label: "Gold (placeholder)" },
                  { value: "copper", label: "Copper (placeholder)" },
                ]}
              />
            </label>
            <label>
              Radio de busqueda (km)
              <PlaceholderSelect
                options={[
                  { value: "5", label: "5 km" },
                  { value: "15", label: "15 km" },
                  { value: "30", label: "30 km" },
                  { value: "60", label: "60 km" },
                ]}
              />
            </label>
            <button type="button">Buscar en zona</button>
          </div>
          <div className={styles.placeholderArea}>Espacio reservado para mapa y resultados de zona.</div>
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
