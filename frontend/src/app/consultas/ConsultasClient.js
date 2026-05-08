"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { CircleMarker, MapContainer, TileLayer, Tooltip, useMap } from "react-leaflet";
import "leaflet/dist/leaflet.css";
import L from "leaflet";
import styles from "./consultas.module.css";

const MODES = [
  { id: "deposits", label: "Depositos por mineral" },
  { id: "combined", label: "Minerales combinados" },
  { id: "spatial", label: "Consulta espacial" },
  { id: "profile", label: "Perfil de pais" },
];

const DEFAULT_CENTER = [15, -20];
const DEFAULT_ZOOM = 2;

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

function formatNumber(value, decimals = 0) {
  if (value === null || value === undefined) return "N/A";
  const num = Number(value);
  if (!Number.isFinite(num)) return "N/A";
  return new Intl.NumberFormat("es-ES", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  }).format(num);
}

function toCsv(rows) {
  if (!rows.length) return "";
  const headers = Object.keys(rows[0]);
  const escape = (value) => `"${String(value ?? "").replace(/"/g, '""')}"`;
  const lines = [headers.join(",")];
  rows.forEach((row) => {
    const line = headers
      .map((key) => {
        const cell = Array.isArray(row[key]) ? row[key].join(" | ") : row[key];
        return escape(cell);
      })
      .join(",");
    lines.push(line);
  });
  return lines.join("\n");
}

function downloadText(filename, content, mimeType) {
  const blob = new Blob([content], { type: mimeType });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
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

export default function ConsultasClient() {
  const [activeMode, setActiveMode] = useState("deposits");
  const [countries, setCountries] = useState([]);
  const [minerals, setMinerals] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [result, setResult] = useState({ result_count: 0, summary: "", rows: [] });

  const [depositFilters, setDepositFilters] = useState({
    countryIso: "",
    mineral: "",
    status: "",
    minMinerals: 1,
    limit: 100,
  });
  const [combinedFilters, setCombinedFilters] = useState({
    countryIso: "",
    mineralA: "",
    mineralB: "",
    excludeMineral: "",
    limit: 100,
  });
  const [spatialFilters, setSpatialFilters] = useState({
    countryIso: "",
    baseDepId: "",
    radiusKm: 20,
    mineral: "",
    limit: 150,
  });
  const [profileFilters, setProfileFilters] = useState({
    minDeposits: 0,
    gdpMin: "",
    gdpMax: "",
    cpiMin: "",
    cpiMax: "",
    fsiMin: "",
    fsiMax: "",
    limit: 200,
  });
  const [spatialCountryDeposits, setSpatialCountryDeposits] = useState([]);
  const [spatialMinerals, setSpatialMinerals] = useState([]);

  useEffect(() => {
    Promise.all([
      fetch("/api/backend/api/v1/countries?limit=300", { cache: "no-store" }).then((r) => r.json()),
      fetch("/api/backend/api/v1/top-minerals?limit=120", { cache: "no-store" }).then((r) => r.json()),
    ])
      .then(([countriesPayload, mineralsPayload]) => {
        setCountries(Array.isArray(countriesPayload) ? countriesPayload : []);
        const mineralOptions = Array.isArray(mineralsPayload)
          ? mineralsPayload.map((item) => String(item.commod || "").trim()).filter(Boolean)
          : [];
        setMinerals(mineralOptions);
      })
      .catch(() => {
        setCountries([]);
        setMinerals([]);
      });
  }, []);

  useEffect(() => {
    setSpatialCountryDeposits([]);
    setSpatialFilters((prev) => ({ ...prev, baseDepId: "" }));
    if (!spatialFilters.countryIso) return;
    fetch(
      `/api/backend/api/v1/explore/deposits?country_iso3=${spatialFilters.countryIso}&limit=5000`,
      { cache: "no-store" },
    )
      .then((response) => {
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        return response.json();
      })
      .then((rows) => {
        const options = Array.isArray(rows)
          ? rows.map((row) => ({
              dep_id: Number(row.dep_id),
              name: row.name || `Deposito ${row.dep_id}`,
              latitude: Number(row.latitude),
              longitude: Number(row.longitude),
            }))
          : [];
        setSpatialCountryDeposits(options.filter((d) => Number.isFinite(d.dep_id)));
      })
      .catch(() => setSpatialCountryDeposits([]));
  }, [spatialFilters.countryIso]);

  useEffect(() => {
    setSpatialMinerals([]);
    setSpatialFilters((prev) => ({ ...prev, mineral: "" }));
    if (!spatialFilters.countryIso) return;

    fetch(
      `/api/backend/api/v1/terrain/frequent-minerals?country_iso3=${spatialFilters.countryIso}&show_all=true&limit=50`,
      { cache: "no-store" },
    )
      .then((response) => {
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        return response.json();
      })
      .then((payload) => {
        const options = Array.isArray(payload?.available_minerals)
          ? payload.available_minerals
              .map((item) => {
                if (typeof item === "string") return item;
                if (item && typeof item === "object") return item.mineral || "";
                return "";
              })
              .map((item) => String(item).trim())
              .filter(Boolean)
          : [];
        setSpatialMinerals(options);
      })
      .catch(() => setSpatialMinerals([]));
  }, [spatialFilters.countryIso]);

  const spatialMapRows = useMemo(
    () => (activeMode === "spatial" ? result.rows || [] : []),
    [activeMode, result.rows],
  );

  async function runQuery() {
    setLoading(true);
    setError("");
    try {
      let url = "";
      if (activeMode === "deposits") {
        const qs = new URLSearchParams({
          country_iso3: depositFilters.countryIso,
          mineral: depositFilters.mineral,
          deposit_status: depositFilters.status,
          min_minerals: String(depositFilters.minMinerals),
          limit: String(depositFilters.limit),
        });
        url = `/api/backend/api/v1/queries/deposits-by-mineral?${qs.toString()}`;
      } else if (activeMode === "combined") {
        const qs = new URLSearchParams({
          country_iso3: combinedFilters.countryIso,
          mineral_a: combinedFilters.mineralA,
          mineral_b: combinedFilters.mineralB,
          exclude_mineral: combinedFilters.excludeMineral,
          limit: String(combinedFilters.limit),
        });
        url = `/api/backend/api/v1/queries/combined-minerals?${qs.toString()}`;
      } else if (activeMode === "spatial") {
        if (!spatialFilters.countryIso || !spatialFilters.baseDepId) {
          setError("Selecciona pais y deposito base para la consulta espacial.");
          setLoading(false);
          return;
        }
        const qs = new URLSearchParams({
          country_iso3: spatialFilters.countryIso,
          base_dep_id: String(spatialFilters.baseDepId),
          radius_km: String(spatialFilters.radiusKm),
          mineral: spatialFilters.mineral,
          limit: String(spatialFilters.limit),
        });
        url = `/api/backend/api/v1/queries/spatial-nearby?${qs.toString()}`;
      } else {
        const qs = new URLSearchParams({
          min_deposits: String(profileFilters.minDeposits),
          gdp_min: profileFilters.gdpMin,
          gdp_max: profileFilters.gdpMax,
          cpi_min: profileFilters.cpiMin,
          cpi_max: profileFilters.cpiMax,
          fsi_min: profileFilters.fsiMin,
          fsi_max: profileFilters.fsiMax,
          limit: String(profileFilters.limit),
        });
        url = `/api/backend/api/v1/queries/country-profile?${qs.toString()}`;
      }

      const response = await fetch(url, { cache: "no-store" });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload?.detail || `HTTP ${response.status}`);
      setResult(payload || { result_count: 0, summary: "", rows: [] });
    } catch (queryError) {
      setResult({ result_count: 0, summary: "", rows: [] });
      setError(queryError?.message || "No fue posible ejecutar la consulta.");
    } finally {
      setLoading(false);
    }
  }

  function exportCsv() {
    const csv = toCsv(result.rows || []);
    downloadText(`consultas_${activeMode}.csv`, csv, "text/csv;charset=utf-8;");
  }

  function exportJson() {
    downloadText(
      `consultas_${activeMode}.json`,
      JSON.stringify(result.rows || [], null, 2),
      "application/json;charset=utf-8;",
    );
  }

  function exportGeoJson() {
    const rows = Array.isArray(result.rows) ? result.rows : [];
    const features = rows
      .filter((row) => Number.isFinite(Number(row.lat)) && Number.isFinite(Number(row.lng)))
      .map((row) => ({
        type: "Feature",
        geometry: {
          type: "Point",
          coordinates: [Number(row.lng), Number(row.lat)],
        },
        properties: {
          deposit: row.deposit || "N/A",
          country: row.country || "N/A",
          distance_km: row.distance_km ?? null,
          minerals: row.minerals || [],
        },
      }));
    const geojson = { type: "FeatureCollection", features };
    downloadText(
      `consultas_${activeMode}.geojson`,
      JSON.stringify(geojson, null, 2),
      "application/geo+json;charset=utf-8;",
    );
  }

  function renderFilters() {
    if (activeMode === "deposits") {
      return (
        <div className={styles.filtersGridDeposits}>
          <label className={styles.fieldCountry}>
            Pais
            <select
              value={depositFilters.countryIso}
              onChange={(e) => setDepositFilters((p) => ({ ...p, countryIso: e.target.value }))}
            >
              <option value="">Todos</option>
              {countries.map((country) => (
                <option key={`dep-${country.iso3}-${country.country_name}`} value={country.iso3 || ""}>
                  {country.country_name} ({country.iso3 || "N/A"})
                </option>
              ))}
            </select>
          </label>
          <label className={styles.fieldMineral}>
            Mineral
            <input
              list="consultas-minerals"
              value={depositFilters.mineral}
              onChange={(e) => setDepositFilters((p) => ({ ...p, mineral: e.target.value }))}
              placeholder="Ejemplo: Gold"
            />
          </label>
          <label className={styles.fieldStatus}>
            Estado del deposito
            <select
              value={depositFilters.status}
              onChange={(e) => setDepositFilters((p) => ({ ...p, status: e.target.value }))}
            >
              <option value="">Todos</option>
              <option value="producer">Producer</option>
              <option value="prospect">Prospect</option>
              <option value="occurrence">Occurrence</option>
              <option value="past producer">Past Producer</option>
            </select>
          </label>
          <label className={styles.fieldMin}>
            Minimo minerales asociados
            <input
              type="number"
              min={1}
              max={20}
              value={depositFilters.minMinerals}
              onChange={(e) =>
                setDepositFilters((p) => ({ ...p, minMinerals: Math.max(1, Number(e.target.value) || 1) }))
              }
            />
          </label>
          <label className={styles.fieldLimit}>
            Limite de resultados
            <input
              type="number"
              min={1}
              max={1000}
              value={depositFilters.limit}
              onChange={(e) =>
                setDepositFilters((p) => ({ ...p, limit: Math.max(1, Number(e.target.value) || 100) }))
              }
            />
          </label>
        </div>
      );
    }

    if (activeMode === "combined") {
      return (
        <div className={styles.filtersGridCombined}>
          <label className={styles.fieldCombinedCountry}>
            Pais
            <select
              value={combinedFilters.countryIso}
              onChange={(e) => setCombinedFilters((p) => ({ ...p, countryIso: e.target.value }))}
            >
              <option value="">Todos</option>
              {countries.map((country) => (
                <option key={`comb-${country.iso3}-${country.country_name}`} value={country.iso3 || ""}>
                  {country.country_name} ({country.iso3 || "N/A"})
                </option>
              ))}
            </select>
          </label>
          <label className={styles.fieldCombinedA}>
            Mineral A
            <input
              list="consultas-minerals"
              value={combinedFilters.mineralA}
              onChange={(e) => setCombinedFilters((p) => ({ ...p, mineralA: e.target.value }))}
            />
          </label>
          <label className={styles.fieldCombinedB}>
            Mineral B
            <input
              list="consultas-minerals"
              value={combinedFilters.mineralB}
              onChange={(e) => setCombinedFilters((p) => ({ ...p, mineralB: e.target.value }))}
            />
          </label>
          <label className={styles.fieldCombinedExclude}>
            Excluir mineral (opcional)
            <input
              list="consultas-minerals"
              value={combinedFilters.excludeMineral}
              onChange={(e) => setCombinedFilters((p) => ({ ...p, excludeMineral: e.target.value }))}
            />
          </label>
          <label className={styles.fieldCombinedLimit}>
            Limite
            <input
              type="number"
              min={1}
              max={1000}
              value={combinedFilters.limit}
              onChange={(e) =>
                setCombinedFilters((p) => ({ ...p, limit: Math.max(1, Number(e.target.value) || 100) }))
              }
            />
          </label>
        </div>
      );
    }

    if (activeMode === "spatial") {
      return (
        <div className={styles.filtersGridSpatial}>
          <label className={styles.fieldSpatialCountry}>
            Pais
            <select
              value={spatialFilters.countryIso}
              onChange={(e) => setSpatialFilters((p) => ({ ...p, countryIso: e.target.value }))}
            >
              <option value="">Seleccionar pais</option>
              {countries.map((country) => (
                <option key={`sp-${country.iso3}-${country.country_name}`} value={country.iso3 || ""}>
                  {country.country_name} ({country.iso3 || "N/A"})
                </option>
              ))}
            </select>
          </label>
          <label className={styles.fieldSpatialBase}>
            Deposito base
            <select
              value={spatialFilters.baseDepId}
              onChange={(e) => setSpatialFilters((p) => ({ ...p, baseDepId: e.target.value }))}
              disabled={!spatialFilters.countryIso}
            >
              <option value="">Seleccionar deposito</option>
              {spatialCountryDeposits.map((dep) => (
                <option key={`base-${dep.dep_id}`} value={dep.dep_id}>
                  {dep.name}
                </option>
              ))}
            </select>
          </label>
          <label className={styles.fieldSpatialRadius}>
            Radio (km)
            <input
              type="range"
              min={1}
              max={200}
              step={1}
              value={spatialFilters.radiusKm}
              onChange={(e) =>
                setSpatialFilters((p) => ({ ...p, radiusKm: Math.max(1, Number(e.target.value) || 20) }))
              }
            />
            <span>{formatNumber(spatialFilters.radiusKm)} km</span>
          </label>
          <label className={styles.fieldSpatialMineral}>
            Mineral opcional
            <select
              value={spatialFilters.mineral}
              onChange={(e) => setSpatialFilters((p) => ({ ...p, mineral: e.target.value }))}
            >
              <option value="">Todos</option>
              {(spatialMinerals.length ? spatialMinerals : minerals).map((mineral) => (
                <option key={`spatial-min-${mineral}`} value={mineral}>
                  {mineral}
                </option>
              ))}
            </select>
          </label>
          <label className={styles.fieldSpatialLimit}>
            Limite
            <input
              type="number"
              min={1}
              max={1000}
              value={spatialFilters.limit}
              onChange={(e) =>
                setSpatialFilters((p) => ({ ...p, limit: Math.max(1, Number(e.target.value) || 150) }))
              }
            />
          </label>
        </div>
      );
    }

    return (
      <div className={styles.filtersGrid}>
        <label>
          Minimo depositos
          <input
            type="number"
            min={0}
            value={profileFilters.minDeposits}
            onChange={(e) =>
              setProfileFilters((p) => ({ ...p, minDeposits: Math.max(0, Number(e.target.value) || 0) }))
            }
          />
        </label>
        <label>
          PIB minimo
          <input
            type="number"
            value={profileFilters.gdpMin}
            onChange={(e) => setProfileFilters((p) => ({ ...p, gdpMin: e.target.value }))}
          />
        </label>
        <label>
          PIB maximo
          <input
            type="number"
            value={profileFilters.gdpMax}
            onChange={(e) => setProfileFilters((p) => ({ ...p, gdpMax: e.target.value }))}
          />
        </label>
        <label>
          CPI minimo{" "}
          <InfoHint text="Valores altos indican menor corrupcion percibida." />
          <input
            type="number"
            value={profileFilters.cpiMin}
            onChange={(e) => setProfileFilters((p) => ({ ...p, cpiMin: e.target.value }))}
          />
        </label>
        <label>
          CPI maximo
          <input
            type="number"
            value={profileFilters.cpiMax}
            onChange={(e) => setProfileFilters((p) => ({ ...p, cpiMax: e.target.value }))}
          />
        </label>
        <label>
          FSI minimo{" "}
          <InfoHint text="Valores altos representan mayor fragilidad institucional." />
          <input
            type="number"
            value={profileFilters.fsiMin}
            onChange={(e) => setProfileFilters((p) => ({ ...p, fsiMin: e.target.value }))}
          />
        </label>
        <label>
          FSI maximo
          <input
            type="number"
            value={profileFilters.fsiMax}
            onChange={(e) => setProfileFilters((p) => ({ ...p, fsiMax: e.target.value }))}
          />
        </label>
        <label>
          Limite
          <input
            type="number"
            min={1}
            max={1000}
            value={profileFilters.limit}
            onChange={(e) =>
              setProfileFilters((p) => ({ ...p, limit: Math.max(1, Number(e.target.value) || 200) }))
            }
          />
        </label>
      </div>
    );
  }

  function renderTable() {
    const rows = Array.isArray(result.rows) ? result.rows : [];
    if (!rows.length) {
      return null;
    }

    if (activeMode === "profile") {
      return (
        <div className={styles.tableWrap}>
          <table className={styles.compactTable}>
            <thead>
              <tr>
                <th>Pais</th>
                <th>Depositos</th>
                <th>PIB</th>
                <th>CPI</th>
                <th>FSI</th>
                <th>Intensidad relativa</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row, idx) => (
                <tr key={`profile-${row.iso3}-${idx}`}>
                  <td>{row.country}</td>
                  <td>{formatNumber(row.deposits)}</td>
                  <td>{row.gdp === null ? "N/A" : `${formatNumber(row.gdp / 1_000_000_000, 2)} USD B`}</td>
                  <td>{formatNumber(row.cpi, 2)}</td>
                  <td>{formatNumber(row.fsi, 2)}</td>
                  <td>{row.relative_intensity === null ? "N/A" : formatNumber(row.relative_intensity, 3)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      );
    }

    return (
      <div className={styles.tableWrap}>
        <table className={styles.compactTable}>
          <thead>
            <tr>
              <th>Deposito</th>
              <th>Pais</th>
              {activeMode === "spatial" && <th>Distancia (km)</th>}
              {activeMode === "deposits" && <th>Estado</th>}
              {activeMode === "deposits" && <th>Cantidad minerales</th>}
              <th>Minerales asociados</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row, idx) => (
              <tr key={`row-${idx}-${row.deposit}`}>
                <td>{row.deposit}</td>
                <td>{row.country}</td>
                {activeMode === "spatial" && <td>{formatNumber(row.distance_km, 2)}</td>}
                {activeMode === "deposits" && <td>{row.status || "N/A"}</td>}
                {activeMode === "deposits" && <td>{formatNumber(row.minerals_count || 0)}</td>}
                <td>{Array.isArray(row.minerals) ? row.minerals.join(", ") || "N/A" : "N/A"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
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
        <section className="panel">
          <h2>Consultas</h2>
          <p className="muted">
            Explora depositos, minerales y relaciones espaciales mediante consultas guiadas.
          </p>
          <p className="muted">
            Construye busquedas usando filtros simples sin necesidad de conocimientos tecnicos.
          </p>
        </section>

        <section className="panel">
          <div className={styles.tabs}>
            {MODES.map((mode) => (
              <button
                key={mode.id}
                type="button"
                className={activeMode === mode.id ? styles.tabBtnActive : styles.tabBtn}
                onClick={() => {
                  setActiveMode(mode.id);
                  setResult({ result_count: 0, summary: "", rows: [] });
                  setError("");
                }}
              >
                {mode.label}
              </button>
            ))}
          </div>
        </section>

        <section className="panel">
          <h3>
            {MODES.find((m) => m.id === activeMode)?.label}{" "}
            {activeMode === "spatial" && (
              <InfoHint text="Busqueda basada en proximidad geografica usando registros georreferenciados." />
            )}
          </h3>
          <p className={`muted ${styles.helpText}`}>
            Selecciona filtros y ejecuta una consulta para ver resultados compactos.
          </p>
          {renderFilters()}
          <div className={styles.actionsRow}>
            <button type="button" onClick={runQuery}>
              Ejecutar consulta
            </button>
          </div>
          {loading && <p className="muted">Consultando datos...</p>}
          {error && <p className="muted">Error: {error}</p>}
        </section>

        <section className="panel">
          <div className={styles.resultWrap}>
            <div className={styles.resultHeader}>
              <p className={styles.resultCount}>Resultados: {formatNumber(result.result_count || 0)}</p>
              <div className={styles.actionsRow}>
                <button type="button" className={styles.mutedBtn} onClick={exportCsv}>
                  Exportar CSV
                </button>
                <button type="button" className={styles.mutedBtn} onClick={exportJson}>
                  Exportar JSON
                </button>
                {activeMode === "spatial" && (
                  <button type="button" className={styles.mutedBtn} onClick={exportGeoJson}>
                    Exportar GeoJSON
                  </button>
                )}
              </div>
            </div>

            <p className={`muted ${styles.summaryText}`}>
              {result.summary || "No se encontraron registros para los criterios seleccionados."}
            </p>

            {renderTable()}

            {activeMode === "spatial" && spatialMapRows.length > 0 && (
              <div>
                <p className="muted">Vista espacial compacta</p>
                <div className={styles.smallMap}>
                  <MapContainer center={DEFAULT_CENTER} zoom={DEFAULT_ZOOM} style={{ height: "100%", width: "100%" }}>
                    <TileLayer
                      attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
                      url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
                    />
                    <SpatialAutoZoom rows={spatialMapRows} />
                    {spatialMapRows.map((row, idx) => {
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
                            Distancia: {formatNumber(row.distance_km, 2)} km
                            <br />
                            Minerales: {Array.isArray(row.minerals) ? row.minerals.join(", ") || "N/A" : "N/A"}
                          </Tooltip>
                        </CircleMarker>
                      );
                    })}
                  </MapContainer>
                </div>
              </div>
            )}
          </div>
        </section>

        <section className="panel">
          <p className="muted">
            Los resultados se basan en registros integrados desde datasets geologicos y contextuales.
          </p>
        </section>
      </main>

      <datalist id="consultas-minerals">
        {minerals.map((mineral) => (
          <option key={`m-${mineral}`} value={mineral} />
        ))}
      </datalist>
    </div>
  );
}
