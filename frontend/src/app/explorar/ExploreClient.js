"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { CircleMarker, MapContainer, TileLayer, Tooltip } from "react-leaflet";
import L from "leaflet";
import "leaflet/dist/leaflet.css";

L.Icon.Default.mergeOptions({
  iconRetinaUrl: "https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon-2x.png",
  iconUrl: "https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon.png",
  shadowUrl: "https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png",
});

const DEFAULT_VIEW = [15, -20];
const DEFAULT_ZOOM = 2;
const DEFAULT_LIMIT = 500;
const MAX_MAP_POINTS = 300;

async function getJson(url) {
  const response = await fetch(url, { cache: "no-store" });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.json();
}

function formatNumber(value) {
  if (value === null || value === undefined) return "N/A";
  const num = Number(value);
  if (Number.isNaN(num)) return String(value);
  return new Intl.NumberFormat("es-ES").format(num);
}

export default function ExploreClient() {
  const [countries, setCountries] = useState([]);
  const [countryIsoInput, setCountryIsoInput] = useState("");
  const [mineralInput, setMineralInput] = useState("");
  const [limitInput, setLimitInput] = useState(DEFAULT_LIMIT);
  const [filters, setFilters] = useState({
    countryIso: "",
    mineral: "",
    limit: DEFAULT_LIMIT,
  });
  const [rows, setRows] = useState([]);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  const queryUrl = useMemo(() => {
    const qs = new URLSearchParams();
    if (filters.countryIso) qs.set("country_iso3", filters.countryIso);
    if (filters.mineral.trim()) qs.set("mineral", filters.mineral.trim());
    qs.set("limit", String(filters.limit));
    return `/api/backend/api/v1/explore/deposits?${qs.toString()}`;
  }, [filters]);

  useEffect(() => {
    getJson("/api/backend/api/v1/countries?limit=300")
      .then((data) => setCountries(Array.isArray(data) ? data : []))
      .catch(() => setCountries([]));
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    setError("");
    fetch(queryUrl, { cache: "no-store", signal: controller.signal })
      .then((response) => {
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        return response.json();
      })
      .then((data) => {
        setRows(Array.isArray(data) ? data : []);
      })
      .catch((err) => {
        // Ignore abort errors when a newer request supersedes the previous one.
        if (err?.name === "AbortError") return;
        setRows([]);
        setError(err.message || "Error consultando datos");
      })
      .finally(() => {
        setLoading(false);
      });
    return () => controller.abort();
  }, [queryUrl]);

  const selectedCountryLabel =
    countries.find((country) => country.iso3 === filters.countryIso)?.country_name || "Todos";
  const mapRows = useMemo(() => rows.slice(0, MAX_MAP_POINTS), [rows]);
  const hiddenMapRows = rows.length - mapRows.length;

  function applyFilters(event) {
    event.preventDefault();
    setFilters({
      countryIso: countryIsoInput,
      mineral: mineralInput,
      limit: limitInput,
    });
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
          <a href="#">Analisis</a>
          <a href="#">Consultas</a>
        </nav>
      </header>

      <main className="container">
        <section className="panel">
          <h2>Exploracion geoterritorial</h2>
          <p className="muted">Filtra por pais, mineral y limite de puntos sobre el mapa.</p>
          <form className="explore-filters" onSubmit={applyFilters}>
            <select value={countryIsoInput} onChange={(e) => setCountryIsoInput(e.target.value)}>
              <option value="">Todos los paises</option>
              {countries.map((country) => (
                <option key={`${country.country_name}-${country.iso3}`} value={country.iso3 || ""}>
                  {country.country_name} ({country.iso3 || "N/A"})
                </option>
              ))}
            </select>
            <input
              value={mineralInput}
              onChange={(e) => setMineralInput(e.target.value)}
              placeholder="Mineral (ejemplo: Copper, Gold)"
            />
            <select value={limitInput} onChange={(e) => setLimitInput(Number(e.target.value) || DEFAULT_LIMIT)}>
              <option value={500}>500 puntos</option>
              <option value={800}>800 puntos</option>
              <option value={1200}>1,200 puntos</option>
            </select>
            <button type="submit">Aplicar filtros</button>
          </form>
          {loading && <p className="muted">Cargando resultados...</p>}

          <div className="explore-kpis">
            <div className="summary-item">
              <h3>Pais filtrado</h3>
              <p>{selectedCountryLabel}</p>
            </div>
            <div className="summary-item">
              <h3>Mineral filtrado</h3>
              <p>{filters.mineral.trim() || "Todos"}</p>
            </div>
            <div className="summary-item">
              <h3>Puntos cargados</h3>
              <p>{formatNumber(rows.length)}</p>
            </div>
          </div>
        </section>

        <section className="grid">
          <article className="panel">
            <h2>Mapa de depositos</h2>
            <div className="map-wrap">
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
                {mapRows.map((item) => (
                  <CircleMarker
                    key={item.dep_id}
                    center={[Number(item.latitude), Number(item.longitude)]}
                    radius={4}
                    pathOptions={{ color: "#2e86ff", fillColor: "#42c6b8", fillOpacity: 0.75, weight: 1 }}
                  >
                    <Tooltip direction="top" offset={[0, -2]}>
                      <strong>{item.name || "Deposito"}</strong>
                      <br />
                      Pais: {item.country_name} ({item.iso3 || "N/A"})
                    </Tooltip>
                  </CircleMarker>
                ))}
              </MapContainer>
            </div>
            {hiddenMapRows > 0 && (
              <p className="muted">
                Se muestran {formatNumber(mapRows.length)} puntos en mapa para mantener rendimiento.{" "}
                {formatNumber(hiddenMapRows)} quedan fuera de la visualizacion.
              </p>
            )}
          </article>

          <article className="panel">
            <h2>Resultados</h2>
            {error && <p className="muted">Error: {error}</p>}
            {!error && (
              <ul className="countries-list">
                {rows.slice(0, 20).map((item) => (
                  <li key={`row-${item.dep_id}`}>
                    <strong>{item.name || `Dep. ${item.dep_id}`}</strong> - {item.country_name} - {item.minerals || "N/A"}
                  </li>
                ))}
              </ul>
            )}
            <p className="muted">Mostrando hasta 20 resultados en listado para rendimiento.</p>
          </article>
        </section>
      </main>
    </div>
  );
}
