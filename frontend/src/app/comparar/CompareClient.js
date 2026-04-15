"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  PolarAngleAxis,
  RadialBar,
  RadialBarChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

function formatNumber(value) {
  if (value === null || value === undefined) return "N/A";
  const numeric = Number(value);
  if (Number.isNaN(numeric)) return String(value);
  return new Intl.NumberFormat("es-ES").format(numeric);
}

async function getJson(url) {
  const response = await fetch(url, { cache: "no-store" });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.json();
}

function normalize(value) {
  return String(value || "").trim().toUpperCase();
}

function countryLabel(country) {
  const iso2 = country.iso2 ? ` | ${country.iso2}` : "";
  return `${country.country_name} (${country.iso3 || "N/A"}${iso2})`;
}

function toNumeric(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function AcronymHint({ short, full }) {
  return (
    <span className="acronym-hint" data-tooltip={full} title={full} tabIndex={0}>
      {short}
    </span>
  );
}

function CountryRadialCard({ row, maxDeposits }) {
  const countryIso = row.iso3 || row.iso2 || "N/A";
  const cpiRaw = toNumeric(row.cpi);
  const fsiRaw = toNumeric(row.fsi);
  const depositsRaw = toNumeric(row.deposits);
  const cpiPct = cpiRaw === null ? 0 : Math.max(0, Math.min(100, cpiRaw));
  const fsiPct = fsiRaw === null ? 0 : Math.max(0, Math.min(100, (fsiRaw / 180) * 100));
  const depositsPct =
    depositsRaw === null ? 0 : Math.max(0, Math.min(100, (depositsRaw / Math.max(1, maxDeposits)) * 100));

  const radialData = [
    { metric: "IPC", value: cpiPct, color: "#14b86a", raw: cpiRaw, suffix: "/100" },
    { metric: "EFI", value: fsiPct, color: "#d98a24", raw: fsiRaw, suffix: "/180" },
    { metric: "Depositos", value: depositsPct, color: "#2e86ff", raw: depositsRaw, suffix: "" },
  ];

  return (
    <article className="panel radial-country-card">
      <h4>
        {row.country_name || "Pais"} ({countryIso})
      </h4>
      <div className="radial-chart-wrap">
        <ResponsiveContainer width="100%" height={130}>
          <RadialBarChart
            data={radialData}
            innerRadius="26%"
            outerRadius="96%"
            startAngle={180}
            endAngle={0}
            cx="50%"
            cy="88%"
            barSize={10}
          >
            <PolarAngleAxis type="number" domain={[0, 100]} tick={false} />
            <RadialBar dataKey="value" background={{ fill: "#2a3e55" }} cornerRadius={8}>
              {radialData.map((entry) => (
                <Cell key={`${countryIso}-${entry.metric}`} fill={entry.color} />
              ))}
            </RadialBar>
          </RadialBarChart>
        </ResponsiveContainer>
      </div>
      <div className="radial-legend">
        {radialData.map((item) => (
          <p key={`${countryIso}-${item.metric}`}>
            <span className="legend-dot" style={{ background: item.color }} />
            {item.metric}: {item.raw === null ? "N/A" : formatNumber(item.raw)}
            {item.suffix}
          </p>
        ))}
      </div>
    </article>
  );
}

export default function CompareClient() {
  const [countries, setCountries] = useState([]);
  const [selectedIso, setSelectedIso] = useState([]);
  const [rows, setRows] = useState([]);
  const [search, setSearch] = useState("");
  const [error, setError] = useState("");
  const [dbUp, setDbUp] = useState(true);

  useEffect(() => {
    Promise.all([
      getJson("/api/backend/api/v1/countries?limit=300"),
      getJson("/api/backend/api/v1/home/defaults"),
      getJson("/api/backend/api/v1/health"),
    ])
      .then(([countriesData, defaultsData, healthData]) => {
        const allCountries = Array.isArray(countriesData) ? countriesData : [];
        const defaults = Array.isArray(defaultsData?.compare_iso3)
          ? defaultsData.compare_iso3
          : [];
        const cleanIso = Array.from(
          new Set(defaults.map((iso) => normalize(iso)).filter((iso) => iso.length === 3)),
        ).slice(0, 3);

        setCountries(allCountries);
        setSelectedIso(cleanIso);
        setDbUp(Boolean(healthData?.db));
      })
      .catch(() => {
        setCountries([]);
        setSelectedIso([]);
        setDbUp(false);
      });
  }, []);

  useEffect(() => {
    if (selectedIso.length < 2) {
      return;
    }
    const qs = selectedIso.map((iso) => `iso3=${encodeURIComponent(iso)}`).join("&");
    getJson(`/api/backend/api/v1/countries/compare?${qs}`)
      .then((data) => {
        setRows(Array.isArray(data) ? data : []);
        setError("");
      })
      .catch((err) => {
        setRows([]);
        setError(err.message);
      });
  }, [selectedIso]);

  const availableCountries = useMemo(() => {
    const selected = new Set(selectedIso);
    return countries.filter(
      (country) => country.iso3 && !selected.has(normalize(country.iso3)),
    );
  }, [countries, selectedIso]);

  const filteredCountries = useMemo(() => {
    const term = search.trim().toLowerCase();
    if (!term) return [];
    return availableCountries
      .filter((country) => {
        const name = String(country.country_name || "").toLowerCase();
        const iso3 = String(country.iso3 || "").toLowerCase();
        const iso2 = String(country.iso2 || "").toLowerCase();
        return (
          name.startsWith(term) ||
          iso3.startsWith(term) ||
          iso2.startsWith(term)
        );
      })
      .slice(0, 12);
  }, [availableCountries, search]);

  const canAddMoreCountries = selectedIso.length < 5;

  const visibleRows = selectedIso.length < 2 ? [] : rows;

  const exampleCountries = useMemo(() => {
    const selected = new Set(selectedIso);
    const exampleIso3 = ["CRI", "CHL", "MEX"];
    return exampleIso3
      .map((iso3) =>
        countries.find(
          (country) =>
            normalize(country.iso3) === iso3 &&
            !selected.has(normalize(country.iso3)),
        ),
      )
      .filter(Boolean);
  }, [countries, selectedIso]);

  const maxGdp = Math.max(1, ...visibleRows.map((row) => Number(row.gdp || 0)));
  const chartRows = visibleRows.slice(0, 5);
  const gdpChartData = chartRows.map((row) => ({
    iso3: row.iso3 || "N/A",
    gdpBillion: toNumeric(row.gdp) === null ? 0 : Number(row.gdp) / 1_000_000_000,
  }));
  const depositsChartData = chartRows.map((row) => ({
    iso3: row.iso3 || "N/A",
    deposits: toNumeric(row.deposits) ?? 0,
  }));
  const maxDepositsForRadial = Math.max(1, ...chartRows.map((row) => Number(row.deposits || 0)));

  function addCountry(iso3) {
    const clean = normalize(iso3);
    if (!clean || selectedIso.includes(clean) || selectedIso.length >= 5) return;
    setSelectedIso((prev) => [...prev, clean]);
    setSearch("");
  }

  function removeCountry(iso3) {
    const clean = normalize(iso3);
    setSelectedIso((prev) => prev.filter((item) => item !== clean));
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
        <section className="panel compare-panel">
          <h2>Comparar paises</h2>
          <p className="muted">Selecciona 2 a 5 paises con chips sugeridos o buscador.</p>
          {!dbUp && (
            <p className="muted">
              No hay conexion a base de datos. Revisa variables DB_* en la terminal del backend.
            </p>
          )}

          <div className="chips-wrap">
            {selectedIso.map((iso) => {
              const country = countries.find((item) => normalize(item.iso3) === iso);
              return (
                <button
                  key={`selected-${iso}`}
                  type="button"
                  className="chip chip-selected"
                  onClick={() => removeCountry(iso)}
                >
                  {country ? country.country_name : iso} ✕
                </button>
              );
            })}
          </div>

          <p className="muted">Ejemplos</p>
          <div className="chips-wrap">
            {exampleCountries.map((country) => (
              <button
                key={`suggested-${country.iso3}`}
                type="button"
                className="chip"
                disabled={!canAddMoreCountries}
                onClick={() => addCountry(country.iso3)}
              >
                {country.country_name}
              </button>
            ))}
          </div>

          <div className="compare-search">
            <input
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="Buscar pais (ejemplo: C, Co, Cos...)"
              disabled={!canAddMoreCountries}
            />
          </div>
          {search.trim() && (
            <div className="search-results">
              {filteredCountries.map((country) => (
                <button
                  key={`result-${country.iso3}`}
                  type="button"
                  className="search-item"
                  disabled={!canAddMoreCountries}
                  onClick={() => addCountry(country.iso3)}
                >
                  {countryLabel(country)}
                </button>
              ))}
              {filteredCountries.length === 0 && (
                <p className="muted">Sin coincidencias para {search}.</p>
              )}
            </div>
          )}
          {!canAddMoreCountries && (
            <p className="muted">Ya tienes 5 paises seleccionados. Quita uno para agregar otro.</p>
          )}
          <small className="muted">Maximo 5 paises</small>

          {selectedIso.length < 2 && (
            <p className="muted">Selecciona al menos 2 paises para comparar.</p>
          )}
          {error && <p className="muted">Error: {error}</p>}

          {visibleRows.length > 0 && (
            <div className="compare-layout">
              <div className="compare-left">
                <div className="compare-table-wrap">
                  <table className="compare-table">
                    <thead>
                      <tr>
                        <th>Pais</th>
                        <th>
                          <AcronymHint
                            short="ISO"
                            full="Codigo estandar internacional del pais (ISO 3166-1 alfa-3)."
                          />
                        </th>
                        <th>Depositos</th>
                        <th>
                          <AcronymHint
                            short="PIB"
                            full="Producto Interno Bruto (valor total de bienes y servicios)."
                          />
                        </th>
                        <th>
                          <AcronymHint
                            short="IPC"
                            full="Indice de Percepcion de Corrupcion (0-100, mayor es mejor)."
                          />
                        </th>
                        <th>
                          <AcronymHint
                            short="EFI"
                            full="Indice de Fragilidad del Estado (menor suele indicar mayor estabilidad)."
                          />
                        </th>
                      </tr>
                    </thead>
                    <tbody>
                      {visibleRows.map((row) => {
                        const gdpRatio = Math.max(2, Math.round((Number(row.gdp || 0) / maxGdp) * 100));
                        return (
                          <tr key={row.iso3}>
                            <td>{row.country_name || "N/A"}</td>
                            <td>{row.iso3 || row.iso2 || "N/A"}</td>
                            <td>{formatNumber(row.deposits)}</td>
                            <td>
                              <div className="gdp-cell">
                                <span>{formatNumber(row.gdp)}</span>
                                <span className="gdp-bar" style={{ width: `${gdpRatio}%` }} />
                              </div>
                            </td>
                            <td>{formatNumber(row.cpi)}</td>
                            <td>{formatNumber(row.fsi)}</td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>

                <div className="gauge-grid">
                  {chartRows.map((row) => (
                    <CountryRadialCard
                      key={`radial-${row.iso3 || row.country_name}`}
                      row={row}
                      maxDeposits={maxDepositsForRadial}
                    />
                  ))}
                </div>
              </div>

              <div className="charts-side">
                <div className="panel chart-panel">
                  <h3>Comparacion PIB (miles de millones USD)</h3>
                  <ResponsiveContainer width="100%" height={150}>
                    <BarChart data={gdpChartData}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#2a3e55" />
                      <XAxis dataKey="iso3" stroke="#a8bfd8" />
                      <YAxis stroke="#a8bfd8" />
                      <Tooltip />
                      <Bar dataKey="gdpBillion" fill="#2e86ff" />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
                <div className="panel chart-panel">
                  <h3>Depositos por pais</h3>
                  <ResponsiveContainer width="100%" height={140}>
                    <BarChart data={depositsChartData}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#2a3e55" />
                      <XAxis dataKey="iso3" stroke="#a8bfd8" />
                      <YAxis stroke="#a8bfd8" />
                      <Tooltip />
                      <Bar dataKey="deposits" fill="#42c6b8" />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </div>
            </div>
          )}
        </section>
      </main>
    </div>
  );
}
