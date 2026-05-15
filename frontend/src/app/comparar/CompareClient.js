"use client";

import { useEffect, useMemo, useState } from "react";
import AppHeader from "../../components/AppHeader";
import { t, useLang, withLang } from "../../lib/i18n";
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

// Instruccion de formato numerico para toda la pagina "Comparar":
// - Separador de miles: "."
// - Separador decimal: ","
// - Nulos/invalidos: "N/A"
// - PIB: siempre en miles de millones (USD B) con 2 decimales
const NUMERIC_FORMAT = Object.freeze({
  thousandSeparator: ".",
  decimalSeparator: ",",
  nullLabel: "N/A",
  gdpUnitLabel: "USD B",
  gdpDecimals: 2,
});

function formatNumeric(value, options = {}) {
  const { decimals = null } = options;
  if (value === null || value === undefined) return NUMERIC_FORMAT.nullLabel;
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return NUMERIC_FORMAT.nullLabel;
  const sign = numeric < 0 ? "-" : "";
  const absolute = Math.abs(numeric);
  const base =
    decimals === null
      ? (Number.isInteger(absolute) ? String(absolute) : absolute.toFixed(2))
      : absolute.toFixed(decimals);
  const [integerPart, decimalPart] = base.split(".");
  const groupedInteger = integerPart.replace(/\B(?=(\d{3})+(?!\d))/g, NUMERIC_FORMAT.thousandSeparator);
  if (!decimalPart) return `${sign}${groupedInteger}`;
  return `${sign}${groupedInteger}${NUMERIC_FORMAT.decimalSeparator}${decimalPart}`;
}

function formatNumber(value) {
  return formatNumeric(value);
}

function formatBillions(value) {
  return formatNumeric(value, { decimals: NUMERIC_FORMAT.gdpDecimals });
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
    <span className="acronym-hint" data-tooltip={full} tabIndex={0}>
      {short}
    </span>
  );
}

function InfoHint({ text }) {
  return (
    <span className="acronym-hint" data-tooltip={text} aria-label={text} tabIndex={0}>
      <span className="hint-icon">ⓘ</span>
    </span>
  );
}

function CountryRadialCard({ row, maxDeposits, lang }) {
  const tr = (es, en) => (lang === "en" ? en : es);
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
    { metric: tr("Depositos", "Deposits"), value: depositsPct, color: "#2e86ff", raw: depositsRaw, suffix: "" },
  ];

  return (
    <article className="panel radial-country-card">
      <h4>
        {row.country_name || tr("Pais", "Country")} ({countryIso})
        {" "}
        <InfoHint
          text={tr(
            "Resumen visual comparativo de indicadores seleccionados.",
            "Comparative visual summary of selected indicators.",
          )}
        />
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
            {item.metric}: {item.raw === null ? NUMERIC_FORMAT.nullLabel : formatNumber(item.raw)}
            {item.suffix}
          </p>
        ))}
      </div>
    </article>
  );
}

export default function CompareClient() {
  const lang = useLang();
  const tr = (es, en) => (lang === "en" ? en : es);
  const [countries, setCountries] = useState([]);
  const [selectedIso, setSelectedIso] = useState([]);
  const [rows, setRows] = useState([]);
  const [search, setSearch] = useState("");
  const [error, setError] = useState("");
  const [dbUp, setDbUp] = useState(true);
  const [sortConfig, setSortConfig] = useState({
    key: "country_name",
    direction: "asc",
  });

  useEffect(() => {
    Promise.all([
      getJson(withLang("/api/backend/api/v1/countries?limit=300", lang)),
      getJson("/api/backend/api/v1/home/defaults"),
      getJson("/api/backend/api/v1/health"),
    ])
      .then(([countriesData, defaultsData, healthData]) => {
        const allCountries = Array.isArray(countriesData) ? countriesData : [];
        const defaultsFromBackend = Array.isArray(defaultsData?.compare_iso3)
          ? defaultsData.compare_iso3
          : [];
        const preferredIso = ["CRI", "MEX", "ESP"];
        const availablePreferred = preferredIso.filter((iso) =>
          allCountries.some((country) => normalize(country.iso3) === iso),
        );
        const fallbackIso = Array.from(
          new Set(defaultsFromBackend.map((iso) => normalize(iso)).filter((iso) => iso.length === 3)),
        ).slice(0, 3);
        const cleanIso = (availablePreferred.length >= 2 ? availablePreferred : fallbackIso).slice(0, 3);

        setCountries(allCountries);
        setSelectedIso(cleanIso);
        setDbUp(Boolean(healthData?.db));
      })
      .catch(() => {
        setCountries([]);
        setSelectedIso([]);
        setDbUp(false);
      });
  }, [lang]);

  useEffect(() => {
    if (selectedIso.length < 2) {
      return;
    }
    const qs = selectedIso.map((iso) => `iso3=${encodeURIComponent(iso)}`).join("&");
    getJson(withLang(`/api/backend/api/v1/countries/compare?${qs}`, lang))
      .then((data) => {
        setRows(Array.isArray(data) ? data : []);
        setError("");
      })
      .catch((err) => {
        setRows([]);
        setError(err.message);
      });
  }, [selectedIso, lang]);

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
  const sortedRows = useMemo(() => {
    const rowsCopy = [...visibleRows];
    const { key, direction } = sortConfig;
    const sortOrder = direction === "asc" ? 1 : -1;

    rowsCopy.sort((a, b) => {
      let aValue;
      let bValue;

      if (key === "country_name") {
        aValue = String(a.country_name || "");
        bValue = String(b.country_name || "");
      } else {
        aValue = toNumeric(a[key]);
        bValue = toNumeric(b[key]);
      }

      const aMissing = aValue === null || aValue === "";
      const bMissing = bValue === null || bValue === "";
      if (aMissing && bMissing) return 0;
      if (aMissing) return 1;
      if (bMissing) return -1;

      if (typeof aValue === "string" && typeof bValue === "string") {
        return aValue.localeCompare(bValue, lang === "en" ? "en" : "es") * sortOrder;
      }
      return (aValue - bValue) * sortOrder;
    });

    return rowsCopy;
  }, [visibleRows, sortConfig, lang]);

  const exampleCountries = useMemo(() => {
    const exampleIso3 = ["CRI", "MEX", "ESP"];
    return exampleIso3
      .map((iso3) =>
        countries.find(
          (country) => normalize(country.iso3) === iso3,
        ),
      )
      .filter(Boolean);
  }, [countries]);

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
  const acronymLegend = useMemo(
    () => [
      {
        short: "ISO",
        es: "Codigo estandar internacional del pais (ISO 3166-1 alfa-3).",
        en: "International country standard code (ISO 3166-1 alpha-3).",
      },
      {
        short: "PIB / GDP",
        es: "Producto Interno Bruto. Indica el tamano economico del pais.",
        en: "Gross Domestic Product. Indicates the economic size of the country.",
      },
      {
        short: "IPC / CPI",
        es: "Indice de Percepcion de Corrupcion. Valores altos indican menor corrupcion percibida.",
        en: "Corruption Perceptions Index. Higher values indicate lower perceived corruption.",
      },
      {
        short: "EFI / FSI",
        es: "Indice de Fragilidad Estatal. Valores altos indican mayor fragilidad institucional.",
        en: "Fragile States Index. Higher values indicate greater institutional fragility.",
      },
      {
        short: "Dep/PIB",
        es: "Relacion entre depositos registrados y PIB para comparar intensidad relativa.",
        en: "Ratio between registered deposits and GDP to compare relative intensity.",
      },
    ],
    [],
  );

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

  function toggleSort(key) {
    setSortConfig((prev) => {
      if (prev.key === key) {
        return {
          key,
          direction: prev.direction === "asc" ? "desc" : "asc",
        };
      }
      return { key, direction: "asc" };
    });
  }

  function sortMarker(key) {
    if (sortConfig.key !== key) return "↕";
    return sortConfig.direction === "asc" ? "↑" : "↓";
  }

  function sortHint(key, labelEs, labelEn) {
    const label = lang === "en" ? labelEn : labelEs;
    if (sortConfig.key !== key) {
      return lang === "en"
        ? `Sort by ${label} (click for ascending)`
        : `Ordenar por ${label} (click para ascendente)`;
    }
    if (sortConfig.direction === "asc") {
      return lang === "en"
        ? `Current order by ${label}: ascending (click for descending)`
        : `Orden actual por ${label}: ascendente (click para descendente)`;
    }
    return lang === "en"
      ? `Current order by ${label}: descending (click for ascending)`
      : `Orden actual por ${label}: descendente (click para ascendente)`;
  }

  return (
    <div className="page-shell">
      <AppHeader />

      <main className="container">
        <section className="panel compare-panel">
          <h2>{t(lang, "compareTitle")}</h2>
          <p className="muted">{t(lang, "compareHint")}</p>
          {!dbUp && (
            <p className="muted">
              {tr(
                "No hay conexion a base de datos. Revisa variables DB_* en la terminal del backend.",
                "No database connection. Check DB_* variables in the backend terminal.",
              )}
            </p>
          )}

          <div className="chips-wrap compare-selected-chips">
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

          <p className="muted">{t(lang, "compareExamples")}</p>
          <div className="chips-wrap">
            {exampleCountries.map((country) => (
              <button
                key={`suggested-${country.iso3}`}
                type="button"
                className="chip"
                disabled={!canAddMoreCountries || selectedIso.includes(normalize(country.iso3))}
                onClick={() => addCountry(country.iso3)}
              >
                {country.country_name}
              </button>
            ))}
          </div>
          <div className="compare-search-row">
            <div className="compare-search">
              <input
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder={t(lang, "compareSearch")}
                disabled={!canAddMoreCountries}
              />
            </div>
            <div className="acronym-guide-row">
              <div className="acronym-guide acronym-guide-inline">
                <span className="acronym-guide-title">
                  {tr("Guia de acronimos", "Acronym guide")}
                </span>
                <div className="acronym-guide-chips">
                  {acronymLegend.map((item) => (
                    <span
                      key={`acr-${item.short}`}
                      className="acronym-guide-chip acronym-hint"
                      data-tooltip={lang === "en" ? item.en : item.es}
                      tabIndex={0}
                    >
                      {item.short}
                    </span>
                  ))}
                </div>
              </div>
            </div>
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
                <p className="muted">
                  {lang === "en" ? `No matches for ${search}.` : `Sin coincidencias para ${search}.`}
                </p>
              )}
            </div>
          )}
          {!canAddMoreCountries && (
            <p className="muted">
              {lang === "en"
                ? "You already selected 5 countries. Remove one to add another."
                : "Ya tienes 5 paises seleccionados. Quita uno para agregar otro."}
            </p>
          )}
          <small className="muted">{lang === "en" ? "Maximum 5 countries" : "Maximo 5 paises"}</small>

          {selectedIso.length < 2 && (
            <p className="muted">{t(lang, "compareRunHint")}</p>
          )}
          {error && <p className="muted">{tr("Error", "Error")}: {error}</p>}

          {visibleRows.length > 0 && (
            <div className="compare-layout">
              <div className="compare-left">
                <div className="compare-table-wrap">
                  <table className="compare-table">
                    <thead>
                      <tr>
                        <th>
                          <button
                            type="button"
                            className="table-sort-btn acronym-hint"
                            onClick={() => toggleSort("country_name")}
                            data-tooltip={sortHint("country_name", "Pais", "Country")}
                            aria-label={sortHint("country_name", "Pais", "Country")}
                          >
                            {lang === "en" ? "Country" : "Pais"} {sortMarker("country_name")}
                          </button>
                        </th>
                        <th>
                          <AcronymHint
                            short="ISO"
                            full={
                              lang === "en"
                                ? "International country standard code (ISO 3166-1 alpha-3)."
                                : "Codigo estandar internacional del pais (ISO 3166-1 alfa-3)."
                            }
                          />
                        </th>
                        <th>
                          <button
                            type="button"
                            className="table-sort-btn acronym-hint"
                            onClick={() => toggleSort("deposits")}
                            data-tooltip={sortHint("deposits", "Depositos", "Deposits")}
                            aria-label={sortHint("deposits", "Depositos", "Deposits")}
                          >
                            {lang === "en" ? "Deposits" : "Depositos"}{" "}
                            <AcronymHint
                              short="Dep/PIB"
                              full={
                                lang === "en"
                                  ? "Ratio between registered deposits and country economic size. It compares relative intensity instead of absolute volume."
                                  : "Relacion entre depositos registrados y tamano economico del pais. Permite comparar intensidad relativa y no volumen absoluto."
                              }
                            />{" "}
                            {sortMarker("deposits")}
                          </button>
                        </th>
                        <th>
                          <button
                            type="button"
                            className="table-sort-btn acronym-hint"
                            onClick={() => toggleSort("gdp")}
                            data-tooltip={sortHint("gdp", "PIB", "GDP")}
                            aria-label={sortHint("gdp", "PIB", "GDP")}
                          >
                            <AcronymHint
                              short="PIB"
                              full={
                                lang === "en"
                                  ? "GDP: Gross Domestic Product used as a comparative economic indicator."
                                  : "PIB: Producto Interno Bruto utilizado como indicador economico comparativo."
                              }
                            />{" "}
                            {sortMarker("gdp")}
                          </button>
                        </th>
                        <th>
                          <button
                            type="button"
                            className="table-sort-btn acronym-hint"
                            onClick={() => toggleSort("cpi")}
                            data-tooltip={sortHint("cpi", "IPC", "CPI")}
                            aria-label={sortHint("cpi", "IPC", "CPI")}
                          >
                            <AcronymHint
                              short="IPC"
                              full={
                                lang === "en"
                                  ? "CPI: Corruption Perceptions Index. Higher values indicate lower perceived corruption."
                                  : "IPC/CPI: indice de percepcion de corrupcion. Valores altos indican menor corrupcion percibida."
                              }
                            />{" "}
                            {sortMarker("cpi")}
                          </button>
                        </th>
                        <th>
                          <button
                            type="button"
                            className="table-sort-btn acronym-hint"
                            onClick={() => toggleSort("fsi")}
                            data-tooltip={sortHint("fsi", "EFI", "FSI")}
                            aria-label={sortHint("fsi", "EFI", "FSI")}
                          >
                            <AcronymHint
                              short="EFI"
                              full={
                                lang === "en"
                                  ? "FSI: Fragile States Index. Higher values indicate greater institutional fragility."
                                  : "EFI/FSI: indice de fragilidad estatal. Valores altos indican mayor fragilidad institucional."
                              }
                            />{" "}
                            {sortMarker("fsi")}
                          </button>
                        </th>
                      </tr>
                    </thead>
                    <tbody>
                      {sortedRows.map((row) => {
                        const gdpBillion = toNumeric(row.gdp) === null ? null : Number(row.gdp) / 1_000_000_000;
                        return (
                          <tr key={row.iso3}>
                            <td>{row.country_name || NUMERIC_FORMAT.nullLabel}</td>
                            <td>{row.iso3 || row.iso2 || NUMERIC_FORMAT.nullLabel}</td>
                            <td>{formatNumber(row.deposits)}</td>
                            <td>
                              <div className="gdp-cell">
                                <span>
                                  {gdpBillion === null
                                    ? NUMERIC_FORMAT.nullLabel
                                    : `${formatBillions(gdpBillion)} ${NUMERIC_FORMAT.gdpUnitLabel}`}
                                </span>
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
                      lang={lang}
                    />
                  ))}
                </div>
              </div>

              <div className="charts-side">
                <div className="panel chart-panel">
                  <h3>
                    {`${tr("Comparacion PIB", "GDP comparison")} (${NUMERIC_FORMAT.gdpUnitLabel})`}{" "}
                    <InfoHint text={tr("Valores exactos disponibles al pasar el cursor sobre cada barra.", "Exact values are available when hovering each bar.")} />
                  </h3>
                  <ResponsiveContainer width="100%" height={150}>
                    <BarChart data={gdpChartData}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#2a3e55" />
                      <XAxis dataKey="iso3" stroke="#a8bfd8" />
                      <YAxis stroke="#a8bfd8" tickFormatter={(value) => formatBillions(value)} />
                      <Tooltip
                        formatter={(value) => [
                          `${formatBillions(value)} ${NUMERIC_FORMAT.gdpUnitLabel}`,
                          lang === "en" ? "GDP" : "PIB",
                        ]}
                      />
                      <Bar dataKey="gdpBillion" fill="#2e86ff" />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
                <div className="panel chart-panel">
                  <h3>
                    {tr("Depositos por pais", "Deposits by country")}{" "}
                    <InfoHint text={tr("Valores exactos disponibles al pasar el cursor sobre cada barra.", "Exact values are available when hovering each bar.")} />
                  </h3>
                  <ResponsiveContainer width="100%" height={140}>
                    <BarChart data={depositsChartData}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#2a3e55" />
                      <XAxis dataKey="iso3" stroke="#a8bfd8" />
                      <YAxis stroke="#a8bfd8" tickFormatter={(value) => formatNumber(value)} />
                      <Tooltip formatter={(value) => [formatNumber(value), lang === "en" ? "Deposits" : "Depositos"]} />
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
