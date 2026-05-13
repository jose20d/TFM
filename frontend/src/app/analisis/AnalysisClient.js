"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import AppHeader from "../../components/AppHeader";
import { t, useLang, withLang } from "../../lib/i18n";
import {
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import styles from "./analysis.module.css";

const CPI_COLORS = Object.freeze({
  high: "#d9534f",
  mid: "#f0ad4e",
  low: "#2dcf84",
  unknown: "#7c8da5",
});

function toNumeric(value) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function formatNumber(value, decimals = null) {
  const numeric = toNumeric(value);
  if (numeric === null) return "N/A";
  const sign = numeric < 0 ? "-" : "";
  const absolute = Math.abs(numeric);
  const base = decimals === null
    ? (Number.isInteger(absolute) ? String(absolute) : absolute.toFixed(2))
    : absolute.toFixed(decimals);
  const [integerPart, decimalPart] = base.split(".");
  const groupedInteger = integerPart.replace(/\B(?=(\d{3})+(?!\d))/g, ".");
  if (!decimalPart) return `${sign}${groupedInteger}`;
  return `${sign}${groupedInteger},${decimalPart}`;
}

function cpiCategory(cpiValue) {
  const cpi = toNumeric(cpiValue);
  if (cpi === null) return "unknown";
  if (cpi < 30) return "high";
  if (cpi < 60) return "mid";
  return "low";
}

function groupByCategory(rows) {
  return rows.reduce(
    (acc, row) => {
      acc[row.cpiCategory].push(row);
      return acc;
    },
    { high: [], mid: [], low: [], unknown: [] },
  );
}

function AnalysisTooltip({ active, payload, mode, lang }) {
  if (!active || !payload?.length) return null;
  const data = payload[0].payload;
  return (
    <div className={styles.tooltip}>
      <p><strong>{data.country_name || "N/A"}</strong> ({data.iso3 || "N/A"})</p>
      {mode === "gdp" ? (
        <p>{lang === "en" ? "GDP" : "PIB"}: {formatNumber(data.gdpB, 2)} USD B</p>
      ) : (
        <p>FSI: {formatNumber(data.fsi, 2)}</p>
      )}
      <p>{lang === "en" ? "Deposits" : "Depositos"}: {formatNumber(data.total_deposits)}</p>
      <p>CPI: {formatNumber(data.cpi, 2)}</p>
      {mode === "gdp" ? (
        <p>FSI: {formatNumber(data.fsi, 2)}</p>
      ) : (
        <p>{lang === "en" ? "GDP" : "PIB"}: {formatNumber(data.gdpB, 2)} USD B</p>
      )}
    </div>
  );
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

async function getJson(url) {
  const response = await fetch(url, { cache: "no-store" });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.json();
}

export default function AnalysisClient() {
  const lang = useLang();
  const tr = useCallback((es, en) => (lang === "en" ? en : es), [lang]);
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [dbUp, setDbUp] = useState(true);
  const [minDeposits, setMinDeposits] = useState(1);
  const [cpiFilter, setCpiFilter] = useState("all");

  useEffect(() => {
    setLoading(true);
    Promise.all([
      getJson(withLang("/api/backend/api/v1/analysis/country-overview", lang)),
      getJson("/api/backend/api/v1/health"),
    ])
      .then(([analysisRows, health]) => {
        setRows(Array.isArray(analysisRows) ? analysisRows : []);
        setDbUp(Boolean(health?.db));
        setError("");
      })
      .catch((err) => {
        setRows([]);
        setDbUp(false);
        setError(err.message || tr("No fue posible cargar el analisis.", "Could not load analysis."));
      })
      .finally(() => setLoading(false));
  }, [lang, tr]);

  const normalizedRows = useMemo(
    () =>
      rows.map((row) => ({
        ...row,
        total_deposits: toNumeric(row.total_deposits),
        gdp: toNumeric(row.gdp),
        gdpB: toNumeric(row.gdp) === null ? null : Number(row.gdp) / 1_000_000_000,
        cpi: toNumeric(row.cpi),
        fsi: toNumeric(row.fsi),
        cpiCategory: cpiCategory(row.cpi),
      })),
    [rows],
  );

  const filteredRows = useMemo(
    () =>
      normalizedRows.filter((row) => {
        const deposits = row.total_deposits;
        if (deposits === null || deposits < minDeposits) return false;
        if (cpiFilter === "all") return true;
        return row.cpiCategory === cpiFilter;
      }),
    [normalizedRows, minDeposits, cpiFilter],
  );

  const gdpScatterRows = useMemo(
    () =>
      filteredRows.filter(
        (row) =>
          row.gdpB !== null &&
          row.gdpB > 0 &&
          row.total_deposits !== null &&
          row.total_deposits > 0,
      ),
    [filteredRows],
  );

  const fsiScatterRows = useMemo(
    () =>
      filteredRows.filter(
        (row) =>
          row.fsi !== null &&
          row.fsi > 0 &&
          row.total_deposits !== null &&
          row.total_deposits > 0,
      ),
    [filteredRows],
  );

  const gdpByCategory = useMemo(() => groupByCategory(gdpScatterRows), [gdpScatterRows]);
  const fsiByCategory = useMemo(() => groupByCategory(fsiScatterRows), [fsiScatterRows]);
  const cpiGroups = useMemo(
    () => [
      { id: "high", label: tr("Alta corrupcion percibida (CPI < 30)", "High perceived corruption (CPI < 30)"), color: CPI_COLORS.high },
      { id: "mid", label: tr("Nivel medio (30 <= CPI < 60)", "Medium level (30 <= CPI < 60)"), color: CPI_COLORS.mid },
      { id: "low", label: tr("Baja corrupcion percibida (CPI >= 60)", "Low perceived corruption (CPI >= 60)"), color: CPI_COLORS.low },
      { id: "unknown", label: tr("Sin CPI", "No CPI"), color: CPI_COLORS.unknown },
    ],
    [tr],
  );

  return (
    <div className="page-shell">
      <AppHeader />

      <main className="container">
        <section className={`panel ${styles.headerPanel}`}>
          <h2>{t(lang, "analysisTitle")}</h2>
          <p className="muted">
            {t(lang, "analysisHint")}
          </p>
          <div className={styles.filters}>
            <label>
              {tr("Minimo de depositos", "Minimum deposits")}
              <input
                type="number"
                min={0}
                value={minDeposits}
                onChange={(event) => setMinDeposits(Math.max(0, Number(event.target.value) || 0))}
              />
            </label>
            <label>
              {tr("Categoria CPI", "CPI Category")}
              <select value={cpiFilter} onChange={(event) => setCpiFilter(event.target.value)}>
                <option value="all">{tr("Todas", "All")}</option>
                <option value="high">
                  {tr("Alta corrupcion percibida (CPI < 30)", "High perceived corruption (CPI < 30)")}
                </option>
                <option value="mid">{tr("Nivel medio (30 - 59)", "Medium level (30 - 59)")}</option>
                <option value="low">
                  {tr("Baja corrupcion percibida (CPI >= 60)", "Low perceived corruption (CPI >= 60)")}
                </option>
              </select>
            </label>
            <p className="muted">
              {tr("Paises filtrados", "Filtered countries")}:{" "}
              <strong>{formatNumber(filteredRows.length)}</strong>
            </p>
          </div>
          {!dbUp && (
            <p className="muted">
              {tr(
                "No hay conexion a base de datos. Revisa variables DB_* en la terminal del backend.",
                "No database connection. Check DB_* variables in the backend terminal.",
              )}
            </p>
          )}
          {error && <p className="muted">{tr("Error", "Error")}: {error}</p>}
        </section>

        <section className={styles.chartsGrid}>
          <article className="panel">
            <h3>
              <InfoHint
                label={lang === "en" ? "GDP vs deposits by country (log scale)" : "PIB vs depositos por pais (escala log)"}
                text={
                  lang === "en"
                    ? "Log scale reduces the impact of outliers for better comparative visualization."
                    : "La escala logaritmica reduce el impacto de valores extremos para mejorar la visualizacion comparativa."
                }
              />
            </h3>
            {loading ? (
              <p className="muted">{lang === "en" ? "Loading data..." : "Cargando datos..."}</p>
            ) : (
              <ResponsiveContainer width="100%" height={340}>
                <ScatterChart margin={{ top: 10, right: 20, bottom: 10, left: 10 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2a3e55" />
                  <XAxis
                    type="number"
                    dataKey="gdpB"
                    name={tr("PIB (USD B)", "GDP (USD B)")}
                    scale="log"
                    domain={["auto", "auto"]}
                    stroke="#a8bfd8"
                    tickFormatter={(value) => formatNumber(value)}
                  />
                  <YAxis
                    type="number"
                    dataKey="total_deposits"
                    name={tr("Depositos", "Deposits")}
                    scale="log"
                    domain={["auto", "auto"]}
                    stroke="#a8bfd8"
                    tickFormatter={(value) => formatNumber(value)}
                  />
                  <Tooltip content={<AnalysisTooltip mode="gdp" lang={lang} />} />
                  <Legend wrapperStyle={{ color: "#dbe9f8" }} />
                  {cpiGroups.map((group) => (
                    <Scatter
                      key={`gdp-${group.id}`}
                      name={group.label}
                      data={gdpByCategory[group.id]}
                      fill={group.color}
                    />
                  ))}
                </ScatterChart>
              </ResponsiveContainer>
            )}
            <p className="muted">
              <InfoHint
                label={lang === "en" ? "CPI Legend" : "Leyenda CPI"}
                text={
                  lang === "en"
                    ? "Red = high perceived corruption; Yellow = medium level; Green = low perceived corruption."
                    : "Rojo = alta corrupcion percibida; Amarillo = nivel medio; Verde = baja corrupcion percibida."
                }
              />
            </p>
          </article>

          <article className="panel">
            <h3>
              <InfoHint
                label={lang === "en" ? "FSI vs deposits by country (log scale)" : "FSI vs depositos por pais (escala log)"}
                text={
                  lang === "en"
                    ? "Log scale reduces the impact of outliers for better comparative visualization."
                    : "La escala logaritmica reduce el impacto de valores extremos para mejorar la visualizacion comparativa."
                }
              />
            </h3>
            {loading ? (
              <p className="muted">{lang === "en" ? "Loading data..." : "Cargando datos..."}</p>
            ) : (
              <ResponsiveContainer width="100%" height={340}>
                <ScatterChart margin={{ top: 10, right: 20, bottom: 10, left: 10 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2a3e55" />
                  <XAxis
                    type="number"
                    dataKey="fsi"
                    name="FSI"
                    scale="log"
                    domain={["auto", "auto"]}
                    stroke="#a8bfd8"
                    tickFormatter={(value) => formatNumber(value)}
                  />
                  <YAxis
                    type="number"
                    dataKey="total_deposits"
                    name={tr("Depositos", "Deposits")}
                    scale="log"
                    domain={["auto", "auto"]}
                    stroke="#a8bfd8"
                    tickFormatter={(value) => formatNumber(value)}
                  />
                  <Tooltip content={<AnalysisTooltip mode="fsi" lang={lang} />} />
                  <Legend wrapperStyle={{ color: "#dbe9f8" }} />
                  {cpiGroups.map((group) => (
                    <Scatter
                      key={`fsi-${group.id}`}
                      name={group.label}
                      data={fsiByCategory[group.id]}
                      fill={group.color}
                    />
                  ))}
                </ScatterChart>
              </ResponsiveContainer>
            )}
            <p className="muted">
              <InfoHint
                label={lang === "en" ? "CPI Legend" : "Leyenda CPI"}
                text={
                  lang === "en"
                    ? "Red = high perceived corruption; Yellow = medium level; Green = low perceived corruption."
                    : "Rojo = alta corrupcion percibida; Amarillo = nivel medio; Verde = baja corrupcion percibida."
                }
              />
            </p>
          </article>
        </section>
      </main>
    </div>
  );
}
