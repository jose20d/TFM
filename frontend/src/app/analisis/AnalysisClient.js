"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
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

const CPI_GROUPS = [
  { id: "high", label: "Alta corrupcion percibida (CPI < 30)", color: "#d9534f" },
  { id: "mid", label: "Nivel medio (30 <= CPI < 60)", color: "#f0ad4e" },
  { id: "low", label: "Baja corrupcion percibida (CPI >= 60)", color: "#2dcf84" },
  { id: "unknown", label: "Sin CPI", color: "#7c8da5" },
];

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

function AnalysisTooltip({ active, payload, mode }) {
  if (!active || !payload?.length) return null;
  const data = payload[0].payload;
  return (
    <div className={styles.tooltip}>
      <p><strong>{data.country_name || "N/A"}</strong> ({data.iso3 || "N/A"})</p>
      {mode === "gdp" ? (
        <p>PIB: {formatNumber(data.gdpB, 2)} USD B</p>
      ) : (
        <p>FSI: {formatNumber(data.fsi, 2)}</p>
      )}
      <p>Depositos: {formatNumber(data.total_deposits)}</p>
      <p>CPI: {formatNumber(data.cpi, 2)}</p>
      {mode === "gdp" ? (
        <p>FSI: {formatNumber(data.fsi, 2)}</p>
      ) : (
        <p>PIB: {formatNumber(data.gdpB, 2)} USD B</p>
      )}
    </div>
  );
}

async function getJson(url) {
  const response = await fetch(url, { cache: "no-store" });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.json();
}

export default function AnalysisClient() {
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [dbUp, setDbUp] = useState(true);
  const [minDeposits, setMinDeposits] = useState(1);
  const [cpiFilter, setCpiFilter] = useState("all");

  useEffect(() => {
    setLoading(true);
    Promise.all([
      getJson("/api/backend/api/v1/analysis/country-overview"),
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
        setError(err.message || "No fue posible cargar el analisis.");
      })
      .finally(() => setLoading(false));
  }, []);

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
        <section className={`panel ${styles.headerPanel}`}>
          <h2>Analisis global de resultados</h2>
          <p className="muted">
            Vista de patrones globales por pais usando depositos, PIB, CPI y FSI.
          </p>
          <div className={styles.filters}>
            <label>
              Minimo de depositos
              <input
                type="number"
                min={0}
                value={minDeposits}
                onChange={(event) => setMinDeposits(Math.max(0, Number(event.target.value) || 0))}
              />
            </label>
            <label>
              Categoria CPI
              <select value={cpiFilter} onChange={(event) => setCpiFilter(event.target.value)}>
                <option value="all">Todas</option>
                <option value="high">Alta corrupcion percibida (CPI &lt; 30)</option>
                <option value="mid">Nivel medio (30 - 59)</option>
                <option value="low">Baja corrupcion percibida (CPI &gt;= 60)</option>
              </select>
            </label>
            <p className="muted">
              Paises filtrados: <strong>{formatNumber(filteredRows.length)}</strong>
            </p>
          </div>
          {!dbUp && (
            <p className="muted">
              No hay conexion a base de datos. Revisa variables DB_* en la terminal del backend.
            </p>
          )}
          {error && <p className="muted">Error: {error}</p>}
        </section>

        <section className={styles.chartsGrid}>
          <article className="panel">
            <h3>PIB vs depositos por pais (escala log)</h3>
            {loading ? (
              <p className="muted">Cargando datos...</p>
            ) : (
              <ResponsiveContainer width="100%" height={340}>
                <ScatterChart margin={{ top: 10, right: 20, bottom: 10, left: 10 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2a3e55" />
                  <XAxis
                    type="number"
                    dataKey="gdpB"
                    name="PIB (USD B)"
                    scale="log"
                    domain={["auto", "auto"]}
                    stroke="#a8bfd8"
                    tickFormatter={(value) => formatNumber(value)}
                  />
                  <YAxis
                    type="number"
                    dataKey="total_deposits"
                    name="Depositos"
                    scale="log"
                    domain={["auto", "auto"]}
                    stroke="#a8bfd8"
                    tickFormatter={(value) => formatNumber(value)}
                  />
                  <Tooltip content={<AnalysisTooltip mode="gdp" />} />
                  <Legend wrapperStyle={{ color: "#dbe9f8" }} />
                  {CPI_GROUPS.map((group) => (
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
          </article>

          <article className="panel">
            <h3>FSI vs depositos por pais (escala log)</h3>
            {loading ? (
              <p className="muted">Cargando datos...</p>
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
                    name="Depositos"
                    scale="log"
                    domain={["auto", "auto"]}
                    stroke="#a8bfd8"
                    tickFormatter={(value) => formatNumber(value)}
                  />
                  <Tooltip content={<AnalysisTooltip mode="fsi" />} />
                  <Legend wrapperStyle={{ color: "#dbe9f8" }} />
                  {CPI_GROUPS.map((group) => (
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
          </article>
        </section>
      </main>
    </div>
  );
}
