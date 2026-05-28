"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import dynamic from "next/dynamic";
import "leaflet/dist/leaflet.css";
import styles from "./consultas.module.css";
import AppHeader from "../../components/AppHeader";
import { t, useLang, withLang } from "../../lib/i18n";
import { detectDefaultCountryIso3 } from "../../lib/geo-default";

const MODES = [
  { id: "deposits" },
  { id: "combined" },
  { id: "spatial" },
  { id: "profile" },
];

const SpatialResultsMap = dynamic(() => import("./SpatialResultsMap"), { ssr: false });

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

function formatUsdBillions(value, decimals = 2) {
  if (value === null || value === undefined) return "N/A";
  const num = Number(value);
  if (!Number.isFinite(num)) return "N/A";
  return `${formatNumber(num / 1_000_000_000, decimals)} USD B`;
}

function hasText(value) {
  return value !== null && value !== undefined && String(value).trim() !== "";
}

function toNumberOrNull(value) {
  if (!hasText(value)) return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
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

export default function ConsultasClient() {
  const lang = useLang();
  const tr = (es, en) => (lang === "en" ? en : es);
  const modeLabel = (modeId) => {
    if (modeId === "deposits") return tr("Depositos por mineral", "Deposits by mineral");
    if (modeId === "combined") return tr("Minerales combinados", "Combined minerals");
    if (modeId === "spatial") return tr("Consulta espacial", "Spatial query");
    if (modeId === "profile") return tr("Perfil de pais", "Country profile");
    return modeId;
  };
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
  const [profileBounds, setProfileBounds] = useState(null);
  const geoDefaultAppliedRef = useRef(false);

  useEffect(() => {
    Promise.all([
      fetch(withLang("/api/v1/countries?limit=300", lang), { cache: "no-store" }).then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      }),
      fetch(withLang("/api/v1/minerals?limit=1000", lang), { cache: "no-store" }).then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      }),
    ])
      .then(([countriesPayload, mineralsPayload]) => {
        setCountries(Array.isArray(countriesPayload) ? countriesPayload : []);
        const mineralOptions = Array.isArray(mineralsPayload)
          ? mineralsPayload
              .map((item) => ({
                value: String(item.commod_source || item.commod || "").trim(),
                label: String(item.commod || item.commod_source || "").trim(),
              }))
              .filter((item) => item.value)
          : [];
        setMinerals(mineralOptions);
      })
      .catch(() => {
        setCountries([]);
        setMinerals([]);
      });
  }, [lang]);

  useEffect(() => {
    if (!countries.length) return;
    if (geoDefaultAppliedRef.current) return;
    geoDefaultAppliedRef.current = true;

    let mounted = true;
    detectDefaultCountryIso3(countries).then((iso3) => {
      if (!mounted || !iso3) return;
      setDepositFilters((prev) => ({ ...prev, countryIso: prev.countryIso || iso3 }));
      setCombinedFilters((prev) => ({ ...prev, countryIso: prev.countryIso || iso3 }));
      setSpatialFilters((prev) => ({ ...prev, countryIso: prev.countryIso || iso3 }));
    });
    return () => {
      mounted = false;
    };
  }, [countries]);

  useEffect(() => {
    fetch(withLang("/api/v1/queries/country-profile/bounds", lang), { cache: "no-store" })
      .then((response) => {
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        return response.json();
      })
      .then((payload) => setProfileBounds(payload || null))
      .catch(() => setProfileBounds(null));
  }, [lang]);

  useEffect(() => {
    setSpatialCountryDeposits([]);
    setSpatialFilters((prev) => ({ ...prev, baseDepId: "" }));
    if (!spatialFilters.countryIso) return;
    fetch(
      withLang(`/api/v1/explore/deposits?country_iso3=${spatialFilters.countryIso}&limit=5000`, lang),
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
              name: row.name || (lang === "en" ? `Deposit ${row.dep_id}` : `Deposito ${row.dep_id}`),
              latitude: Number(row.latitude),
              longitude: Number(row.longitude),
            }))
          : [];
        setSpatialCountryDeposits(options.filter((d) => Number.isFinite(d.dep_id)));
      })
      .catch(() => setSpatialCountryDeposits([]));
  }, [spatialFilters.countryIso, lang]);

  useEffect(() => {
    setSpatialMinerals([]);
    setSpatialFilters((prev) => ({ ...prev, mineral: "" }));
    if (!spatialFilters.countryIso) return;

    fetch(
      withLang(`/api/v1/terrain/frequent-minerals?country_iso3=${spatialFilters.countryIso}&show_all=true&limit=50`, lang),
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
  }, [spatialFilters.countryIso, lang]);

  const spatialMapRows = useMemo(
    () => (activeMode === "spatial" ? result.rows || [] : []),
    [activeMode, result.rows],
  );

  useEffect(() => {
    if (activeMode !== "spatial") return;
    if (!spatialFilters.countryIso || !spatialFilters.baseDepId) return;

    const controller = new AbortController();
    setLoading(true);
    setError("");

    const qs = new URLSearchParams({
      country_iso3: spatialFilters.countryIso,
      base_dep_id: String(spatialFilters.baseDepId),
      radius_km: String(spatialFilters.radiusKm),
      mineral: spatialFilters.mineral,
      limit: String(spatialFilters.limit),
    });

    fetch(withLang(`/api/v1/queries/spatial-nearby?${qs.toString()}`, lang), {
      cache: "no-store",
      signal: controller.signal,
    })
      .then(async (response) => {
        const payload = await response.json();
        if (!response.ok) {
          const detail = payload?.detail;
          const detailText = Array.isArray(detail)
            ? detail
                .map((item) => {
                  if (typeof item === "string") return item;
                  if (item && typeof item === "object") {
                    const loc = Array.isArray(item.loc) ? item.loc.join(".") : "";
                    const msg = item.msg || "";
                    return [loc, msg].filter(Boolean).join(": ");
                  }
                  return String(item ?? "");
                })
                .filter(Boolean)
                .join(" | ")
            : typeof detail === "string"
              ? detail
              : "";
          throw new Error(detailText || `HTTP ${response.status}`);
        }
        return payload;
      })
      .then((payload) => {
        setResult(payload || { result_count: 0, summary: "", rows: [] });
      })
      .catch((queryError) => {
        if (queryError?.name === "AbortError") return;
        setResult({ result_count: 0, summary: "", rows: [] });
        setError(queryError?.message || (lang === "en" ? "Could not run query." : "No fue posible ejecutar la consulta."));
      })
      .finally(() => setLoading(false));

    return () => controller.abort();
  }, [
    activeMode,
    spatialFilters.countryIso,
    spatialFilters.baseDepId,
    spatialFilters.radiusKm,
    spatialFilters.mineral,
    spatialFilters.limit,
    lang,
  ]);

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
        url = `/api/v1/queries/deposits-by-mineral?${qs.toString()}`;
      } else if (activeMode === "combined") {
        const qs = new URLSearchParams({
          country_iso3: combinedFilters.countryIso,
          mineral_a: combinedFilters.mineralA,
          mineral_b: combinedFilters.mineralB,
          exclude_mineral: combinedFilters.excludeMineral,
          limit: String(combinedFilters.limit),
        });
        url = `/api/v1/queries/combined-minerals?${qs.toString()}`;
      } else if (activeMode === "spatial") {
        if (!spatialFilters.countryIso || !spatialFilters.baseDepId) {
          setError(tr("Selecciona pais y deposito base para la consulta espacial.", "Select country and base deposit for spatial query."));
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
        url = `/api/v1/queries/spatial-nearby?${qs.toString()}`;
      } else {
        const gdpMinInput = toNumberOrNull(profileFilters.gdpMin);
        const gdpMaxInput = toNumberOrNull(profileFilters.gdpMax);
        const cpiMinInput = toNumberOrNull(profileFilters.cpiMin);
        const cpiMaxInput = toNumberOrNull(profileFilters.cpiMax);
        const fsiMinInput = toNumberOrNull(profileFilters.fsiMin);
        const fsiMaxInput = toNumberOrNull(profileFilters.fsiMax);

        const gdpMinRaw =
          gdpMinInput !== null ? gdpMinInput * 1_000_000_000 : toNumberOrNull(profileBounds?.gdp_min);
        const gdpMaxRaw =
          gdpMaxInput !== null ? gdpMaxInput * 1_000_000_000 : toNumberOrNull(profileBounds?.gdp_max);
        const cpiMinValue = cpiMinInput !== null ? cpiMinInput : toNumberOrNull(profileBounds?.cpi_min);
        const cpiMaxValue = cpiMaxInput !== null ? cpiMaxInput : toNumberOrNull(profileBounds?.cpi_max);
        const fsiMinValue = fsiMinInput !== null ? fsiMinInput : toNumberOrNull(profileBounds?.fsi_min);
        const fsiMaxValue = fsiMaxInput !== null ? fsiMaxInput : toNumberOrNull(profileBounds?.fsi_max);

        const qs = new URLSearchParams({
          min_deposits: String(profileFilters.minDeposits),
          limit: String(profileFilters.limit),
        });
        if (gdpMinRaw !== null) qs.set("gdp_min", String(gdpMinRaw));
        if (gdpMaxRaw !== null) qs.set("gdp_max", String(gdpMaxRaw));
        if (cpiMinValue !== null) qs.set("cpi_min", String(cpiMinValue));
        if (cpiMaxValue !== null) qs.set("cpi_max", String(cpiMaxValue));
        if (fsiMinValue !== null) qs.set("fsi_min", String(fsiMinValue));
        if (fsiMaxValue !== null) qs.set("fsi_max", String(fsiMaxValue));
        url = `/api/v1/queries/country-profile?${qs.toString()}`;
      }

      const response = await fetch(withLang(url, lang), { cache: "no-store" });
      const payload = await response.json();
      if (!response.ok) {
        const detail = payload?.detail;
        const detailText = Array.isArray(detail)
          ? detail
              .map((item) => {
                if (typeof item === "string") return item;
                if (item && typeof item === "object") {
                  const loc = Array.isArray(item.loc) ? item.loc.join(".") : "";
                  const msg = item.msg || "";
                  return [loc, msg].filter(Boolean).join(": ");
                }
                return String(item ?? "");
              })
              .filter(Boolean)
              .join(" | ")
          : typeof detail === "string"
            ? detail
            : "";
        throw new Error(detailText || `HTTP ${response.status}`);
      }
      setResult(payload || { result_count: 0, summary: "", rows: [] });
    } catch (queryError) {
      setResult({ result_count: 0, summary: "", rows: [] });
      setError(queryError?.message || tr("No fue posible ejecutar la consulta.", "Could not run query."));
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
            {tr("Pais", "Country")}
            <select
              value={depositFilters.countryIso}
              onChange={(e) => setDepositFilters((p) => ({ ...p, countryIso: e.target.value }))}
            >
              <option value="">{tr("Todos", "All")}</option>
              {countries.map((country) => (
                <option key={`dep-${country.iso3}-${country.country_name}`} value={country.iso3 || ""}>
                  {country.country_name} ({country.iso3 || "N/A"})
                </option>
              ))}
            </select>
          </label>
          <label className={styles.fieldMineral}>
            {tr("Mineral", "Mineral")}
            <select
              value={depositFilters.mineral}
              onChange={(e) => setDepositFilters((p) => ({ ...p, mineral: e.target.value }))}
            >
              <option value="">{tr("Todos", "All")}</option>
              {minerals.map((mineral) => (
                <option key={`dep-min-${mineral.value}`} value={mineral.value}>
                  {mineral.label}
                </option>
              ))}
            </select>
          </label>
          <label className={styles.fieldStatus}>
            {tr("Estado del deposito", "Deposit status")}
            <select
              value={depositFilters.status}
              onChange={(e) => setDepositFilters((p) => ({ ...p, status: e.target.value }))}
            >
              <option value="">{tr("Todos", "All")}</option>
              <option value="producer">{tr("Productor", "Producer")}</option>
              <option value="prospect">{tr("Prospecto", "Prospect")}</option>
              <option value="occurrence">{tr("Ocurrencia", "Occurrence")}</option>
              <option value="past producer">{tr("Productor historico", "Past Producer")}</option>
            </select>
          </label>
          <label className={styles.fieldMin}>
            {tr("Minimo minerales asociados", "Minimum associated minerals")}
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
            {tr("Limite de resultados", "Result limit")}
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
            {tr("Pais", "Country")}
            <select
              value={combinedFilters.countryIso}
              onChange={(e) => setCombinedFilters((p) => ({ ...p, countryIso: e.target.value }))}
            >
              <option value="">{tr("Todos", "All")}</option>
              {countries.map((country) => (
                <option key={`comb-${country.iso3}-${country.country_name}`} value={country.iso3 || ""}>
                  {country.country_name} ({country.iso3 || "N/A"})
                </option>
              ))}
            </select>
          </label>
          <label className={styles.fieldCombinedA}>
            {tr("Mineral A", "Mineral A")}
            <select
              value={combinedFilters.mineralA}
              onChange={(e) => setCombinedFilters((p) => ({ ...p, mineralA: e.target.value }))}
            >
              <option value="">{tr("Seleccionar", "Select")}</option>
              {minerals.map((mineral) => (
                <option key={`comb-a-${mineral.value}`} value={mineral.value}>
                  {mineral.label}
                </option>
              ))}
            </select>
          </label>
          <label className={styles.fieldCombinedB}>
            {tr("Mineral B", "Mineral B")}
            <select
              value={combinedFilters.mineralB}
              onChange={(e) => setCombinedFilters((p) => ({ ...p, mineralB: e.target.value }))}
            >
              <option value="">{tr("Seleccionar", "Select")}</option>
              {minerals.map((mineral) => (
                <option key={`comb-b-${mineral.value}`} value={mineral.value}>
                  {mineral.label}
                </option>
              ))}
            </select>
          </label>
          <label className={styles.fieldCombinedExclude}>
            {tr("Excluir mineral (opcional)", "Exclude mineral (optional)")}
            <select
              value={combinedFilters.excludeMineral}
              onChange={(e) => setCombinedFilters((p) => ({ ...p, excludeMineral: e.target.value }))}
            >
              <option value="">{tr("Ninguno", "None")}</option>
              {minerals.map((mineral) => (
                <option key={`comb-ex-${mineral.value}`} value={mineral.value}>
                  {mineral.label}
                </option>
              ))}
            </select>
          </label>
          <label className={styles.fieldCombinedLimit}>
            {tr("Limite", "Limit")}
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
            {tr("Pais", "Country")}
            <select
              value={spatialFilters.countryIso}
              onChange={(e) => setSpatialFilters((p) => ({ ...p, countryIso: e.target.value }))}
            >
              <option value="">{tr("Seleccionar pais", "Select country")}</option>
              {countries.map((country) => (
                <option key={`sp-${country.iso3}-${country.country_name}`} value={country.iso3 || ""}>
                  {country.country_name} ({country.iso3 || "N/A"})
                </option>
              ))}
            </select>
          </label>
          <label className={styles.fieldSpatialBase}>
            {tr("Deposito base", "Base deposit")}
            <select
              value={spatialFilters.baseDepId}
              onChange={(e) => setSpatialFilters((p) => ({ ...p, baseDepId: e.target.value }))}
              disabled={!spatialFilters.countryIso}
            >
              <option value="">{tr("Seleccionar deposito", "Select deposit")}</option>
              {spatialCountryDeposits.map((dep) => (
                <option key={`base-${dep.dep_id}`} value={dep.dep_id}>
                  {dep.name}
                </option>
              ))}
            </select>
          </label>
          <label className={styles.fieldSpatialRadius}>
            {tr("Radio (km)", "Radius (km)")}
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
            {tr("Mineral opcional", "Optional mineral")}
            <select
              value={spatialFilters.mineral}
              onChange={(e) => setSpatialFilters((p) => ({ ...p, mineral: e.target.value }))}
            >
              <option value="">{tr("Todos", "All")}</option>
              {(spatialMinerals.length
                ? spatialMinerals.map((mineral) => ({ value: mineral, label: mineral }))
                : minerals
              ).map((mineral, idx) => (
                <option key={`spatial-min-${mineral.value}-${idx}`} value={mineral.value}>
                  {mineral.label}
                </option>
              ))}
            </select>
          </label>
          <label className={styles.fieldSpatialLimit}>
            {tr("Limite", "Limit")}
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
          {tr("Minimo depositos", "Minimum deposits")}
          <input
            type="number"
            min={0}
            value={profileFilters.minDeposits}
            placeholder={profileBounds?.deposits_min !== undefined ? String(profileBounds.deposits_min) : ""}
            onChange={(e) =>
              setProfileFilters((p) => ({ ...p, minDeposits: Math.max(0, Number(e.target.value) || 0) }))
            }
          />
          {profileBounds && (
            <span className={styles.inputHint}>
              {tr("Sugerido", "Suggested")}: {formatNumber(profileBounds.deposits_min)} - {formatNumber(profileBounds.deposits_max)}
            </span>
          )}
        </label>
        <label>
          {tr("PIB minimo (USD B)", "GDP minimum (USD B)")}
          <input
            type="number"
            step="any"
            value={profileFilters.gdpMin}
            placeholder={
              profileBounds?.gdp_min !== null && profileBounds?.gdp_min !== undefined
                ? String((Number(profileBounds.gdp_min) / 1_000_000_000).toFixed(2))
                : ""
            }
            onChange={(e) => setProfileFilters((p) => ({ ...p, gdpMin: e.target.value }))}
          />
          {profileBounds && (
            <span className={styles.inputHint}>{tr("Min registrado", "Recorded min")}: {formatUsdBillions(profileBounds.gdp_min || 0)}</span>
          )}
        </label>
        <label>
          {tr("PIB maximo (USD B)", "GDP maximum (USD B)")}
          <input
            type="number"
            step="any"
            value={profileFilters.gdpMax}
            placeholder={
              profileBounds?.gdp_max !== null && profileBounds?.gdp_max !== undefined
                ? String((Number(profileBounds.gdp_max) / 1_000_000_000).toFixed(2))
                : ""
            }
            onChange={(e) => setProfileFilters((p) => ({ ...p, gdpMax: e.target.value }))}
          />
          {profileBounds && (
            <span className={styles.inputHint}>{tr("Max registrado", "Recorded max")}: {formatUsdBillions(profileBounds.gdp_max || 0)}</span>
          )}
        </label>
        <label>
          {tr("CPI minimo", "CPI minimum")}
          <input
            type="number"
            value={profileFilters.cpiMin}
            placeholder={profileBounds?.cpi_min !== null && profileBounds?.cpi_min !== undefined ? String(Math.trunc(profileBounds.cpi_min)) : ""}
            onChange={(e) => setProfileFilters((p) => ({ ...p, cpiMin: e.target.value }))}
          />
          {profileBounds && (
            <span className={styles.inputHint}>{tr("Min registrado", "Recorded min")}: {formatNumber(profileBounds.cpi_min || 0, 2)}</span>
          )}
        </label>
        <label>
          {tr("CPI maximo", "CPI maximum")}
          <input
            type="number"
            value={profileFilters.cpiMax}
            placeholder={profileBounds?.cpi_max !== null && profileBounds?.cpi_max !== undefined ? String(Math.trunc(profileBounds.cpi_max)) : ""}
            onChange={(e) => setProfileFilters((p) => ({ ...p, cpiMax: e.target.value }))}
          />
          {profileBounds && (
            <span className={styles.inputHint}>{tr("Max registrado", "Recorded max")}: {formatNumber(profileBounds.cpi_max || 0, 2)}</span>
          )}
        </label>
        <label>
          {tr("FSI minimo", "FSI minimum")}
          <input
            type="number"
            value={profileFilters.fsiMin}
            placeholder={profileBounds?.fsi_min !== null && profileBounds?.fsi_min !== undefined ? String(Math.trunc(profileBounds.fsi_min)) : ""}
            onChange={(e) => setProfileFilters((p) => ({ ...p, fsiMin: e.target.value }))}
          />
          {profileBounds && (
            <span className={styles.inputHint}>{tr("Min registrado", "Recorded min")}: {formatNumber(profileBounds.fsi_min || 0, 2)}</span>
          )}
        </label>
        <label>
          {tr("FSI maximo", "FSI maximum")}
          <input
            type="number"
            value={profileFilters.fsiMax}
            placeholder={profileBounds?.fsi_max !== null && profileBounds?.fsi_max !== undefined ? String(Math.trunc(profileBounds.fsi_max)) : ""}
            onChange={(e) => setProfileFilters((p) => ({ ...p, fsiMax: e.target.value }))}
          />
          {profileBounds && (
            <span className={styles.inputHint}>{tr("Max registrado", "Recorded max")}: {formatNumber(profileBounds.fsi_max || 0, 2)}</span>
          )}
        </label>
        <label className={styles.fieldProfileLimit}>
          {tr("Limite", "Limit")}
          <input
            type="number"
            min={1}
            max={1000}
            value={profileFilters.limit}
            onChange={(e) =>
              setProfileFilters((p) => ({ ...p, limit: Math.max(1, Number(e.target.value) || 200) }))
            }
          />
          <span className={`${styles.inputHint} ${styles.inputHintPlaceholder}`} aria-hidden="true">
            {tr("Reservado para alinear", "Reserved for alignment")}
          </span>
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
                <th>{tr("Pais", "Country")}</th>
                <th>{tr("Depositos", "Deposits")}</th>
                <th>PIB</th>
                <th>CPI</th>
                <th>FSI</th>
                <th>{tr("Intensidad relativa", "Relative intensity")}</th>
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
              <th>{tr("Deposito", "Deposit")}</th>
              <th>{tr("Pais", "Country")}</th>
              {activeMode === "spatial" && <th>{tr("Distancia (km)", "Distance (km)")}</th>}
              {activeMode === "deposits" && <th>{tr("Estado", "Status")}</th>}
              {activeMode === "deposits" && <th>{tr("Cantidad minerales", "Minerals count")}</th>}
              <th>{tr("Minerales asociados", "Associated minerals")}</th>
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
      <AppHeader />

      <main className="container">
        <section className="panel">
          <h2>{t(lang, "queriesTitle")}</h2>
          <p className="muted">
            {t(lang, "queriesHint")}
          </p>
          <p className="muted">
            {tr(
              "Construye busquedas usando filtros simples sin necesidad de conocimientos tecnicos.",
              "Build searches using simple filters without requiring technical knowledge.",
            )}
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
                {modeLabel(mode.id)}
              </button>
            ))}
          </div>
        </section>

        <section className="panel">
          <h3>
            {modeLabel(activeMode)}{" "}
            {activeMode === "spatial" && (
              <InfoHint
                text={tr(
                  "Busqueda basada en proximidad geografica usando registros georreferenciados.",
                  "Search based on geographic proximity using georeferenced records.",
                )}
              />
            )}
          </h3>
          <p className={`muted ${styles.helpText}`}>
            {activeMode === "spatial"
              ? tr(
                  "En modo espacial, cada cambio en filtros ejecuta la busqueda automaticamente.",
                  "In spatial mode, each filter change runs the query automatically.",
                )
              : tr(
                  "Selecciona filtros y ejecuta una consulta para ver resultados compactos.",
                  "Select filters and run a query to see compact results.",
                )}
          </p>
          {renderFilters()}
          {activeMode !== "spatial" && (
            <div className={styles.actionsRow}>
              <button type="button" onClick={runQuery}>
                {t(lang, "queriesRun")}
              </button>
            </div>
          )}
          {loading && <p className="muted">{tr("Consultando datos...", "Querying data...")}</p>}
          {error && <p className="muted">{tr("Error", "Error")}: {error}</p>}
        </section>

        <section className="panel">
          <div className={styles.resultWrap}>
            <div className={styles.resultHeader}>
              <p className={styles.resultCount}>{tr("Resultados", "Results")}: {formatNumber(result.result_count || 0)}</p>
              <div className={styles.actionsRow}>
                <button type="button" className={styles.mutedBtn} onClick={exportCsv}>
                  {t(lang, "queriesExportCsv")}
                </button>
                <button type="button" className={styles.mutedBtn} onClick={exportJson}>
                  {t(lang, "queriesExportJson")}
                </button>
                {activeMode === "spatial" && (
                  <button type="button" className={styles.mutedBtn} onClick={exportGeoJson}>
                    {t(lang, "queriesExportGeoJson")}
                  </button>
                )}
              </div>
            </div>

            <p className={`muted ${styles.summaryText}`}>
              {result.summary || tr("No se encontraron registros para los criterios seleccionados.", "No records found for selected criteria.")}
            </p>

            {renderTable()}

            {activeMode === "spatial" && spatialMapRows.length > 0 && (
              <div>
                <p className="muted">{tr("Vista espacial compacta", "Compact spatial view")}</p>
                <div className={styles.smallMap}>
                  <SpatialResultsMap rows={spatialMapRows} />
                </div>
              </div>
            )}
          </div>
        </section>

        <section className="panel">
          <p className="muted">
            {tr(
              "Los resultados se basan en registros integrados desde datasets geologicos y contextuales.",
              "Results are based on records integrated from geological and contextual datasets.",
            )}
          </p>
        </section>
      </main>
    </div>
  );
}
