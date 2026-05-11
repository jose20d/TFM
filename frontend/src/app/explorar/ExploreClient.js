"use client";

import { useEffect, useMemo, useState } from "react";
import dynamic from "next/dynamic";
import AppHeader from "../../components/AppHeader";
import { t, useLang, withLang } from "../../lib/i18n";
const ExploreMap = dynamic(() => import("./ExploreMap"), { ssr: false });

const DEFAULT_LIMIT = 500;
const RESULTS_PAGE_SIZE = 300;

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

export default function ExploreClient() {
  const lang = useLang();
  const [isHydrated, setIsHydrated] = useState(false);
  const [countries, setCountries] = useState([]);
  const [minerals, setMinerals] = useState([]);
  const [maxLimit, setMaxLimit] = useState(DEFAULT_LIMIT);
  const [countryIsoInput, setCountryIsoInput] = useState("");
  const [mineralInput, setMineralInput] = useState("");
  const [limitInput, setLimitInput] = useState(DEFAULT_LIMIT);
  const [filters, setFilters] = useState({
    countryIso: "",
    mineral: "",
    limit: DEFAULT_LIMIT,
  });
  const [listRows, setListRows] = useState([]);
  const [totalRows, setTotalRows] = useState(0);
  const [page, setPage] = useState(0);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);

  const buildExploreParams = useMemo(
    () => (currentFilters) => {
      const qs = new URLSearchParams();
      if (currentFilters.countryIso) qs.set("country_iso3", currentFilters.countryIso);
      if (currentFilters.mineral.trim()) qs.set("mineral", currentFilters.mineral.trim());
      return qs;
    },
    [],
  );

  useEffect(() => {
    Promise.all([
      getJson(withLang("/api/backend/api/v1/countries?limit=300", lang)),
      getJson(withLang("/api/backend/api/v1/minerals?limit=1000", lang)),
    ])
      .then(([countriesData, mineralsData]) => {
        setCountries(Array.isArray(countriesData) ? countriesData : []);
        const options = Array.isArray(mineralsData)
          ? mineralsData
              .map((item) => ({
                value: String(item.commod_source || item.commod || "").trim(),
                label: String(item.commod || item.commod_source || "").trim(),
              }))
              .filter((item) => item.value)
          : [];
        setMinerals(options);
      })
      .catch(() => {
        setCountries([]);
        setMinerals([]);
      });
  }, [lang]);

  useEffect(() => {
    const qs = new URLSearchParams();
    if (countryIsoInput) qs.set("country_iso3", countryIsoInput);
    getJson(withLang(`/api/backend/api/v1/explore/limits?${qs.toString()}`, lang))
      .then((limitsData) => {
        const fetchedMax = Number(limitsData?.max_limit);
        const normalizedMax = Number.isFinite(fetchedMax) && fetchedMax > 0 ? fetchedMax : DEFAULT_LIMIT;
        setMaxLimit(normalizedMax);
        setLimitInput(countryIsoInput ? Math.min(DEFAULT_LIMIT, normalizedMax) : DEFAULT_LIMIT);
      })
      .catch(() => {
        setMaxLimit(DEFAULT_LIMIT);
        setLimitInput(DEFAULT_LIMIT);
      });
  }, [countryIsoInput, lang]);

  const limitOptions = useMemo(() => {
    const max = Math.max(1, Number(maxLimit) || DEFAULT_LIMIT);
    const values = [];
    if (max <= 100) {
      values.push(max);
    } else if (max <= 500) {
      values.push(100, Math.floor(max * 0.8), max);
    } else if (max <= 5000) {
      values.push(500, Math.floor(max * 0.6), max);
    } else {
      values.push(500, 5000, Math.floor(max * 0.25), Math.floor(max * 0.6), max);
    }
    return Array.from(new Set(values.filter((v) => v >= 1 && v <= max))).sort((a, b) => a - b);
  }, [maxLimit]);

  const maxOptionLabel = lang === "en" ? "Maximum" : "Maximo";

  useEffect(() => {
    setIsHydrated(true);
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    const qs = buildExploreParams(filters);
    setLoading(true);
    fetch(withLang(`/api/backend/api/v1/explore/deposits-count?${qs.toString()}`, lang), {
      cache: "no-store",
      signal: controller.signal,
    })
      .then((response) => {
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        return response.json();
      })
      .then((data) => {
        const total = Number(data?.total) || 0;
        setTotalRows(Math.min(total, Math.max(0, Number(filters.limit) || 0)));
      })
      .catch((err) => {
        if (err?.name === "AbortError") return;
        setTotalRows(0);
        setError(err.message || (lang === "en" ? "Error querying data" : "Error consultando datos"));
      })
      .finally(() => setLoading(false));
    return () => controller.abort();
  }, [filters, lang, buildExploreParams]);

  useEffect(() => {
    const controller = new AbortController();
    const qs = buildExploreParams(filters);
    const offset = page * RESULTS_PAGE_SIZE;
    const remaining = Math.max(0, (Number(filters.limit) || 0) - offset);
    if (remaining <= 0) {
      setListRows([]);
      setLoading(false);
      return () => controller.abort();
    }
    const pageLimit = Math.min(RESULTS_PAGE_SIZE, remaining);
    qs.set("limit", String(pageLimit));
    qs.set("offset", String(offset));
    setLoading(true);
    fetch(withLang(`/api/backend/api/v1/explore/deposits?${qs.toString()}`, lang), {
      cache: "no-store",
      signal: controller.signal,
    })
      .then((response) => {
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        return response.json();
      })
      .then((data) => {
        setListRows(Array.isArray(data) ? data : []);
      })
      .catch((err) => {
        if (err?.name === "AbortError") return;
        setListRows([]);
        setError(err.message || (lang === "en" ? "Error querying data" : "Error consultando datos"));
      })
      .finally(() => setLoading(false));
    return () => controller.abort();
  }, [filters, page, lang, buildExploreParams]);

  const selectedCountryLabel = countries.find((country) => country.iso3 === filters.countryIso)?.country_name || "Todos";
  const mapRows = useMemo(() => listRows, [listRows]);
  const renderedUntil = page * RESULTS_PAGE_SIZE + listRows.length;
  const hiddenMapRows = Math.max(0, totalRows - renderedUntil);
  const totalPages = Math.max(1, Math.ceil(totalRows / RESULTS_PAGE_SIZE));

  useEffect(() => {
    if (page > totalPages - 1) setPage(Math.max(0, totalPages - 1));
  }, [page, totalPages]);

  function applyFilters(event) {
    event.preventDefault();
    setLoading(true);
    setError("");
    setPage(0);
    setFilters({
      countryIso: countryIsoInput,
      mineral: mineralInput,
      limit: limitInput,
    });
  }

  return (
    <div className="page-shell">
      <AppHeader />

      <main className="container">
        <section className="panel">
          <h2>{t(lang, "exploreTitle")}</h2>
          <p className="muted">{t(lang, "exploreHint")}</p>
          <form className="explore-filters" onSubmit={applyFilters}>
            <select
              value={countryIsoInput}
              onChange={(e) => {
                const nextIso = e.target.value;
                setCountryIsoInput(nextIso);
              }}
            >
              <option value="">{lang === "en" ? "All countries" : "Todos los paises"}</option>
              {countries.map((country) => (
                <option key={`${country.country_name}-${country.iso3}`} value={country.iso3 || ""}>
                  {country.country_name} ({country.iso3 || "N/A"})
                </option>
              ))}
            </select>
            <select
              value={mineralInput}
              onChange={(e) => setMineralInput(e.target.value)}
            >
              <option value="">{lang === "en" ? "All minerals" : "Todos los minerales"}</option>
              {minerals.map((mineral) => (
                <option key={`exp-min-${mineral.value}`} value={mineral.value}>
                  {mineral.label}
                </option>
              ))}
            </select>
            <select
              value={limitInput}
              onChange={(e) => setLimitInput(Number(e.target.value) || DEFAULT_LIMIT)}
              aria-label="Limite visual utilizado para mantener rendimiento y claridad del mapa."
            >
              {!isHydrated ? (
                <option value={DEFAULT_LIMIT}>
                  {lang === "en" ? `${formatNumber(DEFAULT_LIMIT)} points` : `${formatNumber(DEFAULT_LIMIT)} puntos`}
                </option>
              ) : (
                limitOptions.map((value) => (
                  <option key={`limit-${value}`} value={value}>
                    {value === maxLimit
                      ? `${maxOptionLabel} (${formatNumber(value)})`
                      : lang === "en"
                        ? `${formatNumber(value)} points`
                        : `${formatNumber(value)} puntos`}
                  </option>
                ))
              )}
            </select>
            <button type="submit">{t(lang, "exploreApply")}</button>
          </form>
          {loading && <p className="muted">{lang === "en" ? "Loading results..." : "Cargando resultados..."}</p>}

          <div className="explore-kpis">
            <div className="summary-item">
              <h3>{lang === "en" ? "Filtered country" : "Pais filtrado"}</h3>
              <p>{selectedCountryLabel}</p>
            </div>
            <div className="summary-item">
              <h3>{lang === "en" ? "Filtered mineral" : "Mineral filtrado"}</h3>
              <p>{filters.mineral.trim() || (lang === "en" ? "All" : "Todos")}</p>
            </div>
            <div className="summary-item">
              <h3>{lang === "en" ? "Loaded points" : "Puntos cargados"}</h3>
              <p>{formatNumber(totalRows)}</p>
            </div>
          </div>
          <p className="muted">
            <InfoHint
              label="Limite de puntos"
              text={
                lang === "en"
                  ? "Visual limit used to keep map performance and readability."
                  : "Limite visual utilizado para mantener rendimiento y claridad del mapa."
              }
            />
          </p>
        </section>

        <section className="grid">
          <article className="panel">
            <h2>{t(lang, "exploreMap")}</h2>
            {!filters.countryIso && (
              <p className="muted">
                {lang === "en"
                  ? "Select a country to display map points."
                  : "Selecciona un pais para mostrar puntos en el mapa."}
              </p>
            )}
            <div className="map-wrap">
              <ExploreMap mapRows={mapRows} countryIso={filters.countryIso} loading={loading} lang={lang} />
            </div>
            {hiddenMapRows > 0 && (
              <p className="muted">
                {lang === "en" ? "Showing " : "Mostrando "}
                {formatNumber(mapRows.length)}{" "}
                {lang === "en" ? "points from this page on the map. " : "puntos de esta pagina en el mapa. "}
                {formatNumber(hiddenMapRows)}{" "}
                {lang === "en" ? "remain on next pages." : "quedan para las siguientes paginas."}
              </p>
            )}
          </article>

          <article className="panel results-panel">
            <h2>{t(lang, "exploreResults")}</h2>
            {error && <p className="muted">{lang === "en" ? "Error" : "Error"}: {error}</p>}
            {!error && (
              <div className="results-scroll">
                <ul className="countries-list">
                  {listRows.map((item) => (
                    <li key={`row-${item.dep_id}`}>
                      <strong>{item.name || `Dep. ${item.dep_id}`}</strong> - {item.country_name} -{" "}
                      <span
                        className="acronym-hint"
                        data-tooltip={
                          lang === "en"
                            ? "Associated minerals recorded in this deposit."
                            : "Minerales asociados registrados en este deposito."
                        }
                        tabIndex={0}
                      >
                        {item.minerals || "N/A"}
                      </span>
                    </li>
                  ))}
                </ul>
              </div>
            )}
            {!error && (
              <>
                <p className="muted">
                  {lang === "en"
                    ? `Showing ${formatNumber(listRows.length)} records (page ${page + 1} of ${formatNumber(totalPages)}), out of ${formatNumber(totalRows)} total.`
                    : `Mostrando ${formatNumber(listRows.length)} registros (pagina ${page + 1} de ${formatNumber(totalPages)}), de ${formatNumber(totalRows)} totales.`}
                </p>
                <div style={{ display: "flex", gap: "0.5rem" }}>
                  <button type="button" onClick={() => setPage((p) => Math.max(0, p - 1))} disabled={page <= 0}>
                    {lang === "en" ? "Previous" : "Anterior"}
                  </button>
                  <button
                    type="button"
                    onClick={() => setPage((p) => Math.min(totalPages - 1, p + 1))}
                    disabled={page >= totalPages - 1}
                  >
                    {lang === "en" ? "Next" : "Siguiente"}
                  </button>
                </div>
              </>
            )}
          </article>
        </section>
      </main>
    </div>
  );
}
