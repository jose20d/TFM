import Image from "next/image";
import AppHeader from "../components/AppHeader";
import { normalizeLang, t, withLang } from "../lib/i18n-core";

const BACKEND_URL = process.env.BACKEND_API_URL || "http://127.0.0.1:8001";

function toNumeric(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;

  const raw = String(value).trim();
  if (!raw) return null;

  // Soporta formato ES: 95.350.423,17
  if (/^-?\d{1,3}(\.\d{3})*(,\d+)?$/.test(raw)) {
    const normalized = raw.replace(/\./g, "").replace(",", ".");
    const num = Number(normalized);
    return Number.isFinite(num) ? num : null;
  }

  // Soporta formato simple con punto o coma decimal
  if (/^-?\d+([.,]\d+)?$/.test(raw)) {
    const normalized = raw.replace(",", ".");
    const num = Number(normalized);
    return Number.isFinite(num) ? num : null;
  }

  // Fallback: elimina caracteres no numericos comunes (moneda/unidades)
  const cleaned = raw.replace(/[^\d,.-]/g, "");
  if (!cleaned) return null;
  const normalized = cleaned.includes(",") && cleaned.includes(".")
    ? cleaned.replace(/\./g, "").replace(",", ".")
    : cleaned.replace(",", ".");
  const num = Number(normalized);
  return Number.isFinite(num) ? num : null;
}

function formatInteger(value) {
  const numeric = toNumeric(value);
  if (numeric === null) return "N/A";
  const sign = numeric < 0 ? "-" : "";
  const integerPart = String(Math.trunc(Math.abs(numeric)));
  const groupedInteger = integerPart.replace(/\B(?=(\d{3})+(?!\d))/g, ".");
  return `${sign}${groupedInteger}`;
}

function formatNumber(value) {
  if (value === null || value === undefined) return "N/A";
  const numeric = toNumeric(value);
  if (!Number.isFinite(numeric)) return "N/A";
  const sign = numeric < 0 ? "-" : "";
  const absolute = Math.abs(numeric);
  const base = Number.isInteger(absolute) ? String(absolute) : absolute.toFixed(2);
  const [integerPart, decimalPart] = base.split(".");
  const groupedInteger = integerPart.replace(/\B(?=(\d{3})+(?!\d))/g, ".");
  if (!decimalPart) return `${sign}${groupedInteger}`;
  return `${sign}${groupedInteger},${decimalPart}`;
}

function formatBillions(value) {
  const numeric = toNumeric(value);
  if (numeric === null) return "N/A";
  const billions = numeric / 1_000_000_000;
  const sign = billions < 0 ? "-" : "";
  const fixed = Math.abs(billions).toFixed(2);
  const [integerPart, decimalPart] = fixed.split(".");
  const groupedInteger = integerPart.replace(/\B(?=(\d{3})+(?!\d))/g, ".");
  return `${sign}${groupedInteger},${decimalPart} USD B`;
}

async function readJson(endpoint, fallback) {
  try {
    const response = await fetch(`${BACKEND_URL}${endpoint}`, {
      cache: "no-store",
    });
    if (!response.ok) return fallback;
    return await response.json();
  } catch (_error) {
    return fallback;
  }
}

function normalizeTerm(value) {
  return String(value || "")
    .trim()
    .toUpperCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\s+/g, " ");
}

function countryOptionLabel(country) {
  const iso2Part = country.iso2 ? ` | ${country.iso2}` : "";
  return `${country.country_name} | ${country.iso3 || "N/A"}${iso2Part}`;
}

function findCountryByIso(countries, iso3) {
  const target = normalizeTerm(iso3);
  return countries.find((country) => normalizeTerm(country.iso3) === target);
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

function resolveCountrySelection(rawTerm, countries, fallbackIso3) {
  const term = normalizeTerm(rawTerm);
  const fallbackCountry = findCountryByIso(countries, fallbackIso3);
  const fallback = fallbackCountry
    ? { iso3: fallbackCountry.iso3, label: countryOptionLabel(fallbackCountry) }
    : { iso3: fallbackIso3 || "N/A", label: fallbackIso3 || "N/A" };

  if (!term || countries.length === 0) {
    return fallback;
  }

  const byIso = countries.find((country) => {
    const iso3 = normalizeTerm(country.iso3);
    const iso2 = normalizeTerm(country.iso2);
    return term === iso3 || term === iso2;
  });
  if (byIso) {
    return { iso3: byIso.iso3, label: countryOptionLabel(byIso) };
  }

  const byName = countries.find(
    (country) => normalizeTerm(country.country_name) === term,
  );
  if (byName) {
    return { iso3: byName.iso3, label: countryOptionLabel(byName) };
  }

  const byContains = countries.find((country) =>
    normalizeTerm(countryOptionLabel(country)).includes(term),
  );
  if (byContains) {
    return { iso3: byContains.iso3, label: countryOptionLabel(byContains) };
  }

  return fallback;
}

export default async function Home({ searchParams }) {
  const params = await searchParams;
  const lang = normalizeLang(params?.lang);
  const tr = (es, en) => (lang === "en" ? en : es);
  const [countries, defaults] = await Promise.all([
    readJson(withLang("/api/v1/countries?limit=300", lang), []),
    readJson("/api/v1/home/defaults", {}),
  ]);
  const defaultIso3 = defaults.default_iso3 || "N/A";
  const requestedTerm = params?.pais || params?.iso3 || defaultIso3;
  const selected = resolveCountrySelection(requestedTerm, countries, defaultIso3);
  const iso3 = selected.iso3;

  const [overview, topCountries, topMinerals, country, dataHealth] = await Promise.all([
    readJson(withLang("/api/v1/overview", lang), {}),
    readJson(withLang("/api/v1/top-countries?limit=5", lang), []),
    readJson(withLang("/api/v1/top-minerals?limit=5", lang), []),
    readJson(withLang(`/api/v1/countries/${iso3}/summary`, lang), {}),
    readJson("/api/v1/health", {}),
  ]);

  const topMineralsLabel =
    topMinerals.length > 0 ? topMinerals.slice(0, 3).map((item) => item.commod).join(" | ") : "N/A";

  const insights = [
    lang === "en"
      ? `Top producers: ${topCountries.slice(0, 3).map((item) => item.country_name).join(", ") || "N/A"}`
      : `Top productores: ${topCountries.slice(0, 3).map((item) => item.country_name).join(", ") || "N/A"}`,
    lang === "en"
      ? `Leading minerals: ${topMinerals.slice(0, 3).map((item) => item.commod).join(", ") || "N/A"}`
      : `Minerales lideres: ${topMinerals.slice(0, 3).map((item) => item.commod).join(", ") || "N/A"}`,
    lang === "en"
      ? `Highlighted country: ${country.country_name || "N/A"} (${country.iso3 || iso3})`
      : `Pais destacado: ${country.country_name || "N/A"} (${country.iso3 || iso3})`,
  ];

  return (
    <div className="page-shell">
      <AppHeader />

      <main className="container">
        <section className="hero">
          <div className="hero-visual">
            <div className="hero-image">
              <Image
                src="/images/hero/planet_crop.png"
                alt={tr("Planeta de contexto global", "Planet with global context")}
                fill
                sizes="(max-width: 720px) 100vw, 1240px"
                priority
                className="hero-image-content"
              />
            </div>
            <div className="kpi-row">
              <article className="kpi-card">
                <p className="kpi-label">
                  <InfoHint
                    label={tr("Paises analizados", "Countries analyzed")}
                    text={
                      lang === "en"
                        ? "Number of countries with integrated geological and contextual records."
                        : "Cantidad de paises con registros integrados desde datasets geologicos y contextuales."
                    }
                  />
                </p>
                <p className="kpi-value numeric-value">{formatInteger(overview.countries_count)}</p>
              </article>
              <article className="kpi-card">
                <p className="kpi-label">
                  <InfoHint
                    label={tr("Depositos minerales", "Mineral deposits")}
                    text={
                      lang === "en"
                        ? "Total mineral deposit records integrated into the platform."
                        : "Total de registros mineralogicos integrados en la plataforma."
                    }
                  />
                </p>
                <p className="kpi-value numeric-value">{formatInteger(overview.deposits_count)}</p>
              </article>
              <article className="kpi-card">
                <p className="kpi-label">{lang === "en" ? "Top Minerals" : "Minerales principales"}</p>
                <p className="kpi-value" style={{ fontSize: "1.05rem" }}>{topMineralsLabel}</p>
              </article>
              <article className="kpi-card">
                <p className="kpi-label">
                  <InfoHint
                    label={tr("Prom. IPC", "Avg. CPI")}
                    text={
                      lang === "en"
                        ? "Average Corruption Perception Index (CPI). Higher values indicate lower perceived corruption."
                        : "Promedio del indice de percepcion de corrupcion (CPI). Valores altos indican menor corrupcion percibida."
                    }
                  />
                </p>
                <p className="kpi-value kpi-accent numeric-value">{formatNumber(overview.avg_cpi)}</p>
              </article>
              <article className="kpi-card">
                <p className="kpi-label">
                  <InfoHint
                    label={tr("Prom. EFI", "Avg. FSI")}
                    text={
                      lang === "en"
                        ? "State Fragility Index. Higher values represent higher institutional fragility."
                        : "Indice de fragilidad estatal. Valores altos representan mayor fragilidad institucional."
                    }
                  />
                </p>
                <p className="kpi-value kpi-accent numeric-value">{formatNumber(overview.avg_fsi)}</p>
              </article>
            </div>
          </div>
        </section>

        <section className="grid">
          <article className="panel">
            <h2>{t(lang, "homeTitle")}</h2>
            <ul className="countries-list">
              {topCountries.map((item) => (
                <li key={`${item.country_name}-${item.iso3}`}>
                  <strong>{item.country_name}</strong> ({item.iso3 || "N/A"}) -{" "}
                  <span className="numeric-value">{formatInteger(item.total_deposits)}</span>{" "}
                  {tr("depositos", "deposits")}
                </li>
              ))}
            </ul>
          </article>

          <article className="panel">
            <h2>{t(lang, "homeIdeas")}</h2>
            <div className="insights">
              {insights.map((item) => (
                <p key={item} className="muted">
                  {item}
                </p>
              ))}
            </div>
          </article>
        </section>

        <section className="grid">
          <article className="panel">
            <h2>{t(lang, "homeProfile")}</h2>
            <p className="muted">
              {tr("Busca por nombre, ISO2 o ISO3.", "Search by name, ISO2 or ISO3.")}
            </p>
            <form className="country-form" method="get">
              <input type="hidden" name="lang" value={lang} />
              <input
                name="pais"
                list="countries-options"
                defaultValue={selected.label}
                placeholder={tr("Ejemplo: Costa Rica, CR o CRI", "Example: Costa Rica, CR or CRI")}
              />
              <button type="submit">{t(lang, "homeLoad")}</button>
            </form>
            <datalist id="countries-options">
              {countries.map((country) => (
                <option key={`${country.country_name}-${country.iso3}`} value={countryOptionLabel(country)} />
              ))}
            </datalist>
            {!dataHealth.db && (
              <p className="muted">
                {tr(
                  "No hay conexion a base de datos. Revisa variables DB_* en la terminal del backend.",
                  "No database connection. Check DB_* variables in the backend terminal.",
                )}
              </p>
            )}

            <div className="summary-grid">
              <div className="summary-item">
                <h3>{tr("Pais", "Country")}</h3>
                <p>{country.country_name || "N/A"}</p>
              </div>
              <div className="summary-item">
                <h3>ISO3</h3>
                <p>{country.iso3 || iso3}</p>
              </div>
              <div className="summary-item">
                <h3>{tr("Depositos", "Deposits")}</h3>
                <p className="numeric-value">{formatInteger(country.deposits_count)}</p>
              </div>
              <div className="summary-item">
                <h3>{tr("PIB", "GDP")}</h3>
                <p className="numeric-value">{formatBillions(country.gdp)}</p>
              </div>
              <div className="summary-item">
                <h3>{tr("Indice de corrupcion", "Corruption Index")}</h3>
                <p className="numeric-value">{formatNumber(country.cpi)}</p>
              </div>
              <div className="summary-item">
                <h3>{tr("Indice de fragilidad", "Fragility Index")}</h3>
                <p className="numeric-value">{formatNumber(country.fsi)}</p>
              </div>
            </div>
          </article>

          <article className="panel">
            <h2>
              <InfoHint
                label={tr("Top minerales", "Top minerals")}
                text={
                  lang === "en"
                    ? "Mineral registered across multiple integrated deposits."
                    : "Mineral registrado en multiples depositos integrados."
                }
              />
            </h2>
            <ul className="minerals-list">
              {topMinerals.map((item) => (
                <li key={item.commod}>
                  <strong
                    className="acronym-hint"
                    data-tooltip={
                      lang === "en"
                        ? "Mineral registered across multiple integrated deposits."
                        : "Mineral registrado en multiples depositos integrados."
                    }
                    tabIndex={0}
                  >
                    {item.commod}
                  </strong>{" "}
                  - <span className="numeric-value">{formatInteger(item.occurrences)}</span>{" "}
                  {lang === "en" ? "occurrences" : "ocurrencias"}
                </li>
              ))}
            </ul>
          </article>
        </section>
      </main>
    </div>
  );
}
