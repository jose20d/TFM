import Image from "next/image";
import Link from "next/link";

const BACKEND_URL = process.env.BACKEND_API_URL || "http://127.0.0.1:8001";

function formatNumber(value) {
  if (value === null || value === undefined) return "N/A";
  const numeric = Number(value);
  if (Number.isNaN(numeric)) return String(value);
  return new Intl.NumberFormat("es-ES").format(numeric);
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
  return String(value || "").trim().toUpperCase();
}

function countryOptionLabel(country) {
  const iso2Part = country.iso2 ? ` | ${country.iso2}` : "";
  return `${country.country_name} | ${country.iso3 || "N/A"}${iso2Part}`;
}

function findCountryByIso(countries, iso3) {
  const target = normalizeTerm(iso3);
  return countries.find((country) => normalizeTerm(country.iso3) === target);
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
    countryOptionLabel(country).toUpperCase().includes(term),
  );
  if (byContains) {
    return { iso3: byContains.iso3, label: countryOptionLabel(byContains) };
  }

  return fallback;
}

export default async function Home({ searchParams }) {
  const params = await searchParams;
  const [countries, defaults] = await Promise.all([
    readJson("/api/v1/countries?limit=300", []),
    readJson("/api/v1/home/defaults", {}),
  ]);
  const defaultIso3 = defaults.default_iso3 || "N/A";
  const requestedTerm = params?.pais || params?.iso3 || defaultIso3;
  const selected = resolveCountrySelection(requestedTerm, countries, defaultIso3);
  const iso3 = selected.iso3;

  const [overview, topCountries, topMinerals, country, dataHealth] = await Promise.all([
    readJson("/api/v1/overview", {}),
    readJson("/api/v1/top-countries?limit=5", []),
    readJson("/api/v1/top-minerals?limit=5", []),
    readJson(`/api/v1/countries/${iso3}/summary`, {}),
    readJson("/api/v1/health", {}),
  ]);

  const topMineralsLabel =
    topMinerals.length > 0 ? topMinerals.slice(0, 3).map((item) => item.commod).join(" | ") : "N/A";

  const insights = [
    `Top productores: ${topCountries.slice(0, 3).map((item) => item.country_name).join(", ") || "N/A"}`,
    `Minerales lideres: ${topMinerals.slice(0, 3).map((item) => item.commod).join(", ") || "N/A"}`,
    `Pais destacado: ${country.country_name || "N/A"} (${country.iso3 || iso3})`,
  ];

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
          <Link href="/explorar">Explorar</Link>
          <Link href="/comparar">Comparar</Link>
          <a href="#">Analisis</a>
          <a href="#">Consultas</a>
          <a href="#">Usuario</a>
        </nav>
      </header>

      <main className="container">
        <section className="hero">
          <div className="hero-visual">
            <div className="hero-image">
              <Image
                src="/images/hero/planet_crop.png"
                alt="Planeta de contexto global"
                fill
                priority
                className="hero-image-content"
              />
            </div>
            <div className="kpi-row">
              <article className="kpi-card">
                <p className="kpi-label">Paises analizados</p>
                <p className="kpi-value">{formatNumber(overview.countries_count)}</p>
              </article>
              <article className="kpi-card">
                <p className="kpi-label">Depositos minerales</p>
                <p className="kpi-value">{formatNumber(overview.deposits_count)}</p>
              </article>
              <article className="kpi-card">
                <p className="kpi-label">Minerales principales</p>
                <p className="kpi-value" style={{ fontSize: "1.05rem" }}>{topMineralsLabel}</p>
              </article>
              <article className="kpi-card">
                <p className="kpi-label">Prom. IPC</p>
                <p className="kpi-value kpi-accent">{formatNumber(overview.avg_cpi)}</p>
              </article>
              <article className="kpi-card">
                <p className="kpi-label">Prom. EFI</p>
                <p className="kpi-value kpi-accent">{formatNumber(overview.avg_fsi)}</p>
              </article>
            </div>
          </div>
        </section>

        <section className="grid">
          <article className="panel">
            <h2>Principales paises con recursos</h2>
            <ul className="countries-list">
              {topCountries.map((item) => (
                <li key={`${item.country_name}-${item.iso3}`}>
                  <strong>{item.country_name}</strong> ({item.iso3 || "N/A"}) - {formatNumber(item.total_deposits)} depositos
                </li>
              ))}
            </ul>
          </article>

          <article className="panel">
            <h2>Ideas clave</h2>
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
            <h2>Perfil de pais</h2>
            <p className="muted">Busca por nombre, ISO2 o ISO3.</p>
            <form className="country-form" method="get">
              <input
                name="pais"
                list="countries-options"
                defaultValue={selected.label}
                placeholder="Ejemplo: Costa Rica, CR o CRI"
              />
              <button type="submit">Cargar</button>
            </form>
            <datalist id="countries-options">
              {countries.map((country) => (
                <option key={`${country.country_name}-${country.iso3}`} value={countryOptionLabel(country)} />
              ))}
            </datalist>
            {!dataHealth.db && (
              <p className="muted">
                No hay conexion a base de datos. Revisa variables DB_* en la terminal del backend.
              </p>
            )}

            <div className="summary-grid">
              <div className="summary-item">
                <h3>Pais</h3>
                <p>{country.country_name || "N/A"}</p>
              </div>
              <div className="summary-item">
                <h3>ISO3</h3>
                <p>{country.iso3 || iso3}</p>
              </div>
              <div className="summary-item">
                <h3>Depositos</h3>
                <p>{formatNumber(country.deposits_count)}</p>
              </div>
              <div className="summary-item">
                <h3>PIB</h3>
                <p>{formatNumber(country.gdp)}</p>
              </div>
              <div className="summary-item">
                <h3>Indice de corrupcion</h3>
                <p>{formatNumber(country.cpi)}</p>
              </div>
              <div className="summary-item">
                <h3>Indice de fragilidad</h3>
                <p>{formatNumber(country.fsi)}</p>
              </div>
            </div>
          </article>

          <article className="panel">
            <h2>Top minerales</h2>
            <ul className="minerals-list">
              {topMinerals.map((item) => (
                <li key={item.commod}>
                  <strong>{item.commod}</strong> - {formatNumber(item.occurrences)} ocurrencias
                </li>
              ))}
            </ul>
          </article>
        </section>
      </main>
    </div>
  );
}
