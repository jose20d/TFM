async function getJson(url) {
  const res = await fetch(url);
  if (!res.ok) {
    throw new Error(`HTTP ${res.status}`);
  }
  return res.json();
}

function formatNumber(value) {
  if (value === null || value === undefined) {
    return "N/A";
  }
  const num = Number(value);
  if (Number.isNaN(num)) {
    return String(value);
  }
  return new Intl.NumberFormat("es-ES").format(num);
}

function fillList(targetId, rows, renderer) {
  const target = document.getElementById(targetId);
  target.innerHTML = "";
  rows.forEach((row) => {
    const li = document.createElement("li");
    li.textContent = renderer(row);
    target.appendChild(li);
  });
}

function renderCountrySummary(data) {
  const el = document.getElementById("country-summary");
  const topMinerals = Array.isArray(data.top_minerals) ? data.top_minerals.join(", ") : "N/A";
  const cards = [
    ["Pais", data.country_name || "N/A"],
    ["ISO3", data.iso3 || "N/A"],
    ["Depositos", formatNumber(data.deposits_count)],
    ["PIB", formatNumber(data.gdp)],
    ["Indice de Corrupcion", formatNumber(data.cpi)],
    ["Indice de Fragilidad", formatNumber(data.fsi)],
    ["Minerales top", topMinerals || "N/A"],
  ];

  el.innerHTML = "";
  cards.forEach(([label, value]) => {
    const card = document.createElement("article");
    card.className = "summary-item";
    card.innerHTML = `<h4>${label}</h4><p>${value}</p>`;
    el.appendChild(card);
  });
}

async function loadCountrySummary() {
  const input = document.getElementById("country-iso3");
  const iso3 = input.value.trim().toUpperCase() || "CHL";
  input.value = iso3;

  try {
    const data = await getJson(`/api/v1/countries/${iso3}/summary`);
    renderCountrySummary(data);
  } catch (_error) {
    renderCountrySummary({
      country_name: "No encontrado",
      iso3,
      deposits_count: null,
      gdp: null,
      cpi: null,
      fsi: null,
      top_minerals: [],
    });
  }
}

async function bootstrap() {
  try {
    const [overview, topCountries, topMinerals, mapRows] = await Promise.all([
      getJson("/api/v1/overview"),
      getJson("/api/v1/top-countries?limit=5"),
      getJson("/api/v1/top-minerals?limit=5"),
      getJson("/api/v1/deposits/map?limit=20"),
    ]);

    document.getElementById("kpi-countries").textContent = formatNumber(overview.countries_count);
    document.getElementById("kpi-deposits").textContent = formatNumber(overview.deposits_count);
    document.getElementById("kpi-top-mineral").textContent = overview.top_mineral || "N/A";
    document.getElementById("kpi-cpi").textContent = formatNumber(overview.avg_cpi);
    document.getElementById("kpi-fsi").textContent = formatNumber(overview.avg_fsi);

    fillList("top-countries", topCountries, (r) => `${r.country_name} (${r.iso3 || "N/A"}): ${formatNumber(r.total_deposits)}`);
    fillList("top-minerals", topMinerals, (r) => `${r.commod}: ${formatNumber(r.occurrences)}`);

    document.getElementById("map-preview").textContent = JSON.stringify(mapRows, null, 2);
    await loadCountrySummary();
  } catch (error) {
    document.getElementById("map-preview").textContent = `Error cargando dashboard: ${error.message}`;
  }
}

document.getElementById("btn-country").addEventListener("click", loadCountrySummary);
bootstrap();
