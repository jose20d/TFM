export const SUPPORTED_LANGS = ["es", "en"];

const TEXTS = {
  es: {
    brandSubtitle: "Plataforma Analitica",
    navHome: "Inicio",
    navExplore: "Explorar",
    navCompare: "Comparar",
    navAnalysis: "Analisis",
    navTerrain: "Terreno",
    navQueries: "Consultas",
    homeTitle: "Principales paises con recursos",
    homeIdeas: "Ideas clave",
    homeProfile: "Perfil de pais",
    homeLoad: "Cargar",
    compareTitle: "Comparar paises",
    compareHint: "Selecciona 2 a 5 paises con chips sugeridos o buscador.",
    compareExamples: "Ejemplos",
    compareSearch: "Buscar pais (ejemplo: C, Co, Cos...)",
    compareRunHint: "Selecciona al menos 2 paises para comparar.",
    exploreTitle: "Exploracion geoterritorial",
    exploreHint: "Filtra por pais, mineral y limite de puntos sobre el mapa.",
    exploreApply: "Aplicar filtros",
    exploreMap: "Mapa de depositos",
    exploreResults: "Resultados",
    analysisTitle: "Analisis global de resultados",
    analysisHint: "Vista de patrones globales por pais usando depositos, PIB, CPI y FSI.",
    queriesTitle: "Consultas",
    queriesHint:
      "Explora depositos, minerales y relaciones espaciales mediante consultas guiadas.",
    queriesRun: "Ejecutar consulta",
    queriesExportCsv: "Exportar CSV",
    queriesExportJson: "Exportar JSON",
    queriesExportGeoJson: "Exportar GeoJSON",
    terrainHint:
      "Herramientas para explorar zonas de interes mineralogico a partir de depositos registrados, proximidad espacial y minerales asociados.",
  },
  en: {
    brandSubtitle: "Analytics Platform",
    navHome: "Home",
    navExplore: "Explore",
    navCompare: "Compare",
    navAnalysis: "Analysis",
    navTerrain: "Terrain",
    navQueries: "Queries",
    homeTitle: "Top Countries With Resources",
    homeIdeas: "Key Insights",
    homeProfile: "Country Profile",
    homeLoad: "Load",
    compareTitle: "Compare Countries",
    compareHint: "Select 2 to 5 countries with suggested chips or search.",
    compareExamples: "Examples",
    compareSearch: "Search country (example: C, Co, Cos...)",
    compareRunHint: "Select at least 2 countries to compare.",
    exploreTitle: "Geoterritorial Exploration",
    exploreHint: "Filter by country, mineral, and map point limit.",
    exploreApply: "Apply Filters",
    exploreMap: "Deposits Map",
    exploreResults: "Results",
    analysisTitle: "Global Results Analysis",
    analysisHint: "Global pattern view by country using deposits, GDP, CPI and FSI.",
    queriesTitle: "Queries",
    queriesHint: "Explore deposits, minerals, and spatial relations with guided queries.",
    queriesRun: "Run Query",
    queriesExportCsv: "Export CSV",
    queriesExportJson: "Export JSON",
    queriesExportGeoJson: "Export GeoJSON",
    terrainHint:
      "Tools to explore mineral interest zones from recorded deposits, spatial proximity, and associated minerals.",
  },
};

export function normalizeLang(value) {
  const next = String(value || "es").trim().toLowerCase();
  return SUPPORTED_LANGS.includes(next) ? next : "es";
}

export function t(lang, key, fallback = "") {
  const safeLang = normalizeLang(lang);
  return TEXTS[safeLang]?.[key] || fallback || TEXTS.es?.[key] || key;
}

export function withLang(url, lang) {
  const safeLang = normalizeLang(lang);
  const separator = url.includes("?") ? "&" : "?";
  return `${url}${separator}lang=${encodeURIComponent(safeLang)}`;
}
