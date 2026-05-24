"use client";

const GEO_CACHE_KEY = "tfm_geo_default_iso3";
const GEO_ENDPOINT = "https://ipapi.co/json/";

function normalizeIso(value) {
  const text = String(value || "").trim().toUpperCase();
  return text || "";
}

export async function detectDefaultCountryIso3(countries) {
  if (!Array.isArray(countries) || !countries.length) return "";
  if (typeof window === "undefined") return "";

  const iso3Set = new Set(countries.map((country) => normalizeIso(country?.iso3)).filter(Boolean));
  const byIso2 = new Map(
    countries
      .map((country) => [normalizeIso(country?.iso2), normalizeIso(country?.iso3)])
      .filter(([iso2, iso3]) => iso2 && iso3),
  );

  const cached = normalizeIso(window.localStorage.getItem(GEO_CACHE_KEY));
  if (cached && iso3Set.has(cached)) return cached;

  try {
    const response = await fetch(GEO_ENDPOINT, { cache: "no-store" });
    if (!response.ok) return "";
    const payload = await response.json();
    const detectedIso3 = normalizeIso(payload?.country_code_iso3);
    const detectedIso2 = normalizeIso(payload?.country_code);
    const resolvedIso3 = detectedIso3 || byIso2.get(detectedIso2) || "";
    if (!resolvedIso3 || !iso3Set.has(resolvedIso3)) return "";
    window.localStorage.setItem(GEO_CACHE_KEY, resolvedIso3);
    return resolvedIso3;
  } catch {
    return "";
  }
}
