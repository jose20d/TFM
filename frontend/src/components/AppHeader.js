"use client";

import Link from "next/link";
import { usePathname, useSearchParams } from "next/navigation";
import { normalizeLang, t, useLang } from "../lib/i18n";

function hrefWithLang(pathname, searchParams, lang) {
  const qs = new URLSearchParams(searchParams?.toString() || "");
  qs.set("lang", normalizeLang(lang));
  const query = qs.toString();
  return query ? `${pathname}?${query}` : pathname;
}

function routeWithLang(pathname, lang) {
  return `${pathname}?lang=${encodeURIComponent(normalizeLang(lang))}`;
}

export default function AppHeader() {
  const pathname = usePathname() || "/";
  const searchParams = useSearchParams();
  const lang = useLang();

  return (
    <header className="nav">
      <div className="brand">
        <span className="brand-dot" />
        <div>
          <strong>GeoContext</strong>
          <br />
          <span>{t(lang, "brandSubtitle")}</span>
        </div>
      </div>
      <nav className="menu">
        <Link href={routeWithLang("/", lang)}>{t(lang, "navHome")}</Link>
        <Link href={routeWithLang("/explorar", lang)}>{t(lang, "navExplore")}</Link>
        <Link href={routeWithLang("/comparar", lang)}>{t(lang, "navCompare")}</Link>
        <Link href={routeWithLang("/analisis", lang)}>{t(lang, "navAnalysis")}</Link>
        <Link href={routeWithLang("/terreno", lang)}>{t(lang, "navTerrain")}</Link>
        <Link href={routeWithLang("/consultas", lang)}>{t(lang, "navQueries")}</Link>
        <span className="lang-switch" aria-label="language-switcher">
          <Link
            href={hrefWithLang(pathname, searchParams, "es")}
            className={lang === "es" ? "lang-btn active" : "lang-btn"}
            title="Espanol"
            aria-label="Cambiar a espanol"
          >
            <img src="/flags/es.svg" alt="ES" className="lang-flag" />
          </Link>
          <Link
            href={hrefWithLang(pathname, searchParams, "en")}
            className={lang === "en" ? "lang-btn active" : "lang-btn"}
            title="English"
            aria-label="Switch to English"
          >
            <img src="/flags/gb.svg" alt="EN" className="lang-flag" />
          </Link>
        </span>
      </nav>
    </header>
  );
}
