import AppHeader from "../../components/AppHeader";
import { normalizeLang } from "../../lib/i18n-core";

const CONTENT = {
  es: {
    title: "Acerca de GeoContext",
    seoTitle: "GeoContext | Acerca de",
    seoDescription:
      "Origen, proposito, equipo academico y mantenimiento actual de GeoContext.",
    whatIsTitle: "Que es GeoContext?",
    whatIsText:
      "GeoContext es una plataforma web de exploracion y analisis territorial de recursos minerales que integra informacion geologica con indicadores economicos e institucionales de diferentes paises.",
    purposeText:
      "Su proposito es facilitar el acceso, la visualizacion y el analisis de informacion geologica mediante herramientas interactivas, ofreciendo un recurso util para estudiantes, investigadores, coleccionistas y aficionados a la mineralogia.",
    originTitle: "Origen del proyecto",
    originText1:
      "GeoContext nacio en 2026 como parte de un Trabajo Final de Master en Big Data & Business Analytics de Next Educacion, Espana.",
    originText2:
      "El proyecto surgio de la necesidad de integrar fuentes de informacion geologica, economica e institucional que tradicionalmente se encontraban dispersas, desarrollando una arquitectura capaz de relacionarlas mediante una dimension territorial comun.",
    originText3:
      "Tras finalizar su etapa academica, GeoContext continua activo como una plataforma independiente, con el objetivo de ampliar progresivamente sus funcionalidades y capacidades de exploracion geologica.",
    teamTitle: "Equipo y creditos",
    teamText: "El proyecto academico fue desarrollado por:",
    currentLeadTitle: "Mantenimiento y evolucion actual",
    currentLeadText:
      "Jose Daniel Alfaro Alfaro es actualmente el responsable del mantenimiento, desarrollo y evolucion de GeoContext.",
    sourcesTitle: "Fuentes y tecnologias",
    sourcesText: "GeoContext utiliza informacion publica procedente de:",
    techText:
      "La plataforma esta desarrollada principalmente con Python, FastAPI, PostgreSQL, PostGIS, Next.js y Docker.",
    disclaimer:
      "GeoContext es un proyecto independiente y no representa oficialmente a las instituciones proveedoras de informacion.",
    linksTitle: "Enlaces del proyecto",
    githubLabel: "GitHub — Repositorio del proyecto",
    linkedinLabel: "LinkedIn — Responsable actual",
  },
  en: {
    title: "About GeoContext",
    seoTitle: "GeoContext | About",
    seoDescription:
      "Origin, purpose, academic team, and current maintenance status of GeoContext.",
    whatIsTitle: "What is GeoContext?",
    whatIsText:
      "GeoContext is a web platform for territorial exploration and analysis of mineral resources, integrating geological information with economic and institutional indicators from different countries.",
    purposeText:
      "Its purpose is to facilitate access, visualization, and analysis of geological information through interactive tools, offering a useful resource for students, researchers, collectors, and mineralogy enthusiasts.",
    originTitle: "Project origin",
    originText1:
      "GeoContext was created in 2026 as part of a Master's Final Project in Big Data & Business Analytics at Next Educacion, Spain.",
    originText2:
      "The project emerged from the need to integrate geological, economic, and institutional data sources that were traditionally scattered, developing an architecture capable of linking them through a shared territorial dimension.",
    originText3:
      "After its academic phase ended, GeoContext remains active as an independent platform, with the goal of progressively expanding its functionality and geological exploration capabilities.",
    teamTitle: "Team and credits",
    teamText: "The academic project was developed by:",
    currentLeadTitle: "Current maintenance and evolution",
    currentLeadText:
      "Jose Daniel Alfaro Alfaro is currently responsible for the maintenance, development, and evolution of GeoContext.",
    sourcesTitle: "Sources and technologies",
    sourcesText: "GeoContext uses public information from:",
    techText:
      "The platform is primarily built with Python, FastAPI, PostgreSQL, PostGIS, Next.js, and Docker.",
    disclaimer:
      "GeoContext is an independent project and does not officially represent the data provider institutions.",
    linksTitle: "Project links",
    githubLabel: "GitHub — Project repository",
    linkedinLabel: "LinkedIn — Current maintainer",
  },
};

const ACADEMIC_TEAM = [
  "Jose Daniel Alfaro Alfaro",
  "Daniela Corona Miguel",
  "Feeby Mendez Villalobos",
  "Marco Ramirez Gomez",
];

const DATA_SOURCES = [
  "USGS — Mineral Resources Data System (MRDS).",
  "Banco Mundial — World Development Indicators (WDI) y Worldwide Governance Indicators (WGI).",
  "Transparency International — Corruption Perceptions Index (CPI).",
  "Fund for Peace — Fragile States Index (FSI).",
];

const PROJECT_LINKS = {
  github: "https://github.com/jose20d/TFM",
  linkedin: "https://www.linkedin.com/in/jose-alfaro-alfaro",
};

function GithubIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true" className="about-link-icon">
      <path
        fill="currentColor"
        d="M12 2C6.48 2 2 6.58 2 12.24c0 4.53 2.87 8.37 6.84 9.73.5.1.68-.22.68-.49 0-.24-.01-.88-.01-1.73-2.78.62-3.37-1.37-3.37-1.37-.45-1.18-1.11-1.49-1.11-1.49-.9-.64.07-.63.07-.63 1 .07 1.53 1.05 1.53 1.05.88 1.55 2.32 1.1 2.89.84.09-.66.35-1.1.63-1.35-2.22-.26-4.55-1.14-4.55-5.06 0-1.12.39-2.03 1.03-2.75-.1-.26-.45-1.3.1-2.72 0 0 .84-.27 2.75 1.05A9.3 9.3 0 0 1 12 6.83c.85 0 1.71.12 2.51.35 1.9-1.32 2.74-1.05 2.74-1.05.55 1.42.2 2.46.1 2.72.64.72 1.03 1.63 1.03 2.75 0 3.93-2.33 4.79-4.56 5.05.36.32.67.95.67 1.91 0 1.38-.01 2.5-.01 2.84 0 .27.18.6.69.49A10.19 10.19 0 0 0 22 12.24C22 6.58 17.52 2 12 2Z"
      />
    </svg>
  );
}

function LinkedinIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true" className="about-link-icon">
      <path
        fill="currentColor"
        d="M6.94 8.5a1.56 1.56 0 1 1 0-3.12 1.56 1.56 0 0 1 0 3.12ZM8.3 18.5H5.58V9.55H8.3v8.95Zm10.13 0h-2.7v-4.36c0-1.04-.02-2.38-1.45-2.38-1.45 0-1.67 1.13-1.67 2.3v4.44H9.9V9.55h2.6v1.22h.04c.36-.69 1.24-1.42 2.56-1.42 2.74 0 3.24 1.81 3.24 4.16v4.99Z"
      />
    </svg>
  );
}

export async function generateMetadata({ searchParams }) {
  const params = await searchParams;
  const lang = normalizeLang(params?.lang);
  const content = CONTENT[lang];
  return {
    title: content.seoTitle,
    description: content.seoDescription,
  };
}

export default async function AcercaPage({ searchParams }) {
  const params = await searchParams;
  const lang = normalizeLang(params?.lang);
  const content = CONTENT[lang];

  return (
    <div className="page-shell">
      <AppHeader />
      <main className="container about-container">
        <section className="panel about-hero-panel">
          <h1>{content.title}</h1>
          <p className="muted">{content.whatIsText}</p>
          <p className="muted">{content.purposeText}</p>
        </section>

        <section className="grid about-grid">
          <article className="panel">
            <h2>{content.originTitle}</h2>
            <p className="muted">{content.originText1}</p>
            <p className="muted">{content.originText2}</p>
            <p className="muted">{content.originText3}</p>
          </article>

          <article className="panel">
            <h2>{content.teamTitle}</h2>
            <p className="muted">{content.teamText}</p>
            <ul className="about-team-list">
              {ACADEMIC_TEAM.map((name) => (
                <li key={name} className="about-team-item">
                  {name}
                </li>
              ))}
            </ul>
            <div className="about-maintainer-box">
              <h3>{content.currentLeadTitle}</h3>
              <p className="muted">{content.currentLeadText}</p>
            </div>
          </article>
        </section>

        <section className="grid about-grid">
          <article className="panel">
            <h2>{content.sourcesTitle}</h2>
            <p className="muted">{content.sourcesText}</p>
            <ul className="about-source-list">
              {DATA_SOURCES.map((source) => (
                <li key={source}>{source}</li>
              ))}
            </ul>
            <p className="muted about-disclaimer">{content.techText}</p>
            <p className="muted about-disclaimer-small">{content.disclaimer}</p>
          </article>

          <article className="panel">
            <h2>{content.linksTitle}</h2>
            <footer className="about-links" aria-label={content.linksTitle}>
              <a
                href={PROJECT_LINKS.github}
                target="_blank"
                rel="noopener noreferrer"
                className="about-link-card"
              >
                <GithubIcon />
                <span>{content.githubLabel}</span>
              </a>
              <a
                href={PROJECT_LINKS.linkedin}
                target="_blank"
                rel="noopener noreferrer"
                className="about-link-card"
              >
                <LinkedinIcon />
                <span>{content.linkedinLabel}</span>
              </a>
            </footer>
          </article>
        </section>
      </main>
    </div>
  );
}
