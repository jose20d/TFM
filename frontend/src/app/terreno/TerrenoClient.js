"use client";

import { useMemo, useState } from "react";
import Link from "next/link";
import styles from "./terreno.module.css";

function PlaceholderSelect({ options }) {
  return (
    <select>
      {options.map((option) => (
        <option key={option.value} value={option.value}>
          {option.label}
        </option>
      ))}
    </select>
  );
}

export default function TerrenoClient() {
  const [activeTool, setActiveTool] = useState("corridor");

  const toolTabs = useMemo(
    () => [
      { id: "corridor", label: "Corredor entre depositos" },
      { id: "zone", label: "Zona de interes" },
      { id: "minerals", label: "Minerales frecuentes" },
      { id: "potential", label: "Potencial exploratorio" },
    ],
    [],
  );

  function renderActiveTool() {
    if (activeTool === "corridor") {
      return (
        <>
          <h3>Corredor entre depositos</h3>
          <p className="muted">
            Selecciona dos depositos para analizar distancia, minerales comunes y depositos cercanos
            dentro de un corredor configurable.
          </p>
          <div className={styles.controls}>
            <label>
              Deposito A
              <PlaceholderSelect
                options={[
                  { value: "", label: "Seleccionar deposito A" },
                  { value: "placeholder-a1", label: "Placeholder deposito A-1" },
                  { value: "placeholder-a2", label: "Placeholder deposito A-2" },
                ]}
              />
            </label>
            <label>
              Deposito B
              <PlaceholderSelect
                options={[
                  { value: "", label: "Seleccionar deposito B" },
                  { value: "placeholder-b1", label: "Placeholder deposito B-1" },
                  { value: "placeholder-b2", label: "Placeholder deposito B-2" },
                ]}
              />
            </label>
            <label>
              Radio del corredor (km)
              <PlaceholderSelect
                options={[
                  { value: "10", label: "10 km" },
                  { value: "25", label: "25 km" },
                  { value: "50", label: "50 km" },
                  { value: "100", label: "100 km" },
                ]}
              />
            </label>
            <button type="button">Analizar corredor</button>
          </div>
          <div className={styles.placeholderArea}>Espacio reservado para mapa y resultados del corredor.</div>
        </>
      );
    }

    if (activeTool === "zone") {
      return (
        <>
          <h3>Zona de interes</h3>
          <p className="muted">
            Selecciona una zona o punto de interes para identificar depositos cercanos y minerales
            registrados en el area.
          </p>
          <div className={styles.controls}>
            <label>
              Pais
              <PlaceholderSelect
                options={[
                  { value: "", label: "Seleccionar pais" },
                  { value: "cri", label: "Costa Rica (placeholder)" },
                  { value: "aus", label: "Australia (placeholder)" },
                ]}
              />
            </label>
            <label>
              Mineral (opcional)
              <PlaceholderSelect
                options={[
                  { value: "", label: "Todos" },
                  { value: "gold", label: "Gold (placeholder)" },
                  { value: "copper", label: "Copper (placeholder)" },
                ]}
              />
            </label>
            <label>
              Radio de busqueda (km)
              <PlaceholderSelect
                options={[
                  { value: "5", label: "5 km" },
                  { value: "15", label: "15 km" },
                  { value: "30", label: "30 km" },
                  { value: "60", label: "60 km" },
                ]}
              />
            </label>
            <button type="button">Buscar en zona</button>
          </div>
          <div className={styles.placeholderArea}>Espacio reservado para mapa y resultados de zona.</div>
        </>
      );
    }

    if (activeTool === "minerals") {
      return (
        <>
          <h3>Minerales frecuentes</h3>
          <p className="muted">
            Esta subseccion mostrara los minerales mas frecuentes en la zona seleccionada, usando los
            registros existentes en la base de datos.
          </p>
          <div className={styles.placeholderList}>
            <p>1. Placeholder mineral A - n ocurrencias</p>
            <p>2. Placeholder mineral B - n ocurrencias</p>
            <p>3. Placeholder mineral C - n ocurrencias</p>
          </div>
          <div className={styles.placeholderArea}>Espacio reservado para grafico o tabla de ranking.</div>
        </>
      );
    }

    return (
      <>
        <h3>Potencial exploratorio</h3>
        <p className="muted">
          Visualizacion experimental para identificar zonas con mayor concentracion de registros
          mineralogicos.
        </p>
        <div className={styles.controls}>
          <label>
            Mineral objetivo
            <PlaceholderSelect
              options={[
                { value: "", label: "Seleccionar mineral" },
                { value: "gold", label: "Gold (placeholder)" },
                { value: "nickel", label: "Nickel (placeholder)" },
              ]}
            />
          </label>
          <label>
            Intensidad / radio
            <PlaceholderSelect
              options={[
                { value: "low", label: "Baja (placeholder)" },
                { value: "mid", label: "Media (placeholder)" },
                { value: "high", label: "Alta (placeholder)" },
              ]}
            />
          </label>
        </div>
        <div className={styles.placeholderArea}>Espacio reservado para heatmap o capa geoespacial.</div>
      </>
    );
  }

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
        <section className={`panel ${styles.heroPanel}`}>
          <h2>Terreno</h2>
          <p className="muted">Prospeccion exploratoria para entusiastas y coleccionistas.</p>
          <p className="muted">
            Herramientas para explorar zonas de interes mineralogico a partir de depositos registrados,
            proximidad espacial y minerales asociados.
          </p>
          <p className={styles.disclaimer}>
            Los resultados de esta seccion se basaran en ocurrencias registradas y analisis espacial
            exploratorio. No representan una prediccion geologica ni garantizan la presencia de minerales
            en campo.
          </p>
        </section>

        <section className="panel">
          <div className={styles.toolTabs}>
            {toolTabs.map((tool) => (
              <button
                key={tool.id}
                type="button"
                className={tool.id === activeTool ? styles.toolTabActive : styles.toolTab}
                onClick={() => setActiveTool(tool.id)}
              >
                {tool.label}
              </button>
            ))}
          </div>
        </section>

        <section className="panel">
          {renderActiveTool()}
        </section>
      </main>
    </div>
  );
}
