"use client";

import { useEffect, useState } from "react";

function formatNumber(value) {
  const num = Number(value || 0);
  return new Intl.NumberFormat("es-ES").format(Number.isFinite(num) ? num : 0);
}

export default function InternalAdminVisitsPage() {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [payload, setPayload] = useState(null);

  useEffect(() => {
    let mounted = true;
    setLoading(true);
    setError("");

    fetch("/api/internal-admin/visits?days=30", { cache: "no-store" })
      .then(async (response) => {
        const raw = await response.text();
        const data = raw ? JSON.parse(raw) : {};
        if (!response.ok) {
          throw new Error(data?.detail || `HTTP ${response.status}`);
        }
        return data;
      })
      .then((data) => {
        if (!mounted) return;
        setPayload(data || null);
      })
      .catch((err) => {
        if (!mounted) return;
        setError(err?.message || "No fue posible cargar el panel.");
      })
      .finally(() => {
        if (mounted) setLoading(false);
      });

    return () => {
      mounted = false;
    };
  }, []);

  return (
    <main style={{ maxWidth: 980, margin: "2rem auto", padding: "0 1rem", color: "#e6edf3" }}>
      <h1 style={{ marginBottom: "0.5rem" }}>Panel interno de visitas</h1>
      <p style={{ marginTop: 0, opacity: 0.8 }}>Metrica privada (ultimos 30 dias)</p>

      {loading && <p>Cargando...</p>}
      {error && <p style={{ color: "#ff8080" }}>Error: {error}</p>}

      {payload && (
        <>
          <section style={{ display: "grid", gap: "0.5rem", margin: "1rem 0" }}>
            <div>Total visitas: {formatNumber(payload?.totals?.total_visits)}</div>
            <div>Ultimas 24h: {formatNumber(payload?.totals?.last_24h)}</div>
            <div>Ultimos 7 dias: {formatNumber(payload?.totals?.last_7d)}</div>
            <div>Ultimos 30 dias: {formatNumber(payload?.totals?.last_30d)}</div>
          </section>

          <section style={{ margin: "1.25rem 0" }}>
            <h2 style={{ marginBottom: "0.5rem" }}>Top rutas</h2>
            <ul>
              {(payload?.top_paths || []).map((item, idx) => (
                <li key={`${item.path}-${idx}`}>
                  <code>{item.path}</code> - {formatNumber(item.visits)}
                </li>
              ))}
            </ul>
          </section>

          <section style={{ margin: "1.25rem 0" }}>
            <h2 style={{ marginBottom: "0.5rem" }}>Visitas por dia</h2>
            <ul>
              {(payload?.daily || []).map((item) => (
                <li key={item.day}>
                  {item.day}: {formatNumber(item.visits)}
                </li>
              ))}
            </ul>
          </section>
        </>
      )}
    </main>
  );
}
