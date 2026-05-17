# Dockerización local y operación ETL

Este documento resume el trabajo de dockerización local inicial y los ajustes operativos aplicados para mantener el comportamiento funcional del proyecto.

## 1) Objetivo alcanzado

Se habilitó una ejecución local reproducible con Docker Compose sin cambiar contratos funcionales de backend/frontend/ETL:

- `docker compose up -d --build` para entorno web local.
- ETL separado como proceso puntual bajo profile `jobs`.

## 2) Componentes creados

### 2.1 Backend

- `Dockerfile` en raíz del proyecto.
- Runtime: Python + `requirements.txt`.
- Arranque: `uvicorn web.app:app --host 0.0.0.0 --port 8000`.

### 2.2 Frontend

- `frontend/Dockerfile`.
- Build de Next.js y runtime con `npm start`.
- Puerto expuesto: `3000`.

### 2.3 Orquestación local

- `docker-compose.yml` con servicios:
  - `postgres` (`postgis/postgis`)
  - `backend`
  - `frontend`
  - `etl` (profile `jobs`)

Servicio ETL:

```yaml
etl:
  build:
    context: .
    dockerfile: Dockerfile
  env_file:
    - .env
  depends_on:
    postgres:
      condition: service_healthy
  command: python -m etl.run_etl
  profiles:
    - jobs
```

## 3) Contexto de build optimizado

Para reducir el contexto enviado a Docker y evitar builds de ~1GB:

- `.dockerignore` en raíz.
- `frontend/.dockerignore`.

Se excluyen `node_modules`, `.next`, `data/raw`, `data/processed`, `logs`, `.git`, `.venv`, entre otros.

## 4) Compatibilidad ETL

Se mantuvo compatibilidad de ejecución tras modularizar entrypoints:

- `python3 main.py` (wrapper de compatibilidad)
- `python3 -m etl.run_etl` (entrypoint modular)
- `docker compose --profile jobs run --rm etl` (contenedor puntual)

No se añadió scheduler interno en Python.

## 5) Incidencias resueltas durante la dockerización

1. **Build Next.js (App Router + `useSearchParams`)**
   - Se añadieron `Suspense` boundaries en páginas cliente afectadas.
   - En `terreno`, se usó wrapper cliente para evitar `window is not defined` en prerender.

2. **Carga de indicadores en ETL**
   - Se reforzó observabilidad por dataset (`[load] ...` y `[ok] country_indicator inserted/updated: X`).
   - Se mantuvo carga en `country_indicator` para GDP/Population/FSI/CPI.

3. **Error ISO2 (`character(2)`)**
   - Se incorporó sanitización robusta de `iso2` (tipos mixtos / NaN) en ETL.

4. **Antártica en selectores**
   - Se excluyó explícitamente del catálogo de países para evitar selección inválida en UI.

## 6) Comandos operativos recomendados

Levantar entorno web:

```bash
cp .env.example .env
docker compose up -d --build
```

Ejecutar ETL:

```bash
docker compose --profile jobs run --rm etl
```

Verificar estado:

```bash
docker compose ps
```
