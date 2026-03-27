# TFM — Código principal del proyecto

Este es el **código principal** del TFM. El desarrollo nuevo empieza aquí.

## Alcance actual (Fase 3)

- Descargar datasets crudos para trazabilidad.
- Limpiar y normalizar directamente en PostgreSQL/PostGIS.
- Exponer API REST y una web base (FastAPI + HTML/CSS/JS).

## Trazabilidad: demo de validación de fuentes (Semana 1) — archivada

La demo técnica de Semana 1 para validar las fuentes aprobadas (descarga → JSON/JSONL local → mapa HTML local) se archivó en:

- `archive/week1_data_consumption_demo/`

Esa carpeta está autocontenida (incluye su propio `README`, `requirements.txt` y scripts ejecutables) y se mantiene por trazabilidad. No forma parte del runtime del proyecto principal.

Para ejecutar la demo archivada de Semana 1:

```bash
cd archive/week1_data_consumption_demo && bash ./run_demo.sh
```

## Cómo ejecutar (código principal)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Conexión a base de datos (ejemplo: ajusta según tu entorno)
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=tu_db
export DB_USER=tu_usuario
export DB_PASSWORD=tu_password

# Ejecutar el pipeline ETL (descarga → limpieza → carga)
python3 main.py

# Levantar web + API local
uvicorn web.app:app --reload

# En otra terminal: levantar frontend Next.js
cd frontend
npm install
npm run dev
```

Los archivos crudos siempre se descargan de nuevo para mantener CI determinista.

## Referencia ISO de países (whitelist)

El pipeline descarga los códigos ISO 3166-1 y los carga en PostgreSQL.
Solo se insertan en `dim_country` los países presentes en ese dataset ISO.

- Archivo crudo: `data/raw/iso/country-codes.csv`
- Tabla en BD: `iso_country_codes`
- Uso: filtro whitelist antes de insertar en `dim_country`

## Estado y auditoría del ETL

El ETL registra hashes por dataset y guarda un historial de ejecuciones.

- Tabla de estado: `etl_dataset_state`
- Log histórico: `etl_dataset_run_log`
- Comportamiento: si el hash no cambia, se omite la carga.

## Web y API local

La aplicación web y los endpoints API comparten el mismo backend FastAPI:

- Web: `http://127.0.0.1:8000/`
- API docs: `http://127.0.0.1:8000/docs`

## Frontend web (Next.js)

Se añadió un frontend independiente en `frontend/` para la nueva experiencia visual.

- Frontend: `http://127.0.0.1:3000/`
- Backend API: `http://127.0.0.1:8000/` (o el puerto que uses para FastAPI)

Si ejecutas FastAPI en un puerto distinto, exporta en el frontend:

```bash
export BACKEND_API_URL=http://127.0.0.1:8001
npm run dev
```

## Prerrequisitos

- **SO**: Linux (Ubuntu 22.04+ recomendado). El proyecto está probado en entornos Linux.
- **Python**: 3.10+ (recomendado 3.12).
- **PostgreSQL**: 14+ (servidor y herramientas cliente).
- **PostGIS**: habilitado en la base de datos destino.

### Instalación de PostgreSQL

Consulta la guía oficial de instalación para Linux:
- PostgreSQL Global Development Group. (2024). *PostgreSQL: Linux downloads (Debian/Ubuntu)*. https://www.postgresql.org/download/linux/ubuntu/

### Instalación de PostGIS

Consulta la documentación oficial de PostGIS:
- PostGIS Project. (2024). *PostGIS: Installation*. https://postgis.net/documentation/

Tras instalar PostGIS, habilítalo en la base de datos (como superusuario):

```sql
CREATE EXTENSION IF NOT EXISTS postgis;
```

## Convenciones del repositorio

- **Idioma**: el código y la documentación principal están en **inglés**.
- **Capa de base de datos**: el pipeline principal carga en PostgreSQL/PostGIS.
- **Sin datos generados en Git**: `data/`, `output/` y `otros/` son generados y están ignorados por `.gitignore`.

## Restricciones de diseño / guardrails

- El pipeline no requiere superusuario; PostGIS debe habilitarlo un administrador previamente.
- Las descargas crudas se preservan para trazabilidad y auditoría.
- No hay staging en JSONL en la ruta principal; los datos se limpian en memoria y se cargan directo a PostgreSQL.
- `dataset_config` es el único registro de metadatos; no se usa `dim_dataset`.
- Un solo comando (`python3 main.py`) ejecuta el flujo ETL completo sin prompts interactivos.



