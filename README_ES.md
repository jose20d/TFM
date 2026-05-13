# TFM — Código principal del proyecto

Este repositorio contiene el código operativo del TFM: ETL, API analítica y frontend bilingüe.

## Alcance actual

- ETL extremo a extremo (`python3 main.py`) desde fuentes oficiales hacia PostgreSQL/PostGIS.
- API con FastAPI (`web/app.py`) para dashboard, terreno y consultas guiadas.
- Frontend Next.js (`frontend/`) con interfaz bilingüe (`es`/`en`).
- Estrategia i18n híbrida para datos de dominio:
  - diccionario canónico (`i18n_term_catalog`, `i18n_term_translation`),
  - tabla materializada de serving (`i18n_term_materialized`),
  - semilla ETL (`database/i18n_terms_seed.csv`).

## Ejecución rápida (Linux)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=tu_db
export DB_USER=tu_usuario
export DB_PASSWORD=tu_password

python3 main.py
uvicorn web.app:app --reload --port 8000
```

En otra terminal:

```bash
cd frontend
npm install
export BACKEND_API_URL=http://127.0.0.1:8000
npm run dev
```

## URLs locales

- Frontend: `http://127.0.0.1:3000/`
- Backend/API: `http://127.0.0.1:8000/`
- Documentación OpenAPI: `http://127.0.0.1:8000/docs`

## Comportamientos clave del runtime

- Los crudos se descargan y preservan en `data/raw/` para trazabilidad.
- Idempotencia ETL por hash:
  - `etl_dataset_state`
  - `etl_dataset_run_log`
- Normalización territorial con whitelist ISO:
  - crudo: `data/raw/iso/country-codes.csv`
  - tabla: `iso_country_codes`
- Capa i18n de datos servida por:
  - `i18n_term_catalog`
  - `i18n_term_translation`
  - `i18n_term_materialized`

## Notas del frontend

- El idioma se controla con query param `lang` (`es` o `en`).
- El frontend propaga idioma hacia endpoints backend.
- `Explorar` usa límites por país y paginación para casos de alto volumen.

## Demo archivada de Semana 1

La demo original de validación de fuentes sigue archivada por trazabilidad:

- `archive/week1_data_consumption_demo/`

Ejecución:

```bash
cd archive/week1_data_consumption_demo && bash ./run_demo.sh
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

- **Idioma**: código en inglés; documentación técnica principal en español/inglés según archivo.
- **Capa de base de datos**: el pipeline principal carga en PostgreSQL/PostGIS.
- **Sin datos generados en Git**: `data/`, `output/` y `otros/` son generados y están ignorados por `.gitignore`.

## Restricciones de diseño / guardrails

- El pipeline no requiere superusuario; PostGIS debe habilitarlo un administrador previamente.
- Las descargas crudas se preservan para trazabilidad y auditoría.
- No hay staging en JSONL en la ruta principal; los datos se limpian en memoria y se cargan directo a PostgreSQL.
- `dataset_config` es el único registro de metadatos; no se usa `dim_dataset`.
- Un solo comando (`python3 main.py`) ejecuta el flujo ETL completo sin prompts interactivos.



