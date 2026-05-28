# TFM — Código principal del proyecto

Este repositorio contiene el código operativo del TFM: ETL, API analítica y frontend bilingüe.

## Alcance actual

- ETL extremo a extremo desde fuentes oficiales hacia PostgreSQL/PostGIS.
- Backend FastAPI (`web/app.py`) para analítica, terreno y consultas guiadas.
- Frontend Next.js (`frontend/`) con UX bilingüe (`es` / `en`).
- i18n híbrido para datos de dominio (`country`, `mineral` y dominios relacionados).
- Flujo local con Docker para backend, frontend, postgres/postgis y ETL bajo demanda.

## Modos de ejecución

### 1) Modo local (sin Docker)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=tu_db
export DB_USER=tu_usuario
export DB_PASSWORD=tu_password

# ETL (ambos comandos son válidos)
python3 main.py
python3 -m etl.run_etl

# Backend
uvicorn web.app:app --reload --port 8000
```

En otra terminal:

```bash
cd frontend
npm install
export BACKEND_API_URL=http://127.0.0.1:8000
npm run dev
```

### 2) Modo Docker local

```bash
cp .env.example .env
docker compose up -d --build
```

Ejecutar ETL bajo demanda (no automático en `up`):

```bash
docker compose --profile jobs run --rm etl
```

## URLs locales

- Frontend: `http://127.0.0.1:3000/`
- Backend/API: `http://127.0.0.1:8000/`
- OpenAPI docs: `http://127.0.0.1:8000/docs`

## Despliegue en producción

- Workflow de GitHub Actions: `.github/workflows/deploy-production.yml`.
- Disparo: push a `main`.
- El playbook de deploy (`ansible/deploy.yml`) ahora escribe `/opt/tfm-geocontext/.env` desde GitHub Secrets antes de `docker compose up -d`.
- Secrets recomendados en el repositorio:
  - `EC2_HOST`, `EC2_USER`, `EC2_SSH_KEY`
  - `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`
  - `NEXT_PUBLIC_API_URL`
  - `INTERNAL_ADMIN_TOKEN`, `ADMIN_PANEL_USER`, `ADMIN_PANEL_PASSWORD`
  - `ETL_SCHEDULE_CRON`, `ETL_TIMEZONE` (opcionales)

## HTTPS (Let's Encrypt)

- Dominio productivo previsto: `geocontext.app` (con `www.geocontext.app` opcional).
- Flujo recomendado:
  1. Publicar DNS (`A`) de `geocontext.app` y `www` hacia la IP pública EC2.
  2. Aplicar configuración HTTP base:
     `ansible-playbook -i ansible/inventory.ini ansible/nginx.yml`
  3. Emitir certificado y activar TLS:
     `ansible-playbook -i ansible/inventory.ini ansible/https.yml`
- El playbook `ansible/https.yml`:
  - solicita certificado por webroot (`/var/www/certbot`),
  - activa redirección HTTP->HTTPS,
  - instala hook de renovación para recargar Nginx.

## Notas ETL

- Módulo principal ETL: `etl/run_etl.py`
- Wrapper de compatibilidad: `main.py`
- Los crudos se preservan en `data/raw/` para trazabilidad.
- Idempotencia y observabilidad ETL:
  - `etl_dataset_state`
  - `etl_dataset_run_log`
  - `etl_load_log`
- Referencia ISO para normalización territorial:
  - archivo crudo: `data/raw/iso/country-codes.csv`
  - tabla: `iso_country_codes`

## Notas i18n

- Diccionario + traducción + tabla materializada de serving:
  - `i18n_term_catalog`
  - `i18n_term_translation`
  - `i18n_term_materialized`
- El idioma en frontend se controla con `lang` (`es`, `en`).
- El backend localiza valores de dominio antes de responder.

## Prerrequisitos

- **SO**: Linux (Ubuntu 22.04+ recomendado).
- **Python**: 3.10+.
- **Node.js**: 20+.
- **PostgreSQL**: 14+.
- **PostGIS**: habilitado en la base de datos destino.

Habilitar PostGIS una vez (como administrador):

```sql
CREATE EXTENSION IF NOT EXISTS postgis;
```

## Restricciones arquitectónicas clave

- No hay capa JSON intermedia en la ruta principal ETL.
- `dataset_config` es el registro de metadatos.
- El ETL sigue siendo proceso finito (ejecuta y termina), sin scheduler interno.
- El ETL en Docker corre bajo el profile `jobs` para no ejecutarse al levantar web.

## Convenciones del repositorio

- Los datos generados y salidas locales están ignorados por `.gitignore`.
- Archivos de entorno sensibles se ignoran (`.env*`, excepto `.env.example`).
- `README_WINDOWS.md` se mantiene solo como documentación local (no trackeado en Git).



