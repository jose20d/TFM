# TFM — Código principal del proyecto

Este es el **código principal** del TFM. El desarrollo nuevo empieza aquí.

## Alcance actual (Fase 2)

- Descargar datasets crudos para trazabilidad.
- Limpiar y normalizar directamente en PostgreSQL/PostGIS.
- Explorar relaciones localmente con Streamlit (opcional).

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

# Conexión a base de datos (ajusta según tu entorno)
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=tfm
export DB_USER=tfm
export DB_PASSWORD='tu_password'

# Ejecutar el pipeline completo (descarga → limpieza → carga → Streamlit)
python3 main.py
```

Nota: CPI está configurado en `configs/datasets.json` pero la descarga está desactivada por defecto.
Actualiza la URL al XLSX directo y cambia `"download": true` para habilitarlo.

## UI opcional (Streamlit local)

```bash
streamlit run streamlit_app.py
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
- Un solo comando (`python3 main.py`) ejecuta el flujo completo sin prompts interactivos.



