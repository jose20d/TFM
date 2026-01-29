# TFM — Código principal del proyecto

Este es el **código principal** del TFM. El desarrollo nuevo empieza aquí.

## Alcance actual (Fase 2)

- Normalizar datasets de referencia (CSV/XLSX) a JSONL, sin base de datos.
- Filtrar por país usando ISO3 o nombre (con aliases).
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

# Normalizar XLSX → JSONL (PIB, Población, CPI, FSI)
python scripts/normalize_xlsx.py

# Construir mapa dep_id → país para MRDS
python scripts/build_mrds_country_map.py

# Ejecutar ejemplos de consulta (CPI/FSI/PIB/Población)
python scripts/run_queries.py

# Filtrar tablas MRDS por país
python scripts/filter_mrds_by_country.py --input references/Rocks.csv --country "Chile" --out output/queries/rocks_chile.json
```

## UI opcional (Streamlit local)

```bash
streamlit run streamlit_app.py
```

## Convenciones del repositorio

- **Idioma**: el código y la documentación principal están en **inglés**.
- **Sin base de datos**: los artefactos de Semana 1 generan solo archivos locales.
- **Sin datos generados en Git**: `data/`, `output/` y `otros/` son generados y están ignorados por `.gitignore`.



