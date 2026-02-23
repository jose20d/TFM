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

# Ejecutar el pipeline completo (descarga → limpieza → carga → Streamlit)
python3 main.py
```

## UI opcional (Streamlit local)

```bash
streamlit run streamlit_app.py
```

## Convenciones del repositorio

- **Idioma**: el código y la documentación principal están en **inglés**.
- **Sin base de datos**: los artefactos de Semana 1 generan solo archivos locales.
- **Sin datos generados en Git**: `data/`, `output/` y `otros/` son generados y están ignorados por `.gitignore`.



