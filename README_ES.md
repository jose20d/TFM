# Semana 1 — Demo reproducible de consumo de datos (sin BD, sin servidor)

Este repositorio es una **demo de Semana 1** para un TFM de Big Data & BI. El objetivo **no** es análisis/visualización como tal, sino demostrar que los **orígenes públicos aprobados** se pueden consumir de forma **real y reproducible**, con **manejo profesional de errores**, generando **archivos locales** (sin base de datos, sin servidor).

## Orígenes aprobados (Semana 1)

Todos los orígenes están declarados en `configs/sources.json`:

- **World Bank — Worldwide Governance Indicators (WGI / PV.EST)** (`worldbank_wgi`) — API REST (JSON)
- **USGS — Mineral Resources Data System (MRDS)** (`usgs_mrds`) — descarga de fichero (CSV)
- **OneGeology — Geological Map Data** (`onegeology_wms`) — WMS GetCapabilities (XML)

## Qué genera esta demo

### Descargas (siempre local)

Tras ejecutar el descargador, se generan:

- `data/demo/<source_name>/records_100.json` (lista de dicts, hasta `--limit`)
- `data/demo/<source_name>/records_100.jsonl` (JSON Lines)
- `data/demo/<source_name>/metadata.json` (siempre se crea, incluso en fallo)
- `data/demo/<source_name>/debug_payload_snippet.txt` (solo si aplica)
- `data/demo/demo_report.json` (estado por fuente a nivel de ejecución)

### Resultado visible para evaluación “no técnica”

Tras generar el mapa:

- `output/demo_map.html` (mapa interactivo, se abre con doble clic)
- `output/demo_summary.json` (resumen con métricas y el WGI elegido)

> Nota: `data/` y `output/` son **generados** y están ignorados por Git.

## Inicio rápido (1 comando)

```bash
bash ./run_demo.sh
```

Este script:

1. Crea/activa `.venv`
2. Instala dependencias desde `requirements.txt`
3. Borra lo generado previamente
4. Descarga las fuentes aprobadas (best-effort: continúa aunque falle alguna)
5. Genera `output/demo_map.html` e intenta abrirlo con `xdg-open`

## Ejecución manual (paso a paso)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

python scripts/clean_data.py --yes
python scripts/demo_fetch.py --limit 100 --sources all
python scripts/generate_demo_map.py \
  --mrds ./data/demo/usgs_mrds/records_100.json \
  --wgi  ./data/demo/worldbank_wgi/records_100.json \
  --out  ./output/demo_map.html
```

## Estructura del proyecto

- `run_demo.sh`: ejecución en 1 comando (limpia → descarga → mapa)
- `scripts/demo_fetch.py`: descarga fuentes y escribe JSON/JSONL + metadata + `demo_report.json`
- `scripts/generate_demo_map.py`: lee MRDS+WGI **locales** y genera `output/demo_map.html`
- `scripts/clean_data.py`: borra carpetas generadas (`data/demo`, `output`, etc.)

## Notas / limitación (OneGeology)

OneGeology se consume mediante un endpoint **WMS GetCapabilities**. En algunos entornos el host `portal.onegeology.org` puede no resolver (DNS/NXDOMAIN) o estar bloqueado. En ese caso:

- `demo_fetch.py` marcará `onegeology_wms` como `failed` en `metadata.json` y `demo_report.json`
- el resto de la demo (MRDS + World Bank + mapa HTML) seguirá funcionando



