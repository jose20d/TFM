# Semana 1 — Demo archivada (consumo de datos + mapa HTML local)

Esta carpeta contiene la **demo de Semana 1** que se entregó para validar que los **orígenes públicos aprobados** son consumibles de forma real y reproducible (sin BD, sin servidor), generando salidas locales en JSON/JSONL y un mapa HTML interactivo.

> Esta demo se archiva por **trazabilidad** y no forma parte del código principal del proyecto.

## Orígenes aprobados (Semana 1)

Todos los orígenes están declarados en `configs/sources.json`:

- **World Bank — Worldwide Governance Indicators (WGI / PV.EST)** (`worldbank_wgi`) — API REST (JSON)
- **USGS — Mineral Resources Data System (MRDS)** (`usgs_mrds`) — descarga de fichero (CSV)
- **OneGeology — Geological Map Data** (`onegeology_wms`) — WMS GetCapabilities (XML)

## Qué genera esta demo

### Descargas (siempre local)

- `data/demo/<source_name>/records_100.json` (lista de dicts, hasta `--limit`)
- `data/demo/<source_name>/records_100.jsonl` (JSON Lines)
- `data/demo/<source_name>/metadata.json` (siempre se crea, incluso en fallo)
- `data/demo/<source_name>/debug_payload_snippet.txt` (solo si aplica)
- `data/demo/demo_report.json` (estado por fuente a nivel de ejecución)

### Resultado visible para revisión “no técnica”

- `output/demo_map.html` (mapa interactivo, se abre con doble clic)
- `output/demo_summary.json` (resumen con métricas y el WGI elegido)

> Nota: `data/` y `output/` son generados y están ignorados por Git a nivel del repositorio.

## Inicio rápido (1 comando)

```bash
bash ./run_demo.sh
```

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

## Notas / limitación (OneGeology)

En algunos entornos el host `portal.onegeology.org` puede no resolver (DNS/NXDOMAIN) o estar bloqueado. En ese caso la fuente quedará como fallida, pero el resto de la demo (MRDS + World Bank + mapa HTML) seguirá funcionando.

