# ANEXO B — Evidencia tecnica del proceso ETL

## 1. Objetivo del anexo

Este anexo documenta con evidencia tecnica el ETL implementado en el proyecto, para complementar el apartado principal de "Procesos ETL" con:

- scripts reales existentes en el repositorio;
- ejemplos de datos crudos tal como llegan desde la fuente;
- transformaciones intermedias aplicadas;
- consultas SQL de carga y validacion;
- evidencias de salida del proceso.

La intencion es que el lector pueda seguir el flujo de extremo a extremo (Extract -> Transform -> Load) con trazabilidad reproducible.

## 2. Inventario de componentes ETL reales

Entrypoints y componentes operativos en el codigo:

- `main.py`: wrapper de compatibilidad que ejecuta `etl.run_etl.main()`.
- `etl/run_etl.py`: orquestador E2E (descarga, extraccion MRDS ZIP, carga a PostgreSQL).
- `scripts/download_datasets.py`: descarga de fuentes crudas a `data/raw/` con reintentos y backoff.
- `scripts/load_to_db.py`: transformaciones, limpieza, normalizacion y carga idempotente.
- `configs/datasets.json`: catalogo de fuentes y rutas de salida raw.
- `database/create_schema.sql`: definicion del modelo relacional y tablas de auditoria ETL.

Comandos de ejecucion:

```bash
# compatibilidad legacy
python3 main.py

# entrypoint modular actual
python3 -m etl.run_etl

# ejecucion en contenedor (job puntual)
docker compose --profile jobs run --rm etl
```

## 3. Etapa Extract (evidencia de datos de origen)

### 3.1 Configuracion de datasets

El archivo `configs/datasets.json` declara las fuentes activas:

- `mrds_csv` (USGS, ZIP tabular)
- `worldbank_gdp` (JSON API)
- `worldbank_population` (JSON API)
- `fsi` (Excel)
- `cpi` (Excel)
- `iso_country_codes` (CSV)

### 3.2 Ejemplo real de payload crudo (World Bank)

Fuente:
`https://api.worldbank.org/v2/country/all/indicator/NY.GDP.MKTP.CD?format=json&per_page=2`

Extracto real:

```json
[
  {
    "page": 1,
    "pages": 8778,
    "per_page": 2,
    "total": 17556,
    "sourceid": "2",
    "lastupdated": "2026-04-08"
  },
  [
    {
      "country": {"id": "ZH", "value": "Africa Eastern and Southern"},
      "countryiso3code": "AFE",
      "date": "2025",
      "value": null
    },
    {
      "country": {"id": "ZH", "value": "Africa Eastern and Southern"},
      "countryiso3code": "AFE",
      "date": "2024",
      "value": 1242693542929.85
    }
  ]
]
```

### 3.3 Ejemplo real de payload crudo (ISO country codes CSV)

Fuente:
`https://raw.githubusercontent.com/datasets/country-codes/master/data/country-codes.csv`

Extracto real (cabecera + filas):

```csv
FIFA,Dial,ISO3166-1-Alpha-3,...,ISO3166-1-Alpha-2,...,CLDR display name,...
AFG,93,AFG,...,AF,...,Afghanistan,...
ALB,355,ALB,...,AL,...,Albania,...
DZA,213,DZA,...,DZ,...,Algeria,...
```

### 3.4 Evidencia de script de extraccion

El proceso de descarga usa streaming, timeout, retries y backoff exponencial:

```python
with requests.get(url, stream=True, timeout=timeout) as resp:
    resp.raise_for_status()
    with dest.open("wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)
```

## 4. Etapa Transform (evidencia de limpieza y normalizacion)

## 4.1 Normalizacion territorial y codigos

La canonizacion territorial se apoya en:

- normalizacion de nombre (`normalize_country_name`);
- normalizacion ISO3 (`normalize_iso3`);
- filtros por whitelist ISO (`iso_country_codes`);
- consolidacion de duplicados por `iso3` en `dim_country`.

Fragmento:

```python
def _norm_country(value: str, aliases: dict[str, str]) -> str:
    norm = normalize_country_name(value)
    return aliases.get(norm, norm)

def _norm_iso3(value: str | None) -> str | None:
    if not value:
        return None
    text = str(value).strip()
    return normalize_iso3(text) if text else None
```

## 4.2 Transformacion intermedia (ejemplo World Bank)

Desde el payload crudo anterior:

- se descartan filas sin `value` valido;
- se convierte `value` a numerico;
- se mantiene solo el ultimo anio por `iso3`.

Ejemplo de fila transformada (estructura intermedia en memoria):

```json
{
  "dataset_id": "worldbank_gdp",
  "indicator_code": "NY.GDP.MKTP.CD",
  "country": "Africa Eastern and Southern",
  "country_norm": "africa eastern and southern",
  "iso3": "AFE",
  "year": 2024,
  "value": 1242693542929
}
```

## 4.3 Transformaciones especificas de calidad

- FSI: convierte `"144th"` a `144` y usa `Year` o anio inferido.
- CPI: soporta archivos strict OOXML con reescritura temporal de namespaces.
- MRDS `Location`: elimina codigos no pais (`AF`, `EU`, `AS`, `OC`, `SA`, `CR`) y limpia vacios.
- Reconciliacion geoespacial: usa `reverse_geocoder` para completar `country_id`/`state_prov`.

Fragmento de limpieza MRDS Location:

```python
invalid_countries = {"AF", "EU", "AS", "OC", "SA", "CR"}
df = df[~df["country"].isin(invalid_countries)]
df = df[df["country"] != ""]
df["country_norm"] = df["country"].apply(normalize_country_name)
```

## 5. Etapa Load (evidencia SQL y persistencia)

## 5.1 Modelo de destino

El ETL carga en:

- dimensiones: `dim_country`, `iso_country_codes`;
- hechos/indicadores: `country_indicator`;
- tablas geologicas: `mrds_deposit`, `mrds_location`, `mrds_commodity`, `mrds_material`, `mrds_ownership`, `mrds_physiography`, `mrds_ages`, `mrds_rocks`;
- auditoria: `etl_load_log`, `etl_dataset_state`, `etl_dataset_run_log`.

## 5.2 Evidencia de upsert idempotente

Ejemplo real de carga incremental en `country_indicator`:

```sql
INSERT INTO country_indicator (country_id, dataset_id, indicator_code, year, value)
VALUES %s
ON CONFLICT (country_id, dataset_id, indicator_code, year) DO UPDATE
SET value = EXCLUDED.value
```

## 5.3 Evidencia de carga espacial MRDS

La geometria se materializa en PostGIS al cargar depositos:

```python
f"SRID=4326;POINT({lon} {lat})" if pd.notna(lat) and pd.notna(lon) else None
```

y se inserta con:

```sql
... ST_GeomFromText(%s) ...
```

## 6. Idempotencia, auditoria y observabilidad

El pipeline no recarga un dataset si no cambia el hash del archivo crudo:

```python
hash_value = _file_hash(raw_path)
last_hash, _ = _get_dataset_state(cur, dataset_id)
if hash_value and last_hash == hash_value:
    log_no_change(dataset_id, hash_value, duration_ms)
    return
```

Tablas de trazabilidad:

- `etl_dataset_state`: ultimo hash y ultimo estado.
- `etl_dataset_run_log`: historial de ejecuciones por dataset.
- `etl_load_log`: bitacora de carga (archivo, tamano, estado, error).

## 7. Evidencia de resultados (consultas reproducibles)

Las siguientes consultas permiten verificar resultados en la base de datos despues de ejecutar ETL:

```sql
SELECT COUNT(*) AS total_paises FROM dim_country;
SELECT COUNT(*) AS total_depositos FROM mrds_deposit;
SELECT COUNT(*) AS total_indicadores FROM country_indicator;
```

```sql
SELECT dataset_id, COUNT(*) AS total
FROM country_indicator
GROUP BY dataset_id
ORDER BY dataset_id;
```

```sql
SELECT
  SUM(CASE WHEN country_id IS NULL THEN 1 ELSE 0 END) AS country_id_null,
  SUM(CASE WHEN state_prov IS NULL OR TRIM(state_prov) = '' THEN 1 ELSE 0 END) AS state_prov_null_blank
FROM mrds_location;
```

## 8. Evidencia de etapas intermedias recomendada para lectura del TFM

Para responder al objetivo de "ver el dato en cada etapa", se recomienda incluir en el texto principal una secuencia minima por dataset:

1. **Raw source**: fila o payload original (JSON/CSV/XLSX).
2. **Transformed row**: registro ya normalizado en memoria.
3. **Loaded row**: fila final en tabla destino (`SELECT ... LIMIT 5`).

Ejemplo de consulta para mostrar fila final (GDP):

```sql
SELECT c.country_name, c.iso3, ci.year, ci.value
FROM country_indicator ci
JOIN dim_country c ON c.country_id = ci.country_id
WHERE ci.dataset_id = 'worldbank_gdp'
ORDER BY ci.year DESC, ci.value DESC
LIMIT 5;
```

## 9. Nota metodologica sobre evidencias de archivos raw

En este repositorio los crudos de `data/raw/` no se versionan de forma completa para evitar inflar el control de versiones. Por ello, los extractos de "dato crudo" se documentan a partir de:

- endpoints fuente declarados en `configs/datasets.json`;
- ejemplos reales consultados desde esas fuentes;
- estructura exacta esperada por los parsers de `scripts/load_to_db.py`.

Esto conserva trazabilidad metodologica sin comprometer el tamano del repositorio.

## 10. Conclusion del anexo

El ETL de GeoContext no es una descripcion teorica: es un flujo implementado, reproducible e idempotente, con evidencia en codigo, normalizacion territorial robusta, carga relacional/espacial en PostgreSQL/PostGIS y auditoria historica por dataset.
