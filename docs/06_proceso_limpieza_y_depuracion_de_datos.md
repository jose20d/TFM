# Proceso de limpieza y depuracion de datos

Este documento resume el flujo real de limpieza, depuracion, normalizacion y carga usado por el pipeline ETL del proyecto.

## 1) Objetivo del proceso

- Preservar trazabilidad de los datos crudos.
- Limpiar y normalizar datos en memoria.
- Cargar resultados a PostgreSQL/PostGIS de forma idempotente.
- Registrar estado del ETL para evitar recargas innecesarias.
- Mantener calidad de datos geograficos en `mrds_location` usando:
  - archivo oficial `Location.txt` (fuente primaria),
  - inferencia por coordenadas como respaldo.

## 1.1) Entrypoints operativos ETL

- Comando legado (compatibilidad): `python3 main.py`
- Comando modular actual: `python3 -m etl.run_etl`
- Docker (ejecución puntual): `docker compose --profile jobs run --rm etl`

Los tres caminos ejecutan el mismo flujo ETL y terminan al finalizar (sin scheduler interno).

## 2) Entradas y fuentes

Fuentes principales configuradas en `configs/datasets.json`:

- MRDS (USGS): `rdbms-tab-all.zip`
- World Bank GDP
- World Bank Population
- Fragile States Index (FSI)
- Corruption Perceptions Index (CPI)
- ISO 3166-1 country codes

Descarga:

- Script: `scripts/download_datasets.py`
- Se descarga en `data/raw/` sin transformar archivos crudos.

## 3) Reglas generales de limpieza

Aplicadas en `scripts/load_to_db.py`:

- Estandarizacion de nombres de pais con `normalize_country_name`.
- Normalizacion de codigos ISO3 con `normalize_iso3`.
- Uso de alias de pais (`references/country_aliases.json`) para unificar variantes.
- Canonicalizacion por ISO3: si una fila trae `iso3`, se fuerza el `country_norm` canonico de `iso_country_codes`.
- Filtros de calidad para descartar valores vacios, nulos o no validos.
- Filtrado por whitelist ISO para evitar paises no reconocidos en el modelo.

Fragmento relevante (normalizacion y filtro por whitelist ISO):

```python
def _norm_country(value: str, aliases: dict[str, str]) -> str:
    norm = normalize_country_name(value)
    return aliases.get(norm, norm)

def _norm_iso3(value: str | None) -> str | None:
    if not value:
        return None
    text = str(value).strip()
    return normalize_iso3(text) if text else None

def _filter_countries_by_iso(
    rows: Iterable[tuple[str, str, str | None]],
    iso3_set: set[str],
    name_set: set[str],
) -> list[tuple[str, str, str | None]]:
    if not iso3_set and not name_set:
        return list(rows)
    filtered = []
    for name, norm, iso3 in rows:
        if iso3 and normalize_iso3(iso3) in iso3_set:
            filtered.append((name, norm, iso3))
            continue
        if norm and norm in name_set:
            filtered.append((name, norm, iso3))
    return filtered
```

## 4) Flujo por dataset

### 4.1 ISO country codes

- Lee CSV ISO 3166-1.
- Identifica columnas de nombre/ISO2/ISO3 aunque cambien etiquetas.
- Normaliza nombre e ISO3.
- Normaliza ISO2 con validación defensiva:
  - descarta nulos/NaN,
  - conserva solo códigos de 2 caracteres,
  - aplica cast a `text` en SQL para evitar fallos por tipos mixtos.
- Carga en `iso_country_codes` con upsert por `iso3`.
- Ejecuta consolidacion de `dim_country` por `iso3` para eliminar duplicados historicos
  (ej.: variantes como "United States" vs "United States of America").

### 4.2 World Bank (GDP, Population)

- Parsea JSON.
- Conserva filas con pais e ISO3 validos.
- Convierte `value` a numerico.
- Conserva ultimo anio por ISO3.
- Inserta/actualiza en `country_indicator`.

### 4.3 FSI

- Lee Excel.
- Extrae rank numerico (ej. "144th" -> 144).
- Usa columna `Year` si existe, o infiere anio por metadatos.
- Inserta/actualiza en `country_indicator`.

### 4.4 CPI

- Soporta Excel strict OOXML (reescritura temporal de namespaces).
- Detecta dinamicamente fila de encabezados.
- Detecta columna de score CPI y anio.
- Convierte valores a rango esperado.
- Inserta/actualiza en `country_indicator`.

## 5) Flujo MRDS (limpieza y depuracion)

### 5.1 Carga base de depositos

- Lee `MRDS.txt`:
  - `dep_id`, `name`, `dev_stat`, `code_list`, `latitude`, `longitude`.
- Convierte coordenadas a numerico.
- Crea geometria PostGIS (`SRID=4326;POINT(lon lat)`).
- Upsert en `mrds_deposit`.

### 5.2 Carga primaria de ubicacion (`Location.txt`)

- Lee: `dep_id`, `country`, `state_prov`, `region`, `county`.
- Limpia `country` y elimina codigos regionales no-pais (`AF`, `EU`, `AS`, `OC`, `SA`, `CR`).
- Normaliza nombre de pais y aplica alias.
- Para `state_prov`, `region`, `county`:
  - blanks/nan/None -> `N/A` (regla historica del ETL).
- Mapea `country_norm -> country_id` (via `dim_country`).
- Upsert en `mrds_location`.

Fragmento relevante (limpieza de `Location.txt`):

```python
def _load_mrds_location(path: Path, aliases: dict[str, str]) -> pd.DataFrame:
    df = _read_mrds_table(path, usecols=["dep_id", "country", "state_prov", "region", "county"])
    df = df[df["country"].notna()]
    df["country"] = df["country"].astype(str).str.strip()
    invalid_countries = {"AF", "EU", "AS", "OC", "SA", "CR"}
    df = df[~df["country"].isin(invalid_countries)]
    df = df[df["country"] != ""]
    df["country_norm"] = df["country"].apply(normalize_country_name)
    df["country_norm"] = df["country_norm"].map(lambda x: aliases.get(x, x))
    for col in ["state_prov", "region", "county"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df.loc[df[col].isin(["", "nan", "None"]), col] = "N/A"
    return df
```

### 5.3 Carga de tablas relacionadas MRDS

Tablas 1-N por deposito:

- `mrds_commodity`
- `mrds_material`
- `mrds_ownership`
- `mrds_physiography`
- `mrds_ages`
- `mrds_rocks`

Se filtran por `dep_id` valido y se recargan por lote.

## 6) Reconciliacion geoespacial de `mrds_location`

Para mejorar cobertura cuando falta `Location.txt` o viene incompleto:

- Se usa `reverse_geocoder` con (`latitude`, `longitude`).
- Se obtiene:
  - ISO2 (`cc`) para mapear pais.
  - `admin1` para `state_prov`.
- Mapeo de pais:
  - `ISO2 -> ISO3` via `iso_country_codes`
  - `ISO3 -> country_id` via `dim_country`

### Orden de reconciliacion

1. Backfill de `country_id` en filas existentes de `mrds_location` con pais nulo.
2. Insercion de filas faltantes (depositos sin registro en `mrds_location`).
3. Reparacion de filas existentes con `country_id` nulo o `state_prov` nulo/blank/`N/A`.

La reconciliacion se ejecuta dentro de la carga MRDS y tambien al final del bloque MRDS
como paso de "reconcile", para cubrir escenarios donde el hash del dataset no cambia
pero aun existen huecos de calidad en `mrds_location`.

Fragmento relevante (orquestacion de reconciliacion):

```python
def _run_mrds_location_reconcile(cur) -> tuple[int, int, int]:
    iso2_map = _iso2_country_id_map(cur)
    pre_country = _repair_existing_mrds_locations(
        cur, iso2_map, fix_country=True, fix_state=False
    )
    inserted_missing = _infer_missing_mrds_locations(cur, iso2_map)
    post_repair = _repair_existing_mrds_locations(
        cur, iso2_map, fix_country=True, fix_state=True
    )
    return pre_country, inserted_missing, post_repair
```

### Regla clave

- `N/A` debe quedar solo cuando no se logra resolver valor confiable despues
  de completar los 3 pasos de reconciliacion.

## 7) Validaciones de calidad recomendadas

### Integridad relacional

```sql
SELECT COUNT(*) AS commodities_sin_deposito
FROM mrds_commodity mc
LEFT JOIN mrds_deposit md ON md.dep_id = mc.dep_id
WHERE md.dep_id IS NULL;
```

```sql
SELECT COUNT(*) AS depositos_sin_location
FROM mrds_deposit md
LEFT JOIN mrds_location ml ON ml.dep_id = md.dep_id
WHERE ml.dep_id IS NULL;
```

### Calidad de `mrds_location`

```sql
SELECT
  SUM(CASE WHEN country_id IS NULL THEN 1 ELSE 0 END) AS country_id_null,
  SUM(CASE WHEN state_prov IS NULL OR TRIM(state_prov) = '' THEN 1 ELSE 0 END) AS state_prov_null_blank,
  SUM(CASE WHEN state_prov = 'N/A' THEN 1 ELSE 0 END) AS state_prov_na
FROM mrds_location;
```

## 8) Idempotencia y auditoria

- Se usa hash de archivo para detectar cambios (`etl_dataset_state`).
- Si hash no cambia, se puede omitir carga del dataset.
- Se registran ejecuciones en `etl_dataset_run_log`.
- El esquema se inicializa de forma idempotente en cada corrida.

Fragmento relevante (skip por hash + logging de corrida):

```python
hash_value = _file_hash(raw_path)
last_hash, _ = _get_dataset_state(cur, dataset_id)
if hash_value and last_hash == hash_value:
    log_no_change(dataset_id, hash_value, int((time.time() - start) * 1000))
    conn.commit()
    return

rows_inserted, rows_updated = loader()
_upsert_dataset_state(cur, dataset_id, hash_value or "", True)
_insert_run_log(
    cur,
    dataset_id=dataset_id,
    download_success=True,
    hash_value=hash_value,
    has_changes=True,
    load_success=True,
    rows_inserted=rows_inserted,
    rows_updated=rows_updated,
    duration_ms=int((time.time() - start) * 1000),
    error_message=None,
)
```

## 9) Limitaciones actuales

- Geocodificacion inversa basada en centroides/cercania, no en poligonos administrativos precisos.
- Algunos territorios pueden no mapear a `dim_country` si no estan presentes en la dimension.
- Existen casos fronterizos donde `admin1` puede no ser exacto al 100%.

## 10) Control de duplicados en `dim_country`

### Problema detectado

Se observaron duplicidades con mismo `iso3` y distinto nombre normalizado en `dim_country`
(por ejemplo `USA`, `KOR`, `RUS`, `VEN`, entre otros), originadas por diferencias de nomenclatura
entre fuentes (nombre corto vs nombre oficial).

### Regla de limpieza aplicada

1. Si una fila de indicadores trae `iso3`, el ETL sustituye `country_norm` por el valor canonico
   de `iso_country_codes` para ese `iso3`.
2. En cada carga de ISO, el ETL consolida `dim_country` por `iso3`:
   - conserva una fila canonica por `iso3` (priorizando la que coincide con ISO),
   - reasigna FK en `country_indicator` y `mrds_location`,
   - elimina filas duplicadas sobrantes.

Fragmento relevante (merge de duplicados por `iso3`):

```python
def _merge_dim_country_duplicates_by_iso3(cur) -> int:
    cur.execute(
        """
        SELECT iso3
        FROM dim_country
        WHERE iso3 IS NOT NULL AND TRIM(iso3) <> ''
        GROUP BY iso3
        HAVING COUNT(*) > 1
        ORDER BY iso3
        """
    )
    duplicate_iso3 = [row[0] for row in cur.fetchall()]
    merged = 0
    for iso3 in duplicate_iso3:
        # ... seleccion de country_id canonico ...
        # ... reasignacion de FKs en country_indicator y mrds_location ...
        # ... borrado de duplicados sobrantes ...
        merged += 1
    return merged
```

### Consulta de verificacion recomendada

```sql
SELECT iso3, COUNT(*) AS total
FROM dim_country
WHERE iso3 IS NOT NULL AND TRIM(iso3) <> ''
GROUP BY iso3
HAVING COUNT(*) > 1
ORDER BY total DESC, iso3;
```

## 11) Mejoras futuras sugeridas

- Migrar reconciliacion geografica a `ST_Contains`/`ST_Intersects` con capas Admin0/Admin1 en PostGIS.
- Guardar `NULL` semantico en BD y renderizar `N/A` solo en capa de presentacion.
- Agregar reporte automatico de calidad por corrida ETL.

## 12) Integracion i18n en pipeline de carga

El flujo ETL actual incorpora una capa de localizacion de dominio:

- semilla inicial versionada (`database/i18n_terms_seed.csv`),
- catalogacion automatica de terminos detectados en tablas de negocio,
- materializacion bilingue (`i18n_term_materialized`) para serving rapido en API.

Esto evita hardcodes de traduccion en frontend y permite filtrar/mostrar datos en `es` y `en` con consistencia.

## 13) Control de volumen en exploracion por pais

Para escenarios de alta cardinalidad (ej. USA), el flujo operativo se completa con reglas de serving:

- no retornar puntos en `Explorar` cuando no hay pais seleccionado,
- aplicar limite efectivo por `country_iso3`,
- usar paginacion (`limit` + `offset`) para evitar cargas masivas en una sola respuesta.

Estas reglas protegen rendimiento y estabilidad sin alterar la calidad de datos ETL.
