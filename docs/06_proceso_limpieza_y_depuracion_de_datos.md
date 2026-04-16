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
- Filtros de calidad para descartar valores vacios, nulos o no validos.
- Filtrado por whitelist ISO para evitar paises no reconocidos en el modelo.

## 4) Flujo por dataset

### 4.1 ISO country codes

- Lee CSV ISO 3166-1.
- Identifica columnas de nombre/ISO2/ISO3 aunque cambien etiquetas.
- Normaliza nombre e ISO3.
- Carga en `iso_country_codes` con upsert por `iso3`.

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

## 9) Limitaciones actuales

- Geocodificacion inversa basada en centroides/cercania, no en poligonos administrativos precisos.
- Algunos territorios pueden no mapear a `dim_country` si no estan presentes en la dimension.
- Existen casos fronterizos donde `admin1` puede no ser exacto al 100%.

## 10) Mejoras futuras sugeridas

- Migrar reconciliacion geografica a `ST_Contains`/`ST_Intersects` con capas Admin0/Admin1 en PostGIS.
- Guardar `NULL` semantico en BD y renderizar `N/A` solo en capa de presentacion.
- Agregar reporte automatico de calidad por corrida ETL.
