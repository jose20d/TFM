# Hallazgos y obstaculos del codigo

Este documento resume hallazgos tecnicos y obstaculos identificados al revisar el estado actual del codigo del proyecto.

## 1) Alcance de la revision

- Pipeline ETL en Python (`main.py`, `scripts/download_datasets.py`, `scripts/load_to_db.py`, `src/`).
- API backend con FastAPI (`web/app.py`).
- Frontend con Next.js (`frontend/src/app/`).

## 2) Hallazgos principales

### 2.1 Arquitectura y datos

- **Pipeline ETL idempotente y con trazabilidad**: se usa hash por dataset (`etl_dataset_state`) y log de ejecuciones (`etl_dataset_run_log`) para evitar recargas innecesarias y dejar auditoria.
- **Normalizacion de paises robusta**: existe canonicalizacion por ISO3 y merge de duplicados en `dim_country`, lo que reduce inconsistencias entre fuentes heterogeneas.
- **Capa geo con reconciliacion incremental**: para `mrds_location` se combina carga primaria (`Location.txt`) con inferencia por coordenadas (`reverse_geocoder`) en varias fases.
- **Backend orientado a consulta analitica**: los endpoints de `web/app.py` ya exponen KPIs, comparativas y exploracion con SQL orientado a dashboard.

### 2.2 Producto y UX tecnica

- **Frontend bien separado por casos de uso**: inicio, exploracion y comparacion tienen paginas dedicadas y un proxy interno para consumir backend.
- **Visualizacion geoespacial y comparativa ya integrada**: Leaflet (mapa) y Recharts (barras/radiales) cubren el caso analitico principal.
- **Fallbacks de resiliencia en cliente**: manejo de estados vacios y errores de red en vistas clave.

## 3) Obstaculos detectados

### 3.1 Deuda tecnica y mantenibilidad

- **Alta concentracion de logica en un solo archivo ETL**: `scripts/load_to_db.py` centraliza muchas responsabilidades (parseo, limpieza, carga, reconciliacion, auditoria), lo que dificulta testing, onboarding y cambios seguros.
- **Duplicacion de patrones de carga**: bloques similares en `load_worldbank`, `load_fsi` y `load_cpi` para construir payload y hacer upsert en `country_indicator`.
- **Acoplamiento fuerte SQL-codigo**: gran volumen de SQL embebido en strings dentro de funciones de aplicacion.

### 3.2 Calidad y operacion

- **No hay suite de tests automatizados versionada**: no se encontraron archivos de test, lo que aumenta riesgo de regresiones en ETL y API.
- **Dependencia operacional de entorno**: la ejecucion depende de variables `DB_*` y de PostGIS habilitado por admin; sin bootstrap automatizado completo.
- **Dependencia de estructura de datasets externos**: aunque hay heuristicas robustas (cabeceras dinamicas, strict OOXML), cambios grandes en formatos pueden romper carga.
- **Inconsistencia de parseo/formato numerico en frontend**: se detecto que distintas vistas aplicaban reglas diferentes para representar los mismos indicadores (ej. PIB absoluto en `Inicio` vs PIB en `USD B` en `Comparar`, y miles sin separador en algunos listados). El backend expone valores crudos; la divergencia ocurria en la capa de presentacion por funciones de parseo/formateo no unificadas.
- **Distribucion altamente sesgada en analisis global**: al graficar indicadores por pais (PIB/FSI vs depositos), la presencia de outliers y varios ordenes de magnitud comprimio la mayor parte de puntos cerca del origen. Se requirio escala logaritmica en ejes para mejorar legibilidad y poder identificar patrones sin distorsion visual extrema.
- **Outliers geograficos en visualizacion por pais**: en `Explorar`, al filtrar por un pais (ej. Australia) se observan puntos aislados fuera del territorio esperado. Esto sugiere ruido de coordenadas o asignacion pais-coordenada no siempre consistente en una fraccion de registros MRDS, y puede sesgar interpretacion territorial si no se controla.
- **Spanglish estructural en valores de negocio**: los datasets fuente llegan mayoritariamente en ingles (`Gold`, `Silver`, nombres de paises, etc.) mientras la UX objetivo es bilingue (100% espanol / 100% ingles). Sin una capa i18n de datos, la interfaz mezcla idiomas y degrada consistencia semantica.

### 3.3 Rendimiento y escalabilidad

- **ETL de gran volumen ejecutado en un solo proceso**: el pipeline completo puede volverse costoso en tiempo/memoria al crecer datos.
- **Consultas de mapa con limites altos**: endpoints con limite hasta 10k puntos requieren control de paginacion/cluster si aumenta uso concurrente.
- **Caso extremo USA (volumen masivo de depositos)**: en exploracion, USA concentra un volumen muy superior al resto (~263k depositos georreferenciados). Intentar cargar ese universo en un solo ciclo (consulta + JSON + render) degrada severamente backend y frontend, y puede percibirse como "falla" por parte del usuario final.

## 4) Riesgos asociados

- Regresiones silenciosas en transformaciones por falta de tests.
- Aumento del tiempo de entrega por complejidad ciclomatica del ETL.
- Mayor costo de mantenimiento al incorporar nuevas fuentes de datos o indicadores.

## 5) Recomendaciones priorizadas

1. **Prioridad alta**: modularizar `scripts/load_to_db.py` por capas (`extract`, `transform`, `load`, `reconcile`, `audit`).
2. **Prioridad alta**: incorporar tests minimos de humo y regresion:
   - tests unitarios para normalizacion pais/ISO,
   - tests de parseo para FSI/CPI,
   - tests de endpoints criticos (`/health`, `/overview`, `/countries/compare`).
3. **Prioridad media**: factorizar utilidades repetidas de upsert en `country_indicator`.
4. **Prioridad media**: mover SQL critico a archivos `.sql` versionados por modulo para mejorar legibilidad.
5. **Prioridad media**: introducir reportes de calidad ETL por corrida (nulos, duplicados, outliers) y umbrales de alerta.
6. **Prioridad media**: centralizar utilidades de parseo y formato numerico en frontend (unidad unica para miles/decimales, politicas de `N/A`, y reglas por indicador como `PIB -> USD B`) para evitar regresiones visuales entre paginas.
7. **Prioridad media**: documentar y estandarizar criterios de escala en visualizaciones (lineal vs logaritmica) segun distribucion de datos, para evitar lecturas engañosas cuando existan outliers fuertes.
8. **Prioridad media**: definir una estrategia de control geoespacial para outliers (reglas de bounding box por pais, validacion con fronteras Admin0/Admin1 y bandera de calidad de coordenadas) y aplicarla antes de renderizar mapas de exploracion.
9. **Prioridad alta**: mantener una estrategia i18n hibrida para datos de dominio (diccionario canonico + materializacion bilingue en ETL) para evitar traduccion ad-hoc en frontend y preservar rendimiento en consultas.
10. **Prioridad alta**: formalizar una politica unica para distribuciones extremas: paginacion y limites por pais en vistas geoespaciales, y uso de escala logaritmica en analisis cuando existan outliers de varios ordenes de magnitud.

## 6) Fragmentos de solucion aplicados

### 6.1 Idempotencia ETL con hash + estado + log historico

Evidencia de control de cambios por dataset para evitar recargas innecesarias:

```python
hash_value = _file_hash(raw_path)
last_hash, _ = _get_dataset_state(cur, dataset_id)
if hash_value and last_hash == hash_value:
    log_no_change(dataset_id, hash_value, int((time.time() - start) * 1000))
    conn.commit()
    return

_upsert_dataset_state(cur, dataset_id, hash_value or "", True)
_insert_run_log(cur, dataset_id=dataset_id, has_changes=True, ...)
```

### 6.2 Canonicalizacion ISO3 para reducir duplicados de pais

Se normaliza por referencia ISO y luego se consolidan duplicados en `dim_country`:

```python
def _canonicalize_country_rows(rows, iso3_to_norm, iso3_to_name):
    out = []
    for name, norm, iso3 in rows:
        iso3_norm = _norm_iso3(iso3)
        if iso3_norm and iso3_norm in iso3_to_norm:
            out.append((iso3_to_name.get(iso3_norm, name), iso3_to_norm[iso3_norm], iso3_norm))
            continue
        out.append((name, norm, iso3_norm))
    return out
```

```python
def _merge_dim_country_duplicates_by_iso3(cur) -> int:
    # selecciona ISO3 con >1 fila, reasigna FK y elimina duplicados
    ...
    cur.execute("UPDATE mrds_location SET country_id = %s WHERE country_id = %s", (keep_id, dup_id))
    cur.execute("DELETE FROM dim_country WHERE country_id = %s", (dup_id,))
```

### 6.3 Reconciliacion geoespacial incremental MRDS

Se combina carga primaria de ubicacion con inferencia por coordenadas y reparacion posterior:

```python
def _infer_missing_mrds_locations(cur, iso2_to_country_id):
    if rg is None:
        print("[warn] reverse_geocoder not installed; skipping coordinate-based location inference")
        return 0
    ...
```

```python
def _run_mrds_location_reconcile(cur):
    pre_country = _repair_existing_mrds_locations(cur, iso2_map, fix_country=True, fix_state=False)
    inserted_missing = _infer_missing_mrds_locations(cur, iso2_map)
    post_repair = _repair_existing_mrds_locations(cur, iso2_map, fix_country=True, fix_state=True)
    return pre_country, inserted_missing, post_repair
```

### 6.4 Backend analitico orientado a dashboard

Ejemplo de endpoint especifico para analisis global por pais:

```python
@app.get("/api/v1/analysis/country-overview")
def api_analysis_country_overview() -> list[dict]:
    sql = """
        SELECT cb.country_name, cb.iso3, COALESCE(d.total_deposits, 0) AS total_deposits,
               (SELECT value FROM latest WHERE iso3 = cb.iso3 AND indicator_code = 'NY.GDP.MKTP.CD') AS gdp,
               (SELECT value FROM latest WHERE iso3 = cb.iso3 AND indicator_code = 'CPI') AS cpi,
               (SELECT value FROM latest WHERE iso3 = cb.iso3 AND indicator_code = 'RANK') AS fsi
        FROM country_bucket cb LEFT JOIN deposits d ON d.iso3 = cb.iso3
    """
    return _fetch_all(sql)
```

### 6.5 Resiliencia en cliente (fallos de red / datos vacios)

Patron aplicado en vistas para degradacion controlada:

```javascript
fetch(queryUrl, { cache: "no-store", signal: controller.signal })
  .then((response) => {
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return response.json();
  })
  .then((data) => setRows(Array.isArray(data) ? data : []))
  .catch((err) => {
    if (err?.name === "AbortError") return;
    setRows([]);
    setError(err.message || "Error consultando datos");
  });
```

### 6.6 Regla unica de formato numerico en frontend

Se centralizo formato para miles, decimales, `N/A` y unidad de PIB (`USD B`) en comparacion:

```javascript
const NUMERIC_FORMAT = Object.freeze({
  thousandSeparator: ".",
  decimalSeparator: ",",
  nullLabel: "N/A",
  gdpUnitLabel: "USD B",
  gdpDecimals: 2,
});

function formatNumeric(value, options = {}) {
  const { decimals = null } = options;
  if (value === null || value === undefined) return NUMERIC_FORMAT.nullLabel;
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return NUMERIC_FORMAT.nullLabel;
  ...
}
```

### 6.7 Escala logaritmica para analisis global con outliers

Para visualizar mejor dispersion con varios ordenes de magnitud:

```javascript
<XAxis type="number" dataKey="gdpB" scale="log" domain={["auto", "auto"]} ... />
<YAxis type="number" dataKey="total_deposits" scale="log" domain={["auto", "auto"]} ... />
```

### 6.8 Correccion de auto-zoom por pais en mapa

Se evita bloquear zoom por filas con `iso3` nulo o distinto:

```javascript
const targetRows = rows.filter(
  (item) => String(item.iso3 || "").toUpperCase() === countryIso,
);
if (!targetRows.length) return;

const points = targetRows
  .map((item) => [Number(item.latitude), Number(item.longitude)])
  .filter(([lat, lon]) => Number.isFinite(lat) && Number.isFinite(lon));
```

### 6.9 Estrategia i18n hibrida (diccionario canonico + materializacion ETL)

Se implemento una capa de traduccion de datos en dos niveles:

1. **Diccionario canonico** (`i18n_term_catalog` + `i18n_term_translation`): fuente de verdad para terminos y traducciones por idioma.
2. **Tabla materializada de serving** (`i18n_term_materialized`): etiquetas `es/en` pre-resueltas para consulta rapida en API.

Para acelerar arranque del diccionario, se incorporo una **semilla inicial** (`database/i18n_terms_seed.csv`) creada con IA y luego validada en el proyecto como punto de partida operativo.

Fragmento ETL aplicado:

```python
def _refresh_i18n_materialized(cur) -> int:
    cur.execute("TRUNCATE TABLE i18n_term_materialized")
    cur.execute(
        """
        INSERT INTO i18n_term_materialized (
            domain, source_value_norm, source_value_original, canonical_key, label_es, label_en
        )
        SELECT c.domain,
               c.source_value_norm,
               c.source_value_original,
               c.canonical_key,
               COALESCE(es.label, c.source_value_original) AS label_es,
               COALESCE(en.label, c.source_value_original) AS label_en
        FROM i18n_term_catalog c
        LEFT JOIN i18n_term_translation es
               ON es.canonical_key = c.canonical_key AND es.lang = 'es'
        LEFT JOIN i18n_term_translation en
               ON en.canonical_key = c.canonical_key AND en.lang = 'en'
        """
    )
    return cur.rowcount
```

Fragmento API aplicado (entrada/salida bilingue):

```python
selected_mineral = (_resolve_source_term("mineral", mineral) or "").strip()
...
return _localize_payload(payload, lang)
```

Resultado: se evita el spanglish visible en frontend, se permite filtrar por terminos en ambos idiomas y se mantiene costo bajo por consulta gracias a materializacion previa.

### 6.10 Cobertura de traduccion ampliada en base de datos

Ademas de `country` y `mineral`, el ETL ahora extrae terminos para dominios adicionales que tambien aparecen en consultas o en futuras vistas:

- `deposit_status` (`mrds_deposit.dev_stat`)
- `region` / `state_province` (`mrds_location.region`, `mrds_location.state_prov`)
- `ownership_type` (`mrds_ownership.owner_tp`)
- `material` / `ore_gangue` (`mrds_material.material`, `mrds_material.ore_gangue`)
- `rock_class` + ordenes litologicos (`mrds_rocks.*`)
- `age_type` (`mrds_ages.age_tp`)
- `phys_division` / `phys_province` / `phys_section` / `phys_detail` (`mrds_physiography.*`)

Con esto, el proceso de traduccion queda preparado para cubrir la mayor parte de texto de dominio de la BD sin depender de hardcodes en frontend.

### 6.11 Caso USA: volumen extremo y decision tecnica de visualizacion

Se incorporo este hallazgo porque impacta directamente percepcion de calidad del producto:

- **Hecho observado**: USA tiene un volumen excepcionalmente alto de depositos georreferenciados (~263k), muy por encima de la mayoria de paises.
- **Impacto tecnico**: devolver/renderizar todo en una sola carga provoca latencia alta, payloads pesados y bloqueo de interfaz.
- **Decision aplicada en Exploracion**: no renderizar mundo sin pais, paginar resultados y sincronizar mapa-lista por pagina (mismo subconjunto visible).
- **Decision aplicada en Analisis**: mantener ejes logaritmicos en graficos globales para evitar compresion visual de paises no outlier.

Fragmentos representativos:

```python
# Exploracion: sin pais, sin puntos
iso3 = (country_iso3 or "").strip().upper()
if not iso3:
    return []
```

```javascript
// Analisis global: escala logaritmica para outliers
<XAxis type="number" dataKey="gdpB" scale="log" domain={["auto", "auto"]} />
<YAxis type="number" dataKey="total_deposits" scale="log" domain={["auto", "auto"]} />
```

### 6.12 Unificacion i18n visible y robustez de layout en Consultas/Analisis

Durante la estabilizacion de UI bilingue se detectaron dos frentes:

- mezcla parcial de idioma en textos visibles por cadenas hardcodeadas;
- solapamiento de controles en `Consultas` por layout con offsets manuales (`translateX`) sensible a longitud de labels.

Correcciones aplicadas:

1. **Unificacion de textos visibles** en `Consultas` y `Analisis` para que todos respondan al idioma activo (`es/en`).
2. **Refactor de grillas de formulario en `Consultas`**:
   - eliminacion de offsets manuales por campo,
   - grillas fluidas con `minmax(...)`,
   - breakpoints coherentes para desktop/tablet/mobile.
3. **Selectores de minerales con clave estable** (`value` fuente + `label` traducida) para evitar colisiones visuales por etiquetas duplicadas.

Resultado: desaparicion de mezcla de idioma en UI visible y eliminacion de superposiciones de inputs/selects en modos `deposits`, `combined` y `spatial`.

## 7) Conclusion ejecutiva

El codigo tiene una base funcional solida para ETL + analitica web, con buenas decisiones de trazabilidad e integracion geoespacial. El principal obstaculo no es funcional sino de mantenibilidad: la concentracion de complejidad en el ETL y la ausencia de pruebas automatizadas. Atacando esos dos frentes, el riesgo operativo baja de forma significativa.

## 8) Caracterizacion exacta de datasets (snapshot operativo)

Esta seccion caracteriza los datasets usados en el TFM con metricas observadas en la base de datos local del proyecto (snapshot tecnico del entorno actual).

### 8.1 Inventario de datasets y periodicidad

- `mrds_csv` (USGS MRDS): formato `zip`, periodicidad `irregular`.
- `worldbank_gdp` (World Bank GDP): formato `json`, periodicidad `annual`.
- `worldbank_population` (World Bank Population): formato `json`, periodicidad `annual`.
- `fsi` (Fragile States Index): formato `xlsx`, periodicidad `annual`.
- `cpi` (Transparency International CPI): formato `xlsx`, periodicidad `annual`.
- `iso_country_codes` (ISO 3166-1): formato `csv`, periodicidad `annual`.

### 8.2 Estructura de datos (tablas, columnas, claves y tipos)

- **Tablas core del modelo analitico**: 11 tablas principales:
  - `iso_country_codes` (7 columnas)
  - `dim_country` (5 columnas)
  - `country_indicator` (7 columnas)
  - `mrds_deposit` (8 columnas)
  - `mrds_location` (6 columnas)
  - `mrds_commodity` (8 columnas)
  - `mrds_material` (6 columnas)
  - `mrds_ownership` (5 columnas)
  - `mrds_physiography` (7 columnas)
  - `mrds_ages` (5 columnas)
  - `mrds_rocks` (8 columnas)
- **Claves primarias (PK) relevantes**:
  - `mrds_deposit.dep_id`
  - `mrds_location.dep_id`
  - `dim_country.country_id`
  - `iso_country_codes.iso_id`
  - `country_indicator.indicator_id`
- **Claves foraneas (FK) relevantes**:
  - `mrds_location.dep_id -> mrds_deposit.dep_id`
  - `mrds_location.country_id -> dim_country.country_id`
  - tablas MRDS 1-N (`mrds_commodity`, `mrds_material`, `mrds_ownership`, `mrds_physiography`, `mrds_ages`, `mrds_rocks`) con `dep_id -> mrds_deposit.dep_id`
  - `country_indicator.country_id -> dim_country.country_id`
  - `country_indicator.dataset_id -> dataset_config.dataset_id`
- **Restricciones de unicidad clave**:
  - `dim_country.country_norm` (UNIQUE)
  - `iso_country_codes.iso3` (UNIQUE)
  - `country_indicator(country_id, dataset_id, indicator_code, year)` (UNIQUE)
- **Tipos de datos predominantes**:
  - Identificadores y llaves: `SERIAL`, `BIGINT`, `INTEGER`, `CHAR(2)`, `CHAR(3)`, `TEXT`
  - Variables numericas: `NUMERIC`, `NUMERIC(9,6)`
  - Geoespacial: `geometry(Point, 4326)`
  - Trazabilidad temporal: `TIMESTAMP`

### 8.3 Granularidad por dataset

- `mrds_csv`: granularidad primaria a nivel **deposito mineral** (`dep_id`), con tablas hijas de detalle 1-N por deposito (commodities, materiales, propiedad, geologia, etc.).
- `iso_country_codes`: granularidad a nivel **pais ISO**.
- `worldbank_gdp`, `worldbank_population`, `fsi`, `cpi`: granularidad **pais-anio-indicador** en `country_indicator`.

### 8.4 Volumen exacto (registros y bytes)

- `iso_country_codes`: 249 filas, 122880 bytes.
- `dim_country`: 218 filas, 139264 bytes.
- `country_indicator` (todos los indicadores): 775 filas, 294912 bytes.
- `mrds_deposit`: 304388 filas, 61292544 bytes.
- `mrds_location`: 304378 filas, 51855360 bytes.
- `mrds_commodity`: 488829 filas, 69632000 bytes.
- `mrds_material`: 261161 filas, 29597696 bytes.
- `mrds_ownership`: 106131 filas, 14802944 bytes.
- `mrds_physiography`: 241397 filas, 41205760 bytes.
- `mrds_ages`: 123697 filas, 15933440 bytes.
- `mrds_rocks`: 120717 filas, 19750912 bytes.

Detalle en `country_indicator` por dataset:

- `cpi`: 181 filas.
- `fsi`: 167 filas.
- `worldbank_gdp`: 212 filas.
- `worldbank_population`: 215 filas.

Nota metodologica: los bytes para `cpi`, `fsi`, `worldbank_gdp` y `worldbank_population` no se almacenan en tablas fisicas separadas; comparten `country_indicator`, por lo que el peso exacto a nivel fisico se reporta a nivel de tabla compartida.

### 8.5 Calidad de datos observada (duplicados y faltantes)

- `country_indicator`:
  - duplicados por clave compuesta (`country_id`, `dataset_id`, `indicator_code`, `year`): 0.
  - valores `value` nulos: 0.
- `mrds_deposit`:
  - filas con latitud/longitud faltante: 0.
- `mrds_location`:
  - filas sin `country_id`: 202 (riesgo para analisis territorial por pais).
- `dim_country`:
  - filas sin `iso3`: 2.
  - grupos duplicados por `iso3`: 0.
- `mrds_commodity`:
  - filas con `commod` nulo/vacio: 0.

### 8.6 Cobertura temporal (indicadores periodicos)

- `cpi`: min/max anio 2025, 1 anio distinto.
- `fsi`: min/max anio 2023, 1 anio distinto.
- `worldbank_gdp`: anios entre 2011 y 2024, 8 anios distintos cargados en el snapshot.
- `worldbank_population`: min/max anio 2024, 1 anio distinto.

### 8.7 Atributos clave para el desarrollo del TFM

- **Integracion pais-fuente**: `iso3`, `country_norm`, `country_id`.
- **Analitica comparativa y global**: `value`, `indicator_code`, `year`, `dataset_id`.
- **Analitica geoespacial**: `dep_id`, `latitude`, `longitude`, `geom`, `country_id`.
- **Escalamiento y trazabilidad ETL**: `file_hash`, `last_hash`, `load_success`, `rows_inserted`, `rows_updated`, `duration_ms`.

### 8.8 Retos tecnicos que impone el dataset

- **Heterogeneidad de fuentes** (`zip/json/xlsx/csv`) con estructuras y cobertura no uniformes.
- **Asimetria temporal** entre indicadores (series incompletas o de un solo anio segun fuente cargada).
- **Volumen alto en MRDS** con varias tablas 1-N, que exige estrategia de consultas, indices y agregaciones para front analitico.
- **Calidad geoespacial no perfecta** (casos sin `country_id` en `mrds_location`) que puede afectar filtros por pais y lectura visual en mapa.
- **Riesgo de drift de formato externo** (cambios de columnas/nombres en fuentes publicas) que obliga a mantener validaciones y fallbacks de parseo.

### 8.9 Refactor tecnico FastAPI a arquitectura modular

- Se completo la migracion de backend FastAPI desde un archivo monolitico hacia una estructura modular por capas:
  - `web/app.py` como punto de entrada y composicion de routers.
  - `web/routers/*` para definicion de rutas por dominio funcional.
  - `web/services/*` para logica de negocio y consultas.
  - `web/services/common/*` para funciones compartidas (query DB, i18n y limites de exploracion).
  - `web/db.py` y `web/utils/*` para utilidades transversales.
- Se mantuvieron contratos de API y comportamiento funcional (rutas, parametros y payloads) para evitar regresiones en frontend.
- Como cierre del refactor, se elimino la implementacion legacy (`web/services/api_impl.py`) al quedar sin referencias activas.
- Beneficio principal: menor acoplamiento, mejor mantenibilidad y mayor capacidad para evolucionar endpoints por dominio sin afectar el resto del sistema.

### 8.10 Hallazgos recientes: Docker local + ETL + catalogo de paises

#### 8.10.1 Docker local consolidado sin alterar contratos

**Justificacion tecnica**

- Se formalizo una orquestacion local unica con `docker-compose.yml`.
- El ETL se separo como job puntual (`profile: jobs`) para evitar ejecucion implicita al levantar frontend/backend.

**Muestra de codigo (`docker-compose.yml`)**

```yaml
  etl:
    build:
      context: .
      dockerfile: Dockerfile
    env_file:
      - .env
    depends_on:
      postgres:
        condition: service_healthy
    command: python -m etl.run_etl
    profiles:
      - jobs
```

#### 8.10.2 Error ISO2 (`CHAR(2)`) en `iso_country_codes`

**Justificacion tecnica**

- El fallo se originaba cuando `iso2` llegaba con tipos mixtos (por ejemplo `NaN` como float).
- Se reforzo la validacion previa y se blindo la insercion SQL con cast explicito a `text`.

**Muestra de codigo (`scripts/load_to_db.py`)**

```python
def _norm_iso2(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip().upper()
    if len(text) != 2:
        return None
    return text
```

```python
execute_values(cur, sql, rows, template="(%s, %s, NULLIF(LEFT(%s::text, 2), ''), %s, %s)")
```

#### 8.10.3 `country_indicator` en cero tras corrida ETL

**Justificacion tecnica**

- Se detecto escenario de hash sin cambios pero tabla destino vacia.
- Se aplico recarga forzada para indicadores cuando `country_indicator` del dataset esta en cero.
- Se mejoro observabilidad con trazas `[load]`, `[ok]` y sanity final por dataset.

**Muestra de codigo (`scripts/load_to_db.py`)**

```python
if hash_value and last_hash == hash_value:
    if dataset_id in {"worldbank_gdp", "worldbank_population", "cpi", "fsi"}:
        cur.execute("SELECT COUNT(*) FROM country_indicator WHERE dataset_id = %s", (dataset_id,))
        existing = int(cur.fetchone()[0] or 0)
        if existing == 0:
            print(f"[load] {dataset_id} (hash unchanged, country_indicator empty -> forced reload)")
```

```python
print(f"[ok] country_indicator inserted/updated: {len(payload)} ({dataset_label})")
```

#### 8.10.4 Build Next.js en contenedor (prerender)

**Justificacion tecnica**

- Se registraron fallos de prerender por uso de `useSearchParams` sin boundary de `Suspense`.
- Se resolvio encapsulando paginas cliente con `Suspense` para mantener build estable en Docker.

**Muestra de codigo (`frontend/src/app/comparar/page.js`)**

```javascript
export default function CompararPage() {
  return (
    <Suspense fallback={null}>
      <CompareClient />
    </Suspense>
  );
}
```

#### 8.10.5 Antartica en selectores de pais

**Justificacion tecnica**

- La opcion aparecia en UI y provocaba flujo de consulta no operativo.
- Se aplico exclusion explicita en servicio de catalogo antes del retorno al frontend.

**Muestra de codigo (`web/services/overview_service.py`)**

```python
EXCLUDED_COUNTRY_KEYS = {"antartica", "antarctica", "antartida", "antarctida"}

localized = [
    item
    for item in localized
    if sort_key_localized(item.get("country_name")) not in EXCLUDED_COUNTRY_KEYS
]
```
