# Modelo Físico

## 1. Implementación sobre PostgreSQL

El modelo físico ha sido implementado sobre PostgreSQL como sistema de gestión de bases de datos relacional. La elección responde a su solidez en integridad referencial declarativa, estabilidad transaccional y compatibilidad con extensiones geoespaciales mediante PostGIS.

El DDL completo del esquema se encuentra en los archivos SQL del repositorio, donde se definen:

- Tablas
- Claves primarias
- Claves foráneas
- Restricciones de unicidad
- Restricciones CHECK
- Índices

La implementación física respeta la estructura definida en el modelo lógico, manteniendo coherencia entre las tres capas de diseño.

---

## 2. Tipos de datos y decisiones técnicas

La selección de tipos de datos responde a criterios de precisión, consistencia y eficiencia.

### 2.1 Identificadores

- `INTEGER` y `BIGINT` para claves primarias sustitutas.
- Uso de identificadores naturales cuando el dataset lo proporciona de forma estable (por ejemplo, `dep_id` en `mrds_deposit`).

### 2.2 Coordenadas geográficas

- `NUMERIC(9,6)` para latitud y longitud, preservando precisión decimal.
- `geometry(Point, 4326)` en la columna `geom` de `mrds_deposit`.

El sistema de referencia 4326 (WGS84) permite compatibilidad con estándares geoespaciales y herramientas SIG.

### 2.3 Atributos descriptivos

- `TEXT` para campos de descripción y códigos.
- `INTEGER` o `NUMERIC` para valores cuantitativos.
- `TIMESTAMP` en tablas de control ETL para trazabilidad temporal.

---

## 3. Restricciones declarativas

El modelo físico implementa restricciones declarativas para garantizar integridad estructural:

### 3.1 Claves primarias

Definidas en todas las tablas principales, garantizando unicidad de cada registro.

Ejemplos:

- `mrds_deposit(dep_id)`
- `dim_country(country_id)`
- `country_indicator(indicator_id)`

### 3.2 Claves foráneas

Aseguran consistencia entre entidades relacionadas:

- `mrds_location.dep_id` → `mrds_deposit(dep_id)`
- `mrds_location.country_id` → `dim_country(country_id)`
- Tablas de detalle → `mrds_deposit(dep_id)`
- `country_indicator.country_id` → `dim_country(country_id)`
- `country_indicator.dataset_id` → `dataset_config(dataset_id)`

### 3.3 Restricciones de unicidad

Se implementan restricciones `UNIQUE` para evitar duplicidad semántica:
- `dim_country(country_norm)`
- `iso_country_codes(iso3)`
- Restricción compuesta en `country_indicator`:
  (`country_id`, `dataset_id`, `indicator_code`, `year`)

Esta última impide registrar múltiples valores para un mismo país, indicador y año.

### 3.4 Restricciones CHECK

Se emplean restricciones `CHECK` en tablas de control ETL para validar estados de carga.

---

## 4. Estrategia de inserción y actualización de datos

Durante el proceso ETL se implementa una lógica de inserción con actualización automática.

En términos operativos:

- Si el registro no existe, se inserta.
- Si el registro ya existe, se actualizan los valores correspondientes.

Esta estrategia evita duplicidades y permite que el proceso de carga sea idempotente, es decir, que pueda ejecutarse múltiples veces sin generar inconsistencias.

En PostgreSQL, esta lógica se implementa mediante la cláusula:

```sql
INSERT INTO country_indicator (
    country_id,
    dataset_id,
    indicator_code,
    year,
    value
)
VALUES (...)
ON CONFLICT (country_id, dataset_id, indicator_code, year)
DO UPDATE SET
    value = EXCLUDED.value;
```

La existencia de una restricción única compuesta es condición necesaria para que esta estrategia funcione correctamente.

---

## 5. Índices y optimización

Los índices definidos responden a los patrones de consulta previstos.

### 5.1 Índices estructurales

- Índices sobre claves foráneas para optimizar operaciones JOIN.
- Índice compuesto en `country_indicator` para consultas por país, dataset, indicador y año.
- Índice por `country_id` en `mrds_location`.

### 5.2 Índices espaciales

Se define un índice GIST sobre la columna `geom` en `mrds_deposit`, lo que permite:

- Consultas por proximidad.
- Filtrado espacial.
- Extensión futura hacia análisis geográfico avanzado.

---

## 6. Preparación para PostGIS

Aunque el MVP no explota plenamente capacidades geoespaciales avanzadas, el modelo físico incorpora:

- Columna geométrica normalizada.
- Sistema de referencia EPSG:4326.
- Índice espacial.

Esto permite evolucionar hacia análisis espaciales sin rediseñar el esquema.

---

## 7. Coherencia entre modelo lógico y físico

El modelo físico respeta la estructura definida en el modelo lógico:

- Cada entidad conceptual se materializa como tabla.
- Las cardinalidades se implementan mediante claves foráneas.
- La normalización se mantiene en Tercera Forma Normal.
- No se introducen redundancias estructurales innecesarias.

El diseño resultante es consistente, extensible y alineado con los objetivos académicos del proyecto.