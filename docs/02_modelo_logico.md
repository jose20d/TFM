# Modelo Lógico

## 1. Transformación del modelo conceptual

El modelo lógico constituye la traducción formal del modelo conceptual a un esquema relacional compuesto por tablas, claves primarias y claves foráneas. Su objetivo es materializar las entidades y relaciones definidas conceptualmente bajo una estructura que garantice integridad referencial y coherencia estructural.

La entidad central del dominio es `mrds_deposit`, a partir de la cual se articulan las tablas de detalle geológico. La dimensión territorial se implementa mediante `dim_country`, que actúa como eje de integración entre el dominio geológico y los indicadores socioeconómicos almacenados en `country_indicator`.

Adicionalmente, el esquema incluye tablas orientadas a la gestión y control del proceso ETL.

---

## 2. Dominio geológico

### 2.1 Tabla principal

- **`mrds_deposit`**
  - Clave primaria: `dep_id`
  - Representa el identificador único del yacimiento mineral.
  - Contiene atributos descriptivos y coordenadas geográficas.
  - Relación uno-a-uno con `mrds_location`.
  - Relación uno-a-muchos con las tablas de detalle.

### 2.2 Ubicación

- **`mrds_location`**
  - Clave primaria: `dep_id`
  - Clave foránea: `dep_id` → `mrds_deposit(dep_id)`
  - Clave foránea: `country_id` → `dim_country(country_id)`
  - Relación:
    - Un depósito tiene una única ubicación.
    - Un país puede tener múltiples ubicaciones asociadas.

### 2.3 Tablas de detalle geológico

Cada una de las siguientes tablas presenta una relación uno-a-muchos con `mrds_deposit`:

- `mrds_commodity`
- `mrds_material`
- `mrds_ownership`
- `mrds_physiography`
- `mrds_ages`
- `mrds_rocks`

Características comunes:

- Clave primaria propia (identificador sustituto).
- Clave foránea `dep_id` → `mrds_deposit(dep_id)`.
- Modelan atributos específicos del depósito sin introducir redundancia en la tabla principal.

---

## 3. Dimensión territorial e indicadores

### 3.1 Dimensión país

- **`dim_country`**
  - Clave primaria: `country_id`
  - Restricción de unicidad sobre `country_norm`.
  - Relación uno-a-muchos con:
    - `mrds_location`
    - `country_indicator`

### 3.2 Indicadores

- **`country_indicator`**
  - Clave primaria: `indicator_id`
  - Clave foránea: `country_id` → `dim_country(country_id)`
  - Clave foránea: `dataset_id` → `dataset_config(dataset_id)`
  - Restricción de unicidad compuesta:
    (`country_id`, `dataset_id`, `indicator_code`, `year`)

Esta restricción garantiza que no existan registros duplicados para un mismo país, indicador y año.

### 3.3 Normalización ISO

- **`iso_country_codes`**
  - Clave primaria: `iso_id`
  - Restricción de unicidad sobre `iso3`
  - Funciona como tabla de referencia para procesos de normalización territorial.

Actualmente no se define clave foránea explícita hacia `dim_country`, lo cual constituye una posible mejora futura.

---

## 4. Metadatos y control del proceso ETL

### 4.1 Configuración de datasets

- **`dataset_config`**
  - Clave primaria: `dataset_id`
  - Relación uno-a-muchos con:
    - `country_indicator`
    - `etl_load_log`

### 4.2 Registro de cargas

- **`etl_load_log`**
  - Clave primaria: `load_id`
  - Clave foránea: `dataset_id` → `dataset_config(dataset_id)`
  - Permite registrar estado y fecha de ejecución.

- **`etl_dataset_state`**
  - Clave primaria: `dataset_id`
  - Mantiene el estado actual del dataset.

- **`etl_dataset_run_log`**
  - Clave primaria: `id`
  - Tabla orientada a auditoría de ejecuciones.

---

## 5. Normalización

El modelo relacional cumple con los principios de Tercera Forma Normal (3FN):

- Cada tabla representa una única entidad o relación.
- Los atributos son atómicos.
- Los atributos no clave dependen exclusivamente de la clave primaria.
- No existen dependencias transitivas entre atributos no clave.

La separación entre `mrds_deposit` y sus tablas de detalle evita redundancia estructural y facilita la extensibilidad del esquema.

La separación entre `dim_country` y `country_indicator` permite almacenar múltiples indicadores por país y año sin duplicar información territorial.

---

## 6. Separación por capas lógicas

El modelo lógico puede interpretarse en cuatro bloques estructurales:

1. **Dominio geológico**
   - `mrds_deposit`
   - Tablas de detalle
   - `mrds_location`

2. **Dimensión territorial**
   - `dim_country`
   - `iso_country_codes`

3. **Indicadores**
   - `country_indicator`
   - `dataset_config`

4. **Control ETL**
   - `etl_load_log`
   - `etl_dataset_state`
   - `etl_dataset_run_log`

Esta separación favorece claridad estructural, mantenibilidad y coherencia analítica.