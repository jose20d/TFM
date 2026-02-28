# Modelo Conceptual

## 1. Contexto y dominio del problema

El modelo conceptual del presente proyecto responde a la necesidad de integrar información geológica global con indicadores socioeconómicos e institucionales bajo una arquitectura relacional coherente.

El dominio principal está constituido por los yacimientos minerales documentados en el sistema MRDS (Mineral Resources Data System), los cuales se vinculan con una dimensión territorial normalizada por país. Sobre dicha dimensión territorial se integran indicadores macroeconómicos y de gobernanza, permitiendo análisis multidimensionales.

Adicionalmente, el modelo contempla entidades orientadas al control y trazabilidad del proceso ETL, con el fin de garantizar reproducibilidad y auditoría de cargas.

---

## 2. Entidades principales

### 2.1 Dominio geológico

- **Depósito (`mrds_deposit`)**  
  Entidad central que representa un yacimiento mineral. Contiene identificador único, coordenadas geográficas y atributos descriptivos básicos.

- **Ubicación (`mrds_location`)**  
  Define la localización administrativa del depósito y establece la vinculación con el país correspondiente.

- **Entidades de detalle geológico**  
  Conjunto de tablas asociadas al depósito que modelan características específicas:
  - `mrds_commodity`
  - `mrds_rocks`
  - `mrds_ages`
  - `mrds_material`
  - `mrds_ownership`
  - `mrds_physiography`

Estas entidades reflejan relaciones uno-a-muchos respecto al depósito.

---

### 2.2 Dimensión territorial

- **País (`dim_country`)**  
  Dimensión normalizada de países utilizada como eje de integración entre el dominio geológico y los indicadores.

- **Referencias ISO (`iso_country_codes`)**  
  Tabla auxiliar de normalización que permite homogeneizar denominaciones territoriales y facilitar la integración de fuentes heterogéneas.

---

### 2.3 Indicadores y metadatos

- **Indicador por país (`country_indicator`)**  
  Representa valores anuales asociados a un país y a un dataset específico.

- **Configuración de datasets (`dataset_config`)**  
  Catálogo de fuentes, formato, periodicidad y metadatos de origen.

- **Tablas de control ETL**
  - `etl_load_log`
  - `etl_dataset_state`
  - `etl_dataset_run_log`

Estas entidades permiten la trazabilidad, auditoría y control de ejecución del proceso de carga.

---

## 3. Relaciones conceptuales

El modelo conceptual establece las siguientes relaciones fundamentales:

- Un depósito posee una única ubicación principal.
- Un depósito puede tener múltiples registros de detalle geológico.
- Un país puede contener múltiples depósitos.
- Un país puede tener múltiples indicadores en distintos años.
- Un dataset puede proveer múltiples indicadores.
- Un dataset puede registrar múltiples ejecuciones de carga.

---

## 4. Principios de diseño conceptual

El diseño conceptual se fundamenta en los siguientes principios:

- Separación clara entre dominio geológico e indicadores socioeconómicos.
- Uso del país como eje integrador entre dimensiones heterogéneas.
- Independencia entre datos sustantivos y metadatos operativos.
- Preparación para integración geoespacial futura.
- Orientación a reproducibilidad y trazabilidad del proceso ETL.

La representación gráfica del modelo se encuentra en `docs/ERD.png`, mientras que su materialización lógica y física se documenta en los apartados siguientes.