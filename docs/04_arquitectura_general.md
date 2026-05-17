# Arquitectura General del Sistema

## 1. Visión general

La arquitectura del sistema responde a un enfoque estructurado en capas, donde la integración, persistencia y consulta de datos se organizan de manera clara y desacoplada.

El flujo general puede representarse de la siguiente forma:

ETL → PostgreSQL/PostGIS → API FastAPI → Frontend Next.js

Esta separación permite mantener coherencia estructural, trazabilidad y capacidad analítica sin mezclar responsabilidades.

---

## 2. Componentes principales

### 2.1 Capa de integración (ETL)

La capa ETL (Extract, Transform, Load) es responsable de:

- Descargar datasets desde sus fuentes oficiales.
- Normalizar identificadores territoriales.
- Limpiar valores inconsistentes.
- Transformar formatos heterogéneos.
- Insertar o actualizar registros en la base de datos.

El proceso se ejecuta de forma controlada y registra su estado en tablas de auditoría (`etl_load_log`, `etl_dataset_state`, `etl_dataset_run_log`), garantizando reproducibilidad.

---

### 2.2 Capa de persistencia (PostgreSQL)

La base de datos PostgreSQL constituye el núcleo del sistema. En ella se materializan:

- El dominio geológico (MRDS).
- La dimensión territorial.
- Los indicadores socioeconómicos.
- Los metadatos operativos del ETL.

El modelo relacional implementa integridad referencial declarativa y restricciones estructurales que aseguran coherencia entre entidades.

Además, la inclusión de soporte geoespacial mediante PostGIS permite extender el sistema hacia análisis espaciales sin rediseñar el esquema.

---

### 2.3 Capa de servicios (FastAPI)

La capa de servicios expone endpoints REST para consumo del frontend analítico.

Sus responsabilidades incluyen:

- Encapsular consultas SQL para dashboards y exploración.
- Aplicar localización de payloads (`lang=es/en`) sobre datos de dominio.
- Exponer endpoints espaciales (`terrain/*`) soportados por PostGIS.
- Controlar validaciones de entrada y límites de consulta.

### 2.4 Capa de visualización (Next.js)

La capa de visualización se implementa en `frontend/` con Next.js y React.

Sus responsabilidades incluyen:

- Render de vistas analíticas (`Inicio`, `Explorar`, `Comparar`, `Analisis`, `Consultas`, `Terreno`).
- Enrutado por páginas y estado de filtros en cliente.
- Propagación del idioma activo hacia el backend.
- Visualización geoespacial (Leaflet) y gráfica (Recharts).

---

### 2.5 Capa de ejecución local con contenedores (Docker Compose)

El proyecto incorpora una orquestación local para operación reproducible:

- `postgres` (imagen `postgis/postgis`) como base de datos local.
- `backend` (FastAPI/Uvicorn) con variables `DB_*`.
- `frontend` (Next.js build/start) con proxy interno hacia backend.
- `etl` como servicio bajo profile `jobs` (ejecución puntual, no automática).

Esta capa no altera contratos funcionales de API ni lógica de negocio; solo estandariza el entorno de ejecución.

---

## 3. Separación por capas lógicas

Desde una perspectiva estructural, el sistema puede dividirse en cinco bloques:

1. **Dominio geológico**
   - `mrds_deposit`
   - Tablas de detalle
   - `mrds_location`

2. **Dimensión territorial**
   - `dim_country`
   - `iso_country_codes`

3. **Indicadores socioeconómicos**
   - `country_indicator`
   - `dataset_config`

4. **Control y trazabilidad ETL**
   - `etl_load_log`
   - `etl_dataset_state`
   - `etl_dataset_run_log`

5. **Servicio y presentación**
   - API FastAPI (`web/app.py`)
   - Frontend Next.js (`frontend/src/app/*`)
   - i18n de dominio (catálogo/traducción/materialización)

Esta separación reduce acoplamiento, mejora mantenibilidad y facilita evolución futura.

---

## 4. Principios arquitectónicos

La arquitectura se fundamenta en los siguientes principios:

- **Separación de responsabilidades:** cada capa cumple una función específica.
- **Reproducibilidad:** el proceso ETL puede ejecutarse múltiples veces sin generar inconsistencias.
- **Integridad estructural:** la base de datos aplica restricciones declarativas.
- **Extensibilidad:** el modelo permite incorporar nuevos indicadores o datasets sin rediseño completo.
- **Preparación geoespacial:** la inclusión de columnas geométricas habilita evolución hacia análisis espaciales.

---

## 5. Alcance y limitaciones arquitectónicas

La arquitectura implementada corresponde a un entorno académico y local. No incluye:

- Arquitectura distribuida.
- Microservicios desacoplados por dominio.
- Gestión avanzada de usuarios.
- Despliegue en contenedores productivos (sí existe dockerización local de desarrollo).
- Infraestructura de alta disponibilidad.

Estas decisiones responden al alcance del proyecto, centrado en integración de datos, analítica y visualización operativa más que en despliegue enterprise.