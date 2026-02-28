# Arquitectura General del Sistema

## 1. Visión general

La arquitectura del sistema responde a un enfoque estructurado en capas, donde la integración, persistencia y consulta de datos se organizan de manera clara y desacoplada.

El flujo general puede representarse de la siguiente forma:

ETL → PostgreSQL → Capa de visualización (Streamlit)

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

### 2.3 Capa de consulta y visualización

La capa de visualización, implementada mediante Streamlit, cumple una función de validación funcional del modelo.

Sus responsabilidades incluyen:

- Ejecutar consultas representativas.
- Mostrar resultados agregados por país o mineral.
- Permitir filtrado dinámico.
- Validar la integración entre dominio geológico e indicadores.

Esta capa no constituye un sistema de producción ni una arquitectura de servicios, sino un entorno exploratorio que demuestra la operatividad del modelo relacional.

---

## 3. Separación por capas lógicas

Desde una perspectiva estructural, el sistema puede dividirse en cuatro bloques:

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
- API REST desacoplada.
- Gestión avanzada de usuarios.
- Despliegue en contenedores productivos.
- Infraestructura de alta disponibilidad.

Estas decisiones responden al alcance del proyecto, centrado en modelado y persistencia más que en ingeniería de producción.