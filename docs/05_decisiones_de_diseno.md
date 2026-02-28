# Decisiones de Diseño

## 1. Elección del sistema de gestión de bases de datos

Se seleccionó PostgreSQL como sistema de gestión de bases de datos relacional debido a las siguientes razones:

- Soporte robusto de integridad referencial declarativa.
- Cumplimiento del estándar SQL.
- Estabilidad transaccional y madurez tecnológica.
- Capacidad de ejecución en entorno local sin dependencias externas.
- Compatibilidad con la extensión PostGIS para análisis geoespacial.

Dado que el objetivo del proyecto es académico y centrado en modelado relacional, PostgreSQL ofrece un equilibrio adecuado entre formalismo estructural y extensibilidad futura.

---

## 2. Uso de claves sustitutas y claves naturales

En el diseño del modelo se adoptó un enfoque mixto:

- Se preservan claves naturales cuando el dataset original provee un identificador estable (por ejemplo, `dep_id` en `mrds_deposit`).
- Se utilizan claves sustitutas (identificadores enteros autogenerados) en tablas de detalle y dimensiones.

Esta decisión permite:

- Simplificar relaciones y operaciones JOIN.
- Evitar dependencia estructural de atributos descriptivos.
- Mantener estabilidad ante cambios en datos de origen.

---

## 3. Separación entre dominio y metadatos

El modelo distingue claramente entre:

- Datos sustantivos (depósitos, ubicaciones, indicadores).
- Metadatos operativos (configuración de datasets y control ETL).

Esta separación evita mezclar lógica operativa con información analítica, mejorando claridad estructural y mantenibilidad.

---

## 4. ISO3 como eje de integración territorial

La integración entre datasets heterogéneos se realiza utilizando códigos ISO3 como referencia común.

La tabla `iso_country_codes` actúa como mecanismo de normalización semántica, permitiendo:

- Unificar denominaciones territoriales.
- Reducir ambigüedades.
- Facilitar integración con fuentes internacionales.

El país funciona como dimensión central que conecta dominio geológico e indicadores socioeconómicos.

---

## 5. Diseño normalizado frente a desnormalización

Se optó por un diseño normalizado (Tercera Forma Normal) en lugar de desnormalizar el esquema para optimización prematura.

Las razones principales son:

- Prioridad en integridad estructural.
- Claridad en relaciones entre entidades.
- Reducción de redundancia.
- Facilitar evolución futura del modelo.

Dado el volumen de datos manejado en el proyecto, la normalización no compromete rendimiento significativo.

---

## 6. Estrategia de actualización de datos

El proceso de carga implementa una estrategia de inserción con actualización automática.

Esto significa que:

- Si un registro no existe, se inserta.
- Si ya existe, se actualizan sus valores.

Esta decisión permite que el proceso ETL sea idempotente, evitando duplicidades ante ejecuciones repetidas y manteniendo consistencia estructural.

---

## 7. Preparación para analítica geoespacial

Aunque el MVP no explota completamente capacidades espaciales avanzadas, el modelo físico incluye:

- Columna geométrica (`geometry(Point, 4326)`).
- Índice espacial GIST.

Esta decisión anticipa una evolución hacia consultas por proximidad, regiones o agrupaciones espaciales sin necesidad de rediseño estructural.

---

## 8. Limitaciones actuales del diseño

El modelo presenta algunas limitaciones conscientes:

- No se ha implementado particionamiento por volumen.
- No existe desacoplamiento mediante API independiente.
- No se modela historial temporal detallado en tablas de dominio MRDS.
- No existe clave foránea explícita entre `iso_country_codes` y `dim_country`.

Estas limitaciones no afectan el objetivo académico del proyecto, pero representan posibles líneas de mejora futura.

---

## 9. Evolución prevista

En futuras extensiones podrían incorporarse:

- Vistas materializadas para optimización analítica.
- Formalización de relaciones entre tablas ISO y dimensión país.
- Incorporación de nuevos indicadores mediante catálogo estructurado.
- Activación de análisis espaciales avanzados.
- Optimización mediante particionamiento si el volumen de datos aumenta.

El diseño actual permite dichas ampliaciones sin necesidad de reestructuración profunda.