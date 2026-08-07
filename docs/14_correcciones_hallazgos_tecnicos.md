# Correcciones al documento "Hallazgos Tecnicos y Retos de Integracion"

Este documento resume correcciones detectadas al contrastar el contenido del borrador con el estado actual del codigo y el snapshot operativo de base de datos.

> Convencion usada: cada cambio propuesto inicia con la etiqueta **[Correccion]**.

## 1) Seccion 9.3 (calidad geoespacial MRDS)

**Texto actual (desactualizado):**

- `mrds_location:- filas sin country_id: 202`

**[Correccion] Sustituir por:**

- `mrds_location: filas sin country_id: 4 (snapshot operativo actual validado en BD local)`

**Evidencia tecnica (consulta usada):**

```sql
SELECT COUNT(*) FROM mrds_location WHERE country_id IS NULL;
-- resultado: 4
```

## 2) Seccion 9.6 (volumen extremo por pais y rendimiento)

**Texto actual (desalineado):**

- Se menciona que Australia es el segundo pais con mayor volumen y que ronda ~33 mil.

**[Correccion] Sustituir por:**

- `En el snapshot operativo actual, Estados Unidos concentra el mayor volumen (~263k).`
- `El segundo volumen ya no corresponde a Australia; actualmente aparecen por encima Mexico, Chile, Canada y Peru.`

**[Correccion] Tabla recomendada para dejar trazabilidad numerica actual:**

- `USA: 263433`
- `MEX: 5285`
- `CHL: 4081`
- `CAN: 3162`
- `PER: 3055`
- `AUS: 1195`

**Evidencia tecnica (consulta usada):**

```sql
SELECT c.iso3, c.country_name, COUNT(*) AS n
FROM mrds_location l
JOIN dim_country c ON c.country_id = l.country_id
GROUP BY c.iso3, c.country_name
ORDER BY n DESC
LIMIT 6;
```

## 3) Seccion 9.1/9.x (descripcion de arquitectura backend)

**Texto actual (parcialmente viejo):**

- Se referencia backend como si estuviera centrado en `web/app.py`.

**[Correccion] Ajustar redaccion a estado actual:**

- `El backend mantiene FastAPI con arquitectura modular; web/app.py funciona como entrypoint/composicion de routers, mientras la logica de negocio se distribuye en web/routers/* y web/services/*.`

Esto evita que el texto contradiga el refactor ya implementado.

## 4) Seccion 9.4 (cobertura temporal de indicadores)

**Validacion:** esta seccion esta correcta con el snapshot actual.

**[Correccion] Mantener sin cambios** (solo opcionalmente agregar la etiqueta de fecha de corte del snapshot para contexto metodologico).

**Evidencia tecnica (consulta usada):**

```sql
SELECT dataset_id, MIN(year), MAX(year), COUNT(DISTINCT year)
FROM country_indicator
GROUP BY dataset_id
ORDER BY dataset_id;
```

Resultados observados:

- `cpi: 2025-2025 (1)`
- `fsi: 2023-2023 (1)`
- `worldbank_gdp: 2011-2024 (8)`
- `worldbank_population: 2024-2024 (1)`

## 5) Parrafo de cierre sugerido para tu documento principal

**[Correccion] Texto recomendado:**

`Las cifras de calidad geoespacial y distribucion por pais reportadas en este capitulo corresponden al snapshot operativo vigente del entorno local al momento de la revision tecnica. Debido a la naturaleza incremental del ETL, estos valores pueden variar entre corridas, por lo que se recomienda conservar las consultas de validacion junto a la narrativa del hallazgo.`

