# Reglas y casos de limpieza de datos (Punto 3)

Este documento lista los **casos explícitos de limpieza por archivo** y las reglas a aplicar antes de usar los datos para filtros e indicadores. Sirve como especificación para el script de limpieza.

Reglas generales (aplican a todos los datasets):
- Recortar espacios en campos de texto y normalizar mayúsculas/minúsculas cuando corresponda.
- Cualquier valor numérico **nulo/vacío** se convierte en `NA` (o `null` en JSON).
- Si un campo numérico contiene texto no numérico (letras donde solo deben existir números), la fila se marca como inválida y se excluye de indicadores.
- Toda eliminación debe quedar registrada en un reporte de limpieza con su motivo.

---

## worldbank_population.xlsx

**Casos / reglas**
- Eliminar filas agregadas que no son países (regiones y grupos de ingreso del Banco Mundial).
  - Ejemplos (no exhaustivo): `Africa Eastern and Southern`, `Africa Western and Central`, `Arab World`, `East Asia & Pacific`, `Europe & Central Asia`, `Latin America & Caribbean`, `Middle East & North Africa`, `North America`, `South Asia`, `Sub-Saharan Africa`, `World`, `High income`, `Upper middle income`, `Lower middle income`, `Low income`, `OECD members`, `Euro area`.
- Para cada país, **usar el último año disponible** con valor no nulo.
- Etiqueta de salida: **`Poblacion (AÑO)`**.
- Los valores deben ser numéricos y > 0. De lo contrario, `NA` y excluir de métricas.

---

## worldbank_gdp.xlsx

**Casos / reglas**
- Eliminar filas agregadas que no son países (mismo criterio que población).
- Para cada país, **usar el último año disponible** con valor no nulo.
- Etiqueta de salida: **`PIB (AÑO)`**.
- Los valores deben ser numéricos y > 0. De lo contrario, `NA` y excluir de métricas.

---

## CPI2023_Global_Results__Trends.xlsx

**Casos / reglas**
- Conservar solo países válidos en `Country / Territory`; eliminar regiones o agregados.
- Usar **`CPI score 2023`** por país.
- Valores numéricos entre **0 y 100**.
- Valores faltantes se convierten en `NA`.

---

## FSI-2023-DOWNLOAD.xlsx

**Casos / reglas**
- Conservar solo países válidos; eliminar regiones o agregados.
- Usar **`Rank`** como indicador con etiqueta **`Indice de fragilidad (Rank 2023)`**.
- `Rank` debe ser numérico (entero). Valores faltantes o no numéricos → `NA`.

---

## MRDS Location.csv

**Casos / reglas**
- `country` debe ser un país válido (usar aliases + lista ISO).
- Eliminar filas donde `country` sea una **región** o un **código regional** (ej.: `AF`, `EU`, `AS`, `OC`, `SA`, `CR`).
- Eliminar filas con `country` vacío.
- Normalizar `state_prov` eliminando caracteres especiales finales (ej.: `Roraima*` → `Roraima`).

---

## MRDS.csv

**Casos / reglas**
- `latitude` numérico en rango **[-90, 90]**.
- `longitude` numérico en rango **[-180, 180]**.
- Filas con coordenadas inválidas se excluyen de salidas georreferenciadas.

---

## Ownership.csv

**Casos / reglas**
- `pct` numérico entre **0 y 100**. De lo contrario, `NA`.
- `beg_yr`, `end_yr`, `info_yr` deben ser años de 4 dígitos en rango razonable (ej. 1800–año actual).
- Si existen `beg_yr` y `end_yr`, **`beg_yr <= end_yr`**.

---

## Ages.csv

**Casos / reglas**
- `age_young`, `age_old`, `age_young_ba`, `age_old_ba` deben ser numéricos si existen.
- Si existen edades young y old, **`age_young <= age_old`**.
- Edades negativas son inválidas.

---

## Commodity.csv

**Casos / reglas**
- Validar que `code` y campos categóricos clave sigan el formato esperado (mayúsculas o tokens estándar).
- Marcar filas donde campos numéricos tengan letras.

---

## Materials.csv

**Casos / reglas**
- `ore_gangue` debe ser **`Ore`** o **`Gangue`** (insensible a mayúsculas).
- Marcar filas donde campos numéricos tengan letras.

---

## Rocks.csv / Physiography.csv / Otras tablas MRDS

**Casos / reglas**
- `dep_id` debe existir y no estar vacío.
- Marcar filas donde campos numéricos tengan letras.
- Eliminar nombres de regiones en cualquier campo relacionado con país si aparecen.

