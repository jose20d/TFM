# Bibliotecas mas usadas y por que

Este documento lista las bibliotecas mas usadas actualmente en el proyecto y su razon de uso desde perspectiva tecnica y de producto.

## 1) Metodo de analisis

- Dependencias declaradas:
  - Python: `requirements.txt`
  - Frontend: `frontend/package.json`
- Uso real en codigo:
  - revision de imports en frontend/backend y scripts ETL.
- Nota: hay paquetes usados de forma indirecta (por ejemplo `jinja2` via FastAPI templates u `openpyxl` via `pandas.read_excel`) que pueden no aparecer con import explicito.

## 2) Bibliotecas clave por capa

## 2.1 Frontend

1. **next**
   - **Por que se usa**: framework principal del frontend.
   - **Aporte**:
     - enrutado por carpetas (`app/`),
     - rendering server/client segun pagina,
    - API route proxy (`frontend/src/app/api/v1/[...path]/route.js`).

2. **react**
   - **Por que se usa**: capa de componentes y estado en vistas interactivas.
   - **Aporte**:
     - hooks (`useState`, `useEffect`, `useMemo`) para filtros y carga de datos,
     - composicion de UI para comparacion y exploracion.

3. **leaflet + react-leaflet**
   - **Por que se usa**: visualizacion geoespacial de depositos.
   - **Aporte**:
     - render de mapa base OSM,
     - marcadores y tooltips para puntos de interes,
     - autoajuste de vista segun filtros.

4. **recharts**
   - **Por que se usa**: graficos comparativos del modulo de paises.
   - **Aporte**:
     - barras para PIB/depositos,
     - radial bars para resumen de IPC/EFI/depositos.

## 2.2 Backend y ETL (Python)

1. **fastapi**
   - **Por que se usa**: framework API principal.
   - **Aporte**:
     - endpoints REST para dashboard,
     - validacion de query params,
     - middleware CORS y respuestas HTML/API en el mismo servicio.

2. **psycopg2**
   - **Por que se usa**: acceso a PostgreSQL/PostGIS.
   - **Aporte**:
     - conexiones y cursores SQL,
     - cargas batch eficientes (`execute_values`) en ETL.

3. **pandas**
   - **Por que se usa**: transformacion tabular de datasets heterogeneos.
   - **Aporte**:
     - lectura CSV/XLSX/JSON,
     - limpieza, coercion y deduplicacion de datos en memoria.

4. **requests**
   - **Por que se usa**: descarga HTTP de fuentes externas.
   - **Aporte**:
     - streaming de archivos grandes,
     - retries con backoff para robustez de ingestion.

5. **reverse_geocoder**
   - **Por que se usa**: inferencia geografica cuando faltan datos de ubicacion.
   - **Aporte**:
     - estimacion de pais/provincia desde coordenadas para mejorar cobertura de `mrds_location`.

## 3) Bibliotecas relevantes con uso indirecto

- **openpyxl**
  - Se usa indirectamente en lectura de Excel desde `pandas.read_excel`.
  - Es clave para parsear FSI/CPI sin depender de conversion manual.

- **jinja2**
  - Se usa indirectamente a traves de `Jinja2Templates` en FastAPI.
  - Habilita render HTML server-side para la capa web tradicional.

- **uvicorn**
  - No aparece como import principal porque se ejecuta como servidor ASGI desde comando.
  - Es runtime necesario para levantar el backend FastAPI.

- **numpy**
  - Se usa como dependencia de soporte para cálculos numéricos y operaciones ETL/analíticas.
  - Contribuye a rendimiento en transformaciones tabulares junto a `pandas`.

## 4) Conclusiones

- La base tecnica esta bien alineada con el problema:
  - `pandas + psycopg2` para ETL y persistencia,
  - `fastapi` para exponer datos analiticos,
  - `next/react` para experiencia de usuario,
  - `leaflet/recharts` para visualizacion.
- El stack soporta adecuadamente:
  - localizacion bilingue en frontend y backend,
  - consultas geoespaciales con PostGIS,
  - escenarios de alto volumen mediante paginacion y limites por pais.
- No se observa sobre-ingenieria de dependencias: el stack es relativamente compacto y orientado al caso de uso del TFM.
- La mejora principal no pasa por agregar mas librerias, sino por modularizar ETL y elevar cobertura de pruebas.
