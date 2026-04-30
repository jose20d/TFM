# Bibliotecas mas usadas y por que

Este documento lista las bibliotecas mas usadas actualmente en el proyecto y su razon de uso desde perspectiva tecnica y de producto.

## 1) Metodo de analisis

- Dependencias declaradas:
  - Python: `requirements.txt`
  - Frontend: `frontend/package.json`
- Uso real en codigo:
  - Conteo de imports sobre archivos trackeados en git (`.py` y `.js`).
- Nota: hay paquetes usados de forma indirecta (por ejemplo `jinja2` via FastAPI templates u `openpyxl` via `pandas.read_excel`) que pueden no aparecer con import explicito.

## 2) Top bibliotecas por uso real

## 2.1 Frontend

1. **next** (5 imports, 4 archivos)
   - **Por que se usa**: framework principal del frontend.
   - **Aporte**:
     - enrutado por carpetas (`app/`),
     - rendering server/client segun pagina,
     - API route proxy (`frontend/src/app/api/backend/[...path]/route.js`).

2. **react** (2 imports, 2 archivos)
   - **Por que se usa**: capa de componentes y estado en vistas interactivas.
   - **Aporte**:
     - hooks (`useState`, `useEffect`, `useMemo`) para filtros y carga de datos,
     - composicion de UI para comparacion y exploracion.

3. **leaflet + react-leaflet** (1+1 imports, 1 archivo)
   - **Por que se usa**: visualizacion geoespacial de depositos.
   - **Aporte**:
     - render de mapa base OSM,
     - marcadores y tooltips para puntos de interes,
     - autoajuste de vista segun filtros.

4. **recharts** (1 import, 1 archivo)
   - **Por que se usa**: graficos comparativos del modulo de paises.
   - **Aporte**:
     - barras para PIB/depositos,
     - radial bars para resumen de IPC/EFI/depositos.

## 2.2 Backend y ETL (Python)

1. **fastapi** (5 imports, 1 archivo)
   - **Por que se usa**: framework API principal.
   - **Aporte**:
     - endpoints REST para dashboard,
     - validacion de query params,
     - middleware CORS y respuestas HTML/API en el mismo servicio.

2. **psycopg2** (3 imports, 2 archivos)
   - **Por que se usa**: acceso a PostgreSQL/PostGIS.
   - **Aporte**:
     - conexiones y cursores SQL,
     - cargas batch eficientes (`execute_values`) en ETL.

3. **pandas** (2 imports, 2 archivos)
   - **Por que se usa**: transformacion tabular de datasets heterogeneos.
   - **Aporte**:
     - lectura CSV/XLSX/JSON,
     - limpieza, coercion y deduplicacion de datos en memoria.

4. **requests** (2 imports, 2 archivos)
   - **Por que se usa**: descarga HTTP de fuentes externas.
   - **Aporte**:
     - streaming de archivos grandes,
     - retries con backoff para robustez de ingestion.

5. **reverse_geocoder** (1 import, 1 archivo)
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

## 4) Conclusiones

- La base tecnica esta bien alineada con el problema:
  - `pandas + psycopg2` para ETL y persistencia,
  - `fastapi` para exponer datos analiticos,
  - `next/react` para experiencia de usuario,
  - `leaflet/recharts` para visualizacion.
- No se observa sobre-ingenieria de dependencias: el stack es relativamente compacto y orientado al caso de uso del TFM.
- La mejora principal no pasa por agregar mas librerias, sino por modularizar ETL y elevar cobertura de pruebas.
