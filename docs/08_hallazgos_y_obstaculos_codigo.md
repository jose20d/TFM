# Hallazgos y obstaculos del codigo

Este documento resume hallazgos tecnicos y obstaculos identificados al revisar el estado actual del codigo del proyecto.

## 1) Alcance de la revision

- Pipeline ETL en Python (`main.py`, `scripts/download_datasets.py`, `scripts/load_to_db.py`, `src/`).
- API backend con FastAPI (`web/app.py`).
- Frontend con Next.js (`frontend/src/app/`).

## 2) Hallazgos principales

### 2.1 Arquitectura y datos

- **Pipeline ETL idempotente y con trazabilidad**: se usa hash por dataset (`etl_dataset_state`) y log de ejecuciones (`etl_dataset_run_log`) para evitar recargas innecesarias y dejar auditoria.
- **Normalizacion de paises robusta**: existe canonicalizacion por ISO3 y merge de duplicados en `dim_country`, lo que reduce inconsistencias entre fuentes heterogeneas.
- **Capa geo con reconciliacion incremental**: para `mrds_location` se combina carga primaria (`Location.txt`) con inferencia por coordenadas (`reverse_geocoder`) en varias fases.
- **Backend orientado a consulta analitica**: los endpoints de `web/app.py` ya exponen KPIs, comparativas y exploracion con SQL orientado a dashboard.

### 2.2 Producto y UX tecnica

- **Frontend bien separado por casos de uso**: inicio, exploracion y comparacion tienen paginas dedicadas y un proxy interno para consumir backend.
- **Visualizacion geoespacial y comparativa ya integrada**: Leaflet (mapa) y Recharts (barras/radiales) cubren el caso analitico principal.
- **Fallbacks de resiliencia en cliente**: manejo de estados vacios y errores de red en vistas clave.

## 3) Obstaculos detectados

### 3.1 Deuda tecnica y mantenibilidad

- **Alta concentracion de logica en un solo archivo ETL**: `scripts/load_to_db.py` centraliza muchas responsabilidades (parseo, limpieza, carga, reconciliacion, auditoria), lo que dificulta testing, onboarding y cambios seguros.
- **Duplicacion de patrones de carga**: bloques similares en `load_worldbank`, `load_fsi` y `load_cpi` para construir payload y hacer upsert en `country_indicator`.
- **Acoplamiento fuerte SQL-codigo**: gran volumen de SQL embebido en strings dentro de funciones de aplicacion.

### 3.2 Calidad y operacion

- **No hay suite de tests automatizados versionada**: no se encontraron archivos de test, lo que aumenta riesgo de regresiones en ETL y API.
- **Dependencia operacional de entorno**: la ejecucion depende de variables `DB_*` y de PostGIS habilitado por admin; sin bootstrap automatizado completo.
- **Dependencia de estructura de datasets externos**: aunque hay heuristicas robustas (cabeceras dinamicas, strict OOXML), cambios grandes en formatos pueden romper carga.

### 3.3 Rendimiento y escalabilidad

- **ETL de gran volumen ejecutado en un solo proceso**: el pipeline completo puede volverse costoso en tiempo/memoria al crecer datos.
- **Consultas de mapa con limites altos**: endpoints con limite hasta 10k puntos requieren control de paginacion/cluster si aumenta uso concurrente.

## 4) Riesgos asociados

- Regresiones silenciosas en transformaciones por falta de tests.
- Aumento del tiempo de entrega por complejidad ciclomatica del ETL.
- Mayor costo de mantenimiento al incorporar nuevas fuentes de datos o indicadores.

## 5) Recomendaciones priorizadas

1. **Prioridad alta**: modularizar `scripts/load_to_db.py` por capas (`extract`, `transform`, `load`, `reconcile`, `audit`).
2. **Prioridad alta**: incorporar tests minimos de humo y regresion:
   - tests unitarios para normalizacion pais/ISO,
   - tests de parseo para FSI/CPI,
   - tests de endpoints criticos (`/health`, `/overview`, `/countries/compare`).
3. **Prioridad media**: factorizar utilidades repetidas de upsert en `country_indicator`.
4. **Prioridad media**: mover SQL critico a archivos `.sql` versionados por modulo para mejorar legibilidad.
5. **Prioridad media**: introducir reportes de calidad ETL por corrida (nulos, duplicados, outliers) y umbrales de alerta.

## 6) Conclusion ejecutiva

El codigo tiene una base funcional solida para ETL + analitica web, con buenas decisiones de trazabilidad e integracion geoespacial. El principal obstaculo no es funcional sino de mantenibilidad: la concentracion de complejidad en el ETL y la ausencia de pruebas automatizadas. Atacando esos dos frentes, el riesgo operativo baja de forma significativa.
