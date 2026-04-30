# Backlog sugerido Jira (TFM)

Documento de referencia con historias/tareas sugeridas para los epics del TFM.

## SCRUM-1 Fuentes y Obtencion de Datos

### US-1.1 Validar actualizacion automatica de fuentes
- Como analista, quiero detectar cambios de fuente por hash para evitar recargas innecesarias.
- Tareas:
  - Probar datasets actualizados/no actualizados.
  - Registrar decision de carga/skip en logs.
  - Emitir reporte de datasets omitidos.
- Prioridad: Media

### US-1.2 Control de calidad de extraccion
- Como equipo de datos, quiero un reporte de calidad por descarga para detectar archivos corruptos o incompletos.
- Tareas:
  - Checks de tamano minimo.
  - Parseo basico por tipo de archivo.
  - Conteo de filas y estado por dataset.
- Prioridad: Alta

## SCRUM-2 Gestion de Bases de Datos

### US-2.1 Reconciliacion de ubicacion MRDS
- Como equipo tecnico, quiero completar `mrds_location` por coordenadas cuando falte location.
- Tareas:
  - Backfill de `country_id`.
  - Insercion de faltantes.
  - Reparacion de `state_prov` en `NULL/N/A`.
- Prioridad: Alta

### US-2.2 Informe de integridad relacional
- Como DBA, quiero un reporte SQL de integridad (`commodity -> deposit`, `deposit -> location`) para auditoria.
- Tareas:
  - Consultas de control.
  - Metricas antes/despues ETL.
  - Documento operativo de verificaciones.
- Prioridad: Alta

### US-2.3 Homologacion de nombres canonicos de pais
- Como desarrollador, quiero evitar duplicados por ISO3 en APIs de comparacion.
- Tareas:
  - Logica canonica por `ISO3`.
  - Ajuste del endpoint de comparar.
  - Test con casos duplicados (ej. USA).
- Prioridad: Media

## SCRUM-3 Machine Learning

### US-3.1 Dataset de features reproducible
- Como cientifico de datos, quiero un dataset entrenable versionado por fecha/hash.
- Tareas:
  - Script de construccion de features.
  - Diccionario de variables.
  - Export reproducible.
- Prioridad: Alta

### US-3.2 Baseline y metricas
- Como equipo, quiero un baseline claro para comparar modelos del TFM.
- Tareas:
  - Baseline inicial.
  - Metricas principales (MAE/F1/AUC segun objetivo).
  - Reporte de resultados.
- Prioridad: Alta

### US-3.3 Pipeline de entrenamiento reproducible
- Como analista, quiero entrenar con comando unico y semilla fija.
- Tareas:
  - Split train/val/test.
  - Configuracion de seed.
  - Guardado de modelo y metricas.
- Prioridad: Media

## SCRUM-4 Reporting and Business Intelligence

### US-4.1 Dashboard KPI ejecutivo
- Como tutor/tribunal, quiero ver KPIs clave en una sola vista.
- Tareas:
  - Definir 5-7 KPIs.
  - Filtros por pais.
  - Fecha de ultima actualizacion.
- Prioridad: Alta

### US-4.2 Reporte comparativo por pais
- Como usuario, quiero comparar paises sin duplicados ni inconsistencias.
- Tareas:
  - Tabla comparativa limpia.
  - Export CSV.
  - Validacion de nulos/inconsistencias.
- Prioridad: Media

### US-4.3 Reporte de calidad de datos
- Como equipo, quiero visualizar nulos, `N/A`, cobertura geografica y evolucion por corrida.
- Tareas:
  - Panel de calidad.
  - Indicadores de completitud.
  - Tendencias por corrida ETL.
- Prioridad: Alta

## SCRUM-5 Inteligencia Artificial

### US-5.1 Asistente de consulta en lenguaje natural (MVP)
- Como usuario, quiero consultar "top minerales por pais" en lenguaje natural.
- Tareas:
  - Intents acotados.
  - Mapeo a SQL seguro.
  - Respuestas trazables.
- Prioridad: Media

### US-5.2 Explicabilidad de resultados
- Como usuario, quiero saber de donde sale cada respuesta de IA.
- Tareas:
  - Mostrar consulta ejecutada.
  - Mostrar fuentes/tablas usadas.
  - Mostrar timestamp de datos.
- Prioridad: Alta

## SCRUM-6 Documentacion TFM y Mejora Continua

### US-6.1 Documentacion tecnica final del flujo ETL
- Como equipo, quiero cerrar documentacion tecnica lista para tribunal.
- Tareas:
  - Arquitectura.
  - Limpieza y depuracion.
  - Decisiones de diseno y riesgos.
- Prioridad: Alta

### US-6.2 Plan de pruebas y evidencia
- Como equipo, quiero evidencia reproducible de pruebas funcionales y de datos.
- Tareas:
  - Checklist de pruebas.
  - Capturas/evidencias.
  - Casos borde y resultados esperados/reales.
- Prioridad: Alta

## Recomendacion de carga

- Sprint siguiente: 6 historias (3 alta + 3 media).
- Backlog inmediato: 5 historias.
- Backlog de cierre TFM: 4 historias.
