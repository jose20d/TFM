# Interaccion de PostGIS en la pagina Terreno

Este documento describe, de extremo a extremo, como interviene PostGIS en la pagina `Terreno` (subseccion `Corredor entre depositos`) y que partes de la pagina aun no dependen de analisis espacial avanzado.

## 1) Rol de PostGIS en esta pagina

En `Terreno`, PostGIS se usa para resolver el problema espacial central del corredor:

- construir una linea entre deposito A y deposito B;
- generar un buffer (corredor) con ancho configurable en km;
- detectar todos los depositos dentro del corredor;
- calcular distancias geodesicas reales;
- devolver geometrias en GeoJSON para visualizacion en frontend.

Sin PostGIS, estas operaciones se tendrian que aproximar con logica manual sobre latitud/longitud, con menor precision y mayor complejidad.

## 2) Habilitacion de capacidades espaciales en el proyecto

La inicializacion del esquema exige que la extension PostGIS este activa y declara un campo geometrico en depositos.

Referencia: `src/init_db.py`

```python
def initialize_schema() -> None:
    """
    Initialize database schema and indexes in an idempotent way.

    PostGIS is enabled by the schema script to support spatial features
    even if the first iterations only use basic geometry fields.
    """
    ...
    cur.execute("SELECT 1 FROM pg_extension WHERE extname = 'postgis'")
    if cur.fetchone() is None:
        raise RuntimeError(
            "PostGIS is installed but not enabled in this database. "
            "Connect as admin and run: CREATE EXTENSION postgis; then rerun."
        )
```

Referencia: `database/create_schema.sql`

```sql
CREATE TABLE IF NOT EXISTS mrds_deposit (
    dep_id BIGINT PRIMARY KEY,
    name TEXT,
    dev_stat TEXT,
    code_list TEXT,
    latitude NUMERIC(9,6),
    longitude NUMERIC(9,6),
    geom geometry(Point, 4326),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## 3) Endpoint espacial de Terreno

La pagina consume un endpoint dedicado:

- `GET /api/v1/terrain/corridor`
- parametros: `country_iso3`, `from_dep_id`, `to_dep_id`, `width_km`, `lang`

Referencia: `web/app.py`

```python
@app.get("/api/v1/terrain/corridor")
def api_terrain_corridor(
    country_iso3: str = Query(...),
    from_dep_id: int = Query(..., ge=1),
    to_dep_id: int = Query(..., ge=1),
    width_km: float = Query(default=2, ge=1, le=50),
    lang: str = Query(default="es"),
) -> dict:
    ...
```

Validaciones funcionales relevantes:

- ISO3 valido;
- extremos distintos (`from_dep_id != to_dep_id`);
- pais existente;
- pais con al menos 2 depositos georreferenciados;
- ambos extremos dentro del mismo pais y con coordenadas validas.

Nota de serving: el endpoint retorna payload localizado mediante `lang=es/en` para mantener consistencia de idioma entre frontend y backend.

## 4) Operaciones PostGIS usadas en el corredor

### 4.1 Construccion de eje y corredor

Referencia: `web/app.py`

```sql
WITH endpoints AS (
    SELECT
        ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography AS from_geog,
        ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography AS to_geog
),
corridor AS (
    SELECT ST_MakeLine(from_geog::geometry, to_geog::geometry)::geography AS axis_geog
    FROM endpoints
)
SELECT
    ROUND((ST_Distance(e.from_geog, e.to_geog) / 1000.0)::numeric, 3) AS distance_km,
    ST_AsGeoJSON(c.axis_geog::geometry) AS line_geojson,
    ST_AsGeoJSON(ST_Buffer(c.axis_geog, %s)::geometry) AS corridor_geojson
FROM endpoints e
CROSS JOIN corridor c
```

Funciones espaciales clave:

- `ST_MakePoint`: crea puntos A/B desde lon/lat.
- `ST_SetSRID(..., 4326)`: define referencia geografica WGS84.
- `ST_MakeLine`: crea el eje A-B.
- `ST_Buffer`: genera el poligono del corredor con ancho en metros.
- `ST_Distance`: calcula distancia real A-B.
- `ST_AsGeoJSON`: serializa geometria para frontend.

### 4.2 Seleccion de depositos dentro del corredor

Referencia: `web/app.py`

```sql
...
WHERE dc.iso3 = %s
  AND d.latitude IS NOT NULL
  AND d.longitude IS NOT NULL
  AND (
    ST_DWithin(
        ST_SetSRID(ST_MakePoint(d.longitude, d.latitude), 4326)::geography,
        c.axis_geog,
        %s
    )
    OR d.dep_id = %s
    OR d.dep_id = %s
  )
```

Puntos relevantes:

- `ST_DWithin` identifica depositos a distancia menor o igual al ancho del corredor.
- se fuerzan A y B dentro del resultado con `OR d.dep_id = ...`.
- el backend tambien calcula `distance_to_axis_km` por deposito con `ST_Distance`.

## 5) Agregacion mineralogica sobre el resultado espacial

Despues del filtro espacial, el backend:

- arma `deposits_in_corridor` con minerales por deposito;
- calcula `corridor_minerals` (frecuencia y porcentaje);
- clasifica intensidad (`high`, `medium`, `low`);
- calcula `common_endpoint_minerals` como dato adicional.

Este bloque ya no es PostGIS puro, pero depende completamente del subconjunto espacial generado por PostGIS.

## 6) Como lo consume el frontend de Terreno

La subseccion `Corredor entre depositos` dispara el endpoint y renderiza geometria espacial.

Referencia: `frontend/src/app/terreno/TerrenoClient.js`

```javascript
const qs = new URLSearchParams({
  country_iso3: countryIso,
  from_dep_id: String(selectedFromId),
  to_dep_id: String(selectedToId),
  width_km: String(widthKm),
});

const response = await fetch(`/api/backend/api/v1/terrain/corridor?${qs.toString()}`, {
  cache: "no-store",
});
```

El mapa usa la geometria devuelta por PostGIS:

```javascript
{corridorFeature && (
  <GeoJSON
    key={`corridor-${corridorGeoKey}`}
    data={corridorFeature}
    style={() => ({
      color: "#2563eb",
      weight: 1.5,
      fillColor: "#60a5fa",
      fillOpacity: 0.15,
    })}
  />
)}
{lineFeature && (
  <GeoJSON
    key={`line-${lineGeoKey}`}
    data={lineFeature}
    style={() => ({
      color: "#f97316",
      weight: 3,
      opacity: 0.95,
    })}
  />
)}
```

La ejecucion del analisis es automatica al tener A y B (y cambia con el slider):

```javascript
useEffect(() => {
  if (activeTool !== "corridor") return;
  if (!countryIso || !selectedFromId || !selectedToId) return;
  if (selectedFromId === selectedToId) return;

  const timeoutId = setTimeout(() => {
    void analyzeCorridor();
  }, 400);

  return () => clearTimeout(timeoutId);
}, [activeTool, countryIso, selectedFromId, selectedToId, widthKm, analyzeCorridor]);
```

## 7) Alcance actual dentro de toda la pagina Terreno

- `Corredor entre depositos`: implementacion real con PostGIS (productiva).
- `Zona de interes`, `Minerales frecuentes`, `Potencial exploratorio`: actualmente en modo placeholder; aun no ejecutan analisis PostGIS en backend.

## 8) Resumen ejecutivo

PostGIS interviene en el nucleo funcional de `Terreno` al convertir una seleccion visual (A/B + ancho) en analisis espacial reproducible: eje, corredor, intersecciones, distancias y geometrias renderizables. Sobre ese resultado espacial, el backend construye la lectura mineralogica (ranking e intensidad) que consume la UI.
