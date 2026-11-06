# Evidencia — Comentarios de la capa oro aplicados en el securable

- **Qué demuestra:** que la documentación del contrato de oro vive **dentro del
  securable** de Unity Catalog (comentarios de tabla y de columna), no sólo en
  `docs/`, y que es visible por `DESCRIBE TABLE EXTENDED` e `information_schema`.
- **Cómo se generó:** ejecución de `resources/sql/fase5_comentarios_oro.sql`
  (43 sentencias `COMMENT ON TABLE/COLUMN` idempotentes) vía la SQL Statement
  Execution API sobre el SQL warehouse serverless `377600528fed7c47`, seguida de
  `SELECT ... FROM oceanwatch.information_schema.tables/columns WHERE table_schema='gold'`
  y `DESCRIBE TABLE EXTENDED`.
- **Fecha:** 2026-11-05
- **Origen:** `resources/sql/fase5_comentarios_oro.sql`
- **Estado:** Verificado (ejecutado)

## 1. Alcance (qué se comentó y qué no)

| Tabla de oro | Estado previo | Acción de esta fase |
|---|---|---|
| `gold.world_port_index` | sin comentario de tabla ni de columna | COMMENT ON TABLE + 7 columnas |
| `gold.trayectorias_diarias` | sin comentario de tabla ni de columna | COMMENT ON TABLE + 11 columnas |
| `gold.presencia_puertos` | sin comentario de tabla ni de columna | COMMENT ON TABLE + 10 columnas |
| `gold.matriz_trafico` | sin comentario de tabla ni de columna | COMMENT ON TABLE + 5 columnas |
| `gold.matriz_trafico_diaria` | sólo comentario de **tabla** (Fase 7) | **solo** 5 columnas (la tabla se preserva) |
| `gold.anomalias_velocidad` | tabla **y** columnas (Fase 6) | **sin cambios** (ya documentada) |

`COMMENT ON TABLE/COLUMN` es **idempotente** y **no destructivo**: sólo actualiza
metadatos del catálogo; no recrea las tablas, no reescribe datos ni cambia el
esquema. El ejecutor (`_dbx_sql.py`) reportó las **43 sentencias con estado `OK` y
0 `FAIL`**.

## 2. Comentario de TABLA (salida real de `information_schema.tables`, `table_schema='gold'`)

```
- anomalias_velocidad
    Anomalias de velocidad SOG detectadas por el modelo @champion (ADR-005/ADR-016), 1 fila por posicion anomala. Insumo del visual anomalias del dia del dashboard (Req.4).
- matriz_trafico
    Hecho: densidad de trafico por celda espacial (redondeo lat/lon a 2 decimales, ADR-001) y HORA. 1 fila por (celda_lat, celda_lon, hora) sobre silver.ais_clean. Fuente de la vista diaria pre-agregada gold.matriz_trafico_diaria (Fase 7).
- matriz_trafico_diaria
    Densidad de trafico por celda (lat/lon 2 decimales, ADR-001) y DIA. Agregado de gold.matriz_trafico (colapsa la hora) para el mapa de intensidad del dashboard Req.4. Solo-oro pre-agregado (D2/D4, ADR-017, Fase 7).
- presencia_puertos
    Hecho: presencia de buques en puertos (1 fila por MMSI, INDEX_NO y dia). Cruce silver.ais_clean x gold.world_port_index por celda de grilla a 2 decimales (Reference Data Join, ADR-001). "En puerto" = al menos 3 posiciones en la celda-puerto ese dia (ADR-012).
- trayectorias_diarias
    Hecho: trayectoria diaria por buque (1 fila por MMSI y dia). Suma haversine (R=6371 km) entre posiciones consecutivas (LAG por MMSI,dia) sobre silver.ais_clean. El oro NO limpia: hereda las garantias de plata (ADR-012).
- world_port_index
    Dimension de referencia: World Port Index (NGA). 1 fila por puerto (clave INDEX_NO), 3.669 puertos. Cargada desde el .dbf convertido a CSV localmente con dbfread (7 campos clave) para evitar geopandas/pyshp en serverless (ADR-012).
```

Las 6 tablas de oro tienen ahora comentario de tabla (antes sólo 2: `matriz_trafico_diaria`
y `anomalias_velocidad`).

## 3. Comentario de COLUMNA (salida real de `information_schema.columns`, `table_schema='gold'`)

```
matriz_trafico         | celda_lat                | round(LAT, 2), clave de celda (ADR-001).
matriz_trafico         | celda_lon                | round(LON, 2), clave de celda (ADR-001).
matriz_trafico         | hora                     | date_trunc hour de BaseDateTime, UTC, clave temporal (ADR-004).
matriz_trafico         | n_posiciones             | Numero de posiciones AIS en la celda y hora.
matriz_trafico         | n_buques                 | Numero de buques distintos (MMSI) en la celda y hora.
matriz_trafico_diaria  | celda_lat                | round(LAT, 2), clave de celda (ADR-001).
matriz_trafico_diaria  | celda_lon                | round(LON, 2), clave de celda (ADR-001).
matriz_trafico_diaria  | dia                      | date(hora), UTC, clave temporal. Parametro :fecha del dashboard (ADR-017).
matriz_trafico_diaria  | intensidad               | SUM(n_posiciones): posiciones AIS en la celda ese dia (metrica principal del mapa).
matriz_trafico_diaria  | buques                   | SUM(n_buques): metrica secundaria. Suma conteos de buques distintos POR HORA, por lo que un buque en varias horas se cuenta mas de una vez (NO es buques unicos por dia).
presencia_puertos      | MMSI                     | Identificador del buque (clave).
presencia_puertos      | INDEX_NO                 | Puerto del WPI (clave, FK a gold.world_port_index.INDEX_NO).
presencia_puertos      | PORT_NAME                | Nombre del puerto (WPI).
presencia_puertos      | COUNTRY                  | Pais del puerto (WPI).
presencia_puertos      | dia                      | Dia, UTC. Clave (ADR-004).
presencia_puertos      | n_posiciones_en_puerto   | Numero de posiciones del buque en la celda-puerto ese dia (al menos 3, permanencia minima).
presencia_puertos      | primera_posicion         | Primera posicion en puerto ese dia.
presencia_puertos      | ultima_posicion          | Ultima posicion en puerto ese dia.
presencia_puertos      | HARBORSIZE               | Tamano del puerto (WPI).
presencia_puertos      | HARBORTYPE               | Tipo de puerto (WPI).
trayectorias_diarias   | MMSI                     | Identificador del buque (clave).
trayectorias_diarias   | dia                      | Dia de la trayectoria = to_date(BaseDateTime), UTC. Clave (ADR-004).
trayectorias_diarias   | n_posiciones             | Numero de posiciones AIS del buque ese dia.
trayectorias_diarias   | distancia_total_km       | Suma haversine de distancias entre posiciones consecutivas (km). Puede ser no fisica si el AIS salta (plata garantiza rango, no continuidad).
trayectorias_diarias   | primera_posicion         | Primer BaseDateTime del dia.
trayectorias_diarias   | ultima_posicion          | Ultimo BaseDateTime del dia.
trayectorias_diarias   | duracion_horas           | Duracion (ultima - primera) en horas, DECIMAL(27,6).
trayectorias_diarias   | lat_min                  | Bounding box: latitud minima del dia.
trayectorias_diarias   | lat_max                  | Bounding box: latitud maxima del dia.
trayectorias_diarias   | lon_min                  | Bounding box: longitud minima del dia.
trayectorias_diarias   | lon_max                  | Bounding box: longitud maxima del dia.
world_port_index       | INDEX_NO                 | Identificador NGA del puerto (clave).
world_port_index       | PORT_NAME                | Nombre del puerto.
world_port_index       | COUNTRY                  | Codigo de pais (2 letras).
world_port_index       | LATITUDE                 | Latitud del puerto (grados decimales).
world_port_index       | LONGITUDE                | Longitud del puerto (grados decimales).
world_port_index       | HARBORSIZE               | Tamano del puerto (V/S/M/L, codigo NGA).
world_port_index       | HARBORTYPE               | Tipo de puerto (codigo NGA, p. ej. CN/CB).
```

(Las columnas de `anomalias_velocidad` ya estaban comentadas desde la Fase 6 y no se
reescribieron.)

## 4. `DESCRIBE TABLE EXTENDED` (fragmentos reales)

### `gold.trayectorias_diarias`

```
MMSI               | bigint        | Identificador del buque (clave).
dia                | date          | Dia de la trayectoria = to_date(BaseDateTime), UTC. Clave (ADR-004).
n_posiciones       | bigint        | Numero de posiciones AIS del buque ese dia.
distancia_total_km | double        | Suma haversine de distancias entre posiciones consecutivas (km). Puede ser no fisica si el AIS salta (plata garantiza rango, no continuidad).
primera_posicion   | timestamp     | Primer BaseDateTime del dia.
ultima_posicion    | timestamp     | Ultimo BaseDateTime del dia.
duracion_horas     | decimal(27,6) | Duracion (ultima - primera) en horas, DECIMAL(27,6).
lat_min            | double        | Bounding box: latitud minima del dia.
lat_max            | double        | Bounding box: latitud maxima del dia.
lon_min            | double        | Bounding box: longitud minima del dia.
lon_max            | double        | Bounding box: longitud maxima del dia.
...
Statistics | 2713017 bytes, 61768 rows
Comment    | Hecho: trayectoria diaria por buque (1 fila por MMSI y dia). Suma haversine (R=6371 km) entre posiciones consecutivas (LAG por MMSI,dia) sobre silver.ais_clean. El oro NO limpia: hereda las garantias de plata (ADR-012).
```

**Confirmación clave del contrato:** `duracion_horas` es **`decimal(27,6)`** (no `double`),
lo que corrige la divergencia del contrato previo (ver `docs/data_contract_gold.md` §
`trayectorias_diarias`).

### `gold.world_port_index`

```
INDEX_NO   | int    | Identificador NGA del puerto (clave).
PORT_NAME  | string | Nombre del puerto.
COUNTRY    | string | Codigo de pais (2 letras).
LATITUDE   | double | Latitud del puerto (grados decimales).
LONGITUDE  | double | Longitud del puerto (grados decimales).
HARBORSIZE | string | Tamano del puerto (V/S/M/L, codigo NGA).
HARBORTYPE | string | Tipo de puerto (codigo NGA, p. ej. CN/CB).
...
Statistics | 58908 bytes, 3669 rows
Comment    | Dimension de referencia: World Port Index (NGA). 1 fila por puerto (clave INDEX_NO), 3.669 puertos. Cargada desde el .dbf convertido a CSV localmente con dbfread (7 campos clave) para evitar geopandas/pyshp en serverless (ADR-012).
```

## 5. Idempotencia y reversibilidad

- **Idempotente:** reejecutar `fase5_comentarios_oro.sql` sobrescribe cada comentario con
  el mismo texto, sin tocar datos ni esquema. Verificado al reaplicar el script tras un
  primer intento parcial (las 43 sentencias quedaron `OK`).
- **Reversible:** `COMMENT ON TABLE/COLUMN ... IS NULL` elimina cualquier comentario; la
  operación no afecta a las filas.
- **Síntoma→causa→solución (callejón real):** el primer intento dejó varias sentencias en
  `FAIL` con `PARSE_SYNTAX_ERROR`; la causa fue que el ejecutor por lotes divide el `.sql`
  por `;` y algunos textos de `COMMENT` contenían `;` internos (se partían a la mitad). La
  solución fue reescribir esos textos sin `;`. Tras ello, 43/43 `OK`.
