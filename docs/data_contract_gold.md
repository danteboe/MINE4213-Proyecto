# Contrato de datos — capa oro

Esquema, clave, granularidad y garantías de calidad de las tablas de la capa oro
(`oceanwatch.gold`), modelo dimensional de negocio (ADR-012). Todas las tablas de
hechos se alimentan **SOLO** de `oceanwatch.silver.ais_clean` (el oro NO limpia):
heredan sus garantías de calidad (coordenadas en rango, MMSI bien formado, timestamp
válido, atributos nulificados con bandera; ver `docs/data_quality_rules.md` y ADR-009).

Estado: **Verificado (ejecutado)** en Databricks serverless el 2026-11-06
(`evidence/req02-capas-calidad/capa_oro.md`). Grilla espacial por redondeo lat/lon a
2 decimales (ADR-001, NO H3).

## Resumen de tablas

| Tabla | Granularidad | Clave | Filas reales | Origen (linaje) |
|---|---|---|---|---|
| `gold.world_port_index` | 1 fila por puerto | `INDEX_NO` | 3.669 | WPI (`.dbf` → CSV local) |
| `gold.trayectorias_diarias` | 1 fila por buque y día | `(MMSI, dia)` | 61.768 | `silver.ais_clean` |
| `gold.presencia_puertos` | 1 fila por buque, puerto y día | `(MMSI, INDEX_NO, dia)` | 9.153 | `silver.ais_clean` ⋈ `gold.world_port_index` |
| `gold.matriz_trafico` | 1 fila por celda y hora | `(celda_lat, celda_lon, hora)` | 2.319.365 | `silver.ais_clean` |

## `gold.world_port_index` (dimensión de referencia)

Carga del World Port Index de la NGA. Para evitar instalar `geopandas`/`pyshp` en
serverless (no preinstaladas, ADR-012), el `.dbf` (64.8 MB, 78 campos, 3.669 puertos)
se convirtió a CSV localmente con `dbfread` quedándose con los 7 campos clave.

| Columna | Tipo | Descripción |
|---|---|---|
| `INDEX_NO` | INT | Identificador NGA del puerto (**clave**). |
| `PORT_NAME` | STRING | Nombre del puerto. |
| `COUNTRY` | STRING | Código de país (2 letras). |
| `LATITUDE` | DOUBLE | Latitud del puerto (grados decimales). |
| `LONGITUDE` | DOUBLE | Longitud del puerto (grados decimales). |
| `HARBORSIZE` | STRING | Tamaño del puerto (V/S/M/L, código NGA). |
| `HARBORTYPE` | STRING | Tipo de puerto (código NGA, p.ej. CN/CB). |

- **Granularidad:** 1 fila por puerto. **Clave:** `INDEX_NO` (3.669 distintos = 3.669 filas).
- **Garantías:** fuente de referencia externa (NGA); lat/lon en grados decimales.

## `gold.trayectorias_diarias` (hecho)

Trayectoria diaria por buque: ordena por `(MMSI, dia, BaseDateTime)` y suma la distancia
haversine (R=6371 km) entre posiciones consecutivas (`LAG` por `(MMSI, dia)`).

| Columna | Tipo | Descripción |
|---|---|---|
| `MMSI` | LONG | Identificador del buque (**clave**). |
| `dia` | DATE | Día de la trayectoria = `to_date(BaseDateTime)`, UTC (**clave**). |
| `n_posiciones` | LONG | Nº de posiciones AIS del buque ese día. |
| `distancia_total_km` | DOUBLE | Suma haversine de distancias entre posiciones consecutivas. |
| `primera_posicion` | TIMESTAMP | Primer `BaseDateTime` del día. |
| `ultima_posicion` | TIMESTAMP | Último `BaseDateTime` del día. |
| `duracion_horas` | DOUBLE | `(ultima - primera)` en horas. |
| `lat_min`, `lat_max` | DOUBLE | *Bounding box* de latitud. |
| `lon_min`, `lon_max` | DOUBLE | *Bounding box* de longitud. |

- **Granularidad:** 1 fila por `(MMSI, dia)`. **Clave:** `(MMSI, dia)`.
- **Garantías:** LAT/LON en rango y MMSI bien formado (heredado de plata). **Límite
  conocido:** `distancia_total_km` puede ser no física si el buque tiene posiciones AIS
  que "saltan" (plata garantiza rango, no continuidad); ver `capa_oro.md` y ADR-012.

## `gold.presencia_puertos` (hecho)

Cruce AIS↔WPI por celda de grilla a 2 decimales (*Reference Data Join*). 'En puerto' =
compartir celda-puerto; permanencia mínima = ≥ 3 posiciones en la celda-puerto ese día.

| Columna | Tipo | Descripción |
|---|---|---|
| `MMSI` | LONG | Buque (**clave**). |
| `INDEX_NO` | INT | Puerto del WPI (**clave**). |
| `PORT_NAME` | STRING | Nombre del puerto. |
| `COUNTRY` | STRING | País del puerto. |
| `dia` | DATE | Día, UTC (**clave**). |
| `n_posiciones_en_puerto` | LONG | Nº de posiciones del buque en la celda-puerto ese día (≥ 3). |
| `primera_posicion` | TIMESTAMP | Primera posición en puerto ese día. |
| `ultima_posicion` | TIMESTAMP | Última posición en puerto ese día. |
| `HARBORSIZE` | STRING | Tamaño del puerto (WPI). |
| `HARBORTYPE` | STRING | Tipo de puerto (WPI). |

- **Granularidad:** 1 fila por `(MMSI, INDEX_NO, dia)`. **Clave:** `(MMSI, INDEX_NO, dia)`.
- **Garantías:** heredadas de plata; 349 puertos del WPI cruzados por tráfico real.
- **Parámetros (ADR-012, `conf/config.yaml`):** `decimales_celda_puerto=2`,
  `permanencia_minima_posiciones=3`.

## `gold.matriz_trafico` (hecho)

Densidad de tráfico por celda espacial (redondeo lat/lon a 2 decimales, ADR-001) y hora.

| Columna | Tipo | Descripción |
|---|---|---|
| `celda_lat` | DOUBLE | `round(LAT, 2)` (**clave**). |
| `celda_lon` | DOUBLE | `round(LON, 2)` (**clave**). |
| `hora` | TIMESTAMP | `date_trunc('hour', BaseDateTime)`, UTC (**clave**). |
| `n_posiciones` | LONG | Nº de posiciones AIS en la celda y hora. |
| `n_buques` | LONG | Nº de buques distintos (MMSI) en la celda y hora. |

- **Granularidad:** 1 fila por `(celda_lat, celda_lon, hora)`. **Clave:** las tres.
- **Garantías:** LAT/LON en rango (heredado de plata); celda ~1.1 km de lado en latitud.

## Versionado

- Todas las tablas son Delta (viaje en el tiempo por versión de tabla).
- Materialización idempotente: `CREATE OR REPLACE TABLE` (SQL) / `overwrite`
  (`overwriteSchema=true`) desde el notebook. Reejecutar regenera el estado.
- Reglas de negocio versionadas por parámetros en `conf/config.yaml` (bloque `gold`).

## PENDIENTE - Fase 5 (gobernanza y evolución futura)

- Gobernanza de Unity Catalog (grants, *lineage* formal, *tags* de clasificación).
- Política de evolución de esquema del oro (compatibilidad, *constraints* Delta,
  expectativas de calidad sobre el oro).
- SCD / historización de la dimensión `world_port_index` si el WPI se actualiza.
- Posible filtro de continuidad/velocidad en plata para sanear `distancia_total_km`.
