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
| `gold.anomalias_velocidad` | 1 fila por posición AIS anómala | `(MMSI, BaseDateTime)` | 67.828 (`_batch_date=2023-06-03`) | `silver.ais_clean` + modelo UC `oceanwatch.ml.detector_anomalias_velocidad` `@champion` |

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
| `duracion_horas` | DECIMAL(27,6) | `(ultima - primera)` en horas (tipo REAL verificado por `DESCRIBE TABLE EXTENDED`, 2026-11-05). |
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

## `gold.anomalias_velocidad` (hecho — salida del modelo de ML)

Posiciones AIS marcadas como anómalas en velocidad (SOG) por el modelo de detección de
anomalías de la Fase 6 (Req. 6, ADR-005/ADR-016). Se expone en la capa **oro** (no en `ml`)
para que el dashboard (Req. 4) la consuma directamente en el visual "anomalías del día". La
pueblan **solo** las filas anómalas: la tabla de inferencia de la tarea `ml_anomalias` del Job
escribe **1 fila por posición anómala** (`es_anomalia = true`). Tabla Delta, `USING DELTA`.

| Columna | Tipo | Descripción |
|---|---|---|
| `MMSI` | BIGINT | Identificador del buque (**clave lógica**). |
| `BaseDateTime` | TIMESTAMP | Instante de la posición AIS anómala, UTC (**clave lógica**). |
| `LAT` | DOUBLE | Latitud de la posición (grados decimales). |
| `LON` | DOUBLE | Longitud de la posición (grados decimales). |
| `SOG` | DOUBLE | Velocidad sobre el fondo (*Speed Over Ground*, nudos). |
| `VesselType` | INT | Tipo de buque (código AIS); define la normalidad de SOG. |
| `z_sog` | DOUBLE | Z-score de SOG relativo al tipo: `(SOG - media_tipo) / max(std_tipo, 1e-6)`. |
| `score_anomalia` | DOUBLE | Score continuo del IsolationForest (`score_samples`; más negativo = más anómalo). |
| `es_anomalia` | BOOLEAN | Marca de anomalía (siempre `true` en esta tabla: solo se escriben anómalas). |
| `razon` | STRING | Explicación legible, p. ej. `z_sog=36.7 (>3 por tipo) tipo 36 \| score=-0.7447`. |
| `_batch_date` | DATE | Fecha de proceso (partición lógica de inferencia = `fecha_proceso` del Job). |

- **Granularidad:** 1 fila por posición AIS anómala. **Clave lógica:** `(MMSI, BaseDateTime)`.
- **Linaje:** `oceanwatch.silver.ais_clean` (features cinemáticas: `z_sog` por tipo, `delta_sog`
  por buque, hora cíclica) puntuadas por el modelo registrado en Unity Catalog
  `oceanwatch.ml.detector_anomalias_velocidad` `@champion` (IsolationForest, ADR-016). Se
  excluyen las posiciones con SOG centinela (`sog_no_disponible = true`) y SOG nulo.
- **Conteo real:** **67.828** filas para `_batch_date = 2023-06-03` (= total de la tabla; la
  escritura `DELETE`+`INSERT` idempotente deja una sola partición de fecha). Proporción ≈ 0.85 %
  de las 8.001.918 posiciones puntuadas del día, coherente con `contamination = 0.01` del modelo.
- **Escritura (serverless):** la carga por fecha la hace el notebook de inferencia
  (`notebooks/fase6_tarea_ml_anomalias.py`) con `spark.sql` (`DELETE`+`INSERT` idempotente de la
  partición `_batch_date`), **NUNCA** `df.write.saveAsTable` (fricción serverless, igual que el
  resto del oro). DDL en `resources/sql/fase6_anomalias.sql`.
- **Evidencia:** `evidence/req06-ml-mlflow/tabla_anomalias.md` (DDL, conteo y muestra de filas),
  `evidence/req06-ml-mlflow/integracion_job_ml.md` (corrida del Job SUCCESS).

## Versionado

- Todas las tablas son Delta (viaje en el tiempo por versión de tabla).
- Materialización idempotente: `CREATE OR REPLACE TABLE` (SQL) / `overwrite`
  (`overwriteSchema=true`) desde el notebook. Reejecutar regenera el estado.
- Reglas de negocio versionadas por parámetros en `conf/config.yaml` (bloque `gold`).

## Gobernanza (Fase 5, Req. 5 — ADR-018)

Estado: **Verificado (ejecutado)** el 2026-11-05 sobre el warehouse serverless
`377600528fed7c47`. La gobernanza de la capa oro se apoya en tres mecanismos de Unity
Catalog, todos ejecutados y verificados (no diseñados):

- **Permisos de mínimo privilegio (RBAC).** El acceso a `oceanwatch.gold` se concede a
  **grupos de cuenta**, no a usuarios (ADR-018). La capa de servicio `gold` la leen los 4
  grupos de rol: `oceanwatch_ingenieria_datos` (SELECT+MODIFY), `oceanwatch_analistas` y
  `oceanwatch_ciencia_datos` (SELECT), y `oceanwatch_consumidores_dashboard` (SELECT, única
  capa que ve este rol). `bronze`/`quarantine` **no** son visibles para el rol de dashboard.
  Matriz completa y salida real de `SHOW GRANTS` en
  `evidence/req05-gobernanza/permisos_rbac.md`.
- **Linaje verificado por API.** Las 6 tablas de oro quedan trazadas aguas arriba hasta
  `silver.ais_clean` ← `bronze.ais_raw` ← volumen `bronze.landing`, y
  `gold.presencia_puertos` suma como *upstream* `gold.world_port_index`;
  `gold.matriz_trafico_diaria` es *downstream* de `gold.matriz_trafico`. Verificado con
  `GET /api/2.0/lineage-tracking/table-lineage` y `.../column-lineage`
  (`gold.trayectorias_diarias.MMSI` ← `silver.ais_clean.MMSI`). JSON real en
  `evidence/req05-gobernanza/linaje.md`. El screenshot de Catalog Explorer queda
  **PENDIENTE DE CAPTURA** (requiere navegador autenticado).
- **Documentación en el securable.** Cada tabla y columna clave de este contrato tiene su
  `COMMENT` aplicado en el metastore (`resources/sql/fase5_comentarios_oro.sql`), visible por
  `DESCRIBE TABLE EXTENDED` e `information_schema`. Evidencia en
  `evidence/req05-gobernanza/comentarios_oro.md`.

### Evolución futura (deuda reconocida, fuera del alcance de la Fase 5)

- Política de evolución de esquema del oro (compatibilidad, *constraints* Delta,
  expectativas de calidad sobre el oro).
- *Tags* de clasificación de datos en Unity Catalog.
- SCD / historización de la dimensión `world_port_index` si el WPI se actualiza.
- Posible filtro de continuidad/velocidad en plata para sanear `distancia_total_km`.
