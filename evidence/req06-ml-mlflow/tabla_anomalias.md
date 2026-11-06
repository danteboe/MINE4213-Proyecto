# Evidencia — Tabla `gold.anomalias_velocidad` poblada (Req. 6)

- **Qué demuestra:** que la tabla `oceanwatch.gold.anomalias_velocidad` existe con el
  esquema declarado y está poblada de verdad para `_batch_date = 2023-06-03` con un conteo
  real > 0, coherente con el `contamination` del modelo (~1 %). Es el insumo del visual
  "anomalías del día" del dashboard (Req. 4).
- **Cómo se generó:** la tarea `ml_anomalias` del Job (corrida real `483234774995672`,
  task run `1116962065387574`) cargó el modelo `@champion`, puntuó las posiciones de
  `oceanwatch.silver.ais_clean` de 2023-06-03 y escribió las anómalas con `spark.sql`.
  Conteos y muestra leídos con el helper REST (`_dbx_sql.py`, warehouse serverless
  `377600528fed7c47`) desde un script temporal FUERA del repo.
- **Fecha:** 2026-11-05.
- **Estado:** Verificado (ejecutado).

## DDL (contrato de la tabla)

Fuente: `resources/sql/fase6_anomalias.sql`. Esquema REAL verificado con
`DESCRIBE TABLE oceanwatch.gold.anomalias_velocidad`:

| Columna          | Tipo      | Comentario (resumen)                                            |
|------------------|-----------|-----------------------------------------------------------------|
| `MMSI`           | bigint    | Identificador MMSI del buque.                                   |
| `BaseDateTime`   | timestamp | Instante de la posición AIS anómala (UTC, ADR-004).             |
| `LAT`            | double    | Latitud (grados decimales).                                     |
| `LON`            | double    | Longitud (grados decimales).                                    |
| `SOG`            | double    | Velocidad sobre el fondo (knots).                               |
| `VesselType`     | int       | Tipo de buque AIS (referencia del z-score relativo).            |
| `z_sog`          | double    | z-score de SOG relativo al tipo: `(SOG-media_tipo)/std_tipo` (D1).|
| `score_anomalia` | double    | `score_samples` del IsolationForest (mayor = más normal).       |
| `es_anomalia`    | boolean   | True si el modelo marcó la posición como anómala (`predict==-1`).|
| `razon`          | string    | Explicación legible (p. ej. `z_sog=4.2 (>3 por tipo) tipo 31`). |
| `_batch_date`    | date      | Fecha de proceso (partición lógica para refresco idempotente).  |

La tabla se crea con `CREATE TABLE IF NOT EXISTS ... USING DELTA` (no destruye datos de
otras fechas); la carga por fecha la hace el notebook con `DELETE`+`INSERT` idempotente
vía `spark.sql` (NUNCA `df.write.saveAsTable`).

## Conteo real para 2023-06-03

```
SELECT count(*) FROM oceanwatch.gold.anomalias_velocidad WHERE _batch_date=DATE('2023-06-03')
-> 67828
SELECT count(*) FROM oceanwatch.gold.anomalias_velocidad
-> 67828   (= conteo del día: la escritura idempotente dejó una sola partición)
SELECT es_anomalia, count(*) ... GROUP BY es_anomalia
-> true  67828   (todas las filas son anomalías)
```

Proporción respecto a las posiciones puntuadas del día:
`67 828 / 8 001 918 ≈ 0,85 %` (posiciones de 2023-06-03 con `SOG IS NOT NULL` y
`NOT sog_no_disponible`). Coherente con `contamination = 0.01` del IsolationForest y con
la métrica `pct_flagged_test ≈ 0,0089` registrada en el experimento MLflow (FEAT-002).

## Muestra de 10 filas anómalas (ordenadas por `z_sog` desc)

| MMSI       | BaseDateTime            | LAT     | LON       | SOG   | VesselType | z_sog  | score   | razon                                   |
|------------|-------------------------|---------|-----------|-------|------------|--------|---------|-----------------------------------------|
| 338333702  | 2023-06-03T19:06:28Z    | 6.8326  | -122.5962 | 102.2 | 36         | 36.700 | -0.7447 | z_sog=36.7 (>3 por tipo) tipo 36        |
| 368101730  | 2023-06-03T11:59:56Z    | 29.0445 | -89.1955  | 33.3  | 3          | 32.156 | -0.6449 | z_sog=32.16 (>3 por tipo) tipo 3        |
| 368101730  | 2023-06-03T11:58:53Z    | 29.0525 | -89.2016  | 33.1  | 3          | 31.962 | -0.6449 | z_sog=31.96 (>3 por tipo) tipo 3        |
| 368101730  | 2023-06-03T11:59:59Z    | 29.0441 | -89.1951  | 33.0  | 3          | 31.865 | -0.6547 | z_sog=31.86 (>3 por tipo) tipo 3        |
| 368101730  | 2023-06-03T11:57:51Z    | 29.0610 | -89.2064  | 32.9  | 3          | 31.768 | -0.7525 | z_sog=31.77 (>3 por tipo) tipo 3        |
| 368101730  | 2023-06-03T12:29:23Z    | 28.8956 | -88.9923  | 32.1  | 3          | 30.991 | -0.6776 | z_sog=30.99 (>3 por tipo) tipo 3        |
| 368101730  | 2023-06-03T12:25:13Z    | 28.9187 | -89.0241  | 32.0  | 3          | 30.894 | -0.6700 | z_sog=30.89 (>3 por tipo) tipo 3        |
| 368101730  | 2023-06-03T12:14:55Z    | 28.9710 | -89.1081  | 31.9  | 3          | 30.796 | -0.6486 | z_sog=30.8 (>3 por tipo) tipo 3         |
| 368101730  | 2023-06-03T12:13:53Z    | 28.9761 | -89.1168  | 31.8  | 3          | 30.699 | -0.6985 | z_sog=30.7 (>3 por tipo) tipo 3         |
| 368101730  | 2023-06-03T12:16:00Z    | 28.9657 | -89.0990  | 31.8  | 3          | 30.699 | -0.6513 | z_sog=30.7 (>3 por tipo) tipo 3         |

Las filas con mayor `z_sog` corresponden a SOG extremas para su tipo (p. ej. 33 kn en un
tipo 3, muy por encima de su media), lo que valida que la anomalía es RELATIVA al tipo
(D1, ADR-005) y que el `score_anomalia` del IsolationForest es coherente con el z-score.
