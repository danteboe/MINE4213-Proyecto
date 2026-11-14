# Evidencia — Consultas SQL de los 4 visuales (resultados reales ejecutados)

- **Qué demuestra:** que cada uno de los 4 visuales obligatorios del dashboard (Req. 4)
  consume una tabla de la capa **oro** y retorna **filas reales**. Confirma la fuente
  solo-oro de cada visual.
- **Cómo se generó:** ejecutando las consultas exactas de cada dataset del
  `.lvdash.json` vía la SQL Statement Execution API (helper `_dbx_sql.py`, warehouse
  serverless `377600528fed7c47`), sustituyendo el parámetro `:fecha` por literales de
  fecha del corpus. Script temporal `_fase7_verificar.py` (fuera del repo, eliminado al
  terminar).
- **Fecha:** 2026-11-06.
- **IDs reales:** warehouse `377600528fed7c47`; dashboard `01f1da0367841f3db6c6ddcb8edaa849`.
- **Estado:** **Verificado (ejecutado)**.

## Fechas disponibles por tabla oro (contexto del corpus)

| Tabla oro | Columna fecha | Valores (conteo real) |
|---|---|---|
| `gold.matriz_trafico` | `date(hora)` | 2023-06-01 (767.052), 2023-06-02 (840.240), 2023-06-03 (712.073) |
| `gold.presencia_puertos` | `dia` | 2023-06-01 (3.086), 2023-06-02 (3.141), 2023-06-03 (2.926) |
| `gold.anomalias_velocidad` | `_batch_date` | 2023-06-03 (67.828) — **solo 06-03** (test set del modelo, ADR-016) |

## Visual 1 — Mapa de intensidad de tráfico por celda

**Tabla oro:** `oceanwatch.gold.matriz_trafico_diaria` (vista de apoyo pre-agregada, ver
`vista_apoyo_matriz_diaria.md`). **Tipo:** mapa de puntos (`symbol-map`), color = intensidad.

```sql
SELECT celda_lat, celda_lon, intensidad, buques
FROM oceanwatch.gold.matriz_trafico_diaria
WHERE dia = :fecha            -- verificado con DATE'2023-06-02'
ORDER BY intensidad DESC
LIMIT 2000
```

**Resultado real (`:fecha = 2023-06-02`, top 10 de 2.000):**

| celda_lat | celda_lon | intensidad | buques |
|---|---|---|---|
| 47.63 | -122.39 | 39.470 | 2.157 |
| 47.66 | -122.38 | 27.179 | 1.185 |
| 32.72 | -117.23 | 26.255 | 1.451 |
| 47.68 | -122.41 | 25.597 | 1.701 |
| 33.72 | -118.28 | 25.141 | 1.301 |
| 33.98 | -118.45 | 18.333 | 1.441 |
| 32.71 | -117.23 | 18.179 | 1.144 |
| 29.76 | -95.09 | 17.978 | 866 |

Filas devueltas: **2.000** (tope del `LIMIT`; hay 370.872 celdas el 06-02). La celda más
intensa es la zona de Seattle (47.63, -122.39). *(La misma agregación sobre la tabla horaria
`gold.matriz_trafico` da idéntico top-8 — ver `vista_apoyo_matriz_diaria.md`.)*

## Visual 2 — Ranking de puertos (Top-15)

**Tabla oro:** `oceanwatch.gold.presencia_puertos`. **Tipo:** barras horizontales.

```sql
SELECT PORT_NAME, COUNTRY,
       COUNT(DISTINCT MMSI)        AS buques_distintos,
       SUM(n_posiciones_en_puerto) AS visitas
FROM oceanwatch.gold.presencia_puertos
WHERE dia = :fecha            -- verificado con DATE'2023-06-02'
GROUP BY PORT_NAME, COUNTRY
ORDER BY buques_distintos DESC
LIMIT 15
```

**Resultado real (`:fecha = 2023-06-02`, top 8 de 15):**

| PORT_NAME | COUNTRY | buques_distintos | visitas |
|---|---|---|---|
| NEW YORK CITY | US | 92 | 1.744 |
| ANNAPOLIS | US | 82 | 2.618 |
| GRETNA | US | 82 | 1.680 |
| GALVESTON | US | 77 | 7.521 |
| NORFOLK | US | 76 | 1.035 |
| PORT EVERGLADES | US | 65 | 11.653 |
| RESERVE | US | 54 | 3.148 |
| BAYTOWN | US | 52 | 6.736 |

Filas devueltas: **15**.

## Visual 3 — Buques en puerto en la fecha seleccionada

**Tabla oro:** `oceanwatch.gold.presencia_puertos`. **Tipo:** tabla.

```sql
SELECT MMSI, PORT_NAME, COUNTRY,
       n_posiciones_en_puerto,
       primera_posicion, ultima_posicion
FROM oceanwatch.gold.presencia_puertos
WHERE dia = :fecha            -- verificado con DATE'2023-06-02'
ORDER BY n_posiciones_en_puerto DESC
LIMIT 500
```

**Resultado real (`:fecha = 2023-06-02`, top 8 de 500):**

| MMSI | PORT_NAME | COUNTRY | n_posiciones_en_puerto | primera_posicion | ultima_posicion |
|---|---|---|---|---|---|
| 367331730 | WARRENTON | US | 1.347 | 2023-06-02T00:00:00Z | 2023-06-02T23:59:30Z |
| 367422020 | POINT RICHMOND | US | 1.252 | 2023-06-02T00:00:07Z | 2023-06-02T23:59:57Z |
| 367071960 | PORT HUENEME | US | 1.246 | 2023-06-02T00:00:08Z | 2023-06-02T23:59:16Z |
| 366924720 | POINT RICHMOND | US | 1.246 | 2023-06-02T00:00:04Z | 2023-06-02T23:59:35Z |
| 366896060 | SABINE | US | 1.245 | 2023-06-02T00:00:02Z | 2023-06-02T23:59:52Z |
| 367494020 | ANACORTES | US | 1.245 | 2023-06-02T00:00:04Z | 2023-06-02T23:59:34Z |
| 367389840 | POINT RICHMOND | US | 1.243 | 2023-06-02T00:00:05Z | 2023-06-02T23:59:56Z |
| 367579120 | GALVESTON | US | 1.242 | 2023-06-02T00:00:06Z | 2023-06-02T23:59:06Z |

Filas devueltas: **500** (tope del `LIMIT`; hay 3.141 filas el 06-02).

## Visual 4 — Anomalías del día

**Tabla oro:** `oceanwatch.gold.anomalias_velocidad`. **Tipo:** tabla + mapa de puntos.
**Definición de "anomalía del día" (ADR-016):** posición AIS con `es_anomalia = true` del
día `_batch_date = :fecha`. El modelo campeón es un **IsolationForest**
(`contamination=0.01`, `random_state=42`) sobre `[z_sog, delta_sog, hora_sin, hora_cos]`,
con `z_sog` = z-score de SOG **relativo al `VesselType`**; la línea base interpretable marca
`|z_sog| > 3` por tipo (**umbral documentado = 3.0**, `conf/config.yaml`
`ml.umbral_zscore_base`). `es_anomalia` = (`predict == -1` del IsolationForest).

```sql
SELECT MMSI, BaseDateTime, LAT, LON, SOG, VesselType,
       z_sog, score_anomalia, razon
FROM oceanwatch.gold.anomalias_velocidad
WHERE _batch_date = :fecha AND es_anomalia = true   -- verificado con DATE'2023-06-03'
ORDER BY score_anomalia ASC
LIMIT 500
```

**Resultado real (`:fecha = 2023-06-03`, top 8 de 500):**

| MMSI | BaseDateTime | LAT | LON | SOG | VesselType | z_sog | score_anomalia | razon |
|---|---|---|---|---|---|---|---|---|
| 367500120 | 2023-06-03T22:53:15Z | 27.99549 | -82.80045 | 42.7 | 37 | 9.03 | -0.76548 | z_sog=9.03 (>3 por tipo) tipo 37 \| score=-0.7655 |
| 316009467 | 2023-06-03T22:24:44Z | 48.65402 | -123.38738 | 45.3 | 37 | 9.60 | -0.76548 | z_sog=9.6 (>3 por tipo) tipo 37 \| score=-0.7655 |
| 316004448 | 2023-06-03T22:58:38Z | 48.47477 | -123.17315 | 35.6 | 37 | 7.47 | -0.76548 | z_sog=7.47 (>3 por tipo) tipo 37 \| score=-0.7655 |
| 338340072 | 2023-06-03T22:02:11Z | 28.51122 | -95.03068 | 47.0 | 37 | 9.97 | -0.76548 | z_sog=9.97 (>3 por tipo) tipo 37 \| score=-0.7655 |
| 338395329 | 2023-06-03T22:49:16Z | 29.54858 | -95.01538 | 28.7 | 31 | 8.95 | -0.76548 | z_sog=8.95 (>3 por tipo) tipo 31 \| score=-0.7655 |
| 338401512 | 2023-06-03T22:07:43Z | 29.48859 | -94.86334 | 29.8 | 30 | 7.42 | -0.76548 | z_sog=7.42 (>3 por tipo) tipo 30 \| score=-0.7655 |
| 367500120 | 2023-06-03T22:38:14Z | 27.92399 | -82.82767 | 42.0 | 37 | 8.88 | -0.76548 | z_sog=8.88 (>3 por tipo) tipo 37 \| score=-0.7655 |
| 368165150 | 2023-06-03T22:04:42Z | 45.78576 | -84.71786 | 29.5 | 0 | 7.06 | -0.76548 | z_sog=7.06 (>3 por tipo) tipo 0 \| score=-0.7655 |

Filas devueltas: **500** (tope del `LIMIT`; hay 67.828 anomalías el 06-03).

> **Nota de datos (correcta, no es un error):** con `:fecha` = 2023-06-01 o 2023-06-02 este
> visual sale **vacío** por diseño del corpus — `gold.anomalias_velocidad` solo tiene
> `_batch_date = 2023-06-03` (test set del modelo, ADR-016). Por eso el default del dashboard
> es **2023-06-03**, para que los 4 visuales tengan datos al abrir. Ver
> `validacion_parametro_fecha.md`.

## Confirmación de fuente solo-oro

Las 4 consultas leen **exclusivamente** tablas de `oceanwatch.gold`
(`matriz_trafico_diaria`, `presencia_puertos` x2, `anomalias_velocidad`). **Ningún** visual
consulta `oceanwatch.bronze` ni `oceanwatch.silver`. Ver `confirmacion_solo_oro.md`.
