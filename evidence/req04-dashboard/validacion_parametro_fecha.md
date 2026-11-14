# Evidencia — Validación del parámetro de fecha

- **Qué demuestra:** que el parámetro `:fecha` del dashboard afecta a los 4 visuales y que
  el comportamiento por fecha es el esperado del corpus (V4 solo tiene datos el 2023-06-03).
- **Cómo se generó:** ejecutando las 4 consultas de los datasets con `:fecha` sustituida por
  cada una de las 3 fechas del corpus, vía `_dbx_sql.py` sobre el warehouse
  `377600528fed7c47`. Script temporal `_fase7_param.py` (fuera del repo, eliminado al
  terminar).
- **Fecha:** 2026-11-06.
- **IDs reales:** dashboard `01f1da0367841f3db6c6ddcb8edaa849`; warehouse `377600528fed7c47`.
- **Estado:** **Verificado (ejecutado)**.

## Cómo se implementó el parámetro (decisión D3, ADR-017)

Un **único** parámetro de dashboard `fecha` de tipo `DATE` (selección única,
`defaultSelection = 2023-06-03`), declarado en los **4 datasets** del `.lvdash.json` con la
sintaxis de *named parameter marker* `:fecha`. Se expone en el canvas con un widget
**filtro** (`filter-single-select`) cuyas `encodings.fields` enlazan el `parameterName:
fecha` de los 4 datasets, de modo que al cambiar la fecha en el filtro cambian los 4
visuales a la vez. El filtro entra en el `WHERE` **antes** de agregar
(`WHERE date=... / _batch_date=...`), por lo que es eficiente sobre las tablas grandes.

Cableado por visual:
- V1: `WHERE dia = :fecha` (sobre `gold.matriz_trafico_diaria`).
- V2 y V3: `WHERE dia = :fecha` (sobre `gold.presencia_puertos`).
- V4: `WHERE _batch_date = :fecha AND es_anomalia = true` (sobre `gold.anomalias_velocidad`).

## Conteos reales por fecha (filas que recibiría cada visual, con su `LIMIT`)

| :fecha | V1 (celdas, tope 2000) | V2 (puertos, tope 15) | V3 (buques, tope 500) | V4 (anomalías) |
|---|---|---|---|---|
| 2023-06-01 | 2.000 | 15 | 500 | **0** |
| 2023-06-02 | 2.000 | 15 | 500 | **0** |
| 2023-06-03 | 2.000 | 15 | 500 | **67.828** |

**Interpretación:** V1–V3 tienen datos para las 3 fechas (los topes reflejan el `LIMIT`,
hay muchas más filas detrás). V4 solo tiene filas el **2023-06-03** porque
`gold.anomalias_velocidad` solo contiene ese `_batch_date` (test set del modelo, ADR-016).
Esto es correcto por diseño del corpus, no un fallo; por eso el default del parámetro es
2023-06-03, para que los 4 visuales muestren datos al abrir el tablero. 0 errores de SQL.
