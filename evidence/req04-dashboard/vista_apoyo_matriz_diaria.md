# Evidencia — Vista de apoyo en oro `gold.matriz_trafico_diaria`

- **Qué demuestra:** que el visual 1 (mapa de intensidad) consume una tabla **oro
  pre-agregada por día** y no recomputa la agregación horaria en cada render (decisión
  D2/D4, ADR-017). El dashboard NO toca silver/bronze.
- **Cómo se generó:** DDL `CREATE OR REPLACE TABLE ... AS SELECT` ejecutado vía
  `_dbx_sql.py` sobre el warehouse serverless `377600528fed7c47`, leyendo el archivo
  versionado `resources/sql/fase7_matriz_trafico_diaria.sql`. Verificación de conteos con
  el script temporal `_fase7_apoyo.py` (fuera del repo, eliminado al terminar).
- **Fecha:** 2026-11-06.
- **IDs reales:** warehouse `377600528fed7c47`; tabla `oceanwatch.gold.matriz_trafico_diaria`.
- **Estado:** **Verificado (ejecutado)**.

## Motivo (rendimiento)

`gold.matriz_trafico` tiene granularidad celda × **hora** (2.319.365 filas; 324k–371k
celdas distintas por día). El mapa necesita **1 fila por celda y día** (colapsando las
horas). Calcular ese `GROUP BY` sobre 2,3 M filas en cada render es caro y repetido, así que
se materializa **una sola vez** en oro.

## DDL ejecutado (de `resources/sql/fase7_matriz_trafico_diaria.sql`)

```sql
CREATE OR REPLACE TABLE oceanwatch.gold.matriz_trafico_diaria
USING DELTA
COMMENT 'Densidad de trafico por celda (lat/lon 2 decimales, ADR-001) y DIA. Agregado de gold.matriz_trafico (colapsa la hora) para el mapa de intensidad del dashboard Req.4. Solo-oro pre-agregado (D2/D4, ADR-017, Fase 7).'
AS
SELECT
  celda_lat,
  celda_lon,
  date(hora) AS dia,
  SUM(n_posiciones) AS intensidad,
  SUM(n_buques)     AS buques
FROM oceanwatch.gold.matriz_trafico
GROUP BY celda_lat, celda_lon, date(hora);
```

Salida de ejecución (helper `_dbx_sql.py`):

```
OK    | CREATE SCHEMA IF NOT EXISTS oceanwatch.gold
OK    | CREATE OR REPLACE TABLE oceanwatch.gold.matriz_trafico_diaria USING DE...
```

## Conteos reales de la tabla resultante

`SELECT dia, COUNT(*) celdas, SUM(intensidad) intensidad_total FROM oceanwatch.gold.matriz_trafico_diaria GROUP BY dia ORDER BY dia`:

| dia | celdas (filas) | intensidad_total |
|---|---|---|
| 2023-06-01 | 332.177 | 8.796.495 |
| 2023-06-02 | 370.872 | 9.037.664 |
| 2023-06-03 | 324.586 | 8.022.942 |

**Total de filas:** `1.027.635` (1 fila por celda-día; 3 días). Coherente con la
exploración (~324k–371k celdas/día).

## Equivalencia con la agregación directa (control)

La consulta V1 contra `gold.matriz_trafico_diaria` (`:fecha = 2023-06-02`, top por
intensidad) devuelve el **mismo** top que agregar en vivo la tabla horaria
`gold.matriz_trafico` (top: 47.63,-122.39 → 39.470; 47.66,-122.38 → 27.179; 32.72,-117.23 →
26.255; …). Ver `consultas_visuales.md` (V1).

## Nota sobre la métrica `buques`

`buques = SUM(n_buques)` suma los conteos de buques distintos **por hora**; un buque
presente en varias horas de la celda se cuenta más de una vez. Es una métrica
**secundaria** de actividad (no "buques únicos por día"). La métrica principal del mapa es
`intensidad` (= `SUM(n_posiciones)`). Documentado en `docs/data_contract_gold.md`.
