# Invariante de conciliación bronce -> plata + cuarentena + duplicados (FEAT-003)

Estado: **Verificado (ejecutado)** en Databricks serverless el 2026-11-05.

## Invariante

La materialización de la capa plata debe conservar cada fila de bronce: una fila o
pasa a plata, o va a cuarentena, o se elimina como duplicado exacto por la clave
natural `(MMSI, BaseDateTime)` (ADR-006). El invariante (ADR-011) es:

```
registros_bronce = registros_plata + registros_cuarentena + duplicados_descartados
```

Una fila puede violar VARIAS reglas de descarte, pero cuenta UNA sola vez en cuarentena
(su columna `causas` es `array<string>`); los duplicados se cuentan aparte, como los
elimina la deduplicación (`dropDuplicatesWithinWatermark` en streaming / `ROW_NUMBER()=1`
en el CTAS batch equivalente).

## Conteos reales (SQL warehouse serverless 377600528fed7c47)

Salida literal (`SELECT count(*)` de cada tabla y de los duplicados):

```
cols: ['bronce', 'plata', 'cuarentena', 'duplicados']
['25897493', '25857101', '39734', '658']
```

Donde `duplicados = count(*) - count(DISTINCT MMSI, BaseDateTime)` sobre bronce.

## Aritmética exacta

```
25.897.493 (bronce)
  = 25.857.101 (plata)
  +     39.734 (cuarentena)
  +        658 (duplicados)

  25.857.101 + 39.734 + 658 = 25.897.493
  diferencia = 25.897.493 - 25.897.493 = 0
```

**El invariante CUADRA** (diferencia = 0).

## Verificación independiente por día (métricas)

La suma de `evaluados` por día en `oceanwatch.quality.metricas_calidad` (filas tras
dedup) es:

```
8.808.720 (2023-06-01) + 9.052.006 (2023-06-02) + 8.036.109 (2023-06-03) = 25.896.835
```

que es exactamente `plata + cuarentena = 25.857.101 + 39.734 = 25.896.835`, y
`bronce - 25.896.835 = 658` duplicados. Doble confirmación de la aritmética.

## Reproducción

- Materialización: `resources/sql/fase3_materializar_plata.sql` (CTAS en el warehouse).
- Conteos: `python _dbx_sql.py 377600528fed7c47 <archivo.sql>` con los `SELECT count(*)`
  de arriba (o el `SELECT` conjunto de `bronce/plata/cuarentena/duplicados`).
- Comprobación aritmética pura: `oceanwatch.quality.conciliacion.verificar_invariante(
  25897493, 25857101, 39734, 658)` -> `cuadra = True`, `diferencia = 0`
  (cubierto por `tests/test_conciliacion.py`).
