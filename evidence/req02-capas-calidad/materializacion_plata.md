# Materialización de la capa PLATA — conteos reales (FEAT-003, Req. 2)

Estado: **Verificado (ejecutado)** en Databricks serverless el 2026-11-05.

Este documento recoge la salida LITERAL de la materialización de la capa plata, la
cuarentena y las métricas de calidad sobre las **25.897.493** filas reales de
`oceanwatch.bronze.ais_raw`. Las compuertas aplicadas son las de
`src/oceanwatch/quality/compuertas.py` (ADR-009/010/011).

## Entorno y camino de ejecución

- Catálogo/esquemas: `oceanwatch.{silver,quarantine,quality}` (Unity Catalog).
- Cómputo: **serverless**. SQL warehouse serverless id `377600528fed7c47`.
- Sentencias CTAS: `resources/sql/fase3_materializar_plata.sql`, ejecutadas vía SQL
  Statement Execution API (`/api/2.0/sql/statements/`) con el helper `_dbx_sql.py`.
- Las tres tablas resultado se **conservan** (no son sandboxes efímeros).

### Fricción serverless real (síntoma -> causa -> solución)

El notebook `notebooks/fase3_plata_calidad.py` orquesta el MISMO pipeline con la API
de Spark (`materializar.materializar_batch` -> `foreachBatch` + escritura Delta).
Lanzado como one-time run serverless (`jobs submit`, notebook_task sin `new_cluster`):

| Intento | run_id (job) | Síntoma | Causa | Solución |
|---|---|---|---|---|
| 1 | 503963852473476 | `CACHE_REQUIRES_NEWER_CLIENT_ON_SERVERLESS_GC` | `df.cache()`/`unpersist()` no soportados por el cliente en serverless GC | Se eliminó todo `cache()`/`unpersist()`; los conteos se leen de las tablas Delta materializadas |
| 2 | 963318912213288 | `INTERNAL_ERROR` en `saveAsTable` | UDF escalar de `global_land_mask` fila-a-fila sobre 25.9M filas | Se reescribió `en_agua` como `pandas_udf` vectorizado y se desacopló del núcleo |
| 3 | 974943142152810 | `INTERNAL_ERROR` en `saveAsTable` (plata, SIN UDF) | Error interno del cómputo serverless (Spark Connect GC) al escribir Delta a esa escala; no es un fallo de la lógica (plan 100% nativo) | Se ejecutó el MISMO pipeline como **SQL CTAS** en el SQL warehouse serverless (camino alternativo, motor distinto), que SÍ escribe las tablas de forma fiable |

Diagnóstico: el fallo 3 ocurre en el `DataFrameWriter.saveAsTable` con un plan de solo
expresiones nativas (sin UDF, sin cache), por lo que es una limitación del cómputo
serverless Spark Connect para la escritura Delta a esta escala, no de las compuertas.
Se verificó que el SQL warehouse serverless escribe una tabla de 25.9M filas desde
bronce sin problema (prueba CTAS previa), y se adoptó ese camino. La lógica de las
compuertas en SQL reproduce EXACTAMENTE la de `compuertas.py` (misma dedup por
ROW_NUMBER, mismas reglas de descarte y de nulificación, mismos umbrales de
`conf/config.yaml`).

## Conteos reales (SQL warehouse 377600528fed7c47)

| Tabla | Conteo real |
|---|---|
| `oceanwatch.bronze.ais_raw` (origen) | **25.897.493** |
| `oceanwatch.silver.ais_clean` (plata, size(causas)=0) | **25.857.101** |
| `oceanwatch.quarantine.ais_rejected` (cuarentena, size(causas)>0) | **39.734** |
| duplicados exactos por (MMSI, BaseDateTime) | **658** |

Salida literal del conteo conjunto:

```
cols: ['bronce', 'plata', 'cuarentena', 'duplicados']
['25897493', '25857101', '39734', '658']
```

## Cuarentena por causa (explode de `causas`)

Una fila con varias causas aparece en varias reglas; el número de FILAS en cuarentena
es 39.734 (no la suma por regla).

```
cols: ['regla', 'filas_con_causa']
['mmsi_mid_valido', '39734']
['mmsi_bien_formado', '21958']
```

Distribución del número de causas por fila:

```
cols: ['n_causas', 'filas']
['1', '17776']   -- solo mmsi_mid_valido (9 dígitos pero MID fuera de [201,775])
['2', '21958']   -- mmsi_bien_formado + mmsi_mid_valido (MMSI no de 9 dígitos)
```

Las **21.958** filas de MMSI mal formado (no de 9 dígitos) cumplen el criterio de
aceptación (~21.960 del diagnóstico; la diferencia de 2 son duplicados exactos
eliminados por la dedup). Toda fila sin 9 dígitos también falla `mmsi_mid_valido`
(el MID exige 9 dígitos primero), de ahí las 2 causas.

Muestra literal de cuarentena (registro original + causas):

```
cols: ['MMSI', 'BaseDateTime', '_batch_date', 'causas']
['0', '2023-06-01T12:20:30.000Z', '2023-06-01', '["mmsi_bien_formado","mmsi_mid_valido"]']
['0', '2023-06-01T13:20:15.000Z', '2023-06-01', '["mmsi_bien_formado","mmsi_mid_valido"]']
```

## Nulificación de atributos en plata (banderas True) — coincide con el diagnóstico

```
cols: ['sog_nulos','heading_nulos','cog_nulos','length_nulos','width_nulos','draft_nulos','imo_nulos']
['65501', '14338249', '4373778', '892239', '953110', '1165760', '582979']
```

(Los conteos son sobre la plata ya deduplicada: p. ej. Heading=511 ≈ 14,34M frente a
14.367.009 en bronce antes de dedup; SOG≥102.3 = 65.501 frente a 65.510.)

Coherencia bandera->atributo (bandera True debe implicar atributo NULL; resultado = 0):

```
cols: ['sog_incoherente','heading_incoherente','imo_incoherente']
['0', '0', '0']
```

Muestra literal de plata con Heading nulificado:

```
cols: ['MMSI','Heading','heading_desconocido','COG','cog_desconocido','SOG','sog_no_disponible']
['202182469', None, 'true', '78.4', 'false', '4.5', 'false']
```

## Métricas de calidad por día y por regla (`oceanwatch.quality.metricas_calidad`)

15 filas = 3 días (2023-06-01/02/03) × 5 reglas de descarte. `evaluados` = filas
deduplicadas del día; `rechazados` = filas del día cuya `causas` contiene esa regla;
`pasados = evaluados - rechazados`; `pct_rechazo` y `alerta` (umbral 5%). Salida literal:

```
cols: ['_batch_date','regla','evaluados','pasados','rechazados','pct_rechazo','alerta']
['2023-06-01','coordenada_no_centinela','8808720','8808720','0','0.0','false']
['2023-06-01','coordenadas_en_rango','8808720','8808720','0','0.0','false']
['2023-06-01','mmsi_bien_formado','8808720','8801809','6911','7.85E-4','false']
['2023-06-01','mmsi_mid_valido','8808720','8796495','12225','0.001388','false']
['2023-06-01','timestamp_valido_no_futuro','8808720','8808720','0','0.0','false']
['2023-06-02','coordenada_no_centinela','9052006','9052006','0','0.0','false']
['2023-06-02','coordenadas_en_rango','9052006','9052006','0','0.0','false']
['2023-06-02','mmsi_bien_formado','9052006','9044132','7874','8.7E-4','false']
['2023-06-02','mmsi_mid_valido','9052006','9037664','14342','0.001584','false']
['2023-06-02','timestamp_valido_no_futuro','9052006','9052006','0','0.0','false']
['2023-06-03','coordenada_no_centinela','8036109','8036109','0','0.0','false']
['2023-06-03','coordenadas_en_rango','8036109','8036109','0','0.0','false']
['2023-06-03','mmsi_bien_formado','8036109','8028936','7173','8.93E-4','false']
['2023-06-03','mmsi_mid_valido','8036109','8022942','13167','0.001638','false']
['2023-06-03','timestamp_valido_no_futuro','8036109','8036109','0','0.0','false']
```

`evaluados` por día: 8.808.720 + 9.052.006 + 8.036.109 = 25.896.835 = plata + cuarentena
(coincide con la eliminación de 658 duplicados respecto a bronce). Solo las reglas de
MMSI rechazan; coordenadas y timestamp dan 0 rechazos, como anticipó el diagnóstico.
Ninguna regla supera el umbral de alerta del 5% (la máxima es ~0.16%).

## Bandera `en_agua` (ADR-010) — PENDIENTE DE ENRIQUECIMIENTO

La columna `en_agua` existe en `oceanwatch.silver.ais_clean` y está en **NULL** para las
25.857.101 filas:

```
cols: ['en_agua_nulos', 'en_agua_no_nulos']
['25857101', '0']
```

Es NULIFICAR-ATRIBUTO informativa (ADR-010): **no afecta la invariante** ni la
clasificación plata/cuarentena. Su cálculo requiere `global_land_mask` (no disponible
en SQL puro) y el único camino con la librería es el `pandas_udf`
`compuertas._columna_en_agua`, que corre en el notebook de Spark; ese camino está
bloqueado por la fricción serverless descrita arriba. La exactitud y el coste de la
máscara ya se midieron en FEAT-002 (run serverless 614704376036187, SUCCESS; ver
`evidence/req02-capas-calidad/mascara_tierra_agua.md`).

**Procedimiento exacto para enriquecer `en_agua`** (cuando el cómputo serverless Spark
Connect lo permita, o en un cluster all-purpose):
1. En un notebook serverless: `%pip install global-land-mask==1.0.0` y `dbutils.library.restartPython()`.
2. `materializar.enriquecer_en_agua(spark, cfg)` recalcula solo la columna `en_agua`
   con el `pandas_udf` vectorizado y reescribe `oceanwatch.silver.ais_clean` por
   `overwrite` (el conteo de plata no cambia; solo se puebla la bandera).
3. Verificar con `SELECT en_agua, count(*) FROM oceanwatch.silver.ais_clean GROUP BY en_agua`.
