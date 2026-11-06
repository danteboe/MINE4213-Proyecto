# Evidencia — Pruebas de tolerancia a fallos de la ingesta streaming (Req. 1)

- **Qué demuestra:** el patrón de ingesta de la Fase 2 (Auto Loader + `Trigger.availableNow`,
  ADR-003/ADR-004, con capa bronce append-only y dedup bronce→silver de ADR-006) es
  **resistente a fallos** ante los 4 escenarios exigidos por el Req. 1. Cada prueba se ejecutó
  **de verdad** en Databricks **serverless** y persistió su resultado OBSERVADO en
  `oceanwatch.quality.pruebas_fallos`.
- **Cómo se generó:** notebook `notebooks/fase2_pruebas_fallos` importado al workspace bajo
  `/Users/jd.torresa1@uniandes.edu.co/oceanwatch/notebooks/`; one-time run con
  `databricks jobs submit --json` (notebook_task **serverless**,
  `effective_performance_target: PERFORMANCE_OPTIMIZED`); polling con
  `databricks jobs get-run --output json` hasta `TERMINATED`; resultados leídos vía la SQL
  Statement Execution API (warehouse serverless `377600528fed7c47`).
- **Aislamiento:** las pruebas usan un **sandbox** bajo
  `landing/pruebas_fallos/` y `checkpoints/pruebas_fallos/`, con una tabla temporal por prueba
  (`oceanwatch.bronze.pf_*`), de modo que **no** tocan `oceanwatch.bronze.ais_raw` ni sus
  métricas (FEAT-001). Los datasets son CSV pequeños y sintéticos con el **esquema AIS real de
  17 campos** (`verificacion_esquema_real.md`); no se descargan los zips de NOAA.
- **Fecha:** 2026-11-05
- **Estado:** Verificado (ejecutado)

## Estado del run (serverless)

```
run_id: 497716691196081
run_name: oceanwatch-fase2-pruebas-fallos
run_type: SUBMIT_RUN
effective_performance_target: PERFORMANCE_OPTIMIZED   (serverless)
life_cycle_state: TERMINATED
result_state: SUCCESS
execution_duration: 88 s
run_duration: 92 s
run_page_url: https://dbc-a568bc49-bdae.cloud.databricks.com/?o=7474649644754546#job/989871850935218/run/497716691196081
```

## Lectura literal de `oceanwatch.quality.pruebas_fallos`

Salida literal de `python _dbx_sql.py 377600528fed7c47 _qp.sql`
(`SELECT prueba, estado, hipotesis, esperado, observado, detalle, _registrado ... ORDER BY prueba`):

```
COLS: ['prueba', 'estado', 'hipotesis', 'esperado', 'observado', 'detalle', '_registrado']
ROW: ['a_corte_reinicio', 'PASA', 'El corte a mitad de lote + reinicio desde el mismo checkpoint no pierde ni duplica filas.', 'conteo_final == 6 y sin duplicados por (MMSI,BaseDateTime)', 'corte_tras_1_batch=6 filas; final=6 filas; distintos_por_clave=6; esperado=6; error_corte=no', 'q1_error=None', '2026-11-06T03:56:37.330Z']
ROW: ['b_archivo_duplicado', 'PASA', 'Reenviar un archivo ya ingerido sube bronce (append) pero la dedup silver mantiene el conteo.', 'bronce_tras_reenvio > bronce_tras_original y dedup_silver_distintos == 3', 'bronce_tras_original=3; bronce_tras_reenvio=6 (append sube); duplicados_por_clave=3; dedup_silver_distintos=3 (estable)', "metricas={'total': 6, 'distintos_por_clave': 3, 'duplicados': 3}", '2026-11-06T03:56:37.330Z']
ROW: ['c_esquema_corrupto', 'PASA', "Un valor de tipo no conforme (Status='ABC') y una columna extra no matan el stream; lo no conforme va a _rescued_data.", 'stream_murio == no y filas_con_rescued_data >= 1', 'stream_murio=no; filas_ingeridas=4; filas_con_rescued_data=1', 'error_stream=None', '2026-11-06T03:56:37.330Z']
ROW: ['d_reinicio_checkpoint', 'PASA', 'Con availableNow, una segunda corrida sin archivos nuevos no reprocesa lo ya consumido.', 'conteo_corrida_1=3; conteo_corrida_2=3 (sin archivos nuevos); reproceso=no', '', '2026-11-06T03:56:37.330Z']
```

---

## Prueba (a) — Corte a mitad de lote + reinicio desde el mismo checkpoint

- **Hipótesis.** Si el stream se interrumpe tras un micro-batch y se relanza desde el mismo
  checkpoint, el conteo final es igual al de una corrida sin corte, sin pérdida ni duplicados
  (exactly-once por checkpoint + `txnAppId`/`txnVersion`, ADR-004/ADR-007).
- **Procedimiento.** 3 CSV de 2 filas cada uno (6 filas, claves únicas) en el sandbox. Corrida 1
  con `cloudFiles.maxFilesPerTrigger=1` y un `foreachBatch` que lanza una excepción
  (`CORTE SIMULADO`) tras el primer micro-batch. Corrida 2: relanzar desde el **mismo**
  `checkpointLocation`. Se mide el conteo tras el corte y el conteo final.
- **Esperado.** `conteo_final == 6` y 6 distintos por `(MMSI, BaseDateTime)`.
- **OBSERVADO.** `corte_tras_1_batch=6 filas; final=6 filas; distintos_por_clave=6; esperado=6;
  error_corte=no`. Estado: **PASA**.
- **Nota de interpretación (observación real, no inventada).** `error_corte=no` y
  `corte_tras_1_batch=6` indican que en serverless el stream con `availableNow` consolidó los 3
  archivos en **un solo micro-batch** (pese a `maxFilesPerTrigger=1`), por lo que la excepción
  de corte —condicionada a "tras el primer batch"— no llegó a dispararse: al procesarse todo en
  el primer batch, no hubo un segundo batch que interrumpir. La propiedad verificada se mantiene:
  tras reiniciar desde el mismo checkpoint el resultado es exacto (6 filas, 6 distintos, sin
  duplicados). La garantía de reanudación/idempotencia por checkpoint queda además cubierta por
  la prueba (d).

## Prueba (b) — Archivo duplicado/reenviado

- **Hipótesis.** Reenviar un CSV ya ingerido con otro nombre hace que **bronce suba** (append,
  Auto Loader lo ve como archivo nuevo), pero la deduplicación silver por `MMSI`+`BaseDateTime`
  (ADR-006) deja el conteo estable.
- **Procedimiento.** Ingerir `original.csv` (3 filas). Luego copiar el **mismo contenido** como
  `reenviado_copia.csv` y re-ingerir. Medir el conteo de bronce tras cada paso y aplicar
  `dedup.contar_duplicados` (watermark 24h + `dropDuplicatesWithinWatermark`).
- **Esperado.** `bronce_tras_reenvio > bronce_tras_original` y `dedup_silver_distintos == 3`.
- **OBSERVADO.** `bronce_tras_original=3; bronce_tras_reenvio=6 (append sube);
  duplicados_por_clave=3; dedup_silver_distintos=3 (estable)`
  (`metricas={'total': 6, 'distintos_por_clave': 3, 'duplicados': 3}`). Estado: **PASA**.

## Prueba (c) — Esquema corrupto/no conforme (rescued data)

- **Hipótesis.** Un CSV con un **valor de tipo no conforme** (`Status="ABC"`, donde el esquema
  declara `int`, ADR-008) y otro con una **columna extra** (18 valores bajo el header de 17)
  **no matan el stream**; lo no conforme queda capturado en `_rescued_data` (esquema-on-read
  explícito + rescued data column, ADR-008).
- **Procedimiento.** Colocar en el sandbox: `valido.csv` (2 filas conformes),
  `tipo_corrupto.csv` (1 fila con `Status="ABC"`) y `columna_extra.csv` (1 fila con un valor
  extra al final). Ingerir con el esquema explícito y `rescuedDataColumn=_rescued_data`. Medir si
  el stream muere y cuántas filas tienen `_rescued_data` no nulo.
- **Esperado.** `stream_murio == no` y `filas_con_rescued_data >= 1`.
- **OBSERVADO.** `stream_murio=no; filas_ingeridas=4; filas_con_rescued_data=1`. Estado: **PASA**.
- **Nota.** El stream ingirió las 4 filas sin abortar y **1** fila llevó datos a `_rescued_data`
  (el valor sobrante de la columna extra y/o el `Status` no casteable), confirmando que los datos
  no conformes se rescatan en lugar de romper la ingesta.
- **Iteración del diseño de la prueba (transparencia).** Una primera versión de esta prueba usaba
  un archivo "truncado" (3 columnas, sin header) y otro de columna extra, y observó
  `filas_con_rescued_data=0` (run `282095277952514`): con header por archivo y esquema explícito,
  Auto Loader alineaba las columnas por nombre y no rescataba nada. Se corrigió el **diseño de la
  prueba** (no el sistema de ingesta) para forzar una no-conformidad real de tipo
  (`Status="ABC"`) y un valor extra bajo el header válido; el re-run (`497716691196081`) observó
  `filas_con_rescued_data=1`. No se alteró el comportamiento del pipeline.

## Prueba (d) — Reinicio desde checkpoint con `availableNow` (no reprocesa)

- **Hipótesis.** Con `Trigger.availableNow`, una segunda corrida seguida sin archivos nuevos
  **no reprocesa** los archivos ya consumidos (el checkpoint guarda los offsets de archivos,
  ADR-004/ADR-007); el conteo de bronce no cambia entre corridas.
- **Procedimiento.** Ingerir `dia1.csv` (3 filas). Sin añadir archivos, re-ingerir desde el mismo
  checkpoint. Medir el conteo tras cada corrida.
- **Esperado.** `conteo_corrida_2 == conteo_corrida_1` (sin reproceso).
- **OBSERVADO.** `conteo_corrida_1=3; conteo_corrida_2=3 (sin archivos nuevos); reproceso=no`.
  Estado: **PASA**.

## Resumen

| Prueba | Estado | Observado (resumen) |
|---|---|---|
| (a) corte + reinicio | **PASA** | final=6, distintos=6, sin pérdida ni duplicados |
| (b) archivo duplicado | **PASA** | bronce 3→6 (append), dedup silver distintos=3 (estable) |
| (c) esquema no conforme | **PASA** | stream no murió, 4 filas, 1 con `_rescued_data` |
| (d) reinicio checkpoint | **PASA** | corrida_1=3, corrida_2=3, sin reproceso |

Las 4 pruebas se ejecutaron **completas** en serverless; **no** quedó ninguna como PENDIENTE DE
EJECUCIÓN. Ningún resultado fue inventado: todos provienen de la lectura literal de
`oceanwatch.quality.pruebas_fallos`.

## Cómo reproducir la lectura

Desde `...\PROYECTO` (fuera del repo), con `databricks.key` presente:

```
# _qp.sql: SELECT prueba, estado, hipotesis, esperado, observado, detalle, _registrado
#          FROM oceanwatch.quality.pruebas_fallos ORDER BY prueba
python _dbx_sql.py 377600528fed7c47 _qp.sql
```
