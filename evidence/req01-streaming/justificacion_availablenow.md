# Evidencia — Justificación de `Trigger.availableNow` con comportamiento real (Req. 1)

- **Qué demuestra:** `Trigger.availableNow` es el modo de disparo correcto para la ingesta de
  OceanWatch sobre cómputo serverless (ADR-004), y su comportamiento **real observado** en los
  runs de Fase 2 cumple lo que la decisión promete: el stream procesa lo disponible y se detiene;
  dos corridas seguidas no reprocesan archivos ya consumidos ni duplican a nivel de archivo.
- **Fecha:** 2026-11-05
- **Estado:** Verificado (ejecutado)

## Por qué `availableNow` y no un trigger continuo

El entorno es **serverless** sin all-purpose cluster: no hay un clúster siempre encendido para
sostener un stream continuo, y el feed AIS de NOAA es **diario**, no de baja latencia. Un trigger
por tiempo (`ProcessingTime`) o continuo implicaría cómputo permanente y no encaja con serverless.
`Trigger.availableNow` ejecuta un micro-batch que consume todo lo disponible en `landing` y se
detiene, lo que permite orquestarlo con un Job programado (cron en UTC, Req. 3) y mantener la
idempotencia exactly-once vía `txnAppId`/`txnVersion` en `foreachBatch`. El detalle de la decisión
y sus alternativas está en **ADR-004**.

## Comportamiento REAL observado

### 1) Procesa lo disponible y se detiene (ingesta FEAT-001)

El run de ingesta real de los 3 días (`run_id 847693335141936`, ver `ejecucion_ingesta.md`)
**terminó solo** (`life_cycle_state: TERMINATED`, `result_state: SUCCESS`,
`execution_duration: 167 s`): el stream consumió los CSV de las 3 particiones `date=` y se detuvo
sin quedarse activo. Esto es precisamente la semántica de `availableNow`.

### 2) Dos corridas seguidas no reprocesan a nivel de archivo (prueba (d))

La prueba (d) de tolerancia a fallos (`run_id 497716691196081`, tabla
`oceanwatch.quality.pruebas_fallos`, ver `pruebas_tolerancia_fallos.md`) observó, de forma literal:

```
prueba=d_reinicio_checkpoint  estado=PASA
observado: conteo_corrida_1=3; conteo_corrida_2=3 (sin archivos nuevos); reproceso=no
```

Es decir: tras una primera corrida que ingirió 3 filas, una segunda corrida **sin archivos nuevos**
dejó el conteo en 3 (no reprocesó los archivos ya consumidos). El checkpoint de Auto Loader guarda
los offsets de los archivos procesados, de modo que `availableNow` no vuelve a leerlos.

### 3) El corte + reinicio no pierde ni duplica (prueba (a))

La prueba (a) observó `final=6 filas; distintos_por_clave=6` tras reiniciar desde el mismo
checkpoint, confirmando la reanudación exacta propia de `availableNow` + checkpoint (ver nota de
interpretación en `pruebas_tolerancia_fallos.md`: en serverless los archivos se consolidaron en un
único micro-batch).

## Relación con la deduplicación

`availableNow` + checkpoint evita el **reproceso de archivos** ya consumidos. La deduplicación por
**contenido** (misma clave natural `MMSI`+`BaseDateTime` reenviada en un archivo distinto) es
responsabilidad del paso bronce→silver (ADR-006), como demostró la prueba (b): bronce es
append-only y sube con el reenvío, pero `dropDuplicatesWithinWatermark` deja el conteo estable.

## Referencias

- Triggers de Structured Streaming — Databricks:
  https://docs.databricks.com/gcp/en/structured-streaming/triggers (acceso 2026-11-05).
- Checkpoints de Structured Streaming — Databricks:
  https://docs.databricks.com/gcp/en/structured-streaming/checkpoints (acceso 2026-11-05).
- ADR-004 (modo de disparo `Trigger.availableNow`), ADR-007 (estrategia de checkpoints).
- Evidencia de ingesta real: `evidence/req01-streaming/ejecucion_ingesta.md`.
- Evidencia de pruebas de fallos: `evidence/req01-streaming/pruebas_tolerancia_fallos.md`.
