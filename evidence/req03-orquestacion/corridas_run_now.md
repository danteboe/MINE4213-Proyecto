# Evidencia — Corrida BASE run-now del Job (FEAT-002)

- **Qué demuestra:** que el Job `oceanwatch_pipeline_entrega2` ejecuta el pipeline
  end-to-end con `fecha_proceso = 2023-06-03` y termina en `result_state = SUCCESS`
  con las 4 tareas OK (ingesta -> plata_calidad -> conciliacion -> oro).
- **Cómo se generó:** `databricks jobs run-now --json @_run_base.json`
  (`{"job_id":202547459111516,"job_parameters":{"fecha_proceso":"2023-06-03"}}`) y
  polling de `databricks jobs get-run <run_id> --output json` hasta
  `life_cycle_state = TERMINATED`. CLI v1.24.0; ENV exportado FUERA del repo desde
  `databricks.key`.
- **Fecha:** 2026-11-05.
- **`job_id`:** `202547459111516`.
- **`run_id` de la corrida base:** `249039226964396` (trigger `ONE_TIME` = manual).
- **Estado:** Verificado (ejecutado).

## Resultado de la corrida (literal de `jobs get-run`)

```
RUN 249039226964396 | life_cycle=TERMINATED | result=SUCCESS | trigger=ONE_TIME
  ingesta        | TERMINATED  | SUCCESS | attempt=0 | run_id=632126531199658
  plata_calidad  | TERMINATED  | SUCCESS | attempt=0 | run_id=702492582880484
  conciliacion   | TERMINATED  | SUCCESS | attempt=0 | run_id=331661400464526
  oro            | TERMINATED  | SUCCESS | attempt=0 | run_id=236147110169698
```

## Estados por tarea y duraciones (de `jobs get-run --output json`)

| Tarea          | task run_id        | result  | duración ejecución | intento |
|----------------|--------------------|---------|--------------------|---------|
| ingesta        | 632126531199658    | SUCCESS | ~96 s              | 0       |
| plata_calidad  | 702492582880484    | SUCCESS | ~75 s              | 0       |
| conciliacion   | 331661400464526    | SUCCESS | ~11 s              | 0       |
| oro            | 236147110169698    | SUCCESS | ~28 s              | 0       |

Duración total de la corrida: ~223 s (`run_duration` = 223439 ms). Las tareas de
capas (`plata_calidad`, `oro`) ejecutaron el SQL CTAS fiable vía `spark.sql`, NO
`df.write.saveAsTable` (camino serverless verificado en Fase 3). La compuerta
`conciliacion` (Write-Audit-Publish) verificó el invariante y dejó pasar la publicación
de `oro`.

## Trigger (manual vs scheduled)

`databricks jobs list-runs --job-id 202547459111516 --output json` lista esta corrida
con `trigger = ONE_TIME` (run-now manual). El scheduling automático (cron) es alcance
de FEAT-003. No se forzó ningún callejón: la primera corrida terminó en SUCCESS sin
necesidad de diagnósticos ni correcciones (el Job reutiliza los notebooks/SQL ya
verificados en fases 1-3).
