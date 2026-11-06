# Evidencia — Reintento real de tarea (FEAT-002)

- **Qué demuestra:** que el mecanismo de reintentos por tarea del Job funciona DE VERDAD:
  una tarea con `max_retries: 1` que FALLA en el primer intento (attempt 0) y TERMINA en
  SUCCESS en el reintento (attempt 1), dejando la corrida en `result_state = SUCCESS`.
- **Cómo se generó:** job EFÍMERO de una sola tarea `notebook_task` **serverless** con
  `max_retries: 1`; el notebook usa la tabla efímera `oceanwatch.quality._retry_probe`
  como contador DURABLE de intentos por `{{job.run_id}}`: registra una fila por ejecución
  y hace `raise` mientras el contador sea < 2 (primer intento), y sale OK en el reintento
  (contador = 2). `databricks jobs run-now` + `jobs get-run --output json`. CLI v1.24.0.
- **Fecha:** 2026-11-05.
- **Job efímero `job_id`:** `175182281157488` (ELIMINADO al final de FEAT-002).
- **`run_id` de la demostración:** `626548229252476` (trigger `ONE_TIME`).
- **Estado:** Verificado (ejecutado).

## Por qué un job efímero y no el Job de producción

Forzar un fallo transitorio en una de las 4 tareas reales del Job de producción es frágil
y arriesga el estado de las tablas (25.9M filas). El patrón más limpio y reproducible es
un job efímero de UNA tarea cuyo único propósito es fallar-y-reintentar de forma
determinista (contador en una tabla efímera). La CONFIGURACIÓN de reintentos del Job de
producción (ingesta 2 / plata 1 / oro 1 / conciliacion 0) queda verificada por separado en
`jobs_get.md`. Este job efímero y su tabla se ELIMINAN al cerrar la feature (ver más abajo).

## Resultado (literal de `jobs get-run 626548229252476 --output json`)

```
RUN 626548229252476 | life_cycle=TERMINATED | result=SUCCESS | trigger=ONE_TIME
  tarea_que_falla_y_reintenta | TERMINATED | FAILED  | attempt=0 | run_id=789377104890365
  tarea_que_falla_y_reintenta | TERMINATED | SUCCESS | attempt=1 | run_id=1054283792195712
```

- attempt 0 (`run_id=789377104890365`): `FAILED`, `state_message = "Workload failed, see
  run output for details"` (el notebook hizo `raise RuntimeError` del fallo simulado).
- attempt 1 (`run_id=1054283792195712`): `SUCCESS` (el contador ya era 2 -> `dbutils.notebook.exit`).
- Corrida completa: `result_state = SUCCESS` porque el reintento tuvo éxito.

Confirmación del contador de intentos en la tabla efímera (antes de borrarla):

```
job_run_id        | intentos
626548229252476   | 2
```

## Relación con la configuración del Job de producción (ADR-014)

- `ingesta`: `max_retries 2` (red NOAA + arranque serverless: fallos transitorios).
- `plata_calidad`: `max_retries 1`; `oro`: `max_retries 1` (recomputo determinista
  `CREATE OR REPLACE`: 1 reintento ante arranque transitorio).
- `conciliacion`: `max_retries 0` (compuerta Write-Audit-Publish: un invariante que no
  cuadra NO es transitorio; reintentar a ciegas no lo arregla -> falla el Job).

## Limpieza (ejecutada al cerrar FEAT-002)

- Se elimina el job efímero `175182281157488` (`databricks jobs delete`).
- Se elimina la tabla efímera `oceanwatch.quality._retry_probe` (`DROP TABLE`).
- Se elimina el notebook efímero `/Users/jd.torresa1@uniandes.edu.co/_demo_reintento_fase4`.
