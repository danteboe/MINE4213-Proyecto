# Evidencia — Corridas SCHEDULED (disparadas por el scheduler) y cron final (FEAT-003)

- **Qué demuestra:**
  1. Que el **scheduler** del Job dispara corridas automáticas reales (trigger
     `PERIODIC`/CRON), distinguibles de las run-now manuales (trigger `ONE_TIME`) de FEAT-002.
  2. Que el estado **final** del Job es el cron de **producción** `0 0 6 * * ?`
     (`America/Bogota`, `PAUSED`), restablecido tras la demostración.
- **Cómo se generó:**
  1. Se cambió el schedule del Job al cron de **demostración** de alta frecuencia
     `0 */5 * * * ?` (`America/Bogota`, `pause_status = UNPAUSED`) con
     `databricks jobs update --json @_update_demo.json`.
  2. Se dejó al scheduler lanzar corridas durante una ventana ~15 min y se capturó
     `databricks jobs list-runs --job-id 202547459111516 --output json` y
     `databricks jobs get-run <run_id> --output json` de cada corrida automática.
  3. Se **restableció** el cron de **producción** con
     `databricks jobs update --json @_update_prod.json` y se verificó con
     `databricks jobs get 202547459111516 --output json`.
  Scripts/JSON temporales (`_update_demo.json`, `_update_prod.json`, `_parse_*.py`) FUERA
  del repo; CLI v1.24.0; ENV exportado desde `databricks.key`. El mismo Job idempotente con
  la `fecha_proceso` default (`2023-06-03`) ya ingerida: cada corrida es real, rápida y NO
  destructiva (conteos estables).
- **Fecha:** 2026-11-05.
- **`job_id`:** `202547459111516`.
- **Estado:** Verificado (ejecutado).

## 1. Corridas disparadas por el scheduler (trigger `PERIODIC`)

`databricks jobs list-runs --job-id 202547459111516 --output json` tras la ventana de
demostración (6 corridas en total: 3 manuales de FEAT-002 + 3 automáticas de FEAT-003):

```
run_id=707241716873589 | trigger=PERIODIC | life=TERMINATED | result=SUCCESS | start=2026-11-06 12:45:03 UTC
run_id=544541082194734 | trigger=PERIODIC | life=TERMINATED | result=SUCCESS | start=2026-11-06 12:40:04 UTC
run_id=232561725749917 | trigger=PERIODIC | life=TERMINATED | result=SUCCESS | start=2026-11-06 12:35:03 UTC
run_id=406283395698923 | trigger=ONE_TIME | life=TERMINATED | result=SUCCESS | start=2026-11-06 12:18:35 UTC  (backfill, FEAT-002)
run_id=768406241318622 | trigger=ONE_TIME | life=TERMINATED | result=SUCCESS | start=2026-11-06 12:15:00 UTC  (idempotencia, FEAT-002)
run_id=249039226964396 | trigger=ONE_TIME | life=TERMINATED | result=SUCCESS | start=2026-11-06 12:09:04 UTC  (base, FEAT-002)
```

Las 3 corridas automáticas se dispararon **cada 5 minutos** exactos (12:35, 12:40, 12:45
UTC = 07:35, 07:40, 07:45 America/Bogota), coherente con el cron `0 */5 * * * ?`. El campo
`trigger = PERIODIC` las identifica como disparadas por el **scheduler** (cron), frente a
`trigger = ONE_TIME` de las run-now manuales de FEAT-002.

## 2. Detalle por corrida SCHEDULED (de `jobs get-run <run_id> --output json`)

Cada corrida ejecutó las 4 tareas (ingesta → plata_calidad → conciliacion → oro) en SUCCESS:

```
=== RUN 232561725749917 | trigger=PERIODIC | SUCCESS | start=2026-11-06 12:35:03 UTC | run_duration=226153 ms
    ingesta        | SUCCESS | attempt=0 | task_run_id=422425415008202  | exec~88 s
    plata_calidad  | SUCCESS | attempt=0 | task_run_id=1613841682223    | exec~84 s
    conciliacion   | SUCCESS | attempt=0 | task_run_id=477277269122745  | exec~12 s
    oro            | SUCCESS | attempt=0 | task_run_id=693346182980919  | exec~30 s

=== RUN 544541082194734 | trigger=PERIODIC | SUCCESS | start=2026-11-06 12:40:04 UTC | run_duration=165147 ms
    ingesta        | SUCCESS | attempt=0 | task_run_id=862130147110583  | exec~52 s
    plata_calidad  | SUCCESS | attempt=0 | task_run_id=1112278140068156 | exec~67 s
    conciliacion   | SUCCESS | attempt=0 | task_run_id=516560053387085  | exec~11 s
    oro            | SUCCESS | attempt=0 | task_run_id=813461376139447  | exec~26 s

=== RUN 707241716873589 | trigger=PERIODIC | SUCCESS | start=2026-11-06 12:45:03 UTC | run_duration=160063 ms
    ingesta        | SUCCESS | attempt=0 | task_run_id=427319925451488  | exec~49 s
    plata_calidad  | SUCCESS | attempt=0 | task_run_id=991945458381728  | exec~64 s
    conciliacion   | SUCCESS | attempt=0 | task_run_id=618773159265936  | exec~11 s
    oro            | SUCCESS | attempt=0 | task_run_id=1055517852322851 | exec~26 s
```

## 3. Auditoría poblada por las corridas SCHEDULED

`SELECT job_run_id, task_key, fecha_proceso, estado FROM oceanwatch.quality.ejecuciones_job
WHERE job_run_id IN (232561725749917, 544541082194734, 707241716873589)` → **12 filas**
(3 corridas × 4 tareas), todas `estado = SUCCESS`, `fecha_proceso = 2023-06-03` (el default
del Job, que es el que usa una corrida disparada por el scheduler). El total de la tabla de
auditoría pasó de 12 (FEAT-002) a **24** filas. Las tablas del pipeline quedaron **intactas**
(bronce 25.897.493, plata 25.857.101, cuarentena 39.734): el disparo automático es idempotente.

## 4. Restablecimiento del cron de PRODUCCIÓN (estado final del Job)

`databricks jobs get 202547459111516 --output json` → bloque `schedule` **final** (literal):

```json
{
  "pause_status": "PAUSED",
  "quartz_cron_expression": "0 0 6 * * ?",
  "timezone_id": "America/Bogota"
}
```

**Confirmación:** el estado FINAL del Job es el cron de **producción** `0 0 6 * * ?` en
`America/Bogota`, `PAUSED` (06:00 Bogota = 11:00 UTC, margen tras la publicación diaria del
feed NOAA; ADR-015). El cron de alta frecuencia de la demostración quedó **revertido**. No se
disparó ninguna corrida PERIODIC después de las 12:45 UTC (el `list-runs` posterior no añadió
corridas automáticas nuevas), lo que confirma que la restauración surtió efecto.

## 5. UTC vs. America/Bogota (justificación)

El cómputo serverless corre en **UTC** por defecto (ADR-004), pero el Quartz cron del Job
respeta el `timezone_id` configurado. Se fija `America/Bogota` para que el horario sea
legible por el operador local sin aritmética de offset: las corridas de demostración se
observaron a :35/:40/:45 UTC, es decir :35/:40/:45 en Bogota también para la cadencia de 5
min, pero la hora **absoluta** del cron de producción (06:00 Bogota) se traduce a 11:00 UTC.
Detalle y alternativas en [ADR-015](../../docs/adr/ADR-015-schedule-monitoreo.md).
