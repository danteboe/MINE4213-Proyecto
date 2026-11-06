# Runbook de operación — Job de orquestación OceanWatch (Req. 3, Fase 4)

Procedimientos operativos del Job de Databricks `oceanwatch_pipeline_entrega2`
(`job_id` real **202547459111516**), que orquesta el pipeline medallion AIS en serverless:

```
ingesta  ->  plata_calidad  ->  conciliacion (compuerta WAP)  ->  oro
```

El Job está definido como código en
[`resources/jobs/oceanwatch_pipeline.job.yml`](../resources/jobs/oceanwatch_pipeline.job.yml)
(Databricks Asset Bundle). Diseño en [ADR-013](adr/ADR-013-diseno-job-orquestacion.md),
idempotencia/reintentos en [ADR-014](adr/ADR-014-idempotencia-reintentos.md), schedule y
monitoreo en [ADR-015](adr/ADR-015-schedule-monitoreo.md). DAG en
[`architecture/fase4-dag-orquestacion.md`](architecture/fase4-dag-orquestacion.md).

> **Secretos.** `DATABRICKS_HOST`/`DATABRICKS_TOKEN` viven FUERA del repo (en
> `databricks.key`). Nunca se copian al repositorio. La CLI de Databricks (v1.24.0) lee las
> variables de entorno exportadas desde esa key. El warehouse serverless de lectura es
> `377600528fed7c47`.

## 0. Preparación del entorno (CLI)

Exportar las credenciales desde la key (PowerShell), fuera del repo:

```powershell
foreach ($line in Get-Content <ruta>\databricks.key) {
  if ($line -match 'DATABRICKS_HOST\s*=\s*(.+)')  { $env:DATABRICKS_HOST  = $matches[1].Trim().TrimEnd('/') }
  if ($line -match 'DATABRICKS_TOKEN\s*=\s*(.+)') { $env:DATABRICKS_TOKEN = $matches[1].Trim() }
}
```

> Nota operativa: los comandos `list*` requieren `--output json`. Redirigir la salida a un
> archivo (`> _salida.json`) y parsearla; PowerShell escribe UTF-16 (BOM), así que al leer
> con Python hay que decodificar `utf-16`/`utf-8-sig`.

## 1. Lanzar el Job (run-now con fecha_proceso)

```powershell
databricks jobs run-now --json '{"job_id":202547459111516,"job_parameters":{"fecha_proceso":"2023-06-03"}}'
```

Devuelve un `run_id`. Hacer *polling* hasta terminar:

```powershell
databricks jobs get-run --run-id <run_id> --output json
# esperar life_cycle_state = TERMINATED y result_state = SUCCESS
```

Una corrida base real tardó ~223 s con las 4 tareas en SUCCESS (ver
[`evidence/req03-orquestacion/corridas_run_now.md`](../evidence/req03-orquestacion/corridas_run_now.md)).

## 2. Relanzar / reparar tras un fallo (repair run)

Si una corrida falla en una tarea intermedia, no hace falta rehacer todo: reparar desde la
tarea que falló (Databricks reejecuta esa tarea y las que dependen de ella).

```powershell
# Ver qué tarea falló:
databricks jobs get-run --run-id <run_id> --output json
# Reparar reejecutando solo las tareas indicadas (y sus dependientes):
databricks jobs repair-run --run-id <run_id> --rerun-tasks plata_calidad,conciliacion,oro
```

El pipeline es **idempotente** (ADR-014): reejecutar una tarea para la misma `fecha_proceso`
reproduce el mismo estado (plata/oro con `CREATE OR REPLACE`, ingesta con checkpoint +
`txnAppId`/`txnVersion`, auditoría con `MERGE`). Es seguro reparar/relanzar.

## 3. Backfill (reprocesar una fecha pasada)

El backfill es simplemente pasar una `fecha_proceso` anterior:

```powershell
databricks jobs run-now --json '{"job_id":202547459111516,"job_parameters":{"fecha_proceso":"2023-06-01"}}'
```

Verificado real: `fecha_proceso=2023-06-01` (`run_id` 406283395698923) terminó en SUCCESS y
los conteos quedaron estables (Auto Loader no re-ingiere lo ya ingerido; plata/oro recomputan
determinísticamente). Evidencia:
[`backfill_idempotente.md`](../evidence/req03-orquestacion/backfill_idempotente.md).

## 4. Leer la tabla de auditoría `oceanwatch.quality.ejecuciones_job`

Una fila por corrida+tarea (`MERGE` idempotente por `(job_run_id, task_key)`):

```sql
SELECT job_run_id, task_key, fecha_proceso, estado, ts_inicio, ts_fin, filas_clave
FROM oceanwatch.quality.ejecuciones_job
ORDER BY job_run_id, ts_inicio;
```

Columnas útiles: `estado` (SUCCESS/FAILED), `filas_clave` (JSON con los conteos por tarea:
total_bronce, plata, cuarentena, invariante, conteos de oro), `detalle` (diagnóstico libre).
Para ver solo la última corrida, filtrar por el `job_run_id` mayor. Esta tabla es el
mecanismo de **monitoreo persistente** (ADR-015).

## 5. Dónde ver las notificaciones

El Job envía correo a `jd.torresa1@uniandes.edu.co` en tres eventos (configurados en el YAML):

- `on_success` — corrida terminada OK.
- `on_failure` — corrida fallida (p. ej. la compuerta de conciliación hizo `raise`).
- `on_duration_warning_threshold_exceeded` — la corrida superó el umbral de salud
  `RUN_DURATION_SECONDS > 3600` (1 hora).

Verificar/editar en la UI: *Workflows → oceanwatch_pipeline_entrega2 → Notifications*, o con
`databricks jobs get --job-id 202547459111516 --output json` (bloque `email_notifications` y
`health`).

## 6. Limpiar checkpoints de Auto Loader (reset de ingesta)

El checkpoint de la ingesta vive en el Volume `oceanwatch.bronze.checkpoints`, subruta
`bronze/ais_raw` (ver `conf/config.yaml` → `ingesta.checkpoint`). Da exactly-once: Auto Loader
**no** reprocesa archivos ya vistos.

- **No borrar el checkpoint en operación normal**: es lo que garantiza que un relanzamiento no
  duplique filas en bronce.
- **Solo** borrarlo para un *reset intencional* (p. ej. reingerir desde cero un landing
  limpio). Procedimiento:
  ```sql
  -- Inspeccionar
  LIST '/Volumes/oceanwatch/bronze/checkpoints/bronze/ais_raw';
  ```
  ```python
  # En un notebook serverless (acción destructiva, intencional):
  dbutils.fs.rm("/Volumes/oceanwatch/bronze/checkpoints/bronze/ais_raw", recurse=True)
  ```
  Tras borrarlo, la siguiente ingesta reconstruye el checkpoint y reingiere lo que haya en
  `landing/csv/`; la idempotencia a nivel bronce la sostiene además `txnAppId`
  (`oceanwatch_bronze_ais_raw`) / `txnVersion` (ADR-004). **No** combinar un reset de
  checkpoint con un landing que ya tenga los CSV si no se quiere reingerir.

## 7. Diagnóstico por tarea (qué revisar cuando falla)

| Tarea | Síntomas típicos | Qué revisar / acción |
|---|---|---|
| **ingesta** | fallo de descarga NOAA; timeout de arranque serverless; error de checkpoint | Red hacia NOAA (`coast.noaa.gov`); que el `.zip` de la fecha exista; estado del checkpoint. Tiene `max_retries: 2` para fallos transitorios. Si persiste, revisar `landing/zips/` y `landing/csv/`. |
| **plata_calidad** | `INTERNAL_ERROR` en escritura Delta; fallo de CTAS | El camino fiable es **SQL CTAS vía `spark.sql`**, NO `df.write.saveAsTable` (fricción serverless real: `INTERNAL_ERROR` sobre 25.9M filas en Spark Connect — ADR-013). Si falla, revisar `resources/sql/fase3_materializar_plata.sql` / `fase3_enriquecer_en_agua.sql`. `max_retries: 1`. |
| **conciliacion** | el Job **falla aquí a propósito** | La compuerta WAP hace `raise` si `bronce != plata + cuarentena + duplicados`. **NO reintentar a ciegas** (`max_retries: 0`): un invariante que no cuadra es un **fallo de datos**, no transitorio. Diagnosticar por qué no concilia (dedup, cuarentena, ingesta incompleta) antes de reejecutar. |
| **oro** | fallo de CTAS; WPI no cargado | Mismo patrón que plata (CTAS vía `spark.sql`, `resources/sql/fase3_materializar_oro.sql`). Revisar que `gold.world_port_index` exista (tabla de referencia). `max_retries: 1`. |

Regla general: **conciliación que falla = problema de datos, no se reintenta**; el resto de
tareas sí reintentan fallos transitorios según su `max_retries`.

## 8. Schedule del Job

- **Producción (estado final):** cron Quartz `0 0 6 * * ?`, timezone `America/Bogota`,
  `pause_status = PAUSED`. 06:00 Bogota = 11:00 UTC (margen tras la publicación diaria del
  feed NOAA). Serverless corre en UTC, pero el Job respeta `timezone_id` (ADR-015).
- **Demostración (temporal):** cron `0 */5 * * * ?` (cada 5 min), `UNPAUSED`, usado por una
  ventana corta para capturar corridas automáticas y luego **restablecer producción**.

Cambiar el schedule con:

```powershell
databricks jobs update --json '{"job_id":202547459111516,"new_settings":{"schedule":{"quartz_cron_expression":"0 0 6 * * ?","timezone_id":"America/Bogota","pause_status":"PAUSED"}}}'
databricks jobs get --job-id 202547459111516 --output json   # verificar
```

Evidencia de las corridas automáticas y de la restauración del cron de producción:
[`corridas_scheduled.md`](../evidence/req03-orquestacion/corridas_scheduled.md).
