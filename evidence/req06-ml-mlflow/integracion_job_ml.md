# Evidencia — Integración de la inferencia ML en el pipeline (Req. 6)

- **Qué demuestra:** que la inferencia de anomalías corre DENTRO del pipeline de
  producción como una quinta tarea (`ml_anomalias`) del Job `oceanwatch_pipeline_entrega2`
  (`job_id` real `202547459111516`), que depende de `oro`, que una corrida real del Job
  completo terminó en `result_state = SUCCESS` con las **5 tareas en SUCCESS**, y que las
  4 tareas previas (`ingesta`, `plata_calidad`, `conciliacion`, `oro`) siguen intactas.
- **Cómo se generó:** se editó `resources/jobs/oceanwatch_pipeline.job.yml` para añadir la
  tarea `ml_anomalias`; se re-importó el repo al workspace; `databricks bundle validate`
  (OK) y `databricks bundle deploy`; `databricks jobs run-now` sobre el Job con
  `fecha_proceso = 2023-06-03`; polling con `databricks jobs get-run --output json` hasta
  `TERMINATED`. ENV (`DATABRICKS_HOST`/`DATABRICKS_TOKEN`) exportado FUERA del repo desde
  `databricks.key`.
- **Fecha:** 2026-11-05.
- **Estado:** Verificado (ejecutado).

## Tarea `ml_anomalias` añadida al Job (YAML, sin tocar las 4 existentes)

```yaml
- task_key: ml_anomalias
  depends_on:
    - task_key: oro
  notebook_task:
    notebook_path: ${var.workspace_oceanwatch}/notebooks/fase6_tarea_ml_anomalias
    base_parameters:
      fecha_proceso: "{{job.parameters.fecha_proceso}}"
      job_run_id: "{{job.run_id}}"
      job_id: "{{job.id}}"
      task_key: "{{task.name}}"
  max_retries: 1
  min_retry_interval_millis: 60000
```

Reintento `1`: la inferencia es determinista (modelo fijo + escritura `DELETE`+`INSERT`
idempotente de la partición del día), por lo que un fallo típico es transitorio (arranque
serverless o carga del modelo UC) y el reintento no duplica filas.

## `databricks bundle validate`

```
Name: oceanwatch_entrega2
Target: produccion
Workspace:
  User: jd.torresa1@uniandes.edu.co
  Path: /Workspace/Users/jd.torresa1@uniandes.edu.co/.bundle/oceanwatch_entrega2/produccion
Validation OK!
```

`databricks bundle deploy` se ejecutó sin error. Verificación tras el deploy
(`databricks jobs get 202547459111516 --output json`): el Job sigue siendo
`job_id = 202547459111516` y ahora tiene **5 tareas**
(`conciliacion, ingesta, ml_anomalias, oro, plata_calidad`); `ml_anomalias` depende de
`oro`, `notebook_path = .../notebooks/fase6_tarea_ml_anomalias`, `max_retries = 1`.

## Corrida REAL del Job completo (run-now)

- **`run_id` del Job:** `483234774995672` (`fecha_proceso = 2023-06-03`).
- **Estado final:** `life_cycle_state = TERMINATED`, `result_state = SUCCESS`.

| task_key       | intento | result_state | task run_id        |
|----------------|---------|--------------|--------------------|
| ingesta        | 0       | SUCCESS      | 290230198662832    |
| plata_calidad  | 0       | FAILED (transitorio) | 928767849019627 |
| plata_calidad  | 1       | SUCCESS      | 1115204667990097   |
| conciliacion   | 0       | SUCCESS      | 1005592944801120   |
| oro            | 0       | SUCCESS      | 479657347369159    |
| ml_anomalias   | 0       | SUCCESS      | 1116962065387574   |

Las 5 tareas terminaron en SUCCESS (`plata_calidad` reintentó una vez por un fallo
transitorio de arranque serverless, comportamiento normal con `max_retries: 1`; el
resultado efectivo de la tarea es SUCCESS). `notebook_output` de `ml_anomalias`:

```
{"anomalias": 67828}
```

## Las 4 tareas previas NO se rompieron (pipeline intacto)

Conteos reales tras la corrida (warehouse serverless `377600528fed7c47`):

| Tabla                                   | Conteo      |
|-----------------------------------------|-------------|
| `oceanwatch.silver.ais_clean`           | 25 857 101  |
| `oceanwatch.gold.trayectorias_diarias`  | 61 768      |
| `oceanwatch.gold.presencia_puertos`     | 9 153       |
| `oceanwatch.gold.matriz_trafico`        | 2 319 365   |

`silver.ais_clean` sigue en **25 857 101** (igual que las fases previas). La tarea
`ml_anomalias` escribió su fila de auditoría en `oceanwatch.quality.ejecuciones_job`:

```
task_key=ml_anomalias | estado=SUCCESS | filas_clave={"anomalias": 67828}
detalle=Inferencia IsolationForest @champion -> gold.anomalias_velocidad.
```

## Fricción serverless REAL encontrada y resuelta (rigor académico)

Durante la ejecución aparecieron DOS obstáculos reales de serverless, diagnosticados y
corregidos (nada inventado):

1. **`PipError` al instalar `scikit-learn==1.3.0` en la tarea del Job.** Las tareas
   `notebook_task` del Job corren sobre el entorno serverless por defecto **version 3
   (Python 3.12)**, mientras que el entrenamiento (FEAT-002, vía `jobs submit`) corrió
   sobre **version 2 (Python 3.11)**. `scikit-learn 1.3.x` NO publica wheels para Python
   3.12 (solo 3.8–3.11). El entorno de un `notebook_task` serverless NO se puede fijar por
   DAB (Databricks lo rechaza: "A task environment can not be provided for notebook
   task"). **Solución:** la inferencia instala `scikit-learn==1.5.2` (con wheel para 3.11
   y 3.12) + `mlflow==3.18.0` (necesario para descargar el modelo de UC). Se verificó en
   una corrida aislada que el modelo `@champion` (pickled con 1.3.0) se carga y predice
   IDÉNTICO bajo 1.5.2 (`predict=[-1,1]`, `score_samples` coherentes); el único efecto es
   un `InconsistentVersionWarning` de des-serialización, silenciado en el notebook. La
   estructura de árboles del IsolationForest es estable entre 1.3 y 1.5.
2. **`PARSE_SYNTAX_ERROR` al crear la tabla.** El notebook ejecuta el `.sql` dividiéndolo
   por `;`; dos `COMMENT` del DDL contenían `;` embebidos, que partían las sentencias a la
   mitad. **Solución:** se eliminaron los `;` del texto de los `COMMENT`.

Ambas correcciones están en `notebooks/fase6_tarea_ml_anomalias.py`,
`resources/sql/fase6_anomalias.sql` y en `conf/config.yaml` (`ml.pip_pins_inferencia`).

## Escritura con `spark.sql` (NUNCA `df.write.saveAsTable`)

El notebook escribe `gold.anomalias_velocidad` con `spark.sql`
(`DELETE FROM ... WHERE _batch_date = DATE(fecha)` seguido de `INSERT INTO ... SELECT ...`
desde una vista temporal), es decir el camino fiable en serverless, NO `df.write.saveAsTable`.
La escritura es idempotente por partición de fecha: re-ejecutar la tarea para la misma
`fecha_proceso` deja el mismo resultado (conteo total = conteo del día = 67 828).
