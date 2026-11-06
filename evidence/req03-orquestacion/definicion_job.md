# Evidencia — Definición del Job como código (FEAT-001)

- **Qué:** validación de la definición del Job de orquestación como Databricks Asset Bundle (DAB).
- **Cómo:** `databricks bundle validate` (CLI v1.24.0) desde la raíz del repo, con
  `DATABRICKS_HOST`/`DATABRICKS_TOKEN` exportados FUERA del repo desde `databricks.key`.
- **Fecha:** 2026-11-05.
- **Estado:** Verificado (ejecutado).

## Definición adoptada

**Databricks Asset Bundle (DAB)**, no JSON Jobs API. Archivos versionados:

- `databricks.yml` (bundle `oceanwatch_entrega2`; target `produccion`, `mode: production`,
  `workspace.root_path` fijado; host/token por ENV, SIN secretos; `include: resources/jobs/*.yml`).
- `resources/jobs/oceanwatch_pipeline.job.yml` (Job `oceanwatch_pipeline_entrega2`):
  - 4 tareas `notebook_task` **serverless** (sin `new_cluster`): `ingesta` ->
    `plata_calidad` -> `conciliacion` -> `oro` (`depends_on` lineal).
  - Parámetro `fecha_proceso` (default `2023-06-03`) propagado a cada tarea vía
    `base_parameters` con `{{job.parameters.fecha_proceso}}` + widgets
    `{{job.run_id}}`/`{{job.id}}`/`{{task.name}}`.
  - Reintentos por tarea: ingesta 2, plata_calidad 1, oro 1, conciliacion 0
    (`min_retry_interval_millis` 60000 donde aplica).
  - Schedule de producción Quartz `0 0 6 * * ?`, `timezone_id America/Bogota`,
    `pause_status PAUSED`.
  - `email_notifications` on_success/on_failure/on_duration_warning a
    `jd.torresa1@uniandes.edu.co` (+ `health` RUN_DURATION_SECONDS > 3600).
  - Comentarios YAML con el lugar de los nodos futuros `ml` (Fase 6) y
    `refresh_dashboard` (Fase 7).

## Diagnóstico de fricción (síntoma -> causa -> solución)

1. **Síntoma:** `bundle validate` -> `invalid character "{" in host name` / "Variable
   interpolation is not supported ... at workspace.host".
   **Causa:** se puso `workspace.host: ${DATABRICKS_HOST}`; la interpolación `${...}` no
   está permitida en campos de autenticación.
   **Solución:** se quitó `workspace.host`; la CLI resuelve el host desde la variable de
   entorno `DATABRICKS_HOST` (exportada desde `databricks.key`).
2. **Síntoma:** `target with 'mode: production' must set 'workspace.root_path'`.
   **Causa:** `mode: production` exige una ruta de despliegue única.
   **Solución:** se fijó `root_path:
   /Workspace/Users/jd.torresa1@uniandes.edu.co/.bundle/${bundle.name}/${bundle.target}`.

## Salida literal (tras resolver la fricción)

```
Databricks CLI v1.24.0
---- bundle validate ----
Name: oceanwatch_entrega2
Target: produccion
Workspace:
  User: jd.torresa1@uniandes.edu.co
  Path: /Workspace/Users/jd.torresa1@uniandes.edu.co/.bundle/oceanwatch_entrega2/produccion
Validation OK!
```

## Pendiente de ejecución (FEAT-002)

- `databricks bundle deploy` (crear/actualizar el Job real; guardar el `job_id`).
- `databricks jobs run-now --job-id <id> --json '{"job_parameters":{"fecha_proceso":"2023-06-03"}}'`
  y polling de `jobs get-run` hasta `TERMINATED`/`SUCCESS`.
