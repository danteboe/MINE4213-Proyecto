# Evidencia — `databricks jobs get` del Job desplegado (FEAT-002)

- **Qué demuestra:** que el Job `oceanwatch_pipeline_entrega2` fue CREADO de verdad en
  Databricks (vía `databricks bundle deploy`) con las 4 tareas, dependencias lineales,
  reintentos por tarea, schedule de producción (PAUSED) y notificaciones por correo.
- **Cómo se generó:** `databricks bundle deploy` desde la raíz del repo con
  `DATABRICKS_HOST`/`DATABRICKS_TOKEN` exportados FUERA del repo desde `databricks.key`;
  luego `databricks jobs get 202547459111516 --output json` (CLI v1.24.0).
- **Fecha:** 2026-11-05.
- **`job_id` REAL:** `202547459111516`.
- **Estado:** Verificado (ejecutado).

## Resumen verificado

- 4 tareas `notebook_task` **serverless** (sin `new_cluster`), apuntando a los notebooks
  `/Workspace/Users/jd.torresa1@uniandes.edu.co/oceanwatch/notebooks/fase4_tarea_*`.
- Dependencias lineales: `ingesta` -> `plata_calidad` -> `conciliacion` -> `oro`
  (`depends_on` + `run_if: ALL_SUCCESS`).
- Parámetro del Job `fecha_proceso` (default `2023-06-03`) propagado a cada tarea vía
  `base_parameters` con `{{job.parameters.fecha_proceso}}` + widgets de identidad
  `{{job.run_id}}` / `{{job.id}}` / `{{task.name}}`.
- Reintentos por tarea: `ingesta` 2, `plata_calidad` 1, `oro` 1, `conciliacion` 0
  (`min_retry_interval_millis` 60000 donde aplica).
- Schedule de producción Quartz `0 0 6 * * ?`, `timezone_id America/Bogota`,
  `pause_status PAUSED` (el scheduling automático es FEAT-003).
- `email_notifications` on_success/on_failure/on_duration_warning a
  `jd.torresa1@uniandes.edu.co`; `health` RUN_DURATION_SECONDS > 3600.
- `deployment.kind = BUNDLE` (gestionado por el Databricks Asset Bundle).

## Salida literal de `databricks jobs get 202547459111516 --output json`

```json
{
  "created_time": 1793966856584,
  "creator_user_name": "jd.torresa1@uniandes.edu.co",
  "job_id": 202547459111516,
  "run_as_user_name": "jd.torresa1@uniandes.edu.co",
  "settings": {
    "deployment": {
      "kind": "BUNDLE",
      "metadata_file_path": "/Workspace/Users/jd.torresa1@uniandes.edu.co/.bundle/oceanwatch_entrega2/produccion/state/metadata.json"
    },
    "description": "Pipeline medallion AIS (NOAA): ingesta Auto Loader -> plata/calidad (CTAS) -> conciliación (compuerta Write-Audit-Publish) -> oro (CTAS). Cómputo serverless. Req. 3 (Orquestación) de la Entrega 2, MINE 4213.",
    "edit_mode": "UI_LOCKED",
    "email_notifications": {
      "on_duration_warning_threshold_exceeded": ["jd.torresa1@uniandes.edu.co"],
      "on_failure": ["jd.torresa1@uniandes.edu.co"],
      "on_success": ["jd.torresa1@uniandes.edu.co"]
    },
    "format": "MULTI_TASK",
    "health": { "rules": [ { "metric": "RUN_DURATION_SECONDS", "op": "GREATER_THAN", "value": 3600 } ] },
    "max_concurrent_runs": 1,
    "name": "oceanwatch_pipeline_entrega2",
    "parameters": [ { "default": "2023-06-03", "name": "fecha_proceso" } ],
    "queue": { "enabled": true },
    "schedule": {
      "pause_status": "PAUSED",
      "quartz_cron_expression": "0 0 6 * * ?",
      "timezone_id": "America/Bogota"
    },
    "tasks": [
      {
        "depends_on": [ { "task_key": "plata_calidad" } ],
        "notebook_task": {
          "base_parameters": { "fecha_proceso": "{{job.parameters.fecha_proceso}}", "job_id": "{{job.id}}", "job_run_id": "{{job.run_id}}", "task_key": "{{task.name}}" },
          "notebook_path": "/Workspace/Users/jd.torresa1@uniandes.edu.co/oceanwatch/notebooks/fase4_tarea_conciliacion",
          "source": "WORKSPACE"
        },
        "run_if": "ALL_SUCCESS",
        "task_key": "conciliacion"
      },
      {
        "max_retries": 2,
        "min_retry_interval_millis": 60000,
        "notebook_task": {
          "base_parameters": { "fecha_proceso": "{{job.parameters.fecha_proceso}}", "job_id": "{{job.id}}", "job_run_id": "{{job.run_id}}", "task_key": "{{task.name}}" },
          "notebook_path": "/Workspace/Users/jd.torresa1@uniandes.edu.co/oceanwatch/notebooks/fase4_tarea_ingesta",
          "source": "WORKSPACE"
        },
        "run_if": "ALL_SUCCESS",
        "task_key": "ingesta"
      },
      {
        "depends_on": [ { "task_key": "conciliacion" } ],
        "max_retries": 1,
        "min_retry_interval_millis": 60000,
        "notebook_task": {
          "base_parameters": { "fecha_proceso": "{{job.parameters.fecha_proceso}}", "job_id": "{{job.id}}", "job_run_id": "{{job.run_id}}", "task_key": "{{task.name}}" },
          "notebook_path": "/Workspace/Users/jd.torresa1@uniandes.edu.co/oceanwatch/notebooks/fase4_tarea_oro",
          "source": "WORKSPACE"
        },
        "run_if": "ALL_SUCCESS",
        "task_key": "oro"
      },
      {
        "depends_on": [ { "task_key": "ingesta" } ],
        "max_retries": 1,
        "min_retry_interval_millis": 60000,
        "notebook_task": {
          "base_parameters": { "fecha_proceso": "{{job.parameters.fecha_proceso}}", "job_id": "{{job.id}}", "job_run_id": "{{job.run_id}}", "task_key": "{{task.name}}" },
          "notebook_path": "/Workspace/Users/jd.torresa1@uniandes.edu.co/oceanwatch/notebooks/fase4_tarea_plata",
          "source": "WORKSPACE"
        },
        "run_if": "ALL_SUCCESS",
        "task_key": "plata_calidad"
      }
    ],
    "timeout_seconds": 0,
    "webhook_notifications": {}
  }
}
```

## Despliegue (salida literal de `databricks bundle deploy`)

```
Uploading bundle files to /Workspace/Users/jd.torresa1@uniandes.edu.co/.bundle/oceanwatch_entrega2/produccion/files...
Created jobs.oceanwatch_pipeline_entrega2
Files: 138 uploaded, 0 deleted
Resources: 1 created, 0 changed, 0 deleted, 0 unchanged
```
