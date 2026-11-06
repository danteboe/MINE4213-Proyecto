# Evidencia — Tabla de auditoría del Job y no-ruptura de tablas existentes (FEAT-001)

- **Qué:** creación idempotente de `oceanwatch.quality.ejecuciones_job` (DDL como código)
  y comprobación de que ninguna tabla existente del pipeline fue modificada.
- **Cómo:** `resources/sql/fase4_auditoria.sql` ejecutado vía `_dbx_sql.py` (SQL Statement
  Execution API, warehouse serverless `377600528fed7c47`). Script temporal FUERA del repo.
- **Fecha:** 2026-11-05.
- **Estado:** Verificado (ejecutado).

## Diagnóstico de fricción (síntoma -> causa -> solución)

- **Síntoma:** el DDL falló con `PARSE_SYNTAX_ERROR` cerca de `';'` dentro del `COMMENT`
  de la tabla.
  **Causa:** el divisor de sentencias (split naíf por `;`, usado por `_dbx_sql.py` y por
  los notebooks) cortó el `COMMENT` que contenía un `;` literal.
  **Solución:** se reemplazó el `;` del texto del `COMMENT` por `,` para que el DDL sea
  una sola sentencia bajo cualquier divisor por `;` (los `.sql` del proyecto no usan `;`
  dentro de literales). Reejecutado: OK.

## Salida literal

```
==== 1) DDL fase4_auditoria.sql (idempotente) ====
OK    | CREATE SCHEMA IF NOT EXISTS oceanwatch.quality
OK    | CREATE TABLE IF NOT EXISTS oceanwatch.quality.ejecuciones_job ( job_ru

==== 2) SHOW TABLES IN oceanwatch.quality ====
    ['quality', 'ejecuciones_job', 'false']
    ['quality', 'ingesta_metricas', 'false']
    ['quality', 'metricas_calidad', 'false']
    ['quality', 'pruebas_fallos', 'false']

==== 3) Conteos de tablas existentes (no se rompió nada) ====
   bronce ais_raw = 25897493
   plata ais_clean = 25857101
   cuarentena ais_rejected = 39734
   silver celda_en_agua = 726513
   gold world_port_index = 3669
   quality ingesta_metricas = 3
   quality ejecuciones_job (NUEVA) = 0

==== 4) DESCRIBE de la tabla de auditoria ====
    ['job_run_id', 'bigint', 'Id de la corrida del Job ({{job.run_id}}).']
    ['job_id', 'bigint', 'Id del Job de Databricks ({{job.id}}).']
    ['task_key', 'string', 'Nombre de la tarea: ingesta/plata_calidad/conciliacion/oro.']
    ['fecha_proceso', 'string', 'Parámetro fecha_proceso (YYYY-MM-DD) de la corrida.']
    ['estado', 'string', 'Estado de la tarea (SUCCESS, FAILED, ...).']
    ['trigger', 'string', 'Origen de la corrida: manual (run-now) o scheduled (cron).']
    ['ts_inicio', 'timestamp', 'Marca de tiempo de inicio de la tarea.']
    ['ts_fin', 'timestamp', 'Marca de tiempo de fin de la tarea.']
    ['filas_clave', 'string', 'Conteos/métricas de la tarea serializados como JSON.']
    ['detalle', 'string', 'Texto libre: diagnóstico, invariante, notas.']
    ['_registrado', 'timestamp', 'Marca de tiempo de inserción/actualización del registro.']
```

## Conclusiones

- La tabla **nueva** `oceanwatch.quality.ejecuciones_job` existe, con el esquema exacto
  del DDL, 0 filas (se poblará al correr el Job en FEAT-002).
- Los conteos de bronce (25.897.493), plata (25.857.101), cuarentena (39.734),
  `silver.celda_en_agua` (726.513), `gold.world_port_index` (3.669) e
  `ingesta_metricas` (3) **coinciden exactamente** con `context.estado_real_verificado`:
  no se modificó ni borró ninguna tabla existente.
