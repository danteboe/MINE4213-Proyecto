-- =============================================================================
-- FASE 4 (Req. 3) — Tabla de auditoría de ejecuciones del Job (Orquestación).
-- =============================================================================
-- Una fila por (corrida del Job, tarea). La pueblan los notebooks orquestadores de
-- tarea (notebooks/fase4_tarea_*.py) con un MERGE idempotente por (job_run_id, task_key)
-- construido por oceanwatch.orquestacion.merge_auditoria_sql, ejecutado vía spark.sql.
-- NO reemplaza ni modifica ninguna tabla existente: es una tabla NUEVA en el esquema
-- quality. IF NOT EXISTS la hace idempotente (reejecutable sin romper datos previos).
-- Patrón: Audit log / Observabilidad del pipeline (complementa la compuerta WAP).
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS oceanwatch.quality;

CREATE TABLE IF NOT EXISTS oceanwatch.quality.ejecuciones_job (
  job_run_id    BIGINT    COMMENT 'Id de la corrida del Job ({{job.run_id}}).',
  job_id        BIGINT    COMMENT 'Id del Job de Databricks ({{job.id}}).',
  task_key      STRING    COMMENT 'Nombre de la tarea: ingesta/plata_calidad/conciliacion/oro.',
  fecha_proceso STRING    COMMENT 'Parámetro fecha_proceso (YYYY-MM-DD) de la corrida.',
  estado        STRING    COMMENT 'Estado de la tarea (SUCCESS, FAILED, ...).',
  trigger       STRING    COMMENT 'Origen de la corrida: manual (run-now) o scheduled (cron).',
  ts_inicio     TIMESTAMP COMMENT 'Marca de tiempo de inicio de la tarea.',
  ts_fin        TIMESTAMP COMMENT 'Marca de tiempo de fin de la tarea.',
  filas_clave   STRING    COMMENT 'Conteos/métricas de la tarea serializados como JSON.',
  detalle       STRING    COMMENT 'Texto libre: diagnóstico, invariante, notas.',
  _registrado   TIMESTAMP COMMENT 'Marca de tiempo de inserción/actualización del registro.'
)
USING DELTA
COMMENT 'Auditoría de ejecuciones del Job oceanwatch_pipeline_entrega2 (Req. 3, Fase 4). Una fila por corrida+tarea, poblada por MERGE idempotente sobre (job_run_id, task_key).';
