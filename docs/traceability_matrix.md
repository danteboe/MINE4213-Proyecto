# Matriz de trazabilidad — OceanWatch Analytics (Entrega 2)

Una fila por requisito de la rúbrica de **Entrega 2**. Los ADRs de Fase 1 ya están rellenados
donde aplican; el resto queda "Pendiente" para fases posteriores.

| Req | Peso | Criterio de aceptación | ADR(s) | Código | Evidencia | Bitácora | Estado |
|---|---|---|---|---|---|---|---|
| 1 | 15% | Ingesta incremental de zips NOAA vía descompresión + Auto Loader con esquema explícito, resistencia a archivos nuevos y tolerancia a fallos | ADR-003, ADR-004, ADR-006, ADR-007, ADR-008 | `src/oceanwatch/ingest/` (schema, paths, download, autoloader, dedup) + `notebooks/fase2_*` | `evidence/req01-streaming/` (ejecucion_ingesta, pruebas_tolerancia_fallos, justificacion_availablenow, verificacion_esquema_real) | 2026-11-05 | Verificado (ejecutado) |
| 2 | 15% | Capas medallion con reglas de calidad y cuarentena | ADR-001, ADR-006, ADR-009, ADR-010, ADR-011, ADR-012 | `src/oceanwatch/quality/` + `src/oceanwatch/gold/` + `notebooks/fase3_*` + `resources/sql/fase3_*` | `evidence/req02-capas-calidad/` (diagnostico, disponibilidad_librerias, mascara_tierra_agua, materializacion_plata, invariante_conciliacion, capa_oro, prueba_compuerta) | 2026-11-05 | Verificado (ejecutado) |
| 3 | 20% | Orquestación por Job con cron (Quartz, America/Bogota) e idempotencia | ADR-004, ADR-013, ADR-014, ADR-015 | `resources/jobs/oceanwatch_pipeline.job.yml` + `databricks.yml` + `notebooks/fase4_tarea_*` + `resources/sql/fase4_auditoria.sql` + `src/oceanwatch/orquestacion/` | `evidence/req03-orquestacion/` (definicion_job, jobs_get, corridas_run_now, backfill_idempotente, reintento, corridas_scheduled, tabla_auditoria) | 2026-11-05 | Verificado (ejecutado) |
| 4 | 15% | Dashboard de Databricks sobre tablas de oro | ADR-001 | `resources/dashboards/` | `evidence/req04-dashboard/` | 2026-11-05 | Pendiente |
| 5 | 10% | Gobernanza UC: catálogo/esquemas/volúmenes, comentarios y GRANTs | ADR-002 | `resources/sql/bootstrap_oceanwatch.sql` | `evidence/req05-gobernanza/` | 2026-11-05 | Diseñado (no ejecutado) |
| 6 | 10% | Modelo de anomalías de velocidad (SOG) versionado con MLflow | ADR-005, ADR-016 | `src/oceanwatch/ml/` (features) + `notebooks/fase6_*` + `resources/jobs/oceanwatch_pipeline.job.yml` (ml_anomalias) + `resources/sql/fase6_anomalias.sql` | `evidence/req06-ml-mlflow/` (distribucion_sog, experimento_mlflow, registro_modelo_uc, integracion_job_ml, tabla_anomalias, reproducibilidad) | 2026-11-05 | Verificado (ejecutado) |
| 7 | 5% | Repo estructurado + bitácora + ADRs + trazabilidad | ADR-001, ADR-002 | todo el repo | `evidence/req07-repo-bitacora/` | 2026-11-05 | En progreso (Fase 1) |
| 8 | 10% | Sustentación/guía | — | `docs/guia_sustentacion.md` | `evidence/req08-sustentacion/` | 2026-11-05 | Pendiente |

> **Nota.** Los pesos corresponden a la rúbrica de **Entrega 2** (Req1 15%, Req2 15%, Req3 20%,
> Req4 15%, Req5 10%, Req6 10%, Req7 5%, Req8 10%); no deben confundirse con los de Entrega 1.
> "Diseñado (no ejecutado)" significa que el artefacto ya existe en el repo pero aún no se ha
> corrido en Databricks serverless (eso ocurre en fases posteriores).
