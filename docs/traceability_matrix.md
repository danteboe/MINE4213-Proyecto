# Matriz de trazabilidad — OceanWatch Analytics (Entrega 2)

Una fila por requisito de la rúbrica de **Entrega 2**. Las 6 fases técnicas (Req. 1–6) y el
Req. 7 (repo + bitácora) están **Verificados (ejecutados)**; el Req. 8 es una **sustentación
presencial** sin entregable de software (ver nota al pie).

| Req | Peso | Criterio de aceptación | ADR(s) | Código | Evidencia | Bitácora | Estado |
|---|---|---|---|---|---|---|---|
| 1 | 15% | Ingesta incremental de zips NOAA vía descompresión + Auto Loader con esquema explícito, resistencia a archivos nuevos y tolerancia a fallos | ADR-003, ADR-004, ADR-006, ADR-007, ADR-008 | `src/oceanwatch/ingest/` (schema, paths, download, autoloader, dedup) + `notebooks/fase2_*` | `evidence/req01-streaming/` (ejecucion_ingesta, pruebas_tolerancia_fallos, justificacion_availablenow, verificacion_esquema_real) | 2026-11-05 | Verificado (ejecutado) |
| 2 | 15% | Capas medallion con reglas de calidad y cuarentena | ADR-001, ADR-006, ADR-009, ADR-010, ADR-011, ADR-012 | `src/oceanwatch/quality/` + `src/oceanwatch/gold/` + `notebooks/fase3_*` + `resources/sql/fase3_*` | `evidence/req02-capas-calidad/` (diagnostico, disponibilidad_librerias, mascara_tierra_agua, materializacion_plata, invariante_conciliacion, capa_oro, prueba_compuerta) | 2026-11-05 | Verificado (ejecutado) |
| 3 | 20% | Orquestación por Job con cron (Quartz, America/Bogota) e idempotencia | ADR-004, ADR-013, ADR-014, ADR-015 | `resources/jobs/oceanwatch_pipeline.job.yml` + `databricks.yml` + `notebooks/fase4_tarea_*` + `resources/sql/fase4_auditoria.sql` + `src/oceanwatch/orquestacion/` | `evidence/req03-orquestacion/` (definicion_job, jobs_get, corridas_run_now, backfill_idempotente, reintento, corridas_scheduled, tabla_auditoria) | 2026-11-05 | Verificado (ejecutado) |
| 4 | 15% | Dashboard de Databricks (AI/BI) sobre tablas de oro, 4 visuales + parámetro de fecha, desplegado y publicado | ADR-001, ADR-012, ADR-016, ADR-017 | `resources/dashboards/oceanwatch_oro.lvdash.json` + `resources/sql/fase7_matriz_trafico_diaria.sql` | `evidence/req04-dashboard/` (consultas_visuales, vista_apoyo_matriz_diaria, despliegue_dashboard, validacion_parametro_fecha, confirmacion_solo_oro, capturas_PENDIENTE) | 2026-11-05 | Verificado (ejecutado) |
| 5 | 10% | Gobernanza UC: catálogo/esquemas/volúmenes, comentarios y GRANTs | ADR-002, ADR-018 | `resources/sql/bootstrap_oceanwatch.sql` + `resources/sql/fase5_permisos.sql` + `resources/sql/fase5_comentarios_oro.sql` | `evidence/req05-gobernanza/` (bootstrap_unity_catalog, permisos_rbac, linaje, comentarios_oro) | 2026-11-05 | Verificado (ejecutado) — grants, linaje por API y comentarios; **Screenshot de linaje PENDIENTE DE CAPTURA** |
| 6 | 10% | Modelo de anomalías de velocidad (SOG) versionado con MLflow | ADR-005, ADR-016 | `src/oceanwatch/ml/` (features) + `notebooks/fase6_*` + `resources/jobs/oceanwatch_pipeline.job.yml` (ml_anomalias) + `resources/sql/fase6_anomalias.sql` | `evidence/req06-ml-mlflow/` (distribucion_sog, experimento_mlflow, registro_modelo_uc, integracion_job_ml, tabla_anomalias, reproducibilidad) | 2026-11-05 | Verificado (ejecutado) |
| 7 | 5% | Repo estructurado + bitácora cronológica + ADRs (alternativas, evidencia, patrón nombrado) + matriz de trazabilidad + evidencia por requisito | ADR-001, ADR-002 (y los 18 ADRs ADR-001..018 como cuerpo de decisiones) | todo el repo (`src/`, `notebooks/`, `resources/`, `conf/`, `docs/`) | `evidence/req07-repo-bitacora/` + estructura del repo + `docs/bitacora.md` + `docs/adr/` (18 ADRs) + esta matriz | 2026-11-05 | Verificado (ejecutado) — repo estructurado, bitácora cronológica, 18 ADRs aceptados, matriz y evidencia por requisito; `pytest` 109 passed / 1 skipped |
| 8 | 10% | Sustentación de la solución | — (no aplica ADR) | — (no aplica software) | `docs/guia_sustentacion.md` (material de apoyo opcional) + `evidence/req08-sustentacion/` | 2026-11-05 | Sustentación presencial en la última clase — **sin entregable de software**. Guía de apoyo en `docs/guia_sustentacion.md` (resumen de arquitectura, guion de demo, banco de preguntas, 10 decisiones defendibles / 5 débiles con mitigación) |

> **Nota.** Los pesos corresponden a la rúbrica de **Entrega 2** (Req1 15%, Req2 15%, Req3 20%,
> Req4 15%, Req5 10%, Req6 10%, Req7 5%, Req8 10%); no deben confundirse con los de Entrega 1.
> "Verificado (ejecutado)" significa que el artefacto existe en el repo **y** se corrió de verdad
> en Databricks serverless con evidencia real; no queda nada en estado "Diseñado (no ejecutado)".
>
> **Único pendiente del equipo humano (no de software):** capturas de pantalla de UI —las 4
> visuales del dashboard con el parámetro de fecha (`evidence/req04-dashboard/capturas_PENDIENTE.md`)
> y el grafo de linaje en Catalog Explorer (`evidence/req05-gobernanza/linaje.md` §4)—, no
> automatizables sin navegador autenticado. El linaje y el dashboard ya están **verificados por API**.
> Detalle y procedimiento en [`pendientes_equipo.md`](pendientes_equipo.md).
>
> **Fuera de alcance por decisión del equipo:** el bono de **Delta Sharing** (no se realizó) y el
> **pronóstico de arribos** (aplazado; ver ADR-005). No son brechas de la Entrega 2.
