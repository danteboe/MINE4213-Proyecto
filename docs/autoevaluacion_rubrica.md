# Autoevaluación de la rúbrica — OceanWatch Analytics (Entrega 2)

Autoevaluación obligatoria del enunciado. Fecha: **2026-11-05**. Entorno: Databricks serverless,
catálogo `oceanwatch`, metastore `us-east-2`, host `dbc-a568bc49-bdae`.

Toda evidencia citada es real y vive en `evidence/reqNN-*/`. Somos honestos con las brechas: lo
único que falta es la **captura manual de UI** (dashboard y linaje), que no es software y ya está
verificado por API. El bono **Delta Sharing** y el **pronóstico de arribos** quedaron **fuera de
alcance** por decisión del equipo (no son brechas).

## 1. Cumplimiento requisito por requisito

| Req | Peso | Cumplimiento | Evidencia concreta (ruta real) | Brechas |
|---|---|---|---|---|
| 1 — Ingesta streaming | 15% | **Sí** | `evidence/req01-streaming/` (`ejecucion_ingesta.md`, `pruebas_tolerancia_fallos.md`, `justificacion_availablenow.md`, `verificacion_esquema_real.md`). `bronze.ais_raw` = 25.897.493 filas. | Ninguna. |
| 2 — Capas + calidad + cuarentena | 15% | **Sí** | `evidence/req02-capas-calidad/` (`diagnostico_calidad_bronce.md`, `mascara_tierra_agua.md`, `materializacion_plata.md`, `invariante_conciliacion.md`, `enriquecimiento_en_agua.md`, `capa_oro.md`, `prueba_compuerta.md`). Invariante 25.897.493 = 25.857.101 + 39.734 + 658. | Ninguna. `en_agua` marca 71.65% en tierra (puertos/canales), esperado por la grilla; documentado en ADR-010. |
| 3 — Orquestación por Job | 20% | **Sí** | `evidence/req03-orquestacion/` (`definicion_job.md`, `jobs_get.md`, `corridas_run_now.md`, `backfill_idempotente.md`, `reintento.md`, `corridas_scheduled.md`, `tabla_auditoria.md`). Job `202547459111516`, 5 tareas, 3 corridas SCHEDULED reales. | Ninguna. |
| 4 — Dashboard AI/BI sobre oro | 15% | **Sí** (software); **captura de UI pendiente del equipo** | `evidence/req04-dashboard/` (`consultas_visuales.md`, `vista_apoyo_matriz_diaria.md`, `despliegue_dashboard.md`, `validacion_parametro_fecha.md`, `confirmacion_solo_oro.md`, `capturas_PENDIENTE.md`). Dashboard `01f1da0367841f3db6c6ddcb8edaa849` ACTIVE/publicado. | Capturas de pantalla de los 4 visuales + parámetro de fecha (manual, no automatizable). |
| 5 — Gobernanza UC | 10% | **Sí** (software); **captura de linaje pendiente del equipo** | `evidence/req05-gobernanza/` (`bootstrap_unity_catalog.md`, `permisos_rbac.md`, `linaje.md`, `comentarios_oro.md`). 4 grupos, 18 grants (0 FAIL), linaje por API, 43 comentarios. | Screenshot del grafo de linaje en Catalog Explorer (manual; ya verificado por API). |
| 6 — ML con MLflow | 10% | **Sí** | `evidence/req06-ml-mlflow/` (`distribucion_sog_insumo_modelo.md`, `experimento_mlflow.md`, `registro_modelo_uc.md`, `integracion_job_ml.md`, `tabla_anomalias.md`, `reproducibilidad.md`). Modelo `@champion` v1, tarea `ml_anomalias` SUCCESS, 67.828 anomalías. | Modelo no supervisado sin ground truth fuerte y corpus de 3 días (decisiones conscientes; ver ADR-005/016 y §3 de `guia_sustentacion.md`). |
| 7 — Repo + bitácora + ADRs | 5% | **Sí** | Todo el repo (`src/`, `notebooks/`, `resources/`, `conf/`, `docs/`); `docs/bitacora.md` (cronológica), `docs/adr/ADR-001..018` (18 ADRs aceptados), `docs/traceability_matrix.md`, `evidence/req07-repo-bitacora/`. `pytest` 109 passed / 1 skipped. | Ninguna. |
| 8 — Sustentación | 10% | **Presencial** (sin entregable de software) | Material de apoyo `docs/guia_sustentacion.md`. | No aplica software: la sustentación es presencial en la última clase. |

**Resumen:** 7/7 requisitos de software **cumplidos (Sí)** y verificados con evidencia real; el
Req. 8 es presencial. Pendientes reales: solo **2 capturas de UI** (dashboard y linaje), ya
cubiertas por verificación por API. Ver [`pendientes_equipo.md`](pendientes_equipo.md).

## 2. Checklist final del enunciado

| # | Ítem | Sí/No | Dónde se demuestra |
|---|---|---|---|
| 1 | Cada decisión relevante tiene ADR con **alternativas**, **evidencia** y **patrón nombrado**. | **Sí** | `docs/adr/ADR-001..018` (18 ADRs aceptados; cada uno con criterios de decisión, alternativas consideradas y patrón). Índice en `traceability_matrix.md`. |
| 2 | Bitácora **cronológica**, específica, incluye **errores**, enlaza a evidencia. | **Sí** | `docs/bitacora.md` (secciones "Síntoma→causa→solución" con callejones reales; enlaces a `evidence/`). |
| 3 | Invariante **bronce = plata + cuarentena + descartes** probada. | **Sí** | 25.897.493 = 25.857.101 + 39.734 + 658 (diferencia 0). `evidence/req02-capas-calidad/invariante_conciliacion.md`. |
| 4 | Las **4 pruebas de tolerancia a fallos** con evidencia. | **Sí** | `evidence/req01-streaming/pruebas_tolerancia_fallos.md`; tabla `quality.pruebas_fallos` = 4 filas. |
| 5 | Job **como código** + evidencia de corridas **programadas**, **reintento** y **backfill**. | **Sí** | `resources/jobs/oceanwatch_pipeline.job.yml` + `databricks.yml`; `evidence/req03-orquestacion/` (`corridas_scheduled.md`, `reintento.md`, `backfill_idempotente.md`). |
| 6 | Dashboard de **4 visuales** consumiendo **solo oro**. | **Sí** (captura de UI pendiente) | `resources/dashboards/oceanwatch_oro.lvdash.json`; `evidence/req04-dashboard/confirmacion_solo_oro.md`, `consultas_visuales.md`. |
| 7 | **Permisos, linaje y contrato de datos** documentados con evidencia. | **Sí** (captura de linaje pendiente) | `fase5_permisos.sql` + `permisos_rbac.md`; `linaje.md` (API); `docs/data_contract_gold.md`. |
| 8 | Modelo en el **registro** y su tarea corre **dentro del job**. | **Sí** | `oceanwatch.ml.detector_anomalias_velocidad` v1 `@champion`; tarea `ml_anomalias` del Job; `evidence/req06-ml-mlflow/registro_modelo_uc.md`, `integracion_job_ml.md`. |
| 9 | README **reproducible desde cero**. | **Sí** | `README.md` → "Cómo reproducir desde cero" (bootstrap → bundle → run por fecha → en_agua → permisos → dashboard). |
| 10 | La **guía de sustentación** cubre las decisiones. | **Sí** | `docs/guia_sustentacion.md` (arquitectura, guion de demo, banco de preguntas por ADR, 10 defendibles / 5 débiles). |
| 11 | Todo lo **no ejecutado** marcado como **PENDIENTE** con procedimiento. | **Sí** | `docs/pendientes_equipo.md` (capturas de UI con procedimiento exacto); `capturas_PENDIENTE.md`; `linaje.md` §4. No hay nada de software pendiente. |

## 3. Brechas reales conocidas (honestas)

1. **Capturas de UI (dashboard y linaje):** pendientes de tomar por una persona del equipo con
   sesión en el workspace. Mitigación: ya verificado por API; procedimiento en
   `pendientes_equipo.md`.
2. **Modelo de anomalías no supervisado sin ground truth fuerte:** es una decisión de diseño (el
   objetivo es la integración MLOps, no la precisión). Mitigación: antifuga por partición
   temporal, acuerdo 98.86% con un baseline z-score, modelo versionado y re-entrenable (ADR-016).
3. **Corpus de 3 días (2023-06-01..03):** limita la generalización temporal. Mitigación: pipeline
   idempotente y parametrizado por fecha → backfill trivial de más días.
4. **`en_agua` marca ~71.65% en tierra:** la grilla de celdas a 2 decimales ubica puertos/canales
   "en tierra"; es esperado y no descarta filas (nulifica atributo, ADR-010).

Fuera de alcance por decisión del equipo (no son brechas): **Delta Sharing** y **pronóstico de
arribos** (ADR-005).
