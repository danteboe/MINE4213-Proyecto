# ADR-013: Diseño del Job de orquestación (Lakeflow Jobs, notebook_task serverless + SQL CTAS)

- **Estado:** Aceptado
- **Fecha:** 2026-11-05
- **Requisito(s) de la rúbrica:** Req. 3 (Orquestación, 20%)
- **Autores:** Alejandro Abril (202224328), David Fernando Caro (202222073), Juan David Torres Albarracín (202317608)

## 1. Contexto y problema

El pipeline medallion AIS (bronce -> plata/calidad -> oro) ya está construido y
verificado en las fases 1-3 (bronce `ais_raw` = 25.897.493; plata `ais_clean` =
25.857.101; cuarentena `ais_rejected` = 39.734; duplicados = 658; invariante cuadra).
La Fase 4 (Req. 3) debe **orquestarlo como un Job de Databricks definido COMO CÓDIGO
versionado**, con un parámetro de fecha que permita reproceso/backfill, dependencias
explícitas entre etapas, reintentos, agendamiento y notificaciones, sin romper nada de
lo existente. Restricciones duras del entorno (context verificado): cómputo **SOLO
serverless** (NO existe all-purpose cluster; `jobs list` estaba vacío); la **API de
escritura Delta de Spark** (`df.write.saveAsTable`) sobre 25.9M filas devolvió
`INTERNAL_ERROR` en Spark Connect GC (y antes `CACHE_REQUIRES_NEWER_CLIENT_ON_SERVERLESS_GC`
por `df.cache()`); el camino FIABLE verificado en fase 3 fue **SQL CTAS** en el warehouse
serverless `377600528fed7c47`. Hay que decidir el tipo de tarea, cómo se escribe la capa
pesada, el grafo de dependencias y la herramienta de definición-como-código.

## 2. Criterios de decisión

- Compatibilidad serverless real (evitar el `INTERNAL_ERROR` de `df.write` sobre 25.9M filas).
- Definición del Job como código versionable y reproducible (no clic-ops en la UI).
- Modelo único para las 4 etapas: ingesta (descarga + `%pip` + Auto Loader), capas
  (escritura pesada), compuerta de calidad (fallar el Job) y auditoría.
- Dependencias explícitas y lineales (linaje bronce -> plata -> oro).
- Parametrización por fecha (reproceso/backfill) y propagación a todas las tareas.
- Extensibilidad: dejar sitio a nodos futuros (ml, dashboard) sin rediseñar.
- SIN secretos en el repo (host/token fuera, en `databricks.key`).

## 3. Alternativas consideradas

### 3.1 Tipo de tarea y escritura de la capa pesada

| Alternativa | Descripción | Pros | Contras | Evidencia |
|---|---|---|---|---|
| A. `notebook_task` serverless que ejecuta SQL CTAS vía `spark.sql` | 4 notebooks delgados; las etapas de capas leen `resources/sql/fase3_*.sql` y lo corren con `spark.sql` | Mismo modelo para ingesta/capas/compuerta/auditoría; usa el camino FIABLE (CTAS); sin `df.write` | Un notebook por tarea | Cabeceras de `resources/sql/fase3_materializar_plata.sql` y `fase3_materializar_oro.sql`; `evidence/req02-capas-calidad/materializacion_plata.md`; `bundle validate` OK (`evidence/req03-orquestacion/definicion_job.md`) |
| B. `df.write.saveAsTable` desde notebooks (API Spark) | Escribir Delta con la API de DataFrames | Código "idiomático" PySpark | **Falla**: `INTERNAL_ERROR` en Spark Connect GC sobre 25.9M filas (fase 3) | Nota de ejecución en `notebooks/fase3_plata_calidad.py`; `materializacion_plata.md` |
| C. `sql_task` (consulta SQL del Job) para las capas | Tareas de tipo SQL puras sobre el warehouse | Nativo para CTAS | No cubre la ingesta (descarga NOAA + `%pip` + Auto Loader) ni la compuerta que debe *fallar* el Job ni la auditoría con el mismo modelo; mezclaría dos tipos de tarea | Documentación Databricks Jobs task types (ver §9) |

### 3.2 Herramienta de definición-como-código

| Alternativa | Descripción | Pros | Contras | Evidencia |
|---|---|---|---|---|
| A. Databricks Asset Bundle (DAB) | `databricks.yml` + `resources/jobs/*.yml`; `databricks bundle validate/deploy` | Estándar actual de Databricks para IaC de jobs; validación local; targets; variables | Curva de configuración (host por ENV, `root_path` en `mode: production`) | `bundle validate` -> `Validation OK!` tras resolver host por ENV y fijar `root_path` (`definicion_job.md`) |
| B. JSON Jobs API 2.1 (`databricks jobs create --json`) | Un `job.json` con tasks/depends_on/schedule/notifications | Simple y directo | Menos ergonómico para variables/targets; sin `validate` local equivalente | Documentación Jobs API 2.1 (ver §9) |

### 3.3 Herramienta de pipeline

| Alternativa | Descripción | Pros | Contras | Evidencia |
|---|---|---|---|---|
| A. Databricks Jobs (Lakeflow Jobs/Workflows) | Orquestador de tareas con DAG, reintentos, cron, notificaciones | Soporta `notebook_task` serverless; compuerta que falla el Job; parámetros | — | Este ADR; `bundle validate` OK |
| B. Delta Live Tables (DLT) / Lakeflow Declarative Pipelines | Pipeline declarativo con expectativas | Calidad declarativa integrada | DLT en serverless NO confirmado en el entorno (fase 3 lo descartó); el pipeline ya está materializado con CTAS, no declarativamente | Context de fase 3 (DLT no confirmado); `materializacion_plata.md` |

## 4. Decisión

Se orquesta el pipeline con un **Databricks Job (Lakeflow Jobs)** definido como
**Databricks Asset Bundle** (`databricks.yml` + `resources/jobs/oceanwatch_pipeline.job.yml`),
compuesto por **cuatro tareas `notebook_task` serverless** (`ingesta` -> `plata_calidad`
-> `conciliacion` -> `oro`), donde las tareas de capas ejecutan el **SQL CTAS fiable**
(`resources/sql/fase3_*.sql`) vía `spark.sql` y **nunca** `df.write.saveAsTable`.

## 5. Justificación basada en evidencia

`df.write.saveAsTable` sobre 25.9M filas falló con `INTERNAL_ERROR` en Spark Connect GC
(alternativa 3.1.B; nota de ejecución en `notebooks/fase3_plata_calidad.py` y
`evidence/req02-capas-calidad/materializacion_plata.md`); el CTAS en el warehouse
serverless `377600528fed7c47` SÍ materializó las tablas con los conteos verificados, por
eso las tareas de capas ejecutan ese `.sql` con `spark.sql` (3.1.A). El `notebook_task`
unifica las 4 etapas bajo un mismo modelo (ingesta con descarga + `%pip` + Auto Loader,
capas con CTAS, compuerta que hace `raise`, auditoría con `MERGE`), cosa que `sql_task`
(3.1.C) no logra. Se eligió **DAB** (3.2.A) porque validó localmente (`databricks bundle
validate` -> `Validation OK!`, tras resolver el host por `DATABRICKS_HOST` y fijar
`workspace.root_path` como exige `mode: production` — ver `definicion_job.md`); el JSON
Jobs API (3.2.B) queda como fallback equivalente no adoptado. Se usan **Jobs** y no
**DLT** (3.3.B) porque DLT en serverless no está confirmado en este entorno y el pipeline
ya está materializado con CTAS, no de forma declarativa. Las tablas existentes se
verificaron intactas tras crear la auditoría (bronce 25.897.493, plata 25.857.101,
cuarentena 39.734, wpi 3.669; ver `evidence/req03-orquestacion/tabla_auditoria.md`).

## 6. Patrón de diseño / ingeniería de datos aplicado

**Orchestration DAG / Workflow** (grafo acíclico dirigido lineal de tareas con
dependencias explícitas `depends_on`), combinado con **Infrastructure as Code** (el Job
definido como DAB versionado) y **Write-Audit-Publish** (la tarea `conciliacion` audita
el invariante antes de publicar la capa oro; detalle en ADR-014).

## 7. Consecuencias

- Positivas: el Job es reproducible y versionado; usa el único camino de escritura fiable
  en serverless; parámetro `fecha_proceso` para reproceso/backfill; linaje explícito; la
  auditoría (`quality.ejecuciones_job`) da observabilidad por corrida+tarea.
- Negativas: un notebook por tarea (más archivos); el acoplamiento al warehouse CTAS
  implica que el `.sql` es la fuente única de verdad (los notebooks lo leen, no lo duplican).
- Deuda asumida: los nodos `ml` (Fase 6) y `refresh_dashboard` (Fase 7) quedan documentados
  como comentarios en el YAML pero NO implementados.

> **Actualización (ejecutado, FEAT-002/003).** El Job se desplegó con `databricks bundle
> deploy` (`job_id` real **202547459111516**) y corrió de verdad: corrida base
> `fecha_proceso=2023-06-03` (`run_id` 249039226964396) → SUCCESS con las 4 tareas OK
> (ingesta 632126531199658, plata 702492582880484, conciliación 331661400464526, oro
> 236147110169698). La demostración SCHEDULED se completó en FEAT-003 (corridas con
> `trigger=PERIODIC` en `evidence/req03-orquestacion/corridas_scheduled.md`). Ya NO queda
> nada PENDIENTE DE EJECUCIÓN en este ADR.

## 8. Cómo se verifica / reversibilidad

- Verificación: `databricks bundle validate` -> `Validation OK!` (evidencia en
  `evidence/req03-orquestacion/definicion_job.md`); `SHOW TABLES IN oceanwatch.quality`
  incluye `ejecuciones_job`; `python -m pytest -q` verde (helpers puros de
  `src/oceanwatch/orquestacion/`). **Ejecutado (FEAT-002/003):** `databricks bundle deploy`
  creó el Job (`job_id` 202547459111516; `Resources: 1 created`); `databricks jobs get
  --job-id 202547459111516 --output json` confirma las 4 tareas `notebook_task` serverless,
  el `depends_on` lineal, los reintentos y el schedule; `databricks jobs run-now --job-id
  202547459111516 --json '{"job_parameters":{"fecha_proceso":"2023-06-03"}}'` + polling de
  `jobs get-run --run-id 249039226964396` → `result_state=SUCCESS`. Evidencia en
  `evidence/req03-orquestacion/jobs_get.md` y `.../corridas_run_now.md`.
- Reversibilidad: `databricks bundle destroy` elimina el Job desplegado; la tabla de
  auditoría se revierte con `DROP TABLE IF EXISTS oceanwatch.quality.ejecuciones_job`
  (no afecta a ninguna tabla del pipeline). El pipeline en sí es idempotente (ADR-014).

## 9. Referencias

- Databricks Jobs (Lakeflow Jobs) — tipos de tarea: https://docs.databricks.com/aws/en/jobs/ (acceso 2026-11-05).
- Databricks Asset Bundles: https://docs.databricks.com/aws/en/dev-tools/bundles/ (acceso 2026-11-05).
- Jobs API 2.1: https://docs.databricks.com/api/workspace/jobs/create (acceso 2026-11-05).
- Delta Live Tables / Lakeflow Declarative Pipelines: https://docs.databricks.com/aws/en/delta-live-tables/ (acceso 2026-11-05).
- ADR-004 (UTC serverless + Trigger.availableNow + idempotencia txn), ADR-012 (modelo oro, CTAS idempotente), ADR-014 (idempotencia y reintentos).
- Evidencia interna: `evidence/req02-capas-calidad/materializacion_plata.md`, `evidence/req03-orquestacion/definicion_job.md`, `.../tabla_auditoria.md`.
