# Bitácora — OceanWatch Analytics (Entrega 2)

Registro cronológico del trabajo del equipo (Req. 7). Cada sesión se documenta con la
plantilla de abajo. Las entradas se ordenan de la más reciente a la más antigua.

## Plantilla de entrada

```markdown
### <AAAA-MM-DD> — <Fase N: nombre> — <integrante(s)>
- **Objetivo de la sesión:** ...
- **Qué se hizo:** ...
- **Decisiones/ADRs tocados:** ADR-NNN ...
- **Estado Verificado vs Diseñado:** ...
- **Evidencia:** <ruta en evidence/ o enlace>
- **Pendientes / siguiente paso:** ...
```

## Entradas

### 2026-11-05 — Fase 6: ML con MLflow (Req. 6) — Equipo OceanWatch
- **Objetivo de la sesión:** cerrar el Req. 6 con **evidencia real**: implementar el caso de ML
  ya fijado por ADR-005 (detección de anomalías de velocidad SOG), demostrando la **integración
  MLOps** completa — features desde el Lakehouse, experimento MLflow, modelo en Unity Catalog con
  alias `@champion` e inferencia batch como tarea del Job que puebla una tabla de oro — todo
  **ejecutado** en Databricks serverless, no solo diseñado. El objetivo es la integración, no
  maximizar precisión (modelo deliberadamente simple).
- **Qué se hizo:**
  - **Features puras + tests (FEAT-001):** `src/oceanwatch/ml/features.py` con funciones puras de
    ingeniería de características (`z_sog_por_tipo`, `delta_sog`, `hora_a_ciclico`,
    `construir_vector_features`, `clasificar_por_umbral_z`, `estadisticas_por_tipo`), imports
    PySpark/numpy perezosos, vector de orden fijo `[z_sog, delta_sog, hora_sin, hora_cos]`; bloque
    `ml` en `conf/config.yaml` (sin secretos); 12 tests nuevos en `tests/test_ml_features.py`.
    `python -m pytest -q` = **109 passed, 1 skipped**.
  - **Experimento MLflow + registro en UC (FEAT-002):** notebook
    `notebooks/fase6_entrenamiento_mlflow.py` ejecutado como one-time run serverless
    (`347173419720341`, task `386348858221273`, Python 3.11.10). Experimento
    `/Users/jd.torresa1@uniandes.edu.co/oceanwatch_anomalias_sog` (`experiment_id 2474832545018011`)
    con **2 runs reales**: `baseline_zscore` `a0cca5b5a4bd4373a7c0cd8ed04dc753`
    (`pct_flagged_test=0.01203`) e `isolation_forest` `be8f2ef2c2634efcaffa7fd89d3daf8f`
    (campeón; `pct_flagged_test=0.00888`, `acuerdo_con_baseline=0.9886`, `separacion_scores=0.2070`,
    `signature` + `input_example`). Registro en Models in Unity Catalog
    `oceanwatch.ml.detector_anomalias_velocidad` **versión 1** (`READY`), alias **`@champion`** → v1
    (verificado por la MLflow REST API de UC). Antifuga por partición temporal (train 2023-06-01/02,
    test 2023-06-03; stats solo del train).
  - **Integración en el Job + tabla de oro (FEAT-003):** `resources/sql/fase6_anomalias.sql` (DDL
    `gold.anomalias_velocidad`) y notebook `notebooks/fase6_tarea_ml_anomalias.py` (carga
    `@champion`, puntúa la fecha, escribe las anómalas con `spark.sql` `DELETE`+`INSERT`,
    auditoría). Nueva tarea `ml_anomalias` (`depends_on` oro, `max_retries 1`) en el Job
    `oceanwatch_pipeline_entrega2` (`job_id 202547459111516`) — las 4 tareas previas intactas.
    `bundle validate`/`deploy` OK (Job ahora con **5 tareas**). Corrida real `run-now`
    `fecha_proceso=2023-06-03`: `run_id` **483234774995672** → **SUCCESS** con las 5 tareas SUCCESS.
    `gold.anomalias_velocidad` poblada con **67.828** filas anómalas para 2023-06-03 (~0.85 %).
  - **Documentación + trazabilidad (FEAT-004):** ADR-016 (9 secciones, Aceptado), contrato de oro
    con `gold.anomalias_velocidad`, `evidence/req06-ml-mlflow/reproducibilidad.md`, esta entrada y
    la fila Req.6 de la matriz a **Verificado (ejecutado)**. El DAG ya muestra `ml` como nodo REAL.
- **Decisiones/ADRs tocados:** **ADR-016** (modelo de anomalías SOG + MLflow + registro en UC +
  integración en el Job), que implementa y cita **ADR-005** (caso de ML ya elegido). Se apoyan
  ADR-013/014 (diseño del Job, idempotencia/reintentos: base de `ml_anomalias`) y ADR-004 (UTC
  serverless para `hora`).
- **Estado Verificado vs Diseñado:** **Verificado (ejecutado)** = experimento con 2 runs reales,
  modelo `@champion` v1 en UC, tarea `ml_anomalias` corriendo DENTRO del Job (5/5 SUCCESS) y tabla
  `gold.anomalias_velocidad` con 67.828 filas reales para 2023-06-03. Nada quedó PENDIENTE DE
  EJECUCIÓN en el Req. 6.
- **Síntoma→causa→solución (callejones reales de la ejecución):**
  - *scikit-learn 1.3.0 no instala en el Job:* síntoma = `PipError` al pinnear `scikit-learn==1.3.0`
    en la tarea del Job; causa = el `notebook_task` serverless corre en el entorno por defecto v3
    (Python **3.12**) y 1.3.x no tiene wheel para 3.12; el entorno no es fijable por DAB; solución =
    inferencia con `scikit-learn==1.5.2` (wheel 3.11/3.12) + `mlflow==3.18.0` (necesario para
    descargar el modelo de UC; `mlflow 2.11.4` preinstalado falla con S3 HeadObject 400). Verificado
    que el modelo serializado con 1.3.0 predice **idéntico** bajo 1.5.2 (solo
    `InconsistentVersionWarning`, silenciado).
  - *CREATE TABLE con PARSE_SYNTAX_ERROR:* síntoma = el DDL fallaba; causa = dos `COMMENT` del
    `.sql` contenían `;` que el splitter por sentencias del notebook partía a la mitad; solución =
    quitar los `;` del texto de los `COMMENT`.
  - *Escritura Delta masiva en serverless:* (heredado) `df.write.saveAsTable` da `INTERNAL_ERROR`;
    solución = escribir con `spark.sql` (`DELETE`+`INSERT` idempotente), y entrenar sobre una
    muestra reproducible (~200k, semilla 42) traída al driver con `toPandas()`.
- **Evidencia:** `evidence/req06-ml-mlflow/` — `distribucion_sog_insumo_modelo.md`,
  `experimento_mlflow.md`, `registro_modelo_uc.md`, `integracion_job_ml.md`, `tabla_anomalias.md`,
  `reproducibilidad.md`. Código: `src/oceanwatch/ml/features.py`, `tests/test_ml_features.py`,
  `notebooks/fase6_entrenamiento_mlflow.py`, `notebooks/fase6_tarea_ml_anomalias.py`,
  `resources/sql/fase6_anomalias.sql`, `resources/jobs/oceanwatch_pipeline.job.yml` (tarea
  `ml_anomalias`), `conf/config.yaml` (bloque `ml`). Docs: ADR-016,
  `docs/data_contract_gold.md`, `docs/architecture/fase4-dag-orquestacion.md`.
- **Pendientes / siguiente paso:** ninguno del Req. 6. El `refresh_dashboard` (Fase 7) sigue como
  nodo futuro del DAG; el dashboard (Req. 4) consumirá `gold.anomalias_velocidad`.

### 2026-11-05 — Fase 4: Orquestación por Job de Databricks (Req. 3) — Equipo OceanWatch
- **Objetivo de la sesión:** cerrar el Req. 3 con **evidencia real**: definir el pipeline
  medallion como un Job de Databricks versionado (DAB), ejecutarlo de verdad (base,
  idempotencia, backfill, reintento), demostrar corridas disparadas por el **scheduler**
  (cron) y dejar el Job con el cron de **producción** como estado final, con monitoreo y
  documentación (ADRs 013-015, runbook, DAG, matriz).
- **Qué se hizo:**
  - **Definición como código (FEAT-001):** Job `oceanwatch_pipeline_entrega2` como
    **Databricks Asset Bundle** (`databricks.yml` + `resources/jobs/oceanwatch_pipeline.job.yml`):
    4 tareas `notebook_task` serverless (ingesta → plata_calidad → conciliacion → oro),
    `depends_on` lineal, parámetro `fecha_proceso` (default 2023-06-03) propagado a las 4
    tareas, reintentos por tarea (ingesta 2 / plata 1 / oro 1 / conciliacion 0), cron
    producción `0 0 6 * * ?` (America/Bogota, PAUSED), notificaciones email
    (success/failure/duration) y health `RUN_DURATION_SECONDS>3600`. Tabla de auditoría
    nueva `oceanwatch.quality.ejecuciones_job` (DDL `resources/sql/fase4_auditoria.sql`,
    MERGE por `(job_run_id, task_key)`). Helpers puros en `src/oceanwatch/orquestacion/`.
    Las tareas de capas ejecutan el **SQL CTAS fiable** vía `spark.sql`, NUNCA
    `df.write.saveAsTable`. `databricks bundle validate` → `Validation OK!`; `pytest` 97 passed.
  - **Ejecución real (FEAT-002):** `databricks bundle deploy` creó el Job (`job_id`
    **202547459111516**). Corrida base `fecha_proceso=2023-06-03` (`run_id` 249039226964396)
    → SUCCESS (4/4 tareas). Idempotencia: re-corrida 2023-06-03 (`run_id` 768406241318622)
    con conteos ANTES==DESPUÉS. Backfill 2023-06-01 (`run_id` 406283395698923) estable.
    Reintento real: job efímero `max_retries:1` (`run_id` 626548229252476) con attempt 0
    FAILED → attempt 1 SUCCESS. Auditoría poblada (12 filas, 3 corridas × 4 tareas).
  - **Scheduling real + cierre (FEAT-003):** se activó el cron de **demostración**
    `0 */5 * * * ?` (America/Bogota, UNPAUSED) vía `databricks jobs update`; el **scheduler**
    lanzó **3 corridas automáticas** (`trigger=PERIODIC`) a las 12:35/12:40/12:45 UTC
    (run_ids **232561725749917**, **544541082194734**, **707241716873589**), las 3 SUCCESS
    con las 4 tareas OK, distinguibles de las run-now manuales (`trigger=ONE_TIME`) de
    FEAT-002. La auditoría sumó 12 filas (total 24). Luego se **restableció** el cron de
    **producción** `0 0 6 * * ?` (America/Bogota, PAUSED) — estado FINAL verificado con
    `jobs get`. Documentación: runbook completo (lanzar/reparar/backfill/leer auditoría/
    limpiar checkpoints/diagnóstico por tarea), DAG mermaid
    (`docs/architecture/fase4-dag-orquestacion.md` con nodos futuros ml/refresh_dashboard
    punteados), ADR-015 (schedule + monitoreo), ADR-013/014 completados con run_ids reales,
    matriz Req.3 y este registro.
- **Decisiones/ADRs tocados:** **ADR-013** (diseño del Job: Lakeflow Jobs + notebook_task
  serverless + SQL CTAS, DAB), **ADR-014** (idempotencia por tarea, reintentos y compuerta
  Write-Audit-Publish), **ADR-015** (cron Quartz + timezone America/Bogota vs UTC, y
  monitoreo: notificaciones + health + tabla de auditoría). Se citan ADR-004 (UTC
  serverless), ADR-006/011 (invariante/cuarentena), ADR-012 (CTAS idempotente).
- **Estado Verificado vs Diseñado:** **Verificado (ejecutado)** = despliegue del Job,
  corridas base/idempotencia/backfill/reintento (FEAT-002), **3 corridas SCHEDULED reales**
  por el scheduler y restauración del cron de producción (FEAT-003), auditoría poblada (24
  filas) y tablas del pipeline intactas. No quedó nada PENDIENTE DE EJECUCIÓN en el Req. 3.
- **Síntoma→causa→solución (callejones de la ejecución):**
  - *`df.write.saveAsTable` en serverless:* síntoma = `INTERNAL_ERROR` en Spark Connect GC
    sobre 25.9M filas (heredado de fase 3); causa = limitación del cómputo serverless para la
    escritura Delta a esa escala; solución = las tareas de capas ejecutan el **SQL CTAS** vía
    `spark.sql` leyendo `resources/sql/fase3_*.sql` (ADR-013).
  - *Captura de corridas SCHEDULED:* síntoma = el cron de producción (06:00) no es
    demostrable en la ventana de trabajo; causa = una corrida diaria tarda hasta 24 h en
    dispararse; solución = cron temporal de alta frecuencia `0 */5 * * * ?` ~15 min para
    capturar 3 corridas PERIODIC reales y luego restablecer producción (ADR-015).
  - *Lectura de salidas de la CLI:* síntoma = `databricks jobs get-run --run-id` devolvía
    archivo vacío y PowerShell escribe UTF-16; causa = en CLI v1.24.0 el `run_id` es
    posicional (`get-run <id>`), y la salida redirigida queda en UTF-16; solución =
    argumento posicional + parseo de JSON decodificando UTF-16/UTF-8-sig (documentado en el
    runbook).
- **Evidencia:** `evidence/req03-orquestacion/` — `definicion_job.md`, `jobs_get.md`,
  `corridas_run_now.md`, `backfill_idempotente.md`, `reintento.md`, **`corridas_scheduled.md`**
  (FEAT-003), `tabla_auditoria.md`. Código: `resources/jobs/`, `databricks.yml`,
  `notebooks/fase4_tarea_*`, `resources/sql/fase4_auditoria.sql`, `src/oceanwatch/orquestacion/`.
  Docs: `docs/runbook.md`, `docs/architecture/fase4-dag-orquestacion.md`, ADR-013/014/015.
- **Pendientes / siguiente paso:** Fase siguiente (Req. 4) — dashboard de Databricks sobre
  las tablas de oro. Los nodos `ml` (Fase 6) y `refresh_dashboard` (Fase 7) quedan
  documentados como futuros en el DAG/YAML, no implementados. El cron de producción queda
  PAUSED (activarlo es alternar `pause_status` cuando se opere en continuo).

### 2026-11-05 — Fase 3: Materialización de la bandera `en_agua` en la plata (cierre de brecha) — Equipo OceanWatch
- **Objetivo de la sesión:** poblar la bandera de calidad `en_agua` de
  `oceanwatch.silver.ais_clean`, que había quedado en NULL tras FEAT-003.
- **Qué se hizo (síntoma → causa → solución → verificación):**
  - **Síntoma:** `en_agua` estaba **TODA en NULL** sobre las 25.857.101 filas de la plata.
  - **Causa:** el `pandas_udf` de `global_land_mask` (`compuertas._columna_en_agua`) **falló**
    en el cómputo **serverless** (Spark Connect) al evaluarse fila-a-partición sobre las ~25.9M
    filas; la bandera quedó sin materializar.
  - **Solución (sin `pandas_udf`):** clasificar las **726.513 celdas DISTINTAS** de la grilla
    (`round(LAT,2)`,`round(LON,2)` — ADR-001/`geo.celda_grilla`), no las 25.9M filas. Extracción
    local de las celdas vía SQL Statement Execution API (paginada), clasificación vectorizada con
    `global-land-mask==1.0.0` local (`globe.is_land` sobre numpy), CSV subido al Volume
    `oceanwatch.bronze.landing/ref/`, CTAS de la tabla de referencia
    `oceanwatch.silver.celda_en_agua` (642.018 agua / 84.495 tierra) y `MERGE` que enriquece la
    plata por join de celda. Script `resources/sql/fase3_enriquecer_en_agua.sql`; lógica reusable
    `materializar.clasificar_celdas_en_agua` / `enriquecer_en_agua_por_celda`.
  - **Verificación (conteos reales):** `no_null`=**25.857.101** (0 NULL), `en_agua_true`=**7.329.406**,
    `en_tierra`=**18.527.695** ⇒ **71.65 %** tierra (coherente con el 72.28 % de la muestra de
    FEAT-002). Invariante INTACTA: `25.857.101 + 39.734 + 658 = 25.897.493 = bronce` (diferencia 0);
    la plata sigue en 25.857.101 filas. `python -m pytest -q` sigue **verde**.
- **Decisiones/ADRs tocados:** ADR-010 §8 (verificación: bandera materializada). No cambia la
  decisión de diseño (sigue NULIFICAR-ATRIBUTO, no descarta filas).
- **Estado Verificado vs Diseñado:** **Verificado (ejecutado)**. Statements reales: extracción
  `01f1d9a8-7811-1519-84de-e57af6b08984`, MERGE `01f1d9a8-befe-18b5-a972-b7bcbfd6b468`,
  verificación `01f1d9a8-c9cb-15b6-83b8-3d2cf7ab60e6`.
- **Evidencia:** `evidence/req02-capas-calidad/enriquecimiento_en_agua.md`.
- **Pendientes / siguiente paso:** ninguno para esta brecha; la bandera queda materializada y la
  tabla de referencia `silver.celda_en_agua` conservada. Fase 4 (Req. 3).

### 2026-11-05 — Fase 3: Verificación de integración entre FEATs (paso FIXER) — Equipo OceanWatch
- **Objetivo de la sesión:** re-ejecutar, sin confiar en los `.md` previos, la verificación de
  integración extremo a extremo del Req. 2 (costuras FEAT-001 → FEAT-003 → FEAT-004 → FEAT-005)
  con conteos **reales** leídos en vivo del serverless.
- **Qué se hizo:** `python -m pytest -q` = **76 passed, 4 skipped**; imports perezosos de
  `quality`/`gold` verificados sin Spark. Consultas vivas (SQL warehouse `377600528fed7c47`):
  invariante `25.897.493 = 25.857.101 (plata) + 39.734 (cuarentena) + 658 (duplicados)`,
  diferencia 0; causas cuarentena `mmsi_mid_valido`=39.734 / `mmsi_bien_formado`=21.958; suma
  `evaluados` métricas = 25.896.835 = plata + cuarentena; 0 incoherencias bandera→null. **Oro
  limpio**: 0 MMSI huérfanos, 0 MMSI de oro en cuarentena, 0 MMSI mal formados, 0 celdas fuera de
  rango, grilla a 2 decimales (0 violaciones, ADR-001), 0 puertos huérfanos. Tablas quality
  previas (`ingesta_metricas`, `pruebas_fallos`) intactas.
- **Decisiones/ADRs tocados:** ninguno nuevo; se ratifican ADR-009/010/011/012 con datos vivos.
  No hizo falta tocar código: las costuras entre FEATs ya cuadraban sobre los 25.9M reales.
- **Estado Verificado vs Diseñado:** **Verificado (ejecutado)**. (En ese momento `en_agua` estaba
  en `null`; informativo, no afecta invariante ni limpieza de oro. **Ya materializada**
  posteriormente el 2026-11-05, ver entrada superior.)
- **Evidencia:** `evidence/req02-capas-calidad/verificacion_integracion_feats.md`.
- **Pendientes / siguiente paso:** cierre del bucle de convergencia (veredicto); Fase 4 (Req. 3).

### 2026-11-05 — Fase 3: Capas de calidad (plata + cuarentena + métricas + oro) y prueba de compuerta — Equipo OceanWatch
- **Objetivo de la sesión:** cerrar el Req. 2 con **evidencia real**: materializar la capa
  **plata** con compuertas de calidad y cuarentena, las **métricas** de calidad, la capa **oro**
  (modelo dimensional de negocio) y demostrar extremo a extremo, con conteos reales, que la
  compuerta bloquea el 100% de los registros sucios por todas las rutas.
- **Qué se hizo:**
  - **Plata/cuarentena/métricas (FEAT-003):** compuertas puras (FEAT-001) + máscara tierra/agua
    (FEAT-002) envueltas como expresiones nativas en `src/oceanwatch/quality/compuertas.py`;
    materialización en serverless (SQL CTAS `resources/sql/fase3_materializar_plata.sql`, mismo
    pipeline que el notebook `notebooks/fase3_plata_calidad`). Conteos reales:
    `oceanwatch.silver.ais_clean`=**25.857.101**, `oceanwatch.quarantine.ais_rejected`=**39.734**
    (causas `mmsi_mid_valido`=39.734, `mmsi_bien_formado`=21.958; una fila cuenta una vez),
    `oceanwatch.quality.metricas_calidad`=**15** (3 días × 5 reglas de descarte), duplicados
    dedup=**658**. **Invariante** bronce = plata + cuarentena + duplicados:
    `25.897.493 = 25.857.101 + 39.734 + 658` (diferencia 0). Nulificación con banderas
    (heading_desconocido=14.338.249, cog_desconocido=4.373.778, etc.), 0 incoherencias
    bandera→null.
  - **Oro (FEAT-004):** alimentada SOLO de silver (el oro NO limpia). SQL CTAS
    `resources/sql/fase3_materializar_oro.sql` (= notebook `notebooks/fase3_oro`). Conteos reales:
    `gold.world_port_index`=**3.669** puertos (WPI convertido a CSV localmente con `dbfread`,
    sin instalar geopandas/pyshp en serverless), `gold.trayectorias_diarias`=**61.768**
    (1 fila por MMSI+día, haversine R=6371), `gold.presencia_puertos`=**9.153** (1 fila por
    MMSI+puerto+día; 349 puertos cruzados), `gold.matriz_trafico`=**2.319.365** (1 fila por
    celda+hora; grilla redondeo lat/lon 2 dec, ADR-001, NO H3).
  - **Prueba de compuerta (FEAT-005):** notebook `notebooks/fase3_prueba_compuerta` y SQL
    `resources/sql/fase3_prueba_compuerta.sql` inyectaron en un sandbox aislado
    (`oceanwatch.quality.pf_*`) **9 filas** (un sucio por cada regla de descarte + duplicado +
    3 limpios de control en la celda-puerto NEW YORK CITY). Resultado real: `pf_quarantine`=**5**
    (cada sucio con su causa correcta), `pf_silver`=**3** (solo los limpios; el duplicado lo
    eliminó la dedup), `pf_gold_trayectorias`=**1**, `pf_gold_presencia`=**1** (NEW YORK CITY,
    INDEX_NO 7640, 3 posiciones), `pf_gold_matriz`=**1**. **Ningún** sucio llegó a silver ni a
    oro (0 filas/0 celdas); invariante sandbox `9 = 3 + 5 + 1`. `statement_id` serverless
    capturados. Sandbox `pf_*` eliminado tras capturar la evidencia (`SHOW TABLES LIKE 'pf_*'`
    vacío); tablas de producción intactas.
  - Tests locales (sin Spark): `tests/test_conciliacion.py`, `tests/test_gold_puertos.py`,
    `tests/test_prueba_compuerta.py` (complemento puro de la prueba de compuerta). `pytest -q`:
    **76 passed, 4 skipped**.
- **Decisiones/ADRs tocados:** **ADR-009** (catálogo de reglas, clasificación descartar/nulificar),
  **ADR-010** (máscara tierra/agua, acción nulificar), **ADR-011** (estrategia de cuarentena,
  patrón Dead-letter/Quarantine, causas `array<string>`), **ADR-012** (modelo oro: granularidad,
  claves, 'en puerto' por celda + permanencia ≥3, haversine, grilla redondeo ratificando ADR-001).
- **Estado Verificado vs Diseñado:** **Verificado (ejecutado)** en serverless = plata, cuarentena,
  métricas, invariante, las 4 tablas oro y la prueba de compuerta extremo a extremo. Límite
  conocido (heredado de plata, documentado): `en_agua` queda en `null` en plata (requiere
  `global_land_mask`, no disponible en SQL; procedimiento de enriquecimiento documentado) y
  `distancia_total_km` puede ser no física para MMSI con posiciones AIS que "saltan" (el oro NO
  limpia; filtro de velocidad = regla de plata de fase futura).
- **Síntoma→causa→solución:**
  - *Escritura Delta serverless:* síntoma = `saveAsTable` sobre 25.9M en Spark Connect GC devolvió
    `INTERNAL_ERROR` (y antes `CACHE_REQUIRES_NEWER_CLIENT_ON_SERVERLESS_GC`); causa = limitación
    del cómputo serverless para la escritura Delta a esa escala, no de la lógica; solución (track
    distinto) = ejecutar el MISMO pipeline como **SQL CTAS** en el SQL warehouse serverless, que
    reproduce exactamente las compuertas/transformaciones. Notebooks conservados como orquestador
    de referencia.
  - *Carga del WPI:* síntoma = `read_files` devolvía 0 filas pese a archivo íntegro; causa = un
    `read_files` lanzado antes de completar la subida envenenó la caché de listado del warehouse;
    solución = subir a un path nuevo (`wpi_ref_v2/`) nunca consultado → 3.669 filas.
  - *Presencia de control en puerto (FEAT-005):* síntoma = los limpios no aparecían en
    `pf_gold_presencia`; causa = sus coordenadas redondeaban a (40.70,-74.01) y NEW YORK CITY cae
    en (40.70,-74.02); solución = ubicar los 3 pings en LON -74.016/-74.017/-74.018 (celda
    -74.02) → presencia confirmada (3 posiciones ≥ permanencia 3).
- **Evidencia:** `evidence/req02-capas-calidad/` — `diagnostico_calidad_bronce.md`,
  `disponibilidad_librerias_serverless.md`, `mascara_tierra_agua.md`, `materializacion_plata.md`,
  `invariante_conciliacion.md`, `capa_oro.md`, `prueba_compuerta.md`. Tablas creadas y
  conservadas: `oceanwatch.silver.ais_clean`, `oceanwatch.quarantine.ais_rejected`,
  `oceanwatch.quality.metricas_calidad`, `oceanwatch.gold.world_port_index`,
  `oceanwatch.gold.trayectorias_diarias`, `oceanwatch.gold.presencia_puertos`,
  `oceanwatch.gold.matriz_trafico`. Código: `src/oceanwatch/quality/`, `src/oceanwatch/gold/`,
  `notebooks/fase3_*`, `resources/sql/fase3_*`.
- **Pendientes / siguiente paso:** Fase 4 (Req. 3) — **orquestación** por Job de Databricks con
  cron en UTC e idempotencia (encadenar ingesta → plata/calidad → oro). Enriquecimiento opcional
  de `en_agua` en plata (procedimiento en `materializacion_plata.md`).

### 2026-11-05 — Fase 2: Ingesta streaming (pipeline real + pruebas de fallos + ADRs) — Equipo OceanWatch
- **Objetivo de la sesión:** cerrar el Req. 1 con **evidencia real**: construir y **ejecutar** el
  pipeline de ingesta incremental a bronce en serverless, probar su tolerancia a fallos con 4
  escenarios **ejecutados de verdad**, justificar `Trigger.availableNow` con comportamiento
  observado y documentar las decisiones (ADR-006/007/008).
- **Qué se hizo:**
  - Módulos puros en `src/oceanwatch/ingest/` (`schema`, `paths`, `download`, `autoloader`,
    `dedup`) con imports PySpark perezosos y tests unitarios (`pytest -q`: 20 passed, 1 skipped).
  - Notebook orquestador `notebooks/fase2_ingesta_streaming` e **ingesta real de los 3 días**
    (2023-06-01/02/03) a `oceanwatch.bronze.ais_raw` (Auto Loader + `Trigger.availableNow`,
    append-only). Conteos reales: **25 897 493** filas totales
    (8 808 904 + 9 052 241 + 8 036 348) y **658 duplicados** por `(MMSI, BaseDateTime)`
    (~0,0025 %). Run serverless `847693335141936` (SUCCESS, 167 s).
  - Notebook `notebooks/fase2_pruebas_fallos` con las **4 pruebas de tolerancia a fallos**,
    ejecutado en serverless (run `497716691196081`, SUCCESS, 88 s), persistiendo resultados en
    `oceanwatch.quality.pruebas_fallos`. Las 4 **PASA**: (a) corte+reinicio (final=6, sin
    pérdida/duplicados), (b) archivo reenviado (bronce 3→6, dedup silver estable=3), (c) esquema
    no conforme (stream vivo, 1 fila a `_rescued_data`), (d) reinicio sin reproceso (3→3).
  - ADRs aceptados: **ADR-006** (dedup `MMSI`+`BaseDateTime` con watermark 24h en bronce→silver),
    **ADR-007** (checkpoints en Volume de UC), **ADR-008** (`Status` int + `_rescued_data`).
- **Decisiones/ADRs tocados:** ADR-003 y ADR-004 (respetados y ahora verificados con ejecución
  real), ADR-006, ADR-007, ADR-008 (nuevos).
- **Estado Verificado vs Diseñado:** Verificado (ejecutado) = ingesta real de los 3 días, esquema
  bronce (`Status int` + auditoría), 658 duplicados reales, y las 4 pruebas de fallos en
  serverless. No quedó ninguna prueba como PENDIENTE DE EJECUCIÓN.
- **Síntoma→causa→solución:**
  - *Status string→int:* síntoma = Entrega 1 declaró `Status` como `string`; causa = la muestra
    real de NOAA trae código **entero** (`verificacion_esquema_real.md`); solución = `Status int`
    en el esquema explícito y lo no conforme a `_rescued_data` (ADR-008).
  - *Prueba (c) sin rescatados:* síntoma = primera versión observó `filas_con_rescued_data=0`
    (run `282095277952514`); causa = con header por archivo y esquema explícito Auto Loader
    alineaba columnas por nombre y no rescataba; solución = ajustar el **diseño de la prueba**
    (valor `Status="ABC"` no casteable + valor extra bajo header válido), re-run
    `497716691196081` → `filas_con_rescued_data=1`. No se alteró el pipeline.
- **Evidencia:** `evidence/req01-streaming/ejecucion_ingesta.md`,
  `evidence/req01-streaming/pruebas_tolerancia_fallos.md`,
  `evidence/req01-streaming/justificacion_availablenow.md`,
  `evidence/req01-streaming/verificacion_esquema_real.md`; tablas
  `oceanwatch.quality.ingesta_metricas` y `oceanwatch.quality.pruebas_fallos`.
- **Pendientes / siguiente paso:** Fase 3 (Req. 2) — materializar la tabla **silver** con la dedup
  de ADR-006 y las reglas de calidad/cuarentena (incluida la inspección de `_rescued_data`);
  catálogo de códigos `Status`/`VesselType` para gold.

### 2026-11-05 — Fase 2 (inicio): Bootstrap ejecutado + descarga de corpus — Equipo OceanWatch
- **Objetivo de la sesión:** ejecutar realmente el bootstrap de Unity Catalog (dejar de ser
  "Diseñado") y preparar el corpus del feed para la ingesta streaming.
- **Qué se hizo:** se ejecutó `resources/sql/bootstrap_oceanwatch.sql` (9 sentencias
  idempotentes) contra el workspace institucional usando el SQL warehouse serverless existente
  ("Serverless Starter Warehouse", id `377600528fed7c47`) vía la SQL Statement Execution API.
  Se verificó con `SHOW CATALOGS/SCHEMAS/VOLUMES` que existen el catálogo `oceanwatch`, los 6
  esquemas (bronze, silver, gold, quarantine, quality, ml) y los 2 volúmenes (landing,
  checkpoints). Se inició la descarga de los zips NOAA del corpus base (1–3 jun 2023) para
  inspeccionar el esquema real de los CSV.
- **Decisiones/ADRs tocados:** ADR-002 (nombres verificados en el catálogo real). Corpus del
  feed acotado a 3 días (1–3 jun 2023) para pruebas rápidas de streaming y fallos; diseño
  preparado para fechas nuevas.
- **Estado Verificado vs Diseñado:** Verificado (ejecutado) = bootstrap de Unity Catalog
  (catálogo, esquemas, volúmenes). En curso = descarga del corpus y verificación del esquema
  real de los CSV.
- **Evidencia:** `evidence/req05-gobernanza/bootstrap_unity_catalog.md` (salida real de las
  9 sentencias + SHOW CATALOGS/SCHEMAS/VOLUMES).
- **Pendientes / siguiente paso:** descomprimir los CSV, verificar columnas reales vs esquema
  de 17 campos, e implementar el pipeline Auto Loader → bronze con Trigger.availableNow, dedup
  por MMSI+BaseDateTime y las 4 pruebas de tolerancia a fallos.

### 2026-11-05 — Fase 1: Fundaciones — Equipo OceanWatch
- **Objetivo de la sesión:** establecer la estructura del repositorio y las decisiones de
  arquitectura (ADRs) para la Entrega 2, partiendo del entorno Databricks serverless creado
  desde cero.
- **Qué se hizo:** creada estructura de carpetas (docs/, src/oceanwatch/, resources/,
  evidence/, conf/, tests/, notebooks/, data_samples/); plantilla ADR y bitácora; 5 ADRs
  aceptados (001–005); scripts de bootstrap idempotentes (Python y SQL) marcados PENDIENTE DE
  EJECUCIÓN; config.yaml centralizado sin secretos; .gitignore; README actualizado con pesos
  de Entrega 2 corregidos; matriz de trazabilidad.
- **Decisiones/ADRs tocados:** ADR-001 (medallion serverless), ADR-002 (nombres/catálogo/
  esquemas), ADR-003 (ingesta zip→Auto Loader), ADR-004 (Trigger.availableNow), ADR-005 (ML
  anomalías de velocidad).
- **Estado Verificado vs Diseñado:** Verificado (ejecutado) = inventario del entorno
  Databricks (host, metastore `metastore_aws_us_east_2` en us-east-2, catálogos existentes,
  ausencia de SQL warehouses, cómputo serverless). Diseñado (no ejecutado) = todo el resto,
  incluidos los scripts de bootstrap.
- **Evidencia:** este repositorio (estructura y documentos de Fase 1); inventario del entorno
  citado en ADR-001/ADR-002.
- **Pendientes / siguiente paso:** Fase 2 — implementar la ingesta streaming (Req. 1) y las
  capas bronze/silver con Auto Loader + Trigger.availableNow.
