# Revisión semántica — FASE 2, Requisito 1 (Ingesta en streaming)

Pipeline de ingesta incremental AIS→bronce con Auto Loader y `Trigger.availableNow`, ejecutado de verdad en Databricks serverless sobre los 3 días del corpus (2023-06-01/02/03), más 4 pruebas de tolerancia a fallos también ejecutadas y una capa de documentación académica (ADR-003/004/006/007/008, bitácora, matriz de trazabilidad). El código pesado vive en módulos puros de `src/oceanwatch/ingest/` con imports PySpark perezosos; los notebooks solo orquestan. La evidencia registra salidas literales de tablas Delta (`oceanwatch.quality.ingesta_metricas` y `oceanwatch.quality.pruebas_fallos`) con `run_id`, estado del run y conteos reales, no inventados.

**Watch for:** (1) ADR-004 §8 quedó con "Estado actual: Diseñado (no ejecutado)" pese a que el trigger ya se ejecutó y verificó esta fase — inconsistencia documental menor (confirmado). (2) La prueba (a) no llegó a interrumpir el stream tras el primer micro-batch porque serverless consolidó los 3 archivos en un único batch; el equipo lo documenta con transparencia y la propiedad queda cubierta por la prueba (d) (confirmado, no bloqueante).

**Verdict**: APPROVED

## Vista de alto nivel

El diseño separa funciones puras testeables (schema, paths, download, dedup) de la frontera con Spark (autoloader), con imports perezosos de modo que todo `src/oceanwatch/ingest/` se importa sin un entorno Spark. Esto permite que `pytest` corra localmente sin PySpark y que la ejecución real viva en los notebooks sobre serverless. La verificación local confirma 20 passed / 1 skipped y que los imports perezosos funcionan sin Spark.

La ingesta real de los 3 días produjo 25 897 493 filas con 658 duplicados exactos por `(MMSI, BaseDateTime)` (~0,0025 %); bronce es append-only y la deduplicación se difiere a bronce→silver con watermark de 24h (ADR-006). El `run_id 847693335141936` terminó en SUCCESS y los conteos están persistidos y transcritos literalmente en la evidencia.

Las 4 pruebas de resiliencia corrieron en un sandbox aislado (`landing/pruebas_fallos`, `checkpoints/pruebas_fallos`, tablas `pf_*`) que no toca `ais_raw`, y persistieron su resultado OBSERVADO en `oceanwatch.quality.pruebas_fallos` (`run_id 497716691196081`, SUCCESS). Las 4 marcan PASA. La prueba (c) tuvo una iteración de diseño honesta: una primera versión observó 0 filas rescatadas y el equipo corrigió el diseño de la prueba (no el pipeline) hacia una no-conformidad de tipo real.

La postura de seguridad y manejo de credenciales es correcta: la key de Databricks vive fuera del repo, `config.yaml` solo contiene el host como referencia sin token, y los scripts temporales con prefijo `_` (incluido `_dbx_sql.py`, que lee el token) residen fuera del repo. El escaneo confirma que no hay `*.key` ni scripts temporales dentro del repo; los únicos archivos con guion bajo dentro son `__init__.py` y bytecode `__pycache__`.

Los cinco ADRs cumplen la plantilla de 9 secciones con Estado Aceptado, 3 alternativas en tabla con columna de evidencia, decisión inequívoca y referencias fechadas; citan los conteos reales de la ejecución. La única fricción documental es que ADR-004 dejó su línea de estado en "Diseñado", desalineada con la bitácora y la matriz que ya lo reportan verificado.

<details>
<summary>Issues (4)</summary>

1. **ADR-004 estado desactualizado** — §8 dice "Estado actual: Diseñado (no ejecutado)" aunque el trigger se ejecutó y verificó esta fase (pruebas (a)/(d), run 497716691196081). Actualizar a "Verificado (ejecutado)" como los demás ADRs. (severidad: low)
2. **Prueba (a) no ejerció el corte** — serverless consolidó los 3 archivos en un micro-batch y la excepción "tras el primer batch" no se disparó; está documentado con transparencia y la propiedad la cubre la prueba (d). Opcional: forzar más de un micro-batch con `maxBytesPerTrigger` muy bajo para ejercer el corte explícitamente. (severidad: low, no bloqueante)
3. **`__pycache__` no ignorado** — hay bytecode `__pycache__/*.pyc` bajo `src/` como archivos sin seguir; no es un temporal de ejecución ni un secreto, pero convendría añadirlo a `.gitignore`. (severidad: info)
4. **Trabajo Req.1 sin commitear por completo** — buena parte de `src/`, `tests/`, `evidence/` y ADRs figuran como untracked en `git status`; no afecta la corrección pero conviene confirmar que entrará al commit/entrega. (severidad: info)

</details>

<details>
<summary>Details</summary>

## Abstracción de ingesta: módulos puros + frontera Spark perezosa

El núcleo del diseño es la separación entre lógica pura y la frontera con Spark. `paths.py`, `schema.py` (salvo `ais_struct_type`), `dedup.py` (salvo `deduplicar`) y las partes de decisión de `download.py` no importan PySpark y se prueban con dobles ligeros. El único punto que necesita Spark, `ais_struct_type`, difiere el import de `pyspark.sql.types` al interior de la función (`oceanwatch.common.config`). El resultado es que `PYTHONPATH=src python -c "import oceanwatch.ingest.autoloader, ..."` importa sin Spark — verificado en esta revisión (IMPORTS OK) — y `pytest -q` corre sin PySpark instalado, saltando únicamente el test de `StructType` vía `pytest.importorskip`.

`autoloader.py` concentra la frontera streaming: `leer_stream_csv` fija esquema explícito de 17 campos, `rescuedDataColumn`, `cloudFiles.schemaLocation` coubicado en el Volume de checkpoints y lectura recursiva de `date=`. La escritura usa `foreachBatch` con `txnAppId`/`txnVersion=batch_id` para idempotencia exactly-once y `trigger(availableNow=True)`. El parámetro `fecha` de `leer_stream_csv`/`ejecutar_ingesta` quedó reservado y sin uso (la lectura es recursiva por Auto Loader); está documentado como compatibilidad futura y no afecta la corrección.

## Ejecución real de la ingesta y conteos

`ejecucion_ingesta.md` transcribe la salida literal: filas por día (8 808 904 / 9 052 241 / 8 036 348), total 25 897 493, y duplicados por clave natural (total 25 897 493, distintos 25 896 835, duplicados 658). El `DESCRIBE TABLE` confirma los 17 campos AIS con `Status int` (corrección frente a Entrega 1) más las 4 columnas de auditoría. `run_id 847693335141936`, SUCCESS, 167 s, `effective_performance_target PERFORMANCE_OPTIMIZED` (serverless). Las métricas quedaron persistidas en `oceanwatch.quality.ingesta_metricas` (3 filas, una por día) con marca `_registrado`, lo que ancla la evidencia a una ejecución concreta y reproducible vía `_dbx_sql.py`.

Los 658 duplicados comparten `BaseDateTime`, muy por debajo del watermark de 24h, lo que sustenta la elección de ventana de ADR-006; la dedup diferida a silver es consistente entre evidencia, ADR-006 y `dedup.py`.

## Pruebas de tolerancia a fallos: sandbox aislado y resultados observados

Las 4 pruebas corrieron en `run_id 497716691196081` (SUCCESS, 88 s) sobre un sandbox (`landing/pruebas_fallos`, `checkpoints/pruebas_fallos`, tablas `oceanwatch.bronze.pf_*`) que no toca `ais_raw` ni sus métricas, evitando contaminar la ingesta real. Cada prueba escribe su resultado OBSERVADO a `oceanwatch.quality.pruebas_fallos` y la evidencia transcribe la lectura literal de esa tabla.

- (b) archivo reenviado: bronce 3→6 (append sube), dedup silver estable en 3 distintos. Verifica directamente ADR-006 a pequeña escala.
- (c) esquema no conforme: `stream_murio=no; filas_ingeridas=4; filas_con_rescued_data=1`. Verifica ADR-008.
- (d) reinicio con availableNow: `conteo_corrida_1=3; conteo_corrida_2=3; reproceso=no`. Verifica ADR-004/ADR-007.
- (a) corte + reinicio: `final=6; distintos=6`, pero el corte no llegó a ejercerse porque serverless consolidó los 3 archivos en un único micro-batch pese a `maxFilesPerTrigger=1`, y la excepción condicionada a "tras el primer batch" no se disparó.

Dos observaciones honestas del equipo merecen reconocimiento: la nota de interpretación de la prueba (a) no disfraza el hecho de que el corte no se ejerció, y la iteración de la prueba (c) (del run previo 282095277952514 con 0 rescatados al run final con 1) documenta que se corrigió el *diseño de la prueba* y no el pipeline. Esto es exactamente el tipo de transparencia que evita resultados inflados. La única mejora opcional sería forzar realmente más de un micro-batch en (a) con un `maxBytesPerTrigger` diminuto para ejercer la interrupción; no es bloqueante porque la reanudación exacta queda cubierta por (d).

## Seguridad y manejo de credenciales

El escaneo de patrones de token (`dapi...`, `DATABRICKS_TOKEN`) solo encuentra coincidencias fuera del repo (el `databricks.key` y el helper `_dbx_sql.py` que lo lee) y, dentro del repo, únicamente referencias en prosa de documentación/plan que describen el procedimiento sin exponer valores. `conf/config.yaml` lleva solo el host como referencia, sin token. No hay `*.key` ni scripts temporales con prefijo `_` dentro del repo: los únicos nombres con guion bajo dentro son `__init__.py` y bytecode `__pycache__`.

## ADRs y documentación académica

ADR-006/007/008 (nuevos) y ADR-003/004 cumplen la plantilla: 9 secciones, Estado Aceptado, 3 alternativas en tabla con columna de evidencia, decisión inequívoca (sin "X o Y") y referencias con acceso 2026-11-05. Las justificaciones citan los conteos reales (658 duplicados) y enlazan cada decisión con la prueba que la verifica (ADR-006→(b), ADR-007→(a)/(d), ADR-008→(c)). La bitácora tiene la entrada 2026-11-05 "Fase 2" como la más reciente, con el patrón síntoma→causa→solución para el cambio `Status` string→int y para la iteración de la prueba (c). La matriz de trazabilidad actualizó la fila Req.1 a ADR-003/004/006/007/008, el código, la evidencia completa y estado "Verificado (ejecutado)".

La única inconsistencia es ADR-004 §8, que conserva "Estado actual: Diseñado (no ejecutado)" pese a que el trigger ya se ejecutó y verificó esta fase (pruebas (a)/(d)); la bitácora y la matriz ya lo reportan como verificado, así que la línea del ADR quedó rezagada. Es un ajuste de una línea.

## Verificación ejecutada en esta revisión

Spot-checks estrechos (no re-ejecución de jobs ni de la ingesta): `pytest -q` → 20 passed, 1 skipped (coincide con lo registrado); `PYTHONPATH=src python -c "import ..."` → IMPORTS OK; escaneo de `*.key` y de archivos con prefijo `_` dentro del repo → ninguno (solo `__init__`/`__pycache__`); escaneo de patrones de token → solo fuera del repo. Los conteos de Databricks se tomaron de la evidencia registrada tal como pide la consigna.

</details>

<details>
<summary>Mapa de archivos</summary>

- `conf/config.yaml` — `Status` a int, `fecha_fin_default` 2023-06-03, bloque `ingesta` (sin secretos).
- `src/oceanwatch/ingest/schema.py` — esquema AIS explícito y columnas de auditoría (puro + wrapper perezoso).
- `src/oceanwatch/ingest/paths.py` — rutas/nombres NOAA y de landing/checkpoints (puro, validación de fecha).
- `src/oceanwatch/ingest/download.py` — descarga/verificación/descompresión idempotente (solo stdlib).
- `src/oceanwatch/ingest/autoloader.py` — Auto Loader + availableNow + foreachBatch idempotente (imports perezosos).
- `src/oceanwatch/ingest/dedup.py` — dropDuplicatesWithinWatermark + conteo de duplicados.
- `src/oceanwatch/common/config.py` — carga de config y StructType con import PySpark perezoso.
- `tests/test_{schema,paths,download,dedup}.py` — tests de funciones puras (20 passed, 1 skipped).
- `notebooks/fase2_ingesta_streaming.py` — orquestador de la ingesta real.
- `notebooks/fase2_pruebas_fallos.py` — 4 pruebas de resiliencia en sandbox aislado.
- `docs/adr/ADR-00{3,4,6,7,8}-*.md` — decisiones de ingesta/trigger/dedup/checkpoints/Status.
- `docs/bitacora.md` — entrada 2026-11-05 Fase 2 (más reciente).
- `docs/traceability_matrix.md` — fila Req.1 actualizada a "Verificado (ejecutado)".
- `evidence/req01-streaming/{ejecucion_ingesta,pruebas_tolerancia_fallos,justificacion_availablenow,verificacion_esquema_real}.md` — evidencia con salidas literales.

Diff completo: `git -C <repo> diff c540c40 HEAD` más los archivos untracked de `src/`, `tests/`, `evidence/`, `docs/adr/`.

</details>
