# Plan de implementación — Fase 2: Ingesta en streaming (Req. 1, 15%)

Proyecto OceanWatch Analytics · MINE 4213 · Entrega 2. Este plan lo ejecuta un agente de
código paso a paso. Cada ítem deja el repo en estado compilable/verificable. Todo en español,
con evidencia real. NO se inventan resultados: lo que no se pueda ejecutar en serverless se
documenta como PENDIENTE DE EJECUCIÓN con procedimiento exacto.

Rutas absolutas de trabajo:
- Repo: `c:\Users\JUAN DAVID\Documents\UNIANDES\8VO SEMESTRE\SISTEMAS INTENSIVOS EN DATOS\PROYECTO\MINE4213-Proyecto`
- Key Databricks (FUERA del repo): `...\PROYECTO\databricks.key`
- CLI: `C:\Users\JUAN DAVID\AppData\Local\Microsoft\WinGet\Packages\Databricks.DatabricksCLI_Microsoft.Winget.Source_8wekyb3d8bbwe\databricks.exe`
- Helper REST: `...\PROYECTO\_dbx_sql.py` (`run_sql(stmt)`, warehouse `377600528fed7c47`).

---

## Decisiones de diseño de esta fase (cada una con rationale; se formalizan en ADRs 006/007/008)

1. **Tipo estable de `Status` = `int`.** La muestra real (`verificacion_esquema_real.md`)
   muestra códigos enteros (0, 12, 15). Entrega 1 lo trató como string; se corrige a `int` en
   `conf/config.yaml` y en el `StructType`. Rationale: el campo es un código AIS discreto; un
   entero es el tipo estable que la fuente entrega. Valores no numéricos (si llegaran) caen a
   `_rescued_data` sin romper el stream. Se documenta en ADR-008.

2. **La deduplicación va en el paso bronze→silver, NO en bronze.** Bronze es APPEND-ONLY
   (ADR-001, patrón medallion): preserva el crudo exactamente como llegó, auditable. La
   deduplicación por `MMSI`+`BaseDateTime` con watermark se aplica al derivar silver. En bronze
   solo se *mide* el duplicado (conteo) como evidencia, no se elimina. Rationale: separar
   ingesta cruda (idempotente a nivel de archivo vía checkpoint + txnAppId/txnVersion) de la
   conformación semántica (dedup por clave de negocio). Se documenta en ADR-006.

3. **Ventana de watermark para dedup = 24 horas sobre `BaseDateTime`.** El feed es diario; los
   duplicados reales provienen del mismo archivo o de un reenvío del mismo día. Una ventana de
   24h acota el estado del `dropDuplicatesWithinWatermark` sin descartar duplicados legítimos
   entre lotes consecutivos, y tolera desorden de llegada dentro del día. Rationale y evidencia
   (doc de Databricks sobre watermarking) en ADR-006. La dedup se implementa con
   `withWatermark("BaseDateTime", "24 hours").dropDuplicatesWithinWatermark(["MMSI","BaseDateTime"])`.

4. **Orquestación serverless de esta fase = `databricks jobs submit` (one-time run) con tarea
   de notebook.** Esta fase NO crea el Job con cron (eso es Req. 3, Fase 4). Para ejecutar
   Python/Spark en serverless sin all-purpose cluster se usa un one-time run (`jobs submit`)
   con una `notebook_task` sobre un notebook importado al workspace, con
   `environment`/serverless. Rationale: `submit` no deja un Job persistente que ensucie el
   workspace y es suficiente para ingerir y para las pruebas; `create + run-now` se reserva a
   la orquestación productiva. Se documenta en ADR-007 (checkpoints) y se referencia en la
   bitácora.

5. **Lectura de conteos reales de vuelta.** El notebook de ingesta escribe sus métricas
   (filas por día, total, duplicados detectados) a una tabla Delta de auditoría
   `oceanwatch.quality.ingesta_metricas` y además las imprime. El orquestador local las lee con
   `_dbx_sql.py` (`SELECT * FROM oceanwatch.quality.ingesta_metricas`) vía el SQL warehouse
   serverless, y copia la salida literal a `evidence/req01-streaming/`. Rationale: el SQL
   warehouse ya está verificado y `_dbx_sql.py` ya funciona; evita depender del parseo de logs
   del Job.

6. **Metadatos de auditoría en bronze.** Cada fila lleva `_ingest_timestamp` (timestamp de
   ingesta), `_source_file` (`_metadata.file_path` de Auto Loader), `_batch_date` (fecha de
   lote derivada de la subruta `date=YYYY-MM-DD`) y `_rescued_data` (columna de datos
   rescatados). Rationale: trazabilidad y soporte de las pruebas de fallo (idempotencia por
   archivo, cuarentena de corruptos).

> **Inconsistencias detectadas en el repo que este plan corrige.** (a) `conf/config.yaml`
> declara `Status: string` → se cambia a `int` (decisión 1). (b) `conf/config.yaml` tiene
> `fecha_fin_default: 2023-06-07`; el corpus de esta fase son 3 días → se ajusta a
> `2023-06-03` dejando nota de que el diseño admite rangos mayores. (c) La fila Req.1 de la
> matriz dice "Diseñado (no ejecutado)" → pasará a "Verificado (ejecutado)" al cerrar la fase.

---

## Esquema AIS final (17 campos, orden exacto verificado)

`MMSI long` · `BaseDateTime timestamp` · `LAT double` · `LON double` · `SOG double` ·
`COG double` · `Heading double` · `VesselName string` · `IMO string` · `CallSign string` ·
`VesselType int` · **`Status int`** · `Length double` · `Width double` · `Draft double` ·
`Cargo int` · `TransceiverClass string`.

---

## Plan ordenado (por dependencia)

- [ ] 1. **Corregir `conf/config.yaml` y añadir parámetros de ingesta.**
      Cambiar `Status` de `string` a `int`; ajustar `corpus.fecha_fin_default` a `2023-06-03`
      con comentario de resiliencia; añadir bloque `ingesta` con: `tabla_bronze: "ais_raw"`,
      `tabla_metricas` (`quality.ingesta_metricas`), `watermark_dedup: "24 hours"`,
      `clave_dedup: ["MMSI","BaseDateTime"]`, subrutas `landing/csv/date=YYYY-MM-DD/` y
      `landing/zips/`, nombre de checkpoint `bronze/ais_raw`, `rescued_column: "_rescued_data"`,
      `txn_app_id: "oceanwatch_bronze_ais_raw"`. No añadir secretos.
      Files: `conf/config.yaml`
      Verify: `python -c "from oceanwatch.common.config import load_config, ais_struct_type; c=load_config(); print([f for f in c['esquema_ais'] if f['nombre']=='Status'])"` ejecutado con `PYTHONPATH=src` imprime `Status` con `tipo: int`.

- [ ] 2. **Crear el módulo de esquema AIS reutilizable `schema.py`.**
      Funciones puras: `ais_struct_type(cfg)` (puede reusar/mover la de `common/config.py` o
      envolverla), `columnas_ais(cfg)` (lista ordenada de nombres), `columnas_auditoria()`
      (nombres de `_ingest_timestamp`, `_source_file`, `_batch_date`, `_rescued_data`). Import
      de PySpark perezoso (patrón ya usado en `common/config.py`).
      Files: `src/oceanwatch/ingest/schema.py`
      Verify: `PYTHONPATH=src pytest tests/test_schema.py` pasa (ver ítem 9).

- [ ] 3. **Crear el módulo de rutas/fechas `paths.py` (funciones puras).**
      `fecha_a_subruta(fecha_iso)` → `date=2023-06-01`; `landing_csv_dir(cfg, fecha)` y
      `landing_zip_path(cfg, fecha)` → rutas absolutas en el Volume
      `/Volumes/oceanwatch/bronze/landing/...`; `nombre_zip_noaa(fecha)` → `AIS_2023_06_01.zip`;
      `url_zip_noaa(cfg, fecha)` → URL completa a partir de `fuente_noaa`; `clave_dedup(cfg)` →
      lista de columnas. Validar formato de fecha (`YYYY-MM-DD`) y lanzar `ValueError` claro.
      Files: `src/oceanwatch/ingest/paths.py`
      Verify: `PYTHONPATH=src pytest tests/test_paths.py` pasa (ver ítem 9).

- [ ] 4. **Crear el módulo de descarga + descompresión `download.py`.**
      Funciones: `descargar_zip(url, destino, reintentos=3)` que verifica `Content-Length`
      (HEAD) contra el tamaño descargado y aplica reintentos con backoff; `verificar_zip(path)`
      que confirma que el zip abre (`zipfile.is_zipfile` + `testzip`); `descomprimir_a_landing
      (zip_path, destino_dir)` idempotente (si el CSV destino ya existe y no está vacío, no
      reextrae). Pensadas para correr DENTRO de Databricks (descarga rápida por región) sobre el
      Volume; usan stdlib (`urllib`, `zipfile`) sin dependencias nuevas. Las partes puras
      (construcción de nombres, decisión de idempotencia por existencia) deben ser testeables
      sin red.
      Files: `src/oceanwatch/ingest/download.py`
      Verify: `PYTHONPATH=src pytest tests/test_download.py` pasa (tests con archivos zip
      temporales locales, sin red; ver ítem 9).

- [ ] 5. **Crear el pipeline Auto Loader → bronze `autoloader.py`.**
      Función `leer_stream_csv(spark, cfg, fecha=None)`: `spark.readStream.format("cloudFiles")`
      con `cloudFiles.format=csv`, `header=true`, `schema` explícito (ítem 2),
      `cloudFiles.schemaLocation` en `/Volumes/oceanwatch/bronze/checkpoints/bronze/ais_raw/_schema`,
      `rescuedDataColumn="_rescued_data"`, lectura de `landing/csv/` (recursivo por `date=`).
      Función `enriquecer_auditoria(df, cfg)`: añade `_ingest_timestamp=current_timestamp()`,
      `_source_file=col("_metadata.file_path")`, `_batch_date` derivada de la ruta. Función
      `escribir_bronze(df, cfg)`: `writeStream` con `Trigger.availableNow`, `checkpointLocation`
      en el Volume checkpoints, salida a Delta `oceanwatch.bronze.ais_raw` en modo APPEND, e
      idempotencia vía `foreachBatch` que setea `txnAppId`/`txnVersion` antes del `append`.
      Función `ejecutar_ingesta(spark, cfg, fechas)` que orquesta read→enriquecer→write y espera
      a que termine (`awaitTermination`/`processAllAvailable`). Imports PySpark perezosos.
      Files: `src/oceanwatch/ingest/autoloader.py`
      Verify: `PYTHONPATH=src python -c "import oceanwatch.ingest.autoloader"` importa sin error
      (sin Spark, gracias a imports perezosos). Verificación funcional real en ítem 11.

- [ ] 6. **Crear el módulo de dedup bronze→silver `dedup.py` (funciones puras + transform).**
      `deduplicar(df, cfg)`: aplica `withWatermark("BaseDateTime", cfg...watermark)` +
      `dropDuplicatesWithinWatermark(clave_dedup(cfg))`. `contar_duplicados(df, cfg)`: devuelve
      `total`, `distintos_por_clave`, `duplicados = total - distintos` (para evidencia). La
      selección de columnas de clave es pura y testeable.
      Files: `src/oceanwatch/ingest/dedup.py`
      Verify: `PYTHONPATH=src pytest tests/test_dedup.py` pasa (test de la construcción de clave
      y del cálculo de duplicados con listas simuladas; ver ítem 9).

- [ ] 7. **Crear `pyproject.toml` mínimo para empaquetado y pytest.**
      Declarar `[tool.pytest.ini_options] pythonpath = ["src"]` y `testpaths = ["tests"]`, y
      metadatos mínimos del paquete `oceanwatch`. Rationale: hoy no existe config de Python; sin
      `pythonpath=src` pytest no resuelve el paquete. No agregar dependencias nuevas más allá de
      `pyyaml` (ya presente). PySpark NO es dependencia de test (imports perezosos).
      Files: `pyproject.toml`
      Verify: `pytest --collect-only` desde la raíz del repo descubre los tests sin
      `ModuleNotFoundError`.

- [ ] 8. **Crear tests unitarios de funciones puras.**
      `test_schema.py` (17 campos, orden exacto, `Status` es `IntegerType`), `test_paths.py`
      (fecha→`date=YYYY-MM-DD`, nombre y URL del zip, rechazo de fecha inválida),
      `test_download.py` (idempotencia por existencia, verificación de zip con un zip temporal
      creado en el test), `test_dedup.py` (clave de dedup, cálculo de duplicados). Marcar con
      `pytest.importorskip("pyspark")` los asserts que construyan `StructType` si PySpark no
      está; el resto corre sin Spark.
      Files: `tests/test_schema.py`, `tests/test_paths.py`, `tests/test_download.py`, `tests/test_dedup.py`
      Verify: `pytest -q` desde la raíz del repo pasa todos los tests.

- [ ] 9. **Crear el notebook orquestador de ingesta `notebooks/fase2_ingesta_streaming.py`
      (formato `# Databricks notebook source`).**
      Celdas: (0) `%pip`/añadir `src` a `sys.path` o `dbutils` para importar el paquete;
      (1) cargar `conf/config.yaml`; (2) descarga+descompresión de los 3 días a `landing`
      (invoca `download.py`); (3) ingesta Auto Loader→bronze (invoca `autoloader.py`);
      (4) derivar conteos por día/total y duplicados por clave (invoca `dedup.py`) y escribir a
      `oceanwatch.quality.ingesta_metricas`; (5) imprimir métricas. El código pesado vive en
      `src/`; el notebook solo orquesta.
      Files: `notebooks/fase2_ingesta_streaming.py`
      Verify: importación al workspace (`databricks workspace import`) sin error de sintaxis en
      ítem 11; la ejecución real produce las métricas.

- [ ] 10. **Crear el notebook de pruebas de tolerancia a fallos
      `notebooks/fase2_pruebas_fallos.py`.** Cuatro bloques ejecutables en serverless:
      (a) *stream a mitad de lote + reinicio*: ingerir con un límite por micro-batch
      (`cloudFiles.maxFilesPerTrigger=1` o `maxBytesPerTrigger`) para forzar varios batches,
      simular corte (no re-ejecutar el `append` del batch en curso) y reiniciar desde el mismo
      checkpoint; esperado: conteo final == conteo sin corte, sin duplicados.
      (b) *archivo duplicado/reenviado*: copiar un CSV ya ingerido con otro nombre a `landing` y
      re-correr; esperado: Auto Loader lo trata como archivo nuevo → la dedup silver (MMSI+
      BaseDateTime) absorbe los duplicados; documentar conteo bronze (sube) vs silver (estable).
      (c) *esquema corrupto/truncado*: colocar en `landing` un CSV con una columna extra y otro
      truncado a media línea; esperado: el stream NO muere; filas problemáticas quedan en
      `_rescued_data` (evolución de esquema) y/o se desvían con `badRecordsPath`; registrar el
      conteo de filas con `_rescued_data` no nulo.
      (d) *reinicio desde checkpoint*: detener el stream y relanzarlo; esperado: `availableNow`
      no reprocesa archivos ya consumidos (continúa donde quedó); evidencia por conteo estable y
      por el estado del checkpoint. Cada bloque escribe su resultado OBSERVADO a
      `oceanwatch.quality.pruebas_fallos`. Si alguna prueba no se puede ejecutar completa en
      serverless, el notebook deja la limitación registrada y marca PENDIENTE DE EJECUCIÓN con
      el procedimiento exacto (NO inventar resultados).
      Files: `notebooks/fase2_pruebas_fallos.py`
      Verify: ejecución real en ítem 12; resultados OBSERVADOS en la tabla y en evidencia.

- [ ] 11. **Ejecutar la ingesta real de los 3 días en Databricks serverless y capturar conteos.**
      Secuencia exacta (desde `...\PROYECTO`, PowerShell): (1) leer `databricks.key` y exportar
      `$env:DATABRICKS_HOST`/`$env:DATABRICKS_TOKEN`; (2) importar el paquete `src/oceanwatch` y
      el notebook `fase2_ingesta_streaming` al workspace
      (`databricks workspace import-dir` / `import --format SOURCE --language PYTHON`,
      destino bajo `/Users/jd.torresa1@uniandes.edu.co/oceanwatch/`); (3) lanzar un one-time run
      con un script temporal `_submit_ingesta.ps1`/`.py` que llama
      `databricks jobs submit --json ...` con una `notebook_task` serverless apuntando al
      notebook; (4) hacer polling del run (`databricks jobs get-run --output json`) hasta
      `TERMINATED`; (5) leer métricas reales con `_dbx_sql.py`:
      `python _dbx_sql.py 377600528fed7c47 _q.sql` donde `_q.sql` hace
      `SELECT _batch_date, count(*) ... FROM oceanwatch.bronze.ais_raw GROUP BY _batch_date` y
      `SELECT * FROM oceanwatch.quality.ingesta_metricas`.
      Files: scripts temporales `_submit_ingesta.ps1`, `_q.sql` (prefijo `_`, en `...\PROYECTO`,
      FUERA del repo); evidencia nueva `evidence/req01-streaming/ejecucion_ingesta.md`.
      Verify: el run termina `SUCCEEDED/TERMINATED`; `evidence/req01-streaming/ejecucion_ingesta.md`
      contiene la salida literal con filas por día, total y duplicados por MMSI+BaseDateTime.
      Si serverless bloquea algún paso, documentar la limitación real y el procedimiento exacto.

- [ ] 12. **Ejecutar las 4 pruebas de fallo y capturar lo OBSERVADO.**
      Importar y submit del notebook `fase2_pruebas_fallos` (mismo mecanismo que ítem 11), leer
      `oceanwatch.quality.pruebas_fallos` con `_dbx_sql.py`, y escribir
      `evidence/req01-streaming/pruebas_tolerancia_fallos.md` con, por prueba:
      hipótesis / procedimiento / esperado / **OBSERVADO** (salida real). Cualquier prueba no
      ejecutable completa en serverless queda como PENDIENTE DE EJECUCIÓN POR EL EQUIPO con
      procedimiento exacto. También capturar evidencia que **justifique `availableNow`** con
      comportamiento real observado (p. ej. el stream procesa lo disponible y se detiene; dos
      corridas seguidas no duplican en bronze a nivel de archivo).
      Files: scripts temporales `_submit_pruebas.ps1`; evidencia
      `evidence/req01-streaming/pruebas_tolerancia_fallos.md` y
      `evidence/req01-streaming/justificacion_availablenow.md`.
      Verify: los dos archivos de evidencia existen y contienen salida real (o PENDIENTE
      justificado); la tabla `oceanwatch.quality.pruebas_fallos` tiene una fila por prueba.

- [ ] 13. **Escribir ADR-006 (dedup + watermark).**
      Plantilla de 9 secciones (`docs/adr/ADR-TEMPLATE.md`), Estado Aceptado, fecha 2026-11-05,
      Req. 1 (15%), autores del equipo. Contexto: duplicados por reenvío/overlap del feed
      diario. Alternativas: (A) `dropDuplicatesWithinWatermark` 24h en silver [elegida];
      (B) dedup en bronze (rechazada: rompe APPEND-ONLY); (C) `MERGE`/clave en bronze (rechazada:
      coste y acoplamiento). Decisión inequívoca: dedup por MMSI+BaseDateTime, watermark 24h, en
      el paso bronze→silver. Justificación con evidencia (doc Databricks de watermarking) y con
      los conteos reales de duplicados del ítem 11. Patrón: Idempotent Consumer / dedup con
      watermark. Verificación: conteo silver estable ante archivo reenviado (prueba b).
      Files: `docs/adr/ADR-006-dedup-watermark.md`
      Verify: revisión manual — 9 secciones presentes, sin "X o Y", cita la evidencia real.

- [ ] 14. **Escribir ADR-007 (estrategia de checkpoints).**
      9 secciones, Aceptado, 2026-11-05, Req. 1 (15%). Contexto: Auto Loader + availableNow
      requieren estado persistente. Cubrir: qué guarda el checkpoint (offsets de archivos
      procesados, metadatos de commits, `_schema` de Auto Loader), dónde vive
      (`oceanwatch.bronze.checkpoints/bronze/ais_raw`), qué pasa si se pierde (re-descubrimiento
      y posible reprocesamiento → la dedup silver protege contra duplicados; a nivel bronze se
      asume append), y la relación con `txnAppId`/`txnVersion`. Alternativas: checkpoint en
      Volume UC [elegida] vs DBFS vs sin checkpoint (rechazadas). Justificación con la prueba (d)
      del ítem 12. Patrón: Checkpointing / exactly-once de Structured Streaming.
      Files: `docs/adr/ADR-007-estrategia-checkpoints.md`
      Verify: revisión manual — 9 secciones, decisión única, referencia a la prueba (d).

- [ ] 15. **Escribir ADR-008 (tipo estable de `Status` y política de datos rescatados).**
      9 secciones, Aceptado, 2026-11-05, Req. 1 (15%). Decisión no trivial: fijar `Status` como
      `int` (corrige a Entrega 1) y establecer que los valores no conformes caen a
      `_rescued_data` sin matar el stream. Alternativas: `int` [elegida] vs `string` (Entrega 1,
      rechazada: pierde semántica numérica) vs `int` con columna cruda paralela. Justificación
      con la muestra real de `verificacion_esquema_real.md`. Patrón: Schema-on-read explícito +
      rescued data column.
      Files: `docs/adr/ADR-008-tipo-status-rescued-data.md`
      Verify: revisión manual — 9 secciones; coherente con el cambio del ítem 1.

- [ ] 16. **Añadir entrada de bitácora fechada 2026-11-05 (Fase 2: ingesta streaming).**
      Insertar como entrada MÁS RECIENTE (arriba de las existentes), usando la plantilla de
      `docs/bitacora.md`: objetivo, qué se hizo (código modular, notebooks, ingesta real de 3
      días, 4 pruebas), ADRs tocados (003, 004, 006, 007, 008), estado Verificado (lo ejecutado)
      vs Diseñado/PENDIENTE (lo que serverless no permitió), evidencia (rutas en
      `evidence/req01-streaming/`), pendientes. Estructura síntoma→causa→solución donde aplique
      (p. ej. corrección de `Status` string→int).
      Files: `docs/bitacora.md`
      Verify: revisión manual — entrada 2026-11-05 Fase 2 arriba del todo, orden reverso-cronológico intacto.

- [ ] 17. **Actualizar la matriz de trazabilidad (fila Req. 1).**
      ADR(s): `ADR-003, ADR-004, ADR-006, ADR-007, ADR-008`. Código:
      `src/oceanwatch/ingest/ (schema, paths, download, autoloader, dedup)` +
      `notebooks/fase2_*`. Evidencia: `evidence/req01-streaming/` (ejecucion_ingesta,
      pruebas_tolerancia_fallos, justificacion_availablenow, verificacion_esquema_real).
      Bitácora: 2026-11-05. Estado: `Verificado (ejecutado)` — o
      `Verificado parcial + PENDIENTE` si alguna prueba quedó sin ejecutar por límite serverless.
      Files: `docs/traceability_matrix.md`
      Verify: revisión manual — la fila Req. 1 refleja ADRs, código, evidencia y estado reales.

- [ ] 18. **Verificación integral final.**
      Ejecutar `pytest -q` (todos los tests pasan); confirmar que todos los `.md` de evidencia
      existen y contienen salida real o PENDIENTE justificado; confirmar que ningún secreto ni
      archivo `_*.ps1`/`_*.py`/`_*.sql` temporal quedó dentro del repo (viven en `...\PROYECTO`,
      fuera). Limpiar temporales.
      Files: —
      Verify: `pytest -q` verde; `git -C <repo> status` no muestra archivos con prefijo `_` ni
      `*.key` dentro del repo.

---

## Secuencia de ejecución en Databricks (resumen operativo)

1. Autenticación (PowerShell, cada sesión): leer `databricks.key`, exportar
   `$env:DATABRICKS_HOST` y `$env:DATABRICKS_TOKEN`.
2. Importar paquete `src/oceanwatch` y notebooks al workspace
   (`/Users/jd.torresa1@uniandes.edu.co/oceanwatch/`). Comandos multilinea → en scripts `.ps1`
   temporales (prefijo `_`, FUERA del repo); listados con `--output json`.
3. `databricks jobs submit --json @_submit_ingesta.json` (notebook_task serverless) →
   polling `get-run --output json` hasta `TERMINATED`.
4. Leer conteos/métricas con `python _dbx_sql.py 377600528fed7c47 _q.sql`.
5. Repetir submit para el notebook de pruebas de fallo.
6. Copiar salidas literales a `evidence/req01-streaming/`.
7. Escribir ADRs 006/007/008, bitácora y matriz.
8. `pytest -q` y limpieza de temporales.

## Supuestos y lagunas (asumidos razonablemente, documentar al ejecutar)

- Si `jobs submit` con `notebook_task` serverless presenta fricción (p. ej. requiere
  `environment` explícito), usar `jobs create` + `run-now` + `jobs delete` como alternativa,
  documentándolo. No cambia el diseño del pipeline.
- `badRecordsPath` puede no estar disponible con `cloudFiles`+csv en serverless; la vía
  primaria para corruptos es `_rescued_data` + evolución de esquema. Si `badRecordsPath` no
  aplica, documentarlo como limitación real en la prueba (c).
- Retención del zip crudo en `landing`: fuera del alcance de Req. 1 (deuda de ADR-003,
  Fase 4). No se borra el zip en esta fase.
