# Databricks notebook source
# MAGIC %md
# MAGIC # Fase 2 — Pruebas de tolerancia a fallos de la ingesta streaming (Req. 1)
# MAGIC
# MAGIC Ejecuta **4 pruebas de resiliencia** sobre el mismo patrón de ingesta de
# MAGIC `notebooks/fase2_ingesta_streaming` (Auto Loader + `Trigger.availableNow`,
# MAGIC ADR-003/ADR-004) y persiste el resultado **OBSERVADO** de cada una en
# MAGIC `oceanwatch.quality.pruebas_fallos`:
# MAGIC
# MAGIC 1. **(a)** Stream cortado a mitad de lote + reinicio desde el mismo checkpoint.
# MAGIC 2. **(b)** Archivo duplicado/reenviado (append en bronce vs. dedup en silver).
# MAGIC 3. **(c)** Esquema corrupto/truncado (rescued data, el stream no muere).
# MAGIC 4. **(d)** Reinicio desde checkpoint con `availableNow` (no reprocesa).
# MAGIC
# MAGIC Las pruebas usan un **sandbox aislado** bajo
# MAGIC `landing/pruebas_fallos/` y una tabla temporal por prueba en el esquema
# MAGIC `bronze`, para **no** tocar `oceanwatch.bronze.ais_raw` ni sus métricas.
# MAGIC El dataset de cada prueba es un CSV pequeño y sintético con el esquema AIS
# MAGIC real de 17 campos (no se descargan los zips de NOAA). El código pesado vive
# MAGIC en `src/oceanwatch/ingest/`; este notebook solo orquesta.
# MAGIC
# MAGIC Cómputo: **serverless** (una sola corrida vía `jobs submit`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Poner `src` en el path e importar el paquete

# COMMAND ----------

import os
import sys

_NB_DIR = os.path.dirname(os.path.abspath("__file__")) if "__file__" in dir() else os.getcwd()
_candidatos = [
    "/Workspace/Users/jd.torresa1@uniandes.edu.co/oceanwatch/src",
    os.path.abspath(os.path.join(_NB_DIR, "..", "src")),
    os.path.abspath(os.path.join(_NB_DIR, "src")),
]
for _c in _candidatos:
    if os.path.isdir(_c) and _c not in sys.path:
        sys.path.insert(0, _c)

import oceanwatch.ingest.dedup as dedup
from oceanwatch.common.config import catalog_name, fqn, load_config, schema
from oceanwatch.ingest.schema import ais_struct_type

print("Paquete oceanwatch importado. sys.path[0] =", sys.path[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuración y utilidades del sandbox de pruebas

# COMMAND ----------

_CONFIG_CANDIDATOS = [
    "/Workspace/Users/jd.torresa1@uniandes.edu.co/oceanwatch/conf/config.yaml",
    os.path.abspath(os.path.join(_NB_DIR, "..", "conf", "config.yaml")),
]
_cfg_path = next((p for p in _CONFIG_CANDIDATOS if os.path.isfile(p)), None)
cfg = load_config(_cfg_path)

_CAT = catalog_name(cfg)
_BRONZE = schema(cfg, "bronze")
_LANDING = cfg["volumenes"]["landing"]
_CHECKPOINTS = cfg["volumenes"]["checkpoints"]

# Raíces aisladas del sandbox de pruebas (NO tocan landing/csv ni checkpoints de ingesta).
SANDBOX_LANDING = f"/Volumes/{_CAT}/{_BRONZE}/{_LANDING}/pruebas_fallos"
SANDBOX_CKPT = f"/Volumes/{_CAT}/{_BRONZE}/{_CHECKPOINTS}/pruebas_fallos"
TABLA_RESULTADOS = fqn(cfg, "quality", "pruebas_fallos")
RESCUED = cfg["ingesta"]["rescued_column"]

print("SANDBOX_LANDING :", SANDBOX_LANDING)
print("SANDBOX_CKPT    :", SANDBOX_CKPT)
print("TABLA_RESULTADOS:", TABLA_RESULTADOS)

# COMMAND ----------

from pyspark.sql import functions as F  # noqa: E402

# Header AIS real de 17 campos (ver evidence/req01-streaming/verificacion_esquema_real.md).
HEADER_AIS = (
    "MMSI,BaseDateTime,LAT,LON,SOG,COG,Heading,VesselName,IMO,CallSign,"
    "VesselType,Status,Length,Width,Draft,Cargo,TransceiverClass"
)


def _reset_dir(ruta: str) -> None:
    """Borra y recrea un directorio del Volume (idempotencia entre corridas)."""
    try:
        dbutils.fs.rm(ruta, recurse=True)  # noqa: F821
    except Exception:
        pass
    dbutils.fs.mkdirs(ruta)  # noqa: F821


def _escribir_csv(ruta_dir: str, nombre: str, filas: list[str], con_header: bool = True) -> str:
    """Escribe un CSV sintético (header AIS + filas) en el Volume y devuelve su ruta."""
    dbutils.fs.mkdirs(ruta_dir)  # noqa: F821
    cuerpo = ([HEADER_AIS] if con_header else []) + list(filas)
    ruta = f"{ruta_dir}/{nombre}"
    dbutils.fs.put(ruta, "\n".join(cuerpo) + "\n", overwrite=True)  # noqa: F821
    return ruta


def _fila_ais(mmsi: int, ts: str, extra: str = "") -> str:
    """Genera una fila AIS válida (17 campos) con MMSI y BaseDateTime dados."""
    base = f"{mmsi},{ts},30.1,-91.2,0.0,273.0,511.0,BUQUE {mmsi},,WDD{mmsi},60,0,43,18,2.0,60,A"
    return base + extra


def _leer_stream(ruta_landing: str, schema_loc: str):
    """readStream cloudFiles con el mismo patrón que la ingesta real (esquema explícito + rescued)."""
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", schema_loc)
        .option("rescuedDataColumn", RESCUED)
        .schema(ais_struct_type(cfg))
        .load(ruta_landing)
    )


RESULTADOS = []  # acumula un dict por prueba; se persiste al final.


def _registrar(prueba, hipotesis, esperado, observado, estado, detalle):
    RESULTADOS.append(
        {
            "prueba": prueba,
            "hipotesis": hipotesis,
            "esperado": esperado,
            "observado": observado,
            "estado": estado,  # PASA | PENDIENTE
            "detalle": detalle,
        }
    )
    print(f"[{prueba}] estado={estado} | observado={observado}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba (a) — Corte a mitad de lote + reinicio desde el mismo checkpoint
# MAGIC
# MAGIC **Hipótesis:** con `maxFilesPerTrigger=1` el stream procesa los archivos en
# MAGIC varios micro-batches; si se interrumpe tras el primero y se relanza desde el
# MAGIC mismo checkpoint, el conteo final es igual al de una corrida sin corte, sin
# MAGIC pérdida ni duplicados (exactly-once por checkpoint + `txnAppId`/`txnVersion`).

# COMMAND ----------

def _prueba_a():
    base = f"{SANDBOX_LANDING}/a_corte"
    ckpt = f"{SANDBOX_CKPT}/a_corte"
    tabla = f"{_CAT}.{_BRONZE}.pf_a_corte"
    _reset_dir(base)
    _reset_dir(ckpt)
    spark.sql(f"DROP TABLE IF EXISTS {tabla}")  # noqa: F821

    # 3 archivos, 2 filas cada uno (6 filas en total), claves únicas.
    for i in range(3):
        filas = [
            _fila_ais(100000000 + i * 2 + 0, f"2023-06-01T00:0{i}:01"),
            _fila_ais(100000000 + i * 2 + 1, f"2023-06-01T00:0{i}:02"),
        ]
        _escribir_csv(base, f"parte_{i}.csv", filas)

    txn = "pf_a_corte"

    def _batch(bdf, bid):
        (bdf.write.format("delta").mode("append")
            .option("txnAppId", txn).option("txnVersion", bid).saveAsTable(tabla))

    # --- Corrida 1: forzar 1 archivo por trigger y "cortar" tras el primer micro-batch ---
    cortado = {"n": 0}

    def _batch_cortado(bdf, bid):
        if cortado["n"] >= 1:
            raise RuntimeError("CORTE SIMULADO a mitad de lote")
        cortado["n"] += 1
        _batch(bdf, bid)

    q1_error = None
    try:
        q1 = (_leer_stream(base, f"{ckpt}/_schema").writeStream
              .option("checkpointLocation", f"{ckpt}/_ckpt")
              .option("cloudFiles.maxFilesPerTrigger", "1")
              .foreachBatch(_batch_cortado).trigger(availableNow=True).start())
        q1.awaitTermination()
    except Exception as exc:  # el corte propaga una excepción al driver
        q1_error = str(exc)

    conteo_tras_corte = spark.table(tabla).count()  # noqa: F821

    # --- Corrida 2: reiniciar desde el MISMO checkpoint; debe terminar lo pendiente ---
    q2 = (_leer_stream(base, f"{ckpt}/_schema").writeStream
          .option("checkpointLocation", f"{ckpt}/_ckpt")
          .option("cloudFiles.maxFilesPerTrigger", "1")
          .foreachBatch(_batch).trigger(availableNow=True).start())
    q2.awaitTermination()

    conteo_final = spark.table(tabla).count()  # noqa: F821
    distintos = spark.table(tabla).select("MMSI", "BaseDateTime").distinct().count()  # noqa: F821
    esperado_filas = 6

    estado = "PASA" if (conteo_final == esperado_filas and distintos == esperado_filas) else "PENDIENTE"
    observado = (
        f"corte_tras_1_batch={conteo_tras_corte} filas; "
        f"final={conteo_final} filas; distintos_por_clave={distintos}; "
        f"esperado={esperado_filas}; error_corte={'si' if q1_error else 'no'}"
    )
    _registrar(
        "a_corte_reinicio",
        "El corte a mitad de lote + reinicio desde el mismo checkpoint no pierde ni duplica filas.",
        f"conteo_final == {esperado_filas} y sin duplicados por (MMSI,BaseDateTime)",
        observado,
        estado,
        f"q1_error={q1_error}",
    )

_prueba_a()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba (b) — Archivo duplicado/reenviado
# MAGIC
# MAGIC **Hipótesis:** reenviar un CSV ya ingerido con otro nombre hace que **bronce
# MAGIC suba** (es append-only, Auto Loader lo ve como archivo nuevo), pero la
# MAGIC deduplicación silver por `MMSI`+`BaseDateTime` (ADR-006) deja el conteo
# MAGIC estable. Se documentan ambos conteos (bronce vs. dedup).

# COMMAND ----------

def _prueba_b():
    base = f"{SANDBOX_LANDING}/b_duplicado"
    ckpt = f"{SANDBOX_CKPT}/b_duplicado"
    tabla = f"{_CAT}.{_BRONZE}.pf_b_duplicado"
    _reset_dir(base)
    _reset_dir(ckpt)
    spark.sql(f"DROP TABLE IF EXISTS {tabla}")  # noqa: F821

    filas = [
        _fila_ais(200000001, "2023-06-01T01:00:01"),
        _fila_ais(200000002, "2023-06-01T01:00:02"),
        _fila_ais(200000003, "2023-06-01T01:00:03"),
    ]
    _escribir_csv(base, "original.csv", filas)

    txn = "pf_b_duplicado"

    def _batch(bdf, bid):
        (bdf.write.format("delta").mode("append")
            .option("txnAppId", txn).option("txnVersion", bid).saveAsTable(tabla))

    def _ingerir():
        q = (_leer_stream(base, f"{ckpt}/_schema").writeStream
             .option("checkpointLocation", f"{ckpt}/_ckpt")
             .foreachBatch(_batch).trigger(availableNow=True).start())
        q.awaitTermination()

    _ingerir()
    bronce_1 = spark.table(tabla).count()  # noqa: F821

    # Reenviar el MISMO contenido con otro nombre (archivo nuevo para Auto Loader).
    _escribir_csv(base, "reenviado_copia.csv", filas)
    _ingerir()
    bronce_2 = spark.table(tabla).count()  # noqa: F821

    # Dedup silver: watermark 24h + dropDuplicatesWithinWatermark(MMSI,BaseDateTime).
    bronce_df = spark.table(tabla)  # noqa: F821
    metricas = dedup.contar_duplicados(bronce_df, cfg)
    dedup_conteo = metricas["distintos_por_clave"]

    estado = "PASA" if (bronce_2 > bronce_1 and dedup_conteo == 3) else "PENDIENTE"
    observado = (
        f"bronce_tras_original={bronce_1}; bronce_tras_reenvio={bronce_2} (append sube); "
        f"duplicados_por_clave={metricas['duplicados']}; "
        f"dedup_silver_distintos={dedup_conteo} (estable)"
    )
    _registrar(
        "b_archivo_duplicado",
        "Reenviar un archivo ya ingerido sube bronce (append) pero la dedup silver mantiene el conteo.",
        "bronce_tras_reenvio > bronce_tras_original y dedup_silver_distintos == 3",
        observado,
        estado,
        f"metricas={metricas}",
    )

_prueba_b()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba (c) — Esquema corrupto/no conforme (rescued data)
# MAGIC
# MAGIC **Hipótesis:** colocar en landing un CSV con un **valor de tipo no conforme**
# MAGIC (`Status="ABC"` donde el esquema declara `int`, ADR-008) y otro con una
# MAGIC **columna extra** (18 valores bajo el header de 17) **no mata el stream**; los
# MAGIC valores no conformes quedan capturados en `_rescued_data` (esquema-on-read
# MAGIC explícito + rescued data column, ADR-008). Se registra el conteo de filas con
# MAGIC `_rescued_data` no nulo.

# COMMAND ----------

def _prueba_c():
    base = f"{SANDBOX_LANDING}/c_esquema"
    ckpt = f"{SANDBOX_CKPT}/c_esquema"
    tabla = f"{_CAT}.{_BRONZE}.pf_c_esquema"
    _reset_dir(base)
    _reset_dir(ckpt)
    spark.sql(f"DROP TABLE IF EXISTS {tabla}")  # noqa: F821

    # Archivo válido (2 filas conformes).
    _escribir_csv(base, "valido.csv", [
        _fila_ais(300000001, "2023-06-01T02:00:01"),
        _fila_ais(300000002, "2023-06-01T02:00:02"),
    ])
    # Archivo con TIPO CORRUPTO: Status (declarado int en el esquema, ADR-008) trae
    # un valor no numérico "ABC"; el valor no conforme debe caer a _rescued_data sin
    # matar el stream. La fila se construye a mano respetando el header de 17 campos.
    fila_status_corrupto = (
        "300000003,2023-06-01T02:00:03,30.1,-91.2,0.0,273.0,511.0,BUQUE 300000003,,"
        "WDD300000003,60,ABC,43,18,2.0,60,A"
    )
    _escribir_csv(base, "tipo_corrupto.csv", [fila_status_corrupto])
    # Archivo con COLUMNA EXTRA al final (18 valores bajo el header de 17): el valor
    # sobrante no tiene columna en el esquema y debe caer a _rescued_data.
    _escribir_csv(base, "columna_extra.csv", [
        _fila_ais(300000004, "2023-06-01T02:00:04", extra=",CAMPO_EXTRA"),
    ])

    txn = "pf_c_esquema"
    error_stream = None

    def _batch(bdf, bid):
        (bdf.write.format("delta").mode("append")
            .option("txnAppId", txn).option("txnVersion", bid).saveAsTable(tabla))

    try:
        q = (_leer_stream(base, f"{ckpt}/_schema").writeStream
             .option("checkpointLocation", f"{ckpt}/_ckpt")
             .foreachBatch(_batch).trigger(availableNow=True).start())
        q.awaitTermination()
    except Exception as exc:
        error_stream = str(exc)

    total = spark.table(tabla).count()  # noqa: F821
    con_rescatado = spark.table(tabla).where(F.col(RESCUED).isNotNull()).count()  # noqa: F821

    # El stream NO debe morir y debe haber al menos una fila con datos rescatados.
    estado = "PASA" if (error_stream is None and con_rescatado >= 1) else "PENDIENTE"
    observado = (
        f"stream_murio={'si' if error_stream else 'no'}; "
        f"filas_ingeridas={total}; filas_con_rescued_data={con_rescatado}"
    )
    _registrar(
        "c_esquema_corrupto",
        "Un valor de tipo no conforme (Status='ABC') y una columna extra no matan el stream; lo no conforme va a _rescued_data.",
        "stream_murio == no y filas_con_rescued_data >= 1",
        observado,
        estado,
        f"error_stream={error_stream}",
    )

_prueba_c()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prueba (d) — Reinicio desde checkpoint con `availableNow` (no reprocesa)
# MAGIC
# MAGIC **Hipótesis:** con `Trigger.availableNow` una segunda corrida seguida, sin
# MAGIC archivos nuevos en landing, **no reprocesa** los archivos ya consumidos
# MAGIC (el checkpoint guarda los offsets de archivos); el conteo de bronce no
# MAGIC cambia entre la primera y la segunda corrida (ADR-004/ADR-007).

# COMMAND ----------

def _prueba_d():
    base = f"{SANDBOX_LANDING}/d_reinicio"
    ckpt = f"{SANDBOX_CKPT}/d_reinicio"
    tabla = f"{_CAT}.{_BRONZE}.pf_d_reinicio"
    _reset_dir(base)
    _reset_dir(ckpt)
    spark.sql(f"DROP TABLE IF EXISTS {tabla}")  # noqa: F821

    _escribir_csv(base, "dia1.csv", [
        _fila_ais(500000001, "2023-06-01T03:00:01"),
        _fila_ais(500000002, "2023-06-01T03:00:02"),
        _fila_ais(500000003, "2023-06-01T03:00:03"),
    ])

    txn = "pf_d_reinicio"

    def _batch(bdf, bid):
        (bdf.write.format("delta").mode("append")
            .option("txnAppId", txn).option("txnVersion", bid).saveAsTable(tabla))

    def _ingerir():
        q = (_leer_stream(base, f"{ckpt}/_schema").writeStream
             .option("checkpointLocation", f"{ckpt}/_ckpt")
             .foreachBatch(_batch).trigger(availableNow=True).start())
        q.awaitTermination()

    _ingerir()
    conteo_1 = spark.table(tabla).count()  # noqa: F821
    # Segunda corrida SIN archivos nuevos: availableNow no debe reprocesar.
    _ingerir()
    conteo_2 = spark.table(tabla).count()  # noqa: F821

    estado = "PASA" if (conteo_1 == 3 and conteo_2 == conteo_1) else "PENDIENTE"
    observado = (
        f"conteo_corrida_1={conteo_1}; conteo_corrida_2={conteo_2} "
        f"(sin archivos nuevos); reproceso={'no' if conteo_2 == conteo_1 else 'si'}"
    )
    _registrar(
        "d_reinicio_checkpoint",
        "Con availableNow, una segunda corrida sin archivos nuevos no reprocesa lo ya consumido.",
        "conteo_corrida_2 == conteo_corrida_1 (sin reproceso)",
        observado,
        estado,
        "",
    )

_prueba_d()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persistir resultados OBSERVADOS en `oceanwatch.quality.pruebas_fallos`

# COMMAND ----------

resultados_df = (
    spark.createDataFrame(RESULTADOS)  # noqa: F821
    .select("prueba", "hipotesis", "esperado", "observado", "estado", "detalle")
    .withColumn("_registrado", F.current_timestamp())
)
resultados_df.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(TABLA_RESULTADOS)
print("Resultados escritos en", TABLA_RESULTADOS)

# COMMAND ----------

display(spark.table(TABLA_RESULTADOS).orderBy("prueba"))  # noqa: F821
