# Databricks notebook source
# MAGIC %md
# MAGIC # Fase 3 — PRUEBA DE COMPUERTA extremo a extremo (Req. 2, FEAT-005)
# MAGIC
# MAGIC Inyecta registros **sucios conocidos** (uno por cada regla de DESCARTE) y
# MAGIC registros **limpios de control** en un sandbox AISLADO (esquema
# MAGIC `oceanwatch.quality`, tablas con prefijo `pf_`), los pasa por EXACTAMENTE las
# MAGIC mismas funciones de `src/oceanwatch/quality/` (dedup reutilizada de
# MAGIC `oceanwatch.ingest.dedup` + `aplicar_compuertas`) y por la lógica de oro de
# MAGIC `src/oceanwatch/gold/`, escribiendo a `pf_silver` / `pf_quarantine` /
# MAGIC `pf_gold_*`. Demuestra con **conteos reales** que:
# MAGIC
# MAGIC 1. Cada registro sucio aparece en `pf_quarantine` con su causa correcta.
# MAGIC 2. NINGÚN sucio llega a `pf_silver` ni a `pf_gold_*`.
# MAGIC 3. Los limpios de control sí llegan a `pf_silver` y a `pf_gold_*`.
# MAGIC
# MAGIC **NO toca** las tablas de producción (`bronze.ais_raw`, `silver.ais_clean`,
# MAGIC `quarantine.ais_rejected`, `quality.metricas_calidad`, `gold.*`). El sandbox
# MAGIC `pf_*` se ELIMINA tras capturar la evidencia.
# MAGIC
# MAGIC > **Nota de ejecución (fricción serverless, FEAT-003/004):** este notebook
# MAGIC > orquesta el pipeline con la API de Spark. En serverless (Spark Connect GC)
# MAGIC > la escritura Delta a 25.9M falla con `INTERNAL_ERROR`; a la escala pequeña
# MAGIC > de este sandbox (10 filas) no aplica, pero el camino REAL ejecutado y
# MAGIC > versionado es el SQL CTAS `resources/sql/fase3_prueba_compuerta.sql`, que
# MAGIC > reproduce EXACTAMENTE estas compuertas. Este notebook es el orquestador de
# MAGIC > referencia.

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

import oceanwatch.gold.matriz_trafico as matriz_trafico
import oceanwatch.gold.puertos as puertos
import oceanwatch.gold.trayectorias as trayectorias
from oceanwatch.common.config import catalog_name, fqn, load_config, schema
from oceanwatch.ingest.dedup import deduplicar
from oceanwatch.quality.compuertas import aplicar_compuertas

print("Paquete oceanwatch importado. sys.path[0] =", sys.path[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Cargar configuración y nombres de las tablas sandbox `pf_*`

# COMMAND ----------

_CONFIG_CANDIDATOS = [
    "/Workspace/Users/jd.torresa1@uniandes.edu.co/oceanwatch/conf/config.yaml",
    os.path.abspath(os.path.join(_NB_DIR, "..", "conf", "config.yaml")),
]
_cfg_path = next((p for p in _CONFIG_CANDIDATOS if os.path.isfile(p)), None)
cfg = load_config(_cfg_path)

_cat = catalog_name(cfg)
_q = schema(cfg, "quality")  # sandbox vive bajo oceanwatch.quality con prefijo pf_
PF_INPUT = f"{_cat}.{_q}.pf_gate_input"
PF_SILVER = f"{_cat}.{_q}.pf_silver"
PF_QUAR = f"{_cat}.{_q}.pf_quarantine"
PF_TRAY = f"{_cat}.{_q}.pf_gold_trayectorias"
PF_PRES = f"{_cat}.{_q}.pf_gold_presencia"
PF_MATRIZ = f"{_cat}.{_q}.pf_gold_matriz"
TABLA_WPI = fqn(cfg, "gold", cfg["gold"]["tabla_wpi"])

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_cat}.{_q}")  # noqa: F821
print("Sandbox bajo:", f"{_cat}.{_q}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Construir el conjunto sandbox: sucios (uno por regla) + limpios de control

# COMMAND ----------

from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Esquema = 2 columnas de prueba (_caso, _regla_esperada) + 17 AIS + 4 auditoría.
_schema = StructType(
    [
        StructField("_caso", StringType()),
        StructField("_regla_esperada", StringType()),
        StructField("MMSI", LongType()),
        StructField("BaseDateTime", TimestampType()),
        StructField("LAT", DoubleType()),
        StructField("LON", DoubleType()),
        StructField("SOG", DoubleType()),
        StructField("COG", DoubleType()),
        StructField("Heading", DoubleType()),
        StructField("VesselName", StringType()),
        StructField("IMO", StringType()),
        StructField("CallSign", StringType()),
        StructField("VesselType", IntegerType()),
        StructField("Status", IntegerType()),
        StructField("Length", DoubleType()),
        StructField("Width", DoubleType()),
        StructField("Draft", DoubleType()),
        StructField("Cargo", IntegerType()),
        StructField("TransceiverClass", StringType()),
        StructField("_rescued_data", StringType()),
        StructField("_ingest_timestamp", TimestampType()),
        StructField("_source_file", StringType()),
        StructField("_batch_date", StringType()),
    ]
)


def _row(caso, esperada, mmsi, bdt, lat, lon, sog, cog, hdg, ig):
    return (
        caso, esperada, mmsi, datetime.fromisoformat(bdt), lat, lon, sog, cog, hdg,
        "PF_" + caso[:6].upper(), None, "PFX", 70, 0, 100.0, 20.0, 5.0, 70, "A",
        None, datetime.fromisoformat(ig), "pf_gate", "2023-06-01",
    )


_filas = [
    _row("sucio_coord_rango", "coordenadas_en_rango", 367000010, "2023-06-01 10:00:00", 200.0, -74.01, 5.0, 90.0, 180.0, "2023-06-01 10:00:01"),
    _row("sucio_centinela_lat", "coordenada_no_centinela", 367000011, "2023-06-01 10:01:00", 91.0, -74.01, 5.0, 90.0, 180.0, "2023-06-01 10:01:01"),
    _row("sucio_mmsi_8digitos", "mmsi_bien_formado", 30012345, "2023-06-01 10:02:00", 40.70, -74.01, 5.0, 90.0, 180.0, "2023-06-01 10:02:01"),
    _row("sucio_mmsi_mid", "mmsi_mid_valido", 999000000, "2023-06-01 10:03:00", 40.70, -74.01, 5.0, 90.0, 180.0, "2023-06-01 10:03:01"),
    _row("sucio_ts_futuro", "timestamp_valido_no_futuro", 367000012, "2099-01-01 00:00:00", 40.70, -74.01, 5.0, 90.0, 180.0, "2023-06-01 10:04:01"),
    # Duplicado exacto (misma clave MMSI+BaseDateTime que limpio_ctrl_1).
    _row("dup_exacto", "duplicado", 367000020, "2023-06-01 11:00:00", 40.70, -74.01, 6.0, 91.0, 182.0, "2023-06-01 11:00:30"),
    # Limpios de control: 3 pings en la celda-puerto NEW YORK CITY (INDEX_NO 7640,
    # 40.7/-74.016667 => celda 40.70/-74.02), permanencia>=3 => pf_gold_presencia.
    _row("limpio_ctrl_1", "(limpio)", 367000020, "2023-06-01 11:00:00", 40.701, -74.016, 6.0, 90.0, 180.0, "2023-06-01 11:00:05"),
    _row("limpio_ctrl_2", "(limpio)", 367000020, "2023-06-01 11:05:00", 40.702, -74.017, 6.1, 95.0, 100.0, "2023-06-01 11:05:05"),
    _row("limpio_ctrl_3", "(limpio)", 367000020, "2023-06-01 11:10:00", 40.703, -74.018, 6.2, 97.0, 110.0, "2023-06-01 11:10:05"),
]

df_input = spark.createDataFrame(_filas, schema=_schema)  # noqa: F821
df_input.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(PF_INPUT)
print("pf_gate_input filas:", spark.table(PF_INPUT).count())  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Dedup reutilizada (ADR-006) + compuertas (las MISMAS de plata)

# COMMAND ----------

# Dedup bronce->plata reutilizada tal cual (ingest.dedup.deduplicar). Como el notebook
# corre batch, usamos dropDuplicates por la clave natural (equivalente a
# dropDuplicatesWithinWatermark sobre un solo lote); conservamos _caso/_regla_esperada.
clave = cfg["ingesta"]["clave_dedup"]
df_dedup = df_input.dropDuplicates(clave)

col_causas = cfg["quarantine"]["columna_causas"]
df_gated = aplicar_compuertas(df_dedup, cfg, incluir_en_agua=False)

df_silver = df_gated.filter(F.size(F.col(col_causas)) == 0)
df_quar = df_gated.filter(F.size(F.col(col_causas)) > 0).select(
    "_caso", "_regla_esperada", "MMSI", "BaseDateTime", "LAT", "LON",
    F.col(col_causas).alias("causas"), F.current_timestamp().alias("_fecha_cuarentena"),
)

df_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(PF_SILVER)
df_quar.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(PF_QUAR)
print("pf_silver     =", spark.table(PF_SILVER).count())  # noqa: F821
print("pf_quarantine =", spark.table(PF_QUAR).count())  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Capa oro sandbox (lee SOLO de pf_silver, como en producción)

# COMMAND ----------

df_wpi = spark.table(TABLA_WPI)  # noqa: F821
df_silver_pf = spark.table(PF_SILVER)  # noqa: F821

trayectorias.trayectorias_diarias(df_silver_pf, cfg).write.format("delta").mode(
    "overwrite"
).option("overwriteSchema", "true").saveAsTable(PF_TRAY)
puertos.presencia_puertos(df_silver_pf, df_wpi, cfg).write.format("delta").mode(
    "overwrite"
).option("overwriteSchema", "true").saveAsTable(PF_PRES)
matriz_trafico.matriz_trafico(df_silver_pf, cfg).write.format("delta").mode(
    "overwrite"
).option("overwriteSchema", "true").saveAsTable(PF_MATRIZ)

n_tray = spark.table(PF_TRAY).count()  # noqa: F821
n_pres = spark.table(PF_PRES).count()  # noqa: F821
n_matriz = spark.table(PF_MATRIZ).count()  # noqa: F821
print("pf_gold_trayectorias =", n_tray)
print("pf_gold_presencia    =", n_pres)
print("pf_gold_matriz       =", n_matriz)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Resultado para el orquestador (`dbutils.notebook.exit`)

# COMMAND ----------

import json

salida = {
    "pf_input": int(spark.table(PF_INPUT).count()),  # noqa: F821
    "pf_silver": int(spark.table(PF_SILVER).count()),  # noqa: F821
    "pf_quarantine": int(spark.table(PF_QUAR).count()),  # noqa: F821
    "pf_gold_trayectorias": int(n_tray),
    "pf_gold_presencia": int(n_pres),
    "pf_gold_matriz": int(n_matriz),
}
dbutils.notebook.exit(json.dumps(salida))  # noqa: F821
