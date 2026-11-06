# Databricks notebook source
# MAGIC %md
# MAGIC # Fase 2 — Ingesta en streaming (Req. 1)
# MAGIC
# MAGIC Orquesta la ingesta incremental de los CSV AIS de NOAA a la capa bronce
# MAGIC (`oceanwatch.bronze.ais_raw`) con Auto Loader + `Trigger.availableNow`
# MAGIC (ADR-003, ADR-004) y registra conteos reales en
# MAGIC `oceanwatch.quality.ingesta_metricas`.
# MAGIC
# MAGIC El código pesado vive en `src/oceanwatch/ingest/`; este notebook solo orquesta.
# MAGIC Cómputo: **serverless** (una sola corrida vía `jobs submit`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Poner `src` en el path e importar el paquete

# COMMAND ----------

import os
import sys

# El repo se importa al workspace bajo /Users/<usuario>/oceanwatch/. El notebook
# queda en .../oceanwatch/notebooks/ y el paquete en .../oceanwatch/src/.
_NB_DIR = os.path.dirname(os.path.abspath("__file__")) if "__file__" in dir() else os.getcwd()
_candidatos = [
    "/Workspace/Users/jd.torresa1@uniandes.edu.co/oceanwatch/src",
    os.path.abspath(os.path.join(_NB_DIR, "..", "src")),
    os.path.abspath(os.path.join(_NB_DIR, "src")),
]
for _c in _candidatos:
    if os.path.isdir(_c) and _c not in sys.path:
        sys.path.insert(0, _c)

import oceanwatch.ingest.autoloader as autoloader
import oceanwatch.ingest.dedup as dedup
import oceanwatch.ingest.download as download
import oceanwatch.ingest.paths as paths
from oceanwatch.common.config import fqn, load_config

print("Paquete oceanwatch importado. sys.path[0] =", sys.path[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Cargar configuración

# COMMAND ----------

_CONFIG_CANDIDATOS = [
    "/Workspace/Users/jd.torresa1@uniandes.edu.co/oceanwatch/conf/config.yaml",
    os.path.abspath(os.path.join(_NB_DIR, "..", "conf", "config.yaml")),
]
_cfg_path = next((p for p in _CONFIG_CANDIDATOS if os.path.isfile(p)), None)
cfg = load_config(_cfg_path)

FECHAS = ["2023-06-01", "2023-06-02", "2023-06-03"]
print("Fechas del corpus:", FECHAS)
print("Tabla bronce:", autoloader.tabla_bronze_fqn(cfg))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Descarga + descompresión de los 3 días a landing
# MAGIC
# MAGIC Se descarga cada zip de NOAA al Volume `landing/zips/`, se verifica su
# MAGIC integridad y se descomprime a `landing/csv/date=YYYY-MM-DD/` de forma
# MAGIC idempotente (no reextrae si el CSV ya existe).

# COMMAND ----------

for fecha in FECHAS:
    url = paths.url_zip_noaa(cfg, fecha)
    zip_dest = paths.landing_zip_path(cfg, fecha)
    csv_dir = paths.landing_csv_dir(cfg, fecha)
    print(f"[{fecha}] descargando {url}")
    download.descargar_zip(url, zip_dest)
    if not download.verificar_zip(zip_dest):
        raise RuntimeError(f"Zip corrupto para {fecha}: {zip_dest}")
    extraidos = download.descomprimir_a_landing(zip_dest, csv_dir)
    print(f"[{fecha}] CSV en landing: {extraidos}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Ingesta Auto Loader -> bronce (append-only, Trigger.availableNow)

# COMMAND ----------

query = autoloader.ejecutar_ingesta(spark, cfg, FECHAS)  # noqa: F821  (spark lo provee Databricks)
print("Ingesta terminada. Última progreso:")
print(query.lastProgress)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Conteos por día/total y duplicados por clave -> quality.ingesta_metricas

# COMMAND ----------

from pyspark.sql import functions as F

tabla_bronze = autoloader.tabla_bronze_fqn(cfg)
bronze_df = spark.table(tabla_bronze)  # noqa: F821

# Conteo por día (columna de auditoría _batch_date).
conteo_por_dia = {
    r["_batch_date"]: r["filas"]
    for r in bronze_df.groupBy("_batch_date").agg(F.count("*").alias("filas")).collect()
}
total = bronze_df.count()

# Duplicados por clave natural (MMSI + BaseDateTime) sobre todo bronze.
metricas_dup = dedup.contar_duplicados(bronze_df, cfg)

print("Filas por día:", conteo_por_dia)
print("Total:", total)
print("Duplicados por clave:", metricas_dup)

# COMMAND ----------

# Persistir métricas en oceanwatch.quality.ingesta_metricas (una fila por corrida/día).
tabla_metricas = fqn(
    cfg, cfg["ingesta"]["tabla_metricas"]["esquema"], cfg["ingesta"]["tabla_metricas"]["objeto"]
)

filas_metricas = []
for fecha, filas in conteo_por_dia.items():
    filas_metricas.append(
        {
            "fecha_lote": str(fecha),
            "filas": int(filas),
            "total_corpus": int(total),
            "total_filas_dedup_clave": int(metricas_dup["total"]),
            "distintos_por_clave": int(metricas_dup["distintos_por_clave"]),
            "duplicados_por_clave": int(metricas_dup["duplicados"]),
        }
    )

metricas_df = spark.createDataFrame(filas_metricas).withColumn(  # noqa: F821
    "_registrado", F.current_timestamp()
)
metricas_df.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(tabla_metricas)
print("Métricas escritas en", tabla_metricas)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Imprimir métricas finales

# COMMAND ----------

display(spark.table(tabla_metricas).orderBy("fecha_lote"))  # noqa: F821
