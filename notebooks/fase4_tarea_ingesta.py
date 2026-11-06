# Databricks notebook source
# MAGIC %md
# MAGIC # Fase 4 — Tarea `ingesta` del Job (Req. 3, Orquestación)
# MAGIC
# MAGIC Primera tarea del Job `oceanwatch_pipeline_entrega2`. Descarga y descomprime
# MAGIC **SOLO** el día `fecha_proceso` del feed AIS de NOAA al Volume landing y corre
# MAGIC la ingesta Auto Loader + `Trigger.availableNow` (ADR-003/ADR-004) hacia
# MAGIC `oceanwatch.bronze.ais_raw` (append-only). La idempotencia exactly-once la dan el
# MAGIC checkpoint del stream + `txnAppId`/`txnVersion` y la descarga/extracción idempotente.
# MAGIC
# MAGIC El código pesado vive en `src/oceanwatch/ingest/`; este notebook SOLO orquesta.
# MAGIC Cómputo: **serverless** (tarea `notebook_task` del Job, sin `new_cluster`).

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

import oceanwatch.ingest.autoloader as autoloader
import oceanwatch.ingest.dedup as dedup
import oceanwatch.ingest.download as download
import oceanwatch.ingest.paths as paths
import oceanwatch.orquestacion as orq
from oceanwatch.common.config import fqn, load_config

print("Paquete oceanwatch importado. sys.path[0] =", sys.path[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parámetros del Job (widgets) y configuración

# COMMAND ----------

dbutils.widgets.text("fecha_proceso", "2023-06-03")  # noqa: F821
dbutils.widgets.text("job_run_id", "0")              # noqa: F821
dbutils.widgets.text("job_id", "0")                  # noqa: F821
dbutils.widgets.text("task_key", "ingesta")          # noqa: F821

fecha_proceso = orq.validar_fecha_proceso(dbutils.widgets.get("fecha_proceso"))  # noqa: F821
job_run_id = int(dbutils.widgets.get("job_run_id"))  # noqa: F821
job_id = int(dbutils.widgets.get("job_id"))          # noqa: F821
task_key = dbutils.widgets.get("task_key") or "ingesta"  # noqa: F821

_CONFIG_CANDIDATOS = [
    "/Workspace/Users/jd.torresa1@uniandes.edu.co/oceanwatch/conf/config.yaml",
    os.path.abspath(os.path.join(_NB_DIR, "..", "conf", "config.yaml")),
]
_cfg_path = next((p for p in _CONFIG_CANDIDATOS if os.path.isfile(p)), None)
cfg = load_config(_cfg_path)

from datetime import datetime, timezone

_ts_inicio = datetime.now(timezone.utc).isoformat()
print("fecha_proceso =", fecha_proceso)
print("Tabla bronce:", autoloader.tabla_bronze_fqn(cfg))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Descarga + descompresión SOLO de `fecha_proceso` a landing (idempotente)

# COMMAND ----------

url = paths.url_zip_noaa(cfg, fecha_proceso)
zip_dest = paths.landing_zip_path(cfg, fecha_proceso)
csv_dir = paths.landing_csv_dir(cfg, fecha_proceso)
print(f"[{fecha_proceso}] descargando {url}")
download.descargar_zip(url, zip_dest)
if not download.verificar_zip(zip_dest):
    raise RuntimeError(f"Zip corrupto para {fecha_proceso}: {zip_dest}")
extraidos = download.descomprimir_a_landing(zip_dest, csv_dir)
print(f"[{fecha_proceso}] CSV en landing: {extraidos}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Ingesta Auto Loader -> bronce (append-only, Trigger.availableNow)
# MAGIC
# MAGIC Auto Loader descubre los archivos NUEVOS en landing; el checkpoint evita
# MAGIC reprocesar lo ya ingerido (idempotencia incremental). Por eso reejecutar la
# MAGIC tarea para la MISMA fecha no duplica filas en bronce.

# COMMAND ----------

query = autoloader.ejecutar_ingesta(spark, cfg, [fecha_proceso])  # noqa: F821
print("Ingesta terminada. Último progreso:")
print(query.lastProgress)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Conteos de la fecha + métricas a quality.ingesta_metricas

# COMMAND ----------

from pyspark.sql import functions as F

tabla_bronze = autoloader.tabla_bronze_fqn(cfg)
bronze_df = spark.table(tabla_bronze)  # noqa: F821
bronze_fecha = bronze_df.filter(F.col("_batch_date") == F.lit(fecha_proceso))

filas_fecha = bronze_fecha.count()
total = bronze_df.count()
metricas_dup = dedup.contar_duplicados(bronze_fecha, cfg)
print("Filas de la fecha:", filas_fecha, "| total bronce:", total)
print("Duplicados por clave (fecha):", metricas_dup)

# COMMAND ----------

# Persistir métricas de ESTA fecha en quality.ingesta_metricas con MERGE idempotente
# (por fecha_lote) — escritura vía spark.sql, NO df.write (fricción serverless).
tabla_metricas = fqn(
    cfg, cfg["ingesta"]["tabla_metricas"]["esquema"], cfg["ingesta"]["tabla_metricas"]["objeto"]
)
spark.sql(  # noqa: F821
    f"""
    CREATE TABLE IF NOT EXISTS {tabla_metricas} (
      fecha_lote STRING, filas BIGINT, total_corpus BIGINT,
      total_filas_dedup_clave BIGINT, distintos_por_clave BIGINT,
      duplicados_por_clave BIGINT, _registrado TIMESTAMP
    ) USING DELTA
    """
)
spark.sql(  # noqa: F821
    f"""
    MERGE INTO {tabla_metricas} AS t
    USING (SELECT '{fecha_proceso}' AS fecha_lote) AS s
    ON t.fecha_lote = s.fecha_lote
    WHEN MATCHED THEN UPDATE SET
      filas = {int(filas_fecha)}, total_corpus = {int(total)},
      total_filas_dedup_clave = {int(metricas_dup['total'])},
      distintos_por_clave = {int(metricas_dup['distintos_por_clave'])},
      duplicados_por_clave = {int(metricas_dup['duplicados'])},
      _registrado = current_timestamp()
    WHEN NOT MATCHED THEN INSERT (fecha_lote, filas, total_corpus,
      total_filas_dedup_clave, distintos_por_clave, duplicados_por_clave, _registrado)
    VALUES ('{fecha_proceso}', {int(filas_fecha)}, {int(total)},
      {int(metricas_dup['total'])}, {int(metricas_dup['distintos_por_clave'])},
      {int(metricas_dup['duplicados'])}, current_timestamp())
    """
)
print("Métricas de la fecha escritas en", tabla_metricas)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Auditoría de la tarea (MERGE idempotente por job_run_id+task_key)

# COMMAND ----------

tabla_aud = orq.tabla_auditoria_fqn(cfg)
filas_clave = {
    "filas_fecha": int(filas_fecha),
    "total_bronce": int(total),
    "duplicados_fecha": int(metricas_dup["duplicados"]),
}
fila = orq.fila_auditoria(
    job_run_id=job_run_id, job_id=job_id, task_key=task_key, fecha_proceso=fecha_proceso,
    estado="SUCCESS", ts_inicio=_ts_inicio,
    ts_fin=datetime.now(timezone.utc).isoformat(),
    filas_clave=filas_clave, detalle="Ingesta Auto Loader de la fecha_proceso.",
)
spark.sql(orq.merge_auditoria_sql(tabla_aud, fila))  # noqa: F821
print("Auditoría escrita en", tabla_aud)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Resultado para el orquestador (`dbutils.notebook.exit`)

# COMMAND ----------

import json

dbutils.notebook.exit(json.dumps(filas_clave))  # noqa: F821
