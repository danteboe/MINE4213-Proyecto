# Databricks notebook source
# MAGIC %md
# MAGIC # Fase 3 — Capa PLATA y compuertas de calidad (Req. 2)
# MAGIC
# MAGIC Materializa la capa plata con las compuertas de calidad (Structured Streaming
# MAGIC + `foreachBatch`, NO DLT), la cuarentena (patrón Dead-letter/Quarantine,
# MAGIC ADR-011) y las métricas de calidad, sobre las 25.897.493 filas reales de
# MAGIC `oceanwatch.bronze.ais_raw`:
# MAGIC
# MAGIC - `oceanwatch.silver.ais_clean`         — pasa TODAS las compuertas de descarte.
# MAGIC - `oceanwatch.quarantine.ais_rejected`  — registro original + causas + fechas.
# MAGIC - `oceanwatch.quality.metricas_calidad` — por lote/día y por regla.
# MAGIC
# MAGIC El código pesado vive en `src/oceanwatch/quality/`; este notebook SOLO orquesta.
# MAGIC Cómputo: **serverless** (una sola corrida vía `jobs submit`).
# MAGIC
# MAGIC > **Nota de ejecución (friccción serverless real):** este notebook orquesta el
# MAGIC > pipeline con la API de Spark (`materializar.materializar_batch` ->
# MAGIC > `foreachBatch`/escritura Delta). En el cómputo *serverless* (Spark Connect GC)
# MAGIC > la escritura Delta de las tablas sobre 25.9M filas devolvió `INTERNAL_ERROR`
# MAGIC > (y antes `CACHE_REQUIRES_NEWER_CLIENT_ON_SERVERLESS_GC` por `df.cache()`, ya
# MAGIC > eliminado). El camino de ejecución que SÍ materializó las tres tablas de forma
# MAGIC > fiable, con los MISMOS conteos y las mismas compuertas, fue el SQL CTAS de
# MAGIC > `resources/sql/fase3_materializar_plata.sql` en el SQL warehouse serverless
# MAGIC > (id `377600528fed7c47`). Ver `evidence/req02-capas-calidad/materializacion_plata.md`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. `%pip install` de la máscara tierra/agua (ADR-010) y `src` en el path

# COMMAND ----------

# La regla coordenada_en_agua (ADR-010) usa global-land-mask, que NO viene
# preinstalada en serverless (evidence/.../disponibilidad_librerias_serverless.md).
# Versión pinneada por reproducibilidad; debe coincidir con conf['calidad']['coordenada_en_agua'].
%pip install global-land-mask==1.0.0

# COMMAND ----------

dbutils.library.restartPython()  # noqa: F821  (dbutils lo provee Databricks)

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

import oceanwatch.quality.materializar as materializar
from oceanwatch.common.config import load_config
from oceanwatch.quality.conciliacion import verificar_invariante
from oceanwatch.ingest.dedup import contar_duplicados

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

print("Bronce:     ", materializar.tabla_bronze_fqn(cfg))
print("Plata:      ", materializar.tabla_silver_fqn(cfg))
print("Cuarentena: ", materializar.tabla_cuarentena_fqn(cfg))
print("Métricas:   ", materializar.tabla_metricas_fqn(cfg))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Crear esquemas destino si no existen (Unity Catalog)

# COMMAND ----------

from oceanwatch.common.config import catalog_name, schema

_cat = catalog_name(cfg)
for _esq in ("silver", "quarantine", "quality"):
    _nombre = schema(cfg, _esq)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_cat}.{_nombre}")  # noqa: F821
    print("Esquema listo:", f"{_cat}.{_nombre}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Materializar plata + cuarentena + métricas (batch idempotente)
# MAGIC
# MAGIC Lee bronce, deduplica (ADR-006, reutiliza `ingest.dedup`), aplica las compuertas
# MAGIC (nulifica atributos + banderas y construye `causas`), separa y escribe las tres
# MAGIC tablas por `overwrite` (reejecutable). La cuarentena es un *side-output*: no
# MAGIC detiene el flujo.

# COMMAND ----------

conteos = materializar.materializar_batch(spark, cfg)  # noqa: F821
print("Conteos de materialización:", conteos)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Conteos reales y duplicados de la dedup (658)

# COMMAND ----------

from pyspark.sql import functions as F

bronze_df = spark.table(materializar.tabla_bronze_fqn(cfg))  # noqa: F821
n_bronce = bronze_df.count()
n_plata = spark.table(materializar.tabla_silver_fqn(cfg)).count()  # noqa: F821
n_cuarentena = spark.table(materializar.tabla_cuarentena_fqn(cfg)).count()  # noqa: F821

# Duplicados exactos por clave natural (los elimina dropDuplicatesWithinWatermark).
dup = contar_duplicados(bronze_df, cfg)
n_duplicados = dup["duplicados"]

print("bronce     =", n_bronce)
print("plata      =", n_plata)
print("cuarentena =", n_cuarentena)
print("duplicados =", n_duplicados)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verificar el invariante de conciliación

# COMMAND ----------

inv = verificar_invariante(n_bronce, n_plata, n_cuarentena, n_duplicados)
print(inv["descripcion"])
print("¿Cuadra?:", inv["cuadra"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Métricas por día y por regla + conteo de cuarentena por regla

# COMMAND ----------

print("== metricas_calidad (por día y por regla) ==")
display(  # noqa: F821
    spark.table(materializar.tabla_metricas_fqn(cfg)).orderBy("_batch_date", "regla")  # noqa: F821
)

# COMMAND ----------

# Conteo de cuarentena por regla (una fila puede aparecer en varias reglas vía explode;
# el conteo de FILAS en cuarentena es n_cuarentena, no la suma por regla).
col_causas = cfg["quarantine"]["columna_causas"]
cuar_df = spark.table(materializar.tabla_cuarentena_fqn(cfg))  # noqa: F821
print("== cuarentena: filas por causa (explode) ==")
display(  # noqa: F821
    cuar_df.select(F.explode(F.col(col_causas)).alias("regla"))
    .groupBy("regla")
    .agg(F.count("*").alias("filas_con_causa"))
    .orderBy(F.desc("filas_con_causa"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Enriquecer la bandera `en_agua` (ADR-010) — paso SEPARADO y GUARDADO
# MAGIC
# MAGIC La bandera `en_agua` es NULIFICAR-ATRIBUTO (informativa): NO entra en `causas`
# MAGIC ni en la invariante. Se calcula con `global_land_mask` (pandas_udf vectorizado)
# MAGIC en un paso aparte para que un fallo de la máscara sobre 25.9M filas no corrompa
# MAGIC la plata/cuarentena ya materializadas. Si falla, `en_agua` queda en `null` y se
# MAGIC documenta como PENDIENTE DE EJECUCION.

# COMMAND ----------

res_en_agua = materializar.enriquecer_en_agua(spark, cfg)  # noqa: F821
print("Enriquecimiento en_agua:", res_en_agua)

# El conteo de plata no cambia al enriquecer (solo se recalcula una columna).
n_plata_post = spark.table(materializar.tabla_silver_fqn(cfg)).count()  # noqa: F821
print("plata (post-enriquecimiento) =", n_plata_post)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Resultado para el orquestador (`dbutils.notebook.exit`)

# COMMAND ----------

import json

salida = {
    "bronce": int(n_bronce),
    "plata": int(n_plata),
    "cuarentena": int(n_cuarentena),
    "duplicados": int(n_duplicados),
    "invariante_cuadra": bool(inv["cuadra"]),
    "invariante_descripcion": inv["descripcion"],
    "en_agua": res_en_agua,
}
dbutils.notebook.exit(json.dumps(salida))  # noqa: F821
