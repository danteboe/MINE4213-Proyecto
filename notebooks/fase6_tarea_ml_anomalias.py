# Databricks notebook source
# MAGIC %md
# MAGIC # Fase 6 — Tarea `ml_anomalias` del Job (Req. 6, ML en producción)
# MAGIC
# MAGIC Quinta tarea del Job `oceanwatch_pipeline_entrega2` (depende de `oro`). Es la
# MAGIC **inferencia batch** del detector de anomalías de velocidad SOG (ADR-005 /
# MAGIC ADR-016): carga el modelo campeón `@champion` desde Models in Unity Catalog,
# MAGIC puntúa las posiciones de `oceanwatch.silver.ais_clean` de `fecha_proceso` y
# MAGIC puebla `oceanwatch.gold.anomalias_velocidad` con las posiciones anómalas.
# MAGIC
# MAGIC El vector de features replica con Spark nativo la lógica PURA de
# MAGIC `src/oceanwatch/ml/features.py` EN EL MISMO ORDEN que el entrenamiento
# MAGIC (`[z_sog, delta_sog, hora_sin, hora_cos]`, contrato del modelo). Las
# MAGIC estadísticas por tipo se recalculan SOLO con el train (D3, antifuga), igual
# MAGIC que en `fase6_entrenamiento_mlflow.py`.
# MAGIC
# MAGIC > **Fricción serverless real:** la tabla se escribe con `spark.sql`
# MAGIC > (DELETE + INSERT idempotente de la partición de la fecha), NUNCA
# MAGIC > `df.write.saveAsTable` (INTERNAL_ERROR de Spark Connect sobre volúmenes grandes).
# MAGIC
# MAGIC Cómputo: **serverless** (tarea `notebook_task` del Job, sin `new_cluster`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Instalación pinneada de librerías ML (reproducibilidad, D7)
# MAGIC
# MAGIC `mlflow==3.18.0` (igual que el entrenamiento, FEAT-002: necesario para descargar
# MAGIC el modelo registrado en Unity Catalog) y `scikit-learn==1.5.2` para la inferencia.
# MAGIC
# MAGIC > **Fricción serverless REAL (verificada 2026-11-05):** el entrenamiento (FEAT-002)
# MAGIC > se ejecutó con `jobs submit` sobre el entorno serverless **version 2** (Python
# MAGIC > 3.11) con `scikit-learn==1.3.0`. Las TAREAS del Job corren sobre el entorno
# MAGIC > serverless por defecto **version 3 (Python 3.12)**, y `scikit-learn==1.3.x` NO
# MAGIC > publica wheels para Python 3.12 (solo 3.8–3.11) -> `%pip install scikit-learn==1.3.0`
# MAGIC > falla con `PipError`. El entorno de una tarea `notebook_task` serverless NO se
# MAGIC > puede fijar por DAB (Databricks lo rechaza: "A task environment can not be provided
# MAGIC > for notebook task"), así que se instala `scikit-learn==1.5.2` (sí tiene wheel para
# MAGIC > 3.11 y 3.12). Verificado en una corrida aislada (one-time run, 2026-11-05): el
# MAGIC > modelo @champion (pickled con 1.3.0) se carga y predice IDÉNTICO bajo 1.5.2
# MAGIC > (predict=[-1,1], score_samples coherentes); solo emite un
# MAGIC > `InconsistentVersionWarning` de des-serialización que se silencia abajo. La
# MAGIC > estructura del IsolationForest (árboles) es estable entre 1.3 y 1.5, por lo que
# MAGIC > las predicciones no cambian. `numpy`/`pandas` vienen preinstalados en el runtime.

# COMMAND ----------

# MAGIC %pip install -q mlflow==3.18.0 scikit-learn==1.5.2
# MAGIC # numpy/pandas vienen en el runtime serverless; scikit-learn 1.5.2 tiene wheel para 3.11 y 3.12.

# COMMAND ----------

dbutils.library.restartPython()  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Poner `src` en el path e importar el paquete

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

import oceanwatch.ml.features as feats
import oceanwatch.orquestacion as orq
from oceanwatch.common.config import fqn, load_config

print("Paquete oceanwatch importado. sys.path[0] =", sys.path[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Parámetros del Job (widgets) y configuración

# COMMAND ----------

dbutils.widgets.text("fecha_proceso", "2023-06-03")  # noqa: F821
dbutils.widgets.text("job_run_id", "0")              # noqa: F821
dbutils.widgets.text("job_id", "0")                  # noqa: F821
dbutils.widgets.text("task_key", "ml_anomalias")     # noqa: F821

fecha_proceso = orq.validar_fecha_proceso(dbutils.widgets.get("fecha_proceso"))  # noqa: F821
job_run_id = int(dbutils.widgets.get("job_run_id"))  # noqa: F821
job_id = int(dbutils.widgets.get("job_id"))          # noqa: F821
task_key = dbutils.widgets.get("task_key") or "ml_anomalias"  # noqa: F821

_CONFIG_CANDIDATOS = [
    "/Workspace/Users/jd.torresa1@uniandes.edu.co/oceanwatch/conf/config.yaml",
    os.path.abspath(os.path.join(_NB_DIR, "..", "conf", "config.yaml")),
]
_cfg_path = next((p for p in _CONFIG_CANDIDATOS if os.path.isfile(p)), None)
cfg = load_config(_cfg_path)

ml = cfg["ml"]
TABLA_SILVER = fqn(cfg, "silver", cfg["silver"]["tabla_clean"])
TABLA_ANOMALIAS = fqn(cfg, ml["tabla_anomalias"]["esquema"], ml["tabla_anomalias"]["objeto"])
BANDERA_SOG = cfg["silver"]["banderas"]["sog"]
TRAIN_DATES = ml["train_batch_dates"]
UMBRAL_Z = float(ml["umbral_zscore_base"])
FEATURES_ORDEN = ml["features_orden"]
MODELO_UC = ml["modelo_uc"]
ALIAS_CAMPEON = ml["alias_campeon"]

from datetime import datetime, timezone

_ts_inicio = datetime.now(timezone.utc).isoformat()

print("silver =", TABLA_SILVER, "| anomalias =", TABLA_ANOMALIAS)
print("fecha_proceso =", fecha_proceso, "| train (stats) =", TRAIN_DATES)
print("modelo =", MODELO_UC, "@", ALIAS_CAMPEON, "| features =", FEATURES_ORDEN)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Crear la tabla de anomalías si no existe (DDL fiable vía spark.sql)

# COMMAND ----------

def _ruta_sql(nombre: str) -> str:
    candidatos = [
        f"/Workspace/Users/jd.torresa1@uniandes.edu.co/oceanwatch/resources/sql/{nombre}",
        os.path.abspath(os.path.join(_NB_DIR, "..", "resources", "sql", nombre)),
        os.path.abspath(os.path.join(_NB_DIR, "resources", "sql", nombre)),
    ]
    for c in candidatos:
        if os.path.isfile(c):
            return c
    raise FileNotFoundError(f"No se encontró el SQL '{nombre}'. Buscado en: {candidatos}")


def _sentencias(texto_sql: str) -> list[str]:
    lineas = [ln for ln in texto_sql.splitlines() if not ln.strip().startswith("--")]
    cuerpo = "\n".join(lineas)
    return [s.strip() for s in cuerpo.split(";") if s.strip()]


def _ejecutar_sql(nombre: str):
    ruta = _ruta_sql(nombre)
    with open(ruta, encoding="utf-8") as fh:
        texto = fh.read()
    sentencias = _sentencias(texto)
    print(f"[{nombre}] {len(sentencias)} sentencias desde {ruta}")
    for i, stmt in enumerate(sentencias, 1):
        print(f"  -> sentencia {i}/{len(sentencias)}: {stmt.splitlines()[0][:80]} ...")
        spark.sql(stmt)  # noqa: F821


_ejecutar_sql("fase6_anomalias.sql")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Cargar el modelo campeón por alias (@champion) desde Unity Catalog

# COMMAND ----------

import warnings

import mlflow

mlflow.set_registry_uri("databricks-uc")
# El modelo se entrenó con scikit-learn 1.3.0 y la inferencia usa 1.5.2 (ver celda 0);
# des-serializarlo emite InconsistentVersionWarning (predicciones IDÉNTICAS, verificado).
# Se silencia SOLO ese warning esperado para no contaminar los logs de la tarea.
with warnings.catch_warnings():
    try:
        from sklearn.exceptions import InconsistentVersionWarning

        warnings.simplefilter("ignore", InconsistentVersionWarning)
    except Exception:
        warnings.simplefilter("ignore", UserWarning)
    modelo = mlflow.sklearn.load_model(f"models:/{MODELO_UC}@{ALIAS_CAMPEON}")
print("modelo campeón cargado:", f"models:/{MODELO_UC}@{ALIAS_CAMPEON}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Features en Spark nativo (replica features.py, mismo orden que el train)
# MAGIC
# MAGIC - `delta_sog`: `F.lag('SOG')` sobre `Window.partitionBy('MMSI').orderBy('BaseDateTime')`;
# MAGIC   primera posición del buque -> 0.0.
# MAGIC - `hora`: `F.hour('BaseDateTime')` (serverless UTC, ADR-004) -> `hora_sin`/`hora_cos`.
# MAGIC - `z_sog`: `(SOG - media_tipo)/max(std_tipo, 1e-6)` con stats SOLO del train (D3).

# COMMAND ----------

import math

from pyspark.sql import Window
from pyspark.sql import functions as F

# Base de la fecha a inferir: posiciones con SOG real (excluye centinela y SOG nulo).
base_dia = (
    spark.table(TABLA_SILVER)  # noqa: F821
    .filter((F.col("SOG").isNotNull()) & (~F.col(BANDERA_SOG)))
    .filter(F.col("_batch_date") == F.lit(fecha_proceso))
)

# Estadísticas por tipo recalculadas SOLO con el train (antifuga, reproducible).
df_train = (
    spark.table(TABLA_SILVER)  # noqa: F821
    .filter((F.col("SOG").isNotNull()) & (~F.col(BANDERA_SOG)))
    .filter(F.col("_batch_date").isin(TRAIN_DATES))
)
stats = feats.estadisticas_por_tipo(df_train, cfg)
print("tipos con estadisticas (train):", len(stats))

_w = Window.partitionBy("MMSI").orderBy("BaseDateTime")
_ang = (2.0 * math.pi) * (F.hour("BaseDateTime").cast("double") / F.lit(24.0))

# Mapas {tipo:(media,std)} como expresiones Spark (replica z_sog_por_tipo: tipo no
# visto -> 0.0; std con cota inferior 1e-6).
_media_map = F.create_map(*sum(([F.lit(int(t)), F.lit(float(m))] for t, (m, s) in stats.items()), []))
_std_map = F.create_map(*sum(([F.lit(int(t)), F.lit(float(s))] for t, (m, s) in stats.items()), []))

_media = _media_map[F.col("VesselType")]
_std = _std_map[F.col("VesselType")]

df_feat = (
    base_dia
    .withColumn("_sog_previo", F.lag("SOG").over(_w))
    .withColumn(
        "delta_sog",
        F.when(F.col("_sog_previo").isNull(), F.lit(0.0)).otherwise(F.col("SOG") - F.col("_sog_previo")),
    )
    .withColumn("hora_sin", F.sin(_ang))
    .withColumn("hora_cos", F.cos(_ang))
    .withColumn(
        "z_sog",
        F.when(_media.isNull(), F.lit(0.0)).otherwise((F.col("SOG") - _media) / F.greatest(_std, F.lit(1e-6))),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Puntuar con el modelo campeón (pandas_udf, score_samples + predict)
# MAGIC
# MAGIC El IsolationForest se aplica en el driver/ejecutores con un `pandas_udf`
# MAGIC vectorizado: `predict == -1` => anomalía; `score_samples` => score continuo
# MAGIC (mayor = más normal). El vector respeta el orden fijo `FEATURES_ORDEN`.

# COMMAND ----------

from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType, IntegerType

# El modelo se captura por CLOUSURE en los pandas_udf (Spark Connect serverless NO expone
# spark.sparkContext.broadcast); el estimador sklearn se serializa con la UDF a los
# ejecutores. _ORDEN garantiza el orden fijo del contrato de features.
_modelo = modelo
_ORDEN = list(FEATURES_ORDEN)


@pandas_udf(IntegerType())
def _predecir_udf(z_sog, delta_sog, hora_sin, hora_cos):  # noqa: ANN001
    import pandas as pd

    X = pd.concat([z_sog, delta_sog, hora_sin, hora_cos], axis=1)
    X.columns = _ORDEN
    return pd.Series(_modelo.predict(X.values)).astype("int32")


@pandas_udf(DoubleType())
def _score_udf(z_sog, delta_sog, hora_sin, hora_cos):  # noqa: ANN001
    import pandas as pd

    X = pd.concat([z_sog, delta_sog, hora_sin, hora_cos], axis=1)
    X.columns = _ORDEN
    return pd.Series(_modelo.score_samples(X.values)).astype("float64")


df_scored = (
    df_feat
    .withColumn("_pred", _predecir_udf(F.col("z_sog"), F.col("delta_sog"), F.col("hora_sin"), F.col("hora_cos")))
    .withColumn("score_anomalia", _score_udf(F.col("z_sog"), F.col("delta_sog"), F.col("hora_sin"), F.col("hora_cos")))
    .withColumn("es_anomalia", F.col("_pred") == F.lit(-1))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Construir las filas de anomalías (solo es_anomalia) + razón legible

# COMMAND ----------

# Razón legible: distingue si además supera la línea base |z|>3 por tipo.
_razon = F.concat(
    F.lit("z_sog="), F.round(F.col("z_sog"), 2).cast("string"),
    F.when(F.abs(F.col("z_sog")) > F.lit(UMBRAL_Z), F.lit(f" (>{UMBRAL_Z:g} por tipo)")).otherwise(F.lit(" (<=umbral)")),
    F.lit(" tipo "), F.coalesce(F.col("VesselType").cast("string"), F.lit("NA")),
    F.lit(" | score="), F.round(F.col("score_anomalia"), 4).cast("string"),
)

df_anom = (
    df_scored
    .filter(F.col("es_anomalia"))
    .select(
        F.col("MMSI").cast("bigint").alias("MMSI"),
        F.col("BaseDateTime").cast("timestamp").alias("BaseDateTime"),
        F.col("LAT").cast("double").alias("LAT"),
        F.col("LON").cast("double").alias("LON"),
        F.col("SOG").cast("double").alias("SOG"),
        F.col("VesselType").cast("int").alias("VesselType"),
        F.col("z_sog").cast("double").alias("z_sog"),
        F.col("score_anomalia").cast("double").alias("score_anomalia"),
        F.col("es_anomalia").cast("boolean").alias("es_anomalia"),
        _razon.alias("razon"),
        F.to_date(F.lit(fecha_proceso)).alias("_batch_date"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Escritura idempotente por fecha (DELETE + INSERT vía spark.sql)
# MAGIC
# MAGIC Camino FIABLE en serverless: se registra el DataFrame de anomalías como vista
# MAGIC temporal y se escribe la partición de la fecha con `spark.sql` (DELETE de la
# MAGIC fecha + INSERT), NUNCA `df.write.saveAsTable`. Re-ejecutar la tarea para la
# MAGIC misma fecha deja el MISMO resultado (idempotente).

# COMMAND ----------

df_anom.createOrReplaceTempView("_anomalias_dia")

# 1) Borrar lo ya escrito para esta fecha (idempotencia por partición lógica).
spark.sql(  # noqa: F821
    f"DELETE FROM {TABLA_ANOMALIAS} WHERE _batch_date = DATE('{fecha_proceso}')"
)
# 2) Insertar las anomalías de la fecha (columnas en el orden del DDL).
spark.sql(  # noqa: F821
    f"""
    INSERT INTO {TABLA_ANOMALIAS}
      (MMSI, BaseDateTime, LAT, LON, SOG, VesselType, z_sog, score_anomalia, es_anomalia, razon, _batch_date)
    SELECT MMSI, BaseDateTime, LAT, LON, SOG, VesselType, z_sog, score_anomalia, es_anomalia, razon, _batch_date
    FROM _anomalias_dia
    """
)

n_anomalias = spark.table(TABLA_ANOMALIAS).filter(F.col("_batch_date") == F.lit(fecha_proceso)).count()  # noqa: F821
print("anomalias escritas para", fecha_proceso, "=", n_anomalias)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Auditoría de la tarea (MERGE idempotente por job_run_id+task_key)

# COMMAND ----------

tabla_aud = orq.tabla_auditoria_fqn(cfg)
filas_clave = {"anomalias": int(n_anomalias)}
fila = orq.fila_auditoria(
    job_run_id=job_run_id, job_id=job_id, task_key=task_key, fecha_proceso=fecha_proceso,
    estado="SUCCESS", ts_inicio=_ts_inicio,
    ts_fin=datetime.now(timezone.utc).isoformat(),
    filas_clave=filas_clave,
    detalle="Inferencia IsolationForest @champion -> gold.anomalias_velocidad.",
)
spark.sql(orq.merge_auditoria_sql(tabla_aud, fila))  # noqa: F821
print("Auditoría escrita en", tabla_aud)

# COMMAND ----------

import json

dbutils.notebook.exit(json.dumps(filas_clave))  # noqa: F821
