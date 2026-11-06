# Databricks notebook source
# MAGIC %md
# MAGIC # Fase 6 — Entrenamiento + experimento MLflow + registro en UC (Req. 6, ML)
# MAGIC
# MAGIC Notebook de **entrenamiento** del detector de anomalías de velocidad SOG
# MAGIC (ADR-005 / ADR-016). Demuestra la integración **MLOps** pedida por la rúbrica
# MAGIC (features del Lakehouse -> experimento MLflow -> modelo en Unity Catalog), NO la
# MAGIC maximización de precisión: el modelo es deliberadamente SIMPLE.
# MAGIC
# MAGIC El experimento compara dos enfoques y elige campeón:
# MAGIC 1. **Línea base** `baseline_zscore`: regla interpretable `|z_sog| > 3` por tipo de
# MAGIC    buque (sin entrenamiento), `ml.features.clasificar_por_umbral_z`.
# MAGIC 2. **IsolationForest** de scikit-learn (score continuo, `contamination` explícito,
# MAGIC    reproducible con `random_state=42`) -> **campeón** registrado en UC con alias
# MAGIC    `@champion`.
# MAGIC
# MAGIC Antifuga (D3): las estadísticas por tipo (media/std) y el IsolationForest se
# MAGIC ajustan SOLO con el train (2023-06-01/02); el test (2023-06-03) aplica lo ya
# MAGIC ajustado. Partición **temporal** (no aleatoria): es el escenario real de
# MAGIC inferencia batch del día siguiente y evita filtrar futuro->pasado del mismo buque.
# MAGIC
# MAGIC Fricción serverless (D4): se entrena sobre una **muestra reproducible** del train
# MAGIC traída al driver con `toPandas()` (evita el INTERNAL_ERROR de escritura masiva en
# MAGIC Spark Connect). El vector de features replica con Spark nativo la lógica PURA de
# MAGIC `src/oceanwatch/ml/features.py` (patrón de `gold/trayectorias.py`).
# MAGIC
# MAGIC Cómputo: **serverless** (one-time run `notebook_task` sin `new_cluster`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Instalación pinneada de librerías ML (reproducibilidad, D7)
# MAGIC
# MAGIC `%pip` pinneado de mlflow/scikit-learn con las versiones EXACTAS que resolvió el
# MAGIC runtime serverless en la 1a corrida real (FEAT-002, run 347173419720341,
# MAGIC 2026-11-05): `mlflow==3.18.0`, `scikit-learn==1.3.0`. `numpy==1.26.4` y
# MAGIC `pandas==1.5.3` vienen preinstalados en el runtime serverless (Python 3.11.10);
# MAGIC sus versiones se reportan y se registran en `conf/config.yaml` `ml.pip_pins`.

# COMMAND ----------

# MAGIC %pip install -q mlflow==3.18.0 scikit-learn==1.3.0
# MAGIC # numpy==1.26.4 y pandas==1.5.3 vienen en el runtime serverless; se reportan sus versiones.

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
from oceanwatch.common.config import fqn, load_config

print("Paquete oceanwatch importado. sys.path[0] =", sys.path[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuración y parámetros

# COMMAND ----------

_CONFIG_CANDIDATOS = [
    "/Workspace/Users/jd.torresa1@uniandes.edu.co/oceanwatch/conf/config.yaml",
    os.path.abspath(os.path.join(_NB_DIR, "..", "conf", "config.yaml")),
]
_cfg_path = next((p for p in _CONFIG_CANDIDATOS if os.path.isfile(p)), None)
cfg = load_config(_cfg_path)

ml = cfg["ml"]
TABLA_SILVER = fqn(cfg, "silver", cfg["silver"]["tabla_clean"])
BANDERA_SOG = cfg["silver"]["banderas"]["sog"]
TRAIN_DATES = ml["train_batch_dates"]
TEST_DATE = ml["test_batch_date"]
SEMILLA = int(ml["semilla"])
MUESTRA_FILAS = int(ml["muestra_entrenamiento_filas"])
UMBRAL_Z = float(ml["umbral_zscore_base"])
IF_PARAMS = ml["isolation_forest"]
FEATURES_ORDEN = ml["features_orden"]
EXPERIMENTO = ml["experimento"]
MODELO_UC = ml["modelo_uc"]
ALIAS_CAMPEON = ml["alias_campeon"]

print("silver =", TABLA_SILVER)
print("train =", TRAIN_DATES, "| test =", TEST_DATE)
print("features_orden =", FEATURES_ORDEN)
print("experimento =", EXPERIMENTO)
print("modelo_uc =", MODELO_UC, "| alias =", ALIAS_CAMPEON)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Versiones EFECTIVAS del runtime serverless (reproducibilidad D7)

# COMMAND ----------

import importlib

import mlflow
import numpy as np
import pandas as pd
import sklearn

VERSIONES = {
    "mlflow": mlflow.__version__,
    "scikit-learn": sklearn.__version__,
    "numpy": np.__version__,
    "pandas": pd.__version__,
    "python": sys.version.split()[0],
}
print("VERSIONES EFECTIVAS:", VERSIONES)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Construcción del vector de features en Spark nativo (replica features.py)
# MAGIC
# MAGIC Replica con expresiones Spark la lógica PURA de `src/oceanwatch/ml/features.py`:
# MAGIC - `delta_sog`: `F.lag('SOG')` sobre `Window.partitionBy('MMSI').orderBy('BaseDateTime')`;
# MAGIC   la primera posición del buque -> 0.0.
# MAGIC - `hora`: `F.hour('BaseDateTime')` (serverless UTC, ADR-004) -> `hora_sin`/`hora_cos`
# MAGIC   con ángulo `2*pi*hora/24`.
# MAGIC - `z_sog`: `(SOG - media_tipo) / max(std_tipo, 1e-6)` con los stats SOLO del train.

# COMMAND ----------

import math

from pyspark.sql import Window
from pyspark.sql import functions as F

# Base común: posiciones con SOG real (excluye centinela sog_no_disponible y SOG nulo).
base = (
    spark.table(TABLA_SILVER)  # noqa: F821
    .filter((F.col("SOG").isNotNull()) & (~F.col(BANDERA_SOG)))
    .filter(F.col("_batch_date").isin(TRAIN_DATES + [TEST_DATE]))
)

_w = Window.partitionBy("MMSI").orderBy("BaseDateTime")
_ang = (2.0 * math.pi) * (F.hour("BaseDateTime").cast("double") / F.lit(24.0))

base = (
    base.withColumn("_sog_previo", F.lag("SOG").over(_w))
    .withColumn("delta_sog", F.when(F.col("_sog_previo").isNull(), F.lit(0.0)).otherwise(F.col("SOG") - F.col("_sog_previo")))
    .withColumn("hora", F.hour("BaseDateTime"))
    .withColumn("hora_sin", F.sin(_ang))
    .withColumn("hora_cos", F.cos(_ang))
)

df_train = base.filter(F.col("_batch_date").isin(TRAIN_DATES))
df_test = base.filter(F.col("_batch_date") == F.lit(TEST_DATE))

print("particion temporal construida (train/test por _batch_date).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Estadísticas por tipo SOLO con el train (antifuga D3) y z_sog

# COMMAND ----------

# Reutiliza la función PURA del paquete (import perezoso de PySpark dentro de ella).
stats = feats.estadisticas_por_tipo(df_train, cfg)
print("tipos con estadisticas (train):", len(stats))

# Mapa {tipo: (media, std)} como expresiones Spark para el z_sog (broadcast trivial).
# Replica z_sog_por_tipo: tipos no vistos -> 0.0; std con cota inferior 1e-6.
_media_map = F.create_map(*sum(([F.lit(int(t)), F.lit(float(m))] for t, (m, s) in stats.items()), []))
_std_map = F.create_map(*sum(([F.lit(int(t)), F.lit(float(s))] for t, (m, s) in stats.items()), []))


def _con_z(df):
    media = _media_map[F.col("VesselType")]
    std = _std_map[F.col("VesselType")]
    z = F.when(media.isNull(), F.lit(0.0)).otherwise(
        (F.col("SOG") - media) / F.greatest(std, F.lit(1e-6))
    )
    return df.withColumn("z_sog", z)


df_train = _con_z(df_train)
df_test = _con_z(df_test)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Muestra reproducible del train al driver (toPandas, D4)

# COMMAND ----------

_cols = ["z_sog", "delta_sog", "hora_sin", "hora_cos"]
assert _cols == FEATURES_ORDEN, f"orden de features roto: {_cols} != {FEATURES_ORDEN}"

n_train_total = df_train.count()
n_test_total = df_test.count()
fraccion = min(1.0, float(MUESTRA_FILAS) / max(1, n_train_total))

muestra_train_sdf = df_train.select(*_cols).sample(withReplacement=False, fraction=fraccion, seed=SEMILLA)
X_train = muestra_train_sdf.toPandas()

# El test se usa para métricas; también se acota con una muestra reproducible por escala.
fraccion_test = min(1.0, float(MUESTRA_FILAS) / max(1, n_test_total))
X_test = df_test.select(*_cols).sample(withReplacement=False, fraction=fraccion_test, seed=SEMILLA).toPandas()

print(f"n_train_total={n_train_total} n_test_total={n_test_total}")
print(f"muestra X_train={len(X_train)} X_test={len(X_test)} (fraccion train={fraccion:.6f})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Experimento MLflow — RUN línea base `baseline_zscore`

# COMMAND ----------

import json

mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(EXPERIMENTO)


def _pct_flagged_baseline(df_pandas):
    z = df_pandas["z_sog"].astype(float)
    return float((z.abs() > UMBRAL_Z).mean())


run_id_baseline = None
with mlflow.start_run(run_name="baseline_zscore") as run:
    run_id_baseline = run.info.run_id
    pct_train = _pct_flagged_baseline(X_train)
    pct_test = _pct_flagged_baseline(X_test)
    mlflow.log_param("umbral_zscore", UMBRAL_Z)
    mlflow.log_param("features_orden", FEATURES_ORDEN)
    mlflow.log_param("train_dates", TRAIN_DATES)
    mlflow.log_param("test_date", TEST_DATE)
    mlflow.log_param("semilla", SEMILLA)
    mlflow.log_metric("pct_flagged_train", pct_train)
    mlflow.log_metric("pct_flagged_test", pct_test)
    mlflow.log_metric("n_train", float(len(X_train)))
    mlflow.log_metric("n_test", float(len(X_test)))
    mlflow.set_tag("modelo", "baseline_zscore")
    mlflow.set_tag("caso", "anomalias_sog")
    mlflow.set_tag("req", "6")
    mlflow.set_tag("antifuga", "stats_solo_train")
    mlflow.set_tag("particion", "temporal")

print("baseline run_id =", run_id_baseline, "| pct_train =", pct_train, "| pct_test =", pct_test)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Experimento MLflow — RUN `isolation_forest` (campeón) + log_model

# COMMAND ----------

from mlflow.models import infer_signature
from sklearn.ensemble import IsolationForest

run_id_iforest = None
with mlflow.start_run(run_name="isolation_forest") as run:
    run_id_iforest = run.info.run_id

    modelo = IsolationForest(
        n_estimators=int(IF_PARAMS["n_estimators"]),
        contamination=float(IF_PARAMS["contamination"]),
        random_state=int(IF_PARAMS["random_state"]),
    )
    modelo.fit(X_train[FEATURES_ORDEN])

    # predict: -1 anomalía, 1 normal. score_samples: mayor = más normal.
    pred_train = modelo.predict(X_train[FEATURES_ORDEN])
    pred_test = modelo.predict(X_test[FEATURES_ORDEN])
    score_test = modelo.score_samples(X_test[FEATURES_ORDEN])

    pct_train_if = float((pred_train == -1).mean())
    pct_test_if = float((pred_test == -1).mean())

    # separacion_scores: diferencia de medias de score_samples entre normales y anómalas.
    import numpy as _np

    mask_anom = pred_test == -1
    sep = float(_np.mean(score_test[~mask_anom]) - _np.mean(score_test[mask_anom])) if mask_anom.any() and (~mask_anom).any() else 0.0

    # acuerdo_con_baseline: fracción de coincidencia con la regla |z|>3 (proxy de recall).
    base_flag = (X_test["z_sog"].astype(float).abs() > UMBRAL_Z).to_numpy()
    if_flag = mask_anom
    acuerdo = float((base_flag == if_flag).mean())

    mlflow.log_param("n_estimators", int(IF_PARAMS["n_estimators"]))
    mlflow.log_param("contamination", float(IF_PARAMS["contamination"]))
    mlflow.log_param("random_state", int(IF_PARAMS["random_state"]))
    mlflow.log_param("features_orden", FEATURES_ORDEN)
    mlflow.log_param("train_dates", TRAIN_DATES)
    mlflow.log_param("test_date", TEST_DATE)
    mlflow.log_metric("pct_flagged_train", pct_train_if)
    mlflow.log_metric("pct_flagged_test", pct_test_if)
    mlflow.log_metric("separacion_scores", sep)
    mlflow.log_metric("acuerdo_con_baseline", acuerdo)
    mlflow.log_metric("n_train", float(len(X_train)))
    mlflow.log_metric("n_test", float(len(X_test)))
    mlflow.set_tag("modelo", "isolation_forest")
    mlflow.set_tag("caso", "anomalias_sog")
    mlflow.set_tag("req", "6")
    mlflow.set_tag("antifuga", "stats_solo_train")
    mlflow.set_tag("particion", "temporal")
    mlflow.set_tag("campeon", "true")

    signature = infer_signature(X_test[FEATURES_ORDEN], pred_test)
    input_example = X_test[FEATURES_ORDEN].head()
    mlflow.sklearn.log_model(
        sk_model=modelo,
        name="modelo",
        signature=signature,
        input_example=input_example,
    )

print("iforest run_id =", run_id_iforest)
print("pct_train_if =", pct_train_if, "| pct_test_if =", pct_test_if)
print("separacion_scores =", sep, "| acuerdo_con_baseline =", acuerdo)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Registro del campeón en Models in Unity Catalog (D5, alias @champion)

# COMMAND ----------

from mlflow.tracking import MlflowClient

model_uri = f"runs:/{run_id_iforest}/modelo"
version = mlflow.register_model(model_uri=model_uri, name=MODELO_UC)
print("registrado:", MODELO_UC, "version", version.version)

client = MlflowClient()
client.set_registered_model_alias(name=MODELO_UC, alias=ALIAS_CAMPEON, version=version.version)

_descripcion = (
    "Detector de anomalias de velocidad SOG relativa al tipo de buque (ADR-005/ADR-016). "
    f"IsolationForest(n_estimators={IF_PARAMS['n_estimators']}, "
    f"contamination={IF_PARAMS['contamination']}, random_state={IF_PARAMS['random_state']}). "
    f"Entrenado con _batch_date {TRAIN_DATES} (antifuga: stats por tipo y modelo SOLO del train); "
    f"evaluado con {TEST_DATE}. Vector de features (orden fijo) = {FEATURES_ORDEN}. "
    f"Run MLflow: {run_id_iforest}. Experimento: {EXPERIMENTO}. Semilla {SEMILLA}."
)
client.update_registered_model(name=MODELO_UC, description=_descripcion)
client.update_model_version(name=MODELO_UC, version=version.version, description=_descripcion)

# Verificación del alias (salida literal para la evidencia).
mv_alias = client.get_model_version_by_alias(MODELO_UC, ALIAS_CAMPEON)
print("alias champion -> version", mv_alias.version)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Salida del notebook (JSON con run_ids, versiones y métricas reales)

# COMMAND ----------

_salida = {
    "run_id_baseline": run_id_baseline,
    "run_id_iforest": run_id_iforest,
    "experimento": EXPERIMENTO,
    "modelo_uc": MODELO_UC,
    "version_campeon": str(version.version),
    "alias_campeon": ALIAS_CAMPEON,
    "alias_apunta_a_version": str(mv_alias.version),
    "versiones_librerias": VERSIONES,
    "metricas": {
        "baseline": {"pct_flagged_train": pct_train, "pct_flagged_test": pct_test},
        "isolation_forest": {
            "pct_flagged_train": pct_train_if,
            "pct_flagged_test": pct_test_if,
            "separacion_scores": sep,
            "acuerdo_con_baseline": acuerdo,
        },
        "n_train_total": int(n_train_total),
        "n_test_total": int(n_test_total),
        "n_muestra_train": int(len(X_train)),
        "n_muestra_test": int(len(X_test)),
        "tipos_con_stats": len(stats),
    },
    "run_page_url_baseline": mlflow.get_run(run_id_baseline).info.run_name,
}
dbutils.notebook.exit(json.dumps(_salida))  # noqa: F821
