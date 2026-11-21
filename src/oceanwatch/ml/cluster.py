"""Entrenamiento reproducible en MLflow y puntuación por lotes Arrow en Spark."""

from __future__ import annotations

import json
from math import fsum, sqrt, tau
from pathlib import Path

import mlflow
import mlflow.sklearn
import pandas as pd
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException
from mlflow.models import infer_signature
from pyspark.sql import Window
from pyspark.sql import functions as F
from sklearn.ensemble import IsolationForest

from oceanwatch.common.config import fqn
from oceanwatch.orquestacion.cluster import dia_col, reemplazar_dia
from oceanwatch.quality.materializar import tabla_silver_fqn


def _estadisticas(train):
    """Tabla pequeña por tipo y fallback global, calculados solo con el train."""
    resumen = train.groupBy("VesselType").agg(
        F.count("*").alias("n"),
        F.avg("SOG").alias("media"),
        F.stddev_pop("SOG").alias("std"),
    ).limit(1025).collect()
    if len(resumen) > 1024:
        raise ValueError("El dominio de VesselType excede 1024 categorías")
    total = sum(r.n for r in resumen)
    if not total:
        raise ValueError("El train está vacío")
    # La varianza total combina la dispersión dentro de cada tipo y entre medias.
    # Así se deriva el fallback con el mismo agregado distribuido por tipo.
    media = fsum(r.n * r.media for r in resumen) / total
    varianza = fsum(r.n * (r.std**2 + (r.media - media)**2) for r in resumen) / total
    if not varianza:
        raise ValueError("El train necesita velocidades válidas y variación")
    tipos = [
        [r.VesselType, r.media, r.std]
        for r in resumen
        if r.VesselType is not None and r.n >= 100 and r.std
    ]
    return {"global": [media, sqrt(varianza)], "tipos": sorted(tipos)}, total


def _variables(df, estadisticas):
    spark = df.sparkSession
    tipos = spark.createDataFrame(estadisticas["tipos"], "VesselType INT, media DOUBLE, std DOUBLE")
    global_media, global_std = estadisticas["global"]
    base = df.select("MMSI", "BaseDateTime", "LAT", "LON", "SOG", "VesselType", "_batch_date")
    ventana = Window.partitionBy("_batch_date", "MMSI").orderBy("BaseDateTime")
    anterior = F.lag("SOG").over(ventana)
    tiempo = F.lag("BaseDateTime").over(ventana)
    brecha = F.col("BaseDateTime").cast("long") - tiempo.cast("long")
    hora = F.hour("BaseDateTime") * F.lit(tau / 24)
    return (
        base.where(F.col("SOG").isNotNull())
        .withColumn("delta_sog", F.coalesce(F.when(brecha <= 900, F.col("SOG") - anterior), F.lit(0.0)))
        .join(F.broadcast(tipos), "VesselType", "left")
        .withColumn(
            "z_sog",
            (F.col("SOG") - F.coalesce(F.col("media"), F.lit(global_media)))
            / F.coalesce(F.col("std"), F.lit(global_std)),
        )
        .withColumn("hora_sin", F.sin(hora))
        .withColumn("hora_cos", F.cos(hora))
        .drop("media", "std")
    )


def _muestra(df, n, limite, features, semilla):
    """Muestreo por hash de clave, independiente del orden físico de la tabla."""
    buckets = 1_000_000
    fraccion = min(1, 0.85 * limite / max(1, n))
    clave = F.pmod(F.xxhash64("MMSI", "BaseDateTime", F.lit(semilla)), F.lit(buckets))
    columnas = ["MMSI", "BaseDateTime", *features]
    while True:
        datos = df.where(clave < max(1, int(fraccion * buckets))).select(
            *columnas
        ).limit(limite + 1).toPandas()
        if len(datos) <= limite:
            break
        if fraccion * buckets <= 1:
            raise ValueError("No se pudo acotar la muestra por hash")
        fraccion /= 2
    if datos.empty:
        raise ValueError("Muestra de ML vacía")
    return datos.sort_values(["MMSI", "BaseDateTime"])[features].reset_index(drop=True)


def _campeon(client, nombre, alias):
    try:
        return client.get_model_version_by_alias(nombre, alias)
    except MlflowException as exc:
        if exc.error_code != "RESOURCE_DOES_NOT_EXIST":
            raise
        return None


def _entrenar(spark, cfg, client, nombre, alias, job_run_id):
    vigente = _campeon(client, nombre, alias)
    if vigente and vigente.tags.get("oceanwatch_job_run_id") == str(job_run_id):
        return vigente

    ml = cfg["ml"]
    params = cfg["ml_cluster"]
    tabla = tabla_silver_fqn(cfg)
    version_delta = spark.sql(f"DESCRIBE HISTORY {tabla} LIMIT 1").first().version
    silver = spark.read.format("delta").option("versionAsOf", version_delta).table(tabla)
    base = silver.where(F.col("SOG").isNotNull())
    train = base.where(F.col("_batch_date").isin(params["train_dates"]))
    validacion = base.where(F.col("_batch_date") == F.lit(params["validation_date"]).cast("date"))
    dias_con_sog = {str(r._batch_date) for r in train.select("_batch_date").distinct().collect()}
    if dias_con_sog != set(params["train_dates"]):
        raise ValueError(f"Faltan días de entrenamiento con SOG válida: {sorted(dias_con_sog)}")

    estadisticas, n_train = _estadisticas(train)
    features = ml["features_orden"]
    x_train = _muestra(
        _variables(train, estadisticas), n_train, params["max_filas_train"], features, ml["semilla"]
    )
    x_val = _muestra(
        _variables(validacion, estadisticas), validacion.count(),
        params["max_filas_validacion"], features, ml["semilla"],
    )
    modelo = IsolationForest(**ml["isolation_forest"], n_jobs=1).fit(x_train)
    scores = modelo.decision_function(x_val)
    mlflow.set_experiment(params["experimento"])
    with mlflow.start_run(run_name=f"oceanwatch-{job_run_id}") as run:
        mlflow.log_params({
            **ml["isolation_forest"],
            "tabla_silver": tabla, "version_delta": version_delta,
            "train_dates": ",".join(params["train_dates"]),
            "validation_date": params["validation_date"], "seed_muestreo": ml["semilla"],
            "gap_max_segundos": 900, "min_mensajes_por_tipo": 100,
            "regla_agua": cfg["calidad"]["reglas"]["coordenada_en_agua"]["version"],
        })
        mlflow.log_metrics({
            "filas_train": len(x_train), "filas_validacion": len(x_val),
            "fraccion_anomala_validacion": float((scores < 0).mean()),
            "fraccion_base_z_validacion": float((x_val["z_sog"].abs() > ml["umbral_zscore_base"]).mean()),
            "score_medio_validacion": float(scores.mean()),
        })
        mlflow.log_dict({
            "estadisticas": estadisticas, "features": features,
            "train_max": max(params["train_dates"]),
        }, "features.json")
        info = mlflow.sklearn.log_model(
            modelo, name="isolation_forest", pyfunc_predict_fn="decision_function",
            signature=infer_signature(x_val.iloc[:5], scores[:5]), input_example=x_val.iloc[:5],
        )
        mlflow.set_tag("oceanwatch.job_run_id", str(job_run_id))
        version = mlflow.register_model(info.model_uri, nombre, await_registration_for=300)
        if version.status != "READY":
            raise RuntimeError(f"La versión {version.version} de la corrida {run.info.run_id} no está lista")
    client.set_model_version_tag(nombre, version.version, "oceanwatch_job_run_id", str(job_run_id))
    client.set_registered_model_alias(nombre, alias, version.version)
    return client.get_model_version(nombre, version.version)


def ejecutar_ml(spark, cfg, fechas, entrenar, job_run_id):
    mlflow.set_tracking_uri("databricks")
    mlflow.set_registry_uri("databricks-uc")
    ml = cfg["ml"]
    cliente = MlflowClient()
    nombre = fqn(cfg, "ml", ml["modelo_uc"].rsplit(".", 1)[1])
    alias = ml["alias_campeon"]
    version = (
        _entrenar(spark, cfg, cliente, nombre, alias, job_run_id)
        if entrenar == "true" else _campeon(cliente, nombre, alias)
    )
    if version is None:
        raise ValueError("No hay campeón en Unity Catalog. Ejecute el Job con entrenar=true.")
    ruta = mlflow.artifacts.download_artifacts(run_id=version.run_id, artifact_path="features.json")
    definicion = json.loads(Path(ruta).read_text(encoding="utf-8"))
    modelo = mlflow.sklearn.load_model(f"models:/{nombre}/{version.version}")
    features = definicion["features"]
    umbral = ml["umbral_zscore_base"]
    salida = ["MMSI", "BaseDateTime", "LAT", "LON", "SOG", "VesselType", "z_sog", "_batch_date"]

    def solo_anomalias(lotes):
        for lote in lotes:
            score = modelo.decision_function(lote[features])
            mascara = score < 0
            sospechas = lote.loc[mascara, salida].copy()
            sospechas["score_anomalia"] = score[mascara]
            sospechas["es_anomalia"] = True
            sospechas["razon"] = "Patrón multivariado de velocidad y hora"
            sospechas.loc[sospechas["z_sog"].abs() > umbral, "razon"] = "Velocidad atípica para el tipo"
            yield sospechas

    esquema = (
        "MMSI BIGINT, BaseDateTime TIMESTAMP, LAT DOUBLE, LON DOUBLE, SOG DOUBLE, "
        "VesselType INT, z_sog DOUBLE, _batch_date DATE, score_anomalia DOUBLE, "
        "es_anomalia BOOLEAN, razon STRING"
    )
    tabla = fqn(cfg, "gold", ml["tabla_anomalias"]["objeto"])
    conteos = {}
    for fecha in fechas:
        if fecha <= definicion["train_max"]:
            continue
        silver = dia_col(spark, tabla_silver_fqn(cfg), fecha)
        variables = _variables(silver, definicion["estadisticas"])
        anomalias = variables.mapInPandas(solo_anomalias, schema=esquema).withColumns({
            "modelo_version": F.lit(str(version.version)),
            "modelo_run_id": F.lit(version.run_id),
        })
        anomalias.explain("formatted")
        reemplazar_dia(
            anomalias, tabla, fecha,
            "Posiciones con decision_function < 0 del IsolationForest de Unity Catalog. UTC.",
        )
        filas = spark.sql(f"DESCRIBE HISTORY {tabla} LIMIT 1").first().operationMetrics
        conteos[fecha] = int(filas["numOutputRows"]) if "numOutputRows" in filas else None
    return {
        "version": str(version.version), "run_mlflow": version.run_id,
        "filas_publicadas": conteos, "omitidas_por_fecha_train": sorted(set(fechas) - set(conteos)),
    }
