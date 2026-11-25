# Databricks notebook source
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

raiz = Path.cwd().parent
sys.path.insert(0, str(raiz / "src"))

from oceanwatch.common.config import load_cluster_config
from oceanwatch.gold.diaria import publicar_dia
from oceanwatch.orquestacion.cluster import (
    confirmar_entradas,
    ingerir,
    manifiesto_corrida,
    preparar,
    registrar,
)
from oceanwatch.quality.diaria import conciliar_dia, materializar_dia

for nombre, valor in {
    "fecha_proceso": "",
    "fecha_inicio": "",
    "entrenar": "false",
    "task_key": "preparar",
    "job_run_id": "0",
    "job_id": "0",
    "trigger": "manual",
    "ruta_entrada": "",
}.items():
    dbutils.widgets.text(nombre, valor)

cfg = load_cluster_config()
cfg["ruta_entrada"] = dbutils.widgets.get("ruta_entrada")
fecha = dbutils.widgets.get("fecha_proceso")
desde = dbutils.widgets.get("fecha_inicio")
entrenar = dbutils.widgets.get("entrenar")
tarea = dbutils.widgets.get("task_key")
contexto = {
    "fecha_proceso": fecha or None,
    "task_key": tarea,
    "job_run_id": int(dbutils.widgets.get("job_run_id")),
    "job_id": int(dbutils.widgets.get("job_id")),
    "trigger": dbutils.widgets.get("trigger"),
}

spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "auto")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
inicio = datetime.now(timezone.utc).isoformat()

try:
    if tarea not in {"preparar", "ingesta"}:
        lote = json.loads(manifiesto_corrida(cfg, contexto["job_run_id"]).read_text(encoding="utf-8"))
        fechas = lote["fechas"]
        contexto["fecha_proceso"] = fechas[-1] if fechas else None
    if tarea == "preparar":
        resultado = preparar(spark, dbutils, cfg)
    elif tarea == "ingesta":
        resultado = ingerir(spark, dbutils, cfg, fecha, desde, entrenar, contexto["job_run_id"])
        contexto["fecha_proceso"] = resultado["fechas"][-1] if resultado["fechas"] else None
    elif tarea == "plata_calidad":
        resultado = {dia: materializar_dia(spark, cfg, dia) for dia in fechas}
    elif tarea == "conciliacion":
        resultado = {dia: conciliar_dia(spark, cfg, dia) for dia in fechas}
    elif tarea == "oro":
        resultado = {dia: publicar_dia(spark, cfg, dia) for dia in fechas}
    elif tarea == "ml_anomalias":
        resultado = {"fechas": []}
        if lote["fechas_ml"]:
            from oceanwatch.ml.cluster import ejecutar_ml

            resultado = ejecutar_ml(spark, cfg, lote["fechas_ml"], entrenar, contexto["job_run_id"])
        confirmar_entradas(dbutils, cfg, lote)
    else:
        raise ValueError(f"Tarea desconocida: {tarea}")
except Exception as error:
    try:
        registrar(spark, cfg, contexto, inicio, "FAILED", detalle=str(error))
    except Exception as error_auditoria:
        print("Auditoría no disponible:", error_auditoria)
    raise

registrar(spark, cfg, contexto, inicio, "SUCCESS", resultado)
dbutils.notebook.exit(json.dumps(resultado, ensure_ascii=False, default=str))
