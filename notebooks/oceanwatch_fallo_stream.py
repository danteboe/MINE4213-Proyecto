# Databricks notebook source
"""Ejecuta el commit fallido del sandbox en un contexto de notebook separado."""

from pathlib import Path
import re
import sys

sys.path.insert(0, str(Path.cwd().parent / "src"))

from oceanwatch.common.config import load_cluster_config
from oceanwatch.ingest.autoloader import ejecutar_ingesta

dbutils.widgets.text("prueba_id", "")
prueba_id = dbutils.widgets.get("prueba_id")
if not re.fullmatch(r"[0-9a-f]{12}", prueba_id):
    raise ValueError("Identificador de prueba inválido")

cfg = load_cluster_config()
cfg["ruta_volumen"] += f"/pruebas/{prueba_id}"
cfg["ingesta"]["tabla_bronze"] = f"prueba_{prueba_id}"
spark.conf.set("spark.sql.session.timeZone", "UTC")
try:
    ejecutar_ingesta(spark, cfg)
except Exception as error:
    if "fallo_inyectado" in str(error).lower():
        dbutils.fs.put(f"{cfg['ruta_volumen']}/fallo_delta_confirmado", "CHECK fallo_inyectado")
    raise
