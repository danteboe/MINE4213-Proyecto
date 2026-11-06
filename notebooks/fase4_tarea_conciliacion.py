# Databricks notebook source
# MAGIC %md
# MAGIC # Fase 4 — Tarea `conciliacion` del Job (Req. 3, Orquestación)
# MAGIC
# MAGIC Tercera tarea del Job y **COMPUERTA Write-Audit-Publish (WAP)**: lee los conteos
# MAGIC reales de bronce/plata/cuarentena y los duplicados por clave, verifica el
# MAGIC invariante de conciliación (ADR-006/ADR-011)
# MAGIC `bronce = plata + cuarentena + duplicados` y, si NO cuadra, hace `raise` para
# MAGIC FALLAR el Job antes de publicar la capa oro. No reintenta (ADR-014): que el
# MAGIC invariante no cuadre NO es un fallo transitorio.
# MAGIC
# MAGIC Esta tarea SOLO LEE; no materializa tablas. Cómputo: **serverless**.

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

import oceanwatch.orquestacion as orq
from oceanwatch.common.config import fqn, load_config
from oceanwatch.ingest.dedup import contar_duplicados
from oceanwatch.quality.conciliacion import verificar_invariante

print("Paquete oceanwatch importado. sys.path[0] =", sys.path[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parámetros del Job (widgets) y configuración

# COMMAND ----------

dbutils.widgets.text("fecha_proceso", "2023-06-03")  # noqa: F821
dbutils.widgets.text("job_run_id", "0")              # noqa: F821
dbutils.widgets.text("job_id", "0")                  # noqa: F821
dbutils.widgets.text("task_key", "conciliacion")     # noqa: F821

fecha_proceso = orq.validar_fecha_proceso(dbutils.widgets.get("fecha_proceso"))  # noqa: F821
job_run_id = int(dbutils.widgets.get("job_run_id"))  # noqa: F821
job_id = int(dbutils.widgets.get("job_id"))          # noqa: F821
task_key = dbutils.widgets.get("task_key") or "conciliacion"  # noqa: F821

_CONFIG_CANDIDATOS = [
    "/Workspace/Users/jd.torresa1@uniandes.edu.co/oceanwatch/conf/config.yaml",
    os.path.abspath(os.path.join(_NB_DIR, "..", "conf", "config.yaml")),
]
_cfg_path = next((p for p in _CONFIG_CANDIDATOS if os.path.isfile(p)), None)
cfg = load_config(_cfg_path)

from datetime import datetime, timezone

_ts_inicio = datetime.now(timezone.utc).isoformat()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Conteos reales y duplicados

# COMMAND ----------

bronze_df = spark.table(fqn(cfg, "bronze", cfg["ingesta"]["tabla_bronze"]))  # noqa: F821
n_bronce = bronze_df.count()
n_plata = spark.table(fqn(cfg, "silver", cfg["silver"]["tabla_clean"])).count()          # noqa: F821
n_cuar = spark.table(fqn(cfg, "quarantine", cfg["quarantine"]["tabla_rejected"])).count()  # noqa: F821
n_dup = contar_duplicados(bronze_df, cfg)["duplicados"]

print("bronce =", n_bronce, "| plata =", n_plata, "| cuarentena =", n_cuar, "| duplicados =", n_dup)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verificar el invariante (compuerta WAP)

# COMMAND ----------

inv = verificar_invariante(n_bronce, n_plata, n_cuar, n_dup)
print(inv["descripcion"])
print("¿Cuadra?:", inv["cuadra"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Auditoría de la tarea (antes del raise: se registra el resultado SIEMPRE)

# COMMAND ----------

tabla_aud = orq.tabla_auditoria_fqn(cfg)
filas_clave = {
    "bronce": int(n_bronce), "plata": int(n_plata),
    "cuarentena": int(n_cuar), "duplicados": int(n_dup),
}
fila = orq.fila_auditoria(
    job_run_id=job_run_id, job_id=job_id, task_key=task_key, fecha_proceso=fecha_proceso,
    estado="SUCCESS" if inv["cuadra"] else "FAILED", ts_inicio=_ts_inicio,
    ts_fin=datetime.now(timezone.utc).isoformat(),
    filas_clave=filas_clave, detalle=inv["descripcion"],
)
spark.sql(orq.merge_auditoria_sql(tabla_aud, fila))  # noqa: F821
print("Auditoría escrita en", tabla_aud)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Compuerta: FALLAR el Job si el invariante NO cuadra

# COMMAND ----------

if not inv["cuadra"]:
    raise RuntimeError(
        "COMPUERTA WAP: el invariante de conciliación NO cuadra -> se detiene el Job "
        f"antes de publicar oro. {inv['descripcion']}"
    )

# COMMAND ----------

import json

dbutils.notebook.exit(json.dumps({**filas_clave, "invariante_cuadra": bool(inv["cuadra"])}))  # noqa: F821
