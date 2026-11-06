# Databricks notebook source
# MAGIC %md
# MAGIC # Fase 4 — Tarea `oro` del Job (Req. 3, Orquestación)
# MAGIC
# MAGIC Cuarta y última tarea del Job. Materializa la capa ORO (modelo dimensional,
# MAGIC ADR-012) ejecutando el **SQL CTAS fiable** de
# MAGIC `resources/sql/fase3_materializar_oro.sql` vía `spark.sql`: recrea
# MAGIC `gold.world_port_index` (dimensión de referencia), `gold.trayectorias_diarias`,
# MAGIC `gold.presencia_puertos` y `gold.matriz_trafico`. El oro se alimenta SOLO de
# MAGIC `oceanwatch.silver.ais_clean` (el oro NO limpia).
# MAGIC
# MAGIC > **Fricción serverless real:** se usa `CREATE OR REPLACE TABLE AS SELECT` con
# MAGIC > `spark.sql`, NUNCA `df.write.saveAsTable` (INTERNAL_ERROR sobre 25.9M filas).
# MAGIC
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
dbutils.widgets.text("task_key", "oro")              # noqa: F821

fecha_proceso = orq.validar_fecha_proceso(dbutils.widgets.get("fecha_proceso"))  # noqa: F821
job_run_id = int(dbutils.widgets.get("job_run_id"))  # noqa: F821
job_id = int(dbutils.widgets.get("job_id"))          # noqa: F821
task_key = dbutils.widgets.get("task_key") or "oro"  # noqa: F821

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
# MAGIC ## 2. Utilidades: localizar y ejecutar el `.sql` sentencia por sentencia

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
    """Divide un script SQL en sentencias, ignorando líneas de comentario y vacías."""
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Materializar la capa oro (CTAS fiable vía spark.sql)

# COMMAND ----------

_ejecutar_sql("fase3_materializar_oro.sql")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Conteos reales de las tablas oro

# COMMAND ----------

n_tray = spark.table(fqn(cfg, "gold", cfg["gold"]["tabla_trayectorias"])).count()           # noqa: F821
n_pres = spark.table(fqn(cfg, "gold", cfg["gold"]["tabla_presencia_puertos"])).count()       # noqa: F821
n_matriz = spark.table(fqn(cfg, "gold", cfg["gold"]["tabla_matriz_trafico"])).count()        # noqa: F821
n_wpi = spark.table(fqn(cfg, "gold", cfg["gold"]["tabla_wpi"])).count()                      # noqa: F821
print("trayectorias =", n_tray, "| presencia =", n_pres, "| matriz =", n_matriz, "| wpi =", n_wpi)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Auditoría de la tarea (MERGE idempotente por job_run_id+task_key)

# COMMAND ----------

tabla_aud = orq.tabla_auditoria_fqn(cfg)
filas_clave = {
    "trayectorias_diarias": int(n_tray), "presencia_puertos": int(n_pres),
    "matriz_trafico": int(n_matriz), "world_port_index": int(n_wpi),
}
fila = orq.fila_auditoria(
    job_run_id=job_run_id, job_id=job_id, task_key=task_key, fecha_proceso=fecha_proceso,
    estado="SUCCESS", ts_inicio=_ts_inicio,
    ts_fin=datetime.now(timezone.utc).isoformat(),
    filas_clave=filas_clave, detalle="Materialización de la capa oro (CTAS).",
)
spark.sql(orq.merge_auditoria_sql(tabla_aud, fila))  # noqa: F821
print("Auditoría escrita en", tabla_aud)

# COMMAND ----------

import json

dbutils.notebook.exit(json.dumps(filas_clave))  # noqa: F821
