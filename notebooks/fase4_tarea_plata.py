# Databricks notebook source
# MAGIC %md
# MAGIC # Fase 4 — Tarea `plata_calidad` del Job (Req. 3, Orquestación)
# MAGIC
# MAGIC Segunda tarea del Job. Materializa la capa PLATA + cuarentena + métricas de
# MAGIC calidad ejecutando el **SQL CTAS fiable** de `resources/sql/fase3_materializar_plata.sql`
# MAGIC y luego el enriquecimiento de la bandera `en_agua` de
# MAGIC `resources/sql/fase3_enriquecer_en_agua.sql`, ambos vía `spark.sql`.
# MAGIC
# MAGIC > **Fricción serverless real (ADR-013/ADR-014):** la API de escritura Delta de
# MAGIC > Spark (`df.write.saveAsTable`) sobre 25.9M filas devolvió `INTERNAL_ERROR` en
# MAGIC > Spark Connect GC. El camino FIABLE es `CREATE OR REPLACE TABLE AS SELECT`
# MAGIC > ejecutado con `spark.sql`. Por eso esta tarea NO usa `df.write`: lee el `.sql`
# MAGIC > (fuente única de verdad) y ejecuta sentencia por sentencia.
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
dbutils.widgets.text("task_key", "plata_calidad")    # noqa: F821

fecha_proceso = orq.validar_fecha_proceso(dbutils.widgets.get("fecha_proceso"))  # noqa: F821
job_run_id = int(dbutils.widgets.get("job_run_id"))  # noqa: F821
job_id = int(dbutils.widgets.get("job_id"))          # noqa: F821
task_key = dbutils.widgets.get("task_key") or "plata_calidad"  # noqa: F821

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
# MAGIC ## 2. Utilidades: localizar y ejecutar un `.sql` sentencia por sentencia
# MAGIC
# MAGIC El `.sql` del repo se importa al workspace bajo `.../oceanwatch/resources/sql/`.
# MAGIC Es la FUENTE ÚNICA DE VERDAD del CTAS; el notebook solo lo lee y lo corre con
# MAGIC `spark.sql` (nunca `df.write`). El `SELECT` final de verificación se ejecuta
# MAGIC igual (devuelve filas, no escribe).

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
    lineas = []
    for linea in texto_sql.splitlines():
        if linea.strip().startswith("--"):
            continue
        lineas.append(linea)
    cuerpo = "\n".join(lineas)
    return [s.strip() for s in cuerpo.split(";") if s.strip()]


def _ejecutar_sql(nombre: str):
    ruta = _ruta_sql(nombre)
    with open(ruta, encoding="utf-8") as fh:
        texto = fh.read()
    sentencias = _sentencias(texto)
    print(f"[{nombre}] {len(sentencias)} sentencias desde {ruta}")
    ultimo = None
    for i, stmt in enumerate(sentencias, 1):
        print(f"  -> sentencia {i}/{len(sentencias)}: {stmt.splitlines()[0][:80]} ...")
        ultimo = spark.sql(stmt)  # noqa: F821
    return ultimo

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Materializar plata + cuarentena + métricas (CTAS fiable vía spark.sql)

# COMMAND ----------

_ejecutar_sql("fase3_materializar_plata.sql")
_ejecutar_sql("fase3_enriquecer_en_agua.sql")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Conteos reales plata/cuarentena

# COMMAND ----------

n_plata = spark.table(fqn(cfg, "silver", cfg["silver"]["tabla_clean"])).count()         # noqa: F821
n_cuar = spark.table(fqn(cfg, "quarantine", cfg["quarantine"]["tabla_rejected"])).count()  # noqa: F821
print("plata =", n_plata, "| cuarentena =", n_cuar)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Auditoría de la tarea (MERGE idempotente por job_run_id+task_key)

# COMMAND ----------

tabla_aud = orq.tabla_auditoria_fqn(cfg)
filas_clave = {"plata": int(n_plata), "cuarentena": int(n_cuar)}
fila = orq.fila_auditoria(
    job_run_id=job_run_id, job_id=job_id, task_key=task_key, fecha_proceso=fecha_proceso,
    estado="SUCCESS", ts_inicio=_ts_inicio,
    ts_fin=datetime.now(timezone.utc).isoformat(),
    filas_clave=filas_clave,
    detalle="Materialización plata+cuarentena+métricas (CTAS) y enriquecimiento en_agua.",
)
spark.sql(orq.merge_auditoria_sql(tabla_aud, fila))  # noqa: F821
print("Auditoría escrita en", tabla_aud)

# COMMAND ----------

import json

dbutils.notebook.exit(json.dumps(filas_clave))  # noqa: F821
