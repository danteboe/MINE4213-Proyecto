# Databricks notebook source
# MAGIC %md
# MAGIC # Fase 3 — Capa ORO (modelo dimensional de negocio, Req. 2)
# MAGIC
# MAGIC Materializa la capa oro (ADR-012) alimentada **SOLO** de
# MAGIC `oceanwatch.silver.ais_clean` (el oro NO limpia). Construye tres tablas con
# MAGIC granularidad y clave EXPLÍCITAS:
# MAGIC
# MAGIC - `oceanwatch.gold.trayectorias_diarias` — 1 fila por `(MMSI, dia)`; distancia
# MAGIC   haversine entre posiciones consecutivas (`gold.trayectorias`).
# MAGIC - `oceanwatch.gold.presencia_puertos` — 1 fila por `(MMSI, puerto, dia)`; cruce
# MAGIC   con `oceanwatch.gold.world_port_index` por celda de grilla + regla 'en puerto'
# MAGIC   y permanencia mínima (`gold.puertos`).
# MAGIC - `oceanwatch.gold.matriz_trafico` — 1 fila por `(celda_lat, celda_lon, hora)`;
# MAGIC   grilla por redondeo lat/lon (ADR-001, NO H3) (`gold.matriz_trafico`).
# MAGIC
# MAGIC La tabla de referencia `oceanwatch.gold.world_port_index` (~3.669 puertos) se
# MAGIC carga aparte desde el WPI convertido a CSV localmente (ADR-012: evitar instalar
# MAGIC geopandas/pyshp en serverless, ver evidence/.../disponibilidad_librerias_serverless.md).
# MAGIC
# MAGIC El código pesado vive en `src/oceanwatch/gold/`; este notebook SOLO orquesta.
# MAGIC Cómputo: **serverless** (una sola corrida vía `jobs submit`). Todas las
# MAGIC transformaciones usan expresiones nativas (sin UDF escalar: no viable a 25.9M
# MAGIC filas, ver findings de FEAT-003).

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

import oceanwatch.gold.matriz_trafico as matriz_trafico
import oceanwatch.gold.puertos as puertos
import oceanwatch.gold.trayectorias as trayectorias
from oceanwatch.common.config import fqn, load_config

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

TABLA_SILVER = fqn(cfg, "silver", cfg["silver"]["tabla_clean"])
TABLA_WPI = fqn(cfg, "gold", cfg["gold"]["tabla_wpi"])

print("Plata (origen):     ", TABLA_SILVER)
print("WPI (referencia):   ", TABLA_WPI)
print("Trayectorias:       ", trayectorias.tabla_trayectorias_fqn(cfg))
print("Presencia puertos:  ", puertos.tabla_presencia_puertos_fqn(cfg))
print("Matriz tráfico:     ", matriz_trafico.tabla_matriz_trafico_fqn(cfg))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Crear el esquema oro si no existe (Unity Catalog)

# COMMAND ----------

from oceanwatch.common.config import catalog_name, schema

_cat = catalog_name(cfg)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_cat}.{schema(cfg, 'gold')}")  # noqa: F821
print("Esquema oro listo:", f"{_cat}.{schema(cfg, 'gold')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Leer plata y WPI de referencia
# MAGIC
# MAGIC La tabla `oceanwatch.gold.world_port_index` se materializa ANTES (carga del WPI
# MAGIC convertido a CSV, ADR-012). Aquí solo se lee como dimensión de referencia.

# COMMAND ----------

df_silver = spark.table(TABLA_SILVER)  # noqa: F821
df_wpi = spark.table(TABLA_WPI)  # noqa: F821

n_silver = df_silver.count()
n_wpi = df_wpi.count()
print("Filas plata:", n_silver)
print("Puertos WPI:", n_wpi)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Trayectorias diarias (1 fila por MMSI+día)

# COMMAND ----------

df_tray = trayectorias.trayectorias_diarias(df_silver, cfg)
(
    df_tray.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(trayectorias.tabla_trayectorias_fqn(cfg))
)
n_tray = spark.table(trayectorias.tabla_trayectorias_fqn(cfg)).count()  # noqa: F821
print("trayectorias_diarias =", n_tray)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Presencia en puertos (1 fila por MMSI+puerto+día)

# COMMAND ----------

df_pres = puertos.presencia_puertos(df_silver, df_wpi, cfg)
(
    df_pres.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(puertos.tabla_presencia_puertos_fqn(cfg))
)
n_pres = spark.table(puertos.tabla_presencia_puertos_fqn(cfg)).count()  # noqa: F821
print("presencia_puertos =", n_pres)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Matriz de tráfico (1 fila por celda+hora)

# COMMAND ----------

df_matriz = matriz_trafico.matriz_trafico(df_silver, cfg)
(
    df_matriz.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(matriz_trafico.tabla_matriz_trafico_fqn(cfg))
)
n_matriz = spark.table(matriz_trafico.tabla_matriz_trafico_fqn(cfg)).count()  # noqa: F821
print("matriz_trafico =", n_matriz)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Resultado para el orquestador (`dbutils.notebook.exit`)

# COMMAND ----------

import json

salida = {
    "silver": int(n_silver),
    "wpi": int(n_wpi),
    "trayectorias_diarias": int(n_tray),
    "presencia_puertos": int(n_pres),
    "matriz_trafico": int(n_matriz),
}
dbutils.notebook.exit(json.dumps(salida))  # noqa: F821
