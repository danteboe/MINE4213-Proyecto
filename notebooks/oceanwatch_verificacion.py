# Databricks notebook source
"""Pruebas con checkpoint aislado y medición de poda en la tabla institucional."""

from contextlib import redirect_stdout
from io import StringIO
import json
from pathlib import Path
import sys
from uuid import uuid4

from pyspark.sql import functions as F

sys.path.insert(0, str(Path.cwd().parent / "src"))

from oceanwatch.common.config import fqn, load_cluster_config
from oceanwatch.ingest.autoloader import ejecutar_ingesta, tabla_bronze_fqn
from oceanwatch.ingest.paths import landing_csv_dir
from oceanwatch.orquestacion import validar_fecha_proceso
from oceanwatch.orquestacion.cluster import preparar
from oceanwatch.quality.diaria import conciliar_dia, materializar_dia
from oceanwatch.quality.materializar import tabla_cuarentena_fqn, tabla_silver_fqn

spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.shuffle.partitions", "auto")


def prueba_streaming():
    cfg = load_cluster_config()
    sufijo = uuid4().hex[:12]
    cfg["ruta_volumen"] += f"/pruebas/{sufijo}"
    for bloque, clave in (
        ("ingesta", "tabla_bronze"), ("silver", "tabla_clean"),
        ("quarantine", "tabla_rejected"), ("quality_plata", "tabla_metricas_calidad"),
    ):
        cfg[bloque][clave] = f"prueba_{sufijo}"
    fecha = "2023-06-03"
    folder = landing_csv_dir(cfg, fecha)
    tabla = tabla_bronze_fqn(cfg)
    header = ",".join(c["nombre"] for c in cfg["esquema_ais"])

    def fila(mmsi, ts, lat=36, lon=-122.6, sog=12):
        return f"{mmsi},{ts},{lat},{lon},{sog},1,1,TEST,,,70,0,10,5,2,70,A"

    def llegada(nombre, filas):
        dbutils.fs.put(f"{folder}/{nombre}.csv", "\n".join([header, *filas]) + "\n")

    preparar(spark, dbutils, cfg)
    try:
        dbutils.fs.mkdirs(folder)
        ts = f"{fecha}T12:00:00"
        llegada("a", [fila(366000001, ts), fila(366000001, ts, sog=13)])
        q0 = ejecutar_ingesta(spark, cfg)
        assert spark.table(tabla).count() == 2

        spark.sql(f"ALTER TABLE {tabla} ADD CONSTRAINT fallo_inyectado CHECK (MMSI <> 366000002)")
        llegada("b", [
            fila(366000002, ts),
            fila(366000003, ts, sog=-1),
            fila(366000004, ts, lat=38.5, lon=-98),
            fila(366000005, "2023-06-04T00:00:01"),
        ])
        try:
            dbutils.notebook.run("oceanwatch_fallo_stream", 300, {"prueba_id": sufijo})
        except Exception:
            # Jobs marca un notebook con un stream fallido aunque capture su excepción.
            # El hijo confirma la causa antes de que el notebook padre continúe.
            if not Path(cfg["ruta_volumen"], "fallo_delta_confirmado").is_file():
                raise
        else:
            raise AssertionError("La restricción debió interrumpir el commit")
        assert spark.table(tabla).count() == 2
        spark.sql(f"ALTER TABLE {tabla} DROP CONSTRAINT fallo_inyectado")

        q1 = ejecutar_ingesta(spark, cfg)
        q2 = ejecutar_ingesta(spark, cfg)
        assert q0.id == q1.id == q2.id
        assert spark.table(tabla).count() == 6
        assert sum(p["numInputRows"] for p in q2.recentProgress) == 0
        materializar_dia(spark, cfg, fecha)
        conciliacion = conciliar_dia(spark, cfg, fecha)
        motivos = {
            motivo for r in spark.table(tabla_cuarentena_fqn(cfg)).select("causas").collect()
            for motivo in r.causas
        }
        assert motivos == {
            "sin_evidencia_de_agua", "sog_fuera_de_rango", "timestamp_fuera_del_dia_fichero",
        }
        assert spark.table(tabla_cuarentena_fqn(cfg)).where("MMSI = 366000003").first().SOG == -1
        return {
            "captura_fallo_commit": True, "checkpoint": str(q0.id),
            "filas_antes_del_fallo": 2, "filas_tras_reanudar": 6, "filas_replay": 0,
            "conciliacion": conciliacion, "causas": sorted(motivos),
        }
    finally:
        for capa, nombre in (
            ("bronze", cfg["ingesta"]["tabla_bronze"]),
            ("silver", cfg["silver"]["tabla_clean"]),
            ("quarantine", cfg["quarantine"]["tabla_rejected"]),
            ("quality", cfg["quality_plata"]["tabla_metricas_calidad"]),
        ):
            spark.sql(f"DROP TABLE IF EXISTS {fqn(cfg, capa, nombre)}")
        dbutils.fs.rm(cfg["ruta_volumen"], recurse=True)


def _medir(df):
    medidas = df.first().asDict()
    plan = StringIO()
    with redirect_stdout(plan):
        df.explain("formatted")
    return {
        **medidas,
        "filtros_particion": [
            linea.strip() for linea in plan.getvalue().splitlines()
            if "PartitionFilters" in linea
        ],
    }


def prueba_poda(fecha):
    # input_file_name() no está disponible bajo Unity Catalog. _metadata expone
    # los archivos efectivamente tocados por el query a través de su ruta y byte size.
    tabla = tabla_silver_fqn(load_cluster_config())
    version_delta = spark.sql(f"DESCRIBE HISTORY {tabla} LIMIT 1").first().version
    base = spark.read.format("delta").option("versionAsOf", version_delta).table(tabla)
    dia = F.col("_batch_date") == F.lit(fecha).cast("date")
    zona = F.col("LAT").between(25, 32) & F.col("LON").between(-98, -88)
    zonas = base.select(
        "_batch_date", "LAT", "LON",
        F.col("_metadata.file_path").alias("ruta"),
        F.col("_metadata.file_size").alias("bytes"),
    )
    podada = zonas.where(dia).groupBy("ruta", "bytes").agg(
        F.count(F.when(zona, 1)).alias("coincidencias")
    )
    completa = zonas.groupBy("ruta", "bytes").agg(
        F.count(F.when(dia & zona, 1)).alias("coincidencias")
    )
    resultados = {}
    for nombre, df in (("sin_poda", completa), ("con_poda", podada)):
        q = df.agg(F.sum("coincidencias").alias("n"), F.count("*").alias("archivos"),
                   F.sum("bytes").alias("bytes_archivos"))
        resultados[nombre] = _medir(q)
    assert resultados["con_poda"]["n"] == resultados["sin_poda"]["n"]
    assert resultados["con_poda"]["archivos"] < resultados["sin_poda"]["archivos"]
    return {"version_delta": version_delta, "fecha": fecha, **resultados}


dbutils.widgets.text("fecha_proceso", "2023-06-08")
dbutils.widgets.text("medir_poda", "true")
resultado = {"streaming": prueba_streaming()}
if dbutils.widgets.get("medir_poda") == "true":
    resultado["poda"] = prueba_poda(validar_fecha_proceso(dbutils.widgets.get("fecha_proceso")))
dbutils.notebook.exit(json.dumps(resultado, ensure_ascii=False, default=str))
