"""Agregados por fecha de evento para el tablero y el análisis AIS."""

from pyspark import StorageLevel
from pyspark.sql import functions as F

from oceanwatch.common.config import fqn
from oceanwatch.gold.matriz_trafico import matriz_trafico, tabla_matriz_trafico_fqn
from oceanwatch.gold.puertos import presencia_puertos, tabla_presencia_puertos_fqn
from oceanwatch.gold.trayectorias import tabla_trayectorias_fqn, trayectorias_diarias
from oceanwatch.orquestacion.cluster import dia_col, reemplazar_dia
from oceanwatch.quality.materializar import tabla_silver_fqn


def publicar_dia(spark, cfg, fecha):
    ais = dia_col(spark, tabla_silver_fqn(cfg), fecha).select(
        "MMSI", "BaseDateTime", "LAT", "LON"
    ).persist(StorageLevel.MEMORY_AND_DISK)
    wpi = spark.table(fqn(cfg, "gold", cfg["gold"]["tabla_wpi"]))
    tabla_hora = tabla_matriz_trafico_fqn(cfg)
    tabla_dia = fqn(cfg, "gold", "matriz_trafico_diaria")
    try:
        salidas = [
            (trayectorias_diarias(ais, cfg), tabla_trayectorias_fqn(cfg),
             "Trayectoria por buque y día UTC. Distancia entre posiciones sucesivas."),
            (presencia_puertos(ais, wpi, cfg), tabla_presencia_puertos_fqn(cfg),
             "Presencia de buques por puerto del World Port Index y día UTC."),
            (matriz_trafico(ais, cfg).withColumn("dia", F.to_date("hora")), tabla_hora,
             "Posiciones y buques distintos por celda y hora UTC."),
        ]
        for df, tabla, comentario in salidas:
            df.explain("formatted")
            reemplazar_dia(df, tabla, fecha, comentario, columna="dia")
        diaria = dia_col(spark, tabla_hora, fecha, "dia").groupBy(
            "celda_lat", "celda_lon", "dia"
        ).agg(
            F.sum("n_posiciones").alias("intensidad"),
            F.sum("n_buques").alias("buques"),
        )
        reemplazar_dia(
            diaria, tabla_dia, fecha, "Posiciones y buque-horas por celda y día UTC.", columna="dia",
        )
    finally:
        ais.unpersist()
    return {"fecha": fecha, "tablas": [tabla for _, tabla, _ in salidas] + [tabla_dia]}
