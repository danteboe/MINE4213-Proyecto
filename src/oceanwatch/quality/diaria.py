"""Compuertas y conciliación de un lote NOAA, en UTC."""

from __future__ import annotations

from pyspark import StorageLevel
from pyspark.sql import functions as F

from oceanwatch.common.config import fqn
from oceanwatch.ingest.paths import clave_dedup
from oceanwatch.orquestacion.cluster import dia_col, reemplazar_dia
from oceanwatch.quality.compuertas import aplicar_compuertas
from oceanwatch.quality.conciliacion import verificar_invariante
from oceanwatch.quality.hidrografia import agregar_evidencia_hidrografica
from oceanwatch.quality.materializar import (
    columnas_originales,
    tabla_bronze_fqn,
    tabla_cuarentena_fqn,
    tabla_metricas_fqn,
    tabla_silver_fqn,
)


def _redondear(df, decimales):
    return (
        df.withColumn("celda_lat", F.round("LAT", decimales))
        .withColumn("celda_lon", F.round("LON", decimales))
    )


def _agua_por_celda(spark, bronce, cfg):
    """Evalúa una vez cada celda y transmite solo tres columnas al cruce."""
    import numpy as np
    from global_land_mask import globe

    limite = cfg["max_celdas_por_dia"]
    celdas = (
        bronce.where(F.col("LAT").between(-90, 90) & F.col("LON").between(-180, 180))
        .select("celda_lat", "celda_lon").distinct().limit(limite + 1).collect()
    )
    if len(celdas) > limite:
        raise ValueError(f"El lote excede el máximo de {limite} celdas para la máscara")
    tipo = "celda_lat DOUBLE, celda_lon DOUBLE, en_agua BOOLEAN"
    if not celdas:
        return spark.createDataFrame([], tipo)

    puntos = np.array(celdas, dtype=np.float64)
    lat, lon = puntos.T
    margen = cfg["tolerancia_costera_grados"]
    agua = np.zeros(len(celdas), dtype=bool)
    for dlat in (-margen, 0, margen):
        for dlon in (-margen, 0, margen):
            agua |= globe.is_ocean(
                np.clip(lat + dlat, -90, 90), (lon + dlon + 180) % 360 - 180
            )
    agregar_evidencia_hidrografica(agua, lat, lon, margen)
    puertos = spark.table(fqn(cfg, "gold", cfg["gold"]["tabla_wpi"]))
    decimales = cfg["gold"]["decimales_grilla"]
    celdas_puerto = {
        tuple(r) for r in puertos.select(
            F.round("LATITUDE", decimales), F.round("LONGITUDE", decimales)
        ).collect()
    }
    return spark.createDataFrame(
        [(r[0], r[1], bool(es_agua) or tuple(r) in celdas_puerto)
         for r, es_agua in zip(celdas, agua)],
        tipo,
    )


def _clasificar(spark, bronce, fecha, cfg):
    originales = columnas_originales(cfg)
    causas = cfg["quarantine"]["columna_causas"]
    reglas = cfg["calidad"]["reglas"]
    decimales = cfg["gold"]["decimales_grilla"]
    posiciones = _redondear(bronce, decimales)
    agua = _agua_por_celda(spark, posiciones, cfg)
    saneadas = aplicar_compuertas(posiciones.withColumn("_original", F.struct(*originales)), cfg)
    saneadas = saneadas.drop("en_agua").join(
        F.broadcast(agua), ["celda_lat", "celda_lon"], "left"
    )
    sog = F.col("_original.SOG")
    sog_cfg = cfg["calidad"]["sog"]
    fuera_de_rango = F.isnan(sog) | (sog < 0) | (
        (sog > sog_cfg["max_fisico"]) & (sog < sog_cfg["centinela"])
    )
    adicionales = F.filter(
        F.array(
            F.when(F.col("en_agua") == F.lit(False), F.lit(reglas["coordenada_en_agua"]["nombre"])),
            F.when(fuera_de_rango, F.lit(reglas["sog_fuera_de_rango"]["nombre"])),
            F.when(
                F.to_date("BaseDateTime") != F.lit(fecha).cast("date"),
                F.lit(reglas["timestamp_fuera_del_dia_fichero"]["nombre"]),
            ),
        ),
        lambda causa: causa.isNotNull(),
    )
    return saneadas.withColumn(causas, F.concat(causas, adicionales)).withColumn(
        "_original", F.when(F.size(causas) > 0, F.col("_original"))
    )


def _silver(validas, cfg):
    clave = clave_dedup(cfg)
    otras_columnas = [
        c for c in [*columnas_originales(cfg), *cfg["silver"]["banderas"].values()]
        if c not in clave
    ]
    elegible = F.struct(
        F.col("SOG").isNotNull().alias("_sog_valida"),
        F.col("_rescued_data").isNull().alias("_sin_rescate"),
        *otras_columnas,
    )
    return validas.groupBy(*clave).agg(
        F.max(elegible).alias("_elegida"),
        (F.count("*") - F.lit(1)).alias("_duplicados_origen"),
    ).select(*clave, *(F.col(f"_elegida.{c}").alias(c) for c in otras_columnas), "_duplicados_origen")


def _metricas(clasificadas, cfg):
    causas = cfg["quarantine"]["columna_causas"]
    reglas = [
        v["nombre"] for v in cfg["calidad"]["reglas"].values()
        if v["accion"] == "descartar_fila"
    ]
    conteos = clasificadas.groupBy("_batch_date").agg(
        F.count("*").alias("evaluados"),
        *[
            F.count(F.when(F.array_contains(causas, regla), 1)).alias(regla)
            for regla in reglas
        ],
    )
    umbral = float(cfg["quality_plata"]["umbral_alerta_pct_rechazo"])
    return (
        conteos.unpivot(["_batch_date", "evaluados"], reglas, "regla", "rechazados")
        .withColumn("pasados", F.col("evaluados") - F.col("rechazados"))
        .withColumn("pct_rechazo", F.col("rechazados") / F.col("evaluados"))
        .withColumn("alerta", F.col("pct_rechazo") >= umbral)
        .withColumn("umbral_alerta", F.lit(umbral))
        .withColumn("_registrado", F.current_timestamp())
    )


def materializar_dia(spark, cfg, fecha):
    raw = dia_col(spark, tabla_bronze_fqn(cfg), fecha)
    clasificadas = _clasificar(spark, raw, fecha, cfg).persist(StorageLevel.DISK_ONLY)
    causas = cfg["quarantine"]["columna_causas"]
    try:
        metricas = _metricas(clasificadas, cfg)
        validas = clasificadas.where(F.size(causas) == 0)
        silver = _silver(validas, cfg)
        rechazadas = clasificadas.where(F.size(causas) > 0).select(
            "_original.*", causas, F.current_timestamp().alias("_fecha_cuarentena")
        )
        silver.explain("formatted")
        reemplazar_dia(
            silver, tabla_silver_fqn(cfg), fecha,
            "Posiciones AIS admitidas y deduplicadas por MMSI y timestamp UTC.", particionar=True,
        )
        reemplazar_dia(
            rechazadas, tabla_cuarentena_fqn(cfg), fecha,
            "Registro original de bronce con las causas de rechazo.",
        )
        reemplazar_dia(
            metricas, tabla_metricas_fqn(cfg), fecha,
            "Reglas de rechazo por lote, calculadas sobre todas las posiciones crudas.",
        )
    finally:
        clasificadas.unpersist()
    return {"fecha": fecha, "regla_agua": cfg["calidad"]["reglas"]["coordenada_en_agua"]["version"]}


def conciliar_dia(spark, cfg, fecha):
    n_bronce = dia_col(spark, tabla_bronze_fqn(cfg), fecha).count()
    silver = dia_col(spark, tabla_silver_fqn(cfg), fecha).agg(
        F.count("*").alias("filas"),
        F.coalesce(F.sum("_duplicados_origen"), F.lit(0)).alias("duplicados"),
    ).first()
    n_cuarentena = dia_col(spark, tabla_cuarentena_fqn(cfg), fecha).count()
    resultado = verificar_invariante(n_bronce, silver.filas, n_cuarentena, silver.duplicados)
    if not resultado["cuadra"] or n_bronce == 0:
        raise RuntimeError(f"Conciliación {fecha}: {resultado['descripcion']}")
    return resultado
