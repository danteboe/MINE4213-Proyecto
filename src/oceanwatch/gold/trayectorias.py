"""Capa ORO — trayectorias diarias por buque (modelo dimensional, ADR-012).

Transformación de negocio alimentada SOLO de ``oceanwatch.silver.ais_clean``
(el oro NO limpia). Ordena las posiciones por ``(MMSI, BaseDateTime)`` y, con una
``Window`` por buque y día, calcula la distancia haversine entre posiciones
consecutivas (fórmula pura de ``oceanwatch.quality.geo.haversine_km``, replicada
aquí como expresión nativa de columna por rendimiento sobre 25.9M filas: una UDF
escalar fila-a-fila no es viable a esa escala, ver findings de FEAT-003).

Granularidad declarada = **una fila por (MMSI, dia)**.
Clave primaria lógica = ``(MMSI, dia)``.

Todos los imports de PySpark son perezosos: el módulo se importa sin Spark.
"""

from __future__ import annotations

from typing import Any

from oceanwatch.common.config import fqn
from oceanwatch.quality.geo import RADIO_TERRESTRE_KM


def tabla_trayectorias_fqn(cfg: dict[str, Any]) -> str:
    """FQN de la tabla oro de trayectorias (``oceanwatch.gold.trayectorias_diarias``)."""
    return fqn(cfg, "gold", cfg["gold"]["tabla_trayectorias"])


def _haversine_km_expr(lat1: Any, lon1: Any, lat2: Any, lon2: Any):
    """Expresión nativa de columna equivalente a ``geo.haversine_km`` (great-circle).

    Reproduce EXACTAMENTE la aritmética de ``oceanwatch.quality.geo.haversine_km``
    (radio terrestre medio ``RADIO_TERRESTRE_KM``=6371 km) usando únicamente
    funciones de ``pyspark.sql.functions``, para que el cálculo sea 100% nativo
    (sin UDF). El import de PySpark es perezoso.
    """
    from pyspark.sql import functions as F

    rlat1 = F.radians(lat1)
    rlat2 = F.radians(lat2)
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = F.pow(F.sin(dlat / 2), 2) + F.cos(rlat1) * F.cos(rlat2) * F.pow(F.sin(dlon / 2), 2)
    c = 2 * F.asin(F.sqrt(F.when(a > 1, F.lit(1.0)).otherwise(a)))
    return F.lit(RADIO_TERRESTRE_KM) * c


def trayectorias_diarias(df_silver: Any, cfg: dict[str, Any]):
    """Construye la trayectoria diaria por buque a partir de la plata.

    Alimentada SOLO de ``oceanwatch.silver.ais_clean``. Pasos:

    1. Deriva el día ``dia`` = ``to_date(BaseDateTime)`` (serverless usa UTC, ADR-004).
    2. Ordena por ``(MMSI, dia, BaseDateTime)`` y, con una ``Window`` particionada por
       ``(MMSI, dia)``, obtiene la posición anterior (``lag`` de LAT/LON y BaseDateTime).
    3. Calcula la distancia haversine (km) entre cada posición y la anterior
       (``_haversine_km_expr``; la primera posición del día tiene distancia 0).
    4. Agrega por ``(MMSI, dia)``: distancia total recorrida, nº de posiciones,
       instantes inicial/final y *bounding box* (min/max de LAT y LON).

    Args:
        df_silver: DataFrame de ``oceanwatch.silver.ais_clean`` (debe tener MMSI,
            BaseDateTime, LAT, LON no nulos; la plata ya garantiza coordenadas válidas).
        cfg: configuración cargada.

    Returns:
        DataFrame con **una fila por (MMSI, dia)** y las columnas:
        ``MMSI``, ``dia``, ``n_posiciones``, ``distancia_total_km``,
        ``primera_posicion``, ``ultima_posicion``, ``duracion_horas``,
        ``lat_min``, ``lat_max``, ``lon_min``, ``lon_max``.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    base = df_silver.select("MMSI", "BaseDateTime", "LAT", "LON").withColumn(
        "dia", F.to_date("BaseDateTime")
    )

    w = Window.partitionBy("MMSI", "dia").orderBy("BaseDateTime")
    con_lag = (
        base.withColumn("_lat_prev", F.lag("LAT").over(w))
        .withColumn("_lon_prev", F.lag("LON").over(w))
    )

    # Distancia al punto anterior (0 en la primera posición del día = sin lag).
    con_dist = con_lag.withColumn(
        "_dist_km",
        F.when(
            F.col("_lat_prev").isNotNull(),
            _haversine_km_expr(
                F.col("_lat_prev"), F.col("_lon_prev"), F.col("LAT"), F.col("LON")
            ),
        ).otherwise(F.lit(0.0)),
    )

    agregado = con_dist.groupBy("MMSI", "dia").agg(
        F.count("*").alias("n_posiciones"),
        F.sum("_dist_km").alias("distancia_total_km"),
        F.min("BaseDateTime").alias("primera_posicion"),
        F.max("BaseDateTime").alias("ultima_posicion"),
        F.min("LAT").alias("lat_min"),
        F.max("LAT").alias("lat_max"),
        F.min("LON").alias("lon_min"),
        F.max("LON").alias("lon_max"),
    )

    return agregado.withColumn(
        "duracion_horas",
        (
            F.col("ultima_posicion").cast("long") - F.col("primera_posicion").cast("long")
        )
        / F.lit(3600.0),
    ).select(
        "MMSI",
        "dia",
        "n_posiciones",
        "distancia_total_km",
        "primera_posicion",
        "ultima_posicion",
        "duracion_horas",
        "lat_min",
        "lat_max",
        "lon_min",
        "lon_max",
    )
