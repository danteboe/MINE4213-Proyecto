"""Capa ORO — matriz de tráfico por celda espacial y hora (modelo dimensional, ADR-012).

Agrega el tráfico AIS saneado (``oceanwatch.silver.ais_clean``) en una grilla
espacial por redondeo lat/lon a ``decimales_grilla`` decimales (ADR-001: NO H3) y
por hora (truncando ``BaseDateTime`` a la hora, UTC en serverless, ADR-004).

Granularidad declarada = **una fila por (celda_lat, celda_lon, hora)**.
Clave primaria lógica = ``(celda_lat, celda_lon, hora)``.

Todos los imports de PySpark son perezosos: el módulo se importa sin Spark.
"""

from __future__ import annotations

from typing import Any

from oceanwatch.common.config import fqn


def tabla_matriz_trafico_fqn(cfg: dict[str, Any]) -> str:
    """FQN de la tabla oro de matriz de tráfico (``oceanwatch.gold.matriz_trafico``)."""
    return fqn(cfg, "gold", cfg["gold"]["tabla_matriz_trafico"])


def matriz_trafico(df_silver: Any, cfg: dict[str, Any]):
    """Agrega conteos de posiciones por celda espacial y hora.

    Alimentada SOLO de ``oceanwatch.silver.ais_clean``. Redondea LAT/LON a
    ``cfg['gold']['decimales_grilla']`` decimales (grilla por redondeo, ADR-001;
    coherente con ``oceanwatch.quality.geo.celda_grilla``) y trunca ``BaseDateTime``
    a la hora. Agrega por ``(celda_lat, celda_lon, hora)``:

    - ``n_posiciones``: nº de posiciones AIS en esa celda y hora.
    - ``n_buques``: nº de buques distintos (``MMSI``) en esa celda y hora.

    Args:
        df_silver: DataFrame de ``oceanwatch.silver.ais_clean`` (MMSI, BaseDateTime, LAT, LON).
        cfg: configuración cargada.

    Returns:
        DataFrame con **una fila por (celda_lat, celda_lon, hora)** y las columnas
        ``celda_lat``, ``celda_lon``, ``hora``, ``n_posiciones``, ``n_buques``.
    """
    from pyspark.sql import functions as F

    dec = int(cfg["gold"]["decimales_grilla"])

    base = (
        df_silver.select("MMSI", "BaseDateTime", "LAT", "LON")
        .withColumn("celda_lat", F.round("LAT", dec))
        .withColumn("celda_lon", F.round("LON", dec))
        .withColumn("hora", F.date_trunc("hour", "BaseDateTime"))
    )

    return base.groupBy("celda_lat", "celda_lon", "hora").agg(
        F.count("*").alias("n_posiciones"),
        F.countDistinct("MMSI").alias("n_buques"),
    ).select("celda_lat", "celda_lon", "hora", "n_posiciones", "n_buques")
