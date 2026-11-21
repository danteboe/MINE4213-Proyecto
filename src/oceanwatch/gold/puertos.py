"""Capa ORO — presencia de buques en puertos (modelo dimensional, ADR-012).

Cruza el tráfico AIS saneado (``oceanwatch.silver.ais_clean``) con la tabla de
referencia World Port Index (``oceanwatch.gold.world_port_index``) por la GRILLA
lat/lon a ``decimales_celda_puerto`` decimales (patrón **Reference Data Join**,
ratifica ADR-001: grilla por redondeo, NO H3). Aplica la regla geoespacial de
'en puerto' y una permanencia mínima, AMBAS parametrizadas desde
``cfg['gold']['presencia_puertos_param']`` y justificadas en ADR-012.

Definición de 'en puerto' (ADR-012): una posición AIS está 'en puerto' si su celda
de grilla (lat/lon redondeadas) coincide con la celda de algún puerto del WPI. Un
buque tiene 'presencia' en un puerto un día si acumula al menos
``permanencia_minima_posiciones`` posiciones en esa celda-puerto ese día (evita
contar un tránsito de 1-2 pings como presencia).

Granularidad declarada = **una fila por (MMSI, puerto, dia)**.
Clave primaria lógica = ``(MMSI, INDEX_NO, dia)``.

Todos los imports de PySpark son perezosos: el módulo se importa sin Spark.
"""

from __future__ import annotations

from typing import Any

from oceanwatch.common.config import fqn


def tabla_presencia_puertos_fqn(cfg: dict[str, Any]) -> str:
    """FQN de la tabla oro de presencia en puertos (``oceanwatch.gold.presencia_puertos``)."""
    return fqn(cfg, "gold", cfg["gold"]["tabla_presencia_puertos"])


def _decimales_celda(cfg: dict[str, Any]) -> int:
    """Decimales de la celda-puerto; por defecto ``gold.decimales_grilla`` (ADR-001)."""
    param = cfg["gold"].get("presencia_puertos_param", {})
    return int(param.get("decimales_celda_puerto", cfg["gold"]["decimales_grilla"]))


def _permanencia_minima(cfg: dict[str, Any]) -> int:
    """Nº mínimo de posiciones en la celda-puerto/día para declarar 'presencia'."""
    param = cfg["gold"].get("presencia_puertos_param", {})
    return int(param.get("permanencia_minima_posiciones", 1))


def presencia_puertos(df_silver: Any, df_wpi: Any, cfg: dict[str, Any]):
    """Cruza el AIS saneado con el WPI por celda de grilla y aplica la regla 'en puerto'.

    Alimentada SOLO de ``oceanwatch.silver.ais_clean`` (tráfico) y de la tabla de
    referencia ``oceanwatch.gold.world_port_index`` (puertos). Pasos:

    1. Deriva ``dia`` y la celda de grilla de cada posición AIS (redondeo lat/lon a
       ``decimales_celda_puerto``).
    2. Deriva la celda de grilla de cada puerto del WPI (mismos decimales).
    3. *Reference Data Join* AIS <-> WPI por ``(celda_lat, celda_lon)``: toda posición
       que cae en una celda que contiene al menos un puerto queda 'en puerto'.
    4. Agrega por ``(MMSI, INDEX_NO, dia)`` el nº de posiciones en la celda-puerto y
       los instantes de entrada/salida; filtra por
       ``n_posiciones >= permanencia_minima_posiciones`` (regla de permanencia).

    Args:
        df_silver: DataFrame de ``oceanwatch.silver.ais_clean`` (MMSI, BaseDateTime, LAT, LON).
        df_wpi: DataFrame de referencia WPI con INDEX_NO, PORT_NAME, COUNTRY,
            LATITUDE, LONGITUDE, HARBORSIZE, HARBORTYPE.
        cfg: configuración cargada.

    Returns:
        DataFrame con **una fila por (MMSI, puerto, dia)** y las columnas:
        ``MMSI``, ``INDEX_NO``, ``PORT_NAME``, ``COUNTRY``, ``dia``,
        ``n_posiciones_en_puerto``, ``primera_posicion``, ``ultima_posicion``,
        ``HARBORSIZE``, ``HARBORTYPE``.
    """
    from pyspark.sql import functions as F

    dec = _decimales_celda(cfg)
    minimo = _permanencia_minima(cfg)

    ais = (
        df_silver.select("MMSI", "BaseDateTime", "LAT", "LON")
        .withColumn("dia", F.to_date("BaseDateTime"))
        .withColumn("celda_lat", F.round("LAT", dec))
        .withColumn("celda_lon", F.round("LON", dec))
    )

    puertos = df_wpi.select(
        "INDEX_NO",
        "PORT_NAME",
        "COUNTRY",
        "LATITUDE",
        "LONGITUDE",
        "HARBORSIZE",
        "HARBORTYPE",
    ).withColumn("celda_lat", F.round("LATITUDE", dec)).withColumn(
        "celda_lon", F.round("LONGITUDE", dec)
    )

    cruce = ais.join(F.broadcast(puertos), on=["celda_lat", "celda_lon"], how="inner")

    agregado = cruce.groupBy(
        "MMSI", "INDEX_NO", "PORT_NAME", "COUNTRY", "HARBORSIZE", "HARBORTYPE", "dia"
    ).agg(
        F.count("*").alias("n_posiciones_en_puerto"),
        F.min("BaseDateTime").alias("primera_posicion"),
        F.max("BaseDateTime").alias("ultima_posicion"),
    )

    return agregado.filter(F.col("n_posiciones_en_puerto") >= F.lit(minimo)).select(
        "MMSI",
        "INDEX_NO",
        "PORT_NAME",
        "COUNTRY",
        "dia",
        "n_posiciones_en_puerto",
        "primera_posicion",
        "ultima_posicion",
        "HARBORSIZE",
        "HARBORTYPE",
    )
