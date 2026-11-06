"""Ingeniería de características para la detección de anomalías de SOG (ADR-005).

Fuente de verdad TESTEABLE del caso de ML de la Fase 6: funciones PURAS que
definen cómo se construye el vector de características de cada posición AIS. Los
notebooks serverless de entrenamiento (FEAT-002) e inferencia (FEAT-003) replican
esta misma lógica con expresiones Spark nativas (patrón de
``oceanwatch.gold.trayectorias.py``: una expresión de columna que reproduce
exactamente una función pura). Mantener ambas implementaciones de acuerdo es la
razón por la que estas funciones son la referencia testeada sin Spark.

Decisión central (D1, ADR-005): la anomalía de velocidad es RELATIVA al
``VesselType``. La distribución real de SOG (ver
``evidence/req06-ml-mlflow/distribucion_sog_insumo_modelo.md``) muestra medias muy
distintas por tipo (carga ~6.43 kn, pasajeros ~4.22 kn, remolque ~1.73 kn), por lo
que el z-score se calcula contra la media y desviación del propio tipo:
``z_sog = (SOG - media_tipo) / std_tipo``.

Orden FIJO del vector de características (CONTRATO del modelo, no reordenar):
``[z_sog, delta_sog, hora_sin, hora_cos]``.

Todos los imports de PySpark/numpy son PEREZOSOS (dentro de las funciones que los
necesitan). Las funciones escalares (``z_sog_por_tipo``, ``delta_sog``,
``hora_a_ciclico``, ``construir_vector_features``, ``clasificar_por_umbral_z``) NO
tocan Spark y son testeables sin ninguna dependencia de cómputo.
"""

from __future__ import annotations

from typing import Any, Optional


def estadisticas_por_tipo(df_train: Any, cfg: dict[str, Any]) -> dict[int, tuple[float, float]]:
    """Media y desviación estándar de SOG por ``VesselType``, SOLO del train.

    Calcula los estadísticos que normalizan el z-score (D1) ajustándolos
    EXCLUSIVAMENTE sobre los datos de entrenamiento (D3, antifuga). Excluye las
    posiciones con SOG centinela (``sog_no_disponible = true``, ADR-009) y con SOG
    nulo, que no representan velocidad real.

    El import de PySpark es perezoso: la agregación se hace en Spark con
    ``groupBy`` y se trae al driver con ``toPandas``/``collect`` (un par de valores
    por tipo, volumen trivial).

    Args:
        df_train: DataFrame de ``oceanwatch.silver.ais_clean`` del train (debe tener
            las columnas ``SOG``, ``VesselType`` y la bandera ``sog_no_disponible``).
        cfg: configuración cargada (se usa el nombre de la bandera de SOG).

    Returns:
        ``dict`` que mapea ``VesselType`` (int) a una tupla ``(media, std)`` de SOG.
        Los tipos con un único registro (std nulo) devuelven ``std = 0.0``; el
        consumidor (``z_sog_por_tipo``) aplica ``std_minimo`` para no dividir por 0.
    """
    from pyspark.sql import functions as F  # import perezoso

    bandera_sog = cfg["silver"]["banderas"]["sog"]

    agregado = (
        df_train.filter(
            (F.col("SOG").isNotNull()) & (~F.col(bandera_sog))
        )
        .groupBy("VesselType")
        .agg(
            F.avg("SOG").alias("media"),
            F.stddev("SOG").alias("std"),
        )
    )

    stats: dict[int, tuple[float, float]] = {}
    for fila in agregado.collect():
        if fila["VesselType"] is None:
            continue
        media = float(fila["media"]) if fila["media"] is not None else 0.0
        std = float(fila["std"]) if fila["std"] is not None else 0.0
        stats[int(fila["VesselType"])] = (media, std)
    return stats


def z_sog_por_tipo(
    sog: float,
    vessel_type: Optional[int],
    stats: dict[int, tuple[float, float]],
    std_minimo: float = 1e-6,
) -> float:
    """z-score de SOG RELATIVO al tipo de buque (feature central, D1/ADR-005).

    Fórmula: ``(sog - media_tipo) / max(std_tipo, std_minimo)``.

    Args:
        sog: velocidad sobre el fondo (knots) de la posición.
        vessel_type: tipo de buque (clave en ``stats``).
        stats: estadísticos ``{tipo: (media, std)}`` ajustados SOLO con el train
            (``estadisticas_por_tipo``).
        std_minimo: cota inferior de la desviación para evitar división por cero
            (tipos con un único registro o std 0).

    Returns:
        El z-score como ``float``. ``0.0`` si el tipo no está en ``stats`` (tipo no
        visto en el train: no hay base para considerarlo anómalo).
    """
    if vessel_type is None or vessel_type not in stats:
        return 0.0
    media, std = stats[vessel_type]
    return (float(sog) - media) / max(std, std_minimo)


def delta_sog(sog_actual: float, sog_previo: Optional[float]) -> float:
    """Cambio de SOG respecto a la posición anterior del MISMO buque.

    Captura aceleraciones/frenadas bruscas (una señal de anomalía complementaria al
    z-score). En Spark se materializa con ``F.lag('SOG')`` sobre una ``Window``
    particionada por ``MMSI`` y ordenada por ``BaseDateTime`` (patrón de
    ``trayectorias.py``).

    Args:
        sog_actual: SOG de la posición actual (knots).
        sog_previo: SOG de la posición anterior del buque, o ``None`` si es la
            primera posición de su secuencia.

    Returns:
        ``sog_actual - sog_previo`` como ``float``; ``0.0`` cuando ``sog_previo`` es
        ``None`` (sin referencia anterior => sin cambio medible).
    """
    if sog_previo is None:
        return 0.0
    return float(sog_actual) - float(sog_previo)


def hora_a_ciclico(hora: int) -> tuple[float, float]:
    """Codifica la hora del día (0-23) como un par cíclico ``(sin, cos)``.

    Proyecta la hora sobre el círculo de 24 horas para que instantes contiguos en el
    tiempo (p. ej. 23:00 y 00:00) queden contiguos en el espacio de características y
    no artificialmente lejanos como ocurriría con la hora cruda. El import de ``math``
    es estándar (sin dependencias de cómputo).

    Args:
        hora: hora del día en ``[0, 23]`` (``F.hour(BaseDateTime)`` en Spark).

    Returns:
        Tupla ``(sin, cos)`` con ambos valores en ``[-1, 1]``.
    """
    import math

    angulo = 2.0 * math.pi * (float(hora) / 24.0)
    return (math.sin(angulo), math.cos(angulo))


def construir_vector_features(
    sog: float,
    vessel_type: Optional[int],
    sog_previo: Optional[float],
    hora: int,
    stats: dict[int, tuple[float, float]],
) -> list[float]:
    """Ensambla el vector de características en el orden FIJO del contrato del modelo.

    Orden (NO reordenar; es el contrato que consume el modelo en entrenamiento e
    inferencia): ``[z_sog, delta_sog, hora_sin, hora_cos]``.

    Args:
        sog: SOG actual (knots).
        vessel_type: tipo de buque.
        sog_previo: SOG de la posición anterior del buque (o ``None``).
        hora: hora del día (0-23).
        stats: estadísticos por tipo ajustados con el train.

    Returns:
        Lista de 4 ``float`` en el orden fijo documentado.
    """
    z = z_sog_por_tipo(sog, vessel_type, stats)
    d = delta_sog(sog, sog_previo)
    hora_sin, hora_cos = hora_a_ciclico(hora)
    return [z, d, hora_sin, hora_cos]


def clasificar_por_umbral_z(z_sog: float, umbral: float = 3.0) -> bool:
    """Clasificador LÍNEA BASE por umbral de z-score (D2, interpretable, sin entrenar).

    Marca una posición como anómala si su velocidad se aleja del comportamiento
    típico de su tipo más de ``umbral`` desviaciones. Es la referencia que el
    experimento MLflow compara contra IsolationForest (FEAT-002).

    Args:
        z_sog: z-score de SOG relativo al tipo (``z_sog_por_tipo``).
        umbral: nº de desviaciones estándar para considerar anomalía (por defecto
            ``3.0``, ``cfg['ml']['umbral_zscore_base']``).

    Returns:
        ``True`` si ``|z_sog| > umbral`` (el borde exacto NO es anomalía), si no
        ``False``.
    """
    return abs(float(z_sog)) > umbral
