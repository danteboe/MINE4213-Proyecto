"""Detección de anomalías de velocidad (SOG) con MLflow (Req. 6, ADR-005).

Caso de machine learning de la Entrega 2: detectar posiciones AIS cuya velocidad
sobre el fondo (SOG) es anómala RELATIVA al tipo de buque (``VesselType``). La
distribución real de SOG (ver ``evidence/req06-ml-mlflow``) tiene medias muy
distintas por tipo, por lo que la normalidad se define por tipo mediante el z-score
``z_sog = (SOG - media_tipo) / std_tipo``. NO se implementa pronóstico de arribos
en esta entrega.

El objetivo (según la rúbrica) es DEMOSTRAR la integración MLOps: características
derivadas del Lakehouse, experimento MLflow (línea base por umbral de z-score vs
IsolationForest), modelo registrado en Unity Catalog con alias ``@champion`` y una
tarea del Job que puebla ``oceanwatch.gold.anomalias_velocidad``. El modelo es
deliberadamente SIMPLE.

Este subpaquete reexporta las funciones puras de ingeniería de características de
``features.py``. Los imports de PySpark/numpy/sklearn son PEREZOSOS (nunca a nivel
de módulo): el paquete se importa y testea sin esas dependencias.
"""

from oceanwatch.ml.features import (
    clasificar_por_umbral_z,
    construir_vector_features,
    delta_sog,
    estadisticas_por_tipo,
    hora_a_ciclico,
    z_sog_por_tipo,
)

__all__ = [
    "estadisticas_por_tipo",
    "z_sog_por_tipo",
    "delta_sog",
    "hora_a_ciclico",
    "construir_vector_features",
    "clasificar_por_umbral_z",
]
