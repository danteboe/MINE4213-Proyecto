"""Funciones geoespaciales puras reutilizables por la capa oro (sin PySpark).

Aritmética pura, testeable con ``pytest`` sin Spark:
- ``haversine_km``: distancia de círculo máximo entre dos coordenadas (base de
  ``gold.trayectorias_diarias``, que suma distancias recorridas por buque/día).
- ``celda_grilla``: redondeo lat/lon a una grilla regular (base de
  ``gold.matriz_trafico``). Respeta la decisión de diseño de Entrega 1 de usar
  redondeo lat/lon y NO H3 (ADR-001); por defecto a ``cfg['gold']['decimales_grilla']``=2.
"""

from __future__ import annotations

from math import asin, cos, radians, sin, sqrt
from typing import Any, Optional, Union

# Radio terrestre medio en km (esfera de referencia estándar para haversine).
RADIO_TERRESTRE_KM = 6371.0


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distancia en km entre dos puntos (lat, lon en grados) por la fórmula haversine.

    Usa el radio terrestre medio ``RADIO_TERRESTRE_KM``=6371 km. La fórmula
    haversine calcula la distancia de círculo máximo (great-circle) sobre una
    esfera; el error frente a un elipsoide es < 0.5%, suficiente para distancias
    recorridas por buques en la capa oro.
    """
    rlat1, rlon1, rlat2, rlon2 = map(radians, (lat1, lon1, lat2, lon2))
    dlat = rlat2 - rlat1
    dlon = rlon2 - rlon1
    a = sin(dlat / 2) ** 2 + cos(rlat1) * cos(rlat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    return RADIO_TERRESTRE_KM * c


def celda_grilla(
    lat: float,
    lon: float,
    decimales: Optional[int] = None,
    cfg: Optional[dict[str, Any]] = None,
    como_cadena: bool = False,
) -> Union[tuple[float, float], str]:
    """Asigna una coordenada a una celda de grilla por redondeo lat/lon.

    Redondea ``lat`` y ``lon`` a ``decimales`` decimales (grilla regular). Si
    ``decimales`` es ``None`` se toma de ``cfg['gold']['decimales_grilla']``
    (por defecto 2, ADR-001: NO H3). Devuelve la celda como tupla
    ``(lat_red, lon_red)`` o, si ``como_cadena=True``, una cadena estable
    ``"lat,lon"`` apta como clave de agrupación.

    A 2 decimales cada celda mide ~1.1 km de lado en latitud, resolución acorde
    con la densidad del tráfico AIS del corpus.
    """
    if decimales is None:
        if cfg is not None:
            decimales = int(cfg["gold"]["decimales_grilla"])
        else:
            decimales = 2
    lat_red = round(float(lat), decimales)
    lon_red = round(float(lon), decimales)
    if como_cadena:
        return f"{lat_red:.{decimales}f},{lon_red:.{decimales}f}"
    return (lat_red, lon_red)
