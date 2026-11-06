"""Materialización de la capa ORO (modelo dimensional de negocio, ADR-012).

Transformaciones alimentadas SOLO de ``oceanwatch.silver.ais_clean`` (el oro NO
limpia). Cada módulo declara su granularidad y clave EXPLÍCITAS:

- ``trayectorias``  -> ``oceanwatch.gold.trayectorias_diarias`` (1 fila por MMSI+día;
  distancia haversine entre posiciones consecutivas).
- ``puertos``       -> ``oceanwatch.gold.presencia_puertos`` (1 fila por MMSI+puerto+día;
  cruce con el World Port Index por celda de grilla + regla 'en puerto' y permanencia).
- ``matriz_trafico``-> ``oceanwatch.gold.matriz_trafico`` (1 fila por celda+hora;
  grilla por redondeo lat/lon, ADR-001, NO H3).

La función pura ``en_misma_celda`` (decisión 'en puerto' por celda de grilla) vive
aquí y se reutiliza con ``oceanwatch.quality.geo.celda_grilla``.
"""

from __future__ import annotations

from typing import Any, Optional

from oceanwatch.quality.geo import celda_grilla


def en_misma_celda(
    lat_a: float,
    lon_a: float,
    lat_b: float,
    lon_b: float,
    decimales: Optional[int] = None,
    cfg: Optional[dict[str, Any]] = None,
) -> bool:
    """Decide (función PURA) si dos coordenadas caen en la misma celda de grilla.

    Es la definición geoespacial de 'en puerto' de ADR-012: una posición AIS está
    'en puerto' si comparte celda de grilla (lat/lon redondeadas a ``decimales``)
    con el puerto. Reutiliza ``oceanwatch.quality.geo.celda_grilla`` para que la
    decisión sea idéntica a la grilla de ``matriz_trafico`` (ADR-001, NO H3). Si
    ``decimales`` es ``None`` se toma de ``cfg['gold']['decimales_grilla']`` (=2).
    """
    celda_a = celda_grilla(lat_a, lon_a, decimales=decimales, cfg=cfg)
    celda_b = celda_grilla(lat_b, lon_b, decimales=decimales, cfg=cfg)
    return celda_a == celda_b
