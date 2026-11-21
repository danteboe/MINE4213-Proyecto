"""Índice espacial pequeño para ríos navegables y lagos."""

from functools import lru_cache
import gzip

from oceanwatch.common.config import default_config_path


@lru_cache(maxsize=1)
def _indice_hidrografico():
    import shapely

    ruta = default_config_path().parents[1] / "resources" / "hidrografia.wkb.gz"
    with gzip.open(ruta, "rt", encoding="ascii") as archivo:
        geometrias = shapely.from_wkb([linea.strip() for linea in archivo])
    return shapely.STRtree(geometrias)


def agregar_evidencia_hidrografica(agua, lat, lon, margen):
    """Marca celdas antes terrestres próximas a una geometría; conserva el ráster."""
    import numpy as np
    import shapely

    sin_evidencia = np.flatnonzero(~agua)
    puntos = shapely.points(lon[sin_evidencia], lat[sin_evidencia])
    # query_nearest devuelve como máximo un par por punto y evita un producto
    # con todas las geometrías alcanzables en zonas con mucha densidad.
    ids = _indice_hidrografico().query_nearest(puntos, max_distance=margen, all_matches=False)
    agua[sin_evidencia[ids[0]]] = True
