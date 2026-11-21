"""Tests de la máscara tierra/agua (Req. 2, ADR-010), sin Spark.

Cubre la parte DETERMINISTA:
- El baseline ``en_agua_bbox`` (sin dependencias) pasa SIEMPRE.
- El método principal ``en_agua`` solo se ejercita si ``global_land_mask`` está
  instalada (``pytest.importorskip``); si no, se verifica que el import PEREZOSO
  eleva ``MaskNoDisponibleError`` sin romper el import del módulo.
"""

from __future__ import annotations

import pytest

from oceanwatch.quality.tierra_agua import (
    MaskNoDisponibleError,
    en_agua,
    en_agua_bbox,
)


# --- Baseline sin dependencias (siempre debe pasar) ----------------------

def test_bbox_punto_en_oceano_abierto_es_agua():
    # Atlántico medio (0, -30): dentro de la franja oceánica -> True.
    assert en_agua_bbox(0.0, -30.0) is True


def test_bbox_punto_fuera_de_franja_navegable_no_es_agua():
    # Polo (89.9, 0): fuera de la caja latitudinal [-80, 80] -> False.
    assert en_agua_bbox(89.9, 0.0) is False


def test_bbox_coordenada_nula_devuelve_none():
    assert en_agua_bbox(None, -30.0) is None
    assert en_agua_bbox(10.0, None) is None


def test_bbox_coordenada_no_convertible_devuelve_none():
    assert en_agua_bbox("x", "y") is None


def test_bbox_parametrizable_desde_cfg():
    # cfg con una caja restringida: un punto fuera de ella devuelve False.
    cfg = {"calidad": {"coordenada_en_agua": {"bbox_agua": [[0.0, 10.0, 0.0, 10.0]]}}}
    assert en_agua_bbox(5.0, 5.0, cfg=cfg) is True
    assert en_agua_bbox(50.0, 50.0, cfg=cfg) is False


# --- Método principal: comportamiento del import perezoso ----------------

def test_en_agua_coordenada_nula_devuelve_none_sin_tocar_libreria():
    # Para coord. nula no se intenta importar la librería: devuelve None.
    assert en_agua(None, None) is None


def test_en_agua_sin_libreria_eleva_error_claro_o_clasifica():
    """Si global_land_mask no está, ``en_agua`` eleva MaskNoDisponibleError;
    si sí está, clasifica un punto conocido correctamente."""
    try:
        import global_land_mask  # noqa: F401
        disponible = True
    except ImportError:
        disponible = False

    if not disponible:
        with pytest.raises(MaskNoDisponibleError):
            en_agua(0.0, -30.0)
    else:
        # Atlántico medio es agua; centro de Kansas (EE. UU.) es tierra.
        assert en_agua(0.0, -30.0) is True
        assert en_agua(38.5, -98.0) is False


@pytest.mark.parametrize(
    "lat,lon,esperado",
    [
        (0.0, -30.0, True),   # Atlántico medio -> agua
        (38.5, -98.0, False),  # centro de Kansas -> tierra
        (19.4, -99.1, False),  # Ciudad de México (interior) -> tierra
    ],
)
def test_en_agua_puntos_conocidos_si_hay_libreria(lat, lon, esperado):
    pytest.importorskip("global_land_mask")
    assert en_agua(lat, lon) is esperado


# --- Clasificación por celda SIN pandas_udf (ADR-010 §8, enfoque materializado) ----

def test_clasificar_celdas_en_agua_vectorizado_si_hay_libreria():
    """``clasificar_celdas_en_agua`` (sin Spark, sin pandas_udf) clasifica un lote de
    celdas distintas: agua donde corresponde y tierra donde corresponde. Es el núcleo
    del camino que materializó ``en_agua`` sobre la plata (celdas distintas, no filas)."""
    pytest.importorskip("global_land_mask")
    pytest.importorskip("numpy")
    from oceanwatch.quality.materializar import clasificar_celdas_en_agua

    celdas = [
        (0.0, -30.0),    # Atlántico medio -> agua
        (36.0, -122.6),  # Pacífico frente a Monterey -> agua
        (38.5, -98.0),   # centro de Kansas -> tierra
        (48.9, 2.3),     # París -> tierra
    ]
    res = clasificar_celdas_en_agua(celdas)
    # Preserva orden, lat/lon y añade el booleano en_agua.
    assert [(r[0], r[1]) for r in res] == celdas
    assert [r[2] for r in res] == [True, True, False, False]


def test_clasificar_celdas_en_agua_lista_vacia():
    """Entrada vacía -> salida vacía, sin tocar la librería."""
    from oceanwatch.quality.materializar import clasificar_celdas_en_agua

    assert clasificar_celdas_en_agua([]) == []


def test_hidrografia_recupera_canales_y_lagos_sin_admitir_tierra_lejana():
    pytest.importorskip("shapely")
    np = pytest.importorskip("numpy")
    from oceanwatch.quality.hidrografia import agregar_evidencia_hidrografica

    # Houston Ship Channel, Salmon Bay, tierra en Kansas y lago Michigan.
    lat = np.array([29.77, 47.66, 38.5, 43.0])
    lon = np.array([-95.10, -122.38, -98.0, -87.0])
    agua = np.zeros(len(lat), dtype=bool)
    agregar_evidencia_hidrografica(agua, lat, lon, margen=0.02)
    assert agua.tolist() == [True, True, False, True]
