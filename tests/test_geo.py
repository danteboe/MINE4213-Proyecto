"""Tests de las funciones geoespaciales puras (haversine y grilla), sin Spark."""

from __future__ import annotations

import pytest

from oceanwatch.quality.geo import celda_grilla, haversine_km


# --- Haversine -----------------------------------------------------------

def test_haversine_mismo_punto_es_cero():
    assert haversine_km(40.0, -74.0, 40.0, -74.0) == pytest.approx(0.0, abs=1e-9)


def test_haversine_un_grado_longitud_en_ecuador():
    # Un grado de longitud sobre el ecuador ~ 111.19 km (distancia tabulada).
    d = haversine_km(0.0, 0.0, 0.0, 1.0)
    assert d == pytest.approx(111.19, abs=0.5)


def test_haversine_puertos_conocidos_los_angeles_nueva_york():
    # Aeropuertos LAX (33.9425,-118.4081) y JFK (40.6413,-73.7781): ~3983 km
    # (distancia great-circle tabulada). Tolerancia de 20 km.
    d = haversine_km(33.9425, -118.4081, 40.6413, -73.7781)
    assert d == pytest.approx(3983.0, abs=20.0)


def test_haversine_es_simetrica():
    d1 = haversine_km(10.0, 20.0, 30.0, 40.0)
    d2 = haversine_km(30.0, 40.0, 10.0, 20.0)
    assert d1 == pytest.approx(d2, abs=1e-9)


# --- Celda de grilla -----------------------------------------------------

def test_celda_grilla_redondea_a_2_decimales_por_defecto(cfg):
    # decimales desde cfg['gold']['decimales_grilla'] = 2.
    assert celda_grilla(40.123456, -74.987654, cfg=cfg) == (40.12, -74.99)


def test_celda_grilla_decimales_explicitos():
    assert celda_grilla(40.123456, -74.987654, decimales=1) == (40.1, -75.0)


def test_celda_grilla_como_cadena():
    assert celda_grilla(40.126, -74.984, decimales=2, como_cadena=True) == "40.13,-74.98"


def test_celda_grilla_default_sin_cfg_usa_2_decimales():
    assert celda_grilla(1.239, 2.001) == (1.24, 2.0)
