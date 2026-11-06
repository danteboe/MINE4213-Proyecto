"""Tests de las reglas de calidad escalares (funciones puras, sin Spark).

Umbrales y conteos reales citados: evidence/req02-capas-calidad/diagnostico_calidad_bronce.md.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from oceanwatch.quality.reglas import (
    cog_es_centinela,
    cog_valido,
    coordenada_no_centinela,
    coordenadas_en_rango,
    dimension_fisica,
    es_centinela_coordenada,
    heading_es_centinela,
    heading_valido,
    sog_es_centinela,
    sog_plausible,
    timestamp_valido_no_futuro,
)


# --- Coordenadas ---------------------------------------------------------

@pytest.mark.parametrize(
    "lat,lon,esperado",
    [
        (40.0, -74.0, True),      # dentro de rango (Nueva York aprox.)
        (-90.0, -180.0, True),    # límite inferior inclusivo
        (90.0, 180.0, True),      # límite superior inclusivo
        (90.1, 0.0, False),       # LAT fuera de rango
        (0.0, 180.5, False),      # LON fuera de rango
        (None, 0.0, False),       # None => no válido
        ("x", 0.0, False),        # no numérico => no válido
    ],
)
def test_coordenadas_en_rango(cfg, lat, lon, esperado):
    assert coordenadas_en_rango(lat, lon, cfg) is esperado


def test_centinela_coordenada_lat91_lon181(cfg):
    assert es_centinela_coordenada(91.0, 0.0, cfg) is True
    assert es_centinela_coordenada(0.0, 181.0, cfg) is True
    assert es_centinela_coordenada(40.0, -74.0, cfg) is False


def test_coordenada_no_centinela_combina_rango_y_centinela(cfg):
    assert coordenada_no_centinela(40.0, -74.0, cfg) is True
    # En rango sintáctico pero centinela => no válida.
    assert coordenada_no_centinela(91.0, 0.0, cfg) is False


# --- SOG -----------------------------------------------------------------

def test_sog_centinela_vs_plausible(cfg):
    # 102.3 es el centinela "no disponible": ES centinela y NO plausible.
    assert sog_es_centinela(102.3, cfg) is True
    assert sog_plausible(102.3, cfg) is False
    # Velocidad real moderada: plausible y no centinela.
    assert sog_plausible(12.0, cfg) is True
    assert sog_es_centinela(12.0, cfg) is False
    # Buque rápido real (entre P99.9=32.5 y max_fisico=80) sigue siendo plausible.
    assert sog_plausible(45.0, cfg) is True
    # Por encima del máximo físico pero bajo el centinela: no plausible.
    assert sog_plausible(90.0, cfg) is False
    assert sog_plausible(None, cfg) is False


# --- Heading / COG -------------------------------------------------------

def test_heading_511_centinela(cfg):
    assert heading_es_centinela(511.0, cfg) is True
    assert heading_valido(511.0, cfg) is False
    assert heading_valido(180.0, cfg) is True
    assert heading_valido(0.0, cfg) is True
    assert heading_valido(359.0, cfg) is True
    assert heading_valido(360.0, cfg) is False
    assert heading_valido(None, cfg) is False


def test_cog_360_centinela(cfg):
    assert cog_es_centinela(360.0, cfg) is True
    assert cog_valido(360.0, cfg) is False
    assert cog_valido(0.0, cfg) is True
    assert cog_valido(359.0, cfg) is True
    assert cog_valido(None, cfg) is False


# --- Timestamp -----------------------------------------------------------

def test_timestamp_no_futuro():
    ahora = datetime(2023, 6, 3, 23, 59, 59)
    assert timestamp_valido_no_futuro(datetime(2023, 6, 1, 0, 0, 0), ahora) is True
    assert timestamp_valido_no_futuro(ahora, ahora) is True
    # Futuro => rechazado.
    assert timestamp_valido_no_futuro(ahora + timedelta(hours=1), ahora) is False
    # None / tipo incorrecto => rechazado.
    assert timestamp_valido_no_futuro(None, ahora) is False
    assert timestamp_valido_no_futuro("2023-06-01", ahora) is False


# --- Dimensiones ---------------------------------------------------------

@pytest.mark.parametrize(
    "valor,esperado",
    [(100.0, True), (0.1, True), (0.0, False), (-5.0, False), (None, False)],
)
def test_dimension_fisica(valor, esperado):
    assert dimension_fisica(valor) is esperado
