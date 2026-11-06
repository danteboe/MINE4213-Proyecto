"""Tests de las funciones puras de ingeniería de características de ML (sin Spark).

Validan el contrato del vector de características de la detección de anomalías de SOG
(ADR-005, D1). Las funciones escalares no tocan Spark; ``estadisticas_por_tipo`` sí,
por lo que su test se salta si PySpark no está instalado (``importorskip``).
"""

from __future__ import annotations

import math

import pytest

from oceanwatch.ml.features import (
    clasificar_por_umbral_z,
    construir_vector_features,
    delta_sog,
    hora_a_ciclico,
    z_sog_por_tipo,
)


# --- z_sog_por_tipo ------------------------------------------------------

# Estadísticos de prueba: tipo 70 (carga) media 6.0 std 2.0; tipo 80 std 0.
STATS = {70: (6.0, 2.0), 80: (5.0, 0.0)}


def test_z_sog_positivo():
    # SOG 10 con media 6 y std 2 => (10-6)/2 = 2.0 (más rápido de lo típico).
    assert z_sog_por_tipo(10.0, 70, STATS) == pytest.approx(2.0)


def test_z_sog_negativo():
    # SOG 2 con media 6 y std 2 => (2-6)/2 = -2.0 (más lento de lo típico).
    assert z_sog_por_tipo(2.0, 70, STATS) == pytest.approx(-2.0)


def test_z_sog_cero_en_la_media():
    assert z_sog_por_tipo(6.0, 70, STATS) == pytest.approx(0.0)


def test_z_sog_tipo_ausente_devuelve_cero():
    # Tipo no visto en el train => sin base para considerarlo anómalo.
    assert z_sog_por_tipo(100.0, 999, STATS) == 0.0
    assert z_sog_por_tipo(100.0, None, STATS) == 0.0


def test_z_sog_std_cero_usa_std_minimo():
    # Tipo 80 tiene std 0: debe usar std_minimo (1e-6) y no dividir por cero.
    z = z_sog_por_tipo(5.5, 80, STATS)
    assert math.isfinite(z)
    assert z == pytest.approx(0.5 / 1e-6)


# --- delta_sog -----------------------------------------------------------

def test_delta_sog_con_previo_signo_correcto():
    assert delta_sog(12.0, 5.0) == pytest.approx(7.0)   # aceleración
    assert delta_sog(3.0, 9.0) == pytest.approx(-6.0)   # frenada


def test_delta_sog_sin_previo_es_cero():
    assert delta_sog(8.0, None) == 0.0


# --- hora_a_ciclico ------------------------------------------------------

def test_hora_a_ciclico_valores_en_rango():
    for h in range(24):
        s, c = hora_a_ciclico(h)
        assert -1.0 <= s <= 1.0
        assert -1.0 <= c <= 1.0


def test_hora_a_ciclico_0_y_23_contiguas_en_coseno():
    # 23:00 y 00:00 son contiguas en el tiempo: su coseno debe ser casi idéntico.
    _, c0 = hora_a_ciclico(0)
    _, c23 = hora_a_ciclico(23)
    assert c0 == pytest.approx(1.0)
    assert c23 == pytest.approx(math.cos(2.0 * math.pi * 23.0 / 24.0))
    assert abs(c0 - c23) < 0.05


def test_hora_a_ciclico_6_y_18_opuestas():
    # 06:00 y 18:00 están en lados opuestos del círculo de 24h => senos opuestos.
    s6, c6 = hora_a_ciclico(6)
    s18, c18 = hora_a_ciclico(18)
    assert s6 == pytest.approx(-s18)
    assert c6 == pytest.approx(c18, abs=1e-9)


# --- construir_vector_features ------------------------------------------

def test_construir_vector_features_orden():
    # Orden FIJO del contrato del modelo: [z_sog, delta_sog, hora_sin, hora_cos].
    vec = construir_vector_features(
        sog=10.0, vessel_type=70, sog_previo=5.0, hora=0, stats=STATS
    )
    assert len(vec) == 4
    z, d, hora_sin, hora_cos = vec
    assert z == pytest.approx(2.0)             # (10-6)/2
    assert d == pytest.approx(5.0)             # 10-5
    assert hora_sin == pytest.approx(0.0)      # sin(0)
    assert hora_cos == pytest.approx(1.0)      # cos(0)


# --- clasificar_por_umbral_z --------------------------------------------

def test_clasificar_por_umbral_z():
    assert clasificar_por_umbral_z(4.0) is True     # |z|>3 => anomalía
    assert clasificar_por_umbral_z(-4.0) is True
    assert clasificar_por_umbral_z(1.5) is False    # |z|<=3 => normal
    assert clasificar_por_umbral_z(3.0) is False    # borde exacto: NO anomalía
    assert clasificar_por_umbral_z(-3.0) is False
