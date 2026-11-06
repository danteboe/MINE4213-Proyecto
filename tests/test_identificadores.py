"""Tests de las reglas de identificadores de buque (funciones puras, sin Spark).

Conteos reales: evidence/req02-capas-calidad/diagnostico_calidad_bronce.md
(MMSI no 9 dígitos=21.960; IMO vacío=11.015.974 => vacío es VÁLIDO).

IMO de ejemplo: 9074729 (buque real; dígito verificador correcto 9). Su variante
con un dígito alterado (9074720) debe rechazarse.
"""

from __future__ import annotations

import pytest

from oceanwatch.quality.identificadores import (
    callsign_razonable,
    imo_valido,
    mmsi_bien_formado,
    mmsi_mid_valido,
    vessel_name_razonable,
)


# --- MMSI ----------------------------------------------------------------

@pytest.mark.parametrize(
    "mmsi,esperado",
    [
        (367000000, True),    # 9 dígitos (int)
        ("367000000", True),  # 9 dígitos (str)
        (36700000, False),    # 8 dígitos
        (3670000000, False),  # 10 dígitos
        (None, False),        # nulo
        ("36700a000", False), # no numérico
    ],
)
def test_mmsi_bien_formado(cfg, mmsi, esperado):
    assert mmsi_bien_formado(mmsi, cfg) is esperado


def test_mmsi_mid_valido_ejemplo_real(cfg):
    # MID 367 = Estados Unidos (dentro del rango ITU [201,775]) => válido.
    assert mmsi_mid_valido(367000000, cfg) is True
    # MID 100 (< 201) no corresponde a ningún país marítimo => inválido.
    assert mmsi_mid_valido(100000000, cfg) is False
    # Mal formado (8 dígitos) no puede tener MID válido.
    assert mmsi_mid_valido(36700000, cfg) is False


# --- IMO -----------------------------------------------------------------

def test_imo_vacio_es_valido(cfg):
    assert imo_valido("", cfg) is True
    assert imo_valido(None, cfg) is True
    assert imo_valido("   ", cfg) is True


def test_imo_check_digit_correcto_y_alterado(cfg):
    # 9074729: 9*7+0*6+7*5+4*4+7*3+2*2 = 139 -> último dígito 9 == 7.º dígito.
    assert imo_valido("9074729", cfg) is True
    assert imo_valido("IMO9074729", cfg) is True
    # Dígito verificador alterado (9 -> 0): inválido.
    assert imo_valido("9074720", cfg) is False
    assert imo_valido("IMO9074720", cfg) is False
    # Formas inesperadas: inválidas.
    assert imo_valido("ABC123", cfg) is False
    assert imo_valido("907472", cfg) is False  # solo 6 dígitos


# --- CallSign / VesselName ----------------------------------------------

def test_callsign_razonable():
    assert callsign_razonable("WDL1234") is True
    assert callsign_razonable("wdl1234") is True   # se normaliza a mayúsculas
    assert callsign_razonable("") is False
    assert callsign_razonable(None) is False
    assert callsign_razonable("A B!") is False


def test_vessel_name_razonable():
    assert vessel_name_razonable("EVER GIVEN") is True
    assert vessel_name_razonable("  ") is False
    assert vessel_name_razonable(None) is False
    assert vessel_name_razonable("X" * 101) is False
