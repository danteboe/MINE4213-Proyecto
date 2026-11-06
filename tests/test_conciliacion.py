"""Tests del invariante de conciliación (aritmética pura, sin Spark).

Invariante (ADR-006/ADR-011): bronce = plata + cuarentena + duplicados. Una fila con
varias causas cuenta UNA vez en cuarentena; los duplicados exactos se cuentan aparte.
"""

from __future__ import annotations

import pytest

from oceanwatch.quality.conciliacion import verificar_invariante


def test_invariante_cuadra_numeros_simulados():
    res = verificar_invariante(bronce=1000, plata=900, cuarentena=90, duplicados=10)
    assert res["cuadra"] is True
    assert res["suma"] == 1000
    assert res["diferencia"] == 0


def test_invariante_cuadra_corpus_real_simulado():
    # Forma del invariante del corpus: 25.897.493 = plata + cuarentena + 658.
    bronce = 25_897_493
    duplicados = 658
    cuarentena = 21_960  # al menos las filas de MMSI mal formado
    plata = bronce - cuarentena - duplicados
    res = verificar_invariante(bronce, plata, cuarentena, duplicados)
    assert res["cuadra"] is True
    assert res["plata"] + res["cuarentena"] + res["duplicados"] == bronce


def test_invariante_no_cuadra():
    # Falta 1 fila: la suma no llega a bronce => no cuadra, diferencia positiva.
    res = verificar_invariante(bronce=1000, plata=899, cuarentena=90, duplicados=10)
    assert res["cuadra"] is False
    assert res["diferencia"] == 1
    assert "!=" in res["descripcion"]


def test_invariante_rechaza_negativos():
    with pytest.raises(ValueError):
        verificar_invariante(bronce=-1, plata=0, cuarentena=0, duplicados=0)


def test_invariante_rechaza_no_int():
    with pytest.raises(TypeError):
        verificar_invariante(bronce=1000.0, plata=900, cuarentena=90, duplicados=10)
