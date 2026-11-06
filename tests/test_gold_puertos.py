"""Tests de la decisión pura 'en puerto' por celda de grilla (ADR-012), sin Spark.

La regla geoespacial de 'en puerto' de la capa oro es: una posición AIS está en
puerto si comparte celda de grilla (lat/lon redondeadas a ``decimales_grilla``=2,
ADR-001) con el puerto del WPI. Se verifica ``oceanwatch.gold.en_misma_celda``,
que reutiliza ``oceanwatch.quality.geo.celda_grilla``.
"""

from __future__ import annotations

from oceanwatch.gold import en_misma_celda


def test_en_misma_celda_coincide_a_2_decimales(cfg):
    # 40.123 y 40.124 redondean ambos a 40.12; -74.987 y -74.985 a -74.99/-74.98.
    # Se eligen puntos que comparten celda a 2 decimales.
    assert en_misma_celda(40.121, -74.991, 40.124, -74.993, cfg=cfg) is True


def test_en_distinta_celda_a_2_decimales(cfg):
    # Diferencia > 0.01 en latitud: celdas distintas.
    assert en_misma_celda(40.12, -74.99, 40.15, -74.99, cfg=cfg) is False


def test_en_misma_celda_mismo_punto():
    assert en_misma_celda(10.0, 20.0, 10.0, 20.0, decimales=2) is True


def test_en_misma_celda_decimales_explicitos():
    # A 1 decimal 40.12 y 40.17 redondean a 40.1 y 40.2 -> distintas.
    assert en_misma_celda(40.12, -74.99, 40.17, -74.99, decimales=1) is False
    # A 0 decimales ambos redondean a 40 / -75 -> misma celda.
    assert en_misma_celda(40.12, -74.99, 40.17, -74.99, decimales=0) is True
