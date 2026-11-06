"""Tests de deduplicación: clave y cálculo de duplicados (con un DataFrame simulado).

``contar_duplicados`` solo usa ``df.count``, ``df.select(...).distinct().count``,
así que se prueba con un doble ligero que imita esa interfaz, sin Spark.
"""

from __future__ import annotations

from oceanwatch.ingest.dedup import contar_duplicados
from oceanwatch.ingest.paths import clave_dedup


class _FakeDistinct:
    def __init__(self, filas):
        self._filas = filas

    def count(self):
        return len({tuple(f) for f in self._filas})


class _FakeSelected:
    def __init__(self, filas):
        self._filas = filas

    def distinct(self):
        return _FakeDistinct(self._filas)


class _FakeDataFrame:
    """Doble de DataFrame: filas como dicts; soporta count y select(clave)."""

    def __init__(self, filas):
        self._filas = filas

    def count(self):
        return len(self._filas)

    def select(self, *cols):
        proyectadas = [[f[c] for c in cols] for f in self._filas]
        return _FakeSelected(proyectadas)


def test_clave_dedup(cfg):
    assert clave_dedup(cfg) == ["MMSI", "BaseDateTime"]


def test_contar_duplicados(cfg):
    filas = [
        {"MMSI": 1, "BaseDateTime": "t1"},
        {"MMSI": 1, "BaseDateTime": "t1"},  # duplicado exacto por clave
        {"MMSI": 2, "BaseDateTime": "t1"},
        {"MMSI": 2, "BaseDateTime": "t2"},
    ]
    df = _FakeDataFrame(filas)
    res = contar_duplicados(df, cfg)
    assert res == {"total": 4, "distintos_por_clave": 3, "duplicados": 1}


def test_contar_duplicados_sin_duplicados(cfg):
    filas = [
        {"MMSI": 1, "BaseDateTime": "t1"},
        {"MMSI": 2, "BaseDateTime": "t2"},
    ]
    res = contar_duplicados(_FakeDataFrame(filas), cfg)
    assert res["duplicados"] == 0
