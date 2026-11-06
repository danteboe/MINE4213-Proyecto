"""Tests del esquema AIS y columnas de auditoría (funciones puras)."""

from __future__ import annotations

import pytest

from oceanwatch.ingest.schema import columnas_ais, columnas_auditoria

_ESPERADO_17 = [
    "MMSI", "BaseDateTime", "LAT", "LON", "SOG", "COG", "Heading",
    "VesselName", "IMO", "CallSign", "VesselType", "Status",
    "Length", "Width", "Draft", "Cargo", "TransceiverClass",
]


def test_columnas_ais_17_en_orden(cfg):
    assert columnas_ais(cfg) == _ESPERADO_17


def test_columnas_auditoria():
    assert columnas_auditoria() == [
        "_ingest_timestamp", "_source_file", "_batch_date", "_rescued_data",
    ]


def test_ais_struct_type_status_es_integer(cfg):
    pyspark = pytest.importorskip("pyspark")  # noqa: F841
    from pyspark.sql.types import IntegerType, LongType, TimestampType

    from oceanwatch.ingest.schema import ais_struct_type

    st = ais_struct_type(cfg)
    campos = {f.name: f.dataType for f in st.fields}

    # 17 campos, en el orden exacto del header real.
    assert [f.name for f in st.fields] == _ESPERADO_17
    # Status corregido a entero (muestra real: verificacion_esquema_real.md).
    assert isinstance(campos["Status"], IntegerType)
    assert isinstance(campos["MMSI"], LongType)
    assert isinstance(campos["BaseDateTime"], TimestampType)
