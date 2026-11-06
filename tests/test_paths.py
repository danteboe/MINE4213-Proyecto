"""Tests de rutas y nombres derivados (funciones puras, sin red ni Spark)."""

from __future__ import annotations

import pytest

from oceanwatch.ingest.paths import (
    clave_dedup,
    fecha_a_subruta,
    landing_csv_dir,
    landing_zip_path,
    nombre_zip_noaa,
    url_zip_noaa,
)


def test_fecha_a_subruta():
    assert fecha_a_subruta("2023-06-01") == "date=2023-06-01"


def test_nombre_zip_noaa():
    assert nombre_zip_noaa("2023-06-03") == "AIS_2023_06_03.zip"


def test_url_zip_noaa(cfg):
    url = url_zip_noaa(cfg, "2023-06-01")
    assert url == (
        "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/AIS_2023_06_01.zip"
    )


def test_landing_csv_dir(cfg):
    ruta = landing_csv_dir(cfg, "2023-06-02")
    assert ruta == "/Volumes/oceanwatch/bronze/landing/csv/date=2023-06-02"


def test_landing_zip_path(cfg):
    ruta = landing_zip_path(cfg, "2023-06-02")
    assert ruta == "/Volumes/oceanwatch/bronze/landing/zips/AIS_2023_06_02.zip"


def test_clave_dedup(cfg):
    assert clave_dedup(cfg) == ["MMSI", "BaseDateTime"]


@pytest.mark.parametrize("mala", ["2023-6-1", "01-06-2023", "2023/06/01", "", "2023-13-01"])
def test_fecha_invalida_lanza_valueerror(mala):
    with pytest.raises(ValueError):
        fecha_a_subruta(mala)
