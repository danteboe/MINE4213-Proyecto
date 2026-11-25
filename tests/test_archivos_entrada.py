import zipfile

import pytest

from oceanwatch.ingest.entradas import extraer_csv, fecha_archivo, validar_csv


@pytest.mark.parametrize("nombre", ["AIS_2023_06_09.zip", "AIS_2023_06_09_parte-2.csv"])
def test_fecha_del_archivo(nombre):
    assert fecha_archivo(nombre) == "2023-06-09"


@pytest.mark.parametrize("nombre", ["AIS_2023_02_30.zip", "../AIS_2023_06_09.csv", "otro.csv"])
def test_archivo_rechaza_fecha_o_nombre_invalidos(nombre):
    with pytest.raises(ValueError):
        fecha_archivo(nombre)


def test_zip_usa_solo_el_csv_y_valida_el_encabezado(tmp_path):
    origen = tmp_path / "AIS_2023_06_09.zip"
    destino = tmp_path / "normalizado.csv"
    with zipfile.ZipFile(origen, "w") as archivo:
        archivo.writestr("../original.csv", "MMSI,BaseDateTime,extra\n1,2023-06-09T00:00:00,nuevo\n")
    extraer_csv(origen, destino)
    assert not (tmp_path / "original.csv").exists()
    validar_csv(destino, ["MMSI", "BaseDateTime"])
    with pytest.raises(ValueError, match="encabezado"):
        validar_csv(destino, ["BaseDateTime", "MMSI"])


def test_zip_con_varios_csv_se_rechaza(tmp_path):
    origen = tmp_path / "AIS_2023_06_09.zip"
    with zipfile.ZipFile(origen, "w") as archivo:
        archivo.writestr("a.csv", "a\n1\n")
        archivo.writestr("b.csv", "a\n2\n")
    with pytest.raises(ValueError, match="un solo CSV"):
        extraer_csv(origen, tmp_path / "normalizado.csv")
