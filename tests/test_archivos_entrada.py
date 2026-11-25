import csv
import zipfile

import pytest

from oceanwatch.ingest.entradas import extraer_csv, fecha_archivo, normalizar_noaa_csv2, validar_csv


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


def test_csv2_nombre_original_y_coordenadas(tmp_path):
    from pyzstd import compress

    origen = tmp_path / "ais-2026-06-29.csv.zst"
    destino = tmp_path / "ais-2026-06-29.csv"
    columnas = ["MMSI", "BaseDateTime", "LAT", "LON", "VesselName", "TransceiverClass"]
    origen.write_bytes(compress(
        b'mmsi,base_date_time,longitude,latitude,vessel_name,transceiver,extra\r\n'
        b'366000001,2026-06-29 05:00:00,-87.1,28.7,"NAME, TWO",A,ignored\r\n'
    ))
    assert fecha_archivo(origen.name) == "2026-06-29"
    normalizar_noaa_csv2(origen, destino, columnas)
    validar_csv(destino, columnas)
    with destino.open(newline="", encoding="utf-8") as salida:
        assert list(csv.reader(salida)) == [
            columnas, ["366000001", "2026-06-29T05:00:00", "28.7", "-87.1", "NAME, TWO", "A"],
        ]


@pytest.mark.parametrize("defecto", ["truncado", "encabezado", "fila_incompleta"])
def test_csv2_invalido_se_rechaza(tmp_path, defecto):
    from pyzstd import compress

    origen = tmp_path / "ais-2026-06-29.csv.zst"
    contenido = {
        "truncado": b"mmsi,base_date_time,latitude\n1,2026-06-29 00:00:00,27\n",
        "encabezado": b"mmsi,mmsi\n1,2\n",
        "fila_incompleta": b"mmsi,base_date_time,latitude\n1\n",
    }[defecto]
    archivo = compress(contenido)
    origen.write_bytes(archivo[:-1] if defecto == "truncado" else archivo)
    error = EOFError if defecto == "truncado" else ValueError
    with pytest.raises(error):
        normalizar_noaa_csv2(origen, tmp_path / "staging.csv", ["MMSI", "BaseDateTime", "LAT"])
