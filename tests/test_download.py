"""Tests de descarga/descompresión: idempotencia y verificación (sin red).

Se crea un zip temporal en el propio test; no se hace ninguna petición de red.
"""

from __future__ import annotations

import os
import zipfile

from oceanwatch.ingest.download import (
    descomprimir_a_landing,
    verificar_zip,
)


def _crear_zip_valido(path: str, nombre_csv: str = "AIS_2023_06_01.csv", contenido: bytes = b"a,b\n1,2\n") -> None:
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(nombre_csv, contenido)


def test_verificar_zip_valido(tmp_path):
    zpath = os.path.join(tmp_path, "ok.zip")
    _crear_zip_valido(zpath)
    assert verificar_zip(zpath) is True


def test_verificar_zip_invalido(tmp_path):
    # Archivo que no es un zip.
    noz = os.path.join(tmp_path, "no.zip")
    with open(noz, "wb") as f:
        f.write(b"esto no es un zip")
    assert verificar_zip(noz) is False
    # Archivo inexistente.
    assert verificar_zip(os.path.join(tmp_path, "falta.zip")) is False


def test_descomprimir_extrae_csv(tmp_path):
    zpath = os.path.join(tmp_path, "d.zip")
    _crear_zip_valido(zpath, contenido=b"col\nx\n")
    destino = os.path.join(tmp_path, "landing", "date=2023-06-01")
    rutas = descomprimir_a_landing(zpath, destino)
    assert len(rutas) == 1
    assert os.path.isfile(rutas[0])
    with open(rutas[0], "rb") as f:
        assert f.read() == b"col\nx\n"


def test_descomprimir_es_idempotente(tmp_path):
    zpath = os.path.join(tmp_path, "d.zip")
    _crear_zip_valido(zpath, contenido=b"col\nx\n")
    destino = os.path.join(tmp_path, "landing", "date=2023-06-01")

    descomprimir_a_landing(zpath, destino)
    csv_destino = os.path.join(destino, "AIS_2023_06_01.csv")
    mtime_1 = os.path.getmtime(csv_destino)

    # Segunda corrida: no debe reextraer (el CSV ya existe y no está vacío).
    descomprimir_a_landing(zpath, destino)
    mtime_2 = os.path.getmtime(csv_destino)
    assert mtime_1 == mtime_2
