"""Convención de nombres y validación del archivo que se publica para Auto Loader."""

import csv
import re
import shutil
import zipfile
from pathlib import Path

from oceanwatch.ingest.paths import _validar_fecha


def fecha_archivo(nombre: str) -> str:
    coincidencia = re.fullmatch(
        r"AIS_(\d{4}_\d{2}_\d{2})(?:_[A-Za-z0-9-]{1,32})?\.(?:csv|zip)", nombre
    )
    if not coincidencia:
        raise ValueError(f"Nombre AIS inválido: {nombre}. Use AIS_AAAA_MM_DD.csv o .zip")
    return _validar_fecha(coincidencia[1].replace("_", "-"))


def extraer_csv(source: Path, destino: Path) -> None:
    """Lee un solo CSV hasta EOF para validar su CRC sin extraer rutas del ZIP."""
    with zipfile.ZipFile(source) as archive:
        miembros = [i for i in archive.infolist() if not i.is_dir() and i.filename.lower().endswith(".csv")]
        if len(miembros) != 1 or miembros[0].file_size == 0:
            raise ValueError(f"Se esperaba un solo CSV no vacío en {source.name}")
        with archive.open(miembros[0]) as entrada, destino.open("wb") as salida:
            shutil.copyfileobj(entrada, salida, 1 << 20)


def validar_csv(ruta: Path, columnas: list[str]) -> None:
    with ruta.open(encoding="utf-8-sig", newline="") as archivo:
        filas = csv.reader(archivo)
        if next(filas, [])[:len(columnas)] != columnas:
            raise ValueError(f"El encabezado AIS de {ruta.name} no coincide con el esquema y su orden")
        if not next(filas, []):
            raise ValueError(f"{ruta.name} debe contener al menos una fila AIS")
