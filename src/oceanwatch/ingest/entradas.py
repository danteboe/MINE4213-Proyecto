"""Convención de nombres y validación del archivo que se publica para Auto Loader."""

import csv
import io
import re
import shutil
import zipfile
from pathlib import Path

from oceanwatch.ingest.paths import _validar_fecha


_NOAA_CSV2_POR_COLUMNA_AIS = {
    "BaseDateTime": "base_date_time",
    "LAT": "latitude",
    "LON": "longitude",
    "VesselName": "vessel_name",
    "CallSign": "call_sign",
    "VesselType": "vessel_type",
    "TransceiverClass": "transceiver",
}


def fecha_archivo(nombre: str) -> str:
    coincidencia = re.fullmatch(
        r"ais-([0-9]{4}-[0-9]{2}-[0-9]{2})\.csv\.zst", nombre
    ) or re.fullmatch(
        r"AIS_([0-9]{4}_[0-9]{2}_[0-9]{2})(?:_[A-Za-z0-9-]{1,32})?\.(?:csv|zip)", nombre
    )
    if not coincidencia:
        raise ValueError(
            f"Nombre AIS inválido: {nombre}. Use ais-AAAA-MM-DD.csv.zst, AIS_AAAA_MM_DD.csv o .zip"
        )
    return _validar_fecha(coincidencia[1].replace("_", "-"))


def normalizar_noaa_csv2(origen: Path, destino: Path, columnas: list[str]) -> None:
    """Proyecta NOAA CSV2 al contrato de bronce con memoria acotada."""
    from pyzstd import ZstdFile

    esperadas = [_NOAA_CSV2_POR_COLUMNA_AIS.get(c, c.lower()) for c in columnas]
    with (
        ZstdFile(origen) as comprimido,
        io.TextIOWrapper(comprimido, encoding="utf-8-sig", newline="") as entrada,
        destino.open("w", encoding="utf-8", newline="") as salida,
    ):
        filas = csv.reader(entrada, strict=True)
        encabezado = next(filas, [])
        if len(set(encabezado)) != len(encabezado) or not set(esperadas).issubset(encabezado):
            raise ValueError(f"El encabezado NOAA CSV2 de {origen.name} no coincide con el esquema AIS")
        indices = [encabezado.index(columna) for columna in esperadas]
        posicion_utc = columnas.index("BaseDateTime")
        escritor = csv.writer(salida, lineterminator="\n")
        escritor.writerow(columnas)
        for numero, fila in enumerate(filas, start=2):
            if len(fila) != len(encabezado):
                raise ValueError(f"Fila {numero} de {origen.name} con longitud distinta al encabezado")
            valores = [fila[i] for i in indices]
            valores[posicion_utc] = valores[posicion_utc].replace(" ", "T", 1)
            escritor.writerow(valores)


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
