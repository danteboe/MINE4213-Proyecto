"""Genera el recurso hidrográfico a partir del archivo NWN y los lagos de Natural Earth."""

import argparse
import gzip
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
from zipfile import ZipFile

from pyogrio.raw import read
import shapely


def leer_geometrias(path, layer=None):
    metadatos, _, geometrias, _ = read(path, layer=layer, columns=[], force_2d=True)
    if metadatos["crs"] != "EPSG:4326":
        raise ValueError(f"CRS no esperado: {metadatos['crs']}")
    validas = shapely.make_valid(shapely.from_wkb(geometrias))
    # 0,0005° mantiene el error de la simplificación muy por debajo de la celda de 0,01°.
    return shapely.get_parts(shapely.simplify(validas, 0.0005, preserve_topology=True))


def generar(nwn_zip: Path, lagos_zip: Path, destino: Path):
    with ZipFile(nwn_zip) as archivo:
        nombre = next(n for n in archivo.namelist() if "NTAD2023_GDB" in n and n.endswith(".zip"))
        with ZipFile(BytesIO(archivo.read(nombre))) as instantanea:
            gdb_zip = next(n for n in instantanea.namelist() if n.endswith(".gdb.zip"))
            with TemporaryDirectory() as carpeta, ZipFile(BytesIO(instantanea.read(gdb_zip))) as gdb:
                gdb.extractall(carpeta)
                geodatabase = next(Path(carpeta).rglob("*.gdb"))
                vias = leer_geometrias(geodatabase, "navigable_waterway_network_lines_2023_01_18")
    lagos = leer_geometrias(lagos_zip)

    with destino.open("wb") as output, gzip.GzipFile(filename="", mode="wb", fileobj=output, mtime=0) as gzip_out:
        for geometria in (*vias, *lagos):
            if geometria is not None and not geometria.is_empty:
                wkb_hex = shapely.to_wkb(geometria, hex=True, byte_order=1)
                gzip_out.write(wkb_hex.encode("ascii") + b"\n")
    print(f"{len(vias)} líneas, {len(lagos)} polígonos, {destino.stat().st_size} bytes comprimidos")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("nwn", type=Path)
    parser.add_argument("lagos", type=Path)
    parser.add_argument("destino", type=Path)
    args = parser.parse_args()
    generar(args.nwn, args.lagos, args.destino)
