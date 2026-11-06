"""Descarga y descompresión de los zips diarios de NOAA (solo stdlib).

Pensado para ejecutarse DENTRO de Databricks (serverless), donde la región
permite descargar los ~344 MB diarios de NOAA. No añade dependencias: usa
``urllib`` y ``zipfile`` de la librería estándar.

Las partes puras (decisión de idempotencia por existencia de archivo, verificación
de un zip) son testeables sin red creando un zip temporal en el propio test.
"""

from __future__ import annotations

import os
import time
import urllib.request
import zipfile


def _content_length(url: str, timeout: int = 60) -> int | None:
    """Tamaño declarado por el servidor (``Content-Length``) vía HEAD, o ``None``.

    Si el servidor no responde el header o falla el HEAD, devuelve ``None`` (la
    verificación de tamaño se omite en ese caso, no se trata como error).
    """
    req = urllib.request.Request(url, method="HEAD")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            valor = resp.headers.get("Content-Length")
            return int(valor) if valor is not None else None
    except Exception:
        return None


def descargar_zip(url: str, destino: str, reintentos: int = 3, timeout: int = 600) -> str:
    """Descarga ``url`` a ``destino`` verificando tamaño, con backoff exponencial.

    Compara los bytes descargados contra el ``Content-Length`` (si el servidor lo
    expone). Reintenta hasta ``reintentos`` veces con espera creciente (1s, 2s, 4s).

    Args:
        url: URL del zip NOAA.
        destino: ruta de salida (en el Volume landing/zips).
        reintentos: número máximo de intentos.
        timeout: timeout por intento, en segundos.

    Returns:
        La ruta ``destino``.

    Raises:
        RuntimeError: si tras todos los reintentos no se obtiene un archivo con el
            tamaño esperado.
    """
    os.makedirs(os.path.dirname(destino), exist_ok=True)
    esperado = _content_length(url, timeout=min(timeout, 60))
    ultimo_error: Exception | None = None
    for intento in range(1, reintentos + 1):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as resp, open(destino, "wb") as out:
                while True:
                    chunk = resp.read(1 << 20)  # 1 MB
                    if not chunk:
                        break
                    out.write(chunk)
            descargado = os.path.getsize(destino)
            if esperado is not None and descargado != esperado:
                raise RuntimeError(
                    f"Descarga incompleta: {descargado} bytes != Content-Length {esperado}."
                )
            return destino
        except Exception as exc:  # reintentable
            ultimo_error = exc
            if intento < reintentos:
                time.sleep(2 ** (intento - 1))
    raise RuntimeError(
        f"No se pudo descargar '{url}' tras {reintentos} intentos: {ultimo_error}"
    )


def verificar_zip(path: str) -> bool:
    """Verifica que ``path`` sea un zip íntegro.

    Usa ``zipfile.is_zipfile`` (firma) y ``testzip`` (CRC de cada miembro).

    Returns:
        ``True`` si el zip es válido e íntegro; ``False`` en caso contrario.
    """
    if not os.path.isfile(path) or not zipfile.is_zipfile(path):
        return False
    try:
        with zipfile.ZipFile(path) as zf:
            # testzip devuelve el nombre del primer miembro corrupto, o None.
            return zf.testzip() is None
    except zipfile.BadZipFile:
        return False


def _csv_destino_existe_no_vacio(destino_dir: str, nombre_csv: str) -> bool:
    """Decisión pura de idempotencia: ¿el CSV destino ya existe y no está vacío?"""
    destino = os.path.join(destino_dir, nombre_csv)
    return os.path.isfile(destino) and os.path.getsize(destino) > 0


def descomprimir_a_landing(zip_path: str, destino_dir: str) -> list[str]:
    """Descomprime los miembros CSV de ``zip_path`` en ``destino_dir`` (idempotente).

    Si el CSV destino ya existe y no está vacío, no se vuelve a extraer (idempotencia
    por existencia). Solo extrae miembros con extensión ``.csv``.

    Args:
        zip_path: ruta del zip verificado.
        destino_dir: directorio destino (en el Volume landing/csv/date=...).

    Returns:
        Lista de rutas de los CSV presentes en el destino tras la operación.
    """
    os.makedirs(destino_dir, exist_ok=True)
    extraidos: list[str] = []
    with zipfile.ZipFile(zip_path) as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            nombre_csv = os.path.basename(info.filename)
            if not nombre_csv.lower().endswith(".csv"):
                continue
            destino = os.path.join(destino_dir, nombre_csv)
            if not _csv_destino_existe_no_vacio(destino_dir, nombre_csv):
                with zf.open(info) as src, open(destino, "wb") as out:
                    while True:
                        chunk = src.read(1 << 20)
                        if not chunk:
                            break
                        out.write(chunk)
            extraidos.append(destino)
    return extraidos
