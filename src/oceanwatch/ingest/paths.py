"""Rutas y nombres derivados para la ingesta (funciones puras, sin PySpark).

Todas las rutas de landing/checkpoints se resuelven bajo los volúmenes de Unity
Catalog ``/Volumes/<catalogo>/<bronze>/<volumen>/...``. Las funciones son puras:
reciben la configuración y una fecha ISO, y devuelven cadenas; son testeables sin
red ni Spark.
"""

from __future__ import annotations

import re
from datetime import date
from typing import Any

from oceanwatch.common.config import catalog_name, schema

# YYYY-MM-DD estricto (se valida además que sea una fecha de calendario real).
_FORMATO_FECHA = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _validar_fecha(fecha_iso: str) -> str:
    """Valida que ``fecha_iso`` tenga formato ``YYYY-MM-DD`` y sea real.

    Raises:
        ValueError: si el formato no es ``YYYY-MM-DD`` o no es una fecha válida.
    """
    if not isinstance(fecha_iso, str) or not _FORMATO_FECHA.match(fecha_iso):
        raise ValueError(
            f"Fecha inválida '{fecha_iso}': se espera el formato YYYY-MM-DD."
        )
    try:
        date.fromisoformat(fecha_iso)
    except ValueError as exc:
        raise ValueError(
            f"Fecha inválida '{fecha_iso}': no es una fecha de calendario válida."
        ) from exc
    return fecha_iso


def _volumen_landing_base(cfg: dict[str, Any]) -> str:
    """Ruta base del volumen de landing: ``/Volumes/<cat>/<bronze>/<landing>``."""
    if "ruta_volumen" in cfg:
        return f"{cfg['ruta_volumen']}/landing"
    catalogo = catalog_name(cfg)
    bronze = schema(cfg, "bronze")
    landing = cfg["volumenes"]["landing"]
    return f"/Volumes/{catalogo}/{bronze}/{landing}"


def fecha_a_subruta(fecha_iso: str) -> str:
    """Convierte ``'2023-06-01'`` en la partición ``'date=2023-06-01'``."""
    return f"date={_validar_fecha(fecha_iso)}"


def landing_csv_dir(cfg: dict[str, Any], fecha_iso: str) -> str:
    """Directorio de CSV descomprimidos para una fecha.

    ``/Volumes/<cat>/<bronze>/<landing>/<csv>/date=YYYY-MM-DD``
    """
    subruta_csv = cfg["ingesta"]["subruta_csv"]
    return f"{_volumen_landing_base(cfg)}/{subruta_csv}/{fecha_a_subruta(fecha_iso)}"


def landing_csv_root(cfg: dict[str, Any]) -> str:
    """Raíz recursiva de CSV (todas las particiones ``date=``) para Auto Loader."""
    subruta_csv = cfg["ingesta"]["subruta_csv"]
    return f"{_volumen_landing_base(cfg)}/{subruta_csv}"


def nombre_zip_noaa(fecha_iso: str) -> str:
    """Nombre del zip NOAA para una fecha: ``'AIS_2023_06_01.zip'``.

    El patrón NOAA usa guiones bajos en la fecha (``YYYY_MM_DD``).
    """
    fecha_guion_bajo = _validar_fecha(fecha_iso).replace("-", "_")
    return f"AIS_{fecha_guion_bajo}.zip"


def landing_zip_path(cfg: dict[str, Any], fecha_iso: str) -> str:
    """Ruta absoluta del zip descargado en landing.

    ``/Volumes/<cat>/<bronze>/<landing>/<zips>/AIS_YYYY_MM_DD.zip``
    """
    subruta_zips = cfg["ingesta"]["subruta_zips"]
    return f"{_volumen_landing_base(cfg)}/{subruta_zips}/{nombre_zip_noaa(fecha_iso)}"


def url_zip_noaa(cfg: dict[str, Any], fecha_iso: str) -> str:
    """URL de descarga del zip diario de NOAA para una fecha.

    Construida desde ``fuente_noaa`` (``base_url`` + ``patron_archivo``), donde
    ``{fecha}`` se reemplaza por ``YYYY_MM_DD``.
    """
    fuente = cfg["fuente_noaa"]
    base_url = str(fuente["base_url"]).rstrip("/")
    if "ruta_volumen" in cfg:
        base_url = f"{base_url.rsplit('/', 1)[0]}/{_validar_fecha(fecha_iso)[:4]}"
    fecha_guion_bajo = _validar_fecha(fecha_iso).replace("-", "_")
    nombre = str(fuente["patron_archivo"]).format(fecha=fecha_guion_bajo)
    return f"{base_url}/{nombre}"


def clave_dedup(cfg: dict[str, Any]) -> list[str]:
    """Clave natural de deduplicación (p. ej. ``['MMSI', 'BaseDateTime']``)."""
    return [str(c) for c in cfg["ingesta"]["clave_dedup"]]
