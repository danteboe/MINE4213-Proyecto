"""Esquema AIS explícito y columnas de auditoría de la capa bronce.

Funciones puras y testeables fuera de Spark. El único punto que necesita PySpark
(``ais_struct_type``) reutiliza la implementación de ``oceanwatch.common.config``,
cuyo import de ``pyspark.sql.types`` es perezoso (ver ADR-003).
"""

from __future__ import annotations

from typing import Any

from oceanwatch.common.config import ais_struct_type as _ais_struct_type


def ais_struct_type(cfg: dict[str, Any]):
    """Construye el ``StructType`` del esquema AIS explícito (17 campos).

    Envuelve ``oceanwatch.common.config.ais_struct_type`` para que la ingesta
    tenga un único punto de acceso al esquema. El import de PySpark sigue siendo
    perezoso (ocurre dentro de la función envuelta).
    """
    return _ais_struct_type(cfg)


def columnas_ais(cfg: dict[str, Any]) -> list[str]:
    """Lista ordenada de los 17 nombres de columna del esquema AIS.

    El orden proviene de ``conf/config.yaml`` (``esquema_ais``) y debe coincidir
    con el header real de los CSV NOAA (ver verificacion_esquema_real.md).
    """
    return [str(col["nombre"]) for col in cfg["esquema_ais"]]


def columnas_auditoria() -> list[str]:
    """Columnas de auditoría que la capa bronce añade a cada fila (ADR-006).

    - ``_ingest_timestamp``: instante de ingesta (``current_timestamp``).
    - ``_source_file``: ruta del archivo de origen (``_metadata.file_path``).
    - ``_batch_date``: fecha del lote derivada de la ruta ``date=YYYY-MM-DD``.
    - ``_rescued_data``: columna de datos rescatados de Auto Loader.
    """
    return ["_ingest_timestamp", "_source_file", "_batch_date", "_rescued_data"]
