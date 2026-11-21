"""Carga y acceso tipado a la configuración centralizada (``conf/config.yaml``).

Este módulo NO contiene credenciales. La key de Databricks vive FUERA del repo.
Los imports de PySpark son perezosos (dentro de las funciones que los necesitan)
para que el módulo sea importable y testeable fuera de un entorno Spark.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

import yaml


def _repo_root() -> Path:
    """Devuelve la raíz del repositorio.

    ``config.py`` vive en ``<REPO>/src/oceanwatch/common/``; subir cuatro niveles
    (common -> oceanwatch -> src -> <REPO>) da la raíz del repo.
    """
    return Path(__file__).resolve().parents[3]


def default_config_path() -> Path:
    """Ruta por defecto de ``conf/config.yaml`` relativa a la raíz del repo."""
    return _repo_root() / "conf" / "config.yaml"


def load_config(path: Optional[str] = None) -> dict[str, Any]:
    """Lee y valida mínimamente el YAML de configuración.

    Args:
        path: ruta al YAML. Si es ``None`` se usa ``conf/config.yaml``.

    Returns:
        El contenido del YAML como ``dict``.

    Raises:
        FileNotFoundError: si la ruta no existe.
        ValueError: si el YAML no es un mapeo.
    """
    cfg_path = Path(path) if path else default_config_path()
    if not cfg_path.exists():
        raise FileNotFoundError(f"No se encontró el archivo de configuración: {cfg_path}")
    with cfg_path.open(encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    if not isinstance(data, dict):
        raise ValueError("El archivo de configuración debe ser un mapeo YAML (clave: valor).")
    return data


def catalog_name(cfg: dict[str, Any]) -> str:
    """Nombre del catálogo de Unity Catalog (ver ADR-002)."""
    return str(cfg["catalogo"]["nombre"])


def schema(cfg: dict[str, Any], nombre: str) -> str:
    """Devuelve el nombre físico del esquema por su clave lógica.

    Args:
        cfg: configuración cargada.
        nombre: clave lógica (``bronze``, ``silver``, ``gold``, ``quarantine``,
            ``quality``, ``ml``, ``entrega1``).
    """
    esquemas = cfg["catalogo"]["esquemas"]
    if nombre not in esquemas:
        raise KeyError(f"Esquema desconocido '{nombre}'. Disponibles: {sorted(esquemas)}")
    return str(esquemas[nombre])


def fqn(cfg: dict[str, Any], esquema: str, objeto: str) -> str:
    """Nombre totalmente calificado ``catalogo.esquema.objeto`` (three-part name)."""
    if "namespace" in cfg:
        return f"{cfg['namespace']}.oceanwatch_{esquema}_{objeto}"
    return f"{catalog_name(cfg)}.{schema(cfg, esquema)}.{objeto}"


def load_cluster_config() -> dict[str, Any]:
    """Resuelve el almacenamiento institucional sin alterar los artefactos de la Parte 1."""
    cfg = load_config()
    cfg.update(cfg["institucional"])
    cfg["calidad"]["reglas"].update(cfg["compuertas_adicionales"])
    return cfg


def ais_struct_type(cfg: dict[str, Any]):
    """Construye el ``StructType`` del esquema AIS explícito (17 campos).

    El import de ``pyspark.sql.types`` es perezoso: solo se evalúa al invocar
    esta función, de modo que el módulo pueda importarse fuera de Spark.
    """
    from pyspark.sql.types import (  # import perezoso
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    tipos = {
        "long": LongType(),
        "timestamp": TimestampType(),
        "double": DoubleType(),
        "int": IntegerType(),
        "string": StringType(),
    }
    campos = []
    for col in cfg["esquema_ais"]:
        tipo = tipos.get(col["tipo"])
        if tipo is None:
            raise ValueError(f"Tipo no soportado en esquema_ais: {col['tipo']}")
        campos.append(StructField(col["nombre"], tipo, nullable=True))
    return StructType(campos)
