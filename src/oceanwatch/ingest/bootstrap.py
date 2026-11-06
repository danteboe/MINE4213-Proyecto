"""Bootstrap idempotente de Unity Catalog para OceanWatch Analytics.

# PENDIENTE DE EJECUCIÓN — NO ejecutar en Fase 1.

Crea, de forma idempotente (``IF NOT EXISTS``), el catálogo, los esquemas por
capa y los volúmenes de aterrizaje/checkpoints según las convenciones de ADR-002.
Está parametrizado por ``conf/config.yaml`` (ver ``oceanwatch.common.config``) y
no contiene credenciales ni rutas personales.

Diseñado (no ejecutado): requiere una sesión Spark con permisos sobre el
metastore ``metastore_aws_us_east_2`` (us-east-2). La ejecución real pertenece a
Fase 2.

Objetos que crea (convención ADR-002):
- Catálogo: ``oceanwatch``.
- Esquemas: ``bronze``, ``silver``, ``gold``, ``quarantine``, ``quality``, ``ml``.
  NO toca ``oceanwatch.entrega1`` (se preserva intacto de Entrega 1).
- Volúmenes: ``oceanwatch.bronze.landing`` (zips/CSV descomprimidos) y
  ``oceanwatch.bronze.checkpoints`` (checkpoints de Structured Streaming).
"""

from __future__ import annotations

from typing import Any, Optional

from oceanwatch.common.config import catalog_name, load_config, schema

# Esquemas de nueva creación. 'entrega1' se excluye a propósito: se preserva.
_ESQUEMAS_NUEVOS = ["bronze", "silver", "gold", "quarantine", "quality", "ml"]


def create_catalog(spark: Any, cfg: dict[str, Any]) -> None:
    """Crea el catálogo del proyecto si no existe (idempotente)."""
    catalogo = catalog_name(cfg)
    spark.sql(
        f"CREATE CATALOG IF NOT EXISTS {catalogo} "
        "COMMENT 'OceanWatch Analytics — lakehouse de tráfico marítimo AIS (Entrega 2)'"
    )


def create_schemas(spark: Any, cfg: dict[str, Any]) -> None:
    """Crea los esquemas por capa si no existen (idempotente).

    No modifica ``entrega1`` (continuidad con Entrega 1, ver ADR-002).
    """
    catalogo = catalog_name(cfg)
    comentarios = {
        "bronze": "Capa bronce: ingesta cruda de CSV AIS vía Auto Loader.",
        "silver": "Capa plata: datos saneados y conformados tras reglas de calidad.",
        "gold": "Capa oro: matrices de tráfico por grilla lat/lon y cruce con WPI.",
        "quarantine": "Registros rechazados por reglas de calidad (dead-letter).",
        "quality": "Métricas y reportes de calidad de datos.",
        "ml": "Features y artefactos del modelo de anomalías de velocidad (SOG).",
    }
    for clave in _ESQUEMAS_NUEVOS:
        nombre = schema(cfg, clave)
        comentario = comentarios[clave].replace("'", "''")
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {catalogo}.{nombre} COMMENT '{comentario}'"
        )


def create_volumes(spark: Any, cfg: dict[str, Any]) -> None:
    """Crea los volúmenes de landing y checkpoints si no existen (idempotente)."""
    catalogo = catalog_name(cfg)
    bronze = schema(cfg, "bronze")
    landing = cfg["volumenes"]["landing"]
    checkpoints = cfg["volumenes"]["checkpoints"]
    spark.sql(
        f"CREATE VOLUME IF NOT EXISTS {catalogo}.{bronze}.{landing} "
        "COMMENT 'Zona de aterrizaje: zips NOAA y CSV descomprimidos.'"
    )
    spark.sql(
        f"CREATE VOLUME IF NOT EXISTS {catalogo}.{bronze}.{checkpoints} "
        "COMMENT 'Checkpoints de Structured Streaming (Auto Loader).'"
    )


def run(spark: Optional[Any] = None, config_path: Optional[str] = None) -> None:
    """Orquesta el bootstrap completo de forma idempotente.

    Args:
        spark: sesión Spark activa. Si es ``None``, intenta obtener la sesión
            activa (``SparkSession.getActiveSession``); fuera de Databricks será
            ``None`` y se lanza un error explicativo.
        config_path: ruta opcional al YAML de configuración.
    """
    cfg = load_config(config_path)
    if spark is None:
        from pyspark.sql import SparkSession  # import perezoso

        spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError(
            "Se requiere una sesión Spark activa (ejecutar en Databricks serverless). "
            "PENDIENTE DE EJECUCIÓN — este script no se corre en Fase 1."
        )
    create_catalog(spark, cfg)
    create_schemas(spark, cfg)
    create_volumes(spark, cfg)


if __name__ == "__main__":
    # PENDIENTE DE EJECUCIÓN — NO ejecutar en Fase 1.
    # Requiere una sesión Spark con permisos sobre Unity Catalog en Databricks
    # serverless. Ejecutar únicamente a partir de Fase 2.
    raise SystemExit(
        "bootstrap.py está PENDIENTE DE EJECUCIÓN. Ejecútelo desde un notebook "
        "de Databricks serverless (Fase 2) invocando oceanwatch.ingest.bootstrap.run(spark)."
    )
