"""Ingesta incremental a bronce con Auto Loader + Trigger.availableNow.

Implementa el patrón de ADR-003 (Landing Zone + Auto Loader, esquema explícito +
rescued data) y ADR-004 (Trigger.availableNow + idempotencia exactly-once con
``txnAppId``/``txnVersion`` en ``foreachBatch``). La capa bronce es append-only
(ADR-006): la deduplicación se hace aguas abajo (``dedup.py``).

Todos los imports de PySpark son perezosos: este módulo se importa sin Spark.
"""

from __future__ import annotations

from typing import Any, Optional

from oceanwatch.common.config import catalog_name, fqn, schema
from oceanwatch.ingest.paths import landing_csv_root
from oceanwatch.ingest.schema import ais_struct_type


def _checkpoint_base(cfg: dict[str, Any]) -> str:
    """Ruta base del checkpoint del stream bajo el Volume de checkpoints."""
    catalogo = catalog_name(cfg)
    bronze = schema(cfg, "bronze")
    checkpoints = cfg["volumenes"]["checkpoints"]
    sub = cfg["ingesta"]["checkpoint"]
    return f"/Volumes/{catalogo}/{bronze}/{checkpoints}/{sub}"


def tabla_bronze_fqn(cfg: dict[str, Any]) -> str:
    """Nombre totalmente calificado de la tabla bronce de ingesta."""
    return fqn(cfg, "bronze", cfg["ingesta"]["tabla_bronze"])


def leer_stream_csv(spark: Any, cfg: dict[str, Any], fecha: Optional[str] = None):
    """Lee los CSV de landing como stream con Auto Loader (``cloudFiles``).

    Usa esquema explícito de 17 campos, ``rescuedDataColumn`` para evolución de
    esquema y ``cloudFiles.schemaLocation`` en el Volume de checkpoints. Lee de
    forma recursiva las particiones ``date=YYYY-MM-DD`` bajo ``landing/csv``.

    Args:
        spark: sesión Spark activa (Databricks serverless).
        cfg: configuración cargada.
        fecha: opcional; actualmente la lectura es recursiva sobre todas las
            particiones ``date=`` (Auto Loader descubre los archivos nuevos). Se
            mantiene el parámetro por compatibilidad futura.
    """
    ruta = landing_csv_root(cfg)
    schema_location = f"{_checkpoint_base(cfg)}/_schema"
    rescued = cfg["ingesta"]["rescued_column"]
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("rescuedDataColumn", rescued)
        .option("recursiveFileLookup", "true")
        .schema(ais_struct_type(cfg))
        .load(ruta)
    )


def enriquecer_auditoria(df: Any, cfg: dict[str, Any]):
    """Añade las columnas de auditoría de la capa bronce (ADR-006).

    - ``_ingest_timestamp`` = ``current_timestamp()``
    - ``_source_file`` = ``_metadata.file_path``
    - ``_batch_date`` = fecha extraída de la ruta ``date=YYYY-MM-DD`` del origen
    """
    from pyspark.sql.functions import col, current_timestamp, regexp_extract, to_date

    source_file = col("_metadata.file_path")
    batch_date = to_date(
        regexp_extract(source_file, r"date=(\d{4}-\d{2}-\d{2})", 1)
    )
    return (
        df.withColumn("_ingest_timestamp", current_timestamp())
        .withColumn("_source_file", source_file)
        .withColumn("_batch_date", batch_date)
    )


def _escribir_batch(batch_df: Any, batch_id: int, cfg: dict[str, Any]) -> None:
    """Escribe un micro-batch en la tabla bronce con idempotencia exactly-once.

    Usa ``txnAppId``/``txnVersion`` (ADR-004) para que un reintento del mismo
    ``batch_id`` no duplique filas en Delta.
    """
    tabla = tabla_bronze_fqn(cfg)
    txn_app_id = cfg["ingesta"]["txn_app_id"]
    (
        batch_df.write.format("delta")
        .mode("append")
        .option("txnAppId", txn_app_id)
        .option("txnVersion", batch_id)
        .saveAsTable(tabla)
    )


def escribir_bronze(df: Any, cfg: dict[str, Any]):
    """Escribe el stream a la tabla bronce (append-only) con Trigger.availableNow.

    Idempotencia exactly-once vía ``foreachBatch`` + ``txnAppId``/``txnVersion``.
    El checkpoint vive en el Volume de checkpoints. Devuelve el ``StreamingQuery``.
    """
    checkpoint = f"{_checkpoint_base(cfg)}/_checkpoint"
    return (
        df.writeStream.option("checkpointLocation", checkpoint)
        .foreachBatch(lambda bdf, bid: _escribir_batch(bdf, bid, cfg))
        .trigger(availableNow=True)
        .start()
    )


def ejecutar_ingesta(spark: Any, cfg: dict[str, Any], fechas: Optional[list[str]] = None):
    """Orquesta la ingesta: leer -> enriquecer -> escribir y esperar a que termine.

    Con ``Trigger.availableNow`` el stream consume todo lo disponible en landing y
    se detiene; ``awaitTermination`` bloquea hasta que termine el micro-batch.

    Args:
        spark: sesión Spark activa.
        cfg: configuración cargada.
        fechas: opcional; reservado para filtrado futuro. La lectura actual es
            recursiva sobre todas las particiones ``date=`` presentes en landing.

    Returns:
        El ``StreamingQuery`` ya terminado (para inspección de métricas).
    """
    df = leer_stream_csv(spark, cfg, fecha=None)
    df = enriquecer_auditoria(df, cfg)
    query = escribir_bronze(df, cfg)
    query.awaitTermination()
    return query
