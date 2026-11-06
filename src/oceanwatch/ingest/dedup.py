"""Deduplicación bronce->silver y conteo de duplicados (imports PySpark perezosos).

La capa bronce es append-only (ADR-006); la deduplicación se aplica al leer hacia
silver con un watermark sobre ``BaseDateTime`` y ``dropDuplicatesWithinWatermark``
sobre la clave natural ``['MMSI','BaseDateTime']``.

La construcción de la clave de dedup es pura y testeable sin Spark.
"""

from __future__ import annotations

from typing import Any

from oceanwatch.ingest.paths import clave_dedup


def deduplicar(df: Any, cfg: dict[str, Any]):
    """Deduplica un DataFrame (streaming o batch) por la clave natural.

    Aplica ``withWatermark('BaseDateTime', watermark_dedup)`` seguido de
    ``dropDuplicatesWithinWatermark(clave_dedup)``.
    """
    watermark = cfg["ingesta"]["watermark_dedup"]
    clave = clave_dedup(cfg)
    return df.withWatermark("BaseDateTime", watermark).dropDuplicatesWithinWatermark(clave)


def contar_duplicados(df: Any, cfg: dict[str, Any]) -> dict[str, int]:
    """Cuenta filas totales, distintas por clave y duplicados (sobre un DataFrame batch).

    ``duplicados = total - distintos_por_clave``.

    Returns:
        dict con ``total``, ``distintos_por_clave`` y ``duplicados``.
    """
    clave = clave_dedup(cfg)
    total = df.count()
    distintos = df.select(*clave).distinct().count()
    return {
        "total": total,
        "distintos_por_clave": distintos,
        "duplicados": total - distintos,
    }
