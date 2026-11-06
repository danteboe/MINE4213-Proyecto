"""Paquete OceanWatch Analytics — lakehouse de tráfico marítimo AIS (Entrega 2).

Subpaquetes:
- ``common``: utilidades compartidas (carga de configuración, esquema AIS).
- ``ingest``: ingesta incremental (bootstrap de catálogo/esquemas/volúmenes, Auto Loader).
- ``quality``: reglas de calidad y cuarentena (PENDIENTE - Fase 3).
- ``gold``: materialización de la capa oro (PENDIENTE - Fase 3).
- ``ml``: detección de anomalías de velocidad con MLflow (PENDIENTE - Fase posterior).
"""

__all__ = ["common", "ingest", "quality", "gold", "ml"]
