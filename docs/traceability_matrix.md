# Trazabilidad · Entrega 2

| Requisito | Implementación | Verificación institucional |
|---|---|---|
| 1 · Ingesta en streaming | Auto Loader, esquema explícito, Volume, checkpoint y Delta append-only. Dedup en plata por clave de negocio. | [Corrida inicial](https://dbc-b38f0852-ecf9.cloud.databricks.com/jobs/83886562675136/runs/828952167457977) y [Job de verificación](https://dbc-b38f0852-ecf9.cloud.databricks.com/jobs/719836273900213). |
| 2 · Capas y calidad | Plata, cuarentena original, métricas y conciliación diaria antes de oro. | [Reglas](data_quality_rules.md), [contrato de oro](data_contract_gold.md) y [evidencia](../evidence/institucional/). |
| 3 · Orquestación | Detección automática de archivos pendientes, seis tareas en clúster dedicado, backfill parametrizado y reintentos. | [Detector](../resources/jobs/oceanwatch_detector.job.yml), [runbook](runbook.md) y [prueba de carga](../evidence/institucional/llegada_archivos.md). |
| 4 · Visualización | Consola con cuatro consultas parametrizadas sobre oro. | [Tablero publicado](https://dbc-b38f0852-ecf9.cloud.databricks.com/dashboardsv3/01f1e5507d1b18a5963d1a49e4329629/published). |
| 5 · Gobernanza | Tablas y modelo en Unity Catalog, comentarios, contrato, grupo institucional y permisos en recursos. | [ADR-019](adr/ADR-019-computo-y-almacenamiento-institucional.md). La segregación por rol y esquema requiere acceso administrativo adicional. |
| 6 · MLflow | Variables SOG por tipo/buque/hora, split temporal, experimento, versión UC, inferencia distribuida y salida oro. | [ADR-021](adr/ADR-021-modelo-sog-en-cluster.md) y artefactos de la [evidencia institucional](../evidence/institucional/). |
| 7 · Repo y bitácora | Bundle, código, documentos, tests y decisiones versionados junto a la Entrega 1. | [Bitácora](bitacora.md) e historial Git. |
| 8 · Sustentación | Guía de demostración, decisiones comparadas y límites. | [Guía](guia_sustentacion.md). La presentación se hace en clase. |

La [evidencia institucional](../evidence/institucional/) recoge resultados ejecutados. Las pruebas del prototipo se conservan en las carpetas históricas.
