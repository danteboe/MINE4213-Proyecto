# Decisiones de arquitectura

Las decisiones vigentes describen el despliegue de la Parte 2 en el workspace institucional y apuntan a la evidencia que permite comprobarlas.

| Decisión | Alternativa considerada y razón | Evidencia |
|---|---|---|
| [ADR-019 · Cómputo y almacenamiento](ADR-019-computo-y-almacenamiento-institucional.md) | Clúster clásico dedicado en vez del cómputo serverless del prototipo. Tablas en el esquema del curso y archivos en el volumen existente por los permisos efectivos. | [Inventario y permisos](../../evidence/institucional/gobernanza.md) · [Recursos desplegados](../directorio_recursos.md). |
| [ADR-020 · Procesamiento incremental y calidad](ADR-020-procesamiento-incremental-y-calidad.md) | Sink Delta con checkpoint, deduplicación determinista por fecha, poda, broadcast y publicación por predicado. Evita rematerializar el histórico y añade evidencia hidrográfica a la máscara. | [Fallo y reanudación, compuerta de agua y planes Spark](../../evidence/institucional/). |
| [ADR-021 · Modelo SOG](ADR-021-modelo-sog-en-cluster.md) | Isolation Forest en una muestra acotada, con z-score como línea base. El orden temporal preserva una validación fuera de muestra. La inferencia por lotes Arrow mantiene el cómputo en Spark. | [Modelo y replay](../../evidence/institucional/#modelo-y-consola) · [Registro UC](../directorio_recursos.md#tablas-y-modelo). |
| [ADR-022 · Llegada de archivos](ADR-022-llegada-de-archivos.md) | Bandeja separada del checkpoint y metadatos de control. Detector en el warehouse mientras se habilitan eventos nativos. Acepta NOAA CSV2 sin conversión manual. | [Corrida programada, plan del detector y publicación](../../evidence/institucional/llegada_archivos.md). |
| [Arquitectura y contrato de oro](../architecture/medallion-overview.md#grilla-y-productos) | Grilla de 0,01° con operaciones nativas y continuidad con la Parte 1. Cómputo de pings, buques-horas y presencia con granos explícitos. | [Contrato](../data_contract_gold.md) · [Planes Spark](../../evidence/institucional/#planes-spark). |
| [Consola AI/BI](../data_contract_gold.md#consumo-en-el-tablero) | Consulta directa de oro, filtro compartido y límites para el render interactivo. La identidad del lector conserva el control de acceso de Unity Catalog. | [Consola publicada y validación](../../evidence/institucional/llegada_archivos.md#consola-publicada). |

## Registro del prototipo

Los ADR 001 a 018 conservan la evolución en Free Edition. Sus IDs, catálogos, permisos y resultados corresponden a ese entorno. Los ADR institucionales describen la implementación evaluable.

| Tema | Historial |
|---|---|
| Capas, cómputo y nombres | [001](ADR-001-arquitectura-medallion-serverless.md) · [002](ADR-002-convenciones-nombres-catalogo-esquemas.md) |
| Ingesta, checkpoint, deduplicación y esquema | [003](ADR-003-estrategia-ingesta-zip-autoloader.md) · [004](ADR-004-modo-trigger-availableNow.md) · [006](ADR-006-dedup-watermark.md) · [007](ADR-007-estrategia-checkpoints.md) · [008](ADR-008-tipo-status-rescued-data.md) |
| Calidad, cuarentena y modelo de datos | [009](ADR-009-catalogo-reglas-calidad.md) · [010](ADR-010-mascara-tierra-agua.md) · [011](ADR-011-estrategia-cuarentena.md) · [012](ADR-012-modelo-oro.md) |
| Orquestación, reintentos y monitoreo | [013](ADR-013-diseno-job-orquestacion.md) · [014](ADR-014-idempotencia-reintentos.md) · [015](ADR-015-schedule-monitoreo.md) |
| Selección y diseño inicial de ML | [005](ADR-005-eleccion-ml-anomalias-velocidad.md) · [016](ADR-016-modelo-anomalias-sog-mlflow.md) |
| Tablero y gobernanza | [017](ADR-017-dashboard-aibi-oro.md) · [018](ADR-018-gobernanza-permisos-linaje.md) |
