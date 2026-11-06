# ADR-007: Estrategia de checkpoints de Structured Streaming en Volume de Unity Catalog

- **Estado:** Aceptado
- **Fecha:** 2026-11-05
- **Requisito(s) de la rúbrica:** Req. 1 (15%)
- **Autores:** Alejandro Abril (202224328), David Fernando Caro (202222073), Juan David Torres Albarracín (202317608)

## 1. Contexto y problema
La ingesta incremental con Auto Loader + `Trigger.availableNow` (ADR-003/ADR-004) necesita un
**checkpoint** para reanudarse tras una interrupción y para no reprocesar archivos ya consumidos.
Sobre cómputo serverless y Unity Catalog hay que decidir **qué** guarda el checkpoint, **dónde**
vive, **qué** pasa si se pierde y cómo se relaciona con la idempotencia exactly-once
(`txnAppId`/`txnVersion`).

## 2. Criterios de decisión
- Reanudación tras fallo/reinicio sin pérdida ni reproceso de archivos.
- Compatibilidad con serverless y Unity Catalog (gobernanza y permisos del almacenamiento).
- Coubicación con el `schemaLocation` de Auto Loader (evolución de esquema).
- Durabilidad y aislamiento del estado por stream.
- Reversibilidad (poder reconstruir el estado si hiciera falta).

## 3. Alternativas consideradas
| Alternativa | Descripción | Pros | Contras | Evidencia |
|---|---|---|---|---|
| A. Checkpoint en **Volume de UC** (`oceanwatch.bronze.checkpoints`) | `checkpointLocation` y `cloudFiles.schemaLocation` bajo `/Volumes/oceanwatch/bronze/checkpoints/bronze/ais_raw` | Gobernado por UC; durable; coubicable con el `_schema`; aislado por stream | Hay que gestionar su ciclo de vida (no borrarlo por accidente) | [Checkpoints de Structured Streaming — Databricks](https://docs.databricks.com/gcp/en/structured-streaming/checkpoints) (acceso 2026-11-05); [Esquema en Auto Loader (schemaLocation junto al checkpoint)](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema) (acceso 2026-11-05) |
| B. Checkpoint en **DBFS** (`/dbfs/...`) | Ubicación heredada fuera de UC | Simple históricamente | No gobernado por UC; desaconsejado con UC; permisos difusos | [Checkpoints de Structured Streaming — Databricks](https://docs.databricks.com/gcp/en/structured-streaming/checkpoints) (acceso 2026-11-05) |
| C. **Sin checkpoint** (batch estático) | `spark.read` + append programado | Trivial | Pierde offsets/estado; reprocesa todo; sin reanudación ni exactly-once | [Qué es un checkpoint de streaming — Databricks](https://answers.databricks.com/streaming-checkpoint) (acceso 2026-11-05) |

## 4. Decisión
El checkpoint del stream (y el `cloudFiles.schemaLocation`) vive en el **Volume de Unity Catalog**
`oceanwatch.bronze.checkpoints`, bajo `bronze/ais_raw/` (`_checkpoint` y `_schema`), como define
`conf/config.yaml` (`ingesta.checkpoint`).

## 5. Justificación basada en evidencia
La documentación de
[checkpoints de Structured Streaming](https://docs.databricks.com/gcp/en/structured-streaming/checkpoints)
(acceso 2026-11-05) explica que el checkpoint, junto con el write-ahead log, aporta las garantías de
procesamiento rastreando la información que identifica la query, su estado y los registros
procesados; y el detalle de
[qué guarda un checkpoint](https://answers.databricks.com/streaming-checkpoint) (acceso 2026-11-05)
precisa que se persisten los **offsets** (el rango de archivos/offsets procesados en cada trigger) y
los agregados en curso, lo que permite recuperarse tras un fallo o apagado. Para Auto Loader, la
[guía de inferencia y evolución de esquema](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema)
(acceso 2026-11-05) indica que `schemaLocation` puede coubicarse en el mismo directorio del
`checkpointLocation`; por eso ambos cuelgan del mismo Volume por stream. Se elige el Volume de UC
(A) sobre DBFS (B) por gobernanza y permisos de Unity Catalog, y frente a no tener checkpoint (C),
que perdería la reanudación y el exactly-once. El comportamiento se verificó de verdad: la
**prueba (d)** observó que una segunda corrida sin archivos nuevos **no reprocesa**
(`conteo_corrida_1=3; conteo_corrida_2=3; reproceso=no`) y la **prueba (a)** que el reinicio desde
el mismo checkpoint deja el conteo exacto (6 filas, 6 distintos). La relación con
`txnAppId`/`txnVersion` (ADR-004) es complementaria: el checkpoint evita reprocesar **archivos**, y
el par transaccional garantiza que un reintento del **mismo** `batch_id` no duplique filas en Delta.

## 6. Patrón de diseño / ingeniería de datos aplicado
**Checkpointing / write-ahead log** para reanudación exactly-once, con **estado gobernado por Unity
Catalog** (Volume) coubicado con el `schemaLocation` de Auto Loader.

## 7. Consecuencias
- Positivas: reanudación sin pérdida ni reproceso; estado durable y gobernado por UC; evolución de
  esquema coubicada; aislamiento por stream.
- Negativas: el checkpoint es estado operacional que hay que preservar (borrarlo fuerza
  re-descubrimiento y posible reprocesamiento de archivos).
- Deuda asumida: definir política de retención/backup del Volume de checkpoints (Fase 3/4).

## 8. Cómo se verifica / reversibilidad
Verificado por las **pruebas (a) y (d)** de
`evidence/req01-streaming/pruebas_tolerancia_fallos.md` (`run_id 497716691196081`). Si se pierde el
checkpoint, Auto Loader **re-descubre** los archivos de `landing` y puede reprocesarlos; en ese
caso la deduplicación silver (ADR-006) protege el conteo final frente a duplicados por contenido.
Reversible recreando el stream apuntando a un `checkpointLocation` nuevo. Estado actual:
**Verificado (ejecutado)** a escala de prueba.

## 9. Referencias
- Checkpoints de Structured Streaming — Databricks: https://docs.databricks.com/gcp/en/structured-streaming/checkpoints (acceso 2026-11-05).
- Qué guarda un checkpoint de streaming (offsets) — Databricks: https://answers.databricks.com/streaming-checkpoint (acceso 2026-11-05).
- Inferencia y evolución de esquema en Auto Loader (schemaLocation junto al checkpoint) — Databricks: https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema (acceso 2026-11-05).
- Verificación: `evidence/req01-streaming/pruebas_tolerancia_fallos.md` (pruebas (a) y (d), run 497716691196081).
- ADR-003 (ingesta zip→Auto Loader), ADR-004 (Trigger.availableNow + txnAppId/txnVersion), ADR-006 (dedup+watermark).
