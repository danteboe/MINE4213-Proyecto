# ADR-006: Deduplicación por MMSI+BaseDateTime con watermark 24h en bronce→silver

- **Estado:** Aceptado
- **Fecha:** 2026-11-05
- **Requisito(s) de la rúbrica:** Req. 1 (15%)
- **Autores:** Alejandro Abril (202224328), David Fernando Caro (202222073), Juan David Torres Albarracín (202317608)

## 1. Contexto y problema
La capa bronce (`oceanwatch.bronze.ais_raw`) es **append-only** (ADR-003/ADR-004): cada corrida de
Auto Loader añade las filas de los archivos nuevos y la idempotencia exactly-once
(`txnAppId`/`txnVersion`) evita duplicar **archivos**, pero **no** elimina duplicados por
**contenido**. La ejecución real de la ingesta (FEAT-001, run `847693335141936`) confirmó que el
feed AIS trae duplicados exactos por la clave natural: sobre **25 897 493** filas de los 3 días,
hay **25 896 835** distintas por `(MMSI, BaseDateTime)`, es decir **658 duplicados** (~0,0025 %).
Además, un archivo reenviado (prueba (b) de tolerancia a fallos) sube el conteo de bronce. Hay que
decidir **dónde** y **cómo** deduplicar sin romper el carácter append-only de bronce y
controlando el estado del stream.

## 2. Criterios de decisión
- Preservar bronce **append-only** (trazabilidad y reprocesabilidad).
- Semántica correcta de deduplicación sobre la clave natural `MMSI`+`BaseDateTime`.
- Estado **acotado** del stream (evitar estado ilimitado que cause OOM).
- Compatibilidad con serverless y con `Trigger.availableNow`.
- Reproducibilidad y verificabilidad con datos reales.

## 3. Alternativas consideradas
| Alternativa | Descripción | Pros | Contras | Evidencia |
|---|---|---|---|---|
| A. `dropDuplicatesWithinWatermark(MMSI,BaseDateTime)` con watermark 24h en silver | Deduplicar al leer bronce→silver, con `withWatermark('BaseDateTime','24 hours')` | Bronce intacto (append-only); estado acotado por el watermark; semántica clara de ventana | El estado vive durante la ventana; duplicados separados por más de 24h no se detectan (no aplica a este feed) | [dropDuplicatesWithinWatermark — Databricks](https://docs.databricks.com/aws/en/pyspark/reference/classes/dataframe/dropDuplicatesWithinWatermark) (acceso 2026-11-05); [Streaming deduplication — Databricks](https://community.databricks.com/t5/technical-blog/deep-dive-streaming-deduplication/ba-p/105062) (acceso 2026-11-05) |
| B. Deduplicar en bronce (borrar/`dropDuplicates` sobre la tabla bronce) | Mantener bronce ya sin duplicados | Una sola tabla "limpia" | **Rompe APPEND-ONLY**; `dropDuplicates` sin watermark mantiene estado global ilimitado (riesgo OOM) | [Streaming deduplication — estado global sin watermark](https://community.databricks.com/t5/technical-blog/deep-dive-streaming-deduplication/ba-p/105062) (acceso 2026-11-05) |
| C. `MERGE` por clave en bronce | Upsert por `(MMSI,BaseDateTime)` en cada micro-batch | Idempotente por clave | Acopla la ingesta a la clave; coste de MERGE en tabla enorme; rompe append-only | [Watermark en Structured Streaming — Databricks](https://answers.databricks.com/watermark-structured-streaming) (acceso 2026-11-05) |

## 4. Decisión
Se deduplica en el paso **bronce→silver** con
`withWatermark('BaseDateTime', '24 hours')` seguido de
`dropDuplicatesWithinWatermark(['MMSI','BaseDateTime'])`, dejando la capa bronce **append-only**.

## 5. Justificación basada en evidencia
La documentación de
[`dropDuplicatesWithinWatermark`](https://docs.databricks.com/aws/en/pyspark/reference/classes/dataframe/dropDuplicatesWithinWatermark)
(acceso 2026-11-05) indica que, con un watermark configurado, se mantiene estado intermedio para
garantizar que los eventos se deduplican mientras la distancia temporal entre el más temprano y el
más tardío sea menor que el umbral del watermark; esto **acota** el estado, a diferencia de un
`dropDuplicates` global sobre bronce, que —según el
[análisis de deduplicación en streaming de Databricks](https://community.databricks.com/t5/technical-blog/deep-dive-streaming-deduplication/ba-p/105062)
(acceso 2026-11-05)— conserva todas las claves entre triggers (estado ilimitado, riesgo de OOM),
descartando la alternativa B. La alternativa C (`MERGE` en bronce) también rompe el append-only y
acopla la ingesta a la clave. El umbral de **24 h** se elige porque el corpus se particiona por día
(`date=YYYY-MM-DD`) y los duplicados observados en FEAT-001 son del **mismo** `BaseDateTime` (misma
marca temporal), muy por debajo de 24 h; la guía de
[watermark en Structured Streaming](https://answers.databricks.com/watermark-structured-streaming)
(acceso 2026-11-05) recomienda fijar el umbral por encima de la diferencia máxima de timestamps
entre eventos duplicados, condición que 24 h satisface holgadamente para este feed. Los **658
duplicados reales** (25 897 493 − 25 896 835) de FEAT-001 son exactamente los que esta dedup
eliminará, y la prueba (b) de tolerancia a fallos lo verificó en pequeño:
`dedup_silver_distintos=3 (estable)` pese a que bronce subió de 3 a 6 filas tras el reenvío.

## 6. Patrón de diseño / ingeniería de datos aplicado
**Idempotent Consumer** / **deduplicación con watermark** (estado acotado por ventana de evento)
sobre una arquitectura **Medallion** append-only en bronce.

## 7. Consecuencias
- Positivas: bronce se mantiene append-only (auditable y reprocesable); el estado del stream queda
  acotado por el watermark (sin OOM); la clave natural queda deduplicada de forma reproducible.
- Negativas: duplicados separados por más de 24 h no se detectarían (no ocurre en este feed, donde
  el duplicado comparte `BaseDateTime`); se añade una tabla silver dedup como paso extra.
- Deuda asumida: implementación de la tabla silver materializada (Fase 3, Req. 2).

## 8. Cómo se verifica / reversibilidad
Verificado por la **prueba (b)** de `evidence/req01-streaming/pruebas_tolerancia_fallos.md`
(`run_id 497716691196081`): `bronce_tras_reenvio=6 (append sube); dedup_silver_distintos=3
(estable)`, y por los conteos reales de FEAT-001 (658 duplicados por clave en
`evidence/req01-streaming/ejecucion_ingesta.md`). Reversible cambiando la clave o el umbral del
watermark en `src/oceanwatch/ingest/dedup.py` (`cfg['ingesta']['clave_dedup']`,
`watermark_dedup`). Estado actual: **Verificado (ejecutado)** a escala de prueba; la materialización
silver se ejecuta en Fase 3.

## 9. Referencias
- `dropDuplicatesWithinWatermark` — Databricks: https://docs.databricks.com/aws/en/pyspark/reference/classes/dataframe/dropDuplicatesWithinWatermark (acceso 2026-11-05).
- Deduplicación en streaming (estado global vs. watermark) — Databricks: https://community.databricks.com/t5/technical-blog/deep-dive-streaming-deduplication/ba-p/105062 (acceso 2026-11-05).
- Watermark en Structured Streaming — Databricks: https://answers.databricks.com/watermark-structured-streaming (acceso 2026-11-05).
- Conteos reales de duplicados: `evidence/req01-streaming/ejecucion_ingesta.md` (658 duplicados, run 847693335141936).
- Verificación de la dedup: `evidence/req01-streaming/pruebas_tolerancia_fallos.md` (prueba (b), run 497716691196081).
- ADR-003 (ingesta zip→Auto Loader), ADR-004 (Trigger.availableNow), ADR-007 (checkpoints).
