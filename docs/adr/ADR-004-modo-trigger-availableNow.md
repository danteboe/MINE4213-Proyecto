# ADR-004: Modo de disparo de Structured Streaming — Trigger.availableNow

- **Estado:** Aceptado
- **Fecha:** 2026-11-05
- **Requisito(s) de la rúbrica:** Req. 1 (15%), Req. 3 (20%)
- **Autores:** Alejandro Abril (202224328), David Fernando Caro (202222073), Juan David Torres Albarracín (202317608)

## 1. Contexto y problema
La ingesta incremental (ADR-003) se ejecuta con Structured Streaming sobre cómputo
**serverless** (inventario verificado). El feed AIS es diario, no continuo. Hay que elegir el
modo de disparo del stream de forma que sea compatible con serverless, idempotente, barato
(sin clúster siempre encendido) y programable por un Job con cron.

## 2. Criterios de decisión
- Compatibilidad con serverless (qué triggers están permitidos).
- Idempotencia (no duplicar filas entre corridas).
- Coste (evitar cómputo continuo para un feed diario).
- Programación por cron (y en qué zona horaria se interpreta).

## 3. Alternativas consideradas
| Alternativa | Descripción | Pros | Contras | Evidencia |
|---|---|---|---|---|
| A. `Trigger.availableNow` | Micro-batch que consume todo lo disponible y se detiene | Compatible con serverless, barato, reanudable por checkpoint, programable por Job | No es "tiempo real" (latencia = periodo del cron) | [Triggers de Structured Streaming](https://docs.databricks.com/gcp/en/structured-streaming/triggers) (acceso 2026-11-05) |
| B. `Trigger.ProcessingTime` / continuo | Stream siempre activo por intervalo de tiempo | Baja latencia | NO soportado en serverless; cómputo continuo caro | [Simular streaming en Free Edition (comunidad)](https://community.databricks.com/t5/data-engineering/simulating-real-time-streaming-in-databricks-free-edition/td-p/131869) (acceso 2026-11-05) |
| C. Batch estático periódico | `spark.read` + append programado | Simple | Pierde checkpoint/exactly-once; reprocesa; sin idempotencia nativa | [Idempotencia en foreachBatch](https://answers.databricks.com/use-availablenow-trigger-incremental-batch) (acceso 2026-11-05) |

## 4. Decisión
Se usa **`Trigger.availableNow`** orquestado por un Job con schedule cron, con idempotencia en
`foreachBatch` mediante `txnAppId`/`txnVersion` y el cron expresado en **UTC** (zona horaria
por defecto de serverless).

## 5. Justificación basada en evidencia
La [documentación de triggers](https://docs.databricks.com/gcp/en/structured-streaming/triggers)
(acceso 2026-11-05) describe `availableNow` como el micro-batch que procesa lo disponible y se
detiene, ideal para feeds programados; el hilo de la comunidad sobre
[streaming en Free Edition](https://community.databricks.com/t5/data-engineering/simulating-real-time-streaming-in-databricks-free-edition/td-p/131869)
(acceso 2026-11-05) confirma que los triggers por tiempo/continuos no aplican en entornos
serverless, descartando la alternativa B. La idempotencia exactly-once en escrituras Delta se
logra con `txnAppId`/`txnVersion` en `foreachBatch`, según
[esta referencia](https://answers.databricks.com/use-availablenow-trigger-incremental-batch)
(acceso 2026-11-05), lo que hace innecesario el batch manual (C). Finalmente, las
[limitaciones de serverless](https://docs.databricks.com/aws/en/compute/serverless/limitations)
(acceso 2026-11-05) indican que el entorno usa **UTC** por defecto, por lo que el cron del Job
se define en UTC para evitar desfases con el feed diario.

## 6. Patrón de diseño / ingeniería de datos aplicado
**Trigger-once / AvailableNow incremental batch** (micro-batch programado, Kappa simplificado)
combinado con **Idempotent Consumer** (`txnAppId`/`txnVersion`).

## 7. Consecuencias
- Positivas: procesamiento incremental barato y reanudable; idempotencia exactly-once;
  programación simple por Job.
- Negativas: no hay latencia de "tiempo real" (la latencia es el periodo del cron).
- Consideración: el cron debe razonarse en UTC para alinear con el feed diario.

## 8. Cómo se verifica / reversibilidad
Al ejecutar (Fase 2/3): dos corridas seguidas sin datos nuevos no deben duplicar filas en
`bronze` (prueba de idempotencia). Reversible cambiando el modo de trigger en la definición
del stream. Estado actual: **Diseñado (no ejecutado)**.

## 9. Referencias
- Triggers de Structured Streaming — Databricks: https://docs.databricks.com/gcp/en/structured-streaming/triggers (acceso 2026-11-05).
- Streaming en Free Edition (comunidad): https://community.databricks.com/t5/data-engineering/simulating-real-time-streaming-in-databricks-free-edition/td-p/131869 (acceso 2026-11-05).
- Idempotencia en foreachBatch (txnAppId/txnVersion): https://answers.databricks.com/use-availablenow-trigger-incremental-batch (acceso 2026-11-05).
- Limitaciones de serverless (timezone UTC): https://docs.databricks.com/aws/en/compute/serverless/limitations (acceso 2026-11-05).
- ADR-003 (estrategia de ingesta).
