# ADR-008: Tipo de `Status` como entero y captura de valores no conformes en `_rescued_data`

- **Estado:** Aceptado
- **Fecha:** 2026-11-05
- **Requisito(s) de la rúbrica:** Req. 1 (15%)
- **Autores:** Alejandro Abril (202224328), David Fernando Caro (202222073), Juan David Torres Albarracín (202317608)

## 1. Contexto y problema
La Entrega 1 declaró el campo AIS `Status` como `string`. La verificación del esquema real de los
CSV de NOAA (`evidence/req01-streaming/verificacion_esquema_real.md`) muestra que `Status` llega
como **código entero** (p. ej. `0`, `12`, `15`), igual que `VesselType`. Con un esquema explícito
(ADR-003), un valor que no case con el tipo declarado podría abortar el micro-batch. Hay que decidir
el **tipo** de `Status` y **qué hacer** con los valores que no cumplan el esquema, sin que el stream
muera ante datos sucios o campos nuevos del feed.

## 2. Criterios de decisión
- Fidelidad al dato real de la fuente (muestra verificada).
- Resiliencia del stream ante valores no conformes (no abortar la ingesta).
- Continuidad/corrección respecto a Entrega 1.
- Trazabilidad de lo no conforme (poder auditarlo, no descartarlo en silencio).
- Coherencia con las reglas de calidad posteriores (Req. 2).

## 3. Alternativas consideradas
| Alternativa | Descripción | Pros | Contras | Evidencia |
|---|---|---|---|---|
| A. `Status` como **int** + `rescuedDataColumn` | Declarar `Status` entero en el esquema explícito y dejar que lo no conforme caiga a `_rescued_data` | Fiel a la muestra real; el stream no muere; lo no conforme queda auditable | Requiere reglas de calidad posteriores sobre `_rescued_data` | [Muestra real (`Status` entero)](../../evidence/req01-streaming/verificacion_esquema_real.md); [Rescued data / rescue mode — Databricks](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema) (acceso 2026-11-05) |
| B. `Status` como **string** (Entrega 1) | Mantener el tipo de Entrega 1 | No rompe con Entrega 1 | Contradice la muestra real (código entero); pierde semántica numérica; arrastra el error | [Muestra real contradice string](../../evidence/req01-streaming/verificacion_esquema_real.md) |
| C. `Status` int + **columna cruda paralela** por cada campo | Duplicar cada campo con su versión string cruda | Conserva el crudo | Duplica el ancho del esquema; redundante con `_rescued_data` | [`_rescued_data` ya captura lo no conforme](https://community.databricks.com/t5/databricks-free-edition-help/handling-new-columns-using-auto-loader-rescue-mode/m-p/155232) (acceso 2026-11-05) |

## 4. Decisión
`Status` se declara como **`int`** en el esquema explícito (corrigiendo Entrega 1) y los valores que
no casen con el esquema se capturan en **`_rescued_data`** (columna `rescuedDataColumn`), sin abortar
el stream.

## 5. Justificación basada en evidencia
La [muestra real verificada](../../evidence/req01-streaming/verificacion_esquema_real.md) (acceso
2026-11-05) muestra `Status` con valores enteros (`0`, `12`, `15`), lo que contradice el `string` de
Entrega 1 y respalda el tipo `int` (alternativa A frente a B). La documentación de Auto Loader sobre
[rescate de datos](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema)
(acceso 2026-11-05) indica que los datos inesperados —por ejemplo, de un tipo distinto al declarado—
se "rescatan" en una columna JSON (`_rescued_data`) que puede consultarse después; y el hilo de la
comunidad sobre
[rescue mode](https://community.databricks.com/t5/databricks-free-edition-help/handling-new-columns-using-auto-loader-rescue-mode/m-p/155232)
(acceso 2026-11-05) confirma que, con el esquema congelado, lo no conforme aterriza en
`_rescued_data` en lugar de romper el pipeline, lo que hace redundante una columna cruda paralela por
campo (alternativa C). Esto se verificó de verdad: la **prueba (c)** de tolerancia a fallos colocó
una fila con `Status="ABC"` (no casteable a int) y una fila con columna extra, y observó
`stream_murio=no; filas_ingeridas=4; filas_con_rescued_data=1`, es decir el stream siguió vivo y la
fila no conforme quedó capturada en `_rescued_data`. El esquema real de la tabla bronce
(`DESCRIBE TABLE` en `ejecucion_ingesta.md`) confirma además `Status int` en producción.

## 6. Patrón de diseño / ingeniería de datos aplicado
**Schema-on-read explícito + Rescued Data Column** (esquema congelado con captura de lo no conforme),
sobre la base de la **Landing Zone + Auto Loader** de ADR-003.

## 7. Consecuencias
- Positivas: tipo fiel a la fuente; stream resiliente a datos sucios y a campos nuevos; lo no
  conforme queda auditable en `_rescued_data` en vez de descartarse o romper la ingesta.
- Negativas: las reglas de calidad (Req. 2) deberán inspeccionar `_rescued_data` para cuantificar y
  tratar lo rescatado; `Status` numérico requiere mapear códigos AIS a significado en gold.
- Deuda asumida: regla de calidad sobre `_rescued_data` y catálogo de códigos `Status`/`VesselType`
  (Fase 3, Req. 2).

## 8. Cómo se verifica / reversibilidad
Verificado por la **prueba (c)** de `evidence/req01-streaming/pruebas_tolerancia_fallos.md`
(`run_id 497716691196081`: `filas_con_rescued_data=1`, stream no murió) y por el `DESCRIBE TABLE` de
`evidence/req01-streaming/ejecucion_ingesta.md` (`Status int`). Reversible cambiando el tipo en
`conf/config.yaml` (`esquema_ais`, campo `Status`). Estado actual: **Verificado (ejecutado)**.

## 9. Referencias
- Muestra real del esquema NOAA (`Status` entero): `evidence/req01-streaming/verificacion_esquema_real.md` (acceso 2026-11-05).
- Inferencia/evolución de esquema y rescate de datos en Auto Loader — Databricks: https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema (acceso 2026-11-05).
- Rescue mode y `_rescued_data` (comunidad) — Databricks: https://community.databricks.com/t5/databricks-free-edition-help/handling-new-columns-using-auto-loader-rescue-mode/m-p/155232 (acceso 2026-11-05).
- Verificación: `evidence/req01-streaming/pruebas_tolerancia_fallos.md` (prueba (c), run 497716691196081) y `evidence/req01-streaming/ejecucion_ingesta.md` (DESCRIBE TABLE).
- ADR-003 (ingesta zip→Auto Loader, esquema explícito + rescued data).
