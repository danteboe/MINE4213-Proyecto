# ADR-011: Estrategia de cuarentena de la capa plata (Dead-letter/Quarantine)

- **Estado:** Aceptado
- **Fecha:** 2026-11-05
- **Requisito(s) de la rúbrica:** Req. 2 (Capas de calidad)
- **Autores:** Alejandro Abril (202224328), David Fernando Caro (202222073), Juan David Torres Albarracín (202317608)

## 1. Contexto y problema

La materialización bronce -> plata (FEAT-003) separa las filas que pasan TODAS las
compuertas de descarte (a `oceanwatch.silver.ais_clean`) de las que violan al menos una
regla de descarte. El diagnóstico real de 25.897.493 filas
(`evidence/req02-capas-calidad/diagnostico_calidad_bronce.md`) muestra que una fracción
pequeña pero no trivial del corpus incumple reglas de descarte: MMSI no de 9 dígitos =
21.960, y MMSI con MID fuera del rango ITU. Descartar esas filas en silencio destruiría
trazabilidad (¿por qué se perdieron?, ¿cuántas?, ¿de qué día?) e impediría auditar la
calidad del feed AIS o reprocesar si una regla resultara demasiado agresiva. El problema:
**cómo representar y almacenar las filas rechazadas** de forma que (a) no detengan el
flujo, (b) conserven el registro original íntegro, (c) permitan saber QUÉ reglas violó
cada fila, y (d) sean auditables en el tiempo. Restricciones: Unity Catalog + Delta,
cómputo serverless, resistencia a datos AIS nuevos.

## 2. Criterios de decisión

- **No detención del flujo:** una fila mala no puede abortar el lote (contraste con
  `expect_or_fail`).
- **Trazabilidad de causas:** debe quedar registrado qué regla(s) violó cada fila.
- **Integridad del registro original:** la cuarentena guarda las 17 columnas AIS + 4 de
  auditoría sin transformar, para poder reprocesar.
- **Conciliación verificable:** debe cumplirse `bronce = plata + cuarentena + duplicados`.
- **Coste y simplicidad de consulta** en Delta/SQL (agregar por regla, por día).
- **Reproducibilidad** y evolución del esquema sin romper consumidores.

## 3. Alternativas consideradas

| Alternativa | Descripción | Pros | Contras | Evidencia |
|---|---|---|---|---|
| A. Una fila por rechazo con `causas array<string>` | Cada fila rechazada se guarda UNA vez; las reglas violadas van en un array | Conciliación directa (1 fila = 1 registro de bronce); conserva el registro íntegro; fácil `explode` para contar por regla | Hay que usar `size()`/`array_contains` para filtrar por regla | Patrón Dead-letter Queue; `evidence/.../materializacion_plata.md` (39.734 filas, hasta 2 causas) |
| B. Una fila por (registro, causa) | Si una fila viola N reglas se escriben N filas | Consulta por regla trivial (`WHERE regla=...`) | Rompe la conciliación simple (una fila de bronce genera varias en cuarentena); duplica el registro original; infla almacenamiento | Práctica de data quality; descartada por conciliación |
| C. Columna booleana por regla (una por cada regla) | El registro lleva N columnas `viola_<regla>` | Consulta por regla trivial | Esquema rígido: añadir/quitar una regla cambia el esquema de la tabla; no escala al crecer el catálogo | Anti-patrón de esquema ancho |
| D. `expect_or_drop` sin cuarentena (descartar en silencio) | Se filtran las filas malas y no se guardan | Máxima simplicidad | Pérdida total de trazabilidad; imposible auditar/reprocesar; rompe el requisito de capas de calidad | DLT expectations (referencia) |

## 4. Decisión

La cuarentena es la tabla Delta `oceanwatch.quarantine.ais_rejected` con **una fila por
registro rechazado** que conserva las 17 columnas AIS + 4 de auditoría originales, más
una columna **`causas` de tipo `array<string>`** con los NOMBRES de las reglas de
descarte violadas, más `_batch_date` (ya presente) y `_fecha_cuarentena = current_timestamp()`
(alternativa A).

## 5. Justificación basada en evidencia

La alternativa A es la única que mantiene el invariante de conciliación de forma directa
y verificable con los conteos REALES: `25.897.493 = 25.857.101 (plata) + 39.734
(cuarentena) + 658 (duplicados)`, diferencia 0
(`evidence/req02-capas-calidad/invariante_conciliacion.md`). El array de causas captura
la realidad observada: de las 39.734 filas en cuarentena, 21.958 violan DOS reglas a la
vez (`mmsi_bien_formado` + `mmsi_mid_valido`, porque un MMSI que no tiene 9 dígitos
tampoco tiene un MID válido) y 17.776 violan solo `mmsi_mid_valido`
(`evidence/.../materializacion_plata.md`). La alternativa B habría contado esas 21.958
filas DOS veces (61.692 filas de cuarentena), rompiendo la conciliación; la C habría
fijado el esquema a las reglas de hoy, contra el requisito de resistencia a datos nuevos
(un feed AIS futuro podría activar reglas de coordenadas/timestamp que hoy dan 0); la D
contradice el propio Req. 2 (capas de calidad auditables). El registro original íntegro
permite reprocesar si una regla se relaja (p. ej. ampliar el rango de MID). Las métricas
por día y por regla (`oceanwatch.quality.metricas_calidad`) confirman que ninguna regla
supera el umbral de alerta del 5% (máx. ~0.16%).

## 6. Patrón de diseño / ingeniería de datos aplicado

**Dead-letter / Quarantine** (cola de mensajes muertos aplicada a datos): los registros
que no superan la validación se desvían a un almacén de cuarentena como *side-output* en
lugar de descartarse o de abortar el proceso. Se combina con el patrón **Data Quality
Gate** (compuertas estilo `expect`/`expect_or_drop` de ADR-009) y, para la bandera
`en_agua`, con **Reference Data Enrichment** (ADR-010). La separación plata/cuarentena es
un *side-output* en el `foreachBatch` (o en el `WHERE size(causas)>0` del CTAS
equivalente): **la cuarentena NO lanza excepción ni detiene el flujo** (a diferencia de
`expect_or_fail`); es un segundo destino de escritura del mismo lote.

## 7. Consecuencias

- **Positivas:** trazabilidad completa (qué, cuánto, cuándo, por qué); conciliación
  exacta y verificable; reprocesamiento posible desde el registro original; esquema
  estable ante el crecimiento del catálogo de reglas (añadir una regla solo añade un
  posible valor al array, no una columna); consultas por regla vía `explode`.
- **Negativas / deuda:** filtrar por una regla concreta exige `array_contains`/`explode`
  (no una columna booleana directa); el conteo por regla no suma al total de filas en
  cuarentena (hay que distinguir "filas" de "ocurrencias de causa"), algo ya documentado
  en las métricas. La bandera `en_agua` (NULIFICAR, ADR-010) queda pendiente de
  enriquecer por una fricción del cómputo serverless Spark Connect
  (`evidence/.../materializacion_plata.md`), pero no afecta a la cuarentena ni al
  invariante.

## 8. Cómo se verifica / reversibilidad

- **Verificación observable:** `SELECT count(*) FROM oceanwatch.quarantine.ais_rejected`
  = 39.734; `explode(causas)` da `mmsi_mid_valido`=39.734 y `mmsi_bien_formado`=21.958;
  el invariante `bronce = plata + cuarentena + duplicados` cuadra (diferencia 0). Test
  unitario de la aritmética en `tests/test_conciliacion.py`
  (`verificar_invariante(25897493, 25857101, 39734, 658) -> cuadra=True`).
- **Reversibilidad:** la tabla se regenera por `CREATE OR REPLACE`/`overwrite`
  idempotente desde bronce (append-only, intacta). Cambiar la representación (p. ej. a
  la alternativa B) solo exige reescribir la proyección final del CTAS; no se pierde dato
  porque bronce es la fuente inmutable.

## 9. Referencias

- `docs/adr/ADR-006-dedup-watermark.md` (dedup bronce->silver; los 658 duplicados). Acceso 2026-11-05.
- `docs/adr/ADR-009-catalogo-reglas-calidad.md` (catálogo de reglas; Data Quality Gate). Acceso 2026-11-05.
- `docs/adr/ADR-010-mascara-tierra-agua.md` (bandera `en_agua`, NULIFICAR). Acceso 2026-11-05.
- `evidence/req02-capas-calidad/diagnostico_calidad_bronce.md` (umbrales reales). Acceso 2026-11-05.
- `evidence/req02-capas-calidad/materializacion_plata.md` y `invariante_conciliacion.md` (conteos reales). Acceso 2026-11-05.
- Patrón Dead Letter Queue / Dead-letter Channel: Hohpe & Woolf, *Enterprise Integration Patterns* — https://www.enterpriseintegrationpatterns.com/patterns/messaging/DeadLetterChannel.html. Acceso 2026-11-05.
- Databricks — Expectativas de calidad (`expect`, `expect_or_drop`, `expect_or_fail`): https://docs.databricks.com/aws/en/dlt/expectations. Acceso 2026-11-05.
