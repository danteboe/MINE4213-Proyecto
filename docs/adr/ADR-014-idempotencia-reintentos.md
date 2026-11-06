# ADR-014: Idempotencia por tarea, reintentos y compuerta Write-Audit-Publish

- **Estado:** Aceptado
- **Fecha:** 2026-11-05
- **Requisito(s) de la rúbrica:** Req. 3 (Orquestación, 20%)
- **Autores:** Alejandro Abril (202224328), David Fernando Caro (202222073), Juan David Torres Albarracín (202317608)

## 1. Contexto y problema

El Job de orquestación (ADR-013) debe poder **reejecutarse y reprocesar fechas
(backfill)** sin duplicar ni corromper datos, y debe **detenerse** si la calidad no
cuadra antes de publicar la capa oro. Hay que decidir, por tarea: qué la hace
idempotente, cuántos reintentos tiene sentido y qué fallos son reintentables vs. no
(un reintento ciego de un error no transitorio solo esconde el problema). El entorno es
serverless (UTC por defecto, ADR-004), la escritura pesada es CTAS (no `df.write`,
ADR-013), y el invariante de conciliación es `bronce = plata + cuarentena + duplicados`
(ADR-006/ADR-011), verificado real (25.897.493 = 25.857.101 + 39.734 + 658).

## 2. Criterios de decisión

- Idempotencia: reejecutar una tarea para la misma `fecha_proceso` debe dar el mismo estado.
- Reintentos proporcionales a la naturaleza del fallo (transitorio vs. determinista).
- Seguridad de publicación: no publicar oro si plata/cuarentena no concilian.
- Backfill reproducible por `fecha_proceso`.
- Observabilidad: registrar cada corrida+tarea sin duplicar filas de auditoría.

## 3. Alternativas consideradas

### 3.1 Reintentos de la tarea de conciliación (la compuerta)

| Alternativa | Descripción | Pros | Contras | Evidencia |
|---|---|---|---|---|
| A. `max_retries: 0` en conciliación | La compuerta NO reintenta; si el invariante no cuadra, el Job falla | Un invariante que no cuadra NO es transitorio; reintentar no lo arregla; falla rápido y visible | Requiere intervención humana (correcto para una compuerta) | ADR-006/ADR-011 (invariante); `src/oceanwatch/quality/conciliacion.py` |
| B. Reintentar la conciliación N veces | Reintentar como las demás tareas | "Uniforme" | Oculta un problema de datos real; retrasa la alerta sin resolver nada | — |

### 3.2 Idempotencia de la escritura de capas

| Alternativa | Descripción | Pros | Contras | Evidencia |
|---|---|---|---|---|
| A. `CREATE OR REPLACE TABLE AS SELECT` (plata/oro) + `MERGE` (en_agua, auditoría) | Reconstrucción total determinista; MERGE por clave para enriquecer/auditar | Reejecutable; mismos conteos; sin duplicados | Recomputa toda la tabla (coste) | `resources/sql/fase3_materializar_plata.sql`, `fase3_materializar_oro.sql`, `fase3_enriquecer_en_agua.sql` (MERGE); `src/oceanwatch/orquestacion/__init__.py` (`merge_auditoria_sql`) |
| B. `INSERT INTO` incremental sin dedup/merge | Append por corrida | Barato | Duplica filas al reejecutar; rompe la idempotencia | — |

### 3.3 Idempotencia de la ingesta

| Alternativa | Descripción | Pros | Contras | Evidencia |
|---|---|---|---|---|
| A. Auto Loader checkpoint + `txnAppId`/`txnVersion` + descarga/extracción idempotente | Exactly-once sobre bronce append-only (ADR-004); no reextrae CSV existente | Reejecución segura; no duplica en bronce | El checkpoint debe conservarse | `src/oceanwatch/ingest/autoloader.py` (`_escribir_batch`), `download.py` (`descomprimir_a_landing`), ADR-004 |
| B. Append sin checkpoint ni txn | Reingerir todo cada vez | Simple | Duplica filas; rompe la idempotencia | — |

## 4. Decisión

Cada tarea del Job es **idempotente por construcción** y los reintentos son **por tarea
según la naturaleza del fallo**: ingesta `max_retries: 2` (red NOAA + arranque serverless
= transitorios), plata_calidad `1`, oro `1` (CTAS determinista; reintento solo por fallo
transitorio de arranque) y **conciliación `0`** (compuerta Write-Audit-Publish: un
invariante que no cuadra NO es transitorio, el Job debe fallar); el reproceso/backfill se
hace por el parámetro `fecha_proceso`.

## 5. Justificación basada en evidencia

La ingesta es exactly-once por checkpoint + `txnAppId`/`txnVersion` (3.3.A, ADR-004) y la
descarga/extracción no reextrae CSV ya presentes (`download.descomprimir_a_landing`), así
que reejecutar la misma fecha no duplica filas en bronce. Plata y oro usan
`CREATE OR REPLACE TABLE AS SELECT` (3.2.A), reconstrucción total determinista que
reproduce los conteos verificados (plata = 25.857.101, cuarentena = 39.734); el
enriquecimiento `en_agua` y la auditoría usan `MERGE` por clave (en_agua por celda;
auditoría por `(job_run_id, task_key)` — `merge_auditoria_sql`), de modo que un reintento
de la misma tarea en la misma corrida **actualiza** su fila de auditoría en vez de
duplicarla. La conciliación NO reintenta (3.1.A): si `bronce != plata + cuarentena +
duplicados`, `verificar_invariante` devuelve `cuadra = False` y el notebook hace `raise`,
fallando el Job **antes** de publicar oro — reintentar (3.1.B) solo escondería un problema
de datos. Los valores de reintento viven en `conf/config.yaml` (bloque `orquestacion`) y
se reflejan en el YAML del Job. La no-ruptura de las tablas existentes se verificó
(conteos idénticos a los de fase 3; `evidence/req03-orquestacion/tabla_auditoria.md`).

## 6. Patrón de diseño / ingeniería de datos aplicado

**Idempotent pipeline** (reconstrucción determinista por `CREATE OR REPLACE` + MERGE por
clave + exactly-once en la ingesta), **Write-Audit-Publish (WAP)** (la conciliación es la
compuerta que audita y bloquea la publicación de oro) y **Retry with backoff** (reintentos
por tarea con `min_retry_interval_millis`, calibrados por la naturaleza del fallo).

## 7. Consecuencias

- Positivas: reejecución y backfill seguros; fallos de datos visibles y bloqueantes (no se
  publica oro con datos que no concilian); auditoría sin duplicados por `MERGE`.
- Negativas: `CREATE OR REPLACE` recomputa toda la tabla (coste frente a un incremental);
  la conciliación sin reintentos exige intervención humana ante un invariante roto (es el
  comportamiento deseado de una compuerta).
- Deuda asumida: el backfill actual reconstruye las capas completas (no particiona por
  fecha dentro de plata/oro); una materialización incremental por partición sería una
  optimización futura. La validación de los reintentos y de la compuerta con corridas
  reales ya se ejecutó (FEAT-002; ver §8).

## 8. Cómo se verifica / reversibilidad

- Verificación (parte Verificado): `python -m pytest -q` cubre `verificar_invariante`
  (`tests/test_conciliacion.py`) y los helpers de auditoría/MERGE
  (`tests/test_orquestacion.py`); la tabla de auditoría se creó idempotente
  (`CREATE TABLE IF NOT EXISTS`, reejecutable) y los conteos de las tablas del pipeline
  quedaron intactos (`tabla_auditoria.md`).
- Verificación (EJECUTADO, FEAT-002; `job_id` 202547459111516): (a) **idempotencia** =
  re-correr `fecha_proceso=2023-06-03` (`run_id` base 249039226964396 y re-corrida
  768406241318622, ambas SUCCESS) dejó los conteos IGUALES (bronce 25.897.493, plata
  25.857.101, cuarentena 39.734, oro trayectorias 61.768 / presencia 9.153 / matriz
  2.319.365); (b) **backfill** = `fecha_proceso=2023-06-01` (`run_id` 406283395698923,
  SUCCESS) mantuvo los conteos estables y el invariante cuadrando
  (25.857.101 + 39.734 + 658 = 25.897.493); (c) **reintento** real = job efímero de una
  tarea `max_retries:1` (`run_id` 626548229252476) con attempt 0 FAILED
  (789377104890365) seguido de attempt 1 SUCCESS (1054283792195712); la **compuerta**
  (conciliación `max_retries:0`) terminó SUCCESS en las 3 corridas porque el invariante
  cuadró — de no cuadrar habría hecho `raise` y fallado el Job antes de `oro`. Evidencia:
  `evidence/req03-orquestacion/backfill_idempotente.md` y `.../reintento.md`.
- Reversibilidad: las capas se regeneran reejecutando los CTAS (`CREATE OR REPLACE`); la
  auditoría se limpia con `DROP TABLE IF EXISTS oceanwatch.quality.ejecuciones_job`.

## 9. Referencias

- Databricks Jobs — reintentos de tareas: https://docs.databricks.com/aws/en/jobs/ (acceso 2026-11-05).
- Delta Lake `MERGE INTO`: https://docs.databricks.com/aws/en/delta/merge (acceso 2026-11-05).
- Auto Loader (exactly-once, checkpoint): https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/ (acceso 2026-11-05).
- Patrón Write-Audit-Publish: https://lakefs.io/blog/data-engineering-patterns-write-audit-publish/ (acceso 2026-11-05).
- ADR-004 (idempotencia txn / UTC serverless), ADR-006 (dedup/invariante), ADR-011 (cuarentena), ADR-013 (diseño del Job).
- Evidencia interna: `evidence/req03-orquestacion/tabla_auditoria.md`, `.../definicion_job.md`.
