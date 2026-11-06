# Evidencia — Tabla de auditoría del Job y no-ruptura de tablas existentes (FEAT-001)

- **Qué:** creación idempotente de `oceanwatch.quality.ejecuciones_job` (DDL como código)
  y comprobación de que ninguna tabla existente del pipeline fue modificada.
- **Cómo:** `resources/sql/fase4_auditoria.sql` ejecutado vía `_dbx_sql.py` (SQL Statement
  Execution API, warehouse serverless `377600528fed7c47`). Script temporal FUERA del repo.
- **Fecha:** 2026-11-05.
- **Estado:** Verificado (ejecutado).

## Diagnóstico de fricción (síntoma -> causa -> solución)

- **Síntoma:** el DDL falló con `PARSE_SYNTAX_ERROR` cerca de `';'` dentro del `COMMENT`
  de la tabla.
  **Causa:** el divisor de sentencias (split naíf por `;`, usado por `_dbx_sql.py` y por
  los notebooks) cortó el `COMMENT` que contenía un `;` literal.
  **Solución:** se reemplazó el `;` del texto del `COMMENT` por `,` para que el DDL sea
  una sola sentencia bajo cualquier divisor por `;` (los `.sql` del proyecto no usan `;`
  dentro de literales). Reejecutado: OK.

## Salida literal

```
==== 1) DDL fase4_auditoria.sql (idempotente) ====
OK    | CREATE SCHEMA IF NOT EXISTS oceanwatch.quality
OK    | CREATE TABLE IF NOT EXISTS oceanwatch.quality.ejecuciones_job ( job_ru

==== 2) SHOW TABLES IN oceanwatch.quality ====
    ['quality', 'ejecuciones_job', 'false']
    ['quality', 'ingesta_metricas', 'false']
    ['quality', 'metricas_calidad', 'false']
    ['quality', 'pruebas_fallos', 'false']

==== 3) Conteos de tablas existentes (no se rompió nada) ====
   bronce ais_raw = 25897493
   plata ais_clean = 25857101
   cuarentena ais_rejected = 39734
   silver celda_en_agua = 726513
   gold world_port_index = 3669
   quality ingesta_metricas = 3
   quality ejecuciones_job (NUEVA) = 0

==== 4) DESCRIBE de la tabla de auditoria ====
    ['job_run_id', 'bigint', 'Id de la corrida del Job ({{job.run_id}}).']
    ['job_id', 'bigint', 'Id del Job de Databricks ({{job.id}}).']
    ['task_key', 'string', 'Nombre de la tarea: ingesta/plata_calidad/conciliacion/oro.']
    ['fecha_proceso', 'string', 'Parámetro fecha_proceso (YYYY-MM-DD) de la corrida.']
    ['estado', 'string', 'Estado de la tarea (SUCCESS, FAILED, ...).']
    ['trigger', 'string', 'Origen de la corrida: manual (run-now) o scheduled (cron).']
    ['ts_inicio', 'timestamp', 'Marca de tiempo de inicio de la tarea.']
    ['ts_fin', 'timestamp', 'Marca de tiempo de fin de la tarea.']
    ['filas_clave', 'string', 'Conteos/métricas de la tarea serializados como JSON.']
    ['detalle', 'string', 'Texto libre: diagnóstico, invariante, notas.']
    ['_registrado', 'timestamp', 'Marca de tiempo de inserción/actualización del registro.']
```

## Conclusiones

- La tabla **nueva** `oceanwatch.quality.ejecuciones_job` existe, con el esquema exacto
  del DDL, 0 filas (se poblará al correr el Job en FEAT-002).
- Los conteos de bronce (25.897.493), plata (25.857.101), cuarentena (39.734),
  `silver.celda_en_agua` (726.513), `gold.world_port_index` (3.669) e
  `ingesta_metricas` (3) **coinciden exactamente** con `context.estado_real_verificado`:
  no se modificó ni borró ninguna tabla existente.

---

# FEAT-002 — Tabla de auditoría POBLADA desde el Job (real)

- **Qué demuestra:** que cada corrida del Job escribe su auditoría en
  `oceanwatch.quality.ejecuciones_job` (una fila por corrida+tarea) con el MERGE
  idempotente por `(job_run_id, task_key)`.
- **Cómo se generó:** `SELECT` real vía `_dbx_sql.py` (`run_sql` + `extract_rows`,
  warehouse serverless `377600528fed7c47`) tras las corridas base, idempotencia y backfill.
- **Fecha:** 2026-11-05.
- **`job_id`:** `202547459111516`.
- **Estado:** Verificado (ejecutado).

`SELECT count(*) FROM oceanwatch.quality.ejecuciones_job` -> **12** filas (3 corridas × 4 tareas).

Salida literal de
`SELECT job_run_id, task_key, fecha_proceso, estado, ts_inicio, ts_fin
 FROM oceanwatch.quality.ejecuciones_job ORDER BY job_run_id, task_key`:

```
job_run_id        | task_key      | fecha_proceso | estado  | ts_inicio                | ts_fin
249039226964396   | conciliacion  | 2023-06-03    | SUCCESS | 2026-11-06T12:12:07.684Z | 2026-11-06T12:12:12.047Z
249039226964396   | ingesta       | 2023-06-03    | SUCCESS | 2026-11-06T12:09:18.572Z | 2026-11-06T12:10:36.155Z
249039226964396   | oro           | 2023-06-03    | SUCCESS | 2026-11-06T12:12:21.057Z | 2026-11-06T12:12:42.209Z
249039226964396   | plata_calidad | 2023-06-03    | SUCCESS | 2026-11-06T12:10:48.676Z | 2026-11-06T12:11:57.230Z
406283395698923   | conciliacion  | 2023-06-01    | SUCCESS | 2026-11-06T12:20:37.709Z | 2026-11-06T12:20:41.605Z
406283395698923   | ingesta       | 2023-06-01    | SUCCESS | 2026-11-06T12:18:47.912Z | 2026-11-06T12:19:20.767Z
406283395698923   | oro           | 2023-06-01    | SUCCESS | 2026-11-06T12:20:51.427Z | 2026-11-06T12:21:09.344Z
406283395698923   | plata_calidad | 2023-06-01    | SUCCESS | 2026-11-06T12:19:32.223Z | 2026-11-06T12:20:28.626Z
768406241318622   | conciliacion  | 2023-06-03    | SUCCESS | 2026-11-06T12:17:03.939Z | 2026-11-06T12:17:07.622Z
768406241318622   | ingesta       | 2023-06-03    | SUCCESS | 2026-11-06T12:15:12.913Z | 2026-11-06T12:15:46.172Z
768406241318622   | oro           | 2023-06-03    | SUCCESS | 2026-11-06T12:17:17.047Z | 2026-11-06T12:17:35.592Z
768406241318622   | plata_calidad | 2023-06-03    | SUCCESS | 2026-11-06T12:15:55.978Z | 2026-11-06T12:16:55.027Z
```

Columna `filas_clave` (JSON) por tarea — muestra literal:

```
249039226964396 | ingesta       | {"duplicados_fecha": 239, "filas_fecha": 8036348, "total_bronce": 25897493}
249039226964396 | plata_calidad | {"cuarentena": 39734, "plata": 25857101}
249039226964396 | conciliacion  | {"bronce": 25897493, "cuarentena": 39734, "duplicados": 658, "plata": 25857101}
249039226964396 | oro           | {"matriz_trafico": 2319365, "presencia_puertos": 9153, "trayectorias_diarias": 61768, "world_port_index": 3669}
406283395698923 | ingesta       | {"duplicados_fecha": 184, "filas_fecha": 8808904, "total_bronce": 25897493}
```

## Conclusiones (FEAT-002)

- Las 12 filas (3 corridas × 4 tareas) reflejan las corridas base (`249039226964396`),
  idempotencia (`768406241318622`) y backfill (`406283395698923`), todas con `estado =
  SUCCESS`. El MERGE por `(job_run_id, task_key)` evitó duplicados dentro de cada corrida.
- La columna `trigger` quedó `''` (vacía): los notebooks de tarea aún no la completan.
  La distinción manual (ONE_TIME/run-now) vs scheduled (CRON/PERIODIC) se obtiene de
  `databricks jobs list-runs --job-id 202547459111516 --output json` (las 3 corridas
  figuran con `trigger = ONE_TIME` = manual). Poblar `trigger` desde el notebook es un
  refinamiento menor para FEAT-003 (scheduling/docs).
- `filas_clave` conserva los conteos reales por tarea (coherentes con
  `backfill_idempotente.md`): la invariante cuadra en conciliación en las 3 corridas.
