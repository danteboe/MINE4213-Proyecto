# Evidencia — Idempotencia y Backfill del Job (FEAT-002)

- **Qué demuestra:**
  1. **Idempotencia:** re-correr la MISMA fecha (`2023-06-03`) deja los conteos de
     todas las capas IGUALES (el pipeline es reejecutable sin duplicar ni alterar datos).
  2. **Backfill:** correr una fecha PASADA distinta del default (`2023-06-01`) termina en
     SUCCESS y mantiene los conteos estables y el invariante cuadrando.
- **Cómo se generó:** `databricks jobs run-now --json @_run_*.json` (parámetro
  `fecha_proceso`) + polling `jobs get-run`; conteos reales leídos con un script temporal
  `_conteos.py` (FUERA del repo) que importa `_dbx_sql.py` (`run_sql` + `extract_rows`,
  warehouse serverless `377600528fed7c47`). CLI v1.24.0.
- **Fecha:** 2026-11-05.
- **`job_id`:** `202547459111516`.
- **Estado:** Verificado (ejecutado).

## 1. Idempotencia — re-correr `fecha_proceso = 2023-06-03`

- **`run_id` corrida base (1ª vez):** `249039226964396` -> SUCCESS.
- **`run_id` re-corrida (idempotencia):** `768406241318622` -> SUCCESS (trigger `ONE_TIME`).

Resultado de la re-corrida (literal):

```
RUN 768406241318622 | life_cycle=TERMINATED | result=SUCCESS | trigger=ONE_TIME
  ingesta        | TERMINATED  | SUCCESS | attempt=0 | run_id=804281928882231
  plata_calidad  | TERMINATED  | SUCCESS | attempt=0 | run_id=33005172705428
  conciliacion   | TERMINATED  | SUCCESS | attempt=0 | run_id=751442770345577
  oro            | TERMINATED  | SUCCESS | attempt=0 | run_id=998133234000351
```

Conteos ANTES vs DESPUÉS de la re-corrida (iguales => idempotente):

| Objeto                         | ANTES (tras base) | DESPUÉS (tras re-corrida) | ¿Igual? |
|--------------------------------|-------------------|---------------------------|---------|
| bronce `ais_raw`               | 25 897 493        | 25 897 493                | sí      |
| plata `ais_clean`              | 25 857 101        | 25 857 101                | sí      |
| cuarentena `ais_rejected`      | 39 734            | 39 734                    | sí      |
| gold `trayectorias_diarias`    | 61 768            | 61 768                    | sí      |
| gold `presencia_puertos`       | 9 153             | 9 153                     | sí      |
| gold `matriz_trafico`          | 2 319 365         | 2 319 365                 | sí      |
| gold `world_port_index`        | 3 669             | 3 669                     | sí      |
| silver `celda_en_agua`         | 726 513           | 726 513                   | sí      |
| quality `ingesta_metricas`     | 3                 | 3                         | sí      |

Bronce por fecha (estable en ambas lecturas):
`2023-06-01 = 8 808 904 ; 2023-06-02 = 9 052 241 ; 2023-06-03 = 8 036 348`.

**Por qué es idempotente (ADR-014):** la ingesta usa Auto Loader (checkpoint +
`txnAppId`/`txnVersion`) y descarga/extracción idempotente, así que re-ingerir la misma
fecha no añade filas a bronce; plata y oro se recomputan por `CREATE OR REPLACE TABLE AS
SELECT` (reconstrucción total determinista: mismos conteos); `en_agua` se enriquece por
`MERGE`; la conciliación solo lee.

## 2. Backfill — `fecha_proceso = 2023-06-01` (fecha pasada)

- **`run_id` backfill:** `406283395698923` -> SUCCESS (trigger `ONE_TIME`).

Resultado (literal):

```
RUN 406283395698923 | life_cycle=TERMINATED | result=SUCCESS | trigger=ONE_TIME
  ingesta        | TERMINATED  | SUCCESS | attempt=0 | run_id=376127540931959
  plata_calidad  | TERMINATED  | SUCCESS | attempt=0 | run_id=96770753752246
  conciliacion   | TERMINATED  | SUCCESS | attempt=0 | run_id=547492740026363
  oro            | TERMINATED  | SUCCESS | attempt=0 | run_id=601903446038567
```

Conteos DESPUÉS del backfill (iguales a los de arriba => el backfill de una fecha ya
presente en bronce no altera nada; Auto Loader no re-ingiere, y plata/oro recomputan los
mismos números):

| Objeto                         | DESPUÉS del backfill (2023-06-01) |
|--------------------------------|-----------------------------------|
| bronce `ais_raw`               | 25 897 493                        |
| plata `ais_clean`              | 25 857 101                        |
| cuarentena `ais_rejected`      | 39 734                            |
| gold `trayectorias_diarias`    | 61 768                            |
| gold `presencia_puertos`       | 9 153                             |
| gold `matriz_trafico`          | 2 319 365                         |
| gold `world_port_index`        | 3 669                             |
| silver `celda_en_agua`         | 726 513                           |
| quality `ingesta_metricas`     | 3                                 |

**Invariante de conciliación (ADR-006/ADR-011):**
`plata + cuarentena + duplicados = 25 857 101 + 39 734 + 658 = 25 897 493 = bronce` -> CUADRA
(la tarea `conciliacion` terminó en SUCCESS en las tres corridas, de lo contrario habría
fallado el Job y bloqueado la publicación de oro).

> **Nota de alcance:** el corpus verificado abarca 2023-06-01/02/03, ya presentes en
> bronce. Por eso el backfill de `2023-06-01` demuestra la ruta de reproceso de una fecha
> pasada SIN alterar el estado (idempotente). Para una fecha NUEVA del feed AIS (resiliencia
> ante datos nuevos), el mismo `run-now` con otra `fecha_proceso` descargaría e ingeriría
> ese día; el camino es idéntico y está diseñado para ello.
