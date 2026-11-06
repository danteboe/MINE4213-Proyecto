# ADR-015: Agendamiento (cron Quartz + timezone) y monitoreo/alertas del Job

- **Estado:** Aceptado
- **Fecha:** 2026-11-05
- **Requisito(s) de la rúbrica:** Req. 3 (Orquestación, 20%)
- **Autores:** Alejandro Abril (202224328), David Fernando Caro (202222073), Juan David Torres Albarracín (202317608)

## 1. Contexto y problema

El Job `oceanwatch_pipeline_entrega2` (`job_id` **202547459111516**, ADR-013) ya corre
limpio bajo demanda (FEAT-002: corridas base/idempotencia/backfill en SUCCESS). Falta
decidir **cuándo se dispara solo** y **cómo se observa/alerta**. El feed AIS de NOAA se
publica a diario; el cómputo es **serverless**, que corre en **UTC** por defecto (ADR-004),
pero el equipo opera en `America/Bogota`. Hay que fijar: (a) la expresión cron de
producción y su zona horaria, justificando UTC vs. hora local; (b) cómo demostrar corridas
disparadas por el scheduler sin esperar al horario de producción; y (c) el mecanismo de
monitoreo (notificaciones + auditoría persistente) para saber si una corrida tuvo éxito,
falló o se alargó.

## 2. Criterios de decisión

- Alinear el disparo con la disponibilidad del dato (tras la publicación diaria de NOAA).
- Claridad horaria: la hora programada debe entenderse sin ambigüedad pese a que serverless
  corre en UTC.
- Demostrabilidad: poder evidenciar corridas **SCHEDULED** reales en una ventana corta.
- Seguridad operativa: no disparar corridas no deseadas al desplegar (crear PAUSED).
- Observabilidad: enterarse de éxito/fallo/duración anómala sin vigilar la UI.
- Idempotencia del disparo: que una corrida automática no corrompa datos (ADR-014).

## 3. Alternativas consideradas

### 3.1 Zona horaria del cron

| Alternativa | Descripción | Pros | Contras | Evidencia |
|---|---|---|---|---|
| A. `timezone_id: America/Bogota` | El Quartz cron del Job se interpreta en hora de Bogotá | Hora legible por el equipo; el Job respeta `timezone_id` aunque serverless corra en UTC; sin aritmética mental de offset | Hay que recordar que 06:00 Bogota = 11:00 UTC al leer logs en UTC | Databricks Jobs schedule / `timezone_id` (ver §9); `jobs get` muestra `timezone_id=America/Bogota` |
| B. `timezone_id: UTC` | Cron en UTC (como corre serverless) | Coincide con la zona interna del cómputo | El horario deja de ser legible para el operador local; riesgo de programar a una hora equivocada | ADR-004 (serverless en UTC) |

### 3.2 Expresión cron de producción

| Alternativa | Descripción | Pros | Contras | Evidencia |
|---|---|---|---|---|
| A. `0 0 6 * * ?` (06:00 diario) | Una corrida diaria a las 06:00 Bogota | 06:00 Bogota = 11:00 UTC: margen amplio tras la publicación diaria del feed NOAA; barato | Latencia de hasta ~24 h para un dato muy reciente | `conf/config.yaml` (`orquestacion.cron_produccion`); feed diario NOAA |
| B. Cron cada pocas horas | Varias corridas al día | Menor latencia | El dato es diario: recomputar varias veces desperdicia cómputo serverless sin aportar datos nuevos | — |

### 3.3 Cómo demostrar corridas disparadas por el scheduler

| Alternativa | Descripción | Pros | Contras | Evidencia |
|---|---|---|---|---|
| A. Cron temporal de alta frecuencia `0 */5 * * * ?` (cada 5 min) UNPAUSED por una ventana corta, luego restablecer producción | Activar un cron rápido ~15 min para capturar 2-3 corridas PERIODIC y volver al cron diario PAUSED | Evidencia real de corridas SCHEDULED sin esperar 24 h; no destructivo (idempotente, misma `fecha_proceso`) | Hay que acordarse de restablecer el cron de producción | `evidence/req03-orquestacion/corridas_scheduled.md`; `jobs list-runs` con `trigger=PERIODIC` |
| B. Esperar al cron de producción (06:00) | No tocar nada | Cero cambios | No demostrable en la ventana de trabajo; no aporta evidencia | — |

### 3.4 Mecanismo de monitoreo/alertas

| Alternativa | Descripción | Pros | Contras | Evidencia |
|---|---|---|---|---|
| A. Notificaciones email `on_success`/`on_failure`/`on_duration_warning` + health `RUN_DURATION_SECONDS > 3600` + tabla de auditoría `quality.ejecuciones_job` | Alertas push por correo y registro persistente consultable por corrida+tarea | Entera de fallos sin vigilar la UI; auditoría histórica consultable con SQL; umbral de duración anómala | El correo depende de la bandeja del destinatario | `jobs get` (bloque `email_notifications` + `health`); `evidence/req03-orquestacion/tabla_auditoria.md` |
| B. Solo revisar la UI de Workflows | Mirar el estado manualmente | Cero configuración | No escala; nadie se entera de un fallo nocturno; sin histórico estructurado | — |

## 4. Decisión

El Job se agenda con **cron Quartz `0 0 6 * * ?` en timezone `America/Bogota`** (06:00
Bogota diario, creado `PAUSED`) como estado de **producción**, y la demostración de corridas
disparadas por el scheduler se hace con un **cron temporal de alta frecuencia
`0 */5 * * * ?` (cada 5 minutos) UNPAUSED por una ventana corta, restableciendo luego el
cron de producción**; el **monitoreo** combina **notificaciones email**
(`on_success`/`on_failure`/`on_duration_warning_threshold_exceeded`) con el **health rule
`RUN_DURATION_SECONDS > 3600`** y la **tabla de auditoría** `oceanwatch.quality.ejecuciones_job`.

## 5. Justificación basada en evidencia

El dato de NOAA es **diario**, así que una corrida diaria a las 06:00 Bogota (= 11:00 UTC)
da margen tras la publicación del feed sin recomputar en vano (3.2.A frente a 3.2.B). Se fija
`timezone_id: America/Bogota` (3.1.A) para que el horario sea legible por el operador pese a
que serverless corre en UTC (ADR-004): el Job respeta `timezone_id`, confirmado por
`databricks jobs get --job-id 202547459111516` (`schedule.timezone_id = America/Bogota`,
`quartz_cron_expression = 0 0 6 * * ?`). Las corridas disparadas por el scheduler se
demostraron con el cron de alta frecuencia (3.3.A): tras activar `0 */5 * * * ?` UNPAUSED, el
scheduler lanzó corridas automáticas con `trigger = PERIODIC`, distinguibles de las
run-now manuales (`trigger = ONE_TIME`: run_ids 249039226964396 base, 768406241318622
idempotencia, 406283395698923 backfill — FEAT-002). Los `run_id` SCHEDULED reales, con sus
timestamps y duraciones, están en `evidence/req03-orquestacion/corridas_scheduled.md`.
Tras la captura se restableció el cron de **producción** (`0 0 6 * * ?`, `PAUSED`), estado
**final** del Job verificado con `jobs get`. El monitoreo (3.4.A) usa notificaciones email a
`jd.torresa1@uniandes.edu.co` en los tres eventos + el health rule de duración (>3600 s) +
la tabla `ejecuciones_job` (12 filas reales de las corridas de FEAT-002; una fila por
corrida+tarea con estado y conteos), consultable con SQL — preferible a vigilar la UI (3.4.B).

## 6. Patrón de diseño / ingeniería de datos aplicado

**Scheduled batch** (orquestación por cron sobre un dato de periodicidad diaria) combinado
con **Observability / Monitoring & Alerting** (notificaciones push por evento + umbral de
salud de duración + tabla de auditoría persistente como registro histórico consultable).

## 7. Consecuencias

- Positivas: disparo automático alineado con la llegada del dato; horario legible; corridas
  SCHEDULED demostradas con evidencia real; fallos y duraciones anómalas notificados sin
  vigilancia manual; auditoría histórica consultable.
- Negativas: latencia de hasta ~24 h para un dato muy reciente (aceptable para un feed
  diario); el correo depende de la bandeja del destinatario; hay que recordar restablecer el
  cron de producción tras cualquier demostración de alta frecuencia.
- Deuda asumida: el cron de producción queda `PAUSED` (el entorno del curso no debe lanzar
  corridas no supervisadas); activarlo (`UNPAUSED`) es un cambio de una línea cuando se
  opere en continuo.

## 8. Cómo se verifica / reversibilidad

- Verificación (Verificado — ejecutado): `databricks jobs list-runs --job-id 202547459111516
  --output json` muestra ≥2 corridas con `trigger = PERIODIC` (scheduler) distintas de las
  `ONE_TIME` manuales (evidencia literal en
  `evidence/req03-orquestacion/corridas_scheduled.md`); `databricks jobs get --job-id
  202547459111516 --output json` muestra el **estado final**: `quartz_cron_expression =
  0 0 6 * * ?`, `timezone_id = America/Bogota`, `pause_status = PAUSED`, bloque
  `email_notifications` con los tres eventos y `health` con `RUN_DURATION_SECONDS > 3600`;
  `SELECT ... FROM oceanwatch.quality.ejecuciones_job` devuelve las filas de auditoría.
- Reversibilidad: el schedule se cambia con `databricks jobs update --json '{"job_id":...,
  "new_settings":{"schedule":{...}}}'` (demostración ↔ producción); activar/pausar es alternar
  `pause_status`. El Job y su schedule se revierten por completo con `databricks bundle
  destroy` (no afecta a las tablas del pipeline).

## 9. Referencias

- Databricks Jobs — programación (schedule, cron Quartz, `timezone_id`): https://docs.databricks.com/aws/en/jobs/scheduled (acceso 2026-11-05).
- Sintaxis de cron Quartz: http://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/crontrigger.html (acceso 2026-11-05).
- Databricks Jobs — notificaciones y reglas de salud (`health`, `RUN_DURATION_SECONDS`): https://docs.databricks.com/aws/en/jobs/notifications (acceso 2026-11-05).
- ADR-004 (serverless en UTC + Trigger.availableNow), ADR-013 (diseño del Job), ADR-014 (idempotencia y reintentos).
- Evidencia interna: `evidence/req03-orquestacion/corridas_scheduled.md`, `.../jobs_get.md`, `.../tabla_auditoria.md`.
