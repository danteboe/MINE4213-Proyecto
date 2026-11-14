# Guía de sustentación — OceanWatch Analytics (Entrega 2)

Material de **apoyo** para la defensa **presencial** (última clase). No es un entregable de
software: la sustentación se hace en vivo. Esta guía está pensada para que **cualquiera de los 3
integrantes** pueda responder cualquier parte. Curso MINE 4213.

Equipo: Alejandro Abril (202224328), David Fernando Caro (202222073),
Juan David Torres Albarracín (202317608).

---

## 1. Arquitectura en una página

**Flujo end-to-end:**

```
NOAA AIS (zip diario)
   │  descarga + verificación de integridad (Content-Length, SHA-256, zip válido)
   ▼
Volume bronze.landing  ──(descompresión → CSV)──►  Auto Loader (Trigger.availableNow, esquema explícito)
   ▼
bronze.ais_raw  (25.897.493)
   │  compuertas de calidad + dedup (MMSI+BaseDateTime, watermark 24h)
   ├───► quarantine.ais_rejected (39.734)   +  duplicados descartados (658)
   ▼
silver.ais_clean (25.857.101)   ◄── silver.celda_en_agua (726.513, máscara tierra/agua)
   │  modelo dimensional (el oro NO limpia)
   ▼
gold.*  trayectorias_diarias (61.768) · presencia_puertos (9.153) · matriz_trafico (2.319.365)
        matriz_trafico_diaria (1.027.635) · anomalias_velocidad (67.828) · world_port_index (3.669)
   ├───► Dashboard AI/BI (4 visuales + parámetro de fecha, solo oro)
   └───► Modelo @champion (IsolationForest) → anomalias_velocidad
```

**Invariante que hay que saber de memoria:**
`25.897.493 (bronce) = 25.857.101 (plata) + 39.734 (cuarentena) + 658 (duplicados)` → diferencia 0.

**Orquestación:** Job `oceanwatch_pipeline_entrega2` (`job_id 202547459111516`), 5 tareas
`ingesta → plata_calidad → conciliacion → oro → ml_anomalias`, cron de producción
`0 0 6 * * ?` (America/Bogota, PAUSED), reintentos por tarea.

**Decisiones clave (enlazadas a ADRs):**

- Medallion + Delta sobre serverless — [ADR-001](adr/ADR-001-arquitectura-medallion-serverless.md).
- Ingesta zip NOAA + Auto Loader — [ADR-003](adr/ADR-003-estrategia-ingesta-zip-autoloader.md),
  modo `Trigger.availableNow` — [ADR-004](adr/ADR-004-modo-trigger-availableNow.md).
- Dedup con watermark — [ADR-006](adr/ADR-006-dedup-watermark.md).
- Reglas de calidad (descartar vs nulificar) — [ADR-009](adr/ADR-009-catalogo-reglas-calidad.md);
  máscara tierra/agua — [ADR-010](adr/ADR-010-mascara-tierra-agua.md);
  cuarentena — [ADR-011](adr/ADR-011-estrategia-cuarentena.md).
- Modelo oro dimensional — [ADR-012](adr/ADR-012-modelo-oro.md).
- Job + idempotencia + schedule — [ADR-013](adr/ADR-013-diseno-job-orquestacion.md),
  [ADR-014](adr/ADR-014-idempotencia-reintentos.md), [ADR-015](adr/ADR-015-schedule-monitoreo.md).
- ML de anomalías SOG — [ADR-005](adr/ADR-005-eleccion-ml-anomalias-velocidad.md) +
  [ADR-016](adr/ADR-016-modelo-anomalias-sog-mlflow.md).
- Dashboard AI/BI — [ADR-017](adr/ADR-017-dashboard-aibi-oro.md).
- Gobernanza (grants + linaje + comentarios) — [ADR-018](adr/ADR-018-gobernanza-permisos-linaje.md).

---

## 2. Guion de demostración en vivo

Orden sugerido (~10–12 min). Antes de empezar: iniciar sesión en
`https://dbc-a568bc49-bdae.cloud.databricks.com` y **arrancar el warehouse `377600528fed7c47`**
(tarda ~10–20 s en frío; hacerlo al inicio para que esté caliente).

1. **El Job y sus corridas (Req. 3).** Workflows → `oceanwatch_pipeline_entrega2`. Mostrar las 5
   tareas encadenadas, el cron y el historial con corridas `SUCCESS` (incluidas las 3 `SCHEDULED`).
   *Comando alterno:* `databricks jobs get 202547459111516`.
2. **La invariante (Req. 2).** En un editor SQL, contar bronce/plata/cuarentena y mostrar que
   `25.897.493 = 25.857.101 + 39.734 + 658`. Evidencia guardada:
   `evidence/req02-capas-calidad/invariante_conciliacion.md`.
3. **El dashboard (Req. 4).** Abrir la URL publicada
   (`.../dashboardsv3/01f1da0367841f3db6c6ddcb8edaa849/published`), mostrar los 4 visuales con
   `fecha = 2023-06-03` y cambiar el filtro a `2023-06-02` (V4 queda vacío → comportamiento del
   corpus).
4. **El modelo en UC (Req. 6).** Catalog → `oceanwatch.ml.detector_anomalias_velocidad` → versión
   `1`, alias `@champion`. Mostrar que `gold.anomalias_velocidad` = 67.828 filas y que la tarea
   `ml_anomalias` corre dentro del Job.
5. **Gobernanza (Req. 5), si hay tiempo.** `SHOW GRANTS ON SCHEMA oceanwatch.gold` y la pestaña
   Lineage de `silver.ais_clean`.

**Plan B (si algo falla en vivo):**

- *El warehouse no arranca / consulta lenta:* mostrar la **evidencia guardada** en `evidence/`
  (los `.md` tienen los conteos y JSON reales) en lugar de ejecutar en vivo.
- *El dashboard no renderiza mapas:* los visuales caen a *scatter* lat×lon (supuesto D5 de
  ADR-017); igual cumple. Mostrar `evidence/req04-dashboard/consultas_visuales.md`.
- *No hay red / sesión caída:* todo el relato se sostiene con `README.md`,
  `traceability_matrix.md` y las carpetas `evidence/req01..req06`.
- *Preguntan por un conteo exacto:* están todos en el README (inventario) y en los `.md` de
  evidencia.

---

## 3. Banco de preguntas probables (con respuestas)

**Por requisito y por decisión; cada respuesta enlaza su ADR.**

- **¿Por qué `Trigger.availableNow` y no streaming continuo?** Procesamos lotes diarios de NOAA; no
  hay un flujo en tiempo real que justifique un clúster siempre encendido. `availableNow` procesa
  todo lo nuevo y se detiene, con semántica incremental y costo acotado en serverless — ADR-004.
- **¿Cómo resiste la llegada de datos nuevos de AIS?** Auto Loader con checkpoints en Volume
  (ADR-007) y esquema explícito: archivos nuevos se ingieren incrementalmente sin reprocesar; el
  Job es idempotente y parametrizado por `fecha_proceso` (backfill trivial) — ADR-003/004/014.
- **¿Por qué la regla "en agua" nulifica el atributo y no descarta la fila?** Descartar perdería
  posiciones válidas de puertos/canales (la grilla marca ~71.65% "en tierra"); nulificamos la
  bandera y conservamos la fila. Se descarta solo lo que viola reglas duras — ADR-009/010.
- **¿Por qué SQL CTAS (`spark.sql`) y no `df.write.saveAsTable`?** `saveAsTable` sobre ~25.9M filas
  en Spark Connect serverless daba `INTERNAL_ERROR`; el mismo pipeline como CTAS en el warehouse
  serverless reproduce exactamente las transformaciones y es fiable e idempotente (DELETE+INSERT /
  CREATE OR REPLACE). Fricción documentada en ADR-013 y en la bitácora.
- **¿Cómo prueban la cuarentena?** Patrón Dead-letter/Quarantine (ADR-011): cada fila sucia va a
  `quarantine.ais_rejected` con sus causas (`array<string>`). La prueba de compuerta inyectó 9
  filas en un sandbox y verificó que ningún registro sucio llegó a plata ni a oro
  (`evidence/req02-capas-calidad/prueba_compuerta.md`).
- **¿Cómo evitan data leakage en el modelo?** Partición temporal: train 2023-06-01/02, test
  2023-06-03; las estadísticas por tipo (`z_sog`) se calculan **solo con el train** — ADR-016.
- **¿Por qué IsolationForest y no un modelo supervisado?** No hay etiquetas de "anomalía real"
  (no supervisado). El objetivo es la integración MLOps, no maximizar precisión; validamos con
  acuerdo 98.86% frente a un baseline z-score — ADR-005/016.
- **¿Por qué la grilla por redondeo lat/lon y no H3?** Heredada de Entrega 1 y suficiente para la
  consulta objetivo (día + zona); se evaluó H3 y no aportaba frente al redondeo a 2 decimales para
  este caso — ADR-001/012.
- **¿El dashboard consume algo fuera de oro?** No: los 4 datasets leen solo
  `gold.matriz_trafico_diaria`, `gold.presencia_puertos` y `gold.anomalias_velocidad` (0
  referencias a bronze/silver) — ADR-017, `confirmacion_solo_oro.md`.
- **¿Cómo garantizan mínimo privilegio?** 4 grupos de rol de cuenta; cada capa recibe solo lo que
  su rol necesita (p. ej. `bronze`/`quarantine` solo ingeniería; `gold` también consumidores del
  dashboard en SELECT). Verificado por `SHOW GRANTS`, 0 grants sobre `account users`/`admins` —
  ADR-018.
- **¿Qué pasa si una tarea del Job falla?** Reintentos por tarea (ingesta 2 / plata 1 / oro 1 /
  ml 1) y compuerta Write-Audit-Publish; se demostró un reintento real (attempt 0 FAILED → attempt
  1 SUCCESS) — ADR-014, `evidence/req03-orquestacion/reintento.md`.
- **¿Dónde está el linaje?** Verificado por API (tabla y columna): `silver.ais_clean` ←
  `bronze.ais_raw` + `silver.celda_en_agua`, → las 4 tablas de oro — `linaje.md`. La captura de UI
  queda pendiente del equipo (no automatizable).

---

## 4. Las 10 decisiones más defendibles y las 5 más débiles (con mitigación)

### 10 decisiones más defendibles

1. **Invariante de conciliación con diferencia 0** (bronce = plata + cuarentena + duplicados):
   prueba cuantitativa de que no se pierde ni se inventa un solo registro. ADR-011.
2. **Arquitectura medallion + Delta en serverless**, coherente de bronce a oro. ADR-001.
3. **Job como código (DAB)** con 5 tareas, reintentos, schedule y auditoría — reproducible y
   versionado. ADR-013/014/015.
4. **Corridas SCHEDULED reales** capturadas (no solo manuales), distinguibles por `trigger`. ADR-015.
5. **Cuarentena con causas tipadas** (`array<string>`) y prueba de compuerta end-to-end. ADR-009/011.
6. **Modelo en Unity Catalog `@champion` corriendo dentro del Job**, con antifuga temporal. ADR-016.
7. **Dashboard que consume solo oro**, con parámetro de fecha validado. ADR-017.
8. **Gobernanza de mínimo privilegio** verificada por `SHOW GRANTS` + linaje por API. ADR-018.
9. **Esquema explícito + checkpoints en Volume**, resistente a datos nuevos de AIS. ADR-003/004/007.
10. **18 ADRs con alternativas y evidencia + bitácora con errores reales** (síntoma→causa→solución).

### 5 decisiones más débiles (honestas) y su mitigación

1. **Modelo no supervisado sin ground truth fuerte.** *Mitigación:* el objetivo es la integración
   MLOps; antifuga temporal, acuerdo 98.86% con baseline, modelo versionado y re-entrenable.
2. **Máscara tierra/agua marca ~72% "en tierra"** por puertos/canales en la grilla a 2 decimales.
   *Mitigación:* se nulifica la bandera, no se descarta la fila; decisión documentada en ADR-010.
3. **Corpus de solo 3 días** (2023-06-01..03). *Mitigación:* pipeline idempotente y parametrizado
   por fecha → backfill de más días sin cambios de código.
4. **Capturas de dashboard/linaje pendientes.** *Mitigación:* ya verificado por API/SQL;
   procedimiento exacto en `pendientes_equipo.md`.
5. **Escritura vía SQL CTAS en vez de `df.write`** por fricción del serverless. *Mitigación:* el
   CTAS reproduce exactamente las transformaciones; los notebooks quedan como orquestador de
   referencia; documentado en ADR-013 y en la bitácora.

> Fuera de alcance por decisión del equipo (no son debilidades a defender): **Delta Sharing** y el
> **pronóstico de arribos** (ADR-005).
