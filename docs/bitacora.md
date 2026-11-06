# Bitácora — OceanWatch Analytics (Entrega 2)

Registro cronológico del trabajo del equipo (Req. 7). Cada sesión se documenta con la
plantilla de abajo. Las entradas se ordenan de la más reciente a la más antigua.

## Plantilla de entrada

```markdown
### <AAAA-MM-DD> — <Fase N: nombre> — <integrante(s)>
- **Objetivo de la sesión:** ...
- **Qué se hizo:** ...
- **Decisiones/ADRs tocados:** ADR-NNN ...
- **Estado Verificado vs Diseñado:** ...
- **Evidencia:** <ruta en evidence/ o enlace>
- **Pendientes / siguiente paso:** ...
```

## Entradas

### 2026-11-05 — Fase 3: Capas de calidad (plata + cuarentena + métricas + oro) y prueba de compuerta — Equipo OceanWatch
- **Objetivo de la sesión:** cerrar el Req. 2 con **evidencia real**: materializar la capa
  **plata** con compuertas de calidad y cuarentena, las **métricas** de calidad, la capa **oro**
  (modelo dimensional de negocio) y demostrar extremo a extremo, con conteos reales, que la
  compuerta bloquea el 100% de los registros sucios por todas las rutas.
- **Qué se hizo:**
  - **Plata/cuarentena/métricas (FEAT-003):** compuertas puras (FEAT-001) + máscara tierra/agua
    (FEAT-002) envueltas como expresiones nativas en `src/oceanwatch/quality/compuertas.py`;
    materialización en serverless (SQL CTAS `resources/sql/fase3_materializar_plata.sql`, mismo
    pipeline que el notebook `notebooks/fase3_plata_calidad`). Conteos reales:
    `oceanwatch.silver.ais_clean`=**25.857.101**, `oceanwatch.quarantine.ais_rejected`=**39.734**
    (causas `mmsi_mid_valido`=39.734, `mmsi_bien_formado`=21.958; una fila cuenta una vez),
    `oceanwatch.quality.metricas_calidad`=**15** (3 días × 5 reglas de descarte), duplicados
    dedup=**658**. **Invariante** bronce = plata + cuarentena + duplicados:
    `25.897.493 = 25.857.101 + 39.734 + 658` (diferencia 0). Nulificación con banderas
    (heading_desconocido=14.338.249, cog_desconocido=4.373.778, etc.), 0 incoherencias
    bandera→null.
  - **Oro (FEAT-004):** alimentada SOLO de silver (el oro NO limpia). SQL CTAS
    `resources/sql/fase3_materializar_oro.sql` (= notebook `notebooks/fase3_oro`). Conteos reales:
    `gold.world_port_index`=**3.669** puertos (WPI convertido a CSV localmente con `dbfread`,
    sin instalar geopandas/pyshp en serverless), `gold.trayectorias_diarias`=**61.768**
    (1 fila por MMSI+día, haversine R=6371), `gold.presencia_puertos`=**9.153** (1 fila por
    MMSI+puerto+día; 349 puertos cruzados), `gold.matriz_trafico`=**2.319.365** (1 fila por
    celda+hora; grilla redondeo lat/lon 2 dec, ADR-001, NO H3).
  - **Prueba de compuerta (FEAT-005):** notebook `notebooks/fase3_prueba_compuerta` y SQL
    `resources/sql/fase3_prueba_compuerta.sql` inyectaron en un sandbox aislado
    (`oceanwatch.quality.pf_*`) **9 filas** (un sucio por cada regla de descarte + duplicado +
    3 limpios de control en la celda-puerto NEW YORK CITY). Resultado real: `pf_quarantine`=**5**
    (cada sucio con su causa correcta), `pf_silver`=**3** (solo los limpios; el duplicado lo
    eliminó la dedup), `pf_gold_trayectorias`=**1**, `pf_gold_presencia`=**1** (NEW YORK CITY,
    INDEX_NO 7640, 3 posiciones), `pf_gold_matriz`=**1**. **Ningún** sucio llegó a silver ni a
    oro (0 filas/0 celdas); invariante sandbox `9 = 3 + 5 + 1`. `statement_id` serverless
    capturados. Sandbox `pf_*` eliminado tras capturar la evidencia (`SHOW TABLES LIKE 'pf_*'`
    vacío); tablas de producción intactas.
  - Tests locales (sin Spark): `tests/test_conciliacion.py`, `tests/test_gold_puertos.py`,
    `tests/test_prueba_compuerta.py` (complemento puro de la prueba de compuerta). `pytest -q`:
    **76 passed, 4 skipped**.
- **Decisiones/ADRs tocados:** **ADR-009** (catálogo de reglas, clasificación descartar/nulificar),
  **ADR-010** (máscara tierra/agua, acción nulificar), **ADR-011** (estrategia de cuarentena,
  patrón Dead-letter/Quarantine, causas `array<string>`), **ADR-012** (modelo oro: granularidad,
  claves, 'en puerto' por celda + permanencia ≥3, haversine, grilla redondeo ratificando ADR-001).
- **Estado Verificado vs Diseñado:** **Verificado (ejecutado)** en serverless = plata, cuarentena,
  métricas, invariante, las 4 tablas oro y la prueba de compuerta extremo a extremo. Límite
  conocido (heredado de plata, documentado): `en_agua` queda en `null` en plata (requiere
  `global_land_mask`, no disponible en SQL; procedimiento de enriquecimiento documentado) y
  `distancia_total_km` puede ser no física para MMSI con posiciones AIS que "saltan" (el oro NO
  limpia; filtro de velocidad = regla de plata de fase futura).
- **Síntoma→causa→solución:**
  - *Escritura Delta serverless:* síntoma = `saveAsTable` sobre 25.9M en Spark Connect GC devolvió
    `INTERNAL_ERROR` (y antes `CACHE_REQUIRES_NEWER_CLIENT_ON_SERVERLESS_GC`); causa = limitación
    del cómputo serverless para la escritura Delta a esa escala, no de la lógica; solución (track
    distinto) = ejecutar el MISMO pipeline como **SQL CTAS** en el SQL warehouse serverless, que
    reproduce exactamente las compuertas/transformaciones. Notebooks conservados como orquestador
    de referencia.
  - *Carga del WPI:* síntoma = `read_files` devolvía 0 filas pese a archivo íntegro; causa = un
    `read_files` lanzado antes de completar la subida envenenó la caché de listado del warehouse;
    solución = subir a un path nuevo (`wpi_ref_v2/`) nunca consultado → 3.669 filas.
  - *Presencia de control en puerto (FEAT-005):* síntoma = los limpios no aparecían en
    `pf_gold_presencia`; causa = sus coordenadas redondeaban a (40.70,-74.01) y NEW YORK CITY cae
    en (40.70,-74.02); solución = ubicar los 3 pings en LON -74.016/-74.017/-74.018 (celda
    -74.02) → presencia confirmada (3 posiciones ≥ permanencia 3).
- **Evidencia:** `evidence/req02-capas-calidad/` — `diagnostico_calidad_bronce.md`,
  `disponibilidad_librerias_serverless.md`, `mascara_tierra_agua.md`, `materializacion_plata.md`,
  `invariante_conciliacion.md`, `capa_oro.md`, `prueba_compuerta.md`. Tablas creadas y
  conservadas: `oceanwatch.silver.ais_clean`, `oceanwatch.quarantine.ais_rejected`,
  `oceanwatch.quality.metricas_calidad`, `oceanwatch.gold.world_port_index`,
  `oceanwatch.gold.trayectorias_diarias`, `oceanwatch.gold.presencia_puertos`,
  `oceanwatch.gold.matriz_trafico`. Código: `src/oceanwatch/quality/`, `src/oceanwatch/gold/`,
  `notebooks/fase3_*`, `resources/sql/fase3_*`.
- **Pendientes / siguiente paso:** Fase 4 (Req. 3) — **orquestación** por Job de Databricks con
  cron en UTC e idempotencia (encadenar ingesta → plata/calidad → oro). Enriquecimiento opcional
  de `en_agua` en plata (procedimiento en `materializacion_plata.md`).

### 2026-11-05 — Fase 2: Ingesta streaming (pipeline real + pruebas de fallos + ADRs) — Equipo OceanWatch
- **Objetivo de la sesión:** cerrar el Req. 1 con **evidencia real**: construir y **ejecutar** el
  pipeline de ingesta incremental a bronce en serverless, probar su tolerancia a fallos con 4
  escenarios **ejecutados de verdad**, justificar `Trigger.availableNow` con comportamiento
  observado y documentar las decisiones (ADR-006/007/008).
- **Qué se hizo:**
  - Módulos puros en `src/oceanwatch/ingest/` (`schema`, `paths`, `download`, `autoloader`,
    `dedup`) con imports PySpark perezosos y tests unitarios (`pytest -q`: 20 passed, 1 skipped).
  - Notebook orquestador `notebooks/fase2_ingesta_streaming` e **ingesta real de los 3 días**
    (2023-06-01/02/03) a `oceanwatch.bronze.ais_raw` (Auto Loader + `Trigger.availableNow`,
    append-only). Conteos reales: **25 897 493** filas totales
    (8 808 904 + 9 052 241 + 8 036 348) y **658 duplicados** por `(MMSI, BaseDateTime)`
    (~0,0025 %). Run serverless `847693335141936` (SUCCESS, 167 s).
  - Notebook `notebooks/fase2_pruebas_fallos` con las **4 pruebas de tolerancia a fallos**,
    ejecutado en serverless (run `497716691196081`, SUCCESS, 88 s), persistiendo resultados en
    `oceanwatch.quality.pruebas_fallos`. Las 4 **PASA**: (a) corte+reinicio (final=6, sin
    pérdida/duplicados), (b) archivo reenviado (bronce 3→6, dedup silver estable=3), (c) esquema
    no conforme (stream vivo, 1 fila a `_rescued_data`), (d) reinicio sin reproceso (3→3).
  - ADRs aceptados: **ADR-006** (dedup `MMSI`+`BaseDateTime` con watermark 24h en bronce→silver),
    **ADR-007** (checkpoints en Volume de UC), **ADR-008** (`Status` int + `_rescued_data`).
- **Decisiones/ADRs tocados:** ADR-003 y ADR-004 (respetados y ahora verificados con ejecución
  real), ADR-006, ADR-007, ADR-008 (nuevos).
- **Estado Verificado vs Diseñado:** Verificado (ejecutado) = ingesta real de los 3 días, esquema
  bronce (`Status int` + auditoría), 658 duplicados reales, y las 4 pruebas de fallos en
  serverless. No quedó ninguna prueba como PENDIENTE DE EJECUCIÓN.
- **Síntoma→causa→solución:**
  - *Status string→int:* síntoma = Entrega 1 declaró `Status` como `string`; causa = la muestra
    real de NOAA trae código **entero** (`verificacion_esquema_real.md`); solución = `Status int`
    en el esquema explícito y lo no conforme a `_rescued_data` (ADR-008).
  - *Prueba (c) sin rescatados:* síntoma = primera versión observó `filas_con_rescued_data=0`
    (run `282095277952514`); causa = con header por archivo y esquema explícito Auto Loader
    alineaba columnas por nombre y no rescataba; solución = ajustar el **diseño de la prueba**
    (valor `Status="ABC"` no casteable + valor extra bajo header válido), re-run
    `497716691196081` → `filas_con_rescued_data=1`. No se alteró el pipeline.
- **Evidencia:** `evidence/req01-streaming/ejecucion_ingesta.md`,
  `evidence/req01-streaming/pruebas_tolerancia_fallos.md`,
  `evidence/req01-streaming/justificacion_availablenow.md`,
  `evidence/req01-streaming/verificacion_esquema_real.md`; tablas
  `oceanwatch.quality.ingesta_metricas` y `oceanwatch.quality.pruebas_fallos`.
- **Pendientes / siguiente paso:** Fase 3 (Req. 2) — materializar la tabla **silver** con la dedup
  de ADR-006 y las reglas de calidad/cuarentena (incluida la inspección de `_rescued_data`);
  catálogo de códigos `Status`/`VesselType` para gold.

### 2026-11-05 — Fase 2 (inicio): Bootstrap ejecutado + descarga de corpus — Equipo OceanWatch
- **Objetivo de la sesión:** ejecutar realmente el bootstrap de Unity Catalog (dejar de ser
  "Diseñado") y preparar el corpus del feed para la ingesta streaming.
- **Qué se hizo:** se ejecutó `resources/sql/bootstrap_oceanwatch.sql` (9 sentencias
  idempotentes) contra el workspace institucional usando el SQL warehouse serverless existente
  ("Serverless Starter Warehouse", id `377600528fed7c47`) vía la SQL Statement Execution API.
  Se verificó con `SHOW CATALOGS/SCHEMAS/VOLUMES` que existen el catálogo `oceanwatch`, los 6
  esquemas (bronze, silver, gold, quarantine, quality, ml) y los 2 volúmenes (landing,
  checkpoints). Se inició la descarga de los zips NOAA del corpus base (1–3 jun 2023) para
  inspeccionar el esquema real de los CSV.
- **Decisiones/ADRs tocados:** ADR-002 (nombres verificados en el catálogo real). Corpus del
  feed acotado a 3 días (1–3 jun 2023) para pruebas rápidas de streaming y fallos; diseño
  preparado para fechas nuevas.
- **Estado Verificado vs Diseñado:** Verificado (ejecutado) = bootstrap de Unity Catalog
  (catálogo, esquemas, volúmenes). En curso = descarga del corpus y verificación del esquema
  real de los CSV.
- **Evidencia:** `evidence/req05-gobernanza/bootstrap_unity_catalog.md` (salida real de las
  9 sentencias + SHOW CATALOGS/SCHEMAS/VOLUMES).
- **Pendientes / siguiente paso:** descomprimir los CSV, verificar columnas reales vs esquema
  de 17 campos, e implementar el pipeline Auto Loader → bronze con Trigger.availableNow, dedup
  por MMSI+BaseDateTime y las 4 pruebas de tolerancia a fallos.

### 2026-11-05 — Fase 1: Fundaciones — Equipo OceanWatch
- **Objetivo de la sesión:** establecer la estructura del repositorio y las decisiones de
  arquitectura (ADRs) para la Entrega 2, partiendo del entorno Databricks serverless creado
  desde cero.
- **Qué se hizo:** creada estructura de carpetas (docs/, src/oceanwatch/, resources/,
  evidence/, conf/, tests/, notebooks/, data_samples/); plantilla ADR y bitácora; 5 ADRs
  aceptados (001–005); scripts de bootstrap idempotentes (Python y SQL) marcados PENDIENTE DE
  EJECUCIÓN; config.yaml centralizado sin secretos; .gitignore; README actualizado con pesos
  de Entrega 2 corregidos; matriz de trazabilidad.
- **Decisiones/ADRs tocados:** ADR-001 (medallion serverless), ADR-002 (nombres/catálogo/
  esquemas), ADR-003 (ingesta zip→Auto Loader), ADR-004 (Trigger.availableNow), ADR-005 (ML
  anomalías de velocidad).
- **Estado Verificado vs Diseñado:** Verificado (ejecutado) = inventario del entorno
  Databricks (host, metastore `metastore_aws_us_east_2` en us-east-2, catálogos existentes,
  ausencia de SQL warehouses, cómputo serverless). Diseñado (no ejecutado) = todo el resto,
  incluidos los scripts de bootstrap.
- **Evidencia:** este repositorio (estructura y documentos de Fase 1); inventario del entorno
  citado en ADR-001/ADR-002.
- **Pendientes / siguiente paso:** Fase 2 — implementar la ingesta streaming (Req. 1) y las
  capas bronze/silver con Auto Loader + Trigger.availableNow.
