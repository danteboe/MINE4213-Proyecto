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
