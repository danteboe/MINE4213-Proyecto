# MINE4213 · Proyecto — OceanWatch Analytics

OceanWatch Analytics es el lakehouse del tráfico marítimo: ingesta, gobierna y analiza datos
AIS (NOAA) sobre Databricks con Unity Catalog y cómputo **serverless**, diseñado para resistir
la llegada de datos nuevos del feed AIS.

## Visión y alcance (Entrega 2)

La Entrega 2 lleva el análisis exploratorio de Entrega 1 a un lakehouse productivo:

- **Ingesta streaming incremental** de los zips diarios de NOAA (descompresión + Auto Loader).
- **Capas medallion + calidad** (bronze → silver → gold) con cuarentena de registros inválidos.
- **Orquestación** mediante un Job de Databricks (5 tareas) con cron Quartz (America/Bogota),
  idempotencia, reintentos por tarea y backfill.
- **Dashboard de Databricks** (AI/BI, 4 visuales + parámetro de fecha) sobre las tablas de oro.
- **Gobernanza** con Unity Catalog: catálogo/esquemas/volúmenes, comentarios en el securable,
  GRANTs de mínimo privilegio a 4 grupos de rol y linaje verificado por API.
- **ML con MLflow**: detección de anomalías de velocidad (SOG), modelo en Unity Catalog
  `@champion`, inferencia batch como tarea del Job. El pronóstico de arribos se aplaza (ver ADR-005).
- **Repo + bitácora** con ADRs y matriz de trazabilidad.

> El **bono de Delta Sharing NO se realiza**. El dashboard es de Databricks Dashboards. Los
> puertos se cruzan con el World Port Index (WPI) local.

## Arquitectura

El flujo de extremo a extremo (NOAA zip → descompresión → Volume `landing` → Auto Loader con
`Trigger.availableNow` → `bronze` → `silver` + `quality`/`quarantine` → `gold` con grilla
lat/lon + WPI → Dashboard y ML de anomalías SOG) está diagramado en
[`docs/architecture/medallion-overview.md`](docs/architecture/medallion-overview.md)
(diagrama mermaid editable + explicación).

La **orquestación** de ese flujo (el Job de Databricks que encadena ingesta → plata/calidad →
conciliación → oro, con su cron y monitoreo) está diagramada en
[`docs/architecture/fase4-dag-orquestacion.md`](docs/architecture/fase4-dag-orquestacion.md)
(DAG mermaid + explicación) y operada según [`docs/runbook.md`](docs/runbook.md).

## Estado por requisito (rúbrica Entrega 2)

| Req | Peso | Descripción | Estado |
|---|---|---|---|
| 1 | 15% | Ingesta streaming incremental (NOAA zip → Auto Loader → bronze) | **Verificado (ejecutado)** — Fase 2 |
| 2 | 15% | Capas medallion + calidad + cuarentena | **Verificado (ejecutado)** — Fase 3 |
| 3 | 20% | Orquestación por Job (cron, idempotencia, reintentos, backfill) | **Verificado (ejecutado)** — Fase 4 |
| 4 | 15% | Dashboard AI/BI sobre oro (4 visuales + parámetro de fecha) | **Verificado (ejecutado)** — Fase 7 |
| 5 | 10% | Gobernanza UC (grants, linaje, comentarios) | **Verificado (ejecutado)** — Fase 5 |
| 6 | 10% | ML con MLflow (anomalías SOG, modelo en UC) | **Verificado (ejecutado)** — Fase 6 |
| 7 | 5% | Repo + bitácora + 18 ADRs + matriz + evidencia | **Verificado (ejecutado)** — Fase 1/cierre |
| 8 | 10% | Sustentación | **Sustentación presencial** en la última clase — sin entregable de software; material de apoyo en [`docs/guia_sustentacion.md`](docs/guia_sustentacion.md) |

> Estos son los pesos de **Entrega 2**. Corrigen los que aparecían antes en este README,
> heredados de la rúbrica de **Entrega 1** (Req. 2 20%, Req. 3 25%, Req. 4 25%).
>
> **Único pendiente del equipo humano (no de software):** capturas de pantalla de UI —4 visuales
> del dashboard + parámetro de fecha, y grafo de linaje en Catalog Explorer—, no automatizables
> sin navegador autenticado. Dashboard y linaje ya están **verificados por API**. Detalle y
> procedimiento en [`docs/pendientes_equipo.md`](docs/pendientes_equipo.md).
>
> **Fuera de alcance por decisión del equipo:** el bono de **Delta Sharing** (no se realizó) y el
> **pronóstico de arribos** (aplazado, ver [ADR-005](docs/adr/ADR-005-eleccion-ml-anomalias-velocidad.md)).
> No son brechas de la Entrega 2.

Detalle de criterios, ADRs, código y evidencia por requisito en
[`docs/traceability_matrix.md`](docs/traceability_matrix.md). La autoevaluación requisito por
requisito (con el checklist del enunciado) está en
[`docs/autoevaluacion_rubrica.md`](docs/autoevaluacion_rubrica.md).

## Inventario de recursos reales en Databricks

Entorno: host `dbc-a568bc49-bdae`, metastore `us-east-2`, cómputo **serverless** (SQL warehouse
`377600528fed7c47`). Todo lo siguiente está **vivo y verificado** en el workspace.

**Unity Catalog.** Catálogo `oceanwatch` con esquemas `bronze`, `silver`, `gold`, `quarantine`,
`quality`, `ml` (+ `default`, `information_schema` y el `entrega1` preservado). Volúmenes
`bronze.landing` (zips/CSV de NOAA) y `bronze.checkpoints` (Auto Loader).

**Tablas y conteos reales:**

| Tabla | Filas |
|---|---|
| `bronze.ais_raw` | 25.897.493 |
| `silver.ais_clean` | 25.857.101 |
| `silver.celda_en_agua` | 726.513 |
| `quarantine.ais_rejected` | 39.734 |
| `quality.ingesta_metricas` | 3 |
| `quality.metricas_calidad` | 15 |
| `quality.pruebas_fallos` | 4 |
| `quality.ejecuciones_job` | 50 |
| `gold.trayectorias_diarias` | 61.768 |
| `gold.presencia_puertos` | 9.153 |
| `gold.matriz_trafico` | 2.319.365 |
| `gold.matriz_trafico_diaria` | 1.027.635 |
| `gold.anomalias_velocidad` | 67.828 |
| `gold.world_port_index` | 3.669 |

> **Invariante de conciliación (diferencia 0):**
> `25.897.493` (bronce) `= 25.857.101` (plata) `+ 39.734` (cuarentena) `+ 658` (duplicados).

**Job.** `oceanwatch_pipeline_entrega2` (`job_id 202547459111516`), 5 tareas encadenadas
`ingesta → plata_calidad → conciliacion → oro → ml_anomalias`, cron de producción
`0 0 6 * * ?` (America/Bogota, `pause_status = PAUSED`), reintentos por tarea
(ingesta 2 / plata 1 / oro 1 / ml 1 / conciliacion 0). Corridas reales: 3 `SCHEDULED` (scheduler)
+ manuales, todas `SUCCESS`. Definido como código en
[`resources/jobs/oceanwatch_pipeline.job.yml`](resources/jobs/oceanwatch_pipeline.job.yml) +
[`databricks.yml`](databricks.yml). Operación en [`docs/runbook.md`](docs/runbook.md).

**Modelo (MLflow + Unity Catalog).** `oceanwatch.ml.detector_anomalias_velocidad` **v1
`@champion`** (IsolationForest; run MLflow `be8f2ef2c2634efcaffa7fd89d3daf8f`; experimento
`/Users/jd.torresa1@uniandes.edu.co/oceanwatch_anomalias_sog`). Inferencia batch como quinta
tarea `ml_anomalias` del Job: 67.828 anomalías para 2023-06-03.

**Dashboard AI/BI.** `OceanWatch - Consola de trafico (oro)` (id
`01f1da0367841f3db6c6ddcb8edaa849`), `ACTIVE`/publicado, warehouse `377600528fed7c47`, 4 visuales
que consumen **solo oro** (`matriz_trafico_diaria`, `presencia_puertos`, `anomalias_velocidad`).
URL publicada:
`https://dbc-a568bc49-bdae.cloud.databricks.com/dashboardsv3/01f1da0367841f3db6c6ddcb8edaa849/published`

**Gobernanza.** 4 grupos de rol de cuenta (`oceanwatch_ingenieria_datos`, `oceanwatch_analistas`,
`oceanwatch_ciencia_datos`, `oceanwatch_consumidores_dashboard`) con grants de **mínimo
privilegio** verificados por `SHOW GRANTS`. Linaje verificado por API
(`silver.ais_clean ← [bronze.ais_raw, silver.celda_en_agua]`, `→` las 4 tablas de oro).

## Estructura del repositorio

```
MINE4213-Proyecto/
├── README.md                              # Este archivo
├── .gitignore                             # Excluye secretos y artefactos
├── entrega1_oceanwatch.ipynb              # Notebook de Entrega 1 (no se modifica)
├── conf/
│   └── config.yaml                        # Configuración centralizada (sin secretos)
├── databricks.yml                         # Databricks Asset Bundle (Job + targets)
├── docs/
│   ├── bitacora.md                        # Bitácora cronológica + plantilla
│   ├── traceability_matrix.md             # Matriz de trazabilidad (Req. 1–8)
│   ├── autoevaluacion_rubrica.md          # Autoevaluación + checklist del enunciado
│   ├── pendientes_equipo.md               # Acciones manuales del equipo (capturas de UI)
│   ├── guia_sustentacion.md               # Material de apoyo para la defensa presencial
│   ├── data_contract_gold.md              # Contrato de datos oro (Fase 3/5)
│   ├── data_quality_rules.md              # Catálogo de reglas de calidad (Fase 3)
│   ├── runbook.md                         # Operación del Job (Fase 4)
│   ├── adr/                               # ADR-TEMPLATE + ADR-001..018 (18 ADRs)
│   └── architecture/
│       ├── medallion-overview.md          # Diagrama mermaid + explicación
│       └── fase4-dag-orquestacion.md      # DAG del Job (mermaid) + explicación
├── src/
│   └── oceanwatch/                        # Paquete Python (common, ingest, quality, gold, ml, orquestacion)
├── resources/
│   ├── jobs/oceanwatch_pipeline.job.yml   # Definición del Job (5 tareas)
│   ├── dashboards/oceanwatch_oro.lvdash.json  # Dashboard AI/BI como código
│   └── sql/                               # bootstrap + fase3/4/5/6/7 (CTAS, permisos, auditoría)
├── notebooks/                             # fase2..fase6 (ingesta, calidad, oro, orquestación, ML)
├── tests/                                 # Pruebas unitarias (pytest)
├── data_samples/                          # Muestras de datos
└── evidence/                              # Evidencia por requisito (req01..req08)
```

El bootstrap existe en **dos formas coherentes**: Python parametrizado
(`src/oceanwatch/ingest/bootstrap.py`, lógica centralizada por `conf/`) y SQL declarativo
(`resources/sql/bootstrap_oceanwatch.sql`, ejecución directa y auditable). La razón se
documenta en [ADR-002](docs/adr/ADR-002-convenciones-nombres-catalogo-esquemas.md).

## Cómo reproducir desde cero

Orden end-to-end para alguien externo. Prerrequisitos: **CLI de Databricks** configurada con la
key **fuera del repo** (`../databricks.key`, nunca dentro), cómputo **serverless** y acceso a
NOAA. No se añade ningún secreto al repo.

1. **Configurar** `conf/config.yaml` si cambian catálogo, esquemas, volúmenes o fechas del corpus.
2. **Bootstrap (Req. 5):** crear catálogo, esquemas y volúmenes con
   [`resources/sql/bootstrap_oceanwatch.sql`](resources/sql/bootstrap_oceanwatch.sql) (o
   `oceanwatch.ingest.bootstrap.run(spark)`). Evidencia:
   `evidence/req05-gobernanza/bootstrap_unity_catalog.md`.
3. **Desplegar el Job (bundle):** `databricks bundle validate` y `databricks bundle deploy` con
   [`databricks.yml`](databricks.yml) + [`resources/jobs/oceanwatch_pipeline.job.yml`](resources/jobs/oceanwatch_pipeline.job.yml).
   Crea el Job `oceanwatch_pipeline_entrega2` (5 tareas).
4. **Ejecutar el Job por fecha:** `databricks bundle run ... --params fecha_proceso=2023-06-03`
   (o `databricks jobs run-now`). Encadena ingesta (Req. 1, carga los CSV de NOAA a
   `bronze.ais_raw`) → plata/calidad+cuarentena (Req. 2) → conciliación (invariante) → oro
   (Req. 2) → `ml_anomalias` (Req. 6, carga el modelo `@champion` y puebla
   `gold.anomalias_velocidad`). Notebooks `notebooks/fase2..fase6`, SQL `resources/sql/fase3*..fase6*`.
   Para backfill, re-ejecutar con otra `fecha_proceso` (idempotente).
5. **Enriquecer `en_agua` (una vez):** clasificar las celdas de la grilla y materializar
   `silver.celda_en_agua` con
   [`resources/sql/fase3_enriquecer_en_agua.sql`](resources/sql/fase3_enriquecer_en_agua.sql)
   (evidencia `evidence/req02-capas-calidad/enriquecimiento_en_agua.md`).
6. **Permisos y comentarios (Req. 5):**
   [`resources/sql/fase5_permisos.sql`](resources/sql/fase5_permisos.sql) (grants de mínimo
   privilegio a los 4 grupos) y
   [`resources/sql/fase5_comentarios_oro.sql`](resources/sql/fase5_comentarios_oro.sql).
7. **Dashboard (Req. 4):** vista de apoyo
   [`resources/sql/fase7_matriz_trafico_diaria.sql`](resources/sql/fase7_matriz_trafico_diaria.sql)
   y despliegue de [`resources/dashboards/oceanwatch_oro.lvdash.json`](resources/dashboards/oceanwatch_oro.lvdash.json)
   vía `POST /api/2.0/lakeview/dashboards` + `.../published`.
8. **Entrenamiento del modelo (opcional, reproducir el registro):** notebook
   `notebooks/fase6_entrenamiento_mlflow.py` como one-time run serverless; registra
   `oceanwatch.ml.detector_anomalias_velocidad` v1 `@champion`.

Todas las fases están **ejecutadas y verificadas**; la evidencia por requisito está en
`evidence/req01..req06`. El único paso manual restante son las **capturas de UI**
(ver [`docs/pendientes_equipo.md`](docs/pendientes_equipo.md)).

## Requisitos previos

- Workspace de Databricks con **Unity Catalog** (metastore en `us-east-2`).
- Permisos de administrador sobre el metastore (grupo `admins`).
- Cómputo **serverless**: notebooks serverless + SQL warehouse serverless (`377600528fed7c47`).
- Acceso de red a NOAA para descargar los zips diarios.
- Shapefile del World Port Index en `world_port_index/` (fuera del repo, en el workspace local).
- CLI de Databricks configurada con la key **fuera** del repo (`databricks.key`).

## Fuentes de datos (fecha de acceso: 2026-11-05)

- **AIS — Marine Cadastre / NOAA:** https://hub.marinecadastre.gov/pages/vesseltraffic ;
  base de descarga https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023 (archivos
  `AIS_YYYY_MM_DD.zip`). Dominio público (U.S. Government).
- **World Port Index (NGA):** shapefile local `world_port_index/WPI.shp`. Dominio público.
  Corrección respecto a Entrega 1: es un **shapefile**, no un CSV de Kaggle.
- **Documentación de Databricks** citada en los ADRs (acceso 2026-11-05).

## Gobernanza de secretos

El repositorio tiene **CERO secretos**. El `.gitignore` excluye `*.key`, `.databrickscfg`,
`conf/*.secret.yaml`, `*.env`, `__pycache__/`, `.agents` y `.databricks`. La key de Databricks
(`databricks.key`) permanece FUERA del repositorio y nunca se copia dentro. El único `host` que
aparece en `conf/config.yaml` es la URL del workspace (referencia sin token), no un secreto.

---

## Entrega 1 (preservada)

Explorar, responder y almacenar de forma óptima el tráfico marítimo AIS de los días 1 al 7 de
junio de 2023 (fuente oficial: [Marine Cadastre / NOAA](https://hub.marinecadastre.gov/pages/vesseltraffic)).

Corpus procesado: 60.5M posiciones · 31,871 buques únicos · 7 días.

### Estructura del notebook "entrega1_oceanwatch.ipynb"

> Los pesos de la tabla siguiente son los de la rúbrica de **Entrega 1** (no los de Entrega 2).

| Sección | Contenido | Requisito (Entrega 1) |
|---|---|---|
| 0 | Configuración e imports (parámetros centralizados, estilo visual) | — |
| 1 | Ingesta documentada: descarga con reintentos + verificación de integridad (Content-Length, SHA-256, zip válido), descompresión en Volume, lectura con esquema explícito | Req. 1 (15%) |
| 2 | Exploración y perfilamiento: posiciones/día, buques únicos, distribución por tipo y tamaño, patrones temporales y espaciales | Req. 2 (20%) |
| 3 | Calidad de datos: detección y cuantificación de problemas (nulos, coordenadas, velocidades, rumbos, MMSI, dimensiones, duplicados) | Req. 2 (20%) |
| 4 | Preguntas de negocio (5) con Spark eficiente y planes de ejecución (".explain") | Req. 3 (25%) |
| 5 | Almacenamiento óptimo: Parquet plano vs. Delta particionado vs. Delta + "OPTIMIZE ZORDER", con evidencia (bytes, archivos leídos) | Req. 4 (25%) |
| 6 | Gobernanza: Unity Catalog (catálogo/esquema/volume) + comentarios y metadatos en tablas y columnas | Req. 5 (10%) |

### Cómo ejecutar (Entrega 1)

1. Importar "entrega1_oceanwatch.ipynb" en el workspace de Databricks (Workspace → Import).
2. Ajustar, si hace falta, los parámetros de la primera celda de código:
   - "CATALOG = oceanwatch", "SCHEMA = entrega1", "VOLUME = landing".
3. Ejecutar las celdas en orden. El notebook:
   - crea catálogo / esquema / volume en Unity Catalog,
   - descarga y descomprime los 7 días desde NOAA (idempotente),
   - lee con esquema explícito y ejecuta perfilamiento, calidad, preguntas de negocio,
     almacenamiento óptimo y gobernanza.

### Decisiones técnicas (Entrega 1)

- Esquema explícito (no "inferSchema"): control de tipos y menos pasadas de lectura sobre ~60M filas.
- Agregar con Spark → "toPandas()" → graficar; muestreo para distribuciones finas. Se evita traer
  el dataset completo al driver.
- Grilla espacial por redondeo lat/lon (alternativa aceptada a H3), justificada en el notebook.
- Almacenamiento: propósito de consulta = consulta diaria del operador por fecha y zona marítima;
  se compara Parquet plano vs. Delta particionado por "date" + "ZORDER (LAT, LON)", con evidencia de
  bytes y archivos leídos por la consulta objetivo (partition pruning + data skipping).
  - **Cruce con World Port Index (WPI):** redondeo de coordenadas de puertos y del tráfico AIS a dos
    decimales, "left join" sobre esa grilla espacial y top-10 de celdas con mayor tráfico junto con el
    puerto correspondiente.
    > **Corrección Entrega 2:** el WPI es un **shapefile** (`world_port_index/WPI.shp`), no un CSV de
    > Kaggle como decía el README original de Entrega 1. En Entrega 2 se lee el shapefile.

### Resultados clave (Entrega 1)

- Perfilamiento: 60.5M posiciones, 31,871 buques, 7 días completos. Tráfico dominado por Towing
  (16.7M) y Pleasure craft (14.5M); atributos estáticos con muchos nulos (Draft 64%, IMO 43%).
- Calidad: sentinelas AIS esperados (Heading=511 en 55.4%, COG=360 en 17.0%, SOG≥102.3 en 0.26%),
  dimensiones no físicas (Width/Length ≤0 ~3.5%), MMSI anómalos (0.08%), duplicados mínimos (<0.01%).
  Coordenadas sin valores fuera de rango.
- Preguntas de negocio: el conteo aproximado (HLL++) subestima ~5.6% en promedio (cardinalidad
  baja → se prefiere el exacto); Towing lidera el tráfico pero Cargo/Tanker son los más veloces; 39.8%
  de los buques transmiten los 7 días y 18.8% son visitantes de un solo día.
- Almacenamiento (evidencia): para la consulta objetivo (día + zona), Parquet plano lee 69/69
  archivos; Delta particionado por "date" lee 1 (partition pruning) y baja de 2,218 MB a 1,457 MB;
  "OPTIMIZE ZORDER (LAT, LON)" reduce a 1,357 MB. Layout recomendado: Delta particionado por "date" +
  ZORDER (LAT, LON).

### Artefactos en Unity Catalog (Entrega 1, parámetros por defecto)

- "oceanwatch.entrega1.vessel_type_catalog" — códigos VesselType → categoría legible.
- "oceanwatch.entrega1.data_quality_report" — diagnóstico de calidad (insumo Entrega 2).
- "oceanwatch.entrega1.positions_delta" — posiciones saneadas, particionadas por "date" y optimizadas.
- Volume "oceanwatch.entrega1.landing" — zips, CSV, Parquet baseline y manifiesto de ingesta.

> El esquema `oceanwatch.entrega1` se **preserva** sin modificaciones en Entrega 2 (ver ADR-002).

---

## Bitácora

La bitácora cronológica completa (con plantilla de entrada y las sesiones de Entrega 2) está en
[`docs/bitacora.md`](docs/bitacora.md). La tabla histórica de aportes de Entrega 1 se conserva aquí:

| Fecha | Integrante(s) | Aporte |
|---|---|---|
| Semana 5 | Todo el equipo | Ingesta y esquema explícito |
| Semana 5 | Todo el equipo | Perfilamiento y calidad de datos |
| Semana 6 | Todo el equipo | Preguntas de negocio |
| Semana 7 | Todo el equipo | Almacenamiento óptimo y gobernanza |

## Equipo

- Alejandro Abril - 202224328
- David Fernando Caro - 202222073
- Juan David Torres Albarracín - 202317608

## Fuentes

- AIS Data — Marine Cadastre / NOAA: https://hub.marinecadastre.gov/pages/vesseltraffic
- World Port Index (puertos): shapefile `world_port_index/WPI.shp` (NGA, dominio público).
