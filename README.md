# MINE4213 · Proyecto — OceanWatch Analytics

OceanWatch Analytics es el lakehouse del tráfico marítimo: ingesta, gobierna y analiza datos
AIS (NOAA) sobre Databricks con Unity Catalog y cómputo **serverless**, diseñado para resistir
la llegada de datos nuevos del feed AIS.

## Visión y alcance (Entrega 2)

La Entrega 2 lleva el análisis exploratorio de Entrega 1 a un lakehouse productivo:

- **Ingesta streaming incremental** de los zips diarios de NOAA (descompresión + Auto Loader).
- **Capas medallion + calidad** (bronze → silver → gold) con cuarentena de registros inválidos.
- **Orquestación** mediante Jobs de Databricks con cron en UTC e idempotencia.
- **Dashboard de Databricks** sobre las tablas de oro.
- **Gobernanza** con Unity Catalog (catálogo, esquemas, volúmenes, comentarios y GRANTs).
- **ML con MLflow**: detección de anomalías de velocidad (SOG). El pronóstico de arribos se
  aplaza (ver ADR-005).
- **Repo + bitácora** con ADRs y matriz de trazabilidad.

> El **bono de Delta Sharing NO se realiza**. El dashboard es de Databricks Dashboards. Los
> puertos se cruzan con el World Port Index (WPI) local.

## Arquitectura

El flujo de extremo a extremo (NOAA zip → descompresión → Volume `landing` → Auto Loader con
`Trigger.availableNow` → `bronze` → `silver` + `quality`/`quarantine` → `gold` con grilla
lat/lon + WPI → Dashboard y ML de anomalías SOG) está diagramado en
[`docs/architecture/medallion-overview.md`](docs/architecture/medallion-overview.md)
(diagrama mermaid editable + explicación).

## Estado por requisito (rúbrica Entrega 2)

| Req | Peso | Descripción | Estado |
|---|---|---|---|
| 1 | 15% | Ingesta streaming | **Verificado (ejecutado)** — Fase 2 |
| 2 | 15% | Capas + calidad | Pendiente |
| 3 | 20% | Orquestación | Pendiente |
| 4 | 15% | Dashboard | Pendiente |
| 5 | 10% | Gobernanza | Diseñado (Fase 1); implementación pendiente |
| 6 | 10% | ML con MLflow | Diseñado (Fase 1); implementación pendiente |
| 7 | 5% | Repo + bitácora | **Fase 1 en progreso** |
| 8 | 10% | Sustentación | Pendiente |

> Estos son los pesos de **Entrega 2**. Corrigen los que aparecían antes en este README,
> heredados de la rúbrica de **Entrega 1** (Req. 2 20%, Req. 3 25%, Req. 4 25%).

Detalle de criterios, ADRs, código y evidencia por requisito en
[`docs/traceability_matrix.md`](docs/traceability_matrix.md).

## Estructura del repositorio

```
MINE4213-Proyecto/
├── README.md                              # Este archivo
├── .gitignore                             # Excluye secretos y artefactos
├── entrega1_oceanwatch.ipynb              # Notebook de Entrega 1 (no se modifica)
├── conf/
│   └── config.yaml                        # Configuración centralizada (sin secretos)
├── docs/
│   ├── bitacora.md                        # Bitácora cronológica + plantilla
│   ├── traceability_matrix.md             # Matriz de trazabilidad (Req. 1–8)
│   ├── data_contract_gold.md              # PENDIENTE - Fase 3
│   ├── data_quality_rules.md              # PENDIENTE - Fase 3
│   ├── runbook.md                         # PENDIENTE - Fase 4
│   ├── guia_sustentacion.md               # PENDIENTE - Fase 9
│   ├── adr/                               # ADR-TEMPLATE + ADR-001..008
│   └── architecture/
│       └── medallion-overview.md          # Diagrama mermaid + explicación
├── src/
│   └── oceanwatch/                        # Paquete Python (common, ingest, quality, gold, ml)
├── resources/
│   ├── jobs/                              # Definiciones de Jobs (fases posteriores)
│   ├── pipelines/                         # Definiciones de pipelines (fases posteriores)
│   ├── dashboards/                        # Dashboards (fases posteriores)
│   └── sql/
│       └── bootstrap_oceanwatch.sql       # Bootstrap SQL (ejecutado en Fase 2)
├── notebooks/                             # fase2_ingesta_streaming, fase2_pruebas_fallos (Req. 1)
├── tests/                                 # Pruebas unitarias de la ingesta (pytest)
├── data_samples/                          # Muestras de datos (fases posteriores)
└── evidence/                              # Evidencia por requisito (req01..req08)
```

El bootstrap existe en **dos formas coherentes**: Python parametrizado
(`src/oceanwatch/ingest/bootstrap.py`, lógica centralizada por `conf/`) y SQL declarativo
(`resources/sql/bootstrap_oceanwatch.sql`, ejecución directa y auditable). La razón se
documenta en [ADR-002](docs/adr/ADR-002-convenciones-nombres-catalogo-esquemas.md).

## Cómo reproducir desde cero

1. **Prerrequisitos** (ver sección siguiente).
2. **Configurar** `conf/config.yaml` si cambian catálogo, esquemas, volúmenes o fechas del
   corpus. No se añaden secretos: la key de Databricks vive FUERA del repo.
3. **Ejecutar el bootstrap** en Databricks serverless —`resources/sql/bootstrap_oceanwatch.sql`
   o `oceanwatch.ingest.bootstrap.run(spark)`— para crear catálogo, esquemas y volúmenes.
   **Ejecutado en Fase 2** (ver `evidence/req05-gobernanza/bootstrap_unity_catalog.md`).
4. **Ejecutar la ingesta streaming (Req. 1, Fase 2):** notebook
   `notebooks/fase2_ingesta_streaming` como one-time run serverless; carga los CSV AIS de NOAA a
   `oceanwatch.bronze.ais_raw` (evidencia en `evidence/req01-streaming/`). Pruebas de tolerancia a
   fallos en `notebooks/fase2_pruebas_fallos`.
5. **Fases siguientes:** capas/calidad y oro (Fase 3), orquestación (Fase 4), dashboard, ML y
   sustentación.

## Requisitos previos

- Workspace de Databricks con **Unity Catalog** (metastore en `us-east-2`).
- Permisos de administrador sobre el metastore (grupo `admins`).
- Cómputo **serverless** (no hay SQL warehouses en el entorno).
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
`conf/*.secret.yaml` y `*.env`. La key de Databricks (`databricks.key`) permanece FUERA del
repositorio y nunca se copia dentro.

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
