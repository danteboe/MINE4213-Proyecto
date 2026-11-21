# OceanWatch Analytics · MINE 4213

OceanWatch procesa mensajes AIS de NOAA para consultar el tráfico, la presencia de buques en puertos y las anomalías de velocidad. La Entrega 2 corre en el workspace institucional de Databricks con un **clúster clásico de Unity Catalog**. Reutiliza el esquema AIS, la grilla espacial y el diagnóstico de calidad de la Entrega 1.

## Entrega 2

- [Job de Lakeflow](https://dbc-b38f0852-ecf9.cloud.databricks.com/jobs/83886562675136) · seis tareas en el clúster personal de Juan David.
- [Tablero AI/BI](https://dbc-b38f0852-ecf9.cloud.databricks.com/dashboardsv3/01f1e5507d1b18a5963d1a49e4329629/published) · mapa de intensidad, ranking de puertos, buques en puerto y anomalías por fecha UTC.
- [Arquitectura Spark](docs/architecture/medallion-overview.md) · particiones, broadcast, ventanas y conciliación.
- [Runbook](docs/runbook.md) · despliegue, entrenamiento inicial, backfill, agenda y reparación.
- [Trazabilidad](docs/traceability_matrix.md) y [evidencia institucional](evidence/institucional/) · alcance y resultados observados.

El DAG ejecuta **preparación → ingesta → plata → conciliación → oro → ML**. Auto Loader usa un checkpoint persistente y un sink Delta append-only. Plata aplica reglas, conserva los rechazos con causas y deduplica por buque y timestamp. Los pings del día que pasa conciliación alimentan los agregados y la inferencia. El IsolationForest queda versionado en Unity Catalog mediante MLflow. El SQL warehouse institucional sirve las consultas del tablero.

El corpus institucional amplía la semana de la Entrega 1 al **1–8 de junio de 2023**. Los ocho días conciliaron con diferencia cero sobre **69.454.003 posiciones** en bronce.

| Recurso | Ubicación |
|---|---|
| Tablas y modelo institucionales | `clase14.equipo9.oceanwatch_<capa>_<objeto>` |
| Landing y checkpoint | `/Volumes/clase10/equipo9/checkpoints/oceanwatch_equipo9` |
| Configuración | [conf/config.yaml](conf/config.yaml), sección `institucional` |
| Despliegue | [databricks.yml](databricks.yml), [Jobs](resources/jobs/), [tablero](resources/dashboards/) y [recursos ML](resources/ml/) |
| Entrypoints | [Pipeline](notebooks/oceanwatch_cluster.py) y [verificación](notebooks/oceanwatch_verificacion.py) |
| Decisiones | [ADR-019](docs/adr/ADR-019-computo-y-almacenamiento-institucional.md), [ADR-020](docs/adr/ADR-020-procesamiento-incremental-y-calidad.md), [ADR-021](docs/adr/ADR-021-modelo-sog-en-cluster.md) |

El workspace del curso otorga privilegios al grupo `mine-group9` en un único esquema. Los prefijos distinguen capas y los permisos para el Job y el tablero se asignan por recurso. El [ADR-019](docs/adr/ADR-019-computo-y-almacenamiento-institucional.md) detalla el estado de los roles y los permisos que requiere separar capas en esquemas físicos.

### Reproducir

Estos comandos reproducen la corrida inicial después de [autenticar la CLI con OAuth](docs/runbook.md).

```sh
databricks bundle validate --strict
databricks bundle deploy
databricks bundle run oceanwatch_pipeline_entrega2 --params fecha_proceso=2023-06-08,entrenar=true
databricks bundle run oceanwatch_verificacion --params fecha_proceso=2023-06-08
```

La primera corrida incluye las fechas 1, 2 y 3 para construir el train y la validación temporal. Un backfill posterior al train solo requiere `fecha_proceso` y reutiliza el campeón. El 8 de junio amplía el corpus de la Parte 1. El calendario corre un replay histórico. El repositorio excluye tokens, perfiles de autenticación y archivos `*.key`.

### Fuentes

- AIS diarios, Marine Cadastre / NOAA. [Archivo 2023](https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/).
- [World Port Index de NGA](https://msi.nga.mil/Publications/WPI). El CSV versionado extrae 7 campos y 3.669 registros del shapefile de la Entrega 1. `INDEX_NO` se normaliza a entero. El ZIP original queda fuera del repositorio.
- Global Land Mask 1.0.0, máscara ráster derivada de GLOBE. [Código y datos](https://github.com/toddkarin/global-land-mask).
- Canales y ríos navegables de NWN, instantánea del 18 de enero de 2023, y lagos de Natural Earth 5.0.0. El [ADR-020](docs/adr/ADR-020-procesamiento-incremental-y-calidad.md) documenta la proyección hidrográfica reproducible. [NTAD de BTS](https://rosap.ntl.bts.gov/view/dot/56264) y [Natural Earth](https://www.naturalearthdata.com/downloads/10m-physical-vectors/).
- `notebooks/fase*`, `resources/sql/fase*`, ADR-001 a 018 y las carpetas de evidencia `req01` a `req08` registran el prototipo en Free Edition. Los entrypoints de arriba definen el flujo vigente.

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
