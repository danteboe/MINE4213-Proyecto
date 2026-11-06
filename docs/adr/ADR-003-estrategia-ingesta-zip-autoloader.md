# ADR-003: Estrategia de ingesta — descompresión de zip NOAA y Auto Loader

- **Estado:** Aceptado
- **Fecha:** 2026-11-05
- **Requisito(s) de la rúbrica:** Req. 1 (15%)
- **Autores:** Alejandro Abril (202224328), David Fernando Caro (202222073), Juan David Torres Albarracín (202317608)

## 1. Contexto y problema
La fuente NOAA entrega archivos **`.zip` diarios** (`AIS_YYYY_MM_DD.zip`, base
`https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023`), cada uno con un CSV diario. La
solución debe ingerir de forma incremental y resistir archivos futuros del feed AIS. El
problema central es que **Spark y Auto Loader no leen `.zip` directamente**: hay que decidir
cómo convertir el zip en una fuente ingerible y cómo detectar incrementalmente los nuevos
archivos manteninendo el esquema explícito heredado de Entrega 1 (17 campos).

## 2. Criterios de decisión
- Resistencia a archivos nuevos (ingesta incremental sin reprocesar lo ya cargado).
- Evolución de esquema (campos nuevos del feed AIS sin romper la ingesta).
- Idempotencia (una corrida sin datos nuevos no duplica filas).
- Compatibilidad con serverless.
- Esquema explícito (continuidad con Entrega 1).

## 3. Alternativas consideradas
| Alternativa | Descripción | Pros | Contras | Evidencia |
|---|---|---|---|---|
| A. Descomprimir a Volume + Auto Loader | Descomprimir el zip a `landing` y leer los CSV con `cloudFiles` | Ingesta incremental, descubrimiento de archivos, evolución de esquema + rescued data | Paso extra de descompresión; gestionar retención del zip | [Auto Loader — formatos soportados](https://docs.gcp.databricks.com/ingestion/auto-loader/index.html) (acceso 2026-11-05) |
| B. Leer el zip directo con Auto Loader | Apuntar `cloudFiles` al `.zip` | — | INVIABLE: los formatos soportados son JSON/CSV/XML/PARQUET/AVRO/ORC/TEXT/BINARYFILE, no zip | [Lectura de zip requiere descompresión](https://community.databricks.com/t5/data-engineering/databricks-reading-from-a-zip-file/td-p/25861) (acceso 2026-11-05) |
| C. `spark.read` batch estático | Leer todo el directorio en cada corrida | Simple | No resiste llegada incremental; sin checkpoint ni exactly-once; reprocesa todo | [Auto Loader — formatos soportados](https://docs.gcp.databricks.com/ingestion/auto-loader/index.html) (acceso 2026-11-05) |

## 4. Decisión
Se adopta un paso de **descompresión del zip a CSV en el Volume `landing`** seguido de
**Auto Loader (`cloudFiles`) sobre los CSV**, con **esquema explícito** AIS de 17 campos,
evolución de esquema habilitada (`cloudFiles.schemaLocation` + columna de datos rescatados),
y lectura del World Port Index como **shapefile** (`world_port_index/WPI.shp`), corrigiendo
la descripción errónea de Entrega 1 que lo llamaba CSV de Kaggle.

## 5. Justificación basada en evidencia
La documentación de [Auto Loader](https://docs.gcp.databricks.com/ingestion/auto-loader/index.html)
(acceso 2026-11-05) enumera los formatos ingeribles (JSON, CSV, XML, PARQUET, AVRO, ORC,
TEXT, BINARYFILE) y el zip no está entre ellos; el hilo de la comunidad sobre
[lectura de archivos zip](https://community.databricks.com/t5/data-engineering/databricks-reading-from-a-zip-file/td-p/25861)
(acceso 2026-11-05) confirma que hay que descomprimir primero. Por eso la alternativa B es
inviable. Auto Loader aporta descubrimiento incremental de archivos y manejo de evolución de
esquema con columna de datos rescatados, lo que satisface la resistencia a archivos nuevos y
a campos nuevos del feed, frente al batch estático (C) que reprocesaría todo. El esquema
explícito de 17 campos proviene directamente de Entrega 1 (celda 0 del notebook,
`BASE_URL` y `StructType`).

## 6. Patrón de diseño / ingeniería de datos aplicado
**Landing Zone + Incremental File Ingestion (Auto Loader)** con **Schema-on-read explícito**
(esquema declarado + rescued data column para evolución controlada).

## 7. Consecuencias
- Positivas: ingesta incremental robusta, resistente a archivos y campos nuevos; esquema
  controlado; continuidad con Entrega 1.
- Negativas: paso extra de descompresión y manejo de la ventana de archivos en `landing`.
- Deuda asumida: política de retención del zip crudo (PENDIENTE - Fase 2/4).

## 8. Cómo se verifica / reversibilidad
Al ejecutar (Fase 2): colocar nuevos CSV en `landing` debe generar filas nuevas en `bronze`
sin reprocesar lo ya ingerido (comprobable por conteo y por el estado del checkpoint).
Reversible limpiando el checkpoint del stream. Estado actual: **Diseñado (no ejecutado)**.

## 9. Referencias
- Auto Loader (formatos soportados) — Databricks: https://docs.gcp.databricks.com/ingestion/auto-loader/index.html (acceso 2026-11-05).
- Lectura de archivos zip en Databricks (comunidad): https://community.databricks.com/t5/data-engineering/databricks-reading-from-a-zip-file/td-p/25861 (acceso 2026-11-05).
- Notebook `entrega1_oceanwatch.ipynb` (`BASE_URL`, esquema AIS de 17 campos).
- ADR-004 (modo trigger availableNow).
