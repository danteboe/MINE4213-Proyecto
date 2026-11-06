# Evidencia — Ejecución real de la ingesta streaming (Req. 1)

- **Qué demuestra:** la ingesta incremental de los 3 días del corpus
  (2023-06-01/02/03) se ejecutó **de verdad** en Databricks **serverless**, cargó los
  CSV AIS de NOAA a `oceanwatch.bronze.ais_raw` (append-only, Auto Loader +
  `Trigger.availableNow`, ADR-003/ADR-004) y registró los conteos reales en
  `oceanwatch.quality.ingesta_metricas`.
- **Cómo se generó:** se importó el paquete `src/oceanwatch`, `conf/config.yaml` y el
  notebook `notebooks/fase2_ingesta_streaming` al workspace bajo
  `/Users/jd.torresa1@uniandes.edu.co/oceanwatch/`; se lanzó un one-time run con
  `databricks jobs submit --json` (notebook_task serverless); polling con
  `databricks jobs get-run --output json` hasta `TERMINATED`; conteos leídos vía la
  SQL Statement Execution API (warehouse serverless `377600528fed7c47`).
- **Fecha:** 2026-11-05
- **Estado:** Verificado (ejecutado)

## Estado del run (serverless)

```
run_id: 847693335141936
run_name: oceanwatch-fase2-ingesta-streaming
run_type: SUBMIT_RUN
effective_performance_target: PERFORMANCE_OPTIMIZED   (serverless)
life_cycle_state: TERMINATED
result_state: SUCCESS
setup_duration: 4 s
execution_duration: 167 s
run_duration: 171.6 s
notebook_task.source: WORKSPACE
run_page_url: https://dbc-a568bc49-bdae.cloud.databricks.com/?o=7474649644754546#job/935587776487948/run/847693335141936
```

## Filas por día (salida literal de `SELECT _batch_date, count(*) ...`)

```
COLS: ['_batch_date', 'filas']
ROW: ['2023-06-01', '8808904']
ROW: ['2023-06-02', '9052241']
ROW: ['2023-06-03', '8036348']
```

| Día (`_batch_date`) | Filas |
|---|---|
| 2023-06-01 | 8 808 904 |
| 2023-06-02 | 9 052 241 |
| 2023-06-03 | 8 036 348 |
| **Total** | **25 897 493** |

## Total (salida literal)

```
COLS: ['total']
ROW: ['25897493']
```

## Duplicados por clave natural MMSI + BaseDateTime (salida literal)

```
COLS: ['total', 'distintos_por_clave', 'duplicados']
ROW: ['25897493', '25896835', '658']
```

- Total de filas en bronce: **25 897 493**
- Distintos por clave (MMSI, BaseDateTime): **25 896 835**
- **Duplicados por clave: 658** (= total − distintos)

> Interpretación: bronce es **append-only** (ADR-006); estos 658 duplicados exactos por
> (MMSI, BaseDateTime) representan la deduplicación que `dropDuplicatesWithinWatermark`
> eliminará en el paso bronce→silver (watermark 24h). Son un porcentaje ínfimo
> (~0,0025 %) del corpus, coherente con un feed AIS de alta frecuencia.

## Métricas persistidas en `oceanwatch.quality.ingesta_metricas` (salida literal)

```
COLS: ['distintos_por_clave', 'duplicados_por_clave', 'fecha_lote', 'filas', 'total_corpus', 'total_filas_dedup_clave', '_registrado']
ROW: ['25896835', '658', '2023-06-01', '8808904', '25897493', '25897493', '2026-11-06T03:46:12.356Z']
ROW: ['25896835', '658', '2023-06-02', '9052241', '25897493', '25897493', '2026-11-06T03:46:12.356Z']
ROW: ['25896835', '658', '2023-06-03', '8036348', '25897493', '25897493', '2026-11-06T03:46:12.356Z']
```

## Esquema real de `oceanwatch.bronze.ais_raw` (`DESCRIBE TABLE`, salida literal)

17 campos AIS + 4 columnas de auditoría. **`Status` es `int`** (corregido respecto a la
Entrega 1) y las columnas de auditoría están presentes.

```
MMSI               bigint
BaseDateTime       timestamp
LAT                double
LON                double
SOG                double
COG                double
Heading            double
VesselName         string
IMO                string
CallSign           string
VesselType         int
Status             int
Length             double
Width              double
Draft              double
Cargo              int
TransceiverClass   string
_rescued_data      string
_ingest_timestamp  timestamp
_source_file       string
_batch_date        date
```

## Cómo reproducir la lectura de conteos

Desde `...\PROYECTO` (fuera del repo), con `databricks.key` presente:

```
# _q.sql contiene los SELECT sobre oceanwatch.bronze.ais_raw y oceanwatch.quality.ingesta_metricas
python _dbx_sql.py 377600528fed7c47 _q.sql
```

(Para obtener las filas completas y no solo OK/FAIL se usó un lector auxiliar que llama a
`run_sql`/`extract_rows` del mismo helper e imprime `data_array`.)

## Fricciones reales de serverless (para FEAT-002)

- **Sin all-purpose cluster:** el cómputo se ejecutó como one-time run (`jobs submit`)
  con `notebook_task` **sin** `new_cluster`/`existing_cluster_id`, lo que resuelve a
  serverless (`effective_performance_target: PERFORMANCE_OPTIMIZED`). No hizo falta
  `jobs create` + `run-now`.
- **Importación del paquete al workspace:** `workspace import-dir` sube `src/` como
  archivos (`.py`) y `conf/config.yaml` como archivo de workspace; el notebook los
  resuelve bajo `/Workspace/Users/.../oceanwatch/{src,conf}`. Los `.py` del paquete se
  importan correctamente (imports PySpark perezosos).
- **Descarga NOAA desde dentro de Databricks:** la descarga de los ~344 MB/día se hace
  desde el runtime (región AWS us-east-2), no desde la máquina local. La ejecución
  completa (descarga + descompresión + Auto Loader) tardó ~167 s para los 3 días.
- **UTC por defecto:** serverless interpreta `BaseDateTime` (ISO sin zona) en UTC
  (coherente con ADR-004).
