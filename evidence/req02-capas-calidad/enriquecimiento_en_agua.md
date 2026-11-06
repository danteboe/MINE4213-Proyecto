# Evidencia — Materialización de la bandera `en_agua` en la capa plata (ADR-010, Req. 2)

- **Qué demuestra:** que la bandera de calidad `en_agua` de `oceanwatch.silver.ais_clean`
  quedó **materializada** (0 NULL) sobre las **25.857.101** filas de la plata, poblándola
  SIN `pandas_udf` (el camino que falló en serverless), y que la **invariante de
  conciliación sigue cuadrando** (la bandera es NULIFICAR-ATRIBUTO, no descarta filas).
- **Cómo se generó:** enfoque por **celda distinta** (ADR-010 §8), ejecutado de verdad
  contra el warehouse serverless `377600528fed7c47` el 2026-11-05:
  1. **Extracción local** de las **726.513 celdas distintas** de la grilla
     (`SELECT DISTINCT round(LAT,2), round(LON,2) FROM oceanwatch.silver.ais_clean WHERE
     LAT IS NOT NULL AND LON IS NOT NULL`) vía la **SQL Statement Execution API** con
     paginación por `next_chunk_index`. `statement_id` de la extracción:
     **`01f1d9a8-7811-1519-84de-e57af6b08984`** (`total_row_count = 726513`).
  2. **Clasificación local** con `global-land-mask==1.0.0` (ya instalado por `pip` en la
     máquina, idéntico al `%pip install` de serverless): `globe.is_land(lat_array,
     lon_array)` vectorizado sobre numpy; `en_agua = NOT is_land`. **NO** se usó
     `pandas_udf` ni Spark para clasificar.
  3. **CSV** `celda_en_agua.csv` (726.513 filas) subido al Volume
     `oceanwatch.bronze.landing/ref/celda_en_agua.csv` con `databricks fs cp`.
  4. **CTAS** de la tabla de referencia `oceanwatch.silver.celda_en_agua` leyendo el CSV
     con `read_files` (esquema explícito `celda_lat DOUBLE, celda_lon DOUBLE, en_agua
     BOOLEAN`).
  5. **MERGE** que enriquece la plata por join de celda (solo `UPDATE SET en_agua`).
  - Script SQL reproducible: [`resources/sql/fase3_enriquecer_en_agua.sql`](../../resources/sql/fase3_enriquecer_en_agua.sql).
    Lógica reutilizable (sin Spark) en
    `src/oceanwatch/quality/materializar.py::clasificar_celdas_en_agua` y
    `::enriquecer_en_agua_por_celda`.
- **Fecha:** 2026-11-05
- **Estado:** Verificado (ejecutado)

## Por qué NO `pandas_udf` (fricción real de serverless)

La función original `compuertas._columna_en_agua` usa un `pandas_udf` de
`global_land_mask` sobre las 25.9M filas. En el cómputo **serverless** (Spark Connect GC)
ese `pandas_udf` falló y la columna `en_agua` quedó **TODA en NULL** tras FEAT-003. La
máscara es la MISMA (`global-land-mask==1.0.0`, misma versión pinneada de ADR-010); lo que
cambia es **dónde** se evalúa: se clasifican las **726.513 celdas distintas** (cientos de
miles) en lugar de las **25.857.101 filas**. Dos celdas con el mismo `round(LAT,2)/round(LON,2)`
comparten clasificación, así que clasificar por celda es equivalente y ~35× más pequeño.

La definición de celda (`round(LAT,2)`, `round(LON,2)`, 2 decimales) coincide
**exactamente** con `src/oceanwatch/quality/geo.py::celda_grilla` y con
`gold.decimales_grilla=2` (ADR-001: grilla por redondeo lat/lon, NO H3), de modo que la
bandera es consistente con la grilla usada por la capa oro.

## Identificación de las ejecuciones (statement_id reales)

| Paso | statement_id | Estado |
|---|---|---|
| Extracción de 726.513 celdas distintas (SQL API, chunked) | `01f1d9a8-7811-1519-84de-e57af6b08984` | SUCCEEDED |
| CTAS `silver.celda_en_agua` (desde CSV en Volume) | SUCCEEDED (`CREATE OR REPLACE TABLE ... AS read_files`) | SUCCEEDED |
| MERGE que puebla `en_agua` en la plata | `01f1d9a8-befe-18b5-a972-b7bcbfd6b468` | SUCCEEDED |
| Verificación de conteos de `en_agua` | `01f1d9a8-c9cb-15b6-83b8-3d2cf7ab60e6` | SUCCEEDED |
| Verificación de invariante (bronce/plata/cuarentena) | `01f1d9a8-cabb-1e9e-ae13-0e1dbadfd4ca` | SUCCEEDED |

## Conteos reales (ejecutados)

### Tabla de referencia `oceanwatch.silver.celda_en_agua`

```
filas  = 726513
agua   = 642018   (en_agua = True)
tierra = 84495    (en_agua = False)
nulos  = 0
```

El **11.63 %** de las CELDAS distintas es tierra, pero —ver más abajo— el **71.65 %** de
las FILAS cae en tierra: las celdas de tierra son las **más densas** del corpus (puertos y
canales interiores navegables), lo que reproduce el hallazgo de FEAT-002/ADR-010.

### Plata `oceanwatch.silver.ais_clean` (tras el MERGE)

Consulta de verificación del enunciado (statement `01f1d9a8-c9cb-15b6-83b8-3d2cf7ab60e6`):

```sql
SELECT count(*) filas,
       sum(CASE WHEN en_agua IS NOT NULL THEN 1 ELSE 0 END) no_null,
       sum(CASE WHEN en_agua THEN 1 ELSE 0 END)             en_agua_true,
       sum(CASE WHEN NOT en_agua THEN 1 ELSE 0 END)         en_tierra
FROM oceanwatch.silver.ais_clean;
```

| métrica | valor real |
|---|---|
| `filas` | **25.857.101** |
| `no_null` | **25.857.101** (0 NULL — todas clasificadas) |
| `en_agua_true` | **7.329.406** |
| `en_tierra` | **18.527.695** |
| **% tierra** (`en_tierra/filas`) | **71.6542 %** |
| **% agua** | **28.3458 %** |

**Coherencia con FEAT-002:** la medición previa sobre una muestra de 60.000 filas
(run `614704376036187`) dio **72.28 %** de tierra; la materialización completa sobre las
25.857.101 filas da **71.65 %**. La diferencia (<0.7 pp) es la esperada entre muestra y
corpus completo: la clasificación es coherente.

## Invariante de conciliación — intacta

La bandera `en_agua` es NULIFICAR-ATRIBUTO: **no descarta filas**. El MERGE solo actualiza
una columna; el número de filas de la plata no cambia. Verificado
(statement `01f1d9a8-cabb-1e9e-ae13-0e1dbadfd4ca`):

| tabla | filas |
|---|---|
| `oceanwatch.bronze.ais_raw` | 25.897.493 |
| `oceanwatch.silver.ais_clean` | 25.857.101 |
| `oceanwatch.quarantine.ais_rejected` | 39.734 |

`plata + cuarentena + 658 = 25.857.101 + 39.734 + 658 = 25.897.493 = bronce` ⇒
**diferencia 0, la invariante cuadra** (los 658 = duplicados exactos eliminados por la
dedup con watermark, ADR-006). La plata sigue en **25.857.101** filas (igual que antes del
enriquecimiento): poblar `en_agua` no alteró la conciliación.

## Reproducibilidad / limpieza

- Tabla de referencia `oceanwatch.silver.celda_en_agua` y CSV en
  `oceanwatch.bronze.landing/ref/celda_en_agua.csv` **se conservan** (referencia útil).
- Script reproducible: `resources/sql/fase3_enriquecer_en_agua.sql` (CTAS + MERGE +
  verificación). Lógica de clasificación reutilizable (sin Spark, sin `pandas_udf`):
  `materializar.clasificar_celdas_en_agua` / `materializar.enriquecer_en_agua_por_celda`.
- Scripts temporales de extracción/clasificación (`_*.py`, `_*.csv`) viven FUERA del repo
  (raíz `PROYECTO`, prefijo `_`) y se eliminan al cerrar la tarea. Ningún secreto en el repo.
