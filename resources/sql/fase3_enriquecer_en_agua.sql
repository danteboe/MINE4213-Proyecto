-- =============================================================================
-- Fase 3 (Entrega 2) — Materializar la bandera de calidad `en_agua` en la capa plata
-- Requisito 2 (capas de calidad), ADR-010 (máscara tierra/agua, NULIFICAR-ATRIBUTO).
--
-- Contexto: tras la materialización de plata (FEAT-003) la columna `en_agua` de
-- `oceanwatch.silver.ais_clean` quedó en NULL porque el `pandas_udf` de
-- `global_land_mask` falló en el cómputo serverless (Spark Connect) sobre las
-- 25.857.101 filas. Enfoque robusto (sin pandas_udf): clasificar las CELDAS
-- DISTINTAS de la grilla (round(LAT,2),round(LON,2) — ADR-001, geo.celda_grilla),
-- no las 25.9M filas, y enriquecer la plata por join de celda.
--
-- Camino real de ejecución (verificado 2026-11-05, warehouse serverless
-- `377600528fed7c47`):
--   1) Extracción local de las 726.513 celdas distintas vía SQL Statement Execution
--      API (paginación por next_chunk_index) + clasificación vectorizada con
--      global-land-mask LOCAL (globe.is_land sobre arrays numpy; en_agua = NOT is_land).
--   2) CSV subido al Volume oceanwatch.bronze.landing/ref/celda_en_agua.csv
--      (`databricks fs cp`).
--   3) Este script: CTAS de la tabla de referencia + MERGE sobre la plata.
--
-- La bandera `en_agua` es NULIFICAR-ATRIBUTO (informativa): NO descarta filas, NO
-- entra en `causas`, NO afecta la invariante de conciliación (bronce = plata +
-- cuarentena + 658). Solo se POBLA (ADR-010 §4). Idempotente (reejecutable).
-- =============================================================================

-- (1) Tabla de referencia celda -> en_agua (726.513 celdas distintas de la plata).
--     Se lee el CSV del Volume con esquema explícito (read_files) y se persiste como
--     Delta vía CTAS. NOTA: Databricks NO permite declarar la lista de columnas en un
--     CREATE OR REPLACE TABLE AS SELECT (RTAS); los nombres/tipos vienen de los alias y
--     CAST del SELECT. Se CONSERVA como referencia reutilizable (no es temporal).
CREATE OR REPLACE TABLE oceanwatch.silver.celda_en_agua
COMMENT 'Referencia tierra/agua por celda de grilla (ADR-010). Poblada desde global-land-mask sobre las celdas distintas de silver.ais_clean.'
AS
SELECT
  CAST(celda_lat AS DOUBLE)  AS celda_lat,
  CAST(celda_lon AS DOUBLE)  AS celda_lon,
  CAST(en_agua   AS BOOLEAN) AS en_agua
FROM read_files(
  '/Volumes/oceanwatch/bronze/landing/ref/celda_en_agua.csv',
  format => 'csv',
  header => true,
  schema => 'celda_lat DOUBLE, celda_lon DOUBLE, en_agua BOOLEAN'
);

-- (2) Enriquecer la plata: poblar `en_agua` por join de celda (redondeo a 2 decimales,
--     EXACTAMENTE la definición de celda de ADR-001 / geo.celda_grilla). MERGE INTO
--     porque Delta soporta MERGE con condición de join por celda de forma robusta.
MERGE INTO oceanwatch.silver.ais_clean AS c
USING oceanwatch.silver.celda_en_agua AS r
ON round(c.LAT, 2) = r.celda_lat AND round(c.LON, 2) = r.celda_lon
WHEN MATCHED THEN UPDATE SET c.en_agua = r.en_agua;

-- (3) Verificación de conteos (debe quedar 0 NULL salvo filas con LAT/LON nulas, que
--     no existen en plata porque coordenadas_en_rango es regla de DESCARTE).
SELECT
  count(*)                                              AS filas,
  sum(CASE WHEN en_agua IS NOT NULL THEN 1 ELSE 0 END)  AS no_null,
  sum(CASE WHEN en_agua THEN 1 ELSE 0 END)              AS en_agua_true,
  sum(CASE WHEN NOT en_agua THEN 1 ELSE 0 END)          AS en_tierra
FROM oceanwatch.silver.ais_clean;
