-- =============================================================================
-- FEAT-004 (Req. 2) — Materialización de la capa ORO en SQL (serverless).
-- =============================================================================
-- Camino de ejecución REAL en serverless. El notebook notebooks/fase3_oro.py
-- orquesta el MISMO pipeline con la API de Spark (gold.trayectorias / gold.puertos /
-- gold.matriz_trafico), pero el cómputo serverless (Spark Connect GC) devuelve
-- INTERNAL_ERROR al escribir Delta sobre 25.9M filas (friccion documentada en
-- FEAT-003, evidence/req02-capas-calidad/materializacion_plata.md). Estas sentencias
-- CTAS, ejecutadas en el SQL warehouse serverless (id 377600528fed7c47) vía SQL
-- Statement Execution API, SÍ materializan las tablas de forma fiable y reproducen
-- EXACTAMENTE la lógica de src/oceanwatch/gold/.
--
-- El ORO se alimenta SOLO de oceanwatch.silver.ais_clean (el oro NO limpia).
-- Patrones (ADR-012): Modelo dimensional (star-schema-lite) + Reference Data Join.
-- Grilla espacial por redondeo lat/lon a 2 decimales (ADR-001, NO H3).
-- =============================================================================

-- ---------- 0) REFERENCIA: oceanwatch.gold.world_port_index (~3.669 puertos) ----
-- El WPI (world_port_index/WPI.dbf, 64.8 MB, 78 campos) se convirtió a CSV LOCALMENTE
-- con dbfread (puro Python), evitando instalar geopandas/pyshp en serverless (no
-- preinstaladas, ver evidence/.../disponibilidad_librerias_serverless.md; ADR-012).
-- El CSV (7 campos clave) se subió al Volume landing y se carga aquí como dimensión.
CREATE SCHEMA IF NOT EXISTS oceanwatch.gold;
CREATE OR REPLACE TABLE oceanwatch.gold.world_port_index AS
SELECT
  CAST(INDEX_NO AS INT)      AS INDEX_NO,
  CAST(PORT_NAME AS STRING)  AS PORT_NAME,
  CAST(COUNTRY AS STRING)    AS COUNTRY,
  CAST(LATITUDE AS DOUBLE)   AS LATITUDE,
  CAST(LONGITUDE AS DOUBLE)  AS LONGITUDE,
  CAST(HARBORSIZE AS STRING) AS HARBORSIZE,
  CAST(HARBORTYPE AS STRING) AS HARBORTYPE
FROM read_files('/Volumes/oceanwatch/bronze/landing/wpi_ref_v2/wpi_ref.csv',
     format => 'csv', header => true, inferSchema => true);

-- ---------- 1) TRAYECTORIAS: oceanwatch.gold.trayectorias_diarias ---------------
-- Granularidad = 1 fila por (MMSI, dia). Clave = (MMSI, dia).
-- Distancia haversine (RADIO=6371 km) entre posiciones consecutivas vía LAG en una
-- ventana por (MMSI, dia) ordenada por BaseDateTime. Reproduce gold/trayectorias.py.
CREATE OR REPLACE TABLE oceanwatch.gold.trayectorias_diarias AS
WITH base AS (
  SELECT MMSI, BaseDateTime, LAT, LON, to_date(BaseDateTime) AS dia
  FROM oceanwatch.silver.ais_clean
),
con_lag AS (
  SELECT b.*,
    LAG(LAT) OVER (PARTITION BY MMSI, dia ORDER BY BaseDateTime) AS _lat_prev,
    LAG(LON) OVER (PARTITION BY MMSI, dia ORDER BY BaseDateTime) AS _lon_prev
  FROM base b
),
con_dist AS (
  SELECT c.*,
    CASE WHEN _lat_prev IS NOT NULL THEN
      6371.0 * 2 * ASIN(SQRT(
        POW(SIN(RADIANS(LAT - _lat_prev) / 2), 2)
        + COS(RADIANS(_lat_prev)) * COS(RADIANS(LAT))
          * POW(SIN(RADIANS(LON - _lon_prev) / 2), 2)
      ))
    ELSE 0.0 END AS _dist_km
  FROM con_lag c
)
SELECT
  MMSI, dia,
  count(*)                                                   AS n_posiciones,
  sum(_dist_km)                                              AS distancia_total_km,
  min(BaseDateTime)                                          AS primera_posicion,
  max(BaseDateTime)                                          AS ultima_posicion,
  (CAST(max(BaseDateTime) AS LONG) - CAST(min(BaseDateTime) AS LONG)) / 3600.0 AS duracion_horas,
  min(LAT)                                                   AS lat_min,
  max(LAT)                                                   AS lat_max,
  min(LON)                                                   AS lon_min,
  max(LON)                                                   AS lon_max
FROM con_dist
GROUP BY MMSI, dia;

-- ---------- 2) PRESENCIA EN PUERTOS: oceanwatch.gold.presencia_puertos ----------
-- Granularidad = 1 fila por (MMSI, puerto, dia). Clave = (MMSI, INDEX_NO, dia).
-- Reference Data Join AIS<->WPI por celda de grilla (redondeo lat/lon a 2 decimales,
-- ADR-001); regla 'en puerto' = compartir celda-puerto; permanencia minima = 3
-- posiciones en la celda-puerto ese dia (ADR-012). Reproduce gold/puertos.py.
CREATE OR REPLACE TABLE oceanwatch.gold.presencia_puertos AS
WITH ais AS (
  SELECT MMSI, BaseDateTime, to_date(BaseDateTime) AS dia,
         round(LAT, 2) AS celda_lat, round(LON, 2) AS celda_lon
  FROM oceanwatch.silver.ais_clean
),
puertos AS (
  SELECT INDEX_NO, PORT_NAME, COUNTRY, HARBORSIZE, HARBORTYPE,
         round(LATITUDE, 2) AS celda_lat, round(LONGITUDE, 2) AS celda_lon
  FROM oceanwatch.gold.world_port_index
),
cruce AS (
  SELECT a.MMSI, a.dia, a.BaseDateTime,
         p.INDEX_NO, p.PORT_NAME, p.COUNTRY, p.HARBORSIZE, p.HARBORTYPE
  FROM ais a
  JOIN puertos p ON a.celda_lat = p.celda_lat AND a.celda_lon = p.celda_lon
),
agg AS (
  SELECT MMSI, INDEX_NO, PORT_NAME, COUNTRY, HARBORSIZE, HARBORTYPE, dia,
         count(*)          AS n_posiciones_en_puerto,
         min(BaseDateTime) AS primera_posicion,
         max(BaseDateTime) AS ultima_posicion
  FROM cruce
  GROUP BY MMSI, INDEX_NO, PORT_NAME, COUNTRY, HARBORSIZE, HARBORTYPE, dia
)
SELECT MMSI, INDEX_NO, PORT_NAME, COUNTRY, dia,
       n_posiciones_en_puerto, primera_posicion, ultima_posicion,
       HARBORSIZE, HARBORTYPE
FROM agg
WHERE n_posiciones_en_puerto >= 3;

-- ---------- 3) MATRIZ DE TRÁFICO: oceanwatch.gold.matriz_trafico ----------------
-- Granularidad = 1 fila por (celda_lat, celda_lon, hora). Clave = las tres.
-- Grilla por redondeo lat/lon a 2 decimales (ADR-001); hora = date_trunc('hour').
-- Reproduce gold/matriz_trafico.py.
CREATE OR REPLACE TABLE oceanwatch.gold.matriz_trafico AS
SELECT
  round(LAT, 2)                 AS celda_lat,
  round(LON, 2)                 AS celda_lon,
  date_trunc('hour', BaseDateTime) AS hora,
  count(*)                      AS n_posiciones,
  count(DISTINCT MMSI)          AS n_buques
FROM oceanwatch.silver.ais_clean
GROUP BY round(LAT, 2), round(LON, 2), date_trunc('hour', BaseDateTime);
