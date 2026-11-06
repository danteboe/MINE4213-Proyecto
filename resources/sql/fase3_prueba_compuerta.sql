-- =============================================================================
-- FEAT-005 (Req. 2) — PRUEBA DE COMPUERTA extremo a extremo (serverless).
-- =============================================================================
-- Inyecta registros SUCIOS conocidos (uno por cada regla de DESCARTE) + registros
-- LIMPIOS de control en un sandbox AISLADO (esquema oceanwatch.quality, tablas con
-- prefijo pf_), los pasa por EXACTAMENTE las mismas compuertas de la capa plata
-- (resources/sql/fase3_materializar_plata.sql = src/oceanwatch/quality/compuertas.py)
-- y por la MISMA lógica de la capa oro (resources/sql/fase3_materializar_oro.sql =
-- src/oceanwatch/gold/), escribiendo a tablas sandbox pf_silver / pf_quarantine /
-- pf_metricas / pf_gold_*. Demuestra con CONTEOS REALES que NINGÚN sucio llega a
-- silver ni a oro y que TODOS aparecen en cuarentena con su causa correcta.
--
-- NO toca oceanwatch.bronze.ais_raw, silver.ais_clean, quarantine.ais_rejected,
-- quality.metricas_calidad ni gold.* de PRODUCCIÓN. Todo lo sandbox es pf_* y se
-- ELIMINA tras capturar la evidencia (ver fase3_limpieza_sandbox.sql).
-- =============================================================================

-- ---------- 0) ENTRADA SANDBOX: oceanwatch.quality.pf_gate_input ----------------
-- Mismo esquema que oceanwatch.bronze.ais_raw (17 AIS + 4 auditoría). Cada fila
-- lleva un identificador de caso (_caso) y la regla de descarte que se ESPERA que
-- viole (_regla_esperada); los limpios llevan _regla_esperada = '(limpio)'.
-- Un puerto real del WPI (NEW YORK CITY, INDEX_NO 7640, 40.7/-74.016667 => celda
-- 40.70/-74.02) se usa para que los LIMPIOS de control (3 pings en esa celda-puerto)
-- lleguen también a pf_gold_presencia (permanencia >= 3 posiciones).
CREATE SCHEMA IF NOT EXISTS oceanwatch.quality;
CREATE OR REPLACE TABLE oceanwatch.quality.pf_gate_input AS
SELECT * FROM VALUES
  -- _caso, _regla_esperada, MMSI, BaseDateTime, LAT, LON, SOG, COG, Heading,
  --   VesselName, IMO, CallSign, VesselType, Status, Length, Width, Draft, Cargo,
  --   TransceiverClass, _rescued_data, _ingest_timestamp, _source_file, _batch_date
  -- ----- SUCIOS (uno por regla de DESCARTE) -----
  ( 'sucio_coord_rango',   'coordenadas_en_rango',
    CAST(367000010 AS BIGINT), TIMESTAMP'2023-06-01 10:00:00', 200.0, -74.01, 5.0, 90.0, 180.0,
    'PF_BAD_COORD', NULL, 'PFA1', 70, 0, 100.0, 20.0, 5.0, 70, 'A',
    NULL, TIMESTAMP'2023-06-01 10:00:01', 'pf_gate', DATE'2023-06-01' ),
  ( 'sucio_centinela_lat', 'coordenada_no_centinela',
    CAST(367000011 AS BIGINT), TIMESTAMP'2023-06-01 10:01:00', 91.0, -74.01, 5.0, 90.0, 180.0,
    'PF_SENTINEL', NULL, 'PFA2', 70, 0, 100.0, 20.0, 5.0, 70, 'A',
    NULL, TIMESTAMP'2023-06-01 10:01:01', 'pf_gate', DATE'2023-06-01' ),
  ( 'sucio_mmsi_8digitos', 'mmsi_bien_formado',
    CAST(30012345 AS BIGINT), TIMESTAMP'2023-06-01 10:02:00', 40.70, -74.01, 5.0, 90.0, 180.0,
    'PF_MMSI8', NULL, 'PFA3', 70, 0, 100.0, 20.0, 5.0, 70, 'A',
    NULL, TIMESTAMP'2023-06-01 10:02:01', 'pf_gate', DATE'2023-06-01' ),
  ( 'sucio_mmsi_mid',      'mmsi_mid_valido',
    CAST(999000000 AS BIGINT), TIMESTAMP'2023-06-01 10:03:00', 40.70, -74.01, 5.0, 90.0, 180.0,
    'PF_MID', NULL, 'PFA4', 70, 0, 100.0, 20.0, 5.0, 70, 'A',
    NULL, TIMESTAMP'2023-06-01 10:03:01', 'pf_gate', DATE'2023-06-01' ),
  ( 'sucio_ts_futuro',     'timestamp_valido_no_futuro',
    CAST(367000012 AS BIGINT), TIMESTAMP'2099-01-01 00:00:00', 40.70, -74.01, 5.0, 90.0, 180.0,
    'PF_FUTURO', NULL, 'PFA5', 70, 0, 100.0, 20.0, 5.0, 70, 'A',
    NULL, TIMESTAMP'2023-06-01 10:04:01', 'pf_gate', DATE'2023-06-01' ),
  -- ----- DUPLICADO EXACTO (misma clave MMSI+BaseDateTime que un limpio) -----
  -- El limpio_ctrl_1 (abajo) y este par comparten (367000020, 2023-06-01 11:00:00):
  -- la dedup (ROW_NUMBER()=1) conserva UNO y descarta el otro; ninguno de los dos
  -- es "sucio" (ambos son filas válidas), pero el duplicado NO debe duplicar silver.
  ( 'dup_exacto',          'duplicado',
    CAST(367000020 AS BIGINT), TIMESTAMP'2023-06-01 11:00:00', 40.70, -74.01, 6.0, 91.0, 182.0,
    'PF_DUP', NULL, 'PFD1', 70, 0, 110.0, 21.0, 6.0, 70, 'A',
    NULL, TIMESTAMP'2023-06-01 11:00:30', 'pf_gate', DATE'2023-06-01' ),
  -- ----- LIMPIOS de control (deben llegar a pf_silver y a pf_gold) -----
  -- Tres pings del mismo buque en la celda-puerto de NEW YORK CITY (40.70/-74.01)
  -- => permanencia >= 3 => aparece en pf_gold_presencia. limpio_ctrl_1 comparte
  -- clave con dup_exacto (uno de los dos sobrevive a la dedup).
  ( 'limpio_ctrl_1',       '(limpio)',
    CAST(367000020 AS BIGINT), TIMESTAMP'2023-06-01 11:00:00', 40.701, -74.016, 6.0, 90.0, 180.0,
    'PF_CLEAN', 'IMO9074729', 'PFC1', 70, 0, 110.0, 21.0, 6.0, 70, 'A',
    NULL, TIMESTAMP'2023-06-01 11:00:05', 'pf_gate', DATE'2023-06-01' ),
  ( 'limpio_ctrl_2',       '(limpio)',
    CAST(367000020 AS BIGINT), TIMESTAMP'2023-06-01 11:05:00', 40.702, -74.017, 6.1, 95.0, 100.0,
    'PF_CLEAN', 'IMO9074729', 'PFC1', 70, 0, 110.0, 21.0, 6.0, 70, 'A',
    NULL, TIMESTAMP'2023-06-01 11:05:05', 'pf_gate', DATE'2023-06-01' ),
  ( 'limpio_ctrl_3',       '(limpio)',
    CAST(367000020 AS BIGINT), TIMESTAMP'2023-06-01 11:10:00', 40.703, -74.018, 6.2, 97.0, 110.0,
    'PF_CLEAN', 'IMO9074729', 'PFC1', 70, 0, 110.0, 21.0, 6.0, 70, 'A',
    NULL, TIMESTAMP'2023-06-01 11:10:05', 'pf_gate', DATE'2023-06-01' )
AS t(_caso, _regla_esperada, MMSI, BaseDateTime, LAT, LON, SOG, COG, Heading,
      VesselName, IMO, CallSign, VesselType, Status, Length, Width, Draft, Cargo,
      TransceiverClass, _rescued_data, _ingest_timestamp, _source_file, _batch_date);

-- ---------- 1) pf_silver (compuertas de plata; size(causas)=0) ------------------
-- Reproduce EXACTAMENTE la CTE de fase3_materializar_plata.sql, pero sobre el
-- sandbox pf_gate_input. Conserva _caso/_regla_esperada para cotejar la evidencia.
CREATE OR REPLACE TABLE oceanwatch.quality.pf_silver AS
WITH dedup AS (
  SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY MMSI, BaseDateTime ORDER BY _ingest_timestamp) AS _rn
    FROM oceanwatch.quality.pf_gate_input
  ) WHERE _rn = 1
),
gated AS (
  SELECT d.*,
    CASE WHEN CAST(MMSI AS STRING) RLIKE '^[0-9]+$' THEN CAST(MMSI AS STRING) ELSE NULL END AS _mmsi_txt
  FROM dedup d
),
flags AS (
  SELECT g.*,
    array_except(array(
      CASE WHEN NOT COALESCE(LAT IS NOT NULL AND LON IS NOT NULL AND LAT BETWEEN -90.0 AND 90.0 AND LON BETWEEN -180.0 AND 180.0, false) THEN 'coordenadas_en_rango' END,
      CASE WHEN COALESCE(LAT = 91.0 OR LON = 181.0, false) THEN 'coordenada_no_centinela' END,
      CASE WHEN NOT COALESCE(_mmsi_txt IS NOT NULL AND length(_mmsi_txt) = 9, false) THEN 'mmsi_bien_formado' END,
      CASE WHEN NOT COALESCE(_mmsi_txt IS NOT NULL AND length(_mmsi_txt) = 9 AND CAST(substring(_mmsi_txt,1,3) AS INT) BETWEEN 201 AND 775, false) THEN 'mmsi_mid_valido' END,
      CASE WHEN NOT COALESCE(BaseDateTime IS NOT NULL AND BaseDateTime <= current_timestamp(), false) THEN 'timestamp_valido_no_futuro' END
    ), array(CAST(NULL AS STRING))) AS causas
  FROM gated g
)
SELECT _caso, _regla_esperada, MMSI, BaseDateTime, LAT, LON,
  CASE WHEN COALESCE(SOG >= 102.3, false) THEN NULL ELSE SOG END     AS SOG,
  CASE WHEN COALESCE(COG = 360.0, false)  THEN NULL ELSE COG END     AS COG,
  CASE WHEN COALESCE(Heading = 511.0, false) THEN NULL ELSE Heading END AS Heading,
  VesselName, IMO, CallSign, VesselType, Status, Length, Width, Draft, Cargo, TransceiverClass
FROM flags
WHERE size(causas) = 0;

-- ---------- 2) pf_quarantine (size(causas)>0) -----------------------------------
CREATE OR REPLACE TABLE oceanwatch.quality.pf_quarantine AS
WITH dedup AS (
  SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY MMSI, BaseDateTime ORDER BY _ingest_timestamp) AS _rn
    FROM oceanwatch.quality.pf_gate_input
  ) WHERE _rn = 1
),
gated AS (
  SELECT d.*,
    CASE WHEN CAST(MMSI AS STRING) RLIKE '^[0-9]+$' THEN CAST(MMSI AS STRING) ELSE NULL END AS _mmsi_txt
  FROM dedup d
),
flags AS (
  SELECT g.*,
    array_except(array(
      CASE WHEN NOT COALESCE(LAT IS NOT NULL AND LON IS NOT NULL AND LAT BETWEEN -90.0 AND 90.0 AND LON BETWEEN -180.0 AND 180.0, false) THEN 'coordenadas_en_rango' END,
      CASE WHEN COALESCE(LAT = 91.0 OR LON = 181.0, false) THEN 'coordenada_no_centinela' END,
      CASE WHEN NOT COALESCE(_mmsi_txt IS NOT NULL AND length(_mmsi_txt) = 9, false) THEN 'mmsi_bien_formado' END,
      CASE WHEN NOT COALESCE(_mmsi_txt IS NOT NULL AND length(_mmsi_txt) = 9 AND CAST(substring(_mmsi_txt,1,3) AS INT) BETWEEN 201 AND 775, false) THEN 'mmsi_mid_valido' END,
      CASE WHEN NOT COALESCE(BaseDateTime IS NOT NULL AND BaseDateTime <= current_timestamp(), false) THEN 'timestamp_valido_no_futuro' END
    ), array(CAST(NULL AS STRING))) AS causas
  FROM gated g
)
SELECT _caso, _regla_esperada, MMSI, BaseDateTime, LAT, LON, causas,
  current_timestamp() AS _fecha_cuarentena
FROM flags
WHERE size(causas) > 0;

-- ---------- 3) pf_gold_trayectorias (lee SOLO de pf_silver) ---------------------
CREATE OR REPLACE TABLE oceanwatch.quality.pf_gold_trayectorias AS
WITH base AS (
  SELECT MMSI, BaseDateTime, LAT, LON, to_date(BaseDateTime) AS dia
  FROM oceanwatch.quality.pf_silver
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
SELECT MMSI, dia, count(*) AS n_posiciones, sum(_dist_km) AS distancia_total_km,
  min(BaseDateTime) AS primera_posicion, max(BaseDateTime) AS ultima_posicion
FROM con_dist GROUP BY MMSI, dia;

-- ---------- 4) pf_gold_presencia (lee SOLO de pf_silver x WPI real) -------------
CREATE OR REPLACE TABLE oceanwatch.quality.pf_gold_presencia AS
WITH ais AS (
  SELECT MMSI, BaseDateTime, to_date(BaseDateTime) AS dia,
         round(LAT, 2) AS celda_lat, round(LON, 2) AS celda_lon
  FROM oceanwatch.quality.pf_silver
),
puertos AS (
  SELECT INDEX_NO, PORT_NAME, COUNTRY,
         round(LATITUDE, 2) AS celda_lat, round(LONGITUDE, 2) AS celda_lon
  FROM oceanwatch.gold.world_port_index
),
cruce AS (
  SELECT a.MMSI, a.dia, a.BaseDateTime, p.INDEX_NO, p.PORT_NAME, p.COUNTRY
  FROM ais a JOIN puertos p ON a.celda_lat = p.celda_lat AND a.celda_lon = p.celda_lon
),
agg AS (
  SELECT MMSI, INDEX_NO, PORT_NAME, COUNTRY, dia, count(*) AS n_posiciones_en_puerto
  FROM cruce GROUP BY MMSI, INDEX_NO, PORT_NAME, COUNTRY, dia
)
SELECT * FROM agg WHERE n_posiciones_en_puerto >= 3;

-- ---------- 5) pf_gold_matriz (lee SOLO de pf_silver) ---------------------------
CREATE OR REPLACE TABLE oceanwatch.quality.pf_gold_matriz AS
SELECT round(LAT, 2) AS celda_lat, round(LON, 2) AS celda_lon,
  date_trunc('hour', BaseDateTime) AS hora, count(*) AS n_posiciones,
  count(DISTINCT MMSI) AS n_buques
FROM oceanwatch.quality.pf_silver
GROUP BY round(LAT, 2), round(LON, 2), date_trunc('hour', BaseDateTime);
