-- =============================================================================
-- FEAT-003 (Req. 2) — Materialización de la capa PLATA en SQL (serverless).
-- =============================================================================
-- Camino de ejecución REAL en serverless. El notebook notebooks/fase3_plata_calidad.py
-- orquesta el MISMO pipeline con la API de Spark (foreachBatch), pero el cómputo
-- serverless (Spark Connect GC) devolvió un error interno (INTERNAL_ERROR) al escribir
-- Delta sobre 25.9M filas (ver evidence/req02-capas-calidad/materializacion_plata.md).
-- Estas sentencias CTAS, ejecutadas en el SQL warehouse serverless (id 377600528fed7c47)
-- vía SQL Statement Execution API, SÍ materializan las tres tablas de forma fiable y
-- reproducen EXACTAMENTE las compuertas de src/oceanwatch/quality/compuertas.py:
--   - dedup (ADR-006): ROW_NUMBER() por (MMSI, BaseDateTime) = 1 (elimina los 658 dup).
--   - NULIFICAR-ATRIBUTO: SOG>=102.3, Heading=511, COG=360, Length/Width/Draft<=0,
--     IMO patrón inválido -> atributo NULL + bandera booleana.
--   - DESCARTAR-FILA (causas array<string>): coordenadas_en_rango, coordenada_no_centinela,
--     mmsi_bien_formado, mmsi_mid_valido, timestamp_valido_no_futuro.
--   - en_agua (ADR-010, NULIFICAR informativa): NULL aquí (requiere global_land_mask,
--     no disponible en SQL; se enriquece con el pandas_udf de compuertas._columna_en_agua).
-- Patrón: Data Quality Gate + Dead-letter/Quarantine (ADR-011).
-- =============================================================================

-- ---------- 1) PLATA: oceanwatch.silver.ais_clean (size(causas)=0) ----------
CREATE OR REPLACE TABLE oceanwatch.silver.ais_clean AS
WITH dedup AS (
  SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY MMSI, BaseDateTime ORDER BY _ingest_timestamp) AS _rn
    FROM oceanwatch.bronze.ais_raw
  ) WHERE _rn = 1
),
imo_calc AS (
  SELECT d.*,
    CASE
      WHEN IMO IS NULL OR trim(CAST(IMO AS STRING)) = '' THEN NULL
      WHEN trim(CAST(IMO AS STRING)) RLIKE '(?i)^IMO[0-9]{7}$' THEN regexp_extract(trim(CAST(IMO AS STRING)), '(?i)^IMO([0-9]{7})$', 1)
      WHEN trim(CAST(IMO AS STRING)) RLIKE '^[0-9]{7}$' THEN trim(CAST(IMO AS STRING))
      ELSE NULL
    END AS _imo7
  FROM dedup d
),
gated AS (
  SELECT g.*,
    CASE
      WHEN IMO IS NULL OR trim(CAST(IMO AS STRING)) = '' THEN true
      WHEN _imo7 IS NULL THEN false
      ELSE CAST(substring(_imo7,7,1) AS INT) = (
        ( CAST(substring(_imo7,1,1) AS INT)*7 + CAST(substring(_imo7,2,1) AS INT)*6
        + CAST(substring(_imo7,3,1) AS INT)*5 + CAST(substring(_imo7,4,1) AS INT)*4
        + CAST(substring(_imo7,5,1) AS INT)*3 + CAST(substring(_imo7,6,1) AS INT)*2 ) % 10 )
    END AS _imo_valido,
    CASE WHEN CAST(MMSI AS STRING) RLIKE '^[0-9]+$' THEN CAST(MMSI AS STRING) ELSE NULL END AS _mmsi_txt
  FROM imo_calc g
),
flags AS (
  SELECT g.*,
    COALESCE(SOG IS NOT NULL AND SOG >= 102.3, false)     AS sog_no_disponible,
    COALESCE(Heading = 511.0, false)                      AS heading_desconocido,
    COALESCE(COG = 360.0, false)                          AS cog_desconocido,
    COALESCE(Length IS NOT NULL AND Length <= 0.0, false) AS length_no_fisica,
    COALESCE(Width  IS NOT NULL AND Width  <= 0.0, false) AS width_no_fisica,
    COALESCE(Draft  IS NOT NULL AND Draft  <= 0.0, false) AS draft_no_fisica,
    (NOT _imo_valido)                                     AS imo_invalido,
    array_except(array(
      CASE WHEN NOT COALESCE(LAT IS NOT NULL AND LON IS NOT NULL AND LAT BETWEEN -90.0 AND 90.0 AND LON BETWEEN -180.0 AND 180.0, false) THEN 'coordenadas_en_rango' END,
      CASE WHEN COALESCE(LAT = 91.0 OR LON = 181.0, false) THEN 'coordenada_no_centinela' END,
      CASE WHEN NOT COALESCE(_mmsi_txt IS NOT NULL AND length(_mmsi_txt) = 9, false) THEN 'mmsi_bien_formado' END,
      CASE WHEN NOT COALESCE(_mmsi_txt IS NOT NULL AND length(_mmsi_txt) = 9 AND CAST(substring(_mmsi_txt,1,3) AS INT) BETWEEN 201 AND 775, false) THEN 'mmsi_mid_valido' END,
      CASE WHEN NOT COALESCE(BaseDateTime IS NOT NULL AND BaseDateTime <= current_timestamp(), false) THEN 'timestamp_valido_no_futuro' END
    ), array(CAST(NULL AS STRING))) AS causas
  FROM gated g
)
SELECT
  MMSI, BaseDateTime, LAT, LON,
  CASE WHEN sog_no_disponible   THEN NULL ELSE SOG END     AS SOG,
  CASE WHEN cog_desconocido     THEN NULL ELSE COG END     AS COG,
  CASE WHEN heading_desconocido THEN NULL ELSE Heading END AS Heading,
  VesselName,
  CASE WHEN imo_invalido THEN NULL ELSE IMO END            AS IMO,
  CallSign, VesselType, Status,
  CASE WHEN length_no_fisica THEN NULL ELSE Length END     AS Length,
  CASE WHEN width_no_fisica  THEN NULL ELSE Width END      AS Width,
  CASE WHEN draft_no_fisica  THEN NULL ELSE Draft END      AS Draft,
  Cargo, TransceiverClass,
  _rescued_data, _ingest_timestamp, _source_file, _batch_date,
  sog_no_disponible, heading_desconocido, cog_desconocido,
  length_no_fisica, width_no_fisica, draft_no_fisica, imo_invalido,
  CAST(NULL AS BOOLEAN) AS en_agua
FROM flags
WHERE size(causas) = 0;

-- ---------- 2) CUARENTENA: oceanwatch.quarantine.ais_rejected (size(causas)>0) --
CREATE OR REPLACE TABLE oceanwatch.quarantine.ais_rejected AS
WITH dedup AS (
  SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY MMSI, BaseDateTime ORDER BY _ingest_timestamp) AS _rn
    FROM oceanwatch.bronze.ais_raw
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
SELECT
  MMSI, BaseDateTime, LAT, LON, SOG, COG, Heading, VesselName, IMO, CallSign,
  VesselType, Status, Length, Width, Draft, Cargo, TransceiverClass,
  _rescued_data, _ingest_timestamp, _source_file, _batch_date,
  causas,
  current_timestamp() AS _fecha_cuarentena
FROM flags
WHERE size(causas) > 0;

-- ---------- 3) MÉTRICAS: oceanwatch.quality.metricas_calidad (día x regla) -----
CREATE OR REPLACE TABLE oceanwatch.quality.metricas_calidad AS
WITH dedup AS (
  SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY MMSI, BaseDateTime ORDER BY _ingest_timestamp) AS _rn
    FROM oceanwatch.bronze.ais_raw
  ) WHERE _rn = 1
),
gated AS (
  SELECT _batch_date,
    CASE WHEN CAST(MMSI AS STRING) RLIKE '^[0-9]+$' THEN CAST(MMSI AS STRING) ELSE NULL END AS _mmsi_txt,
    LAT, LON, BaseDateTime
  FROM dedup
),
causas AS (
  SELECT _batch_date,
    array_except(array(
      CASE WHEN NOT COALESCE(LAT IS NOT NULL AND LON IS NOT NULL AND LAT BETWEEN -90.0 AND 90.0 AND LON BETWEEN -180.0 AND 180.0, false) THEN 'coordenadas_en_rango' END,
      CASE WHEN COALESCE(LAT = 91.0 OR LON = 181.0, false) THEN 'coordenada_no_centinela' END,
      CASE WHEN NOT COALESCE(_mmsi_txt IS NOT NULL AND length(_mmsi_txt) = 9, false) THEN 'mmsi_bien_formado' END,
      CASE WHEN NOT COALESCE(_mmsi_txt IS NOT NULL AND length(_mmsi_txt) = 9 AND CAST(substring(_mmsi_txt,1,3) AS INT) BETWEEN 201 AND 775, false) THEN 'mmsi_mid_valido' END,
      CASE WHEN NOT COALESCE(BaseDateTime IS NOT NULL AND BaseDateTime <= current_timestamp(), false) THEN 'timestamp_valido_no_futuro' END
    ), array(CAST(NULL AS STRING))) AS causas
  FROM gated
),
evaluados AS (SELECT _batch_date, count(*) AS evaluados FROM causas GROUP BY _batch_date),
reglas AS (
  SELECT explode(array('coordenadas_en_rango','coordenada_no_centinela','mmsi_bien_formado',
    'mmsi_mid_valido','timestamp_valido_no_futuro')) AS regla
),
esqueleto AS (SELECT e._batch_date, r.regla, e.evaluados FROM evaluados e CROSS JOIN reglas r),
rechazos AS (
  SELECT _batch_date, regla, count(*) AS rechazados
  FROM (SELECT _batch_date, explode(causas) AS regla FROM causas WHERE size(causas) > 0)
  GROUP BY _batch_date, regla
)
SELECT
  s._batch_date, s.regla, s.evaluados,
  s.evaluados - COALESCE(x.rechazados, 0)           AS pasados,
  COALESCE(x.rechazados, 0)                         AS rechazados,
  COALESCE(x.rechazados, 0) / s.evaluados           AS pct_rechazo,
  (COALESCE(x.rechazados, 0) / s.evaluados) >= 0.05 AS alerta,
  0.05                                              AS umbral_alerta,
  current_timestamp()                               AS _registrado
FROM esqueleto s
LEFT JOIN rechazos x ON s._batch_date = x._batch_date AND s.regla = x.regla;
