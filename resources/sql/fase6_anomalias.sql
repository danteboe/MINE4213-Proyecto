-- =============================================================================
-- FEAT-003 (Req. 6, Fase 6) — Tabla de anomalías de velocidad SOG (serverless).
-- =============================================================================
-- DDL de la tabla de salida del detector de anomalías (ADR-005 / ADR-016). Se
-- expone en el esquema GOLD (no ml, D8) para que el dashboard de Databricks (Req.4)
-- la consuma directamente en el visual 'anomalías del día': una fila por POSICIÓN
-- AIS marcada como anómala por el modelo campeón @champion.
--
-- Fricción serverless REAL (igual que fase3_materializar_oro.sql): el cómputo
-- serverless (Spark Connect) devuelve INTERNAL_ERROR al escribir Delta con
-- df.write.saveAsTable sobre volúmenes grandes. El camino FIABLE es SQL vía
-- spark.sql: este .sql SOLO crea la tabla (CREATE TABLE IF NOT EXISTS, no destruye
-- datos de otras fechas) y la carga POR FECHA la hace el notebook de inferencia
-- (notebooks/fase6_tarea_ml_anomalias.py) con un DELETE + INSERT idempotente de la
-- partición _batch_date = fecha_proceso (NUNCA df.write.saveAsTable).
--
-- La tabla se alimenta SOLO de oceanwatch.silver.ais_clean (posiciones con SOG real)
-- puntuadas por el IsolationForest registrado en Unity Catalog.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS oceanwatch.gold;

-- ---------- Tabla de anomalías: oceanwatch.gold.anomalias_velocidad -------------
-- Granularidad = 1 fila por posición AIS anómala (es_anomalia = true) de un día.
-- Clave lógica natural = (MMSI, BaseDateTime). Particionada lógicamente por
-- _batch_date (fecha de proceso) para refresco idempotente por día.
CREATE TABLE IF NOT EXISTS oceanwatch.gold.anomalias_velocidad (
  MMSI           BIGINT    COMMENT 'Identificador MMSI del buque.',
  BaseDateTime   TIMESTAMP COMMENT 'Instante de la posición AIS anómala (UTC, ADR-004).',
  LAT            DOUBLE    COMMENT 'Latitud de la posición (grados decimales).',
  LON            DOUBLE    COMMENT 'Longitud de la posición (grados decimales).',
  SOG            DOUBLE    COMMENT 'Velocidad sobre el fondo (knots) de la posición.',
  VesselType     INT       COMMENT 'Tipo de buque AIS (referencia del z-score relativo).',
  z_sog          DOUBLE    COMMENT 'z-score de SOG relativo al tipo: (SOG-media_tipo)/std_tipo (D1, ADR-005).',
  score_anomalia DOUBLE    COMMENT 'score_samples del IsolationForest (mayor es mas normal, menor es mas anomalo).',
  es_anomalia    BOOLEAN   COMMENT 'True si el modelo campeón marcó la posición como anómala (predict == -1).',
  razon          STRING    COMMENT 'Explicación legible de la anomalía (p. ej. "z_sog=4.2 (>3) tipo 31").',
  _batch_date    DATE      COMMENT 'Fecha de proceso (partición lógica para refresco idempotente por día).'
)
USING DELTA
COMMENT 'Anomalias de velocidad SOG detectadas por el modelo @champion (ADR-005/ADR-016), 1 fila por posicion anomala. Insumo del visual anomalias del dia del dashboard (Req.4).';
