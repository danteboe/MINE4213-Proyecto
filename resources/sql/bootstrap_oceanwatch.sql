-- Bootstrap idempotente de Unity Catalog — OceanWatch Analytics (Entrega 2)
-- PENDIENTE DE EJECUCIÓN — NO ejecutar en Fase 1.
--
-- Variante SQL declarativa equivalente a src/oceanwatch/ingest/bootstrap.py.
-- Se mantiene esta forma para ejecución directa y auditable en un notebook o
-- SQL editor de Databricks serverless (ver ADR-002). Todas las sentencias usan
-- IF NOT EXISTS para ser idempotentes. No contiene secretos.
--
-- Convención de nombres (ADR-002):
--   Catálogo único 'oceanwatch'; un esquema por capa medallion más esquemas
--   auxiliares. El esquema 'entrega1' NO se toca aquí (se preserva de Entrega 1).

-- Catálogo del proyecto
CREATE CATALOG IF NOT EXISTS oceanwatch
  COMMENT 'OceanWatch Analytics — lakehouse de tráfico marítimo AIS (Entrega 2)';

-- Esquemas por capa medallion + auxiliares
CREATE SCHEMA IF NOT EXISTS oceanwatch.bronze
  COMMENT 'Capa bronce: ingesta cruda de CSV AIS vía Auto Loader.';
CREATE SCHEMA IF NOT EXISTS oceanwatch.silver
  COMMENT 'Capa plata: datos saneados y conformados tras reglas de calidad.';
CREATE SCHEMA IF NOT EXISTS oceanwatch.gold
  COMMENT 'Capa oro: matrices de tráfico por grilla lat/lon y cruce con WPI.';
CREATE SCHEMA IF NOT EXISTS oceanwatch.quarantine
  COMMENT 'Registros rechazados por reglas de calidad (dead-letter).';
CREATE SCHEMA IF NOT EXISTS oceanwatch.quality
  COMMENT 'Métricas y reportes de calidad de datos.';
CREATE SCHEMA IF NOT EXISTS oceanwatch.ml
  COMMENT 'Features y artefactos del modelo de anomalías de velocidad (SOG).';

-- Volúmenes de la capa bronce
CREATE VOLUME IF NOT EXISTS oceanwatch.bronze.landing
  COMMENT 'Zona de aterrizaje: zips NOAA y CSV descomprimidos.';
CREATE VOLUME IF NOT EXISTS oceanwatch.bronze.checkpoints
  COMMENT 'Checkpoints de Structured Streaming (Auto Loader).';
