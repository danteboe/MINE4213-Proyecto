-- =============================================================================
-- FEAT-005 (Req. 2) — Limpieza del sandbox EFÍMERO de la prueba de compuerta.
-- =============================================================================
-- Elimina las tablas pf_* creadas por fase3_prueba_compuerta.sql tras capturar la
-- evidencia (evidence/req02-capas-calidad/prueba_compuerta.md). NO toca las tablas
-- de producción (silver.ais_clean, quarantine.ais_rejected, quality.metricas_calidad,
-- gold.*), que se CONSERVAN.
-- =============================================================================
DROP TABLE IF EXISTS oceanwatch.quality.pf_gate_input;
DROP TABLE IF EXISTS oceanwatch.quality.pf_silver;
DROP TABLE IF EXISTS oceanwatch.quality.pf_quarantine;
DROP TABLE IF EXISTS oceanwatch.quality.pf_gold_trayectorias;
DROP TABLE IF EXISTS oceanwatch.quality.pf_gold_presencia;
DROP TABLE IF EXISTS oceanwatch.quality.pf_gold_matriz;
