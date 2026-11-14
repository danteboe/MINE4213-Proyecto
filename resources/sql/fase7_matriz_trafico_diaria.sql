-- =============================================================================
-- Req. 4 (Fase 7) — Vista de apoyo en ORO: gold.matriz_trafico_diaria.
-- =============================================================================
-- El visual 1 del dashboard (mapa de intensidad de trafico por celda) necesita
-- UNA fila por celda y DIA, pero gold.matriz_trafico tiene granularidad celda x
-- HORA (2.319.365 filas; ~324k-371k celdas distintas por dia). Calcular ese
-- GROUP BY (colapsar las horas) en cada render del dashboard sobre 2,3 M filas es
-- caro y repetido.
--
-- Decision D2/D4 (ADR-017): se MATERIALIZA la agregacion UNA sola vez en la capa
-- ORO (no en silver, no en el dashboard). La tabla resultante tiene 1 fila por
-- (celda_lat, celda_lon, dia) con intensidad = SUM(n_posiciones) y buques =
-- SUM(n_buques). Asi el dashboard consume SOLO oro pre-agregada y NO recomputa.
--
-- Linaje: oceanwatch.gold.matriz_trafico (que a su vez viene de silver.ais_clean).
-- Idempotente: CREATE OR REPLACE TABLE ... AS SELECT regenera el estado completo
-- (no hay carga por fecha; se reconstruye entera desde la matriz horaria de oro).
-- No depende de silver/bronze en tiempo de consulta del dashboard.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS oceanwatch.gold;

-- Columnas resultantes:
--   celda_lat  DOUBLE  round(LAT, 2), clave de celda (ADR-001).
--   celda_lon  DOUBLE  round(LON, 2), clave de celda (ADR-001).
--   dia        DATE    date(hora), clave temporal del dashboard (parametro :fecha).
--   intensidad BIGINT  SUM(n_posiciones): posiciones AIS en la celda ese dia (metrica principal del mapa).
--   buques     BIGINT  SUM(n_buques): metrica SECUNDARIA; suma los conteos de buques distintos-por-hora,
--                      por lo que un buque presente en varias horas se cuenta mas de una vez (aproximacion
--                      de actividad, NO buques unicos por dia). Documentado en el contrato de oro.
CREATE OR REPLACE TABLE oceanwatch.gold.matriz_trafico_diaria
USING DELTA
COMMENT 'Densidad de trafico por celda (lat/lon 2 decimales, ADR-001) y DIA. Agregado de gold.matriz_trafico (colapsa la hora) para el mapa de intensidad del dashboard Req.4. Solo-oro pre-agregado (D2/D4, ADR-017, Fase 7).'
AS
SELECT
  celda_lat,
  celda_lon,
  date(hora) AS dia,
  SUM(n_posiciones) AS intensidad,
  SUM(n_buques)     AS buques
FROM oceanwatch.gold.matriz_trafico
GROUP BY celda_lat, celda_lon, date(hora);
