-- =============================================================================
-- FASE 5 (Req. 5) — Documentacion en el securable: comentarios de la capa ORO.
-- =============================================================================
-- QUE HACE: aplica COMMENT ON TABLE y COMMENT ON COLUMN a las tablas y columnas
-- clave de oceanwatch.gold que hoy NO estan documentadas en el metastore
-- (world_port_index, trayectorias_diarias, presencia_puertos, matriz_trafico) y
-- completa los comentarios de columna de gold.matriz_trafico_diaria (que solo
-- tenia comentario de TABLA, de la Fase 7). NO re-comenta anomalias_velocidad ni
-- el COMMENT de tabla de matriz_trafico_diaria: ya estaban documentadas (Fase 6/7)
-- y se mantiene su texto para no sobreescribirlo.
--
-- POR QUE: la gobernanza (Req. 5) exige que la documentacion viva DENTRO del
-- securable de Unity Catalog (visible en Catalog Explorer e information_schema),
-- no solo en docs/. Los textos coinciden con docs/data_contract_gold.md.
--
-- PATRON DE INGENIERIA: "documentacion en el securable" (metadata-as-governance)
-- sobre el espacio de nombres de tres niveles de Unity Catalog (ADR-002, ADR-018).
--
-- IDEMPOTENTE Y NO DESTRUCTIVO: COMMENT ON TABLE/COLUMN solo ACTUALIZA metadatos.
-- NO recrea las tablas, NO reescribe datos y NO cambia esquema. Reaplicarlo deja
-- el mismo estado (sobrescribe el texto del comentario con el mismo valor).
-- NOTA: los textos de COMMENT no llevan ';' porque el ejecutor divide por ';'.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- gold.world_port_index (dimension de referencia WPI de la NGA)
-- -----------------------------------------------------------------------------
COMMENT ON TABLE oceanwatch.gold.world_port_index IS 'Dimension de referencia: World Port Index (NGA). 1 fila por puerto (clave INDEX_NO), 3.669 puertos. Cargada desde el .dbf convertido a CSV localmente con dbfread (7 campos clave) para evitar geopandas/pyshp en serverless (ADR-012).';
COMMENT ON COLUMN oceanwatch.gold.world_port_index.INDEX_NO   IS 'Identificador NGA del puerto (clave).';
COMMENT ON COLUMN oceanwatch.gold.world_port_index.PORT_NAME  IS 'Nombre del puerto.';
COMMENT ON COLUMN oceanwatch.gold.world_port_index.COUNTRY    IS 'Codigo de pais (2 letras).';
COMMENT ON COLUMN oceanwatch.gold.world_port_index.LATITUDE   IS 'Latitud del puerto (grados decimales).';
COMMENT ON COLUMN oceanwatch.gold.world_port_index.LONGITUDE  IS 'Longitud del puerto (grados decimales).';
COMMENT ON COLUMN oceanwatch.gold.world_port_index.HARBORSIZE IS 'Tamano del puerto (V/S/M/L, codigo NGA).';
COMMENT ON COLUMN oceanwatch.gold.world_port_index.HARBORTYPE IS 'Tipo de puerto (codigo NGA, p. ej. CN/CB).';

-- -----------------------------------------------------------------------------
-- gold.trayectorias_diarias (hecho: trayectoria diaria por buque)
-- -----------------------------------------------------------------------------
COMMENT ON TABLE oceanwatch.gold.trayectorias_diarias IS 'Hecho: trayectoria diaria por buque (1 fila por MMSI y dia). Suma haversine (R=6371 km) entre posiciones consecutivas (LAG por MMSI,dia) sobre silver.ais_clean. El oro NO limpia: hereda las garantias de plata (ADR-012).';
COMMENT ON COLUMN oceanwatch.gold.trayectorias_diarias.MMSI               IS 'Identificador del buque (clave).';
COMMENT ON COLUMN oceanwatch.gold.trayectorias_diarias.dia                IS 'Dia de la trayectoria = to_date(BaseDateTime), UTC. Clave (ADR-004).';
COMMENT ON COLUMN oceanwatch.gold.trayectorias_diarias.n_posiciones       IS 'Numero de posiciones AIS del buque ese dia.';
COMMENT ON COLUMN oceanwatch.gold.trayectorias_diarias.distancia_total_km IS 'Suma haversine de distancias entre posiciones consecutivas (km). Puede ser no fisica si el AIS salta (plata garantiza rango, no continuidad).';
COMMENT ON COLUMN oceanwatch.gold.trayectorias_diarias.primera_posicion   IS 'Primer BaseDateTime del dia.';
COMMENT ON COLUMN oceanwatch.gold.trayectorias_diarias.ultima_posicion    IS 'Ultimo BaseDateTime del dia.';
COMMENT ON COLUMN oceanwatch.gold.trayectorias_diarias.duracion_horas     IS 'Duracion (ultima - primera) en horas, DECIMAL(27,6).';
COMMENT ON COLUMN oceanwatch.gold.trayectorias_diarias.lat_min            IS 'Bounding box: latitud minima del dia.';
COMMENT ON COLUMN oceanwatch.gold.trayectorias_diarias.lat_max            IS 'Bounding box: latitud maxima del dia.';
COMMENT ON COLUMN oceanwatch.gold.trayectorias_diarias.lon_min            IS 'Bounding box: longitud minima del dia.';
COMMENT ON COLUMN oceanwatch.gold.trayectorias_diarias.lon_max            IS 'Bounding box: longitud maxima del dia.';

-- -----------------------------------------------------------------------------
-- gold.presencia_puertos (hecho: cruce AIS x WPI por celda)
-- -----------------------------------------------------------------------------
COMMENT ON TABLE oceanwatch.gold.presencia_puertos IS 'Hecho: presencia de buques en puertos (1 fila por MMSI, INDEX_NO y dia). Cruce silver.ais_clean x gold.world_port_index por celda de grilla a 2 decimales (Reference Data Join, ADR-001). "En puerto" = al menos 3 posiciones en la celda-puerto ese dia (ADR-012).';
COMMENT ON COLUMN oceanwatch.gold.presencia_puertos.MMSI                   IS 'Identificador del buque (clave).';
COMMENT ON COLUMN oceanwatch.gold.presencia_puertos.INDEX_NO               IS 'Puerto del WPI (clave, FK a gold.world_port_index.INDEX_NO).';
COMMENT ON COLUMN oceanwatch.gold.presencia_puertos.PORT_NAME              IS 'Nombre del puerto (WPI).';
COMMENT ON COLUMN oceanwatch.gold.presencia_puertos.COUNTRY                IS 'Pais del puerto (WPI).';
COMMENT ON COLUMN oceanwatch.gold.presencia_puertos.dia                    IS 'Dia, UTC. Clave (ADR-004).';
COMMENT ON COLUMN oceanwatch.gold.presencia_puertos.n_posiciones_en_puerto IS 'Numero de posiciones del buque en la celda-puerto ese dia (al menos 3, permanencia minima).';
COMMENT ON COLUMN oceanwatch.gold.presencia_puertos.primera_posicion       IS 'Primera posicion en puerto ese dia.';
COMMENT ON COLUMN oceanwatch.gold.presencia_puertos.ultima_posicion        IS 'Ultima posicion en puerto ese dia.';
COMMENT ON COLUMN oceanwatch.gold.presencia_puertos.HARBORSIZE             IS 'Tamano del puerto (WPI).';
COMMENT ON COLUMN oceanwatch.gold.presencia_puertos.HARBORTYPE             IS 'Tipo de puerto (WPI).';

-- -----------------------------------------------------------------------------
-- gold.matriz_trafico (hecho: densidad por celda y hora)
-- -----------------------------------------------------------------------------
COMMENT ON TABLE oceanwatch.gold.matriz_trafico IS 'Hecho: densidad de trafico por celda espacial (redondeo lat/lon a 2 decimales, ADR-001) y HORA. 1 fila por (celda_lat, celda_lon, hora) sobre silver.ais_clean. Fuente de la vista diaria pre-agregada gold.matriz_trafico_diaria (Fase 7).';
COMMENT ON COLUMN oceanwatch.gold.matriz_trafico.celda_lat    IS 'round(LAT, 2), clave de celda (ADR-001).';
COMMENT ON COLUMN oceanwatch.gold.matriz_trafico.celda_lon    IS 'round(LON, 2), clave de celda (ADR-001).';
COMMENT ON COLUMN oceanwatch.gold.matriz_trafico.hora         IS 'date_trunc hour de BaseDateTime, UTC, clave temporal (ADR-004).';
COMMENT ON COLUMN oceanwatch.gold.matriz_trafico.n_posiciones IS 'Numero de posiciones AIS en la celda y hora.';
COMMENT ON COLUMN oceanwatch.gold.matriz_trafico.n_buques     IS 'Numero de buques distintos (MMSI) en la celda y hora.';

-- -----------------------------------------------------------------------------
-- gold.matriz_trafico_diaria (ya tenia COMMENT de TABLA en Fase 7: NO se re-comenta
-- la tabla; se completan solo los comentarios de COLUMNA que faltaban)
-- -----------------------------------------------------------------------------
COMMENT ON COLUMN oceanwatch.gold.matriz_trafico_diaria.celda_lat  IS 'round(LAT, 2), clave de celda (ADR-001).';
COMMENT ON COLUMN oceanwatch.gold.matriz_trafico_diaria.celda_lon  IS 'round(LON, 2), clave de celda (ADR-001).';
COMMENT ON COLUMN oceanwatch.gold.matriz_trafico_diaria.dia        IS 'date(hora), UTC, clave temporal. Parametro :fecha del dashboard (ADR-017).';
COMMENT ON COLUMN oceanwatch.gold.matriz_trafico_diaria.intensidad IS 'SUM(n_posiciones): posiciones AIS en la celda ese dia (metrica principal del mapa).';
COMMENT ON COLUMN oceanwatch.gold.matriz_trafico_diaria.buques     IS 'SUM(n_buques): metrica secundaria. Suma conteos de buques distintos POR HORA, por lo que un buque en varias horas se cuenta mas de una vez (NO es buques unicos por dia).';
