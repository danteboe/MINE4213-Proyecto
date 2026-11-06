-- =============================================================================
-- FASE 5 (Req. 5) — Permisos por rol (RBAC de mínimo privilegio) en Unity Catalog.
-- =============================================================================
-- Qué hace: concede a los 4 grupos de rol del proyecto (oceanwatch_*) los privilegios
-- mínimos necesarios para su función, capa por capa, sobre el catálogo `oceanwatch`.
-- Por qué (mínimo privilegio): cada grupo recibe sólo lo indispensable —
--   USE CATALOG para resolver nombres, USE SCHEMA para ver el esquema,
--   SELECT para leer y MODIFY para escribir DATOS (no DDL)— y nada más.
-- Patrón de ingeniería: RBAC / Least Privilege sobre Unity Catalog, apoyado en
--   ADR-002 ("un catálogo oceanwatch, un esquema por capa") que habilita el
--   aislamiento de privilegios por capa.
-- Este script SOLO AÑADE grants: es idempotente (GRANT es reaplicable sin efecto
--   secundario) y NO toca a los grupos de sistema `admins` ni `account users`, ni
--   ejecuta REVOKE alguno sobre grupos reales. No crea ni destruye tablas ni datos.
-- =============================================================================

-- --- Nivel catálogo: los 4 grupos pueden resolver nombres dentro de oceanwatch -----------
GRANT USE CATALOG ON CATALOG oceanwatch TO `oceanwatch_ingenieria_datos`;
GRANT USE CATALOG ON CATALOG oceanwatch TO `oceanwatch_analistas`;
GRANT USE CATALOG ON CATALOG oceanwatch TO `oceanwatch_ciencia_datos`;
GRANT USE CATALOG ON CATALOG oceanwatch TO `oceanwatch_consumidores_dashboard`;

-- --- Ingeniería de datos: lectura + escritura de datos en TODAS las capas -----------------
-- (USE SCHEMA + SELECT + MODIFY en bronze, silver, gold, quarantine, quality, ml)
GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA oceanwatch.bronze     TO `oceanwatch_ingenieria_datos`;
GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA oceanwatch.silver     TO `oceanwatch_ingenieria_datos`;
GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA oceanwatch.gold       TO `oceanwatch_ingenieria_datos`;
GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA oceanwatch.quarantine TO `oceanwatch_ingenieria_datos`;
GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA oceanwatch.quality    TO `oceanwatch_ingenieria_datos`;
GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA oceanwatch.ml         TO `oceanwatch_ingenieria_datos`;

-- --- Analistas: sólo lectura en silver, gold y quality ------------------------------------
-- (USE SCHEMA + SELECT; sin MODIFY)
GRANT USE SCHEMA, SELECT ON SCHEMA oceanwatch.silver  TO `oceanwatch_analistas`;
GRANT USE SCHEMA, SELECT ON SCHEMA oceanwatch.gold    TO `oceanwatch_analistas`;
GRANT USE SCHEMA, SELECT ON SCHEMA oceanwatch.quality TO `oceanwatch_analistas`;

-- --- Ciencia de datos: lectura en silver/gold/quality + lectura y escritura en ml ---------
-- (USE SCHEMA + SELECT en silver/gold/quality; USE SCHEMA + SELECT + MODIFY en ml)
GRANT USE SCHEMA, SELECT ON SCHEMA oceanwatch.silver          TO `oceanwatch_ciencia_datos`;
GRANT USE SCHEMA, SELECT ON SCHEMA oceanwatch.gold            TO `oceanwatch_ciencia_datos`;
GRANT USE SCHEMA, SELECT ON SCHEMA oceanwatch.quality         TO `oceanwatch_ciencia_datos`;
GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA oceanwatch.ml      TO `oceanwatch_ciencia_datos`;

-- --- Consumidores del dashboard: sólo lectura en gold -------------------------------------
-- (USE SCHEMA + SELECT únicamente en gold; es la capa de servicio del dashboard)
GRANT USE SCHEMA, SELECT ON SCHEMA oceanwatch.gold TO `oceanwatch_consumidores_dashboard`;

-- =============================================================================
-- Bloque demostrativo de REVOKE (COMENTADO, NO SE EJECUTA).
-- Ilustra el ciclo auto-contenido sobre un grupo de PRUEBA desechable: se crea,
-- se le concede un grant, se le revoca y se elimina en la misma corrida, sin tocar
-- jamás grupos reales, `admins` ni `account users`. Se deja comentado porque la
-- creación/borrado de grupos no es DDL de SQL warehouse (va por SCIM/CLI); queda
-- como referencia del patrón GRANT/REVOKE reversible.
--
--   -- (vía CLI, fuera de este .sql) databricks groups create --display-name oceanwatch_tmp_revoke_demo
--   GRANT USE CATALOG ON CATALOG oceanwatch TO `oceanwatch_tmp_revoke_demo`;
--   SHOW GRANTS `oceanwatch_tmp_revoke_demo` ON CATALOG oceanwatch;
--   REVOKE USE CATALOG ON CATALOG oceanwatch FROM `oceanwatch_tmp_revoke_demo`;
--   -- (vía CLI, fuera de este .sql) databricks groups delete <id-del-grupo-tmp>
-- =============================================================================
