# Evidencia — Bootstrap de Unity Catalog ejecutado

- **Qué demuestra:** el catálogo `oceanwatch`, los esquemas por capa y los volúmenes de la
  capa bronce se crearon realmente en el workspace institucional (no es diseño).
- **Cómo se generó:** ejecución de `resources/sql/bootstrap_oceanwatch.sql` vía la SQL
  Statement Execution API de Databricks sobre un SQL warehouse serverless, seguida de
  `SHOW CATALOGS/SCHEMAS/VOLUMES`.
- **Fecha:** 2026-11-05
- **Origen:** `resources/sql/bootstrap_oceanwatch.sql`
- **Estado:** Verificado (ejecutado)

## Resultado del bootstrap (9 sentencias idempotentes)

```
OK  | CREATE CATALOG IF NOT EXISTS oceanwatch
OK  | CREATE SCHEMA IF NOT EXISTS oceanwatch.bronze
OK  | CREATE SCHEMA IF NOT EXISTS oceanwatch.silver
OK  | CREATE SCHEMA IF NOT EXISTS oceanwatch.gold
OK  | CREATE SCHEMA IF NOT EXISTS oceanwatch.quarantine
OK  | CREATE SCHEMA IF NOT EXISTS oceanwatch.quality
OK  | CREATE SCHEMA IF NOT EXISTS oceanwatch.ml
OK  | CREATE VOLUME IF NOT EXISTS oceanwatch.bronze.landing
OK  | CREATE VOLUME IF NOT EXISTS oceanwatch.bronze.checkpoints
```

## Verificación en Unity Catalog

`SHOW CATALOGS LIKE 'oceanwatch'`:
```
oceanwatch
```

`SHOW SCHEMAS IN oceanwatch`:
```
bronze
default
gold
information_schema
ml
quality
quarantine
silver
```
(Los esquemas `default` e `information_schema` son del sistema; los 6 del proyecto están
presentes. El esquema `entrega1` no aparece porque la Entrega 1 fue solo un notebook de EDA y
no dejó tablas persistentes en este workspace.)

`SHOW VOLUMES IN oceanwatch.bronze`:
```
bronze | checkpoints
bronze | landing
```

## Nota sobre el SQL warehouse

El workspace ya tenía un warehouse serverless ("Serverless Starter Warehouse",
id `377600528fed7c47`). Se reutilizó en vez de crear uno nuevo, para no multiplicar recursos.
Este mismo warehouse alimentará el dashboard (Req. 4).
