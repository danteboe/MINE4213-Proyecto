# Evidencia — Permisos por rol (RBAC de mínimo privilegio) ejecutados

- **Qué demuestra:** que los 4 grupos de rol de OceanWatch existen realmente como
  **grupos de cuenta** (account groups, los únicos que Unity Catalog puede usar en `GRANT`)
  y que la matriz de mínimo privilegio por capa fue **concedida y verificada** en el
  metastore (no es diseño en papel).
- **Cómo se generó:** alta de grupos vía el endpoint SCIM de cuenta accesible desde el
  workspace (`POST /api/2.0/account/scim/v2/Groups`); ejecución de
  `resources/sql/fase5_permisos.sql` vía la SQL Statement Execution API sobre el SQL
  warehouse serverless `377600528fed7c47`; verificación con `SHOW GRANTS ON CATALOG` y
  `SHOW GRANTS ON SCHEMA` para las 6 capas.
- **Fecha:** 2026-11-05
- **Origen:** `resources/sql/fase5_permisos.sql`
- **Estado:** Verificado (ejecutado)

## 1. Modelo de roles y grupos creados

Se usan **grupos de cuenta** porque un grupo *workspace-local* (el que crea
`databricks groups create`) **no puede** recibir privilegios sobre securables de Unity
Catalog: el `GRANT` falla con `PRINCIPAL_DOES_NOT_EXIST`. Por eso los grupos se dan de
alta por el SCIM de cuenta (`/api/2.0/account/scim/v2/Groups`). Los grupos se crean
**vacíos** (sin usuarios reales como miembros), según el alcance de la fase.

| Grupo (account group)                 | Id de cuenta        | Rol en el lakehouse                                   |
|---------------------------------------|---------------------|-------------------------------------------------------|
| `oceanwatch_ingenieria_datos`         | `80978239208281`    | Construye y mantiene el pipeline (todas las capas).   |
| `oceanwatch_analistas`                | `87920639605010`    | Consulta datos curados y de calidad (sólo lectura).   |
| `oceanwatch_ciencia_datos`            | `83869049126007`    | Lee curado/calidad; experimenta y escribe en `ml`.    |
| `oceanwatch_consumidores_dashboard`   | `82369926225593`    | Lee únicamente la capa de servicio `gold`.            |

Listado real del SCIM de cuenta tras el alta (idempotente):

```
80978239208281 | oceanwatch_ingenieria_datos
82369926225593 | oceanwatch_consumidores_dashboard
83869049126007 | oceanwatch_ciencia_datos
87313330332486 | account users
87920639605010 | oceanwatch_analistas
```

## 2. Matriz de mínimo privilegio (rol × esquema × privilegio)

Convención de privilegios de Unity Catalog: `USE CATALOG` para resolver nombres,
`USE SCHEMA` para ver el esquema, `SELECT` para leer datos y `MODIFY` para escribir
**datos** (no DDL). Cada rol recibe sólo lo indispensable.

| Esquema \ Rol    | ingenieria_datos            | analistas        | ciencia_datos               | consumidores_dashboard |
|------------------|-----------------------------|------------------|-----------------------------|------------------------|
| **(catálogo)**   | USE CATALOG                 | USE CATALOG      | USE CATALOG                 | USE CATALOG            |
| `bronze`         | USE SCHEMA + SELECT + MODIFY| —                | —                           | —                      |
| `silver`         | USE SCHEMA + SELECT + MODIFY| USE SCHEMA+SELECT| USE SCHEMA + SELECT         | —                      |
| `gold`           | USE SCHEMA + SELECT + MODIFY| USE SCHEMA+SELECT| USE SCHEMA + SELECT         | USE SCHEMA + SELECT    |
| `quarantine`     | USE SCHEMA + SELECT + MODIFY| —                | —                           | —                      |
| `quality`        | USE SCHEMA + SELECT + MODIFY| USE SCHEMA+SELECT| USE SCHEMA + SELECT         | —                      |
| `ml`             | USE SCHEMA + SELECT + MODIFY| —                | USE SCHEMA + SELECT + MODIFY| —                      |

El script **sólo AÑADE** grants (patrón *Least Privilege* / RBAC, apoyado en ADR-002:
un catálogo, un esquema por capa). No ejecuta ningún `REVOKE` sobre grupos reales y
**no toca** a `admins` ni a `account users`.

## 3. SQL aplicado

Archivo `resources/sql/fase5_permisos.sql` (idempotente; `GRANT` es reaplicable sin
efecto secundario). Resultado real de la ejecución vía
`python ..\_dbx_sql.py 377600528fed7c47 resources\sql\fase5_permisos.sql`
(18 sentencias, **0 FAIL**):

```
OK    | GRANT USE CATALOG ON CATALOG oceanwatch TO `oceanwatch_ingenieria_datos`
OK    | GRANT USE CATALOG ON CATALOG oceanwatch TO `oceanwatch_analistas`
OK    | GRANT USE CATALOG ON CATALOG oceanwatch TO `oceanwatch_ciencia_datos`
OK    | GRANT USE CATALOG ON CATALOG oceanwatch TO `oceanwatch_consumidores_dashboard`
OK    | GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA oceanwatch.bronze     TO `oceanwatch_ingenieria_datos`
OK    | GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA oceanwatch.silver     TO `oceanwatch_ingenieria_datos`
OK    | GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA oceanwatch.gold       TO `oceanwatch_ingenieria_datos`
OK    | GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA oceanwatch.quarantine TO `oceanwatch_ingenieria_datos`
OK    | GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA oceanwatch.quality    TO `oceanwatch_ingenieria_datos`
OK    | GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA oceanwatch.ml         TO `oceanwatch_ingenieria_datos`
OK    | GRANT USE SCHEMA, SELECT ON SCHEMA oceanwatch.silver  TO `oceanwatch_analistas`
OK    | GRANT USE SCHEMA, SELECT ON SCHEMA oceanwatch.gold    TO `oceanwatch_analistas`
OK    | GRANT USE SCHEMA, SELECT ON SCHEMA oceanwatch.quality TO `oceanwatch_analistas`
OK    | GRANT USE SCHEMA, SELECT ON SCHEMA oceanwatch.silver          TO `oceanwatch_ciencia_datos`
OK    | GRANT USE SCHEMA, SELECT ON SCHEMA oceanwatch.gold            TO `oceanwatch_ciencia_datos`
OK    | GRANT USE SCHEMA, SELECT ON SCHEMA oceanwatch.quality         TO `oceanwatch_ciencia_datos`
OK    | GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA oceanwatch.ml      TO `oceanwatch_ciencia_datos`
OK    | GRANT USE SCHEMA, SELECT ON SCHEMA oceanwatch.gold TO `oceanwatch_consumidores_dashboard`
```

## 4. Verificación — SHOW GRANTS (salida real)

`SHOW GRANTS ON CATALOG oceanwatch`:

```
oceanwatch_ingenieria_datos       | USE CATALOG | CATALOG | oceanwatch
oceanwatch_consumidores_dashboard | USE CATALOG | CATALOG | oceanwatch
oceanwatch_ciencia_datos          | USE CATALOG | CATALOG | oceanwatch
oceanwatch_analistas              | USE CATALOG | CATALOG | oceanwatch
```

`SHOW GRANTS ON SCHEMA oceanwatch.bronze`:

```
oceanwatch_ingenieria_datos | MODIFY     | SCHEMA | oceanwatch.bronze
oceanwatch_ingenieria_datos | SELECT     | SCHEMA | oceanwatch.bronze
oceanwatch_ingenieria_datos | USE SCHEMA | SCHEMA | oceanwatch.bronze
```

`SHOW GRANTS ON SCHEMA oceanwatch.silver`:

```
oceanwatch_ingenieria_datos | MODIFY     | SCHEMA | oceanwatch.silver
oceanwatch_ingenieria_datos | SELECT     | SCHEMA | oceanwatch.silver
oceanwatch_ingenieria_datos | USE SCHEMA | SCHEMA | oceanwatch.silver
oceanwatch_ciencia_datos    | SELECT     | SCHEMA | oceanwatch.silver
oceanwatch_ciencia_datos    | USE SCHEMA | SCHEMA | oceanwatch.silver
oceanwatch_analistas        | SELECT     | SCHEMA | oceanwatch.silver
oceanwatch_analistas        | USE SCHEMA | SCHEMA | oceanwatch.silver
```

`SHOW GRANTS ON SCHEMA oceanwatch.gold`:

```
oceanwatch_ingenieria_datos       | MODIFY     | SCHEMA | oceanwatch.gold
oceanwatch_ingenieria_datos       | SELECT     | SCHEMA | oceanwatch.gold
oceanwatch_ingenieria_datos       | USE SCHEMA | SCHEMA | oceanwatch.gold
oceanwatch_consumidores_dashboard | SELECT     | SCHEMA | oceanwatch.gold
oceanwatch_consumidores_dashboard | USE SCHEMA | SCHEMA | oceanwatch.gold
oceanwatch_ciencia_datos          | SELECT     | SCHEMA | oceanwatch.gold
oceanwatch_ciencia_datos          | USE SCHEMA | SCHEMA | oceanwatch.gold
oceanwatch_analistas              | SELECT     | SCHEMA | oceanwatch.gold
oceanwatch_analistas              | USE SCHEMA | SCHEMA | oceanwatch.gold
```

`SHOW GRANTS ON SCHEMA oceanwatch.quarantine`:

```
oceanwatch_ingenieria_datos | MODIFY     | SCHEMA | oceanwatch.quarantine
oceanwatch_ingenieria_datos | SELECT     | SCHEMA | oceanwatch.quarantine
oceanwatch_ingenieria_datos | USE SCHEMA | SCHEMA | oceanwatch.quarantine
```

`SHOW GRANTS ON SCHEMA oceanwatch.quality`:

```
oceanwatch_ingenieria_datos | MODIFY     | SCHEMA | oceanwatch.quality
oceanwatch_ingenieria_datos | SELECT     | SCHEMA | oceanwatch.quality
oceanwatch_ingenieria_datos | USE SCHEMA | SCHEMA | oceanwatch.quality
oceanwatch_ciencia_datos    | SELECT     | SCHEMA | oceanwatch.quality
oceanwatch_ciencia_datos    | USE SCHEMA | SCHEMA | oceanwatch.quality
oceanwatch_analistas        | SELECT     | SCHEMA | oceanwatch.quality
oceanwatch_analistas        | USE SCHEMA | SCHEMA | oceanwatch.quality
```

`SHOW GRANTS ON SCHEMA oceanwatch.ml`:

```
oceanwatch_ingenieria_datos | MODIFY     | SCHEMA | oceanwatch.ml
oceanwatch_ingenieria_datos | SELECT     | SCHEMA | oceanwatch.ml
oceanwatch_ingenieria_datos | USE SCHEMA | SCHEMA | oceanwatch.ml
oceanwatch_ciencia_datos    | MODIFY     | SCHEMA | oceanwatch.ml
oceanwatch_ciencia_datos    | SELECT     | SCHEMA | oceanwatch.ml
oceanwatch_ciencia_datos    | USE SCHEMA | SCHEMA | oceanwatch.ml
```

**Chequeo de no-regresión sobre principals de sistema:** la verificación recorrió todas
las filas de `SHOW GRANTS` (catálogo + 6 esquemas) y confirmó **0 grants** sobre
`account users`, `admins` o `users`. Sólo aparecen los 4 grupos `oceanwatch_*`.

## 5. Reversibilidad

Todos los cambios son **reversibles**: el usuario `jd.torresa1@uniandes.edu.co` es admin
del metastore, por lo que conceder grants a grupos nuevos no altera su acceso. Un
`REVOKE <privilegio> ON <securable> FROM <grupo>` deshace cualquier grant; eliminar un
grupo de cuenta retira todos sus privilegios de golpe. El archivo SQL incluye, **comentado**,
el patrón `GRANT/REVOKE` auto-contenido sobre un grupo de prueba desechable para ilustrar
la reversibilidad sin tocar grupos reales.
