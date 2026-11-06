# ADR-018: Gobernanza de Unity Catalog — permisos de mínimo privilegio, linaje y documentación en el securable

- **Estado:** Aceptado
- **Fecha:** 2026-11-05
- **Requisito(s) de la rúbrica:** Req. 5 (10%)
- **Autores:** Alejandro Abril (202224328), David Fernando Caro (202222073), Juan David Torres Albarracín (202317608)

## 1. Contexto y problema
La Fase 5 (Req. 5) exige **gobernanza real** del lakehouse OceanWatch sobre Unity Catalog
(UC), no un diseño en papel. El inventario verificado del entorno confirmó que, antes de esta
fase, **no existía ningún grupo de rol del proyecto** y que `SHOW GRANTS ON CATALOG oceanwatch`
y `SHOW GRANTS ON SCHEMA` de los 6 esquemas devolvían **0 filas**: el acceso dependía
únicamente de que `jd.torresa1@uniandes.edu.co` es admin del metastore `metastore_aws_us_east_2`
(us-east-2). Además, 4 de las 6 tablas de oro no tenían comentarios en el securable. Hay que
decidir **cómo** conceder acceso (a quién y con qué privilegios), **cómo** demostrar el linaje
del pipeline y **dónde** documentar el contrato, con las restricciones de un workspace
**serverless** y de un **metastore único** compartido, de forma **idempotente** y
**reversible** y sin alterar el acceso del administrador ni de `account users`.

## 2. Criterios de decisión
- **Mínimo privilegio:** cada rol recibe sólo los privilegios indispensables para su función.
- **Idempotencia:** los `GRANT` y los `COMMENT` deben poder reaplicarse sin efecto secundario.
- **Reversibilidad:** todo cambio debe poder deshacerse (`REVOKE`, `DROP GROUP`, `COMMENT ... IS NULL`).
- **Auditoría / verificabilidad:** el estado debe poder leerse en vivo (`SHOW GRANTS`, API de
  linaje, `information_schema`), sin inspeccionar código.
- **No regresión:** no tocar `admins` ni `account users`; sólo **añadir** grants.
- **Mantenibilidad del acceso:** el acceso debe escalar sin reconfigurar permisos por persona.

## 3. Alternativas consideradas
| Alternativa | Descripción | Pros | Contras | Evidencia |
|---|---|---|---|---|
| A. GRANT a **grupos** (elegida) | Conceder a 4 grupos de rol (`oceanwatch_*`), nunca a usuarios individuales | Escala sin reconfigurar por persona; alta/baja de personas = membresía del grupo; auditable por rol | Requiere crear grupos de **cuenta** (los *workspace-local* fallan en UC con `PRINCIPAL_DOES_NOT_EXIST`) | `evidence/req05-gobernanza/permisos_rbac.md` (SHOW GRANTS real); [Manage privileges in UC](https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/) (acceso 2026-11-05) |
| B. GRANT a **usuarios** individuales | Conceder los privilegios directamente a cada cuenta | Setup inmediato para 1 persona | No escala; frágil ante altas/bajas; auditoría por persona en vez de por rol; anti-patrón de RBAC | [Manage privileges in UC](https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/) (acceso 2026-11-05) |
| C. `ALL PRIVILEGES` por **catálogo** | Un único grant amplio sobre `oceanwatch` a cada grupo | Simplísimo de aplicar | Viola mínimo privilegio: el rol de dashboard vería `bronze`/`quarantine`; sin aislamiento por capa | ADR-002 (un esquema por capa habilita GRANT por capa) |
| D. Mínimo privilegio **por esquema** (elegida) | `USE CATALOG` + `USE SCHEMA`/`SELECT`/`MODIFY` capa por capa | Aísla capas (p. ej. dashboard sólo `gold`); auditable por securable | Más sentencias que administrar | `evidence/req05-gobernanza/permisos_rbac.md`; [UC privileges](https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/privileges.html) (acceso 2026-11-05) |

## 4. Decisión
La gobernanza del catálogo `oceanwatch` se implementa concediendo privilegios **a cuatro
grupos de cuenta de rol** (`oceanwatch_ingenieria_datos`, `oceanwatch_analistas`,
`oceanwatch_ciencia_datos`, `oceanwatch_consumidores_dashboard`) con **mínimo privilegio por
esquema** (`USE CATALOG` en el catálogo, más `USE SCHEMA`/`SELECT`/`MODIFY` sólo en las capas
que cada rol necesita), se **verifica el linaje** del pipeline con la API de linaje de UC a
nivel de tabla y de columna, y se **documenta cada tabla y columna clave en el securable**
mediante `COMMENT ON TABLE/COLUMN`. El script de permisos **sólo añade** grants: no ejecuta
`REVOKE` sobre grupos reales ni toca `admins` ni `account users`.

## 5. Justificación basada en evidencia
La documentación de [Unity Catalog](https://docs.databricks.com/aws/en/data-governance/unity-catalog/)
(acceso 2026-11-05) modela el acceso por *securable* sobre el espacio de nombres de tres
niveles `catálogo.esquema.objeto`; conceder por **grupo** y por **esquema** materializa el
mínimo privilegio que ADR-002 anticipó ("un esquema por capa"). La elección de **grupos de
cuenta** (no *workspace-local*) no es teórica: en la ejecución, los grupos creados con
`databricks groups create` fallaron en `GRANT` con `PRINCIPAL_DOES_NOT_EXIST`, y se recrearon
vía el SCIM de cuenta (`POST /api/2.0/account/scim/v2/Groups`). La matriz quedó **verificada**
por `SHOW GRANTS` real (catálogo + 6 esquemas) con **0 grants** sobre `account users`/`admins`
(`evidence/req05-gobernanza/permisos_rbac.md`): `fase5_permisos.sql` ejecutó **18 sentencias,
0 FAIL**. El **linaje** se confirmó con `GET /api/2.0/lineage-tracking/table-lineage` y
`.../column-lineage` (el `POST` responde `404` en este workspace): `silver.ais_clean` tiene
como *upstreams* `bronze.ais_raw` y `silver.celda_en_agua`, y como *downstreams* las 4 tablas
de oro; `gold.trayectorias_diarias.MMSI` proviene de `silver.ais_clean.MMSI`
(`evidence/req05-gobernanza/linaje.md`). La **documentación en el securable** se aplicó con
`fase5_comentarios_oro.sql` (**43 sentencias, 0 FAIL**) y se verificó por
`DESCRIBE TABLE EXTENDED` e `information_schema`, donde además se confirmó que
`gold.trayectorias_diarias.duracion_horas` es `DECIMAL(27,6)` (corrección del contrato).

## 6. Patrón de diseño / ingeniería de datos aplicado
**RBAC / Least Privilege** sobre securables de Unity Catalog (grants a grupos, por esquema),
combinado con **Data Lineage** (linaje de tabla y de columna de UC para análisis de impacto y
trazabilidad) y **Documentation-in-the-securable** (metadata-as-governance: `COMMENT` que vive
en el metastore y es consultable por `information_schema`). Se apoya en el
**Namespacing** de tres niveles fijado por ADR-002.

## 7. Consecuencias
- **Positivas:** acceso por rol auditable y escalable (alta/baja de personas = membresía del
  grupo); el rol de dashboard sólo ve `gold`; el linaje habilita análisis de impacto sin leer
  código; el contrato de oro queda documentado dentro de UC, no sólo en `docs/`.
- **Negativas / deuda:** hay más sentencias de grant que administrar; los grupos se crean
  **vacíos** (añadir miembros reales es paso operativo posterior); quedan pendientes los *tags*
  de clasificación de datos y la política de evolución de esquema del oro; el **screenshot** de
  Catalog Explorer no es automatizable y queda **PENDIENTE DE CAPTURA**.

## 8. Cómo se verifica / reversibilidad
- **Permisos:** `SHOW GRANTS ON CATALOG oceanwatch` y `SHOW GRANTS ON SCHEMA oceanwatch.<capa>`
  muestran la matriz por grupo; **0 grants** sobre `admins`/`account users`.
- **Linaje:** `GET /api/2.0/lineage-tracking/table-lineage?table_name=oceanwatch.silver.ais_clean`
  y `.../column-lineage?...&column_name=MMSI` devuelven *upstreams/downstreams* no vacíos.
- **Comentarios:** `DESCRIBE TABLE EXTENDED oceanwatch.gold.<tabla>` y
  `SELECT comment FROM oceanwatch.information_schema.tables/columns WHERE table_schema='gold'`.
- **Reversibilidad:** todo es reversible — `REVOKE <priv> ON <securable> FROM <grupo>`,
  eliminar un grupo de cuenta retira sus privilegios de golpe, y `COMMENT ... IS NULL` borra un
  comentario; ninguna operación toca datos ni esquema, y el admin del metastore conserva su
  acceso en todo momento.

## 9. Referencias
- Unity Catalog — Databricks: https://docs.databricks.com/aws/en/data-governance/unity-catalog/ (acceso 2026-11-05).
- Manage privileges in Unity Catalog: https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/ (acceso 2026-11-05).
- Unity Catalog privileges and securable objects: https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/privileges.html (acceso 2026-11-05).
- Capture and view data lineage with Unity Catalog: https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-lineage.html (acceso 2026-11-05).
- Evidencia interna: `evidence/req05-gobernanza/permisos_rbac.md`, `linaje.md`, `comentarios_oro.md`.
- ADRs relacionados: ADR-002 (convenciones de nombres y un esquema por capa), ADR-001 (Medallion), ADR-012 (modelo dimensional de oro).
