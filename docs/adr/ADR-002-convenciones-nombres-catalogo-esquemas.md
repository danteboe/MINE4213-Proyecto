# ADR-002: Convenciones de nombres de catálogo, esquemas, tablas y volúmenes

- **Estado:** Aceptado
- **Fecha:** 2026-11-05
- **Requisito(s) de la rúbrica:** Req. 5 (10%), Req. 7 (5%)
- **Autores:** Alejandro Abril (202224328), David Fernando Caro (202222073), Juan David Torres Albarracín (202317608)

## 1. Contexto y problema
La gobernanza (Req. 5) y la reproducibilidad (Req. 7) exigen nombres consistentes y estables
en Unity Catalog. El inventario verificado del entorno confirma que **no existe** el catálogo
del proyecto (solo `workspace`, `system`, `samples`) y que Entrega 1 usó el esquema
`entrega1` dentro de un catálogo `oceanwatch`. Hay que definir una convención única que
coexista con Entrega 1, habilite el mínimo privilegio futuro (GRANTs por esquema) y se pueda
parametrizar sin secretos.

## 2. Criterios de decisión
- Legibilidad y estabilidad de los identificadores.
- Trazabilidad por capa (saber a qué capa pertenece cada objeto).
- Coexistencia sin modificar los artefactos de Entrega 1.
- Habilitar mínimo privilegio futuro (GRANTs por esquema, Req. 5).
- Parametrización centralizada sin credenciales ni rutas personales.

## 3. Alternativas consideradas
| Alternativa | Descripción | Pros | Contras | Evidencia |
|---|---|---|---|---|
| A. Un catálogo, un esquema por capa | `oceanwatch` con `bronze`/`silver`/`gold`/`quarantine`/`quality`/`ml` y `entrega1` preservado | GRANTs por esquema, linaje claro, coexiste con Entrega 1 | Más esquemas que administrar | [Unity Catalog — Databricks](https://docs.databricks.com/aws/en/data-governance/unity-catalog/) (acceso 2026-11-05) |
| B. Un esquema único con prefijos de tabla | `oceanwatch.lakehouse` con tablas `bronze_*`, `silver_*`, … | Pocos esquemas | GRANT por capa imposible (todo en un esquema); nombres largos | [Unity Catalog — Databricks](https://docs.databricks.com/aws/en/data-governance/unity-catalog/) (acceso 2026-11-05) |
| C. Un catálogo por capa | `oceanwatch_bronze`, `oceanwatch_silver`, … | Aislamiento fuerte | Fragmenta linaje, multiplica permisos de catálogo; choca con metastore único | Metastore verificado `metastore_aws_us_east_2` (us-east-2) |

## 4. Decisión
Se usa un **catálogo único `oceanwatch`** con un esquema por capa
(`bronze`, `silver`, `gold`, `quarantine`, `quality`, `ml`), se **preserva** `oceanwatch.entrega1`
sin modificarlo, las tablas y columnas se nombran en `snake_case`, los volúmenes son `landing`
y `checkpoints` bajo `oceanwatch.bronze`, y el bootstrap se mantiene en dos formas coherentes:
Python parametrizado (`src/oceanwatch/ingest/bootstrap.py`) y SQL declarativo
(`resources/sql/bootstrap_oceanwatch.sql`).

## 5. Justificación basada en evidencia
La documentación de [Unity Catalog](https://docs.databricks.com/aws/en/data-governance/unity-catalog/)
(acceso 2026-11-05) organiza los objetos en el espacio de nombres de tres niveles
`catálogo.esquema.objeto` y aplica permisos por securable; un esquema por capa permite
conceder privilegios mínimos por capa (p. ej. lectura de `gold` al rol de dashboard sin
exponer `bronze`), lo que sustenta Req. 5. El metastore verificado es
`metastore_aws_us_east_2` en `us-east-2`, único, de modo que fragmentar en varios catálogos
(alternativa C) no aporta aislamiento real y sí complejidad. La dualidad Python/SQL responde
a dos usos: Python para lógica parametrizada por `conf/config.yaml` (reutilizable por las
fases de ingesta) y SQL para ejecución directa y auditable en un editor de Databricks.

## 6. Patrón de diseño / ingeniería de datos aplicado
**Naming Convention / Namespacing** (espacio de nombres de tres niveles de Unity Catalog)
combinado con **Infrastructure-as-Code idempotente** (sentencias `IF NOT EXISTS`).

## 7. Consecuencias
- Positivas: gobernanza clara, GRANTs por esquema, linaje legible, coexistencia con Entrega 1.
- Negativas: requiere disciplina de nombres y administrar más esquemas.
- Deuda asumida: definir el detalle de GRANTs de mínimo privilegio (PENDIENTE - Fase 5).

## 8. Cómo se verifica / reversibilidad
`SHOW SCHEMAS IN oceanwatch` tras ejecutar el bootstrap (Fase 2) debe listar los seis
esquemas nuevos más `entrega1`. Reversible con `DROP SCHEMA ... CASCADE` sobre los esquemas
nuevos (nunca sobre `entrega1`). Estado actual: **Diseñado (no ejecutado)**.

## 9. Referencias
- Unity Catalog — Databricks: https://docs.databricks.com/aws/en/data-governance/unity-catalog/ (acceso 2026-11-05).
- Inventario verificado del entorno (metastore `metastore_aws_us_east_2`, us-east-2).
- ADR-001 (arquitectura Medallion).
