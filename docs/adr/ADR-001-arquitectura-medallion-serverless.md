# ADR-001: Arquitectura Medallion sobre Delta Lake en Databricks serverless

- **Estado:** Aceptado
- **Fecha:** 2026-11-05
- **Requisito(s) de la rúbrica:** Req. 2 (15%), Req. 3 (20%); transversal a todos los requisitos.
- **Autores:** Alejandro Abril (202224328), David Fernando Caro (202222073), Juan David Torres Albarracín (202317608)

## 1. Contexto y problema
Se construye desde cero el lakehouse de tráfico marítimo AIS en Databricks con Unity
Catalog, usando exclusivamente cómputo **serverless** (inventario verificado: no hay SQL
warehouses, no existe el catálogo del proyecto). La solución debe resistir la llegada de
datos nuevos del feed AIS de NOAA (archivos diarios futuros, posible evolución de esquema)
y mantener continuidad con las decisiones de almacenamiento de Entrega 1 (Delta
particionado por `date` + `ZORDER(LAT, LON)`). El problema es cómo organizar las capas de
datos para que haya separación de responsabilidades, trazabilidad de calidad y reproceso
idempotente sin depender de capacidades no disponibles en serverless.

## 2. Criterios de decisión
- Separación de responsabilidades por capa (crudo vs. saneado vs. agregado).
- Trazabilidad de calidad (dónde quedan los registros rechazados y las métricas).
- Reproceso idempotente y linaje claro de datos.
- Compatibilidad con serverless (sin clústeres dedicados ni streaming continuo).
- Continuidad con Entrega 1 (esquema explícito, Delta particionado por `date` + ZORDER).

## 3. Alternativas consideradas
| Alternativa | Descripción | Pros | Contras | Evidencia |
|---|---|---|---|---|
| A. Medallion 3 capas + auxiliares | `bronze`/`silver`/`gold` más `quarantine`, `quality`, `ml` | Separación clara, linaje, dead-letter explícito, alineada con Delta | Más esquemas/tablas que gobernar | [Medallion de Databricks](https://docs.databricks.com/aws/en/lakehouse/medallion) (acceso 2026-11-05) |
| B. Dos capas `raw` → `curated` | Solo cruda y curada, sin oro ni cuarentena separadas | Menos objetos, más simple | Pierde separación agregados/calidad; difícil versionar reglas | [Medallion de Databricks](https://docs.databricks.com/aws/en/lakehouse/medallion) (acceso 2026-11-05) |
| C. Tablas planas monolíticas | Una tabla única saneada sin capas | Mínimo número de objetos | Sin linaje, mezcla crudo y agregados, reproceso costoso; no resiste evolución | Evidencia de Entrega 1 (Delta particionado + ZORDER superó a Parquet plano) |

## 4. Decisión
Se adopta una **arquitectura Medallion de tres capas** (`bronze` → `silver` → `gold`) sobre
Delta Lake, complementada con los esquemas auxiliares `quarantine` (registros rechazados por
calidad), `quality` (métricas y reportes) y `ml` (features y artefactos del modelo); la
grilla espacial de la capa oro se mantiene por **redondeo lat/lon** (NO H3).

## 5. Justificación basada en evidencia
La guía oficial de la [arquitectura Medallion de Databricks](https://docs.databricks.com/aws/en/lakehouse/medallion)
(acceso 2026-11-05) describe el patrón multi-hop bronze/silver/gold como la organización
recomendada para lakehouse sobre Delta, lo que habilita Req. 2 (capas + calidad) y Req. 3
(orquestación). Las [limitaciones de serverless](https://docs.databricks.com/aws/en/compute/serverless/limitations)
(acceso 2026-11-05) condicionan el diseño hacia micro-batch incremental y UTC por defecto
(desarrollado en ADR-004), lo que encaja con capas Delta reprocesables. La continuidad con
Entrega 1 se sostiene en su evidencia de almacenamiento: para la consulta objetivo (día +
zona), Delta particionado por `date` + `ZORDER(LAT, LON)` leyó 1 archivo (partition pruning)
frente a 69 de Parquet plano, bajando de 2.218 MB a 1.357 MB. La grilla por redondeo lat/lon
se mantiene porque ya produjo evidencia utilizable (join con WPI y top-celdas) y es una
función pura determinista sin dependencias nativas; H3 exigiría una librería nativa y UDFs
que añaden fricción en serverless sin beneficio demostrado para el objetivo "consulta diaria
por fecha + zona". H3 queda como alternativa reevaluable en Fase 3 solo con evidencia nueva
(distorsión de área por latitud), que abriría un ADR de reemplazo.

## 6. Patrón de diseño / ingeniería de datos aplicado
**Medallion Architecture** (multi-hop: bronze → silver → gold) combinada con el patrón
**Dead-letter / Quarantine** (esquema `quarantine` para registros que no superan las reglas
de calidad).

## 7. Consecuencias
- Positivas: linaje y claridad por capa; cuarentena y métricas de calidad versionables;
  habilita Req. 2 y Req. 3; reproceso idempotente por capa.
- Negativas: más esquemas y tablas que gobernar (mitigado por ADR-002).
- Deuda asumida: definir el contrato de datos de la capa oro (PENDIENTE - Fase 3).

## 8. Cómo se verifica / reversibilidad
Se verifica al ejecutar el bootstrap (ADR-002) y materializar al menos una tabla por capa en
Fase 2+ (`SHOW SCHEMAS IN oceanwatch`). Reversible borrando los esquemas vacíos con
`DROP SCHEMA ... CASCADE` sobre los esquemas nuevos (no se hace en Fase 1). Estado actual:
**Diseñado (no ejecutado)**.

## 9. Referencias
- Arquitectura Medallion — Databricks: https://docs.databricks.com/aws/en/lakehouse/medallion (acceso 2026-11-05).
- Limitaciones de serverless — Databricks: https://docs.databricks.com/aws/en/compute/serverless/limitations (acceso 2026-11-05).
- Notebook `entrega1_oceanwatch.ipynb` (evidencia de almacenamiento Delta vs. Parquet).
- ADR-002 (convenciones de nombres), ADR-004 (trigger availableNow).
