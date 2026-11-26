# Gobernanza institucional y alcance verificado

Verificación del 26 de noviembre de 2026 en el workspace del curso. Las consultas se ejecutaron con la identidad institucional del proyecto. Los permisos corresponden a `mine-group9` y a los recursos publicados.

| Recurso | Acceso observado para el grupo |
|---|---|
| Catálogos `clase14` y `clase10` | `USE CATALOG` |
| Esquema `clase14.equipo9` | `USE SCHEMA`, `CREATE TABLE`, `CREATE MODEL`, `SELECT` y `MODIFY` |
| Volumen `clase10.equipo9.checkpoints` | `READ VOLUME` y `WRITE VOLUME` |
| Detector, pipeline y verificación | `CAN_MANAGE_RUN` en cada Job |
| Consola AI/BI | `CAN_READ`. La revisión publicada usa las credenciales de quien consulta y requiere acceso a oro y al warehouse. |

El inventario de Unity Catalog devolvió **11 tablas administradas con comentario** en `clase14.equipo9`. El [directorio](../../docs/directorio_recursos.md#tablas-y-modelo) lista sus nombres exactos y el [contrato de oro](../../docs/data_contract_gold.md) define granos, medidas y límites.

## Linaje comprobado

La consulta de linaje de Unity Catalog mostró `oceanwatch_bronze_ais_raw → oceanwatch_silver_ais_clean`. Para plata registró salidas directas hacia trayectorias, presencia en puertos, matriz horaria y anomalías de velocidad. La tabla de anomalías reportó a plata como entrada directa. La matriz diaria se deriva de la matriz horaria, como muestra el código. Los cuatro datasets de la consola apuntan a tablas de oro. [Agregados](../../src/oceanwatch/gold/diaria.py) · [Tablero versionado](../../resources/dashboards/oceanwatch_oro.lvdash.json).

Para consultarlo en Databricks, abra **Catalog → clase14 → equipo9 → oceanwatch_silver_ais_clean → Lineage**. Los enlaces dependen de los permisos de quien consulta.

## Cobertura de la rúbrica

La segregación de esquemas por rol queda **parcial**. Las capas se distinguen por prefijos en `clase14.equipo9`. Los grants de lectura y modificación en el esquema se heredan en las tablas, así que dentro de `mine-group9` no hay una frontera efectiva entre capas por rol. El grupo tiene `USE CATALOG` en los dos catálogos examinados. El despliegue original también rechazó la creación de un volumen en `clase14.equipo9` por falta de `CREATE VOLUME`. [Decisión de despliegue](../../docs/adr/ADR-019-computo-y-almacenamiento-institucional.md).

| Rol previsto | Acceso de mínimo privilegio al separar esquemas |
|---|---|
| Ingeniería y orquestación | Leer el volumen y mantener staging, checkpoint y tablas del pipeline. El Job escribe con su identidad de ejecución. |
| Calidad | Leer bronce y los resultados de plata y cuarentena. Consultar métricas y conciliación. |
| Ciencia de datos | Leer plata, operar su experimento y registrar el modelo. |
| Consumo analítico | Consultar oro, tablero y warehouse para visualización. |

Para aplicar esa matriz, la administración del curso debe aprovisionar esquemas y grupos separados y ajustar los grants heredados de acuerdo con su política. La separación solo se puede acreditar después de probar el acceso efectivo con las identidades de cada rol.

## Consultas de comprobación

```sql
SHOW GRANTS ON CATALOG clase14;
SHOW GRANTS ON SCHEMA clase14.equipo9;
SHOW GRANTS ON VOLUME clase10.equipo9.checkpoints;

SELECT table_name, table_type, comment
FROM clase14.information_schema.tables
WHERE table_schema = 'equipo9' AND startswith(table_name, 'oceanwatch_')
ORDER BY table_name;
```
