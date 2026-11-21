# Evidencia institucional de la Entrega 2

## Recursos

| Recurso | Resultado observado |
|---|---|
| Cómputo | DBR 19.x, Spark 4.2.0, Python 3.12.3, paralelismo por defecto 4. Acceso dedicado, un nodo `i3.xlarge` y motor estándar. |
| Despliegue | `databricks bundle validate --strict` y despliegue de Jobs, permisos y tablero en el workspace del curso. |
| Job de negocio | [OceanWatch · Entrega 2](https://dbc-b38f0852-ecf9.cloud.databricks.com/jobs/83886562675136), clúster `1114-165401-a0ye3pxs`. |
| Job de verificación | [OceanWatch · Verificación](https://dbc-b38f0852-ecf9.cloud.databricks.com/jobs/719836273900213). |
| Tablero | [Consola publicada](https://dbc-b38f0852-ecf9.cloud.databricks.com/dashboardsv3/01f1e5507d1b18a5963d1a49e4329629/published). La API de publicación devuelve `embed_credentials=false` y warehouse `4c751147a4c66a5e`. |

## Ingesta inicial

[Corrida inicial](https://dbc-b38f0852-ecf9.cloud.databricks.com/jobs/83886562675136/runs/828952167457977), tarea `707387404388396`. Auto Loader reportó 34.817.937 entradas y el conteo independiente de Delta dio el mismo total. El ID persistente de ese stream es `740d3841-e1b0-4040-b730-c8fc0d536dc8`.

| Fecha UTC | Filas de bronce | Tamaño CSV (bytes) | SHA-256 del ZIP |
|---|---:|---:|---|
| 2023-06-01 | 8.808.904 | 943.431.681 | `e6287c47f9659fd3bc6c0456579e93f07e3ec099c347514757a86413d0c99d6a` |
| 2023-06-02 | 9.052.241 | 969.444.078 | `9f01ff4c3a21240c41ed0ca64a945be7fb8bee0f5bff049dc5c3e727aa02b8ac` |
| 2023-06-03 | 8.036.348 | 860.511.883 | `070ccec839de290cba810308753b56d2bac6839a9fd076df6ab089b525e046c8` |
| 2023-06-08 | 8.920.444 | 955.097.423 | `72b7a149cb50de4d2e45d98f3c881ce234dc99c3a7c1cb568dbfd81345f149b7` |

La preparación tuvo dos fallos por conversión del identificador decimal del WPI. El tercer intento leyó la proyección normalizada y terminó correctamente. La corrida se detuvo después de medir la máscara inicial para incorporar la corrección hidrográfica. La [corrida con la regla v3](https://dbc-b38f0852-ecf9.cloud.databricks.com/jobs/83886562675136/runs/75579463741923) reutilizó los cuatro CSV, conservó el mismo ID de stream y reportó **0 entradas nuevas**.

## Prueba de fallo y reanudación

[Corrida de verificación](https://dbc-b38f0852-ecf9.cloud.databricks.com/jobs/719836273900213/runs/518309915499507). La restricción `fallo_inyectado` provocó un fallo de commit en el notebook hijo. La prueba confirmó la causa, retiró la restricción en el sandbox y reanudó con el mismo checkpoint. Bronce conservó 2 filas durante el fallo y llegó a 6 al reanudar. Un tercer drenado leyó 0 archivos nuevos.

La conciliación verificó **6 bronce = 2 plata + 3 cuarentena + 1 duplicado**. Las causas fueron posición terrestre, SOG negativa y timestamp de otro día. El test comprobó que la cuarentena mantuvo la SOG original negativa.

En local, con dependencias de máscara e hidrografía, pytest devolvió **110 aprobadas y 1 omitida**. La omitida requiere PySpark. Los Jobs realizan esa integración en Databricks.

## Gobierno observado

| Ámbito | Principal | Acceso efectivo |
|---|---|---|
| Catálogo `clase14` | `mine-group9` | `USE CATALOG` |
| Esquema `clase14.equipo9` | `mine-group9` | `USE SCHEMA`, `CREATE TABLE`, `CREATE MODEL`, `SELECT`, `MODIFY` |
| Volumen `clase10.equipo9.checkpoints` | `mine-group9` | `READ VOLUME`, `WRITE VOLUME` |
| Jobs | `mine-group9` | `CAN_MANAGE_RUN` |
| Tablero | `mine-group9` | `CAN_READ` |

El esquema y el volumen pertenecen al docente. La API devolvió un rechazo al intentar crear un volumen nuevo y confirmó que DBFS público está deshabilitado. Los permisos de esquemas por rol requieren aprovisionamiento del administrador. [ADR-019](../../docs/adr/ADR-019-computo-y-almacenamiento-institucional.md).
