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

### Ampliación del corpus

Se prepararon los cuatro archivos restantes en el volumen mientras la primera corrida agregaba oro. Al ejecutar el [backfill del 3 al 7 de junio](https://dbc-b38f0852-ecf9.cloud.databricks.com/jobs/83886562675136/runs/69239439205439), Auto Loader conservó el ID de stream e incorporó **34.636.066** pings nuevos. El 3 de junio ya estaba en bronce. La extensión dejó **69.454.003** posiciones para el período del 1 al 8 de junio.

| Fecha UTC | Filas nuevas de bronce | Tamaño CSV (bytes) | SHA-256 del ZIP |
|---|---:|---:|---|
| 2023-06-04 | 8.522.645 | 913.283.650 | `396efd491ea992aded336ba85bfed1cd8079baf294fb1fadf215e69d40ed42a0` |
| 2023-06-05 | 8.613.757 | 922.808.494 | `00899e6450a4cc3170059ed990c94e060257e86f22edd5c6683f282e5ce7f4c8` |
| 2023-06-06 | 8.587.938 | 918.827.336 | `c8b4acd430a675fbffe3665d9ed9843ce0696c947e4d35411a504ad2ffc595f4` |
| 2023-06-07 | 8.911.726 | 953.614.322 | `03b9b5ddb1247f08a4c3bab738c7b6c569c93094f82e8830cfea4853f9a5bed7` |

## Prueba de fallo y reanudación

[Corrida de verificación](https://dbc-b38f0852-ecf9.cloud.databricks.com/jobs/719836273900213/runs/518309915499507). La restricción `fallo_inyectado` provocó un fallo de commit en el notebook hijo. La prueba confirmó la causa, retiró la restricción en el sandbox y reanudó con el mismo checkpoint. Bronce conservó 2 filas durante el fallo y llegó a 6 al reanudar. Un tercer drenado leyó 0 archivos nuevos.

La conciliación verificó **6 bronce = 2 plata + 3 cuarentena + 1 duplicado**. Las causas fueron posición terrestre, SOG negativa y timestamp de otro día. El test comprobó que la cuarentena mantuvo la SOG original negativa.

En local, con dependencias de máscara e hidrografía, pytest devolvió **110 aprobadas y 1 omitida**. La omitida requiere PySpark. Los Jobs realizan esa integración en Databricks.

## Revisión de la compuerta de agua

| Política sobre bronce del 1 de junio | Evaluados | Sin evidencia de agua | Tasa |
|---|---:|---:|---:|
| v2 · Ráster, tolerancia y WPI | 8.808.904 | 3.493.229 | 39,66% |
| v3 · Añade NWN y Natural Earth | 8.808.904 | 272.801 | 3,10% |

El análisis de las celdas con más rechazos en v2 incluyó Houston Ship Channel y Salmon Bay. Los casos de regresión para ambas ubicaciones, lago Michigan y tierra en Kansas pasaron. La diferencia cuantifica el cambio de cobertura de la regla. Estimar la precisión requiere etiquetas navegables.

## Conciliación del corpus con la regla v3

Las tareas de conciliación de la [carga inicial](https://dbc-b38f0852-ecf9.cloud.databricks.com/jobs/83886562675136/runs/75579463741923) y del [backfill](https://dbc-b38f0852-ecf9.cloud.databricks.com/jobs/83886562675136/runs/69239439205439) verificaron que bronce = plata + cuarentena + duplicados válidos para cada fecha. El 3 de junio se reconstruyó en el backfill y reprodujo exactamente los cuatro conteos de la corrida inicial.

| Fecha UTC | Bronce | Plata | Cuarentena | Duplicados retirados | Diferencia |
|---|---:|---:|---:|---:|---:|
| 2023-06-01 | 8.808.904 | 8.523.701 | 285.023 | 180 | 0 |
| 2023-06-02 | 9.052.241 | 8.768.221 | 283.792 | 228 | 0 |
| 2023-06-03 | 8.036.348 | 7.786.382 | 249.741 | 225 | 0 |
| 2023-06-04 | 8.522.645 | 8.244.326 | 278.021 | 298 | 0 |
| 2023-06-05 | 8.613.757 | 8.329.899 | 283.600 | 258 | 0 |
| 2023-06-06 | 8.587.938 | 8.309.297 | 278.408 | 233 | 0 |
| 2023-06-07 | 8.911.726 | 8.612.811 | 298.721 | 194 | 0 |
| 2023-06-08 | 8.920.444 | 8.616.743 | 303.450 | 251 | 0 |
| **Total** | **69.454.003** | **67.191.380** | **2.260.756** | **1.867** | **0** |

## Oro del 8 de junio

| Producto | Filas | Pings representados |
|---|---:|---:|
| Trayectorias | 19.893 | 8.616.743 |
| Presencias de puerto | 3.053 | 290.300 |
| Matriz horaria | 802.602 | 8.616.743 |
| Matriz diaria | 357.165 | 8.616.743 |

Las trayectorias y las dos matrices conservaron el total de pings de plata. Una consulta independiente verificó la igualdad para las ocho fechas. Los pings de presencia se limitan a coincidencias WPI que cumplen el mínimo. La API de linaje mostró bronce como entrada de plata y trayectorias, presencia y matriz horaria como salidas directas.

## Modelo y consola

La [corrida de punta a punta](https://dbc-b38f0852-ecf9.cloud.databricks.com/jobs/83886562675136/runs/75579463741923) terminó en `SUCCESS` con las seis tareas en el clúster personal. El registro `clase14.equipo9.oceanwatch_ml_detector_anomalias_velocidad` resolvió `champion` a la versión **1** en estado `READY`. La API de linaje mostró plata como entrada directa de la tabla de anomalías.

La corrida tomó 24 min 6 s de reloj. Lakeflow registró 16 min 1 s en plata, 3 min 46 s en oro y 2 min 45 s en ML. La ingesta reutilizó el checkpoint y leyó cero posiciones nuevas en esta corrida.

| Seguimiento MLflow | Valor observado |
|---|---|
| Experimento | `2530466454792025` |
| Run | [6a2e6962426e4497885abab387ecdc18](https://dbc-b38f0852-ecf9.cloud.databricks.com/ml/experiments/2530466454792025/runs/6a2e6962426e4497885abab387ecdc18) |
| Snapshot de plata | Versión Delta 7, compuerta de agua v3 |
| Muestreo | 84.866 train del 1 y 2 de junio, 42.606 validación del 3 de junio |
| Alertas en validación | 0,94% IsolationForest y 1,16% línea base por z-score |
| Inferencia del 8 de junio | **85.587** pings anómalos publicados en oro con la versión 1 |

El backfill también terminó en `SUCCESS` y usó el mismo run y la versión 1. Oro contiene **438.441 anomalías** para las fechas posteriores al train. La consulta de control encontró una fila por clave AIS, puntuaciones negativas y versión 1 en las seis fechas.

| Fecha UTC | Anomalías publicadas |
|---|---:|
| 2023-06-03 | 65.964 |
| 2023-06-04 | 65.275 |
| 2023-06-05 | 65.661 |
| 2023-06-06 | 75.452 |
| 2023-06-07 | 80.502 |
| 2023-06-08 | 85.587 |

Las cuatro consultas parametrizadas del tablero respondieron para el 8 de junio. Sus límites devolvieron 2.000 celdas de intensidad, 15 puertos del ranking, 500 filas de buques en puerto y 500 de anomalías. La API confirmó la revisión publicada y el contexto SQL en UTC. La validación del tablero se hizo por API y ejecución SQL. El navegador integrado pidió una sesión institucional para inspeccionar el render.

## Scheduler

Databricks creó una [corrida `PERIODIC`](https://dbc-b38f0852-ecf9.cloud.databricks.com/jobs/83886562675136/runs/261133119127309) a las 21:34 de Bogotá del 20 de noviembre de 2026. La cola respetó el máximo de una corrida activa y liberó el replay al terminar el backfill. El replay terminó en **`SUCCESS`**. Al observar el evento se restauró el cron diario de las 06:00 en estado `PAUSED`.

El replay del 8 de junio reutilizó el CSV y el checkpoint, leyó 0 posiciones nuevas y reprodujo la conciliación de ese día. La comparación SQL de la tabla de anomalías con el snapshot Delta 2 usó `EXCEPT ALL` en ambas direcciones. Los dos resultados contenían **85.587 filas** de la versión 1 del modelo, con **0 faltantes y 0 adicionales**.

## Planes Spark

Con filtro del 1 de junio sobre plata, `explain("formatted")` mostró `PartitionFilters` sobre `_batch_date` en presencia, trayectorias y matriz. Trayectorias ordenó la ventana tras `hashpartitioning(MMSI, dia)`. La matriz tuvo un intercambio por `(celda_lat, celda_lon, hora, MMSI)` para unicidad y otro por `(celda_lat, celda_lon, hora)` para consolidar la medida.

En presencia, el primer plan redistribuía MMSI, código, nombre, país, tamaño y tipo de puerto, y día. Se redujo la llave a `hashpartitioning(MMSI, INDEX_NO, dia)`. El plan posterior mostró dos `BroadcastHashJoin Inner BuildRight`, uno para el cruce por celda y otro para incorporar atributos después del agregado. La inspección valida los operadores. Medir latencia de red requiere workers remotos.

### Medición de poda

El [Job de verificación](https://dbc-b38f0852-ecf9.cloud.databricks.com/jobs/719836273900213/runs/1003769994856895) ejecutó ambas consultas sobre la versión **6** de plata. Las dos contaron pings del 1 de junio dentro de LAT 25–32 y LON −98 a −88. El caso sin poda evaluó el día como condición del agregado. El caso con poda lo aplicó como filtro de lectura.

| Consulta | Pings en objetivo | Archivos tocados | Suma de tamaño de archivos |
|---|---:|---:|---:|
| Sin poda | 2.637.032 | 3 | 317.639.538 bytes |
| Con poda | 2.637.032 | 1 | 107.683.008 bytes |

El tamaño de archivos tocados bajó **66,1%**. El plan con poda registró `PartitionFilters: [isnotnull(_batch_date), (_batch_date = 2023-06-01)]`. Los bytes corresponden al tamaño total de los objetos, mientras que la proyección Parquet puede leer menos bytes físicos.

## Gobierno observado

| Ámbito | Principal | Acceso efectivo |
|---|---|---|
| Catálogo `clase14` | `mine-group9` | `USE CATALOG` |
| Esquema `clase14.equipo9` | `mine-group9` | `USE SCHEMA`, `CREATE TABLE`, `CREATE MODEL`, `SELECT`, `MODIFY` |
| Volumen `clase10.equipo9.checkpoints` | `mine-group9` | `READ VOLUME`, `WRITE VOLUME` |
| Jobs | `mine-group9` | `CAN_MANAGE_RUN` |
| Tablero | `mine-group9` | `CAN_READ` |
| Experimento MLflow | `mine-group9` | `CAN_READ` |
| Modelo registrado en UC | `mine-group9` | `EXECUTE` |

El esquema y el volumen pertenecen al docente. La API devolvió un rechazo al intentar crear un volumen nuevo y confirmó que DBFS público está deshabilitado. Los permisos de esquemas por rol requieren aprovisionamiento del administrador. [ADR-019](../../docs/adr/ADR-019-computo-y-almacenamiento-institucional.md).
