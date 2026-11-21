# Operación de OceanWatch en Databricks institucional

## Despliegue

1. Autenticar la CLI con OAuth en el [workspace institucional](https://dbc-b38f0852-ecf9.cloud.databricks.com). El perfil debe resolver a la identidad autorizada para el clúster dedicado. Para desplegar con otro clúster de Unity Catalog, pasar `--var cluster_id=<id>` al bundle. Usar DBR 18 o posterior y comprobar compatibilidad de las bibliotecas instaladas por las tareas.
2. Ejecutar `databricks bundle validate --strict` y `databricks bundle deploy` desde la raíz. El bundle sincroniza el código, la configuración, los notebooks y las referencias geográficas. Aprovisiona dos Jobs, el tablero, el experimento y el registro UC. El destino por defecto es `institucional`. Si cambia de usuario, ajuste la ruta del experimento en `conf/config.yaml`.
3. Lanzar la primera corrida con entrenamiento y los días de referencia.

```sh
databricks auth login --host https://dbc-b38f0852-ecf9.cloud.databricks.com --profile oceanwatch
databricks bundle validate -p oceanwatch --strict
databricks bundle deploy -p oceanwatch
databricks bundle run -p oceanwatch oceanwatch_pipeline_entrega2 --params fecha_proceso=2023-06-08,entrenar=true
```

La preparación valida el uso de `clase14.equipo9` al crear los objetos y utiliza `/Volumes/clase10/equipo9/checkpoints/oceanwatch_equipo9`. Compruebe `CREATE TABLE`, `CREATE MODEL`, `USE SCHEMA`, `USE CATALOG`, `READ VOLUME` y `WRITE VOLUME` si cambia de identidad o de workspace. Una ruta de volumen nueva y un nombre de destino Delta nuevo deben configurarse juntos si se quiere reproducir desde cero. Para otro almacén SQL use `--var warehouse_id=<id>`; su cuenta y el grupo consumidor requieren `CAN USE`.

## Backfill, reintentos y horario

- `fecha_proceso` es la fecha UTC de observación NOAA en formato ISO. La fecha histórica por defecto es el 8 de junio de 2023. El cron del ejercicio es diario a las 06:00 de Bogotá y repite ese día de archivo. Queda en pausa al terminar la prueba programada. Los endpoints de la NOAA son un archivo histórico y tienen rezago de publicación. Un feed con descubrimiento de última fecha publicada requiere su propio criterio de disponibilidad.
- Para una nueva fecha, ejecute `databricks bundle run oceanwatch_pipeline_entrega2 --params fecha_proceso=AAAA-MM-DD`. Use `fecha_inicio` para un intervalo inclusivo de hasta 31 días. El Job espera en cola si otro lote está activo. En `entrenar=true` se incluyen train y validación para construir la plata de entrenamiento. La tarea final puntúa las fechas solicitadas posteriores al train. El modelo se promueve cuando su versión está lista en Unity Catalog.
- Los reintentos de ingesta reutilizan los CSV publicados, la tabla Delta y el checkpoint. Si la descarga se interrumpe, compare la longitud y vuelva a ejecutar la tarea. Conserve el archivo que ya haya sido consumido con su nombre estable. Ante la pérdida del checkpoint, la recuperación requiere reconstruir en destinos aislados y validar conteos antes de conmutar. Eliminar solo el checkpoint de producción puede reingestar todo el landing.
- Consulte la interfaz de Lakeflow Jobs para fallos, número de intento y tiempo por tarea. Consulte `clase14.equipo9.oceanwatch_quality_ejecuciones_job` para resultados y conteos por tarea. Las tareas que ni siquiera arrancan, como un fallo de librería o aprovisionamiento, se diagnostican en Lakeflow. Una discrepancia de conciliación bloquea oro y ML. Corrija insumos y repare desde plata o desde la primera tarea afectada.
- La publicación por fecha es atómica por tabla. Los productos de oro pueden reflejar versiones de corridas distintas mientras termina la publicación múltiple. Si una tarea falla entre escrituras, el reintento sustituye el día y conserva el histórico de los demás. Valide el resultado completo al finalizar el Job.

## Verificación y consola

```sh
databricks bundle run oceanwatch_verificacion --params fecha_proceso=2023-06-08
```

El segundo Job usa un checkpoint y tablas efímeras para simular un commit fallido, reanudarlo y comprobar cuarentena, duplicados y replay vacío. También compara el mismo resultado de una consulta de zona y día con y sin poda de particiones. El tamaño de archivos informado corresponde al tamaño de los objetos tocados y es una cota del I/O físico de las columnas Parquet solicitadas.

El [tablero institucional](https://dbc-b38f0852-ecf9.cloud.databricks.com/dashboardsv3/01f1e5507d1b18a5963d1a49e4329629/published) consulta solo los productos `oceanwatch_gold_*`. Para publicar un cambio, ejecute `databricks lakeview publish <id> --embed-credentials=false` si el despliegue actualiza el borrador. El lector ejecuta consultas con su propia identidad. El SQL warehouse institucional sirve la visualización y las consultas interactivas. El procesamiento de datos y ML corre en el clúster.
