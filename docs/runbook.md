# Operación de OceanWatch en Databricks institucional

## Despliegue

1. Autenticar la CLI con OAuth en el [workspace institucional](https://dbc-b38f0852-ecf9.cloud.databricks.com). El perfil debe resolver a la identidad autorizada para el clúster dedicado. Para desplegar con otro clúster de Unity Catalog, pasar `--var cluster_id=<id>` al bundle. Usar DBR 18 o posterior y comprobar compatibilidad de las bibliotecas instaladas por las tareas.
2. Ejecutar `databricks bundle validate --strict` y `databricks bundle deploy` desde la raíz. El bundle sincroniza el código, la configuración, los notebooks y las referencias geográficas. Aprovisiona tres Jobs, el tablero, el experimento y el registro UC. El destino por defecto es `institucional`. Si cambia de usuario, ajuste la ruta del experimento en `conf/config.yaml`.
3. En un entorno nuevo, iniciar con `--var pausa_detector=PAUSED`. Lanzar la primera corrida con entrenamiento y los días de referencia para crear los directorios del volumen y el campeón. Desplegar de nuevo con el detector activo. Si cambia el volumen de trabajo, ajustar también `ruta_confirmados` en la tarea SQL del detector.

```sh
databricks auth login --host https://dbc-b38f0852-ecf9.cloud.databricks.com --profile oceanwatch
databricks bundle validate -p oceanwatch --strict
databricks bundle deploy -p oceanwatch
databricks bundle run -p oceanwatch oceanwatch_pipeline_entrega2 --params fecha_proceso=2023-06-08,entrenar=true
```

La preparación valida el uso de `clase14.equipo9` al crear los objetos y utiliza `/Volumes/clase10/equipo9/checkpoints/oceanwatch_equipo9`. Compruebe `CREATE TABLE`, `CREATE MODEL`, `USE SCHEMA`, `USE CATALOG`, `READ VOLUME` y `WRITE VOLUME` si cambia de identidad o de workspace. Una ruta de volumen nueva y un nombre de destino Delta nuevo deben configurarse juntos si se quiere reproducir desde cero. La variable `--var warehouse_id=<id>` permite elegir otro almacén SQL con `CAN USE` para su cuenta y el grupo consumidor.

## Cargar un archivo

1. Abra el volumen `clase10.equipo9.checkpoints` en Catalog y navegue a `oceanwatch_equipo9/landing/inbox/`. Cargue el archivo directamente en esa carpeta. El CSV2 moderno conserva el nombre `ais-AAAA-MM-DD.csv.zst`. Para archivos de la serie original use `AIS_AAAA_MM_DD.zip` o `AIS_AAAA_MM_DD.csv`. El formato original admite un sufijo de lote como `_parte2`.
2. El [detector](https://dbc-b38f0852-ecf9.cloud.databricks.com/jobs/342422518655466) consulta a las horas y medias horas en UTC. La siguiente revisión disponible inicia el [pipeline](https://dbc-b38f0852-ecf9.cloud.databricks.com/jobs/83886562675136) si aparece una entrega pendiente. Las revisiones sin entradas dejan la tarea `procesar` en `EXCLUDED`. Si una revisión está ocupada con el pipeline, Lakeflow omite el siguiente tick y vuelve a consultar en uno posterior.
3. Tras `SUCCESS`, recargue el tablero para consultar los datos nuevos. El filtro acepta una fecha UTC en formato AAAA-MM-DD y **Última fecha** muestra el día más reciente de oro.

```powershell
.\tools\subir_ais.ps1 -Archivo 'C:\Descargas\ais-2026-06-29.csv.zst' -Perfil oceanwatch
```

El script resuelve la carpeta del bundle y sube el archivo sin sobrescribir. Databricks se encarga de iniciar el procesamiento. La carpeta recibe entregas inmutables identificadas por nombre. Para CSV2, el Job valida el encabezado y proyecta las columnas al esquema de bronce mientras descomprime el archivo en disco local. Para el ZIP comprueba el CRC. Publica el CSV procesable en `landing/csv/date=AAAA-MM-DD/` y registra las fechas de la corrida en `control_landing/corridas/`. Los archivos se confirman en `control_landing/confirmados/` al terminar inferencia. El Job reutiliza el checkpoint persistente de Auto Loader. Si hay un archivo inválido o una colisión con otro contenido, la corrida falla con el nombre del archivo. Corrija la bandeja antes de reintentar.

Para una demostración en clase, se puede desplegar temporalmente con `--var "cron_detector=0 0/2 * * * ?"` y volver a desplegar con el valor por defecto de 30 minutos. Las revisiones usan el warehouse y pueden iniciarlo si está detenido.

### Activar File arrival

El administrador del almacenamiento debe habilitar los permisos de eventos del almacenamiento administrado. La prueba devolvió `INVALID_STATE` al intentar aprovisionar un tópico con `sns:CreateTopic` en el rol AWS del workspace. Tras corregirlo, ejecute `databricks bundle deploy -p oceanwatch --var pause_status=UNPAUSED --var pausa_detector=PAUSED` para cambiar de mecanismo sin activar ambos. Mantenga los overrides en los siguientes despliegues.

Lakeflow vigila solo `landing/inbox/`, espera 60 segundos sin nuevos archivos y deja al menos 60 segundos entre disparos. Suba un archivo con nombre nuevo únicamente desde Catalog o `databricks fs cp` y compruebe una corrida `FILE_ARRIVAL` hasta `SUCCESS`. Consulte `GET /api/2.2/jobs/get?job_id=83886562675136&include_trigger_state=true&include_trigger_history=true` si falta la corrida. [Trigger por llegada de archivos](https://docs.databricks.com/aws/en/jobs/file-arrival-triggers) · [Eventos de archivos](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/file-events-faq).

## Backfill y reintentos

- El valor vacío de `fecha_proceso` drena las entradas pendientes, tanto desde el detector como con **Run now**. Si la bandeja está al día, el Job registra una corrida vacía. El cron de las 06:00 es evidencia histórica del prototipo institucional.
- Para descargar una fecha del archivo histórico ZIP directamente de NOAA, ejecute `databricks bundle run oceanwatch_pipeline_entrega2 --params fecha_proceso=AAAA-MM-DD`. Use `fecha_inicio` para un intervalo inclusivo de hasta 31 días. Los archivos CSV2 se incorporan por la bandeja. El Job espera en cola si otro lote está activo. En `entrenar=true` se incluyen train y validación para construir la plata de entrenamiento. La tarea final puntúa las fechas solicitadas posteriores al train. El modelo se promueve cuando su versión está lista en Unity Catalog.
- Los reintentos de ingesta reutilizan los CSV publicados, la tabla Delta y el checkpoint. Si la descarga se interrumpe, compare la longitud y vuelva a ejecutar la tarea. Conserve el archivo que ya haya sido consumido con su nombre estable. Ante la pérdida del checkpoint, la recuperación requiere reconstruir en destinos aislados y validar conteos antes de conmutar. Eliminar solo el checkpoint de producción puede reingestar todo el landing.
- Consulte la interfaz de Lakeflow Jobs para fallos, número de intento y tiempo por tarea. Consulte `clase14.equipo9.oceanwatch_quality_ejecuciones_job` para resultados y conteos por tarea. Las tareas que ni siquiera arrancan, como un fallo de librería o aprovisionamiento, se diagnostican en Lakeflow. Una discrepancia de conciliación bloquea oro y ML. Corrija insumos y repare desde plata o desde la primera tarea afectada.
- La publicación por fecha es atómica por tabla. Los productos de oro pueden reflejar versiones de corridas distintas mientras termina la publicación múltiple. Si una tarea falla entre escrituras, el reintento sustituye el día y conserva el histórico de los demás. Valide el resultado completo al finalizar el Job.

## Verificación y consola

```sh
databricks bundle run oceanwatch_verificacion --params fecha_proceso=2023-06-08
```

El Job de verificación usa un checkpoint y tablas efímeras para simular un commit fallido, reanudarlo y comprobar cuarentena, duplicados y replay vacío. También compara el mismo resultado de una consulta de zona y día con y sin poda de particiones. El tamaño de archivos informado corresponde al tamaño de los objetos tocados y es una cota del I/O físico de las columnas Parquet solicitadas.

El [tablero institucional](https://dbc-b38f0852-ecf9.cloud.databricks.com/dashboardsv3/01f1e5507d1b18a5963d1a49e4329629/published) consulta solo los productos `oceanwatch_gold_*`. Para publicar un cambio, ejecute `databricks lakeview publish <id> --embed-credentials=false` si el despliegue actualiza el borrador. El lector ejecuta consultas con su propia identidad. El SQL warehouse institucional sirve la visualización y las consultas interactivas. El procesamiento de datos y ML corre en el clúster.
