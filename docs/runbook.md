# Operación de OceanWatch en Databricks institucional

## Despliegue

1. Autenticar la CLI con OAuth en el [workspace institucional](https://dbc-b38f0852-ecf9.cloud.databricks.com). El perfil debe resolver a la identidad autorizada para el clúster dedicado. Para desplegar con otro clúster de Unity Catalog, pasar `--var cluster_id=<id>` al bundle. Usar DBR 18 o posterior y comprobar compatibilidad de las bibliotecas instaladas por las tareas.
2. Ejecutar `databricks bundle validate --strict` y `databricks bundle deploy` desde la raíz. El bundle sincroniza el código, la configuración, los notebooks y las referencias geográficas. Aprovisiona tres Jobs, el tablero, el experimento y el registro UC. El destino por defecto es `institucional`. Si cambia de usuario, ajuste la ruta del experimento en `conf/config.yaml`.
3. En un entorno nuevo, iniciar con `--var pausa_detector=PAUSED`. Lanzar la primera corrida con entrenamiento y los días de referencia para crear los directorios del volumen y el campeón. Desplegar de nuevo con el detector activo. Si cambia el volumen de trabajo, ajustar también `ruta_confirmados` en la tarea SQL del detector.

```sh
databricks auth login --host https://dbc-b38f0852-ecf9.cloud.databricks.com --profile oceanwatch
databricks bundle validate -p oceanwatch --strict
databricks bundle deploy -p oceanwatch --var pausa_detector=PAUSED
databricks bundle run -p oceanwatch oceanwatch_pipeline_entrega2 --params fecha_proceso=2023-06-08,entrenar=true
databricks bundle deploy -p oceanwatch
```

La preparación valida el uso de `clase14.equipo9` al crear los objetos y utiliza `/Volumes/clase10/equipo9/checkpoints/oceanwatch_equipo9`. Compruebe `CREATE TABLE`, `CREATE MODEL`, `USE SCHEMA`, `USE CATALOG`, `READ VOLUME` y `WRITE VOLUME` si cambia de identidad o de workspace. Una ruta de volumen nueva y un nombre de destino Delta nuevo deben configurarse juntos si se quiere reproducir desde cero. La variable `--var warehouse_id=<id>` permite elegir otro almacén SQL con `CAN USE` para su cuenta y el grupo consumidor.

## Cargar un archivo

1. Abra el volumen `clase10.equipo9.checkpoints` en Catalog y navegue a `oceanwatch_equipo9/landing/inbox/`. Cargue el archivo directamente en esa carpeta. El CSV2 moderno conserva el nombre `ais-AAAA-MM-DD.csv.zst`. Para archivos de la serie original use `AIS_AAAA_MM_DD.zip` o `AIS_AAAA_MM_DD.csv`. El formato original admite un sufijo de lote como `_parte2`.
2. El detector consulta a la hora y a la media hora en Bogotá. Cuando encuentra una entrega pendiente, la tarea `Run Job` inicia Entrega 2 en el clúster personal. Databricks lo arranca si está apagado. Si la bandeja está al día, `procesar` queda en `EXCLUDED` y no se solicita cómputo clásico. Si el detector sigue ocupado con un hijo, Lakeflow omite el siguiente tick y vuelve a consultar en uno posterior. [Detector](https://dbc-b38f0852-ecf9.cloud.databricks.com/jobs/342422518655466) · [Pipeline](https://dbc-b38f0852-ecf9.cloud.databricks.com/jobs/83886562675136) · [Arranque automático de cómputo](https://docs.databricks.com/aws/en/compute/clusters-manage#compute-autostart-for-jobs-and-jdbcodbc-queries).
3. Tras `SUCCESS`, recargue el tablero para consultar los datos nuevos. El filtro acepta una fecha UTC en formato AAAA-MM-DD y **Última fecha** muestra el día más reciente de oro.

```powershell
.\tools\subir_ais.ps1 -Archivo 'C:\Descargas\ais-2026-06-29.csv.zst' -Perfil oceanwatch
```

El script resuelve la carpeta del bundle y sube el archivo sin sobrescribir. Databricks se encarga de iniciar el procesamiento. La carpeta recibe entregas inmutables identificadas por nombre. Para CSV2, el Job valida el encabezado y proyecta las columnas al esquema de bronce mientras descomprime el archivo en disco local. Para el ZIP comprueba el CRC. Publica el CSV procesable en `landing/csv/date=AAAA-MM-DD/` y registra las fechas de la corrida en `control_landing/corridas/`. Los archivos se confirman en `control_landing/confirmados/` al terminar inferencia. El Job reutiliza el checkpoint persistente de Auto Loader. Si hay un archivo inválido o una colisión con otro contenido, la corrida falla con el nombre del archivo. Corrija la bandeja antes de reintentar.

El intervalo vigente es **30 minutos**. El cambio temporal a 2 minutos está reservado para la sustentación y se ejecuta cuando el equipo lo indique. Las revisiones usan el warehouse y pueden iniciarlo si está detenido. [Guía de sustentación](guia_sustentacion.md#demostración-de-llegada).

### Activar File arrival

El administrador del almacenamiento debe habilitar los permisos de eventos del almacenamiento administrado. La prueba devolvió `INVALID_STATE` al intentar aprovisionar un tópico con `sns:CreateTopic` en el rol AWS del workspace. Tras corregirlo, ejecute `databricks bundle deploy -p oceanwatch --var pause_status=UNPAUSED --var pausa_detector=PAUSED` para cambiar de mecanismo sin activar ambos. Mantenga los overrides en los siguientes despliegues.

Lakeflow vigila solo `landing/inbox/`, espera 60 segundos sin nuevos archivos y deja al menos 60 segundos entre disparos. Suba un archivo con nombre nuevo únicamente desde Catalog o `databricks fs cp` y compruebe una corrida `FILE_ARRIVAL` hasta `SUCCESS`. Consulte `GET /api/2.2/jobs/get?job_id=83886562675136&include_trigger_state=true&include_trigger_history=true` si falta la corrida. [Trigger por llegada de archivos](https://docs.databricks.com/aws/en/jobs/file-arrival-triggers) · [Eventos de archivos](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/file-events-faq).

## Backfill y reintentos

- El valor vacío de `fecha_proceso` drena las entradas pendientes, tanto desde el detector como con **Run now**. Si la bandeja está al día, una corrida manual registra un lote vacío. El cron de las 06:00 es evidencia histórica del prototipo institucional.
- Para descargar una fecha del archivo histórico ZIP directamente de NOAA, ejecute `databricks bundle run oceanwatch_pipeline_entrega2 --params fecha_proceso=AAAA-MM-DD`. Use `fecha_inicio` para un intervalo inclusivo de hasta 31 días. Los archivos CSV2 se incorporan por la bandeja. El Job espera en cola si otro lote está activo. En `entrenar=true` se incluyen train y validación para construir la plata de entrenamiento. La tarea final puntúa las fechas solicitadas posteriores al train. El modelo se promueve cuando su versión está lista en Unity Catalog.
- La ingesta de backfill también incorpora archivos pendientes de la bandeja. Los reintentos reutilizan los CSV publicados, la tabla Delta y el checkpoint. Si una carga se interrumpe, valide su integridad y repita la copia antes de reintentar. Ante la pérdida del checkpoint, reconstruya en destinos aislados y valide conteos antes de conmutar. Eliminar solo el checkpoint de producción puede reingestar todo el landing.
- Consulte la interfaz de Lakeflow Jobs para fallos, número de intento y tiempo por tarea. Consulte `clase14.equipo9.oceanwatch_quality_ejecuciones_job` para resultados y conteos por tarea. Las tareas que ni siquiera arrancan, como un fallo de librería o aprovisionamiento, se diagnostican en Lakeflow. Una discrepancia de conciliación bloquea oro y ML. Corrija insumos y repare desde plata o desde la primera tarea afectada.
- La publicación por fecha es atómica por tabla. Los productos de oro pueden reflejar versiones de corridas distintas mientras termina la publicación múltiple. Si una tarea falla entre escrituras, el reintento sustituye el día y conserva el histórico de los demás. Valide el resultado completo al finalizar el Job.

### Seguimiento de una carga

| Observación | Dónde revisar |
|---|---|
| La carga terminó y todavía no hay una corrida | En el detector, revise la siguiente ejecución programada y compruebe el nombre y la subruta del archivo. |
| `detectar` terminó con `pendientes = 0` | Compare `landing/inbox/` con `control_landing/confirmados/`. El nombre confirmado identifica una entrega ya publicada. |
| `procesar` inició | Abra la corrida hija de Entrega 2 desde la tarea `Run Job`. El notebook de ingesta imprime las fechas y el ID del stream. |
| El Job falló | Abra la tarea fallida, su intento y la auditoría. La próxima revisión vuelve a encontrar el archivo mientras esté pendiente. Puede reparar desde la primera tarea que requiera corrección. |
| ML terminó correctamente | Compruebe el marcador en `control_landing/confirmados/` y consulte la fecha en la consola. |

```sql
SELECT task_key, fecha_proceso, estado, trigger, ts_inicio, ts_fin, filas_clave, detalle
FROM clase14.equipo9.oceanwatch_quality_ejecuciones_job
WHERE job_run_id = 796009776834361
ORDER BY ts_inicio;
```

## Verificación y consola

Las pruebas unitarias se pueden ejecutar sin credenciales ni Spark.

```sh
uv run --no-project --python 3.12 --with pytest --with pyyaml --with pyzstd==0.20.0 --with global-land-mask==1.0.0 --with shapely==2.1.2 python -m pytest -q
```

Use el Job institucional para la prueba de integración con Spark y Unity Catalog.

```sh
databricks bundle run -p oceanwatch oceanwatch_verificacion --params fecha_proceso=2023-06-01,medir_poda=true
```

El Job de verificación usa un checkpoint y tablas efímeras para simular un commit fallido, reanudarlo y comprobar cuarentena, duplicados y replay vacío. También compara el mismo resultado de una consulta de zona y día con y sin poda de particiones. El tamaño de archivos informado corresponde al tamaño de los objetos tocados y es una cota del I/O físico de las columnas Parquet solicitadas.

El [tablero institucional](https://dbc-b38f0852-ecf9.cloud.databricks.com/dashboardsv3/01f1e5507d1b18a5963d1a49e4329629/published) consulta solo los productos `oceanwatch_gold_*`. Para publicar un cambio, ejecute `databricks lakeview publish <id> --embed-credentials=false` si el despliegue actualiza el borrador. El lector ejecuta consultas con su propia identidad. El SQL warehouse institucional sirve la visualización y las consultas interactivas. El procesamiento de datos y ML corre en el clúster.
