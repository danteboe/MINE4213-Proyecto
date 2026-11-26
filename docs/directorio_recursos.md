# Directorio de recursos de OceanWatch

Este directorio corresponde a la Parte 2 en el workspace institucional. Los enlaces de Databricks requieren sesión institucional y los permisos de cada recurso. El despliegue se define en el [bundle](../databricks.yml).

## Jobs y cómputo

| Recurso | Dónde abrirlo | Función |
|---|---|---|
| **Detectar archivos AIS** | [Job 342422518655466](https://dbc-b38f0852-ecf9.cloud.databricks.com/jobs/342422518655466) · [Definición](../resources/jobs/oceanwatch_detector.job.yml) | Revisa a la hora y a la media hora si hay nombres sin confirmar. Ejecuta `detectar → hay_archivos → procesar`. La última tarea llama a Entrega 2 cuando la condición es verdadera. |
| **Entrega 2** | [Job 83886562675136](https://dbc-b38f0852-ecf9.cloud.databricks.com/jobs/83886562675136) · [Definición](../resources/jobs/oceanwatch_pipeline.job.yml) | Ejecuta preparación, ingesta de bronce, calidad de plata, conciliación, oro e inferencia. También admite backfill y entrenamiento explícitos. |
| **Verificación** | [Job 719836273900213](https://dbc-b38f0852-ecf9.cloud.databricks.com/jobs/719836273900213) · [Definición](../resources/jobs/oceanwatch_verificacion.job.yml) | Prueba fallo de commit, reanudación, replay, cuarentena y deduplicación en tablas y rutas de prueba. Opcionalmente mide poda contra plata en modo lectura. |
| **Clúster personal** | Compute → Personal Compute de Juan David Torres · ID `1114-165401-a0ye3pxs` | Cómputo clásico dedicado para Entrega 2 y Verificación. Arranca al ejecutar el Job si está apagado. Se apaga tras 60 min de inactividad. |
| **Serverless Starter Warehouse** | SQL Warehouses → Serverless Starter Warehouse · ID `4c751147a4c66a5e` | Ejecuta el detector de metadatos y las consultas del tablero. El trabajo de datos y ML usa el clúster clásico. |
| **Consola de tráfico** | [Tablero publicado](https://dbc-b38f0852-ecf9.cloud.databricks.com/dashboardsv3/01f1e5507d1b18a5963d1a49e4329629/published) · [Definición](../resources/dashboards/oceanwatch_oro.lvdash.json) | Mapa de intensidad, ranking, buques en puerto, tabla y mapa de anomalías por fecha UTC. |

La dependencia entre los dos Jobs de producción es `Detectar archivos AIS → Entrega 2`. Las dependencias internas, parámetros y reintentos se detallan en el [DAG](architecture/fase4-dag-orquestacion.md).

## Volumen y carpetas

En Databricks, abra **Catalog → clase10 → equipo9 → Volumes → checkpoints**. Todas las subrutas de la tabla son relativas a `/Volumes/clase10/equipo9/checkpoints/oceanwatch_equipo9/`. El proyecto usa un volumen administrado existente para estos archivos y guarda las tablas Delta administradas en `clase14.equipo9`.

| Subruta | Qué guarda | Quién la usa |
|---|---|---|
| `landing/inbox/` | Entregas originales e inmutables de NOAA, por ejemplo `ais-2026-06-30.csv.zst`. | El equipo sube aquí. El detector y la ingesta consultan esta carpeta. |
| `landing/csv/date=AAAA-MM-DD/` | CSV validados y normalizados con nombres estables para Auto Loader. | La ingesta escribe y Auto Loader lee. |
| `stream/bronze/v1/_checkpoint/` | Progreso del stream y estado necesario para reanudar el sink Delta. | Structured Streaming. |
| `stream/bronze/v1/_schema/` | Metadatos del esquema de Auto Loader. | Auto Loader. |
| `control_landing/corridas/<run_id>.json` | Fechas y archivos seleccionados en la corrida. | Ingesta escribe. Plata, conciliación, oro y ML leen. |
| `control_landing/confirmados/` | Un marcador con el nombre original del archivo tras publicar oro e inferencia. | ML escribe. Ingesta y detector consultan. |
| `pruebas/<id>/` | Landing, checkpoint y marcadores de pruebas aisladas. | Verificación. Su limpieza ocurre al terminar la prueba. |

El ZIP histórico descargado por backfill y el CSV temporal se procesan en disco local del clúster. Para consultar los originales subidos por el equipo, use `landing/inbox/`. [Operación y recuperación](runbook.md).

## Notebooks y código desplegado

En Workspace, la copia del bundle está bajo
`/Workspace/Users/jd.torresa1@uniandes.edu.co/.bundle/oceanwatch_entrega2/institucional/files/`.
Para ver la ejecución y el output de un notebook, abra su tarea en el Job correspondiente.

| Ruta en el repo | Ruta relativa en Workspace | Función |
|---|---|---|
| [oceanwatch_cluster.py](../notebooks/oceanwatch_cluster.py) | `notebooks/oceanwatch_cluster` | Recibe `task_key` y los parámetros de Lakeflow. Ejecuta la etapa solicitada y registra el resultado de la tarea. |
| [oceanwatch_verificacion.py](../notebooks/oceanwatch_verificacion.py) | `notebooks/oceanwatch_verificacion` | Prepara la muestra aislada, prueba reanudación y calidad y compara I/O con y sin poda. |
| [oceanwatch_fallo_stream.py](../notebooks/oceanwatch_fallo_stream.py) | `notebooks/oceanwatch_fallo_stream` | Notebook hijo de Verificación. Ejecuta el stream contra una restricción Delta que inyecta un fallo controlado. |
| [detectar_entradas.sql](../resources/jobs/detectar_entradas.sql) | `resources/jobs/detectar_entradas.sql` | Cruza solo rutas del volumen y marcadores de finalización. Devuelve el número de entradas pendientes. |

| Ubicación en el repo | Responsabilidad |
|---|---|
| [conf/config.yaml](../conf/config.yaml) | Contrato AIS, namespace institucional, reglas, parámetros espaciales, fechas de ML y configuración conservada del prototipo. |
| [src/oceanwatch/ingest](../src/oceanwatch/ingest/) | Esquema, lectura CSV2/ZIP, rutas y Auto Loader. |
| [src/oceanwatch/orquestacion](../src/oceanwatch/orquestacion/) | Staging, manifiestos, confirmación, reemplazos Delta y auditoría por tarea. |
| [src/oceanwatch/quality](../src/oceanwatch/quality/) | Reglas y compuertas, máscara hidrográfica, plata, cuarentena y conciliación. |
| [src/oceanwatch/gold](../src/oceanwatch/gold/) | Trayectorias, presencia, matrices y publicación por fecha. |
| [src/oceanwatch/ml](../src/oceanwatch/ml/) y [recursos ML](../resources/ml/) | Features, muestreo, entrenamiento, registro y puntuación. El bundle declara el experimento y el modelo. |
| [World Port Index](../resources/world_port_index.csv) y [referencia hidrográfica](../resources/hidrografia.wkb.gz) | Dimensión portuaria y geometrías de aguas navegables que consume el clúster. |
| [tools](../tools/) y [tests](../tests/) | Carga local, preparación reproducible de hidrografía y pruebas de regresión. |

### Notebooks de la Parte 1 y del prototipo

El [notebook de la Parte 1](../entrega1_oceanwatch.ipynb) contiene la exploración, el diagnóstico de calidad, las preguntas de negocio y la comparación de layouts. Los notebooks de fase documentan prototipos de Free Edition. El bundle institucional excluye ambos conjuntos.

| Notebook histórico | Función |
|---|---|
| [fase2_ingesta_streaming](../notebooks/fase2_ingesta_streaming.py) | Ingesta inicial y conteo de duplicados. |
| [fase2_pruebas_fallos](../notebooks/fase2_pruebas_fallos.py) | Fallo, reenvío, rescate de esquema y reinicio en sandbox. |
| [fase3_plata_calidad](../notebooks/fase3_plata_calidad.py) | Materialización de plata, cuarentena y métricas del prototipo. |
| [fase3_prueba_compuerta](../notebooks/fase3_prueba_compuerta.py) | Datos sintéticos para comprobar el aislamiento de registros rechazados. |
| [fase3_oro](../notebooks/fase3_oro.py) | Primeras implementaciones de trayectorias, puertos y matrices. |
| [fase4_tarea_ingesta](../notebooks/fase4_tarea_ingesta.py) | Primera tarea parametrizada de ingesta. |
| [fase4_tarea_plata](../notebooks/fase4_tarea_plata.py) | Orquestación histórica de plata mediante SQL. |
| [fase4_tarea_conciliacion](../notebooks/fase4_tarea_conciliacion.py) | Verificación de la identidad de conteos y bloqueo de oro. |
| [fase4_tarea_oro](../notebooks/fase4_tarea_oro.py) | Materialización histórica de oro mediante SQL. |
| [fase6_entrenamiento_mlflow](../notebooks/fase6_entrenamiento_mlflow.py) | Comparación inicial con z-score y entrenamiento en Free Edition. |
| [fase6_tarea_ml_anomalias](../notebooks/fase6_tarea_ml_anomalias.py) | Puntuación con el primer campeón y auditoría del prototipo. |

Los [scripts SQL históricos](../resources/sql/) contienen el bootstrap de catálogo, las materializaciones y las pruebas que acompañan estas fases, más los DDL de comentarios, permisos, anomalías y la matriz diaria. La versión institucional crea y escribe las tablas mediante el notebook `oceanwatch_cluster` y sus módulos.

## Tablas y modelo

En Databricks, abra **Catalog → clase14 → equipo9**. Los siguientes nombres se califican con ese catálogo y esquema.

| Objeto | Qué contiene |
|---|---|
| `oceanwatch_bronze_ais_raw` | Registros AIS interpretados con esquema explícito, columnas de origen, fecha del archivo y rescate. Tabla Delta append-only. |
| `oceanwatch_silver_ais_clean` | Posiciones válidas y deduplicadas por `(MMSI, BaseDateTime)`, más atributos saneados y multiplicidad retirada. |
| `oceanwatch_quarantine_ais_rejected` | Mensajes originales rechazados y sus causas. |
| `oceanwatch_quality_metricas_calidad` | Conteos, tasa y alerta por fecha y regla de descarte. |
| `oceanwatch_quality_ejecuciones_job` | Una fila de auditoría por corrida del pipeline y tarea. |
| `oceanwatch_gold_trayectorias_diarias` | Trayectorias, distancias e intervalo observado por buque y día. |
| `oceanwatch_gold_presencia_puertos` | Pings e intervalo observado por buque, puerto y día. |
| `oceanwatch_gold_matriz_trafico` | Pings y buques únicos por celda y hora. |
| `oceanwatch_gold_matriz_trafico_diaria` | Pings y buque-horas por celda y día. |
| `oceanwatch_gold_anomalias_velocidad` | Pings con score anómalo, versión del modelo y corrida de MLflow. |
| `oceanwatch_gold_world_port_index` | Dimensión portuaria de referencia de NGA. |
| [oceanwatch_ml_detector_anomalias_velocidad](https://dbc-b38f0852-ecf9.cloud.databricks.com/explore/data/models/clase14/equipo9/oceanwatch_ml_detector_anomalias_velocidad?w=7474646467813584) | Modelo registrado en Unity Catalog. El alias `champion` apunta a la versión `1` verificada. |
| [Experimento MLflow](https://dbc-b38f0852-ecf9.cloud.databricks.com/ml/experiments/2530466454792025?w=7474646467813584) | Muestras, métricas, features y artefacto reproducible. El [run de entrenamiento](https://dbc-b38f0852-ecf9.cloud.databricks.com/ml/experiments/2530466454792025/runs/6a2e6962426e4497885abab387ecdc18) alimenta el campeón. |

Consulte el [contrato de oro](data_contract_gold.md) para interpretar las medidas y la [verificación de gobernanza](../evidence/institucional/gobernanza.md) para consultar los accesos observados.
