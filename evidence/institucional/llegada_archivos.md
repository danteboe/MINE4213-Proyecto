# Incorporación desde un volumen · 2026-11-25

## Evaluación de File arrival

Se reemplazó el cron de las 06:00, que estaba pausado, por una definición de **File arrival** con 60 segundos de espera y 60 segundos mínimos entre disparos. La ubicación vigilada fue `/Volumes/clase10/equipo9/checkpoints/oceanwatch_equipo9/landing/inbox/` en el volumen del equipo. Se activó temporalmente el trigger y se cargó el archivo diario oficial `AIS_2023_06_09.zip` de **343.405.316 bytes**. [Archivo NOAA 2023](https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/).

Se consultó Lakeflow Jobs 2.2 con `include_trigger_state=true` e `include_trigger_history=true`. El estado devolvió `using_file_events: true`, `last_failed: INVALID_STATE` y HTTP 403 al llamar a `sns:CreateTopic`. La llegada no creó una corrida `FILE_ARRIVAL`. El inventario de Unity Catalog mostró la ubicación administrada institucional sin permiso `MANAGE` para la cuenta y sin otra ubicación del proyecto. La definición se dejó pausada. [Operación y activación](../../docs/runbook.md).

## Ejecución con el archivo cargado

Se lanzó el mismo Job con `fecha_proceso` vacía y `entrenar=false`. Lakeflow registró el modo de disparo `ONE_TIME`. El manifiesto de ingesta seleccionó `AIS_2023_06_09.zip` y la fecha **2023-06-09** para las etapas de datos e inferencia. Auto Loader incorporó **8.791.124** posiciones con el mismo ID persistente de la carga inicial `740d3841-e1b0-4040-b730-c8fc0d536dc8`. [Corrida institucional](https://dbc-b38f0852-ecf9.cloud.databricks.com/jobs/83886562675136/runs/338695807776741).

| Bronce | Plata | Cuarentena | Duplicados válidos | Diferencia |
|---:|---:|---:|---:|---:|
| 8.791.124 | 8.524.896 | 265.934 | 294 | 0 |

Las seis tareas terminaron en `SUCCESS` en 9 min 32 s. Oro escribió las cuatro tablas del día. La inferencia publicó **95.687 anomalías** con la versión **1** del campeón y dejó la confirmación `control_landing/confirmados/AIS_2023_06_09.zip`.

La segunda ejecución con la misma bandeja terminó en `SUCCESS`. Ingesta devolvió **0 filas nuevas** y un manifiesto con fechas y entradas vacías. Inferencia salió sin fechas de trabajo. [Corrida de verificación](https://dbc-b38f0852-ecf9.cloud.databricks.com/jobs/83886562675136/runs/576869843843158).

La prueba local del script verificó que solicita la corrida tras una copia exitosa y detiene el flujo si la carga falla. La suite local registró 117 pruebas aprobadas y 1 omitida porque requiere PySpark.

## Consola publicada

En la revisión `2026-11-25T18:38:23.270Z` se comprobaron el ranking, las dos tablas y los dos mapas con puntos. Al elegir `2023-06-07`, las dos tablas mostraron registros con esa fecha y el ranking se actualizó. Las consultas del tablero devolvieron 2.000 celdas, 15 puertos y 500 filas por tabla para el 7 de junio y, con el valor **Última fecha**, para el 9 de junio recién incorporado. La corrección usó mapas `symbol-map` versión 2 con `coordinates` y las propiedades de columnas requeridas por la tabla importada. El filtro comparte el parámetro de fecha entre los cuatro datasets. [Tablero publicado](https://dbc-b38f0852-ecf9.cloud.databricks.com/dashboardsv3/01f1e5507d1b18a5963d1a49e4329629/published) · [Especificación de widgets](https://github.com/databricks/databricks-agent-skills/blob/main/plugins/databricks/copilot/skills/databricks-aibi-dashboards/references/1-widget-specifications.md).
