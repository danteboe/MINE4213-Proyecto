# Guía para la sustentación

Los tres integrantes deben poder explicar el flujo completo. Abran con una sesión institucional el [directorio](directorio_recursos.md), el [detector](https://dbc-b38f0852-ecf9.cloud.databricks.com/jobs/342422518655466), el [pipeline de Entrega 2](https://dbc-b38f0852-ecf9.cloud.databricks.com/jobs/83886562675136) y el [tablero publicado](https://dbc-b38f0852-ecf9.cloud.databricks.com/dashboardsv3/01f1e5507d1b18a5963d1a49e4329629/published).

## Demostración de llegada

1. Seleccionen un NOAA CSV2 cuyo nombre todavía no aparezca en `control_landing/confirmados/`. Copien el `.csv.zst` original directamente a `landing/inbox/`. El archivo del 29 de junio de 2026 ya está confirmado y sirve como evidencia de una corrida terminada. [Rutas del volumen](directorio_recursos.md#volumen-y-carpetas).
2. En el detector, abran la próxima corrida programada y muestren la salida `pendientes`. Cuando sea positiva, abran la corrida hija desde la tarea `procesar`. La corrida del detector es `PERIODIC` y la de Entrega 2 es `RUN_JOB_TASK`. [Ejemplo del detector](https://dbc-b38f0852-ecf9.cloud.databricks.com/jobs/342422518655466/runs/789140629150470).
3. En la tarea de ingesta, muestren la fecha derivada del nombre, el manifiesto y el ID del stream. Distingan el marcador por archivo, el checkpoint que controla la reanudación de Auto Loader y la deduplicación por `(MMSI, BaseDateTime)` en plata.
4. Si la corrida nueva continúa durante la exposición, usen la corrida completa del 29 de junio para recorrer los resultados. La prueba tomó **15 min 24 s** desde que se creó la corrida hija. El intervalo del detector mide la espera hasta revisar la carpeta. El arranque y el procesamiento tienen tiempos propios. [Corrida completa](https://dbc-b38f0852-ecf9.cloud.databricks.com/jobs/83886562675136/runs/796009776834361).

El detector permanece en **30 minutos**. Cuando el equipo indique que empieza la sustentación, puede usarse el intervalo temporal de **2 minutos** ya probado. Desde la raíz del repo y con el perfil institucional autenticado, la orden es

```powershell
databricks bundle deploy -p oceanwatch --var "cron_detector=0 0/2 * * * ?"
```

El despliegue con los valores por defecto vuelve a dejar la revisión en 30 minutos al finalizar la demostración. **File arrival** permanece pausado por los permisos de eventos de AWS.

## Recorrido técnico

| Qué mostrar | Qué explicar |
|---|---|
| **Parte 1 → Parte 2** | El esquema AIS, el diagnóstico y la grilla de la Parte 1 se reutilizan. La Parte 2 automatiza la incorporación, aplica quality gates, genera oro y sirve un tablero. [Arquitectura](architecture/medallion-overview.md). |
| **Fallo de commit** | Bronce conservó 2 filas durante el fallo, llegó a 6 al reanudar con el mismo checkpoint y leyó 0 filas al repetir. [Corrida](https://dbc-b38f0852-ecf9.cloud.databricks.com/jobs/719836273900213/runs/518309915499507). |
| **Calidad y conciliación** | Comparen bronce, plata, cuarentena y duplicados del mismo día. El sandbox conserva la SOG negativa original en cuarentena. Expliquen cuándo se bloquean oro y ML. [Reglas y conteos](data_quality_rules.md). |
| **Consultas y Spark** | Señalen `PartitionFilters`, los `BroadcastHashJoin` de WPI y los exchanges de ventana y conteo exacto. Con el mismo conteo, la prueba de poda tocó 1 archivo en vez de 3, un 66,1% menos de huella en bytes. El clúster de un nodo permite verificar el plan y la poda. Una medición de red distribuida requiere workers remotos. [Planes y medición](../evidence/institucional/#planes-spark). |
| **Registro e inferencia** | Abran el experimento, el alias `champion` y la firma. Expliquen train/validación temporal, fallback de velocidad, línea base por z-score y modelo. Identifiquen `modelo_version` y `modelo_run_id` en oro. [Modelo y experimento](directorio_recursos.md#tablas-y-modelo). |
| **Consola** | Seleccionen **Última fecha** y luego `2023-06-07`. Muestren ambos mapas, ranking y tablas. Distingan pings, buques distintos, buque-horas e intervalo de presencia observado. [Contrato de oro](data_contract_gold.md). |
| **Gobernanza** | Abran el linaje de plata, el contrato y los grants. Distingan el grupo que puede operar Jobs de los permisos sobre el tablero. Los esquemas con segregación efectiva por rol requieren intervención de la administración académica. [Evidencia de gobernanza](../evidence/institucional/gobernanza.md). |

Después de revisar el acceso de la persona evaluadora, usen el repositorio como entrada a las evidencias. La entrega formal se realiza en **Bloque Neón**. [Estado de entrega](autoevaluacion_rubrica.md).
