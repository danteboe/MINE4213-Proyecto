# DAG de la Entrega 2

```mermaid
flowchart LR
    V[Detector de archivos pendientes] --> A
    A[preparar] --> B[ingesta]
    B --> C[plata_calidad]
    C --> D{conciliacion}
    D -->|Éxito| E[oro]
    E --> F[ml_anomalias]
    D -->|Fallo| X[Revisar auditoría y reparar]
```

| Tarea | Unidad de trabajo | Reintentos |
|---|---|---|
| `preparar` | Crear bronce, WPI y auditoría con permisos del grupo | 2 |
| `ingesta` | Publicar CSV completos y drenar archivos nuevos con el checkpoint persistente | 2 |
| `plata_calidad` | Una fecha a la vez, cuarentena original y conteos por regla | 1 |
| `conciliacion` | Verificar bronce = plata + cuarentena + duplicados válidos para cada fecha | 0 |
| `oro` | Reemplazar la fecha en trayectorias, presencia y dos matrices | 1 |
| `ml_anomalias` | Entrenar y registrar cuando se solicita, luego puntuar el día con una versión fija | 1 |

El Job admite una corrida activa. Lakeflow guarda las solicitudes adicionales en cola y evita dos escritores simultáneos sobre el checkpoint. Cada tarea usa la identidad propietaria de la corrida y se vincula por `existing_cluster_id`. El grupo tiene capacidad de solicitar y seguir corridas. La auditoría usa una fila por corrida y tarea, por lo que un reintento actualiza el resultado previo y los intentos individuales permanecen en Lakeflow.

El detector es un Job aparte que revisa metadatos en el warehouse SQL a las horas y medias horas en UTC y llama al DAG solo si hay archivos pendientes. La definición de File arrival permanece pausada hasta que el rol institucional pueda aprovisionar eventos de AWS. Con `fecha_proceso` vacía, ingesta extrae las fechas de los nombres, normaliza los CSV2 Zstandard y persiste un manifiesto por corrida. Plata, conciliación, oro e inferencia leen ese mismo manifiesto. Las confirmaciones se escriben tras inferencia. Un operador también puede pasar `fecha_proceso` para descargar y procesar una fecha explícita de NOAA. El Job de verificación usa un espacio aislado para inyectar fallos.

Definiciones en [DAB del pipeline](../../resources/jobs/oceanwatch_pipeline.job.yml), [ADR-022](../adr/ADR-022-llegada-de-archivos.md) y [runbook](../runbook.md).
