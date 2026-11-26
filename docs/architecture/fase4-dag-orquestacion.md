# DAG de la Entrega 2

```mermaid
flowchart LR
    V[Consulta programada en warehouse] --> U{Entradas pendientes}
    U -->|Sí, Run Job| A
    U -->|Cero| N[Termina la revisión]
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

## Parámetros de operación

| Parámetro | Valor automático | Uso explícito |
|---|---|---|
| `fecha_proceso` | Vacío. Ingesta deriva las fechas del nombre de los archivos pendientes. | Último día del backfill en formato `AAAA-MM-DD`. |
| `fecha_inicio` | Vacío. | Primer día del backfill inclusivo. Admite hasta 31 días por intervalo explícito. |
| `entrenar` | `false`. Inferencia con una versión resuelta desde `champion`. | `true` añade train y validación al trabajo de datos y registra un nuevo modelo al terminar el entrenamiento. |
| `ruta_entrada` | `/Volumes/clase10/equipo9/checkpoints/oceanwatch_equipo9/landing/inbox/`, resuelta desde el bundle. | Se configura al desplegar, junto con la ruta de lectura del detector. |

Los reintentos reutilizan el staging y el checkpoint. La auditoría conserva la última salida de la tarea por corrida y Lakeflow conserva cada intento. Una entrega se vuelve a considerar en el detector hasta que la etapa final deja el marcador. El [directorio](../directorio_recursos.md#volumen-y-carpetas) ubica el manifiesto, el checkpoint y las confirmaciones.
