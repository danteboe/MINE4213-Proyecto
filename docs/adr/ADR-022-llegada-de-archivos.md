# ADR-022 · Incorporación de archivos AIS desde un volumen

**Estado** Adoptado. El detector periódico inicia las corridas en el entorno institucional. El trigger nativo está pausado por permisos de eventos en AWS.

## Contexto y objetivo

El cron anterior estaba pausado y el DAG usaba una fecha predeterminada. Auto Loader empezaba a leer después de que el Job se ejecutaba. El requisito nuevo es que una entrega inmutable de NOAA pueda incorporarse con solo subirla y conservar la capacidad de backfill explícito.

## Alternativas

| Opción | Efecto |
|---|---|
| Cron en el clúster clásico | Detectaría archivos en el siguiente intervalo. Arrancaría el clúster aunque no hubiera archivos para procesar. |
| Structured Streaming continuo | Detectaría cambios en el CSV normalizado. Exigiría cómputo persistente y coordinación separada para descomprimir ZIP y completar el DAG hasta ML. |
| File arrival sobre una bandeja y Auto Loader con `availableNow` | Lakeflow inicia el clúster cuando detecta una llegada. Spark consume los CSV nuevos con checkpoint y termina. |
| Detección periódica de metadatos y tarea Run Job | El warehouse existente compara nombres de entradas y confirmaciones. La condición abre el DAG clásico solo si hay una entrega pendiente. La latencia depende del intervalo. |

## Decisión

- Reutilizar `clase10.equipo9.checkpoints` y vigilar solo `landing/inbox/`. El staging de CSV, los manifiestos y el checkpoint quedan fuera del prefijo. Así las escrituras internas carecen de efecto sobre el disparador.
- Usar el nombre AIS como identidad y fuente de fecha UTC. Un sufijo admite cargas adicionales del mismo día. Validar nombre, encabezado y CRC al publicar un CSV. Comparar tamaño y SHA-256 si el destino ya existe. El ZIP se descomprime secuencialmente en disco temporal y Spark lee el CSV en paralelo. El driver manipula metadatos y archivos individuales, sin materializar sus registros en memoria.
- Guardar un manifiesto por corrida después de publicar los CSV completos. Todas las etapas usan las fechas del mismo manifiesto. La cola y la concurrencia de una corrida aseguran un escritor del checkpoint. Auto Loader descarta archivos ya consumidos y los productos se escriben por fecha. Los archivos se confirman solo tras oro e inferencia. Si queda una entrega sin confirmar, una nueva corrida puede completar sus productos reusando lo ya escrito.
- Conservar las descargas explícitas de NOAA para backfill y el mismo checkpoint de Auto Loader. Las corridas que provienen solo de un archivo reutilizan el campeón para inferencia. Spark lee solo las fechas de trabajo mediante filtros de partición. El cambio de trigger no introduce joins ni shuffles sobre pings.
- Aceptar el nombre oficial CSV2 `ais-AAAA-MM-DD.csv.zst`. Leer Zstandard en disco temporal por flujo, proyectar las columnas por nombre a la posición exigida por bronce y escribir UTC con separador `T`. La normalización mantiene un registro en memoria y valida que no falten campos. Se conserva el nombre base para distinguir la edición CSV2 de la edición original y el CSV resultante permite lecturas Spark por rangos. Esta traducción secuencial añade I/O por archivo, sin un shuffle previo a bronce. Ambos formatos reutilizan el mismo checkpoint, contrato y validación de fechas.
- Mantener la definición de File arrival con 60 segundos de espera tras el último cambio y 60 segundos mínimos entre disparos. Ante el bloqueo del rol AWS, ejecutar el detector cada 30 minutos en el warehouse SQL institucional. La consulta proyecta solo la ruta que devuelve `binaryFile`. Se declara el esquema para que las carpetas vacías también devuelvan cero filas. Se deshabilita la caché de resultados y se compara con las confirmaciones fuera de la bandeja. El Job detector permite una corrida a la vez y omite ticks mientras tiene un hijo activo. El DAG Spark, la publicación y ML siguen en el clúster personal.

## Consecuencias y verificación

La prueba sobre el volumen administrado falló en la evaluación del trigger antes de arrancar cómputo, con HTTP 403 al aprovisionar `sns:CreateTopic`. La cuenta del proyecto tiene acceso al volumen y carece de administración sobre la ubicación de almacenamiento. El administrador debe habilitar los permisos de eventos del rol AWS y entonces se puede reactivar la definición desplegada. Se deja en pausa para evitar reintentos del trigger sin efecto. [Prueba institucional](../../evidence/institucional/llegada_archivos.md) · [Runbook](../runbook.md) · [Documentación de eventos](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/file-events-faq).

Las revisiones programadas consumen el warehouse aunque la bandeja esté al día. La separación de Jobs evita arranques periódicos del clúster personal sin trabajo y permite la pausa independiente del detector. La proyección excluye la columna `content`. Con retención larga, el listado sigue creciendo con el número de archivos. Una modificación de un archivo confirmado requiere un procedimiento de corrección porque tanto su nombre como el CSV publicado representan una entrega inmutable. [Consulta del detector](../../resources/jobs/detectar_entradas.sql) · [CSV2 de NOAA 2026](https://www.fisheries.noaa.gov/inport/item/80362).
