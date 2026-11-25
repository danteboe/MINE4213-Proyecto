# ADR-022 · Incorporación de archivos AIS desde un volumen

**Estado** Adoptado · 2026-11-25. La ejecución del DAG desde el volumen está verificada. El trigger nativo está pausado por permisos de eventos en AWS.

## Contexto y objetivo

El cron anterior estaba pausado y el DAG usaba una fecha predeterminada. Auto Loader empezaba a leer después de que el Job se ejecutaba. El requisito nuevo es que una entrega inmutable de NOAA pueda incorporarse con solo subirla y conservar la capacidad de backfill explícito.

## Alternativas

| Opción | Efecto |
|---|---|
| Cron en el clúster clásico | Detectaría archivos en el siguiente intervalo. Arrancaría el clúster aunque no hubiera archivos para procesar. |
| Structured Streaming continuo | Detectaría cambios en el CSV normalizado. Exigiría cómputo persistente y coordinación separada para descomprimir ZIP y completar el DAG hasta ML. |
| File arrival sobre una bandeja y Auto Loader con `availableNow` | Lakeflow inicia el clúster cuando detecta una llegada. Spark consume los CSV nuevos con checkpoint y termina. |

## Decisión

- Reutilizar `clase10.equipo9.checkpoints` y vigilar solo `landing/inbox/`. El staging de CSV, los manifiestos y el checkpoint quedan fuera del prefijo. Así las escrituras internas carecen de efecto sobre el disparador.
- Usar el nombre AIS como identidad y fuente de fecha UTC. Un sufijo admite cargas adicionales del mismo día. Validar nombre, encabezado y CRC al publicar un CSV. Comparar tamaño y SHA-256 si el destino ya existe. El ZIP se descomprime secuencialmente en disco temporal y Spark lee el CSV en paralelo. El driver manipula metadatos y archivos individuales, sin materializar sus registros en memoria.
- Guardar un manifiesto por corrida después de publicar los CSV completos. Todas las etapas usan las fechas del mismo manifiesto. La cola y la concurrencia de una corrida aseguran un escritor del checkpoint. Auto Loader descarta archivos ya consumidos y los productos se escriben por fecha. Los archivos se confirman solo tras oro e inferencia. Si queda una entrega sin confirmar, una nueva corrida puede completar sus productos reusando lo ya escrito.
- Conservar las descargas explícitas de NOAA para backfill y el mismo checkpoint de Auto Loader. Las corridas que provienen solo de un archivo reutilizan el campeón para inferencia. Spark lee solo las fechas de trabajo mediante filtros de partición. El cambio de trigger no introduce joins ni shuffles sobre pings.
- Configurar 60 segundos de espera tras el último cambio y 60 segundos mínimos entre disparos. Con el trigger pausado, la alternativa operativa es `tools/subir_ais.ps1`, que solicita una corrida solo al finalizar la copia, o **Run now** después de subir desde Catalog.

## Consecuencias y verificación

La prueba sobre el volumen administrado falló en la evaluación del trigger antes de arrancar cómputo, con HTTP 403 al aprovisionar `sns:CreateTopic`. La cuenta del proyecto tiene acceso al volumen y carece de administración sobre la ubicación de almacenamiento. El administrador debe habilitar los permisos de eventos del rol AWS y entonces se puede reactivar la definición desplegada. Se deja en pausa para evitar reintentos del trigger sin efecto. [Prueba institucional](../../evidence/institucional/llegada_archivos.md) · [Runbook](../runbook.md) · [Documentación de eventos](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/file-events-faq).

El alcance es el de las entregas del curso. La selección lista los nombres de la bandeja y las confirmaciones una vez por corrida. Con una retención larga o una frecuencia alta, conviene medir el costo del listado y considerar una tabla de control incremental. Una modificación de un archivo confirmado requiere un procedimiento de corrección de datos, porque tanto su nombre como el CSV publicado representan una entrega inmutable.
