# ADR-020 · Procesamiento diario, compuertas y plan físico

**Estado** Aceptado · 2026-11-20

## Decisiones operativas

1. Cada fecha tiene un CSV inmutable. La descarga por bloques comprueba longitud cuando la fuente la expone. La extracción valida el CRC del ZIP y publica el CSV completo con un nombre estable. El ZIP vive en almacenamiento temporal del driver durante la extracción. Spark lee el CSV divisible en particiones.
2. Auto Loader descubre archivos nuevos y drena su snapshot con `availableNow`. Esquema AIS explícito, `_rescued_data` para diferencias de esquema y sink Delta de streaming directo. El checkpoint y el log Delta controlan reanudación y repetición sin una función `foreachBatch`. El límite de un Job activo protege este contrato. [Auto Loader con Unity Catalog](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/unity-catalog).
3. Plata aplica controles al día del archivo. Los timestamps de otra fecha se aíslan, lo cual hace posible deduplicar por `(MMSI, BaseDateTime)` sin estado de watermark ni lectura sobre otros días. La deduplicación ocurre tras validar el mensaje y registra la multiplicidad de cada clave. Una posición inválida se conserva en cuarentena aunque exista otra posición válida con la misma clave.
4. La compuerta admite evidencia de agua en la máscara costera, proximidad a la red navegable NWN o a un lago de Natural Earth, o coincidencia con una celda del World Port Index. La tolerancia es de 0,02° sobre la grilla WGS84 de 0,01°. El primer ensayo institucional con ráster, tolerancia y WPI rechazó 3.493.229 de 8.808.904 pings del 1 de junio. La inspección de las celdas más frecuentes reveló canales navegables, incluidos tramos de Houston. Se añadió hidrografía antes de entrenar. La tasa de rechazo cuantifica el efecto de la regla. Medir la precisión requiere etiquetas o cartas de navegación de referencia. [Global Land Mask](https://github.com/toddkarin/global-land-mask), [NWN de BTS](https://rosap.ntl.bts.gov/view/dot/56264) y [Natural Earth](https://www.naturalearthdata.com/downloads/10m-physical-vectors/).
5. El reemplazo por predicado sustituye una fecha en una transacción Delta, incluida una salida vacía. Evita una ventana de ausencia entre DELETE y INSERT y conserva fechas ajenas al trabajo. La compuerta coteja hechos escritos antes de habilitar oro. La transacción agrupa las filas de cada tabla. El DAG y el reintento recuperan publicaciones parciales de varias tablas. [Sobrescritura selectiva de Delta](https://docs.databricks.com/aws/en/delta/selective-overwrite).

## Exchanges y almacenamiento

- Evitamos agregaciones globales a lo largo del DAG. El `_batch_date` permite poda en tablas de hechos. La elección favorece lecturas y reconstrucción por fecha como en la Parte 1. Oro y cuarentena usan Delta sin partición física específica porque sus volúmenes diarios son inferiores. Solo se justifica pagar un layout espacial adicional si se mide una mejora en consultas reales. [Diseño de particiones Delta](https://docs.databricks.com/aws/en/tables/partitions).
- La máscara importa un ráster y un índice `STRtree` en el driver una vez por sesión. Solo las celdas sin agua en el ráster consultan su geometría más cercana dentro de la tolerancia. Clasificar una tabla de celdas distintas evita carga por proceso Python, UDF por posición y joins espaciales entre hechos y polígonos. Existe un tope explícito de 750.000 celdas. La difusión de la dimensión elimina el exchange de las posiciones en el join. Si crece el dominio, precomputar Delta por mosaicos reducirá el límite de memoria en el driver. El WPI y las estadísticas por tipo también se difunden por su cardinalidad acotada.
- Los conteos de reglas usan un solo agregado condicional por fecha. El agrupamiento por clave para deduplicar y el orden de las ventanas de trayectorias/ML son redistribuciones necesarias. La proyección temprana, el broadcast, el reúso temporal de DataFrames y AQE limitan los datos que pasan por ellas. La matriz exacta de buques requiere agrupar por celda, hora y MMSI. Particionar manualmente antes incrementaría intercambios sin reutilización demostrada. [Spark SQL Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html).
- El Job de verificación compara dos consultas equivalentes para zona y día. Se informa el filtro de partición en el plan, archivos que aportan filas y suma de tamaños de los objetos. La proyección de columnas permite al lector Parquet procesar una fracción de esa huella de archivos.

Actualiza ADR-004/006/009/010/011/012/014 y reemplaza las variantes CTAS sobre el histórico en el flujo institucional.

## Reproducir la referencia hidrográfica

Descargue el [archivo NWN de NTAD](https://rosap.ntl.bts.gov/view/dot/56264/dot_56264_DS1.zip) y [Natural Earth 10m Lakes](https://naturalearth.s3.amazonaws.com/10m_physical/ne_10m_lakes.zip). El script usa las líneas de la instantánea del 18 de enero de 2023 y los polígonos de lagos v5.0.0 en WGS84. Corrige geometrías inválidas, simplifica a 0,0005° (hasta unos 56 m en el eje de latitud) y separa multipartes. El bundle incluye 8.232 geometrías y 3,26 MB comprimidos.

```sh
uv run --no-project --with pyogrio --with shapely python tools/preparar_hidrografia.py nwn.zip lakes.zip resources/hidrografia.wkb.gz
```
