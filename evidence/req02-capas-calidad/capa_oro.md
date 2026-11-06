# Materialización de la capa ORO — conteos reales (FEAT-004, Req. 2)

Estado: **Verificado (ejecutado)** en Databricks serverless el 2026-11-06.

Este documento recoge la salida LITERAL de la materialización de la capa oro (modelo
dimensional de negocio, ADR-012), alimentada **SOLO** de `oceanwatch.silver.ais_clean`
(25.857.101 filas; el oro NO limpia). Se construyen una tabla de referencia (World
Port Index) y tres tablas de hechos con granularidad y clave EXPLÍCITAS.

## Entorno y camino de ejecución

- Catálogo/esquema: `oceanwatch.gold` (Unity Catalog).
- Cómputo: **serverless**. SQL warehouse serverless id `377600528fed7c47`.
- Sentencias CTAS: `resources/sql/fase3_materializar_oro.sql`, ejecutadas vía SQL
  Statement Execution API (`/api/2.0/sql/statements/`) con el helper `_dbx_sql.py`.
- Las cuatro tablas resultado se **conservan** (no son sandboxes efímeros).
- Origen (linaje): todas las tablas de hechos leen de `oceanwatch.silver.ais_clean`;
  `presencia_puertos` cruza además con `oceanwatch.gold.world_port_index`. **Ninguna**
  lee de `oceanwatch.bronze.ais_raw` (verificable en el `FROM` de cada CTAS).

### Camino SQL (misma decisión que FEAT-003)

El notebook `notebooks/fase3_oro.py` orquesta el MISMO pipeline con la API de Spark
(`gold.trayectorias` / `gold.puertos` / `gold.matriz_trafico` -> escritura Delta).
Siguiendo la fricción serverless documentada en FEAT-003 (el cómputo serverless Spark
Connect GC devuelve `INTERNAL_ERROR` en `saveAsTable` sobre 25.9M filas, ver
`materializacion_plata.md`), el camino de ejecución REAL es el SQL CTAS en el SQL
warehouse serverless, que reproduce EXACTAMENTE la lógica de `src/oceanwatch/gold/`.
El código pesado vive en `src/`; el notebook y el SQL son dos expresiones del mismo
pipeline (el notebook es el orquestador de referencia, el SQL el camino ejecutado).

### Carga del World Port Index (ADR-012)

`geopandas`/`pyshp` NO vienen preinstaladas en serverless (verificado en
`disponibilidad_librerias_serverless.md`). Por eso el WPI (`world_port_index/WPI.dbf`,
64.8 MB, 78 campos, 3.669 puertos) se convirtió a CSV **localmente** con `dbfread`
(lector puro de `.dbf` en Python, sin dependencias nativas), quedándose con los 7
campos clave (INDEX_NO, PORT_NAME, COUNTRY, LATITUDE, LONGITUDE, HARBORSIZE,
HARBORTYPE). El CSV (166.826 bytes, 3.669 filas) se subió al Volume
`oceanwatch.bronze.landing` (`.../wpi_ref_v2/wpi_ref.csv`) y se cargó con
`read_files(...)` a `oceanwatch.gold.world_port_index`.

> Nota operativa (síntoma -> causa -> solución): la primera subida quedó bajo
> `.../wpi/_wpi_ref.csv`, pero un `read_files` lanzado ANTES de completarse la subida
> envenenó la caché de listado de archivos del warehouse (devolvía 0 filas pese a que
> el archivo existía íntegro, verificado por round-trip de 166.826 bytes / 3.670
> líneas). Solución: subir a un path NUEVO nunca consultado (`wpi_ref_v2/`), que
> `read_files` leyó correctamente (3.669 filas).

## Conteos reales (salida literal, SQL warehouse `377600528fed7c47`)

```
oceanwatch.gold.world_port_index : n_wpi=3669   idx_distintos=3669
oceanwatch.gold.trayectorias_diarias : n=61768  buques(MMSI)=26266  dias=3
oceanwatch.gold.presencia_puertos : n=9153      puertos_cruzados(INDEX_NO)=349  buques=4592
oceanwatch.gold.matriz_trafico : n=2319365      celdas_distintas=726513  horas=72
```

- `world_port_index` = **3.669** puertos (coincide con el .dbf: 3.669 registros).
- `trayectorias_diarias` = **61.768** filas = 1 por (MMSI, día); 26.266 buques × 3 días
  (no todos los buques reportan los 3 días).
- `presencia_puertos` = **9.153** filas = 1 por (MMSI, puerto, día); **349** puertos del
  WPI fueron cruzados por tráfico AIS con permanencia ≥ 3 posiciones; 4.592 buques.
- `matriz_trafico` = **2.319.365** filas = 1 por (celda_lat, celda_lon, hora); 726.513
  celdas de grilla distintas a lo largo de 72 horas (3 días × 24 h, UTC).

### Muestras reales

Trayectorias (top distancia total; ver nota de calidad abajo):
```
MMSI       dia         n_pos  dist_km       horas  lat_min   lat_max
338561000  2023-06-02  812    1610450.42    20.43  18.38466  43.79516
368124160  2023-06-02  575    1181547.07    23.96  26.08719  33.72429
368124160  2023-06-01  568    1066001.66    23.96  26.08719  33.72429
```

Presencia en puertos (top posiciones en puerto):
```
MMSI       INDEX_NO  PORT_NAME       COUNTRY  dia         n_posiciones_en_puerto
367331730  16840     WARRENTON       US       2023-06-02  1347
367155110  16840     WARRENTON       US       2023-06-01  1320
367422020  16370     POINT RICHMOND  US       2023-06-02  1252
```

Puertos con más presencias (buque·día):
```
NEW YORK CITY (US)    260
GRETNA (US)           260
ANNAPOLIS (US)        222
GALVESTON (US)        217
PORT EVERGLADES (US)  206
```

Matriz de tráfico (celdas·hora más densas; 47.63/-122.39 = Puget Sound/Seattle):
```
celda_lat  celda_lon  hora                      n_posiciones  n_buques
47.63      -122.39    2023-06-01T18:00:00Z      1791          96
47.63      -122.39    2023-06-01T17:00:00Z      1785          96
47.63      -122.39    2023-06-01T03:00:00Z      1766          93
```

## Granularidad y clave de cada tabla

| Tabla | Granularidad | Clave lógica | Origen |
|---|---|---|---|
| `gold.world_port_index` | 1 fila por puerto | `INDEX_NO` | WPI (CSV local) |
| `gold.trayectorias_diarias` | 1 fila por buque y día | `(MMSI, dia)` | `silver.ais_clean` |
| `gold.presencia_puertos` | 1 fila por buque, puerto y día | `(MMSI, INDEX_NO, dia)` | `silver.ais_clean` ⋈ `gold.world_port_index` |
| `gold.matriz_trafico` | 1 fila por celda y hora | `(celda_lat, celda_lon, hora)` | `silver.ais_clean` |

## Definición geoespacial de 'en puerto' y permanencia (ADR-012)

- **'En puerto'**: una posición AIS está 'en puerto' si su celda de grilla (lat/lon
  redondeadas a **2 decimales**, ADR-001) coincide con la celda de algún puerto del
  WPI (patrón *Reference Data Join* por celda). Radio efectivo < ~1.6 km del puerto.
- **Permanencia mínima**: un buque tiene 'presencia' en un puerto un día si acumula
  **≥ 3** posiciones en esa celda-puerto ese día (parámetro
  `gold.presencia_puertos_param.permanencia_minima_posiciones`), lo que descarta
  tránsitos de 1-2 pings.
- **Haversine**: `trayectorias_diarias` suma la distancia great-circle (radio 6371 km)
  entre posiciones consecutivas (`LAG` por `(MMSI, dia)` ordenado por `BaseDateTime`).

## Nota de calidad heredada (límite conocido)

La capa plata garantiza coordenadas **dentro de rango** físico, NO continuidad de
trayectoria. Hay buques cuyas posiciones AIS "saltan" entre puntos lejanos el mismo
día (posiciones erróneas que pasan la compuerta de rango). La suma haversine es
aritméticamente correcta sobre esas entradas, por lo que `distancia_total_km` puede
ser no física para esos MMSI (p.ej. 1.6M km en 20 h). No se corrige en oro (el oro NO
limpia): se documenta como límite heredado de plata. Un filtro de velocidad entre
posiciones sería una regla de calidad adicional de plata (candidata a Fase futura),
no una tarea de la capa oro.

## Reproducibilidad

1. Convertir el WPI a CSV local: `dbfread` sobre `world_port_index/WPI.dbf` (script
   temporal fuera del repo), 7 campos clave, 3.669 filas.
2. Subir el CSV a `/Volumes/oceanwatch/bronze/landing/wpi_ref_v2/wpi_ref.csv` con la
   CLI (`databricks fs cp`).
3. Ejecutar `resources/sql/fase3_materializar_oro.sql` en el SQL warehouse serverless
   `377600528fed7c47` (crea el WPI de referencia y las 3 tablas de hechos).
4. Leer conteos con `_dbx_sql.py` (los de este documento).

Equivalente con la API de Spark: importar `src/oceanwatch` y `notebooks/fase3_oro.py`
al workspace y lanzar un one-time run serverless (`jobs submit`, notebook_task sin
`new_cluster`). Sujeto a la fricción `INTERNAL_ERROR` de FEAT-003 a 25.9M filas.
