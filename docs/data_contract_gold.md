# Contrato de datos de oro

Todas las tablas físicas están en `clase14.equipo9` y empiezan con `oceanwatch_gold_`. Las fechas de evento corresponden a UTC. Los agregados se publican solo después de la conciliación diaria. Unity Catalog contiene un comentario para cada tabla.

| Tabla (sufijo) | Grano lógico | Medidas y atributos |
|---|---|---|
| `trayectorias_diarias` | `MMSI, dia` | `n_posiciones`, `distancia_total_km` por Haversine, `primera_posicion`, `ultima_posicion`, `duracion_horas`, `lat_min`, `lat_max`, `lon_min` y `lon_max`. Reinicia la trayectoria en cada cambio de día. |
| `presencia_puertos` | `MMSI, INDEX_NO, dia` | `PORT_NAME`, `COUNTRY`, `HARBORSIZE`, `HARBORTYPE`, `n_posiciones_en_puerto`, `primera_posicion` y `ultima_posicion`. Se requieren ≥3 pings en la misma celda del puerto. |
| `matriz_trafico` | `celda_lat, celda_lon, hora` | `n_posiciones` y `n_buques` exacto dentro de la hora. `dia` se conserva como filtro. La grilla redondea a 0,01°. |
| `matriz_trafico_diaria` | `celda_lat, celda_lon, dia` | `intensidad` suma pings horarios. `buques` es la suma de buques distintos por hora y se muestra como buque-horas. |
| `anomalias_velocidad` | `MMSI, BaseDateTime` | `LAT`, `LON`, `SOG` en nudos, `VesselType`, `z_sog`, `score_anomalia`, `es_anomalia`, `razon`, `_batch_date`, `modelo_version` y `modelo_run_id`. Una puntuación más negativa de `decision_function` es más anómala. Contiene las filas con score menor que cero. |
| `world_port_index` | `INDEX_NO` | Dimensión pública NGA con 3.669 puertos, coordenadas y atributos básicos. |

La proximidad por celda puede producir más de una coincidencia si dos puertos comparten celda. El contrato por puerto conserva ambas coincidencias. Los intervalos entre el primer y último ping pueden incluir salidas o interrupciones de cobertura. La distancia mide segmentos entre posiciones observadas dentro del día y depende de la frecuencia de reporte. Las matrices preservan los pings admitidos, y un buque puede contribuir a varias horas o celdas.

El `timestamp` de Spark se interpreta en UTC durante procesamiento y escritura. Los nombres AIS mayúsculos se conservan por continuidad con la Parte 1. La tabla de anomalías admite días sin filas, y las fechas de entrenamiento quedan fuera del dominio de puntuación para evitar fuga temporal.

## Consumo en el tablero

La consola usa cuatro datasets de oro con un parámetro de fecha compartido. **Última fecha** se resuelve con el máximo día de trayectorias. Al introducir una fecha UTC se aplica el mismo día al mapa, al ranking y a las dos tablas. Consulte los resultados tras el `SUCCESS` del Job porque cada tabla se publica en su propia transacción.

| Dataset | Salida interactiva |
|---|---|
| Matriz diaria | Mapa de las **2.000 celdas** con más posiciones. El tamaño del símbolo representa la intensidad. |
| Ranking | **15 puertos** ordenados en la consulta por buques distintos. Incluye el conteo de posiciones como medida auxiliar. |
| Buques en puerto | Hasta **500 filas** de presencia ordenadas por cantidad de pings. |
| Anomalías | Hasta **500 pings** de acuerdo con el score. Alimenta una tabla y un mapa. |

Los límites acotan la transferencia y el render. Las tablas Delta conservan los resultados completos. La revisión publicada ejecuta con credenciales del lector para aplicar permisos de Unity Catalog en cada consulta. [Definiciones SQL y widgets](../resources/dashboards/oceanwatch_oro.lvdash.json) · [Validación](../evidence/institucional/llegada_archivos.md#consola-publicada) · [Directorio](directorio_recursos.md).
