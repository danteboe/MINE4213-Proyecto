# Contrato de datos de oro

Todas las tablas físicas están en `clase14.equipo9` y empiezan con `oceanwatch_gold_`. Las fechas de evento corresponden a UTC. Los agregados se publican solo después de la conciliación diaria. Unity Catalog contiene un comentario para cada tabla.

| Tabla (sufijo) | Grano lógico | Medidas y atributos |
|---|---|---|
| `trayectorias_diarias` | `MMSI, dia` | `n_posiciones`, distancia great-circle en km entre pings consecutivos, inicio/fin, duración en horas y extremos lat/lon. Reinicia la trayectoria en cada cambio de día. |
| `presencia_puertos` | `MMSI, INDEX_NO, dia` | Nombre, país, tipo y tamaño del puerto y recuento de pings admitidos. `primera_posicion` y `ultima_posicion` delimitan el intervalo observado. Se requieren ≥3 pings en la misma celda del puerto. |
| `matriz_trafico` | `celda_lat, celda_lon, hora` | `n_posiciones` y `n_buques` exacto dentro de la hora. `dia` se conserva como filtro. La grilla redondea a 0,01°. |
| `matriz_trafico_diaria` | `celda_lat, celda_lon, dia` | `intensidad` suma pings horarios. `buques` es la suma de buques distintos por hora y se muestra como buque-horas. |
| `anomalias_velocidad` | `MMSI, BaseDateTime` | LAT/LON, SOG en nudos, tipo, z-score, `score_anomalia` de `decision_function`, bandera, razón, `_batch_date`, versión UC y run MLflow. Una puntuación más negativa es más anómala. Contiene las filas con score menor que cero. |
| `world_port_index` | `INDEX_NO` | Dimensión pública NGA con 3.669 puertos, coordenadas y atributos básicos. |

La proximidad por celda puede producir más de una coincidencia si dos puertos comparten celda. El contrato de presencia es por puerto y por ello conserva ambas coincidencias. Los intervalos entre primer y último ping incluyen posibles interrupciones y no son tiempos continuos de atraque. El mapa del tablero recorta a las 2.000 celdas más intensas para mantener una respuesta interactiva. El ranking cuenta buques distintos por puerto y muestra posiciones AIS como métrica auxiliar.

El `timestamp` de Spark se interpreta en UTC durante procesamiento y escritura. Los nombres AIS mayúsculos se conservan por continuidad con la Parte 1. La tabla de anomalías admite días sin filas, y las fechas de entrenamiento quedan fuera del dominio de puntuación para evitar fuga temporal.
