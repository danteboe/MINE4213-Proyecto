# Guía para la sustentación

1. Muestre [el DAG institucional](https://dbc-b38f0852-ecf9.cloud.databricks.com/jobs/83886562675136) y el parámetro de fecha. Identifique el clúster clásico y explique por qué el calendario de replay y el huso de los registros son decisiones distintas.
2. Abra la evidencia de streaming. Explique qué guarda el checkpoint, qué garantiza el sink Delta y por qué se deduplica aguas abajo por MMSI y timestamp. Distinga la repetición de un archivo de la repetición de una posición dentro de un archivo.
3. Compare bronce, cuarentena, métricas y plata para una misma fecha. Explique qué ocurre si dos pings de la misma clave tienen distinta calidad. Muestre qué condición impide publicar y cómo se conserva el original.
4. Use los planes y la comparación de poda. Identifique la redistribución necesaria para la ventana por buque y para agregados de grilla. Explique por qué WPI se difunde y cómo la máscara evita una copia por ejecutor. La evidencia de un nodo mide poda y el plan, no la latencia de una red distribuida.
5. Abra MLflow y la versión UC. Muestre la separación temporal, el fallback de tipo, la línea base y las variables. Explique qué mide la fracción de alertas cuando no hay etiquetas y cómo recuperar la versión desde una fila de oro.
6. Cambie el filtro de fecha del [tablero](https://dbc-b38f0852-ecf9.cloud.databricks.com/dashboardsv3/01f1e5507d1b18a5963d1a49e4329629/published). Diferencie pings, buques distintos, buque-horas y duración observada de presencia. Identifique los permisos reales del entorno académico.

Material de apoyo en [arquitectura](architecture/medallion-overview.md), [contrato](data_contract_gold.md) y [evidencia institucional](../evidence/institucional/).
