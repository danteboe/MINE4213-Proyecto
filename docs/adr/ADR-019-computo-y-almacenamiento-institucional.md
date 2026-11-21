# ADR-019 · Cómputo y almacenamiento institucionales

**Estado** Aceptado · 2026-11-20

## Contexto

La Entrega 2 debe correr en el workspace institucional. El prototipo de Free Edition resolvía todas las tareas con cómputo serverless, archivos importados en una ruta personal y objetos en `oceanwatch`. El clúster dedicado disponible es `1114-165401-a0ye3pxs`, DBR 19.x, `i3.xlarge`, sin workers remotos y con apagado por inactividad a los 60 minutos.

La API de catálogos devolvió que el metastore carece de una raíz de almacenamiento general. La creación de un volumen en `clase14.equipo9` devolvió falta de `CREATE VOLUME`. DBFS público está deshabilitado. El grupo de cuenta `mine-group9` sí dispone de uso de catálogo y esquema, creación de tablas y modelos, lectura y modificación de tablas en `clase14.equipo9`, y lectura y escritura en el volumen `clase10.equipo9.checkpoints`.

## Decisión

- Usar `existing_cluster_id` en todas las tareas Spark. El bundle resuelve el clúster por nombre, sube el código y parametriza el ID para otro cómputo de acceso dedicado con Unity Catalog. Las bibliotecas se declaran en las tareas. Así se evita reinstalarlas con celdas y reiniciar Python durante el trabajo. [Referencia de bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/reference).
- Escribir en tablas Delta administradas del esquema del equipo. Los prefijos preservan las fronteras lógicas de las capas. Crear nuevas capas físicas requiere `CREATE SCHEMA` en un catálogo apto para cómputo clásico. La semántica de prefijos facilita la migración, pero los permisos heredados se aplican a todo el esquema. [Almacenamiento por defecto](https://docs.databricks.com/aws/en/storage/default-storage).
- Aislar landing y checkpoints en subrutas del volumen existente con almacenamiento persistente. La ruta del checkpoint pertenece al par origen/destino y permanece estable en reinicios. Las pruebas crean un espacio distinto por corrida. [Volúmenes de Unity Catalog](https://docs.databricks.com/aws/en/volumes/).
- El propietario corre las tareas en el clúster dedicado. El grupo puede gestionar corridas del Job y leer el tablero. El tablero del equipo usa permisos individuales con el SQL warehouse institucional. La consulta del tablero es el único tramo con ese warehouse. [ACL de recursos](https://docs.databricks.com/aws/en/dev-tools/bundles/permissions) y [publicación de tableros](https://docs.databricks.com/aws/en/dashboards/tutorials/dashboard-crud-api).

## Consecuencias y alternativas

El clúster académico de clase 14 tiene capacidad de workers, pero es un recurso distinto de la máquina pedida para el despliegue. El diseño mantiene operaciones distribuidas y el personal permite verificar el flujo y los planes. Una comparación de red, escalado y skew requiere repetir el ensayo con workers. La instalación en un clúster Standard también exige validar sus restricciones de archivos y bibliotecas. [Requisitos de cómputo dedicado](https://docs.databricks.com/aws/en/compute/dedicated-limitations).

La segregación de analistas, científicos y operadores en esquemas físicos requiere roles y grants de la administración académica. La cuenta del equipo puede agregar permisos sobre objetos que posee, pero no retirar los grants heredados ni crear catálogos y esquemas independientes con los privilegios actuales. Para una separación estricta, aprovisionar primero almacenamiento administrado y esquemas propios, otorgar a consumidores lectura de oro, a ciencia de datos lectura de plata y ejecución del modelo, y reservar escritura de bronce y checkpoint a ingeniería.

Este ADR actualiza las decisiones vinculadas al entorno en ADR-001/002/003/007/013/015/017/018.
