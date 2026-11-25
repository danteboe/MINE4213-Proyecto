# Alcance y dependencias del equipo

- La política del workspace limita la segregación física de capas y roles. El grupo tiene acceso heredado a tablas en `clase14.equipo9`. La administración tendría que aprovisionar esquemas propios y roles diferenciados para aplicar mínimos privilegios por capa. [ADR-019](adr/ADR-019-computo-y-almacenamiento-institucional.md).
- El clúster personal tiene un nodo. La medición de tráfico de red, skew y escalamiento horizontal requiere autorización y acceso a una máquina con workers. [Arquitectura](architecture/medallion-overview.md).
- Solicitar al administrador los permisos de eventos del almacenamiento AWS para activar File arrival. Durante la pausa, subir al volumen y lanzar la corrida con el script o desde Lakeflow. [Runbook](runbook.md).
- La sustentación presencial y la entrega en Bloque Neón corresponden al equipo. [Guía](guia_sustentacion.md).
