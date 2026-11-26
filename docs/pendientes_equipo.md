# Alcance y dependencias del equipo

- Para el cierre académico, el equipo confirma el acceso de la persona evaluadora, realiza la entrega en Bloque Neón y sustenta en clase. [Estado de entrega](autoevaluacion_rubrica.md).
- La política del workspace limita la segregación física de capas y roles. El grupo tiene acceso heredado a tablas en `clase14.equipo9`. La administración tendría que aprovisionar esquemas propios y roles diferenciados para aplicar mínimos privilegios por capa. [ADR-019](adr/ADR-019-computo-y-almacenamiento-institucional.md).
- El clúster personal tiene un nodo. La medición de tráfico de red, skew y escalamiento horizontal requiere autorización y acceso a una máquina con workers. [Arquitectura](architecture/medallion-overview.md).
- Solicitar al administrador los permisos de eventos del almacenamiento AWS si se requiere File arrival nativo. El detector de metadatos inicia el pipeline automáticamente cada vez que una revisión encuentra archivos pendientes. [Runbook](runbook.md).
- El bono opcional de Delta Sharing queda fuera del alcance implementado. [Matriz](traceability_matrix.md).
