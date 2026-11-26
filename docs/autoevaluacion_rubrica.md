# Estado de la entrega

Revisión del **26 de noviembre de 2026** contra el enunciado de la Parte 2 del proyecto final de MINE 4213, período 2026-20.

- **Verificados** los criterios 1, 2, 3, 4, 6 y 7. La [matriz de trazabilidad](traceability_matrix.md) desglosa cada subcriterio y enlaza el código, la configuración y las corridas observadas.
- **Gobernanza parcialmente cubierta** en el criterio 5. Unity Catalog contiene once tablas administradas y comentadas, linaje y contrato de oro. La separación de esquemas con mínimos privilegios por rol depende de los grants de la administración del curso. [Detalle y consultas](../evidence/institucional/gobernanza.md).
- **Sustentación preparada** para el criterio 8. El guion está listo para que lo ejecuten los tres integrantes en la última clase. [Guía](guia_sustentacion.md).
- **Bono opcional** de Delta Sharing fuera del alcance ejecutado. Su condición es compartir oro mediante OpenSharing y consumirlo desde un tablero en un workspace independiente de Free Edition.

## Ruta de revisión

1. Abra el [README](../README.md) y ubique los artefactos con el [directorio](directorio_recursos.md).
2. Consulte las [decisiones de diseño](adr/) y la [evidencia institucional](../evidence/institucional/), que registran el fallo inducido, la poda de particiones y el análisis de la compuerta de agua.
3. Abra la cadena [detector programado](https://dbc-b38f0852-ecf9.cloud.databricks.com/jobs/342422518655466/runs/789140629150470) → [pipeline exitoso](https://dbc-b38f0852-ecf9.cloud.databricks.com/jobs/83886562675136/runs/796009776834361) → [consola](https://dbc-b38f0852-ecf9.cloud.databricks.com/dashboardsv3/01f1e5507d1b18a5963d1a49e4329629/published) y contraste la carga CSV2 con el producto de oro.

## Cierre por parte del equipo

- Confirmar que la persona evaluadora puede abrir el repositorio y los recursos institucionales que se presentarán.
- Entregar en **Bloque Neón** con el formato habilitado por el curso. El README sirve como punto de entrada a los artefactos.
- Realizar la sustentación. Mantener el detector cada 30 minutos hasta que el equipo indique el cambio temporal para la demostración.
