# Reglas de calidad de la Entrega 2

Las compuertas registran causas estables. Una posición rechazada puede contar en varias métricas por regla y cuenta una vez en la cuarentena. El registro original incluye las 17 columnas AIS, `_rescued_data` y los metadatos de bronce. Un timestamp o MMSI que no se pudo parsear puede conservar información original en `_rescued_data`.

| Regla o dominio | Acción | Justificación |
|---|---|---|
| LAT/LON nulas, inválidas, fuera del rango físico, centinelas | Cuarentena | Invalida la ubicación y puede romper la máscara o la grilla. |
| `sin_evidencia_de_agua` | Cuarentena | Acepta agua en el ráster en el centro o en los ocho puntos vecinos a 0,02°, proximidad de hasta 0,02° a una vía navegable de NWN o lago de Natural Earth, o una celda del WPI. |
| `mmsi_bien_formado`, `mmsi_mid_valido` | Cuarentena | El comportamiento se hereda del diagnóstico AIS. La política admite MMSI de buques en el rango definido en configuración; otras clases de identificador AIS quedan fuera del alcance. |
| Timestamp nulo o futuro, `timestamp_fuera_del_dia_fichero` | Cuarentena | Una fecha de evento UTC válida y coincidente con el archivo permite garantizar unicidad con un lote acotado. |
| `sog_fuera_de_rango` | Cuarentena | Rechaza NaN, valores negativos y velocidades por encima de 80 kn por debajo del primer centinela AIS. |
| SOG ausente o centinela ≥ 102,3 | Nulo en plata y excluido de ML | La falta de medición deja otras variables útiles. El centinela se marca con `sog_no_disponible`. |
| COG y Heading centinela o inválidos, dimensiones no físicas, IMO inválido | Nulificar con bandera | Son atributos complementarios. Se conserva la posición válida. |
| Duplicado válido de `(MMSI, BaseDateTime)` | Conservar un representante | Se prefiere SOG conocida y fila sin rescate. Con las restantes columnas como desempate total, el resultado es reproducible. |

La máscara aproxima agua sobre una grilla. Puede excluir cuerpos estrechos ausentes en las referencias o admitir posiciones terrestres cerca de la costa y los ríos. `en_agua` significa evidencia de agua con tolerancia. Las tasas de rechazo se auditan por día antes de interpretar resultados. Se registra una alerta al llegar a 5% para una regla. La alerta y el conteo de causas informan revisión. La compuerta que detiene oro es una conciliación fallida. [ADR-020](adr/ADR-020-procesamiento-incremental-y-calidad.md).
