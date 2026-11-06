# Reglas de calidad de datos — capa plata (Req. 2)

Catálogo completo de reglas de calidad aplicadas en la compuerta de la capa plata
(`oceanwatch.silver`). Cada regla es una **función pura** implementada en
`src/oceanwatch/quality/` (`reglas.py`, `identificadores.py`, `geo.py`), versionada,
parametrizada desde el bloque `calidad` de `conf/config.yaml` y justificada por el
diagnóstico real de **25.897.493 filas** (3 días) en
[`evidence/req02-capas-calidad/diagnostico_calidad_bronce.md`](../evidence/req02-capas-calidad/diagnostico_calidad_bronce.md).

La clasificación **descartar-fila** (a cuarentena) vs **nulificar-atributo** (la fila
pasa, el atributo va a `null` + bandera) se formaliza en
[ADR-009](adr/ADR-009-catalogo-reglas-calidad.md). Patrón aplicado: **Data Quality Gate**
con expectativas estilo `expect` / `expect_or_drop`.

## Tabla resumen

| Regla (nombre estable) | Ver. | Función | Umbral / parámetro (`conf['calidad']`) | Acción | Justificación (conteo real) |
|---|---|---|---|---|---|
| `coordenadas_en_rango` | v1 | `reglas.coordenadas_en_rango` | LAT∈[-90,90], LON∈[-180,180] | descartar-fila | 0 fuera de rango; regla por robustez |
| `coordenada_no_centinela` | v1 | `reglas.es_centinela_coordenada` / `coordenada_no_centinela` | LAT≠91, LON≠181 | descartar-fila | 0 centinelas en el corpus; robustez |
| `mmsi_bien_formado` | v1 | `identificadores.mmsi_bien_formado` | `mmsi.digitos`=9 | descartar-fila | MMSI no 9 dígitos = **21.960** (~0.085%) |
| `mmsi_mid_valido` | v1 | `identificadores.mmsi_mid_valido` | MID∈[`mid_min`,`mid_max`]=[201,775] | descartar-fila | MID fuera del rango ITU => identidad inválida |
| `timestamp_valido_no_futuro` | v1 | `reglas.timestamp_valido_no_futuro` | `ts` no nulo y `ts<=ahora` (UTC) | descartar-fila | 0 futuros/nulos; regla por robustez |
| `sog_plausible` | v1 | `reglas.sog_plausible` / `sog_es_centinela` | `sog.centinela`=102.3, `sog.max_fisico`=80 | nulificar-atributo | SOG≥102.3 = **65.510** (centinela) |
| `heading_valido` | v1 | `reglas.heading_valido` / `heading_es_centinela` | `heading.centinela`=511, [0,359] | nulificar-atributo | Heading=511 = **14.367.009** (~55.5%) |
| `cog_valido` | v1 | `reglas.cog_valido` / `cog_es_centinela` | `cog.centinela`=360, [0,359] | nulificar-atributo | COG=360 = **4.376.798** (~16.9%) |
| `dimension_fisica` | v1 | `reglas.dimension_fisica` | valor > `dimensiones.min_exclusivo`=0 | nulificar-atributo | Length≤0=**905.785**, Width≤0=**967.816**, Draft≤0=**1.180.265** |
| `imo_valido` | v1 | `identificadores.imo_valido` | `imo.digitos`=7; vacío VÁLIDO; check digit | nulificar-atributo | IMO vacío = **11.015.974** (~42.5%); vacío es válido |
| `coordenada_en_agua` | v1 | `tierra_agua.en_agua` (baseline `en_agua_bbox`) | `coordenada_en_agua.metodo`=`global_land_mask` v1.0.0; baseline `bbox_agua` | nulificar-atributo | **72.28%** de la muestra AIS cae "en tierra" (puertos/canales válidos); medición real run `614704376036187` |

Funciones geoespaciales de apoyo (para la capa oro, no son reglas de calidad):
`geo.haversine_km` (distancia great-circle, radio 6371 km) y `geo.celda_grilla`
(redondeo lat/lon a `gold.decimales_grilla`=2, ADR-001: NO H3).

## Detalle por regla

### `coordenadas_en_rango` (v1) — descartar-fila
Valida que LAT∈[-90,90] y LON∈[-180,180]. En el corpus 0 filas violan el rango, pero
la regla existe por resiliencia ante datos AIS futuros. Una posición fuera del planeta
no es localizable: la fila va a cuarentena.

### `coordenada_no_centinela` (v1) — descartar-fila
Detecta el centinela de coordenada "no disponible" (LAT=91 / LON=181). 0 casos en el
corpus; regla por robustez. Sin coordenada real la fila no aporta tráfico => cuarentena.

### `mmsi_bien_formado` (v1) — descartar-fila
El MMSI (Maritime Mobile Service Identity, ITU-R M.585) debe tener exactamente 9
dígitos. **21.960** filas (~0.085%) no cumplen. Sin identidad bien formada el registro
no es conciliable => cuarentena.

### `mmsi_mid_valido` (v1) — descartar-fila
Los 3 primeros dígitos del MMSI son el MID (Maritime Identification Digits) y codifican
el país de registro; deben caer en el rango ITU [201,775]
([ITU, Table of MID](https://www.itu.int/en/ITU-R/terrestrial/fmd/Pages/mid.aspx),
acceso 2026-11-05). Un MID fuera de rango indica un MMSI inválido => cuarentena.

### `timestamp_valido_no_futuro` (v1) — descartar-fila
`BaseDateTime` debe estar presente y no ser futuro respecto a "ahora" (UTC, ADR-004).
0 futuros/nulos en el corpus; regla por robustez.

### `sog_plausible` (v1) — nulificar-atributo
SOG≥102.3 es el centinela AIS "no disponible" (**65.510** filas), no velocidad. El
máximo físico plausible se fija en **80 kn** (muy por encima de P99.9=32.5 kn y por
debajo del centinela 102.3). Una velocidad ausente o no física no invalida la posición:
el atributo va a `null` + bandera.

### `heading_valido` (v1) — nulificar-atributo
Heading=511 es el centinela "no disponible" (**14.367.009** filas, ~55.5%). Rango válido
[0,359]. Descartar estas filas destruiría más de la mitad del corpus; se nulifica el
atributo.

### `cog_valido` (v1) — nulificar-atributo
COG=360 es el centinela "no disponible" (**4.376.798** filas, ~16.9%). Rango válido
[0,359]. Mismo razonamiento que Heading: nulificar.

### `dimension_fisica` (v1) — nulificar-atributo
Length/Width/Draft deben ser > 0. Valores no físicos: Length≤0=**905.785**,
Width≤0=**967.816**, Draft≤0=**1.180.265**. Son atributos estáticos del buque, no de la
posición; se nulifican sin descartar la observación.

### `imo_valido` (v1) — nulificar-atributo
Vacío/None es **VÁLIDO** (IMO vacío=**11.015.974**, ~42.5%; no todos los buques tienen
IMO). Si viene (`IMO`+7 dígitos o 7 dígitos), se valida el **dígito verificador** del
esquema de numeración IMO ([OMI, Resolución A.1117(30)](https://www.imo.org), acceso
2026-11-05): la suma de los 6 primeros dígitos multiplicados por 7,6,5,4,3,2 tiene como
unidad el 7.º dígito. Patrón inválido => nulificar atributo (la fila pasa).

### `coordenada_en_agua` (v1) — nulificar-atributo
Máscara tierra/agua: ¿la posición cae en el agua? Método principal
`global-land-mask==1.0.0` (máscara ráster global ~1/120°, `globe.is_land(lat,lon)`),
instalado vía `%pip install` pinneado en el notebook serverless porque **no** viene
preinstalado ([disponibilidad de librerías](../evidence/req02-capas-calidad/disponibilidad_librerias_serverless.md));
baseline `en_agua_bbox` sin dependencias como salvaguarda. La **medición real** en
serverless (run `614704376036187`, SUCCESS;
[evidencia](../evidence/req02-capas-calidad/mascara_tierra_agua.md)) dio costo de
instalación+import ≈3.65 s, clasificación de 60.000 puntos en 0.006 s, y **0 falsos
positivos / 0 falsos negativos** contra la verdad de referencia (océano abierto vs
tierra adentro), pero **72.28%** de la muestra AIS clasificada como "tierra" porque la
máscara de costa marca como tierra los puertos y aguas interiores navegables (Houston
Ship Channel, delta del Mississippi, Puget Sound) que son tráfico AIS **válido**. Por
eso la acción es **nulificar-atributo** (bandera `en_agua`), NO descartar: descartar
eliminaría ~72% del corpus legítimo. Decisión formalizada en
[ADR-010](adr/ADR-010-mascara-tierra-agua.md).

## Verificación

- Local: `pytest -q` (tests de `test_reglas.py`, `test_identificadores.py`,
  `test_geo.py`) — ejecutado: 57 passed, 1 skipped.
- Real (conteos de cuarentena y conciliación): PENDIENTE DE EJECUCIÓN en FEAT-003.

## Referencias

- [ADR-009 — Catálogo de reglas de calidad](adr/ADR-009-catalogo-reglas-calidad.md)
- [ADR-010 — Máscara tierra/agua](adr/ADR-010-mascara-tierra-agua.md)
- [Diagnóstico de calidad real sobre bronce](../evidence/req02-capas-calidad/diagnostico_calidad_bronce.md)
- [Medición de la máscara tierra/agua en serverless](../evidence/req02-capas-calidad/mascara_tierra_agua.md)
