# Evidencia — Verificación del esquema real de los CSV NOAA

- **Qué demuestra:** las columnas reales del archivo `AIS_2023_06_01.csv` (dentro del zip
  diario de NOAA) coinciden exactamente, en nombre y orden, con las 17 columnas de referencia
  del enunciado. Esto valida el esquema explícito de 17 campos (ADR-003) con datos reales.
- **Cómo se generó:** descarga parcial (ranged request, primeros 3 MB) del zip NOAA +
  parseo del local file header del ZIP + raw-inflate (zlib) del inicio del stream, sin
  descargar los 344 MB completos. Script: `_probe_header.py` (temporal, fuera del repo).
- **Fecha:** 2026-11-05
- **Fuente:** https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/AIS_2023_06_01.zip
- **Estado:** Verificado (ejecutado)

## Header real (17 columnas, en orden)

```
MMSI,BaseDateTime,LAT,LON,SOG,COG,Heading,VesselName,IMO,CallSign,VesselType,Status,Length,Width,Draft,Cargo,TransceiverClass
```

Coincide 1:1 con las columnas de referencia del enunciado. **No hay diferencias** de nombre
ni de orden respecto a lo documentado.

## Muestra real (primeras 4 filas de datos)

```
367138690,2023-06-01T00:00:01,30.28335,-91.22283,0.0,273.0,511.0,ST FRANCISVILLE,,WDD3968,60,0,43,18,,60,A
368036960,2023-06-01T00:00:01,40.49818,-80.07596,3.3,118.6,131.0,GALE R RHODES,IMO8199788,WDK2590,0,12,20,7,2.6,0,A
368095340,2023-06-01T00:00:01,29.99678,-93.94959,0.0,309.8,511.0,GAMBLER,,WDK8562,52,15,19,8,0.0,52,A
248301000,2023-06-01T00:00:03,26.13342,-80.11909,0.0,260.5,86.0,ATLAS,,9HB5599,37,5,31,8,3.0,37,A
```

## Observaciones para las reglas de calidad (Fase 3, Req. 2)

Confirmadas con datos reales, consistentes con el diagnóstico de la Entrega 1:

- **`Heading=511.0`** aparece como centinela "no disponible" (filas 1 y 3). Regla de calidad
  debe tratar 511 como centinela, no como rumbo válido.
- **`IMO`** tiene dos formas: vacío, o prefijo `IMO` + dígitos (`IMO8199788`). La regla de IMO
  bien formado debe contemplar el prefijo y permitir vacío (no todos los buques tienen IMO).
- **`SOG=0.0`** frecuente (buque detenido/en puerto); no es error.
- **`VesselType`** y **`Status`** son códigos enteros (60, 0, 52, 37 / 0, 12, 15).
- **`Draft`** y a veces otros campos numéricos vienen vacíos (fila 1 sin Draft).
- **`BaseDateTime`** en ISO 8601 sin zona (`2023-06-01T00:00:01`); se interpreta en UTC
  (serverless usa UTC por defecto).
- **`TransceiverClass`** = `A`/`B`.

## Tipos del esquema explícito (coherentes con la muestra)

| Columna | Tipo | Nota |
|---|---|---|
| MMSI | long | 9 dígitos |
| BaseDateTime | timestamp | ISO 8601 |
| LAT, LON | double | grados decimales |
| SOG, COG, Heading | double | nudos / grados; centinelas 511 (Heading), 360 (COG) |
| VesselName, IMO, CallSign | string | pueden venir vacíos |
| VesselType | int | código AIS |
| Status | int | código AIS (Entrega 1 lo trató como string; la muestra real es entero — corregido a `int` en Fase 2, ADR-008) |
| Length, Width, Draft | double | metros; pueden venir vacíos o ≤0 |
| Cargo | int | código |
| TransceiverClass | string | A/B |

> Nota de consistencia (resuelta en Fase 2): esta muestra real mostró `Status` como código
> entero, en contra del `string` que declaraba la Entrega 1. La decisión quedó tomada en
> **ADR-008**: `conf/config.yaml` declara ahora `Status` como `int` y los valores no conformes
> caen a `_rescued_data`. El `DESCRIBE TABLE` de `ejecucion_ingesta.md` confirma `Status int` en
> la tabla bronce real.
