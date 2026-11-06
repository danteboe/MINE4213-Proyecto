# Verificación de integración entre FEATs — Fase 3 (Req. 2)

Estado: **Verificado (ejecutado)** en Databricks serverless (SQL warehouse `377600528fed7c47`).

Este documento recoge la **re-ejecución de la verificación de integración** del paso FIXER del
bucle de convergencia (iteración 1): se comprueban, con conteos **reales** leídos vía la SQL
Statement Execution API (`_dbx_sql.py`), las costuras entre FEAT-001 (reglas puras) →
FEAT-003 (plata/cuarentena/métricas) → FEAT-004 (oro) → FEAT-005 (prueba de compuerta). No se
confía en los `.md` previos: todas las cifras de abajo salen de consultas vivas sobre las tablas.

## 1. Build y pruebas locales (sin Spark)

```
python -m pytest -q
76 passed, 4 skipped in ~1s
```

Imports perezosos verificados (`$env:PYTHONPATH='src'`): `oceanwatch.quality` y submódulos
(`reglas`, `geo`, `identificadores`, `compuertas`, `conciliacion`, `materializar`, `tierra_agua`)
y `oceanwatch.gold` (`trayectorias`, `puertos`, `matriz_trafico`) importan **sin** PySpark →
`IMPORTS_OK`.

## 2. Invariante de conciliación (conteos reales, consulta única)

```
cols=['bronce', 'plata', 'cuarentena', 'duplicados']
['25897493', '25857101', '39734', '658']
```

```
25.897.493 (bronce) = 25.857.101 (plata) + 39.734 (cuarentena) + 658 (duplicados)
diferencia = 0  -> CUADRA
```

Donde `duplicados = count(*) - count(DISTINCT MMSI, BaseDateTime)` sobre bronce.

## 3. Costura FEAT-001 → FEAT-003 (semántica de compuertas en los datos materializados)

- Cuarentena por causa (`explode(causas)`):
  ```
  cols=['regla', 'filas']
  ['mmsi_mid_valido', '39734']
  ['mmsi_bien_formado', '21958']
  ```
- Suma de `evaluados` por día en `oceanwatch.quality.metricas_calidad`:
  ```
  cols=['suma_evaluados']
  ['25896835']
  ```
  `25.896.835 = plata (25.857.101) + cuarentena (39.734)` → coincide (bronce − 658 duplicados).
- Coherencia bandera→atributo en `silver.ais_clean` (bandera True ⟹ atributo NULL):
  ```
  cols=['sog_incoh', 'head_incoh', 'cog_incoh', 'imo_incoh']
  ['0', '0', '0', '0']
  ```
- Esquema real: `silver.ais_clean` NO tiene columna `causas` (solo filas limpias + banderas de
  nulificación); `quarantine.ais_rejected` sí tiene `causas array<string>` + `_fecha_cuarentena`.
  La separación plata/cuarentena por `size(causas)` es correcta.

## 4. Costura FEAT-003 → FEAT-004: ningún registro sucio llega a ORO

Conteos reales de oro: `trayectorias_diarias`=**61.768**, `presencia_puertos`=**9.153**,
`matriz_trafico`=**2.319.365**, `world_port_index`=**3.669**.

Comprobaciones (todas = 0, es decir, oro limpio):

```
[tray_mmsi_no_en_plata]     mmsi_huerfanos = 0   (todo MMSI de oro existe en plata)
[tray_mmsi_en_cuarentena]   n = 0                (ningún MMSI de oro está en cuarentena)
[tray_mmsi_mal_formado]     n = 0                (ningún MMSI de oro con != 9 dígitos)
[puerto_mmsi_mal_formado]   n = 0                (presencia_puertos: MMSI siempre 9 dígitos)
[matriz_coords_fuera_rango] n = 0                (ninguna celda con lat/lon fuera de rango)
[matriz_grilla_no_2dec]     n = 0                (grilla redondeada a 2 decimales, ADR-001)
[puerto_index_huerfano]     n = 0                (todo INDEX_NO cruza con world_port_index)
```

Conclusión: el oro se alimenta **solo** de la plata limpia; no hay fuga de registros sucios
(MMSI mal formado, coordenadas fuera de rango) ni puertos huérfanos. La grilla espacial respeta
el redondeo a 2 decimales de ADR-001 (no H3).

## 5. Tablas preexistentes intactas

`SHOW TABLES IN oceanwatch.quality` → `ingesta_metricas`, `pruebas_fallos` (Req. 1, intactas) +
`metricas_calidad` (añadida en Fase 3). No se rompió ninguna tabla de bronce/quality previa.

## 6. Límite conocido (no bloquea la invariante ni la limpieza de oro)

`silver.ais_clean.en_agua` sigue en `NULL` para las 25.857.101 filas (bandera informativa
NULIFICAR-ATRIBUTO, ADR-010). No participa en `causas` ni en la invariante ni en la limpieza del
oro. Procedimiento exacto de enriquecimiento documentado en `materializacion_plata.md`.

## 7. Reproducción

Scripts temporales fuera del repo (prefijo `_`, en `...\PROYECTO`), ejecutados con
`python <script>.py`, que usan `_dbx_sql.run_sql`/`extract_rows` contra el warehouse
`377600528fed7c47`. Las consultas exactas son las descritas en las secciones 2–5.
