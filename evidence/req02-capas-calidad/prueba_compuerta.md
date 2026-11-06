# Prueba de compuerta extremo a extremo — evidencia literal (FEAT-005, Req. 2)

Estado: **Verificado (ejecutado)** en Databricks serverless el 2026-11-06.

Esta prueba demuestra, con **conteos reales**, que la compuerta de calidad bloquea el
**100%** de los registros sucios por **todas las rutas** (plata y oro) y que cada
sucio queda en cuarentena con su **causa correcta**, mientras los registros limpios de
control sí atraviesan plata y oro. Es la verificación extremo a extremo de la capa de
calidad (ADR-009/010/011) y del modelo oro (ADR-012) de la Fase 3.

## Entorno y camino de ejecución

- Catálogo/esquema sandbox: `oceanwatch.quality`, **tablas con prefijo `pf_`**
  (AISLADAS; no se tocan `bronze.ais_raw`, `silver.ais_clean`,
  `quarantine.ais_rejected`, `quality.metricas_calidad` ni `gold.*` de producción).
- Cómputo: **serverless**. SQL warehouse serverless id `377600528fed7c47`.
- Sentencias: `resources/sql/fase3_prueba_compuerta.sql`, ejecutadas vía SQL
  Statement Execution API (`/api/2.0/sql/statements/`). El notebook
  `notebooks/fase3_prueba_compuerta.py` es el orquestador de referencia (API de
  Spark) del MISMO pipeline; el camino REAL ejecutado es el SQL CTAS (misma decisión
  que FEAT-003/004 por la fricción `INTERNAL_ERROR` de Spark Connect GC).
- Las compuertas sandbox reproducen EXACTAMENTE `src/oceanwatch/quality/compuertas.py`
  (y su CTAS `fase3_materializar_plata.sql`); la lógica oro reproduce
  `src/oceanwatch/gold/` (y `fase3_materializar_oro.sql`). `pf_gold_presencia` cruza
  contra la tabla de referencia REAL `oceanwatch.gold.world_port_index` (3.669 puertos).

### `statement_id` serverless (identificador de ejecución, análogo a run_id)

Construcción del sandbox (SQL warehouse `377600528fed7c47`, estado `SUCCEEDED`):

```
CREATE SCHEMA ...quality                  statement_id=01f1d9a5-827b-12ce-b5ab-3b22d56edba9
CREATE OR REPLACE TABLE ...pf_gate_input  statement_id=01f1d9a5-830c-15d3-897b-ac07c2d7ee1b
CREATE OR REPLACE TABLE ...pf_silver      statement_id=01f1d9a5-8443-199d-bfdf-602aca623c5c
CREATE OR REPLACE TABLE ...pf_quarantine  statement_id=01f1d9a5-85dc-1bc7-8c5d-6919b0a2dd4a
CREATE OR REPLACE TABLE ...pf_gold_trayectorias  statement_id=01f1d9a5-8770-1a3a-ae3e-af1ee65aaf55
CREATE OR REPLACE TABLE ...pf_gold_presencia     statement_id=01f1d9a5-88e8-14e9-8dee-2431b44620fc
CREATE OR REPLACE TABLE ...pf_gold_matriz        statement_id=01f1d9a5-8a7f-1dbd-90a3-64cb1352530e
```

Se pueden consultar con `GET /api/2.0/sql/statements/<statement_id>`. (El camino con
`databricks.exe jobs submit` del notebook está sujeto a la fricción serverless
documentada en FEAT-003; el camino SQL es el ejecutado y reproducible.)

## Conjunto inyectado (9 filas): sucios (uno por regla de descarte) + limpios

| `_caso` | Regla de descarte esperada | Dato clave |
|---|---|---|
| `sucio_coord_rango` | `coordenadas_en_rango` | LAT=200.0 (fuera de [-90,90]) |
| `sucio_centinela_lat` | `coordenada_no_centinela` | LAT=91.0 (centinela AIS) |
| `sucio_mmsi_8digitos` | `mmsi_bien_formado` | MMSI=30012345 (8 dígitos) |
| `sucio_mmsi_mid` | `mmsi_mid_valido` | MMSI=999000000 (MID=999 ∉ [201,775]) |
| `sucio_ts_futuro` | `timestamp_valido_no_futuro` | BaseDateTime=2099-01-01 |
| `dup_exacto` | `duplicado` | misma clave (MMSI,BaseDateTime) que `limpio_ctrl_1` |
| `limpio_ctrl_1/2/3` | `(limpio)` | 3 pings en la celda-puerto NEW YORK CITY (40.70/-74.02) |

## Conteos reales (salida literal, SQL warehouse `377600528fed7c47`)

```
pf_gate_input         = 9
pf_silver             = 3
pf_quarantine         = 5
pf_gold_trayectorias  = 1
pf_gold_presencia     = 1
pf_gold_matriz        = 1
```

**Invariante sandbox** (misma aritmética que producción):

```
input(9) = silver(3) + quarantine(5) + duplicados(1)
```

```
cols: ['input', 'silver', 'quarantine', 'duplicados']
['9', '3', '5', '1']
```

**CUADRA** (9 = 3 + 5 + 1, diferencia 0). El duplicado exacto `dup_exacto` (misma
clave que `limpio_ctrl_1`) lo elimina la dedup `ROW_NUMBER()=1` (ADR-006) por orden de
`_ingest_timestamp`: sobrevive `limpio_ctrl_1` (ingesta 11:00:05) y se descarta el
duplicado (ingesta 11:00:30); por eso silver tiene 3 (los tres limpios), no 4.

## (a) Cada sucio en cuarentena con su causa observada

```
_caso                _regla_esperada             MMSI       causas
sucio_centinela_lat  coordenada_no_centinela     367000011  ["coordenadas_en_rango","coordenada_no_centinela"]
sucio_coord_rango    coordenadas_en_rango        367000010  ["coordenadas_en_rango"]
sucio_mmsi_8digitos  mmsi_bien_formado           30012345   ["mmsi_bien_formado","mmsi_mid_valido"]
sucio_mmsi_mid       mmsi_mid_valido             999000000  ["mmsi_mid_valido"]
sucio_ts_futuro      timestamp_valido_no_futuro  367000012  ["timestamp_valido_no_futuro"]
```

Los **5** registros sucios aparecen en `pf_quarantine`, cada uno con su causa esperada
**contenida** en el array `causas`. Dos casos acumulan DOS causas, lo cual es correcto
y esperado (no un error):

- `sucio_centinela_lat` (LAT=91): 91 está **fuera de rango** (>90) **y** es el
  **centinela** AIS, así que viola `coordenadas_en_rango` **y** `coordenada_no_centinela`.
- `sucio_mmsi_8digitos` (8 dígitos): falla `mmsi_bien_formado` (no son 9 dígitos) **y**
  `mmsi_mid_valido` (la validación de MID exige MMSI de 9 dígitos). Es la misma
  coocurrencia observada en producción (21.958 filas con 2 causas, findings FEAT-003).

Una fila cuenta UNA sola vez en cuarentena aunque tenga varias causas (patrón
Dead-letter/Quarantine, ADR-011): por eso `pf_quarantine = 5` filas, no 7 causas.

## (b) NINGÚN sucio llega a plata ni a oro

Casos sucios presentes en `pf_silver` (debe ser vacío):

```
SELECT _caso FROM pf_silver WHERE _regla_esperada NOT IN ('(limpio)','duplicado')
-> (sin filas)
```

MMSIs sucios presentes en las tablas de oro sandbox (debe ser 0 en las tres):

```
tray  : (ninguna fila)      -- ningún MMSI sucio en pf_gold_trayectorias
pres  : (ninguna fila)      -- ningún MMSI sucio en pf_gold_presencia
matriz_cells_sucios : 0     -- ninguna celda con lat fuera de [-90,90] en pf_gold_matriz
```

Como las tablas de oro leen **solo** de `pf_silver` (que no contiene ningún sucio),
ningún registro sucio puede alcanzar oro por construcción. Verificado además por
conteo directo (0 filas/0 celdas).

## (c) Los limpios de control SÍ atraviesan plata y oro

`pf_silver` (solo los 3 limpios; el duplicado ya se eliminó):

```
_caso          MMSI       BaseDateTime              SOG  COG   Heading
limpio_ctrl_1  367000020  2023-06-01T11:00:00.000Z  6.0  90.0  180.0
limpio_ctrl_2  367000020  2023-06-01T11:05:00.000Z  6.1  95.0  100.0
limpio_ctrl_3  367000020  2023-06-01T11:10:00.000Z  6.2  97.0  110.0
```

Oro sandbox (el buque limpio 367000020 llega a las tres tablas):

```
tray_limpio    : MMSI=367000020  dia=2023-06-01  n_posiciones=3  dist_km=0.2791
pres_limpio    : MMSI=367000020  INDEX_NO=7640  PORT_NAME=NEW YORK CITY  COUNTRY=US  dia=2023-06-01  n_posiciones_en_puerto=3
matriz_limpio  : celda_lat=40.70  celda_lon=-74.02  hora=2023-06-01T11:00:00.000Z  n_posiciones=3  n_buques=1
```

La presencia en puerto confirma la regla geoespacial de ADR-012: los 3 pings caen en
la celda de grilla (40.70, -74.02) del puerto real NEW YORK CITY (INDEX_NO 7640,
40.7/-74.016667) y superan la permanencia mínima de 3 posiciones.

## Conclusión

La compuerta bloquea el **100%** de los registros sucios por **todas** las rutas:
ninguno aparece en `pf_silver` ni en `pf_gold_trayectorias` / `pf_gold_presencia` /
`pf_gold_matriz`, y los 5 llegan a `pf_quarantine` con su causa correcta. Los 3
registros limpios de control atraviesan plata y las tres tablas de oro. El invariante
sandbox cuadra (9 = 3 + 5 + 1).

## Complemento local (sin Spark)

`tests/test_prueba_compuerta.py` reproduce el MISMO fixture de 9 filas con la parte
PURA de las compuertas (`oceanwatch.quality.reglas.causas_de_descarte`, que replica la
columna `causas`) y verifica que cada sucio recibe su causa esperada y que los limpios
no generan ninguna. `python -m pytest -q` => **76 passed, 4 skipped**.

## Limpieza del sandbox

Tras capturar esta evidencia, las tablas `pf_*` se ELIMINAN con
`resources/sql/fase3_limpieza_sandbox.sql` (ver `SHOW TABLES IN oceanwatch.quality
LIKE 'pf_*'` => sin filas tras el DROP). Las tablas de producción
(`silver.ais_clean`, `quarantine.ais_rejected`, `quality.metricas_calidad`, `gold.*`)
quedan intactas.

## Reproducibilidad

1. Ejecutar `resources/sql/fase3_prueba_compuerta.sql` en el SQL warehouse serverless
   `377600528fed7c47` (crea `pf_gate_input` y deriva `pf_silver`/`pf_quarantine`/`pf_gold_*`).
2. Leer los conteos y las causas con `_dbx_sql.py` (los de este documento).
3. Verificar el complemento local con `python -m pytest -q tests/test_prueba_compuerta.py`.
4. Limpiar con `resources/sql/fase3_limpieza_sandbox.sql`.

Equivalente con la API de Spark: importar `src/oceanwatch` y
`notebooks/fase3_prueba_compuerta.py` al workspace y lanzar un one-time run serverless.
