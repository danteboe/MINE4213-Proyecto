# Evidencia — Medición de la máscara tierra/agua en serverless (FEAT-002, ADR-010, Req. 2)

- **Qué demuestra:** el **costo real** (instalación + clasificación) y la **tasa de falsos
  positivos/negativos** del método elegido (`global-land-mask`) para la regla de calidad
  *coordenada en el agua*, medidos ejecutando de verdad sobre una muestra real de
  `oceanwatch.bronze.ais_raw` en cómputo **serverless**. Es el insumo cuantitativo que cierra
  las secciones 5 (justificación) y 8 (verificación) del ADR-010.
- **Cómo se generó:** notebook temporal `_medicion_tierra_agua` subido al workspace bajo
  `/Users/jd.torresa1@uniandes.edu.co/oceanwatch/_tmp_feat002/` y ejecutado como *one-time run*
  serverless (`jobs submit`, `notebook_task` sin `new_cluster` ⇒ serverless,
  `effective_performance_target = PERFORMANCE_OPTIMIZED`). El notebook hace `%pip install
  global-land-mask==1.0.0`, toma una muestra real, clasifica de forma vectorizada y mide.
- **Fecha:** 2026-11-05
- **Estado:** Verificado (ejecutado)

> **Actualización (2026-11-05):** esta medición es el insumo de FEAT-002 (costo y
> exactitud del método). La bandera `en_agua` **ya quedó MATERIALIZADA** en
> `oceanwatch.silver.ais_clean` (0 NULL sobre 25.857.101 filas; 71.65 % tierra, coherente
> con el 72.28 % de esta muestra) mediante clasificación por **celda distinta** (sin
> `pandas_udf`) + `MERGE`. Ver `enriquecimiento_en_agua.md` y ADR-010 §8.

## Identificación del run serverless

| Campo | Valor real |
|---|---|
| `run_id` (job run) | **614704376036187** |
| `run_id` (task) | 641524386080059 |
| `job_id` | 518925548533365 |
| `life_cycle_state` / `result_state` | `TERMINATED` / **`SUCCESS`** |
| `effective_performance_target` | `PERFORMANCE_OPTIMIZED` (serverless) |
| `run_duration` | 27 565 ms (setup 3 s + ejecución 24 s) |

Cotejo: `databricks.exe jobs get-run --run-id 614704376036187 --output json` y
`databricks.exe jobs get-run-output 641524386080059 --output json` devuelven
`result_state = SUCCESS` y el `notebook_output.result` transcrito abajo.

## Método medido

- **Método principal (ADR-010):** `global-land-mask==1.0.0` (máscara ráster global ~1/120°,
  ~0.9 km, derivada de datos GSHHG). `globe.is_land(lat, lon)` ⇒ `True` sobre tierra;
  `en_agua = not is_land`. Instalación vía `%pip install` pinneado (la librería **no** viene
  preinstalada en serverless, ver `disponibilidad_librerias_serverless.md`).
- **Muestra real:** `oceanwatch.bronze.ais_raw.sample(fraction=0.0025, seed=42).limit(60000)`
  ⇒ **60 000 filas** (de 25 897 493). Columnas `LAT`, `LON`.

## Verdad de referencia (ground truth) usada

Como no existe una etiqueta tierra/agua por fila en el corpus, se definió un conjunto de
verdad **determinista y documentado**, dos clases:

- **Verdad-AGUA (6 puntos):** océano abierto, lejos de costa — Atlántico medio (0,−30),
  Atlántico NW (30,−70), Golfo de México (25,−90), Pacífico frente a Monterey (36,−122.6),
  Pacífico sur (−20,−100), Pacífico NW (40,150). Un buen método debe decir **agua**.
- **Verdad-TIERRA (6 puntos):** centros claramente tierra adentro — Kansas (38.5,−98.0),
  Kansas City (39.1,−94.6), Chicago (41.9,−87.6), Ciudad de México (19.4,−99.1),
  París (48.9,2.3), Brasilia (−15.8,−47.9). Un buen método debe decir **tierra**.

Definiciones: **falso negativo de agua** = punto de agua clasificado como tierra;
**falso positivo de agua** = punto de tierra clasificado como agua.

## Salida LITERAL del notebook (`notebook_output.result`)

```json
{
  "metodo": "global-land-mask==1.0.0",
  "t_import_s": 3.65,
  "t_clasificacion_muestra_s": 0.006,
  "n_muestra": 60000,
  "muestra_n_agua": 16634,
  "muestra_n_tierra": 43366,
  "muestra_pct_tierra": 72.2767,
  "verdad_agua_n": 6,
  "verdad_tierra_n": 6,
  "falsos_negativos_agua": 0,
  "falsos_positivos_agua": 0,
  "tasa_fn_agua": 0.0,
  "tasa_fp_agua": 0.0
}
```

## Interpretación

1. **Costo (serverless):** instalación `%pip` + import ≈ **3.65 s** (una vez por sesión);
   clasificación vectorizada de **60 000 puntos en 0.006 s** (numpy, lookup ráster). El costo
   dominante es el `%pip install`, no la clasificación: el método escala sin problema a los
   ~25.9 M de filas del corpus.
2. **Exactitud en la verdad de referencia:** **FN agua = 0/6, FP agua = 0/6** (tasas 0.0). En
   océano abierto y tierra adentro profunda el método es **perfecto**.
3. **Hallazgo crítico sobre datos AIS reales:** el **72.28 %** de la muestra AIS se clasifica
   como *tierra*. Esto NO son errores de posición: las celdas AIS más densas del corpus son
   **puertos y aguas interiores navegables** que una máscara ráster de costa ~1/120° marca como
   tierra. Evidencia (conteo real, `oceanwatch.bronze.ais_raw`, top celdas 0.1°):

   | lat | lon | filas | lugar |
   |---|---|---|---|
   | 29.8 | −95.1 | 390 510 | Houston Ship Channel (canal interior) |
   | 32.7 | −117.2 | 335 023 | San Diego |
   | 40.7 | −74.0 | 282 138 | puerto de Nueva York/Nueva Jersey |
   | 47.7 | −122.4 | 269 231 | Puget Sound / Seattle |
   | 26.1 | −80.1 | 263 870 | Port Everglades (Fort Lauderdale) |
   | 29.9 | −90.1 | 202 606 | Nueva Orleans / delta del Mississippi |
   | 29.1 | −90.2 | 192 743 | delta del Mississippi (aguas interiores) |

   Son posiciones AIS **válidas** de buques en canales, ríos y dársenas. La máscara de costa no
   distingue un canal de navegación de "tierra".

## Consecuencia de diseño (cierra ADR-010)

La regla `coordenada_en_agua` **NO** puede ser `descartar_fila`: descartaría ~72 % de tráfico
AIS legítimo. Decisión inequívoca (ADR-010): **NULIFICAR-ATRIBUTO** — se materializa una bandera
booleana `en_agua` y la fila **pasa** a plata; ningún registro se envía a cuarentena por esta
regla. El consumidor aguas abajo filtra por la bandera si lo necesita. La invariante de
conciliación de FEAT-003 (`bronce = plata + cuarentena + 658`) **no** se ve afectada por esta
regla, precisamente por ser nulificadora.

## Reproducibilidad / limpieza

- Notebook temporal y `job run` fuera del repo (prefijo `_`). El notebook
  `/Users/.../oceanwatch/_tmp_feat002/_medicion_tierra_agua` y los scripts `_*.ps1/_*.json/_*.py`
  de `...\PROYECTO` se eliminan al cerrar FEAT-002 (ver findings de FEAT-002).
- Para reproducir: subir el notebook, `databricks.exe jobs submit --json @_submit_feat002.json
  --output json`, polling con `jobs get-run --run-id <id>`, leer `jobs get-run-output <task_run_id>`.
