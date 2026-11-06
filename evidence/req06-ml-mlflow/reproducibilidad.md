# Evidencia — Reproducibilidad del modelo de anomalías SOG (Req. 6, decisión D7)

- **Qué demuestra:** que el experimento MLflow, el modelo `@champion` y la inferencia batch
  son **reproducibles**: semilla única, versiones de librerías pinneadas a lo resuelto en
  serverless, partición temporal fija, antifuga (estadísticas solo del train) y pasos exactos
  para re-ejecutar entrenamiento e inferencia.
- **Cómo se generó:** consolida los parámetros reales de ejecución de FEAT-002 (entrenamiento)
  y FEAT-003 (inferencia en el Job). Todos los valores son los realmente resueltos/ejecutados en
  Databricks serverless el 2026-11-05 (ver `experimento_mlflow.md`, `registro_modelo_uc.md`,
  `integracion_job_ml.md`, `tabla_anomalias.md`).
- **Fecha:** 2026-11-05.
- **Estado:** Verificado (ejecutado).

## 1. Semilla y determinismo

- **Semilla única `42`** (`conf/config.yaml` → `ml.semilla`), aplicada en:
  - el muestreo al driver `.sample(fraction, seed=42)` (para traer ~200 000 filas con
    `toPandas()`), y
  - el modelo `IsolationForest(..., random_state=42)`.
- `ml.muestra_entrenamiento_filas = 200000`. Muestra efectiva real: **X_train = 199 930**,
  **X_test = 199 463** filas.

## 2. Versiones EXACTAS pinneadas (resueltas en serverless, Python 3.11.10)

**Entrenamiento** (`notebooks/fase6_entrenamiento_mlflow.py`, `%pip` y `conf/config.yaml`
`ml.pip_pins`):

| Librería | Versión |
|---|---|
| `mlflow` | `3.18.0` |
| `scikit-learn` | `1.3.0` |
| `numpy` | `1.26.4` |
| `pandas` | `1.5.3` |

**Inferencia en el Job** (`notebooks/fase6_tarea_ml_anomalias.py`, `conf/config.yaml`
`ml.pip_pins_inferencia`):

| Librería | Versión |
|---|---|
| `mlflow` | `3.18.0` |
| `scikit-learn` | `1.5.2` |

> **Por qué difiere scikit-learn entre entrenamiento (1.3.0) e inferencia (1.5.2).** Las
> tareas `notebook_task` del Job corren sobre el entorno serverless por defecto versión 3
> (Python **3.12**) y `scikit-learn 1.3.x` no publica wheel para 3.12 (solo 3.8–3.11); el
> entorno del `notebook_task` serverless no es fijable por DAB. Solución verificada: la
> inferencia usa `scikit-learn==1.5.2` (con wheel para 3.11 y 3.12). Se comprobó en una corrida
> aislada que el modelo `@champion` (serializado con 1.3.0) **carga y predice idéntico** bajo
> 1.5.2 (`predict` y `score_samples` coherentes); solo aparece `InconsistentVersionWarning`
> (silenciado). `mlflow 2.11.4` preinstalado NO sirve (falla al descargar artefactos de UC:
> S3 HeadObject 400); por eso se pinnea `mlflow==3.18.0` también en inferencia.

## 3. Partición temporal y antifuga (D3)

- **Train:** `_batch_date ∈ {2023-06-01, 2023-06-02}` (`ml.train_batch_dates`). Filas totales
  tras filtrar `sog_no_disponible = false AND SOG IS NOT NULL`: **17 789 682**.
- **Test / inferencia:** `_batch_date = 2023-06-03` (`ml.test_batch_date`). Filas totales
  (mismo filtro): **8 001 918**.
- **Antifuga:** las estadísticas por tipo (media/std de SOG) y el IsolationForest se ajustan
  **SOLO** con el train; el test aplica lo ya ajustado. Partición **temporal** (no aleatoria)
  para no filtrar futuro→pasado del mismo buque. Tipos de buque con estadísticas ajustadas en el
  train: **67**.
- **Vector de features (orden fijo, contrato del modelo):** `[z_sog, delta_sog, hora_sin, hora_cos]`
  (`ml.features_orden`). `z_sog = (SOG - media_tipo) / max(std_tipo, 1e-6)`; `delta_sog` =
  diferencia de SOG respecto a la posición previa del mismo MMSI (`Window.partitionBy('MMSI').orderBy('BaseDateTime')` + `F.lag('SOG')`; primera posición → 0.0); `hora_sin/hora_cos` = codificación
  cíclica de `F.hour('BaseDateTime')` (serverless UTC).

## 4. Pasos exactos para re-ejecutar

Prerrequisitos: exportar `DATABRICKS_HOST`/`DATABRICKS_TOKEN` desde `..\databricks.key` (FUERA
del repo); re-importar el repo actualizado al workspace
`/Users/jd.torresa1@uniandes.edu.co/oceanwatch/`; CLI de Databricks instalada.

### 4.1 Re-entrenamiento + experimento MLflow + registro del campeón

`notebooks/fase6_entrenamiento_mlflow.py` como **one-time run serverless** (`notebook_task` sin
`new_cluster` ⇒ serverless `PERFORMANCE_OPTIMIZED`):

```jsonc
// _run_entrenamiento.json  (crear FUERA del repo, en ...\PROYECTO)
{
  "run_name": "fase6_entrenamiento_mlflow",
  "tasks": [{
    "task_key": "entrenamiento_ml",
    "notebook_task": {
      "notebook_path": "/Users/jd.torresa1@uniandes.edu.co/oceanwatch/notebooks/fase6_entrenamiento_mlflow"
    }
  }]
}
```

```powershell
databricks jobs submit --json @_run_entrenamiento.json
# polling hasta life_cycle_state = TERMINATED, result_state = SUCCESS:
databricks jobs get-run --run-id <run_id> --output json
# leer notebook_output (JSON de dbutils.notebook.exit): run_ids, métricas, versión/alias.
```

El notebook: construye las features desde `oceanwatch.silver.ais_clean`, registra los runs
`baseline_zscore` e `isolation_forest`, elige IsolationForest campeón, hace
`mlflow.set_registry_uri("databricks-uc")` y `mlflow.register_model(...)` +
`set_registered_model_alias(..., "champion", version)`. Reproducible por la semilla 42 y los
pines de la §2.

### 4.2 Re-inferencia batch (poblar `gold.anomalias_velocidad`)

Opción principal (dentro del pipeline, es el entregable): correr el Job completo:

```powershell
databricks jobs run-now --job-id 202547459111516 --json '{"job_parameters":{"fecha_proceso":"2023-06-03"}}'
databricks jobs get-run --run-id <run_id> --output json   # TERMINATED / SUCCESS, 5 tareas SUCCESS
```

Opción aislada (solo la tarea ML): one-time run de `notebooks/fase6_tarea_ml_anomalias` con
`base_parameters` `{ "fecha_proceso": "2023-06-03", "job_run_id": "...", "job_id": "...",
"task_key": "ml_anomalias" }`. El notebook carga el modelo por alias
(`mlflow.sklearn.load_model("models:/oceanwatch.ml.detector_anomalias_velocidad@champion")` con
`set_registry_uri("databricks-uc")`), recalcula las estadísticas por tipo con el train, puntúa la
fecha y escribe las filas anómalas con `spark.sql` (`DELETE`+`INSERT` idempotente de la partición
`_batch_date`), **nunca** `df.write.saveAsTable`.

## 5. Re-ejecución verificada (valores reales obtenidos)

- Entrenamiento: one-time run `347173419720341` (task `386348858221273`) → SUCCESS; runs
  `a0cca5b5a4bd4373a7c0cd8ed04dc753` (baseline) y `be8f2ef2c2634efcaffa7fd89d3daf8f`
  (IsolationForest, campeón).
- Registro: `oceanwatch.ml.detector_anomalias_velocidad` v1 `READY`, alias `@champion` → v1.
- Inferencia en el Job: `run_id` **483234774995672** → SUCCESS (5 tareas); tabla
  `gold.anomalias_velocidad` para 2023-06-03 = **67 828** filas.
