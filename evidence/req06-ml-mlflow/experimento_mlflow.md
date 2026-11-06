# Evidencia — Experimento MLflow (detección de anomalías SOG, Req. 6)

- **Qué demuestra:** que la integración MLOps pedida por la rúbrica está EJECUTADA de
  verdad en Databricks serverless: se construyen las features desde el Lakehouse
  (`oceanwatch.silver.ais_clean`), se registra un experimento MLflow con **dos runs
  reales** (línea base interpretable vs IsolationForest) con sus parámetros, métricas,
  tags, firma (`signature`) y ejemplo de entrada (`input_example`), y se elige campeón.
  El objetivo NO es maximizar precisión (modelo deliberadamente simple), sino demostrar
  el flujo features -> experimento -> modelo en Unity Catalog.
- **Cómo se generó:** notebook `notebooks/fase6_entrenamiento_mlflow.py` lanzado como
  **one-time run serverless** (`databricks jobs submit --json @_run_entrenamiento.json`,
  `notebook_task` SIN `new_cluster` => serverless `PERFORMANCE_OPTIMIZED`); polling con
  `databricks jobs get-run <run_id> --output json` hasta
  `life_cycle_state = TERMINATED` y `result_state = SUCCESS`; las métricas/`run_id` se
  leyeron del `notebook_output` (JSON de `dbutils.notebook.exit`) y se verificaron
  contra la MLflow REST API. ENV exportado FUERA del repo desde `databricks.key`.
- **Fecha:** 2026-11-05.
- **Estado:** Verificado (ejecutado).

## Identidad de la corrida (serverless)

- **Job one-time run:** `347173419720341` (`run_type = SUBMIT_RUN`,
  `effective_performance_target = PERFORMANCE_OPTIMIZED`).
- **Task run (`entrenamiento_ml`):** `386348858221273` — `TERMINATED` / `SUCCESS`.
- **Runtime serverless:** Python `3.11.10`.
- **Notebook ejecutado:** `/Users/jd.torresa1@uniandes.edu.co/oceanwatch/notebooks/fase6_entrenamiento_mlflow`.

## Experimento MLflow

- **Nombre:** `/Users/jd.torresa1@uniandes.edu.co/oceanwatch_anomalias_sog`.
- **`experiment_id`:** `2474832545018011`.
- **Registry URI:** `databricks-uc` (Models in Unity Catalog, MLflow 3; alias, no stages).

## Partición de datos (temporal, antifuga D3)

Las estadísticas por tipo (media/std de SOG) y el IsolationForest se ajustan SOLO con el
train; el test aplica lo ya ajustado. Se excluyen las posiciones con SOG centinela
(`sog_no_disponible = true`) y SOG nulo.

| Conjunto | `_batch_date` | filas totales (Spark) | muestra al driver (`toPandas`, semilla 42) |
|---|---|---|---|
| train | 2023-06-01, 2023-06-02 | 17 789 682 | 199 930 |
| test  | 2023-06-03             | 8 001 918  | 199 463 |

Tipos de buque con estadísticas ajustadas en el train: **67**.

## RUN 1 — `baseline_zscore` (línea base interpretable, sin entrenamiento)

- **`run_id` real:** `a0cca5b5a4bd4373a7c0cd8ed04dc753` (`status = FINISHED`).
- **Regla:** `clasificar_por_umbral_z` -> `|z_sog| > 3.0` por tipo de buque
  (`z_sog = (SOG - media_tipo) / max(std_tipo, 1e-6)`, stats SOLO del train).
- **Parámetros:** `umbral_zscore = 3.0`, `features_orden = [z_sog, delta_sog, hora_sin, hora_cos]`,
  `train_dates = [2023-06-01, 2023-06-02]`, `test_date = 2023-06-03`, `semilla = 42`.
- **Métricas reales:**

  | métrica | valor |
  |---|---|
  | `pct_flagged_train` | 0.015145300855299354 |
  | `pct_flagged_test`  | 0.012027293282463413 |
  | `n_train` | 199 930 |
  | `n_test`  | 199 463 |

- **Tags:** `modelo=baseline_zscore`, `caso=anomalias_sog`, `req=6`,
  `antifuga=stats_solo_train`, `particion=temporal`.

## RUN 2 — `isolation_forest` (CAMPEÓN) — con `signature` + `input_example`

- **`run_id` real:** `be8f2ef2c2634efcaffa7fd89d3daf8f` (`status = FINISHED`).
- **`artifact_uri`:** `dbfs:/databricks/mlflow-tracking/2474832545018011/be8f2ef2c2634efcaffa7fd89d3daf8f/artifacts`.
- **Modelo:** `sklearn.ensemble.IsolationForest(n_estimators=100, contamination=0.01, random_state=42)`
  entrenado sobre la muestra del train (orden fijo `[z_sog, delta_sog, hora_sin, hora_cos]`).
- **Artefacto de modelo:** `mlflow.sklearn.log_model(name="modelo", signature=..., input_example=...)`
  con `signature = infer_signature(X_test, pred_test)` e `input_example = X_test.head()`.
- **Parámetros:** `n_estimators = 100`, `contamination = 0.01`, `random_state = 42`,
  `features_orden = [z_sog, delta_sog, hora_sin, hora_cos]`, `train_dates`, `test_date`.
- **Métricas reales:**

  | métrica | valor | interpretación |
  |---|---|---|
  | `pct_flagged_train` | 0.010003501225428901 | ~1.00 %, coherente con `contamination=0.01` |
  | `pct_flagged_test`  | 0.008883853145696194 | ~0.89 % de posiciones anómalas en el test |
  | `separacion_scores` | 0.20696867264662072  | diferencia de medias de `score_samples` entre normales y anómalas (mayor = mejor separación) |
  | `acuerdo_con_baseline` | 0.9886244566661486 | 98.86 % de coincidencia con la regla `|z|>3` (proxy de recall) |
  | `n_train` | 199 930 | |
  | `n_test`  | 199 463 | |

- **Tags:** `modelo=isolation_forest`, `caso=anomalias_sog`, `req=6`,
  `antifuga=stats_solo_train`, `particion=temporal`, `campeon=true`.

## Elección del campeón

Se elige **IsolationForest** como campeón (no la línea base) porque:
1. Produce un **score continuo** (`score_samples`) además de la etiqueta, útil para
   priorizar/ordenar anomalías en el dashboard (Req. 4); la regla `|z|>3` solo da
   binario.
2. Tiene `contamination` **explícito y reproducible** (`random_state=42`): la tasa de
   anomalías es un parámetro de diseño, no un efecto del umbral.
3. Mantiene **alto acuerdo con la línea base** (`acuerdo_con_baseline = 0.9886`), lo que
   da confianza de que no se aparta del criterio interpretable, y aporta separación de
   scores positiva (`0.2070`).

La comparación con la línea base sigue registrada en el experimento (run
`a0cca5b5a4bd4373a7c0cd8ed04dc753`) como contraste requerido por la rúbrica.

## Reproducibilidad (D7)

- Semilla única `42` en el muestreo (`.sample(fraction, seed=42)`) y en el modelo
  (`random_state=42`).
- Versiones EXACTAS resueltas en serverless y pinneadas en el notebook y en
  `conf/config.yaml` (`ml.pip_pins`): `mlflow==3.18.0`, `scikit-learn==1.3.0`,
  `numpy==1.26.4`, `pandas==1.5.3` (Python 3.11.10).
- Confirmado como paso EJECUTADO: `mlflow.sklearn.log_model` y `mlflow.register_model`
  funcionan en serverless con `set_registry_uri("databricks-uc")` (ver
  `registro_modelo_uc.md`).
