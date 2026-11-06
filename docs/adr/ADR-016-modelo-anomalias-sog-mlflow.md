# ADR-016: Modelo de anomalías de velocidad (SOG) con MLflow, registro en Unity Catalog e integración en el Job

- **Estado:** Aceptado
- **Fecha:** 2026-11-05
- **Requisito(s) de la rúbrica:** Req. 6 (10%)
- **Autores:** Alejandro Abril (202224328), David Fernando Caro (202222073), Juan David Torres Albarracín (202317608)

## 1. Contexto y problema
El Req. 6 pide un componente de Machine Learning versionado con MLflow. El caso ya quedó
**fijado por [ADR-005](ADR-005-eleccion-ml-anomalias-velocidad.md)**: detección de anomalías
de velocidad (SOG), no pronóstico. ADR-005 quedó en estado *Diseñado (no ejecutado)*; este ADR
lo **implementa y ejecuta** de verdad. El objetivo de la rúbrica es **demostrar la integración
MLOps** (features del Lakehouse → experimento MLflow → modelo en Unity Catalog → tarea en el
Job que puebla una tabla de oro), **no maximizar la precisión**: el modelo es deliberadamente
simple. Restricciones del entorno: cómputo **solo serverless** (`notebook_task` sin
`new_cluster`), donde `df.write.saveAsTable` sobre volúmenes grandes da `INTERNAL_ERROR` en
Spark Connect y el camino fiable de escritura es SQL (CTAS/INSERT); MLflow 3 en Databricks con
**Models in Unity Catalog** (registry URI `databricks-uc`, alias en lugar de stages, nombres
*three-part* `catalogo.esquema.objeto`). Hay que decidir y justificar: features, modelo
campeón, estrategia antifuga, registro en UC e integración en el pipeline.

## 2. Criterios de decisión
- **Interpretabilidad** del resultado (anomalías explicables al operador del dashboard, Req. 4).
- **Antifuga** (data leakage): el ajuste no puede ver datos del futuro ni del mismo buque en test.
- **Reproducibilidad** con MLflow + Unity Catalog (semilla fija, versiones pinneadas, alias).
- **Coste/viabilidad en serverless** (entrenamiento acotado, sin escritura Delta masiva).
- **Integración en el Job** existente (`oceanwatch_pipeline_entrega2`) sin romper lo anterior.

## 3. Alternativas consideradas
| Alternativa | Descripción | Pros | Contras | Evidencia |
|---|---|---|---|---|
| A. Línea base z-score por tipo `\|z_sog\|>3` | Regla estadística sin entrenamiento: anomalía = desviación >3σ respecto a la media/std de SOG del mismo `VesselType`, calculadas solo con el train | Totalmente interpretable; sin dependencia de modelo; barata | Solo salida binaria (sin score continuo para priorizar); umbral rígido | Distribución real de SOG ([`evidence/req06-ml-mlflow/distribucion_sog_insumo_modelo.md`](../../evidence/req06-ml-mlflow/distribucion_sog_insumo_modelo.md)); run MLflow `baseline_zscore` `a0cca5b5a4bd4373a7c0cd8ed04dc753` |
| B. IsolationForest por features (**ELEGIDA — campeón**) | `sklearn.ensemble.IsolationForest(n_estimators=100, contamination=0.01, random_state=42)` sobre `[z_sog, delta_sog, hora_sin, hora_cos]` | Score continuo (`score_samples`) para priorizar; `contamination` explícito y reproducible; alto acuerdo con la base | Requiere entrenamiento y artefacto versionado; menos directamente legible que la regla | Run MLflow `isolation_forest` `be8f2ef2c2634efcaffa7fd89d3daf8f`; `acuerdo_con_baseline=0.9886`, `separacion_scores=0.2070`; [Models in Unity Catalog](https://docs.databricks.com/aws/en/machine-learning/manage-model-lifecycle/) (acceso 2026-11-05) |
| C. LOF / z-score robusto (MAD) | Local Outlier Factor o z-score robusto por mediana/MAD | LOF capta densidad local; MAD es robusto a colas | LOF no expone `score_samples` tan directo para inferir sobre datos nuevos; z-score robusto es equivalente a la línea base A | [scikit-learn IsolationForest](https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.IsolationForest.html) (acceso 2026-11-05) |

## 4. Decisión
Se implementa un **detector de anomalías de velocidad relativa al tipo de buque** con el
vector de features de orden fijo **`[z_sog, delta_sog, hora_sin, hora_cos]`** (z-score de SOG
por `VesselType`, diferencia de SOG respecto a la posición previa del mismo MMSI, y hora del día
codificada de forma cíclica). El experimento MLflow compara la **línea base z-score** (A) contra
un **IsolationForest** (B); se elige **IsolationForest como campeón**. El modelo se registra en
Models in Unity Catalog como **`oceanwatch.ml.detector_anomalias_velocidad`** con el alias
**`@champion`**, y se integra como la tarea **`ml_anomalias`** del Job (depende de `oro`) que
carga el modelo por alias y puebla la tabla **`oceanwatch.gold.anomalias_velocidad`** para
`fecha_proceso`, con auditoría en `oceanwatch.quality.ejecuciones_job`.

## 5. Justificación basada en evidencia
- **Features por tipo.** La distribución real de SOG (verificada sobre `silver.ais_clean`,
  warehouse `377600528fed7c47`) muestra que la normalidad depende del `VesselType`: cargo (70)
  y tanker (80) navegan a 6+ kn de media, mientras towing (31) y pleasure (37) rondan 1.5 kn; una
  misma velocidad es normal en uno y anómala en otro. Por eso la feature central es el
  **z-score por tipo** `z_sog = (SOG - media_tipo) / max(std_tipo, 1e-6)`
  ([`distribucion_sog_insumo_modelo.md`](../../evidence/req06-ml-mlflow/distribucion_sog_insumo_modelo.md)).
- **Campeón IsolationForest.** En el experimento real, el IsolationForest mantiene un
  **acuerdo del 98.86 %** con la regla interpretable `|z|>3` (`acuerdo_con_baseline=0.9886244566661486`),
  lo que da confianza de que no se aparta del criterio legible, y además aporta un **score
  continuo** (`separacion_scores=0.20696867264662072`) y una tasa de anomalías controlada por
  `contamination=0.01` (`pct_flagged_test=0.008883853145696194`, ~0.89 %). La línea base queda
  registrada como contraste (`pct_flagged_test=0.012027293282463413`). Métricas reales del
  experimento `/Users/jd.torresa1@uniandes.edu.co/oceanwatch_anomalias_sog`
  (`experiment_id=2474832545018011`), ver
  [`experimento_mlflow.md`](../../evidence/req06-ml-mlflow/experimento_mlflow.md).
- **Antifuga por partición temporal.** Las estadísticas por tipo y el IsolationForest se ajustan
  **solo con el train** (`_batch_date` 2023-06-01/02); el test/inferencia aplica lo ya ajustado
  sobre 2023-06-03. La partición es temporal (no aleatoria) para no filtrar futuro→pasado del
  mismo buque y para reproducir el escenario real de inferencia batch.
- **Registro en UC.** `mlflow.set_registry_uri("databricks-uc")` +
  `mlflow.register_model("runs:/be8f2ef2c2634efcaffa7fd89d3daf8f/modelo", ...)` + alias
  `@champion` funcionan en serverless (verificado contra la MLflow REST API de UC), usando
  **alias** y no stages, conforme a
  [Models in Unity Catalog](https://docs.databricks.com/aws/en/machine-learning/manage-model-lifecycle/)
  (acceso 2026-11-05).

## 6. Patrón de diseño / ingeniería de datos aplicado
- **Anomaly Detection no supervisado** (IsolationForest sobre features cinemáticas).
- **Feature engineering reproducible**: funciones puras en `src/oceanwatch/ml/features.py`
  replicadas como expresiones Spark nativas en los notebooks (mismo contrato de orden de vector).
- **Model Registry (Unity Catalog) con alias** (`@champion`) como *single source of truth* del
  modelo en producción.
- **Batch Inference en pipeline (Orchestration DAG)**: la inferencia es una tarea más del Job,
  encadenada tras `oro`, idempotente por `DELETE`+`INSERT` de la partición `_batch_date`.

## 7. Consecuencias
- **Positivas:** integración MLOps completa y ejecutada (features → experimento → modelo UC →
  tarea del Job → tabla de oro consumible por el dashboard); modelo simple, interpretable y
  reproducible; la tabla de anomalías queda en `gold` para que el visual del Req. 4 la consuma
  directamente.
- **Negativas / deuda:** el modelo entrena sobre una **muestra reproducible** (~200k filas por la
  fricción de escritura masiva en serverless); la inferencia usa `scikit-learn==1.5.2` mientras el
  entrenamiento usó `1.3.0` (por la ausencia de wheel de 1.3.x para Python 3.12 del entorno del
  Job) — se verificó que el modelo serializado predice idéntico bajo ambas (solo
  `InconsistentVersionWarning`, silenciado). El reentrenamiento programado del modelo queda fuera
  del alcance de esta entrega.

## 8. Cómo se verifica / reversibilidad
- **Experimento:** `/Users/jd.torresa1@uniandes.edu.co/oceanwatch_anomalias_sog` con 2 runs reales
  (`a0cca5b5a4bd4373a7c0cd8ed04dc753` baseline, `be8f2ef2c2634efcaffa7fd89d3daf8f` IsolationForest).
- **Modelo en UC:** `oceanwatch.ml.detector_anomalias_velocidad` versión **1** (`status=READY`),
  alias `@champion` → versión 1 (verificado por la MLflow REST API de UC).
- **Tabla poblada:** `oceanwatch.gold.anomalias_velocidad` para `_batch_date=2023-06-03` con
  **67 828** filas anómalas (~0.85 % de las 8 001 918 posiciones puntuadas del día).
- **Corrida del Job SUCCESS:** `oceanwatch_pipeline_entrega2` (`job_id 202547459111516`),
  `run_id` **483234774995672** → `TERMINATED`/`SUCCESS` con las **5 tareas** SUCCESS
  (ingesta, plata_calidad, conciliacion, oro, ml_anomalias); las 4 previas intactas.
- **Reversibilidad:** archivar el experimento y el modelo registrado (o quitar el alias
  `@champion`) y eliminar la tarea `ml_anomalias` del Job y la tabla `gold.anomalias_velocidad`;
  el resto del pipeline (bronce/plata/oro) no depende del modelo.

## 9. Referencias
- MLflow en Databricks: https://docs.databricks.com/aws/en/mlflow/ (acceso 2026-11-05).
- Models in Unity Catalog (registro, alias, nombres three-part): https://docs.databricks.com/aws/en/machine-learning/manage-model-lifecycle/ (acceso 2026-11-05).
- scikit-learn `IsolationForest`: https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.IsolationForest.html (acceso 2026-11-05).
- [ADR-005](ADR-005-eleccion-ml-anomalias-velocidad.md) (elección del caso de ML: anomalías SOG).
- [ADR-013](ADR-013-diseno-job-orquestacion.md) y [ADR-014](ADR-014-idempotencia-reintentos.md) (diseño del Job, idempotencia y reintentos: base de la tarea `ml_anomalias`).
- Evidencia: [`experimento_mlflow.md`](../../evidence/req06-ml-mlflow/experimento_mlflow.md), [`registro_modelo_uc.md`](../../evidence/req06-ml-mlflow/registro_modelo_uc.md), [`integracion_job_ml.md`](../../evidence/req06-ml-mlflow/integracion_job_ml.md), [`tabla_anomalias.md`](../../evidence/req06-ml-mlflow/tabla_anomalias.md), [`reproducibilidad.md`](../../evidence/req06-ml-mlflow/reproducibilidad.md), [`distribucion_sog_insumo_modelo.md`](../../evidence/req06-ml-mlflow/distribucion_sog_insumo_modelo.md).
