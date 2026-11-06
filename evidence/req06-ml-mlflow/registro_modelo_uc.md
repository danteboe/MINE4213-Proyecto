# Evidencia — Registro del modelo campeón en Unity Catalog (Req. 6)

- **Qué demuestra:** que el modelo campeón (IsolationForest) está registrado de verdad
  en **Models in Unity Catalog** con una versión real y el alias `@champion`, y que la
  descripción incluye trazabilidad al run MLflow y a los datos (fechas de train/test,
  features, semilla). Confirma que `mlflow.register_model` + alias de UC funcionan en
  serverless (no stages deprecados).
- **Cómo se generó:** desde el mismo notebook serverless `fase6_entrenamiento_mlflow`
  (one-time run `347173419720341`, task run `386348858221273`):
  `mlflow.set_registry_uri("databricks-uc")`;
  `mlflow.register_model("runs:/be8f2ef2c2634efcaffa7fd89d3daf8f/modelo", "oceanwatch.ml.detector_anomalias_velocidad")`;
  `MlflowClient().set_registered_model_alias(..., alias="champion", version=1)`;
  `update_registered_model` / `update_model_version` con la descripción de trazabilidad.
  Verificado aparte contra la **MLflow REST API de Unity Catalog** con el token exportado
  FUERA del repo desde `databricks.key`.
- **Fecha:** 2026-11-05.
- **Estado:** Verificado (ejecutado).

## Modelo registrado (nombre three-part, MLflow 3 / UC)

- **Nombre:** `oceanwatch.ml.detector_anomalias_velocidad` (catálogo `oceanwatch`,
  esquema `ml`, objeto `detector_anomalias_velocidad`).
- **Versión:** `1` — `status = READY`.
- **`run_id` de origen (version 1):** `be8f2ef2c2634efcaffa7fd89d3daf8f` (run
  `isolation_forest` del experimento `/Users/jd.torresa1@uniandes.edu.co/oceanwatch_anomalias_sog`).
- **Alias:** `champion` -> versión `1`.

## Verificación literal (MLflow REST API de Unity Catalog)

`GET /api/2.0/mlflow/unity-catalog/registered-models/get?name=oceanwatch.ml.detector_anomalias_velocidad`

```
name=oceanwatch.ml.detector_anomalias_velocidad aliases={"alias":"champion","version":"1"}
```

`GET /api/2.0/mlflow/unity-catalog/registered-models/alias?name=oceanwatch.ml.detector_anomalias_velocidad&alias=champion`

```
alias champion -> version 1 run_id=be8f2ef2c2634efcaffa7fd89d3daf8f
```

`GET /api/2.0/mlflow/unity-catalog/model-versions/get?name=oceanwatch.ml.detector_anomalias_velocidad&version=1`

```
v1 status=READY run_id=be8f2ef2c2634efcaffa7fd89d3daf8f
```

Dentro del propio notebook también se verificó con
`MlflowClient().get_model_version_by_alias("oceanwatch.ml.detector_anomalias_velocidad", "champion")`,
que devolvió `version = 1` (campo `alias_apunta_a_version` del `notebook_output`).

## Descripción / trazabilidad registrada en la versión

> Detector de anomalias de velocidad SOG relativa al tipo de buque (ADR-005/ADR-016).
> IsolationForest(n_estimators=100, contamination=0.01, random_state=42). Entrenado con
> _batch_date [2023-06-01, 2023-06-02] (antifuga: stats por tipo y modelo SOLO del
> train); evaluado con 2023-06-03. Vector de features (orden fijo) = [z_sog, delta_sog,
> hora_sin, hora_cos]. Run MLflow: be8f2ef2c2634efcaffa7fd89d3daf8f. Experimento:
> /Users/jd.torresa1@uniandes.edu.co/oceanwatch_anomalias_sog. Semilla 42.

## Confirmación serverless (parte de ejecución de E)

- `mlflow.sklearn.log_model` (con `signature` e `input_example`): EJECUTADO en serverless
  (artefacto en `dbfs:/databricks/mlflow-tracking/2474832545018011/be8f2ef2c2634efcaffa7fd89d3daf8f/artifacts`).
- `mlflow.register_model` + `set_registered_model_alias` con `set_registry_uri("databricks-uc")`:
  EJECUTADO en serverless (versión 1 READY, alias `@champion`).
- Se usan **ALIAS** (`@champion`), no stages (deprecados en Models in Unity Catalog).

## No se modificó ninguna tabla existente

`SELECT count(*) FROM oceanwatch.silver.ais_clean` = **25 857 101** (igual que antes de
FEAT-002; warehouse serverless `377600528fed7c47`). FEAT-002 solo creó un experimento
MLflow y un modelo en el esquema `ml` (antes vacío); no tocó bronce/plata/oro/quality/
quarantine ni las 4 tareas del Job.
